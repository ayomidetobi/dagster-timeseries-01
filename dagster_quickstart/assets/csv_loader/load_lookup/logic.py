"""Logic for loading lookup tables from CSV to S3 control tables.

Uses S3 Parquet files as the system of record for control tables.
DuckDB is used only as a query engine with views over S3 control tables.

Data flow:
- CSV → DuckDB temp view (for validation) → S3 Parquet control table (versioned)
- DuckDB views are created/updated to point to latest S3 control table versions
"""

import uuid
from typing import TYPE_CHECKING, Dict

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    CODE_BASED_LOOKUPS,
    DB_COLUMNS,
    LOOKUP_TABLE_COLUMNS,
    LOOKUP_TABLE_PROCESSING_ORDER,
    NULL_VALUE_REPRESENTATION,
)
from dagster_quickstart.utils.exceptions import (
    CSVValidationError,
    DatabaseQueryError,
)
from dagster_quickstart.utils.helpers import (
    load_csv_to_temp_table,
    unregister_temp_table,
)
from database.ddl import (
    EXTRACT_LOOKUP,
    LOOKUP_RESULTS_QUERY,
)

if TYPE_CHECKING:
    from database.lookup_tables import LookupTableManager

from ..utils.load_data import get_available_columns


def _build_lookup_results(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_lookup_table: str,
    lookup_type: str,
) -> Dict[str, int]:
    """Build results dictionary mapping name -> row_index from lookup temp table.

    Uses SQL to get deterministic row indices based on order.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_lookup_table: Temporary table name with lookup data.
        lookup_type: Type of lookup table.

    Returns:
        Dictionary mapping name -> row_index (1-based).
    """
    id_column, name_column = DB_COLUMNS[lookup_type]

    # Determine which column to use for name
    if lookup_type in CODE_BASED_LOOKUPS:
        code_field, name_field, _ = CODE_BASED_LOOKUPS[lookup_type]
        # Use code_field for code-based lookups
        name_col = code_field
    else:
        name_col = name_column

    query = LOOKUP_RESULTS_QUERY.format(
        name_column=name_col,
        temp_lookup_table=temp_lookup_table,
    )

    result = duckdb.execute_query(query)
    if result is None or result.empty:
        return {}

    results = {}
    for _, row in result.iterrows():
        name_value = str(row["name_value"]).strip()
        if name_value:
            results[name_value] = int(row["row_index"])

    return results


def _build_extract_lookup_query(
    source_temp_table: str, lookup_type: str, staging_column: str
) -> str:
    """Build SQL query to extract distinct lookup values.

    Args:
        source_temp_table: Temporary table name with lookup data.
        lookup_type: Type of lookup table.
        staging_column: Column name in source table.

    Returns:
        SQL CREATE TABLE query string.
    """
    if lookup_type in CODE_BASED_LOOKUPS:
        code_field, name_field, _ = CODE_BASED_LOOKUPS[lookup_type]
        select_columns = f"{staging_column} AS {code_field}, {staging_column} AS {name_field}"
    else:
        _, name_column = DB_COLUMNS[lookup_type]
        select_columns = f"{staging_column} AS {name_column}"

    return EXTRACT_LOOKUP.format(
        lookup_type=lookup_type,
        uuid="{uuid}",
        staging_column=staging_column,
        select_columns=select_columns,
        source_temp_table=source_temp_table,
    )


def _extract_lookup_table_data_to_temp(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    source_temp_table: str,
    lookup_type: str,
) -> str:
    """Extract distinct lookup values for a specific lookup type to a temp table.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource with S3 access.
        source_temp_table: Temporary table name with lookup data.
        lookup_type: Type of lookup table.

    Returns:
        Temporary table name with extracted lookup data.

    Raises:
        DatabaseQueryError: If extraction fails.
    """
    staging_column = lookup_type
    # Generate UUID once and reuse it for both Python variable and SQL query
    table_uuid = uuid.uuid4().hex
    temp_lookup_table = f"_temp_lookup_{lookup_type}_{table_uuid}"

    query_template = _build_extract_lookup_query(source_temp_table, lookup_type, staging_column)
    create_query = query_template.replace("{uuid}", table_uuid).replace(
        "_temp_lookup_{lookup_type}_{{uuid}}", temp_lookup_table
    )

    try:
        duckdb.execute_command(create_query)
        return temp_lookup_table
    except Exception as e:
        error_msg = f"Error extracting {lookup_type} data: {e}"
        context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


def process_csv_to_s3_control_table_lookup_tables(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    csv_path: str,
    version_date: str,
    lookup_manager: "LookupTableManager",
) -> Dict[str, Dict[str, int]]:
    """Process lookup tables from CSV to S3 control table (versioned, immutable).

    Data flow: CSV → DuckDB temp table (via read_csv) → S3 Parquet control table
    DuckDB views are created/updated to point to latest S3 control table version.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource with S3 access.
        csv_path: Path to CSV file.
        version_date: Version date for this run.
        lookup_manager: LookupTableManager instance (initialized in asset).

    Returns:
        Dictionary mapping lookup_type -> {name: row_index}.

    Raises:
        DatabaseQueryError: If write operation fails.
        CSVValidationError: If CSV cannot be read or has no valid columns.
    """
    context.log.info("Processing lookup tables from CSV to S3 control table (versioned)")

    # Load CSV directly into DuckDB temp table (no pandas)
    temp_table = load_csv_to_temp_table(duckdb, csv_path, NULL_VALUE_REPRESENTATION, context)

    try:
        # Get available columns from temp table
        available_columns = get_available_columns(duckdb, temp_table, LOOKUP_TABLE_COLUMNS)

        if not available_columns:
            raise CSVValidationError(
                f"CSV file {csv_path} must have at least one lookup table column: {LOOKUP_TABLE_COLUMNS}"
            )

        all_results: Dict[str, Dict[str, int]] = {}
        all_lookup_temp_tables: Dict[str, str] = {}

        # Extract all lookup types first
        for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
            if lookup_type not in available_columns:
                continue

            try:
                # Extract distinct lookup values for this type to temp table
                temp_lookup_table = _extract_lookup_table_data_to_temp(
                    context, duckdb, temp_table, lookup_type
                )

                # Check if temp table has data
                count_query = f"SELECT COUNT(*) as cnt FROM {temp_lookup_table}"
                count_result = duckdb.execute_query(count_query)
                has_data = (
                    count_result is not None
                    and not count_result.empty
                    and count_result.iloc[0]["cnt"] > 0
                )

                if has_data:
                    all_lookup_temp_tables[lookup_type] = temp_lookup_table

                    # Build results dictionary (name -> row_index) using SQL
                    results = _build_lookup_results(context, duckdb, temp_lookup_table, lookup_type)
                    all_results[lookup_type] = results

                    context.log.info(
                        f"Extracted {len(results)} {lookup_type} records",
                        extra={"lookup_type": lookup_type, "record_count": len(results)},
                    )
                else:
                    # Clean up empty temp table
                    unregister_temp_table(duckdb, temp_lookup_table, context)
            except DatabaseQueryError:
                raise  # Re-raise domain-specific exceptions
            except Exception as e:
                error_msg = f"Unexpected error processing {lookup_type}: {e}"
                context.log.error(error_msg)
                raise DatabaseQueryError(error_msg) from e

        # Write all lookup tables to a single S3 control table file
        if all_lookup_temp_tables:
            # Use LookupTableManager for CRUD operations (passed from asset)
            lookup_manager.save_lookup_tables_to_s3(
                duckdb, all_lookup_temp_tables, version_date, context
            )

            # Note: View creation is handled in the asset function, not here

            # Clean up lookup temp tables
            for temp_lookup_table in all_lookup_temp_tables.values():
                unregister_temp_table(duckdb, temp_lookup_table, context)

        context.log.info(
            f"Loaded {sum(len(v) for v in all_results.values())} total lookup records to S3 control table"
        )
        return all_results
    finally:
        # Clean up main temp table
        unregister_temp_table(duckdb, temp_table, context)
