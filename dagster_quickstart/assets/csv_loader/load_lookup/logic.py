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
from dagster_quickstart.utils.duckdb_helpers import (
    load_csv_to_temp_table,
    unregister_temp_table,
)
from dagster_quickstart.utils.exceptions import (
    CSVValidationError,
    DatabaseQueryError,
)
from database.ddl import (
    EXTRACT_LOOKUP,
    LOOKUP_RESULTS_QUERY,
)

if TYPE_CHECKING:
    from database.lookup_tables import LookupTableManager

from ..utils.load_data import check_temp_table_has_data, get_available_columns


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


def _save_all_lookups_to_s3(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    all_lookup_temp_tables: Dict[str, str],
    version_date: str,
    lookup_manager: "LookupTableManager",
) -> Dict[str, Dict[str, int]]:
    """Save all lookup tables to S3 and build results dictionary.

    Helper function compatible with process_csv_to_s3_with_validation style.
    Writes all lookup temp tables to S3 via LookupTableManager and builds
    results dictionary mapping lookup_type -> {name: row_index}.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource with S3 access.
        all_lookup_temp_tables: Dictionary mapping lookup_type -> temp table name.
        version_date: Version date for this run.
        lookup_manager: LookupTableManager instance.

    Returns:
        Dictionary mapping lookup_type -> {name: row_index}.

    Raises:
        DatabaseQueryError: If write operation fails.
    """
    if not all_lookup_temp_tables:
        context.log.warning(
            "No lookup tables to save",
            extra={"version_date": version_date},
        )
        return {}

    all_results: Dict[str, Dict[str, int]] = {}

    # Build results for each lookup type before saving
    for lookup_type, temp_lookup_table in all_lookup_temp_tables.items():
        results = _build_lookup_results(context, duckdb, temp_lookup_table, lookup_type)
        all_results[lookup_type] = results
        context.log.info(
            "Extracted lookup records",
            extra={"lookup_type": lookup_type, "record_count": len(results)},
        )

    # Write all lookup tables to S3 control table
    lookup_manager.save_lookup_tables_to_s3(duckdb, all_lookup_temp_tables, version_date, context)

    total_records = sum(len(v) for v in all_results.values())
    context.log.info(
        "Loaded lookup records to S3 control table",
        extra={
            "total_record_count": total_records,
            "lookup_type_count": len(all_results),
            "version_date": version_date,
        },
    )

    return all_results


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


def load_staging_temp_table(
    duckdb: DuckDBResource,
    csv_path: str,
    context: AssetExecutionContext,
) -> str:
    """Load CSV file into staging temp table.

    Wraps load_csv_to_temp_table for consistent usage.

    Args:
        duckdb: DuckDB resource with S3 access
        csv_path: Path to CSV file
        context: Dagster execution context

    Returns:
        Name of the staging temp table

    Raises:
        CSVValidationError: If CSV cannot be read
    """
    return load_csv_to_temp_table(duckdb, csv_path, NULL_VALUE_REPRESENTATION, context)


def extract_all_lookup_temp_tables(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    staging_temp_table: str,
    available_columns: list[str],
) -> Dict[str, str]:
    """Extract all lookup types from staging temp table to individual temp tables.

    Loops over LOOKUP_TABLE_PROCESSING_ORDER, extracts each lookup type that exists
    in available_columns, and returns dictionary mapping lookup_type -> temp table name.

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        staging_temp_table: Name of staging temp table with CSV data
        available_columns: List of column names available in staging temp table

    Returns:
        Dictionary mapping lookup_type -> temp table name (only includes tables with data)

    Raises:
        DatabaseQueryError: If extraction fails
    """
    all_lookup_temp_tables: Dict[str, str] = {}

    # Extract all lookup types from staging temp table
    for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
        if lookup_type not in available_columns:
            continue

        try:
            # Extract distinct lookup values for this type to temp table
            temp_lookup_table = _extract_lookup_table_data_to_temp(
                context, duckdb, staging_temp_table, lookup_type
            )

            # Check if temp table has data using helper function
            if check_temp_table_has_data(duckdb, temp_lookup_table):
                all_lookup_temp_tables[lookup_type] = temp_lookup_table
            else:
                # Clean up empty temp table
                unregister_temp_table(duckdb, temp_lookup_table, context)
        except DatabaseQueryError:
            raise  # Re-raise domain-specific exceptions
        except Exception as e:
            error_msg = f"Unexpected error processing {lookup_type}: {e}"
            context.log.error(
                error_msg,
                extra={"lookup_type": lookup_type},
            )
            raise DatabaseQueryError(error_msg) from e

    return all_lookup_temp_tables


def process_csv_to_s3_control_table_lookup_tables(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    csv_path: str,
    version_date: str,
    lookup_manager: "LookupTableManager",
) -> Dict[str, Dict[str, int]]:
    """Process lookup tables from CSV to S3 control table (versioned, immutable).

    Data flow: CSV → DuckDB temp table (via load_csv_to_temp_table) →
    extract lookup types → S3 Parquet control table.
    DuckDB views are created/updated to point to latest S3 control table version.

    Uses load_csv_to_temp_table directly (consistent with process_csv_to_s3_with_validation
    pattern) for CSV loading, then extracts multiple lookup types before saving to S3.

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
    context.log.info(
        "Processing lookup tables from CSV to S3 control table (versioned)",
        extra={"csv_path": csv_path, "version_date": version_date},
    )

    # Load CSV into staging temp table
    staging_temp_table = load_staging_temp_table(duckdb, csv_path, context)

    try:
        # Get available columns from staging temp table
        available_columns = get_available_columns(duckdb, staging_temp_table, LOOKUP_TABLE_COLUMNS)

        if not available_columns:
            raise CSVValidationError(
                f"CSV file {csv_path} must have at least one lookup table column: {LOOKUP_TABLE_COLUMNS}"
            )

        # Extract all lookup types to individual temp tables
        all_lookup_temp_tables = extract_all_lookup_temp_tables(
            context, duckdb, staging_temp_table, available_columns
        )

        # Save all lookup tables to S3 and build results dictionary
        all_results = _save_all_lookups_to_s3(
            context, duckdb, all_lookup_temp_tables, version_date, lookup_manager
        )

        # Clean up lookup temp tables after saving
        for temp_lookup_table in all_lookup_temp_tables.values():
            unregister_temp_table(duckdb, temp_lookup_table, context)

        return all_results
    finally:
        # Clean up staging temp table
        unregister_temp_table(duckdb, staging_temp_table, context)
