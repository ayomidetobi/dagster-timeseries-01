"""Logic for loading meta series from CSV to S3 control tables.

Uses S3 Parquet files as the system of record for control tables.
DuckDB is used only as a query engine with views over S3 control tables.

Data flow:
- CSV → DuckDB temp table (via read_csv) → validation → S3 Parquet control table (versioned)
- DuckDB views are updated to point to the latest S3 control table version.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource

if TYPE_CHECKING:
    from .config import MetaSeriesCSVConfig
from dagster_quickstart.utils.constants import (
    CODE_BASED_LOOKUPS,
    LOOKUP_TABLE_PROCESSING_ORDER,
    S3_CONTROL_LOOKUP,
    S3_PARQUET_FILE_NAME,
)

# from dagster_quickstart.utils.exceptions import (
#     CSVValidationError,
#     DatabaseQueryError,  # Raised by helper functions
#     ReferentialIntegrityError,  # Raised by validate_referential_integrity_sql
# )
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_control_table_path,
    process_csv_to_s3_with_validation,
    validate_referential_integrity_sql,
)
from database.ddl import (
    META_SERIES_RESULTS_QUERY,
    META_SERIES_VALIDATION_CONDITION,
    META_SERIES_VALIDATION_QUERY,
)

if TYPE_CHECKING:
    from database.meta_series import MetaSeriesManager


from ..utils.load_data import get_available_columns


def load_meta_series_logic(
    context: AssetExecutionContext,
    config: "MetaSeriesCSVConfig",  # type: ignore[name-defined]
    duckdb: DuckDBResource,
    version_date: str,
    meta_manager: "MetaSeriesManager",
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """Load meta series from CSV file and write to S3 control table.

    Data flow: CSV → DuckDB temp table (via read_csv) → validation → S3 Parquet control table (versioned)
    DuckDB views are updated to point to the latest S3 control table version.

    Args:
        context: Dagster execution context.
        config: Meta series CSV configuration.
        duckdb: DuckDB resource.
        version_date: Version date for this run.
        meta_manager: MetaSeriesManager instance (initialized in asset).

    Returns:
        Tuple of (results dictionary for DataFrame conversion, results dictionary mapping series_code -> row_index).
    """
    context.log.info(f"Loading meta series from {config.csv_path}")

    # Process CSV → S3 control table (versioned, immutable)
    # CSV is loaded directly into DuckDB temp table, no pandas involved
    context.log.info("Using CSV → S3 control table flow (versioned, immutable)")
    results = process_csv_to_s3_control_table_meta_series(
        context, duckdb, config.csv_path, version_date, meta_manager
    )

    # Build results dictionary for DataFrame conversion (done in assets.py if needed)
    result_dict = {
        "series_code": list(results.keys()),
        "row_index": list(results.values()),
    }

    return result_dict, results


def _build_meta_series_validation_query(
    temp_table: str,
    duckdb: DuckDBResource,
    version_date: str,
    context: Optional[AssetExecutionContext] = None,
) -> str:
    """Build SQL query to validate meta series against lookup tables.

    Reads directly from S3 Parquet control table instead of views, since views
    may not exist in a fresh DuckDB connection. Only validates lookup types that
    have corresponding columns in the temp table.

    Args:
        temp_table: Temporary table name with meta series data.
        duckdb: DuckDB resource to get bucket and build S3 path.
        context: Optional Dagster context for logging.

    Returns:
        SQL query string to find invalid references.
    """
    # Get available columns from temp table - only validate lookup types that exist
    available_columns = get_available_columns(duckdb, temp_table, LOOKUP_TABLE_PROCESSING_ORDER)

    # Get the latest lookup table version from S3
    # Use the provided version date (lookup tables should be loaded first)
    relative_path = build_s3_control_table_path(
        S3_CONTROL_LOOKUP, version_date, S3_PARQUET_FILE_NAME
    )
    full_s3_path = build_full_s3_path(duckdb, relative_path)

    validation_parts: List[str] = []

    # Only validate lookup types that have corresponding columns in the temp table
    for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
        if lookup_type not in available_columns:
            continue  # Skip validation for columns that don't exist in CSV

        # Determine which canonical column to use for validation
        # Code-based lookups use 'code', simple lookups use 'name'
        if lookup_type in CODE_BASED_LOOKUPS:
            # Code-based: validate against 'code' column in canonical format
            canonical_column = "code"
        else:
            # Simple: validate against 'name' column in canonical format
            canonical_column = "name"

        # Build validation condition that reads from S3 Parquet
        # The S3 control table has canonical format: lookup_type, code, name
        validation_parts.append(
            META_SERIES_VALIDATION_CONDITION.format(
                lookup_type=lookup_type,
                full_s3_path=full_s3_path,
                canonical_column=canonical_column,
            ).strip()
        )

    if not validation_parts:
        # No lookup columns to validate, return query that always passes
        return f"SELECT series_code FROM {temp_table} WHERE 1=0"

    validation_sql = " AND ".join(validation_parts)

    # Build select columns list dynamically based on available columns
    # Always include series_code, then add lookup columns that exist
    select_column_list = ["ms.series_code"]
    for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
        if lookup_type in available_columns:
            select_column_list.append(f"ms.{lookup_type}")

    select_columns = ", ".join(select_column_list)

    # Build validation query
    invalid_query = META_SERIES_VALIDATION_QUERY.format(
        select_columns=select_columns,
        temp_table=temp_table,
        validation_sql=validation_sql,
    ).strip()

    return invalid_query


def _validate_meta_series_against_lookup_tables(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
    version_date: str,
) -> None:
    """Validate meta series data against lookup tables using SQL.

    Validates that all lookup references in meta series exist in lookup tables.
    Reads directly from S3 Parquet control table instead of views, since views
    may not exist in a fresh DuckDB connection.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource with S3 access.
        temp_table: Temporary table name with meta series data.
        version_date: Version date for the lookup table S3 path.

    Raises:
        ReferentialIntegrityError: If validation fails.
    """
    validation_query = _build_meta_series_validation_query(
        temp_table, duckdb, version_date, context
    )
    validate_referential_integrity_sql(
        duckdb=duckdb,
        temp_table=temp_table,
        validation_query=validation_query,
        context=context,
    )


def process_csv_to_s3_control_table_meta_series(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    csv_path: str,
    version_date: str,
    meta_manager: "MetaSeriesManager",  # type: ignore[name-defined]
) -> Dict[str, int]:
    """Process meta series from CSV to S3 control table (versioned, immutable).

    Data flow: CSV → DuckDB temp table (via read_csv) → validation → S3 Parquet control table
    DuckDB view is created/updated to point to latest S3 control table version.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource with S3 access.
        csv_path: Path to CSV file.
        version_date: Version date for this run.
        meta_manager: MetaSeriesManager instance (initialized in asset).

    Returns:
        Dictionary mapping series_code -> row_index (deterministic based on order).

    Raises:
        ReferentialIntegrityError: If referential integrity validation fails.
        DatabaseQueryError: If write operation fails.
        CSVValidationError: If CSV cannot be read.
    """
    context.log.info("Processing meta series from CSV to S3 control table (versioned)")

    # Create wrapper function to match helper's expected signature
    def save_to_s3_wrapper(
        duckdb: DuckDBResource, temp_table: str, version_date: str, context=None, **kwargs: Any
    ) -> str:
        return meta_manager.save_meta_series_to_s3(duckdb, temp_table, version_date, context)

    # Use generic helper for CSV → validate → write → create view flow
    # Note: View creation is disabled here since it's handled in the asset function
    process_csv_to_s3_with_validation(
        context=context,
        duckdb=duckdb,
        csv_path=csv_path,
        version_date=version_date,
        save_to_s3_func=save_to_s3_wrapper,
        create_view_func=None,  # View creation handled in asset
        validation_func=_validate_meta_series_against_lookup_tables,
        view_creation_enabled=False,  # View creation handled in asset
    )

    # Build results dictionary (series_code -> row_index) from saved S3 data
    # Read from S3 control table - row_index is computed based on saved order
    from dagster_quickstart.utils.constants import S3_CONTROL_METADATA_SERIES, S3_PARQUET_FILE_NAME
    from dagster_quickstart.utils.s3_helpers import build_full_s3_path

    relative_path = build_s3_control_table_path(
        S3_CONTROL_METADATA_SERIES, version_date, S3_PARQUET_FILE_NAME
    )
    full_s3_path = build_full_s3_path(duckdb, relative_path)

    # Build results from saved S3 file
    results_query = META_SERIES_RESULTS_QUERY.format(temp_table=f"read_parquet('{full_s3_path}')")
    result = duckdb.execute_query(results_query)

    if result is None or result.empty:
        context.log.warning("No results found in saved S3 control table")
        return {}

    results = {}
    for _, row in result.iterrows():
        series_code = str(row["series_code"]).strip()
        if series_code:
            results[series_code] = int(row["row_index"])

    context.log.info(f"Loaded {len(results)} meta series records to S3 control table")
    return results


def _build_meta_series_results(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
) -> Dict[str, int]:
    """Build results dictionary mapping series_code -> row_index from temp table.

    Uses SQL to get deterministic row indices based on order in CSV.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Temporary table name with meta series data.

    Returns:
        Dictionary mapping series_code -> row_index (1-based).
    """
    query = META_SERIES_RESULTS_QUERY.format(temp_table=temp_table)

    result = duckdb.execute_query(query)
    if result is None or result.empty:
        return {}

    results = {}
    for _, row in result.iterrows():
        series_code = str(row["series_code"]).strip()
        if series_code:
            results[series_code] = int(row["row_index"])

    return results
