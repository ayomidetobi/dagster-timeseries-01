"""Logic for loading series dependencies from CSV to S3 control tables.

Uses S3 Parquet files as the system of record for control tables.
DuckDB is used only as a query engine with views over S3 control tables.

Data flow:
- CSV → DuckDB temp table (via read_csv) → validation → S3 Parquet control table (versioned)
- DuckDB views are updated to point to the latest S3 control table version.
"""

from typing import TYPE_CHECKING, Any, Dict, Tuple

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.csv_loader_helpers import process_csv_to_s3_with_validation
from dagster_quickstart.utils.duckdb_helpers import unregister_temp_table
from dagster_quickstart.utils.validation_helpers import validate_referential_integrity_sql

if TYPE_CHECKING:
    from database.dependency import DependencyManager

    from .config import SeriesDependencyCSVConfig

from database.ddl import (
    DEPENDENCY_ENRICHED_TABLE_QUERY,
    DEPENDENCY_RESULTS_QUERY,
    DEPENDENCY_VALIDATION_QUERY,
)


def load_series_dependencies_logic(
    context: AssetExecutionContext,
    config: "SeriesDependencyCSVConfig",  # type: ignore[name-defined]
    duckdb: DuckDBResource,
    version_date: str,
    dependency_manager: "DependencyManager",  # type: ignore[name-defined]
    meta_manager: Any,  # MetaSeriesManager - avoid circular import
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """Load series dependencies from CSV file and write to S3 control table.

    Data flow: CSV → DuckDB temp table (via process_csv_to_s3_with_validation) →
    validation → enrichment (series IDs) → S3 Parquet control table (versioned).
    DuckDB views are updated to point to the latest S3 control table version.

    Args:
        context: Dagster execution context.
        config: Series dependency CSV configuration.
        duckdb: DuckDB resource.
        version_date: Version date for this run.
        dependency_manager: DependencyManager instance (initialized in asset).
        meta_manager: MetaSeriesManager instance (initialized in asset).

    Returns:
        Tuple of (results dictionary for DataFrame conversion, results dictionary mapping dependency_id -> row_index).
    """
    context.log.info(f"Loading series dependencies from {config.csv_path}")

    # Process CSV → S3 control table (versioned, immutable)
    context.log.info("Using CSV → S3 control table flow (versioned, immutable)")
    results = process_csv_to_s3_control_table_dependencies(
        context, duckdb, config.csv_path, version_date, dependency_manager, meta_manager
    )

    # Build results dictionary for DataFrame conversion
    result_dict = {
        "dependency_id": list(results.keys()),
        "row_index": list(results.values()),
    }

    return result_dict, results


def _validate_dependencies_against_meta_series(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
    version_date: str,
) -> None:
    """Validate that parent_series_code and child_series_code exist in metaSeries.

    Uses validate_referential_integrity_sql for consistent SQL-based validation.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Temporary table name with dependency data.
        version_date: Version date for this run.

    Raises:
        ReferentialIntegrityError: If validation fails.
    """
    # Build validation query to check that both parent and child series codes exist
    validation_query = DEPENDENCY_VALIDATION_QUERY.format(temp_table=temp_table)

    validate_referential_integrity_sql(
        duckdb=duckdb,
        temp_table=temp_table,
        validation_query=validation_query,
        context=context,
    )


def _count_rows_in_temp_table(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
) -> int:
    """Count the number of rows in a temporary table.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Name of temporary table.

    Returns:
        Number of rows in the table.
    """
    count_query = f"SELECT COUNT(*) as count FROM {temp_table}"
    count_result = duckdb.execute_query(count_query)
    return count_result.iloc[0, 0] if count_result is not None and not count_result.empty else 0


def _log_series_code_matching_stats(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
) -> None:
    """Log statistics about how many series codes from CSV match metaSeries.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Name of temporary table with dependency data.
    """
    check_query = f"""
    SELECT 
        COUNT(DISTINCT d.parent_series_code) as parent_codes_in_csv,
        COUNT(DISTINCT d.child_series_code) as child_codes_in_csv,
        COUNT(DISTINCT CASE WHEN p.series_id IS NOT NULL THEN d.parent_series_code END) as parent_codes_found,
        COUNT(DISTINCT CASE WHEN c.series_id IS NOT NULL THEN d.child_series_code END) as child_codes_found
    FROM {temp_table} d
    LEFT JOIN metaSeries p ON d.parent_series_code = p.series_code
    LEFT JOIN metaSeries c ON d.child_series_code = c.series_code
    """
    check_result = duckdb.execute_query(check_query)
    if check_result is not None and not check_result.empty:
        row = check_result.iloc[0]
        context.log.info(
            "Series code matching: %s/%s parent codes found, %s/%s child codes found",
            row["parent_codes_found"],
            row["parent_codes_in_csv"],
            row["child_codes_found"],
            row["child_codes_in_csv"],
        )


def _log_missing_series_codes(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
) -> None:
    """Log sample of series codes that couldn't be matched to metaSeries.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Name of temporary table with dependency data.
    """
    missing_query = f"""
    SELECT DISTINCT
        d.parent_series_code,
        d.child_series_code,
        CASE WHEN p.series_id IS NULL THEN 'MISSING' ELSE 'FOUND' END as parent_status,
        CASE WHEN c.series_id IS NULL THEN 'MISSING' ELSE 'FOUND' END as child_status
    FROM {temp_table} d
    LEFT JOIN metaSeries p ON d.parent_series_code = p.series_code
    LEFT JOIN metaSeries c ON d.child_series_code = c.series_code
    WHERE p.series_id IS NULL OR c.series_id IS NULL
    LIMIT 10
    """
    missing_result = duckdb.execute_query(missing_query)
    if missing_result is not None and not missing_result.empty:
        context.log.warning("Sample of series codes that couldn't be matched:")
        for _, row in missing_result.iterrows():
            context.log.warning(
                "  Parent: %s (%s), Child: %s (%s)",
                row["parent_series_code"],
                row["parent_status"],
                row["child_series_code"],
                row["child_status"],
            )


def _build_enriched_table_query(
    duckdb: DuckDBResource,
    temp_table: str,
    enriched_table: str,
) -> str:
    """Build SQL query to create enriched table with series IDs mapped from codes.

    Args:
        duckdb: DuckDB resource.
        temp_table: Name of temporary table with dependency data (series codes).
        enriched_table: Name for the enriched table to create.

    Returns:
        SQL CREATE TABLE query string.
    """
    from ..utils.load_data import get_available_columns

    # Check if calc_type column exists in the temp table (optional)
    available_columns = get_available_columns(duckdb, temp_table, ["calc_type"])

    # Build expression for optional calc_type column
    calc_type_expr = "d.calc_type" if "calc_type" in available_columns else "NULL AS calc_type"

    # Build CREATE TABLE query with series IDs mapped from codes
    create_query = DEPENDENCY_ENRICHED_TABLE_QUERY.format(
        enriched_table=enriched_table,
        temp_table=temp_table,
        calc_type_expr=calc_type_expr,
    )
    return create_query


def _map_series_codes_to_ids(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
) -> str:
    """Map parent_series_code and child_series_code to IDs and create enriched temp table.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Temporary table name with dependency data.

    Returns:
        Name of the enriched temp table with series IDs.

    Raises:
        DatabaseQueryError: If mapping fails.
    """
    from dagster_quickstart.utils.exceptions import DatabaseQueryError

    enriched_table = f"{temp_table}_enriched"

    try:
        # Log initial row count
        temp_count = _count_rows_in_temp_table(context, duckdb, temp_table)
        context.log.info("Temp table %s has %s rows", temp_table, temp_count)

        # Log series code matching statistics
        _log_series_code_matching_stats(context, duckdb, temp_table)

        # Build and execute query to create enriched table
        create_query = _build_enriched_table_query(duckdb, temp_table, enriched_table)
        duckdb.execute_command(create_query)

        # Log enriched table row count
        enriched_count = _count_rows_in_temp_table(context, duckdb, enriched_table)
        context.log.info(
            "Created enriched temp table %s with %s rows (series IDs mapped)",
            enriched_table,
            enriched_count,
        )

        # Log missing series codes if no rows were created
        if enriched_count == 0:
            _log_missing_series_codes(context, duckdb, temp_table)

        return enriched_table

    except Exception as e:
        error_msg = f"Error mapping series codes to IDs: {e}"
        context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


def _enrich_dependencies_with_series_ids(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
) -> str:
    """Enrich dependency data by mapping series codes to series IDs from metaSeries.

    Creates a new temporary table with parent_series_id and child_series_id columns
    mapped from parent_series_code and child_series_code.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Name of temporary table with dependency data (series codes).

    Returns:
        Name of the enriched temporary table with series IDs.

    Raises:
        DatabaseQueryError: If mapping fails.
    """
    context.log.info("Enriching dependencies with series IDs from metaSeries")
    enriched_table = _map_series_codes_to_ids(context, duckdb, temp_table)
    context.log.info("Created enriched table: %s", enriched_table)
    return enriched_table


def _enrich_and_save_dependencies(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
    version_date: str,
    dependency_manager: "DependencyManager",  # type: ignore[name-defined]
    meta_manager: Any,
) -> str:
    """Enrich dependency data with series IDs, save to S3 control table, and return S3 path.

    This helper is designed to be used as save_to_s3_func with process_csv_to_s3_with_validation.
    It:
      - enriches codes with IDs
      - writes to S3 control table via DependencyManager
      - builds and logs dependency_id -> row_index results
    """
    enriched_table: str | None = None
    try:
        # Optional: enable referential integrity check here if desired
        # _validate_dependencies_against_meta_series(context, duckdb, temp_table, version_date)

        # Enrich with series IDs by mapping codes to IDs from metaSeries
        enriched_table = _enrich_dependencies_with_series_ids(context, duckdb, temp_table)

        # Write enriched data to S3 control table
        relative_path = dependency_manager.save_dependencies_to_s3(
            duckdb, enriched_table, version_date, context
        )

        # Build results dictionary (dependency_id -> row_index)
        results = _build_dependency_results(context, duckdb, enriched_table)
        context.log.info(
            "Successfully processed %d dependency records",
            len(results),
            extra={"record_count": len(results), "s3_path": relative_path},
        )

        return relative_path
    finally:
        # Clean up enriched temp table; the original temp_table is managed by the CSV helper
        if enriched_table:
            unregister_temp_table(duckdb, enriched_table, context)


def process_csv_to_s3_control_table_dependencies(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    csv_path: str,
    version_date: str,
    dependency_manager: "DependencyManager",  # type: ignore[name-defined]
    meta_manager: Any,  # MetaSeriesManager
) -> Dict[str, int]:
    """Process series dependencies from CSV to S3 control table (versioned, immutable).

    Orchestrates the complete data flow via process_csv_to_s3_with_validation:
    CSV → DuckDB temp table → (optional validation) → enrichment (series IDs) → S3 Parquet control table

    Uses the generic CSV helper (process_csv_to_s3_with_validation) for CSV loading,
    optional validation, and temp table cleanup. Enrichment and S3 writing are handled
    by _enrich_and_save_dependencies.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource with S3 access.
        csv_path: Path to CSV file.
        version_date: Version date for this run.
        dependency_manager: DependencyManager instance (initialized in asset).
        meta_manager: MetaSeriesManager instance (initialized in asset).

    Returns:
        Dictionary mapping dependency_id -> row_index (deterministic based on order).

    Raises:
        Exception: If referential integrity validation fails.
        DatabaseQueryError: If write operation fails.
        Exception: If CSV cannot be read.
    """
    context.log.info("Processing series dependencies from CSV to S3 control table (versioned)")

    # Wrap enrich+save so it matches process_csv_to_s3_with_validation's signature
    def save_to_s3_wrapper(
        duckdb_res: DuckDBResource,
        temp_table: str,
        version: str,
        ctx: AssetExecutionContext | None = None,
        **kwargs: Any,
    ) -> str:
        # ctx is ignored because we already close over context, but signature must match
        return _enrich_and_save_dependencies(
            context, duckdb_res, temp_table, version, dependency_manager, meta_manager
        )

    # Use the generic CSV helper for:
    # - CSV → temp table
    # - optional validation (currently disabled for dependencies)
    # - enrich + save to S3
    # - temp table cleanup
    relative_path = process_csv_to_s3_with_validation(
        context=context,
        duckdb=duckdb,
        csv_path=csv_path,
        version_date=version_date,
        save_to_s3_func=save_to_s3_wrapper,
        create_view_func=None,  # view creation is handled in the asset
        validation_func=None,  # _validate_dependencies_against_meta_series can be plugged in later
        view_creation_enabled=False,
    )

    # After writing, read back from S3 to build the dependency_id -> row_index mapping
    from dagster_quickstart.utils.constants import S3_CONTROL_DEPENDENCY, S3_PARQUET_FILE_NAME
    from dagster_quickstart.utils.s3_helpers import build_full_s3_path, build_s3_control_table_path

    control_relative = build_s3_control_table_path(
        S3_CONTROL_DEPENDENCY, version_date, S3_PARQUET_FILE_NAME
    )
    full_s3_path = build_full_s3_path(duckdb, control_relative)

    query = f"""
    SELECT 
        dependency_id,
        row_number() OVER (ORDER BY parent_series_id, child_series_id) AS row_index
    FROM read_parquet('{full_s3_path}')
    ORDER BY row_index
    """
    result = duckdb.execute_query(query)

    if result is None or result.empty:
        context.log.warning("No dependency records found in saved S3 control table")
        return {}

    results: Dict[str, int] = {}
    for _, row in result.iterrows():
        dep_id = int(row["dependency_id"])
        row_index = int(row["row_index"])
        results[str(dep_id)] = row_index

    context.log.info(
        "Built dependency_id -> row_index mapping from S3 control table",
        extra={"record_count": len(results), "s3_path": relative_path},
    )
    return results


def _build_dependency_results(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
) -> Dict[str, int]:
    """Build results dictionary mapping dependency_id -> row_index from temp table.

    Uses SQL to get deterministic row indices based on order in CSV.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Temporary table name with dependency data.

    Returns:
        Dictionary mapping dependency_id -> row_index (1-based).
    """
    query = DEPENDENCY_RESULTS_QUERY.format(temp_table=temp_table)

    result = duckdb.execute_query(query)
    if result is None or result.empty:
        return {}

    results = {}
    for _, row in result.iterrows():
        dependency_id = int(row["dependency_id"])
        row_index = int(row["row_index"])
        results[str(dependency_id)] = row_index

    return results
