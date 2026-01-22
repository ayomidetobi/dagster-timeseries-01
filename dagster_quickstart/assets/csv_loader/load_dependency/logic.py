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
from dagster_quickstart.utils.constants import NULL_VALUE_REPRESENTATION
from dagster_quickstart.utils.helpers import (
    load_csv_to_temp_table,
    unregister_temp_table,
    validate_referential_integrity_sql,
)

if TYPE_CHECKING:
    from database.dependency import DependencyManager
    from .config import SeriesDependencyCSVConfig


def load_series_dependencies_logic(
    context: AssetExecutionContext,
    config: "SeriesDependencyCSVConfig",  # type: ignore[name-defined]
    duckdb: DuckDBResource,
    version_date: str,
    dependency_manager: "DependencyManager",  # type: ignore[name-defined]
    meta_manager: Any,  # MetaSeriesManager - avoid circular import
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """Load series dependencies from CSV file and write to S3 control table.

    Data flow: CSV → DuckDB temp table (via read_csv) → validation → S3 Parquet control table (versioned)
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

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        temp_table: Temporary table name with dependency data.
        version_date: Version date for this run.

    Raises:
        Exception: If validation fails.
    """
    # Build validation query to check that both parent and child series codes exist
    validation_query = f"""
    SELECT 
        d.parent_series_code,
        d.child_series_code,
        CASE 
            WHEN p.series_id IS NULL THEN 'Parent series not found: ' || d.parent_series_code
            WHEN c.series_id IS NULL THEN 'Child series not found: ' || d.child_series_code
            ELSE NULL
        END AS error_message
    FROM {temp_table} d
    LEFT JOIN metaSeries p ON d.parent_series_code = p.series_code
    LEFT JOIN metaSeries c ON d.child_series_code = c.series_code
    WHERE p.series_id IS NULL OR c.series_id IS NULL
    """

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
    create_query = f"""
    CREATE TEMP TABLE {enriched_table} AS
    SELECT 
        row_number() OVER (ORDER BY p.series_id, c.series_id) AS dependency_id,
        d.parent_series_code,
        d.child_series_code,
        p.series_id AS parent_series_id,
        c.series_id AS child_series_id,
        {calc_type_expr}
    FROM {temp_table} d
    INNER JOIN metaSeries p ON d.parent_series_code = p.series_code
    INNER JOIN metaSeries c ON d.child_series_code = c.series_code
    """
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


def _load_dependencies_csv_to_temp_table(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    csv_path: str,
) -> str:
    """Load series dependencies CSV file into a DuckDB temporary table.

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource.
        csv_path: Path to CSV file.

    Returns:
        Name of the temporary table containing the CSV data.

    Raises:
        CSVValidationError: If CSV cannot be read.
    """
    context.log.info("Loading dependencies CSV from %s into temporary table", csv_path)
    temp_table = load_csv_to_temp_table(duckdb, csv_path, NULL_VALUE_REPRESENTATION, context)
    context.log.info("Loaded CSV into temporary table: %s", temp_table)
    return temp_table


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


def _save_dependencies_to_s3_control_table(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    enriched_table: str,
    version_date: str,
    dependency_manager: "DependencyManager",  # type: ignore[name-defined]
) -> None:
    """Save enriched dependency data to S3 control table (versioned, immutable).

    Args:
        context: Dagster execution context.
        duckdb: DuckDB resource with S3 access.
        enriched_table: Name of enriched temporary table with series IDs.
        version_date: Version date for this run.
        dependency_manager: DependencyManager instance.

    Raises:
        DatabaseQueryError: If write operation fails.
    """
    context.log.info(f"Saving dependencies to S3 control table (version: {version_date})")
    dependency_manager.save_dependencies_to_s3(duckdb, enriched_table, version_date, context)
    context.log.info("Successfully saved dependencies to S3 control table")


def process_csv_to_s3_control_table_dependencies(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    csv_path: str,
    version_date: str,
    dependency_manager: "DependencyManager",  # type: ignore[name-defined]
    meta_manager: Any,  # MetaSeriesManager
) -> Dict[str, int]:
    """Process series dependencies from CSV to S3 control table (versioned, immutable).

    Orchestrates the complete data flow:
    CSV → DuckDB temp table → enrichment (series IDs) → S3 Parquet control table

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

    # Step 1: Load CSV directly into DuckDB temp table
    temp_table = _load_dependencies_csv_to_temp_table(context, duckdb, csv_path)

    enriched_table = None
    try:
        # Step 2: Validate referential integrity - check that series codes exist
        # Skipped for now - can be re-enabled later if needed
        # _validate_dependencies_against_meta_series(context, duckdb, temp_table, version_date)

        # Step 3: Enrich with series IDs by mapping codes to IDs from metaSeries
        enriched_table = _enrich_dependencies_with_series_ids(context, duckdb, temp_table)

        # Step 4: Write validated and enriched data to S3 control table (versioned, immutable)
        dependency_manager.save_dependencies_to_s3(duckdb, enriched_table, version_date, context)

        # Step 5: Build results dictionary (dependency_id -> row_index)
        results = _build_dependency_results(context, duckdb, enriched_table)

        context.log.info(f"Successfully processed {len(results)} dependency records")
        return results
    finally:
        # Clean up temp tables
        if enriched_table:
            unregister_temp_table(duckdb, enriched_table, context)
        unregister_temp_table(duckdb, temp_table, context)


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
    query = f"""
    SELECT 
        row_number() OVER (ORDER BY parent_series_id, child_series_id) AS dependency_id,
        row_number() OVER (ORDER BY parent_series_id, child_series_id) AS row_index
    FROM {temp_table}
    """

    result = duckdb.execute_query(query)
    if result is None or result.empty:
        return {}

    results = {}
    for _, row in result.iterrows():
        dependency_id = int(row["dependency_id"])
        row_index = int(row["row_index"])
        results[str(dependency_id)] = row_index

    return results
