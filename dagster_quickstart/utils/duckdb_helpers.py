"""DuckDB helper functions for database operations."""

from typing import Any, Optional

import pandas as pd
from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.resources.duckdb_datacacher import SQL
from dagster_quickstart.utils.constants import SQL_FILE_PATH_PLACEHOLDER
from dagster_quickstart.utils.exceptions import CSVValidationError, DatabaseQueryError
from dagster_quickstart.utils.s3_helpers import build_full_s3_path


def load_series_data_from_duckdb(duckdb: DuckDBResource, series_id: int) -> Optional[pd.DataFrame]:
    """Load time-series data from S3 Parquet files for a given series_id.

    Uses DuckDBResource's load() method with SQL class bindings for S3 path resolution.

    Args:
        duckdb: DuckDB resource with S3 access via httpfs
        series_id: Series ID to load data for

    Returns:
        DataFrame with timestamp and value columns, or None if no data found
    """
    # Get relative S3 path for this series (relative to bucket)
    relative_path = f"value-data/series_id={series_id}/data.parquet"

    try:
        # Use DuckDBResource's load() method with SQL class for proper S3 path resolution
        if SQL is not None:
            # Use $file_path binding - sql_to_string will resolve it to full S3 path
            query = SQL(
                "SELECT timestamp, value FROM read_parquet('$file_path') ORDER BY timestamp",
                file_path=relative_path,
            )

            # Use DuckDBResource.load() which handles S3 path resolution via duckdb_datacacher
            df = duckdb.load(query)

            if df is not None and not df.empty:
                # Ensure we only have timestamp and value columns
                if all(col in df.columns for col in ["timestamp", "value"]):
                    return df[["timestamp", "value"]]
                return df
        else:
            # SQL class not available, use execute_query as fallback
            full_s3_path = build_full_s3_path(duckdb, relative_path)
            query = f"""
            SELECT timestamp, value
            FROM read_parquet('{full_s3_path}')
            ORDER BY timestamp
            """
            df = duckdb.execute_query(query)
            if not df.empty:
                # Ensure we only have timestamp and value columns
                if all(col in df.columns for col in ["timestamp", "value"]):
                    return df[["timestamp", "value"]]
                return df
    except Exception:
        # File doesn't exist, return None
        pass
    return None


def create_sql_query_with_file_path(query_template: str, file_path: str, **kwargs: Any) -> Any:
    """Create SQL query object with file_path binding.

    Args:
        query_template: SQL query template with $file_path placeholder
        file_path: Relative S3 file path (relative to bucket)
        **kwargs: Additional bindings for SQL query

    Returns:
        SQL object with bindings, or query string if SQL class not available

    Raises:
        DatabaseQueryError: If SQL class is required but not available
    """
    if SQL is not None:
        return SQL(query_template, file_path=file_path, **kwargs)
    # Return template string for fallback usage
    return query_template.replace(SQL_FILE_PATH_PLACEHOLDER, file_path)


def load_csv_to_temp_table(
    duckdb: DuckDBResource,
    csv_path: str,
    null_value: str = "\\N",
    context: Optional[AssetExecutionContext] = None,
) -> str:
    r"""Load CSV file directly into DuckDB temp table using read_csv.

    Generic helper function for loading CSV files into temporary tables.

    Args:
        duckdb: DuckDB resource
        csv_path: Path to CSV file
        null_value: String representation of NULL values (default: "\\N")
        context: Optional Dagster context for logging

    Returns:
        Temporary table name

    Raises:
        CSVValidationError: If CSV cannot be read
    """
    import uuid

    temp_table = f"_temp_csv_{uuid.uuid4().hex}"

    try:
        # Use DuckDB's read_csv function directly - no pandas involved
        # DuckDB handles null values, type inference, and ragged lines automatically
        create_table_sql = f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT * FROM read_csv('{csv_path}', 
                nullstr='{null_value}',
                header=true,
                auto_detect=true,
                ignore_errors=false
            )
        """
        duckdb.execute_command(create_table_sql)
        if context:
            context.log.debug(f"Loaded CSV into temp table {temp_table}")
        return temp_table
    except Exception as e:
        error_msg = f"Error reading CSV file {csv_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise CSVValidationError(error_msg) from e


def create_or_update_duckdb_view(
    duckdb: DuckDBResource,
    view_name: str,
    view_sql: str,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Create or update a DuckDB view with the given SQL.

    Generic helper function for creating/updating DuckDB views over S3 control tables.

    Args:
        duckdb: DuckDB resource with S3 access
        view_name: Name of the view to create/update
        view_sql: Complete CREATE OR REPLACE VIEW SQL statement
        context: Optional Dagster context for logging

    Raises:
        DatabaseQueryError: If view creation fails
    """
    try:
        duckdb.execute_command(view_sql)
        if context:
            context.log.info(f"Created/updated view {view_name}")
    except Exception as e:
        error_msg = f"Error creating/updating view {view_name}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


def unregister_temp_table(
    duckdb: DuckDBResource,
    temp_table: str,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Drop temporary table from DuckDB.

    Generic helper function for cleaning up temporary tables.

    Args:
        duckdb: DuckDB resource
        temp_table: Temporary table name to drop
        context: Optional Dagster context for logging
    """
    try:
        duckdb.execute_command(f"DROP TABLE IF EXISTS {temp_table}")
        if context:
            context.log.debug(f"Dropped temp table {temp_table}")
    except Exception:
        # Table may already be dropped, ignore
        pass
