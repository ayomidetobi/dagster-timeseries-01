"""DuckDB data loading functions for S3 Parquet files."""

from datetime import datetime
from typing import Any, Optional

import pandas as pd

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.resources.duckdb_datacacher import SQL
from dagster_quickstart.utils.constants import SQL_FILE_PATH_PLACEHOLDER
from dagster_quickstart.utils.s3_helpers import (
    build_full_s3_path,
    build_s3_value_data_path,
)


def load_series_data_from_duckdb(
    duckdb: DuckDBResource, series_code: str, partition_date: datetime
) -> Optional[pd.DataFrame]:
    """Load time-series data from S3 Parquet files for a given series_code and date.

    Uses unified path (single file per series_code) and filters by partition_date.
    Uses DuckDBResource's load() method with SQL class bindings for S3 path resolution.

    Args:
        duckdb: DuckDB resource with S3 access via httpfs
        series_code: Series code to load data for
        partition_date: Partition date for filtering the data

    Returns:
        DataFrame with timestamp and value columns, or None if no data found
    """
    # Get relative S3 path for this series (unified path, no date partitioning)
    relative_path = build_s3_value_data_path(series_code)
    partition_date_str = partition_date.strftime("%Y-%m-%d")

    try:
        # Use DuckDBResource's load() method with SQL class for proper S3 path resolution
        if SQL is not None:
            # Use $file_path binding - sql_to_string will resolve it to full S3 path
            # Filter by partition_date
            query = SQL(
                "SELECT timestamp, value FROM read_parquet('$file_path') WHERE DATE(timestamp) = DATE('$partition_date') ORDER BY timestamp",
                file_path=relative_path,
                partition_date=partition_date_str,
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
            WHERE DATE(timestamp) = DATE('{partition_date_str}')
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
