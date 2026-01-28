"""S3 debug tools for reading and inspecting data from S3.

Provides helper functions for debugging and testing S3 data access,
commonly used in test scripts and debugging utilities.
"""

from pathlib import Path
from typing import Optional

import pandas as pd
from decouple import config

from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource
from dagster_quickstart.utils.database_config import get_database_resource
from dagster_quickstart.utils.s3_helpers import (
    build_full_s3_path,
    build_s3_value_data_path,
)


def init_duckdb_from_env() -> DuckDBResource:
    """Initialize DuckDB resource from environment variables.

    Uses the same configuration as Dagster (S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION).

    Returns:
        Initialized DuckDBResource instance

    Raises:
        ValueError: If required environment variables are missing
    """
    duckdb_cacher = duckdb_datacacher(
        bucket=config("S3_BUCKET", default=None),
        access_key=config("S3_ACCESS_KEY", default=None),
        secret_key=config("S3_SECRET_KEY", default=None),
        region=config("S3_REGION", default=None),
    )

    duckdb_resource = get_database_resource(duckdb_cacher=duckdb_cacher)
    duckdb_resource.setup_for_execution(None)

    return duckdb_resource


def read_value_data_from_s3(
    duckdb_resource: DuckDBResource,
    series_code: str,
    filter_date: Optional[str] = None,
) -> pd.DataFrame:
    """Read value data from S3 for a given series_code.

    Args:
        duckdb_resource: Initialized DuckDBResource instance
        series_code: Series code to read data for
        filter_date: Optional date filter (YYYY-MM-DD format) to filter data by date

    Returns:
        DataFrame with value data (series_id, timestamp, value).
        Returns empty DataFrame if no data found.

    Raises:
        Exception: If query execution fails
    """
    # Build S3 path
    relative_path = build_s3_value_data_path(series_code)
    full_s3_path = build_full_s3_path(duckdb_resource, relative_path)

    # Build query
    if filter_date:
        query = f"""
            SELECT series_id, timestamp, value
            FROM read_parquet('{full_s3_path}')
            WHERE DATE(timestamp) = DATE('{filter_date}')
            ORDER BY timestamp
        """
    else:
        query = f"""
            SELECT series_id, timestamp, value
            FROM read_parquet('{full_s3_path}')
            ORDER BY timestamp
        """

    # Execute query
    df = duckdb_resource.execute_query(query)

    if df is None or df.empty:
        return pd.DataFrame()

    return df


def save_dataframe_to_file(df: pd.DataFrame, output_path: str, file_format: str = "csv") -> None:
    """Save DataFrame to file.

    Args:
        df: DataFrame to save
        output_path: Output file path
        file_format: File format ('csv' or 'json')

    Raises:
        ValueError: If file_format is not supported
    """
    if df.empty:
        return

    output_path_obj = Path(output_path)
    output_path_obj.parent.mkdir(parents=True, exist_ok=True)

    if file_format == "csv":
        df.to_csv(output_path_obj, index=False)
    elif file_format == "json":
        df.to_json(output_path_obj, orient="records", indent=2, date_format="iso")
    else:
        raise ValueError(f"Unsupported format: {file_format}. Use 'csv' or 'json'.")


def print_data_summary(df: pd.DataFrame, show_rows: int = 10) -> None:
    """Print a summary of the DataFrame.

    Args:
        df: DataFrame to summarize
        show_rows: Number of rows to display (default: 10)
    """
    print("\n" + "=" * 60)
    print("Data Summary")
    print("=" * 60)
    print(f"Total rows: {len(df)}")

    if df.empty:
        print("DataFrame is empty")
        return

    print(f"Columns: {', '.join(df.columns)}")
    print()

    if "timestamp" in df.columns:
        print("Date range:")
        print(f"  Min timestamp: {df['timestamp'].min()}")
        print(f"  Max timestamp: {df['timestamp'].max()}")
        print()

    if "value" in df.columns:
        print("Value statistics:")
        print(df["value"].describe())
        print()

    print(f"First {show_rows} rows:")
    print(df.head(show_rows).to_string())
    print()
