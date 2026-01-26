"""Simple test script to read value data from S3 and save to local file.

Uses the same DuckDB configuration as Dagster (reads from environment variables).
Edit the configuration section below with your series_code.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
from decouple import config

# ============================================================================
# CONFIGURATION - Edit these values
# ============================================================================
SERIES_CODE = "AAPL_US_EQ"  # Change this to your series code
OUTPUT_FILE = "output.csv"  # Output file name
FILTER_DATE = None  # Set to "2025-01-15" to filter by date, or None for all data
# ============================================================================

# Add project root to path
import sys

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource
from dagster_quickstart.utils.database_config import get_database_resource
from dagster_quickstart.utils.s3_helpers import (
    build_full_s3_path,
    build_s3_value_data_path,
)


def main():
    """Read value data from S3 and save to local file."""
    print("=" * 60)
    print("Reading Value Data from S3")
    print("=" * 60)
    print(f"Series Code: {SERIES_CODE}")
    print(f"Output File: {OUTPUT_FILE}")
    if FILTER_DATE:
        print(f"Date Filter: {FILTER_DATE}")
    else:
        print("Date Filter: None (all data)")
    print()

    try:
        # Initialize DuckDB datacacher with S3 credentials from environment
        # Uses the same configuration as Dagster (S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION)
        print("Initializing DuckDB connection...")
        duckdb_cacher = duckdb_datacacher(
            bucket=config("S3_BUCKET", default=None),
            access_key=config("S3_ACCESS_KEY", default=None),
            secret_key=config("S3_SECRET_KEY", default=None),
            region=config("S3_REGION", default=None),
        )

        # Initialize DuckDBResource (same as Dagster definitions.py)
        duckdb_resource = get_database_resource(duckdb_cacher=duckdb_cacher)
        duckdb_resource.setup_for_execution(None)

        # Build S3 path
        relative_path = build_s3_value_data_path(SERIES_CODE)
        full_s3_path = build_full_s3_path(duckdb_resource, relative_path)

        print(f"S3 Path: {full_s3_path}")
        print()

        # Build query
        if FILTER_DATE:
            query = f"""
                SELECT series_id, timestamp, value
                FROM read_parquet('{full_s3_path}')
                WHERE DATE(timestamp) = DATE('{FILTER_DATE}')
                ORDER BY timestamp
            """
            print(f"Filtering data for date: {FILTER_DATE}")
        else:
            query = f"""
                SELECT series_id, timestamp, value
                FROM read_parquet('{full_s3_path}')
                ORDER BY timestamp
            """
            print("Reading all data (no date filter)")

        # Execute query
        print("Executing query...")
        df = duckdb_resource.execute_query(query)

        if df is None or df.empty:
            print(f"\nWARNING: No data found for series_code={SERIES_CODE}")
            print(f"Check if the file exists at: {full_s3_path}")
            return

        print(f"Successfully read {len(df)} rows")
        print()

        # Display summary
        print("=" * 60)
        print("Data Summary")
        print("=" * 60)
        print(f"Total rows: {len(df)}")
        print(f"Columns: {', '.join(df.columns)}")
        print()

        if "timestamp" in df.columns:
            print(f"Date range:")
            print(f"  Min timestamp: {df['timestamp'].min()}")
            print(f"  Max timestamp: {df['timestamp'].max()}")
            print()

        if "value" in df.columns:
            print("Value statistics:")
            print(df["value"].describe())
            print()

        # Display first few rows
        print("First 10 rows:")
        print(df.head(10).to_string())
        print()

        # Save to file
        output_path = Path(OUTPUT_FILE)
        df.to_csv(output_path, index=False)
        print("=" * 60)
        print(f"Data saved to: {output_path.absolute()}")
        print("=" * 60)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
