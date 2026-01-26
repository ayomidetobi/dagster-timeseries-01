"""Test script to read value data from S3 and save to local file for inspection.

Uses the same DuckDB configuration as Dagster (reads from environment variables).
Usage:
    python test_read_s3_value_data.py --series-code AAPL_US_EQ [--output output.csv]
    
Environment variables (same as Dagster):
    S3_BUCKET - S3 bucket name
    S3_ACCESS_KEY - S3 access key
    S3_SECRET_KEY - S3 secret key
    S3_REGION - S3 region (optional, defaults to us-east-1)
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
from decouple import config

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.resources.duckdb_resource import DuckDBResource
from dagster_quickstart.utils.database_config import get_database_resource
from dagster_quickstart.utils.s3_helpers import (
    build_full_s3_path,
    build_s3_value_data_path,
)


def read_value_data_from_s3(
    series_code: str,
    filter_date: str = None,
) -> pd.DataFrame:
    """Read value data from S3 for a given series_code.

    Uses the same DuckDB configuration as Dagster (reads from environment variables).

    Args:
        series_code: Series code to read data for
        filter_date: Optional date filter (YYYY-MM-DD format) to filter data by date

    Returns:
        DataFrame with value data (series_id, timestamp, value)
    """
    # Initialize DuckDB datacacher with S3 credentials from environment
    # Uses the same configuration as Dagster (S3_BUCKET, S3_ACCESS_KEY, S3_SECRET_KEY, S3_REGION)
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
    relative_path = build_s3_value_data_path(series_code)
    full_s3_path = build_full_s3_path(duckdb_resource, relative_path)

    print(f"Reading data from S3 path: {full_s3_path}")

    # Build query
    if filter_date:
        query = f"""
            SELECT series_id, timestamp, value
            FROM read_parquet('{full_s3_path}')
            WHERE DATE(timestamp) = DATE('{filter_date}')
            ORDER BY timestamp
        """
        print(f"Filtering data for date: {filter_date}")
    else:
        query = f"""
            SELECT series_id, timestamp, value
            FROM read_parquet('{full_s3_path}')
            ORDER BY timestamp
        """
        print("Reading all data (no date filter)")

    try:
        # Execute query
        df = duckdb_resource.execute_query(query)

        if df is None or df.empty:
            print(f"Warning: No data found for series_code={series_code}")
            return pd.DataFrame()

        print(f"Successfully read {len(df)} rows")
        return df

    except Exception as e:
        print(f"Error reading data from S3: {e}")
        raise


def save_to_file(df: pd.DataFrame, output_path: str, format: str = "csv"):
    """Save DataFrame to file.

    Args:
        df: DataFrame to save
        output_path: Output file path
        format: File format ('csv' or 'json')
    """
    if df.empty:
        print("No data to save")
        return

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if format == "csv":
        df.to_csv(output_path, index=False)
        print(f"Data saved to CSV: {output_path}")
    elif format == "json":
        df.to_json(output_path, orient="records", indent=2, date_format="iso")
        print(f"Data saved to JSON: {output_path}")
    else:
        raise ValueError(f"Unsupported format: {format}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description="Read value data from S3 and save to local file"
    )
    parser.add_argument(
        "--series-code",
        required=True,
        help="Series code to read data for (e.g., AAPL_US_EQ)",
    )
    parser.add_argument(
        "--output",
        default="output_value_data.csv",
        help="Output file path (default: output_value_data.csv)",
    )
    parser.add_argument(
        "--format",
        choices=["csv", "json"],
        default="csv",
        help="Output file format (default: csv)",
    )
    parser.add_argument(
        "--filter-date",
        help="Filter data by date (YYYY-MM-DD format)",
    )

    args = parser.parse_args()

    # Validate date format if provided
    if args.filter_date:
        try:
            datetime.strptime(args.filter_date, "%Y-%m-%d")
        except ValueError:
            print(f"Error: Invalid date format '{args.filter_date}'. Use YYYY-MM-DD format.")
            sys.exit(1)

    try:
        # Read data from S3 (uses environment variables for S3 credentials)
        df = read_value_data_from_s3(
            series_code=args.series_code,
            filter_date=args.filter_date,
        )

        # Display summary
        print("\n" + "=" * 50)
        print("Data Summary:")
        print("=" * 50)
        print(f"Total rows: {len(df)}")
        if not df.empty:
            print(f"Columns: {', '.join(df.columns)}")
            print(f"\nFirst few rows:")
            print(df.head(10).to_string())
            print(f"\nDate range:")
            if "timestamp" in df.columns:
                print(f"  Min timestamp: {df['timestamp'].min()}")
                print(f"  Max timestamp: {df['timestamp'].max()}")
            print(f"\nValue statistics:")
            if "value" in df.columns:
                print(df["value"].describe())

        # Save to file
        save_to_file(df, args.output, args.format)

        print("\n" + "=" * 50)
        print("Done!")
        print("=" * 50)

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
