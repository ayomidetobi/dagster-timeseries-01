"""Test script to read value data from S3 and save to local file for inspection.

Uses the same DuckDB configuration as Dagster (reads from environment variables).
Usage:
    python scripts/test_read_s3_value_data.py --series-code AAPL_US_EQ [--output output.csv]

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

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dagster_quickstart.utils.s3_debug_tools import (
    init_duckdb_from_env,
    print_data_summary,
    read_value_data_from_s3,
    save_dataframe_to_file,
)


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Read value data from S3 and save to local file")
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
        # Initialize DuckDB resource from environment variables
        print("Initializing DuckDB connection...")
        duckdb_resource = init_duckdb_from_env()

        # Read data from S3
        print(f"Reading data for series_code: {args.series_code}")
        if args.filter_date:
            print(f"Filtering data for date: {args.filter_date}")
        else:
            print("Reading all data (no date filter)")

        df = read_value_data_from_s3(
            duckdb_resource=duckdb_resource,
            series_code=args.series_code,
            filter_date=args.filter_date,
        )

        if df.empty:
            print(f"Warning: No data found for series_code={args.series_code}")
            return

        print(f"Successfully read {len(df)} rows")

        # Display summary
        print_data_summary(df)

        # Save to file
        save_dataframe_to_file(df, args.output, file_format=args.format)
        print("=" * 60)
        print(f"Data saved to: {Path(args.output).absolute()}")
        print("=" * 60)
        print("Done!")

    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
