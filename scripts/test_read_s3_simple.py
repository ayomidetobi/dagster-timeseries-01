"""Simple test script to read value data from S3 and save to local file.

Uses the same DuckDB configuration as Dagster (reads from environment variables).
Edit the configuration section below with your series_code.
"""

import sys
from pathlib import Path

# ============================================================================
# CONFIGURATION - Edit these values
# ============================================================================
SERIES_CODE = "AAPL_US_EQ"  # Change this to your series code
OUTPUT_FILE = "output.csv"  # Output file name
FILTER_DATE = None  # Set to "2025-01-15" to filter by date, or None for all data
# ============================================================================

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
        # Initialize DuckDB resource from environment variables
        print("Initializing DuckDB connection...")
        duckdb_resource = init_duckdb_from_env()

        # Read data from S3
        print(f"Reading data for series_code: {SERIES_CODE}")
        if FILTER_DATE:
            print(f"Filtering data for date: {FILTER_DATE}")
        else:
            print("Reading all data (no date filter)")

        df = read_value_data_from_s3(
            duckdb_resource=duckdb_resource,
            series_code=SERIES_CODE,
            filter_date=FILTER_DATE,
        )

        if df.empty:
            print(f"\nWARNING: No data found for series_code={SERIES_CODE}")
            return

        print(f"Successfully read {len(df)} rows")
        print()

        # Display summary
        print_data_summary(df)

        # Save to file
        save_dataframe_to_file(df, OUTPUT_FILE, format="csv")
        print("=" * 60)
        print(f"Data saved to: {Path(OUTPUT_FILE).absolute()}")
        print("=" * 60)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
