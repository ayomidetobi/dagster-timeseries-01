"""CSV-based loading assets for meta series with validation.

Uses S3 Parquet files as the datalake. CSV data is validated and written to S3 Parquet
control tables (versioned, immutable). DuckDB views are created dynamically over S3
control tables for querying.
"""

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_CSV_LOADER,
    RETRY_POLICY_MAX_RETRIES_CSV_LOADER,
    S3_CONTROL_METADATA_SERIES,
    S3_PARQUET_FILE_NAME,
)
from dagster_quickstart.utils.exceptions import DatabaseQueryError
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_control_table_path,
    get_version_date,
)
from dagster_quickstart.utils.summary.csv_loader import add_csv_loader_summary_metadata
from database.meta_series import MetaSeriesManager

from .config import MetaSeriesCSVConfig
from .logic import load_meta_series_logic


@asset(
    group_name="metadata",
    description="Load meta series from CSV file - uses S3 Parquet staging",
    deps=[
        AssetKey("load_lookup_tables_from_csv"),  # Depends on lookup tables being loaded first
    ],
    io_manager_key="duckdb_io_manager",
    kinds=["csv", "duckdb", "s3"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "architecture": "s3-parquet-datalake"},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def load_meta_series_from_csv(
    context: AssetExecutionContext,
    config: MetaSeriesCSVConfig,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    """Load meta series from CSV file using S3 Parquet control tables.

    Data flow: CSV → DuckDB temp view → validation → S3 Parquet control table (versioned)
    - CSV data is validated in DuckDB temp view against lookup tables
    - Validated data is written to S3 Parquet control tables (versioned, immutable)
    - DuckDB views are created/updated to point to latest S3 control table versions

    Args:
        context: Dagster execution context
        config: Meta series CSV configuration
        duckdb: DuckDB resource with S3 access via httpfs

    Returns:
        DataFrame with meta series results (series_code -> series_id mappings)
    """
    context.log.info("Using S3 Parquet staging architecture for meta series loading")

    # Get version date for this run
    version_date = get_version_date()

    # Initialize manager
    meta_manager = MetaSeriesManager(duckdb)

    # Ensure views exist before calling logic (for validation/reading existing data)
    try:
        meta_manager.create_or_update_view(duckdb, version_date, context=context)
    except DatabaseQueryError:
        # If views don't exist yet (first run), that's okay - logic will create the data
        context.log.info("Meta series views don't exist yet - will be created after data is saved")

    result_dict, results = load_meta_series_logic(
        context, config, duckdb, version_date, meta_manager
    )

    # Create/update DuckDB view over S3 control table (after data is saved)
    meta_manager.create_or_update_view(duckdb, version_date, context=context)

    # Convert to DataFrame only for IO manager compatibility
    result_df = pd.DataFrame(result_dict)

    # Get S3 path for metadata
    relative_path = build_s3_control_table_path(
        S3_CONTROL_METADATA_SERIES, version_date, S3_PARQUET_FILE_NAME
    )
    s3_control_table_path = build_full_s3_path(duckdb, relative_path)

    # Add AssetSummary metadata
    add_csv_loader_summary_metadata(
        context=context,
        records_loaded=len(result_df),
        record_type="meta_series",
        details=results,
        csv_path=config.csv_path,
        version_date=version_date,
        s3_control_table_path=s3_control_table_path,
    )

    return result_df
