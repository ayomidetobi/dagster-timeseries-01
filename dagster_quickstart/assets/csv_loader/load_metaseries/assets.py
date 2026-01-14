"""CSV-based loading assets for meta series with validation.

Uses S3 Parquet files as the datalake. CSV data is validated and written to S3 Parquet
control tables (versioned, immutable). DuckDB views are created dynamically over S3
control tables for querying.
"""

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_CSV_LOADER,
    RETRY_POLICY_MAX_RETRIES_CSV_LOADER,
)

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
    result_dict, results = load_meta_series_logic(context, config, duckdb)

    # Convert to DataFrame only for IO manager compatibility
    result_df = pd.DataFrame(result_dict)

    context.add_output_metadata(
        {
            "series_loaded": MetadataValue.int(len(result_df)),
            "details": MetadataValue.json(results),
        }
    )

    return result_df
