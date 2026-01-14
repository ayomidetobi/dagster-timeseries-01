"""Bloomberg data ingestion assets using pyeqdr.pypdl."""

from typing import Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import DuckDBResource, PyPDLResource
from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_INGESTION,
    RETRY_POLICY_MAX_RETRIES_INGESTION,
)
from dagster_quickstart.utils.partitions import (
    BLOOMBERG_INGESTION_PARTITION,
    get_partition_date,
)

from .config import BloombergIngestionConfig
from .logic import ingest_bloomberg_data_for_series


@asset(
    group_name="bloomberg_ingestion",
    description="Ingest Bloomberg data for a single metaSeries using pyeqdr.pypdl",
    deps=[AssetKey("load_meta_series_from_csv")],
    kinds=["duckdb"],
    io_manager_key="duckdb_io_manager",
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "bloomberg": "", "pypdl": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=BLOOMBERG_INGESTION_PARTITION,
)
def ingest_bloomberg_data_pypdl(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    duckdb: DuckDBResource,
) -> Optional[dict]:
    """Ingest Bloomberg data for a single metaSeries using PyPDL.

    This asset uses multi-dimensional partitions (daily + dynamic series).
    Each partition processes data for one series (by series_code) for one date.
    Dagster handles concurrency by running multiple partitions in parallel.

    This asset:
    1. Gets the metaSeries by series_code from the partition key
    2. Gets the field_type_name from field_type lookup (should contain Bloomberg field code like "PX_LAST")
    3. Constructs data_source as "bloomberg/ts/{field_type_name}"
    4. Uses ticker as data_code
    5. Fetches data from Bloomberg via PyPDL for the partition date
    6. Saves data to S3 Parquet files

    Args:
        context: Dagster execution context (includes partition keys: date and series)
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        duckdb: DuckDB resource

    Returns:
        Result dictionary with ingestion results, or None if skipped/failed
        All reporting is done via Dagster metadata and AssetSummary
    """
    # Extract partition keys from multi-dimensional partition
    if context.partition_key is None:
        raise ValueError("Partition key is required for this asset")

    partition_keys = context.partition_key.keys_by_dimension
    if not partition_keys:
        raise ValueError("Partition keys are empty")

    date_key = partition_keys.get("date")
    series_code = partition_keys.get("series")

    if date_key is None:
        raise ValueError("Date partition key is required")
    if series_code is None:
        raise ValueError("Series partition key is required")

    # Get target date from partition
    target_date = get_partition_date(date_key)

    context.log.info(
        "Processing partition: series_code=%s, date=%s", series_code, target_date.date()
    )

    ingest_bloomberg_data_for_series(
        context, config, pypdl_resource, duckdb, series_code, target_date
    )
    return None
