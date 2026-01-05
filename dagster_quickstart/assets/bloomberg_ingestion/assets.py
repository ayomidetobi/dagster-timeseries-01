"""Bloomberg data ingestion assets using pyeqdr.pypdl."""

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import ClickHouseResource, PyPDLResource
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
    kinds=["clickhouse"],
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
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest Bloomberg data for a single metaSeries using PyPDL.

    This asset uses multi-dimensional partitions (daily + dynamic series).
    Each partition processes data for one series (by series_id) for one date.
    Dagster handles concurrency by running multiple partitions in parallel.

    This asset:
    1. Gets the metaSeries by series_id from the partition key
    2. Gets the field_type_name from field_type lookup (should contain Bloomberg field code like "PX_LAST")
    3. Constructs data_source as "bloomberg/ts/{field_type_name}"
    4. Uses ticker as data_code
    5. Fetches data from Bloomberg via PyPDL for the partition date
    6. Saves data to ClickHouse valueData table

    Args:
        context: Dagster execution context (includes partition keys: date and series)
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        clickhouse: ClickHouse resource

    Returns:
        Summary DataFrame with ingestion results
    """
    # Extract partition keys from multi-dimensional partition
    if context.partition_key is None:
        raise ValueError("Partition key is required for this asset")

    partition_keys = context.partition_key.keys_by_dimension
    if not partition_keys:
        raise ValueError("Partition keys are empty")

    date_key = partition_keys.get("date")
    series_id_str = partition_keys.get("series")

    if date_key is None:
        raise ValueError("Date partition key is required")
    if series_id_str is None:
        raise ValueError("Series partition key is required")

    # Convert series_id from string to int (partition keys are strings)
    try:
        series_id = int(series_id_str)
    except ValueError:
        context.log.error(f"Invalid series_id in partition key: {series_id_str}")
        raise ValueError(f"Invalid series_id: {series_id_str}")

    # Get target date from partition
    target_date = get_partition_date(date_key)

    context.log.info("Processing partition: series_id=%s, date=%s", series_id, target_date.date())

    # Run ingestion function for single series and date
    return ingest_bloomberg_data_for_series(
        context, config, pypdl_resource, clickhouse, series_id, target_date
    )
