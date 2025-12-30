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
from dagster_quickstart.utils.partitions import DAILY_PARTITION, get_partition_date

from .config import BloombergIngestionConfig
from .logic import ingest_bloomberg_data


@asset(
    group_name="ingestion",
    description="Ingest Bloomberg data for all active metaSeries using pyeqdr.pypdl",
    deps=[AssetKey("load_meta_series_from_csv")],
    kinds=["clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "bloomberg": "", "pypdl": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=DAILY_PARTITION,
)
async def ingest_bloomberg_data_async(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest Bloomberg data for all active metaSeries using PyPDL (async).

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date.

    This asset:
    1. Fetches all active metaSeries with Bloomberg ticker source
    2. Gets the field_type_code from field_type lookup
    3. Constructs data_source as "bloomberg/ts/{field_type_code}"
    4. Uses ticker as data_code
    5. Fetches data from Bloomberg via PyPDL asynchronously for the partition date
    6. Saves data to ClickHouse valueData table

    Args:
        context: Dagster execution context (includes partition key)
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        clickhouse: ClickHouse resource

    Returns:
        Summary DataFrame with ingestion results
    """
    # Get partition date from context
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info("Processing partition %s (date: %s)", partition_key, target_date.date())

    # Run async ingestion function (Dagster handles the event loop)
    return await ingest_bloomberg_data(context, config, pypdl_resource, clickhouse, target_date)
