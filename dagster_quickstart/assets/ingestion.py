"""Data ingestion assets for multiple data sources."""

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.utils.exceptions import MetaSeriesNotFoundError
from dagster_quickstart.utils.helpers import get_or_validate_meta_series
from dagster_quickstart.utils.partitions import DAILY_PARTITION, get_partition_date
from database.meta_series import MetaSeriesManager


class IngestionConfig(Config):
    """Configuration for data ingestion."""

    source: str = "BLOOMBERG"
    ticker: str = "AAPL US Equity"
    series_code: str = "AAPL_PX_LAST"


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Bloomberg",
    deps=[AssetKey("load_meta_series_from_csv")],
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    partitions_def=DAILY_PARTITION,
)
def ingest_bloomberg_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from Bloomberg source.

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date.

    Args:
        context: Dagster execution context (includes partition key)
        config: Ingestion configuration
        clickhouse: ClickHouse resource

    Returns:
        DataFrame with ingested data for the partition date
    """
    # Get partition date from context
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info(
        "Ingesting Bloomberg data for ticker: %s, partition: %s (date: %s)",
        config.ticker,
        partition_key,
        target_date.date(),
    )

    # In a real implementation, this would connect to Bloomberg API
    # For now, we'll simulate with sample data for the partition date
    sample_data = pl.DataFrame(
        {
            "timestamp": [target_date],
            "value": [100.0],  # Sample value for the date
        }
    )

    # Get or validate meta series
    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    if meta_series is None:
        raise MetaSeriesNotFoundError(f"Meta series {config.series_code} not found")

    series_id = meta_series["series_id"]

    # Add series_id to dataframe
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "partition_date": MetadataValue.text(str(target_date.date())),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from LSEG",
    deps=[AssetKey("load_meta_series_from_csv")],  # Meta series must exist before ingestion
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    partitions_def=DAILY_PARTITION,
)
def ingest_lseg_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from LSEG source.

    This asset is partitioned by day for backfill-safety.
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info(
        "Ingesting LSEG data for ticker: %s, partition: %s (date: %s)",
        config.ticker,
        partition_key,
        target_date.date(),
    )

    # In a real implementation, this would connect to LSEG API
    sample_data = pl.DataFrame(
        {
            "timestamp": [target_date],
            "value": [200.0],  # Sample value for the date
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    series_id = meta_series["series_id"]
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "partition_date": MetadataValue.text(str(target_date.date())),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Hawkeye",
    deps=[AssetKey("load_meta_series_from_csv")],  # Meta series must exist before ingestion
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    partitions_def=DAILY_PARTITION,
)
def ingest_hawkeye_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from Hawkeye source.

    This asset is partitioned by day for backfill-safety.
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info(
        "Ingesting Hawkeye data for ticker: %s, partition: %s (date: %s)",
        config.ticker,
        partition_key,
        target_date.date(),
    )

    sample_data = pl.DataFrame(
        {
            "timestamp": [target_date],
            "value": [150.0],  # Sample value for the date
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    series_id = meta_series["series_id"]
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "partition_date": MetadataValue.text(str(target_date.date())),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Ramp",
    deps=[AssetKey("load_meta_series_from_csv")],  # Meta series must exist before ingestion
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    partitions_def=DAILY_PARTITION,
)
def ingest_ramp_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from Ramp source.

    This asset is partitioned by day for backfill-safety.
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info(
        "Ingesting Ramp data for ticker: %s, partition: %s (date: %s)",
        config.ticker,
        partition_key,
        target_date.date(),
    )

    sample_data = pl.DataFrame(
        {
            "timestamp": [target_date],
            "value": [175.0],  # Sample value for the date
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    series_id = meta_series["series_id"]
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "partition_date": MetadataValue.text(str(target_date.date())),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from OneTick",
    deps=[AssetKey("load_meta_series_from_csv")],  # Meta series must exist before ingestion
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(max_retries=3, delay=1.0),
    partitions_def=DAILY_PARTITION,
)
def ingest_onetick_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from OneTick source.

    This asset is partitioned by day for backfill-safety.
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info(
        "Ingesting OneTick data for ticker: %s, partition: %s (date: %s)",
        config.ticker,
        partition_key,
        target_date.date(),
    )

    sample_data = pl.DataFrame(
        {
            "timestamp": [target_date],
            "value": [125.0],  # Sample value for the date
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    series_id = meta_series["series_id"]
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "partition_date": MetadataValue.text(str(target_date.date())),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])
