"""Data ingestion assets for multiple data sources."""

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_INGESTION,
    RETRY_POLICY_MAX_RETRIES_INGESTION,
)
from dagster_quickstart.utils.partitions import DAILY_PARTITION, get_partition_date
from database.models import TickerSource

from .config import IngestionConfig
from .logic import ingest_data_for_ticker_source


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from LSEG for all active series",
    deps=[AssetKey("load_meta_series_from_csv")],
    io_manager_key="polars_parquet_io_manager",
    kinds=["duckdb"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=DAILY_PARTITION,
)
def ingest_lseg_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    duckdb: DuckDBResource,
) -> pl.DataFrame:
    """Ingest data from LSEG source for all active series.

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date. It fetches all active metaSeries with LSEG ticker source
    and ingests data for each series.

    Args:
        context: Dagster execution context (includes partition key)
        config: Ingestion configuration
        duckdb: DuckDB resource

    Returns:
        Summary DataFrame with ingestion results
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info("Processing partition %s (date: %s)", partition_key, target_date.date())

    return ingest_data_for_ticker_source(context, config, duckdb, TickerSource.LSEG, target_date)


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Hawkeye for all active series",
    deps=[AssetKey("load_meta_series_from_csv")],
    io_manager_key="polars_parquet_io_manager",
    kinds=["duckdb"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=DAILY_PARTITION,
)
def ingest_hawkeye_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    duckdb: DuckDBResource,
) -> pl.DataFrame:
    """Ingest data from Hawkeye source for all active series.

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date. It fetches all active metaSeries with Hawkeye ticker source
    and ingests data for each series.

    Args:
        context: Dagster execution context (includes partition key)
        config: Ingestion configuration
        duckdb: DuckDB resource

    Returns:
        Summary DataFrame with ingestion results
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info("Processing partition %s (date: %s)", partition_key, target_date.date())

    return ingest_data_for_ticker_source(context, config, duckdb, TickerSource.HAWKEYE, target_date)


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Ramp for all active series",
    deps=[AssetKey("load_meta_series_from_csv")],
    io_manager_key="polars_parquet_io_manager",
    kinds=["duckdb"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=DAILY_PARTITION,
)
def ingest_ramp_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    duckdb: DuckDBResource,
) -> pl.DataFrame:
    """Ingest data from Ramp source for all active series.

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date. It fetches all active metaSeries with Ramp ticker source
    and ingests data for each series.

    Args:
        context: Dagster execution context (includes partition key)
        config: Ingestion configuration
        duckdb: DuckDB resource

    Returns:
        Summary DataFrame with ingestion results
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info("Processing partition %s (date: %s)", partition_key, target_date.date())

    return ingest_data_for_ticker_source(context, config, duckdb, TickerSource.RAMP, target_date)


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from OneTick for all active series",
    deps=[AssetKey("load_meta_series_from_csv")],
    io_manager_key="polars_parquet_io_manager",
    kinds=["duckdb"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=DAILY_PARTITION,
)
def ingest_onetick_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    duckdb: DuckDBResource,
) -> pl.DataFrame:
    """Ingest data from OneTick source for all active series.

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date. It fetches all active metaSeries with OneTick ticker source
    and ingests data for each series.

    Args:
        context: Dagster execution context (includes partition key)
        config: Ingestion configuration
        duckdb: DuckDB resource

    Returns:
        Summary DataFrame with ingestion results
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info("Processing partition %s (date: %s)", partition_key, target_date.date())

    return ingest_data_for_ticker_source(context, config, duckdb, TickerSource.ONETICK, target_date)


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Bloomberg for all active series",
    deps=[AssetKey("load_meta_series_from_csv")],
    io_manager_key="polars_parquet_io_manager",
    kinds=["duckdb"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=DAILY_PARTITION,
)
def ingest_bloomberg_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    duckdb: DuckDBResource,
) -> pl.DataFrame:
    """Ingest data from Bloomberg source for all active series.

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date. It fetches all active metaSeries with Bloomberg ticker source
    and ingests data for each series.

    Args:
        context: Dagster execution context (includes partition key)
        config: Ingestion configuration
        duckdb: DuckDB resource

    Returns:
        Summary DataFrame with ingestion results
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info("Processing partition %s (date: %s)", partition_key, target_date.date())

    return ingest_data_for_ticker_source(
        context, config, duckdb, TickerSource.BLOOMBERG, target_date
    )
