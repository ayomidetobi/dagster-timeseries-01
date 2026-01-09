"""Derived series calculation assets with dependency awareness."""

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_DEFAULT,
    RETRY_POLICY_MAX_RETRIES_DEFAULT,
)
from dagster_quickstart.utils.partitions import DAILY_PARTITION, get_partition_date

from .config import CalculationConfig
from .logic import calculate_sma_series_logic, calculate_weighted_composite_logic


@asset(
    group_name="calculations",
    description="Calculate a simple moving average derived series",
    deps=[
        AssetKey("init_database_schema"),  # Database schema must be initialized first
        AssetKey("load_meta_series_from_csv"),  # Meta series must exist before calculation
        AssetKey("ingest_bloomberg_data"),
        AssetKey("ingest_lseg_data"),
        AssetKey("ingest_hawkeye_data"),
        AssetKey("ingest_ramp_data"),
        AssetKey("ingest_onetick_data"),
    ],  # Depends on schema, metadata and ingestion assets completing first
    kinds=["pandas", "duckdb"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_DEFAULT, delay=RETRY_POLICY_DELAY_DEFAULT
    ),
    partitions_def=DAILY_PARTITION,
)
def calculate_sma_series(
    context: AssetExecutionContext,
    config: CalculationConfig,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    """Calculate a simple moving average derived series.

    This asset is partitioned by day for backfill-safety. Each partition calculates
    the SMA for data up to and including the partition date.
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info(
        "Calculating SMA for series: %s, partition: %s (date: %s)",
        config.derived_series_code,
        partition_key,
        target_date.date(),
    )

    return calculate_sma_series_logic(context, config, duckdb, target_date)


@asset(
    group_name="calculations",
    description="Calculate a weighted composite derived series",
    deps=[
        AssetKey("init_database_schema"),  # Database schema must be initialized first
        AssetKey("load_meta_series_from_csv"),  # Meta series must exist before calculation
        AssetKey("ingest_bloomberg_data"),
        AssetKey("ingest_lseg_data"),
        AssetKey("ingest_hawkeye_data"),
        AssetKey("ingest_ramp_data"),
        AssetKey("ingest_onetick_data"),
    ],  # Depends on schema, metadata and ingestion assets completing first
    kinds=["pandas", "duckdb"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_DEFAULT, delay=RETRY_POLICY_DELAY_DEFAULT
    ),
    partitions_def=DAILY_PARTITION,
)
def calculate_weighted_composite(
    context: AssetExecutionContext,
    config: CalculationConfig,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    """Calculate a weighted composite derived series.

    This asset is partitioned by day for backfill-safety. Each partition calculates
    the weighted composite for data up to and including the partition date.
    """
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info(
        "Calculating weighted composite: %s, partition: %s (date: %s)",
        config.derived_series_code,
        partition_key,
        target_date.date(),
    )

    return calculate_weighted_composite_logic(context, config, duckdb, target_date)
