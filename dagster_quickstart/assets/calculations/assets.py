"""Derived series calculation assets with dependency awareness."""

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
from dagster_quickstart.utils.csv_loader_helpers import ensure_views_exist
from dagster_quickstart.utils.helpers import get_version_date
from dagster_quickstart.utils.partitions import CALCULATION_PARTITION, get_partition_date
from database.dependency import DependencyManager
from database.meta_series import MetaSeriesManager

from .config import CalculationConfig
from .logic import calculate_derived_series_logic


@asset(
    group_name="calculations",
    description="Calculate derived series using formula types: spread, fly, box, ratio",
    deps=[
        AssetKey("load_meta_series_from_csv"),  # Meta series must exist
        AssetKey("load_series_dependencies_from_csv"),  # Dependencies must exist
        # Note: ingest_bloomberg_data_pypdl is not a direct partition dependency because
        # calculation uses different partitions (child_series) than ingestion (parent_series).
        # The calculation logic loads parent data directly from S3 by series_code.
    ],  # Depends on metadata and dependencies assets
    kinds=["pandas", "duckdb", "s3"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_DEFAULT, delay=RETRY_POLICY_DELAY_DEFAULT
    ),
    partitions_def=CALCULATION_PARTITION,
)
def calculate_derived_series(
    context: AssetExecutionContext,
    config: CalculationConfig,
    duckdb: DuckDBResource,
) -> None:
    """Calculate derived series using formula types: spread, fly, box, ratio.

    Formula types:
    - SPREAD: entry1 - entry2
    - FLY: 2*entry1 - entry2 - entry3
    - BOX: (entry1 - entry2) - (entry3 - entry4)
    - RATIO: entry1 / entry2

    This asset uses multi-dimensional partitions (daily + dynamic child series).
    Each partition processes data for one derived series (by child_series_code) for one date.
    Dagster handles concurrency by running multiple partitions in parallel.

    Args:
        context: Dagster execution context (includes partition keys: date and child_series)
        config: Calculation configuration
        duckdb: DuckDB resource

    Returns:
        None (data is saved to S3 and summary is added to context)
    """
    # Extract partition keys from multi-dimensional partition
    if context.partition_key is None:
        raise ValueError("Partition key is required for this asset")

    partition_keys = context.partition_key.keys_by_dimension
    if not partition_keys:
        raise ValueError("Partition keys are empty")

    date_key = partition_keys.get("date")
    child_series_code = partition_keys.get("child_series")

    if date_key is None:
        raise ValueError("Date partition key is required")
    if child_series_code is None:
        raise ValueError("Child series partition key is required")

    # Get target date from partition
    target_date = get_partition_date(date_key)

    context.log.info(
        "Calculating derived series from dependencies, partition: child_series_code=%s, date=%s",
        child_series_code,
        target_date.date(),
    )

    # Get version date and ensure views exist before calculation
    version_date = get_version_date()
    meta_manager = MetaSeriesManager(duckdb)
    dep_manager = DependencyManager(duckdb)

    # Ensure views exist before calling logic (for querying meta series and dependencies)
    ensure_views_exist(
        context=context,
        duckdb=duckdb,
        version_date=version_date,
        create_view_funcs=[
            meta_manager.create_or_update_view,
            dep_manager.create_or_update_view,
        ],
    )

    calculate_derived_series_logic(
        context, config, duckdb, target_date, child_series_code
    )
