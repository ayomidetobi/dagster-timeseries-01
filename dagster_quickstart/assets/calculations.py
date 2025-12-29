"""Derived series calculation assets with dependency awareness."""

from typing import Any, List

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.utils.constants import (
    CALCULATION_TYPES,
    DEFAULT_SMA_WINDOW,
    DEFAULT_WEIGHT_DIVISOR,
    RETRY_POLICY_DELAY_DEFAULT,
    RETRY_POLICY_MAX_RETRIES_DEFAULT,
)
from dagster_quickstart.utils.exceptions import (
    CalculationError,
    MetaSeriesNotFoundError,
)
from dagster_quickstart.utils.helpers import (
    create_calculation_log,
    get_or_validate_meta_series,
    load_series_data_from_clickhouse,
    update_calculation_log_on_error,
    update_calculation_log_on_success,
)
from dagster_quickstart.utils.partitions import DAILY_PARTITION, get_partition_date
from database.dependency import CalculationLogManager, DependencyManager
from database.meta_series import MetaSeriesManager


class CalculationConfig(Config):
    """Configuration for derived series calculation."""

    derived_series_code: str = (
        "TECH_COMPOSITE"  # Must match series_code in meta_series.csv, not series_name
    )
    formula: str = "parent1 * 0.5 + parent2 * 0.5"  # e.g., "parent1 * 0.6 + parent2 * 0.4"
    input_series_codes: List[str] = []  # List of input series codes


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
    kinds=["pandas", "clickhouse"],
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
    clickhouse: ClickHouseResource,
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

    meta_manager = MetaSeriesManager(clickhouse)
    dep_manager = DependencyManager(clickhouse)
    calc_manager = CalculationLogManager(clickhouse)

    # Get derived series metadata
    derived_series = get_or_validate_meta_series(
        meta_manager, config.derived_series_code, context, raise_if_not_found=True
    )

    if derived_series is None:
        raise MetaSeriesNotFoundError(f"Derived series {config.derived_series_code} not found")

    # Get parent dependencies
    parent_deps = dep_manager.get_parent_dependencies(derived_series["series_id"])

    if not parent_deps:
        raise CalculationError(f"No parent dependencies found for {config.derived_series_code}")

    # Start calculation log
    input_series_ids = [dep["parent_series_id"] for dep in parent_deps]
    calc_id = create_calculation_log(
        calc_manager,
        derived_series["series_id"],
        CALCULATION_TYPES["SMA"],
        config.formula,
        input_series_ids,
        parameters=config.formula,
    )

    try:
        # Load parent series data up to partition date
        all_data = []
        for dep in parent_deps:
            parent_id = dep["parent_series_id"]
            df = load_series_data_from_clickhouse(clickhouse, parent_id)
            if df is not None:
                # Filter data to partition date
                df = df[df["timestamp"] <= target_date]
                if len(df) > 0:
                    all_data.append(df)

        if not all_data:
            raise CalculationError("No parent data found")

        # Merge all parent series on timestamp
        merged = all_data[0]
        for df in all_data[1:]:
            merged = merged.merge(df, on="timestamp", how="outer", suffixes=("", "_new"))
            merged["value"] = merged["value"].fillna(0) + merged.get("value_new", 0).fillna(0)
            merged = merged.drop(columns=[col for col in merged.columns if col.endswith("_new")])

        # Calculate SMA (assuming formula contains window size, e.g., "SMA_20")
        window = int(config.formula.split("_")[-1]) if "_" in config.formula else DEFAULT_SMA_WINDOW
        merged["value"] = merged["value"].rolling(window=window, min_periods=1).mean()

        # Prepare output
        output_df = pd.DataFrame(
            {
                "series_id": [derived_series["series_id"]] * len(merged),
                "timestamp": merged["timestamp"],
                "value": merged["value"],
            }
        )

        # Update calculation log
        update_calculation_log_on_success(calc_manager, calc_id, len(output_df))

        context.add_output_metadata(
            {
                "rows_calculated": MetadataValue.int(len(output_df)),
                "calculation_id": MetadataValue.int(calc_id),
                "window_size": MetadataValue.int(window),
            }
        )

        return output_df

    except Exception as e:
        # Update calculation log with error
        update_calculation_log_on_error(calc_manager, calc_id, str(e))
        raise CalculationError(f"Calculation failed: {e}") from e


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
    kinds=["pandas", "clickhouse"],
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
    clickhouse: ClickHouseResource,
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

    meta_manager = MetaSeriesManager(clickhouse)
    dep_manager = DependencyManager(clickhouse)
    calc_manager = CalculationLogManager(clickhouse)

    # Get derived series
    derived_series = get_or_validate_meta_series(
        meta_manager, config.derived_series_code, context, raise_if_not_found=True
    )

    # Get parent dependencies with weights
    parent_deps = dep_manager.get_parent_dependencies(derived_series["series_id"])

    if len(parent_deps) < 2:
        raise CalculationError("Weighted composite requires at least 2 parent series")

    # Start calculation log
    input_series_ids = [dep["parent_series_id"] for dep in parent_deps]
    calc_id = create_calculation_log(
        calc_manager,
        derived_series["series_id"],
        CALCULATION_TYPES["WEIGHTED_COMPOSITE"],
        config.formula,
        input_series_ids,
        parameters=config.formula,
    )

    try:
        # Load all parent series up to partition date
        parent_data = {}
        for dep in parent_deps:
            parent_id = dep["parent_series_id"]
            weight = dep.get("weight", DEFAULT_WEIGHT_DIVISOR / len(parent_deps))

            df = load_series_data_from_clickhouse(clickhouse, parent_id)
            if df is not None:
                # Filter data to partition date
                df = df[df["timestamp"] <= target_date]
                if len(df) > 0:
                    parent_data[parent_id] = {"data": df, "weight": weight}

        if not parent_data:
            raise CalculationError("No parent data found")

        # Merge all series on timestamp
        all_timestamps = set[Any]()
        for data in parent_data.values():
            all_timestamps.update(data["data"]["timestamp"].tolist())

        all_timestamps = sorted(all_timestamps)
        result_df = pd.DataFrame({"timestamp": all_timestamps})

        # Calculate weighted sum
        result_df["value"] = 0.0
        for parent_id, parent_info in parent_data.items():
            df = parent_info["data"]
            weight = parent_info["weight"]
            merged = result_df.merge(df, on="timestamp", how="left", suffixes=("", "_y"))
            result_df["value"] += merged["value_y"].fillna(0) * weight

        # Prepare output
        output_df = pd.DataFrame(
            {
                "series_id": [derived_series["series_id"]] * len(result_df),
                "timestamp": result_df["timestamp"],
                "value": result_df["value"],
            }
        )

        # Update calculation log
        update_calculation_log_on_success(calc_manager, calc_id, len(output_df))

        context.add_output_metadata(
            {
                "rows_calculated": MetadataValue.int(len(output_df)),
                "calculation_id": MetadataValue.int(calc_id),
                "num_parents": MetadataValue.int(len(parent_deps)),
            }
        )

        return output_df

    except Exception as e:
        update_calculation_log_on_error(calc_manager, calc_id, str(e))
        raise CalculationError(f"Calculation failed: {e}") from e
