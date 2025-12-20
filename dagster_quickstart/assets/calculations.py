"""Derived series calculation assets with dependency awareness."""

from typing import List
import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
)
from dagster_clickhouse.resources import ClickHouseResource
from database.meta_series import MetaSeriesManager
from database.dependency import DependencyManager, CalculationLogManager

from dagster_quickstart.utils.helpers import (
    load_series_data_from_clickhouse,
    get_or_validate_meta_series,
    create_calculation_log,
    update_calculation_log_on_success,
    update_calculation_log_on_error,
)
from dagster_quickstart.utils.exceptions import (
    MetaSeriesNotFoundError,
    CalculationError,
)
from dagster_quickstart.utils.constants import (
    CALCULATION_TYPES,
    DEFAULT_SMA_WINDOW,
    DEFAULT_WEIGHT_DIVISOR,
)


class CalculationConfig(Config):
    """Configuration for derived series calculation."""

    derived_series_code: str = "COMPOSITE_001"
    formula: str = "parent1 * 0.5 + parent2 * 0.5"  # e.g., "parent1 * 0.6 + parent2 * 0.4"
    input_series_codes: List[str] = []  # List of input series codes


@asset(kinds=["source"])
def customers() -> str:
    return "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv"



@asset(
    group_name="calculations",
    description="Calculate a simple moving average derived series",
    deps=[
        AssetKey("ingest_bloomberg_data"),
        AssetKey("ingest_lseg_data"),
        AssetKey("ingest_hawkeye_data"),
        AssetKey("ingest_ramp_data"),
        AssetKey("ingest_onetick_data"),
    ],  # Depends on ingestion assets completing first
    kinds=["pandas","clickhouse"],
)
def calculate_sma_series(
    context: AssetExecutionContext,
    config: CalculationConfig,
    clickhouse: ClickHouseResource,
) -> pd.DataFrame:
    """Calculate a simple moving average derived series."""
    context.log.info(f"Calculating SMA for series: {config.derived_series_code}")

    meta_manager = MetaSeriesManager(clickhouse)
    dep_manager = DependencyManager(clickhouse)
    calc_manager = CalculationLogManager(clickhouse)

    # Get derived series metadata
    derived_series = get_or_validate_meta_series(
        meta_manager, config.derived_series_code, context, raise_if_not_found=True
    )
    
    if derived_series is None:
        raise MetaSeriesNotFoundError(
            f"Derived series {config.derived_series_code} not found"
        )

    # Get parent dependencies
    parent_deps = dep_manager.get_parent_dependencies(derived_series["series_id"])

    if not parent_deps:
        raise CalculationError(
            f"No parent dependencies found for {config.derived_series_code}"
        )

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
        # Load parent series data
        all_data = []
        for dep in parent_deps:
            parent_id = dep["parent_series_id"]
            df = load_series_data_from_clickhouse(clickhouse, parent_id)
            if df is not None:
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
        window = (
            int(config.formula.split("_")[-1])
            if "_" in config.formula
            else DEFAULT_SMA_WINDOW
        )
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
        AssetKey("ingest_bloomberg_data"),
        AssetKey("ingest_lseg_data"),
        AssetKey("ingest_hawkeye_data"),
        AssetKey("ingest_ramp_data"),
        AssetKey("ingest_onetick_data"),
    ],  # Depends on ingestion assets completing first
    kinds=["pandas","clickhouse"],
)
def calculate_weighted_composite(
    context: AssetExecutionContext,
    config: CalculationConfig,
    clickhouse: ClickHouseResource,
) -> pd.DataFrame:
    """Calculate a weighted composite derived series."""
    context.log.info(f"Calculating weighted composite: {config.derived_series_code}")

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
        # Load all parent series
        parent_data = {}
        for dep in parent_deps:
            parent_id = dep["parent_series_id"]
            weight = dep.get("weight", DEFAULT_WEIGHT_DIVISOR / len(parent_deps))

            df = load_series_data_from_clickhouse(clickhouse, parent_id)
            if df is not None:
                parent_data[parent_id] = {"data": df, "weight": weight}

        if not parent_data:
            raise CalculationError("No parent data found")

        # Merge all series on timestamp
        all_timestamps = set()
        for data in parent_data.values():
            all_timestamps.update(data["data"]["timestamp"].tolist())

        all_timestamps = sorted(all_timestamps)
        result_df = pd.DataFrame({"timestamp": all_timestamps})

        # Calculate weighted sum
        result_df["value"] = 0.0
        for parent_id, parent_info in parent_data.items():
            df = parent_info["data"]
            weight = parent_info["weight"]
            merged = result_df.merge(df, on="timestamp", how="left")
            result_df["value"] += merged["value"].fillna(0) * weight

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

