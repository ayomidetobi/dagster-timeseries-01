"""Calculation logic for derived series."""

from datetime import datetime
from typing import Any

import pandas as pd
from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    CALCULATION_TYPES,
    DEFAULT_SMA_WINDOW,
    DEFAULT_WEIGHT_DIVISOR,
)
from dagster_quickstart.utils.exceptions import CalculationError, MetaSeriesNotFoundError
from dagster_quickstart.utils.helpers import (
    create_calculation_log,
    load_series_data_from_duckdb,
    update_calculation_log_on_error,
    update_calculation_log_on_success,
)
from dagster_quickstart.utils.summary import AssetSummary
from database.dependency import CalculationLogManager, DependencyManager
from database.meta_series import MetaSeriesManager

from .config import CalculationConfig


def calculate_sma_series_logic(
    context: AssetExecutionContext,
    config: CalculationConfig,
    duckdb: DuckDBResource,
    target_date: datetime,
) -> pd.DataFrame:
    """Calculate a simple moving average derived series.

    Args:
        context: Dagster execution context
        config: Calculation configuration
        duckdb: DuckDB resource
        target_date: Target date for calculation (from partition)

    Returns:
        DataFrame with calculated SMA series data

    Raises:
        MetaSeriesNotFoundError: If derived series not found
        CalculationError: If calculation fails
    """
    meta_manager = MetaSeriesManager(duckdb)
    dep_manager = DependencyManager(duckdb)
    calc_manager = CalculationLogManager(duckdb)

    # Get derived series metadata
    derived_series = meta_manager.get_or_validate_meta_series(
        config.derived_series_code, context, raise_if_not_found=True
    )

    # get_or_validate_meta_series with raise_if_not_found=True should never return None
    # but type checker doesn't know that, so we assert
    if derived_series is None:
        raise MetaSeriesNotFoundError(f"Derived series {config.derived_series_code} not found")

    derived_series_id = derived_series["series_id"]

    # Get parent dependencies
    parent_deps = dep_manager.get_parent_dependencies(derived_series_id)

    if not parent_deps:
        raise CalculationError(f"No parent dependencies found for {config.derived_series_code}")

    # Start calculation log
    input_series_ids = [dep["parent_series_id"] for dep in parent_deps]
    calc_id = create_calculation_log(
        calc_manager,
        derived_series_id,
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
            df = load_series_data_from_duckdb(duckdb, parent_id)
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
                "series_id": [derived_series_id] * len(merged),
                "timestamp": merged["timestamp"],
                "value": merged["value"],
            }
        )

        # Update calculation log
        update_calculation_log_on_success(calc_manager, calc_id, len(output_df))

        # Create summary using optimized AssetSummary class
        summary = AssetSummary.for_calculation(
            rows_calculated=len(output_df),
            calculation_id=calc_id,
            target_date=target_date,
            additional_metadata={"window_size": window},
        )
        summary.add_to_context(context)

        return output_df

    except Exception as e:
        # Update calculation log with error
        update_calculation_log_on_error(calc_manager, calc_id, str(e))
        raise CalculationError(f"Calculation failed: {e}") from e


def calculate_weighted_composite_logic(
    context: AssetExecutionContext,
    config: CalculationConfig,
    duckdb: DuckDBResource,
    target_date: datetime,
) -> pd.DataFrame:
    """Calculate a weighted composite derived series.

    Args:
        context: Dagster execution context
        config: Calculation configuration
        duckdb: DuckDB resource
        target_date: Target date for calculation (from partition)

    Returns:
        DataFrame with calculated weighted composite series data

    Raises:
        MetaSeriesNotFoundError: If derived series not found
        CalculationError: If calculation fails
    """
    meta_manager = MetaSeriesManager(duckdb)
    dep_manager = DependencyManager(duckdb)
    calc_manager = CalculationLogManager(duckdb)

    # Get derived series
    derived_series = meta_manager.get_or_validate_meta_series(
        config.derived_series_code, context, raise_if_not_found=True
    )

    # get_or_validate_meta_series with raise_if_not_found=True should never return None
    if derived_series is None:
        raise MetaSeriesNotFoundError(f"Derived series {config.derived_series_code} not found")

    derived_series_id = derived_series["series_id"]

    # Get parent dependencies with weights
    parent_deps = dep_manager.get_parent_dependencies(derived_series_id)

    if len(parent_deps) < 2:
        raise CalculationError("Weighted composite requires at least 2 parent series")

    # Start calculation log
    input_series_ids = [dep["parent_series_id"] for dep in parent_deps]
    calc_id = create_calculation_log(
        calc_manager,
        derived_series_id,
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

            df = load_series_data_from_duckdb(duckdb, parent_id)
            if df is not None:
                # Filter data to partition date
                df = df[df["timestamp"] <= target_date]
                if len(df) > 0:
                    parent_data[parent_id] = {"data": df, "weight": weight}

        if not parent_data:
            raise CalculationError("No parent data found")

        # Merge all series on timestamp
        all_timestamps_set: set[Any] = set()
        for data in parent_data.values():
            all_timestamps_set.update(data["data"]["timestamp"].tolist())

        all_timestamps = sorted(all_timestamps_set)
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
                "series_id": [derived_series_id] * len(result_df),
                "timestamp": result_df["timestamp"],
                "value": result_df["value"],
            }
        )

        # Update calculation log
        update_calculation_log_on_success(calc_manager, calc_id, len(output_df))

        # Create summary using optimized AssetSummary class
        summary = AssetSummary.for_calculation(
            rows_calculated=len(output_df),
            calculation_id=calc_id,
            target_date=target_date,
            additional_metadata={"num_parents": len(parent_deps)},
        )
        summary.add_to_context(context)

        return output_df

    except Exception as e:
        update_calculation_log_on_error(calc_manager, calc_id, str(e))
        raise CalculationError(f"Calculation failed: {e}") from e
