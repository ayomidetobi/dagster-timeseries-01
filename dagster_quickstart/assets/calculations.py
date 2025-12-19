"""Derived series calculation assets with dependency awareness."""

from typing import Dict, Any, List
from datetime import datetime
import pandas as pd
import numpy as np
from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MetadataValue,
    multi_asset,
    AssetOut,
    Output,
)
from dagster_clickhouse.resources import ClickHouseResource
from database.meta_series import MetaSeriesManager
from database.dependency import DependencyManager, CalculationLogManager
from database.models import CalculationLogBase, CalculationStatus


class CalculationConfig(Config):
    """Configuration for derived series calculation."""

    derived_series_code: str
    formula: str  # e.g., "parent1 * 0.6 + parent2 * 0.4"
    input_series_codes: List[str]


@asset
def customers() -> str:
    return "https://raw.githubusercontent.com/dbt-labs/jaffle-shop-classic/refs/heads/main/seeds/raw_customers.csv"



@asset(
    group_name="calculations",
    description="Calculate a simple moving average derived series",
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
    derived_series = meta_manager.get_meta_series_by_code(config.derived_series_code)
    if not derived_series:
        raise ValueError(f"Derived series {config.derived_series_code} not found")

    # Get parent dependencies
    parent_deps = dep_manager.get_parent_dependencies(derived_series["series_id"])

    if not parent_deps:
        raise ValueError(f"No parent dependencies found for {config.derived_series_code}")

    # Start calculation log
    calc_log = CalculationLogBase(
        series_id=derived_series["series_id"],
        calculation_type="SMA",
        status=CalculationStatus.RUNNING,
        input_series_ids=[dep["parent_series_id"] for dep in parent_deps],
        parameters=config.formula,
        formula=config.formula,
        execution_start=datetime.now(),
    )
    calc_id = calc_manager.create_calculation_log(calc_log)

    try:
        # Load parent series data
        all_data = []
        for dep in parent_deps:
            parent_id = dep["parent_series_id"]
            query = """
            SELECT timestamp, value
            FROM valueData
            WHERE series_id = {series_id:UInt32}
            ORDER BY timestamp
            """
            result = clickhouse.execute_query(query, parameters={"series_id": parent_id})
            if hasattr(result, "result_rows") and result.result_rows:
                df = pd.DataFrame(result.result_rows, columns=["timestamp", "value"])
                all_data.append(df)

        if not all_data:
            raise ValueError("No parent data found")

        # Merge all parent series on timestamp
        merged = all_data[0]
        for df in all_data[1:]:
            merged = merged.merge(df, on="timestamp", how="outer", suffixes=("", "_new"))
            merged["value"] = merged["value"].fillna(0) + merged.get("value_new", 0).fillna(0)
            merged = merged.drop(columns=[col for col in merged.columns if col.endswith("_new")])

        # Calculate SMA (assuming formula contains window size, e.g., "SMA_20")
        window = int(config.formula.split("_")[-1]) if "_" in config.formula else 20
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
        calc_manager.update_calculation_log(
            calculation_id=calc_id,
            status=CalculationStatus.COMPLETED,
            execution_end=datetime.now(),
            rows_processed=len(output_df),
        )

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
        calc_manager.update_calculation_log(
            calculation_id=calc_id,
            status=CalculationStatus.FAILED,
            execution_end=datetime.now(),
            error_message=str(e),
        )
        raise


@asset(
    group_name="calculations",
    description="Calculate a weighted composite derived series",
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
    derived_series = meta_manager.get_meta_series_by_code(config.derived_series_code)
    if not derived_series:
        raise ValueError(f"Derived series {config.derived_series_code} not found")

    # Get parent dependencies with weights
    parent_deps = dep_manager.get_parent_dependencies(derived_series["series_id"])

    if len(parent_deps) < 2:
        raise ValueError("Weighted composite requires at least 2 parent series")

    # Start calculation log
    calc_log = CalculationLogBase(
        series_id=derived_series["series_id"],
        calculation_type="WEIGHTED_COMPOSITE",
        status=CalculationStatus.RUNNING,
        input_series_ids=[dep["parent_series_id"] for dep in parent_deps],
        parameters=config.formula,
        formula=config.formula,
        execution_start=datetime.now(),
    )
    calc_id = calc_manager.create_calculation_log(calc_log)

    try:
        # Load all parent series
        parent_data = {}
        for dep in parent_deps:
            parent_id = dep["parent_series_id"]
            weight = dep.get("weight", 1.0 / len(parent_deps))

            query = """
            SELECT timestamp, value
            FROM valueData
            WHERE series_id = {series_id:UInt32}
            ORDER BY timestamp
            """
            result = clickhouse.execute_query(query, parameters={"series_id": parent_id})
            if hasattr(result, "result_rows") and result.result_rows:
                df = pd.DataFrame(result.result_rows, columns=["timestamp", "value"])
                parent_data[parent_id] = {"data": df, "weight": weight}

        if not parent_data:
            raise ValueError("No parent data found")

        # Merge all series on timestamp
        all_timestamps = set()
        for data in parent_data.values():
            all_timestamps.update(data["data"]["timestamp"].tolist())

        all_timestamps = sorted(list(all_timestamps))
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
        calc_manager.update_calculation_log(
            calculation_id=calc_id,
            status=CalculationStatus.COMPLETED,
            execution_end=datetime.now(),
            rows_processed=len(output_df),
        )

        context.add_output_metadata(
            {
                "rows_calculated": MetadataValue.int(len(output_df)),
                "calculation_id": MetadataValue.int(calc_id),
                "num_parents": MetadataValue.int(len(parent_deps)),
            }
        )

        return output_df

    except Exception as e:
        calc_manager.update_calculation_log(
            calculation_id=calc_id,
            status=CalculationStatus.FAILED,
            execution_end=datetime.now(),
            error_message=str(e),
        )
        raise

