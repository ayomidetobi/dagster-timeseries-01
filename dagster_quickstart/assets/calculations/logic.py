"""Calculation logic for derived series."""

from datetime import datetime

import pandas as pd
from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.resources.duckdb_datacacher import SQL
from dagster_quickstart.utils.constants import S3_BASE_PATH_VALUE_DATA
from dagster_quickstart.utils.exceptions import CalculationError, MetaSeriesNotFoundError
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_value_data_path,
    save_value_data_to_s3,
)
from dagster_quickstart.utils.summary import AssetSummary
from database.dependency import DependencyManager
from database.meta_series import MetaSeriesManager

from .config import CalculationConfig


def calculate_derived_series_logic(
    context: AssetExecutionContext,
    config: CalculationConfig,
    duckdb: DuckDBResource,
    target_date: datetime,
    child_series_code: str,
) -> None:
    """Calculate derived series using formula types: spread, fly, box, ratio.

    Formula types:
    - spread: entry1 - entry2
    - fly: 2*entry1 - entry2 - entry3
    - box: (entry1 - entry2) - (entry3 - entry4)
    - ratio: entry1 / entry2

    Args:
        context: Dagster execution context
        config: Calculation configuration (formula should be one of: SPREAD, FLY, BOX, RATIO)
        duckdb: DuckDB resource
        target_date: Target date for calculation (from partition)
        child_series_code: Child series code (derived series) from partition key

    Returns:
        None (data is saved to S3 and summary is added to context)

    Raises:
        MetaSeriesNotFoundError: If derived series not found
        CalculationError: If calculation fails
    """
    meta_manager = MetaSeriesManager(duckdb)
    dep_manager = DependencyManager(duckdb)

    # Use the child series code from partition key
    derived_series_code = child_series_code
    context.log.info(
        f"Calculating derived series: {derived_series_code} for date: {target_date.date()}"
    )

    # Get derived series metadata
    derived_series = meta_manager.get_or_validate_meta_series(
        derived_series_code, context, raise_if_not_found=True
    )

    if derived_series is None:
        raise MetaSeriesNotFoundError(f"Derived series {derived_series_code} not found")

    derived_series_id = derived_series["series_id"]
    derived_series_code = derived_series["series_code"]

    # Get parent dependencies (ordered by dependency_id for consistent entry ordering)
    parent_deps = dep_manager.get_parent_dependencies(derived_series_id)
    parent_deps.sort(key=lambda x: x.get("dependency_id", 0))

    if not parent_deps:
        raise CalculationError(f"No parent dependencies found for {derived_series_code}")

    # Determine calculation type from calc_type in dependency or config
    calc_type = None
    if parent_deps and "calc_type" in parent_deps[0]:
        calc_type = parent_deps[0]["calc_type"]
    elif config.formula:
        # Try to infer from formula string
        formula_upper = config.formula.upper()
        if "SPREAD" in formula_upper:
            calc_type = "SPREAD"
        elif "FLY" in formula_upper:
            calc_type = "FLY"
        elif "BOX" in formula_upper:
            calc_type = "BOX"
        elif "RATIO" in formula_upper:
            calc_type = "RATIO"

    if not calc_type:
        raise CalculationError(
            f"Could not determine calculation type for {derived_series_code}. "
            f"Set calc_type in dependencies or formula in config."
        )

    # Validate number of parents based on calc type
    num_parents = len(parent_deps)
    if calc_type == "SPREAD" and num_parents < 2:
        raise CalculationError(f"SPREAD calculation requires at least 2 parent series, got {num_parents}")
    elif calc_type == "FLY" and num_parents < 3:
        raise CalculationError(f"FLY calculation requires at least 3 parent series, got {num_parents}")
    elif calc_type == "BOX" and num_parents < 4:
        raise CalculationError(f"BOX calculation requires at least 4 parent series, got {num_parents}")
    elif calc_type == "RATIO" and num_parents < 2:
        raise CalculationError(f"RATIO calculation requires at least 2 parent series, got {num_parents}")

    # Get input series IDs for metadata
    input_series_ids = [dep["parent_series_id"] for dep in parent_deps]
    formula = config.formula or calc_type

    try:
        # Get all parent series codes in one query by joining dependencies with metaSeries
        parent_series_query = f"""
        SELECT 
            d.parent_series_id,
            d.dependency_id,
            m.series_code as parent_series_code
        FROM seriesDependencyGraph d
        INNER JOIN metaSeries m ON d.parent_series_id = m.series_id
        WHERE d.child_series_id = {derived_series_id}
        ORDER BY d.dependency_id
        """
        parent_series_result = duckdb.execute_query(parent_series_query)
        
        if parent_series_result is None or parent_series_result.empty:
            raise CalculationError("No parent series found in metaSeries for dependencies")

        # Build UNION ALL query to load all parent data in one query
        union_parts = []
        parent_series_map = {}  # Map parent_series_id to index for column naming
        
        for idx, row in parent_series_result.iterrows():
            parent_series_id = row["parent_series_id"]
            parent_series_code = row["parent_series_code"]
            parent_series_map[parent_series_id] = idx
            
            # Build S3 path pattern to load all partitions for this series_code
            # Path structure: value-data/series_code={series_code}/date=*/data.parquet
            relative_path_pattern = f"{S3_BASE_PATH_VALUE_DATA}/series_code={parent_series_code}/date=*/data.parquet"
            
            if SQL is not None:
                # Use SQL class for S3 path resolution with glob pattern
                union_parts.append(f"""
                    SELECT 
                        timestamp, 
                        value,
                        {parent_series_id} as parent_series_id
                    FROM read_parquet('$file_path')
                    WHERE timestamp <= $target_date
                """)
            else:
                # Fallback: build full S3 path and query
                full_s3_path = build_full_s3_path(duckdb, relative_path_pattern)
                union_parts.append(f"""
                    SELECT 
                        timestamp, 
                        value,
                        {parent_series_id} as parent_series_id
                    FROM read_parquet('{full_s3_path}')
                    WHERE timestamp <= '{target_date.isoformat()}'
                """)
        
        if not union_parts:
            raise CalculationError("No parent series data paths to load")

        # Combine all UNION parts
        union_query = " UNION ALL ".join(union_parts)
        union_query += " ORDER BY timestamp"
        
        # Execute the combined query
        if SQL is not None:
            # Build SQL object with bindings for all file paths
            # Note: We need to handle multiple file_path bindings
            # For now, use execute_query with the resolved query
            # Get all file paths
            file_paths = [
                f"{S3_BASE_PATH_VALUE_DATA}/series_code={row['parent_series_code']}/date=*/data.parquet"
                for _, row in parent_series_result.iterrows()
            ]
            # Replace $file_path and $target_date in union_query
            resolved_query = union_query.replace("$target_date", f"'{target_date.isoformat()}'")
            # For multiple file paths, we need to replace each occurrence
            # Since we have one per UNION part, replace them sequentially
            for idx, file_path in enumerate(file_paths):
                if SQL is not None:
                    full_s3_path = build_full_s3_path(duckdb, file_path)
                    resolved_query = resolved_query.replace("read_parquet('$file_path')", f"read_parquet('{full_s3_path}')", 1)
            
            all_parent_data = duckdb.execute_query(resolved_query)
        else:
            all_parent_data = duckdb.execute_query(union_query)

        if all_parent_data is None or all_parent_data.empty:
            raise CalculationError("No parent data found for calculation")

        # Pivot the data: group by timestamp and create columns for each parent
        merged = all_parent_data.pivot_table(
            index="timestamp",
            columns="parent_series_id",
            values="value",
            aggfunc="first"
        )
        
        # Rename columns to value_0, value_1, etc. based on parent_series_id order
        column_mapping = {}
        for idx, parent_id in enumerate(input_series_ids):
            if parent_id in merged.columns:
                column_mapping[parent_id] = f"value_{idx}"
        
        merged = merged.rename(columns=column_mapping)
        merged = merged.reset_index()

        # Calculate based on formula type
        if calc_type == "SPREAD":
            # spread: entry1 - entry2
            merged["value"] = merged["value_0"] - merged["value_1"]
        elif calc_type == "FLY":
            # fly: 2*entry1 - entry2 - entry3
            merged["value"] = 2 * merged["value_0"] - merged["value_1"] - merged["value_2"]
        elif calc_type == "BOX":
            # box: (entry1 - entry2) - (entry3 - entry4)
            merged["value"] = (merged["value_0"] - merged["value_1"]) - (merged["value_2"] - merged["value_3"])
        elif calc_type == "RATIO":
            # ratio: entry1 / entry2 (handle division by zero)
            merged["value"] = merged["value_0"] / merged["value_1"].replace(0, pd.NA)
            merged["value"] = merged["value"].fillna(0)  # Replace NaN with 0 for division by zero
        else:
            raise CalculationError(f"Unknown calculation type: {calc_type}")

        # Prepare output DataFrame
        output_df = pd.DataFrame(
            {
                "series_id": [derived_series_id] * len(merged),
                "timestamp": merged["timestamp"],
                "value": merged["value"],
            }
        )

        # Remove rows with null values
        output_df = output_df.dropna(subset=["value"])

        if len(output_df) == 0:
            raise CalculationError("No valid calculated values after processing")

        # Save to S3 using DuckDB macro
        relative_path = build_s3_value_data_path(derived_series_code, target_date)
        # Convert DataFrame to list of dicts for save_value_data_to_s3
        value_data_list = output_df.to_dict("records")
        save_value_data_to_s3(
            duckdb=duckdb,
            value_data=value_data_list,
            series_code=derived_series_code,
            partition_date=target_date,
            context=context,
        )

        # Create summary with calculation log information
        summary = AssetSummary.for_calculation(
            rows_calculated=len(output_df),
            target_date=target_date,
            calculation_type=calc_type,
            formula=formula,
            parent_series_count=num_parents,
            additional_metadata={
                "status": "completed",
                "input_series_ids": input_series_ids,
                "derived_series_id": derived_series_id,
                "derived_series_code": derived_series_code,
                "parameters": formula,
            },
        )
        summary.add_to_context(context)

        context.log.info(
            f"Calculated {calc_type} for {derived_series_code}: {len(output_df)} rows saved to {relative_path}"
        )

    except Exception as e:
        # Create summary with error information
        error_summary = AssetSummary.for_calculation(
            rows_calculated=0,
            target_date=target_date,
            calculation_type=calc_type,
            formula=formula,
            parent_series_count=num_parents,
            additional_metadata={
                "status": "failed",
                "error_message": str(e),
                "input_series_ids": input_series_ids,
                "derived_series_id": derived_series_id,
                "derived_series_code": derived_series_code,
                "parameters": formula,
            },
        )
        error_summary.add_to_context(context)
        raise CalculationError(f"Calculation failed: {e}") from e
