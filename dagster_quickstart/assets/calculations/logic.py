"""Calculation logic for derived series."""

from datetime import datetime
from typing import List

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.duckdb_helpers import (
    build_pivot_columns,
    build_union_query_for_parents,
)
from dagster_quickstart.utils.exceptions import CalculationError, MetaSeriesNotFoundError
from dagster_quickstart.utils.helpers import (
    save_value_data_to_s3,
)
from dagster_quickstart.utils.summary import AssetSummary
from dagster_quickstart.utils.validation_helpers import (
    determine_calculation_type,
    validate_calculation_parent_count,
    validate_parent_dependencies_exist,
    validate_parent_series_in_metaseries,
)
from database.dependency import DependencyManager
from database.meta_series import MetaSeriesManager

from .config import CalculationConfig


def _build_calculation_sql_query(
    union_parts: List[str],
    input_series_ids: List[int],
    calc_type: str,
    derived_series_id: int,
    target_date: datetime,
) -> str:
    """Build SQL query to perform calculation entirely in DuckDB.

    Args:
        union_parts: List of SQL SELECT statements for each parent series
        input_series_ids: List of parent series IDs in order
        calc_type: Calculation type (SPREAD, FLY, BOX, RATIO)
        derived_series_id: Derived series ID
        target_date: Target date for filtering

    Returns:
        Complete SQL query string that performs pivot and calculation
    """
    # Build UNION ALL query for all parent data
    union_query = " UNION ALL ".join(union_parts)

    # Build pivot columns using conditional aggregation
    pivot_select = build_pivot_columns(input_series_ids)

    # Build calculation formula based on type
    if calc_type == "SPREAD":
        # spread: entry1 - entry2
        calc_formula = "value_0 - value_1"
    elif calc_type == "FLY":
        # fly: 2*entry1 - entry2 - entry3
        calc_formula = "2 * value_0 - value_1 - value_2"
    elif calc_type == "BOX":
        # box: (entry1 - entry2) - (entry3 - entry4)
        calc_formula = "(value_0 - value_1) - (value_2 - value_3)"
    elif calc_type == "RATIO":
        # ratio: entry1 / entry2 (handle division by zero with NULLIF)
        calc_formula = "CASE WHEN value_1 = 0 THEN 0 ELSE value_0 / value_1 END"
    else:
        raise CalculationError(f"Unknown calculation type: {calc_type}")

    # Round calculated value to 6 decimal places (matching round_to_six_decimal_places helper)
    rounded_formula = f"ROUND({calc_formula}, 6)"

    # Build complete query with pivot and calculation
    sql_query = f"""
    WITH parent_data AS (
        {union_query}
    ),
    pivoted_data AS (
        SELECT 
            timestamp,
            {pivot_select}
        FROM parent_data
        GROUP BY timestamp
    )
    SELECT 
        {derived_series_id} AS series_id,
        timestamp,
        {rounded_formula} AS value
    FROM pivoted_data
    WHERE value IS NOT NULL
    ORDER BY timestamp
    """

    return sql_query


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

    # Determine effective date range for calculation (from config or partition date)
    range_start, range_end = config.get_date_range(target_date)
    if range_start > range_end:
        raise CalculationError(
            f"start_date ({range_start.date()}) must be <= end_date ({range_end.date()})"
        )

    context.log.info(
        "Using calculation date range %s to %s for derived series %s",
        range_start.date(),
        range_end.date(),
        derived_series_code,
    )

    # Get parent dependencies (ordered by dependency_id for consistent entry ordering)
    parent_deps = dep_manager.get_parent_dependencies(derived_series_id)
    parent_deps.sort(key=lambda x: x.get("dependency_id", 0))

    # Validate parent dependencies exist
    validate_parent_dependencies_exist(parent_deps, derived_series_code)

    # Determine calculation type from dependencies or config
    calc_type = determine_calculation_type(parent_deps, config.formula, derived_series_code)

    # Validate number of parents matches calculation type requirements
    num_parents = len(parent_deps)
    validate_calculation_parent_count(calc_type, num_parents, derived_series_code, context)

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

        # Validate parent series exist in metaSeries
        validate_parent_series_in_metaseries(parent_series_result, derived_series_code)

        # Build UNION ALL query to load parent data for the configured date range
        union_parts = build_union_query_for_parents(
            duckdb=duckdb,
            parent_series_result=parent_series_result,
            start_date=range_start,
            end_date=range_end,
        )

        # Build and execute complete SQL query that performs pivot and calculation in DuckDB
        calculation_query = _build_calculation_sql_query(
            union_parts=union_parts,
            input_series_ids=input_series_ids,
            calc_type=calc_type,
            derived_series_id=derived_series_id,
            target_date=target_date,
        )

        context.log.debug(
            f"Executing calculation query for {calc_type}: {calculation_query[:200]}..."
        )

        # Execute query - all calculation happens in DuckDB
        output_df = duckdb.execute_query(calculation_query)

        if output_df is None or output_df.empty:
            raise CalculationError("No valid calculated values after processing")

        context.log.info(
            "Calculated derived series DataFrame",
            extra={
                "row_count": len(output_df),
                "columns": list(output_df.columns),
                "calculation_type": calc_type,
                "derived_series_code": derived_series_code,
            },
        )

        # Optionally log a small sample in debug mode
        if len(output_df) > 0:
            context.log.debug(
                "Derived series DataFrame sample",
                extra={
                    "sample_rows": output_df.head().to_dict("records"),
                    "calculation_type": calc_type,
                },
            )

        # Save to S3 using DuckDB macro
        # Convert DataFrame to list of dicts for save_value_data_to_s3
        value_data_list = output_df.to_dict("records")
        s3_path = save_value_data_to_s3(
            duckdb=duckdb,
            value_data=value_data_list,
            series_code=derived_series_code,
            partition_date=target_date,
            force_refresh=False,  # Calculations merge with existing data
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
                "result_df": output_df.to_dict("records"),
            },
        )
        summary.add_to_context(context)

        context.log.info(
            f"Calculated {calc_type} for {derived_series_code}: {len(output_df)} rows saved to {s3_path}"
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
