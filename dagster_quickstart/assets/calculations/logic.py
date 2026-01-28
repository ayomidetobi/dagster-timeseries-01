"""Calculation logic for derived series."""

from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd
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
from database.schema import CalculationContext

from .config import CalculationConfig

# ============================================================================
# Calculation Helper Functions
# ============================================================================


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


# ============================================================================
# Calculation Helper Functions
# ============================================================================


def get_and_validate_derived_series(
    meta_manager: MetaSeriesManager,
    derived_series_code: str,
    context: AssetExecutionContext,
) -> Tuple[int, Dict[str, Any]]:
    """Get and validate derived series metadata.

    Args:
        meta_manager: MetaSeriesManager instance
        derived_series_code: Derived series code
        context: Dagster execution context

    Returns:
        Tuple of (derived_series_id, derived_series dict)

    Raises:
        MetaSeriesNotFoundError: If derived series not found
    """
    derived_series = meta_manager.get_or_validate_meta_series(
        derived_series_code, context, raise_if_not_found=True
    )

    if derived_series is None:
        raise MetaSeriesNotFoundError(f"Derived series {derived_series_code} not found")

    derived_series_id = derived_series["series_id"]
    derived_series_code = derived_series["series_code"]

    return derived_series_id, derived_series


def resolve_date_range(
    config: CalculationConfig,
    target_date: datetime,
    derived_series_code: str,
) -> Tuple[datetime, datetime]:
    """Resolve date range for calculation from config.

    Args:
        config: Calculation configuration
        target_date: Target date for calculation
        derived_series_code: Derived series code for error messages

    Returns:
        Tuple of (start_date, end_date)

    Raises:
        CalculationError: If start_date > end_date
    """
    range_start, range_end = config.get_date_range(target_date)
    if range_start > range_end:
        raise CalculationError(
            f"start_date ({range_start.date()}) must be <= end_date ({range_end.date()})"
        )
    return range_start, range_end


def fetch_and_validate_parent_dependencies(
    dep_manager: DependencyManager,
    derived_series_id: int,
    derived_series_code: str,
    config: CalculationConfig,
    context: AssetExecutionContext,
) -> Tuple[List[Dict[str, Any]], str, List[int], int]:
    """Fetch and validate parent dependencies for calculation.

    Args:
        dep_manager: DependencyManager instance
        derived_series_id: Derived series ID
        derived_series_code: Derived series code
        config: Calculation configuration
        context: Dagster execution context

    Returns:
        Tuple of (parent_deps, calc_type, input_series_ids, num_parents)

    Raises:
        CalculationError: If validation fails
    """
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

    return parent_deps, calc_type, input_series_ids, num_parents


def load_parent_series_codes(
    duckdb: DuckDBResource,
    derived_series_id: int,
    derived_series_code: str,
) -> pd.DataFrame:
    """Load parent series codes by joining dependencies with metaSeries.

    Args:
        duckdb: DuckDB resource
        derived_series_id: Derived series ID
        derived_series_code: Derived series code for error messages

    Returns:
        DataFrame with parent series information

    Raises:
        CalculationError: If parent series not found in metaSeries
    """
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

    return parent_series_result


def execute_calculation_query(
    duckdb: DuckDBResource,
    union_parts: List[str],
    calc_ctx: CalculationContext,
    context: AssetExecutionContext,
) -> pd.DataFrame:
    """Execute calculation query and return results.

    Builds the calculation SQL query, executes it, and validates results.

    Args:
        duckdb: DuckDB resource
        union_parts: List of SQL SELECT statements for each parent series
        calc_ctx: CalculationContext with calculation metadata
        context: Dagster execution context

    Returns:
        DataFrame with calculated results

    Raises:
        CalculationError: If query execution fails or returns no results
    """
    # Build and execute complete SQL query that performs pivot and calculation in DuckDB
    calculation_query = _build_calculation_sql_query(
        union_parts=union_parts,
        input_series_ids=calc_ctx.input_series_ids,
        calc_type=calc_ctx.calc_type,
        derived_series_id=calc_ctx.derived_series_id,
        target_date=calc_ctx.target_date,
    )

    context.log.debug(
        f"Executing calculation query for {calc_ctx.calc_type}: {calculation_query[:200]}..."
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
            "calculation_type": calc_ctx.calc_type,
            "derived_series_code": calc_ctx.derived_series_code,
        },
    )

    # Optionally log a small sample in debug mode
    if len(output_df) > 0:
        context.log.debug(
            "Derived series DataFrame sample",
            extra={
                "sample_rows": output_df.head().to_dict("records"),
                "calculation_type": calc_ctx.calc_type,
            },
        )

    return output_df


def save_calculation_results_to_s3(
    output_df: pd.DataFrame,
    duckdb: DuckDBResource,
    calc_ctx: CalculationContext,
    context: AssetExecutionContext,
) -> str:
    """Save calculation results to S3.

    Wrapper over save_value_data_to_s3 for calculation results.

    Args:
        output_df: DataFrame with calculated results
        duckdb: DuckDB resource
        calc_ctx: CalculationContext with calculation metadata
        context: Dagster execution context

    Returns:
        Relative S3 path where data was saved
    """
    # Convert DataFrame to list of dicts for save_value_data_to_s3
    value_data_list = output_df.to_dict("records")
    s3_path = save_value_data_to_s3(
        duckdb=duckdb,
        value_data=value_data_list,
        series_code=calc_ctx.derived_series_code,
        partition_date=calc_ctx.target_date,
        force_refresh=False,  # Calculations merge with existing data
        context=context,
    )
    return s3_path


def build_calculation_summary_success(
    output_df: pd.DataFrame,
    target_date: datetime,
    calc_type: str,
    formula: str,
    num_parents: int,
    input_series_ids: List[int],
    derived_series_id: int,
    derived_series_code: str,
) -> AssetSummary:
    """Build AssetSummary for successful calculation.

    Args:
        output_df: DataFrame with calculated results
        target_date: Target date for calculation
        calc_type: Calculation type
        formula: Formula used
        num_parents: Number of parent series
        input_series_ids: List of input series IDs
        derived_series_id: Derived series ID
        derived_series_code: Derived series code

    Returns:
        AssetSummary instance
    """
    return AssetSummary.for_calculation(
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


def build_calculation_summary_failure(
    calc_ctx: CalculationContext,
    error_message: str,
) -> AssetSummary:
    """Build AssetSummary for failed calculation.

    Args:
        calc_ctx: CalculationContext with calculation metadata
        error_message: Error message

    Returns:
        AssetSummary instance
    """
    return AssetSummary.for_calculation(
        rows_calculated=0,
        target_date=calc_ctx.target_date,
        calculation_type=calc_ctx.calc_type,
        formula=calc_ctx.formula,
        parent_series_count=calc_ctx.num_parents,
        additional_metadata={
            "status": "failed",
            "error_message": error_message,
            "input_series_ids": calc_ctx.input_series_ids,
            "derived_series_id": calc_ctx.derived_series_id,
            "derived_series_code": calc_ctx.derived_series_code,
            "parameters": calc_ctx.formula,
        },
    )


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

    # Get and validate derived series
    derived_series_id, derived_series = get_and_validate_derived_series(
        meta_manager, derived_series_code, context
    )
    derived_series_code = derived_series["series_code"]

    # Resolve date range
    range_start, range_end = resolve_date_range(config, target_date, derived_series_code)

    context.log.info(
        "Using calculation date range %s to %s for derived series %s",
        range_start.date(),
        range_end.date(),
        derived_series_code,
    )

    # Fetch and validate parent dependencies
    _, calc_type, input_series_ids, num_parents = fetch_and_validate_parent_dependencies(
        dep_manager, derived_series_id, derived_series_code, config, context
    )

    formula = config.formula or calc_type

    # Create calculation context
    calc_ctx = CalculationContext(
        derived_series_id=derived_series_id,
        derived_series_code=derived_series_code,
        calc_type=calc_type,
        formula=formula,
        input_series_ids=input_series_ids,
        num_parents=num_parents,
        target_date=target_date,
        range_start=range_start,
        range_end=range_end,
    )

    try:
        # Load parent series codes
        parent_series_result = load_parent_series_codes(
            duckdb, derived_series_id, derived_series_code
        )

        # Build UNION ALL query to load parent data for the configured date range
        union_parts = build_union_query_for_parents(
            duckdb=duckdb,
            parent_series_result=parent_series_result,
            start_date=range_start,
            end_date=range_end,
        )

        # Execute calculation query
        output_df = execute_calculation_query(
            duckdb=duckdb,
            union_parts=union_parts,
            calc_ctx=calc_ctx,
            context=context,
        )

        # Save results to S3
        s3_path = save_calculation_results_to_s3(output_df, duckdb, calc_ctx, context)

        # Build and add success summary
        summary = build_calculation_summary_success(
            output_df=output_df,
            calc_ctx=calc_ctx,
        )
        summary.add_to_context(context)

        context.log.info(
            f"Calculated {calc_ctx.calc_type} for {calc_ctx.derived_series_code}: {len(output_df)} rows saved to {s3_path}"
        )

    except Exception as e:
        # Build and add failure summary
        error_summary = build_calculation_summary_failure(
            calc_ctx=calc_ctx,
            error_message=str(e),
        )
        error_summary.add_to_context(context)
        raise CalculationError(f"Calculation failed: {e}") from e
