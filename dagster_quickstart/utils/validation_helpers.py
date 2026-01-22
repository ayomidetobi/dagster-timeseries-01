"""Validation helper functions for series and metadata validation."""

from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import CALCULATION_FORMULA_TYPES
from dagster_quickstart.utils.exceptions import CalculationError, ReferentialIntegrityError
from database.referential_integrity import ReferentialIntegrityValidator


def validate_series_metadata(
    series: Dict[str, Any], series_id: int, series_code: str, context: AssetExecutionContext
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Validate that series has required metadata (ticker and field_type name).

    Args:
        series: Series dictionary
        series_id: Series ID for logging
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        Tuple of (ticker, field_type_name, error_reason)
        If error_reason is not None, ticker and field_type_name will be None
    """
    ticker = series.get("ticker")
    field_type_name = series.get("field_type")

    if not ticker:
        context.log.warning(f"Series {series_code} has no ticker, skipping")
        return None, None, "no ticker"

    if not field_type_name:
        context.log.warning(f"Series {series_code} has no field_type, skipping")
        return None, None, "no field_type"

    return ticker, field_type_name, None


def validate_field_type_name(
    validator: ReferentialIntegrityValidator,
    field_type_name: str,
    series_id: int,
    series_code: str,
    context: AssetExecutionContext,
) -> Optional[str]:
    """Validate field type name using referential integrity.

    Validates that the field_type_name exists in the lookup table using ReferentialIntegrityValidator.

    Args:
        validator: Referential integrity validator instance
        field_type_name: Field type name to validate
        series_id: Series ID for logging
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        Field type name if valid, None if invalid
    """
    if not field_type_name:
        context.log.warning(f"Series {series_code} has no field_type_name, skipping")
        return None

    # Validate referential integrity using ReferentialIntegrityValidator
    validation_error = validator.validate_lookup_reference(
        "field_type", field_type_name, series_code
    )
    if validation_error:
        context.log.warning(
            f"Invalid field_type_name reference for series {series_code}: {validation_error}"
        )
        return None

    return field_type_name


def validate_calculation_columns(
    merged_df: pd.DataFrame,
    required_series_ids: List[int],
    calculation_type: str,
    derived_series_code: str,
    context: AssetExecutionContext,
) -> None:
    """Validate that all required columns exist after pivot/rename operation.

    This validation ensures that all parent series have data in S3 before calculation.
    If any parent series has no data, the corresponding `value_N` column won't exist
    after pivot/rename, which would cause a KeyError at runtime.

    Args:
        merged_df: DataFrame after pivot and rename operations
        required_series_ids: List of parent series IDs that are required for calculation
        calculation_type: Type of calculation (SPREAD, FLY, BOX, RATIO) for error messages
        derived_series_code: Derived series code for error messages
        context: Dagster execution context for logging

    Raises:
        CalculationError: If any required columns are missing
    """
    # Determine how many columns are required based on calculation type
    required_column_count = CALCULATION_FORMULA_TYPES.get(
        calculation_type, len(required_series_ids)
    )

    # Check that we have at least the required number of columns
    expected_columns = [f"value_{idx}" for idx in range(required_column_count)]
    missing_columns = [col for col in expected_columns if col not in merged_df.columns]

    if missing_columns:
        # Find which parent series are missing data
        missing_series_indices = [int(col.split("_")[1]) for col in missing_columns]
        missing_series_ids = [
            required_series_ids[idx]
            for idx in missing_series_indices
            if idx < len(required_series_ids)
        ]

        missing_ids_str = ", ".join(map(str, missing_series_ids))
        missing_cols_str = ", ".join(missing_columns)

        error_message = (
            f"Missing data for required parent series: {missing_ids_str}. "
            f"Expected columns {missing_cols_str} do not exist after pivot/rename. "
            f"All parent series must have data in S3 before {calculation_type} calculation can proceed."
        )

        context.log.error(f"Validation failed for {derived_series_code}: {error_message}")

        raise CalculationError(error_message)

    # Additional validation: ensure we have the exact number of columns expected
    if len([col for col in merged_df.columns if col.startswith("value_")]) < required_column_count:
        error_message = (
            f"Expected {required_column_count} value columns for {calculation_type} calculation, "
            f"but found {len([col for col in merged_df.columns if col.startswith('value_')])} columns. "
            f"All parent series must have data in S3."
        )

        context.log.error(f"Validation failed for {derived_series_code}: {error_message}")

        raise CalculationError(error_message)

    context.log.debug(
        f"Column validation passed for {derived_series_code}: "
        f"found {required_column_count} required columns"
    )


def determine_calculation_type(
    parent_deps: List[Dict[str, Any]], formula: Optional[str], derived_series_code: str
) -> str:
    """Determine calculation type from dependencies or config formula.

    Args:
        parent_deps: List of parent dependency dictionaries
        formula: Optional formula string from config
        derived_series_code: Derived series code for error messages

    Returns:
        Calculation type string (SPREAD, FLY, BOX, or RATIO)

    Raises:
        CalculationError: If calculation type cannot be determined
    """
    calc_type = None

    # First, try to get calc_type from dependencies
    if parent_deps and "calc_type" in parent_deps[0]:
        calc_type = parent_deps[0]["calc_type"]
    elif formula:
        # Try to infer from formula string
        formula_upper = formula.upper()
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

    return calc_type


def validate_calculation_parent_count(
    calc_type: str,
    num_parents: int,
    derived_series_code: str,
    context: AssetExecutionContext,
) -> None:
    """Validate that the number of parent series matches calculation type requirements.

    Args:
        calc_type: Calculation type (SPREAD, FLY, BOX, RATIO)
        num_parents: Number of parent series available
        derived_series_code: Derived series code for error messages
        context: Dagster execution context for logging

    Raises:
        CalculationError: If number of parents is insufficient for calculation type
    """
    required_count = CALCULATION_FORMULA_TYPES.get(calc_type)
    if required_count is None:
        raise CalculationError(f"Unknown calculation type: {calc_type} for {derived_series_code}")

    if num_parents < required_count:
        raise CalculationError(
            f"{calc_type} calculation requires at least {required_count} parent series, "
            f"got {num_parents} for {derived_series_code}"
        )

    context.log.debug(
        f"Parent count validation passed for {derived_series_code}: "
        f"{calc_type} requires {required_count} parents, found {num_parents}"
    )


def validate_parent_dependencies_exist(
    parent_deps: List[Dict[str, Any]], derived_series_code: str
) -> None:
    """Validate that parent dependencies exist for the derived series.

    Args:
        parent_deps: List of parent dependency dictionaries
        derived_series_code: Derived series code for error messages

    Raises:
        CalculationError: If no parent dependencies found
    """
    if not parent_deps:
        raise CalculationError(f"No parent dependencies found for {derived_series_code}")


def validate_parent_series_in_metaseries(
    parent_series_result: Optional[pd.DataFrame], derived_series_code: str
) -> None:
    """Validate that parent series exist in metaSeries table.

    Args:
        parent_series_result: DataFrame result from querying parent series
        derived_series_code: Derived series code for error messages

    Raises:
        CalculationError: If parent series not found in metaSeries
    """
    if parent_series_result is None or parent_series_result.empty:
        raise CalculationError(
            f"No parent series found in metaSeries for dependencies of {derived_series_code}"
        )


def validate_referential_integrity_sql(
    duckdb: DuckDBResource,
    temp_table: str,
    validation_query: str,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Validate referential integrity using SQL query.

    Generic helper function for SQL-based referential integrity validation.
    Executes a validation query that should return empty results if validation passes.

    Args:
        duckdb: DuckDB resource with S3 access
        temp_table: Temporary table name with data to validate
        validation_query: SQL query that returns invalid rows (empty = valid)
        context: Optional Dagster context for logging

    Raises:
        ReferentialIntegrityError: If validation fails
    """
    if context:
        context.log.info("Validating referential integrity using SQL")

    try:
        invalid_result = duckdb.execute_query(validation_query)
        if invalid_result is not None and not invalid_result.empty:
            error_rows = invalid_result.to_dict("records")
            error_msg = _format_validation_error_message(error_rows)
            if context:
                context.log.error(error_msg)
            raise ReferentialIntegrityError(error_msg)
    except ReferentialIntegrityError:
        raise
    except Exception as e:
        error_msg = f"Error during referential integrity validation: {e}"
        if context:
            context.log.error(error_msg)
        raise ReferentialIntegrityError(error_msg) from e

    if context:
        context.log.info("Referential integrity validation passed")


def _format_validation_error_message(error_rows: List[Dict[str, Any]]) -> str:
    """Format error message for referential integrity validation failures.

    Args:
        error_rows: List of dictionaries representing invalid rows

    Returns:
        Formatted error message string
    """
    error_msg = (
        f"Referential integrity validation failed: {len(error_rows)} rows "
        f"have invalid references. First few errors:\n"
    )
    for row in error_rows[:10]:
        error_msg += f"  - {row}\n"
    if len(error_rows) > 10:
        error_msg += f"  ... and {len(error_rows) - 10} more errors\n"
    return error_msg
