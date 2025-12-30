"""Expectation suite builders for 6 data quality dimensions."""

from typing import Any, Callable, Dict, List, Optional

from dagster import get_dagster_logger
from great_expectations.core import ExpectationConfiguration

from dagster_quickstart.utils.constants import (
    MAX_DATA_AGE_HOURS,
    MAX_NULL_PERCENTAGE_DEFAULT,
    MAX_NULL_PERCENTAGE_REQUIRED_FIELDS,
)

logger = get_dagster_logger()


# ============================================================================
# Private Helper Functions
# ============================================================================


def _create_column_existence_expectation(column: str) -> ExpectationConfiguration:
    """Create an expectation for column existence.

    Args:
        column: Name of the column

    Returns:
        ExpectationConfiguration for column existence
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": column},
    )


def _create_null_check_expectation(
    column: str, max_null_percentage: float = 0.0
) -> ExpectationConfiguration:
    """Create an expectation for null value checking.

    Args:
        column: Name of the column
        max_null_percentage: Maximum percentage of null values allowed (0.0 = no nulls allowed)

    Returns:
        ExpectationConfiguration for null checking
    """
    kwargs: Dict[str, Any] = {"column": column}
    if max_null_percentage > 0.0:
        kwargs["mostly"] = 1.0 - (max_null_percentage / 100.0)

    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs=kwargs,
    )


def _create_quantile_expectation(column: str) -> ExpectationConfiguration:
    """Create an expectation for quantile-based outlier detection.

    Args:
        column: Name of the column

    Returns:
        ExpectationConfiguration for quantile checking
    """
    return ExpectationConfiguration(
        expectation_type="expect_column_quantile_values_to_be_between",
        kwargs={
            "column": column,
            "quantile_ranges": {
                "quantiles": [0.0, 0.25, 0.5, 0.75, 1.0],
                "value_ranges": [
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                    [None, None],
                ],
            },
            "allow_relative_error": True,
        },
    )


def _create_range_expectation(
    column: str, min_value: Optional[float] = None, max_value: Optional[float] = None
) -> ExpectationConfiguration:
    """Create an expectation for value range checking.

    Args:
        column: Name of the column
        min_value: Optional minimum value
        max_value: Optional maximum value

    Returns:
        ExpectationConfiguration for range checking
    """
    kwargs: Dict[str, Any] = {"column": column}
    if min_value is not None:
        kwargs["min_value"] = min_value
    if max_value is not None:
        kwargs["max_value"] = max_value

    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs=kwargs,
    )


def _get_type_list_for_expected_type(expected_type: str) -> List[str]:
    """Get the type list for a given expected type.

    Args:
        expected_type: Expected type name (e.g., "int", "float", "string")

    Returns:
        List of type names to check against
    """
    expected_type_lower = expected_type.lower()
    if "int" in expected_type_lower:
        return ["int", "int64", "Int64"]
    elif "float" in expected_type_lower:
        return ["float", "float64", "Float64"]
    elif "string" in expected_type_lower or "str" in expected_type_lower:
        return ["str", "string", "object"]
    return []


def _create_type_expectation(column: str, expected_type: str) -> Optional[ExpectationConfiguration]:
    """Create an expectation for column type validation.

    Args:
        column: Name of the column
        expected_type: Expected type name

    Returns:
        ExpectationConfiguration for type validation, or None if type not supported
    """
    type_list = _get_type_list_for_expected_type(expected_type)
    if not type_list:
        return None

    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_type_list",
        kwargs={
            "column": column,
            "type_list": type_list,
        },
    )


def _create_format_expectation(column: str, format_type: str) -> Optional[ExpectationConfiguration]:
    """Create an expectation for column format validation.

    Args:
        column: Name of the column
        format_type: Format type (e.g., "email", "date")

    Returns:
        ExpectationConfiguration for format validation, or None if format not supported
    """
    format_handlers = {
        "email": lambda col: ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_regex",
            kwargs={
                "column": col,
                "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            },
        ),
        "date": lambda col: ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_dateutil_parseable",
            kwargs={"column": col},
        ),
    }

    handler = format_handlers.get(format_type.lower())
    return handler(column) if handler else None


def _build_completeness_expectations_for_column(
    column: str, max_null_percentage: float = 0.0
) -> List[ExpectationConfiguration]:
    """Build completeness expectations for a single column.

    Args:
        column: Name of the column
        max_null_percentage: Maximum percentage of null values allowed

    Returns:
        List of ExpectationConfiguration objects
    """
    return [
        _create_column_existence_expectation(column),
        _create_null_check_expectation(column, max_null_percentage),
    ]


def _build_accuracy_expectations_for_column(
    column: str, value_ranges: Optional[Dict[str, float]] = None
) -> List[ExpectationConfiguration]:
    """Build accuracy expectations for a single column.

    Args:
        column: Name of the column
        value_ranges: Optional dict with "min" and/or "max" keys

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = [
        _create_column_existence_expectation(column),
        _create_quantile_expectation(column),
    ]

    # Add value range checks if specified
    if value_ranges:
        min_value = value_ranges.get("min")
        max_value = value_ranges.get("max")
        if min_value is not None or max_value is not None:
            expectations.append(
                _create_range_expectation(column, min_value=min_value, max_value=max_value)
            )

    return expectations


def _build_type_expectations_for_column(
    column: str, expected_type: str
) -> List[ExpectationConfiguration]:
    """Build type validation expectations for a single column.

    Args:
        column: Name of the column
        expected_type: Expected type name

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = [_create_column_existence_expectation(column)]
    type_expectation = _create_type_expectation(column, expected_type)
    if type_expectation:
        expectations.append(type_expectation)
    return expectations


def _build_format_expectations_for_column(
    column: str, format_type: str
) -> List[ExpectationConfiguration]:
    """Build format validation expectations for a single column.

    Args:
        column: Name of the column
        format_type: Format type (e.g., "email", "date")

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = [_create_column_existence_expectation(column)]
    format_expectation = _create_format_expectation(column, format_type)
    if format_expectation:
        expectations.append(format_expectation)
    return expectations


def _add_expectations_if_condition(
    all_expectations: List[ExpectationConfiguration],
    condition: bool,
    expectation_builder: Callable[..., List[ExpectationConfiguration]],
    *args,
    **kwargs,
) -> None:
    """Helper function to conditionally add expectations.

    Args:
        all_expectations: List to extend with new expectations
        condition: Boolean condition to check
        expectation_builder: Function that builds expectations
        *args: Positional arguments for expectation_builder
        **kwargs: Keyword arguments for expectation_builder
    """
    if condition:
        all_expectations.extend(expectation_builder(*args, **kwargs))


def _build_completeness_dimension(
    required_columns: List[str],
) -> List[ExpectationConfiguration]:
    """Build completeness dimension expectations.

    Args:
        required_columns: List of required column names

    Returns:
        List of ExpectationConfiguration objects
    """
    return build_completeness_expectations(
        required_columns=required_columns,
        max_null_percentage=MAX_NULL_PERCENTAGE_REQUIRED_FIELDS,
    )


def _build_timeliness_dimension(
    timestamp_column: str,
) -> List[ExpectationConfiguration]:
    """Build timeliness dimension expectations.

    Args:
        timestamp_column: Name of the timestamp column

    Returns:
        List of ExpectationConfiguration objects
    """
    return build_timeliness_expectations(timestamp_column=timestamp_column)


def _build_validity_dimension(
    column_types: Dict[str, str],
) -> List[ExpectationConfiguration]:
    """Build validity dimension expectations.

    Args:
        column_types: Dict mapping column names to expected types

    Returns:
        List of ExpectationConfiguration objects
    """
    return build_validity_expectations(column_types=column_types)


def _build_uniqueness_dimension(
    unique_columns: Optional[List[str]],
    composite_keys: Optional[List[List[str]]],
) -> List[ExpectationConfiguration]:
    """Build uniqueness dimension expectations.

    Args:
        unique_columns: Optional list of columns that should be unique
        composite_keys: Optional list of composite key combinations

    Returns:
        List of ExpectationConfiguration objects
    """
    return build_uniqueness_expectations(
        unique_columns=unique_columns, composite_keys=composite_keys
    )


def _build_accuracy_dimension(
    numeric_columns: List[str],
) -> List[ExpectationConfiguration]:
    """Build accuracy dimension expectations.

    Args:
        numeric_columns: List of numeric columns for accuracy checks

    Returns:
        List of ExpectationConfiguration objects
    """
    return build_accuracy_expectations(numeric_columns=numeric_columns)


# ============================================================================
# Public Functions
# ============================================================================


def build_timeliness_expectations(
    timestamp_column: str = "timestamp",
    max_age_hours: int = MAX_DATA_AGE_HOURS,
) -> List[ExpectationConfiguration]:
    """Build expectations for timeliness dimension.

    Args:
        timestamp_column: Name of the timestamp column
        max_age_hours: Maximum age in hours for latest data point

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = [
        _create_column_existence_expectation(timestamp_column),
        _create_null_check_expectation(timestamp_column),
    ]
    return expectations


def build_completeness_expectations(
    required_columns: List[str],
    max_null_percentage: float = MAX_NULL_PERCENTAGE_DEFAULT,
) -> List[ExpectationConfiguration]:
    """Build expectations for completeness dimension.

    Args:
        required_columns: List of required column names
        max_null_percentage: Maximum percentage of null values allowed

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = []
    for column in required_columns:
        expectations.extend(
            _build_completeness_expectations_for_column(column, max_null_percentage)
        )
    return expectations


def build_accuracy_expectations(
    numeric_columns: List[str],
    value_ranges: Optional[Dict[str, Dict[str, float]]] = None,
) -> List[ExpectationConfiguration]:
    """Build expectations for accuracy dimension.

    Args:
        numeric_columns: List of numeric column names
        value_ranges: Optional dict mapping column names to min/max ranges

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = []
    for column in numeric_columns:
        column_ranges = value_ranges.get(column) if value_ranges else None
        expectations.extend(_build_accuracy_expectations_for_column(column, column_ranges))
    return expectations


def build_validity_expectations(
    column_types: Optional[Dict[str, str]] = None,
    format_checks: Optional[Dict[str, str]] = None,
) -> List[ExpectationConfiguration]:
    """Build expectations for validity dimension.

    Args:
        column_types: Dict mapping column names to expected types
        format_checks: Dict mapping column names to format patterns

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = []

    if column_types:
        for column, expected_type in column_types.items():
            expectations.extend(_build_type_expectations_for_column(column, expected_type))

    if format_checks:
        for column, format_type in format_checks.items():
            expectations.extend(_build_format_expectations_for_column(column, format_type))

    return expectations


def build_uniqueness_expectations(
    unique_columns: Optional[List[str]] = None,
    composite_keys: Optional[List[List[str]]] = None,
) -> List[ExpectationConfiguration]:
    """Build expectations for uniqueness dimension.

    Args:
        unique_columns: List of columns that should be unique
        composite_keys: List of column combinations that should be unique together

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = []

    if unique_columns:
        for column in unique_columns:
            expectations.append(_create_column_existence_expectation(column))
            expectations.append(
                ExpectationConfiguration(
                    expectation_type="expect_column_values_to_be_unique",
                    kwargs={"column": column},
                )
            )

    if composite_keys:
        for key_columns in composite_keys:
            for column in key_columns:
                expectations.append(_create_column_existence_expectation(column))
            expectations.append(
                ExpectationConfiguration(
                    expectation_type="expect_compound_columns_to_be_unique",
                    kwargs={"column_list": key_columns},
                )
            )

    return expectations


def build_consistency_expectations(
    foreign_keys: Optional[Dict[str, Dict[str, str]]] = None,
) -> List[ExpectationConfiguration]:
    """Build expectations for consistency dimension.

    Note: Referential integrity checks typically require querying the database,
    which is better handled in custom checks. This provides basic structure.

    Args:
        foreign_keys: Dict mapping column names to lookup table info

    Returns:
        List of ExpectationConfiguration objects
    """
    expectations = []

    if foreign_keys:
        for column in foreign_keys.keys():
            expectations.append(_create_column_existence_expectation(column))
            # Note: Actual referential integrity validation is done via custom checks
            # as it requires database queries

    return expectations


def build_all_dimensions_expectations(
    required_columns: List[str],
    timestamp_column: Optional[str] = None,
    unique_columns: Optional[List[str]] = None,
    composite_keys: Optional[List[List[str]]] = None,
    numeric_columns: Optional[List[str]] = None,
    column_types: Optional[Dict[str, str]] = None,
) -> List[ExpectationConfiguration]:
    """Build expectations for all 6 data quality dimensions.

    Args:
        required_columns: List of required column names
        timestamp_column: Optional timestamp column for timeliness checks
        unique_columns: Optional list of columns that should be unique
        composite_keys: Optional list of composite key combinations
        numeric_columns: Optional list of numeric columns for accuracy checks
        column_types: Optional dict mapping column names to expected types

    Returns:
        Combined list of ExpectationConfiguration objects
    """
    all_expectations: List[ExpectationConfiguration] = []

    # Completeness - always required
    _add_expectations_if_condition(
        all_expectations,
        True,
        _build_completeness_dimension,
        required_columns=required_columns,
    )

    # Timeliness - conditional
    _add_expectations_if_condition(
        all_expectations,
        timestamp_column is not None,
        _build_timeliness_dimension,
        timestamp_column=timestamp_column,
    )

    # Validity - conditional
    _add_expectations_if_condition(
        all_expectations,
        column_types is not None and len(column_types) > 0,
        _build_validity_dimension,
        column_types=column_types,
    )

    # Uniqueness - conditional
    _add_expectations_if_condition(
        all_expectations,
        (unique_columns is not None and len(unique_columns) > 0)
        or (composite_keys is not None and len(composite_keys) > 0),
        _build_uniqueness_dimension,
        unique_columns=unique_columns,
        composite_keys=composite_keys,
    )

    # Accuracy - conditional
    _add_expectations_if_condition(
        all_expectations,
        numeric_columns is not None and len(numeric_columns) > 0,
        _build_accuracy_dimension,
        numeric_columns=numeric_columns,
    )

    return all_expectations
