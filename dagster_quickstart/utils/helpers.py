"""Reusable helper functions for Dagster assets."""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl
from dagster import AssetExecutionContext

from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.utils.datetime_utils import UTC, parse_datetime_string
from dagster_quickstart.utils.exceptions import (
    CSVValidationError,
    DataSourceValidationError,
    MetaSeriesNotFoundError,
)
from database.dependency import CalculationLogManager
from database.meta_series import MetaSeriesManager
from database.models import CalculationLogBase, CalculationStatus, DataSource


def safe_int(value: Any, field_name: str, required: bool = True) -> Optional[int]:
    """Safely convert value to int, handling None and empty strings.

    Args:
        value: Value to convert to int
        field_name: Name of the field (for error messages)
        required: Whether the field is required

    Returns:
        Integer value or None if not required and value is empty

    Raises:
        ValueError: If value is required but missing or invalid
    """
    if value is None or (isinstance(value, str) and value.strip() == ""):
        if required:
            raise ValueError(f"Required field '{field_name}' is missing or empty")
        return None
    try:
        return int(value)
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid value for '{field_name}': {value}. Must be an integer.") from e


def parse_data_source(data_source_str: str) -> DataSource:
    """Parse and validate data source string.

    Args:
        data_source_str: String representation of data source

    Returns:
        DataSource enum value

    Raises:
        DataSourceValidationError: If data source is invalid
    """
    if not data_source_str or data_source_str.upper() == "NAN":
        raise DataSourceValidationError("data_source is required and cannot be empty")

    try:
        return DataSource[data_source_str.upper()]
    except KeyError:
        valid_sources = [ds.value for ds in DataSource]
        raise DataSourceValidationError(
            f"Invalid data_source '{data_source_str}'. Must be one of: {valid_sources}"
        )


def validate_csv_columns(df: pl.DataFrame, required_columns: List[str], csv_path: str) -> None:
    """Validate that CSV contains required columns.

    Args:
        df: Polars DataFrame
        required_columns: List of required column names
        csv_path: Path to CSV file (for error messages)

    Raises:
        CSVValidationError: If required columns are missing
    """
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise CSVValidationError(f"CSV file {csv_path} missing required columns: {missing_columns}")


def read_csv_safe(
    csv_path: str,
    null_values: Optional[str] = None,
    truncate_ragged_lines: bool = True,
) -> pl.DataFrame:
    r"""Safely read CSV file with error handling.

    Args:
        csv_path: Path to CSV file
        null_values: String representation of NULL values (default: "\\N")
        truncate_ragged_lines: Whether to truncate ragged lines

    Returns:
        Polars DataFrame

    Raises:
        CSVValidationError: If CSV cannot be read
    """
    try:
        read_options: Dict[str, Any] = {"truncate_ragged_lines": truncate_ragged_lines}
        if null_values is not None:
            read_options["null_values"] = null_values
        return pl.read_csv(csv_path, **read_options)
    except Exception as e:
        raise CSVValidationError(f"Error reading CSV file {csv_path}: {e}") from e


def generate_date_range(start_date: str, end_date: str) -> List[datetime]:
    """Generate list of dates between start and end date (inclusive).

    Args:
        start_date: Start date string in ISO format (YYYY-MM-DD)
        end_date: End date string in ISO format (YYYY-MM-DD)

    Returns:
        List of datetime objects
    """
    # Parse dates using robust dateutil parser and normalize to UTC
    start = parse_datetime_string(start_date).replace(hour=0, minute=0, second=0, microsecond=0)
    end = parse_datetime_string(end_date).replace(hour=0, minute=0, second=0, microsecond=0)
    dates = []
    current = start
    while current <= end:
        dates.append(current)
        current += timedelta(days=1)
    return dates


def load_series_data_from_clickhouse(
    clickhouse: ClickHouseResource, series_id: int
) -> Optional[pd.DataFrame]:
    """Load time-series data from ClickHouse for a given series_id.

    Args:
        clickhouse: ClickHouse resource
        series_id: Series ID to load data for

    Returns:
        DataFrame with timestamp and value columns, or None if no data found
    """
    query = """
    SELECT timestamp, value
    FROM valueData
    WHERE series_id = {series_id:UInt32}
    ORDER BY timestamp
    """
    result = clickhouse.execute_query(query, parameters={"series_id": series_id})
    if hasattr(result, "result_rows") and result.result_rows:
        return pd.DataFrame(result.result_rows, columns=["timestamp", "value"])
    return None


def get_or_validate_meta_series(
    meta_manager: MetaSeriesManager,
    series_code: str,
    context: Optional[AssetExecutionContext] = None,
    raise_if_not_found: bool = True,
) -> Optional[Dict[str, Any]]:
    """Get meta series by code, with optional validation and logging.

    Args:
        meta_manager: MetaSeriesManager instance
        series_code: Series code to look up
        context: Optional Dagster context for logging
        raise_if_not_found: Whether to raise exception if not found

    Returns:
        Meta series dictionary or None if not found and raise_if_not_found=False

    Raises:
        MetaSeriesNotFoundError: If series not found and raise_if_not_found=True
    """
    meta_series = meta_manager.get_meta_series_by_code(series_code)

    if not meta_series:
        if raise_if_not_found:
            raise MetaSeriesNotFoundError(f"Meta series {series_code} must exist before operation")
        if context:
            context.log.warning(f"Meta series {series_code} not found")
        return None

    return meta_series


def create_calculation_log(
    calc_manager: CalculationLogManager,
    series_id: int,
    calculation_type: str,
    formula: str,
    input_series_ids: List[int],
    parameters: Optional[str] = None,
) -> int:
    """Create a calculation log entry.

    Args:
        calc_manager: CalculationLogManager instance
        series_id: Series ID being calculated
        calculation_type: Type of calculation (e.g., "SMA", "WEIGHTED_COMPOSITE")
        formula: Calculation formula
        input_series_ids: List of input series IDs
        parameters: Optional parameters string

    Returns:
        Calculation log ID
    """
    calc_log = CalculationLogBase(
        series_id=series_id,
        calculation_type=calculation_type,
        status=CalculationStatus.RUNNING,
        input_series_ids=input_series_ids,
        parameters=parameters or formula,
        formula=formula,
        execution_start=datetime.now(UTC),  # Not stored in DB, but required by model
        execution_end=None,
    )
    return calc_manager.create_calculation_log(calc_log)


def update_calculation_log_on_success(
    calc_manager: CalculationLogManager,
    calculation_id: int,
    rows_processed: int,
) -> None:
    """Update calculation log with success status.

    Args:
        calc_manager: CalculationLogManager instance
        calculation_id: Calculation log ID
        rows_processed: Number of rows processed
    """
    calc_manager.update_calculation_log(
        calculation_id=calculation_id,
        status=CalculationStatus.COMPLETED,
        rows_processed=rows_processed,
    )


def update_calculation_log_on_error(
    calc_manager: CalculationLogManager,
    calculation_id: int,
    error_message: str,
) -> None:
    """Update calculation log with error status.

    Args:
        calc_manager: CalculationLogManager instance
        calculation_id: Calculation log ID
        error_message: Error message to log
    """
    calc_manager.update_calculation_log(
        calculation_id=calculation_id,
        status=CalculationStatus.FAILED,
        error_message=error_message,
    )


def is_empty_row(row: Dict[str, Any], required_fields: List[str]) -> bool:
    """Check if a row is empty based on required fields.

    Args:
        row: Dictionary representing a row
        required_fields: List of field names that must be non-empty

    Returns:
        True if row is empty (any required field is missing or empty)
    """
    for field in required_fields:
        value = row.get(field)
        if not value or (isinstance(value, str) and not value.strip()):
            return True
    return False


def resolve_lookup_id_from_string(
    row: Dict[str, Any],
    id_field: str,
    string_field: str,
    lookup_manager: Any,
    lookup_method: str,
    context: Optional[AssetExecutionContext] = None,
) -> Optional[int]:
    """Resolve lookup ID from either direct ID field or string lookup.

    Args:
        row: Row dictionary
        id_field: Name of the ID field (e.g., "region_id")
        string_field: Name of the string field (e.g., "region")
        lookup_manager: Lookup table manager instance
        lookup_method: Method name to call on lookup_manager (e.g., "get_region_by_name")
        context: Optional Dagster context for logging

    Returns:
        Resolved lookup ID or None if not found
    """
    # Try to get ID directly
    lookup_id = safe_int(row.get(id_field), id_field, required=False)
    if lookup_id:
        return lookup_id

    # Try to resolve from string field
    string_value = row.get(string_field)
    if not string_value:
        return None

    try:
        lookup_method_func = getattr(lookup_manager, lookup_method)
        lookup_result = lookup_method_func(str(string_value))
        if lookup_result:
            resolved_id = lookup_result.get(id_field)
            if context:
                context.log.debug(
                    f"Resolved {string_field} '{string_value}' to {id_field}={resolved_id}"
                )
            return resolved_id
        else:
            if context:
                context.log.warning(
                    f"{string_field.capitalize()} '{string_value}' not found in lookup table"
                )
    except AttributeError as e:
        if context:
            context.log.error(f"Lookup method {lookup_method} not found: {e}")
    except Exception as e:
        if context:
            context.log.warning(f"Error resolving {string_field} '{string_value}': {e}")

    return None
