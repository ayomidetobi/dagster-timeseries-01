"""Reusable helper functions for Dagster assets."""

from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import AssetExecutionContext, get_dagster_logger

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    S3_BASE_PATH_STAGING,
    S3_BASE_PATH_VALUE_DATA,
    S3_PARQUET_FILE_NAME,
    SQL_FILE_PATH_PLACEHOLDER,
)
from dagster_quickstart.utils.datetime_utils import UTC, parse_datetime_string
from dagster_quickstart.utils.exceptions import (
    CSVValidationError,
    DataSourceValidationError,
    DatabaseQueryError,
    MetaSeriesNotFoundError,
)
from database.dependency import CalculationLogManager
from database.meta_series import MetaSeriesManager
from database.models import CalculationLogBase, CalculationStatus, DataSource

# Try to import SQL class from qr_common, but make it optional
try:
    from qr_common.datacachers.duckdb_datacacher import SQL
except ImportError:
    SQL = None  # type: ignore

logger = get_dagster_logger()


def round_six_dp(value: float | Decimal) -> Decimal:
    """Round a number to exactly 6 decimal places.

    Args:
        value: Number to round (float or Decimal)

    Returns:
        Decimal value rounded to exactly 6 decimal places using ROUND_HALF_UP
    """
    return Decimal(value).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


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


def validate_csv_columns(df: pd.DataFrame, required_columns: List[str], csv_path: str) -> None:
    """Validate that CSV contains required columns.

    Args:
        df: Pandas DataFrame
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
) -> pd.DataFrame:
    r"""Safely read CSV file with error handling.

    Args:
        csv_path: Path to CSV file
        null_values: String representation of NULL values (default: "\\N")
        truncate_ragged_lines: Whether to truncate ragged lines (pandas handles this automatically)

    Returns:
        Pandas DataFrame

    Raises:
        CSVValidationError: If CSV cannot be read
    """
    try:
        read_options: Dict[str, Any] = {}
        if null_values is not None:
            read_options["na_values"] = [null_values]
        # pandas handles ragged lines automatically, so truncate_ragged_lines is ignored
        return pd.read_csv(csv_path, **read_options)
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


def load_series_data_from_duckdb(duckdb: DuckDBResource, series_id: int) -> Optional[pd.DataFrame]:
    """Load time-series data from S3 Parquet files for a given series_id.

    Uses DuckDBResource's load() method with SQL class bindings for S3 path resolution.

    Args:
        duckdb: DuckDB resource with S3 access via httpfs
        series_id: Series ID to load data for

    Returns:
        DataFrame with timestamp and value columns, or None if no data found
    """
    # Get relative S3 path for this series (relative to bucket)
    relative_path = f"value-data/series_id={series_id}/data.parquet"

    try:
        # Use DuckDBResource's load() method with SQL class for proper S3 path resolution
        if SQL is not None:
            # Use $file_path binding - sql_to_string will resolve it to full S3 path
            query = SQL(
                "SELECT timestamp, value FROM read_parquet('$file_path') ORDER BY timestamp",
                file_path=relative_path,
            )

            # Use DuckDBResource.load() which handles S3 path resolution via duckdb_datacacher
            df = duckdb.load(query)

            if df is not None and not df.empty:
                # Ensure we only have timestamp and value columns
                if all(col in df.columns for col in ["timestamp", "value"]):
                    return df[["timestamp", "value"]]
                return df
        else:
            # SQL class not available, use execute_query as fallback
            bucket = duckdb.get_bucket()
            full_s3_path = f"s3://{bucket}/{relative_path}"
            query = f"""
            SELECT timestamp, value
            FROM read_parquet('{full_s3_path}')
            ORDER BY timestamp
            """
            df = duckdb.execute_query(query)
            if df is not None and not df.empty:
                # Ensure we only have timestamp and value columns
                if all(col in df.columns for col in ["timestamp", "value"]):
                    return df[["timestamp", "value"]]
                return df
    except Exception:
        # File doesn't exist, return None
        pass
    return None


def check_series_data_staleness(
    duckdb: DuckDBResource,
    series_id: int,
    lookback_delta_seconds: int = 3600,
) -> bool:
    """Check if series data in S3 Parquet file is stale.

    Uses DuckDBResource's staleness_check() method to check if the data
    needs to be refreshed. Note: This works best with in-memory tables,
    but can also check S3 file metadata if available.

    Args:
        duckdb: DuckDB resource with S3 access via httpfs
        series_id: Series ID to check
        lookback_delta_seconds: Maximum age in seconds before considered stale (default: 1 hour)

    Returns:
        True if stale, False if fresh or unknown
    """
    # Get relative S3 path for this series
    relative_path = f"value-data/series_id={series_id}/data.parquet"

    try:
        # Use DuckDBResource's staleness_check() method
        # Note: This works best with in-memory tables, but can check file metadata
        is_stale = duckdb.staleness_check(
            file_name=relative_path,
            lookback_delta_seconds=lookback_delta_seconds,
            in_memory=False,  # Set to True if using in-memory tables
        )
        return is_stale
    except Exception as e:
        logger.warning(f"Could not check staleness for series_id={series_id}: {e}")
        return False  # If we can't check, assume not stale


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


def get_sql_class() -> Optional[Any]:
    """Get SQL class from qr_common if available.

    Returns:
        SQL class or None if not available
    """
    return SQL


def create_sql_query_with_file_path(query_template: str, file_path: str, **kwargs: Any) -> Any:
    """Create SQL query object with file_path binding.

    Args:
        query_template: SQL query template with $file_path placeholder
        file_path: Relative S3 file path (relative to bucket)
        **kwargs: Additional bindings for SQL query

    Returns:
        SQL object with bindings, or query string if SQL class not available

    Raises:
        DatabaseQueryError: If SQL class is required but not available
    """
    SQL = get_sql_class()
    if SQL is not None:
        return SQL(query_template, file_path=file_path, **kwargs)
    # Return template string for fallback usage
    return query_template.replace(SQL_FILE_PATH_PLACEHOLDER, file_path)


def build_s3_path_for_series(series_id: int) -> str:
    """Build relative S3 path for a series' Parquet file.

    Args:
        series_id: Series ID

    Returns:
        Relative S3 path (e.g., 'value-data/series_id=123/data.parquet')
    """
    return f"{S3_BASE_PATH_VALUE_DATA}/series_id={series_id}/{S3_PARQUET_FILE_NAME}"


def build_s3_staging_path(staging_type: str, timestamp: int, unique_id: str) -> str:
    """Build relative S3 path for staging Parquet file.

    Args:
        staging_type: Type of staging data (e.g., 'lookup_tables', 'meta_series')
        timestamp: Timestamp for unique path generation
        unique_id: Unique identifier for path generation

    Returns:
        Relative S3 path (e.g., 'staging/lookup_tables/1234567890-abc123.parquet')
    """
    return f"{S3_BASE_PATH_STAGING}/{staging_type}/{timestamp}-{unique_id}.parquet"


def build_full_s3_path(bucket: str, relative_path: str) -> str:
    """Build full S3 path from bucket and relative path.

    Args:
        bucket: S3 bucket name
        relative_path: Relative path within bucket

    Returns:
        Full S3 path (e.g., 's3://bucket/path/to/file.parquet')
    """
    return f"s3://{bucket}/{relative_path}"


def load_from_s3_parquet(
    duckdb: DuckDBResource,
    relative_path: str,
    query_template: str,
    context: Optional[AssetExecutionContext] = None,
) -> Optional[pd.DataFrame]:
    """Load data from S3 Parquet file using DuckDBResource.

    Handles both SQL class bindings and fallback to direct queries.

    Args:
        duckdb: DuckDB resource with S3 access
        relative_path: Relative S3 path (relative to bucket)
        query_template: SQL query template with $file_path placeholder
        context: Optional Dagster context for logging

    Returns:
        DataFrame with loaded data, or None if empty/not found

    Raises:
        DatabaseQueryError: If query execution fails
    """
    SQL = get_sql_class()
    bucket = duckdb.get_bucket()
    full_s3_path = build_full_s3_path(bucket, relative_path)

    try:
        if SQL is not None:
            query = create_sql_query_with_file_path(query_template, relative_path)
            df = duckdb.load(query)
            if df is not None and not df.empty:
                return df
        else:
            # Fallback: Use execute_query directly
            resolved_query = query_template.replace(SQL_FILE_PATH_PLACEHOLDER, f"'{full_s3_path}'")
            df = duckdb.execute_query(resolved_query)
            if df is not None and not df.empty:
                return df
    except Exception as e:
        error_msg = f"Failed to load data from S3 path {relative_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e

    return None


def save_to_s3_parquet(
    duckdb: DuckDBResource,
    relative_path: str,
    select_query: Any,
    context: Optional[AssetExecutionContext] = None,
) -> bool:
    """Save data to S3 Parquet file using DuckDBResource.

    Handles both SQL class bindings and fallback to direct COPY commands.

    Args:
        duckdb: DuckDB resource with S3 access
        relative_path: Relative S3 path (relative to bucket)
        select_query: SQL query object or string for selecting data to save
        context: Optional Dagster context for logging

    Returns:
        True if save was successful

    Raises:
        DatabaseQueryError: If save operation fails
    """
    SQL = get_sql_class()
    bucket = duckdb.get_bucket()

    try:
        if SQL is not None:
            # If select_query is a string, create SQL object with file_path binding
            if isinstance(select_query, str):
                select_query = SQL(select_query, file_path=relative_path)
            elif not isinstance(select_query, SQL):
                raise DatabaseQueryError(
                    f"Invalid select_query type: {type(select_query)}. "
                    "Expected SQL object or string."
                )

            success = duckdb.save(
                select_statement=select_query,
                file_path=relative_path,
            )
            if not success:
                raise DatabaseQueryError(f"Failed to save data to S3: {relative_path}")
            return True
        else:
            # Fallback: Use DuckDB's COPY TO directly
            full_s3_path = build_full_s3_path(bucket, relative_path)
            # Extract table name from select_query if it's a string
            if isinstance(select_query, str):
                # Assume format: "SELECT * FROM table_name"
                parts = select_query.upper().split("FROM")
                if len(parts) < 2:
                    raise DatabaseQueryError(
                        f"Invalid select_query format: {select_query}. "
                        "Expected 'SELECT * FROM table_name'"
                    )
                table_match = parts[-1].strip()
                copy_query = f"COPY {table_match} TO '{full_s3_path}' (FORMAT PARQUET)"
            else:
                raise DatabaseQueryError(
                    "Cannot use fallback COPY without SQL class. "
                    "select_query must be a string with table name."
                )
            duckdb.execute_command(copy_query)
            return True
    except DatabaseQueryError:
        raise  # Re-raise domain-specific exceptions
    except Exception as e:
        error_msg = f"Failed to save data to S3 path {relative_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e
