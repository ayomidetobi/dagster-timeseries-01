"""Reusable helper functions for Dagster assets."""

from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import AssetExecutionContext, get_dagster_logger

from dagster_quickstart.resources import DuckDBResource

# Import SQL class and utility functions from local implementation
from dagster_quickstart.resources.duckdb_datacacher import SQL, join_s3
from dagster_quickstart.utils.constants import (
    SQL_FILE_PATH_PLACEHOLDER,
)
from dagster_quickstart.utils.datetime_utils import UTC
from dagster_quickstart.utils.exceptions import (
    CSVValidationError,
    DatabaseQueryError,
)
from database.dependency import CalculationLogManager
from database.models import CalculationLogBase, CalculationStatus

logger = get_dagster_logger()


def round_to_six_decimal_places(value: float | Decimal) -> Decimal:
    """Round a number to exactly 6 decimal places.

    Args:
        value: Number to round (float or Decimal)

    Returns:
        Decimal value rounded to exactly 6 decimal places using ROUND_HALF_UP
    """
    return Decimal(value).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


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
            full_s3_path = build_full_s3_path(duckdb, relative_path)
            query = f"""
            SELECT timestamp, value
            FROM read_parquet('{full_s3_path}')
            ORDER BY timestamp
            """
            df = duckdb.execute_query(query)
            if not df.empty:
                # Ensure we only have timestamp and value columns
                if all(col in df.columns for col in ["timestamp", "value"]):
                    return df[["timestamp", "value"]]
                return df
    except Exception:
        # File doesn't exist, return None
        pass
    return None


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
    if SQL is not None:
        return SQL(query_template, file_path=file_path, **kwargs)
    # Return template string for fallback usage
    return query_template.replace(SQL_FILE_PATH_PLACEHOLDER, file_path)


def build_s3_control_table_path(
    control_type: str, version_date: str, filename: str = "data.parquet"
) -> str:
    """Build relative S3 path for versioned control table Parquet file.

    Control tables are the system of record for lookup tables and metadata_series.
    They are versioned by run date (YYYY-MM-DD) and are immutable.

    Note: Uses 'version-' prefix instead of 'version=' to avoid URL encoding issues
    with DuckDB's httpfs extension, which URL-encodes '=' characters when converting
    S3 URIs to HTTPS URLs.

    Args:
        control_type: Type of control table ('lookup', 'metadata_series', 'field_map')
        version_date: Version date in YYYY-MM-DD format
        filename: Parquet filename (default: 'data.parquet')

    Returns:
        Relative S3 path (e.g., 'control/lookup/version-2026-01-12/data.parquet')
    """
    from dagster_quickstart.utils.constants import S3_BASE_PATH_CONTROL

    # Use 'version-' instead of 'version=' to avoid URL encoding issues with DuckDB httpfs
    return f"{S3_BASE_PATH_CONTROL}/{control_type}/version-{version_date}/{filename}"


def get_version_date() -> str:
    """Get version date (YYYY-MM-DD) from Dagster context or current date.

    Uses the run date from context if available, otherwise uses current UTC date.
    This ensures versioning is consistent across pipeline runs.

    Args:
        context: Optional Dagster execution context

    Returns:
        Version date string in YYYY-MM-DD format
    """
    from dagster_quickstart.utils.datetime_utils import utc_now

    # Fallback to current UTC date
    return utc_now().strftime("%Y-%m-%d")


def build_full_s3_path(duckdb: "DuckDBResource", relative_path: str) -> str:  # type: ignore[name-defined]
    """Build full S3 URI from DuckDB resource and relative path.

    Returns S3 URI format (s3://bucket/path) for DuckDB's httpfs extension.
    DuckDB's httpfs handles S3 URIs directly without URL encoding.

    Note: This is only needed for raw SQL strings. When using SQL objects with
    $file_path bindings, duckdb_datacacher automatically uses join_s3 internally
    via sql_to_string() and save() methods.

    Args:
        duckdb: DuckDB resource to get bucket from
        relative_path: Relative path within bucket (uses version- prefix to avoid URL encoding)

    Returns:
        Full S3 URI (e.g., 's3://bucket/control/lookup/version-2026-01-12/data.parquet')
        Note: Path is NOT URL-encoded - DuckDB's httpfs handles S3 URIs directly
    """
    bucket = duckdb.get_bucket()
    if join_s3 is not None:
        return join_s3(bucket, relative_path)
    # Fallback: construct manually (only for raw SQL strings)
    # Return S3 URI format - do NOT URL-encode
    clean_path = relative_path.lstrip("/")
    return f"s3://{bucket}/{clean_path}"


def load_csv_to_temp_table(
    duckdb: DuckDBResource,
    csv_path: str,
    null_value: str = "\\N",
    context: Optional[AssetExecutionContext] = None,
) -> str:
    r"""Load CSV file directly into DuckDB temp table using read_csv.

    Generic helper function for loading CSV files into temporary tables.

    Args:
        duckdb: DuckDB resource
        csv_path: Path to CSV file
        null_value: String representation of NULL values (default: "\\N")
        context: Optional Dagster context for logging

    Returns:
        Temporary table name

    Raises:
        CSVValidationError: If CSV cannot be read
    """
    import uuid

    temp_table = f"_temp_csv_{uuid.uuid4().hex}"

    try:
        # Use DuckDB's read_csv function directly - no pandas involved
        # DuckDB handles null values, type inference, and ragged lines automatically
        create_table_sql = f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT * FROM read_csv('{csv_path}', 
                nullstr='{null_value}',
                header=true,
                auto_detect=true,
                ignore_errors=false
            )
        """
        duckdb.execute_command(create_table_sql)
        if context:
            context.log.debug(f"Loaded CSV into temp table {temp_table}")
        return temp_table
    except Exception as e:
        error_msg = f"Error reading CSV file {csv_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise CSVValidationError(error_msg) from e


def build_s3_value_data_path(
    series_code: str, partition_date: datetime, filename: str = "data.parquet"
) -> str:
    """Build relative S3 path for value data Parquet file.

    Value data is partitioned by series_code and date for efficient querying and idempotency.
    Path format: value-data/series_code={series_code}/date={date}/data.parquet

    Args:
        series_code: Series code (readable identifier)
        partition_date: Partition date (datetime object)
        filename: Parquet filename (default: 'data.parquet')

    Returns:
        Relative S3 path (e.g., 'value-data/series_code=AAPL_US_EQ/date=2025-12-01/data.parquet')
    """
    from dagster_quickstart.utils.constants import S3_BASE_PATH_VALUE_DATA

    date_str = partition_date.strftime("%Y-%m-%d")
    return f"{S3_BASE_PATH_VALUE_DATA}/series_code={series_code}/date={date_str}/{filename}"


def write_to_s3_control_table(
    duckdb: DuckDBResource,
    relative_path: str,
    select_query: str,
    ordering_column: str,
    context: Optional[AssetExecutionContext] = None,
) -> bool:
    """Write data to S3 control table (versioned, immutable) using DuckDBResource.

    Generic helper function for writing validated data to S3 control tables.

    Args:
        duckdb: DuckDB resource with S3 access
        relative_path: Relative S3 path (relative to bucket)
        select_query: SQL SELECT query string for selecting data to save
        ordering_column: Column name to use for ordering results
        context: Optional Dagster context for logging

    Returns:
        True if write was successful

    Raises:
        DatabaseQueryError: If write operation fails
    """
    try:
        # Add ORDER BY clause if not already present
        if "ORDER BY" not in select_query.upper():
            select_query = f"{select_query} ORDER BY {ordering_column}"

        # DuckDBResource.save() handles SQL validation and conversion internally
        success = duckdb.save(
            select_statement=select_query,
            file_path=relative_path,
        )
        if not success:
            raise DatabaseQueryError(f"Failed to write data to S3 control table: {relative_path}")
        if context:
            context.log.info(
                "Successfully wrote data to S3 control table",
                extra={"s3_path": relative_path},
            )
        return True
    except ValueError as e:
        # Re-raise validation errors as DatabaseQueryError
        error_msg = f"Invalid select_query for S3 control table write: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e
    except Exception as e:
        error_msg = f"Failed to write data to S3 control table {relative_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


def save_value_data_to_s3(
    duckdb: DuckDBResource,
    value_data: List[Dict[str, Any]],
    series_code: str,
    partition_date: datetime,
    context: Optional[AssetExecutionContext] = None,
) -> str:
    """Save value data to S3 Parquet file using DuckDBResource.

    Creates a temp table, writes data to S3, then cleans up the temp table.
    Uses the same pattern as meta series loading for consistency.

    Args:
        duckdb: DuckDB resource with S3 access
        value_data: List of dicts with keys: series_id, timestamp, value
        series_code: Series code for path construction
        partition_date: Partition date for path construction
        context: Optional Dagster context for logging

    Returns:
        Relative S3 path where data was saved

    Raises:
        DatabaseQueryError: If write operation fails
    """
    if not value_data:
        if context:
            context.log.warning(f"No value data to save for series_code={series_code}")
        return ""

    # Build S3 path
    relative_path = build_s3_value_data_path(series_code, partition_date)

    # Create temp table with value data using DuckDB native VALUES
    import uuid

    temp_table = f"_temp_value_data_{uuid.uuid4().hex[:8]}"
    try:
        # Build VALUES clause from value_data
        values_parts = []
        for row in value_data:
            series_id = row.get("series_id")
            timestamp = row.get("timestamp")
            value = row.get("value")
            # Format timestamp and value for SQL
            timestamp_str = (
                f"'{timestamp.isoformat()}'"
                if isinstance(timestamp, datetime)
                else f"'{timestamp}'"
            )
            values_parts.append(f"({series_id}, {timestamp_str}, {value})")

        values_clause = ", ".join(values_parts)

        # Create temp table using VALUES
        create_table_sql = f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT * FROM (VALUES {values_clause}) AS t(series_id, timestamp, value)
        """
        duckdb.execute_command(create_table_sql)

        # Build SELECT query with ordering
        select_query = f"SELECT series_id, timestamp, value FROM {temp_table} ORDER BY timestamp"

        # Write to S3 using DuckDBResource.save()
        success = duckdb.save(
            select_statement=select_query,
            file_path=relative_path,
        )

        if not success:
            raise DatabaseQueryError(f"Failed to write value data to S3: {relative_path}")

        if context:
            # Extract series_id from value_data if available for logging
            series_id = value_data[0].get("series_id") if value_data else None
            context.log.info(
                f"Saved {len(value_data)} rows of value data to S3",
                extra={
                    "series_code": series_code,
                    "series_id": series_id,
                    "partition_date": partition_date.date().isoformat(),
                    "s3_path": relative_path,
                    "row_count": len(value_data),
                },
            )

        return relative_path
    except Exception as e:
        error_msg = f"Failed to save value data to S3 {relative_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e
    finally:
        # Clean up temp table (DROP TABLE for temp tables)
        try:
            duckdb.execute_command(f"DROP TABLE IF EXISTS {temp_table}")
        except Exception:
            pass  # Ignore cleanup errors


def create_ingestion_result_dict(
    series_id: int, series_code: str, rows_ingested: int, status: str, reason: Optional[str] = None
) -> Dict[str, Any]:
    """Create a result dictionary for ingestion operations.

    Args:
        series_id: Series ID
        series_code: Series code
        rows_ingested: Number of rows ingested
        status: Status string ("success", "skipped", "failed")
        reason: Optional reason for status

    Returns:
        Dictionary with ingestion results
    """
    result = {
        "series_id": series_id,
        "series_code": series_code,
        "rows_ingested": rows_ingested,
        "status": status,
    }
    if reason:
        result["reason"] = reason
    return result


def check_existing_value_data_in_s3(
    duckdb: DuckDBResource,
    series_code: str,
    target_date: datetime,
    force_refresh: bool,
    context: Optional[AssetExecutionContext] = None,
) -> bool:
    """Check if value data already exists in S3 for the series and date.

    Args:
        duckdb: DuckDB resource with S3 access
        series_code: Series code for path construction and logging
        target_date: Target date to check
        force_refresh: Whether to force refresh (skip check if True)
        context: Optional Dagster execution context for logging

    Returns:
        True if data exists and should be skipped, False otherwise
    """
    if force_refresh:
        return False

    relative_path = build_s3_value_data_path(series_code, target_date)
    full_s3_path = build_full_s3_path(duckdb, relative_path)

    try:
        # Try to read the file - if it exists and has data, skip
        query = f"SELECT COUNT(*) as count FROM read_parquet('{full_s3_path}')"
        result = duckdb.execute_query(query)
        if result is not None and not result.empty:
            # DuckDB returns results as DataFrame-like object, access first row
            count = result.iloc[0]["count"] if hasattr(result, "iloc") else result["count"].iloc[0]
            if count > 0:
                if context:
                    context.log.info(
                        "Data already exists in S3, skipping",
                        extra={
                            "series_code": series_code,
                            "target_date": target_date.date().isoformat(),
                            "s3_path": relative_path,
                            "existing_rows": int(count),
                        },
                    )
                return True
    except Exception:
        # File doesn't exist or can't be read, proceed with ingestion
        pass

    return False


def process_time_series_data_points(
    data_points: List[Dict[str, Any]],
    series_id: int,
    series_code: str,
    context: Optional[AssetExecutionContext] = None,
) -> List[Dict[str, Any]]:
    """Process and validate time-series data points from external sources.

    Parses timestamps and validates them, converting to UTC with proper precision.
    Filters out invalid data points.

    Args:
        data_points: Raw data points with 'timestamp' and 'value' keys
        series_id: Series ID
        series_code: Series code for logging
        context: Optional Dagster execution context for logging

    Returns:
        List of validated value data dictionaries with keys: series_id, timestamp, value
    """
    from dagster_quickstart.utils.datetime_utils import parse_timestamp, validate_timestamp

    value_data = []
    for point in data_points:
        timestamp = point.get("timestamp")
        value = point.get("value")

        if timestamp and value is not None:
            # Parse and validate timestamp to UTC with DateTime64(6) precision
            parsed_timestamp = parse_timestamp(timestamp)
            if parsed_timestamp is None:
                if context:
                    context.log.warning(
                        "Could not parse timestamp",
                        extra={
                            "series_id": series_id,
                            "series_code": series_code,
                            "timestamp": str(timestamp),
                        },
                    )
                continue

            # Validate and normalize timestamp
            try:
                validated_timestamp = validate_timestamp(parsed_timestamp, field_name="timestamp")
            except ValueError as e:
                if context:
                    context.log.warning(
                        "Invalid timestamp",
                        extra={
                            "series_id": series_id,
                            "series_code": series_code,
                            "timestamp": str(timestamp),
                            "error": str(e),
                        },
                    )
                continue

            value_data.append(
                {
                    "series_id": series_id,
                    "timestamp": validated_timestamp,
                    "value": float(value),
                }
            )

    return value_data


def create_or_update_duckdb_view(
    duckdb: DuckDBResource,
    view_name: str,
    view_sql: str,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Create or update a DuckDB view with the given SQL.

    Generic helper function for creating/updating DuckDB views over S3 control tables.

    Args:
        duckdb: DuckDB resource with S3 access
        view_name: Name of the view to create/update
        view_sql: Complete CREATE OR REPLACE VIEW SQL statement
        context: Optional Dagster context for logging

    Raises:
        DatabaseQueryError: If view creation fails
    """
    try:
        duckdb.execute_command(view_sql)
        if context:
            context.log.info(f"Created/updated view {view_name}")
    except Exception as e:
        error_msg = f"Error creating/updating view {view_name}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


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
    from dagster_quickstart.utils.exceptions import ReferentialIntegrityError

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


def unregister_temp_table(
    duckdb: DuckDBResource,
    temp_table: str,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Drop temporary table from DuckDB.

    Generic helper function for cleaning up temporary tables.

    Args:
        duckdb: DuckDB resource
        temp_table: Temporary table name to drop
        context: Optional Dagster context for logging
    """
    try:
        duckdb.execute_command(f"DROP TABLE IF EXISTS {temp_table}")
        if context:
            context.log.debug(f"Dropped temp table {temp_table}")
    except Exception:
        # Table may already be dropped, ignore
        pass
