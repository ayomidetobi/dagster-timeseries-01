"""S3 helper functions for path building and operations."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.resources.duckdb_datacacher import join_s3
from dagster_quickstart.utils.constants import (
    S3_BASE_PATH_CONTROL,
    S3_BASE_PATH_VALUE_DATA,
    S3_PARQUET_FILE_NAME,
    S3_PARTITION_SERIES_CODE,
    S3_VERSION_PREFIX,
)
from dagster_quickstart.utils.exceptions import DatabaseQueryError


def build_s3_control_table_path(
    control_type: str, version_date: str, filename: str = S3_PARQUET_FILE_NAME
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
        filename: Parquet filename (default: uses S3_PARQUET_FILE_NAME constant)

    Returns:
        Relative S3 path (e.g., 'control/lookup/version-2026-01-12/data.parquet')
    """
    # Use 'version-' instead of 'version=' to avoid URL encoding issues with DuckDB httpfs
    return f"{S3_BASE_PATH_CONTROL}/{control_type}/{S3_VERSION_PREFIX}{version_date}/{filename}"


def build_s3_value_data_path(
    series_code: str, filename: str = S3_PARQUET_FILE_NAME
) -> str:
    """Build relative S3 path for unified value data Parquet file.

    Value data is stored in a single file per series_code, ordered by timestamp.
    Path format: value-data/series_code={series_code}/data.parquet

    Args:
        series_code: Series code (readable identifier)
        filename: Parquet filename (default: uses S3_PARQUET_FILE_NAME constant)

    Returns:
        Relative S3 path (e.g., 'value-data/series_code=AAPL_US_EQ/data.parquet')
    """
    return f"{S3_BASE_PATH_VALUE_DATA}/{S3_PARTITION_SERIES_CODE}={series_code}/{filename}"


def build_full_s3_path(duckdb: DuckDBResource, relative_path: str) -> str:
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


def save_value_data_to_s3(
    duckdb: DuckDBResource,
    value_data: List[Dict[str, Any]],
    series_code: str,
    partition_date: datetime,
    force_refresh: bool = False,
    context: Optional[AssetExecutionContext] = None,
) -> str:
    """Save value data to S3 Parquet file using DuckDBResource.

    Saves all value data for a series_code in a single file, ordered by timestamp.
    If force_refresh is True, overwrites existing data for the specified partition_date.
    If force_refresh is False, merges new data with existing data.

    Creates a temp table, merges with existing data if needed, writes to S3, then cleans up.

    Args:
        duckdb: DuckDB resource with S3 access
        value_data: List of dicts with keys: series_id, timestamp, value
        series_code: Series code for path construction
        partition_date: Partition date for filtering existing data when force_refresh=True
        force_refresh: If True, overwrite existing data for partition_date; if False, merge
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

    # Build unified S3 path (single file per series_code)
    relative_path = build_s3_value_data_path(series_code)
    full_s3_path = build_full_s3_path(duckdb, relative_path)

    # Create temp tables for data processing
    import uuid

    temp_table_new = f"_temp_value_data_new_{uuid.uuid4().hex[:8]}"
    temp_table_existing = f"_temp_value_data_existing_{uuid.uuid4().hex[:8]}"
    temp_table_merged = f"_temp_value_data_merged_{uuid.uuid4().hex[:8]}"

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

        # Create temp table with new data
        create_new_table_sql = f"""
            CREATE TEMP TABLE {temp_table_new} AS
            SELECT * FROM (VALUES {values_clause}) AS t(series_id, timestamp, value)
        """
        duckdb.execute_command(create_new_table_sql)

        # Check if existing file exists and read it
        existing_data_query = f"SELECT COUNT(*) as count FROM read_parquet('{full_s3_path}')"
        has_existing_data = False

        try:
            result = duckdb.execute_query(existing_data_query)
            if result is not None and not result.empty:
                count = result.iloc[0]["count"] if hasattr(result, "iloc") else result["count"].iloc[0]
                has_existing_data = count > 0
        except Exception:
            # File doesn't exist, proceed with new data only
            has_existing_data = False

        if has_existing_data:
            # Read existing data into temp table
            partition_date_str = partition_date.strftime("%Y-%m-%d")
            
            if force_refresh:
                # Filter out existing data for this partition_date
                create_existing_table_sql = f"""
                    CREATE TEMP TABLE {temp_table_existing} AS
                    SELECT series_id, timestamp, value
                    FROM read_parquet('{full_s3_path}')
                    WHERE DATE(timestamp) != DATE('{partition_date_str}')
                """
                if context:
                    context.log.info(
                        f"force_refresh=True: filtering out existing data for partition_date={partition_date_str}"
                    )
            else:
                # Keep all existing data
                create_existing_table_sql = f"""
                    CREATE TEMP TABLE {temp_table_existing} AS
                    SELECT series_id, timestamp, value
                    FROM read_parquet('{full_s3_path}')
                """

            duckdb.execute_command(create_existing_table_sql)

            # Merge existing and new data, removing duplicates by (series_id, timestamp)
            # Prioritize new data over existing data when duplicates exist
            # Use source priority: 1 = new data (higher priority), 0 = existing data (lower priority)
            create_merged_table_sql = f"""
                CREATE TEMP TABLE {temp_table_merged} AS
                SELECT series_id, timestamp, value
                FROM (
                    SELECT series_id, timestamp, value,
                           ROW_NUMBER() OVER (
                               PARTITION BY series_id, timestamp 
                               ORDER BY source_priority DESC, timestamp DESC
                           ) as rn
                    FROM (
                        SELECT series_id, timestamp, value, 1 as source_priority FROM {temp_table_new}
                        UNION ALL
                        SELECT series_id, timestamp, value, 0 as source_priority FROM {temp_table_existing}
                    )
                )
                WHERE rn = 1
                ORDER BY timestamp
            """
            duckdb.execute_command(create_merged_table_sql)

            # Use merged table for saving
            select_query = f"SELECT series_id, timestamp, value FROM {temp_table_merged} ORDER BY timestamp"
        else:
            # No existing data, use new data only
            select_query = f"SELECT series_id, timestamp, value FROM {temp_table_new} ORDER BY timestamp"

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
                    "force_refresh": force_refresh,
                    "merged_with_existing": has_existing_data,
                },
            )

        return relative_path
    except Exception as e:
        error_msg = f"Failed to save value data to S3 {relative_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e
    finally:
        # Clean up temp tables
        for temp_table in [temp_table_new, temp_table_existing, temp_table_merged]:
            try:
                duckdb.execute_command(f"DROP TABLE IF EXISTS {temp_table}")
            except Exception:
                pass  # Ignore cleanup errors


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


def check_existing_value_data_in_s3(
    duckdb: DuckDBResource,
    series_code: str,
    target_date: datetime,
    force_refresh: bool,
    context: Optional[AssetExecutionContext] = None,
) -> bool:
    """Check if value data already exists in S3 for the series and date.

    Uses unified path (single file per series_code) and checks if data exists for the target_date.

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

    relative_path = build_s3_value_data_path(series_code)
    full_s3_path = build_full_s3_path(duckdb, relative_path)
    target_date_str = target_date.strftime("%Y-%m-%d")

    try:
        # Try to read the file - check if data exists for the target_date
        query = f"SELECT COUNT(*) as count FROM read_parquet('{full_s3_path}') WHERE DATE(timestamp) = DATE('{target_date_str}')"
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
