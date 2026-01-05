"""Bloomberg data ingestion logic using pyeqdr.pypdl."""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import polars as pl
from dagster import AssetExecutionContext

from dagster_quickstart.resources import ClickHouseResource, PyPDLResource
from dagster_quickstart.utils.datetime_utils import parse_timestamp, validate_timestamp
from dagster_quickstart.utils.exceptions import DatabaseError, PyPDLError, MetaSeriesNotFoundError
from dagster_quickstart.utils.summary import AssetSummary
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.referential_integrity import ReferentialIntegrityValidator
from database.value_data import ValueDataManager

from .config import BloombergIngestionConfig


def create_skipped_dataframe(series_id: int, reason: str) -> pl.DataFrame:
    """Create a DataFrame indicating a skipped ingestion.

    Args:
        series_id: Series ID
        reason: Reason for skipping

    Returns:
        DataFrame with skip status
    """
    return pl.DataFrame(
        {
            "series_id": [series_id],
            "rows_ingested": [0],
            "status": [f"skipped - {reason}"],
        }
    )


def create_failed_dataframe(series_id: int, reason: str) -> pl.DataFrame:
    """Create a DataFrame indicating a failed ingestion.

    Args:
        series_id: Series ID
        reason: Reason for failure

    Returns:
        DataFrame with failure status
    """
    return pl.DataFrame(
        {
            "series_id": [series_id],
            "rows_ingested": [0],
            "status": [f"failed - {reason}"],
        }
    )


def create_success_dataframe(series_id: int, rows_ingested: int) -> pl.DataFrame:
    """Create a DataFrame indicating a successful ingestion.

    Args:
        series_id: Series ID
        rows_ingested: Number of rows ingested

    Returns:
        DataFrame with success status
    """
    return pl.DataFrame(
        {
            "series_id": [series_id],
            "rows_ingested": [rows_ingested],
            "status": ["success"],
        }
    )


def get_and_validate_series(
    meta_manager: MetaSeriesManager, series_id: int, context: AssetExecutionContext
) -> Dict[str, Any]:
    """Get and validate that a series exists.

    Args:
        meta_manager: Meta series manager instance
        series_id: Series ID to retrieve
        context: Dagster execution context for logging

    Returns:
        Series dictionary

    Raises:
        MetaSeriesNotFoundError: If series is not found
    """
    series = meta_manager.get_meta_series(series_id)
    if not series:
        raise MetaSeriesNotFoundError(f"Series with ID {series_id} not found")
    return series


def validate_bloomberg_ticker_source(
    series: Dict[str, Any],
    lookup_manager: LookupTableManager,
    series_id: int,
    series_code: str,
    context: AssetExecutionContext,
) -> Optional[str]:
    """Validate that series uses Bloomberg ticker source.

    Args:
        series: Series dictionary
        lookup_manager: Lookup table manager instance
        series_id: Series ID for logging
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        None if valid, reason string if invalid
    """
    bloomberg_ticker_source = lookup_manager.get_ticker_source_by_name("Bloomberg")
    bloomberg_ticker_source_id = (
        bloomberg_ticker_source.get("ticker_source_id") if bloomberg_ticker_source else None
    )

    ticker_source_id = series.get("ticker_source_id")
    if not ticker_source_id or (
        ticker_source_id != bloomberg_ticker_source_id
        and ticker_source_id != 1
        and not (bloomberg_ticker_source_id is None and ticker_source_id == 1)
    ):
        context.log.warning(
            f"Series {series_code} (series_id={series_id}) does not use Bloomberg ticker source, skipping"
        )
        return "not Bloomberg"
    return None


def validate_series_metadata(
    series: Dict[str, Any], series_id: int, series_code: str, context: AssetExecutionContext
) -> Tuple[Optional[str], Optional[int], Optional[str]]:
    """Validate that series has required metadata (ticker and field_type_id).

    Args:
        series: Series dictionary
        series_id: Series ID for logging
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        Tuple of (ticker, field_type_id, error_reason)
        If error_reason is not None, ticker and field_type_id will be None
    """
    ticker = series.get("ticker")
    field_type_id = series.get("field_type_id")

    if not ticker:
        context.log.warning(f"Series {series_code} has no ticker, skipping")
        return None, None, "no ticker"

    if not field_type_id:
        context.log.warning(f"Series {series_code} has no field_type_id, skipping")
        return None, None, "no field_type_id"

    return ticker, field_type_id, None


def get_field_type_code(
    lookup_manager: LookupTableManager,
    validator: ReferentialIntegrityValidator,
    field_type_id: int,
    series_id: int,
    series_code: str,
    context: AssetExecutionContext,
) -> Optional[str]:
    """Get field type code from lookup table and validate referential integrity.

    Uses LookupTableManager to get the field type by ID, then validates
    the field_type_code exists in the lookup table using ReferentialIntegrityValidator.

    Args:
        lookup_manager: Lookup table manager instance
        validator: Referential integrity validator instance
        field_type_id: Field type ID to lookup
        series_id: Series ID for logging
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        Field type code or None if not found or invalid
    """
    # Get field type by ID using LookupTableManager
    field_type = lookup_manager.get_field_type_by_id(field_type_id)

    if not field_type:
        context.log.warning(f"Could not find field_type for series {series_code}, skipping")
        return None

    field_code = field_type.get("field_type_code")
    if not field_code:
        context.log.warning(f"Field type {field_type_id} has no field_type_code, skipping")
        return None

    # Validate referential integrity: check that field_type_code exists in lookup table
    # Since field_type is not in CODE_BASED_LOOKUPS, we validate by checking
    # if the field_type_code exists in the lookup table directly
    from dagster_quickstart.utils.constants import DB_TABLES

    table_name = DB_TABLES["field_type"]
    query = (
        f"SELECT field_type_code FROM {table_name} WHERE field_type_code = {{code:String}} LIMIT 1"
    )
    result = validator.clickhouse.execute_query(query, parameters={"code": field_code})
    if not (hasattr(result, "result_rows") and result.result_rows):
        context.log.warning(
            f"Invalid field_type_code '{field_code}' for series {series_code}, skipping"
        )
        return None

    return field_code


def build_pypdl_request_params(
    field_code: str, ticker: str, config: BloombergIngestionConfig, target_date: datetime
) -> Tuple[str, str, datetime, datetime]:
    """Build PyPDL request parameters.

    Args:
        field_code: Field type code
        ticker: Ticker symbol
        config: Bloomberg ingestion configuration
        target_date: Target date for ingestion

    Returns:
        Tuple of (data_source, data_code, start_date, end_date)
    """
    # PyPDL data_source format: "bloomberg/ts/{field_type_code}"
    data_source = f"bloomberg/ts/{field_code}"
    data_code = ticker

    # Use config dates if provided, otherwise use partition date
    start_date = config.start_date if config.start_date else target_date
    end_date = config.end_date if config.end_date else target_date

    return data_source, data_code, start_date, end_date


def fetch_bloomberg_data(
    pypdl_resource: PyPDLResource,
    data_code: str,
    data_source: str,
    start_date: datetime,
    end_date: datetime,
    series_code: str,
    context: AssetExecutionContext,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """Fetch data from Bloomberg via PyPDL.

    Args:
        pypdl_resource: PyPDL resource
        data_code: Data code (ticker)
        data_source: Data source path
        start_date: Start date for data fetch
        end_date: End date for data fetch
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        Tuple of (data_points, error_reason)
        If error_reason is not None, data_points will be None
    """
    context.log.info(
        "Fetching data from Bloomberg via PyPDL",
        extra={
            "series_code": series_code,
            "data_code": data_code,
            "data_source": data_source,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        },
    )
    try:
        data_points = pypdl_resource.fetch_time_series(
            data_code=data_code,
            data_source=data_source,
            start_date=start_date,
            end_date=end_date,
        )
        context.log.info(
            "PyPDL fetch completed",
            extra={"data_point_count": len(data_points)},
        )
        return data_points, None
    except PyPDLError as e:
        context.log.error(
            "PyPDL fetch failed",
            extra={"error": str(e), "series_code": series_code},
        )
        return None, str(e)


def check_existing_data(
    value_manager: ValueDataManager,
    series_id: int,
    series_code: str,
    target_date: datetime,
    force_refresh: bool,
    context: AssetExecutionContext,
) -> bool:
    """Check if data already exists for the series and date.

    Args:
        value_manager: Value data manager instance
        series_id: Series ID
        series_code: Series code for logging
        target_date: Target date to check
        force_refresh: Whether to force refresh (skip check if True)
        context: Dagster execution context for logging

    Returns:
        True if data exists and should be skipped, False otherwise
    """
    if force_refresh:
        return False

    latest_timestamp = value_manager.get_latest_timestamp(series_id)
    if latest_timestamp and latest_timestamp.date() >= target_date.date():
        context.log.info(
            "Data already exists, skipping",
            extra={
                "series_id": series_id,
                "series_code": series_code,
                "target_date": target_date.date().isoformat(),
                "latest_timestamp": latest_timestamp.date().isoformat(),
            },
        )
        return True
    return False


def process_bloomberg_data_points(
    data_points: List[Dict[str, Any]],
    series_id: int,
    series_code: str,
    context: AssetExecutionContext,
) -> List[Dict[str, Any]]:
    """Process and validate Bloomberg data points.

    Args:
        data_points: Raw data points from PyPDL
        series_id: Series ID
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        List of validated value data dictionaries
    """
    value_data = []
    for point in data_points:
        timestamp = point.get("timestamp")
        value = point.get("value")

        if timestamp and value is not None:
            # Parse and validate timestamp to UTC with DateTime64(6) precision
            parsed_timestamp = parse_timestamp(timestamp)
            if parsed_timestamp is None:
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


def insert_bloomberg_value_data(
    value_manager: ValueDataManager,
    value_data: List[Dict[str, Any]],
    series_id: int,
    series_code: str,
    target_date: datetime,
    force_refresh: bool,
    context: AssetExecutionContext,
) -> Tuple[int, Optional[str]]:
    """Insert Bloomberg value data into database.

    Args:
        value_manager: Value data manager instance
        value_data: List of validated value data dictionaries
        series_id: Series ID
        series_code: Series code for logging
        target_date: Target date for partitioning
        force_refresh: Whether to delete before insert
        context: Dagster execution context for logging

    Returns:
        Tuple of (rows_inserted, error_reason)
        If error_reason is not None, rows_inserted will be 0
    """
    try:
        # Use delete_before_insert when force_refresh=True for idempotency
        # This ensures re-running a partition doesn't create duplicates
        rows_inserted = value_manager.insert_batch_value_data(
            value_data,
            delete_before_insert=force_refresh,
            partition_date=target_date,  # Use partition date for partitioning, not config date
        )
        context.log.info(
            f"Inserted {rows_inserted} rows for {series_code} (series_id: {series_id})"
        )
        return rows_inserted, None
    except (DatabaseError, ValueError, TypeError) as e:
        context.log.error(f"Error inserting data for {series_code}: {e}")
        return 0, str(e)
    except Exception as e:
        context.log.error(f"Unexpected error inserting data for {series_code}: {e}")
        return 0, f"unexpected error: {e}"


def create_bloomberg_summary(
    series: Dict[str, Any],
    series_id: int,
    series_code: str,
    rows_inserted: int,
    target_date: datetime,
    context: AssetExecutionContext,
) -> pl.DataFrame:
    """Create summary DataFrame and add metadata to context.

    Args:
        series: Series dictionary
        series_id: Series ID
        series_code: Series code
        rows_inserted: Number of rows inserted
        target_date: Target date for ingestion
        context: Dagster execution context for logging and metadata

    Returns:
        Summary DataFrame
    """
    summary = AssetSummary.for_ingestion(
        matching_series=[series],
        successful_series=[
            {
                "series_id": series_id,
                "series_code": series_code,
                "rows": rows_inserted,
            }
        ],
        failed_series=[],
        total_rows_inserted=rows_inserted,
        target_date=target_date,
    )
    summary.add_to_context(context)
    return create_success_dataframe(series_id, rows_inserted)


def ingest_bloomberg_data_for_series(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    clickhouse: ClickHouseResource,
    series_id: int,
    target_date: datetime,
) -> pl.DataFrame:
    """Internal function for Bloomberg data ingestion using PyPDL for a single series and date.

    This function orchestrates the Bloomberg data ingestion process by calling
    smaller, reusable helper functions for each step.

    Args:
        context: Dagster execution context
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        clickhouse: ClickHouse resource
        series_id: Series ID to ingest (from partition key)
        target_date: Target date for data ingestion (from partition key)

    Returns:
        Summary DataFrame with ingestion results
    """
    # Initialize managers
    meta_manager = MetaSeriesManager(clickhouse)
    lookup_manager = LookupTableManager(clickhouse)
    value_manager = ValueDataManager(clickhouse)

    # Get and validate series exists
    try:
        series = get_and_validate_series(meta_manager, series_id, context)
    except MetaSeriesNotFoundError:
        return create_failed_dataframe(series_id, "series not found")

    series_code = series.get("series_code", f"series_{series_id}")
    context.log.info(
        "Starting Bloomberg data ingestion for series_id=%s (series_code=%s, date: %s)",
        series_id,
        series_code,
        target_date.date(),
    )

    # Validate Bloomberg ticker source
    skip_reason = validate_bloomberg_ticker_source(
        series, lookup_manager, series_id, series_code, context
    )
    if skip_reason:
        return create_skipped_dataframe(series_id, skip_reason)

    # Validate series metadata (ticker and field_type_id)
    ticker, field_type_id, error_reason = validate_series_metadata(
        series, series_id, series_code, context
    )
    if error_reason or ticker is None or field_type_id is None:
        return create_skipped_dataframe(series_id, error_reason or "missing metadata")

    # Get field type code using LookupTableManager and validate with ReferentialIntegrityValidator
    validator = ReferentialIntegrityValidator(clickhouse)
    field_code = get_field_type_code(
        lookup_manager, validator, field_type_id, series_id, series_code, context
    )
    if not field_code:
        return create_skipped_dataframe(series_id, "field_type_code not found")

    # Build PyPDL request parameters
    data_source, data_code, start_date, end_date = build_pypdl_request_params(
        field_code, ticker, config, target_date
    )

    # Fetch data from Bloomberg via PyPDL
    data_points, fetch_error = fetch_bloomberg_data(
        pypdl_resource, data_code, data_source, start_date, end_date, series_code, context
    )
    if fetch_error:
        return create_failed_dataframe(series_id, fetch_error)

    if not data_points:
        context.log.warning(f"No data points for {series_code} ({data_code})")
        return create_skipped_dataframe(series_id, "no data")

    # Check if data already exists
    if check_existing_data(
        value_manager, series_id, series_code, target_date, config.force_refresh, context
    ):
        return create_skipped_dataframe(series_id, "data exists")

    # Process and validate data points
    value_data = process_bloomberg_data_points(data_points, series_id, series_code, context)

    if not value_data:
        context.log.warning(f"No valid data points after processing for {series_code}")
        return create_skipped_dataframe(series_id, "no valid data")

    # Insert data
    rows_inserted, insert_error = insert_bloomberg_value_data(
        value_manager,
        value_data,
        series_id,
        series_code,
        target_date,
        config.force_refresh,
        context,
    )
    if insert_error:
        return create_failed_dataframe(series_id, insert_error)

    # Create summary and return success DataFrame
    return create_bloomberg_summary(
        series, series_id, series_code, rows_inserted, target_date, context
    )
