"""Bloomberg data ingestion logic using pyeqdr.pypdl."""

from datetime import datetime

import polars as pl
from dagster import AssetExecutionContext

from dagster_quickstart.resources import ClickHouseResource, PyPDLResource
from dagster_quickstart.utils.datetime_utils import parse_timestamp, validate_timestamp
from dagster_quickstart.utils.exceptions import DatabaseError, PyPDLError, MetaSeriesNotFoundError
from dagster_quickstart.utils.summary import AssetSummary
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.value_data import ValueDataManager

from .config import BloombergIngestionConfig


def ingest_bloomberg_data_for_series(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    clickhouse: ClickHouseResource,
    series_id: int,
    target_date: datetime,
) -> pl.DataFrame:
    """Internal function for Bloomberg data ingestion using PyPDL for a single series and date.

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

    # Get the specific series by series_id
    series = meta_manager.get_meta_series(series_id)
    if not series:
        raise MetaSeriesNotFoundError(f"Series with ID {series_id} not found")

    series_code = series.get("series_code", f"series_{series_id}")
    context.log.info(
        "Starting Bloomberg data ingestion for series_id=%s (series_code=%s, date: %s)",
        series_id,
        series_code,
        target_date.date(),
    )

    # Check if series uses Bloomberg ticker source
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
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": ["skipped - not Bloomberg"],
            }
        )

    ticker = series.get("ticker")
    field_type_id = series.get("field_type_id")

    if not ticker:
        context.log.warning(f"Series {series_code} has no ticker, skipping")
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": ["skipped - no ticker"],
            }
        )

    if not field_type_id:
        context.log.warning(f"Series {series_code} has no field_type_id, skipping")
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": ["skipped - no field_type_id"],
            }
        )

    # Get field type code from lookup by ID
    query = "SELECT * FROM fieldTypeLookup WHERE field_type_id = {id:UInt32} LIMIT 1"
    result = clickhouse.execute_query(query, parameters={"id": field_type_id})
    field_type = None
    if hasattr(result, "result_rows") and result.result_rows:
        columns = result.column_names
        field_type = dict(zip(columns, result.result_rows[0]))

    if not field_type:
        context.log.warning(f"Could not find field_type for series {series_code}, skipping")
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": ["skipped - field_type not found"],
            }
        )

    field_code = field_type.get("field_type_code")
    if not field_code:
        context.log.warning(f"Field type {field_type_id} has no field_type_code, skipping")
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": ["skipped - no field_type_code"],
            }
        )

    # PyPDL data_source format: "bloomberg/ts/{field_type_code}"
    data_source = f"bloomberg/ts/{field_code}"
    data_code = ticker

    # Use config dates if provided, otherwise use partition date
    start_date = config.start_date if config.start_date else target_date
    end_date = config.end_date if config.end_date else target_date

    # Fetch data using PyPDL for the date range
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
    except PyPDLError as e:
        context.log.error(
            "PyPDL fetch failed",
            extra={"error": str(e), "series_code": series_code},
        )
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": [f"failed - {str(e)}"],
            }
        )

    if not data_points:
        context.log.warning(f"No data points for {series_code} ({data_code})")
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": ["no data"],
            }
        )

    # Check if we should skip (if data already exists and not forcing refresh)
    if not config.force_refresh:
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
            return pl.DataFrame(
                {
                    "series_id": [series_id],
                    "rows_ingested": [0],
                    "status": ["skipped - data exists"],
                }
            )

    # Prepare data for batch insert
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

    if not value_data:
        context.log.warning(f"No valid data points after processing for {series_code}")
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": ["no valid data"],
            }
        )

    # Insert data
    try:
        # Use delete_before_insert when force_refresh=True for idempotency
        # This ensures re-running a partition doesn't create duplicates
        rows_inserted = value_manager.insert_batch_value_data(
            value_data,
            delete_before_insert=config.force_refresh,
            partition_date=target_date,  # Use partition date for partitioning, not config date
        )
        context.log.info(
            f"Inserted {rows_inserted} rows for {series_code} (series_id: {series_id})"
        )

        # Create summary
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

        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [rows_inserted],
                "status": ["success"],
            }
        )
    except (DatabaseError, ValueError, TypeError) as e:
        context.log.error(f"Error inserting data for {series_code}: {e}")
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": [f"failed - {str(e)}"],
            }
        )
    except Exception as e:
        context.log.error(f"Unexpected error inserting data for {series_code}: {e}")
        return pl.DataFrame(
            {
                "series_id": [series_id],
                "rows_ingested": [0],
                "status": [f"failed - unexpected error: {e}"],
            }
        )
