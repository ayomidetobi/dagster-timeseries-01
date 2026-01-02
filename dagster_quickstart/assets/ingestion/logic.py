"""Data ingestion logic for multiple data sources."""

import random
from datetime import datetime
from typing import Dict, Optional

import polars as pl
from dagster import AssetExecutionContext, MetadataValue

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.exceptions import DatabaseError
from dagster_quickstart.utils.helpers import round_six_dp
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.models import TickerSource, TimeSeriesValue
from database.value_data import ValueDataManager

from .config import IngestionConfig


def ingest_data_for_ticker_source(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
    ticker_source: TickerSource,
    target_date: datetime,
) -> pl.DataFrame:
    """Ingest data for all active series of a specific ticker source.

    This is a helper function that processes all active series for a given ticker source.

    Args:
        context: Dagster execution context
        config: Ingestion configuration
        clickhouse: ClickHouse resource
        ticker_source: Ticker source to filter series by
        target_date: Target date for data ingestion (from partition)

    Returns:
        Summary DataFrame with ingestion results
    """
    context.log.info(
        "Starting %s data ingestion for all active series for date %s",
        ticker_source.value,
        target_date.date(),
    )

    # Initialize managers
    meta_manager = MetaSeriesManager(clickhouse)
    lookup_manager = LookupTableManager(clickhouse)
    value_manager = ValueDataManager(clickhouse)

    # Get all active metaSeries
    active_series = meta_manager.get_active_series(limit=10000)
    context.log.info(
        "Found %d total active series",
        len(active_series),
        extra={"total_active_series_count": len(active_series)},
    )

    # Get ticker source ID
    ticker_source_lookup = lookup_manager.get_ticker_source_by_code(ticker_source.value)
    ticker_source_id = (
        ticker_source_lookup.get("ticker_source_id") if ticker_source_lookup else None
    )

    if not ticker_source_id:
        context.log.warning("Ticker source %s not found in lookup table", ticker_source.value)
        return pl.DataFrame(
            {
                "total_series": [0],
                "successful_ingestions": [0],
                "failed_ingestions": [0],
                "total_rows_inserted": [0],
                "target_date": [target_date.isoformat()],
            }
        )

    # 1. Get matching series
    matching_series = [s for s in active_series if s.get("ticker_source_id") == ticker_source_id]
    context.log.info(
        "Found %d %s series to ingest",
        len(matching_series),
        ticker_source.value,
        extra={"ticker_source": ticker_source.value, "matching_series_count": len(matching_series)},
    )

    if not matching_series:
        context.log.warning("No %s series found to ingest", ticker_source.value)
        return pl.DataFrame(
            {
                "total_series": [0],
                "successful_ingestions": [0],
                "failed_ingestions": [0],
                "total_rows_inserted": [0],
                "target_date": [target_date.isoformat()],
            }
        )

    # 2. Fetch latest timestamps in bulk
    latest_timestamps: Dict[int, Optional[datetime]] = {}
    if not config.force_refresh:
        series_ids = [s["series_id"] for s in matching_series]
        latest_timestamps = value_manager.get_latest_timestamps_for_series(series_ids)
        skipped_count = 0
        for s in matching_series:
            latest_ts = latest_timestamps.get(s["series_id"])
            if latest_ts is not None and latest_ts.date() >= target_date.date():
                skipped_count += 1
        if skipped_count > 0:
            context.log.info(
                "Skipping %d series with existing data for target date",
                skipped_count,
                extra={"skipped_count": skipped_count},
            )

    # 3. Accumulate validated rows
    all_validated_data = []
    successful_series = []
    validation_errors = []

    for series in matching_series:
        series_id = series["series_id"]
        series_code = series.get("series_code", f"series_{series_id}")

        try:
            # Skip if data already exists and not forcing refresh
            if not config.force_refresh:
                latest_timestamp = latest_timestamps.get(series_id)
                if latest_timestamp and latest_timestamp.date() >= target_date.date():
                    continue

            # In a real implementation, this would connect to the data source API
            # For now, we'll simulate with sample data for the partition date
            # TODO: Replace with actual API integration

            # Generate random positive value between 1.0 and 1000.0
            raw_value = random.uniform(1.0, 1000.0)
            raw_value_data = [
                {
                    "series_id": series_id,
                    "timestamp": target_date,
                    "value": round_six_dp(raw_value),  # Round to 6 decimal places for consistency
                }
            ]

            # Validate value data using TimeSeriesValue model
            validated_value_data = [
                {
                    "series_id": v.series_id,
                    "timestamp": v.timestamp,
                    "value": float(v.value),  # Convert Decimal to float for database
                }
                for v in [TimeSeriesValue(**item) for item in raw_value_data]
            ]
            all_validated_data.extend(validated_value_data)
            successful_series.append(
                {
                    "series_id": series_id,
                    "series_code": series_code,
                    "rows": len(validated_value_data),
                }
            )

        except Exception as validation_error:
            error_msg = f"Validation failed for series {series_code} (series_id: {series_id}): {validation_error}"
            context.log.error(
                "Validation failed for value data: %s",
                validation_error,
                extra={
                    "series_id": series_id,
                    "series_code": series_code,
                    "validation_error": str(validation_error),
                },
            )
            validation_errors.append(error_msg)

    # Raise exception if there are validation errors to mark asset as failed
    if validation_errors:
        error_summary = f"Validation failed for {len(validation_errors)} series(s): " + "; ".join(
            validation_errors
        )
        context.log.error(error_summary)
        raise ValueError(error_summary)

    # 4. Batch insert all series at once
    total_rows_inserted = 0
    if all_validated_data:
        try:
            rows_inserted = value_manager.insert_batch_value_data(
                all_validated_data,
                delete_before_insert=config.force_refresh,
                partition_date=target_date,
            )
            total_rows_inserted = rows_inserted
            context.log.info(
                "Batch inserted %d rows for %d series",
                total_rows_inserted,
                len(successful_series),
                extra={
                    "total_rows": total_rows_inserted,
                    "successful_series_count": len(successful_series),
                },
            )
        except (DatabaseError, ValueError, TypeError) as e:
            context.log.error(
                "Error during batch insert: %s",
                e,
                extra={"error": str(e), "total_rows_attempted": len(all_validated_data)},
            )
            # Re-raise to mark asset as failed
            raise DatabaseError(
                f"Batch insert failed for {len(successful_series)} series(s): {e}"
            ) from e

    # Create summary DataFrame
    failed_series: list = []  # Only populated if batch insert fails (which would raise)
    summary_data = {
        "total_series": [len(matching_series)],
        "successful_ingestions": [len(successful_series)],
        "failed_ingestions": [len(failed_series)],
        "total_rows_inserted": [total_rows_inserted],
        "target_date": [target_date.isoformat()],
    }

    context.add_output_metadata(
        {
            "total_series": MetadataValue.int(len(matching_series)),
            "successful_ingestions": MetadataValue.int(len(successful_series)),
            "failed_ingestions": MetadataValue.int(len(failed_series)),
            "total_rows_inserted": MetadataValue.int(total_rows_inserted),
            "target_date": MetadataValue.text(target_date.isoformat()),
            "successful_series": MetadataValue.json([s["series_code"] for s in successful_series]),
        }
    )

    context.log.info(
        "%s ingestion complete: %d successful, %d failed, %d total rows inserted",
        ticker_source.value,
        len(successful_series),
        len(failed_series),
        total_rows_inserted,
        extra={
            "ticker_source": ticker_source.value,
            "successful_count": len(successful_series),
            "failed_count": len(failed_series),
            "total_rows": total_rows_inserted,
        },
    )

    return pl.DataFrame(summary_data)
