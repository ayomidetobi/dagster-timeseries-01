"""Data ingestion logic for multiple data sources."""

import random
from datetime import datetime
from typing import Any, Dict, List, Optional

import polars as pl
from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.exceptions import DatabaseError
from dagster_quickstart.utils.helpers import round_to_six_decimal_places
from dagster_quickstart.utils.summary import AssetSummary
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.schema import TickerSource, TimeSeriesValue
from database.value_data import ValueDataManager

from .config import IngestionConfig


def get_ticker_source_id(
    lookup_manager: LookupTableManager, ticker_source: TickerSource, context: AssetExecutionContext
) -> Optional[int]:
    """Get ticker source ID from lookup table.

    Args:
        lookup_manager: Lookup table manager instance
        ticker_source: Ticker source enum
        context: Dagster execution context for logging

    Returns:
        Ticker source ID or None if not found
    """
    ticker_source_lookup = lookup_manager.get_ticker_source_by_code(ticker_source.value)
    ticker_source_id = (
        ticker_source_lookup.get("ticker_source_id") if ticker_source_lookup else None
    )

    if not ticker_source_id:
        context.log.warning("Ticker source %s not found in lookup table", ticker_source.value)

    return ticker_source_id


def filter_series_by_ticker_source(
    active_series: List[Dict[str, Any]], ticker_source_id: int, context: AssetExecutionContext
) -> List[Dict[str, Any]]:
    """Filter active series by ticker source ID.

    Args:
        active_series: List of active series dictionaries
        ticker_source_id: Ticker source ID to filter by
        context: Dagster execution context for logging

    Returns:
        List of matching series dictionaries
    """
    matching_series = [s for s in active_series if s.get("ticker_source_id") == ticker_source_id]
    context.log.info(
        "Found %d series matching ticker source ID %d",
        len(matching_series),
        ticker_source_id,
        extra={"ticker_source_id": ticker_source_id, "matching_series_count": len(matching_series)},
    )
    return matching_series


def fetch_latest_timestamps_bulk(
    value_manager: ValueDataManager,
    matching_series: List[Dict[str, Any]],
    target_date: datetime,
    force_refresh: bool,
    context: AssetExecutionContext,
) -> Dict[int, Optional[datetime]]:
    """Fetch latest timestamps for all matching series in bulk.

    Args:
        value_manager: Value data manager instance
        matching_series: List of matching series dictionaries
        target_date: Target date for ingestion
        force_refresh: Whether to force refresh (skip timestamp check)
        context: Dagster execution context for logging

    Returns:
        Dictionary mapping series_id to latest timestamp (or None)
    """
    latest_timestamps: Dict[int, Optional[datetime]] = {}
    if not force_refresh:
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
    return latest_timestamps


def prepare_value_data_for_series(
    series_id: int, target_date: datetime, context: AssetExecutionContext
) -> List[Dict[str, Any]]:
    """Prepare value data for a single series.

    In a real implementation, this would connect to the data source API.
    For now, we simulate with sample data.

    Args:
        series_id: Series ID
        target_date: Target date for data ingestion
        context: Dagster execution context for logging

    Returns:
        List of raw value data dictionaries

    Todo:
        Replace with actual API integration
    """
    # Generate random positive value between 1.0 and 1000.0
    raw_value = random.uniform(1.0, 1000.0)
    raw_value_data = [
        {
            "series_id": series_id,
            "timestamp": target_date,
            "value": round_to_six_decimal_places(
                raw_value
            ),  # Round to 6 decimal places for consistency
        }
    ]
    return raw_value_data


def validate_value_data(
    raw_value_data: List[Dict[str, Any]], series_code: str, context: AssetExecutionContext
) -> List[Dict[str, Any]]:
    """Validate value data using TimeSeriesValue model.

    Args:
        raw_value_data: List of raw value data dictionaries
        series_code: Series code for error messages
        context: Dagster execution context for logging

    Returns:
        List of validated value data dictionaries

    Raises:
        ValueError: If validation fails
    """
    validated_value_data = []
    for raw_item in raw_value_data:
        try:
            validated_item = TimeSeriesValue(**raw_item)
            validated_value_data.append(
                {
                    "series_id": validated_item.series_id,
                    "timestamp": validated_item.timestamp,
                    "value": float(validated_item.value),  # Convert Decimal to float for database
                }
            )
        except Exception as validation_error:
            context.log.error(
                "Validation failed for value data: %s",
                validation_error,
                extra={
                    "series_code": series_code,
                    "raw_data": raw_item,
                    "validation_error": str(validation_error),
                },
            )
            raise ValueError(
                f"Value data validation failed for series {series_code}: {validation_error}"
            ) from validation_error

    return validated_value_data


def validate_and_accumulate_value_data(
    matching_series: List[Dict[str, Any]],
    latest_timestamps: Dict[int, Optional[datetime]],
    target_date: datetime,
    force_refresh: bool,
    context: AssetExecutionContext,
    summary: Optional[AssetSummary] = None,
) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str], AssetSummary]:
    """Validate and accumulate value data for all matching series.

    Args:
        matching_series: List of matching series dictionaries
        latest_timestamps: Dictionary mapping series_id to latest timestamp
        target_date: Target date for ingestion
        force_refresh: Whether to force refresh (skip timestamp check)
        context: Dagster execution context for logging
        summary: Optional AssetSummary to update incrementally

    Returns:
        Tuple of (all_validated_data, successful_series, validation_errors, summary)
    """
    all_validated_data = []
    successful_series = []
    validation_errors = []

    # Initialize summary if not provided
    if summary is None:
        summary = AssetSummary(
            total_series=len(matching_series),
            target_date=target_date,
            asset_type="ingestion",
        )

    for series in matching_series:
        series_id = series["series_id"]
        series_code = series.get("series_code", f"series_{series_id}")

        try:
            # Skip if data already exists and not forcing refresh
            if not force_refresh:
                latest_timestamp = latest_timestamps.get(series_id)
                if latest_timestamp and latest_timestamp.date() >= target_date.date():
                    continue

            # Prepare value data
            raw_value_data = prepare_value_data_for_series(series_id, target_date, context)

            # Validate value data
            validated_value_data = validate_value_data(raw_value_data, series_code, context)

            all_validated_data.extend(validated_value_data)
            series_info = {
                "series_id": series_id,
                "series_code": series_code,
                "rows": len(validated_value_data),
            }
            successful_series.append(series_info)
            summary.add_successful_series(series_info).increment_successful()

        except Exception as validation_error:
            error_msg = (
                f"Validation failed for series {series_code} (series_id: {series_id}): "
                f"{validation_error}"
            )
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
            summary.add_failed_series(
                {
                    "series_id": series_id,
                    "series_code": series_code,
                    "reason": str(validation_error),
                }
            ).increment_failed()

    return all_validated_data, successful_series, validation_errors, summary


def batch_insert_value_data(
    value_manager: ValueDataManager,
    all_validated_data: List[Dict[str, Any]],
    successful_series: List[Dict[str, Any]],
    target_date: datetime,
    force_refresh: bool,
    context: AssetExecutionContext,
    summary: Optional[AssetSummary] = None,
) -> tuple[int, AssetSummary]:
    """Batch insert value data for all series.

    Args:
        value_manager: Value data manager instance
        all_validated_data: List of validated value data dictionaries
        successful_series: List of successful series dictionaries
        target_date: Target date for ingestion
        force_refresh: Whether to force refresh (delete before insert)
        context: Dagster execution context for logging
        summary: Optional AssetSummary to update with rows inserted

    Returns:
        Tuple of (number of rows inserted, summary)

    Raises:
        DatabaseError: If batch insert fails
    """
    if summary is None:
        summary = AssetSummary(target_date=target_date, asset_type="ingestion")

    if not all_validated_data:
        return 0, summary

    try:
        rows_inserted = value_manager.insert_batch_value_data(
            all_validated_data,
            delete_before_insert=force_refresh,
            partition_date=target_date,
        )
        summary.update(total_rows=rows_inserted)
        context.log.info(
            "Batch inserted %d rows for %d series",
            rows_inserted,
            len(successful_series),
            extra={
                "total_rows": rows_inserted,
                "successful_series_count": len(successful_series),
            },
        )
        return rows_inserted, summary
    except (DatabaseError, ValueError, TypeError) as e:
        context.log.error(
            "Error during batch insert: %s",
            e,
            extra={"error": str(e), "total_rows_attempted": len(all_validated_data)},
        )
        raise DatabaseError(
            f"Batch insert failed for {len(successful_series)} series(s): {e}"
        ) from e


def create_ingestion_summary(
    summary: AssetSummary,
    context: AssetExecutionContext,
) -> pl.DataFrame:
    """Create summary DataFrame and add metadata to context.

    Args:
        summary: AssetSummary instance with all ingestion data
        context: Dagster execution context for logging and metadata

    Returns:
        Summary DataFrame
    """
    summary.add_to_context(context)
    return summary.to_polars_dataframe()


def ingest_data_for_ticker_source(
    context: AssetExecutionContext,
    config: IngestionConfig,
    duckdb: DuckDBResource,
    ticker_source: TickerSource,
    target_date: datetime,
) -> pl.DataFrame:
    """Ingest data for all active series of a specific ticker source.

    This is a helper function that processes all active series for a given ticker source.
    Uses AssetSummary throughout to track progress incrementally.

    Args:
        context: Dagster execution context
        config: Ingestion configuration
        duckdb: DuckDB resource
        ticker_source: Ticker source to filter series by
        target_date: Target date for data ingestion (from partition)

    Returns:
        Summary DataFrame with ingestion results

    Raises:
        ValueError: If validation fails for any series
        DatabaseError: If batch insert fails
    """
    context.log.info(
        "Starting %s data ingestion for all active series for date %s",
        ticker_source.value,
        target_date.date(),
    )

    # Initialize summary early to track progress throughout
    summary = AssetSummary(
        total_series=0,
        target_date=target_date,
        asset_type="ingestion",
    )

    # Initialize managers
    meta_manager = MetaSeriesManager(duckdb)
    lookup_manager = LookupTableManager(duckdb)
    value_manager = ValueDataManager(duckdb)

    # Get all active metaSeries
    active_series = meta_manager.get_active_series(limit=10000)
    context.log.info(
        "Found %d total active series",
        len(active_series),
        extra={"total_active_series_count": len(active_series)},
    )

    # Get ticker source ID
    ticker_source_id = get_ticker_source_id(lookup_manager, ticker_source, context)
    if not ticker_source_id:
        summary.update(total_series=0, successful_count=0, failed_count=0, total_rows=0)
        return create_ingestion_summary(summary, context)

    # Filter series by ticker source
    matching_series = filter_series_by_ticker_source(active_series, ticker_source_id, context)
    if not matching_series:
        context.log.warning("No %s series found to ingest", ticker_source.value)
        summary.update(total_series=0, successful_count=0, failed_count=0, total_rows=0)
        return create_ingestion_summary(summary, context)

    # Update summary with total matching series
    summary.update(total_series=len(matching_series))

    # Fetch latest timestamps in bulk
    latest_timestamps = fetch_latest_timestamps_bulk(
        value_manager, matching_series, target_date, config.force_refresh, context
    )

    # Validate and accumulate value data (updates summary incrementally)
    all_validated_data, successful_series, validation_errors, summary = (
        validate_and_accumulate_value_data(
            matching_series,
            latest_timestamps,
            target_date,
            config.force_refresh,
            context,
            summary=summary,
        )
    )

    # Raise exception if there are validation errors to mark asset as failed
    if validation_errors:
        error_summary = f"Validation failed for {len(validation_errors)} series(s): " + "; ".join(
            validation_errors
        )
        context.log.error(error_summary)
        # Summary already has failed series tracked
        raise ValueError(error_summary)

    # Batch insert all series at once (updates summary with rows inserted)
    total_rows_inserted, summary = batch_insert_value_data(
        value_manager,
        all_validated_data,
        successful_series,
        target_date,
        config.force_refresh,
        context,
        summary=summary,
    )

    # Create and return summary DataFrame
    summary_df = create_ingestion_summary(summary, context)

    context.log.info(
        "%s ingestion complete: %d successful, %d failed, %d total rows inserted",
        ticker_source.value,
        summary.successful_count,
        summary.failed_count,
        summary.total_rows,
        extra={
            "ticker_source": ticker_source.value,
            "successful_count": summary.successful_count,
            "failed_count": summary.failed_count,
            "total_rows": summary.total_rows,
        },
    )

    return summary_df
