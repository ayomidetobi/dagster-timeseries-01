"""Data ingestion logic for multiple data sources."""

from datetime import datetime
from typing import Any, Dict

import polars as pl
from dagster import AssetExecutionContext, MetadataValue

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.exceptions import DatabaseError
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.models import TickerSource
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

    def is_matching_series(series: Dict[str, Any]) -> bool:
        """Check if a series uses the specified ticker source.

        Args:
            series: Series dictionary with ticker_source_id

        Returns:
            True if series uses the specified ticker source
        """
        series_ticker_source_id = series.get("ticker_source_id")
        # Both ticker_source_id (from lookup) and series_ticker_source_id must exist
        if not ticker_source_id or not series_ticker_source_id:
            return False
        return series_ticker_source_id == ticker_source_id

    # Filter series by ticker source
    matching_series = [series for series in active_series if is_matching_series(series)]
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

    total_rows_inserted = 0
    successful_series = []
    failed_series = []

    # Process each series
    for series in matching_series:
        series_id = series["series_id"]
        series_code = series.get("series_code", f"series_{series_id}")

        try:
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
                    continue

            # In a real implementation, this would connect to the data source API
            # For now, we'll simulate with sample data for the partition date
            # TODO: Replace with actual API integration
            value_data = [
                {
                    "series_id": series_id,
                    "timestamp": target_date,
                    "value": 100.0,  # Sample value - replace with actual data
                }
            ]

            # Insert data using idempotent insert
            rows_inserted = value_manager.insert_batch_value_data(
                value_data,
                delete_before_insert=config.force_refresh,
                partition_date=target_date,
            )

            total_rows_inserted += rows_inserted
            successful_series.append(
                {
                    "series_id": series_id,
                    "series_code": series_code,
                    "rows": rows_inserted,
                }
            )
            context.log.info(
                "Inserted %d rows for %s (series_id: %d)",
                rows_inserted,
                series_code,
                series_id,
                extra={
                    "series_id": series_id,
                    "series_code": series_code,
                    "rows_inserted": rows_inserted,
                },
            )

        except (DatabaseError, ValueError, TypeError) as e:
            context.log.error(
                "Error inserting data for %s: %s",
                series_code,
                e,
                extra={"series_id": series_id, "series_code": series_code, "error": str(e)},
            )
            failed_series.append(
                {"series_id": series_id, "series_code": series_code, "reason": str(e)}
            )
        except Exception as e:
            context.log.error(
                "Unexpected error inserting data for %s: %s",
                series_code,
                e,
                extra={"series_id": series_id, "series_code": series_code, "error": str(e)},
            )
            failed_series.append(
                {
                    "series_id": series_id,
                    "series_code": series_code,
                    "reason": f"Unexpected error: {e}",
                }
            )

    # Create summary DataFrame
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
