"""Async Bloomberg data ingestion using xbbg."""

import asyncio
from datetime import datetime
from typing import Any, Dict

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_clickhouse.bloomberg import BloombergResource
from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.utils.exceptions import DatabaseError
from dagster_quickstart.utils.partitions import DAILY_PARTITION, get_partition_date
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.value_data import ValueDataManager


class BloombergIngestionConfig(Config):
    """Configuration for Bloomberg data ingestion."""

    max_concurrent: int = 10  # Maximum concurrent Bloomberg API calls
    force_refresh: bool = False  # Force refresh even if data exists for the date


async def _ingest_bloomberg_data_async(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    bloomberg: BloombergResource,
    clickhouse: ClickHouseResource,
    target_date: datetime,
) -> pl.DataFrame:
    """Internal async function for Bloomberg data ingestion.

    Args:
        context: Dagster execution context
        config: Bloomberg ingestion configuration
        bloomberg: Bloomberg resource
        clickhouse: ClickHouse resource
        target_date: Target date for data ingestion (from partition)

    Returns:
        Summary DataFrame with ingestion results
    """
    context.log.info(
        "Starting Bloomberg data ingestion for all active series for date %s", target_date.date()
    )

    # Test Bloomberg connection before proceeding
    context.log.info("Testing Bloomberg connection...")
    try:
        bloomberg.test_connection()
        context.log.info("Bloomberg connection successful")
    except (ConnectionError, TimeoutError, OSError) as e:
        context.log.error(f"Bloomberg connection failed: {e}")
        context.log.error(
            "Please ensure Bloomberg Terminal is running (Desktop API) "
            "or Bloomberg Server API is accessible (Server API)"
        )
        raise DatabaseError(
            f"Bloomberg connection failed: {e}. "
            "Ensure Bloomberg Terminal is running or Server API is accessible."
        ) from e
    except Exception as e:
        context.log.error(f"Unexpected error testing Bloomberg connection: {e}")
        raise DatabaseError(f"Unexpected error testing Bloomberg connection: {e}") from e

    # Initialize managers
    meta_manager = MetaSeriesManager(clickhouse)
    lookup_manager = LookupTableManager(clickhouse)
    value_manager = ValueDataManager(clickhouse)

    # Get all active metaSeries with Bloomberg ticker source
    active_series = meta_manager.get_active_series(limit=10000)
    context.log.info(f"Found {len(active_series)} active series")

    # Filter for Bloomberg ticker source (ticker_source_id = 1 typically)
    # We'll check by ticker_source_name or assume Bloomberg is ID 1
    bloomberg_ticker_source = lookup_manager.get_ticker_source_by_name("Bloomberg")
    bloomberg_ticker_source_id = (
        bloomberg_ticker_source.get("ticker_source_id") if bloomberg_ticker_source else None
    )

    def is_bloomberg_series(series: Dict[str, Any]) -> bool:
        """Check if a series uses Bloomberg as ticker source.

        Args:
            series: Series dictionary with ticker_source_id

        Returns:
            True if series uses Bloomberg ticker source
        """
        ticker_source_id = series.get("ticker_source_id")
        if not ticker_source_id:
            return False
        # Check if ticker_source_id matches Bloomberg lookup or is 1 (common default)
        return (
            ticker_source_id == bloomberg_ticker_source_id
            or ticker_source_id == 1
            or (bloomberg_ticker_source_id is None and ticker_source_id == 1)
        )

    bloomberg_series = [series for series in active_series if is_bloomberg_series(series)]

    context.log.info("Found %d Bloomberg series to ingest", len(bloomberg_series))

    if not bloomberg_series:
        context.log.warning("No Bloomberg series found to ingest")
        return pl.DataFrame({"series_id": [], "rows_ingested": [], "status": []})

    # Prepare series list for async fetching
    series_list = []
    series_mapping = {}  # Map (ticker, field) to series_id

    for series in bloomberg_series:
        series_id = series["series_id"]
        ticker = series.get("ticker")
        field_type_id = series.get("field_type_id")

        if not ticker:
            context.log.warning(f"Series {series_id} has no ticker, skipping")
            continue

        if not field_type_id:
            context.log.warning(f"Series {series_id} has no field_type_id, skipping")
            continue

        # Get field type code from lookup by ID
        query = f"SELECT * FROM fieldTypeLookup WHERE field_type_id = {{id:UInt32}} LIMIT 1"
        result = clickhouse.execute_query(query, parameters={"id": field_type_id})
        field_type = None
        if hasattr(result, "result_rows") and result.result_rows:
            columns = result.column_names
            field_type = dict(zip(columns, result.result_rows[0]))

        if not field_type:
            context.log.warning(f"Could not find field_type for series {series_id}, skipping")
            continue

        field_code = field_type.get("field_type_code")
        if not field_code:
            context.log.warning(f"Field type {field_type_id} has no field_type_code, skipping")
            continue

        series_list.append({"ticker": ticker, "field": field_code})
        series_mapping[(ticker, field_code)] = {
            "series_id": series_id,
            "series_code": series.get("series_code"),
        }

    context.log.info("Prepared %d series for Bloomberg API calls", len(series_list))

    # Fetch data asynchronously
    context.log.info("Fetching data from Bloomberg API (async)...")
    results = await bloomberg.fetch_multiple_series(
        series_list=series_list,
        start_date=target_date,
        end_date=target_date,
        max_concurrent=config.max_concurrent,
    )

    context.log.info(f"Received {len(results)} successful responses from Bloomberg")

    # Process results and save to ClickHouse
    total_rows_inserted = 0
    successful_series = []
    failed_series = []

    for result in results:
        if not result or "data" not in result:
            continue

        ticker = result.get("ticker")
        field = result.get("field")
        data_points = result.get("data", [])

        if not data_points:
            context.log.warning(f"No data points for {ticker} {field}")
            failed_series.append({"ticker": ticker, "field": field, "reason": "No data"})
            continue

        series_info = series_mapping.get((ticker, field))
        if not series_info:
            context.log.warning(f"No series mapping for {ticker} {field}")
            continue

        series_id = series_info["series_id"]
        series_code = series_info["series_code"]

        # Check if we should skip (if data already exists and not forcing refresh)
        if not config.force_refresh:
            latest_timestamp = value_manager.get_latest_timestamp(series_id)
            if latest_timestamp and latest_timestamp.date() >= target_date.date():
                context.log.info(
                    f"Data already exists for {series_code} on {target_date.date()}, skipping"
                )
                continue

        # Prepare data for batch insert
        value_data = []
        for point in data_points:
            timestamp = point.get("timestamp")
            value = point.get("value")

            if timestamp and value is not None:
                # Ensure timestamp is datetime
                if isinstance(timestamp, str):
                    timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                elif not isinstance(timestamp, datetime):
                    continue

                value_data.append(
                    {
                        "series_id": series_id,
                        "timestamp": timestamp,
                        "value": float(value),
                    }
                )

        if value_data:
            try:
                rows_inserted = value_manager.insert_batch_value_data(value_data)
                total_rows_inserted += rows_inserted
                successful_series.append(
                    {
                        "series_id": series_id,
                        "series_code": series_code,
                        "rows": rows_inserted,
                    }
                )
                context.log.info(
                    f"Inserted {rows_inserted} rows for {series_code} (series_id: {series_id})"
                )
            except (DatabaseError, ValueError, TypeError) as e:
                context.log.error(f"Error inserting data for {series_code}: {e}")
                failed_series.append(
                    {"ticker": ticker, "field": field, "reason": str(e)}
                )
            except Exception as e:
                context.log.error(f"Unexpected error inserting data for {series_code}: {e}")
                failed_series.append(
                    {"ticker": ticker, "field": field, "reason": f"Unexpected error: {e}"}
                )

    # Create summary DataFrame
    summary_data = {
        "total_series": [len(bloomberg_series)],
        "successful_ingestions": [len(successful_series)],
        "failed_ingestions": [len(failed_series)],
        "total_rows_inserted": [total_rows_inserted],
        "target_date": [target_date.isoformat()],
    }

    context.add_output_metadata(
        {
            "total_series": MetadataValue.int(len(bloomberg_series)),
            "successful_ingestions": MetadataValue.int(len(successful_series)),
            "failed_ingestions": MetadataValue.int(len(failed_series)),
            "total_rows_inserted": MetadataValue.int(total_rows_inserted),
            "target_date": MetadataValue.text(target_date.isoformat()),
            "successful_series": MetadataValue.json(
                [s["series_code"] for s in successful_series]
            ),
        }
    )

    context.log.info(
        f"Bloomberg ingestion complete: {len(successful_series)} successful, "
        f"{len(failed_series)} failed, {total_rows_inserted} total rows inserted"
    )

    return pl.DataFrame(summary_data)


@asset(
    group_name="ingestion",
    description="Ingest Bloomberg data for all active metaSeries using xbbg with async operations",
    deps=[AssetKey("load_meta_series_from_csv")],
    kinds=["clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "bloomberg": "", "async": ""},
    retry_policy=RetryPolicy(max_retries=3, delay=2.0),
    partitions_def=DAILY_PARTITION,
)
def ingest_bloomberg_data_async(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    bloomberg: BloombergResource,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest Bloomberg data for all active metaSeries asynchronously.

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date.

    This asset:
    1. Fetches all active metaSeries with Bloomberg ticker source
    2. Gets the Bloomberg field code from field_type lookup
    3. Fetches data from Bloomberg API asynchronously for the partition date
    4. Saves data to ClickHouse valueData table

    Args:
        context: Dagster execution context (includes partition key)
        config: Bloomberg ingestion configuration
        bloomberg: Bloomberg resource
        clickhouse: ClickHouse resource

    Returns:
        Summary DataFrame with ingestion results
    """
    # Get partition date from context
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info("Processing partition %s (date: %s)", partition_key, target_date.date())

    # Run async function using asyncio
    return asyncio.run(
        _ingest_bloomberg_data_async(context, config, bloomberg, clickhouse, target_date)
    )

