"""Bloomberg data ingestion using pyeqdr.pypdl."""

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

from dagster_clickhouse.pypdl_resource import PyPDLResource
from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_INGESTION,
    RETRY_POLICY_MAX_RETRIES_INGESTION,
)
from dagster_quickstart.utils.datetime_utils import parse_timestamp, validate_timestamp
from dagster_quickstart.utils.exceptions import DatabaseError, PyPDLError
from dagster_quickstart.utils.partitions import DAILY_PARTITION, get_partition_date
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.value_data import ValueDataManager


class BloombergIngestionConfig(Config):
    """Configuration for Bloomberg data ingestion."""

    force_refresh: bool = False  # If True, delete existing data for the partition date before inserting (ensures idempotency when re-running a partition). If False, skip insertion if data already exists for the date.
    max_concurrent: int = 3  # Maximum concurrent PyPDL requests (overrides resource default if set)


async def _ingest_bloomberg_data(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    clickhouse: ClickHouseResource,
    target_date: datetime,
) -> pl.DataFrame:
    """Internal function for Bloomberg data ingestion using PyPDL.

    Args:
        context: Dagster execution context
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        clickhouse: ClickHouse resource
        target_date: Target date for data ingestion (from partition)

    Returns:
        Summary DataFrame with ingestion results
    """
    context.log.info(
        "Starting Bloomberg data ingestion for all active series for date %s", target_date.date()
    )

    # Initialize managers
    meta_manager = MetaSeriesManager(clickhouse)
    lookup_manager = LookupTableManager(clickhouse)
    value_manager = ValueDataManager(clickhouse)

    # Get all active metaSeries
    active_series = meta_manager.get_active_series(limit=10000)
    context.log.info(f"Found {len(active_series)} active series")

    # Filter for Bloomberg ticker source
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

    # Prepare series list for PyPDL fetching
    series_list = []
    series_mapping = {}  # Map (data_code, data_source) to series_id

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
        query = "SELECT * FROM fieldTypeLookup WHERE field_type_id = {id:UInt32} LIMIT 1"
        result = clickhouse.execute_query(query, parameters={"id": field_type_id})
        field_type = None
        if hasattr(result, "result_rows") and result.result_rows:
            columns = result.column_names
            field_type = dict[Any, Any](zip[tuple[Any, Any]](columns, result.result_rows[0]))

        if not field_type:
            context.log.warning(f"Could not find field_type for series {series_id}, skipping")
            continue

        field_code = field_type.get("field_type_code")
        if not field_code:
            context.log.warning(f"Field type {field_type_id} has no field_type_code, skipping")
            continue

        # PyPDL data_source format: "bloomberg/ts/{field_type_code}"
        data_source = f"bloomberg/ts/{field_code}"
        data_code = ticker

        series_list.append(
            {
                "data_code": data_code,
                "data_source": data_source,
            }
        )
        series_mapping[(data_code, data_source)] = {
            "series_id": series_id,
            "series_code": series.get("series_code"),
        }

    context.log.info("Prepared %d series for PyPDL API calls", len(series_list))

    # Determine max_concurrent to use (config override or resource default)
    # Pass as parameter instead of mutating resource (thread-safe)
    max_concurrent_override = (
        config.max_concurrent if config.max_concurrent else None
    )
    if max_concurrent_override:
        context.log.info(
            f"Using max_concurrent={max_concurrent_override} from config "
            f"(resource default is {pypdl_resource.max_concurrent})"
        )

    # Fetch data using PyPDL (async)
    context.log.info(
        "Fetching data from Bloomberg via PyPDL",
        extra={"series_count": len(series_list), "target_date": target_date.isoformat()},
    )
    try:
        results = await pypdl_resource.fetch_time_series_batch(
            series_list=series_list,
            start_date=target_date,
            end_date=target_date,
            max_concurrent=max_concurrent_override,
        )
        context.log.info(
            "PyPDL fetch completed",
            extra={"successful_responses": len(results)},
        )
    except PyPDLError as e:
        context.log.error(
            "PyPDL fetch failed",
            extra={"error": str(e), "series_count": len(series_list)},
        )
        raise DatabaseError(f"PyPDL fetch failed: {e}") from e

    # Process results and save to ClickHouse
    total_rows_inserted = 0
    successful_series = []
    failed_series = []

    for result in results:
        if not result or "data" not in result:
            continue

        data_code = result.get("data_code")
        data_source = result.get("data_source")
        data_points = result.get("data", [])

        if not data_points:
            context.log.warning(f"No data points for {data_code} ({data_source})")
            failed_series.append(
                {"data_code": data_code, "data_source": data_source, "reason": "No data"}
            )
            continue

        series_info = series_mapping.get((data_code, data_source))
        if not series_info:
            context.log.warning(f"No series mapping for {data_code} ({data_source})")
            continue

        series_id = series_info["series_id"]
        series_code = series_info["series_code"]

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
                    validated_timestamp = validate_timestamp(
                        parsed_timestamp, field_name="timestamp"
                    )
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

        if value_data:
            try:
                # Use delete_before_insert when force_refresh=True for idempotency
                # This ensures re-running a partition doesn't create duplicates
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
                    f"Inserted {rows_inserted} rows for {series_code} (series_id: {series_id})"
                )
            except (DatabaseError, ValueError, TypeError) as e:
                context.log.error(f"Error inserting data for {series_code}: {e}")
                failed_series.append(
                    {"data_code": data_code, "data_source": data_source, "reason": str(e)}
                )
            except Exception as e:
                context.log.error(f"Unexpected error inserting data for {series_code}: {e}")
                failed_series.append(
                    {
                        "data_code": data_code,
                        "data_source": data_source,
                        "reason": f"Unexpected error: {e}",
                    }
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
            "successful_series": MetadataValue.json([s["series_code"] for s in successful_series]),
        }
    )

    context.log.info(
        f"Bloomberg ingestion complete: {len(successful_series)} successful, "
        f"{len(failed_series)} failed, {total_rows_inserted} total rows inserted"
    )

    return pl.DataFrame(summary_data)


@asset(
    group_name="ingestion",
    description="Ingest Bloomberg data for all active metaSeries using pyeqdr.pypdl",
    deps=[AssetKey("load_meta_series_from_csv")],
    kinds=["clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "bloomberg": "", "pypdl": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=DAILY_PARTITION,
)
async def ingest_bloomberg_data_async(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest Bloomberg data for all active metaSeries using PyPDL (async).

    This asset is partitioned by day for backfill-safety. Each partition processes
    data for a specific date.

    This asset:
    1. Fetches all active metaSeries with Bloomberg ticker source
    2. Gets the field_type_code from field_type lookup
    3. Constructs data_source as "bloomberg/ts/{field_type_code}"
    4. Uses ticker as data_code
    5. Fetches data from Bloomberg via PyPDL asynchronously for the partition date
    6. Saves data to ClickHouse valueData table

    Args:
        context: Dagster execution context (includes partition key)
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        clickhouse: ClickHouse resource

    Returns:
        Summary DataFrame with ingestion results
    """
    # Get partition date from context
    partition_key = context.partition_key
    target_date = get_partition_date(partition_key)
    context.log.info("Processing partition %s (date: %s)", partition_key, target_date.date())

    # Run async ingestion function (Dagster handles the event loop)
    return await _ingest_bloomberg_data(context, config, pypdl_resource, clickhouse, target_date)
