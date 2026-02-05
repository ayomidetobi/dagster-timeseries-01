"""Bloomberg data ingestion assets using pyeqdr.pypdl."""

from typing import List, Optional

from dagster import (
    AssetExecutionContext,
    AssetKey,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import DuckDBResource, PyPDLResource
from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_INGESTION,
    RETRY_POLICY_MAX_RETRIES_INGESTION,
)
from dagster_quickstart.utils.csv_loader_helpers import ensure_views_exist
from dagster_quickstart.utils.exceptions import DatabaseQueryError, S3ControlTableNotFoundError
from dagster_quickstart.utils.helpers import get_version_date
from dagster_quickstart.utils.partitions import (
    META_SERIES_PARTITION,
)
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.schema import TickerSource

from .config import BloombergBackfillIngestionConfig, BloombergIngestionConfig, IngestionMode
from .logic import ingest_bloomberg_data_for_series


def determine_series_codes(
    config: BloombergIngestionConfig,
    series_code_from_partition: Optional[str],
    target_date,  # datetime object
    meta_manager: MetaSeriesManager,
    context: AssetExecutionContext,
) -> Optional[List[str]]:
    """Determine series codes to ingest based on mode and available sources.

    For DAILY mode:
    - Fetch all active Bloomberg series from database

    For BACKFILL mode:
    - Use series_code from partition (if available)
    - Otherwise fallback to config.series_codes

    Args:
        config: Bloomberg ingestion configuration
        series_code_from_partition: Optional series code from partition (for BACKFILL mode)
        target_date: Target date for logging
        meta_manager: MetaSeriesManager instance to fetch series codes from database
        context: Dagster execution context for logging

    Returns:
        List of series codes to ingest, or None if misconfigured or no series found
    """
    if config.mode == IngestionMode.DAILY:
        # DAILY mode: fetch all active Bloomberg series from database
        series_codes_to_ingest = meta_manager.get_all_active_series_codes(
            ticker_source=TickerSource.BLOOMBERG.value
        )
        if not series_codes_to_ingest:
            context.log.warning(
                "Daily mode: no active Bloomberg series codes found in database",
                extra={
                    "mode": config.mode.value,
                    "target_date": target_date.date().isoformat(),
                },
            )
            return None
        context.log.info(
            "Daily mode: fetched all active series codes from database",
            extra={
                "mode": config.mode.value,
                "series_count": len(series_codes_to_ingest),
                "target_date": target_date.date().isoformat(),
            },
        )
        return series_codes_to_ingest
    else:  # BACKFILL mode
        if series_code_from_partition:
            # Use series code from partition
            series_codes_to_ingest = [series_code_from_partition]
            context.log.info(
                "Backfill mode: using series_code from partition",
                extra={
                    "mode": config.mode.value,
                    "ingestion_mode": IngestionMode.BACKFILL.value,
                    "series_code": series_code_from_partition,
                    "target_date": target_date.date().isoformat(),
                },
            )
            return series_codes_to_ingest
        elif config.series_codes:
            # Fallback to config.series_codes
            series_codes_to_ingest = config.series_codes
            context.log.info(
                "Backfill mode: using series_codes from config",
                extra={
                    "mode": config.mode.value,
                    "ingestion_mode": IngestionMode.BACKFILL.value,
                    "series_count": len(series_codes_to_ingest),
                    "target_date": target_date.date().isoformat(),
                },
            )
            return series_codes_to_ingest
        else:
            context.log.error(
                "Backfill mode: no series codes available (partition or config required)",
                extra={
                    "mode": config.mode.value,
                    "ingestion_mode": IngestionMode.BACKFILL.value,
                },
            )
            return None


@asset(
    group_name="bloomberg_ingestion",
    description="Ingest Bloomberg data for a single metaSeries using pyeqdr.pypdl",
    deps=[AssetKey("load_meta_series_from_csv")],
    kinds=["duckdb"],
    io_manager_key="duckdb_io_manager",
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "bloomberg": "", "pypdl": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
)
def ingest_bloomberg_data_pypdl(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    duckdb: DuckDBResource,
) -> Optional[dict]:
    """Ingest Bloomberg data for a single metaSeries using PyPDL.

    This asset uses multi-dimensional partitions (daily + dynamic series).
    Each partition processes data for one series (by series_code) for one date.
    Dagster handles concurrency by running multiple partitions in parallel.

    This asset:
    1. Gets the metaSeries by series_code from the partition key
    2. Gets the field_type_name from field_type lookup (should contain Bloomberg field code like "PX_LAST")
    3. Constructs data_source as "bloomberg/ts/{field_type_name}"
    4. Uses ticker as data_code
    5. Fetches data from Bloomberg via PyPDL for the partition date
    6. Saves data to S3 Parquet files

    Args:
        context: Dagster execution context (includes partition keys: date and series)
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        duckdb: DuckDB resource

    Returns:
        Result dictionary with ingestion results, or None if skipped/failed
        All reporting is done via Dagster metadata and AssetSummary
    """
    # Extract partition keys from multi-dimensional partition
    series_code: Optional[str] = None
    # Get target date from partition
    target_date = config.get_start_date()

    context.log.info(
        "Processing partition: series_code=%s, date=%s", series_code, target_date.date()
    )

    # Ensure views exist before calling logic
    version_date = get_version_date()
    meta_manager = MetaSeriesManager(duckdb)
    lookup_manager = LookupTableManager(duckdb)

    # Ensure views exist before querying (metaSeries and lookup tables)
    try:
        ensure_views_exist(
            context=context,
            duckdb=duckdb,
            version_date=version_date,
            create_view_funcs=[
                meta_manager.create_or_update_view,
                lookup_manager.create_or_update_views,
            ],
        )
    except DatabaseQueryError as e:
        # Handle missing S3 control table
        # Determine which control table failed based on error message
        error_msg = str(e).lower()
        if "metaseries" in error_msg or "meta_series" in error_msg:
            control_type = "metaSeries"
        elif "lookup" in error_msg:
            control_type = "lookup_tables"
        else:
            # Default to metaSeries if we can't determine
            control_type = "metaSeries"

        s3_error = S3ControlTableNotFoundError(control_type=control_type, version_date=version_date)
        context.log.error(
            f"{s3_error.control_type} S3 control table not found for version {s3_error.version_date} - CSV must be loaded first"
        )
        return None

    # Determine series codes based on mode and available sources
    series_codes_to_ingest = determine_series_codes(
        config=config,
        series_code_from_partition=series_code,
        target_date=target_date,
        meta_manager=meta_manager,
        context=context,
    )

    if not series_codes_to_ingest:
        return None

    # Pass resolved series codes directly to logic function (no need to modify config)
    ingest_bloomberg_data_for_series(
        context,
        config,
        pypdl_resource,
        duckdb,
        series_codes_to_ingest,  # Pass resolved series codes directly
        target_date,
        meta_manager,
        lookup_manager,
    )
    return None


@asset(
    group_name="bloomberg_ingestion",
    description="Backfill Bloomberg data for multiple series using pyeqdr.pypdl",
    deps=[AssetKey("load_meta_series_from_csv")],
    kinds=["duckdb"],
    io_manager_key="duckdb_io_manager",
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "bloomberg": "", "pypdl": "", "backfill": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_INGESTION, delay=RETRY_POLICY_DELAY_INGESTION
    ),
    partitions_def=META_SERIES_PARTITION,
)
def ingest_bloomberg_data_backfill_pypdl(
    context: AssetExecutionContext,
    config: BloombergBackfillIngestionConfig,
    pypdl_resource: PyPDLResource,
    duckdb: DuckDBResource,
) -> Optional[dict]:
    """Backfill Bloomberg data for multiple series using PyPDL.

    This asset is used for backfilling historical data for a date range.
    It processes multiple series codes specified in the config.

    This asset:
    1. Gets series codes from config (required for backfill mode)
    2. For each series, gets the field_type_name from field_type lookup
    3. Constructs data_source as "bloomberg/ts/{field_type_name}"
    4. Uses ticker as data_code
    5. Fetches data from Bloomberg via PyPDL for the date range
    6. Saves data to S3 Parquet files

    Args:
        context: Dagster execution context
        config: Bloomberg backfill ingestion configuration (includes date range and series codes)
        pypdl_resource: PyPDL resource
        duckdb: DuckDB resource

    Returns:
        Result dictionary with ingestion results, or None if skipped/failed
        All reporting is done via Dagster metadata and AssetSummary
    """
    start_date = config.get_start_date()

    # Extract partition series_code if available (for backfill mode)
    series_code: Optional[str] = None
    if context.partition_keys is not None:
        series_code = context.partition_keys
        # views exist before calling logic
    version_date = get_version_date()
    meta_manager = MetaSeriesManager(duckdb)
    lookup_manager = LookupTableManager(duckdb)

    # Ensure views exist before querying (metaSeries and lookup tables)
    try:
        ensure_views_exist(
            context=context,
            duckdb=duckdb,
            version_date=version_date,
            create_view_funcs=[
                meta_manager.create_or_update_view,
                lookup_manager.create_or_update_views,
            ],
        )
    except DatabaseQueryError as e:
        # Handle missing S3 control table
        # Determine which control table failed based on error message
        error_msg = str(e).lower()
        if "metaseries" in error_msg or "meta_series" in error_msg:
            control_type = "metaSeries"
        elif "lookup" in error_msg:
            control_type = "lookup_tables"
        else:
            # Default to metaSeries if we can't determine
            control_type = "metaSeries"

        s3_error = S3ControlTableNotFoundError(control_type=control_type, version_date=version_date)
        context.log.error(
            f"{s3_error.control_type} S3 control table not found for version {s3_error.version_date} - CSV must be loaded first"
        )
        return None

    # # Determine series codes based on mode and available sources
    # series_codes_to_ingest = determine_series_codes(
    #     config=config,
    #     series_code_from_partition=series_code,
    #     target_date=start_date,
    #     meta_manager=meta_manager,
    #     context=context,
    # )

    # if not series_codes_to_ingest:
    #     return None

    # Pass resolved series codes directly to logic function (no need to modify config)
    ingest_bloomberg_data_for_series(
        context,
        config,
        pypdl_resource,
        duckdb,
        series_code,  # Pass resolved series codes directly
        start_date,  # target_date for backfill is start_date
        meta_manager,
        lookup_manager,
    )
    return None
