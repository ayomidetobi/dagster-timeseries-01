"""Bloomberg data ingestion logic using pyeqdr.pypdl."""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource, PyPDLResource
from dagster_quickstart.utils.exceptions import DatabaseError
from dagster_quickstart.utils.helpers import (
    check_existing_value_data_in_s3,
    create_ingestion_result_dict,
    process_time_series_data_points,
    save_value_data_to_s3,
)
from dagster_quickstart.utils.pypdl_helpers import (
    build_pypdl_request_params,
    fetch_bloomberg_data,
)
from dagster_quickstart.utils.summary import AssetSummary
from dagster_quickstart.utils.summary.ingestion import (
    add_ingestion_summary_metadata,
)
from dagster_quickstart.utils.validation_helpers import (
    validate_field_type_name,
    validate_series_metadata,
)
from database.referential_integrity import ReferentialIntegrityValidator
from database.schema import SeriesValidationContext

if TYPE_CHECKING:
    from database.lookup_tables import LookupTableManager
    from database.meta_series import MetaSeriesManager

from database.schema import SeriesIngestionContext

from .config import BloombergIngestionConfig, IngestionMode

# ============================================================================
# Bloomberg Ingestion Helper Functions
# ============================================================================


def resolve_series_codes(
    config: BloombergIngestionConfig,
    series_code_from_partition: Optional[str],
    target_date: datetime,
    context: AssetExecutionContext,
) -> Optional[List[str]]:
    """Resolve series codes from config.

    Series codes should be determined in the assets file before calling this function.
    This function validates that series_codes are available in config.

    Args:
        config: Bloomberg ingestion configuration (should have series_codes populated)
        series_code_from_partition: Series code from partition (for logging, optional)
        target_date: Target date for logging
        context: Dagster execution context

    Returns:
        List of series codes to ingest, or None if misconfigured
    """
    if not config.series_codes:
        context.log.error(
            "series_codes is empty in config",
            extra={
                "mode": config.mode.value,
                "target_date": target_date.date().isoformat(),
            },
        )
        return None

    series_codes_to_ingest = config.series_codes
    context.log.info(
        "Using series codes from config",
        extra={
            "mode": config.mode.value,
            "series_count": len(series_codes_to_ingest),
            "target_date": target_date.date().isoformat(),
            "series_code_from_partition": series_code_from_partition,
        },
    )
    return series_codes_to_ingest


def build_series_metadata_map(
    series_codes: List[str],
    meta_manager: "MetaSeriesManager",  # type: ignore[name-defined]
    lookup_manager: "LookupTableManager",  # type: ignore[name-defined]
    context: AssetExecutionContext,
    validator: ReferentialIntegrityValidator,
) -> Dict[str, SeriesIngestionContext]:
    """Build and validate series metadata map.

    Loops over each series_code and validates:
    - Series exists and has series_id
    - Bloomberg ticker source
    - Series metadata (ticker, field_type_name)
    - Field type name

    Args:
        series_codes: List of series codes to validate
        meta_manager: MetaSeriesManager instance
        lookup_manager: LookupTableManager instance
        context: Dagster execution context
        validator: ReferentialIntegrityValidator instance

    Returns:
        Dictionary mapping series_code -> SeriesIngestionContext
    """
    series_metadata: Dict[str, SeriesIngestionContext] = {}

    for sc in series_codes:
        series = meta_manager.get_meta_series_by_code(sc)
        if not series:
            context.log.warning(f"Series {sc} not found, skipping")
            continue

        series_id = series.get("series_id")
        if not series_id:
            context.log.warning(f"Series {sc} has no series_id, skipping")
            continue

        # Create validation context once to avoid repeated parameter passing
        validation_ctx = SeriesValidationContext(series_id=series_id, series_code=sc)

        # Validate Bloomberg ticker source
        skip_reason = validate_bloomberg_ticker_source(
            series, lookup_manager, series_id, sc, context
        )
        if skip_reason:
            context.log.warning(f"Series {sc} skipped: {skip_reason}")
            continue

        # Validate series metadata
        ticker, field_type_name, error_reason = validate_series_metadata(
            series, validation_ctx, context
        )
        if error_reason or ticker is None or field_type_name is None:
            context.log.warning(f"Series {sc} skipped: {error_reason or 'missing metadata'}")
            continue

        # Validate field type name
        field_name = validate_field_type_name(validator, field_type_name, validation_ctx, context)
        if not field_name:
            context.log.warning(f"Series {sc} skipped: field_type_name not found or invalid")
            continue

        series_metadata[sc] = SeriesIngestionContext(
            series_code=sc,
            series_id=series_id,
            ticker=ticker,
            field_name=field_name,
            series_record=series,
        )

    # Clear validator cache to prevent unbounded growth during long runs
    # This ensures cache doesn't accumulate stale data if lookup tables change
    validator.clear_cache()

    return series_metadata


def group_series_by_field(
    series_metadata: Dict[str, SeriesIngestionContext],
) -> Tuple[Dict[str, List[Tuple[str, str]]], Dict[str, str]]:
    """Group series by field_name and create ticker mapping.

    Args:
        series_metadata: Dictionary mapping series_code -> SeriesIngestionContext

    Returns:
        Tuple of (series_by_field dict, ticker_to_series_code dict)
    """
    series_by_field: Dict[
        str, List[Tuple[str, str]]
    ] = {}  # field_name -> [(series_code, ticker), ...]
    ticker_to_series_code: Dict[str, str] = {}  # ticker -> series_code

    for ctx in series_metadata.values():
        field_name = ctx.field_name
        ticker = ctx.ticker
        if field_name not in series_by_field:
            series_by_field[field_name] = []
        series_by_field[field_name].append((ctx.series_code, ticker))
        ticker_to_series_code[ticker] = ctx.series_code

    return series_by_field, ticker_to_series_code


def process_single_series_ingestion(
    sc: str,
    ticker: str,
    data_points_list: List[Dict[str, Any]],
    series_metadata: Dict[str, SeriesIngestionContext],
    duckdb: DuckDBResource,
    target_date: datetime,
    config: BloombergIngestionConfig,
    context: AssetExecutionContext,
) -> Tuple[Dict[str, Any], int, str]:
    """Process ingestion for a single series.

    Args:
        sc: Series code
        ticker: Bloomberg ticker
        data_points_list: List of data points from Bloomberg
        series_metadata: Series metadata dictionary mapping series_code -> SeriesIngestionContext
        duckdb: DuckDB resource
        target_date: Target date for ingestion
        config: Bloomberg ingestion configuration
        context: Dagster execution context

    Returns:
        Tuple of (result_dict, rows_inserted, status)
    """
    ctx = series_metadata[sc]
    series_id = ctx.series_id

    # Ensure data_points_list is a list
    if not isinstance(data_points_list, list):
        context.log.warning(
            f"Expected list of data points for {sc} ({ticker}), got {type(data_points_list)}, skipping"
        )
        return (
            create_ingestion_result_dict(series_id, sc, 0, "skipped", "invalid data type"),
            0,
            "skipped",
        )

    # Type narrowing: data_points_list is confirmed to be List[Dict[str, Any]]
    data_points: List[Dict[str, Any]] = data_points_list

    if not data_points:
        context.log.warning(f"No data points for {sc} ({ticker})")
        add_ingestion_summary_metadata(
            ctx.series_record,
            series_id,
            sc,
            0,
            target_date,
            context,
            "skipped",
            "no data",
        )
        return create_ingestion_result_dict(series_id, sc, 0, "skipped", "no data"), 0, "skipped"

    # Check if data already exists in S3
    if check_existing_value_data_in_s3(duckdb, sc, target_date, config.force_refresh, context):
        add_ingestion_summary_metadata(
            ctx.series_record,
            series_id,
            sc,
            0,
            target_date,
            context,
            "skipped",
            "data exists",
        )
        return (
            create_ingestion_result_dict(series_id, sc, 0, "skipped", "data exists"),
            0,
            "skipped",
        )

    # Process and validate data points
    value_data = process_time_series_data_points(data_points, series_id, sc, context)

    if not value_data:
        context.log.warning(f"No valid data points after processing for {sc}")
        add_ingestion_summary_metadata(
            ctx.series_record,
            series_id,
            sc,
            0,
            target_date,
            context,
            "skipped",
            "no valid data",
        )
        return (
            create_ingestion_result_dict(series_id, sc, 0, "skipped", "no valid data"),
            0,
            "skipped",
        )

    # Save data to S3 for this series
    rows_inserted, insert_error = save_bloomberg_value_data_to_s3(
        duckdb,
        value_data,
        sc,
        target_date,
        config.force_refresh,
        context,
    )

    if insert_error:
        add_ingestion_summary_metadata(
            ctx.series_record,
            series_id,
            sc,
            0,
            target_date,
            context,
            "failed",
            insert_error,
        )
        result_dict = create_ingestion_result_dict(series_id, sc, 0, "failed", insert_error)
        return result_dict, 0, "failed"
    else:
        add_ingestion_summary_metadata(
            ctx.series_record,
            series_id,
            sc,
            rows_inserted,
            target_date,
            context,
            "success",
        )
        result_dict = create_ingestion_result_dict(series_id, sc, rows_inserted, "success")
        return result_dict, rows_inserted, "success"


def ingest_group_for_field(
    field_name: str,
    series_tickers: List[Tuple[str, str]],
    series_metadata: Dict[str, SeriesIngestionContext],
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    duckdb: DuckDBResource,
    target_date: datetime,
    context: AssetExecutionContext,
) -> Tuple[Dict[str, Dict[str, Any]], int, int, int]:
    """Ingest data for a single field group.

    Args:
        field_name: Field name for this group
        series_tickers: List of (series_code, ticker) tuples
        series_metadata: Series metadata dictionary mapping series_code -> SeriesIngestionContext
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        duckdb: DuckDB resource
        target_date: Target date for ingestion
        context: Dagster execution context

    Returns:
        Tuple of (results_dict, successful_count, failed_count, total_rows)
    """
    # Extract tickers and series codes
    tickers = [ticker for _, ticker in series_tickers]
    codes_for_field = [code for code, _ in series_tickers]

    context.log.info(f"Fetching bulk data for {len(tickers)} series with field_type={field_name}")

    # Build PyPDL request parameters
    data_source, data_codes, start_date, end_date = build_pypdl_request_params(
        field_name, tickers, target_date, target_date
    )

    # Fetch data from Bloomberg via PyPDL (bulk call - works for single or multiple)
    data_by_code, fetch_error = fetch_bloomberg_data(
        pypdl_resource,
        data_codes,
        data_source,
        start_date,
        end_date,
        series_code=codes_for_field,
        context=context,
        use_dummy_data=config.use_dummy_data,
    )

    results: Dict[str, Dict[str, Any]] = {}
    successful_count = 0
    failed_count = 0
    total_rows = 0

    if fetch_error:
        context.log.error(f"Bulk fetch failed for field_type={field_name}: {fetch_error}")
        # Mark all series in this group as failed
        for sc in codes_for_field:
            ctx = series_metadata[sc]
            add_ingestion_summary_metadata(
                ctx.series_record,
                ctx.series_id,
                sc,
                0,
                target_date,
                context,
                "failed",
                fetch_error,
            )
            results[sc] = create_ingestion_result_dict(ctx.series_id, sc, 0, "failed", fetch_error)
            failed_count += 1
        return results, successful_count, failed_count, total_rows

    # Handle both single (List) and bulk (Dict) responses
    if isinstance(data_by_code, list):
        # Single series response - convert to dict format
        if len(codes_for_field) == 1 and len(tickers) == 1:
            data_by_code = {tickers[0]: data_by_code}
        else:
            context.log.error(f"Unexpected list response for {len(codes_for_field)} series")
            return results, successful_count, failed_count, total_rows

    if not isinstance(data_by_code, dict):
        context.log.warning(f"No data returned for field_type={field_name}")
        return results, successful_count, failed_count, total_rows

    # Build ticker -> series_code mapping for this field group
    ticker_to_series_code_field: Dict[str, str] = {ticker: code for code, ticker in series_tickers}

    # Process and save each series's data
    for ticker, data_points_list in data_by_code.items():
        # Map ticker back to series_code
        sc_raw = ticker_to_series_code_field.get(ticker)
        if not sc_raw:
            context.log.warning(f"Ticker {ticker} not found in series metadata, skipping")
            continue
        sc: str = sc_raw

        result_dict, rows_inserted, status = process_single_series_ingestion(
            sc=sc,
            ticker=ticker,
            data_points_list=data_points_list,
            series_metadata=series_metadata,
            duckdb=duckdb,
            target_date=target_date,
            config=config,
            context=context,
        )

        results[sc] = result_dict

        if status == "success":
            total_rows += rows_inserted
            successful_count += 1
        elif status == "failed":
            failed_count += 1

    return results, successful_count, failed_count, total_rows


def finalize_bulk_result(
    config: BloombergIngestionConfig,
    all_results: Dict[str, Dict[str, Any]],
    successful_count: int,
    failed_count: int,
    total_rows: int,
    target_date: datetime,
    context: AssetExecutionContext,
) -> Optional[Dict[str, Any]]:
    """Finalize bulk ingestion result with AssetSummary.

    Args:
        config: Bloomberg ingestion configuration
        all_results: Dictionary mapping series_code -> result dict
        successful_count: Number of successful ingestions
        failed_count: Number of failed ingestions
        total_rows: Total rows inserted
        target_date: Target date for ingestion
        context: Dagster execution context

    Returns:
        Aggregate result dictionary, or None if no successful ingestions
    """
    total_series_count = len(all_results)

    context.log.info(
        "Backfill ingestion completed",
        extra={
            "mode": config.mode.value,
            "ingestion_mode": IngestionMode.BACKFILL.value,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "total_rows": total_rows,
            "total_series": total_series_count,
        },
    )

    # Create aggregate AssetSummary for backfill ingestion outcome
    bulk_summary = AssetSummary(
        total_series=total_series_count,
        successful_count=successful_count,
        failed_count=failed_count,
        total_rows=total_rows,
        target_date=target_date,
        asset_type="ingestion",
        asset_metadata={
            "mode": config.mode.value,
            "ingestion_mode": IngestionMode.BACKFILL.value,
        },
    )
    bulk_summary.add_to_context(context)

    if successful_count == 0:
        return None

    # Return aggregate result (using first successful series as base)
    first_success = next((r for r in all_results.values() if r.get("status") == "success"), None)
    if first_success:
        return {
            **first_success,
            "total_series": total_series_count,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "total_rows": total_rows,
            "all_results": all_results,
        }

    return None


def validate_bloomberg_ticker_source(
    series: Dict[str, Any],
    lookup_manager: "LookupTableManager",
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
    # Validate based on ticker_source name only
    ticker_source_name = series.get("ticker_source")

    if not ticker_source_name:
        context.log.warning(
            f"Series {series_code} (series_id={series_id}) has no ticker_source, skipping"
        )
        return "not Bloomberg"

    # Check if ticker_source name matches "Bloomberg" (case-insensitive)
    is_valid = ticker_source_name.strip().lower() == "bloomberg"

    if not is_valid:
        context.log.warning(
            f"Series {series_code} (series_id={series_id}) does not use Bloomberg ticker source "
            f"(has '{ticker_source_name}'), skipping"
        )
        return "not Bloomberg"

    return None


def save_bloomberg_value_data_to_s3(
    duckdb: DuckDBResource,
    value_data: List[Dict[str, Any]],
    series_code: str,
    target_date: datetime,
    force_refresh: bool,
    context: AssetExecutionContext,
) -> Tuple[int, Optional[str]]:
    """Save Bloomberg value data to S3 Parquet file.

    Uses S3 as the datalake, similar to meta series storage pattern.
    For idempotency, if force_refresh=True, the existing file will be overwritten.

    Args:
        duckdb: DuckDB resource with S3 access
        value_data: List of validated value data dictionaries
        series_code: Series code for path construction and logging
        target_date: Target date for partitioning
        force_refresh: Whether to overwrite existing data (always overwrites in S3)
        context: Dagster execution context for logging

    Returns:
        Tuple of (rows_inserted, error_reason)
        If error_reason is not None, rows_inserted will be 0
    """
    try:
        # Build S3 path for logging
        from dagster_quickstart.utils.s3_helpers import build_s3_value_data_path

        relative_path = build_s3_value_data_path(series_code)

        # Log force_refresh if enabled
        if force_refresh:
            context.log.info(
                "force_refresh=True: will overwrite existing data for partition_date",
                extra={
                    "series_code": series_code,
                    "partition_date": target_date.date().isoformat(),
                    "s3_path": relative_path,
                    "force_refresh": force_refresh,
                },
            )

        # Save to S3 using helper function
        s3_path = save_value_data_to_s3(
            duckdb=duckdb,
            value_data=value_data,
            series_code=series_code,
            partition_date=target_date,
            force_refresh=force_refresh,
            context=context,
        )

        rows_inserted = len(value_data)
        context.log.info(
            "Saved Bloomberg value data to S3",
            extra={
                "series_code": series_code,
                "partition_date": target_date.date().isoformat(),
                "row_count": rows_inserted,
                "force_refresh": force_refresh,
                "s3_path": s3_path,
            },
        )
        return rows_inserted, None
    except (DatabaseError, ValueError, TypeError) as e:
        context.log.error(
            "Error saving Bloomberg value data to S3",
            extra={
                "series_code": series_code,
                "partition_date": target_date.date().isoformat(),
                "error": str(e),
            },
        )
        return 0, str(e)
    except Exception as e:
        context.log.error(
            "Unexpected error saving Bloomberg value data to S3",
            extra={
                "series_code": series_code,
                "partition_date": target_date.date().isoformat(),
                "error": str(e),
            },
        )
        return 0, f"unexpected error: {e}"


def ingest_bloomberg_data_for_series(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    duckdb: DuckDBResource,
    series_codes_to_ingest: List[str],
    target_date: datetime,
    meta_manager: "MetaSeriesManager",  # type: ignore[name-defined]
    lookup_manager: "LookupTableManager",  # type: ignore[name-defined]
    series_code: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Unified Bloomberg data ingestion using PyPDL bulk logic for single or multiple series.

    Uses bulk logic for both IngestionMode.DAILY (single series from partition or database) and
    IngestionMode.BACKFILL (multiple series from config) modes.
    Series codes are determined in the assets file and passed directly to this function.

    Args:
        context: Dagster execution context
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        duckdb: DuckDB resource
        series_codes_to_ingest: List of series codes to ingest (determined in assets file)
        target_date: Target date for data ingestion (from partition key)
        meta_manager: MetaSeriesManager instance (initialized in asset)
        lookup_manager: LookupTableManager instance (initialized in asset)
        series_code: Optional series code from partition (for daily mode return logic)

    Returns:
        Result dictionary with ingestion results, or None if skipped/failed.
        For IngestionMode.BACKFILL, returns aggregate result.
    """
    if not series_codes_to_ingest:
        context.log.error(
            "series_codes_to_ingest is empty",
            extra={
                "mode": config.mode.value,
                "target_date": target_date.date().isoformat(),
            },
        )
        return None

    # Build and validate series metadata
    validator = ReferentialIntegrityValidator(duckdb)
    series_metadata = build_series_metadata_map(
        series_codes_to_ingest, meta_manager, lookup_manager, context, validator
    )

    if not series_metadata:
        context.log.error("No valid series found for ingestion")
        return None

    context.log.info(f"Validated {len(series_metadata)} series for ingestion")

    # Group series by field_name
    series_by_field, _ = group_series_by_field(series_metadata)

    # Fetch data in bulk for each field_type group
    all_results: Dict[str, Dict[str, Any]] = {}
    total_rows = 0
    successful_count = 0
    failed_count = 0

    for field_name, series_tickers in series_by_field.items():
        field_results, field_success, field_failed, field_rows = ingest_group_for_field(
            field_name=field_name,
            series_tickers=series_tickers,
            series_metadata=series_metadata,
            config=config,
            pypdl_resource=pypdl_resource,
            duckdb=duckdb,
            target_date=target_date,
            context=context,
        )
        all_results.update(field_results)
        successful_count += field_success
        failed_count += field_failed
        total_rows += field_rows

        return finalize_bulk_result(
            config, all_results, successful_count, failed_count, total_rows, target_date, context
        )
