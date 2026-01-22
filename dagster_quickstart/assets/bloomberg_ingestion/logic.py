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
from dagster_quickstart.utils.summary.ingestion import (
    add_ingestion_summary_metadata,
    handle_ingestion_failure,
)
from dagster_quickstart.utils.validation_helpers import (
    validate_field_type_name,
    validate_series_metadata,
)
from database.referential_integrity import ReferentialIntegrityValidator

if TYPE_CHECKING:
    from database.lookup_tables import LookupTableManager
    from database.meta_series import MetaSeriesManager

from .config import BloombergIngestionConfig


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
        # Log force_refresh if enabled
        if force_refresh:
            from dagster_quickstart.utils.helpers import build_s3_value_data_path

            relative_path = build_s3_value_data_path(series_code, target_date)
            context.log.info(
                f"force_refresh=True: will overwrite existing data for {series_code}",
                extra={"series_code": series_code, "s3_path": relative_path},
            )

        # Save to S3 using helper function
        s3_path = save_value_data_to_s3(
            duckdb=duckdb,
            value_data=value_data,
            series_code=series_code,
            partition_date=target_date,
            context=context,
        )

        rows_inserted = len(value_data)
        context.log.info(
            f"Saved {rows_inserted} rows to S3 for {series_code}",
            extra={"s3_path": s3_path},
        )
        return rows_inserted, None
    except (DatabaseError, ValueError, TypeError) as e:
        context.log.error(f"Error saving data to S3 for {series_code}: {e}")
        return 0, str(e)
    except Exception as e:
        context.log.error(f"Unexpected error saving data to S3 for {series_code}: {e}")
        return 0, f"unexpected error: {e}"


def ingest_bloomberg_data_for_series(
    context: AssetExecutionContext,
    config: BloombergIngestionConfig,
    pypdl_resource: PyPDLResource,
    duckdb: DuckDBResource,
    series_code: str,
    target_date: datetime,
    meta_manager: "MetaSeriesManager",  # type: ignore[name-defined]
    lookup_manager: "LookupTableManager",  # type: ignore[name-defined]
) -> Optional[Dict[str, Any]]:
    """Internal function for Bloomberg data ingestion using PyPDL for a single series and date.

    This function orchestrates the Bloomberg data ingestion process by calling
    smaller, reusable helper functions for each step.

    Args:
        context: Dagster execution context
        config: Bloomberg ingestion configuration
        pypdl_resource: PyPDL resource
        duckdb: DuckDB resource
        series_code: Series code to ingest (from partition key)
        target_date: Target date for data ingestion (from partition key)
        meta_manager: MetaSeriesManager instance (initialized in asset)
        lookup_manager: LookupTableManager instance (initialized in asset)

    Returns:
        Result dictionary with ingestion results, or None if skipped/failed
    """
    # Get and validate series exists by code
    series = meta_manager.get_meta_series_by_code(series_code)
    if not series:
        context.log.error(f"Series with code {series_code} not found")
        return handle_ingestion_failure(
            series_code, target_date, context, f"series {series_code} not found"
        )

    series_id = series.get("series_id")
    if not series_id:
        context.log.error(f"Series {series_code} has no series_id")
        return handle_ingestion_failure(
            series_code, target_date, context, f"series {series_code} has no series_id", series
        )

    context.log.info(
        "Starting Bloomberg data ingestion for series_code=%s (series_id=%s, date: %s)",
        series_code,
        series_id,
        target_date.date(),
    )

    # Validate Bloomberg ticker source
    skip_reason = validate_bloomberg_ticker_source(
        series, lookup_manager, series_id, series_code, context
    )
    if skip_reason:
        add_ingestion_summary_metadata(
            series, series_id, series_code, 0, target_date, context, "skipped", skip_reason
        )
        return create_ingestion_result_dict(series_id, series_code, 0, "skipped", skip_reason)

    # Validate series metadata (ticker and field_type name)
    ticker, field_type_name, error_reason = validate_series_metadata(
        series, series_id, series_code, context
    )
    if error_reason or ticker is None or field_type_name is None:
        reason = error_reason or "missing metadata"
        add_ingestion_summary_metadata(
            series, series_id, series_code, 0, target_date, context, "skipped", reason
        )
        return create_ingestion_result_dict(series_id, series_code, 0, "skipped", reason)

    # Validate field type name using ReferentialIntegrityValidator
    validator = ReferentialIntegrityValidator(duckdb)
    field_name = validate_field_type_name(
        validator, field_type_name, series_id, series_code, context
    )
    if not field_name:
        reason = "field_type_name not found or invalid"
        add_ingestion_summary_metadata(
            series, series_id, series_code, 0, target_date, context, "skipped", reason
        )
        return create_ingestion_result_dict(series_id, series_code, 0, "skipped", reason)

    # Build PyPDL request parameters
    data_source, data_code, start_date, end_date = build_pypdl_request_params(
        field_name, ticker, target_date
    )

    # Fetch data from Bloomberg via PyPDL
    data_points, fetch_error = fetch_bloomberg_data(
        pypdl_resource,
        data_code,
        data_source,
        start_date,
        end_date,
        series_code,
        context,
        use_dummy_data=config.use_dummy_data,
    )
    if fetch_error:
        add_ingestion_summary_metadata(
            series, series_id, series_code, 0, target_date, context, "failed", fetch_error
        )
        return create_ingestion_result_dict(series_id, series_code, 0, "failed", fetch_error)

    if not data_points:
        context.log.warning(f"No data points for {series_code} ({data_code})")
        add_ingestion_summary_metadata(
            series, series_id, series_code, 0, target_date, context, "skipped", "no data"
        )
        return create_ingestion_result_dict(series_id, series_code, 0, "skipped", "no data")

    # Check if data already exists in S3
    if check_existing_value_data_in_s3(
        duckdb, series_code, target_date, config.force_refresh, context
    ):
        add_ingestion_summary_metadata(
            series, series_id, series_code, 0, target_date, context, "skipped", "data exists"
        )
        return create_ingestion_result_dict(series_id, series_code, 0, "skipped", "data exists")

    # Process and validate data points
    value_data = process_time_series_data_points(data_points, series_id, series_code, context)

    if not value_data:
        context.log.warning(f"No valid data points after processing for {series_code}")
        add_ingestion_summary_metadata(
            series, series_id, series_code, 0, target_date, context, "skipped", "no valid data"
        )
        return create_ingestion_result_dict(series_id, series_code, 0, "skipped", "no valid data")

    # Save data to S3
    rows_inserted, insert_error = save_bloomberg_value_data_to_s3(
        duckdb,
        value_data,
        series_code,
        target_date,
        config.force_refresh,
        context,
    )
    if insert_error:
        add_ingestion_summary_metadata(
            series, series_id, series_code, 0, target_date, context, "failed", insert_error
        )
        return create_ingestion_result_dict(series_id, series_code, 0, "failed", insert_error)

    # Add summary metadata and return success result
    add_ingestion_summary_metadata(
        series, series_id, series_code, rows_inserted, target_date, context, "success"
    )
    return create_ingestion_result_dict(series_id, series_code, rows_inserted, "success")
