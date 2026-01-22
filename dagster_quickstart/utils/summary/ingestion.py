"""Helper functions for ingestion operations with error handling and metadata."""

from datetime import datetime
from typing import Any, Dict, Optional

from dagster import AssetExecutionContext

from dagster_quickstart.utils.summary import AssetSummary


def add_bloomberg_ingestion_summary_metadata(
    series: Dict[str, Any],
    series_id: int,
    series_code: str,
    ticker: str,
    field_type_name: str,
    data_source: str,
    data_code: str,
    rows_inserted: int,
    target_date: datetime,
    context: AssetExecutionContext,
    status: str = "success",
    reason: Optional[str] = None,
    s3_path: Optional[str] = None,
    data_points_fetched: Optional[int] = None,
    use_dummy_data: bool = False,
) -> None:
    """Add Bloomberg-specific summary metadata to Dagster context.

    Args:
        series: Series dictionary (can be empty dict if series not found)
        series_id: Series ID
        series_code: Series code
        ticker: Ticker symbol
        field_type_name: Field type name (Bloomberg field code)
        data_source: PyPDL data source path
        data_code: PyPDL data code (ticker)
        rows_inserted: Number of rows inserted
        target_date: Target date for ingestion
        context: Dagster execution context for logging and metadata
        status: Status string ("success", "skipped", "failed")
        reason: Optional reason for status
        s3_path: Optional S3 path where data was saved
        data_points_fetched: Optional number of data points fetched from API
        use_dummy_data: Whether dummy data was used
    """
    summary = AssetSummary.for_bloomberg_ingestion(
        series_code=series_code,
        series_id=series_id,
        ticker=ticker,
        field_type_name=field_type_name,
        data_source=data_source,
        data_code=data_code,
        rows_inserted=rows_inserted,
        target_date=target_date,
        status=status,
        reason=reason,
        s3_path=s3_path,
        data_points_fetched=data_points_fetched,
        use_dummy_data=use_dummy_data,
    )

    # Add summary metadata to context
    summary.add_to_context(context)


def add_ingestion_summary_metadata(
    series: Dict[str, Any],
    series_id: int,
    series_code: str,
    rows_inserted: int,
    target_date: datetime,
    context: AssetExecutionContext,
    status: str = "success",
    reason: Optional[str] = None,
) -> None:
    """Add summary metadata to Dagster context using AssetSummary.

    Generic helper for adding ingestion metadata that works for any ingestion type.
    For Bloomberg ingestion, use add_bloomberg_ingestion_summary_metadata instead.

    Args:
        series: Series dictionary (can be empty dict if series not found)
        series_id: Series ID
        series_code: Series code
        rows_inserted: Number of rows inserted
        target_date: Target date for ingestion
        context: Dagster execution context for logging and metadata
        status: Status string ("success", "skipped", "failed")
        reason: Optional reason for status
    """
    # Build series info dict
    series_info = {
        "series_id": series_id,
        "series_code": series_code,
        "rows": rows_inserted,
    }

    # Create summary based on status
    if status == "success":
        summary = AssetSummary.for_ingestion(
            matching_series=[series] if series else [],
            successful_series=[series_info],
            failed_series=[],
            total_rows_inserted=rows_inserted,
            target_date=target_date,
        )
    else:
        # For skipped/failed, add to failed_series with reason
        failed_info = {**series_info, "reason": reason or status}
        summary = AssetSummary.for_ingestion(
            matching_series=[series] if series else [],
            successful_series=[],
            failed_series=[failed_info],
            total_rows_inserted=0,
            target_date=target_date,
        )

    # Add summary metadata to context
    summary.add_to_context(context)


def handle_ingestion_failure(
    series_code: str,
    target_date: datetime,
    context: AssetExecutionContext,
    reason: str,
    series: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Handle ingestion failure with consistent error reporting.

    Args:
        series_code: Series code
        target_date: Target date for ingestion
        context: Dagster execution context
        reason: Failure reason
        series: Optional series dictionary (if available)

    Returns:
        Result dictionary with failure status
    """
    from dagster_quickstart.utils.helpers import create_ingestion_result_dict

    series_id = series.get("series_id") if series else 0
    add_ingestion_summary_metadata(
        series or {},
        series_id,
        series_code,
        0,
        target_date,
        context,
        "failed",
        reason,
    )
    return create_ingestion_result_dict(series_id, series_code, 0, "failed", reason)
