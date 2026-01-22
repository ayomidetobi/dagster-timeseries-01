"""PyPDL helper functions for Bloomberg data ingestion."""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from dagster import AssetExecutionContext

from dagster_quickstart.utils.exceptions import PyPDLError

if TYPE_CHECKING:
    from dagster_quickstart.resources import PyPDLResource


def build_pypdl_request_params(
    field_name: str, ticker: str, target_date: datetime
) -> Tuple[str, str, datetime, datetime]:
    """Build PyPDL request parameters.

    Args:
        field_name: Field type name (should be Bloomberg field code like "PX_LAST")
        ticker: Ticker symbol
        target_date: Target date for ingestion

    Returns:
        Tuple of (data_source, data_code, start_date, end_date)
    """
    # PyPDL data_source format: "bloomberg/ts/{field_type_name}"
    # Note: field_type_name in the database should contain Bloomberg field codes (e.g., "PX_LAST")
    data_source = f"bloomberg/ts/{field_name}"
    data_code = ticker

    # Use partition date for both start and end date
    start_date = target_date
    end_date = target_date

    return data_source, data_code, start_date, end_date


def fetch_bloomberg_data(
    pypdl_resource: "PyPDLResource",  # type: ignore[name-defined]
    data_code: str,
    data_source: str,
    start_date: datetime,
    end_date: datetime,
    series_code: str,
    context: AssetExecutionContext,
    use_dummy_data: bool = False,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
    """Fetch data from Bloomberg via PyPDL.

    Args:
        pypdl_resource: PyPDL resource
        data_code: Data code (ticker)
        data_source: Data source path
        start_date: Start date for data fetch
        end_date: End date for data fetch
        series_code: Series code for logging
        context: Dagster execution context for logging
        use_dummy_data: If True, return dummy/random data instead of calling API

    Returns:
        Tuple of (data_points, error_reason)
        If error_reason is not None, data_points will be None
    """
    if use_dummy_data:
        import random

        context.log.info(
            "Fetching data from Bloomberg via PyPDL (DUMMY MODE - returning random data)",
            extra={
                "series_code": series_code,
                "data_code": data_code,
                "data_source": data_source,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
        )

        # Generate dummy data points with random values
        # Format: [{"timestamp": datetime, "value": float}, ...]
        
        data_points = []

        # Generate random value between 100 and 200
        value = round(random.uniform(100.0, 200.0), 6)
        # Use the target date as timestamp (or add small random offset)
        timestamp = start_date
        data_points.append(
            {
                "timestamp": timestamp,
                "value": value,
            }
        )

        context.log.info(
            "PyPDL fetch completed (DUMMY MODE)",
            extra={"data_point_count": len(data_points)},
        )
        return data_points, None

    # Real PyPDL API call
    # Import here to avoid circular import

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
        return data_points, None
    except PyPDLError as e:
        context.log.error(
            "PyPDL fetch failed",
            extra={"error": str(e), "series_code": series_code},
        )
        return None, str(e)
