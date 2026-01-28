"""PyPDL helper functions for Bloomberg data ingestion."""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from dagster import AssetExecutionContext

from dagster_quickstart.utils.exceptions import PyPDLError

if TYPE_CHECKING:
    from dagster_quickstart.resources import PyPDLResource


def build_pypdl_request_params(
    field_name: str,
    ticker: Union[str, List[str]],
    start_date: datetime,
    end_date: datetime,
) -> Tuple[str, Union[str, List[str]], datetime, datetime]:
    """Build PyPDL request parameters for single or multiple tickers.

    Automatically handles both single ticker (str) and multiple tickers (List[str]).

    Args:
        field_name: Field type name (should be Bloomberg field code like "PX_LAST")
        ticker: Ticker symbol (str) or list of ticker symbols (List[str])
        start_date: Start date for ingestion
        end_date: End date for ingestion

    Returns:
        Tuple of (data_source, data_code(s), start_date, end_date)
        For single: data_code is str
        For multiple: data_code is List[str]
    """
    # PyPDL data_source format: "bloomberg/ts/{field_type_name}"
    # Note: field_type_name in the database should contain Bloomberg field codes (e.g., "PX_LAST")
    data_source = f"bloomberg/ts/{field_name}"

    if isinstance(ticker, str):
        data_code = ticker
    else:
        data_code = list(ticker)

    return data_source, data_code, start_date, end_date


def fetch_bloomberg_data(
    pypdl_resource: "PyPDLResource",  # type: ignore[name-defined]
    data_code: Union[str, List[str]],
    data_source: str,
    start_date: datetime,
    end_date: datetime,
    series_code: Optional[Union[str, List[str]]] = None,
    context: Optional[AssetExecutionContext] = None,
    use_dummy_data: bool = False,
) -> Tuple[Optional[Union[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]], Optional[str]]:
    """Fetch data from Bloomberg via PyPDL for single or multiple series.

    Automatically detects if data_code is a single string or a list and handles accordingly.
    For single series: returns (List[Dict[str, Any]], error_reason)
    For multiple series: returns (Dict[str, List[Dict[str, Any]]], error_reason)

    Args:
        pypdl_resource: PyPDL resource
        data_code: Data code (ticker) as str or list of tickers as List[str]
        data_source: Data source path
        start_date: Start date for data fetch
        end_date: End date for data fetch
        series_code: Series code(s) for logging (str, List[str], or None)
        context: Dagster execution context for logging (optional)
        use_dummy_data: If True, return dummy/random data instead of calling API

    Returns:
        Tuple of (data_points, error_reason)
        For single: data_points is List[Dict[str, Any]]
        For multiple: data_points is Dict[str, List[Dict[str, Any]]]
        If error_reason is not None, data_points will be None
    """
    is_single = isinstance(data_code, str)

    if use_dummy_data:
        import random

        if is_single:
            log_series_code = series_code if isinstance(series_code, str) else str(series_code)
            if context:
                context.log.info(
                    "Fetching data from Bloomberg via PyPDL (DUMMY MODE - returning random data)",
                    extra={
                        "series_code": log_series_code,
                        "data_code": data_code,
                        "data_source": data_source,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )

            # Generate dummy data points with random values
            value = round(random.uniform(100.0, 200.0), 6)
            timestamp = start_date
            data_points = [{"timestamp": timestamp, "value": value}]

            if context:
                context.log.info(
                    "PyPDL fetch completed (DUMMY MODE)",
                    extra={"data_point_count": len(data_points)},
                )
            return data_points, None
        else:
            # Multiple series
            data_codes = list(data_code)
            series_codes_list = list(series_code) if isinstance(series_code, list) else None

            if context:
                context.log.info(
                    "Fetching data from Bloomberg via PyPDL (DUMMY MODE - bulk)",
                    extra={
                        "series_codes": series_codes_list,
                        "data_codes": data_codes,
                        "data_source": data_source,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )

            data_by_code: Dict[str, List[Dict[str, Any]]] = {}
            for code in data_codes:
                value = round(random.uniform(100.0, 200.0), 6)
                timestamp = start_date
                data_by_code[code] = [{"timestamp": timestamp, "value": value}]

            if context:
                context.log.info(
                    "PyPDL bulk fetch completed (DUMMY MODE)",
                    extra={"data_point_counts": {k: len(v) for k, v in data_by_code.items()}},
                )
            return data_by_code, None

    # Real PyPDL API call
    try:
        if is_single:
            log_series_code = (
                series_code
                if isinstance(series_code, str)
                else str(series_code)
                if series_code
                else "unknown"
            )
            if context:
                context.log.info(
                    "Fetching data from Bloomberg via PyPDL",
                    extra={
                        "series_code": log_series_code,
                        "data_code": data_code,
                        "data_source": data_source,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )

            data_points = pypdl_resource.fetch_time_series(
                data_code=data_code,
                data_source=data_source,
                start_date=start_date,
                end_date=end_date,
            )

            if context:
                context.log.info(
                    "PyPDL fetch completed",
                    extra={"data_point_count": len(data_points)},
                )
            return data_points, None
        else:
            # Multiple series
            data_codes = list(data_code)
            series_codes_list = list(series_code) if isinstance(series_code, list) else None

            if context:
                context.log.info(
                    "Fetching data from Bloomberg via PyPDL (bulk)",
                    extra={
                        "series_codes": series_codes_list,
                        "data_codes": data_codes,
                        "data_source": data_source,
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                )

            results = pypdl_resource.fetch_time_series(
                data_code=data_codes,
                data_source=data_source,
                start_date=start_date,
                end_date=end_date,
            )

            if context:
                context.log.info(
                    "PyPDL bulk fetch completed",
                    extra={
                        "data_point_counts": {code: len(points) for code, points in results.items()}
                    },
                )
            return results, None
    except PyPDLError as e:
        if context:
            if is_single:
                log_series_code = (
                    series_code
                    if isinstance(series_code, str)
                    else str(series_code)
                    if series_code
                    else "unknown"
                )
                context.log.error(
                    "PyPDL fetch failed",
                    extra={"error": str(e), "series_code": log_series_code},
                )
            else:
                context.log.error(
                    "PyPDL bulk fetch failed",
                    extra={"error": str(e), "data_codes": list(data_code)},
                )
        return None, str(e)
