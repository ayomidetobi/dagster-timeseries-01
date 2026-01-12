"""PyPDL data ingestion resource using pyeqdr.pypdl library."""

from datetime import datetime
from typing import Any, Dict, List

from dagster import ConfigurableResource, get_dagster_logger

from dagster_quickstart.utils.datetime_utils import parse_timestamp
from dagster_quickstart.utils.exceptions import PyPDLError, PyPDLExecutionError

try:
    import pyeqdr.pypdl as pypdl
except ImportError:
    pypdl = None  # type: ignore

logger = get_dagster_logger()


class PyPDLResource(ConfigurableResource):
    """PyPDL API resource using pyeqdr.pypdl library.

    This resource uses PyPDL to fetch time-series data from Bloomberg.
    Dagster handles concurrency through dynamic partitions.

    Retries and error handling are managed by Dagster's retry policies
    at the asset level, not within this resource.
    """

    def fetch_time_series(
        self,
        data_code: str,
        data_source: str,
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """Fetch time-series data for a single series.

        Args:
            data_code: The ticker/code (e.g., "FR_BNP", "AAPL US Equity")
            data_source: The data source path (e.g., "bloomberg/ts/PX_LAST")
            start_date: Start date for data retrieval
            end_date: End date for data retrieval

        Returns:
            List of data point dicts with timestamp and value

        Raises:
            PyPDLError: If PyPDL is not installed or execution fails
            PyPDLExecutionError: If PyPDL execution fails
        """
        if pypdl is None:
            raise PyPDLError(
                "pyeqdr.pypdl is not installed. Please install it with: pip install pyeqdr"
            )

        try:
            pdl_dict = _build_pdl_dict_single(data_code, data_source, start_date, end_date)
            program_name = "r_series"
            result_key = "z_series"

            logger.info(
                f"Executing PyPDL program for {data_code} ({data_source})",
                extra={"data_code": data_code, "data_source": data_source},
            )

            out_d = pypdl.gnp_exec(
                pdl_dict,
                program_name,
                self.host,
                self.port,
                self.username,
            )

            return _process_pypdl_result_single(out_d, data_code, data_source, result_key)

        except Exception as e:
            logger.error(
                f"PyPDL execution failed for {data_code}: {e}",
                extra={"data_code": data_code, "data_source": data_source, "error": str(e)},
            )
            raise PyPDLExecutionError(f"PyPDL execution failed for {data_code}") from e


def _build_pdl_dict_single(
    data_code: str,
    data_source: str,
    start_date: datetime,
    end_date: datetime,
) -> Dict[str, Any]:
    """Build PyPDL dictionary for a single series.

    Args:
        data_code: The ticker/code
        data_source: The data source path
        start_date: Start date for data retrieval
        end_date: End date for data retrieval

    Returns:
        PyPDL dictionary
    """
    pdl_dict: Dict[str, Any] = {}

    date_1_str = start_date.strftime("%Y%m%d")
    date_2_str = end_date.strftime("%Y%m%d")

    # Create single series object
    pypdl.pdl_make_object(
        pdl_dict,
        "r_series",
        "py_get_time_serie",
        {
            "date_1": pypdl.to_date(date_1_str),
            "date_2": pypdl.to_date(date_2_str),
            "data_source": data_source,
            "data_code": data_code,
            "result": "z_series",
        },
    )

    return pdl_dict


def _process_pypdl_result_single(
    out_d: Dict[str, Any],
    data_code: str,
    data_source: str,
    result_key: str,
) -> List[Dict[str, Any]]:
    """Process PyPDL execution result and extract data points for a single series.

    Args:
        out_d: PyPDL output dictionary
        data_code: Data code for the series
        data_source: Data source for the series
        result_key: Result key prefix

    Returns:
        List of data point dicts with timestamp and value
    """
    # Check for errors
    error_index_list = out_d.get(f"{result_key}.error_index_list")
    if error_index_list:
        error_stack_key = f"{result_key}.error_stack_1"
        error_stack_list = out_d.get(error_stack_key, [])
        error_stack = ", ".join(str(item) for item in error_stack_list) if error_stack_list else ""
        logger.error(
            f"PyPDL request failed for {data_code}",
            extra={"data_code": data_code, "data_source": data_source, "error_stack": error_stack},
        )
        raise PyPDLExecutionError(f"PyPDL request failed for {data_code}: {error_stack}")

    # Get data
    date_list = out_d.get(f"{result_key}.date_list", [])
    value_list = out_d.get(f"{result_key}.value_list", [])

    if not date_list or not value_list:
        logger.warning(
            f"No data for {data_code} ({data_source})",
            extra={"data_code": data_code, "data_source": data_source},
        )
        return []

    data_points = _convert_to_data_points(date_list, value_list, data_code)
    if data_points:
        logger.info(
            f"Fetched {len(data_points)} data points for {data_code}",
            extra={"data_code": data_code, "data_point_count": len(data_points)},
        )

    return data_points


def _convert_to_data_points(
    date_list: List[Any], value_list: List[Any], data_code: str
) -> List[Dict[str, Any]]:
    """Convert PyPDL date and value lists to data points.

    Args:
        date_list: List of dates from PyPDL
        value_list: List of values from PyPDL
        data_code: Data code for logging

    Returns:
        List of data point dicts with timestamp and value
    """
    data_points: List[Dict[str, Any]] = []

    for dt, spot in zip(date_list, value_list):
        timestamp = _parse_pypdl_date(dt)
        if timestamp is None:
            continue

        try:
            value = float(spot)
            if value != value:  # Check for NaN
                continue
            data_points.append({"timestamp": timestamp, "value": value})
        except (ValueError, TypeError):
            logger.warning(
                f"Could not convert value to float for {data_code}: {spot}",
                extra={"data_code": data_code, "value": str(spot)},
            )
            continue

    return data_points


def _parse_pypdl_date(dt: Any) -> datetime | None:
    """Parse PyPDL date to UTC timezone-aware datetime.

    Uses dateutil.parser.parse for robust parsing and normalizes to UTC
    with DateTime64(6) precision (microseconds).

    Args:
        dt: Date from PyPDL (int, datetime, or str)

    Returns:
        UTC timezone-aware datetime normalized to microsecond precision, or None if parsing fails
    """
    # Try using the centralized parse_timestamp utility which handles:
    # - datetime objects (normalizes to UTC)
    # - int in YYYYMMDD format
    # - strings (uses dateutil.parser.parse for robust parsing)
    parsed = parse_timestamp(dt)
    if parsed is not None:
        return parsed

    # Fallback: Try using pypdl.date_to_string for int dates
    if isinstance(dt, int):
        try:
            date_str = pypdl.date_to_string(dt, short=1)
            parsed = parse_timestamp(date_str)
            if parsed is not None:
                return parsed
        except Exception:
            pass

    logger.warning(
        f"Could not parse PyPDL date: {dt}",
        extra={"date_value": str(dt), "date_type": type(dt).__name__},
    )
    return None
