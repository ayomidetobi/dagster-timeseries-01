"""PyPDL data ingestion resource using pyeqdr.pypdl library."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List

from dagster import ConfigurableResource, get_dagster_logger
from decouple import config

from dagster_quickstart.utils.constants import (
    PYPDL_DEFAULT_HOST,
    PYPDL_DEFAULT_MAX_CONCURRENT,
    PYPDL_DEFAULT_PORT,
    PYPDL_DEFAULT_USERNAME,
)
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
    It supports batch requests for multiple series with async execution.

    Retries and error handling are managed by Dagster's retry policies
    at the asset level, not within this resource.
    """

    # PyPDL connection configuration
    host: str = PYPDL_DEFAULT_HOST
    port: int = PYPDL_DEFAULT_PORT
    username: str = PYPDL_DEFAULT_USERNAME
    max_concurrent: int = PYPDL_DEFAULT_MAX_CONCURRENT

    def __init__(self, **data):
        """Initialize PyPDL resource with optional configuration."""
        super().__init__(**data)
        # Try to get from environment variables if not provided
        if self.host == PYPDL_DEFAULT_HOST:
            self.host = config("PYPDL_HOST", default=self.host)
        if self.port == PYPDL_DEFAULT_PORT:
            self.port = config("PYPDL_PORT", default=self.port, cast=int)
        if self.username == PYPDL_DEFAULT_USERNAME:
            self.username = config("PYPDL_USERNAME", default=self.username)
        if self.max_concurrent == PYPDL_DEFAULT_MAX_CONCURRENT:
            self.max_concurrent = config(
                "PYPDL_MAX_CONCURRENT", default=self.max_concurrent, cast=int
            )

    async def fetch_time_series_batch(
        self,
        series_list: List[Dict[str, Any]],
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict[str, Any]]:
        """Fetch time-series data for multiple series in batch with async execution.

        Args:
            series_list: List of dicts with keys:
                - data_code: The ticker/code (e.g., "FR_BNP", "AAPL US Equity")
                - data_source: The data source path (e.g., "bloomberg/ts/PX_LAST")
            start_date: Start date for data retrieval
            end_date: End date for data retrieval

        Returns:
            List of dicts with keys: data_code, data_source, data (list of {timestamp, value})

        Raises:
            PyPDLError: If PyPDL is not installed or execution fails
            PyPDLConnectionError: If connection to PyPDL server fails
            PyPDLExecutionError: If PyPDL execution fails
        """
        if pypdl is None:
            raise PyPDLError(
                "pyeqdr.pypdl is not installed. Please install it with: pip install pyeqdr"
            )

        # Split series_list into batches for concurrent execution
        batches = _split_into_batches(series_list, self.max_concurrent)
        logger.info(
            f"Processing {len(series_list)} series in {len(batches)} batches "
            f"(max_concurrent={self.max_concurrent})"
        )

        # Execute batches concurrently using thread pool for blocking PyPDL calls
        loop = asyncio.get_running_loop()
        with ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            tasks = [
                loop.run_in_executor(
                    executor,
                    self._fetch_batch_sync,
                    batch,
                    start_date,
                    end_date,
                    batch_idx,
                )
                for batch_idx, batch in enumerate(batches)
            ]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results and handle errors
        all_results: List[Dict[str, Any]] = []
        errors: List[Exception] = []

        for batch_idx, batch_result in enumerate(batch_results):
            if isinstance(batch_result, Exception):
                logger.error(
                    f"Batch {batch_idx} failed: {batch_result}",
                    extra={"batch_index": batch_idx, "batch_size": len(batches[batch_idx])},
                )
                errors.append(batch_result)
            elif isinstance(batch_result, list):
                all_results.extend(batch_result)
                logger.info(
                    f"Batch {batch_idx} succeeded: {len(batch_result)} series fetched",
                    extra={"batch_index": batch_idx, "series_count": len(batch_result)},
                )

        # If all batches failed, raise an exception
        if errors and not all_results:
            error_msg = f"All {len(batches)} PyPDL batches failed"
            logger.error(error_msg, extra={"error_count": len(errors)})
            raise PyPDLExecutionError(error_msg) from errors[0]

        # If some batches failed, log warning but continue with successful results
        if errors:
            logger.warning(
                f"{len(errors)} batches failed but {len(all_results)} series succeeded",
                extra={"failed_batches": len(errors), "successful_series": len(all_results)},
            )

        return all_results

    def _fetch_batch_sync(
        self,
        batch: List[Dict[str, Any]],
        start_date: datetime,
        end_date: datetime,
        batch_idx: int,
    ) -> List[Dict[str, Any]]:
        """Synchronously fetch a batch of series using PyPDL.

        This is a blocking method that will be run in a thread pool.

        Args:
            batch: List of series info dicts
            start_date: Start date for data retrieval
            end_date: End date for data retrieval
            batch_idx: Batch index for logging

        Returns:
            List of result dicts

        Raises:
            PyPDLConnectionError: If connection fails
            PyPDLExecutionError: If execution fails
        """
        try:
            pdl_dict = _build_pdl_dict(batch, start_date, end_date, batch_idx)
            program_name = f"r_batch_{batch_idx}"
            result_key = f"z_batch_{batch_idx}"

            logger.info(
                f"Executing PyPDL program for batch {batch_idx} ({len(batch)} series)",
                extra={"batch_index": batch_idx, "series_count": len(batch)},
            )

            out_d = pypdl.gnp_exec(
                pdl_dict,
                program_name,
                self.host,
                self.port,
                self.username,
            )

            return _process_pypdl_results(out_d, batch, batch_idx, result_key)

        except Exception as e:
            logger.error(
                f"PyPDL execution failed for batch {batch_idx}: {e}",
                extra={"batch_index": batch_idx, "error": str(e)},
            )
            raise PyPDLExecutionError(f"PyPDL execution failed for batch {batch_idx}") from e


def _split_into_batches(
    series_list: List[Dict[str, Any]], max_concurrent: int
) -> List[List[Dict[str, Any]]]:
    """Split series list into batches for concurrent processing.

    Args:
        series_list: List of series info dicts
        max_concurrent: Maximum number of concurrent batches

    Returns:
        List of batches
    """
    if not series_list:
        return []

    batch_size = max(1, len(series_list) // max_concurrent)
    return [series_list[i : i + batch_size] for i in range(0, len(series_list), batch_size)]


def _build_pdl_dict(
    batch: List[Dict[str, Any]],
    start_date: datetime,
    end_date: datetime,
    batch_idx: int,
) -> Dict[str, Any]:
    """Build PyPDL dictionary for a batch of series.

    Args:
        batch: List of series info dicts
        start_date: Start date for data retrieval
        end_date: End date for data retrieval
        batch_idx: Batch index

    Returns:
        PyPDL dictionary

    Raises:
        ValueError: If required fields are missing
    """
    pdl_dict: Dict[str, Any] = {}
    exec_list: List[str] = []

    date_1_str = start_date.strftime("%Y%m%d")
    date_2_str = end_date.strftime("%Y%m%d")

    for i, series_info in enumerate(batch):
        data_code = series_info.get("data_code")
        data_source = series_info.get("data_source")

        if not data_code or not data_source:
            logger.warning(
                f"Batch {batch_idx}: Skipping series {i}: missing data_code or data_source",
                extra={"batch_index": batch_idx, "series_index": i},
            )
            continue

        name = f"r{batch_idx}_{i}"
        result = f"z{batch_idx}_{i}"

        pypdl.pdl_make_object(
            pdl_dict,
            name,
            "py_get_time_serie",
            {
                "date_1": pypdl.to_date(date_1_str),
                "date_2": pypdl.to_date(date_2_str),
                "data_source": data_source,
                "data_code": data_code,
                "result": result,
            },
        )

        exec_list.append(name)

    if not exec_list:
        raise ValueError(f"Batch {batch_idx}: No valid series to fetch")

    # Create program object
    program_name = f"r_batch_{batch_idx}"
    pypdl.pdl_make_object(
        pdl_dict,
        program_name,
        "program",
        {
            "exec_list": exec_list,
            "result": f"z_batch_{batch_idx}",
        },
    )

    return pdl_dict


def _process_pypdl_results(
    out_d: Dict[str, Any],
    batch: List[Dict[str, Any]],
    batch_idx: int,
    result_key: str,
) -> List[Dict[str, Any]]:
    """Process PyPDL execution results and extract data points.

    Args:
        out_d: PyPDL output dictionary
        batch: Original batch of series info dicts
        batch_idx: Batch index
        result_key: Result key prefix

    Returns:
        List of result dicts with data points
    """
    # Process errors first
    error_index_list = out_d.get(f"{result_key}.error_index_list")
    valid_indices = _get_valid_indices(len(batch), error_index_list, batch_idx, out_d)

    # Build series mapping
    series_mapping = {
        f"z{batch_idx}_{i}": {
            "data_code": batch[i]["data_code"],
            "data_source": batch[i]["data_source"],
        }
        for i in valid_indices
    }

    # Process successful results
    results: List[Dict[str, Any]] = []
    for result_name in series_mapping.keys():
        series_info = series_mapping[result_name]
        data_code = series_info["data_code"]
        data_source = series_info["data_source"]

        date_list = out_d.get(f"{result_name}.date_list", [])
        value_list = out_d.get(f"{result_name}.value_list", [])

        if not date_list or not value_list:
            logger.warning(
                f"No data for {data_code} ({data_source})",
                extra={"data_code": data_code, "data_source": data_source},
            )
            continue

        data_points = _convert_to_data_points(date_list, value_list, data_code)
        if data_points:
            results.append(
                {
                    "data_code": data_code,
                    "data_source": data_source,
                    "data": data_points,
                }
            )
            logger.info(
                f"Fetched {len(data_points)} data points for {data_code}",
                extra={"data_code": data_code, "data_point_count": len(data_points)},
            )

    return results


def _get_valid_indices(
    batch_size: int, error_index_list: Any, batch_idx: int, out_d: Dict[str, Any]
) -> List[int]:
    """Get valid indices after removing error indices.

    Args:
        batch_size: Size of the batch
        error_index_list: List of error indices from PyPDL
        batch_idx: Batch index for logging
        out_d: PyPDL output dictionary for error details

    Returns:
        List of valid indices
    """
    if not error_index_list:
        return list(range(batch_size))

    error_indices = set(int(i) for i in error_index_list)
    result_key = f"z_batch_{batch_idx}"

    for error_idx in sorted(error_indices, reverse=True):
        if 0 <= error_idx - 1 < batch_size:
            error_stack_key = f"{result_key}.error_stack_{error_idx}"
            error_stack_list = out_d.get(error_stack_key, [])
            error_stack = (
                ", ".join(str(item) for item in error_stack_list) if error_stack_list else ""
            )
            logger.error(
                f"Batch {batch_idx}: Request {error_idx - 1} failed",
                extra={
                    "batch_index": batch_idx,
                    "error_index": error_idx - 1,
                    "error_stack": error_stack,
                },
            )

    return [i for i in range(batch_size) if i + 1 not in error_indices]


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
