"""Dummy ClickHouse responses for testing Bloomberg ingestion with PyPDL.

This module provides dummy data for 5 series from meta_series.csv to test
the Bloomberg ingestion logic without requiring a real ClickHouse connection.
"""

from datetime import datetime
from typing import Any, Dict, List

# Selected 5 series from meta_series.csv for testing
DUMMY_SERIES = {
    1: {
        "series_id": 1,
        "series_code": "AAPL_PX_LAST",
        "series_name": "AAPL Last Price",
        "ticker": "AAPL US Equity",
        "field_type_id": 1,  # "Last Price"
        "ticker_source_id": 1,  # Bloomberg
        "data_source": "RAW",
        "is_active": 1,
    },
    2: {
        "series_id": 2,
        "series_code": "MSFT_PX_LAST",
        "series_name": "MSFT Last Price",
        "ticker": "MSFT US Equity",
        "field_type_id": 1,  # "Last Price"
        "ticker_source_id": 1,  # Bloomberg
        "data_source": "RAW",
        "is_active": 1,
    },
    3: {
        "series_id": 3,
        "series_code": "GOLD_PX_LAST",
        "series_name": "Gold Spot Price",
        "ticker": "XAU Curncy",
        "field_type_id": 1,  # "Last Price"
        "ticker_source_id": 1,  # Bloomberg
        "data_source": "RAW",
        "is_active": 1,
    },
    4: {
        "series_id": 4,
        "series_code": "WTI_PX_LAST",
        "series_name": "WTI Crude Oil Price",
        "ticker": "CL1 Comdty",
        "field_type_id": 1,  # "Last Price"
        "ticker_source_id": 1,  # Bloomberg
        "data_source": "RAW",
        "is_active": 1,
    },
    5: {
        "series_id": 5,
        "series_code": "EURUSD_SPOT",
        "series_name": "EUR/USD Spot",
        "ticker": "EURUSD Curncy",
        "field_type_id": 1,  # "Last Price"
        "ticker_source_id": 1,  # Bloomberg
        "data_source": "RAW",
        "is_active": 1,
    },
}

# Dummy field type lookup - field_type_name should contain Bloomberg field code
DUMMY_FIELD_TYPES = {
    1: {
        "field_type_id": 1,
        "field_type_name": "PX_LAST",  # Bloomberg field code for Last Price
        "description": "Last Price",
    },
}

# Dummy ticker source lookup
DUMMY_TICKER_SOURCES = {
    1: {
        "ticker_source_id": 1,
        "ticker_source_name": "Bloomberg",
        "ticker_source_code": "BLOOMBERG",
        "description": "Bloomberg Terminal",
    },
}

# Dummy PyPDL responses - sample data points for each series
DUMMY_PYPDL_RESPONSES: Dict[str, List[Dict[str, Any]]] = {
    "AAPL US Equity": [
        {
            "timestamp": datetime(2024, 1, 15, 16, 0, 0),
            "value": 185.50,
        },
        {
            "timestamp": datetime(2024, 1, 16, 16, 0, 0),
            "value": 186.25,
        },
        {
            "timestamp": datetime(2024, 1, 17, 16, 0, 0),
            "value": 187.00,
        },
    ],
    "MSFT US Equity": [
        {
            "timestamp": datetime(2024, 1, 15, 16, 0, 0),
            "value": 380.75,
        },
        {
            "timestamp": datetime(2024, 1, 16, 16, 0, 0),
            "value": 382.50,
        },
        {
            "timestamp": datetime(2024, 1, 17, 16, 0, 0),
            "value": 384.25,
        },
    ],
    "XAU Curncy": [
        {
            "timestamp": datetime(2024, 1, 15, 16, 0, 0),
            "value": 2025.50,
        },
        {
            "timestamp": datetime(2024, 1, 16, 16, 0, 0),
            "value": 2030.75,
        },
        {
            "timestamp": datetime(2024, 1, 17, 16, 0, 0),
            "value": 2035.00,
        },
    ],
    "CL1 Comdty": [
        {
            "timestamp": datetime(2024, 1, 15, 16, 0, 0),
            "value": 72.50,
        },
        {
            "timestamp": datetime(2024, 1, 16, 16, 0, 0),
            "value": 73.25,
        },
        {
            "timestamp": datetime(2024, 1, 17, 16, 0, 0),
            "value": 74.00,
        },
    ],
    "EURUSD Curncy": [
        {
            "timestamp": datetime(2024, 1, 15, 16, 0, 0),
            "value": 1.0850,
        },
        {
            "timestamp": datetime(2024, 1, 16, 16, 0, 0),
            "value": 1.0865,
        },
        {
            "timestamp": datetime(2024, 1, 17, 16, 0, 0),
            "value": 1.0880,
        },
    ],
}


class DummyClickHouseResult:
    """Mock ClickHouse query result for testing."""

    def __init__(self, rows: List[tuple], columns: List[str]):
        """Initialize with rows and columns.

        Args:
            rows: List of tuples representing result rows
            columns: List of column names
        """
        self.result_rows = rows
        self.column_names = columns


def get_dummy_meta_series(series_id: int) -> Dict[str, Any]:
    """Get dummy meta series data for a given series_id.

    Args:
        series_id: Series ID (1-5)

    Returns:
        Dictionary with meta series data
    """
    return DUMMY_SERIES.get(series_id, {})


def get_dummy_field_type(field_type_id: int) -> Dict[str, Any]:
    """Get dummy field type data for a given field_type_id.

    Args:
        field_type_id: Field type ID

    Returns:
        Dictionary with field type data
    """
    return DUMMY_FIELD_TYPES.get(field_type_id, {})


def get_dummy_ticker_source(ticker_source_id: int) -> Dict[str, Any]:
    """Get dummy ticker source data for a given ticker_source_id.

    Args:
        ticker_source_id: Ticker source ID

    Returns:
        Dictionary with ticker source data
    """
    return DUMMY_TICKER_SOURCES.get(ticker_source_id, {})


def get_dummy_pypdl_response(ticker: str) -> List[Dict[str, Any]]:
    """Get dummy PyPDL response data for a given ticker.

    Args:
        ticker: Ticker symbol (e.g., "AAPL US Equity")

    Returns:
        List of data point dictionaries with timestamp and value
    """
    return DUMMY_PYPDL_RESPONSES.get(ticker, [])


def create_dummy_clickhouse_result(data: Dict[str, Any]) -> DummyClickHouseResult:
    """Create a dummy ClickHouse result from a dictionary.

    Args:
        data: Dictionary with data (single row)

    Returns:
        DummyClickHouseResult object
    """
    if not data:
        return DummyClickHouseResult([], [])

    columns = list(data.keys())
    rows = [tuple(data.values())]
    return DummyClickHouseResult(rows, columns)


def create_dummy_clickhouse_result_list(data_list: List[Dict[str, Any]]) -> DummyClickHouseResult:
    """Create a dummy ClickHouse result from a list of dictionaries.

    Args:
        data_list: List of dictionaries (multiple rows)

    Returns:
        DummyClickHouseResult object
    """
    if not data_list:
        return DummyClickHouseResult([], [])

    columns = list(data_list[0].keys())
    rows = [tuple(row.values()) for row in data_list]
    return DummyClickHouseResult(rows, columns)


# Example usage for testing:
# series = get_dummy_meta_series(1)  # Returns AAPL_PX_LAST data
# field_type = get_dummy_field_type(1)  # Returns PX_LAST field type
# pypdl_data = get_dummy_pypdl_response("AAPL US Equity")  # Returns sample price data
