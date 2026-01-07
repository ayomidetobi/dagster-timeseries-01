"""Dummy ClickHouse resource for testing Bloomberg ingestion.

This module provides a mock ClickHouse resource that returns dummy data
from test_dummy_data.py, allowing testing without a real database connection.
"""

from typing import Any, Dict, Optional

from dagster_quickstart.assets.bloomberg_ingestion.test_dummy_data import (
    DummyClickHouseResult,
    get_dummy_field_type,
    get_dummy_meta_series,
    get_dummy_ticker_source,
)


class DummyClickHouseResource:
    """Mock ClickHouse resource that returns dummy data for testing."""

    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None):
        """Mock execute_query that returns dummy data based on query.

        Args:
            query: SQL query string
            parameters: Query parameters

        Returns:
            DummyClickHouseResult with dummy data
        """
        params = parameters or {}

        # Handle meta series query by series_id
        if "series_id" in params or ("id" in params and "metaSeries" in query):
            series_id = params.get("series_id") or params.get("id")
            if series_id:
                series = get_dummy_meta_series(series_id)
                if series:
                    columns = list(series.keys())
                    rows = [tuple(series.values())]
                    return DummyClickHouseResult(rows, columns)

        # Handle field type lookup query
        if "id" in params and "fieldTypeLookup" in query:
            field_type_id = params["id"]
            field_type = get_dummy_field_type(field_type_id)
            if field_type:
                columns = list(field_type.keys())
                rows = [tuple(field_type.values())]
                return DummyClickHouseResult(rows, columns)

        # Handle ticker source lookup by name
        if "tickerSourceLookup" in query or "ticker_source" in query.lower():
            # Check if querying by name
            if "name" in params:
                # For now, return Bloomberg ticker source
                ticker_source = get_dummy_ticker_source(1)
            else:
                # Return Bloomberg ticker source
                ticker_source = get_dummy_ticker_source(1)

            if ticker_source:
                columns = list(ticker_source.keys())
                rows = [tuple(ticker_source.values())]
                return DummyClickHouseResult(rows, columns)

        # Handle latest timestamp query (return None to indicate no existing data)
        if "get_latest_timestamp" in query.lower() or "latest" in query.lower():
            return DummyClickHouseResult([], [])

        # Default: return empty result
        return DummyClickHouseResult([], [])

    def execute_command(self, query: str, parameters: Optional[Dict[str, Any]] = None):
        """Mock execute_command - no-op for testing.

        Args:
            query: SQL command string
            parameters: Command parameters
        """
        # In dummy mode, we don't actually insert data
        pass

    def get_connection(self):
        """Mock get_connection for compatibility."""
        return self
