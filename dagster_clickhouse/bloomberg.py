"""Bloomberg data ingestion using xbbg library."""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster import ConfigurableResource, get_dagster_logger
from decouple import config
from xbbg import blp

logger = get_dagster_logger()


class BloombergResource(ConfigurableResource):
    """Bloomberg API resource using xbbg library.

    The xbbg library connects to Bloomberg automatically when you make API calls.
    It supports three connection modes:
    1. Desktop API (Bloomberg Terminal) - default, connects to local Bloomberg Terminal
    2. Server API - requires host and port configuration
    3. Cloud API - requires API key and other credentials

    Connection is established automatically on first API call, but you can test
    the connection using the test_connection() method.
    """

    # Optional configuration for Server API mode
    host: Optional[str] = None  # Bloomberg Server API host (e.g., 'localhost')
    port: Optional[int] = None  # Bloomberg Server API port (default: 8194)
    timeout: int = 30000  # Connection timeout in milliseconds

    def __init__(self, **data):
        """Initialize Bloomberg resource with optional configuration."""
        super().__init__(**data)
        # Try to get from environment variables if not provided
        if self.host is None:
            self.host = config("BLOOMBERG_HOST", default=None)
        if self.port is None:
            self.port = config("BLOOMBERG_PORT", default=None, cast=int)

    def test_connection(self) -> bool:
        """Test Bloomberg connection by making a simple API call.

        Returns:
            True if connection is successful, False otherwise
        """
        try:
            # Try to get a simple reference data point
            # Using a common ticker and field
            test_data = blp.bdp(
                tickers="AAPL US Equity",
                flds="NAME",
            )
            if test_data is not None and not test_data.empty:
                logger.info("Bloomberg connection successful")
                return True
            else:
                logger.warning("Bloomberg connection test returned empty data")
                return False
        except Exception as e:
            logger.error(f"Bloomberg connection test failed: {e}")
            logger.error(
                "Make sure Bloomberg Terminal is running (Desktop API) "
                "or Bloomberg Server API is accessible (Server API)"
            )
            return False

    def ensure_connection(self) -> None:
        """Ensure Bloomberg connection is established.

        This method tests the connection and logs the result.
        The actual connection happens automatically on first API call.
        """
        logger.info("Testing Bloomberg connection...")
        if not self.test_connection():
            raise ConnectionError(
                "Failed to connect to Bloomberg. "
                "Please ensure Bloomberg Terminal is running or "
                "Bloomberg Server API is accessible."
            )

    def get_bloomberg_data(
        self,
        tickers: List[str],
        flds: List[str],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        """Fetch historical data from Bloomberg.

        Args:
            tickers: List of Bloomberg tickers (e.g., ['AAPL US Equity'])
            flds: List of Bloomberg fields (e.g., ['PX_LAST', 'VOLUME'])
            start_date: Start date for historical data
            end_date: End date for historical data

        Returns:
            Dictionary with ticker as key and DataFrame as value
        """
        try:
            if start_date and end_date:
                # Historical data request
                data = blp.bdp(
                    tickers=tickers,
                    flds=flds,
                    start=start_date.strftime("%Y%m%d"),
                    end=end_date.strftime("%Y%m%d"),
                )
            else:
                # Current day data (default)
                today = datetime.now()
                data = blp.bdp(
                    tickers=tickers,
                    flds=flds,
                    start=today.strftime("%Y%m%d"),
                    end=today.strftime("%Y%m%d"),
                )
            return data
        except Exception as e:
            logger.error(f"Error fetching Bloomberg data: {e}")
            raise

    async def get_bloomberg_data_async(
        self,
        ticker: str,
        field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch historical data from Bloomberg asynchronously for a single ticker.

        Args:
            ticker: Bloomberg ticker (e.g., 'AAPL US Equity')
            field: Bloomberg field (e.g., 'PX_LAST')
            start_date: Start date for historical data
            end_date: End date for historical data

        Returns:
            Dictionary with timestamp and value, or None if error
        """
        try:
            # Run the blocking Bloomberg call in a thread pool
            loop = asyncio.get_event_loop()
            data = await loop.run_in_executor(
                None,
                self._fetch_single_ticker,
                ticker,
                field,
                start_date,
                end_date,
            )
            return data
        except Exception as e:
            logger.error(f"Error fetching Bloomberg data for {ticker} {field}: {e}")
            return None

    def _fetch_single_ticker(
        self,
        ticker: str,
        field: str,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch data for a single ticker (blocking call).

        **Bloomberg Connection:**
        The connection to Bloomberg is established automatically by xbbg when you
        call blp.bdh() or blp.bdp(). The connection happens HERE in this method
        when blp.bdh() is called for the first time.

        Connection modes:
        1. Desktop API (default): Connects to local Bloomberg Terminal
           - Requires Bloomberg Terminal to be running
           - No configuration needed
        2. Server API: Connects to Bloomberg Server API
           - Set BLOOMBERG_HOST and BLOOMBERG_PORT environment variables
           - Or configure host/port in BloombergResource
        3. Cloud API: Uses Bloomberg API credentials
           - Configure via environment variables or Bloomberg API settings
        """
        try:
            # CONNECTION HAPPENS HERE: blp.bdh() establishes connection automatically
            # Use bdh for historical data (bar data historical)
            if start_date and end_date:
                data = blp.bdh(
                    tickers=ticker,
                    flds=field,
                    start_date=start_date.strftime("%Y%m%d"),
                    end_date=end_date.strftime("%Y%m%d"),
                )
            else:
                # Current day data (default - one day)
                today = datetime.now()
                data = blp.bdh(
                    tickers=ticker,
                    flds=field,
                    start_date=today.strftime("%Y%m%d"),
                    end_date=today.strftime("%Y%m%d"),
                )

            if data is not None and not data.empty:
                # Convert to list of dicts with timestamp and value
                result = []
                # xbbg returns DataFrame with dates as index
                for date_idx in data.index:
                    # Get the value for this date
                    value = data.loc[date_idx, field] if field in data.columns else None
                    if value is not None and not (
                        isinstance(value, float) and (value != value)
                    ):  # Check for NaN
                        # Convert date index to datetime
                        if isinstance(date_idx, datetime):
                            timestamp = date_idx
                        else:
                            # Try to parse the date
                            try:
                                timestamp = datetime.strptime(str(date_idx), "%Y-%m-%d")
                            except:
                                timestamp = datetime.now()

                        result.append(
                            {
                                "timestamp": timestamp,
                                "value": float(value),
                            }
                        )
                return {"ticker": ticker, "field": field, "data": result}
            return None
        except Exception as e:
            logger.error(f"Error in _fetch_single_ticker for {ticker} {field}: {e}")
            return None

    async def fetch_multiple_series(
        self,
        series_list: List[Dict[str, Any]],
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        max_concurrent: int = 10,
    ) -> List[Dict[str, Any]]:
        """Fetch data for multiple series concurrently.

        Args:
            series_list: List of dicts with 'ticker' and 'field' keys
            start_date: Start date for historical data
            end_date: End date for historical data
            max_concurrent: Maximum number of concurrent requests

        Returns:
            List of results from get_bloomberg_data_async
        """
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(series: Dict[str, Any]) -> Optional[Dict[str, Any]]:
            async with semaphore:
                return await self.get_bloomberg_data_async(
                    ticker=series["ticker"],
                    field=series["field"],
                    start_date=start_date,
                    end_date=end_date,
                )

        tasks = [fetch_with_semaphore(series) for series in series_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out None and exceptions
        valid_results: List[Dict[str, Any]] = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Exception in fetch_multiple_series: {result}")
            elif result is not None and isinstance(result, dict):
                valid_results.append(result)

        return valid_results
