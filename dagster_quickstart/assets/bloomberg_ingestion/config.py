"""Configuration for Bloomberg data ingestion."""

from datetime import datetime
from typing import List

from dagster import Config

from dagster_quickstart.utils.datetime_utils import parse_datetime_string


class BloombergIngestionConfig(Config):
    """Configuration for Bloomberg data ingestion."""

    mode: str = (
        "daily"  # "daily" uses series_code from partition, "bulk" uses series_codes from config
    )
    force_refresh: bool = True  # If True, delete existing data for the partition date before inserting (ensures idempotency when re-running a partition). If False, skip insertion if data already exists for the date.
    use_dummy_data: bool = True  # If True, use dummy  data instead of real database queries. Useful for testing with PyPDL.
    series_codes: List[str] = [
        "AAPL_PX_LAST",
        "MSFT_PX_LAST",
        "GOLD_PX_LAST",
        "WTI_PX_LAST",
        "EURUSD_SPOT",
    ]  # List of series codes for bulk ingestion (used when mode="bulk")


class BloombergBulkIngestionConfig(Config):
    """Configuration for bulk Bloomberg data ingestion (date range in config)."""

    force_refresh: bool = True  # If True, delete existing data for the partition date before inserting (ensures idempotency when re-running). If False, skip insertion if data already exists for the date.
    use_dummy_data: bool = True  # If True, use dummy  data instead of real database queries. Useful for testing with PyPDL.
    start_date: str  # Start date for bulk ingestion in YYYY-MM-DD format
    end_date: str  # End date for bulk ingestion in YYYY-MM-DD format (inclusive)

    def get_start_date(self) -> datetime:
        """Get start date as datetime object."""
        return parse_datetime_string(self.start_date).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

    def get_end_date(self) -> datetime:
        """Get end date as datetime object."""
        return parse_datetime_string(self.end_date).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
