"""Configuration for derived series calculation."""

from datetime import datetime
from typing import List, Optional, Tuple

from dagster import Config

from dagster_quickstart.utils.datetime_utils import parse_datetime_string


class CalculationConfig(Config):
    """Configuration for derived series calculation.

    Note: derived_series_code is now automatically discovered from dependencies,
    so it's no longer needed in config.
    """

    formula: str = (
        ""  # Optional formula override (usually determined from calc_type in dependencies)
    )
    input_series_codes: List[
        str
    ] = []  # List of input series codes (optional, usually from dependencies)

    # Optional date range for calculations (overrides partition date if provided)
    start_date: Optional[str] = None  # Start date in YYYY-MM-DD format
    end_date: Optional[str] = None  # End date in YYYY-MM-DD format (inclusive)

    def get_date_range(self, default_date: datetime) -> Tuple[datetime, datetime]:
        """Return the effective start and end dates for calculations.

        If start_date or end_date are not provided, default to the partition date.
        """
        if self.start_date:
            start = parse_datetime_string(self.start_date)
        else:
            start = default_date

        if self.end_date:
            end = parse_datetime_string(self.end_date)
        else:
            end = default_date

        return start, end
