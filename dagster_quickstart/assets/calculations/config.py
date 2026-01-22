"""Configuration for derived series calculation."""

from typing import List

from dagster import Config


class CalculationConfig(Config):
    """Configuration for derived series calculation.

    Note: derived_series_code is now automatically discovered from dependencies,
    so it's no longer needed in config.
    """

    formula: str = ""  # Optional formula override (usually determined from calc_type in dependencies)
    input_series_codes: List[str] = []  # List of input series codes (optional, usually from dependencies)
