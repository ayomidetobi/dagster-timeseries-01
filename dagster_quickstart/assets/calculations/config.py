"""Configuration for derived series calculation."""

from typing import List

from dagster import Config


class CalculationConfig(Config):
    """Configuration for derived series calculation."""

    derived_series_code: str = (
        "TECH_COMPOSITE"  # Must match series_code in meta_series.csv, not series_name
    )
    formula: str = "parent1 * 0.5 + parent2 * 0.5"  # e.g., "parent1 * 0.6 + parent2 * 0.4"
    input_series_codes: List[str] = []  # List of input series codes
