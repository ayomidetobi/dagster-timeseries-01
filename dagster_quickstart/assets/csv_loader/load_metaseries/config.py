"""Configuration for loading meta series from CSV."""

from dagster import Config

from dagster_quickstart.utils.constants import DEFAULT_CSV_PATHS


class MetaSeriesCSVConfig(Config):
    """Configuration for loading meta series from CSV."""

    csv_path: str = DEFAULT_CSV_PATHS["meta_series"]  # Path to CSV file with meta series data
