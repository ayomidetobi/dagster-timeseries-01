"""Configuration for loading series dependencies from CSV."""

from dagster import Config

from dagster_quickstart.utils.constants import DEFAULT_CSV_PATHS


class SeriesDependencyCSVConfig(Config):
    """Configuration for loading series dependencies from CSV."""

    csv_path: str = DEFAULT_CSV_PATHS[
        "series_dependencies"
    ]  # Path to CSV file with series dependencies data
