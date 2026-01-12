"""Configuration for loading lookup tables from CSV."""

from dagster import Config

from dagster_quickstart.utils.constants import DEFAULT_CSV_PATHS


class LookupTableCSVConfig(Config):
    """Configuration for loading lookup tables from CSV."""

    csv_path: str = DEFAULT_CSV_PATHS["lookup_tables"]  # Path to CSV file
    allowed_names_csv_path: str = DEFAULT_CSV_PATHS[
        "allowed_names"
    ]  # Path to CSV with allowed names
    lookup_table_type: str = (
        "all"  # Type: "all" (load all types), or specific lookup type from LOOKUP_TABLE_COLUMNS
    )
