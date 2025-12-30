"""Configuration for CSV-based loading."""

from dagster import Config


class LookupTableCSVConfig(Config):
    """Configuration for loading lookup tables from CSV."""

    csv_path: str = "data/lookup_tables.csv"  # Path to CSV file
    allowed_names_csv_path: str = "data/allowed_names.csv"  # Path to CSV with allowed names
    lookup_table_type: str = "all"  # Type: "all" (load all types), or specific: asset_class, product_type, sub_asset_class, data_type, structure_type, market_segment, field_type, ticker_source, region, currency, term, tenor, country


class MetaSeriesCSVConfig(Config):
    """Configuration for loading meta series from CSV."""

    csv_path: str = "data/meta_series.csv"  # Path to CSV file with meta series data
