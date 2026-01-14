"""Configuration for Bloomberg data ingestion."""

from dagster import Config


class BloombergIngestionConfig(Config):
    """Configuration for Bloomberg data ingestion."""

    force_refresh: bool = False  # If True, delete existing data for the partition date before inserting (ensures idempotency when re-running a partition). If False, skip insertion if data already exists for the date.
    use_dummy_data: bool = True  # If True, use dummy ClickHouse data instead of real database queries. Useful for testing with PyPDL.
