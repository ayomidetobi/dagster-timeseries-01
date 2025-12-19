"""Data ingestion assets for multiple data sources."""

import pandas as pd
from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MetadataValue,
)
from dagster_clickhouse.resources import ClickHouseResource
from database.meta_series import MetaSeriesManager


class IngestionConfig(Config):
    """Configuration for data ingestion."""

    source: str
    ticker: str
    series_code: str
    start_date: str
    end_date: str


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Bloomberg",
)
def ingest_bloomberg_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pd.DataFrame:
    """Ingest data from Bloomberg source."""
    context.log.info(f"Ingesting Bloomberg data for ticker: {config.ticker}")

    # In a real implementation, this would connect to Bloomberg API
    # For now, we'll simulate with sample data
    dates = pd.date_range(config.start_date, config.end_date, freq="D")
    sample_data = pd.DataFrame(
        {
            "timestamp": dates,
            "value": [100.0 + i * 0.1 for i in range(len(dates))],
        }
    )

    # Get or create meta series
    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = meta_manager.get_meta_series_by_code(config.series_code)

    if not meta_series:
        context.log.warning(f"Meta series {config.series_code} not found. Creating...")
        # In real implementation, would create meta series first
        # For now, assume it exists
        raise ValueError(f"Meta series {config.series_code} must exist before ingestion")

    series_id = meta_series["series_id"]

    # Add series_id to dataframe
    sample_data["series_id"] = series_id

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data[["series_id", "timestamp", "value"]]


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from LSEG",
)
def ingest_lseg_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pd.DataFrame:
    """Ingest data from LSEG source."""
    context.log.info(f"Ingesting LSEG data for ticker: {config.ticker}")

    # In a real implementation, this would connect to LSEG API
    dates = pd.date_range(config.start_date, config.end_date, freq="D")
    sample_data = pd.DataFrame(
        {
            "timestamp": dates,
            "value": [200.0 + i * 0.15 for i in range(len(dates))],
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = meta_manager.get_meta_series_by_code(config.series_code)

    if not meta_series:
        raise ValueError(f"Meta series {config.series_code} must exist before ingestion")

    series_id = meta_series["series_id"]
    sample_data["series_id"] = series_id

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data[["series_id", "timestamp", "value"]]


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Hawkeye",
)
def ingest_hawkeye_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pd.DataFrame:
    """Ingest data from Hawkeye source."""
    context.log.info(f"Ingesting Hawkeye data for ticker: {config.ticker}")

    dates = pd.date_range(config.start_date, config.end_date, freq="D")
    sample_data = pd.DataFrame(
        {
            "timestamp": dates,
            "value": [150.0 + i * 0.12 for i in range(len(dates))],
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = meta_manager.get_meta_series_by_code(config.series_code)

    if not meta_series:
        raise ValueError(f"Meta series {config.series_code} must exist before ingestion")

    series_id = meta_series["series_id"]
    sample_data["series_id"] = series_id

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data[["series_id", "timestamp", "value"]]


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Ramp",
)
def ingest_ramp_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pd.DataFrame:
    """Ingest data from Ramp source."""
    context.log.info(f"Ingesting Ramp data for ticker: {config.ticker}")

    dates = pd.date_range(config.start_date, config.end_date, freq="D")
    sample_data = pd.DataFrame(
        {
            "timestamp": dates,
            "value": [175.0 + i * 0.13 for i in range(len(dates))],
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = meta_manager.get_meta_series_by_code(config.series_code)

    if not meta_series:
        raise ValueError(f"Meta series {config.series_code} must exist before ingestion")

    series_id = meta_series["series_id"]
    sample_data["series_id"] = series_id

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data[["series_id", "timestamp", "value"]]


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from OneTick",
)
def ingest_onetick_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pd.DataFrame:
    """Ingest data from OneTick source."""
    context.log.info(f"Ingesting OneTick data for ticker: {config.ticker}")

    dates = pd.date_range(config.start_date, config.end_date, freq="D")
    sample_data = pd.DataFrame(
        {
            "timestamp": dates,
            "value": [125.0 + i * 0.11 for i in range(len(dates))],
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = meta_manager.get_meta_series_by_code(config.series_code)

    if not meta_series:
        raise ValueError(f"Meta series {config.series_code} must exist before ingestion")

    series_id = meta_series["series_id"]
    sample_data["series_id"] = series_id

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data[["series_id", "timestamp", "value"]]

