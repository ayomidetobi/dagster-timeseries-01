"""Data ingestion assets for multiple data sources."""

import polars as pl
from dagster import (
    AssetExecutionContext,
    Config,
    MetadataValue,
    asset,
    AssetKey,
)
from dagster_clickhouse.resources import ClickHouseResource
from database.meta_series import MetaSeriesManager

from dagster_quickstart.utils.helpers import (
    generate_date_range,
    get_or_validate_meta_series,
)
from dagster_quickstart.utils.exceptions import MetaSeriesNotFoundError


class IngestionConfig(Config):
    """Configuration for data ingestion."""

    source: str = "BLOOMBERG"
    ticker: str = "AAPL US Equity"
    series_code: str = "AAPL_PX_LAST"
    start_date: str = "2024-01-01"
    end_date: str = "2024-12-31"


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Bloomberg",
    deps=[AssetKey("load_meta_series_from_csv")],
    kinds=["csv","clickhouse"],
)
def ingest_bloomberg_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from Bloomberg source."""
    context.log.info(f"Ingesting Bloomberg data for ticker: {config.ticker}")

    # In a real implementation, this would connect to Bloomberg API
    # For now, we'll simulate with sample data
    dates = generate_date_range(config.start_date, config.end_date)
    
    sample_data = pl.DataFrame(
        {
            "timestamp": dates,
            "value": [100.0 + i * 0.1 for i in range(len(dates))],
        }
    )

    # Get or validate meta series
    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )
    
    if meta_series is None:
        raise MetaSeriesNotFoundError(f"Meta series {config.series_code} not found")

    series_id = meta_series["series_id"]

    # Add series_id to dataframe
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from LSEG",
    deps=[AssetKey("load_meta_series_from_csv")],  # Meta series must exist before ingestion
    kinds=["csv","clickhouse"],
)
def ingest_lseg_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from LSEG source."""
    context.log.info(f"Ingesting LSEG data for ticker: {config.ticker}")

    # In a real implementation, this would connect to LSEG API
    dates = generate_date_range(config.start_date, config.end_date)
    
    sample_data = pl.DataFrame(
        {
            "timestamp": dates,
            "value": [200.0 + i * 0.15 for i in range(len(dates))],
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    series_id = meta_series["series_id"]
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Hawkeye",
    deps=[AssetKey("load_meta_series_from_csv")],  # Meta series must exist before ingestion
    kinds=["csv","clickhouse"],
)
def ingest_hawkeye_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from Hawkeye source."""
    context.log.info(f"Ingesting Hawkeye data for ticker: {config.ticker}")

    dates = generate_date_range(config.start_date, config.end_date)
    
    sample_data = pl.DataFrame(
        {
            "timestamp": dates,
            "value": [150.0 + i * 0.12 for i in range(len(dates))],
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    series_id = meta_series["series_id"]
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from Ramp",
    deps=[AssetKey("load_meta_series_from_csv")],  # Meta series must exist before ingestion
    kinds=["csv","clickhouse"],
)
def ingest_ramp_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from Ramp source."""
    context.log.info(f"Ingesting Ramp data for ticker: {config.ticker}")

    dates = generate_date_range(config.start_date, config.end_date)
    
    sample_data = pl.DataFrame(
        {
            "timestamp": dates,
            "value": [175.0 + i * 0.13 for i in range(len(dates))],
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    series_id = meta_series["series_id"]
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])


@asset(
    group_name="ingestion",
    description="Ingest raw time-series data from OneTick",
    deps=[AssetKey("load_meta_series_from_csv")],  # Meta series must exist before ingestion
    kinds=["csv","clickhouse"],
)
def ingest_onetick_data(
    context: AssetExecutionContext,
    config: IngestionConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Ingest data from OneTick source."""
    context.log.info(f"Ingesting OneTick data for ticker: {config.ticker}")

    dates = generate_date_range(config.start_date, config.end_date)
    
    sample_data = pl.DataFrame(
        {
            "timestamp": dates,
            "value": [125.0 + i * 0.11 for i in range(len(dates))],
        }
    )

    meta_manager = MetaSeriesManager(clickhouse)
    meta_series = get_or_validate_meta_series(
        meta_manager, config.series_code, context, raise_if_not_found=True
    )

    series_id = meta_series["series_id"]
    sample_data = sample_data.with_columns(pl.lit(series_id).alias("series_id"))

    context.add_output_metadata(
        {
            "rows_ingested": MetadataValue.int(len(sample_data)),
            "series_id": MetadataValue.int(series_id),
            "date_range": MetadataValue.text(f"{config.start_date} to {config.end_date}"),
        }
    )

    return sample_data.select(["series_id", "timestamp", "value"])

