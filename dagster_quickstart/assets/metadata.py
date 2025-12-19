"""Metadata registration and management assets."""

from typing import Dict, Any
from datetime import datetime
from dagster import (
    asset,
    AssetExecutionContext,
    Config,
    MetadataValue,
)
from dagster_clickhouse.resources import ClickHouseResource
from database.lookup_tables import LookupTableManager
from database.meta_series import MetaSeriesManager
from database.models import (
    AssetClassLookup,
    ProductTypeLookup,
    FieldTypeLookup,
    TickerSourceLookup,
)


class MetadataConfig(Config):
    """Configuration for metadata registration."""

    operation: str  # "init_lookups" or "create_series"


@asset(
    group_name="metadata",
    description="Initialize lookup tables with default values",
)
def initialize_lookup_tables(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> Dict[str, int]:
    """Initialize lookup tables with standard values."""
    context.log.info("Initializing lookup tables")

    lookup_manager = LookupTableManager(clickhouse)
    results = {}

    # Initialize Asset Classes
    asset_classes = [
        AssetClassLookup(name="Equity", description="Equity securities"),
        AssetClassLookup(name="Fixed Income", description="Fixed income securities"),
        AssetClassLookup(name="Commodity", description="Commodities"),
        AssetClassLookup(name="Currency", description="Currency pairs"),
        AssetClassLookup(name="Derivative", description="Derivative instruments"),
    ]
    for ac in asset_classes:
        existing = lookup_manager.get_asset_class_by_name(ac.name)
        if not existing:
            ac_id = lookup_manager.insert_asset_class(ac)
            results[f"asset_class_{ac.name}"] = ac_id

    # Initialize Product Types
    product_types = [
        ProductTypeLookup(name="Stock", is_derived=False, description="Individual stocks"),
        ProductTypeLookup(name="Bond", is_derived=False, description="Bonds"),
        ProductTypeLookup(name="Index", is_derived=True, description="Indices"),
        ProductTypeLookup(name="Composite", is_derived=True, description="Composite series"),
    ]
    for pt in product_types:
        pt_id = lookup_manager.insert_product_type(pt)
        results[f"product_type_{pt.name}"] = pt_id

    # Initialize Field Types
    field_types = [
        FieldTypeLookup(name="Last Price", field_type_code="PX_LAST", description="Last traded price"),
        FieldTypeLookup(name="Open Interest", field_type_code="OPEN_INT", description="Open interest"),
        FieldTypeLookup(name="Open Price", field_type_code="PX_OPEN", description="Opening price"),
        FieldTypeLookup(name="High Price", field_type_code="PX_HIGH", description="High price"),
        FieldTypeLookup(name="Low Price", field_type_code="PX_LOW", description="Low price"),
        FieldTypeLookup(name="Volume", field_type_code="PX_VOLUME", description="Trading volume"),
    ]
    for ft in field_types:
        ft_id = lookup_manager.insert_field_type(ft)
        results[f"field_type_{ft.field_type_code}"] = ft_id

    # Initialize Ticker Sources
    ticker_sources = [
        TickerSourceLookup(name="Bloomberg", ticker_source_code="BLOOMBERG", description="Bloomberg data"),
        TickerSourceLookup(name="LSEG", ticker_source_code="LSEG", description="LSEG (Refinitiv) data"),
        TickerSourceLookup(name="Hawkeye", ticker_source_code="HAWKEYE", description="Hawkeye data"),
        TickerSourceLookup(name="Ramp", ticker_source_code="RAMP", description="Ramp data"),
        TickerSourceLookup(name="OneTick", ticker_source_code="ONETICK", description="OneTick data"),
    ]
    for ts in ticker_sources:
        ts_id = lookup_manager.insert_ticker_source(ts)
        results[f"ticker_source_{ts.ticker_source_code}"] = ts_id

    context.add_output_metadata(
        {
            "lookups_initialized": MetadataValue.int(len(results)),
            "details": MetadataValue.json(results),
        }
    )

    return results


@asset(
    group_name="metadata",
    description="Register a new meta series",
)
def register_meta_series(
    context: AssetExecutionContext,
    config: MetadataConfig,
    clickhouse: ClickHouseResource,
) -> int:
    """Register a new meta series."""
    # In a real implementation, config would contain series details
    # For now, this is a placeholder that would be called with proper config
    context.log.info("Registering meta series")

    # Example: This would typically come from config
    # meta_series = MetaSeriesCreate(
    #     series_name="Example Series",
    #     series_code="EXAMPLE_001",
    #     data_source=DataSource.RAW,
    #     field_type_id=1,
    #     asset_class_id=1,
    #     product_type_id=1,
    #     data_type_id=1,
    #     ticker_source_id=1,
    #     ticker="EXAMPLE",
    #     valid_from=datetime.now(),
    # )

    # meta_manager = MetaSeriesManager(clickhouse)
    # series_id = meta_manager.create_meta_series(meta_series, created_by="system")

    # For now, return a placeholder
    return 0

