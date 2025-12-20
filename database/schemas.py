"""ClickHouse table schema definitions for the financial platform."""

from typing import Dict, List


class ClickHouseSchema:
    """ClickHouse table schema definitions."""

    # Lookup Tables
    ASSET_CLASS_LOOKUP = """
    CREATE TABLE IF NOT EXISTS assetClassLookup (
        asset_class_id UInt32,
        asset_class_name String,
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (asset_class_id)
    ORDER BY (asset_class_id)
    """

    PRODUCT_TYPE_LOOKUP = """
    CREATE TABLE IF NOT EXISTS productTypeLookup (
        product_type_id UInt32,
        product_type_name String,
        is_derived UInt8 DEFAULT 0,
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (product_type_id)
    ORDER BY (product_type_id)
    """

    SUB_ASSET_CLASS_LOOKUP = """
    CREATE TABLE IF NOT EXISTS subAssetClassLookup (
        sub_asset_class_id UInt32,
        sub_asset_class_name String,
        asset_class_id UInt32,
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (sub_asset_class_id)
    ORDER BY (sub_asset_class_id, asset_class_id)
    """

    DATA_TYPE_LOOKUP = """
    CREATE TABLE IF NOT EXISTS dataTypeLookup (
        data_type_id UInt32,
        data_type_name String,
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (data_type_id)
    ORDER BY (data_type_id)
    """

    STRUCTURE_TYPE_LOOKUP = """
    CREATE TABLE IF NOT EXISTS structureTypeLookup (
        structure_type_id UInt32,
        structure_type_name String,
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (structure_type_id)
    ORDER BY (structure_type_id)
    """

    MARKET_SEGMENT_LOOKUP = """
    CREATE TABLE IF NOT EXISTS marketSegmentLookup (
        market_segment_id UInt32,
        market_segment_name String,
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (market_segment_id)
    ORDER BY (market_segment_id)
    """

    FIELD_TYPE_LOOKUP = """
    CREATE TABLE IF NOT EXISTS fieldTypeLookup (
        field_type_id UInt32,
        field_type_name String,
        field_type_code String,
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (field_type_id)
    ORDER BY (field_type_id)
    """

    TICKER_SOURCE_LOOKUP = """
    CREATE TABLE IF NOT EXISTS tickerSourceLookup (
        ticker_source_id UInt32,
        ticker_source_name String,
        ticker_source_code String,
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (ticker_source_id)
    ORDER BY (ticker_source_id)
    """

    # Meta Series Table
    META_SERIES = """
    CREATE TABLE IF NOT EXISTS metaSeries (
        series_id UInt32,
        series_name String,
        series_code String,
        data_source String,
        field_type_id Nullable(UInt32),
        asset_class_id Nullable(UInt32),
        sub_asset_class_id Nullable(UInt32),
        product_type_id Nullable(UInt32),
        data_type_id Nullable(UInt32),
        structure_type_id Nullable(UInt32),
        market_segment_id Nullable(UInt32),
        ticker_source_id Nullable(UInt32),
        ticker String,
        is_active UInt8 DEFAULT 1,
        is_latest UInt8 DEFAULT 1,
        version UInt32 DEFAULT 1,
        calculation_formula Nullable(String),
        data_quality_score Nullable(Float64),
        description Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3),
        created_by Nullable(String),
        updated_by Nullable(String)
    ) ENGINE = MergeTree
    PRIMARY KEY (series_id)
    ORDER BY (series_id, is_active, is_latest, version)
    """

    # Dependency Graph
    SERIES_DEPENDENCY_GRAPH = """
    CREATE TABLE IF NOT EXISTS seriesDependencyGraph (
        dependency_id UInt64,
        parent_series_id UInt32,
        child_series_id UInt32,
        weight Nullable(Float64) DEFAULT 1.0,
        formula Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (dependency_id)
    ORDER BY (dependency_id, parent_series_id, child_series_id)
    """

    # Calculation Log
    CALCULATION_LOG = """
    CREATE TABLE IF NOT EXISTS calculationLog (
        calculation_id UInt64,
        series_id UInt32,
        calculation_type String,
        status String,
        input_series_ids Array(UInt32),
        parameters String,
        formula String,
        rows_processed Nullable(UInt64),
        error_message Nullable(String),
        created_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (calculation_id)
    ORDER BY (calculation_id, series_id, created_at)
    TTL created_at + INTERVAL 1 YEAR
    """

    # Value Data (Time-Series)
    VALUE_DATA = """
    CREATE TABLE IF NOT EXISTS valueData (
        series_id UInt32,
        timestamp DateTime64(6),
        value Float64,
        created_at DateTime64(3) DEFAULT now64(3),
        updated_at DateTime64(3) DEFAULT now64(3)
    ) ENGINE = MergeTree
    PRIMARY KEY (series_id, timestamp)
    ORDER BY (series_id, timestamp)
    PARTITION BY toYYYYMM(timestamp)
    TTL timestamp + INTERVAL 10 YEAR
    """

    @classmethod
    def get_all_schemas(cls) -> Dict[str, str]:
        """Get all table schemas as a dictionary."""
        return {
            "assetClassLookup": cls.ASSET_CLASS_LOOKUP,
            "productTypeLookup": cls.PRODUCT_TYPE_LOOKUP,
            "subAssetClassLookup": cls.SUB_ASSET_CLASS_LOOKUP,
            "dataTypeLookup": cls.DATA_TYPE_LOOKUP,
            "structureTypeLookup": cls.STRUCTURE_TYPE_LOOKUP,
            "marketSegmentLookup": cls.MARKET_SEGMENT_LOOKUP,
            "fieldTypeLookup": cls.FIELD_TYPE_LOOKUP,
            "tickerSourceLookup": cls.TICKER_SOURCE_LOOKUP,
            "metaSeries": cls.META_SERIES,
            "seriesDependencyGraph": cls.SERIES_DEPENDENCY_GRAPH,
            "calculationLog": cls.CALCULATION_LOG,
            "valueData": cls.VALUE_DATA,
        }

    @classmethod
    def get_indexes(cls) -> List[str]:
        """Get additional index creation statements."""
        return [
            # Indexes for metaSeries
            "CREATE INDEX IF NOT EXISTS idx_meta_series_code ON metaSeries (series_code)",
            "CREATE INDEX IF NOT EXISTS idx_meta_series_active ON metaSeries (is_active, is_latest)",
            "CREATE INDEX IF NOT EXISTS idx_meta_series_ticker ON metaSeries (ticker, ticker_source_id)",
            # Indexes for dependency graph
            "CREATE INDEX IF NOT EXISTS idx_dep_parent ON seriesDependencyGraph (parent_series_id)",
            "CREATE INDEX IF NOT EXISTS idx_dep_child ON seriesDependencyGraph (child_series_id)",
            # Indexes for calculation log
            "CREATE INDEX IF NOT EXISTS idx_calc_series ON calculationLog (series_id, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_calc_status ON calculationLog (status, created_at)",
        ]
