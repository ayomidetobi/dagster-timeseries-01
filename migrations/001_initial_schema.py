"""Initial schema migration.

This migration creates all the base tables for the financial platform.
"""

from clickhouse_migrate import Step

migrations = [
    # Lookup Tables
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS assetClassLookup (
            asset_class_id UInt32,
            asset_class_name String,
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (asset_class_id)
        ORDER BY (asset_class_id)
        """,
        description="Create assetClassLookup table",
    ),
    Step(
        sql="""
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
        """,
        description="Create productTypeLookup table",
    ),
    Step(
        sql="""
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
        """,
        description="Create subAssetClassLookup table",
    ),
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS dataTypeLookup (
            data_type_id UInt32,
            data_type_name String,
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (data_type_id)
        ORDER BY (data_type_id)
        """,
        description="Create dataTypeLookup table",
    ),
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS structureTypeLookup (
            structure_type_id UInt32,
            structure_type_name String,
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (structure_type_id)
        ORDER BY (structure_type_id)
        """,
        description="Create structureTypeLookup table",
    ),
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS marketSegmentLookup (
            market_segment_id UInt32,
            market_segment_name String,
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (market_segment_id)
        ORDER BY (market_segment_id)
        """,
        description="Create marketSegmentLookup table",
    ),
    Step(
        sql="""
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
        """,
        description="Create fieldTypeLookup table",
    ),
    Step(
        sql="""
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
        """,
        description="Create tickerSourceLookup table",
    ),
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS regionLookup (
            region_id UInt32,
            region_name String,
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (region_id)
        ORDER BY (region_id)
        """,
        description="Create regionLookup table",
    ),
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS currencyLookup (
            currency_id UInt32,
            currency_code String,
            currency_name Nullable(String),
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (currency_id)
        ORDER BY (currency_id)
        """,
        description="Create currencyLookup table",
    ),
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS termLookup (
            term_id UInt32,
            term_name String,
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (term_id)
        ORDER BY (term_id)
        """,
        description="Create termLookup table",
    ),
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS tenorLookup (
            tenor_id UInt32,
            tenor_code String,
            tenor_name Nullable(String),
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (tenor_id)
        ORDER BY (tenor_id)
        """,
        description="Create tenorLookup table",
    ),
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS countryLookup (
            country_id UInt32,
            country_code String,
            country_name Nullable(String),
            description Nullable(String),
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (country_id)
        ORDER BY (country_id)
        """,
        description="Create countryLookup table",
    ),
    # Meta Series Table
    Step(
        sql="""
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
            region_id Nullable(UInt32),
            currency_id Nullable(UInt32),
            term_id Nullable(UInt32),
            tenor_id Nullable(UInt32),
            country_id Nullable(UInt32),
            calculation_formula Nullable(String),
            description Nullable(String),
            is_active UInt8 DEFAULT 1,
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3),
            created_by Nullable(String),
            updated_by Nullable(String)
        ) ENGINE = MergeTree
        PRIMARY KEY (series_id)
        ORDER BY (series_id)
        """,
        description="Create metaSeries table",
    ),
    # Dependency Graph
    Step(
        sql="""
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
        """,
        description="Create seriesDependencyGraph table",
    ),
    # Calculation Log
    Step(
        sql="""
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
        """,
        description="Create calculationLog table",
    ),
    # Value Data (Time-Series)
    Step(
        sql="""
        CREATE TABLE IF NOT EXISTS valueData (
            series_id UInt32,
            timestamp DateTime64(6),
            value Float64,
            created_at DateTime64(3) DEFAULT now64(3),
            updated_at DateTime64(3) DEFAULT now64(3)
        ) ENGINE = MergeTree
        PRIMARY KEY (series_id, timestamp)
        ORDER BY (series_id, timestamp)
        PARTITION BY toYYYYMMDD(timestamp)
        TTL timestamp + INTERVAL 10 YEAR
        """,
        description="Create valueData table with daily partitioning for optimal query performance",
    ),
    # Indexes
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_meta_series_code ON metaSeries (series_code)",
        description="Create index on metaSeries.series_code",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_meta_series_ticker ON metaSeries (ticker, ticker_source_id)",
        description="Create index on metaSeries ticker",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_dep_parent ON seriesDependencyGraph (parent_series_id)",
        description="Create index on dependency parent",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_dep_child ON seriesDependencyGraph (child_series_id)",
        description="Create index on dependency child",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_calc_series ON calculationLog (series_id, created_at)",
        description="Create index on calculationLog series",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_calc_status ON calculationLog (status, created_at)",
        description="Create index on calculationLog status",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_region_name ON regionLookup (region_name)",
        description="Create index on regionLookup name",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_currency_code ON currencyLookup (currency_code)",
        description="Create index on currencyLookup code",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_term_name ON termLookup (term_name)",
        description="Create index on termLookup name",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_tenor_code ON tenorLookup (tenor_code)",
        description="Create index on tenorLookup code",
    ),
    Step(
        sql="CREATE INDEX IF NOT EXISTS idx_country_code ON countryLookup (country_code)",
        description="Create index on countryLookup code",
    ),
]
