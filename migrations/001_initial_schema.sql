-- Initial schema migration
-- This migration creates all the base tables for the financial platform

-- Lookup Tables
CREATE TABLE IF NOT EXISTS assetClassLookup (
    asset_class_id UInt32,
    asset_class_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (asset_class_id)
ORDER BY (asset_class_id);

CREATE TABLE IF NOT EXISTS productTypeLookup (
    product_type_id UInt32,
    product_type_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (product_type_id)
ORDER BY (product_type_id);

CREATE TABLE IF NOT EXISTS subAssetClassLookup (
    sub_asset_class_id UInt32,
    sub_asset_class_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (sub_asset_class_id)
ORDER BY (sub_asset_class_id);

CREATE TABLE IF NOT EXISTS dataTypeLookup (
    data_type_id UInt32,
    data_type_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (data_type_id)
ORDER BY (data_type_id);

CREATE TABLE IF NOT EXISTS structureTypeLookup (
    structure_type_id UInt32,
    structure_type_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (structure_type_id)
ORDER BY (structure_type_id);

CREATE TABLE IF NOT EXISTS marketSegmentLookup (
    market_segment_id UInt32,
    market_segment_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (market_segment_id)
ORDER BY (market_segment_id);

CREATE TABLE IF NOT EXISTS fieldTypeLookup (
    field_type_id UInt32,
    field_type_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (field_type_id)
ORDER BY (field_type_id);

CREATE TABLE IF NOT EXISTS tickerSourceLookup (
    ticker_source_id UInt32,
    ticker_source_name String,
    ticker_source_code String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (ticker_source_id)
ORDER BY (ticker_source_id);

CREATE TABLE IF NOT EXISTS regionLookup (
    region_id UInt32,
    region_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (region_id)
ORDER BY (region_id);

CREATE TABLE IF NOT EXISTS currencyLookup (
    currency_id UInt32,
    currency_code String,
    currency_name Nullable(String),
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (currency_id)
ORDER BY (currency_id);

CREATE TABLE IF NOT EXISTS termLookup (
    term_id UInt32,
    term_name String,
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (term_id)
ORDER BY (term_id);

CREATE TABLE IF NOT EXISTS tenorLookup (
    tenor_id UInt32,
    tenor_code String,
    tenor_name Nullable(String),
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (tenor_id)
ORDER BY (tenor_id);

CREATE TABLE IF NOT EXISTS countryLookup (
    country_id UInt32,
    country_code String,
    country_name Nullable(String),
    description Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (country_id)
ORDER BY (country_id);

-- Meta Series Table
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
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6),
    created_by Nullable(String),
    updated_by Nullable(String)
) ENGINE = MergeTree
PRIMARY KEY (series_id)
ORDER BY (series_id);

-- Dependency Graph
CREATE TABLE IF NOT EXISTS seriesDependencyGraph (
    dependency_id UInt64,
    parent_series_id UInt32,
    child_series_id UInt32,
    weight Nullable(Float64) DEFAULT 1.0,
    formula Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (dependency_id)
ORDER BY (dependency_id, parent_series_id, child_series_id);

-- Calculation Log
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
    created_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (calculation_id)
ORDER BY (calculation_id, series_id, created_at)
TTL created_at + INTERVAL 1 YEAR;

-- Value Data (Time-Series)
CREATE TABLE IF NOT EXISTS valueData (
    series_id UInt32,
    timestamp DateTime64(6),
    value Decimal(18, 6),
    created_at DateTime64(6) DEFAULT now64(6),
    updated_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (series_id, timestamp)
ORDER BY (series_id, timestamp)
PARTITION BY toYYYYMMDD(timestamp)
TTL timestamp + INTERVAL 10 YEAR;

