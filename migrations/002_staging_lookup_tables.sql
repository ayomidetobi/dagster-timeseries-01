-- Staging table for CSV ingestion of lookup table data
-- This table accepts denormalized CSV data containing all lookup domain columns
-- Data flows: lookup_csv -> staging_lookup_tables -> dimension_tables -> dictionaries

CREATE TABLE IF NOT EXISTS staging_lookup_tables (
    asset_class Nullable(String),
    product_type Nullable(String),
    sub_asset_class Nullable(String),
    data_type Nullable(String),
    structure_type Nullable(String),
    market_segment Nullable(String),
    field_type Nullable(String),
    ticker_source Nullable(String),
    country Nullable(String),
    currency Nullable(String),
    region Nullable(String),
    term Nullable(String),
    tenor Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (created_at)
ORDER BY (created_at)
TTL created_at + INTERVAL 30 DAY;

-- Comments for documentation
ALTER TABLE staging_lookup_tables COMMENT COLUMN asset_class 'Asset class value from CSV (e.g., Equity, Fixed Income)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN product_type 'Product type value from CSV (e.g., Stock, Bond, Index)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN sub_asset_class 'Sub-asset class value from CSV (e.g., Common Stock, Corporate Bond)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN data_type 'Data type value from CSV (e.g., Price, Volume, Yield)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN structure_type 'Structure type value from CSV (e.g., Simple, Complex, Derivative)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN market_segment 'Market segment value from CSV (e.g., Primary Market, Secondary Market)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN field_type 'Field type value from CSV (e.g., Last Price, Open Interest)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN ticker_source 'Ticker source value from CSV (e.g., Bloomberg, LSEG)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN country 'Country code value from CSV (e.g., US, UK)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN currency 'Currency code value from CSV (e.g., USD, GBP)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN region 'Region value from CSV (e.g., North America, Europe)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN term 'Term value from CSV (e.g., Long-term, Medium-term)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN tenor 'Tenor value from CSV (e.g., 10Y, 5Y)';
ALTER TABLE staging_lookup_tables COMMENT COLUMN created_at 'Timestamp when row was loaded into staging';

