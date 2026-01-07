CREATE TABLE IF NOT EXISTS staging_meta_series (
    series_name Nullable(String),
    series_code Nullable(String),
    data_source Nullable(String),
    field_type Nullable(String),
    asset_class Nullable(String),
    sub_asset_class Nullable(String),
    product_type Nullable(String),
    data_type Nullable(String),
    structure_type Nullable(String),
    market_segment Nullable(String),
    ticker_source Nullable(String),
    ticker Nullable(String),
    region Nullable(String),
    currency Nullable(String),
    term Nullable(String),
    tenor Nullable(String),
    country Nullable(String),
    valid_from Nullable(String),
    valid_to Nullable(String),
    calculation_formula Nullable(String),
    description Nullable(String),
    is_active Nullable(String),
    created_at DateTime64(6) DEFAULT now64(6)
) ENGINE = MergeTree
PRIMARY KEY (created_at)
ORDER BY (created_at)
TTL created_at + INTERVAL 30 DAY;

-- Comments for documentation
ALTER TABLE staging_meta_series COMMENT COLUMN series_name 'Series name from CSV';
ALTER TABLE staging_meta_series COMMENT COLUMN series_code 'Series code from CSV (unique identifier)';
ALTER TABLE staging_meta_series COMMENT COLUMN data_source 'Data source from CSV (RAW, DERIVED, NONE)';
ALTER TABLE staging_meta_series COMMENT COLUMN field_type 'Field type name from CSV (will be resolved to field_type_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN asset_class 'Asset class name from CSV (will be resolved to asset_class_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN sub_asset_class 'Sub-asset class name from CSV (will be resolved to sub_asset_class_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN product_type 'Product type name from CSV (will be resolved to product_type_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN data_type 'Data type name from CSV (will be resolved to data_type_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN structure_type 'Structure type name from CSV (will be resolved to structure_type_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN market_segment 'Market segment name from CSV (will be resolved to market_segment_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN ticker_source 'Ticker source name from CSV (will be resolved to ticker_source_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN ticker 'Ticker symbol from CSV';
ALTER TABLE staging_meta_series COMMENT COLUMN region 'Region name from CSV (will be resolved to region_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN currency 'Currency code from CSV (will be resolved to currency_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN term 'Term name from CSV (will be resolved to term_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN tenor 'Tenor code from CSV (will be resolved to tenor_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN country 'Country code from CSV (will be resolved to country_id)';
ALTER TABLE staging_meta_series COMMENT COLUMN valid_from 'Valid from date/time from CSV';
ALTER TABLE staging_meta_series COMMENT COLUMN valid_to 'Valid to date/time from CSV';
ALTER TABLE staging_meta_series COMMENT COLUMN calculation_formula 'Calculation formula from CSV';
ALTER TABLE staging_meta_series COMMENT COLUMN description 'Description from CSV';
ALTER TABLE staging_meta_series COMMENT COLUMN is_active 'Is active flag from CSV (will be converted to UInt8)';
ALTER TABLE staging_meta_series COMMENT COLUMN created_at 'Timestamp when row was loaded into staging';

