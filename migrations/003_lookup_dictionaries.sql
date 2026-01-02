DROP DICTIONARY IF EXISTS dict_asset_class;
CREATE DICTIONARY dict_asset_class (
    asset_class_id UInt32,
    asset_class_name String,
    description Nullable(String)
)
PRIMARY KEY asset_class_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT asset_class_id, asset_class_name, description FROM assetClassLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Product Type Dictionary
DROP DICTIONARY IF EXISTS dict_product_type;
CREATE DICTIONARY dict_product_type (
    product_type_id UInt32,
    product_type_name String,
    description Nullable(String)
)
PRIMARY KEY product_type_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT product_type_id, product_type_name, description FROM productTypeLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Sub Asset Class Dictionary
DROP DICTIONARY IF EXISTS dict_sub_asset_class;
CREATE DICTIONARY dict_sub_asset_class (
    sub_asset_class_id UInt32,
    sub_asset_class_name String,
    asset_class_id UInt32,
    description Nullable(String)
)
PRIMARY KEY sub_asset_class_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT sub_asset_class_id, sub_asset_class_name, asset_class_id, description FROM subAssetClassLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Data Type Dictionary
DROP DICTIONARY IF EXISTS dict_data_type;
CREATE DICTIONARY dict_data_type (
    data_type_id UInt32,
    data_type_name String,
    description Nullable(String)
)
PRIMARY KEY data_type_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT data_type_id, data_type_name, description FROM dataTypeLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Structure Type Dictionary
DROP DICTIONARY IF EXISTS dict_structure_type;
CREATE DICTIONARY dict_structure_type (
    structure_type_id UInt32,
    structure_type_name String,
    description Nullable(String)
)
PRIMARY KEY structure_type_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT structure_type_id, structure_type_name, description FROM structureTypeLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Market Segment Dictionary
DROP DICTIONARY IF EXISTS dict_market_segment;
CREATE DICTIONARY dict_market_segment (
    market_segment_id UInt32,
    market_segment_name String,
    description Nullable(String)
)
PRIMARY KEY market_segment_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT market_segment_id, market_segment_name, description FROM marketSegmentLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Field Type Dictionary
DROP DICTIONARY IF EXISTS dict_field_type;
CREATE DICTIONARY dict_field_type (
    field_type_id UInt32,
    field_type_name String,
    description Nullable(String)
)
PRIMARY KEY field_type_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT field_type_id, field_type_name, description FROM fieldTypeLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Ticker Source Dictionary
DROP DICTIONARY IF EXISTS dict_ticker_source;
CREATE DICTIONARY dict_ticker_source (
    ticker_source_id UInt32,
    ticker_source_name String,
    ticker_source_code String,
    description Nullable(String)
)
PRIMARY KEY ticker_source_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT ticker_source_id, ticker_source_name, ticker_source_code, description FROM tickerSourceLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Region Dictionary
DROP DICTIONARY IF EXISTS dict_region;
CREATE DICTIONARY dict_region (
    region_id UInt32,
    region_name String,
    description Nullable(String)
)
PRIMARY KEY region_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT region_id, region_name, description FROM regionLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Currency Dictionary
DROP DICTIONARY IF EXISTS dict_currency;
CREATE DICTIONARY dict_currency (
    currency_id UInt32,
    currency_code String,
    currency_name Nullable(String),
    description Nullable(String)
)
PRIMARY KEY currency_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT currency_id, currency_code, currency_name, description FROM currencyLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Term Dictionary
DROP DICTIONARY IF EXISTS dict_term;
CREATE DICTIONARY dict_term (
    term_id UInt32,
    term_name String,
    description Nullable(String)
)
PRIMARY KEY term_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT term_id, term_name, description FROM termLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Tenor Dictionary
DROP DICTIONARY IF EXISTS dict_tenor;
CREATE DICTIONARY dict_tenor (
    tenor_id UInt32,
    tenor_code String,
    tenor_name Nullable(String),
    description Nullable(String)
)
PRIMARY KEY tenor_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT tenor_id, tenor_code, tenor_name, description FROM tenorLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

-- Country Dictionary
DROP DICTIONARY IF EXISTS dict_country;
CREATE DICTIONARY dict_country (
    country_id UInt32,
    country_code String,
    country_name Nullable(String),
    description Nullable(String)
)
PRIMARY KEY country_id
SOURCE(CLICKHOUSE(
    QUERY 'SELECT country_id, country_code, country_name, description FROM countryLookup'
))
LAYOUT(FLAT())
LIFETIME(MIN 3600 MAX 7200);

