"""Constants used across Dagster assets."""

# Lookup table column names
LOOKUP_TABLE_COLUMNS = [
    "asset_class",
    "product_type",
    "sub_asset_class",
    "data_type",
    "structure_type",
    "market_segment",
    "field_type",
    "ticker_source",
    "region",
    "currency",
    "term",
    "tenor",
    "country",
]

# Processing order for lookup tables (all independent now)
LOOKUP_TABLE_PROCESSING_ORDER = [
    "asset_class",
    "product_type",
    "data_type",
    "structure_type",
    "market_segment",
    "field_type",
    "ticker_source",
    "sub_asset_class",
    "region",
    "currency",
    "term",
    "tenor",
    "country",
]

# Required columns for meta series CSV
META_SERIES_REQUIRED_COLUMNS = [
    "series_name",
    "series_code",
    "data_source",
    "ticker",
]

# Required columns for series dependencies CSV
SERIES_DEPENDENCIES_REQUIRED_COLUMNS = [
    "parent_series_code",
    "child_series_code",
]

# Default CSV file paths
DEFAULT_CSV_PATHS = {
    "lookup_tables": "data/lookup_tables.csv",
    "allowed_names": "data/allowed_names.csv",
    "meta_series": "data/meta_series.csv",
    "series_dependencies": "data/series_dependencies.csv",
}

# NULL value representation in CSV files
NULL_VALUE_REPRESENTATION = "\\N"

# Date format strings
DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Database table names
DB_TABLES = {
    "asset_class": "assetClassLookup",
    "product_type": "productTypeLookup",
    "sub_asset_class": "subAssetClassLookup",
    "data_type": "dataTypeLookup",
    "structure_type": "structureTypeLookup",
    "market_segment": "marketSegmentLookup",
    "field_type": "fieldTypeLookup",
    "ticker_source": "tickerSourceLookup",
    "region": "regionLookup",
    "currency": "currencyLookup",
    "term": "termLookup",
    "tenor": "tenorLookup",
    "country": "countryLookup",
    "meta_series": "metaSeries",
    "dependency_graph": "seriesDependencyGraph",
    "calculation_log": "calculationLog",
    "value_data": "valueData",
}

# Database column name mappings (lookup type -> (id_column, name_column)
DB_COLUMNS = {
    "asset_class": ("asset_class_id", "asset_class_name"),
    "product_type": ("product_type_id", "product_type_name"),
    "sub_asset_class": ("sub_asset_class_id", "sub_asset_class_name"),
    "data_type": ("data_type_id", "data_type_name"),
    "structure_type": ("structure_type_id", "structure_type_name"),
    "market_segment": ("market_segment_id", "market_segment_name"),
    "field_type": ("field_type_id", "field_type_name"),
    "ticker_source": ("ticker_source_id", "ticker_source_name"),
    "region": ("region_id", "region_name"),
    "currency": ("currency_id", "currency_code"),
    "term": ("term_id", "term_name"),
    "tenor": ("tenor_id", "tenor_code"),
    "country": ("country_id", "country_code"),
    "meta_series": ("series_id", "series_code"),
}

# Lookup types that require code fields in addition to name
# Format: (code_field, name_field, check_field)
# check_field determines which field to use for duplicate checking
CODE_BASED_LOOKUPS = {
    "currency": ("currency_code", "currency_name", "currency_code"),
    "tenor": ("tenor_code", "tenor_name", "tenor_code"),
    "country": ("country_code", "country_name", "country_code"),
    "ticker_source": ("ticker_source_code", "ticker_source_name", "ticker_source_name"),
}

# Calculation types
CALCULATION_TYPES = {
    "SMA": "SMA",
    "WEIGHTED_COMPOSITE": "WEIGHTED_COMPOSITE",
    "MOVING_AVERAGE": "MOVING_AVERAGE",
    "COMPOSITE": "COMPOSITE",
}

# Default calculation values
DEFAULT_SMA_WINDOW = 20
DEFAULT_WEIGHT_DIVISOR = 1.0  # For equal weight distribution

# Query constants
QUERY_LIMIT_DEFAULT = 1000
QUERY_LIMIT_MAX = 10000

# Batch size constants
DEFAULT_BATCH_SIZE = 10000  # Default batch size for database insertions

# PyPDL constants
PYPDL_DEFAULT_HOST = "gnp-histo.europe.echonet"
PYPDL_DEFAULT_PORT = 12002
PYPDL_DEFAULT_USERNAME = "jess05 Macro Quant"
PYPDL_DEFAULT_MAX_CONCURRENT = 3  # Conservative default for concurrent requests

# Retry policy constants
RETRY_POLICY_MAX_RETRIES_DEFAULT = 3
RETRY_POLICY_DELAY_DEFAULT = 1.0
RETRY_POLICY_MAX_RETRIES_INGESTION = 3
RETRY_POLICY_DELAY_INGESTION = 2.0
RETRY_POLICY_MAX_RETRIES_CSV_LOADER = 2
RETRY_POLICY_DELAY_CSV_LOADER = 1.0

# Data quality constants
# Timeliness thresholds (in hours)
MAX_DATA_AGE_HOURS = 24
MAX_DATA_GAP_MULTIPLIER = 3.0  # Max gap is 3x median gap

# Completeness thresholds (percentage)
MAX_NULL_PERCENTAGE_DEFAULT = 5.0
MAX_NULL_PERCENTAGE_REQUIRED_FIELDS = 0.0

# Accuracy thresholds
MAX_OUTLIER_PERCENTAGE = 5.0  # Max percentage of outliers allowed

# Great Expectations configuration
GE_DATASOURCE_NAME = "dagster_datasource"
GE_DATA_CONNECTOR_NAME = "default_runtime_data_connector"
GE_EXPECTATION_SUITE_PREFIX = "dagster_"

# Data quality dimension names
DATA_QUALITY_DIMENSIONS = [
    "timeliness",
    "completeness",
    "accuracy",
    "validity",
    "uniqueness",
    "consistency",
]
