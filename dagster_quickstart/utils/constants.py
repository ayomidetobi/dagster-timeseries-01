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
]

# Processing order for lookup tables (respects dependencies)
LOOKUP_TABLE_PROCESSING_ORDER = [
    "asset_class",
    "product_type",
    "data_type",
    "structure_type",
    "market_segment",
    "field_type",
    "ticker_source",
]

# Required columns for meta series CSV
META_SERIES_REQUIRED_COLUMNS = [
    "series_name",
    "series_code",
    "data_source",
    "ticker",
]

# Default CSV file paths
DEFAULT_CSV_PATHS = {
    "lookup_tables": "data/lookup_tables.csv",
    "allowed_names": "data/allowed_names.csv",
    "meta_series": "data/meta_series.csv",
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
    "meta_series": ("series_id", "series_code"),
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

