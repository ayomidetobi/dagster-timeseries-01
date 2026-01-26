"""Utility modules for Dagster assets."""

from . import datetime_utils

__all__ = ["datetime_utils"]

from dagster_quickstart.utils.constants import (
    CALCULATION_FORMULA_TYPES,
    CALCULATION_TYPES,
    DB_COLUMNS,
    DB_TABLES,
    DEFAULT_CSV_PATHS,
    DEFAULT_SMA_WINDOW,
    DEFAULT_WEIGHT_DIVISOR,
    LOOKUP_TABLE_COLUMNS,
    LOOKUP_TABLE_PROCESSING_ORDER,
    META_SERIES_REQUIRED_COLUMNS,
    NULL_VALUE_REPRESENTATION,
    QUERY_LIMIT_DEFAULT,
    QUERY_LIMIT_MAX,
)
from dagster_quickstart.utils.exceptions import (
    CalculationError,
    CSVValidationError,
    DatabaseError,
    DatabaseInsertError,
    DatabaseQueryError,
    DatabaseUpdateError,
    DataSourceValidationError,
    LookupTableError,
    MetaSeriesNotFoundError,
    RecordNotFoundError,
    S3ControlTableNotFoundError,
)
from dagster_quickstart.utils.helpers import (
    check_existing_value_data_in_s3,
    create_ingestion_result_dict,
    load_series_data_from_duckdb,
    process_time_series_data_points,
)
from dagster_quickstart.utils.summary import AssetSummary
from dagster_quickstart.utils.summary.csv_loader import add_csv_loader_summary_metadata
from dagster_quickstart.utils.summary.ingestion import (
    add_ingestion_summary_metadata,
    handle_ingestion_failure,
)
from dagster_quickstart.utils.validation_helpers import (
    validate_field_type_name,
    validate_series_metadata,
)

__all__ = [
    # Exceptions
    "CalculationError",
    "CSVValidationError",
    "DatabaseError",
    "DatabaseInsertError",
    "DatabaseQueryError",
    "DatabaseUpdateError",
    "DataSourceValidationError",
    "LookupTableError",
    "MetaSeriesNotFoundError",
    "RecordNotFoundError",
    "S3ControlTableNotFoundError",
    # Constants
    "CALCULATION_FORMULA_TYPES",
    "CALCULATION_TYPES",
    "DB_COLUMNS",
    "DB_TABLES",
    "DEFAULT_CSV_PATHS",
    "DEFAULT_SMA_WINDOW",
    "DEFAULT_WEIGHT_DIVISOR",
    "LOOKUP_TABLE_COLUMNS",
    "LOOKUP_TABLE_PROCESSING_ORDER",
    "META_SERIES_REQUIRED_COLUMNS",
    "NULL_VALUE_REPRESENTATION",
    "QUERY_LIMIT_DEFAULT",
    "QUERY_LIMIT_MAX",
    # Helpers
    "check_existing_value_data_in_s3",
    "create_ingestion_result_dict",
    "load_series_data_from_duckdb",
    "process_time_series_data_points",
    # Validation Helpers
    "validate_field_type_name",
    "validate_series_metadata",
    # Summary
    "AssetSummary",
    "add_csv_loader_summary_metadata",
    "add_ingestion_summary_metadata",
    "handle_ingestion_failure",
]
