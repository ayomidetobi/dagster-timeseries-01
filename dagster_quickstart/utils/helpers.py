"""Reusable helper functions for Dagster assets.

This module provides backward compatibility by re-exporting functions from
specialized helper modules. For new code, prefer importing directly from:
- dagster_quickstart.utils.s3_helpers
- dagster_quickstart.utils.duckdb_helpers
- dagster_quickstart.utils.data_processing_helpers
- dagster_quickstart.utils.general_helpers
- dagster_quickstart.utils.validation_helpers
"""

# CSV loader helpers
from dagster_quickstart.utils.csv_loader_helpers import (
    ensure_views_exist,
    process_csv_to_s3_with_validation,
)

# Data processing helpers
from dagster_quickstart.utils.data_processing_helpers import (
    create_ingestion_result_dict,
    process_time_series_data_points,
)

# DuckDB helpers
from dagster_quickstart.utils.duckdb_helpers import (
    build_pivot_columns,
    build_union_query_for_parents,
    create_or_update_duckdb_view,
    create_sql_query_with_file_path,
    load_csv_to_temp_table,
    load_series_data_from_duckdb,
    unregister_temp_table,
)

# General helpers
from dagster_quickstart.utils.general_helpers import (
    get_version_date,
    round_to_six_decimal_places,
)

# S3 helpers
from dagster_quickstart.utils.s3_helpers import (
    build_full_s3_path,
    build_s3_control_table_path,
    build_s3_value_data_path,
    check_existing_value_data_in_s3,
    save_value_data_to_s3,
    write_to_s3_control_table,
)

# Validation helpers (re-exported for convenience)
from dagster_quickstart.utils.validation_helpers import (
    validate_referential_integrity_sql,
)

__all__ = [
    "build_full_s3_path",
    "build_pivot_columns",
    "build_s3_control_table_path",
    "build_s3_value_data_path",
    "build_union_query_for_parents",
    "check_existing_value_data_in_s3",
    "create_ingestion_result_dict",
    "create_or_update_duckdb_view",
    "create_sql_query_with_file_path",
    "ensure_views_exist",
    "get_version_date",
    "load_csv_to_temp_table",
    "load_series_data_from_duckdb",
    "process_csv_to_s3_with_validation",
    "process_time_series_data_points",
    "round_to_six_decimal_places",
    "save_value_data_to_s3",
    "unregister_temp_table",
    "validate_referential_integrity_sql",
    "write_to_s3_control_table",
]
