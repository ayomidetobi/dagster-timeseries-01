"""DuckDB helper modules for database operations.

This package contains specialized modules for different DuckDB operations:
- data_loading: Loading data from S3 Parquet files
- csv_operations: CSV file operations
- view_operations: View creation and management
- table_operations: Table creation and management
- query_building: SQL query building utilities
- schema_helpers: Schema reading and DDL helpers
"""

from dagster_quickstart.utils.duckdb.csv_operations import load_csv_to_temp_table
from dagster_quickstart.utils.duckdb.data_loading import (
    create_sql_query_with_file_path,
    load_series_data_from_duckdb,
)
from dagster_quickstart.utils.duckdb.query_building import (
    build_pivot_columns,
    build_union_all_query_for_lookup_tables,
    build_union_query_for_parents,
)
from dagster_quickstart.utils.duckdb.schema_helpers import (
    build_select_columns,
    read_parquet_schema,
)
from dagster_quickstart.utils.duckdb.table_operations import (
    create_temp_table_from_query,
    unregister_temp_table,
)
from dagster_quickstart.utils.duckdb.view_operations import (
    build_dependency_view_sql,
    build_lookup_table_view_sql,
    build_meta_series_view_sql,
    build_view_sql,
    create_or_update_duckdb_view,
)

__all__ = [
    "build_dependency_view_sql",
    "build_lookup_table_view_sql",
    "build_meta_series_view_sql",
    "build_pivot_columns",
    "build_select_columns",
    "build_union_all_query_for_lookup_tables",
    "build_union_query_for_parents",
    "build_view_sql",
    "create_or_update_duckdb_view",
    "create_sql_query_with_file_path",
    "create_temp_table_from_query",
    "load_csv_to_temp_table",
    "load_series_data_from_duckdb",
    "read_parquet_schema",
    "unregister_temp_table",
]
