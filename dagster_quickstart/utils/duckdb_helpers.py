"""DuckDB helper functions for database operations.

This module provides backward compatibility by re-exporting functions from
specialized DuckDB helper modules. For new code, prefer importing directly from:
- dagster_quickstart.utils.duckdb.data_loading
- dagster_quickstart.utils.duckdb.csv_operations
- dagster_quickstart.utils.duckdb.view_operations
- dagster_quickstart.utils.duckdb.table_operations
- dagster_quickstart.utils.duckdb.query_building
- dagster_quickstart.utils.duckdb.schema_helpers
"""

# Re-export all functions for backward compatibility
from dagster_quickstart.utils.duckdb import (
    build_dependency_view_sql,
    build_lookup_table_view_sql,
    build_meta_series_view_sql,
    build_pivot_columns,
    build_select_columns,
    build_union_all_query_for_lookup_tables,
    build_union_query_for_parents,
    build_view_sql,
    create_or_update_duckdb_view,
    create_sql_query_with_file_path,
    create_temp_table_from_query,
    load_csv_to_temp_table,
    load_series_data_from_duckdb,
    read_parquet_schema,
    unregister_temp_table,
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
