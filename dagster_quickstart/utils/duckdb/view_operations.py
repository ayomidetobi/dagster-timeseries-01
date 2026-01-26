"""DuckDB view creation and management operations."""

from typing import Optional

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    CODE_BASED_LOOKUPS,
    DB_COLUMNS,
    DB_TABLES,
    META_SERIES_ALL_COLUMNS,
)
from dagster_quickstart.utils.duckdb.schema_helpers import (
    build_select_columns,
    read_parquet_schema,
)
from dagster_quickstart.utils.exceptions import DatabaseQueryError


def create_or_update_duckdb_view(
    duckdb: DuckDBResource,
    view_name: str,
    view_sql: str,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Create or update a DuckDB view with the given SQL.

    Generic helper function for creating/updating DuckDB views over S3 control tables.

    Args:
        duckdb: DuckDB resource with S3 access
        view_name: Name of the view to create/update
        view_sql: Complete CREATE OR REPLACE VIEW SQL statement
        context: Optional Dagster context for logging

    Raises:
        DatabaseQueryError: If view creation fails
    """
    try:
        duckdb.execute_command(view_sql)
        if context:
            context.log.info(f"Created/updated view {view_name}")
    except Exception as e:
        error_msg = f"Error creating/updating view {view_name}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


def build_view_sql(
    view_name: str,
    full_s3_path: str,
    select_columns: str,
    where_clause: str,
    order_by_column: str,
) -> str:
    """Build CREATE OR REPLACE VIEW SQL statement from components.

    Args:
        view_name: Name of the view to create.
        full_s3_path: Full S3 path to Parquet file.
        select_columns: SELECT column list (comma-separated).
        where_clause: WHERE clause (including "WHERE" keyword).
        order_by_column: ORDER BY column(s).

    Returns:
        Complete CREATE OR REPLACE VIEW SQL statement.
    """
    from database.ddl import CREATE_VIEW_FROM_S3_TEMPLATE

    return CREATE_VIEW_FROM_S3_TEMPLATE.format(
        view_name=view_name,
        full_s3_path=full_s3_path,
        select_columns=select_columns,
        where_clause=where_clause,
        order_by_column=order_by_column,
    )


def build_meta_series_view_sql(
    duckdb: DuckDBResource,
    full_s3_path: str,
    context: Optional[AssetExecutionContext] = None,
) -> str:
    """Build SQL to create/update meta series view.

    Dynamically builds SELECT columns based on what actually exists in the S3 Parquet file,
    since CSV files may not have all columns.

    Args:
        duckdb: DuckDB resource to query S3 Parquet schema.
        full_s3_path: Full S3 path to control table Parquet file.
        context: Optional Dagster context for logging.

    Returns:
        SQL CREATE OR REPLACE VIEW statement.

    Raises:
        DatabaseQueryError: If schema reading fails.
    """
    META_SERIES_TABLE = "metaSeries"

    # Read schema from S3 Parquet file
    available_columns = read_parquet_schema(duckdb, full_s3_path, context)

    # Build select columns with computed series_id
    computed_id = "row_number() OVER (ORDER BY series_code) AS series_id"
    select_columns = build_select_columns(
        computed_id_expression=computed_id,
        expected_columns=META_SERIES_ALL_COLUMNS,
        available_columns=available_columns,
        exclude_columns=["series_id"],
    )

    where_clause = "WHERE series_code IS NOT NULL AND series_code != ''"

    return build_view_sql(
        view_name=META_SERIES_TABLE,
        full_s3_path=full_s3_path,
        select_columns=select_columns,
        where_clause=where_clause,
        order_by_column="series_code",
    )


def build_lookup_table_view_sql(lookup_type: str, full_s3_path: str) -> str:
    """Build SQL to create/update a lookup table view.

    The S3 control table uses canonical format: lookup_type, code, name
    This function maps the canonical columns to the expected view column names.

    Args:
        lookup_type: Type of lookup table.
        full_s3_path: Full S3 path to control table Parquet file.

    Returns:
        SQL CREATE OR REPLACE VIEW statement.
    """
    from database.ddl import (
        CREATE_VIEW_FROM_S3_TEMPLATE,
        LOOKUP_TABLE_VIEW_SELECT_CODE_BASED,
        LOOKUP_TABLE_VIEW_SELECT_SIMPLE,
        LOOKUP_TABLE_VIEW_WHERE,
    )

    table_name = DB_TABLES[lookup_type]
    id_column, name_column = DB_COLUMNS[lookup_type]

    if lookup_type in CODE_BASED_LOOKUPS:
        # Code-based lookups: map canonical 'code' and 'name' to code_field and name_field
        code_field, name_field, _ = CODE_BASED_LOOKUPS[lookup_type]
        select_columns = LOOKUP_TABLE_VIEW_SELECT_CODE_BASED.format(
            id_column=id_column,
            code_field=code_field,
            name_field=name_field,
        )
        where_clause = LOOKUP_TABLE_VIEW_WHERE.format(lookup_type=lookup_type, column_name="code")
        order_by_column = "code"
    else:
        # Simple lookups: map canonical 'name' to name_column
        # For simple lookups, code = name in the canonical format
        select_columns = LOOKUP_TABLE_VIEW_SELECT_SIMPLE.format(
            id_column=id_column,
            name_column=name_column,
        )
        where_clause = LOOKUP_TABLE_VIEW_WHERE.format(lookup_type=lookup_type, column_name="name")
        order_by_column = "name"

    # Use generic template
    return CREATE_VIEW_FROM_S3_TEMPLATE.format(
        view_name=table_name,
        full_s3_path=full_s3_path,
        select_columns=select_columns,
        where_clause=where_clause,
        order_by_column=order_by_column,
    )


def build_dependency_view_sql(
    duckdb: DuckDBResource,
    full_s3_path: str,
    context: Optional[AssetExecutionContext] = None,
) -> str:
    """Build SQL to create/update dependency view.

    Dynamically builds SELECT columns based on what actually exists in the S3 Parquet file,
    since CSV files may not have all columns.

    Args:
        duckdb: DuckDB resource to query S3 Parquet schema.
        full_s3_path: Full S3 path to control table Parquet file.
        context: Optional Dagster context for logging.

    Returns:
        SQL CREATE OR REPLACE VIEW statement.

    Raises:
        DatabaseQueryError: If schema reading fails.
    """
    DEPENDENCY_TABLE = "seriesDependencyGraph"

    # Read schema from S3 Parquet file
    available_columns = read_parquet_schema(duckdb, full_s3_path, context)

    # Build select columns with computed dependency_id
    computed_id = "row_number() OVER (ORDER BY parent_series_id, child_series_id) AS dependency_id"
    expected_columns = [
        "parent_series_id",
        "child_series_id",
        "weight",
        "formula",
        "calc_type",
    ]
    select_columns = build_select_columns(
        computed_id_expression=computed_id,
        expected_columns=expected_columns,
        available_columns=available_columns,
        exclude_columns=["dependency_id"],
    )

    where_clause = "WHERE parent_series_id IS NOT NULL AND child_series_id IS NOT NULL"

    return build_view_sql(
        view_name=DEPENDENCY_TABLE,
        full_s3_path=full_s3_path,
        select_columns=select_columns,
        where_clause=where_clause,
        order_by_column="parent_series_id, child_series_id",
    )
