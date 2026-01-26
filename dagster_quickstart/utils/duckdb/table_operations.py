"""DuckDB table creation and management operations."""

from typing import Optional

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.exceptions import DatabaseQueryError


def unregister_temp_table(
    duckdb: DuckDBResource,
    temp_table: str,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Drop temporary table from DuckDB.

    Generic helper function for cleaning up temporary tables.

    Args:
        duckdb: DuckDB resource
        temp_table: Temporary table name to drop
        context: Optional Dagster context for logging
    """
    try:
        from database.ddl import DROP_TEMP_TABLE

        drop_sql = DROP_TEMP_TABLE.format(temp_table=temp_table)
        duckdb.execute_command(drop_sql)
        if context:
            context.log.debug(f"Dropped temp table {temp_table}")
    except Exception:
        # Table may already be dropped, ignore
        pass


def create_temp_table_from_query(
    duckdb: DuckDBResource,
    temp_table_name: str,
    query: str,
    context: Optional[AssetExecutionContext] = None,
) -> None:
    """Create a temporary table from a SQL query.

    Args:
        duckdb: DuckDB resource.
        temp_table_name: Name for the temporary table.
        query: SQL query to execute.
        context: Optional Dagster context for logging.

    Raises:
        DatabaseQueryError: If table creation fails.
    """
    try:
        from database.ddl import CREATE_TEMP_TABLE_FROM_QUERY

        create_table_sql = CREATE_TEMP_TABLE_FROM_QUERY.format(
            temp_table_name=temp_table_name, query=query
        ).strip()
        duckdb.execute_command(create_table_sql)
        if context:
            context.log.debug(f"Created temp table {temp_table_name}")
    except Exception as e:
        error_msg = f"Error creating temp table {temp_table_name}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e
