"""Common utilities for loading data from CSV to S3."""

from typing import List

from dagster_quickstart.resources import DuckDBResource


def get_available_columns(
    duckdb: DuckDBResource, temp_table: str, expected_columns: List[str]
) -> List[str]:
    """Get list of available columns from temp table that match expected columns.

    Args:
        duckdb: DuckDB resource.
        temp_table: Temporary table name.
        expected_columns: List of expected column names.

    Returns:
        List of available column names.
    """
    # Query table schema to get actual columns
    schema_query = f"DESCRIBE {temp_table}"
    schema_result = duckdb.execute_query(schema_query)

    if schema_result is None or schema_result.empty:
        return []

    actual_columns = (
        schema_result["column_name"].tolist() if "column_name" in schema_result.columns else []
    )
    return [col for col in expected_columns if col in actual_columns]


def check_temp_table_has_data(duckdb: DuckDBResource, temp_table: str) -> bool:
    """Check if a temp table has any data.

    Args:
        duckdb: DuckDB resource.
        temp_table: Temporary table name.

    Returns:
        True if table has data, False otherwise.
    """
    count_query = f"SELECT COUNT(*) as cnt FROM {temp_table}"
    count_result = duckdb.execute_query(count_query)
    return count_result is not None and not count_result.empty and count_result.iloc[0]["cnt"] > 0


def get_temp_table_columns(duckdb: DuckDBResource, temp_table: str) -> List[str]:
    """Get column names from a temp table.

    Args:
        duckdb: DuckDB resource.
        temp_table: Temporary table name.

    Returns:
        List of column names, empty if table doesn't exist or has no columns.
    """
    schema_query = f"DESCRIBE {temp_table}"
    schema_result = duckdb.execute_query(schema_query)
    if schema_result is None or schema_result.empty:
        return []
    return schema_result["column_name"].tolist() if "column_name" in schema_result.columns else []
