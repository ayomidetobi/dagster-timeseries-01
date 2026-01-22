"""DuckDB schema reading and DDL helper functions."""

from typing import List, Optional

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.exceptions import DatabaseQueryError


def read_parquet_schema(
    duckdb: DuckDBResource,
    full_s3_path: str,
    context: Optional[AssetExecutionContext] = None,
) -> List[str]:
    """Read column schema from S3 Parquet file.

    Args:
        duckdb: DuckDB resource to query S3 Parquet schema.
        full_s3_path: Full S3 path to Parquet file.
        context: Optional Dagster context for logging.

    Returns:
        List of column names from the Parquet file.

    Raises:
        DatabaseQueryError: If schema reading fails.
    """
    try:
        from database.ddl import READ_PARQUET_SCHEMA

        schema_query = READ_PARQUET_SCHEMA.format(full_s3_path=full_s3_path)
        schema_result = duckdb.execute_query(schema_query)

        if schema_result is None:
            raise DatabaseQueryError(f"Could not read schema from S3 Parquet file: {full_s3_path}")

        available_columns = schema_result.columns.tolist()

        if not available_columns:
            raise DatabaseQueryError(f"No columns found in S3 Parquet file: {full_s3_path}")

        return available_columns

    except DatabaseQueryError:
        raise
    except Exception as e:
        error_msg = f"Error reading schema from S3 Parquet file {full_s3_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


def build_select_columns(
    computed_id_expression: str,
    expected_columns: List[str],
    available_columns: List[str],
    exclude_columns: Optional[List[str]] = None,
) -> str:
    """Build SELECT column list from available and expected columns.

    Args:
        computed_id_expression: SQL expression for computed ID column (e.g., "row_number() ...").
        expected_columns: List of expected column names in desired order.
        available_columns: List of columns actually available in the source.
        exclude_columns: Optional list of columns to exclude from output.

    Returns:
        Comma-separated SELECT column list as string.
    """
    exclude_columns = exclude_columns or []
    select_parts = [computed_id_expression]

    # Add expected columns that exist, in order
    for col in expected_columns:
        if col in available_columns and col not in exclude_columns:
            select_parts.append(col)

    # Add any other columns that exist but aren't in expected list
    for col in available_columns:
        if col not in expected_columns and col not in exclude_columns:
            select_parts.append(col)

    return ",\n        ".join(select_parts)
