"""DuckDB CSV file operations."""

import uuid
from typing import Optional

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.exceptions import CSVValidationError


def load_csv_to_temp_table(
    duckdb: DuckDBResource,
    csv_path: str,
    null_value: str = "\\N",
    context: Optional[AssetExecutionContext] = None,
) -> str:
    r"""Load CSV file directly into DuckDB temp table using read_csv.

    Generic helper function for loading CSV files into temporary tables.

    Args:
        duckdb: DuckDB resource
        csv_path: Path to CSV file
        null_value: String representation of NULL values (default: "\\N")
        context: Optional Dagster context for logging

    Returns:
        Temporary table name

    Raises:
        CSVValidationError: If CSV cannot be read
    """
    temp_table = f"_temp_csv_{uuid.uuid4().hex}"

    try:
        # Use DuckDB's read_csv function directly - no pandas involved
        # DuckDB handles null values, type inference, and ragged lines automatically
        from database.ddl import CREATE_TEMP_TABLE_FROM_CSV

        create_table_sql = CREATE_TEMP_TABLE_FROM_CSV.format(
            temp_table=temp_table, csv_path=csv_path, null_value=null_value
        ).strip()
        duckdb.execute_command(create_table_sql)
        if context:
            context.log.debug(f"Loaded CSV into temp table {temp_table}")
        return temp_table
    except Exception as e:
        error_msg = f"Error reading CSV file {csv_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise CSVValidationError(error_msg) from e
