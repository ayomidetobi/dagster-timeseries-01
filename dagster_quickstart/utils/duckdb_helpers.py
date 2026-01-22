"""DuckDB helper functions for database operations."""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.resources.duckdb_datacacher import SQL
from dagster_quickstart.utils.constants import (
    CODE_BASED_LOOKUPS,
    DB_COLUMNS,
    DB_TABLES,
    META_SERIES_ALL_COLUMNS,
    SQL_FILE_PATH_PLACEHOLDER,
)
from dagster_quickstart.utils.exceptions import CSVValidationError, DatabaseQueryError
from dagster_quickstart.utils.s3_helpers import (
    build_full_s3_path,
    build_s3_value_data_path,
)


def load_series_data_from_duckdb(
    duckdb: DuckDBResource, series_code: str, partition_date: datetime
) -> Optional[pd.DataFrame]:
    """Load time-series data from S3 Parquet files for a given series_code and date.

    Uses DuckDBResource's load() method with SQL class bindings for S3 path resolution.

    Args:
        duckdb: DuckDB resource with S3 access via httpfs
        series_code: Series code to load data for
        partition_date: Partition date for the data

    Returns:
        DataFrame with timestamp and value columns, or None if no data found
    """
    # Get relative S3 path for this series (relative to bucket)
    relative_path = build_s3_value_data_path(series_code, partition_date)

    try:
        # Use DuckDBResource's load() method with SQL class for proper S3 path resolution
        if SQL is not None:
            # Use $file_path binding - sql_to_string will resolve it to full S3 path
            query = SQL(
                "SELECT timestamp, value FROM read_parquet('$file_path') ORDER BY timestamp",
                file_path=relative_path,
            )

            # Use DuckDBResource.load() which handles S3 path resolution via duckdb_datacacher
            df = duckdb.load(query)

            if df is not None and not df.empty:
                # Ensure we only have timestamp and value columns
                if all(col in df.columns for col in ["timestamp", "value"]):
                    return df[["timestamp", "value"]]
                return df
        else:
            # SQL class not available, use execute_query as fallback
            full_s3_path = build_full_s3_path(duckdb, relative_path)
            query = f"""
            SELECT timestamp, value
            FROM read_parquet('{full_s3_path}')
            ORDER BY timestamp
            """
            df = duckdb.execute_query(query)
            if not df.empty:
                # Ensure we only have timestamp and value columns
                if all(col in df.columns for col in ["timestamp", "value"]):
                    return df[["timestamp", "value"]]
                return df
    except Exception:
        # File doesn't exist, return None
        pass
    return None


def create_sql_query_with_file_path(query_template: str, file_path: str, **kwargs: Any) -> Any:
    """Create SQL query object with file_path binding.

    Args:
        query_template: SQL query template with $file_path placeholder
        file_path: Relative S3 file path (relative to bucket)
        **kwargs: Additional bindings for SQL query

    Returns:
        SQL object with bindings, or query string if SQL class not available

    Raises:
        DatabaseQueryError: If SQL class is required but not available
    """
    if SQL is not None:
        return SQL(query_template, file_path=file_path, **kwargs)
    # Return template string for fallback usage
    return query_template.replace(SQL_FILE_PATH_PLACEHOLDER, file_path)


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
    import uuid

    temp_table = f"_temp_csv_{uuid.uuid4().hex}"

    try:
        # Use DuckDB's read_csv function directly - no pandas involved
        # DuckDB handles null values, type inference, and ragged lines automatically
        create_table_sql = f"""
            CREATE TEMP TABLE {temp_table} AS
            SELECT * FROM read_csv('{csv_path}', 
                nullstr='{null_value}',
                header=true,
                auto_detect=true,
                ignore_errors=false
            )
        """
        duckdb.execute_command(create_table_sql)
        if context:
            context.log.debug(f"Loaded CSV into temp table {temp_table}")
        return temp_table
    except Exception as e:
        error_msg = f"Error reading CSV file {csv_path}: {e}"
        if context:
            context.log.error(error_msg)
        raise CSVValidationError(error_msg) from e


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
        duckdb.execute_command(f"DROP TABLE IF EXISTS {temp_table}")
        if context:
            context.log.debug(f"Dropped temp table {temp_table}")
    except Exception:
        # Table may already be dropped, ignore
        pass


def build_union_query_for_parents(
    duckdb: DuckDBResource,
    parent_series_result: pd.DataFrame,
    target_date: datetime,
) -> List[str]:
    """Build UNION ALL query parts for loading parent series data from S3.

    Creates a list of SQL SELECT statements, one for each parent series,
    that can be combined with UNION ALL to load all parent data in a single query.

    Args:
        duckdb: DuckDB resource with S3 access
        parent_series_result: DataFrame with columns parent_series_id and parent_series_code
        target_date: Target date for filtering data (only data <= target_date is loaded)

    Returns:
        List of SQL SELECT statement strings, one per parent series

    Raises:
        DatabaseQueryError: If no parent series data paths can be built
    """
    union_parts: List[str] = []

    for _, row in parent_series_result.iterrows():
        parent_series_id = row["parent_series_id"]
        parent_series_code = row["parent_series_code"]

        # Build S3 path for specific target date partition
        relative_path = build_s3_value_data_path(parent_series_code, target_date)
        full_s3_path = build_full_s3_path(duckdb, relative_path)

        # Load data for target date only (filter to target date or earlier)
        union_parts.append(f"""
            SELECT 
                timestamp, 
                value,
                {parent_series_id} as parent_series_id
            FROM read_parquet('{full_s3_path}')
            WHERE timestamp <= '{target_date.isoformat()}'
        """)

    if not union_parts:
        raise DatabaseQueryError("No parent series data paths to load")

    return union_parts


def build_pivot_columns(input_series_ids: List[int]) -> str:
    """Build pivot columns SQL using conditional aggregation.

    Creates SQL expressions that pivot parent series values into separate columns
    (value_0, value_1, value_2, etc.) for use in calculations.

    Args:
        input_series_ids: List of parent series IDs in order

    Returns:
        Comma-separated string of pivot column expressions
    """
    pivot_columns = []
    for idx, parent_id in enumerate(input_series_ids):
        pivot_columns.append(
            f"MAX(CASE WHEN parent_series_id = {parent_id} THEN value END) AS value_{idx}"
        )

    return ", ".join(pivot_columns)


# ============================================================================
# DDL Helper Functions - Reusable CRUD operations for DuckDB views/tables
# ============================================================================


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
        schema_query = f"SELECT * FROM read_parquet('{full_s3_path}') LIMIT 0"
        schema_result = duckdb.execute_query(schema_query)

        if schema_result is None:
            raise DatabaseQueryError(
                f"Could not read schema from S3 Parquet file: {full_s3_path}"
            )

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
        LOOKUP_TABLE_VIEW_WHERE_CODE_BASED,
        LOOKUP_TABLE_VIEW_WHERE_SIMPLE,
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
        where_clause = LOOKUP_TABLE_VIEW_WHERE_CODE_BASED.format(lookup_type=lookup_type)
        order_by_column = "code"
    else:
        # Simple lookups: map canonical 'name' to name_column
        # For simple lookups, code = name in the canonical format
        select_columns = LOOKUP_TABLE_VIEW_SELECT_SIMPLE.format(
            id_column=id_column,
            name_column=name_column,
        )
        where_clause = LOOKUP_TABLE_VIEW_WHERE_SIMPLE.format(lookup_type=lookup_type)
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
    computed_id = (
        "row_number() OVER (ORDER BY parent_series_id, child_series_id) AS dependency_id"
    )
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


def build_union_all_query_for_lookup_tables(
    duckdb: DuckDBResource, all_lookup_temp_tables: Dict[str, str]
) -> str:
    """Build UNION ALL query to combine all lookup tables into canonical format.

    Projects each lookup table into a canonical schema:
    - lookup_type STRING
    - code STRING
    - name STRING

    For code-based lookups: uses code_field and name_field
    For simple lookups: sets code = name = value

    Args:
        duckdb: DuckDB resource.
        all_lookup_temp_tables: Dictionary mapping lookup_type -> temp table name.

    Returns:
        SQL UNION ALL query string, empty if no valid tables.
    """
    from dagster_quickstart.assets.csv_loader.utils.load_data import check_temp_table_has_data
    from database.ddl import UNION_ALL_SELECT_CODE_BASED, UNION_ALL_SELECT_SIMPLE

    union_parts: List[str] = []
    for lookup_type, temp_table in all_lookup_temp_tables.items():
        if not check_temp_table_has_data(duckdb, temp_table):
            continue

        # Project into canonical format: lookup_type, code, name
        if lookup_type in CODE_BASED_LOOKUPS:
            # Code-based lookups have both code_field and name_field
            code_field, name_field, _ = CODE_BASED_LOOKUPS[lookup_type]
            union_parts.append(
                UNION_ALL_SELECT_CODE_BASED.format(
                    lookup_type=lookup_type,
                    code_field=code_field,
                    name_field=name_field,
                    temp_table=temp_table,
                )
            )
        else:
            # Simple lookups only have a name column
            # Set code = name = value for canonical format
            _, name_column = DB_COLUMNS[lookup_type]
            union_parts.append(
                UNION_ALL_SELECT_SIMPLE.format(
                    lookup_type=lookup_type,
                    name_column=name_column,
                    temp_table=temp_table,
                )
            )

    return " UNION ALL ".join(union_parts)


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
        create_table_sql = f"CREATE TEMP TABLE {temp_table_name} AS {query}"
        duckdb.execute_command(create_table_sql)
        if context:
            context.log.debug(f"Created temp table {temp_table_name}")
    except Exception as e:
        error_msg = f"Error creating temp table {temp_table_name}: {e}"
        if context:
            context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e
