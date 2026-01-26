"""DuckDB SQL query building utilities."""

from datetime import datetime
from typing import Dict, List

import pandas as pd

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import CODE_BASED_LOOKUPS, DB_COLUMNS
from dagster_quickstart.utils.exceptions import DatabaseQueryError
from dagster_quickstart.utils.s3_helpers import (
    build_full_s3_path,
    build_s3_value_data_path,
)


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

        # Build unified S3 path (single file per series_code)
        relative_path = build_s3_value_data_path(parent_series_code)
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
