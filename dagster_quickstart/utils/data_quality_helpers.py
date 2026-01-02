"""Helper functions for data quality operations."""

from typing import Any, Dict

import pandas as pd
import polars as pl
from dagster import OpExecutionContext, get_dagster_logger

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.exceptions import ConsistencyCheckError

logger = get_dagster_logger()


def polars_to_pandas(df: pl.DataFrame) -> pd.DataFrame:
    """Convert Polars DataFrame to Pandas DataFrame.

    Args:
        df: Polars DataFrame

    Returns:
        Pandas DataFrame
    """
    return df.to_pandas()


def validate_foreign_keys(
    context: OpExecutionContext,
    df: pd.DataFrame,
    clickhouse: ClickHouseResource,
    foreign_keys: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    """Validate referential integrity for foreign key columns.

    Args:
        context: Dagster execution context
        df: Pandas DataFrame to validate
        clickhouse: ClickHouse resource
        foreign_keys: Dict mapping column names to lookup table info

    Returns:
        Dict with validation results for each foreign key

    Raises:
        ConsistencyCheckError: If validation fails
    """
    results = {}

    for column, fk_info in foreign_keys.items():
        if column not in df.columns:
            logger.warning(f"Column {column} not found in DataFrame, skipping FK check")
            continue

        lookup_table = fk_info.get("lookup_table")
        lookup_column = fk_info.get("lookup_column", "id")

        df_values = set(df[column].dropna().unique())

        if len(df_values) == 0:
            logger.info(f"No values to validate for {column}")
            results[column] = {"valid": True, "invalid_count": 0}
            continue

        try:
            query = f"SELECT DISTINCT {lookup_column} FROM {lookup_table}"
            lookup_result = clickhouse.execute_query(query)

            if hasattr(lookup_result, "result_rows"):
                valid_ids = {row[0] for row in lookup_result.result_rows if row[0] is not None}
            else:
                valid_ids = set()

            invalid_ids = df_values - valid_ids
            invalid_count = len(invalid_ids)

            results[column] = {
                "valid": invalid_count == 0,
                "invalid_count": invalid_count,
                "invalid_ids": list(invalid_ids)[:10],  # Limit to 10 for logging
                "total_values": len(df_values),
            }

            if invalid_count > 0:
                logger.warning(
                    f"Found {invalid_count} invalid foreign key values in {column} "
                    f"referencing {lookup_table}"
                )

        except Exception as e:
            logger.error(f"Error validating foreign key {column}: {e}")
            raise ConsistencyCheckError(f"Failed to validate foreign key {column}: {e}") from e

    return results
