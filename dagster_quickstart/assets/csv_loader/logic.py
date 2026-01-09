"""CSV loading logic for lookup tables and meta series with validation.

Uses S3 Parquet files as the datalake for staging data, then processes into DuckDB tables.
Uses DuckDBResource.save() and load() methods for S3 operations.
"""

import time
import uuid
from typing import Any, Dict, List

import pandas as pd
from dagster import AssetExecutionContext, get_dagster_logger

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    CODE_BASED_LOOKUPS,
    DB_COLUMNS,
    DB_TABLES,
    LOOKUP_TABLE_COLUMNS,
    LOOKUP_TABLE_PROCESSING_ORDER,
    META_SERIES_STAGING_COLUMNS,
    NULL_VALUE_REPRESENTATION,
    S3_STAGING_LOOKUP_TABLES,
    S3_STAGING_META_SERIES,
    SQL_READ_PARQUET_TEMPLATE,
)
from dagster_quickstart.utils.exceptions import (
    CSVValidationError,
    DatabaseInsertError,
    DatabaseQueryError,
    ReferentialIntegrityError,
)
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_staging_path,
    load_from_s3_parquet,
    read_csv_safe,
    save_to_s3_parquet,
)
from database.referential_integrity import ReferentialIntegrityValidator
from database.schemas import INSERT_META_SERIES_FROM_S3_STAGING_SQL

from .config import MetaSeriesCSVConfig

logger = get_dagster_logger()


def load_meta_series_logic(
    context: AssetExecutionContext,
    config: MetaSeriesCSVConfig,
    duckdb: DuckDBResource,
) -> tuple[pd.DataFrame, Dict[str, int]]:
    """Load meta series from CSV file using staging → metaSeries flow.

    Args:
        context: Dagster execution context
        config: Meta series CSV configuration
        duckdb: DuckDB resource

    Returns:
        Tuple of (DataFrame with loaded meta series results, results dictionary)
    """
    context.log.info(f"Loading meta series from {config.csv_path}")

    # Load meta series data
    df = read_csv_safe(
        config.csv_path,
        null_values=NULL_VALUE_REPRESENTATION,
        truncate_ragged_lines=True,
    )

    # Use staging → metaSeries flow with SQL-based processing
    context.log.info("Using staging → metaSeries flow with deterministic ID generation")
    results = process_staging_to_meta_series(context, duckdb, df)

    # Convert dictionary to pandas DataFrame
    result_df = pd.DataFrame(
        [
            {"series_code": series_code, "series_id": series_id}
            for series_code, series_id in results.items()
        ]
    )

    return result_df, results


def _load_staging_data_for_validation(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    staging_s3_path: str,
    validation_columns: List[str],
) -> List[Dict[str, Any]]:
    """Load staging data from S3 Parquet for referential integrity validation.

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        staging_s3_path: Relative S3 path to staging file
        validation_columns: List of column names to select

    Returns:
        List of dictionaries representing staging data rows
    """
    columns_str = ", ".join(validation_columns)
    query_template = (
        f"SELECT {columns_str} FROM {SQL_READ_PARQUET_TEMPLATE} "
        "WHERE series_code IS NOT NULL AND series_code != ''"
    )

    try:
        df_staging = load_from_s3_parquet(
            duckdb=duckdb,
            relative_path=staging_s3_path,
            query_template=query_template,
            context=context,
        )

        if df_staging is not None and not df_staging.empty:
            return df_staging.to_dict("records")
        return []
    except DatabaseQueryError as e:
        context.log.warning(
            f"Could not load staging data from S3 for validation: {e}",
            extra={"staging_path": staging_s3_path},
        )
        return []


def _validate_meta_series_referential_integrity(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    staging_s3_path: str,
) -> None:
    """Validate referential integrity of meta series staging data.

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        staging_s3_path: Relative S3 path to staging file

    Raises:
        ReferentialIntegrityError: If validation fails
    """
    context.log.info("Validating referential integrity for meta series references")
    validator = ReferentialIntegrityValidator(duckdb)

    validation_columns = ["series_code"] + list(LOOKUP_TABLE_PROCESSING_ORDER)
    staging_data = _load_staging_data_for_validation(
        context, duckdb, staging_s3_path, validation_columns
    )

    try:
        validator.validate_meta_series_references(context, staging_data)
        context.log.info("Referential integrity validation passed")
    except ReferentialIntegrityError:
        raise
    except Exception as e:
        error_msg = f"Referential integrity validation failed: {e}"
        context.log.error(error_msg)
        raise ReferentialIntegrityError(error_msg) from e


def _insert_meta_series_from_staging(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    full_s3_path: str,
) -> None:
    """Insert meta series from S3 Parquet staging file into metaSeries table.

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        full_s3_path: Full S3 path to staging file

    Raises:
        DatabaseInsertError: If insertion fails
    """
    sql = INSERT_META_SERIES_FROM_S3_STAGING_SQL.format(full_s3_path=full_s3_path)

    try:
        duckdb.execute_command(sql)
        context.log.info("Successfully inserted meta series from S3 Parquet staging to metaSeries")
    except Exception as e:
        error_msg = f"Error inserting meta series from S3 Parquet staging: {e}"
        context.log.error(error_msg)
        raise DatabaseInsertError(error_msg) from e


def _fetch_meta_series_results(
    context: AssetExecutionContext, duckdb: DuckDBResource
) -> Dict[str, int]:
    """Fetch meta series results (series_code -> series_id mapping).

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access

    Returns:
        Dictionary mapping series_code -> series_id

    Raises:
        DatabaseQueryError: If query fails
    """
    query = "SELECT series_id, series_code FROM metaSeries ORDER BY series_id"
    try:
        df = duckdb.execute_query(query)
        results = {}
        if not df.empty:
            for _, row in df.iterrows():
                results[row["series_code"]] = row["series_id"]
        return results
    except Exception as e:
        error_msg = f"Error fetching meta series results: {e}"
        context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


def process_staging_to_meta_series(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    df: pd.DataFrame,
) -> Dict[str, int]:
    """Process meta series from S3 Parquet staging to metaSeries table using SQL.

    This function implements the staging → metaSeries flow with deterministic sequential IDs.
    Data flow: CSV → S3 Parquet (staging) → metaSeries (DuckDB table)
    Reads from S3 Parquet files using DuckDBResource.load() and read_parquet().

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        df: Polars DataFrame with meta series data

    Returns:
        Dictionary mapping series_code -> series_id

    Raises:
        ReferentialIntegrityError: If referential integrity validation fails
        DatabaseInsertError: If insertion fails
        DatabaseQueryError: If query fails
    """
    context.log.info("Processing meta series from S3 Parquet staging to metaSeries using SQL")

    # Step 1: Save CSV data to S3 Parquet as staging
    staging_s3_path = load_csv_to_staging_s3(
        context, duckdb, df, S3_STAGING_META_SERIES, META_SERIES_STAGING_COLUMNS
    )

    # Get full S3 path for querying
    bucket = duckdb.get_bucket()
    full_s3_path = build_full_s3_path(bucket, staging_s3_path)

    # Step 1.5: Validate referential integrity before insertion
    _validate_meta_series_referential_integrity(context, duckdb, staging_s3_path)

    # Step 2: Insert into metaSeries using SQL with LEFT JOIN-based ID resolution
    _insert_meta_series_from_staging(context, duckdb, full_s3_path)

    # Step 3: Fetch results (series_code -> series_id mapping)
    results = _fetch_meta_series_results(context, duckdb)
    context.log.info(f"Loaded {len(results)} meta series records")
    return results


def _filter_staging_columns(df: pd.DataFrame, staging_columns: List[str]) -> pd.DataFrame:
    """Filter DataFrame to only include available staging columns.

    Args:
        df: Polars DataFrame with data
        staging_columns: List of column names expected in staging data

    Returns:
        Filtered DataFrame with only available staging columns

    Raises:
        CSVValidationError: If no staging columns are available
    """
    available_columns = [col for col in staging_columns if col in df.columns]
    if not available_columns:
        raise CSVValidationError(
            f"CSV DataFrame must contain at least one of these columns: {staging_columns}"
        )
    return df[available_columns]


def _register_temp_table_for_staging(duckdb: DuckDBResource, df_staging: pd.DataFrame) -> str:
    """Register DataFrame as temporary table in DuckDB.

    Args:
        duckdb: DuckDB resource
        df_staging: Pandas DataFrame to register

    Returns:
        Temporary table name
    """
    temp_table = f"_temp_staging_{uuid.uuid4().hex}"
    duckdb.register_dataframe(temp_table, df_staging)
    return temp_table


def _unregister_temp_table(duckdb: DuckDBResource, temp_table: str) -> None:
    """Unregister temporary table from DuckDB.

    Args:
        duckdb: DuckDB resource
        temp_table: Temporary table name to unregister
    """
    duckdb.unregister_dataframe(temp_table)


def load_csv_to_staging_s3(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    df: pd.DataFrame,
    staging_type: str,
    staging_columns: List[str],
) -> str:
    """Load CSV data to S3 Parquet file as staging data.

    Uses DuckDBResource.save() method to write staging data to S3 Parquet files.
    Returns the S3 path (relative to bucket) for the staging file.

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        df: Polars DataFrame with data
        staging_type: Type of staging data (e.g., "meta_series", "lookup_tables")
        staging_columns: List of column names expected in staging data

    Returns:
        Relative S3 path to the staging Parquet file

    Raises:
        CSVValidationError: If no staging columns are available
        DatabaseQueryError: If save operation fails
    """
    # Filter DataFrame to only include staging columns that exist
    df_staging = _filter_staging_columns(df, staging_columns)

    # Generate unique S3 path for staging data
    timestamp = int(time.time())
    unique_id = uuid.uuid4().hex[:8]
    relative_path = build_s3_staging_path(staging_type, timestamp, unique_id)

    context.log.info(
        f"Saving {len(df_staging)} rows to S3 staging",
        extra={"staging_path": relative_path, "row_count": len(df_staging)},
    )

    # Register DataFrame in DuckDB connection
    temp_table = _register_temp_table_for_staging(duckdb, df_staging)

    try:
        # Use helper function to save to S3 Parquet
        select_query = f"SELECT * FROM {temp_table}"
        save_to_s3_parquet(
            duckdb=duckdb,
            relative_path=relative_path,
            select_query=select_query,
            context=context,
        )
        context.log.info(
            f"Successfully saved {len(df_staging)} rows to S3 staging",
            extra={"staging_path": relative_path, "row_count": len(df_staging)},
        )
        return relative_path
    finally:
        # Clean up temp table
        _unregister_temp_table(duckdb, temp_table)


def _build_lookup_insert_sql(
    lookup_type: str,
    table_name: str,
    id_column: str,
    name_column: str,
    staging_column: str,
    full_s3_path: str,
) -> str:
    """Build SQL query for inserting lookup table data from S3 Parquet staging.

    Args:
        lookup_type: Type of lookup table
        table_name: Target table name
        id_column: ID column name
        name_column: Name column name
        staging_column: Staging column name
        full_s3_path: Full S3 path to staging file

    Returns:
        SQL query string for inserting lookup data
    """
    if lookup_type in CODE_BASED_LOOKUPS:
        code_field, name_field, check_field = CODE_BASED_LOOKUPS[lookup_type]
        insert_fields = f"{id_column}, {code_field}, {name_field}"
        select_fields = f"{staging_column} AS {code_field}, {staging_column} AS {name_field}"
    else:
        insert_fields = f"{id_column}, {name_column}"
        select_fields = f"{staging_column} AS {name_column}"
        check_field = name_column

    return f"""
    INSERT INTO {table_name} ({insert_fields}, created_at, updated_at)
    SELECT
        (SELECT COALESCE(MAX({id_column}), 0) FROM {table_name}) +
        row_number() OVER (ORDER BY {staging_column}) AS {id_column},
        {select_fields},
        CURRENT_TIMESTAMP AS created_at,
        CURRENT_TIMESTAMP AS updated_at
    FROM (
        SELECT DISTINCT {staging_column}
        FROM read_parquet('{full_s3_path}')
        WHERE {staging_column} IS NOT NULL
            AND {staging_column} != ''
            AND {staging_column} NOT IN (SELECT {check_field} FROM {table_name})
    )
    ORDER BY {staging_column}
    """


def _insert_lookup_table_from_staging(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    lookup_type: str,
    full_s3_path: str,
) -> None:
    """Insert lookup table data from S3 Parquet staging into dimension table.

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        lookup_type: Type of lookup table
        full_s3_path: Full S3 path to staging file

    Raises:
        DatabaseInsertError: If insertion fails
    """
    table_name = DB_TABLES[lookup_type]
    id_column, name_column = DB_COLUMNS[lookup_type]
    staging_column = lookup_type

    sql = _build_lookup_insert_sql(
        lookup_type=lookup_type,
        table_name=table_name,
        id_column=id_column,
        name_column=name_column,
        staging_column=staging_column,
        full_s3_path=full_s3_path,
    )

    try:
        duckdb.execute_command(sql)
        context.log.info(
            f"Successfully inserted {lookup_type} from S3 Parquet staging to {table_name}"
        )
    except Exception as e:
        error_msg = f"Error inserting {lookup_type} from S3 Parquet staging: {e}"
        context.log.error(error_msg)
        raise DatabaseInsertError(error_msg) from e


def _fetch_lookup_table_results(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    lookup_type: str,
) -> Dict[str, int]:
    """Fetch lookup table results (name -> id mapping).

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        lookup_type: Type of lookup table

    Returns:
        Dictionary mapping name -> id

    Raises:
        DatabaseQueryError: If query fails
    """
    table_name = DB_TABLES[lookup_type]
    id_column, name_column = DB_COLUMNS[lookup_type]

    query = f"SELECT {id_column}, {name_column} FROM {table_name} ORDER BY {id_column}"
    try:
        df = duckdb.execute_query(query)
        results = {}
        if not df.empty:
            for _, row in df.iterrows():
                results[row[name_column]] = row[id_column]
        return results
    except Exception as e:
        error_msg = f"Error fetching {lookup_type} results: {e}"
        context.log.error(error_msg)
        raise DatabaseQueryError(error_msg) from e


def _process_single_lookup_table(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    lookup_type: str,
    full_s3_path: str,
) -> Dict[str, int]:
    """Process a single lookup table from S3 Parquet staging to dimension table.

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        lookup_type: Type of lookup table
        full_s3_path: Full S3 path to staging file

    Returns:
        Dictionary mapping name -> id

    Raises:
        DatabaseInsertError: If insertion fails
        DatabaseQueryError: If query fails
    """
    table_name = DB_TABLES[lookup_type]
    context.log.info(
        f"Processing {lookup_type} from S3 Parquet staging to {table_name}",
        extra={"lookup_type": lookup_type, "table_name": table_name},
    )

    _insert_lookup_table_from_staging(context, duckdb, lookup_type, full_s3_path)
    results = _fetch_lookup_table_results(context, duckdb, lookup_type)

    context.log.info(
        f"Loaded {len(results)} {lookup_type} records",
        extra={"lookup_type": lookup_type, "record_count": len(results)},
    )
    return results


def process_staging_to_dimensions(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    df: pd.DataFrame,
) -> Dict[str, Dict[str, int]]:
    """Process lookup tables from S3 Parquet staging to dimension tables using SQL.

    This function implements the staging → dimensions flow with deterministic sequential IDs.
    Data flow: CSV → S3 Parquet (staging) → dimension_tables (DuckDB)
    Reads from S3 Parquet files using read_parquet().

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        df: Polars DataFrame with lookup table data

    Returns:
        Dictionary mapping lookup_type -> {name: id}

    Raises:
        DatabaseInsertError: If insertion fails
        DatabaseQueryError: If query fails
    """
    context.log.info("Processing lookup tables from S3 Parquet staging to dimensions using SQL")

    # Step 1: Save CSV data to S3 Parquet as staging
    staging_s3_path = load_csv_to_staging_s3(
        context, duckdb, df, S3_STAGING_LOOKUP_TABLES, LOOKUP_TABLE_COLUMNS
    )

    # Get full S3 path for querying
    bucket = duckdb.get_bucket()
    full_s3_path = build_full_s3_path(bucket, staging_s3_path)

    all_results: Dict[str, Dict[str, int]] = {}
    available_columns = [col for col in LOOKUP_TABLE_COLUMNS if col in df.columns]

    # Step 2: Process independent lookup types first (in dependency order)
    for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
        if lookup_type not in available_columns:
            continue

        try:
            results = _process_single_lookup_table(context, duckdb, lookup_type, full_s3_path)
            all_results[lookup_type] = results
        except (DatabaseInsertError, DatabaseQueryError):
            raise  # Re-raise domain-specific exceptions
        except Exception as e:
            error_msg = f"Unexpected error processing {lookup_type}: {e}"
            context.log.error(error_msg)
            raise DatabaseInsertError(error_msg) from e

    return all_results
