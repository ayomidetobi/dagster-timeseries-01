"""CSV loading logic for lookup tables and meta series with validation."""

from typing import Dict

import polars as pl
from dagster import AssetExecutionContext

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.constants import (
    CODE_BASED_LOOKUPS,
    DB_COLUMNS,
    DB_TABLES,
    LOOKUP_TABLE_PROCESSING_ORDER,
    NULL_VALUE_REPRESENTATION,
)
from dagster_quickstart.utils.exceptions import CSVValidationError
from dagster_quickstart.utils.helpers import read_csv_safe
from database.referential_integrity import ReferentialIntegrityValidator
from database.schemas import INSERT_META_SERIES_FROM_STAGING_SQL

from .config import MetaSeriesCSVConfig


def load_meta_series_logic(
    context: AssetExecutionContext,
    config: MetaSeriesCSVConfig,
    clickhouse: ClickHouseResource,
) -> tuple[pl.DataFrame, Dict[str, int]]:
    """Load meta series from CSV file using staging → metaSeries flow.

    Args:
        context: Dagster execution context
        config: Meta series CSV configuration
        clickhouse: ClickHouse resource

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
    results = process_staging_to_meta_series(context, clickhouse, df)

    # Convert dictionary to Polars DataFrame
    result_df = pl.DataFrame(
        [
            {"series_code": series_code, "series_id": series_id}
            for series_code, series_id in results.items()
        ]
    )

    return result_df, results


def process_staging_to_meta_series(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    df: pl.DataFrame,
) -> Dict[str, int]:
    """Process meta series from staging table to metaSeries table using SQL.

    This function implements the staging → metaSeries flow with deterministic sequential IDs.
    Data flow: CSV → staging_meta_series → metaSeries
    Uses dictionaries to resolve string lookup values (region, currency, term, tenor, country) to IDs.

    Args:
        context: Dagster execution context
        clickhouse: ClickHouse resource
        df: Polars DataFrame with meta series data

    Returns:
        Dictionary mapping series_code -> series_id
    """
    context.log.info("Processing meta series from staging to metaSeries using SQL")

    # Step 1: Load CSV into staging table
    staging_columns = [
        "series_name",
        "series_code",
        "data_source",
        "field_type",
        "asset_class",
        "sub_asset_class",
        "product_type",
        "data_type",
        "structure_type",
        "market_segment",
        "ticker_source",
        "ticker",
        "region",
        "currency",
        "term",
        "tenor",
        "country",
        "valid_from",
        "valid_to",
        "calculation_formula",
        "description",
        "is_active",
    ]
    load_csv_to_staging_table(
        context, clickhouse, df, "staging_meta_series", staging_columns
    )

    # Step 1.5: Validate referential integrity before insertion
    context.log.info("Validating referential integrity for meta series references")
    validator = ReferentialIntegrityValidator(clickhouse)
    
    # Fetch staging data for validation
    query = """
    SELECT series_code, field_type, asset_class, sub_asset_class, product_type,
           data_type, structure_type, market_segment, ticker_source,
           region, currency, term, tenor, country
    FROM staging_meta_series
    WHERE series_code IS NOT NULL AND series_code != ''
    """
    result = clickhouse.execute_query(query)
    staging_data = []
    if hasattr(result, "result_rows") and result.result_rows:
        columns = ["series_code", "field_type", "asset_class", "sub_asset_class", "product_type",
                   "data_type", "structure_type", "market_segment", "ticker_source",
                   "region", "currency", "term", "tenor", "country"]
        for row in result.result_rows:
            staging_data.append(dict(zip(columns, row)))
    
    validator.validate_meta_series_references(context, staging_data)

    # Step 2: Insert into metaSeries using SQL with LEFT JOIN-based ID resolution
    # Resolve all lookup name values to IDs using LEFT JOINs with lookup tables
    # All lookup values come as names from CSV and are resolved to IDs
    sql = INSERT_META_SERIES_FROM_STAGING_SQL

    try:
        clickhouse.execute_command(sql)
        context.log.info("Successfully inserted meta series from staging to metaSeries")

        # Fetch results (series_code -> series_id mapping)
        query = "SELECT series_id, series_code FROM metaSeries ORDER BY series_id"
        result = clickhouse.execute_query(query)
        results = {}
        if hasattr(result, "result_rows") and result.result_rows:
            for row in result.result_rows:
                results[row[1]] = row[0]

        context.log.info(f"Loaded {len(results)} meta series records")
        return results

    except Exception as e:
        context.log.error(f"Error processing meta series from staging: {e}")
        raise


def load_csv_to_staging_table(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    df: pl.DataFrame,
    staging_table_name: str,
    staging_columns: list[str],
) -> None:
    """Load CSV data into a staging table (reusable function).

    Args:
        context: Dagster execution context
        clickhouse: ClickHouse resource
        df: Polars DataFrame with data
        staging_table_name: Name of the staging table (e.g., "staging_lookup_tables")
        staging_columns: List of column names expected in staging table
    """
    context.log.info(f"Truncating {staging_table_name} for fresh load")
    clickhouse.execute_command(f"TRUNCATE TABLE IF EXISTS {staging_table_name}")

    # Filter DataFrame to only include staging columns that exist
    available_columns = [col for col in staging_columns if col in df.columns]
    if not available_columns:
        raise CSVValidationError(
            f"CSV DataFrame must contain at least one of these columns: {staging_columns}"
        )

    df_staging = df.select(available_columns)

    # Convert to list of lists (rows) - clickhouse-connect expects list of lists/tuples
    # Each row is a list/tuple of values in the same order as available_columns
    data = df_staging.to_numpy().tolist()
    context.log.info(f"Inserting {len(data)} rows into {staging_table_name}")

    # Insert data using clickhouse-connect (works over HTTP)
    clickhouse.insert_data(
        table=staging_table_name,
        data=data,
        column_names=available_columns,
    )

    context.log.info(f"Successfully loaded {len(data)} rows into {staging_table_name}")




def process_staging_to_dimensions(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
    df: pl.DataFrame,
) -> Dict[str, Dict[str, int]]:
    """Process lookup tables from staging table to dimension tables using SQL.

    This function implements the staging → dimensions flow with deterministic sequential IDs.
    Data flow: CSV → staging_lookup_tables → dimension_tables

    Args:
        context: Dagster execution context
        clickhouse: ClickHouse resource
        df: Polars DataFrame with lookup table data

    Returns:
        Dictionary mapping lookup_type -> {name: id}
    """
    from dagster_quickstart.utils.constants import LOOKUP_TABLE_COLUMNS

    context.log.info("Processing lookup tables from staging to dimensions using SQL")

    # Step 1: Load CSV into staging table
    staging_columns = [
        "asset_class",
        "product_type",
        "sub_asset_class",
        "data_type",
        "structure_type",
        "market_segment",
        "field_type",
        "ticker_source",
        "country",
        "currency",
        "region",
        "term",
        "tenor",
    ]
    load_csv_to_staging_table(
        context, clickhouse, df, "staging_lookup_tables", staging_columns
    )

    all_results: Dict[str, Dict[str, int]] = {}
    available_columns = [col for col in LOOKUP_TABLE_COLUMNS if col in df.columns]

    # Step 2: Process independent lookup types first (in dependency order)
    for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
        if lookup_type not in available_columns:
            continue

        staging_column = lookup_type
        table_name = DB_TABLES[lookup_type]
        id_column, name_column = DB_COLUMNS[lookup_type]

        context.log.info(f"Processing {lookup_type} from staging to {table_name}")

        try:
            # Determine insert fields and select fields based on lookup type
            # Simple lookups: just id, name, timestamps
            # Code-based lookups: id, name/code fields, timestamps
            if lookup_type in CODE_BASED_LOOKUPS:
                code_field, name_field, check_field = CODE_BASED_LOOKUPS[lookup_type]
                insert_fields = f"{id_column}, {code_field}, {name_field}"
                select_fields = f"{staging_column} AS {code_field}, {staging_column} AS {name_field}"
            else:  # Simple lookups: asset_class, data_type, structure_type, etc.
                insert_fields = f"{id_column}, {name_column}"
                select_fields = f"{staging_column} AS {name_column}"
                check_field = name_column
            
            # Generate and execute SQL (same pattern for all lookup types)
            sql = f"""
            INSERT INTO {table_name} ({insert_fields}, created_at, updated_at)
            SELECT
                (SELECT if(max({id_column}) IS NULL, 0, max({id_column})) FROM {table_name}) +
                row_number() OVER (ORDER BY {staging_column}) AS {id_column},
                {select_fields},
                now64(6) AS created_at,
                now64(6) AS updated_at
            FROM (
                SELECT DISTINCT {staging_column}
                FROM staging_lookup_tables
                WHERE {staging_column} IS NOT NULL
                    AND {staging_column} != ''
                    AND {staging_column} NOT IN (SELECT {check_field} FROM {table_name})
            )
            ORDER BY {staging_column}
            """
            clickhouse.execute_command(sql)

            query = f"SELECT {id_column}, {name_column} FROM {table_name} ORDER BY {id_column}"
            result = clickhouse.execute_query(query)
            results = {}
            if hasattr(result, "result_rows") and result.result_rows:
                for row in result.result_rows:
                    lookup_id, lookup_name = row[0], row[1]
                    results[lookup_name] = lookup_id

            all_results[lookup_type] = results
            context.log.info(f"Loaded {len(results)} {lookup_type} records")

        except Exception as e:
            context.log.error(f"Error processing {lookup_type}: {e}")
            raise

    return all_results
