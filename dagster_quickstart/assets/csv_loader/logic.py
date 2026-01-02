"""CSV loading logic for lookup tables and meta series with validation."""

from typing import Dict

import polars as pl
from dagster import AssetExecutionContext

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.constants import (
    DB_COLUMNS,
    DB_TABLES,
    LOOKUP_TABLE_PROCESSING_ORDER,
    META_SERIES_REQUIRED_COLUMNS,
    NULL_VALUE_REPRESENTATION,
)
from dagster_quickstart.utils.exceptions import CSVValidationError
from dagster_quickstart.utils.helpers import read_csv_safe, validate_csv_columns

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

    # Validate required columns
    validate_csv_columns(df, META_SERIES_REQUIRED_COLUMNS, config.csv_path)

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
        "field_type_id",
        "asset_class_id",
        "sub_asset_class_id",
        "product_type_id",
        "data_type_id",
        "structure_type_id",
        "market_segment_id",
        "ticker_source_id",
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

    # Step 2: Insert into metaSeries using SQL with LEFT JOIN-based ID resolution
    # Resolve string values to IDs using LEFT JOINs with lookup tables
    # Note: CSV may have both direct IDs (field_type_id) and string values (region)
    # We'll use LEFT JOINs to resolve string lookups, keeping direct IDs if provided
    # ClickHouse doesn't support LIMIT in correlated subqueries, so we use JOINs instead
    sql = """
    INSERT INTO metaSeries (
        series_id, series_name, series_code, data_source,
        field_type_id, asset_class_id, sub_asset_class_id, product_type_id,
        data_type_id, structure_type_id, market_segment_id, ticker_source_id,
        ticker, region_id, currency_id, term_id, tenor_id, country_id,
        calculation_formula, description, is_active, created_at, updated_at, created_by
    )
    SELECT
        (SELECT if(max(series_id) IS NULL, 0, max(series_id)) FROM metaSeries) +
        row_number() OVER (ORDER BY st.series_code) AS series_id,
        st.series_name,
        st.series_code,
        st.data_source,
        st.field_type_id,
        st.asset_class_id,
        st.sub_asset_class_id,
        st.product_type_id,
        st.data_type_id,
        st.structure_type_id,
        st.market_segment_id,
        st.ticker_source_id,
        st.ticker,
        -- Resolve region: use LEFT JOIN if string provided
        if(st.region IS NOT NULL AND st.region != '', r.region_id, NULL) AS region_id,
        -- Resolve currency: use LEFT JOIN if string provided
        if(st.currency IS NOT NULL AND st.currency != '', c.currency_id, NULL) AS currency_id,
        -- Resolve term: use LEFT JOIN if string provided
        if(st.term IS NOT NULL AND st.term != '', t.term_id, NULL) AS term_id,
        -- Resolve tenor: use LEFT JOIN if string provided
        if(st.tenor IS NOT NULL AND st.tenor != '', tn.tenor_id, NULL) AS tenor_id,
        -- Resolve country: use LEFT JOIN if string provided
        if(st.country IS NOT NULL AND st.country != '', co.country_id, NULL) AS country_id,
        st.calculation_formula,
        st.description,
        -- Parse is_active: convert string to UInt8 (default to 1 if not provided)
        if(st.is_active IS NULL OR st.is_active = '',
           1,
           if(lower(st.is_active) IN ('1', 'true', 'yes', 'y', 'active'), 1, 0)) AS is_active,
        now64(6) AS created_at,
        now64(6) AS updated_at,
        'csv_loader' AS created_by
    FROM staging_meta_series st
    LEFT JOIN regionLookup r ON st.region = r.region_name
    LEFT JOIN currencyLookup c ON st.currency = c.currency_code
    LEFT JOIN termLookup t ON st.term = t.term_name
    LEFT JOIN tenorLookup tn ON st.tenor = tn.tenor_code
    LEFT JOIN countryLookup co ON st.country = co.country_code
    WHERE st.series_code IS NOT NULL
        AND st.series_code != ''
        AND st.series_code NOT IN (SELECT series_code FROM metaSeries)
    ORDER BY st.series_code
    """

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
            if lookup_type in ["asset_class", "data_type", "structure_type", "market_segment", "region", "term", "product_type", "field_type"]:
                insert_fields = f"{id_column}, {name_column}"
                select_fields = f"{staging_column} AS {name_column}"
                check_field = name_column
            elif lookup_type == "ticker_source":
                insert_fields = f"{id_column}, {name_column}, ticker_source_code"
                select_fields = f"{staging_column} AS {name_column}, {staging_column} AS ticker_source_code"
                check_field = name_column  # Check against ticker_source_name
            else:  # currency, tenor, country
                code_field_map = {
                    "currency": ("currency_code", "currency_name"),
                    "tenor": ("tenor_code", "tenor_name"),
                    "country": ("country_code", "country_name"),
                }
                code_field, name_field = code_field_map[lookup_type]
                insert_fields = f"{id_column}, {code_field}, {name_field}"
                select_fields = f"{staging_column} AS {code_field}, {staging_column} AS {name_field}"
                check_field = code_field
            
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

            # Fetch results (name -> id mapping)
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

    # Step 3: Process sub_asset_class last (depends on asset_class)
    if "sub_asset_class" in available_columns and "asset_class" in available_columns:
        context.log.info("Processing sub_asset_class (requires asset_class mapping)")

        # Get asset_class mapping
        asset_class_mapping = all_results.get("asset_class", {})
        if not asset_class_mapping:
            # Fetch asset_class mapping if not already loaded
            query = "SELECT asset_class_id, asset_class_name FROM assetClassLookup"
            result = clickhouse.execute_query(query)
            asset_class_mapping = {}
            if hasattr(result, "result_rows") and result.result_rows:
                for row in result.result_rows:
                    asset_class_mapping[row[1]] = row[0]

        # Build SQL for sub_asset_class with asset_class_id
        sql = """
        INSERT INTO subAssetClassLookup (sub_asset_class_id, sub_asset_class_name, asset_class_id, created_at, updated_at)
        SELECT
            (SELECT if(max(sub_asset_class_id) IS NULL, 0, max(sub_asset_class_id)) FROM subAssetClassLookup) +
            row_number() OVER (ORDER BY st.sub_asset_class, ac.asset_class_id) AS sub_asset_class_id,
            st.sub_asset_class AS sub_asset_class_name,
            ac.asset_class_id,
            now64(6) AS created_at,
            now64(6) AS updated_at
        FROM (
            SELECT DISTINCT sub_asset_class, asset_class
            FROM staging_lookup_tables
            WHERE sub_asset_class IS NOT NULL
                AND sub_asset_class != ''
                AND asset_class IS NOT NULL
                AND asset_class != ''
                AND (sub_asset_class, asset_class) NOT IN (
                    SELECT sub_asset_class_name, asset_class_name
                    FROM subAssetClassLookup s
                    JOIN assetClassLookup a ON s.asset_class_id = a.asset_class_id
                )
        ) st
        JOIN assetClassLookup ac ON st.asset_class = ac.asset_class_name
        ORDER BY st.sub_asset_class, ac.asset_class_id
        """

        try:
            clickhouse.execute_command(sql)

            # Fetch results
            query = "SELECT sub_asset_class_id, sub_asset_class_name FROM subAssetClassLookup ORDER BY sub_asset_class_id"
            result = clickhouse.execute_query(query)
            results = {}
            if hasattr(result, "result_rows") and result.result_rows:
                for row in result.result_rows:
                    results[row[1]] = row[0]

            all_results["sub_asset_class"] = results
            context.log.info(f"Loaded {len(results)} sub_asset_class records")

        except Exception as e:
            context.log.error(f"Error processing sub_asset_class: {e}")
            raise

    return all_results
