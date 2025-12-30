"""CSV-based loading assets for lookup tables and meta series with validation."""

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.constants import (
    LOOKUP_TABLE_COLUMNS,
    RETRY_POLICY_DELAY_CSV_LOADER,
    RETRY_POLICY_MAX_RETRIES_CSV_LOADER,
)
from dagster_quickstart.utils.datetime_utils import utc_now
from dagster_quickstart.utils.helpers import read_csv_safe
from database.lookup_tables import LookupTableManager

from .config import LookupTableCSVConfig, MetaSeriesCSVConfig
from .logic import (
    load_meta_series_logic,
    process_long_format_lookup,
    process_wide_format_lookup,
)


@asset(
    group_name="metadata",
    description="Initialize database schema - create all required tables",
    io_manager_key="polars_parquet_io_manager",
    kinds=["clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def init_database_schema(
    context: AssetExecutionContext,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Initialize database schema using clickhouse-migrations.

    This asset ensures the database exists and runs all pending migrations
    to set up the database schema.
    """
    context.log.info("Initializing database schema using clickhouse-migrations...")

    try:
        # Ensure database exists
        clickhouse.ensure_database()
        context.log.info("Database ensured")

        # Run migrations
        clickhouse.run_migrations()
        context.log.info("ClickHouse migrations applied successfully")
    except Exception as e:
        context.log.error(f"Error initializing schema with clickhouse-migrations: {e}")
        raise

    context.log.info("Database schema initialized successfully")
    # Return as DataFrame for polars_parquet_io_manager
    return pl.DataFrame({"status": ["Schema initialized"], "timestamp": [utc_now()]})


@asset(
    group_name="metadata",
    description="Load lookup tables from CSV with validation against allowed names",
    deps=[AssetKey("init_database_schema")],  # Schema must be initialized first
    io_manager_key="polars_parquet_io_manager",
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def load_lookup_tables_from_csv(
    context: AssetExecutionContext,
    config: LookupTableCSVConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Load lookup tables from CSV file with validation.

    Supports two CSV formats:
    1. Wide format: Columns are lookup table types (asset_class, product_type, etc.)
       When in wide format and lookup_table_type is "all", processes all lookup tables.
    2. Long format: Has lookup_table_type and name columns
    """
    from dagster_quickstart.utils.exceptions import CSVValidationError

    context.log.info(f"Loading {config.lookup_table_type} from {config.csv_path}")

    # Load lookup table data
    df = read_csv_safe(config.csv_path)

    lookup_manager = LookupTableManager(clickhouse)
    available_columns = [col for col in LOOKUP_TABLE_COLUMNS if col in df.columns]

    if available_columns:
        # Wide format: process all lookup tables
        context.log.info(f"Detected wide format with columns: {available_columns}")
        all_results = process_wide_format_lookup(
            context, lookup_manager, df, config, available_columns
        )

        # Return results based on requested type
        if config.lookup_table_type != "all" and config.lookup_table_type in all_results:
            results = all_results[config.lookup_table_type]
            # Convert dictionary to Polars DataFrame
            result_df = pl.DataFrame(
                [
                    {"lookup_table_type": config.lookup_table_type, "name": name, "id": id_val}
                    for name, id_val in results.items()
                ]
            )
            context.add_output_metadata(
                {
                    "lookups_loaded": MetadataValue.int(len(result_df)),
                    "lookup_table_type": MetadataValue.text(config.lookup_table_type),
                    "details": MetadataValue.json(results),
                }
            )
            return result_df
        elif config.lookup_table_type == "all":
            total_loaded = sum(len(v) for v in all_results.values())
            # Convert all results to a single DataFrame
            rows = []
            for lookup_type, type_results in all_results.items():
                for name, id_val in type_results.items():
                    rows.append({"lookup_table_type": lookup_type, "name": name, "id": id_val})
            result_df = pl.DataFrame(rows)
            context.add_output_metadata(
                {
                    "lookups_loaded": MetadataValue.int(total_loaded),
                    "lookup_table_type": MetadataValue.text("all"),
                    "details": MetadataValue.json(all_results),
                }
            )
            return result_df
        else:
            raise ValueError(
                f"lookup_table_type '{config.lookup_table_type}' not found in CSV columns. "
                f"Available columns: {available_columns}"
            )

    elif "name" in df.columns:
        # Long format: has lookup_table_type and name columns
        results = process_long_format_lookup(context, lookup_manager, df, config)
        # Convert dictionary to Polars DataFrame
        result_df = pl.DataFrame(
            [
                {"lookup_table_type": config.lookup_table_type, "name": name, "id": id_val}
                for name, id_val in results.items()
            ]
        )
        context.add_output_metadata(
            {
                "lookups_loaded": MetadataValue.int(len(result_df)),
                "lookup_table_type": MetadataValue.text(config.lookup_table_type),
                "details": MetadataValue.json(results),
            }
        )
        return result_df
    else:
        raise CSVValidationError(
            f"CSV file {config.csv_path} must have lookup table columns "
            f"(wide format: {LOOKUP_TABLE_COLUMNS}) or 'name' column (long format)"
        )


@asset(
    group_name="metadata",
    description="Load meta series from CSV file",
    deps=[
        AssetKey("init_database_schema"),  # Schema must be initialized first
        AssetKey("load_lookup_tables_from_csv"),  # Depends on lookup tables being loaded first
    ],
    io_manager_key="polars_parquet_io_manager",
    kinds=["csv", "clickhouse"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": ""},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def load_meta_series_from_csv(
    context: AssetExecutionContext,
    config: MetaSeriesCSVConfig,
    clickhouse: ClickHouseResource,
) -> pl.DataFrame:
    """Load meta series from CSV file."""
    result_df, results = load_meta_series_logic(context, config, clickhouse)

    context.add_output_metadata(
        {
            "series_loaded": MetadataValue.int(len(result_df)),
            "details": MetadataValue.json(results),
        }
    )

    return result_df
