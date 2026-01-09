"""CSV-based loading assets for lookup tables and meta series with validation.

Uses S3 Parquet files as the datalake for staging data, then processes into DuckDB tables.
CSV data is saved to S3 Parquet staging files, then read from S3 and processed into DuckDB tables.
"""

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    LOOKUP_TABLE_COLUMNS,
    RETRY_POLICY_DELAY_CSV_LOADER,
    RETRY_POLICY_MAX_RETRIES_CSV_LOADER,
)
from dagster_quickstart.utils.datetime_utils import utc_now
from dagster_quickstart.utils.exceptions import CSVValidationError
from dagster_quickstart.utils.helpers import read_csv_safe

from .config import LookupTableCSVConfig, MetaSeriesCSVConfig
from .logic import (
    load_meta_series_logic,
    process_staging_to_dimensions,
)


@asset(
    group_name="metadata",
    description="Initialize database schema - create all required DuckDB tables",
    io_manager_key="duckdb_io_manager",
    kinds=["duckdb"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "architecture": "s3-parquet-datalake"},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def init_database_schema(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    """Initialize database schema using duckup migrations.

    This asset ensures the DuckDB database exists and runs all pending migrations
    to set up the database schema. This is required before loading lookup tables
    and meta series, which will be stored in DuckDB tables (not S3).

    Note: While time-series value data is stored in S3 Parquet files via the
    duckdb_io_manager, metadata tables (lookup tables, metaSeries) are stored
    in DuckDB tables for fast lookups and joins.

    Returns:
        DataFrame with status and timestamp for tracking
    """
    context.log.info("Initializing DuckDB schema using duckup migrations...")

    try:
        # Ensure database exists (DuckDB with duckdb_datacacher handles this)
        duckdb.ensure_database()
        context.log.info("DuckDB connection ensured")

        # Run migrations to create all required tables
        duckdb.run_migrations()
        context.log.info("DuckDB migrations applied successfully")
    except Exception as e:
        context.log.error(f"Error initializing schema with duckup migrations: {e}")
        raise

    context.log.info("Database schema initialized successfully")
    # Return status DataFrame for tracking/io_manager
    return pd.DataFrame({"status": ["Schema initialized"], "timestamp": [utc_now()]})


@asset(
    group_name="metadata",
    description="Load lookup tables from CSV with validation - uses S3 Parquet staging",
    deps=[AssetKey("init_database_schema")],  # Schema must be initialized first
    io_manager_key="duckdb_io_manager",
    kinds=["csv", "duckdb", "s3"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "architecture": "s3-parquet-datalake"},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def load_lookup_tables_from_csv(
    context: AssetExecutionContext,
    config: LookupTableCSVConfig,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    """Load lookup tables from CSV file with validation.

    Data flow: CSV → S3 Parquet (staging) → DuckDB dimension tables
    - CSV data is saved to S3 Parquet staging files using DuckDBResource.save()
    - Staging data is read from S3 using read_parquet() and processed into DuckDB tables
    - Final lookup tables are stored in DuckDB for fast lookups and joins

    Supports two CSV formats:
    1. Wide format: Columns are lookup table types (asset_class, product_type, etc.)
       When in wide format and lookup_table_type is "all", processes all lookup tables.
    2. Long format: Has lookup_table_type and name columns

    Args:
        context: Dagster execution context
        config: Lookup table CSV configuration
        duckdb: DuckDB resource with S3 access via httpfs

    Returns:
        DataFrame with lookup table results (name -> id mappings)
    """
    context.log.info(
        f"Loading {config.lookup_table_type} from {config.csv_path}",
        extra={"lookup_table_type": config.lookup_table_type, "csv_path": config.csv_path},
    )
    context.log.info("Using S3 Parquet staging architecture")

    # Load lookup table data from CSV
    df = read_csv_safe(config.csv_path)

    available_columns = [col for col in LOOKUP_TABLE_COLUMNS if col in df.columns]

    if not available_columns:
        raise CSVValidationError(
            f"CSV file {config.csv_path} must have lookup table columns: {LOOKUP_TABLE_COLUMNS}"
        )

    # Process all lookup tables using staging → dimensions flow
    # This saves CSV data to S3 Parquet staging, then reads from S3 to process into DuckDB
    context.log.info(
        "Detected wide format with columns",
        extra={"available_columns": available_columns},
    )
    context.log.info(
        "Using S3 Parquet staging → DuckDB dimensions flow with deterministic ID generation"
    )
    all_results = process_staging_to_dimensions(context, duckdb, df)

    # Return results based on requested type
    if config.lookup_table_type != "all" and config.lookup_table_type in all_results:
        results = all_results[config.lookup_table_type]
        # Convert dictionary to pandas DataFrame
        result_df = pd.DataFrame(
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
        result_df = pd.DataFrame(rows)
        context.add_output_metadata(
            {
                "lookups_loaded": MetadataValue.int(total_loaded),
                "lookup_table_type": MetadataValue.text("all"),
                "details": MetadataValue.json(all_results),
            }
        )
        return result_df
    else:
        raise CSVValidationError(
            f"lookup_table_type '{config.lookup_table_type}' not found in CSV columns. "
            f"Available columns: {available_columns}"
        )


@asset(
    group_name="metadata",
    description="Load meta series from CSV file - uses S3 Parquet staging",
    deps=[
        AssetKey("init_database_schema"),  # Schema must be initialized first
        AssetKey("load_lookup_tables_from_csv"),  # Depends on lookup tables being loaded first
    ],
    io_manager_key="duckdb_io_manager",
    kinds=["csv", "duckdb", "s3"],
    owners=["team:mqrm-data-eng"],
    tags={"m360-mqrm": "", "architecture": "s3-parquet-datalake"},
    retry_policy=RetryPolicy(
        max_retries=RETRY_POLICY_MAX_RETRIES_CSV_LOADER, delay=RETRY_POLICY_DELAY_CSV_LOADER
    ),
)
def load_meta_series_from_csv(
    context: AssetExecutionContext,
    config: MetaSeriesCSVConfig,
    duckdb: DuckDBResource,
) -> pd.DataFrame:
    """Load meta series from CSV file using S3 Parquet staging.

    Data flow: CSV → S3 Parquet (staging) → DuckDB metaSeries table
    - CSV data is saved to S3 Parquet staging files using DuckDBResource.save()
    - Staging data is read from S3 using DuckDBResource.load() and read_parquet()
    - Validates referential integrity with lookup tables in DuckDB
    - Final meta series records are stored in DuckDB metaSeries table

    Args:
        context: Dagster execution context
        config: Meta series CSV configuration
        duckdb: DuckDB resource with S3 access via httpfs

    Returns:
        DataFrame with meta series results (series_code -> series_id mappings)
    """
    context.log.info("Using S3 Parquet staging architecture for meta series loading")
    result_df, results = load_meta_series_logic(context, config, duckdb)

    context.add_output_metadata(
        {
            "series_loaded": MetadataValue.int(len(result_df)),
            "details": MetadataValue.json(results),
        }
    )

    return result_df
