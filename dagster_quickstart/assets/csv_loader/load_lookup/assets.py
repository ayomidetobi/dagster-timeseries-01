"""CSV-based loading assets for lookup tables with validation.

Uses S3 Parquet files as the datalake. CSV data is validated and written to S3 Parquet
control tables (versioned, immutable). DuckDB views are created dynamically over S3
control tables for querying.
"""

import pandas as pd
from dagster import (
    AssetExecutionContext,
    RetryPolicy,
    asset,
)

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    RETRY_POLICY_DELAY_CSV_LOADER,
    RETRY_POLICY_MAX_RETRIES_CSV_LOADER,
    S3_CONTROL_LOOKUP,
    S3_PARQUET_FILE_NAME,
)
from dagster_quickstart.utils.exceptions import CSVValidationError
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_control_table_path,
    get_version_date,
)
from dagster_quickstart.utils.summary.csv_loader import add_csv_loader_summary_metadata

from .config import LookupTableCSVConfig
from .logic import process_csv_to_s3_control_table_lookup_tables


@asset(
    group_name="metadata",
    description="Load lookup tables from CSV with validation - uses S3 Parquet staging",
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

    Data flow: CSV → DuckDB temp view → validation → S3 Parquet control table (versioned)
    - CSV data is validated in DuckDB temp view
    - Validated data is written to S3 Parquet control tables (versioned, immutable)
    - DuckDB views are created/updated to point to latest S3 control table versions

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

    context.log.info(
        "Using CSV → S3 control table flow (versioned, immutable control-plane tables)"
    )
    all_results = process_csv_to_s3_control_table_lookup_tables(context, duckdb, config.csv_path)

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
        # Get version date and S3 path for metadata
        version_date = get_version_date()
        bucket = duckdb.get_bucket()
        relative_path = build_s3_control_table_path(
            S3_CONTROL_LOOKUP, version_date, S3_PARQUET_FILE_NAME
        )
        s3_control_table_path = build_full_s3_path(bucket, relative_path)

        # Add AssetSummary metadata
        add_csv_loader_summary_metadata(
            context=context,
            records_loaded=len(result_df),
            record_type="lookup_tables",
            details={config.lookup_table_type: results},
            csv_path=config.csv_path,
            version_date=version_date,
            s3_control_table_path=s3_control_table_path,
            lookup_table_type=config.lookup_table_type,
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
        # Get version date and S3 path for metadata
        version_date = get_version_date()
        bucket = duckdb.get_bucket()
        relative_path = build_s3_control_table_path(
            S3_CONTROL_LOOKUP, version_date, S3_PARQUET_FILE_NAME
        )
        s3_control_table_path = build_full_s3_path(bucket, relative_path)

        # Add AssetSummary metadata
        add_csv_loader_summary_metadata(
            context=context,
            records_loaded=total_loaded,
            record_type="lookup_tables",
            details=all_results,
            csv_path=config.csv_path,
            version_date=version_date,
            s3_control_table_path=s3_control_table_path,
            lookup_table_type="all",
        )
        return result_df
    else:
        raise CSVValidationError(
            f"lookup_table_type '{config.lookup_table_type}' not found in CSV. "
            f"Available lookup types: {list[str](all_results.keys())}"
        )
