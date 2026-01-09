"""DuckDB IO Manager for Dagster assets using S3 Parquet datalake."""

from datetime import datetime
from typing import Any

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    get_dagster_logger,
    io_manager,
)

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    DEFAULT_BATCH_SIZE,
    S3_BASE_PATH_VALUE_DATA,
    SQL_READ_PARQUET_TEMPLATE,
)
from dagster_quickstart.utils.datetime_utils import (
    ensure_utc,
    utc_now_metadata,
)
from dagster_quickstart.utils.exceptions import (
    DatabaseQueryError,
)
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_path_for_series,
    load_from_s3_parquet,
    save_to_s3_parquet,
)
from database.models import TimeSeriesBatch, TimeSeriesValue

# Try to import polars, but make it optional
try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    pl = None  # type: ignore
    POLARS_AVAILABLE = False

logger = get_dagster_logger()


class DuckDBIOManager(ConfigurableIOManager):
    """IO Manager for storing and loading time-series data to/from S3 Parquet files.

    Uses DuckDB's httpfs extension to read/write Parquet files directly from S3.
    Data is organized by series_id in S3: s3://bucket/value-data/series_id={series_id}/data.parquet
    """

    duckdb: DuckDBResource
    s3_base_path: str = S3_BASE_PATH_VALUE_DATA  # Base path in S3 bucket for value data
    batch_size: int = DEFAULT_BATCH_SIZE

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Handle output from assets - save data to S3 Parquet files."""
        if obj is None:
            return

        # Handle Polars DataFrame - keep as Polars for better DuckDB integration
        if POLARS_AVAILABLE and isinstance(obj, pl.DataFrame):
            self._save_polars_dataframe(context, obj)
            return

        # Handle pandas DataFrame
        if isinstance(obj, pd.DataFrame):
            self._save_pandas_dataframe(context, obj)
        # Handle TimeSeriesBatch
        elif isinstance(obj, TimeSeriesBatch):
            self._save_timeseries_batch(context, obj)
        # Handle list of TimeSeriesValue
        elif isinstance(obj, list) and all(isinstance(value, TimeSeriesValue) for value in obj):
            batch = TimeSeriesBatch(series_id=obj[0].series_id, values=obj)
            self._save_timeseries_batch(context, batch)
        else:
            raise ValueError(f"Unsupported output type: {type(obj)}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load input data from S3 Parquet files using DuckDBResource's methods.

        Uses DuckDBResource.load() with SQL class bindings for S3 path resolution,
        and sql_to_string() for debugging when needed.

        Raises:
            ValueError: If series_id is not provided in metadata
            DatabaseQueryError: If query execution fails
        """
        series_id = context.metadata.get("series_id")
        start_time = context.metadata.get("start_time")
        end_time = context.metadata.get("end_time")

        if series_id is None:
            raise ValueError("series_id must be provided in metadata")

        # Get relative S3 path for this series (relative to bucket)
        relative_path = build_s3_path_for_series(series_id)

        # Build query with optional time filters
        where_parts = []
        if start_time:
            where_parts.append(f"timestamp >= '{start_time}'")
        if end_time:
            where_parts.append(f"timestamp <= '{end_time}'")

        where_clause = " AND ".join(where_parts) if where_parts else ""
        query_template = f"SELECT * FROM {SQL_READ_PARQUET_TEMPLATE}"
        if where_clause:
            query_template = f"{query_template} WHERE {where_clause}"
        query_template = f"{query_template} ORDER BY timestamp"

        try:
            df = load_from_s3_parquet(
                duckdb=self.duckdb,
                relative_path=relative_path,
                query_template=query_template,
                context=context,
            )

            if df is not None and not df.empty:
                return df
        except DatabaseQueryError as e:
            bucket = self.duckdb.get_bucket()
            full_s3_path = build_full_s3_path(bucket, relative_path)
            context.log.warning(
                f"Could not load data from S3 path",
                extra={"s3_path": full_s3_path, "series_id": series_id, "error": str(e)},
            )

        # Return empty DataFrame with expected columns
        return pd.DataFrame(columns=["series_id", "timestamp", "value", "created_at", "updated_at"])

    def _normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame for saving to Parquet.

        Args:
            df: Input DataFrame

        Returns:
            Normalized DataFrame with proper columns and timestamps
        """
        required_columns = ["series_id", "timestamp", "value"]
        if not all(col in df.columns for col in required_columns):
            raise ValueError(
                f"DataFrame must contain columns: {required_columns}. Found: {df.columns.tolist()}"
            )

        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Normalize timestamps to UTC
        def normalize_timestamp(ts: Any) -> datetime:
            """Normalize a timestamp to UTC."""
            if hasattr(ts, "to_pydatetime"):
                dt = ts.to_pydatetime()
            elif isinstance(ts, datetime):
                dt = ts
            else:
                dt = pd.to_datetime(ts).to_pydatetime()
            return ensure_utc(dt)

        df["timestamp"] = df["timestamp"].apply(normalize_timestamp)

        # Sort by timestamp
        df = df.sort_values("timestamp")

        # Add metadata columns if not present
        now = utc_now_metadata()
        if "created_at" not in df.columns:
            df["created_at"] = now
        if "updated_at" not in df.columns:
            df["updated_at"] = now

        # Select only the columns we need
        return df[["series_id", "timestamp", "value", "created_at", "updated_at"]]

    def _save_pandas_dataframe(self, context: OutputContext, df: pd.DataFrame) -> None:
        """Save pandas DataFrame to S3 Parquet file."""
        df_normalized = self._normalize_dataframe(df)

        # Group by series_id and save each series to its own Parquet file
        for series_id, group_df in df_normalized.groupby("series_id"):
            self._save_series_to_s3(context, series_id, group_df)

    def _save_polars_dataframe(self, context: OutputContext, df: pl.DataFrame) -> None:
        """Save Polars DataFrame to S3 Parquet file."""
        # Convert to pandas for consistency, or use Polars directly with DuckDB
        df_pandas = df.to_pandas()
        self._save_pandas_dataframe(context, df_pandas)

    def _save_timeseries_batch(self, context: OutputContext, batch: TimeSeriesBatch) -> None:
        """Save TimeSeriesBatch to S3 Parquet file."""
        # Convert batch to DataFrame
        data = []
        now = utc_now_metadata()

        for value in batch.values:
            normalized_timestamp = ensure_utc(value.timestamp)
            data.append(
                {
                    "series_id": value.series_id,
                    "timestamp": normalized_timestamp,
                    "value": value.value,
                    "created_at": now,
                    "updated_at": now,
                }
            )

        df = pd.DataFrame(data)
        self._save_series_to_s3(context, batch.series_id, df)

    def _merge_with_existing_data(
        self, context: OutputContext, series_id: int, relative_path: str, df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge new data with existing data from S3 Parquet file for idempotency.

        Args:
            context: Dagster output context
            series_id: Series ID
            relative_path: Relative S3 path to existing file
            df: New DataFrame to merge

        Returns:
            Merged DataFrame with deduplicated data
        """
        query_template = f"SELECT * FROM {SQL_READ_PARQUET_TEMPLATE}"

        try:
            existing_df = load_from_s3_parquet(
                duckdb=self.duckdb,
                relative_path=relative_path,
                query_template=query_template,
                context=context,
            )

            if existing_df is not None and not existing_df.empty:
                # Merge: combine existing and new, deduplicate by series_id + timestamp
                merge_df = pd.concat([existing_df, df], ignore_index=True)
                # Remove duplicates, keeping most recent based on created_at
                merge_df = merge_df.sort_values("created_at", ascending=False)
                merge_df = merge_df.drop_duplicates(subset=["series_id", "timestamp"], keep="first")
                # Sort by timestamp for proper ordering
                merge_df = merge_df.sort_values("timestamp")
                context.log.info(
                    f"Merged with existing data for series_id={series_id}",
                    extra={"series_id": series_id, "total_rows": len(merge_df)},
                )
                return merge_df
        except DatabaseQueryError:
            # File doesn't exist yet, use new data as-is
            context.log.debug(
                f"No existing file for series_id={series_id}, creating new file",
                extra={"series_id": series_id},
            )

        return df

    def _save_series_to_s3(self, context: OutputContext, series_id: int, df: pd.DataFrame) -> None:
        """Save a DataFrame for a specific series to S3 Parquet file.

        Uses DuckDBResource.save() method which leverages duckdb_datacacher's S3 capabilities.
        Uses sql_to_string() for debugging when needed.
        Merges with existing data if file exists for idempotency, otherwise creates new file.

        Raises:
            DatabaseInsertError: If save operation fails
        """
        if df.empty:
            context.log.warning(
                f"No data to save for series_id={series_id}",
                extra={"series_id": series_id},
            )
            return

        # Get relative S3 path for this series (relative to bucket for save method)
        relative_path = build_s3_path_for_series(series_id)

        # Try to merge with existing data if file exists (for idempotency)
        df = self._merge_with_existing_data(context, series_id, relative_path, df)

        # Register DataFrame in DuckDB connection as temp table
        temp_table = f"_temp_series_{series_id}"
        self.duckdb.register_dataframe(temp_table, df)

        try:
            # Use helper function to save to S3 Parquet
            select_query = f"SELECT * FROM {temp_table}"
            save_to_s3_parquet(
                duckdb=self.duckdb,
                relative_path=relative_path,
                select_query=select_query,
                context=context,
            )
            context.log.info(
                f"Saved {len(df)} rows for series_id={series_id} to S3",
                extra={"series_id": series_id, "row_count": len(df), "s3_path": relative_path},
            )
        finally:
            # Clean up temp table
            self.duckdb.unregister_dataframe(temp_table)


@io_manager(required_resource_keys={"duckdb"})
def duckdb_io_manager(context) -> DuckDBIOManager:
    """Factory function for DuckDB IO Manager with S3 Parquet datalake."""
    return DuckDBIOManager(
        duckdb=context.resources.duckdb,
        s3_base_path=S3_BASE_PATH_VALUE_DATA,
        batch_size=DEFAULT_BATCH_SIZE,
    )
