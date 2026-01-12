"""DuckDB IO Manager for Dagster assets using S3 Parquet datalake.

Uses DuckDBResource's save() and load() methods which leverage duckdb_datacacher's
built-in S3 access. This is a simple wrapper that delegates to DuckDBResource.

Features:
- Value data handling via series_id metadata
- Generic asset handling via asset key
- Input filtering support (start_time/end_time)
- Safe metadata access
- Flexible input type conversion (Series, list -> DataFrame)
- Unique temp table names for parallel execution safety
"""

import uuid
from typing import Any, Optional, Union

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
    S3_BASE_PATH_VALUE_DATA,
    S3_PARQUET_FILE_NAME,
    SQL_READ_PARQUET_TEMPLATE,
)
from dagster_quickstart.utils.helpers import (
    create_sql_query_with_file_path,
)

logger = get_dagster_logger()


class DuckDBIOManager(ConfigurableIOManager):
    """IO Manager for storing and loading data to/from S3 Parquet files.

    Uses DuckDBResource's save() and load() methods which leverage duckdb_datacacher's
    built-in S3 access. DuckDBResource already has S3 bucket access configured.

    For value data: Uses series_id from metadata to build path
    For other data: Uses asset key to build path

    Attributes:
        duckdb: DuckDB resource with S3 access configured
        s3_base_path: Base path in S3 bucket (default: value-data)
    """

    duckdb: DuckDBResource
    s3_base_path: str = S3_BASE_PATH_VALUE_DATA  # Base path in S3 bucket

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Handle output from assets - save data to S3 Parquet files.

        Converts input to DataFrame if needed, then saves to S3 using DuckDBResource.save().

        Args:
            context: Dagster output context
            obj: Data to save (DataFrame, Series, or list)

        Raises:
            ValueError: If obj cannot be converted to DataFrame
        """
        if obj is None:
            return

        # Convert to pandas DataFrame if needed
        df = self._convert_to_dataframe(obj, context)

        if df.empty:
            context.log.warning(f"No data to save for asset {context.asset_key}")
            return

        # Determine S3 path based on asset metadata or asset key
        relative_path = self._resolve_s3_path(context, df)

        # Generate unique temp table name to prevent collisions in parallel runs
        temp_table = self._generate_temp_table_name(context)
        self.duckdb.register_dataframe(temp_table, df)

        try:
            # Use DuckDBResource.save() which handles S3 via duckdb_datacacher
            select_query = f"SELECT * FROM {temp_table}"
            self.duckdb.save(
                select_statement=select_query,
                file_path=relative_path,
            )
            row_count = len(df)
            context.log.info(
                f"Saved {row_count} rows to S3",
                extra={
                    "asset_key": str(context.asset_key),
                    "row_count": row_count,
                    "s3_path": relative_path,
                },
            )
        finally:
            self.duckdb.unregister_dataframe(temp_table)

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load input data from S3 Parquet files using DuckDBResource's methods.

        Uses DuckDBResource.load() which already has S3 access configured via duckdb_datacacher.
        Supports optional time-based filtering via metadata (start_time/end_time).

        Args:
            context: Dagster input context

        Returns:
            DataFrame with loaded data, or empty DataFrame if no data found
        """
        # Determine S3 path from metadata or asset key
        relative_path = self._resolve_s3_path(context)

        # Build query using SQL template with optional filters
        query_template = f"SELECT * FROM {SQL_READ_PARQUET_TEMPLATE}"
        where_clause = self._build_where_clause(context)

        if where_clause:
            query_template = f"{query_template} WHERE {where_clause}"

        # Create SQL query with file_path binding
        query = create_sql_query_with_file_path(query_template, relative_path)

        # Use DuckDBResource.load() which handles S3 via duckdb_datacacher
        df = self.duckdb.load(query)

        row_count = len(df) if df is not None and not df.empty else 0
        context.log.info(
            f"Loaded {row_count} rows from S3",
            extra={
                "asset_key": str(context.asset_key),
                "row_count": row_count,
                "s3_path": relative_path,
            },
        )

        # Return DataFrame or empty DataFrame
        return df if df is not None and not df.empty else pd.DataFrame()

    def _resolve_s3_path(
        self, context: Union[InputContext, OutputContext], df: Optional[pd.DataFrame] = None
    ) -> str:
        """Unified S3 path resolution for both input and output contexts.

        Determines the S3 path based on:
        1. Value data: series_id from metadata or DataFrame (if output)
        2. Generic data: asset key path

        Args:
            context: Dagster context (input or output)
            df: Optional DataFrame (for output context to detect value data)

        Returns:
            Relative S3 path (relative to bucket)
        """
        # Try to get series_id from metadata (safe access)
        series_id = self._get_series_id_from_metadata(context)

        # For output context, also check DataFrame for value data
        if df is not None and series_id is None:
            series_id = self._extract_series_id_from_dataframe(df)

        # If we have a series_id, use value data path
        if series_id is not None:
            return f"{self.s3_base_path}/series_id={series_id}/{S3_PARQUET_FILE_NAME}"

        # For generic DataFrames, use asset key
        return self._get_asset_based_path(context)

    def _get_series_id_from_metadata(
        self, context: Union[InputContext, OutputContext]
    ) -> Optional[int]:
        """Safely extract series_id from context metadata.

        Args:
            context: Dagster context (input or output)

        Returns:
            Series ID if found in metadata, None otherwise
        """
        metadata = getattr(context, "metadata", None)
        if metadata is None or not hasattr(metadata, "get"):
            return None

        series_id = metadata.get("series_id")
        if series_id is not None:
            try:
                return int(series_id)
            except (ValueError, TypeError):
                logger.warning(f"Invalid series_id in metadata: {series_id}")
                return None

        return None

    def _extract_series_id_from_dataframe(self, df: pd.DataFrame) -> Optional[int]:
        """Extract series_id from DataFrame (only if single series).

        Args:
            df: DataFrame to check

        Returns:
            Series ID if DataFrame contains single series, None otherwise
        """
        if df.empty or "series_id" not in df.columns:
            return None

        # Only extract if DataFrame contains a single unique series_id
        series_ids = df["series_id"].unique()
        if len(series_ids) == 1:
            try:
                return int(series_ids[0])
            except (ValueError, TypeError):
                return None

        return None

    def _get_asset_based_path(self, context: Union[InputContext, OutputContext]) -> str:
        """Get S3 path based on asset key.

        Args:
            context: Dagster context (input or output)

        Returns:
            Relative S3 path (relative to bucket)
        """
        asset_key = context.asset_key
        asset_name = asset_key.path[-1] if asset_key.path else "unknown"
        asset_group = asset_key.path[-2] if len(asset_key.path) >= 2 else "default"
        return f"{self.s3_base_path}/{asset_group}/{asset_name}.parquet"

    def _convert_to_dataframe(self, obj: Any, context: OutputContext) -> pd.DataFrame:
        """Convert input object to pandas DataFrame.

        Supports conversion from:
        - pd.DataFrame (returns as-is)
        - pd.Series (converts to DataFrame)
        - list of dicts (converts to DataFrame)
        - Other types raise ValueError

        Args:
            obj: Object to convert
            context: Dagster output context (for logging)

        Returns:
            DataFrame representation of obj

        Raises:
            ValueError: If obj cannot be converted to DataFrame
        """
        if isinstance(obj, pd.DataFrame):
            return obj

        if isinstance(obj, pd.Series):
            context.log.debug("Converting Series to DataFrame")
            return obj.to_frame()

        if isinstance(obj, list):
            if not obj:
                return pd.DataFrame()
            context.log.debug("Converting list to DataFrame")
            return pd.DataFrame(obj)

        raise ValueError(
            f"Unsupported output type: {type(obj)}. "
            "Expected pd.DataFrame, pd.Series, or list of dicts."
        )

    def _generate_temp_table_name(self, context: OutputContext) -> str:
        """Generate unique temporary table name to prevent collisions.

        Uses UUID to ensure uniqueness in parallel execution scenarios.

        Args:
            context: Dagster output context

        Returns:
            Unique temporary table name
        """
        asset_name = context.asset_key.path[-1] if context.asset_key.path else "output"
        unique_id = uuid.uuid4().hex[:8]  # Short UUID for readability
        return f"_temp_{asset_name}_{unique_id}"

    def _build_where_clause(self, context: InputContext) -> str:
        """Build WHERE clause from context metadata for filtering.

        Supports optional start_time and end_time filters from metadata.

        Args:
            context: Dagster input context

        Returns:
            WHERE clause string (empty if no filters)
        """
        metadata = getattr(context, "metadata", None)
        if metadata is None or not hasattr(metadata, "get"):
            return ""

        where_parts = []

        # Safely get start_time from metadata
        start_time = metadata.get("start_time")
        if start_time:
            where_parts.append(f"timestamp >= '{start_time}'")

        # Safely get end_time from metadata
        end_time = metadata.get("end_time")
        if end_time:
            where_parts.append(f"timestamp <= '{end_time}'")

        return " AND ".join(where_parts)


@io_manager(required_resource_keys={"duckdb"})
def duckdb_io_manager(context) -> DuckDBIOManager:
    """Factory function for DuckDB IO Manager with S3 Parquet datalake.

    Uses DuckDBResource which already has S3 access configured via duckdb_datacacher.
    """
    return DuckDBIOManager(
        duckdb=context.resources.duckdb,
        s3_base_path=S3_BASE_PATH_VALUE_DATA,
    )
