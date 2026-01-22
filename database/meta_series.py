"""Meta series management for the financial platform.

With S3 as the datalake, meta series are loaded from CSV → S3 Parquet control tables.
DuckDB views are created over S3 control tables for querying. This manager provides
CRUD operations for meta series data via DuckDB views and S3 control tables.
"""

from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    META_SERIES_ALL_COLUMNS,
    S3_CONTROL_METADATA_SERIES,
    S3_PARQUET_FILE_NAME,
)
from dagster_quickstart.utils.exceptions import DatabaseQueryError
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_control_table_path,
    create_or_update_duckdb_view,
)
from database.models import DataSource
from database.utils import DatabaseResource, query_to_dict, query_to_dict_list

# Constants
META_SERIES_TABLE = "metaSeries"
QUERY_LIMIT_DEFAULT = 1000


class MetaSeriesManager:
    """Manager for meta series CRUD operations.

    With S3 as the datalake, meta series are immutable and versioned in S3 Parquet files.
    DuckDB views are created over S3 control tables for querying. This manager provides:
    - READ operations: Query meta series data via DuckDB views
    - CREATE/UPDATE operations: Create/update DuckDB views over S3 control tables
    - SAVE operations: Save meta series to S3 control tables

    Note: Meta series data is loaded from CSV files via the `load_meta_series_from_csv` asset,
    which uses the save and create/update methods in this manager.
    """

    def __init__(self, database: DatabaseResource):
        """Initialize with database resource (DuckDB).

        Args:
            database: Database resource instance (DuckDBResource) with views over S3 control tables
        """
        self.database = database

    # READ Operations - Query DuckDB views over S3

    def get_meta_series(self, series_id: int) -> Optional[Dict[str, Any]]:
        """Get a meta series by ID.

        Queries DuckDB view over S3 control table.

        Args:
            series_id: Series ID to lookup

        Returns:
            Dictionary with meta series data or None if not found
        """
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE series_id = ? LIMIT 1"
        result = self.database.execute_query(query, parameters=[series_id])
        return query_to_dict(result)

    def get_meta_series_by_code(self, series_code: str) -> Optional[Dict[str, Any]]:
        """Get a meta series by code.

        Queries DuckDB view over S3 control table.

        Args:
            series_code: Series code to lookup

        Returns:
            Dictionary with meta series data or None if not found
        """
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE series_code = ? LIMIT 1"
        result = self.database.execute_query(query, parameters=[series_code])
        return query_to_dict(result)

    def get_active_series(
        self,
        data_source: Optional[DataSource] = None,
        asset_class_id: Optional[int] = None,
        limit: int = QUERY_LIMIT_DEFAULT,
    ) -> List[Dict[str, Any]]:
        """Get active meta series (is_active = 1) with optional filters.

        Queries DuckDB view over S3 control table.

        Args:
            data_source: Optional data source filter
            asset_class_id: Optional asset class ID filter
            limit: Maximum number of results to return

        Returns:
            List of dictionaries with active meta series data
        """
        query = f"SELECT * FROM {META_SERIES_TABLE} WHERE is_active = 1"
        params: List[Any] = []

        if data_source:
            query += " AND data_source = ?"
            params.append(data_source.value)

        if asset_class_id:
            query += " AND asset_class_id = ?"
            params.append(asset_class_id)

        query += " ORDER BY series_id LIMIT ?"
        params.append(limit)

        result = self.database.execute_query(query, parameters=params)
        return query_to_dict_list(result)

    def get_or_validate_meta_series(
        self,
        series_code: str,
        context: Optional[AssetExecutionContext] = None,
        raise_if_not_found: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Get meta series by code, with optional validation and logging.

        Args:
            series_code: Series code to look up
            context: Optional Dagster context for logging
            raise_if_not_found: Whether to raise exception if not found

        Returns:
            Meta series dictionary or None if not found and raise_if_not_found=False

        Raises:
            MetaSeriesNotFoundError: If series not found and raise_if_not_found=True
        """
        from dagster_quickstart.utils.exceptions import MetaSeriesNotFoundError

        meta_series = self.get_meta_series_by_code(series_code)

        if not meta_series:
            if raise_if_not_found:
                raise MetaSeriesNotFoundError(
                    f"Meta series {series_code} must exist before operation"
                )
            if context:
                context.log.warning(f"Meta series {series_code} not found")
            return None

        return meta_series

    # CRUD Operations for S3 Control Tables

    def save_meta_series_to_s3(
        self,
        duckdb: DuckDBResource,
        temp_table: str,
        version_date: str,
        context: Optional[AssetExecutionContext] = None,
    ) -> str:
        """Save validated meta series data to S3 control table (versioned, immutable).

        Args:
            duckdb: DuckDB resource with S3 access.
            temp_table: Temporary table name with validated meta series data.
            version_date: Version date in YYYY-MM-DD format.
            context: Optional Dagster execution context for logging.

        Returns:
            Relative S3 path to the control table file.

        Raises:
            DatabaseQueryError: If write operation fails.
        """
        from dagster_quickstart.utils.helpers import write_to_s3_control_table

        relative_path = build_s3_control_table_path(
            S3_CONTROL_METADATA_SERIES, version_date, S3_PARQUET_FILE_NAME
        )

        if context:
            context.log.info(
                "Writing validated meta series to S3 control table",
                extra={"s3_path": relative_path, "version": version_date},
            )

        try:
            select_query = f"SELECT * FROM {temp_table}"
            write_to_s3_control_table(
                duckdb=duckdb,
                relative_path=relative_path,
                select_query=select_query,
                ordering_column="series_code",
                context=context,
            )
            return relative_path
        except Exception as e:
            error_msg = f"Error writing meta series to S3 control table: {e}"
            if context:
                context.log.error(error_msg)
            raise DatabaseQueryError(error_msg) from e

    def _build_meta_series_view_sql(
        self,
        duckdb: DuckDBResource,
        full_s3_path: str,
        context: Optional[AssetExecutionContext] = None,
    ) -> str:
        """Build SQL to create/update meta series view.

        Dynamically builds SELECT columns based on what actually exists in the S3 Parquet file,
        since CSV files may not have all columns.

        Args:
            duckdb: DuckDB resource to query S3 Parquet schema.
            full_s3_path: Full S3 path to control table Parquet file.
            context: Optional Dagster context for logging.

        Returns:
            SQL CREATE OR REPLACE VIEW statement.

        Raises:
            DatabaseQueryError: If schema reading fails.
        """
        from database.ddl import CREATE_VIEW_FROM_S3_TEMPLATE

        # Get actual columns from S3 Parquet file
        # Query with LIMIT 0 to get schema without reading data
        try:
            schema_query = f"SELECT * FROM read_parquet('{full_s3_path}') LIMIT 0"
            schema_result = duckdb.execute_query(schema_query)

            if schema_result is None:
                raise DatabaseQueryError(
                    f"Could not read schema from S3 Parquet file: {full_s3_path}"
                )

            # Get column names from the empty result (schema is preserved)
            available_columns = schema_result.columns.tolist()

            if not available_columns:
                raise DatabaseQueryError(f"No columns found in S3 Parquet file: {full_s3_path}")

        except Exception as e:
            error_msg = f"Error reading schema from S3 Parquet file {full_s3_path}: {e}"
            if context:
                context.log.error(error_msg)
            raise DatabaseQueryError(error_msg) from e

        # Build select columns dynamically - always include series_id (computed)
        # Then add all other columns that exist in the S3 file
        select_parts = ["row_number() OVER (ORDER BY series_code) AS series_id"]

        # Use META_SERIES_ALL_COLUMNS constant which includes all meta series fields
        # (both specific columns and lookup table columns) in the correct order
        expected_columns = META_SERIES_ALL_COLUMNS

        # Add columns that exist in the S3 file, in the expected order
        for col in expected_columns:
            if col in available_columns:
                select_parts.append(col)

        # Add any other columns that might exist but aren't in expected list
        for col in available_columns:
            if col not in expected_columns and col != "series_id":
                select_parts.append(col)

        select_columns = ",\n        ".join(select_parts)

        where_clause = "WHERE series_code IS NOT NULL AND series_code != ''"

        # Use generic template
        return CREATE_VIEW_FROM_S3_TEMPLATE.format(
            view_name=META_SERIES_TABLE,
            full_s3_path=full_s3_path,
            select_columns=select_columns,
            where_clause=where_clause,
            order_by_column="series_code",
        )

    def create_or_update_view(
        self,
        duckdb: DuckDBResource,
        version_date: str,
        context: Optional[AssetExecutionContext] = None,
    ) -> None:
        """Create or update DuckDB view over S3 control table for meta series.

        Args:
            duckdb: DuckDB resource with S3 access.
            version_date: Version date in YYYY-MM-DD format.
            context: Optional Dagster execution context for logging.

        Raises:
            DatabaseQueryError: If view creation fails.
        """
        relative_path = build_s3_control_table_path(
            S3_CONTROL_METADATA_SERIES, version_date, S3_PARQUET_FILE_NAME
        )
        full_s3_path = build_full_s3_path(duckdb, relative_path)

        view_sql = self._build_meta_series_view_sql(duckdb, full_s3_path, context)

        create_or_update_duckdb_view(
            duckdb=duckdb,
            view_name=META_SERIES_TABLE,
            view_sql=view_sql,
            context=context,
        )

        if context:
            # Get column count for logging
            try:
                schema_query = f"SELECT * FROM read_parquet('{full_s3_path}') LIMIT 0"
                schema_result = duckdb.execute_query(schema_query)
                column_count = len(schema_result.columns) if schema_result is not None else 0
                context.log.info(
                    f"Created/updated {META_SERIES_TABLE} view pointing to S3 control table version {version_date} "
                    f"with {column_count} columns"
                )
            except Exception:
                context.log.info(
                    f"Created/updated {META_SERIES_TABLE} view pointing to S3 control table version {version_date}"
                )
