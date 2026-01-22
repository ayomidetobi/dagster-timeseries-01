"""Dependency graph and calculation log management.

With S3 as the datalake, dependencies are loaded from CSV → S3 Parquet control tables.
DuckDB views are created over S3 control tables for querying. This manager provides
CRUD operations for dependency data via DuckDB views and S3 control tables.
"""

from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import S3_CONTROL_DEPENDENCY, S3_PARQUET_FILE_NAME
from dagster_quickstart.utils.exceptions import DatabaseQueryError
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_control_table_path,
    create_or_update_duckdb_view,
)
from database.utils import DatabaseResource, query_to_dict, query_to_dict_list

# Constants
DEPENDENCY_TABLE = "seriesDependencyGraph"
QUERY_LIMIT_DEFAULT = 1000


class DependencyManager:
    """Manager for dependency graph operations.

    With S3 as the datalake, dependencies are immutable and versioned in S3 Parquet files.
    DuckDB views are created over S3 control tables for querying. This manager provides:
    - READ operations: Query dependency data via DuckDB views
    - CREATE/UPDATE operations: Create/update DuckDB views over S3 control tables
    - SAVE operations: Save dependencies to S3 control tables

    Note: Dependency data is loaded from CSV files via the `load_series_dependencies_from_csv` asset,
    which uses the save and create/update methods in this manager.
    """

    def __init__(self, database: DatabaseResource):
        """Initialize with database resource (DuckDB).

        Args:
            database: Database resource instance (DuckDBResource) with views over S3 control tables
        """
        self.database = database

    # READ Operations - Query DuckDB views over S3

    def get_dependency(self, dependency_id: int) -> Optional[Dict[str, Any]]:
        """Get a dependency by ID.

        Queries DuckDB view over S3 control table.

        Args:
            dependency_id: Dependency ID to lookup

        Returns:
            Dictionary with dependency data or None if not found
        """
        query = f"SELECT * FROM {DEPENDENCY_TABLE} WHERE dependency_id = ? LIMIT 1"
        result = self.database.execute_query(query, parameters=[dependency_id])
        return query_to_dict(result)

    def get_parent_dependencies(self, child_series_id: int) -> List[Dict[str, Any]]:
        """Get all parent dependencies for a child series.

        Queries DuckDB view over S3 control table.

        Args:
            child_series_id: Child series ID

        Returns:
            List of dictionaries with parent dependency data
        """
        query = f"""
        SELECT * FROM {DEPENDENCY_TABLE}
        WHERE child_series_id = ?
        ORDER BY parent_series_id
        """
        result = self.database.execute_query(query, parameters=[child_series_id])
        return query_to_dict_list(result)

    def get_child_dependencies(self, parent_series_id: int) -> List[Dict[str, Any]]:
        """Get all child dependencies for a parent series.

        Queries DuckDB view over S3 control table.

        Args:
            parent_series_id: Parent series ID

        Returns:
            List of dictionaries with child dependency data
        """
        query = f"""
        SELECT * FROM {DEPENDENCY_TABLE}
        WHERE parent_series_id = ?
        ORDER BY child_series_id
        """
        result = self.database.execute_query(query, parameters=[parent_series_id])
        return query_to_dict_list(result)

    def get_all_child_series_codes(self) -> List[str]:
        """Get all unique child series codes from dependencies.

        Queries DuckDB view over S3 control table to get all derived series.

        Returns:
            List of unique child series codes (derived series codes)
        """
        # Query dependencies and join with metaSeries to get series_code
        query = """
        SELECT DISTINCT m.series_code
        FROM seriesDependencyGraph d
        INNER JOIN metaSeries m ON d.child_series_id = m.series_id
        ORDER BY m.series_code
        """
        result = self.database.execute_query(query)
        if result is None or result.empty:
            return []
        return result["series_code"].tolist() if "series_code" in result.columns else []

    def get_dependency_tree(self, series_id: int, max_depth: int = 10) -> Dict[str, Any]:
        """Get full dependency tree for a series (recursive).

        Args:
            series_id: Series ID to get tree for
            max_depth: Maximum recursion depth

        Returns:
            Dictionary with dependency tree structure
        """
        tree = {"series_id": series_id, "parents": [], "children": []}

        if max_depth <= 0:
            return tree

        # Get parents
        parents = self.get_parent_dependencies(series_id)
        if isinstance(tree.get("parents"), list):
            for parent_dep in parents:
                parent_tree = self.get_dependency_tree(
                    parent_dep["parent_series_id"], max_depth - 1
                )
                tree["parents"].append(
                    {
                        "dependency": parent_dep,
                        "series": parent_tree,
                    }
                )

        # Get children
        children = self.get_child_dependencies(series_id)
        if isinstance(tree.get("children"), list):
            for child_dep in children:
                child_tree = self.get_dependency_tree(child_dep["child_series_id"], max_depth - 1)
                tree["children"].append(
                    {
                        "dependency": child_dep,
                        "series": child_tree,
                    }
                )

        return tree

    # CRUD Operations for S3 Control Tables

    def save_dependencies_to_s3(
        self,
        duckdb: DuckDBResource,
        temp_table: str,
        version_date: str,
        context: Optional[AssetExecutionContext] = None,
    ) -> str:
        """Save validated dependency data to S3 control table (versioned, immutable).

        Args:
            duckdb: DuckDB resource with S3 access.
            temp_table: Temporary table name with validated dependency data.
            version_date: Version date in YYYY-MM-DD format.
            context: Optional Dagster execution context for logging.

        Returns:
            Relative S3 path to the control table file.

        Raises:
            DatabaseQueryError: If write operation fails.
        """
        from dagster_quickstart.utils.helpers import write_to_s3_control_table

        relative_path = build_s3_control_table_path(
            S3_CONTROL_DEPENDENCY, version_date, S3_PARQUET_FILE_NAME
        )

        if context:
            context.log.info(
                "Writing validated dependencies to S3 control table",
                extra={"s3_path": relative_path, "version": version_date},
            )

        try:
            select_query = f"SELECT * FROM {temp_table}"
            write_to_s3_control_table(
                duckdb=duckdb,
                relative_path=relative_path,
                select_query=select_query,
                ordering_column="dependency_id",
                context=context,
            )
            return relative_path
        except Exception as e:
            error_msg = f"Error writing dependencies to S3 control table: {e}"
            if context:
                context.log.error(error_msg)
            raise DatabaseQueryError(error_msg) from e

    def _build_dependency_view_sql(
        self,
        duckdb: DuckDBResource,
        full_s3_path: str,
        context: Optional[AssetExecutionContext] = None,
    ) -> str:
        """Build SQL to create/update dependency view.

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

        # Build select columns dynamically - always include dependency_id (computed)
        # Expected columns: dependency_id, parent_series_id, child_series_id, weight, formula, calc_type
        select_parts = ["row_number() OVER (ORDER BY parent_series_id, child_series_id) AS dependency_id"]

        # Expected columns in order
        expected_columns = [
            "parent_series_id",
            "child_series_id",
            "weight",
            "formula",
            "calc_type",
        ]

        # Add columns that exist in the S3 file, in the expected order
        for col in expected_columns:
            if col in available_columns:
                select_parts.append(col)

        # Add any other columns that might exist but aren't in expected list
        for col in available_columns:
            if col not in expected_columns and col != "dependency_id":
                select_parts.append(col)

        select_columns = ",\n        ".join(select_parts)

        where_clause = "WHERE parent_series_id IS NOT NULL AND child_series_id IS NOT NULL"

        # Use generic template
        return CREATE_VIEW_FROM_S3_TEMPLATE.format(
            view_name=DEPENDENCY_TABLE,
            full_s3_path=full_s3_path,
            select_columns=select_columns,
            where_clause=where_clause,
            order_by_column="parent_series_id, child_series_id",
        )

    def create_or_update_view(
        self,
        duckdb: DuckDBResource,
        version_date: str,
        context: Optional[AssetExecutionContext] = None,
    ) -> None:
        """Create or update DuckDB view over S3 control table for dependencies.

        Args:
            duckdb: DuckDB resource with S3 access.
            version_date: Version date in YYYY-MM-DD format.
            context: Optional Dagster execution context for logging.

        Raises:
            DatabaseQueryError: If view creation fails.
        """
        relative_path = build_s3_control_table_path(
            S3_CONTROL_DEPENDENCY, version_date, S3_PARQUET_FILE_NAME
        )
        full_s3_path = build_full_s3_path(duckdb, relative_path)

        view_sql = self._build_dependency_view_sql(duckdb, full_s3_path, context)

        create_or_update_duckdb_view(
            duckdb=duckdb,
            view_name=DEPENDENCY_TABLE,
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
                    f"Created/updated {DEPENDENCY_TABLE} view pointing to S3 control table version {version_date} "
                    f"with {column_count} columns"
                )
            except Exception:
                context.log.info(
                    f"Created/updated {DEPENDENCY_TABLE} view pointing to S3 control table version {version_date}"
                )


