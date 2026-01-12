"""Lookup table management for the financial platform.

With S3 as the datalake, lookup tables are loaded from CSV → S3 Parquet control tables.
DuckDB views are created over S3 control tables for querying. This manager provides
CRUD operations for lookup data via DuckDB views and S3 control tables.
"""

import uuid
from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import (
    CODE_BASED_LOOKUPS,
    DB_COLUMNS,
    DB_TABLES,
    LOOKUP_TABLE_PROCESSING_ORDER,
    S3_CONTROL_LOOKUP,
    S3_PARQUET_FILE_NAME,
)
from dagster_quickstart.utils.exceptions import (
    DatabaseQueryError,  # noqa: F401  # Used in docstrings
)
from dagster_quickstart.utils.helpers import (
    build_full_s3_path,
    build_s3_control_table_path,
    create_or_update_duckdb_view,
    unregister_temp_table,
    write_to_s3_control_table,
)
from database.utils import DatabaseResource, get_by_id, get_by_name


class LookupTableManager:
    """Manager for lookup table CRUD operations.

    With S3 as the datalake, lookup tables are immutable and versioned in S3 Parquet files.
    DuckDB views are created over S3 control tables for querying. This manager provides:
    - READ operations: Query lookup data via DuckDB views
    - CREATE/UPDATE operations: Create/update DuckDB views over S3 control tables
    - SAVE operations: Save lookup tables to S3 control tables

    Note: Lookup data is loaded from CSV files via the `load_lookup_tables_from_csv` asset,
    which uses the save and create/update methods in this manager.
    """

    def __init__(self, database: DatabaseResource):
        """Initialize with database resource (DuckDB).

        Args:
            database: Database resource instance (DuckDBResource) with views over S3 control tables
        """
        self.database = database

    def _get_lookup_by_name(self, lookup_type: str, name: str) -> Optional[Dict[str, Any]]:
        """Generic method to get a lookup record by name.

        Queries DuckDB view over S3 control table.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class")
            name: Name to search for

        Returns:
            Dictionary with record data or None if not found
        """
        table_name = DB_TABLES[lookup_type]
        _, name_column = DB_COLUMNS[lookup_type]
        return get_by_name(self.database, table_name, name_column, name)

    def _get_lookup_by_id(self, lookup_type: str, record_id: int) -> Optional[Dict[str, Any]]:
        """Generic method to get a lookup record by ID.

        Queries DuckDB view over S3 control table.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class")
            record_id: ID to search for

        Returns:
            Dictionary with record data or None if not found
        """
        table_name = DB_TABLES[lookup_type]
        id_column, _ = DB_COLUMNS[lookup_type]
        return get_by_id(self.database, table_name, id_column, record_id)

    # Asset Class methods
    def get_asset_class_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get asset class by name."""
        return self._get_lookup_by_name("asset_class", name)

    # Product Type methods
    def get_product_type_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get product type by name."""
        return self._get_lookup_by_name("product_type", name)

    # Sub Asset Class methods
    def get_sub_asset_class_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get sub-asset class by name."""
        return self._get_lookup_by_name("sub_asset_class", name)

    # Data Type methods
    def get_data_type_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get data type by name."""
        return self._get_lookup_by_name("data_type", name)

    # Structure Type methods
    def get_structure_type_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get structure type by name."""
        return self._get_lookup_by_name("structure_type", name)

    # Market Segment methods
    def get_market_segment_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get market segment by name."""
        return self._get_lookup_by_name("market_segment", name)

    # Field Type methods
    def get_field_type_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get field type by name."""
        return self._get_lookup_by_name("field_type", name)

    def get_field_type_by_id(self, field_type_id: int) -> Optional[Dict[str, Any]]:
        """Get field type by ID.

        Args:
            field_type_id: Field type ID to lookup

        Returns:
            Dictionary with field type data or None if not found
        """
        return self._get_lookup_by_id("field_type", field_type_id)

    # Ticker Source methods
    def get_ticker_source_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get ticker source by name."""
        return self._get_lookup_by_name("ticker_source", name)

    def get_ticker_source_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get ticker source by code."""
        table_name = DB_TABLES["ticker_source"]
        return get_by_name(self.database, table_name, "ticker_source_code", code)

    # Region methods
    def get_region_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get region by name."""
        return self._get_lookup_by_name("region", name)

    # Currency methods
    def get_currency_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get currency by code."""
        table_name = DB_TABLES["currency"]
        return get_by_name(self.database, table_name, "currency_code", code)

    # Term methods
    def get_term_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Get term by name."""
        return self._get_lookup_by_name("term", name)

    # Tenor methods
    def get_tenor_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get tenor by code."""
        table_name = DB_TABLES["tenor"]
        return get_by_name(self.database, table_name, "tenor_code", code)

    # Country methods
    def get_country_by_code(self, code: str) -> Optional[Dict[str, Any]]:
        """Get country by code."""
        table_name = DB_TABLES["country"]
        return get_by_name(self.database, table_name, "country_code", code)

    # Generic CRUD functions for querying lookup tables

    def get_all_lookup_values(
        self, lookup_type: str, order_by: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all lookup values for a given lookup type.

        Queries DuckDB view over S3 control table.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class")
            order_by: Optional column name to order by (defaults to name column)

        Returns:
            List of dictionaries with lookup record data
        """
        table_name = DB_TABLES[lookup_type]
        id_column, name_column = DB_COLUMNS[lookup_type]

        # Determine order by column
        if order_by:
            order_col = order_by
        elif lookup_type in CODE_BASED_LOOKUPS:
            code_field, _, _ = CODE_BASED_LOOKUPS[lookup_type]
            order_col = code_field
        else:
            order_col = name_column

        query = f"SELECT * FROM {table_name} ORDER BY {order_col}"
        result = self.database.execute_query(query)

        if result is None or result.empty:
            return []

        return result.to_dict("records")

    def lookup_exists(self, lookup_type: str, value: str, by_code: bool = False) -> bool:
        """Check if a lookup value exists.

        Args:
            lookup_type: Type of lookup (e.g., "asset_class")
            value: Value to check (name or code)
            by_code: If True, search by code field; if False, search by name field

        Returns:
            True if lookup value exists, False otherwise
        """
        if by_code and lookup_type in CODE_BASED_LOOKUPS:
            code_field, _, _ = CODE_BASED_LOOKUPS[lookup_type]
            table_name = DB_TABLES[lookup_type]
            result = get_by_name(self.database, table_name, code_field, value)
        else:
            result = self._get_lookup_by_name(lookup_type, value)

        return result is not None

    def get_lookup_by_code_or_name(self, lookup_type: str, value: str) -> Optional[Dict[str, Any]]:
        """Get lookup record by code or name (tries code first, then name).

        Useful for code-based lookups where you're not sure if the value is a code or name.

        Args:
            lookup_type: Type of lookup (e.g., "currency")
            value: Code or name to search for

        Returns:
            Dictionary with record data or None if not found
        """
        # Try code first if it's a code-based lookup
        if lookup_type in CODE_BASED_LOOKUPS:
            code_field, _, _ = CODE_BASED_LOOKUPS[lookup_type]
            table_name = DB_TABLES[lookup_type]
            result = get_by_name(self.database, table_name, code_field, value)
            if result:
                return result

        # Fall back to name
        return self._get_lookup_by_name(lookup_type, value)

    def search_lookup_values(
        self, lookup_type: str, search_term: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Search lookup values by name or code (partial match).

        Args:
            lookup_type: Type of lookup (e.g., "asset_class")
            search_term: Search term to match against (will be escaped for SQL safety)
            limit: Maximum number of results to return

        Returns:
            List of dictionaries with matching lookup records
        """
        table_name = DB_TABLES[lookup_type]
        id_column, name_column = DB_COLUMNS[lookup_type]

        # Escape search term to prevent SQL injection
        # Replace single quotes with two single quotes (SQL escape)
        escaped_term = search_term.replace("'", "''")
        search_pattern = f"%{escaped_term}%"

        # Build search query - search in name column and code column if applicable
        if lookup_type in CODE_BASED_LOOKUPS:
            code_field, name_field, _ = CODE_BASED_LOOKUPS[lookup_type]
            where_clause = f"WHERE {name_field} LIKE '{search_pattern}' OR {code_field} LIKE '{search_pattern}'"
            order_col = code_field
        else:
            where_clause = f"WHERE {name_column} LIKE '{search_pattern}'"
            order_col = name_column

        query = f"SELECT * FROM {table_name} {where_clause} ORDER BY {order_col} LIMIT {limit}"
        result = self.database.execute_query(query)

        if result is None or result.empty:
            return []

        return result.to_dict("records")

    # CRUD Operations for S3 Control Tables

    def _build_union_all_query(
        self, duckdb: DuckDBResource, all_lookup_temp_tables: Dict[str, str]
    ) -> str:
        """Build UNION ALL query to combine all lookup tables into canonical format.

        Projects each lookup table into a canonical schema:
        - lookup_type STRING
        - code STRING
        - name STRING

        For code-based lookups: uses code_field and name_field
        For simple lookups: sets code = name = value

        Args:
            duckdb: DuckDB resource.
            all_lookup_temp_tables: Dictionary mapping lookup_type -> temp table name.

        Returns:
            SQL UNION ALL query string, empty if no valid tables.
        """
        from dagster_quickstart.assets.csv_loader.utils.load_data import check_temp_table_has_data
        from database.ddl import UNION_ALL_SELECT_CODE_BASED, UNION_ALL_SELECT_SIMPLE

        union_parts: List[str] = []
        for lookup_type, temp_table in all_lookup_temp_tables.items():
            if not check_temp_table_has_data(duckdb, temp_table):
                continue

            # Project into canonical format: lookup_type, code, name
            if lookup_type in CODE_BASED_LOOKUPS:
                # Code-based lookups have both code_field and name_field
                code_field, name_field, _ = CODE_BASED_LOOKUPS[lookup_type]
                union_parts.append(
                    UNION_ALL_SELECT_CODE_BASED.format(
                        lookup_type=lookup_type,
                        code_field=code_field,
                        name_field=name_field,
                        temp_table=temp_table,
                    )
                )
            else:
                # Simple lookups only have a name column
                # Set code = name = value for canonical format
                _, name_column = DB_COLUMNS[lookup_type]
                union_parts.append(
                    UNION_ALL_SELECT_SIMPLE.format(
                        lookup_type=lookup_type,
                        name_column=name_column,
                        temp_table=temp_table,
                    )
                )

        return " UNION ALL ".join(union_parts)

    def save_lookup_tables_to_s3(
        self,
        duckdb: DuckDBResource,
        all_lookup_temp_tables: Dict[str, str],
        version_date: str,
        context: Optional[AssetExecutionContext] = None,
    ) -> str:
        """Save all lookup table data to S3 control table (versioned, immutable).

        Uses UNION ALL to combine all lookup tables in SQL, no pandas involved.

        Args:
            duckdb: DuckDB resource with S3 access.
            all_lookup_temp_tables: Dictionary mapping lookup_type -> temp table name.
            version_date: Version date in YYYY-MM-DD format.
            context: Optional Dagster execution context for logging.

        Returns:
            Relative S3 path to the control table file.

        Raises:
            DatabaseQueryError: If write operation fails.
        """
        relative_path = build_s3_control_table_path(
            S3_CONTROL_LOOKUP, version_date, S3_PARQUET_FILE_NAME
        )

        if not all_lookup_temp_tables:
            if context:
                context.log.warning("No lookup data to write")
            return ""

        combined_query = self._build_union_all_query(duckdb, all_lookup_temp_tables)
        if not combined_query:
            if context:
                context.log.warning("No lookup data to write")
            return ""

        combined_temp_table = f"_temp_combined_lookup_{uuid.uuid4().hex}"

        try:
            create_combined_sql = f"CREATE TEMP TABLE {combined_temp_table} AS {combined_query}"
            duckdb.execute_command(create_combined_sql)

            count_query = f"SELECT COUNT(*) as cnt FROM {combined_temp_table}"
            count_result = duckdb.execute_query(count_query)
            row_count = (
                count_result.iloc[0]["cnt"]
                if count_result is not None and not count_result.empty
                else 0
            )

            if context:
                context.log.info(
                    f"Writing {row_count} total lookup records to S3 control table",
                    extra={
                        "s3_path": relative_path,
                        "version": version_date,
                        "lookup_types": list(all_lookup_temp_tables.keys()),
                    },
                )

            select_query = f"SELECT * FROM {combined_temp_table}"
            write_to_s3_control_table(
                duckdb=duckdb,
                relative_path=relative_path,
                select_query=select_query,
                ordering_column="lookup_type",
                context=context,
            )
            if context:
                context.log.info(
                    "Successfully wrote all lookup tables to S3 control table",
                    extra={"s3_path": relative_path, "version": version_date},
                )
            return relative_path
        finally:
            unregister_temp_table(duckdb, combined_temp_table, context)

    def _build_lookup_table_view_sql(self, lookup_type: str, full_s3_path: str) -> str:
        """Build SQL to create/update a lookup table view.

        The S3 control table uses canonical format: lookup_type, code, name
        This function maps the canonical columns to the expected view column names.

        Args:
            lookup_type: Type of lookup table.
            full_s3_path: Full S3 path to control table Parquet file.

        Returns:
            SQL CREATE OR REPLACE VIEW statement.
        """
        from database.ddl import (
            CREATE_VIEW_FROM_S3_TEMPLATE,
            LOOKUP_TABLE_VIEW_SELECT_CODE_BASED,
            LOOKUP_TABLE_VIEW_SELECT_SIMPLE,
            LOOKUP_TABLE_VIEW_WHERE_CODE_BASED,
            LOOKUP_TABLE_VIEW_WHERE_SIMPLE,
        )

        table_name = DB_TABLES[lookup_type]
        id_column, name_column = DB_COLUMNS[lookup_type]

        if lookup_type in CODE_BASED_LOOKUPS:
            # Code-based lookups: map canonical 'code' and 'name' to code_field and name_field
            code_field, name_field, _ = CODE_BASED_LOOKUPS[lookup_type]
            select_columns = LOOKUP_TABLE_VIEW_SELECT_CODE_BASED.format(
                id_column=id_column,
                code_field=code_field,
                name_field=name_field,
            )
            where_clause = LOOKUP_TABLE_VIEW_WHERE_CODE_BASED.format(lookup_type=lookup_type)
            order_by_column = "code"
        else:
            # Simple lookups: map canonical 'name' to name_column
            # For simple lookups, code = name in the canonical format
            select_columns = LOOKUP_TABLE_VIEW_SELECT_SIMPLE.format(
                id_column=id_column,
                name_column=name_column,
            )
            where_clause = LOOKUP_TABLE_VIEW_WHERE_SIMPLE.format(lookup_type=lookup_type)
            order_by_column = "name"

        # Use generic template
        return CREATE_VIEW_FROM_S3_TEMPLATE.format(
            view_name=table_name,
            full_s3_path=full_s3_path,
            select_columns=select_columns,
            where_clause=where_clause,
            order_by_column=order_by_column,
        )

    def create_or_update_views(
        self,
        duckdb: DuckDBResource,
        version_date: str,
        context: Optional[AssetExecutionContext] = None,
    ) -> None:
        """Create or update DuckDB views over S3 control table for all lookup tables.

        Args:
            duckdb: DuckDB resource with S3 access.
            version_date: Version date in YYYY-MM-DD format.
            context: Optional Dagster execution context for logging.

        Raises:
            DatabaseQueryError: If view creation fails.
        """
        bucket = duckdb.get_bucket()
        relative_path = build_s3_control_table_path(
            S3_CONTROL_LOOKUP, version_date, S3_PARQUET_FILE_NAME
        )
        full_s3_path = build_full_s3_path(bucket, relative_path)

        for lookup_type in LOOKUP_TABLE_PROCESSING_ORDER:
            table_name = DB_TABLES[lookup_type]
            view_sql = self._build_lookup_table_view_sql(lookup_type, full_s3_path)

            create_or_update_duckdb_view(
                duckdb=duckdb,
                view_name=table_name,
                view_sql=view_sql,
                context=context,
            )
            if context:
                context.log.debug(
                    f"Created/updated {table_name} view pointing to S3 control table version {version_date}"
                )

        if context:
            context.log.info(
                f"Created/updated all lookup table views pointing to S3 control table version {version_date}"
            )
