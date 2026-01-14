"""DuckDB database resource for Dagster.

This resource provides DuckDB database operations with S3 as the datalake.
Uses local DuckDBDataCacher for connection management.
"""

import uuid
from contextlib import contextmanager
from typing import Any, Iterator, Optional

import duckdb
import pandas as pd
from dagster import (
    ConfigurableResource,
    InitResourceContext,
    ResourceDependency,
    get_dagster_logger,
)

from dagster_quickstart.resources.duckdb_datacacher import (
    SQL,
    DuckDBDataCacher,
)

logger = get_dagster_logger()


class DuckDBResource(ConfigurableResource):
    """Resource for interacting with a DuckDB database with S3 as the datalake.

    Uses duckdb_datacacher for connection management.
    Provides methods for querying, inserting data, and managing S3 Parquet files.
    Views are created dynamically over S3 control tables as needed.


    """

    cacher: ResourceDependency[DuckDBDataCacher]

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._con = self.cacher.con

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Get the DuckDB connection.

        Returns:
            DuckDB connection object
        """
        yield self._con

    def execute_query(self, query: str, parameters: Optional[list] = None) -> pd.DataFrame:
        """Execute a query and return results as pandas DataFrame.

        Args:
            query: SQL query string with ? placeholders for parameters
            parameters: Optional list of parameter values in order

        Returns:
            Pandas DataFrame with query results, or empty DataFrame if no results
        """
        if parameters:
            result = self._con.execute(query, parameters)
        else:
            result = self._con.execute(query)

        # DuckDB's execute() returns a relation that has a df() method
        if hasattr(result, "df"):
            df = result.df()
            return df if df is not None else pd.DataFrame()
        # Fallback: try to convert to DataFrame
        try:
            return pd.DataFrame(result.fetchall())
        except Exception:
            return pd.DataFrame()

    def execute_command(self, command: str, parameters: Optional[list] = None) -> None:
        """Execute a command (DDL/DML) without returning results.

        Args:
            command: SQL command string with ? placeholders for parameters
            parameters: Optional list of parameter values in order
        """
        if parameters:
            self._con.execute(command, parameters)
        else:
            self._con.execute(command)

    def insert_data(
        self,
        table: str,
        data: list,
        column_names: Optional[list] = None,
        database: Optional[str] = None,
    ) -> None:
        """Insert data into a DuckDB table using bulk insert.

        Uses DuckDB's register method for efficient bulk inserts with pandas.

        Args:
            table: Table name
            data: List of rows (list of lists or list of tuples)
            column_names: Optional list of column names
            database: Optional database name (ignored for DuckDB)
        """
        if not data:
            return

        # Convert data to pandas DataFrame
        if column_names:
            # Use provided column names
            df = pd.DataFrame(data, columns=column_names)
        else:
            # Create DataFrame without column names (DuckDB will infer)
            df = pd.DataFrame(data)

        # Use DuckDB's register method for bulk insert with unique temp table name
        tmp = f"_tmp_insert_{uuid.uuid4().hex}"
        self.register_dataframe(tmp, df)
        self._con.execute(f"INSERT INTO {table} SELECT * FROM {tmp}")
        self.unregister_dataframe(tmp)

    def _validate_and_convert_sql(self, select_statement: Any) -> Any:
        """Validate and convert select_statement to SQL object if needed.

        Args:
            select_statement: SQL object or string to validate/convert

        Returns:
            SQL object ready for use with duckdb_datacacher

        Raises:
            ValueError: If select_statement is None or invalid type
        """
        if select_statement is None:
            raise ValueError("select_statement is None")

        # If SQL class is available and select_statement is not already a SQL object,
        # try to convert string to SQL object
        if SQL is not None:
            if not isinstance(select_statement, SQL):
                # If it's a string, create a SQL object with it
                if isinstance(select_statement, str):
                    select_statement = SQL(select_statement)
                else:
                    raise ValueError(f"Expected SQL object or string; got {type(select_statement)}")
        else:
            # If SQL class is not available, check if it has SQL-like attributes
            # (sql and bindings) or pass through and let cacher handle validation
            if not (hasattr(select_statement, "sql") and hasattr(select_statement, "bindings")):
                raise ValueError(
                    "SQL class not available. select_statement must be a SQL object "
                    "with 'sql' and 'bindings' attributes. "
                    "Ensure qr_common is properly installed."
                )

        return select_statement

    def save(
        self,
        select_statement: Any,
        file_path: str,
        debug: bool = False,
        credentials: Optional[str] = None,
    ) -> bool:
        """Save query results to S3 as Parquet file.

        Uses the underlying duckdb_datacacher's save method to write data to S3.
        Supports optional Parquet encryption via credentials.

        Args:
            select_statement: SQL object with query and bindings, or SQL query string
            file_path: S3 file path (relative to bucket)
            debug: If True, log the generated query
            credentials: Optional encryption credentials for Parquet file

        Returns:
            True if save was successful

        Raises:
            ValueError: If select_statement is None or invalid type
        """
        select_statement = self._validate_and_convert_sql(select_statement)

        return self.cacher.save(
            select_statement=select_statement,
            file_path=file_path,
            debug=debug,
            credentials=credentials,
        )

    def load(
        self,
        select_statement: Any,
        debug: bool = False,
    ) -> Optional[pd.DataFrame]:
        """Load data from S3 Parquet file into a Pandas DataFrame.

        Uses the underlying duckdb_datacacher's load method to read data from S3.

        Args:
            select_statement: SQL object with query and bindings, or SQL query string
            debug: If True, log the generated query

        Returns:
            Pandas DataFrame with loaded data, or None if result is empty

        Raises:
            ValueError: If select_statement is None or invalid type
        """
        select_statement = self._validate_and_convert_sql(select_statement)

        return self.cacher.load(select_statement=select_statement, debug=debug)

    def staleness_check(
        self,
        file_name: str,
        lookback_delta_seconds: int,
        in_memory: bool = False,
    ) -> bool:
        """Check whether a file or in-memory table is stale.

        Uses the underlying duckdb_datacacher's staleness_check method.
        Checks if the last modified time of a file/table exceeds the lookback delta.

        Args:
            file_name: Name of the file or table to check
            lookback_delta_seconds: Maximum age in seconds before considered stale
            in_memory: If True, check in-memory table (recommended for DuckDB)

        Returns:
            True if stale, False if fresh or unknown

        Note:
            This functionality is best suited for in-memory tables in DuckDB.
        """
        return self.cacher.staleness_check(
            file_name=file_name,
            lookback_delta_seconds=lookback_delta_seconds,
            in_memory=in_memory,
        )

    def get_bucket(self) -> str:
        """Get the S3 bucket name configured for this DuckDB resource.

        Returns:
            S3 bucket name string

        Raises:
            AttributeError: If bucket is not available in the cacher
        """
        if not hasattr(self.cacher, "bucket"):
            raise AttributeError("Bucket not available in duckdb_datacacher")
        return self.cacher.bucket

    def register_dataframe(self, name: str, df: pd.DataFrame) -> None:
        """Register a pandas DataFrame as a temporary table in DuckDB.

        Public method to register DataFrames for use in SQL queries.

        Args:
            name: Name for the temporary table
            df: Pandas DataFrame to register
        """
        self._con.register(name, df)

    def unregister_dataframe(self, name: str) -> None:
        """Unregister a temporary table from DuckDB.

        Public method to clean up registered DataFrames.

        Args:
            name: Name of the temporary table to unregister
        """
        try:
            self._con.unregister(name)
        except Exception:
            # Table may already be unregistered, ignore
            pass
