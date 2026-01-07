"""DuckDB database resource for Dagster.

This resource provides DuckDB database operations with the same interface
as ClickHouseResource, allowing easy switching between databases.
Uses duckdb_datacacher for connection management and duckup for migrations.
"""

import re
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional

import duckdb
import duckup
import pandas as pd
import polars as pl
from dagster import ConfigurableResource, get_dagster_logger
from qr_common.datacachers.duckdb_datacacher import duckdb_datacacher

logger = get_dagster_logger()


class DuckDBResult:
    """Wrapper for DuckDB query results to match ClickHouse result interface."""

    def __init__(self, result: Any):
        """Initialize with DuckDB query result.

        Args:
            result: DuckDB query result (relation or fetchall result)
        """
        self._result = result

        # Handle different result types
        if hasattr(result, "df"):
            # DuckDB relation with df() method
            self._df = result.df()
        elif hasattr(result, "fetchall"):
            # Result from fetchall()
            self._rows = result.fetchall()
            self._columns = [c[0] for c in result.description]
            self._df = None
        else:
            # Try to convert to list of tuples
            try:
                self._rows = list(result) if result else []
                self._columns = []
                self._df = None
            except Exception:
                self._rows = []
                self._columns = []
                self._df = None

    @property
    def result_rows(self) -> list:
        """Get result rows as list of tuples."""
        if self._df is not None:
            return [tuple(row) for row in self._df.values.tolist()]
        elif hasattr(self, "_rows"):
            return self._rows
        return []

    @property
    def column_names(self) -> list:
        """Get column names."""
        if self._df is not None:
            return list(self._df.columns)
        elif hasattr(self, "_columns") and self._columns:
            return [col[0] for col in self._columns]
        return []


class DuckDBResource(ConfigurableResource):
    """Resource for interacting with a DuckDB database.

    Uses duckdb_datacacher for connection management and duckup for migrations.
    Implements the same interface as ClickHouseResource for easy switching.
    """

    def __init__(self, cacher: duckdb_datacacher):
        """Initialize DuckDB resource with datacacher.

        Loads the chsql extension for ClickHouse SQL compatibility.

        Args:
            cacher: DuckDB datacacher instance providing connection
        """
        self._cacher = cacher
        self._con = cacher._con
        self._ensure_chsql_loaded()

    @contextmanager
    def get_connection(self) -> Iterator[duckdb.Connection]:
        """Get the DuckDB connection.

        Returns:
            DuckDB connection object
        """
        yield self._con

    def execute_query(self, query: str, parameters: Optional[dict] = None) -> DuckDBResult:
        """Execute a query and return results.

        Accepts ClickHouse-style SQL. The chsql extension provides compatibility.

        Args:
            query: SQL query string (ClickHouse format)
            parameters: Query parameters (ClickHouse-style dict, converted to DuckDB format)

        Returns:
            DuckDBResult object with result_rows and column_names
        """
        # Convert ClickHouse-style parameters to DuckDB format
        duckdb_query, duckdb_params = self._convert_query_parameters(query, parameters)
        duckdb_params = duckdb_params if duckdb_params is not None else []

        if duckdb_params:
            result = self._con.execute(duckdb_query, duckdb_params)
        else:
            result = self._con.execute(duckdb_query)

        return DuckDBResult(result)

    def execute_command(self, command: str, parameters: Optional[dict] = None) -> None:
        """Execute a command (DDL/DML) without returning results.

        Accepts ClickHouse-style SQL. The chsql extension provides compatibility.

        Args:
            command: SQL command string (ClickHouse format)
            parameters: Command parameters (ClickHouse-style dict, converted to DuckDB format)
        """
        # Convert ClickHouse-style parameters to DuckDB format
        duckdb_command, duckdb_params = self._convert_query_parameters(command, parameters)

        if duckdb_params:
            self._con.execute(duckdb_command, duckdb_params)
        else:
            self._con.execute(duckdb_command)

    def insert_data(
        self,
        table: str,
        data: list,
        column_names: Optional[list] = None,
        database: Optional[str] = None,
    ) -> None:
        """Insert data into a DuckDB table using bulk insert.

        Uses DuckDB's register method for efficient bulk inserts with Pandas.
        Accepts raw data (list of lists/tuples) or Polars DataFrames.

        Args:
            table: Table name
            data: List of rows (list of lists or list of tuples) or Polars DataFrame
            column_names: Optional list of column names (ignored if data is already a DataFrame)
            database: Optional database name (ignored for DuckDB)
        """
        if not data:
            return

        # Handle Polars DataFrame - convert to Pandas
        if isinstance(data, pl.DataFrame):
            df = data.to_pandas()
        # Convert raw data to Pandas DataFrame
        elif column_names:
            # Use provided column names
            df = pd.DataFrame(data, columns=column_names)
        else:
            # Create DataFrame without column names (DuckDB will infer)
            df = pd.DataFrame(data)

        # Use DuckDB's register method for bulk insert with unique temp table name
        tmp = f"_tmp_insert_{uuid.uuid4().hex}"
        self._con.register(tmp, df)
        self._con.execute(f"INSERT INTO {table} SELECT * FROM {tmp}")
        self._con.unregister(tmp)

    def ensure_database(self) -> None:
        """Ensure the database exists.

        For DuckDB with duckdb_datacacher, the database is already connected.
        This method is a no-op but exists for interface compatibility.
        """
        logger.info("DuckDB database connection ensured via duckdb_datacacher")

    def get_migrations_directory(self) -> Path:
        """Get the migrations directory path.

        Returns:
            Path to migrations directory
        """
        # Get project root (two levels up from this file: resources -> dagster_quickstart -> project root)
        current_file = Path(__file__)
        project_root = current_file.parent.parent.parent
        migrations_dir = project_root / "migrations_duckdb"
        migrations_dir.mkdir(exist_ok=True)
        return migrations_dir

    def run_migrations(self) -> None:
        """Run all pending migrations using duckup.

        This method uses the duckup Python API to apply migrations.
        Migrations are tracked and only pending ones are applied.
        """
        try:
            migrations_dir = self.get_migrations_directory()

            if not migrations_dir.exists():
                logger.warning(f"Migrations directory {migrations_dir} does not exist")
                return

            # Run duckup upgrade with connection and migrations directory
            duckup.upgrade(self._con, str(migrations_dir))

            logger.info("DuckDB migrations applied successfully using duckup")

        except ImportError:
            logger.error("duckup library not found. " "Install it with: pip install duckup")
            raise
        except duckup.MigrationError as e:
            logger.error(f"Migration error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during migration: {e}")
            raise

    def setup_schema(self) -> None:
        """Set up database schema using migrations.

        This is a legacy method that now uses migrations instead of direct SQL.
        It ensures the database exists and runs all pending migrations.
        """
        logger.warning(
            "setup_schema() is deprecated. Use ensure_database() and run_migrations() instead."
        )
        self.ensure_database()
        self.run_migrations()

    def _ensure_chsql_loaded(self) -> None:
        """Ensure the chsql extension is installed and loaded.

        The chsql extension provides ClickHouse SQL compatibility for DuckDB,
        including ClickHouse functions and syntax support.
        """
        try:
            # Try to install chsql if not already installed
            try:
                self._con.execute("INSTALL chsql FROM community")
                logger.info("Installed chsql extension")
            except Exception:
                # Extension might already be installed, continue
                pass

            # Load the chsql extension
            self._con.execute("LOAD chsql")
            logger.info("Loaded chsql extension for ClickHouse SQL compatibility")
        except Exception as e:
            logger.warning(
                f"Could not load chsql extension: {e}. "
                "ClickHouse SQL compatibility may be limited. "
                "Install with: INSTALL chsql FROM community; LOAD chsql;"
            )

    # Regex pattern to match ClickHouse-style parameters: {param:Type}
    PARAM_RE = re.compile(r"\{(\w+):[^\}]+\}")

    def _convert_query_parameters(
        self, query: str, parameters: Optional[dict] = None
    ) -> tuple[str, Optional[list]]:
        """Convert ClickHouse-style parameters to DuckDB format.

        ClickHouse uses {param:Type} format, DuckDB uses ? placeholders.
        Parameters are extracted in SQL order (left to right).

        Args:
            query: SQL query with ClickHouse-style parameters
            parameters: Parameter dictionary (ClickHouse-style)

        Returns:
            Tuple of (converted_query, duckdb_params_list)

        Raises:
            KeyError: If a parameter in the query is missing from the parameters dict
        """
        if not parameters:
            return query, None

        duckdb_params: list[Any] = []

        def replacer(match: re.Match) -> str:
            """Replace parameter placeholder with ? and collect parameter value.

            Handles both scalar values and arrays/lists for IN clauses.
            """
            name = match.group(1)
            if name not in parameters:
                raise KeyError(f"Missing parameter: {name}")

            value = parameters[name]

            # Handle array/list parameters for IN clauses
            if isinstance(value, (list, tuple)):
                # Fail fast on empty lists - invalid SQL: WHERE id IN ()
                if not value:
                    raise ValueError(f"Empty list provided for parameter '{name}'")
                placeholders = ", ".join("?" for _ in value)
                duckdb_params.extend(value)
                return f"({placeholders})"

            # Handle scalar parameters
            duckdb_params.append(value)
            return "?"

        converted_query = self.PARAM_RE.sub(replacer, query)

        return converted_query, duckdb_params
