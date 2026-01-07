"""Protocol/interface for database resources.

This protocol defines the common interface that both ClickHouse and DuckDB
resources must implement, allowing managers to work with either database.
"""

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional, Protocol


class DatabaseResource(Protocol):
    """Protocol defining the interface for database resources.

    Both ClickHouseResource and DuckDBResource must implement this interface
    to allow managers to work with either database seamlessly.
    """

    def execute_query(self, query: str, parameters: Optional[dict] = None) -> Any:
        """Execute a query and return results.

        Args:
            query: SQL query string
            parameters: Query parameters (database-specific format)

        Returns:
            Query result object with result_rows and column_names attributes
        """
        ...

    def execute_command(self, command: str, parameters: Optional[dict] = None) -> None:
        """Execute a command (DDL/DML) without returning results.

        Args:
            command: SQL command string
            parameters: Command parameters (database-specific format)
        """
        ...

    def insert_data(
        self,
        table: str,
        data: list,
        column_names: Optional[list] = None,
        database: Optional[str] = None,
    ) -> None:
        """Insert data into a table.

        Args:
            table: Table name
            data: List of rows (list of lists or list of tuples)
            column_names: Optional list of column names
            database: Optional database name
        """
        ...

    def get_connection(self) -> Any:
        """Get a database connection (context manager or direct).

        Returns:
            Database connection object
        """
        ...

    def ensure_database(self) -> None:
        """Ensure the database exists."""
        ...

    def run_migrations(self) -> None:
        """Run all pending migrations."""
        ...

    def get_migrations_directory(self) -> Path:
        """Get the migrations directory path.

        Returns:
            Path to migrations directory
        """
        ...
