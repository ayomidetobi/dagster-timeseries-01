"""Database resource configuration utilities.

Provides functions to get the DuckDB database resource.
"""

from typing import Any, Type

from dagster_quickstart.resources import DuckDBResource

# Type alias for database resources - use the one from database.utils for consistency
from database.utils import DatabaseResource

# Type alias for database resource class
DatabaseResourceType = Type[DuckDBResource]


def get_database_resource_class() -> DatabaseResourceType:
    """Get the DuckDB database resource class.

    Returns:
        DuckDBResource class
    """
    return DuckDBResource


def get_database_resource(duckdb_cacher: Any) -> DatabaseResource:
    """Get the DuckDB database resource instance.

    Args:
        duckdb_cacher: duckdb_datacacher instance for DuckDB (required).

    Returns:
        DuckDBResource instance

    Raises:
        ValueError: If duckdb_cacher is not provided
    """
    if duckdb_cacher is None:
        raise ValueError(
            "DuckDB requires a duckdb_datacacher instance. "
            "Provide it via the duckdb_cacher parameter or set it up in definitions.py"
        )
    return DuckDBResource(cacher=duckdb_cacher)


def is_duckdb() -> bool:
    """Check if DuckDB is the configured database.

    Returns:
        True (always DuckDB now)
    """
    return True
