"""Database resource configuration utilities.

Provides functions to get the appropriate database resource based on configuration.
"""

from typing import Any, Optional, Type, Union

from decouple import config

from dagster_quickstart.resources import ClickHouseResource, DuckDBResource
from dagster_quickstart.utils.constants import DATABASE_TYPE

# Type alias for database resources - use the one from database.utils for consistency
from database.utils import DatabaseResource

# Type alias for database resource classes
DatabaseResourceType = Union[Type[ClickHouseResource], Type[DuckDBResource]]


def get_database_type() -> str:
    """Get the configured database type.

    Checks environment variable first, then falls back to constants.

    Returns:
        Database type string: "clickhouse" or "duckdb"
    """
    return config("DATABASE_TYPE", default=DATABASE_TYPE).lower()


def get_database_resource_class() -> DatabaseResourceType:
    """Get the database resource class based on configuration.

    Returns:
        Database resource class (ClickHouseResource or DuckDBResource)

    Raises:
        ValueError: If database type is not recognized
    """
    db_type = get_database_type()

    if db_type == "clickhouse":
        return ClickHouseResource
    elif db_type == "duckdb":
        return DuckDBResource
    else:
        raise ValueError(
            f"Invalid database type: {db_type}. "
            "Must be 'clickhouse' or 'duckdb'. "
            f"Set via DATABASE_TYPE environment variable or update DATABASE_TYPE in constants.py"
        )


def get_database_resource(duckdb_cacher: Optional[Any] = None) -> DatabaseResource:
    """Get the configured database resource instance.

    Args:
        duckdb_cacher: Optional duckdb_datacacher instance for DuckDB.
            Required if DATABASE_TYPE is "duckdb".

    Returns:
        Database resource instance (ClickHouseResource or DuckDBResource)

    Raises:
        ValueError: If database type is not recognized
        ValueError: If DuckDB is selected but duckdb_cacher is not provided
    """
    db_type = get_database_type()

    if db_type == "clickhouse":
        return ClickHouseResource.from_config()
    elif db_type == "duckdb":
        if duckdb_cacher is None:
            raise ValueError(
                "DuckDB requires a duckdb_datacacher instance. "
                "Provide it via the duckdb_cacher parameter or set it up in definitions.py"
            )
        return DuckDBResource(cacher=duckdb_cacher)
    else:
        raise ValueError(
            f"Invalid database type: {db_type}. "
            "Must be 'clickhouse' or 'duckdb'. "
            f"Set via DATABASE_TYPE environment variable or update DATABASE_TYPE in constants.py"
        )


def is_clickhouse() -> bool:
    """Check if ClickHouse is the configured database.

    Returns:
        True if ClickHouse, False if DuckDB
    """
    return get_database_type() == "clickhouse"


def is_duckdb() -> bool:
    """Check if DuckDB is the configured database.

    Returns:
        True if DuckDB, False if ClickHouse
    """
    return get_database_type() == "duckdb"

