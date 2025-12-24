"""Dagster resources for the financial platform."""

from contextlib import contextmanager
from typing import Any, Iterator, Optional

import clickhouse_connect
from clickhouse_connect.driver import Client
from dagster import ConfigurableResource, InitResourceContext, get_dagster_logger
from decouple import config

logger = get_dagster_logger()


class ClickHouseResource(ConfigurableResource):
    """ClickHouse database resource for Dagster."""

    host: str = "localhost"
    port: int = 9000
    database: str = "financial_platform"
    user: str = "default"
    password: str = ""
    secure: bool = False
    verify: bool = True
    timeout: int = 30

    @classmethod
    def from_config(cls) -> "ClickHouseResource":
        """Create resource from configuration."""
        return cls(
            host=config("CLICKHOUSE_HOST", default="localhost"),
            port=config("CLICKHOUSE_PORT", default=9000),
            database=config("CLICKHOUSE_DATABASE", default="default"),
            user=config("CLICKHOUSE_USER", default="default"),
            password=config("CLICKHOUSE_PASSWORD", default=""),
            secure=config("CLICKHOUSE_SECURE", default=False),
            verify=config("CLICKHOUSE_VERIFY", default=True),
            timeout=config("CLICKHOUSE_TIMEOUT", default=30),
        )

    def get_client(self) -> Client:
        """Get a ClickHouse client connection."""
        return clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            database=self.database,
            username=self.user,
            password=self.password,
            secure=self.secure,
            verify=self.verify,
            connect_timeout=self.timeout,
        )

    @contextmanager
    def get_connection(self) -> Iterator[Client]:
        """Context manager for ClickHouse connections."""
        client = self.get_client()
        try:
            yield client
        finally:
            client.close()

    def execute_query(self, query: str, parameters: Optional[dict] = None) -> Any:
        """Execute a query and return results."""
        with self.get_connection() as client:
            return client.query(query, parameters=parameters)

    def execute_command(self, command: str, parameters: Optional[dict] = None) -> None:
        """Execute a command (DDL/DML) without returning results."""
        with self.get_connection() as client:
            client.command(command, parameters=parameters)

    def insert_data(
        self,
        table: str,
        data: list,
        column_names: Optional[list] = None,
        database: Optional[str] = None,
    ) -> None:
        """Insert data into a ClickHouse table."""
        with self.get_connection() as client:
            db = database or self.database
            client.insert(table, data, column_names=column_names, database=db)

    def ensure_database(self) -> None:
        """Ensure the database exists."""
        with self.get_connection() as client:
            client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")

    def setup_schema(self) -> None:
        """Set up all table schemas."""
        from database.schemas import ClickHouseSchema

        self.ensure_database()

        with self.get_connection() as client:
            # Create all tables
            for table_name, schema_sql in ClickHouseSchema.get_all_schemas().items():
                try:
                    client.command(schema_sql)
                except Exception as e:
                    # Log error but continue
                    logger.warning(f"Could not create table {table_name}: {e}")

            # Create indexes
            for index_sql in ClickHouseSchema.get_indexes():
                try:
                    client.command(index_sql)
                except Exception as e:
                    # Log error but continue
                    logger.warning(f"Could not create index: {e}")


def init_clickhouse_resource(context: InitResourceContext) -> ClickHouseResource:
    """Initialize ClickHouse resource from context."""
    return ClickHouseResource.from_config()
