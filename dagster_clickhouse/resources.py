"""Dagster resources for the financial platform."""

import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
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
        host = config("CLICKHOUSE_HOST", default="localhost")
        port = config("CLICKHOUSE_PORT", default=9000, cast=int)

        # Auto-detect secure connection for cloud ClickHouse
        is_cloud = ".clickhouse.cloud" in host or port in (8443, 443, 9440)
        secure = config("CLICKHOUSE_SECURE", default=is_cloud, cast=bool)

        return cls(
            host=host,
            port=port,
            database=config("CLICKHOUSE_DATABASE", default="default"),
            user=config("CLICKHOUSE_USER", default="default"),
            password=config("CLICKHOUSE_PASSWORD", default=""),
            secure=secure,
            verify=config("CLICKHOUSE_VERIFY", default=True, cast=bool),
            timeout=config("CLICKHOUSE_TIMEOUT", default=30, cast=int),
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
        # Connect without database to create it
        client = clickhouse_connect.get_client(
            host=self.host,
            port=self.port,
            username=self.user,
            password=self.password,
            secure=self.secure,
            verify=self.verify,
            connect_timeout=self.timeout,
        )
        try:
            client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            logger.info(f"Database {self.database} ensured")
        finally:
            client.close()

    def get_migrations_directory(self) -> Path:
        """Get the migrations directory path.

        Returns:
            Path to migrations directory
        """
        # Assume migrations directory is at project root
        # This works when called from dagster_clickhouse package
        current_file = Path(__file__)
        project_root = current_file.parent.parent
        migrations_dir = project_root / "migrations"
        migrations_dir.mkdir(exist_ok=True)
        return migrations_dir

    def get_migration_url(self) -> str:
        """Get ClickHouse connection URL for migrations using clickhousedb protocol.

        Returns:
            Connection URL in format expected by clickhouse-migrate with clickhousedb protocol
        """
        # Use clickhousedb protocol (works with clickhouse-driver)
        if self.password:
            return f"clickhousedb://{self.user}:{self.password}@{self.host}/{self.database}?secure={str(self.secure).lower()}"
        else:
            return f"clickhousedb://{self.user}@{self.host}/{self.database}?secure={str(self.secure).lower()}"

    def run_migrations(self) -> None:
        """Run all pending migrations using clickhouse-migrate.

        This method uses the clickhouse-migrate CLI to apply migrations.
        Migrations are tracked and only pending ones are applied.
        """
        try:
            migrations_dir = self.get_migrations_directory()

            # Set environment variables for clickhouse-migrate
            env = os.environ.copy()
            env["CLICKHOUSE_MIGRATE_DATABASES"] = self.get_migration_url()
            env["CLICKHOUSE_MIGRATE_DIRECTORY"] = str(migrations_dir)

            # Run clickhouse-migrate migrate command
            result = subprocess.run(
                ["clickhouse-migrate", "migrate"],
                env=env,
                cwd=str(migrations_dir.parent),
                capture_output=True,
                text=True,
                check=False,  # We handle errors manually
            )

            if result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                logger.error(f"Migration failed: {error_msg}")
                raise RuntimeError(f"Migration failed: {error_msg}")

            logger.info("Migrations applied successfully")
            if result.stdout:
                logger.info(result.stdout)

        except FileNotFoundError:
            logger.error(
                "clickhouse-migrate CLI not found. "
                "Install it with: pip install clickhouse-migrate"
            )
            raise
        except Exception as e:
            logger.error(f"Error running migrations: {e}")
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


def init_clickhouse_resource(context: InitResourceContext) -> ClickHouseResource:
    """Initialize ClickHouse resource from context."""
    return ClickHouseResource.from_config()
