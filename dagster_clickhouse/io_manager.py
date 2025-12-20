"""ClickHouse IO Manager for Dagster assets."""

from datetime import datetime
from typing import Any

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    io_manager,
)

from dagster_clickhouse.resources import ClickHouseResource
from database.models import TimeSeriesBatch, TimeSeriesValue

# Try to import polars, but make it optional
try:
    import polars as pl

    POLARS_AVAILABLE = True
except ImportError:
    pl = None  # type: ignore
    POLARS_AVAILABLE = False


class ClickHouseIOManager(ConfigurableIOManager):
    """IO Manager for storing and loading time-series data in ClickHouse."""

    clickhouse: ClickHouseResource
    table_name: str = "valueData"
    batch_size: int = 10000

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Handle output from assets - store data in ClickHouse."""
        if obj is None:
            return

        # Handle Polars DataFrame - convert to pandas
        if POLARS_AVAILABLE and isinstance(obj, pl.DataFrame):
            context.log.debug("Converting Polars DataFrame to pandas DataFrame")
            obj = obj.to_pandas()

        # Handle pandas DataFrame
        if isinstance(obj, pd.DataFrame):
            self._insert_dataframe(context, obj)
        # Handle TimeSeriesBatch
        elif isinstance(obj, TimeSeriesBatch):
            self._insert_timeseries_batch(context, obj)
        # Handle list of TimeSeriesValue
        elif isinstance(obj, list) and all(isinstance(v, TimeSeriesValue) for v in obj):
            batch = TimeSeriesBatch(series_id=obj[0].series_id, values=obj)
            self._insert_timeseries_batch(context, batch)
        else:
            raise ValueError(f"Unsupported output type: {type(obj)}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Load input data from ClickHouse."""
        series_id = context.metadata.get("series_id")
        start_time = context.metadata.get("start_time")
        end_time = context.metadata.get("end_time")

        if series_id is None:
            raise ValueError("series_id must be provided in metadata")

        query = f"SELECT * FROM {self.table_name} WHERE series_id = {{series_id:UInt32}}"
        params = {"series_id": series_id}

        if start_time:
            query += " AND timestamp >= {start_time:DateTime64(6)}"
            params["start_time"] = start_time

        if end_time:
            query += " AND timestamp <= {end_time:DateTime64(6)}"
            params["end_time"] = end_time

        query += " ORDER BY timestamp"

        with self.clickhouse.get_connection() as client:
            result = client.query(query, parameters=params)
            if hasattr(result, "result_rows") and hasattr(result, "column_names"):
                df = result.result_rows
                columns = result.column_names

                if not df:
                    return pd.DataFrame(columns=columns)

                return pd.DataFrame(df, columns=columns)
            return pd.DataFrame()

    def _insert_dataframe(self, context: OutputContext, df: pd.DataFrame) -> None:
        """Insert pandas DataFrame into ClickHouse."""
        required_columns = ["series_id", "timestamp", "value"]
        if not all(col in df.columns for col in required_columns):
            raise ValueError(
                f"DataFrame must contain columns: {required_columns}. Found: {df.columns.tolist()}"
            )

        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            df["timestamp"] = pd.to_datetime(df["timestamp"])

        # Sort by timestamp
        df = df.sort_values("timestamp")

        # Add metadata columns
        now = datetime.now()
        df["created_at"] = now
        df["updated_at"] = now

        # Prepare data for insertion
        data = df[["series_id", "timestamp", "value", "created_at", "updated_at"]].values.tolist()

        # Batch insert
        with self.clickhouse.get_connection() as client:
            for i in range(0, len(data), self.batch_size):
                batch = data[i : i + self.batch_size]
                client.insert(
                    self.table_name,
                    batch,
                    column_names=["series_id", "timestamp", "value", "created_at", "updated_at"],
                )

        context.log.info(f"Inserted {len(data)} rows into {self.table_name}")

    def _insert_timeseries_batch(self, context: OutputContext, batch: TimeSeriesBatch) -> None:
        """Insert TimeSeriesBatch into ClickHouse."""
        data = []
        now = datetime.now()

        for value in batch.values:
            data.append(
                [
                    value.series_id,
                    value.timestamp,
                    value.value,
                    now,
                    now,
                ]
            )

        # Batch insert
        with self.clickhouse.get_connection() as client:
            for i in range(0, len(data), self.batch_size):
                batch_data = data[i : i + self.batch_size]
                client.insert(
                    self.table_name,
                    batch_data,
                    column_names=["series_id", "timestamp", "value", "created_at", "updated_at"],
                )

        context.log.info(f"Inserted {len(data)} rows into {self.table_name}")


@io_manager(required_resource_keys={"clickhouse"})
def clickhouse_io_manager(context) -> ClickHouseIOManager:
    """Factory function for ClickHouse IO Manager."""
    return ClickHouseIOManager(
        clickhouse=context.resources.clickhouse,
        table_name="valueData",
        batch_size=10000,
    )


class PassthroughIOManager(ConfigurableIOManager):
    """Simple IO manager that just passes through values without storing them."""

    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Handle output - just pass through, don't store."""
        # For dict outputs, we don't need to store them
        # They're just metadata/results
        context.log.debug(f"Passthrough IO manager: received {type(obj)}")
        return None

    def load_input(self, context: InputContext) -> Any:
        """Load input - not used for passthrough."""
        return None


@io_manager
def passthrough_io_manager(context) -> PassthroughIOManager:
    """Factory function for Passthrough IO Manager."""
    return PassthroughIOManager()
