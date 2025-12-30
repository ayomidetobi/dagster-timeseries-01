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

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.constants import DEFAULT_BATCH_SIZE
from dagster_quickstart.utils.datetime_utils import (
    ensure_utc,
    normalize_timestamp_precision,
    utc_now_metadata,
)
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
    batch_size: int = DEFAULT_BATCH_SIZE

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
        elif isinstance(obj, list) and all(isinstance(value, TimeSeriesValue) for value in obj):
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

        # Normalize timestamps to UTC with DateTime64(6) precision
        # Convert pandas Timestamp to datetime and normalize
        def normalize_timestamp(ts: Any) -> datetime:
            """Normalize a timestamp to UTC with DateTime64(6) precision."""
            if hasattr(ts, "to_pydatetime"):
                # pandas Timestamp
                dt = ts.to_pydatetime()
            elif isinstance(ts, datetime):
                dt = ts
            else:
                # Fallback: try to convert
                dt = pd.to_datetime(ts).to_pydatetime()
            return normalize_timestamp_precision(ensure_utc(dt), 6)

        df["timestamp"] = df["timestamp"].apply(normalize_timestamp)

        # Sort by timestamp
        df = df.sort_values("timestamp")

        # Add metadata columns
        now = utc_now_metadata()
        df["created_at"] = now
        df["updated_at"] = now

        # Prepare data for insertion
        data = df[["series_id", "timestamp", "value", "created_at", "updated_at"]].values.tolist()

        # Batch insert
        with self.clickhouse.get_connection() as client:
            for batch_start_idx in range(0, len(data), self.batch_size):
                batch = data[batch_start_idx : batch_start_idx + self.batch_size]
                client.insert(
                    self.table_name,
                    batch,
                    column_names=["series_id", "timestamp", "value", "created_at", "updated_at"],
                )

        context.log.info(f"Inserted {len(data)} rows into {self.table_name}")

    def _insert_timeseries_batch(self, context: OutputContext, batch: TimeSeriesBatch) -> None:
        """Insert TimeSeriesBatch into ClickHouse."""
        data = []
        now = utc_now_metadata()

        for value in batch.values:
            # Normalize timestamp to UTC with DateTime64(6) precision
            normalized_timestamp = normalize_timestamp_precision(ensure_utc(value.timestamp), 6)
            data.append(
                [
                    value.series_id,
                    normalized_timestamp,
                    value.value,
                    now,
                    now,
                ]
            )

        # Batch insert
        with self.clickhouse.get_connection() as client:
            for batch_start_idx in range(0, len(data), self.batch_size):
                batch_data = data[batch_start_idx : batch_start_idx + self.batch_size]
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
        batch_size=DEFAULT_BATCH_SIZE,
    )
