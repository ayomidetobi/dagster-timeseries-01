"""Value data management for time-series data."""

from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster_clickhouse.resources import ClickHouseResource

# Constants
VALUE_DATA_TABLE = "valueData"
BATCH_SIZE = 1000


class ValueDataManager:
    """Manager for value data operations."""

    def __init__(self, clickhouse: ClickHouseResource):
        """Initialize with ClickHouse resource."""
        self.clickhouse = clickhouse

    def insert_value_data(
        self,
        series_id: int,
        timestamp: datetime,
        value: float,
    ) -> None:
        """Insert a single value data record."""
        now = datetime.now()
        query = f"""
        INSERT INTO {VALUE_DATA_TABLE} (
            series_id, timestamp, value, created_at, updated_at
        ) VALUES (
            {{series_id:UInt32}}, {{timestamp:DateTime64(6)}}, {{value:Float64}},
            {{now:DateTime64(3)}}, {{now:DateTime64(3)}}
        )
        """
        self.clickhouse.execute_command(
            query,
            parameters={
                "series_id": series_id,
                "timestamp": timestamp,
                "value": value,
                "now": now,
            },
        )

    def insert_batch_value_data(
        self,
        data: List[Dict[str, Any]],
    ) -> int:
        """Insert a batch of value data records.

        Args:
            data: List of dicts with keys: series_id, timestamp, value

        Returns:
            Number of rows inserted
        """
        if not data:
            return 0

        now = datetime.now()
        rows_inserted = 0

        # Process in batches
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i : i + BATCH_SIZE]
            values = []
            params = {"now": now}

            for idx, record in enumerate(batch):
                series_id = record["series_id"]
                timestamp = record["timestamp"]
                value = record["value"]

                values.append(
                    f"({{series_id_{idx}:UInt32}}, {{timestamp_{idx}:DateTime64(6)}}, "
                    f"{{value_{idx}:Float64}}, {{now:DateTime64(3)}}, {{now:DateTime64(3)}})"
                )
                params[f"series_id_{idx}"] = series_id
                params[f"timestamp_{idx}"] = timestamp
                params[f"value_{idx}"] = value

            query = f"""
            INSERT INTO {VALUE_DATA_TABLE} (
                series_id, timestamp, value, created_at, updated_at
            ) VALUES {', '.join(values)}
            """

            try:
                self.clickhouse.execute_command(query, parameters=params)
                rows_inserted += len(batch)
            except Exception as e:
                # Log error but continue with next batch
                print(f"Error inserting batch {i}: {e}")
                raise

        return rows_inserted

    def get_value_data(
        self,
        series_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        """Get value data for a series.

        Args:
            series_id: Series ID
            start_date: Optional start date filter
            end_date: Optional end date filter
            limit: Maximum number of records to return

        Returns:
            List of dicts with timestamp and value
        """
        query = f"SELECT timestamp, value FROM {VALUE_DATA_TABLE} WHERE series_id = {{series_id:UInt32}}"
        params = {"series_id": series_id}

        if start_date:
            query += " AND timestamp >= {start_date:DateTime64(6)}"
            params["start_date"] = start_date

        if end_date:
            query += " AND timestamp <= {end_date:DateTime64(6)}"
            params["end_date"] = end_date

        query += " ORDER BY timestamp DESC LIMIT {limit:UInt32}"
        params["limit"] = limit

        result = self.clickhouse.execute_query(query, parameters=params)
        if hasattr(result, "result_rows") and result.result_rows:
            columns = result.column_names
            return [dict(zip(columns, row)) for row in result.result_rows]
        return []

    def get_latest_timestamp(self, series_id: int) -> Optional[datetime]:
        """Get the latest timestamp for a series.

        Args:
            series_id: Series ID

        Returns:
            Latest timestamp or None if no data exists
        """
        query = f"""
        SELECT max(timestamp) as latest_timestamp
        FROM {VALUE_DATA_TABLE}
        WHERE series_id = {{series_id:UInt32}}
        """
        result = self.clickhouse.execute_query(query, parameters={"series_id": series_id})
        if hasattr(result, "result_rows") and result.result_rows and result.result_rows[0][0]:
            return result.result_rows[0][0]
        return None

