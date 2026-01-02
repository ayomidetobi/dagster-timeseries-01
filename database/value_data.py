"""Value data management for time-series data.

Idempotency:
    The insert_batch_value_data method supports idempotent inserts via the
    delete_before_insert parameter. When enabled, existing data for the partition
    date is deleted before insertion, ensuring re-running a partition doesn't
    create duplicates.

    This is essential for backfill-safe assets that may be re-run multiple times.

Example:
        # Idempotent insert (deletes existing data for partition_date first)
        manager.insert_batch_value_data(
            data=value_data,
            delete_before_insert=True,
            partition_date=target_date,
        )

        # Non-idempotent insert (may create duplicates if re-run)
        manager.insert_batch_value_data(data=value_data)
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from dagster import get_dagster_logger

from dagster_quickstart.resources import ClickHouseResource
from dagster_quickstart.utils.constants import DEFAULT_BATCH_SIZE
from dagster_quickstart.utils.datetime_utils import validate_timestamp
from dagster_quickstart.utils.exceptions import DatabaseInsertError

# Constants
VALUE_DATA_TABLE = "valueData"

logger = get_dagster_logger()


class ValueDataManager:
    """Manager for value data operations."""

    def __init__(self, clickhouse: ClickHouseResource, batch_size: int = DEFAULT_BATCH_SIZE):
        """Initialize with ClickHouse resource.

        Args:
            clickhouse: ClickHouse resource instance
            batch_size: Batch size for insertions (default: 10000)
        """
        self.clickhouse = clickhouse
        self.batch_size = batch_size

    def insert_value_data(
        self,
        series_id: int,
        timestamp: datetime,
        value: float,
    ) -> None:
        """Insert a single value data record.

        Note: created_at and updated_at use database defaults (now64(6)).

        Args:
            series_id: Series ID
            timestamp: Timestamp for the value
            value: Value to insert
        """
        # Use batch insert with single record for consistency
        self.insert_batch_value_data(
            [{"series_id": series_id, "timestamp": timestamp, "value": value}]
        )

    def insert_batch_value_data(
        self,
        data: List[Dict[str, Any]],
        delete_before_insert: bool = False,
        partition_date: Optional[datetime] = None,
    ) -> int:
        """Insert a batch of value data records using ClickHouse bulk insert.

        Uses ClickHouse client's native insert method for efficient bulk inserts.
        created_at and updated_at use database defaults (now64(6)).

        For idempotency, set delete_before_insert=True to delete existing data for
        the partition date before inserting. This ensures re-running a partition
        doesn't create duplicates.

        Args:
            data: List of dicts with keys: series_id, timestamp, value
            delete_before_insert: If True, delete existing data for the partition date
                before inserting. Requires partition_date to be set.
            partition_date: Date for partition-based deletion (required if
                delete_before_insert=True). Should be UTC timezone-aware datetime.

        Returns:
            Number of rows inserted

        Raises:
            ValueError: If data format is invalid or partition_date is missing when required
            DatabaseInsertError: If insertion fails
        """
        if not data:
            return 0

        # Handle delete-before-insert for idempotency
        if delete_before_insert:
            if partition_date is None:
                raise ValueError(
                    "partition_date is required when delete_before_insert=True. "
                    "This ensures idempotent inserts by deleting existing data for the partition."
                )

            # Validate and normalize partition date
            normalized_partition_date = validate_timestamp(
                partition_date, field_name="partition_date"
            )

            # Get unique series_ids from the data to delete
            series_ids_to_delete = {int(record["series_id"]) for record in data}

            # Delete data for each series for the partition date (entire day)
            partition_start = normalized_partition_date.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            partition_end = normalized_partition_date.replace(
                hour=23, minute=59, second=59, microsecond=999999
            )

            for series_id in series_ids_to_delete:
                self.delete_partition_data(series_id, partition_start, partition_end)
                logger.info(
                    "Deleted existing data before insert for idempotency",
                    extra={
                        "series_id": series_id,
                        "partition_date": normalized_partition_date.date().isoformat(),
                        "table": VALUE_DATA_TABLE,
                    },
                )

        rows_inserted = 0

        # Convert to list of lists format for ClickHouse insert
        # Only include columns that are not using database defaults
        column_names = ["series_id", "timestamp", "value"]
        batch_data: List[List[Any]] = []

        for record in data:
            if not all(key in record for key in column_names):
                raise ValueError(
                    f"Record missing required keys. Expected: {column_names}, "
                    f"Got: {list(record.keys())}"
                )

            # Validate and normalize timestamp to UTC with DateTime64(6) precision
            timestamp = validate_timestamp(record["timestamp"], field_name="timestamp")

            batch_data.append(
                [
                    int(record["series_id"]),
                    timestamp,
                    float(record["value"]),
                ]
            )

        # Process in batches using ClickHouse native insert
        for batch_start_idx in range(0, len(batch_data), self.batch_size):
            batch = batch_data[batch_start_idx : batch_start_idx + self.batch_size]
            try:
                self.clickhouse.insert_data(
                    table=VALUE_DATA_TABLE,
                    data=batch,
                    column_names=column_names,
                )
                rows_inserted += len(batch)
                logger.info(
                    "Inserted batch of value data",
                    extra={
                        "batch_index": batch_start_idx // self.batch_size,
                        "batch_size": len(batch),
                        "table": VALUE_DATA_TABLE,
                    },
                )
            except Exception as e:
                logger.error(
                    "Error inserting batch of value data",
                    extra={
                        "batch_index": batch_start_idx // self.batch_size,
                        "batch_size": len(batch),
                        "table": VALUE_DATA_TABLE,
                        "error": str(e),
                    },
                )
                raise DatabaseInsertError(f"Failed to insert batch into {VALUE_DATA_TABLE}") from e

        logger.info(
            "Completed batch insert of value data",
            extra={"total_rows": rows_inserted, "table": VALUE_DATA_TABLE},
        )
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
            # Validate and normalize start_date to UTC with DateTime64(6) precision
            normalized_start_date = validate_timestamp(start_date, field_name="start_date")
            query += " AND timestamp >= {start_date:DateTime64(6)}"
            params["start_date"] = normalized_start_date

        if end_date:
            # Validate and normalize end_date to UTC with DateTime64(6) precision
            normalized_end_date = validate_timestamp(end_date, field_name="end_date")
            query += " AND timestamp <= {end_date:DateTime64(6)}"
            params["end_date"] = normalized_end_date

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

    def get_latest_timestamps_for_series(
        self, series_ids: List[int]
    ) -> Dict[int, Optional[datetime]]:
        """Get the latest timestamp for multiple series in a single query.

        Args:
            series_ids: List of series IDs

        Returns:
            Dictionary mapping series_id to latest timestamp (or None if no data exists)
        """
        if not series_ids:
            return {}

        # Build query with IN clause for multiple series_ids
        query = f"""
        SELECT series_id, max(timestamp) as latest_timestamp
        FROM {VALUE_DATA_TABLE}
        WHERE series_id IN ({{series_ids:Array(UInt32)}})
        GROUP BY series_id
        """
        result = self.clickhouse.execute_query(query, parameters={"series_ids": series_ids})

        # Build dictionary from results
        timestamps_map: Dict[int, Optional[datetime]] = {}
        if hasattr(result, "result_rows") and result.result_rows:
            for row in result.result_rows:
                series_id = int(row[0])
                latest_timestamp = row[1] if row[1] else None
                timestamps_map[series_id] = latest_timestamp

        # Ensure all requested series_ids are in the result (with None if no data)
        for series_id in series_ids:
            if series_id not in timestamps_map:
                timestamps_map[series_id] = None

        return timestamps_map

    def delete_partition_data(
        self,
        series_id: int,
        start_date: datetime,
        end_date: datetime,
    ) -> int:
        """Delete value data for a series within a date range (partition-based deletion).

        This method provides idempotency by allowing deletion of data before re-insertion.
        Used for partition-based ingestion where we want to ensure no duplicates when
        re-running a partition.

        Args:
            series_id: Series ID
            start_date: Start date (inclusive) - must be UTC timezone-aware
            end_date: End date (inclusive) - must be UTC timezone-aware

        Returns:
            Number of rows deleted (approximate - ClickHouse ALTER DELETE doesn't return exact count)

        Raises:
            ValueError: If dates are invalid or start_date > end_date
            DatabaseError: If deletion fails
        """
        # Validate and normalize timestamps
        normalized_start = validate_timestamp(start_date, field_name="start_date")
        normalized_end = validate_timestamp(end_date, field_name="end_date")

        if normalized_start > normalized_end:
            raise ValueError(
                f"start_date ({normalized_start}) must be <= end_date ({normalized_end})"
            )

        query = f"""
        ALTER TABLE {VALUE_DATA_TABLE}
        DELETE WHERE series_id = {{series_id:UInt32}}
          AND timestamp >= {{start_date:DateTime64(6)}}
          AND timestamp <= {{end_date:DateTime64(6)}}
        """

        try:
            self.clickhouse.execute_command(
                query,
                parameters={
                    "series_id": series_id,
                    "start_date": normalized_start,
                    "end_date": normalized_end,
                },
            )
            logger.info(
                "Deleted partition data for series",
                extra={
                    "series_id": series_id,
                    "start_date": normalized_start.isoformat(),
                    "end_date": normalized_end.isoformat(),
                    "table": VALUE_DATA_TABLE,
                },
            )
            # ClickHouse ALTER DELETE doesn't return row count, return -1 to indicate unknown
            return -1
        except Exception as e:
            logger.error(
                "Error deleting partition data",
                extra={
                    "series_id": series_id,
                    "start_date": normalized_start.isoformat(),
                    "end_date": normalized_end.isoformat(),
                    "table": VALUE_DATA_TABLE,
                    "error": str(e),
                },
            )
            from dagster_quickstart.utils.exceptions import DatabaseError

            raise DatabaseError(
                f"Failed to delete partition data for series_id {series_id} "
                f"from {normalized_start} to {normalized_end}: {e}"
            ) from e
