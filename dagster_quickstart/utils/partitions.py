"""Partition definitions for backfill-safe assets."""

from datetime import datetime

from dagster import (
    DailyPartitionsDefinition,
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
)

from dagster_quickstart.utils.datetime_utils import parse_datetime_string

# Daily partition for time-series data ingestion and calculations
# Start date can be adjusted based on when historical data begins
DAILY_PARTITION = DailyPartitionsDefinition(start_date="2025-12-01")

# Dynamic partition for meta_series (one partition per series_id)
META_SERIES_PARTITION = DynamicPartitionsDefinition(name="meta_series")

# Multi-dimensional partition combining daily and meta_series partitions
# Each partition represents one series (by series_id) for one date
BLOOMBERG_INGESTION_PARTITION = MultiPartitionsDefinition(
    {
        "date": DAILY_PARTITION,
        "series": META_SERIES_PARTITION,
    }
)


def get_partition_date(partition_key: str) -> datetime:
    """Convert partition key to UTC timezone-aware datetime.

    Args:
        partition_key: Partition key string in format YYYY-MM-DD

    Returns:
        UTC timezone-aware datetime for the partition date (midnight UTC)
    """
    # Parse and normalize to UTC with time at midnight
    dt = parse_datetime_string(partition_key)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def format_partition_key(date: datetime) -> str:
    """Format datetime as partition key.

    Args:
        date: Datetime object

    Returns:
        Partition key string in format YYYY-MM-DD
    """
    return date.strftime("%Y-%m-%d")
