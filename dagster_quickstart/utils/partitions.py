"""Partition definitions for backfill-safe assets."""

from datetime import datetime

from dagster import DailyPartitionsDefinition

# Daily partition for time-series data ingestion and calculations
# Start date can be adjusted based on when historical data begins
DAILY_PARTITION = DailyPartitionsDefinition(start_date="2024-01-01")


def get_partition_date(partition_key: str) -> datetime:
    """Convert partition key to datetime.

    Args:
        partition_key: Partition key string in format YYYY-MM-DD

    Returns:
        Datetime object for the partition date
    """
    return datetime.strptime(partition_key, "%Y-%m-%d").replace(hour=0, minute=0, second=0, microsecond=0)


def format_partition_key(date: datetime) -> str:
    """Format datetime as partition key.

    Args:
        date: Datetime object

    Returns:
        Partition key string in format YYYY-MM-DD
    """
    return date.strftime("%Y-%m-%d")

