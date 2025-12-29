"""Datetime utilities for consistent timezone-aware datetimes and parsing.

This module provides utilities for:
- Creating UTC timezone-aware datetimes
- Parsing timestamps with robust parsing (dateutil.parser.parse)
- Normalizing timestamps to match ClickHouse DateTime64 precision
- Validating timestamps before database insertion
"""

from datetime import datetime, timezone
from typing import Any, Optional

from dateutil import parser as dateutil_parser

# ClickHouse DateTime64 precision constants
# timestamp column uses DateTime64(6) - microsecond precision
TIMESTAMP_PRECISION = 6  # microseconds
# created_at/updated_at columns use DateTime64(3) - millisecond precision
METADATA_PRECISION = 3  # milliseconds

# UTC timezone
UTC = timezone.utc


def utc_now() -> datetime:
    """Get current UTC datetime.

    Returns:
        Current datetime in UTC timezone (microsecond precision)
    """
    return datetime.now(UTC)


def utc_now_metadata() -> datetime:
    """Get current UTC datetime normalized to metadata precision (DateTime64(3)).

    This is for created_at/updated_at columns which use DateTime64(3).

    Returns:
        Current datetime in UTC timezone (millisecond precision)
    """
    dt = datetime.now(UTC)
    # Normalize to millisecond precision
    return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)


def ensure_utc(dt: datetime) -> datetime:
    """Ensure datetime is UTC timezone-aware.

    If datetime is naive, assumes it's UTC.
    If datetime is timezone-aware, converts to UTC.

    Args:
        dt: Datetime object (naive or timezone-aware)

    Returns:
        UTC timezone-aware datetime
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt.replace(tzinfo=UTC)
    # Timezone-aware datetime - convert to UTC
    return dt.astimezone(UTC)


def normalize_timestamp_precision(dt: datetime, precision: int = TIMESTAMP_PRECISION) -> datetime:
    """Normalize datetime to specified microsecond precision.

    ClickHouse DateTime64(6) stores microseconds, so we normalize to 6 digits.

    Args:
        dt: Datetime object (will be converted to UTC if not already)
        precision: Number of decimal places for microseconds (default 6 for timestamp)

    Returns:
        UTC datetime normalized to specified precision
    """
    dt = ensure_utc(dt)
    if precision == 6:
        # Keep full microsecond precision
        return dt
    elif precision == 3:
        # Normalize to millisecond precision (round down to nearest millisecond)
        return dt.replace(microsecond=(dt.microsecond // 1000) * 1000)
    else:
        # For other precisions, truncate microseconds
        divisor = 10 ** (6 - precision)
        return dt.replace(microsecond=(dt.microsecond // divisor) * divisor)


def parse_timestamp(
    timestamp: Any, default_timezone: Optional[timezone] = UTC
) -> Optional[datetime]:
    """Parse timestamp from various formats using dateutil.parser.parse.

    This function uses dateutil.parser.parse for robust parsing of various
    date/time formats. The result is always normalized to UTC.

    Args:
        timestamp: Timestamp in various formats:
            - datetime object (returned as-is, normalized to UTC)
            - str: ISO format, YYYY-MM-DD, YYYYMMDD, or other dateutil-parsable formats
            - int: YYYYMMDD format (8 digits)
        default_timezone: Timezone to assume for naive datetimes (default UTC)

    Returns:
        UTC timezone-aware datetime, or None if parsing fails

    Examples:
        >>> parse_timestamp("2024-01-15")
        datetime.datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
        >>> parse_timestamp("2024-01-15T10:30:00Z")
        datetime.datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)
        >>> parse_timestamp(20240115)
        datetime.datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
    """
    if timestamp is None:
        return None

    # Already a datetime object
    if isinstance(timestamp, datetime):
        return normalize_timestamp_precision(ensure_utc(timestamp), TIMESTAMP_PRECISION)

    # Integer in YYYYMMDD format
    if isinstance(timestamp, int):
        date_str = str(timestamp)
        if len(date_str) == 8:  # YYYYMMDD format
            try:
                parsed = datetime.strptime(date_str, "%Y%m%d")
                return normalize_timestamp_precision(
                    parsed.replace(tzinfo=default_timezone), TIMESTAMP_PRECISION
                )
            except ValueError:
                return None

    # String - use dateutil.parser.parse for robust parsing
    if isinstance(timestamp, str):
        try:
            # dateutil.parser.parse can handle many formats including:
            # - ISO format: "2024-01-15T10:30:00Z"
            # - Date only: "2024-01-15"
            # - Various other formats
            parsed = dateutil_parser.parse(timestamp, default=datetime.now(UTC))
            # If parsed datetime is naive, use default_timezone
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=default_timezone)
            return normalize_timestamp_precision(ensure_utc(parsed), TIMESTAMP_PRECISION)
        except (ValueError, TypeError, OverflowError):
            # Parsing failed, return None
            # Log parsing errors would go here if we had logger access
            return None

    return None


def validate_timestamp(dt: datetime, field_name: str = "timestamp") -> datetime:
    """Validate and normalize timestamp for database insertion.

    Ensures timestamp is UTC timezone-aware and normalized to DateTime64(6) precision.

    Args:
        dt: Datetime object to validate
        field_name: Name of the field for error messages

    Returns:
        Validated UTC datetime normalized to microsecond precision

    Raises:
        ValueError: If datetime is invalid or cannot be normalized
    """
    if not isinstance(dt, datetime):
        raise ValueError(f"{field_name} must be a datetime object, got {type(dt)}")

    try:
        normalized = normalize_timestamp_precision(ensure_utc(dt), TIMESTAMP_PRECISION)
        return normalized
    except Exception as e:
        raise ValueError(f"Failed to normalize {field_name}: {e}") from e


def parse_datetime_string(date_str: str, default_timezone: Optional[timezone] = UTC) -> datetime:
    """Parse datetime string using dateutil.parser.parse.

    Convenience wrapper that raises ValueError on parse failure instead of returning None.

    Args:
        date_str: Datetime string to parse
        default_timezone: Timezone to assume for naive datetimes (default UTC)

    Returns:
        UTC timezone-aware datetime

    Raises:
        ValueError: If parsing fails
    """
    result = parse_timestamp(date_str, default_timezone)
    if result is None:
        raise ValueError(f"Could not parse datetime string: {date_str}")
    return result
