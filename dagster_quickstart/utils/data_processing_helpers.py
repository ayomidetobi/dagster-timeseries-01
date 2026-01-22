"""Data processing helper functions."""

from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext

from dagster_quickstart.utils.datetime_utils import parse_timestamp, validate_timestamp


def process_time_series_data_points(
    data_points: List[Dict[str, Any]],
    series_id: int,
    series_code: str,
    context: Optional[AssetExecutionContext] = None,
) -> List[Dict[str, Any]]:
    """Process and validate time-series data points from external sources.

    Parses timestamps and validates them, converting to UTC with proper precision.
    Filters out invalid data points.

    Args:
        data_points: Raw data points with 'timestamp' and 'value' keys
        series_id: Series ID
        series_code: Series code for logging
        context: Optional Dagster execution context for logging

    Returns:
        List of validated value data dictionaries with keys: series_id, timestamp, value
    """
    value_data = []
    for point in data_points:
        timestamp = point.get("timestamp")
        value = point.get("value")

        if timestamp and value is not None:
            # Parse and validate timestamp to UTC with DateTime64(6) precision
            parsed_timestamp = parse_timestamp(timestamp)
            if parsed_timestamp is None:
                if context:
                    context.log.warning(
                        "Could not parse timestamp",
                        extra={
                            "series_id": series_id,
                            "series_code": series_code,
                            "timestamp": str(timestamp),
                        },
                    )
                continue

            # Validate and normalize timestamp
            try:
                validated_timestamp = validate_timestamp(parsed_timestamp, field_name="timestamp")
            except ValueError as e:
                if context:
                    context.log.warning(
                        "Invalid timestamp",
                        extra={
                            "series_id": series_id,
                            "series_code": series_code,
                            "timestamp": str(timestamp),
                            "error": str(e),
                        },
                    )
                continue

            value_data.append(
                {
                    "series_id": series_id,
                    "timestamp": validated_timestamp,
                    "value": float(value),
                }
            )

    return value_data


def create_ingestion_result_dict(
    series_id: int, series_code: str, rows_ingested: int, status: str, reason: Optional[str] = None
) -> Dict[str, Any]:
    """Create a result dictionary for ingestion operations.

    Args:
        series_id: Series ID
        series_code: Series code
        rows_ingested: Number of rows ingested
        status: Status string ("success", "skipped", "failed")
        reason: Optional reason for status

    Returns:
        Dictionary with ingestion results
    """
    result = {
        "series_id": series_id,
        "series_code": series_code,
        "rows_ingested": rows_ingested,
        "status": status,
    }
    if reason:
        result["reason"] = reason
    return result
