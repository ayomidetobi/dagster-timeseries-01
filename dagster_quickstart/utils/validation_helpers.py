"""Validation helper functions for series and metadata validation."""

from typing import Any, Dict, Optional, Tuple

from dagster import AssetExecutionContext

from database.referential_integrity import ReferentialIntegrityValidator


def validate_series_metadata(
    series: Dict[str, Any], series_id: int, series_code: str, context: AssetExecutionContext
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Validate that series has required metadata (ticker and field_type name).

    Args:
        series: Series dictionary
        series_id: Series ID for logging
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        Tuple of (ticker, field_type_name, error_reason)
        If error_reason is not None, ticker and field_type_name will be None
    """
    ticker = series.get("ticker")
    field_type_name = series.get("field_type")

    if not ticker:
        context.log.warning(f"Series {series_code} has no ticker, skipping")
        return None, None, "no ticker"

    if not field_type_name:
        context.log.warning(f"Series {series_code} has no field_type, skipping")
        return None, None, "no field_type"

    return ticker, field_type_name, None


def validate_field_type_name(
    validator: ReferentialIntegrityValidator,
    field_type_name: str,
    series_id: int,
    series_code: str,
    context: AssetExecutionContext,
) -> Optional[str]:
    """Validate field type name using referential integrity.

    Validates that the field_type_name exists in the lookup table using ReferentialIntegrityValidator.

    Args:
        validator: Referential integrity validator instance
        field_type_name: Field type name to validate
        series_id: Series ID for logging
        series_code: Series code for logging
        context: Dagster execution context for logging

    Returns:
        Field type name if valid, None if invalid
    """
    if not field_type_name:
        context.log.warning(f"Series {series_code} has no field_type_name, skipping")
        return None

    # Validate referential integrity using ReferentialIntegrityValidator
    validation_error = validator.validate_lookup_reference(
        "field_type", field_type_name, series_code
    )
    if validation_error:
        context.log.warning(
            f"Invalid field_type_name reference for series {series_code}: {validation_error}"
        )
        return None

    return field_type_name
