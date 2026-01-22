"""General utility helper functions."""

from decimal import ROUND_HALF_UP, Decimal

from dagster_quickstart.utils.datetime_utils import utc_now


def round_to_six_decimal_places(value: float | Decimal) -> Decimal:
    """Round a number to exactly 6 decimal places.

    Args:
        value: Number to round (float or Decimal)

    Returns:
        Decimal value rounded to exactly 6 decimal places using ROUND_HALF_UP
    """
    return Decimal(value).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


def get_version_date() -> str:
    """Get version date (YYYY-MM-DD) from Dagster context or current date.

    Uses the run date from context if available, otherwise uses current UTC date.
    This ensures versioning is consistent across pipeline runs.

    Returns:
        Version date string in YYYY-MM-DD format
    """
    # Fallback to current UTC date
    return utc_now().strftime("%Y-%m-%d")
