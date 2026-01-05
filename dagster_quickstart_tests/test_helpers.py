"""Tests for helper functions."""

from decimal import Decimal

from dagster_quickstart.utils.helpers import round_six_dp


def test_round_six_dp_with_float():
    """Test rounding a float to 6 decimal places."""
    result = round_six_dp(123.4567890123)
    assert result == Decimal("123.456789")
    assert isinstance(result, Decimal)


def test_round_six_dp_with_decimal():
    """Test rounding a Decimal to 6 decimal places."""
    value = Decimal("987.6543210987")
    result = round_six_dp(value)
    assert result == Decimal("987.654321")
    assert isinstance(result, Decimal)


def test_round_six_dp_rounding_up():
    """Test that rounding follows ROUND_HALF_UP rule."""
    # Use Decimal to avoid floating point precision issues
    # 0.0000005 should round up to 0.000001
    result = round_six_dp(Decimal("0.0000005"))
    assert result == Decimal("0.000001")


def test_round_six_dp_rounding_down():
    """Test that values below 0.5 round down."""
    # 0.0000004 should round down to 0.000000
    result = round_six_dp(0.0000004)
    assert result == Decimal("0.000000")


def test_round_six_dp_fewer_decimal_places():
    """Test rounding a number with fewer than 6 decimal places."""
    result = round_six_dp(42.5)
    assert result == Decimal("42.500000")


def test_round_six_dp_integer():
    """Test rounding an integer (should add .000000)."""
    result = round_six_dp(100)
    assert result == Decimal("100.000000")


def test_round_six_dp_negative_number():
    """Test rounding a negative number."""
    result = round_six_dp(-123.4567890123)
    assert result == Decimal("-123.456789")


def test_round_six_dp_zero():
    """Test rounding zero."""
    result = round_six_dp(0.0)
    assert result == Decimal("0.000000")


def test_round_six_dp_large_number():
    """Test rounding a large number."""
    result = round_six_dp(1234567.890123456)
    assert result == Decimal("1234567.890123")


if __name__ == "__main__":
    import unittest

    unittest.main()
