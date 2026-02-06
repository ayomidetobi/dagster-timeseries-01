"""Database schema definitions and context objects."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List

from pydantic import BaseModel, field_validator

if TYPE_CHECKING:
    from dagster import AssetExecutionContext

    from dagster_quickstart.resources import DuckDBResource

# ============================================================================
# Enums
# ============================================================================


class DataSource(str, Enum):
    """Data source enumeration."""

    RAW = "RAW"
    DERIVED = "DERIVED"
    NONE = "NONE"


class FieldType(str, Enum):
    """Field type enumeration (FLDS)."""

    PX_LAST = "PX_LAST"
    OPEN_INT = "OPEN_INT"
    PX_OPEN = "PX_OPEN"
    PX_HIGH = "PX_HIGH"
    PX_LOW = "PX_LOW"
    PX_VOLUME = "PX_VOLUME"


class CalculationStatus(str, Enum):
    """Calculation status enumeration."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TickerSource(str, Enum):
    """Ticker source enumeration."""

    BLOOMBERG = "Bloomberg"
    HAWKEYE = "Hawkeye"
    LSEG = "LSEG"
    RAMP = "Ramp"
    ONETICK = "OneTick"
    MANUAL_ENTRY = "Manual Entry"
    INTERNAL = "Internal"


# ============================================================================
# Context Objects (Dataclasses)
# ============================================================================


@dataclass
class SeriesIngestionContext:
    """Context object for series ingestion metadata."""

    series_code: str
    series_id: int
    ticker: str
    field_name: str
    series_record: Dict[str, Any]  # the row from meta_series


@dataclass
class CalculationContext:
    """Context object for calculation metadata."""

    derived_series_id: int
    derived_series_code: str
    calc_type: str
    formula: str
    input_series_ids: List[int]
    num_parents: int
    target_date: datetime
    range_start: datetime
    range_end: datetime


@dataclass
class CsvLoadContext:
    """Context object for CSV loading operations."""

    duckdb: "DuckDBResource"
    context: "AssetExecutionContext"
    csv_path: str
    version_date: str


@dataclass
class SeriesValidationContext:
    """Context object for series validation."""

    series_id: int
    series_code: str


# ============================================================================
# Pydantic Models (for validation)
# ============================================================================


class TimeSeriesValue(BaseModel):
    """Model for a single time-series value."""

    series_id: int
    timestamp: datetime
    value: Decimal

    @field_validator("value", mode="after")
    @classmethod
    def must_have_exactly_6_dp(cls, v: Decimal) -> Decimal:
        """Ensure value has exactly 6 decimal places."""
        if abs(int(v.as_tuple().exponent)) != 6:
            raise ValueError(f"value '{v}' must be formatted with exactly 6 decimal places")
        return v
