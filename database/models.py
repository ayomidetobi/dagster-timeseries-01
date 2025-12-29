"""Pydantic models for data validation and schema enforcement."""

from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


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

    BLOOMBERG = "BLOOMBERG"
    HAWKEYE = "HAWKEYE"
    LSEG = "LSEG"
    RAMP = "RAMP"
    ONETICK = "ONETICK"
    MANUAL_ENTRY = "MANUAL_ENTRY"
    INTERNAL = "INTERNAL"


# Lookup Table Models
class LookupTableBase(BaseModel):
    """Base model for lookup tables."""

    name: str = Field(..., min_length=1, max_length=255)
    description: Optional[str] = None


class AssetClassLookup(LookupTableBase):
    """Asset class lookup model."""

    asset_class_id: Optional[int] = None


class ProductTypeLookup(LookupTableBase):
    """Product type lookup model."""

    product_type_id: Optional[int] = None
    is_derived: bool = False


class SubAssetClassLookup(LookupTableBase):
    """Sub-asset class lookup model."""

    sub_asset_class_id: Optional[int] = None
    asset_class_id: int


class DataTypeLookup(LookupTableBase):
    """Data type lookup model."""

    data_type_id: Optional[int] = None


class StructureTypeLookup(LookupTableBase):
    """Structure type lookup model."""

    structure_type_id: Optional[int] = None


class MarketSegmentLookup(LookupTableBase):
    """Market segment lookup model."""

    market_segment_id: Optional[int] = None


class FieldTypeLookup(LookupTableBase):
    """Field type lookup model."""

    field_type_id: Optional[int] = None
    field_type_code: str = Field(..., min_length=1, max_length=50)


class TickerSourceLookup(LookupTableBase):
    """Ticker source lookup model."""

    ticker_source_id: Optional[int] = None
    ticker_source_code: str = Field(..., min_length=1, max_length=50)


class RegionLookup(LookupTableBase):
    """Region lookup model."""

    region_id: Optional[int] = None


class CurrencyLookup(LookupTableBase):
    """Currency lookup model."""

    currency_id: Optional[int] = None
    currency_code: str = Field(..., min_length=1, max_length=24)
    currency_name: Optional[str] = Field(None, max_length=100)


class TermLookup(LookupTableBase):
    """Term lookup model."""

    term_id: Optional[int] = None


class TenorLookup(LookupTableBase):
    """Tenor lookup model."""

    tenor_id: Optional[int] = None
    tenor_code: str = Field(..., min_length=1, max_length=20)
    tenor_name: Optional[str] = Field(None, max_length=100)


class CountryLookup(LookupTableBase):
    """Country lookup model."""

    country_id: Optional[int] = None
    country_code: str = Field(..., min_length=1, max_length=50)
    country_name: Optional[str] = Field(None, max_length=100)


# Meta Series Models
class MetaSeriesBase(BaseModel):
    """Base model for meta series."""

    series_name: str = Field(..., min_length=1, max_length=255)
    series_code: str = Field(..., min_length=1, max_length=100)
    data_source: DataSource
    field_type_id: Optional[int] = None
    asset_class_id: Optional[int] = None
    sub_asset_class_id: Optional[int] = None
    product_type_id: Optional[int] = None
    data_type_id: Optional[int] = None
    structure_type_id: Optional[int] = None
    market_segment_id: Optional[int] = None
    ticker_source_id: Optional[int] = None
    ticker: str = Field(..., min_length=1, max_length=100)
    region_id: Optional[int] = None
    currency_id: Optional[int] = None
    term_id: Optional[int] = None
    tenor_id: Optional[int] = None
    country_id: Optional[int] = None
    calculation_formula: Optional[str] = None
    description: Optional[str] = None
    is_active: bool = True  # Default to active


class MetaSeriesCreate(MetaSeriesBase):
    """Model for creating a meta series."""

    pass


class MetaSeries(MetaSeriesBase):
    """Complete meta series model."""

    series_id: int
    created_at: datetime
    updated_at: datetime
    created_by: Optional[str] = None
    updated_by: Optional[str] = None


# Dependency Models
class SeriesDependencyBase(BaseModel):
    """Base model for series dependencies."""

    parent_series_id: int
    child_series_id: int
    weight: Optional[float] = Field(1.0, ge=0.0, le=1.0)
    formula: Optional[str] = None
    valid_from: datetime
    valid_to: Optional[datetime] = None

    @field_validator("valid_to")
    @classmethod
    def validate_valid_to(cls, v: Optional[datetime], info) -> Optional[datetime]:
        """Ensure valid_to is after valid_from."""
        if v is not None and "valid_from" in info.data:
            if v <= info.data["valid_from"]:
                raise ValueError("valid_to must be after valid_from")
        return v


class SeriesDependency(SeriesDependencyBase):
    """Complete series dependency model."""

    dependency_id: int
    created_at: datetime
    updated_at: datetime


# Calculation Log Models
class CalculationLogBase(BaseModel):
    """Base model for calculation logs."""

    series_id: int
    calculation_type: str
    status: CalculationStatus
    input_series_ids: List[int]
    parameters: str
    formula: str
    execution_start: datetime
    execution_end: Optional[datetime] = None
    rows_processed: Optional[int] = None
    error_message: Optional[str] = None


class CalculationLog(CalculationLogBase):
    """Complete calculation log model."""

    calculation_id: int
    created_at: datetime


# Time-Series Value Models
class TimeSeriesValue(BaseModel):
    """Model for a single time-series value."""

    series_id: int
    timestamp: datetime
    value: float

    @field_validator("value")
    @classmethod
    def validate_value(cls, v: float) -> float:
        """Validate value is finite."""
        if not (v == v and abs(v) != float("inf")):
            raise ValueError("Value must be finite")
        return v


class TimeSeriesBatch(BaseModel):
    """Model for a batch of time-series values."""

    series_id: int
    values: List[TimeSeriesValue]

    @field_validator("values")
    @classmethod
    def validate_values(cls, v: List[TimeSeriesValue]) -> List[TimeSeriesValue]:
        """Validate values are sorted by timestamp."""
        if len(v) > 1:
            timestamps = [val.timestamp for val in v]
            if timestamps != sorted(timestamps):
                raise ValueError("Values must be sorted by timestamp")
        return v


# Data Quality Models
class DataQualityMetrics(BaseModel):
    """Model for data quality metrics."""

    series_id: int
    completeness_score: float = Field(..., ge=0.0, le=1.0)
    timeliness_score: float = Field(..., ge=0.0, le=1.0)
    accuracy_score: float = Field(..., ge=0.0, le=1.0)
    consistency_score: float = Field(..., ge=0.0, le=1.0)
    overall_score: float = Field(..., ge=0.0, le=1.0)
    calculated_at: datetime
