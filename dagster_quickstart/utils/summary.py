"""Time and space optimized Summary class for Dagster assets.

This module provides a reusable Summary class that all assets can use to create
consistent, memory-efficient summaries with metadata tracking.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from dagster import AssetExecutionContext, MetadataValue

    import pandas as pd
    import polars as pl
else:
    from dagster import AssetExecutionContext, MetadataValue

    import pandas as pd
    import polars as pl


class AssetSummary:
    """Time and space optimized summary class for Dagster assets.

    This class uses __slots__ for memory efficiency and provides methods to:
    - Track summary statistics
    - Convert to DataFrame (Polars or Pandas)
    - Add metadata to Dagster context
    - Support both ingestion and calculation asset types

    Attributes:
        total_series: Total number of series processed
        successful_count: Number of successful operations
        failed_count: Number of failed operations
        total_rows: Total number of rows processed/inserted
        target_date: Target date for the operation
        successful_series: List of successful series codes/identifiers
        failed_series: List of failed series codes/identifiers with reasons
        asset_type: Type of asset (e.g., 'ingestion', 'calculation')
        additional_metadata: Additional metadata fields specific to asset type
    """

    __slots__ = (
        "_total_series",
        "_successful_count",
        "_failed_count",
        "_total_rows",
        "_target_date",
        "_successful_series",
        "_failed_series",
        "_asset_type",
        "_additional_metadata",
        "_df_cache",
        "_metadata_cache",
    )

    def __init__(
        self,
        total_series: int = 0,
        successful_count: int = 0,
        failed_count: int = 0,
        total_rows: int = 0,
        target_date: Optional[datetime] = None,
        successful_series: Optional[List[Dict[str, Any]]] = None,
        failed_series: Optional[List[Dict[str, Any]]] = None,
        asset_type: str = "ingestion",
        additional_metadata: Optional[Dict[str, Any]] = None,
    ):
        """Initialize AssetSummary with provided values.

        Args:
            total_series: Total number of series processed
            successful_count: Number of successful operations
            failed_count: Number of failed operations
            total_rows: Total number of rows processed/inserted
            target_date: Target date for the operation
            successful_series: List of successful series dictionaries
            failed_series: List of failed series dictionaries
            asset_type: Type of asset (e.g., 'ingestion', 'calculation')
            additional_metadata: Additional metadata fields specific to asset type
        """
        self._total_series = total_series
        self._successful_count = successful_count
        self._failed_count = failed_count
        self._total_rows = total_rows
        self._target_date = target_date
        self._successful_series = successful_series or []
        self._failed_series = failed_series or []
        self._asset_type = asset_type
        self._additional_metadata = additional_metadata or {}
        # Cache for computed values
        self._df_cache: Optional[pl.DataFrame] = None
        self._metadata_cache: Optional[Dict[str, MetadataValue]] = None

    @property
    def total_series(self) -> int:
        """Get total number of series processed."""
        return self._total_series

    @property
    def successful_count(self) -> int:
        """Get number of successful operations."""
        return self._successful_count

    @property
    def failed_count(self) -> int:
        """Get number of failed operations."""
        return self._failed_count

    @property
    def total_rows(self) -> int:
        """Get total number of rows processed/inserted."""
        return self._total_rows

    @property
    def target_date(self) -> Optional[datetime]:
        """Get target date for the operation."""
        return self._target_date

    @property
    def successful_series(self) -> List[Dict[str, Any]]:
        """Get list of successful series."""
        return self._successful_series

    @property
    def failed_series(self) -> List[Dict[str, Any]]:
        """Get list of failed series."""
        return self._failed_series

    @property
    def asset_type(self) -> str:
        """Get asset type."""
        return self._asset_type

    @property
    def additional_metadata(self) -> Dict[str, Any]:
        """Get additional metadata."""
        return self._additional_metadata

    def _invalidate_cache(self) -> None:
        """Invalidate cached computed values."""
        self._df_cache = None
        self._metadata_cache = None

    def _update_with_cache_invalidation(self, updates: Dict[str, Any]) -> "AssetSummary":
        """Update attributes from dictionary and invalidate cache.

        Args:
            updates: Dictionary mapping attribute names (without underscore) to values

        Returns:
            Self for method chaining
        """
        # Map attribute names to their internal storage names
        attr_map = {
            "total_series": "_total_series",
            "successful_count": "_successful_count",
            "failed_count": "_failed_count",
            "total_rows": "_total_rows",
            "target_date": "_target_date",
            "successful_series": "_successful_series",
            "failed_series": "_failed_series",
        }

        for key, value in updates.items():
            if key == "additional_metadata" and value is not None:
                self._additional_metadata.update(value)
            elif key in attr_map and value is not None:
                setattr(self, attr_map[key], value)

        self._invalidate_cache()
        return self

    def update(
        self,
        total_series: Optional[int] = None,
        successful_count: Optional[int] = None,
        failed_count: Optional[int] = None,
        total_rows: Optional[int] = None,
        target_date: Optional[datetime] = None,
        successful_series: Optional[List[Dict[str, Any]]] = None,
        failed_series: Optional[List[Dict[str, Any]]] = None,
        additional_metadata: Optional[Dict[str, Any]] = None,
    ) -> "AssetSummary":
        """Update summary fields and invalidate cache.

        Args:
            total_series: Total number of series processed
            successful_count: Number of successful operations
            failed_count: Number of failed operations
            total_rows: Total number of rows processed/inserted
            target_date: Target date for the operation
            successful_series: List of successful series dictionaries
            failed_series: List of failed series dictionaries
            additional_metadata: Additional metadata fields

        Returns:
            Self for method chaining
        """
        updates = {
            "total_series": total_series,
            "successful_count": successful_count,
            "failed_count": failed_count,
            "total_rows": total_rows,
            "target_date": target_date,
            "successful_series": successful_series,
            "failed_series": failed_series,
            "additional_metadata": additional_metadata,
        }
        return self._update_with_cache_invalidation(updates)

    def _invalidate_and_return(self) -> "AssetSummary":
        """Invalidate cache and return self for method chaining.

        Returns:
            Self for method chaining
        """
        self._invalidate_cache()
        return self

    def increment_successful(self, count: int = 1) -> "AssetSummary":
        """Increment successful count and invalidate cache.

        Args:
            count: Amount to increment by

        Returns:
            Self for method chaining
        """
        self._successful_count += count
        return self._invalidate_and_return()

    def increment_failed(self, count: int = 1) -> "AssetSummary":
        """Increment failed count and invalidate cache.

        Args:
            count: Amount to increment by

        Returns:
            Self for method chaining
        """
        self._failed_count += count
        return self._invalidate_and_return()

    def add_successful_series(self, series: Dict[str, Any]) -> "AssetSummary":
        """Add a successful series and invalidate cache.

        Args:
            series: Series dictionary with at least 'series_code' key

        Returns:
            Self for method chaining
        """
        self._successful_series.append(series)
        return self._invalidate_and_return()

    def add_failed_series(self, series: Dict[str, Any]) -> "AssetSummary":
        """Add a failed series and invalidate cache.

        Args:
            series: Series dictionary with at least 'series_code' and optionally 'reason' key

        Returns:
            Self for method chaining
        """
        self._failed_series.append(series)
        return self._invalidate_and_return()

    def to_polars_dataframe(self) -> pl.DataFrame:
        """Convert summary to Polars DataFrame (cached).

        Returns:
            Polars DataFrame with summary data
        """
        if self._df_cache is not None:
            return self._df_cache

        summary_data: Dict[str, List[Any]] = {
            "total_series": [self._total_series],
            "successful_ingestions": [self._successful_count],
            "failed_ingestions": [self._failed_count],
            "total_rows_inserted": [self._total_rows],
        }

        if self._target_date:
            summary_data["target_date"] = [self._target_date.isoformat()]

        # Add additional metadata fields
        for key, value in self._additional_metadata.items():
            if key not in summary_data:
                summary_data[key] = [value]

        self._df_cache = pl.DataFrame(summary_data)
        return self._df_cache

    def to_pandas_dataframe(self) -> pd.DataFrame:
        """Convert summary to Pandas DataFrame.

        Returns:
            Pandas DataFrame with summary data
        """
        return self.to_polars_dataframe().to_pandas()

    @staticmethod
    def _convert_to_metadata_value(value: Any) -> MetadataValue:
        """Convert a Python value to Dagster MetadataValue.

        Args:
            value: Value to convert

        Returns:
            Appropriate MetadataValue instance
        """
        # Type-based conversion mapping
        type_converters = {
            int: MetadataValue.int,
            float: MetadataValue.float,
            bool: MetadataValue.bool,
        }

        # Check exact type first
        converter = type_converters.get(type(value))
        if converter:
            return converter(value)

        # Check for list/dict
        if isinstance(value, (list, dict)):
            return MetadataValue.json(value)

        # Default to text
        return MetadataValue.text(str(value))

    def get_metadata_dict(self) -> Dict[str, MetadataValue]:
        """Get metadata dictionary for Dagster context (cached).

        Returns:
            Dictionary of metadata values
        """
        if self._metadata_cache is not None:
            return self._metadata_cache

        # Core metadata fields
        metadata: Dict[str, MetadataValue] = {
            "total_series": MetadataValue.int(self._total_series),
            "successful_ingestions": MetadataValue.int(self._successful_count),
            "failed_ingestions": MetadataValue.int(self._failed_count),
            "total_rows_inserted": MetadataValue.int(self._total_rows),
        }

        # Optional fields
        optional_fields = {
            "target_date": (self._target_date, lambda d: MetadataValue.text(d.isoformat())),
        }

        for key, (value, converter) in optional_fields.items():
            if value:
                metadata[key] = converter(value)

        # Add successful series codes if present
        if self._successful_series:
            series_codes = [
                s.get("series_code", s.get("series_id", "unknown"))
                for s in self._successful_series
            ]
            metadata["successful_series"] = MetadataValue.json(series_codes)

        # Add additional metadata using type-based conversion
        for key, value in self._additional_metadata.items():
            if key not in metadata:
                metadata[key] = self._convert_to_metadata_value(value)

        self._metadata_cache = metadata
        return metadata

    def add_to_context(self, context: AssetExecutionContext) -> "AssetSummary":
        """Add summary metadata to Dagster context.

        Args:
            context: Dagster execution context

        Returns:
            Self for method chaining
        """
        context.add_output_metadata(self.get_metadata_dict())
        return self

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary.

        Returns:
            Dictionary representation of summary
        """
        # Core fields (always present)
        result: Dict[str, Any] = {
            "total_series": self._total_series,
            "successful_count": self._successful_count,
            "failed_count": self._failed_count,
            "total_rows": self._total_rows,
            "asset_type": self._asset_type,
        }

        # Optional fields (only include if present)
        optional_fields = {
            "target_date": (self._target_date, lambda d: d.isoformat()),
            "successful_series": (self._successful_series, lambda s: s),
            "failed_series": (self._failed_series, lambda s: s),
            "additional_metadata": (self._additional_metadata, lambda m: m),
        }

        for key, (value, converter) in optional_fields.items():
            if value:
                result[key] = converter(value)

        return result

    def __repr__(self) -> str:
        """String representation of summary."""
        return (
            f"AssetSummary("
            f"total_series={self._total_series}, "
            f"successful={self._successful_count}, "
            f"failed={self._failed_count}, "
            f"total_rows={self._total_rows}, "
            f"asset_type={self._asset_type}"
            f")"
        )

    @classmethod
    def for_ingestion(
        cls,
        matching_series: List[Dict[str, Any]],
        successful_series: List[Dict[str, Any]],
        failed_series: List[Dict[str, Any]],
        total_rows_inserted: int,
        target_date: datetime,
    ) -> "AssetSummary":
        """Create an ingestion summary from series data.

        Args:
            matching_series: List of all matching series
            successful_series: List of successful series
            failed_series: List of failed series
            total_rows_inserted: Total number of rows inserted
            target_date: Target date for ingestion

        Returns:
            AssetSummary instance configured for ingestion
        """
        return cls(
            total_series=len(matching_series),
            successful_count=len(successful_series),
            failed_count=len(failed_series),
            total_rows=total_rows_inserted,
            target_date=target_date,
            successful_series=successful_series,
            failed_series=failed_series,
            asset_type="ingestion",
        )

    @classmethod
    def for_calculation(
        cls,
        rows_calculated: int,
        calculation_id: Optional[int] = None,
        target_date: Optional[datetime] = None,
        additional_metadata: Optional[Dict[str, Any]] = None,
    ) -> "AssetSummary":
        """Create a calculation summary.

        Args:
            rows_calculated: Number of rows calculated
            calculation_id: Calculation ID (optional)
            target_date: Target date for calculation (optional)
            additional_metadata: Additional metadata (e.g., window_size, num_parents)

        Returns:
            AssetSummary instance configured for calculation
        """
        metadata = additional_metadata or {}
        if calculation_id is not None:
            metadata["calculation_id"] = calculation_id

        return cls(
            total_series=0,
            successful_count=1 if rows_calculated > 0 else 0,
            failed_count=0,
            total_rows=rows_calculated,
            target_date=target_date,
            asset_type="calculation",
            additional_metadata=metadata,
        )

