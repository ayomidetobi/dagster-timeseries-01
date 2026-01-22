"""Time and space optimized Summary class for Dagster assets.

This module provides a reusable Summary class that all assets can use to create
consistent, memory-efficient summaries with asset-specific metadata tracking.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    import polars as pl
    from dagster import AssetExecutionContext, MetadataValue
else:
    from dagster import AssetExecutionContext, MetadataValue


class AssetSummary:
    """Time and space optimized summary class for Dagster assets with asset-specific metadata.

    This class uses __slots__ for memory efficiency and provides methods to:
    - Track summary statistics
    - Generate asset-specific metadata
    - Convert to DataFrame (Polars or Pandas)
    - Add metadata to Dagster context
    - Support ingestion, CSV loader, and calculation asset types

    Attributes:
        total_series: Total number of series processed
        successful_count: Number of successful operations
        failed_count: Number of failed operations
        total_rows: Total number of rows processed/inserted
        target_date: Target date for the operation
        successful_series: List of successful series codes/identifiers
        failed_series: List of failed series codes/identifiers with reasons
        asset_type: Type of asset (e.g., 'ingestion', 'csv_loader', 'calculation')
        asset_metadata: Asset-specific metadata dictionary
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
        "_asset_metadata",
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
        asset_metadata: Optional[Dict[str, Any]] = None,
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
            asset_type: Type of asset (e.g., 'ingestion', 'csv_loader', 'calculation')
            asset_metadata: Asset-specific metadata dictionary
        """
        self._total_series = total_series
        self._successful_count = successful_count
        self._failed_count = failed_count
        self._total_rows = total_rows
        self._target_date = target_date
        self._successful_series = successful_series or []
        self._failed_series = failed_series or []
        self._asset_type = asset_type
        self._asset_metadata = asset_metadata or {}
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
    def asset_metadata(self) -> Dict[str, Any]:
        """Get asset-specific metadata."""
        return self._asset_metadata

    def _invalidate_cache(self) -> None:
        """Invalidate cached computed values."""
        self._df_cache = None
        self._metadata_cache = None

    def update_metadata(self, **kwargs: Any) -> "AssetSummary":
        """Update asset-specific metadata and invalidate cache.

        Args:
            **kwargs: Metadata key-value pairs to update

        Returns:
            Self for method chaining
        """
        self._asset_metadata.update(kwargs)
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
        asset_metadata: Optional[Dict[str, Any]] = None,
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
            asset_metadata: Asset-specific metadata fields

        Returns:
            Self for method chaining
        """
        if total_series is not None:
            self._total_series = total_series
        if successful_count is not None:
            self._successful_count = successful_count
        if failed_count is not None:
            self._failed_count = failed_count
        if total_rows is not None:
            self._total_rows = total_rows
        if target_date is not None:
            self._target_date = target_date
        if successful_series is not None:
            self._successful_series = successful_series
        if failed_series is not None:
            self._failed_series = failed_series
        if asset_metadata is not None:
            self._asset_metadata.update(asset_metadata)

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
        self._invalidate_cache()
        return self

    def increment_failed(self, count: int = 1) -> "AssetSummary":
        """Increment failed count and invalidate cache.

        Args:
            count: Amount to increment by

        Returns:
            Self for method chaining
        """
        self._failed_count += count
        self._invalidate_cache()
        return self

    def add_successful_series(self, series: Dict[str, Any]) -> "AssetSummary":
        """Add a successful series and invalidate cache.

        Args:
            series: Series dictionary with at least 'series_code' key

        Returns:
            Self for method chaining
        """
        self._successful_series.append(series)
        self._invalidate_cache()
        return self

    def add_failed_series(self, series: Dict[str, Any]) -> "AssetSummary":
        """Add a failed series and invalidate cache.

        Args:
            series: Series dictionary with at least 'series_code' and optionally 'reason' key

        Returns:
            Self for method chaining
        """
        self._failed_series.append(series)
        self._invalidate_cache()
        return self

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

    def _build_asset_specific_metadata(self) -> Dict[str, MetadataValue]:
        """Build asset-specific metadata based on asset type.

        Returns:
            Dictionary of asset-specific metadata values
        """
        metadata: Dict[str, MetadataValue] = {}

        if self._asset_type == "bloomberg_ingestion":
            # Bloomberg-specific metadata
            if "ticker" in self._asset_metadata:
                metadata["ticker"] = MetadataValue.text(str(self._asset_metadata["ticker"]))
            if "field_type_name" in self._asset_metadata:
                metadata["field_type"] = MetadataValue.text(
                    str(self._asset_metadata["field_type_name"])
                )
            if "data_source" in self._asset_metadata:
                metadata["data_source"] = MetadataValue.text(
                    str(self._asset_metadata["data_source"])
                )
            if "data_code" in self._asset_metadata:
                metadata["data_code"] = MetadataValue.text(str(self._asset_metadata["data_code"]))
            if "s3_path" in self._asset_metadata:
                metadata["s3_path"] = MetadataValue.text(str(self._asset_metadata["s3_path"]))
            if "data_points_fetched" in self._asset_metadata:
                metadata["data_points_fetched"] = MetadataValue.int(
                    self._asset_metadata["data_points_fetched"]
                )
            if "use_dummy_data" in self._asset_metadata:
                metadata["use_dummy_data"] = MetadataValue.bool(
                    self._asset_metadata["use_dummy_data"]
                )

        elif self._asset_type == "csv_loader":
            # CSV loader-specific metadata
            if "record_type" in self._asset_metadata:
                metadata["record_type"] = MetadataValue.text(
                    str(self._asset_metadata["record_type"])
                )
            if "csv_path" in self._asset_metadata:
                metadata["csv_path"] = MetadataValue.text(str(self._asset_metadata["csv_path"]))
            if "version_date" in self._asset_metadata:
                metadata["version_date"] = MetadataValue.text(
                    str(self._asset_metadata["version_date"])
                )
            if "s3_control_table_path" in self._asset_metadata:
                metadata["s3_control_table_path"] = MetadataValue.text(
                    str(self._asset_metadata["s3_control_table_path"])
                )
            if "lookup_table_type" in self._asset_metadata:
                metadata["lookup_table_type"] = MetadataValue.text(
                    str(self._asset_metadata["lookup_table_type"])
                )
            if "validation_errors" in self._asset_metadata:
                metadata["validation_errors"] = MetadataValue.json(
                    self._asset_metadata["validation_errors"]
                )

        elif self._asset_type == "calculation":
            # Calculation-specific metadata
            if "calculation_id" in self._asset_metadata:
                calc_id = self._asset_metadata["calculation_id"]
                if isinstance(calc_id, int):
                    metadata["calculation_id"] = MetadataValue.int(calc_id)
                else:
                    metadata["calculation_id"] = MetadataValue.text(str(calc_id))
            if "calculation_type" in self._asset_metadata:
                metadata["calculation_type"] = MetadataValue.text(
                    str(self._asset_metadata["calculation_type"])
                )
            if "formula" in self._asset_metadata:
                metadata["formula"] = MetadataValue.text(str(self._asset_metadata["formula"]))
            if "parent_series_count" in self._asset_metadata:
                parent_count = self._asset_metadata["parent_series_count"]
                if isinstance(parent_count, int):
                    metadata["parent_series_count"] = MetadataValue.int(parent_count)
                else:
                    metadata["parent_series_count"] = MetadataValue.text(str(parent_count))
            if "child_series_count" in self._asset_metadata:
                child_count = self._asset_metadata["child_series_count"]
                if isinstance(child_count, int):
                    metadata["child_series_count"] = MetadataValue.int(child_count)
                else:
                    metadata["child_series_count"] = MetadataValue.text(str(child_count))
            # Add calculation log metadata fields
            if "status" in self._asset_metadata:
                metadata["calculation_status"] = MetadataValue.text(
                    str(self._asset_metadata["status"])
                )
            if "input_series_ids" in self._asset_metadata:
                metadata["input_series_ids"] = MetadataValue.json(
                    self._asset_metadata["input_series_ids"]
                )
            if "derived_series_id" in self._asset_metadata:
                derived_id = self._asset_metadata["derived_series_id"]
                if isinstance(derived_id, int):
                    metadata["derived_series_id"] = MetadataValue.int(derived_id)
                else:
                    metadata["derived_series_id"] = MetadataValue.text(str(derived_id))
            if "derived_series_code" in self._asset_metadata:
                metadata["derived_series_code"] = MetadataValue.text(
                    str(self._asset_metadata["derived_series_code"])
                )
            if "parameters" in self._asset_metadata:
                metadata["parameters"] = MetadataValue.text(str(self._asset_metadata["parameters"]))
            if "error_message" in self._asset_metadata:
                metadata["error_message"] = MetadataValue.text(
                    str(self._asset_metadata["error_message"])
                )

        # Add any remaining metadata that wasn't handled above
        for key, value in self._asset_metadata.items():
            if key not in metadata:
                metadata[key] = self._convert_to_metadata_value(value)

        return metadata

    def get_metadata_dict(self) -> Dict[str, MetadataValue]:
        """Get metadata dictionary for Dagster context (cached).

        Returns:
            Dictionary of metadata values with asset-specific fields
        """
        if self._metadata_cache is not None:
            return self._metadata_cache

        # Core metadata fields (always present)
        metadata: Dict[str, MetadataValue] = {
            "total_series": MetadataValue.int(self._total_series),
            "successful_count": MetadataValue.int(self._successful_count),
            "failed_count": MetadataValue.int(self._failed_count),
            "total_rows": MetadataValue.int(self._total_rows),
            "asset_type": MetadataValue.text(self._asset_type),
        }

        # Optional core fields
        if self._target_date:
            metadata["target_date"] = MetadataValue.text(self._target_date.isoformat())

        # Add successful series codes if present
        if self._successful_series:
            series_codes = [
                s.get("series_code", s.get("series_id", "unknown")) for s in self._successful_series
            ]
            metadata["successful_series"] = MetadataValue.json(series_codes)

        # Add failed series with reasons if present
        if self._failed_series:
            failed_info = [
                {
                    "series_code": s.get("series_code", s.get("series_id", "unknown")),
                    "reason": s.get("reason", "unknown"),
                }
                for s in self._failed_series
            ]
            metadata["failed_series"] = MetadataValue.json(failed_info)

        # Add asset-specific metadata
        asset_metadata = self._build_asset_specific_metadata()
        metadata.update(asset_metadata)

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
        result: Dict[str, Any] = {
            "total_series": self._total_series,
            "successful_count": self._successful_count,
            "failed_count": self._failed_count,
            "total_rows": self._total_rows,
            "asset_type": self._asset_type,
        }

        if self._target_date:
            result["target_date"] = self._target_date.isoformat()
        if self._successful_series:
            result["successful_series"] = self._successful_series
        if self._failed_series:
            result["failed_series"] = self._failed_series
        if self._asset_metadata:
            result["asset_metadata"] = self._asset_metadata

        return result

    def __repr__(self) -> str:
        """String representation of summary."""
        return (
            f"AssetSummary("
            f"asset_type={self._asset_type}, "
            f"total_series={self._total_series}, "
            f"successful={self._successful_count}, "
            f"failed={self._failed_count}, "
            f"total_rows={self._total_rows}"
            f")"
        )

    @classmethod
    def for_bloomberg_ingestion(
        cls,
        series_code: str,
        series_id: int,
        ticker: str,
        field_type_name: str,
        data_source: str,
        data_code: str,
        rows_inserted: int,
        target_date: datetime,
        status: str = "success",
        reason: Optional[str] = None,
        s3_path: Optional[str] = None,
        data_points_fetched: Optional[int] = None,
        use_dummy_data: bool = False,
    ) -> "AssetSummary":
        """Create a Bloomberg ingestion summary with specific metadata.

        Args:
            series_code: Series code
            series_id: Series ID
            ticker: Ticker symbol
            field_type_name: Field type name (Bloomberg field code)
            data_source: PyPDL data source path
            data_code: PyPDL data code (ticker)
            rows_inserted: Number of rows inserted
            target_date: Target date for ingestion
            status: Status string ("success", "skipped", "failed")
            reason: Optional reason for status
            s3_path: Optional S3 path where data was saved
            data_points_fetched: Optional number of data points fetched from API
            use_dummy_data: Whether dummy data was used

        Returns:
            AssetSummary instance configured for Bloomberg ingestion
        """
        series_info = {
            "series_code": series_code,
            "series_id": series_id,
            "rows": rows_inserted,
        }

        if status == "success":
            successful_series = [series_info]
            failed_series = []
        else:
            successful_series = []
            failed_series = [{**series_info, "reason": reason or status}]

        asset_metadata = {
            "ticker": ticker,
            "field_type_name": field_type_name,
            "data_source": data_source,
            "data_code": data_code,
            "use_dummy_data": use_dummy_data,
        }

        if s3_path:
            asset_metadata["s3_path"] = s3_path
        if data_points_fetched is not None:
            asset_metadata["data_points_fetched"] = data_points_fetched

        return cls(
            total_series=1,
            successful_count=1 if status == "success" else 0,
            failed_count=0 if status == "success" else 1,
            total_rows=rows_inserted,
            target_date=target_date,
            successful_series=successful_series,
            failed_series=failed_series,
            asset_type="bloomberg_ingestion",
            asset_metadata=asset_metadata,
        )

    @classmethod
    def for_csv_loader(
        cls,
        records_loaded: int,
        record_type: str,
        csv_path: Optional[str] = None,
        version_date: Optional[str] = None,
        s3_control_table_path: Optional[str] = None,
        lookup_table_type: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        validation_errors: Optional[List[str]] = None,
    ) -> "AssetSummary":
        """Create a CSV loader summary with specific metadata.

        Args:
            records_loaded: Number of records loaded
            record_type: Type of records (e.g., "meta_series", "lookup_tables")
            csv_path: Path to CSV file
            version_date: Version date for S3 control table
            s3_control_table_path: S3 path to control table
            lookup_table_type: Type of lookup table (if applicable)
            details: Optional detailed results dictionary
            validation_errors: Optional list of validation errors

        Returns:
            AssetSummary instance configured for CSV loader
        """
        asset_metadata = {
            "record_type": record_type,
        }

        if csv_path:
            asset_metadata["csv_path"] = csv_path
        if version_date:
            asset_metadata["version_date"] = version_date
        if s3_control_table_path:
            asset_metadata["s3_control_table_path"] = s3_control_table_path
        if lookup_table_type:
            asset_metadata["lookup_table_type"] = lookup_table_type
        if validation_errors:
            asset_metadata["validation_errors"] = validation_errors

        # Build successful series from details
        successful_series = []
        if details:
            if record_type == "meta_series":
                for series_code, row_index in details.items():
                    successful_series.append({"series_code": series_code, "row_index": row_index})
            elif record_type == "lookup_tables":
                for lookup_type, type_details in details.items():
                    for name, id_val in type_details.items():
                        successful_series.append(
                            {"lookup_table_type": lookup_type, "name": name, "id": id_val}
                        )

        return cls(
            total_series=records_loaded,
            successful_count=records_loaded,
            failed_count=len(validation_errors) if validation_errors else 0,
            total_rows=records_loaded,
            target_date=datetime.now(),
            successful_series=successful_series,
            failed_series=[],
            asset_type="csv_loader",
            asset_metadata=asset_metadata,
        )

    @classmethod
    def for_calculation(
        cls,
        rows_calculated: int,
        calculation_id: Optional[int] = None,
        calculation_type: Optional[str] = None,
        formula: Optional[str] = None,
        parent_series_count: Optional[int] = None,
        child_series_count: Optional[int] = None,
        target_date: Optional[datetime] = None,
        additional_metadata: Optional[Dict[str, Any]] = None,
    ) -> "AssetSummary":
        """Create a calculation summary with specific metadata.

        Args:
            rows_calculated: Number of rows calculated
            calculation_id: Calculation ID (optional)
            calculation_type: Type of calculation (optional)
            formula: Calculation formula (optional)
            parent_series_count: Number of parent series (optional)
            child_series_count: Number of child series (optional)
            target_date: Target date for calculation (optional)
            additional_metadata: Optional dictionary with additional metadata to include

        Returns:
            AssetSummary instance configured for calculation
        """
        asset_metadata = {}
        if calculation_id is not None:
            asset_metadata["calculation_id"] = calculation_id
        if calculation_type:
            asset_metadata["calculation_type"] = calculation_type
        if formula:
            asset_metadata["formula"] = formula
        if parent_series_count is not None:
            asset_metadata["parent_series_count"] = parent_series_count
        if child_series_count is not None:
            asset_metadata["child_series_count"] = child_series_count
        if additional_metadata:
            asset_metadata.update(additional_metadata)

        return cls(
            total_series=0,
            successful_count=1 if rows_calculated > 0 else 0,
            failed_count=0,
            total_rows=rows_calculated,
            target_date=target_date,
            asset_type="calculation",
            asset_metadata=asset_metadata,
        )

    # Legacy methods for backward compatibility
    @classmethod
    def for_ingestion(
        cls,
        matching_series: List[Dict[str, Any]],
        successful_series: List[Dict[str, Any]],
        failed_series: List[Dict[str, Any]],
        total_rows_inserted: int,
        target_date: datetime,
    ) -> "AssetSummary":
        """Create an ingestion summary (legacy method for backward compatibility).

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
