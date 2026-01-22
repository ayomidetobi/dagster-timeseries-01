"""Helper functions for CSV loader operations with AssetSummary reporting."""

from typing import Any, Dict, List, Optional

from dagster import AssetExecutionContext

from dagster_quickstart.utils.summary import AssetSummary


def add_csv_loader_summary_metadata(
    context: AssetExecutionContext,
    records_loaded: int,
    record_type: str,
    details: Optional[Dict[str, Any]] = None,
    csv_path: Optional[str] = None,
    version_date: Optional[str] = None,
    s3_control_table_path: Optional[str] = None,
    lookup_table_type: Optional[str] = None,
    validation_errors: Optional[List[str]] = None,
) -> None:
    """Add summary metadata to Dagster context using AssetSummary for CSV loader operations.

    Args:
        context: Dagster execution context for logging and metadata
        records_loaded: Number of records loaded
        record_type: Type of records (e.g., "meta_series", "lookup_tables")
        details: Optional dictionary with detailed results
        csv_path: Path to CSV file
        version_date: Version date for S3 control table
        s3_control_table_path: S3 path to control table
        lookup_table_type: Type of lookup table (if applicable)
        validation_errors: Optional list of validation errors
    """
    summary = AssetSummary.for_csv_loader(
        records_loaded=records_loaded,
        record_type=record_type,
        csv_path=csv_path,
        version_date=version_date,
        s3_control_table_path=s3_control_table_path,
        lookup_table_type=lookup_table_type,
        details=details,
        validation_errors=validation_errors,
    )

    # Add summary metadata to context
    summary.add_to_context(context)
