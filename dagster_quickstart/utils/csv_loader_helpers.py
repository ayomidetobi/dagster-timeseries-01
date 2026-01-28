"""Generic helper functions for CSV loader workflows.

Provides reusable functions for the common CSV → validate → write → create view flow
used across all CSV loaders (lookup tables, meta series, dependencies).
"""

from typing import Any, Callable, Optional, Union

from dagster import AssetExecutionContext

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.constants import NULL_VALUE_REPRESENTATION
from dagster_quickstart.utils.duckdb_helpers import load_csv_to_temp_table, unregister_temp_table
from dagster_quickstart.utils.exceptions import (
    DatabaseQueryError,
    S3ControlTableNotFoundError,
)


def _load_csv_to_temp(
    duckdb: DuckDBResource,
    csv_path: str,
    context: AssetExecutionContext,
) -> str:
    """Load CSV file into temporary table.

    Wraps load_csv_to_temp_table with NULL_VALUE_REPRESENTATION for consistent usage.

    Args:
        duckdb: DuckDB resource with S3 access
        csv_path: Path to CSV file
        context: Dagster execution context

    Returns:
        Name of the temporary table

    Raises:
        CSVValidationError: If CSV cannot be read
    """
    return load_csv_to_temp_table(duckdb, csv_path, NULL_VALUE_REPRESENTATION, context)


def _run_validation_and_save(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    temp_table: str,
    version_date: str,
    save_to_s3_func: Union[
        Callable[[DuckDBResource, str, str, Optional[AssetExecutionContext]], str],
        Callable[[DuckDBResource, str, str, Optional[AssetExecutionContext], Any], str],
    ],
    create_view_func: Optional[
        Callable[[DuckDBResource, str, Optional[AssetExecutionContext]], None]
    ] = None,
    validation_func: Optional[
        Callable[[AssetExecutionContext, DuckDBResource, str, str], None]
    ] = None,
    view_creation_enabled: bool = True,
    **save_kwargs: Any,
) -> str:
    """Run validation, save to S3, and create view.

    Orchestrates validation, saving, and view creation steps.

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        temp_table: Name of temporary table with CSV data
        version_date: Version date for this run
        save_to_s3_func: Function to save data to S3
        create_view_func: Optional function to create/update view
        validation_func: Optional validation function
        view_creation_enabled: Whether to create/update view after writing
        **save_kwargs: Additional keyword arguments to pass to save_to_s3_func

    Returns:
        Relative S3 path where data was saved

    Raises:
        DatabaseQueryError: If write operation fails
        ReferentialIntegrityError: If validation fails (when validation_func is provided)
    """
    # Step 2: Validate data (if validation function provided)
    if validation_func is not None:
        context.log.info("Validating data against referential integrity constraints")
        validation_func(context, duckdb, temp_table, version_date)

    # Step 3: Write validated data to S3 control table (versioned, immutable)
    # Call save function with temp_table, version_date, context, and any additional kwargs
    if save_kwargs:
        relative_path = save_to_s3_func(duckdb, temp_table, version_date, context, **save_kwargs)
    else:
        relative_path = save_to_s3_func(duckdb, temp_table, version_date, context)

    # Step 4: Create/update DuckDB view over S3 control table
    if view_creation_enabled and create_view_func is not None:
        context.log.info("Creating/updating DuckDB view over S3 control table")
        create_view_func(duckdb, version_date, context)

    context.log.info(f"Successfully processed CSV to S3: {relative_path}")
    return relative_path


def process_csv_to_s3_with_validation(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    csv_path: str,
    version_date: str,
    save_to_s3_func: Union[
        Callable[[DuckDBResource, str, str, Optional[AssetExecutionContext]], str],
        Callable[[DuckDBResource, str, str, Optional[AssetExecutionContext], Any], str],
    ],
    create_view_func: Optional[
        Callable[[DuckDBResource, str, Optional[AssetExecutionContext]], None]
    ] = None,
    validation_func: Optional[
        Callable[[AssetExecutionContext, DuckDBResource, str, str], None]
    ] = None,
    view_creation_enabled: bool = True,
    **save_kwargs: Any,
) -> str:
    """Generic helper for CSV → validate → write → create view flow.

    This function orchestrates the complete data flow:
    1. Load CSV to temp table
    2. Validate data (optional)
    3. Write validated data to S3 control table
    4. Create/update DuckDB view over S3 control table

    Args:
        context: Dagster execution context
        duckdb: DuckDB resource with S3 access
        csv_path: Path to CSV file
        version_date: Version date for this run (YYYY-MM-DD)
        save_to_s3_func: Function to save data to S3. Signature:
            (duckdb, temp_table, version_date, context, **kwargs) -> str (relative_path)
        create_view_func: Optional function to create/update view. Signature:
            (duckdb, version_date, context) -> None
        validation_func: Optional validation function. Signature:
            (context, duckdb, temp_table, version_date) -> None
        view_creation_enabled: Whether to create/update view after writing (default: True)
        **save_kwargs: Additional keyword arguments to pass to save_to_s3_func

    Returns:
        Relative S3 path where data was saved

    Raises:
        DatabaseQueryError: If write operation fails
        CSVValidationError: If CSV cannot be read
        ReferentialIntegrityError: If validation fails (when validation_func is provided)
    """
    context.log.info(f"Processing CSV to S3 control table: {csv_path}")

    # Step 1: Load CSV directly into DuckDB temp table
    temp_table = _load_csv_to_temp(duckdb, csv_path, context)

    try:
        # Step 2-4: Validate, save to S3, and create view
        relative_path = _run_validation_and_save(
            context=context,
            duckdb=duckdb,
            temp_table=temp_table,
            version_date=version_date,
            save_to_s3_func=save_to_s3_func,
            create_view_func=create_view_func,
            validation_func=validation_func,
            view_creation_enabled=view_creation_enabled,
            **save_kwargs,
        )
        return relative_path

    finally:
        # Clean up temp table
        unregister_temp_table(duckdb, temp_table, context)


def ensure_views_exist(
    duckdb: DuckDBResource,
    version_date: str,
    create_view_funcs: list[Callable[[DuckDBResource, str, Optional[AssetExecutionContext]], None]],
    context: Optional[AssetExecutionContext] = None,
    control_type: Optional[str] = None,
) -> None:
    """Ensure DuckDB views exist before processing (for validation/reading existing data).

    This is useful when views are needed for validation queries that reference existing data.

    Args:
        duckdb: DuckDB resource with S3 access
        version_date: Version date for this run
        create_view_funcs: List of functions to create/update views. Each function signature:
            (duckdb, version_date, context) -> None
        context: Optional Dagster execution context for logging
        control_type: Optional control table type. If provided, raises S3ControlTableNotFoundError
            on failure instead of just logging (useful for sensors).

    Note:
        If views don't exist yet (first run) and control_type is None, this will log a warning
        but not fail. The views will be created after data is saved.
        If control_type is provided, DatabaseQueryError will be converted to
        S3ControlTableNotFoundError.

    Raises:
        S3ControlTableNotFoundError: If control_type is provided and views cannot be created
    """
    for create_view_func in create_view_funcs:
        try:
            create_view_func(duckdb, version_date, context)
        except DatabaseQueryError as db_error:
            if control_type is not None:
                # Convert DatabaseQueryError to S3ControlTableNotFoundError for sensors
                raise S3ControlTableNotFoundError(
                    control_type=control_type, version_date=version_date
                ) from db_error
            # If views don't exist yet (first run), that's okay - logic will create the data
            if context:
                context.log.info("Views don't exist yet - will be created after data is saved")
