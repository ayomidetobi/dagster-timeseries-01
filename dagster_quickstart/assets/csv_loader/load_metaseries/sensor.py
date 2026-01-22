"""Sensor that adds dynamic partitions for meta series based on S3-backed control tables.

Source of truth:
    S3 Parquet control tables → DuckDB view (metaSeries)

This sensor compares what exists in S3 (via the view) with what Dagster
already has registered as dynamic partitions and adds only the missing ones.
"""

from typing import cast

from dagster import (
    AssetKey,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    asset_sensor,
)

from dagster_quickstart.resources import DuckDBResource
from dagster_quickstart.utils.csv_loader_helpers import ensure_views_exist
from dagster_quickstart.utils.exceptions import S3ControlTableNotFoundError
from dagster_quickstart.utils.helpers import get_version_date
from dagster_quickstart.utils.partitions import META_SERIES_PARTITION
from database.meta_series import MetaSeriesManager


@asset_sensor(
    asset_key=AssetKey("load_meta_series_from_csv"),
    name="add_meta_series_partitions_sensor",
    description="Adds dynamic partitions for each series_code in the metaSeries control table",
    minimum_interval_seconds=15,
)
def add_meta_series_partitions_sensor(
    context: SensorEvaluationContext,
    duckdb: DuckDBResource,
) -> SensorResult | SkipReason:
    """Sensor that adds dynamic partitions for meta series using series codes.

    This sensor watches the `load_meta_series_from_csv` asset and when it completes,
    queries DuckDB to get all series codes and adds them as dynamic partitions.

    Args:
        context: Sensor evaluation context
        duckdb: DuckDB resource to query meta series data

    Returns:
        SensorResult with dynamic partition requests, or SkipReason if no new partitions
    """
    try:
        meta_manager = MetaSeriesManager(duckdb)
        version_date = get_version_date()

        # Ensure views exist - will raise S3ControlTableNotFoundError if S3 file doesn't exist
        try:
            ensure_views_exist(
                duckdb=duckdb,
                version_date=version_date,
                create_view_funcs=[meta_manager.create_or_update_view],
                context=None,
                control_type="metaSeries",
            )
        except S3ControlTableNotFoundError as s3_error:
            # Log and skip when S3 control table doesn't exist (expected if CSV hasn't been loaded)
            context.log.info(
                f"{s3_error.control_type} S3 control table not found for version {s3_error.version_date} - CSV may not have been loaded"
            )
            return SkipReason(str(s3_error))

        # Query metaSeries view for all series codes
        query = """
            SELECT DISTINCT series_code
            FROM metaSeries
            WHERE series_code IS NOT NULL
              AND series_code != ''
            ORDER BY series_code
        """

        result = duckdb.execute_query(query)

        if result is None or result.empty:
            context.log.info("No active series codes found in metaSeries view")
            return SkipReason("No active series codes found in metaSeries view")

        # Extract and normalize series codes
        series_codes = [
            str(code).strip()
            for code in result["series_code"].tolist()
            if code and str(code).strip()
        ]

        if not series_codes:
            context.log.info("No valid series codes found after normalization")
            return SkipReason("No valid series codes found after normalization")

        # Get existing partitions from Dagster
        partition_name = cast("str", META_SERIES_PARTITION.name)
        existing_partitions = set[str](context.instance.get_dynamic_partitions(partition_name))

        # Find new partitions to add
        partitions_to_add = [code for code in series_codes if code not in existing_partitions]

        if not partitions_to_add:
            context.log.info(f"All {len(series_codes)} series codes already exist as partitions")
            return SkipReason(f"All {len(series_codes)} series codes already exist as partitions")

        context.log.info(
            f"Adding {len(partitions_to_add)} new partitions out of {len(series_codes)} total series codes"
        )

        # Build request to add new partitions
        dynamic_partitions_request = META_SERIES_PARTITION.build_add_request(partitions_to_add)

        return SensorResult(
            run_requests=[],
            dynamic_partitions_requests=[dynamic_partitions_request],
        )

    except Exception as e:
        context.log.error(f"Failed to update meta-series partitions: {e}", exc_info=True)
        return SkipReason(f"Error adding partitions: {e}")
