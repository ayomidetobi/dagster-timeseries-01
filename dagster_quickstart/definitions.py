from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_clickhouse.resources import ClickHouseResource
from dagster_clickhouse.io_manager import clickhouse_io_manager
from dagster_quickstart.assets import ingestion, metadata, calculations


all_assets = load_assets_from_modules([ingestion, metadata, calculations])

# Define resources
# ClickHouseResource is a ConfigurableResource, so we can instantiate it directly
# The IO manager is a factory function, so we pass it as a reference
resources = {
    "clickhouse": ClickHouseResource.from_config(),
    "io_manager": clickhouse_io_manager,
}
# Define jobs
ingestion_job = define_asset_job(
    name="ingestion_job",
    selection=AssetSelection.groups("ingestion"),
    description="Job for ingesting raw financial data from various sources",
)

metadata_job = define_asset_job(
    name="metadata_job",
    selection=AssetSelection.groups("metadata"),
    description="Job for managing metadata and lookup tables",
)

calculations_job = define_asset_job(
    name="calculations_job",
    selection=AssetSelection.groups("calculations"),
    description="Job for calculating derived series",
)

# Define schedules (optional - can be enabled as needed)
ingestion_schedule = ScheduleDefinition(
    name="daily_ingestion_schedule",
    job=ingestion_job,
    cron_schedule="0 2 * * *",  # Daily at 2 AM
    # default_status=DefaultSensorStatus.STOPPED,
)

calculations_schedule = ScheduleDefinition(
    name="hourly_calculations_schedule",
    job=calculations_job,
    cron_schedule="0 * * * *",  # Every hour
    # default_status=DefaultSensorStatus.STOPPED,
)

defs = Definitions(
    assets=all_assets,
    jobs=[ingestion_job, metadata_job, calculations_job],
    schedules=[ingestion_schedule, calculations_schedule],
    resources=resources,
)
