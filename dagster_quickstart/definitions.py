from dagster import (
    AssetSelection,
    Definitions,
    EnvVar,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_msteams import (
    MSTeamsResource,
    make_teams_on_run_failure_sensor,
)
from dagster_polars import PolarsParquetIOManager

# from dagster_clickhouse.bloomberg import BloombergResource
from dagster_clickhouse.io_manager import clickhouse_io_manager
from dagster_clickhouse.resources import ClickHouseResource
from dagster_quickstart.assets import  calculations, csv_loader, ingestion
from dagster_quickstart.notifications.teams_messages import (
    failure_message_fn,
    success_message_fn,
)
from decouple import config

all_assets = load_assets_from_modules([ingestion, calculations, csv_loader])

# Define resources
# ClickHouseResource is a ConfigurableResource, so we can instantiate it directly
# The IO manager is a factory function, so we pass it as a reference
resources = {
    "clickhouse": ClickHouseResource.from_config(),
    # "bloomberg": BloombergResource(),
    "io_manager": clickhouse_io_manager,
    "polars_parquet_io_manager": PolarsParquetIOManager(base_dir="data/parquet"),
    "msteams": MSTeamsResource(hook_url=config("TEAMS_WEBHOOK_URL")),
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

# Create Teams notification sensors
# Following the docs example: use os.getenv() for environment variables
teams_webhook_url = config("TEAMS_WEBHOOK_URL")
webserver_base_url = config("DAGSTER_WEBSERVER_URL") or None


teams_on_run_failure = make_teams_on_run_failure_sensor(
    hook_url=teams_webhook_url,
    message_fn=failure_message_fn,
    webserver_base_url=webserver_base_url,
)

# # Sensor for job success
# teams_on_run_success = make_teams_on_run_success_sensor(
#     hook_url=teams_webhook_url,
#     message_fn=success_message_fn,
#     monitored_jobs=[ingestion_job, metadata_job, calculations_job],
# )

defs = Definitions(
    assets=all_assets,
    jobs=[ingestion_job, metadata_job, calculations_job],
    schedules=[ingestion_schedule, calculations_schedule],
    sensors=[teams_on_run_failure],
    resources=resources,
)
