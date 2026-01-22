from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_asset_checks_from_modules,
    load_assets_from_modules,
)
from dagster_msteams import (
    MSTeamsResource,
    make_teams_on_run_failure_sensor,
)
from dagster_polars import PolarsParquetIOManager
from decouple import config

from dagster_clickhouse.io_manager import duckdb_io_manager
from dagster_quickstart.assets import (
    bloomberg_ingestion,
    calculations,
    csv_loader,
    hackernews,
)
from dagster_quickstart.assets.csv_loader.load_dependency.sensor import (
    add_child_series_partitions_sensor,
)
from dagster_quickstart.assets.csv_loader.load_metaseries.sensor import (
    add_meta_series_partitions_sensor,
)
from dagster_quickstart.notifications.email_sensors import (
    outlook_email_on_run_failure,
)
from dagster_quickstart.notifications.teams_messages import (
    failure_message_fn,
)
from dagster_quickstart.resources import (
    OutlookEmailResource,
    PyPDLResource,
)
from dagster_quickstart.resources.duckdb_datacacher import duckdb_datacacher
from dagster_quickstart.utils.database_config import get_database_resource

all_assets = load_assets_from_modules([calculations, csv_loader, hackernews, bloomberg_ingestion])

# Load asset checks
all_asset_checks = load_asset_checks_from_modules([csv_loader])

# Initialize DuckDB datacacher with S3 credentials from environment
# You can configure these via environment variables or pass directly
duckdb_cacher = duckdb_datacacher(
    bucket=config("S3_BUCKET", default=None),
    access_key=config("S3_ACCESS_KEY", default=None),
    secret_key=config("S3_SECRET_KEY", default=None),
    region=config("S3_REGION", default=None),
)

database_resource = get_database_resource(duckdb_cacher=duckdb_cacher)

# Define resources
# Using DuckDB with S3 as the datalake
resources = {
    "duckdb": database_resource,
    "pypdl_resource": PyPDLResource(),
    "io_manager": duckdb_io_manager,  # Default IO manager
    "duckdb_io_manager": duckdb_io_manager,  # Explicit key for CSV loader assets
    "polars_parquet_io_manager": PolarsParquetIOManager(base_dir="data/parquet"),
    "msteams": MSTeamsResource(hook_url=config("TEAMS_WEBHOOK_URL")),
    "outlook_email": OutlookEmailResource.from_config(),
}
# Define jobs
# Main ingestion job for assets with daily partitions only
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

bloomberg_ingestion_job = define_asset_job(
    name="bloomberg_ingestion_job",
    selection=AssetSelection.groups("bloomberg_ingestion"),
    description="Job for ingesting Bloomberg data using multi-dimensional partitions (daily + series)",
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

# Outlook email failure sensor (uses same message format as Teams)
outlook_email_failure_sensor = outlook_email_on_run_failure

# # Outlook email success sensor (optional - uncomment to enable)
# outlook_email_success_sensor = outlook_email_on_run_success.configured(
#     {
#         "monitored_jobs": [ingestion_job.name, metadata_job.name, calculations_job.name],
#     },
#     name="outlook_email_on_run_success",
# )

defs = Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=[ingestion_job, bloomberg_ingestion_job, metadata_job, calculations_job],
    schedules=[ingestion_schedule, calculations_schedule],
    sensors=[
        teams_on_run_failure,
        outlook_email_failure_sensor,
        add_meta_series_partitions_sensor,
        add_child_series_partitions_sensor,
    ],
    resources=resources,
)
