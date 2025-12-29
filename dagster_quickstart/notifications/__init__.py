"""Notification utilities for Dagster jobs."""

from dagster_quickstart.notifications.email_messages import (
    failure_message_fn as email_failure_message_fn,
    success_message_fn as email_success_message_fn,
)
from dagster_quickstart.notifications.email_sensors import (
    outlook_email_on_run_failure,
    outlook_email_on_run_success,
)
from dagster_quickstart.notifications.message_builders import (
    build_failure_message,
    build_success_message,
)
from dagster_quickstart.notifications.outlook_email_resource import (
    OutlookEmailResource,
)
from dagster_quickstart.notifications.teams_messages import (
    failure_message_fn,
    success_message_fn,
)

__all__ = [
    # Shared message builders
    "build_failure_message",
    "build_success_message",
    # Teams notifications
    "failure_message_fn",
    "success_message_fn",
    # Email notifications
    "OutlookEmailResource",
    "outlook_email_on_run_failure",
    "outlook_email_on_run_success",
    "email_failure_message_fn",
    "email_success_message_fn",
]
