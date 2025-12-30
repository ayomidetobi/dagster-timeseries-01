"""Email notification message builders using shared message builder utilities."""

from dagster import RunFailureSensorContext, RunStatusSensorContext

from dagster_quickstart.notifications.message_builders import (
    build_failure_message,
    build_success_message,
)


def failure_message_fn(context: RunFailureSensorContext) -> str:
    """Create a detailed failure message for email notification.

    Uses the shared message builder with plain text formatting for email.

    Args:
        context: The run failure sensor context containing job and error information

    Returns:
        Formatted message string for email notification (plain text format)

    Raises:
        Exception: If message generation fails, logs the error and re-raises
    """
    return build_failure_message(context, format_type="email")


def success_message_fn(context: RunStatusSensorContext) -> str:
    """Create a detailed success message for email notification.

    Uses the shared message builder with plain text formatting for email.

    Args:
        context: The run status sensor context containing job information

    Returns:
        Formatted message string for email notification (plain text format)

    Raises:
        Exception: If message generation fails, logs the error and re-raises
    """
    return build_success_message(context, format_type="email")
