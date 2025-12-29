"""Shared message builder utilities for notifications (Teams and Email).

This module contains the common logic for building notification messages
from YAML templates, used by both Teams and Email notifications.
"""

from pathlib import Path
from typing import Literal

import yaml
from dagster import RunFailureSensorContext, RunStatusSensorContext, get_dagster_logger

logger = get_dagster_logger()


def _load_templates() -> dict:
    """Load message templates from YAML file.

    Returns:
        Dictionary containing notification templates

    Raises:
        FileNotFoundError: If template file is not found
    """
    # Get the directory where this file is located
    current_dir = Path(__file__).parent
    template_path = current_dir / "teams_templates.yaml"

    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")

    with open(template_path, "r") as f:
        return yaml.safe_load(f)


def _format_message(
    template: dict,
    format_type: Literal["teams", "email"] = "teams",
    **kwargs,
) -> str:
    """Format a message template with provided values.

    Args:
        template: Template dictionary with title, fields, and footer
        format_type: Format type - "teams" for markdown formatting, "email" for plain text
        **kwargs: Values to format into the template

    Returns:
        Formatted message string for notification
    """
    title = template.get("title", "").format(**kwargs)
    fields = template.get("fields", [])
    footer = template.get("footer", "")

    # Format title based on notification type
    if format_type == "teams":
        message_parts = [f"**{title}**", ""]
    else:  # email
        message_parts = [f"{title}", ""]

    for field in fields:
        field_name = field.get("name", "")
        field_value = field.get("value", "").format(**kwargs)
        # Skip duration field if it's empty (job might not have completed yet)
        if field_name == "Duration" and not field_value:
            continue

        # Format field based on notification type
        if format_type == "teams":
            message_parts.append(f"**{field_name}:** {field_value}")
        else:  # email
            message_parts.append(f"{field_name}: {field_value}")

    if footer:
        message_parts.append("")
        message_parts.append(footer)

    return "\n".join(message_parts)


def _get_run_url(context: RunFailureSensorContext | RunStatusSensorContext) -> str:
    """Get run URL from context.

    Args:
        context: The sensor context (failure or status)

    Returns:
        Run URL string or fallback to run ID
    """
    run_id = context.dagster_run.run_id
    webserver_url = (
        context.instance.webserver_url
        if hasattr(context.instance, "webserver_url") and context.instance.webserver_url
        else None
    )
    return f"{webserver_url}/runs/{run_id}" if webserver_url else f"Run ID: {run_id}"


def _get_duration(context: RunStatusSensorContext) -> str:
    """Get job duration from context.

    Args:
        context: The run status sensor context

    Returns:
        Duration string or empty string if not available
    """
    if context.dagster_run.start_time and context.dagster_run.end_time:
        duration_seconds = (
            context.dagster_run.end_time - context.dagster_run.start_time
        ).total_seconds()
        return f"{duration_seconds:.2f} seconds"
    return ""


def build_failure_message(
    context: RunFailureSensorContext,
    format_type: Literal["teams", "email"] = "teams",
) -> str:
    """Build a failure notification message.

    Args:
        context: The run failure sensor context containing job and error information
        format_type: Format type - "teams" for markdown, "email" for plain text

    Returns:
        Formatted message string for notification

    Raises:
        Exception: If message generation fails, logs the error and re-raises
    """
    try:
        job_name = context.dagster_run.job_name
        run_id = context.dagster_run.run_id

        notification_type = "Teams" if format_type == "teams" else "Email"
        logger.info(
            f"Generating {notification_type} failure notification for job '{job_name}', run_id: {run_id}"
        )

        templates = _load_templates()
        template = templates.get("failure", {})

        # Get error message from failure event
        error_message = "Unknown error"
        if context.failure_event:
            error_message = context.failure_event.message or str(context.failure_event)

        run_url = _get_run_url(context)

        message = _format_message(
            template,
            format_type=format_type,
            job_name=job_name,
            run_id=run_id,
            error_message=error_message,
            run_url=run_url,
        )

        logger.info(
            f"Successfully generated {notification_type} failure notification for job '{job_name}', "
            f"run_id: {run_id}"
        )

        return message

    except Exception as e:
        job_name = context.dagster_run.job_name
        run_id = context.dagster_run.run_id
        notification_type = "Teams" if format_type == "teams" else "Email"
        logger.error(
            f"Failed to generate {notification_type} failure notification for job '{job_name}', "
            f"run_id: {run_id}. Error: {e!s}",
            exc_info=True,
        )
        # Re-raise to let notification handler deal with the error
        raise


def build_success_message(
    context: RunStatusSensorContext,
    format_type: Literal["teams", "email"] = "teams",
) -> str:
    """Build a success notification message.

    Args:
        context: The run status sensor context containing job information
        format_type: Format type - "teams" for markdown, "email" for plain text

    Returns:
        Formatted message string for notification

    Raises:
        Exception: If message generation fails, logs the error and re-raises
    """
    try:
        job_name = context.dagster_run.job_name
        run_id = context.dagster_run.run_id

        notification_type = "Teams" if format_type == "teams" else "Email"
        logger.info(
            f"Generating {notification_type} success notification for job '{job_name}', run_id: {run_id}"
        )

        templates = _load_templates()
        template = templates.get("success", {})

        run_url = _get_run_url(context)
        duration = _get_duration(context)

        message = _format_message(
            template,
            format_type=format_type,
            job_name=job_name,
            run_id=run_id,
            duration=duration,
            run_url=run_url,
        )

        logger.info(
            f"Successfully generated {notification_type} success notification for job '{job_name}', "
            f"run_id: {run_id}"
        )

        return message

    except Exception as e:
        job_name = context.dagster_run.job_name
        run_id = context.dagster_run.run_id
        notification_type = "Teams" if format_type == "teams" else "Email"
        logger.error(
            f"Failed to generate {notification_type} success notification for job '{job_name}', "
            f"run_id: {run_id}. Error: {e!s}",
            exc_info=True,
        )
        # Re-raise to let notification handler deal with the error
        raise

