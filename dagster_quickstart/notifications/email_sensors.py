"""Email sensors for Dagster run notifications."""

from typing import List, Optional

from dagster import (
    RunFailureSensorContext,
    RunStatusSensorContext,
    SkipReason,
    run_failure_sensor,
    run_status_sensor,
)

from dagster_quickstart.notifications.email_messages import (
    failure_message_fn,
    success_message_fn,
)
from dagster_quickstart.notifications.outlook_email_resource import OutlookEmailResource


@run_failure_sensor
def outlook_email_on_run_failure(
    context: RunFailureSensorContext,
    outlook_email: OutlookEmailResource,
) -> SkipReason:
    """Sensor that sends Outlook email notification on run failure.

    Args:
        context: The run failure sensor context
        outlook_email: Outlook email resource for sending notifications

    Returns:
        SkipReason indicating sensor execution result
    """
    try:
        # Generate failure message using same format as Teams
        message = failure_message_fn(context)

        # Get job name for subject
        job_name = context.dagster_run.job_name
        subject = f"Dagster Run Failed: {job_name}"

        # Send email
        outlook_email.send_email(subject=subject, body=message)

        context.log.info(
            f"Outlook email failure notification sent for job '{job_name}', "
            f"run_id: {context.dagster_run.run_id}"
        )

        return SkipReason("Email notification sent successfully")

    except Exception as e:
        context.log.error(
            f"Failed to send Outlook email notification for job '{context.dagster_run.job_name}', "
            f"run_id: {context.dagster_run.run_id}. Error: {e}",
            exc_info=True,
        )
        # Don't fail the sensor, just log the error
        return SkipReason(f"Failed to send email notification: {e}")


@run_status_sensor(
    run_status="SUCCESS",
)
def outlook_email_on_run_success(
    context: RunStatusSensorContext,
    outlook_email: OutlookEmailResource,
    monitored_jobs: Optional[List[str]] = None,
) -> SkipReason:
    """Sensor that sends Outlook email notification on run success.

    Args:
        context: The run status sensor context
        outlook_email: Outlook email resource for sending notifications
        monitored_jobs: Optional list of job names to monitor. If None, monitors all jobs.

    Returns:
        SkipReason indicating sensor execution result
    """
    try:
        job_name = context.dagster_run.job_name

        # Skip if monitoring specific jobs and this job is not in the list
        if monitored_jobs and job_name not in monitored_jobs:
            return SkipReason(f"Job '{job_name}' not in monitored jobs list")

        # Generate success message using same format as Teams
        message = success_message_fn(context)

        subject = f"Dagster Run Succeeded: {job_name}"

        # Send email
        outlook_email.send_email(subject=subject, body=message)

        context.log.info(
            f"Outlook email success notification sent for job '{job_name}', "
            f"run_id: {context.dagster_run.run_id}"
        )

        return SkipReason("Email notification sent successfully")

    except Exception as e:
        context.log.error(
            f"Failed to send Outlook email notification for job '{context.dagster_run.job_name}', "
            f"run_id: {context.dagster_run.run_id}. Error: {e}",
            exc_info=True,
        )
        # Don't fail the sensor, just log the error
        return SkipReason(f"Failed to send email notification: {e}")

