"""Teams notification message builders using YAML templates."""

from pathlib import Path

import yaml
from dagster import RunFailureSensorContext, RunStatusSensorContext, get_dagster_logger

logger = get_dagster_logger()


def _load_templates() -> dict:
    """Load Teams message templates from YAML file."""
    # Get the directory where this file is located
    current_dir = Path(__file__).parent
    template_path = current_dir / "teams_templates.yaml"
    
    if not template_path.exists():
        raise FileNotFoundError(f"Template file not found: {template_path}")
    
    with open(template_path, "r") as f:
        return yaml.safe_load(f)


def _format_message(template: dict, **kwargs) -> str:
    """Format a message template with provided values."""
    title = template.get("title", "").format(**kwargs)
    fields = template.get("fields", [])
    footer = template.get("footer", "")
    
    message_parts = [f"**{title}**", ""]
    
    for field in fields:
        field_name = field.get("name", "")
        field_value = field.get("value", "").format(**kwargs)
        # Skip duration field if it's empty (job might not have completed yet)
        if field_name == "Duration" and not field_value:
            continue
        message_parts.append(f"**{field_name}:** {field_value}")
    
    if footer:
        message_parts.append("")
        message_parts.append(footer)
    
    return "\n".join(message_parts)


def failure_message_fn(context: RunFailureSensorContext) -> str:
    """Create a detailed failure message for Teams notification.
    
    Args:
        context: The run failure sensor context containing job and error information
        
    Returns:
        Formatted message string for Teams notification
        
    Raises:
        Exception: If message generation fails, logs the error and re-raises
    """
    try:
        job_name = context.dagster_run.job_name
        run_id = context.dagster_run.run_id
        
        logger.info(
            f"Generating Teams failure notification for job '{job_name}', run_id: {run_id}"
        )
        
        templates = _load_templates()
        template = templates.get("failure", {})
        
        # Get error message from failure event
        error_message = "Unknown error"
        if context.failure_event:
            error_message = context.failure_event.message or str(context.failure_event)
        
        # Get run URL if available
        webserver_url = (
            context.instance.webserver_url
            if hasattr(context.instance, "webserver_url") and context.instance.webserver_url
            else None
        )
        run_url = f"{webserver_url}/runs/{run_id}" if webserver_url else f"Run ID: {run_id}"
        
        message = _format_message(
            template,
            job_name=job_name,
            run_id=run_id,
            error_message=error_message,
            run_url=run_url,
        )
        
        logger.info(
            f"Successfully generated Teams failure notification for job '{job_name}', run_id: {run_id}"
        )
        
        return message
        
    except Exception as e:
        logger.error(
            f"Failed to generate Teams failure notification for job '{context.dagster_run.job_name}', "
            f"run_id: {context.dagster_run.run_id}. Error: {str(e)}",
            exc_info=True,
        )
        # Re-raise to let dagster-msteams handle the error
        raise


def success_message_fn(context: RunStatusSensorContext) -> str:
    """Create a detailed success message for Teams notification.
    
    Args:
        context: The run status sensor context containing job information
        
    Returns:
        Formatted message string for Teams notification
        
    Raises:
        Exception: If message generation fails, logs the error and re-raises
    """
    try:
        job_name = context.dagster_run.job_name
        run_id = context.dagster_run.run_id
        
        logger.info(
            f"Generating Teams success notification for job '{job_name}', run_id: {run_id}"
        )
        
        templates = _load_templates()
        template = templates.get("success", {})
        
        # Get run URL if available
        webserver_url = (
            context.instance.webserver_url
            if hasattr(context.instance, "webserver_url") and context.instance.webserver_url
            else None
        )
        run_url = f"{webserver_url}/runs/{run_id}" if webserver_url else f"Run ID: {run_id}"
        
        # Get duration if available
        duration = ""
        if context.dagster_run.start_time and context.dagster_run.end_time:
            duration_seconds = (
                context.dagster_run.end_time - context.dagster_run.start_time
            ).total_seconds()
            duration = f"{duration_seconds:.2f} seconds"
        
        message = _format_message(
            template,
            job_name=job_name,
            run_id=run_id,
            duration=duration,
            run_url=run_url,
        )
        
        logger.info(
            f"Successfully generated Teams success notification for job '{job_name}', run_id: {run_id}"
        )
        
        return message
        
    except Exception as e:
        logger.error(
            f"Failed to generate Teams success notification for job '{context.dagster_run.job_name}', "
            f"run_id: {context.dagster_run.run_id}. Error: {str(e)}",
            exc_info=True,
        )
        # Re-raise to let dagster-msteams handle the error
        raise

