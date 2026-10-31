"""Production-grade alerting, safety callbacks, and SLA handlers for Apache Airflow."""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger("airflow.callbacks")


def on_failure_alert(context: dict[str, Any]) -> None:
    """Callback triggered whenever an Airflow task instance fails.

    Extracts execution context (DAG ID, Task ID, execution date, try number, log URL)
    and formats an incident notification payload.
    """
    dag_id = context.get("dag", None)
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    execution_date = context.get("logical_date") or context.get("execution_date")
    try_number = task_instance.try_number if task_instance else 1
    max_tries = task_instance.max_tries if task_instance else 1
    log_url = task_instance.log_url if task_instance else "N/A"
    exception = context.get("exception", "No exception info")

    alert_payload = {
        "event": "TASK_FAILURE",
        "dag_id": str(dag_id),
        "task_id": task_id,
        "try_number": try_number,
        "max_tries": max_tries,
        "logical_date": str(execution_date),
        "log_url": log_url,
        "error_summary": str(exception),
    }

    logger.error("🚨 [AIRFLOW ALERT - TASK FAILED]: %s", alert_payload)
    # In production, dispatch alert_payload to PagerDuty / Slack Webhook / OpsGenie


def on_retry_alert(context: dict[str, Any]) -> None:
    """Callback triggered on intermediate task retry attempts before final failure."""
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    try_number = task_instance.try_number if task_instance else 1
    max_tries = task_instance.max_tries if task_instance else 1
    exception = context.get("exception", "Transient failure")

    logger.warning(
        "⚠️ [AIRFLOW WARNING - TASK RETRY]: Task '%s' failed attempt %d/%d. Exception: %s. Initiating exponential backoff.",
        task_id,
        try_number,
        max_tries,
        exception,
    )


def sla_miss_alert(
    dag: Any,
    task_list: list[Any],
    blocking_task_list: list[Any],
    slas: list[Any],
    blocking_tis: list[Any],
) -> None:
    """Callback triggered when one or more tasks breach their configured Service Level Agreement (SLA).

    Args:
        dag: DAG instance where SLA miss occurred.
        task_list: List of tasks that missed SLA.
        blocking_task_list: List of upstream tasks blocking the missed tasks.
        slas: List of SlaMiss objects.
        blocking_tis: List of task instances blocking execution.
    """
    sla_info = {
        "dag_id": getattr(dag, "dag_id", str(dag)),
        "tasks_missing_sla": [getattr(t, "task_id", str(t)) for t in task_list],
        "blocking_tasks": [getattr(t, "task_id", str(t)) for t in blocking_task_list],
        "sla_count": len(slas),
        "blocking_task_instances_count": len(blocking_tis),
    }

    logger.critical(
        "⏱️ [AIRFLOW SLA BREACH]: Critical workflow breached SLA threshold! Details: %s",
        sla_info,
    )
