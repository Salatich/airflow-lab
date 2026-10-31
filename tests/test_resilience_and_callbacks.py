"""Unit tests for production retry policies, alerting callbacks, and data quality gates."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from airflow_sandbox.callbacks import on_failure_alert, on_retry_alert, sla_miss_alert


def test_on_failure_alert_executes_without_exceptions(caplog):
    """Verifies failure callback extracts context and logs structured alert."""
    mock_ti = MagicMock()
    mock_ti.task_id = "test_failing_task"
    mock_ti.try_number = 3
    mock_ti.max_tries = 3
    mock_ti.log_url = "http://localhost:8080/log?task_id=test_failing_task"

    context = {
        "dag": "test_resilience_dag",
        "task_instance": mock_ti,
        "logical_date": "2026-10-30T20:00:00",
        "exception": RuntimeError("Simulated database connection timeout"),
    }

    with caplog.at_level(logging.ERROR):
        on_failure_alert(context)

    assert "AIRFLOW ALERT - TASK FAILED" in caplog.text
    assert "test_failing_task" in caplog.text


def test_on_retry_alert_executes_without_exceptions(caplog):
    """Verifies retry callback logs backoff event."""
    mock_ti = MagicMock()
    mock_ti.task_id = "transient_api_task"
    mock_ti.try_number = 1
    mock_ti.max_tries = 3

    context = {
        "task_instance": mock_ti,
        "exception": ConnectionResetError("HTTP 429 Too Many Requests"),
    }

    with caplog.at_level(logging.WARNING):
        on_retry_alert(context)

    assert "AIRFLOW WARNING - TASK RETRY" in caplog.text
    assert "transient_api_task" in caplog.text


def test_sla_miss_alert_executes_without_exceptions(caplog):
    """Verifies SLA miss callback logs critical breach."""
    mock_dag = MagicMock()
    mock_dag.dag_id = "sla_critical_pipeline"

    task1 = MagicMock()
    task1.task_id = "slow_extract_task"

    with caplog.at_level(logging.CRITICAL):
        sla_miss_alert(
            dag=mock_dag,
            task_list=[task1],
            blocking_task_list=[task1],
            slas=[MagicMock()],
            blocking_tis=[MagicMock()],
        )

    assert "AIRFLOW SLA BREACH" in caplog.text
    assert "slow_extract_task" in caplog.text
