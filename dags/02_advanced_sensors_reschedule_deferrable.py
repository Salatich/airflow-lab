"""Scenario 2: Advanced Sensors & Resource Safety (Reschedule vs Poke & Deferrable Operators).

Addresses the critical data engineering assessment gap:
Understanding worker concurrency mechanics, slot starvation, and modern
asynchronous scheduling via the Airflow Triggerer.

Deep Dive: Why mode="poke" starves worker concurrency slots:
------------------------------------------------------------
- Under `mode="poke"`, a sensor executes synchronously inside a worker slot.
  Between intervals (e.g., `poke_interval=60`), the worker process simply sleeps
  (`time.sleep()`). If `AIRFLOW__CORE__PARALLELISM=32` and 32 sensors are waiting
  for external data files, ALL 32 worker slots are 100% consumed by sleeping sensors.
  Zero processing slots remain for actual ETL / DAG execution! This causes total cluster
  deadlock ("Sensor Starvation").

- Under `mode="reschedule"`, after each unsuccessful poke, the task saves its state,
  relinquishes the worker slot back to the scheduler pool, and enters `UP_FOR_RESCHEDULE`.
  The worker is immediately freed to run heavy computational tasks. When `poke_interval`
  expires, the scheduler schedules the sensor again for another check.

- Deferrable Operators (Airflow Triggerer):
  Takes resource optimization to the theoretical limit. Instead of repeatedly
  polling via worker slots, the operator raises a `TaskDeferred` exception.
  Airflow suspends the task in the database and hands off event listening to the
  centralized `airflow-triggerer` daemon. A single Triggerer process uses Python's
  asynchronous `asyncio` loop to wait on thousands of concurrent triggers using
  virtually ZERO worker CPU and ZERO worker slots.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.time_delta import TimeDeltaSensorAsync

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=20),
}


@dag(
    dag_id="02_advanced_sensors_reschedule_deferrable",
    default_args=default_args,
    description="Demonstration of mode='reschedule' and Deferrable Operators via Airflow Triggerer",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lead-de", "sensors", "reschedule", "deferrable", "triggerer"],
)
def advanced_sensors_pipeline():
    """Workflow demonstrating worker-safe sensor patterns and deferrable operators."""

    start = EmptyOperator(task_id="start_sensing_pipeline")

    # --------------------------------------------------------------------------
    # 1. External Dependency Sensor with mode="reschedule"
    # --------------------------------------------------------------------------
    # Production best practice: NEVER use mode="poke" for dependencies taking
    # more than a few seconds. Always configure mode="reschedule" and explicit timeout.
    wait_for_upstream_ingestion = ExternalTaskSensor(
        task_id="wait_for_upstream_ingestion",
        external_dag_id="01_modern_operators_dynamic_mapping",
        external_task_id="end_pipeline",
        mode="reschedule",  # <<-- CRITICAL: Frees worker slot during wait intervals
        poke_interval=30,  # Seconds between status checks
        timeout=600,  # Fail fast after 10 minutes if upstream never completes
        soft_fail=True,  # Marks task SKIPPED rather than FAILED on timeout
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # --------------------------------------------------------------------------
    # 2. Deferrable Operator via Airflow Triggerer (Zero Worker Slot Consumption)
    # --------------------------------------------------------------------------
    # TimeDeltaSensorAsync yields execution to the Airflow Triggerer daemon.
    # The worker terminates immediately; the Triggerer monitors the time delta
    # via an asyncio coroutine and resumes execution on an available worker when fired.
    wait_time_window_deferrable = TimeDeltaSensorAsync(
        task_id="wait_time_window_deferrable",
        delta=timedelta(seconds=10),  # 10s async deferral demonstration
        mode="reschedule",
        timeout=120,
    )

    # --------------------------------------------------------------------------
    # 3. Downstream Processing Task
    # --------------------------------------------------------------------------
    @task(task_id="execute_safe_downstream_work")
    def execute_downstream() -> dict[str, Any]:
        """Runs once both reschedule and deferrable sensor gates resolve."""
        logger.info(
            "Worker slot acquired! Both external upstream dependency and deferral window resolved successfully."
        )
        return {
            "status": "COMPLETED",
            "execution_mode": "Zero-starvation reschedule & Triggerer async deferral verified",
            "timestamp": datetime.utcnow().isoformat(),
        }

    downstream_work = execute_downstream()
    end = EmptyOperator(task_id="end_sensing_pipeline")

    # Pipeline Topology
    start >> [wait_for_upstream_ingestion, wait_time_window_deferrable] >> downstream_work >> end


advanced_sensors_pipeline_dag = advanced_sensors_pipeline()
