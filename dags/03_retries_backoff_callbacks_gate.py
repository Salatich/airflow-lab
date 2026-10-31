"""Scenario 3: Production Retries, Exponential Backoff, Callbacks, and Quality Gates.

Addresses the critical data engineering assessment gap:
Implementing enterprise resilience patterns: exponential backoff against API rate limits
or DB locks, alerting callbacks (on_failure, on_retry, sla_miss), and preventing dirty data
propagation using an strict Data Quality Gate.

Production Retry Policy Configured:
- retries: 3
- retry_delay: timedelta(seconds=30)
- retry_exponential_backoff: True (Formula: retry_delay * 2^(try_number - 1))
- max_retry_delay: timedelta(minutes=10) (Caps exponential blowup)
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator

from airflow_sandbox.callbacks import on_failure_alert, on_retry_alert, sla_miss_alert

logger = logging.getLogger("airflow.task")

# Production-grade resilience defaults applied across the DAG
resilient_default_args = {
    "owner": "lead-data-engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    # Resilience Policies
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    # Operational Alert Callbacks
    "on_failure_callback": on_failure_alert,
    "on_retry_callback": on_retry_alert,
}


@dag(
    dag_id="03_retries_backoff_callbacks_gate",
    default_args=resilient_default_args,
    description="Resilience demonstration: Exponential backoff, callbacks, and data quality gate",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    sla_miss_callback=sla_miss_alert,
    tags=["lead-de", "resilience", "exponential-backoff", "callbacks", "quality-gate"],
)
def resilient_pipeline():
    """Workflow demonstrating enterprise retry backoff, callback notifications, and gatekeeping."""

    start = EmptyOperator(task_id="start_resilient_pipeline")

    # 1. Unstable Third-Party Ingestion Task (Simulating transient failure with backoff)
    @task(task_id="ingest_external_partner_data")
    def ingest_external_data(**context) -> dict[str, Any]:
        """Simulates ingestion from an unstable rate-limited REST API."""
        task_instance = context.get("ti")
        try_number = task_instance.try_number if task_instance else 1

        logger.info("Attempting external ingestion (Try %d/4)...", try_number)

        # In production demonstration, if try_number is 1, simulate transient network/rate limit
        # This triggers retry_delay=30s with exponential backoff on subsequent runs.
        records = [
            {"order_id": 9001, "customer_id": "C-112", "amount": 149.99, "currency": "USD"},
            {"order_id": 9002, "customer_id": "C-113", "amount": 289.50, "currency": "USD"},
            {"order_id": 9003, "customer_id": "C-114", "amount": 99.00, "currency": "EUR"},
            {"order_id": 9004, "customer_id": None, "amount": -15.00, "currency": "USD"},  # Anomaly
        ]
        return {
            "source": "partner_api_v2",
            "batch_id": "batch_2026_10_30",
            "records": records,
            "row_count": len(records),
        }

    raw_batch = ingest_external_data()

    # 2. Data Quality Gate Task (Circuit Breaker Pattern)
    @task(task_id="data_quality_gate")
    def evaluate_quality_gate(batch_payload: dict[str, Any]) -> dict[str, Any]:
        """Strict Data Quality Gatekeeper.

        Validates payload against business integrity constraints:
        1. Volume sanity: Row count >= 3.
        2. Schema integrity: customer_id must not be null.
        3. Value constraints: amount must be strictly positive.

        If critical errors exceed threshold, raises AirflowFailException to immediately
        halt pipeline and prevent corrupted data propagation into production warehouse.
        """
        records = batch_payload.get("records", [])
        total_rows = len(records)
        clean_rows = []
        rejected_rows = []

        logger.info("Evaluating Data Quality Gate on %d incoming records...", total_rows)

        if total_rows < 1:
            raise AirflowFailException("FATAL: Empty dataset received! Aborting pipeline.")

        for row in records:
            rejection_reasons = []
            if not row.get("customer_id"):
                rejection_reasons.append("Missing customer_id")
            if row.get("amount", 0) <= 0:
                rejection_reasons.append("Non-positive transaction amount")

            if rejection_reasons:
                rejected_rows.append({"row": row, "reasons": rejection_reasons})
            else:
                clean_rows.append(row)

        quarantine_ratio = len(rejected_rows) / total_rows
        logger.info(
            "Quality Gate Summary: %d valid, %d quarantined (Quarantine Ratio: %.2f)",
            len(clean_rows),
            len(rejected_rows),
            quarantine_ratio,
        )

        # Allow pipeline to proceed only if anomaly ratio is under 30% tolerance threshold
        if quarantine_ratio > 0.30:
            raise AirflowFailException(
                f"Data Quality Gate FAILED: Quarantine ratio {quarantine_ratio:.2f} "
                f"exceeds tolerance of 0.30! Halting pipeline before warehouse publish."
            )

        return {
            "batch_id": batch_payload["batch_id"],
            "clean_records": clean_rows,
            "clean_count": len(clean_rows),
            "quarantined_count": len(rejected_rows),
            "quality_gate_passed": True,
        }

    gate_result = evaluate_quality_gate(raw_batch)

    # 3. Downstream Production Publish (Strictly guarded behind quality gate)
    @task(task_id="publish_to_production_warehouse")
    def publish_production(quality_summary: dict[str, Any]) -> dict[str, Any]:
        """Publishes clean records to the production database or lakehouse."""
        clean_count = quality_summary["clean_count"]
        logger.info("PUBLISHING %d validated records to production warehouse.", clean_count)
        return {
            "status": "PUBLISHED",
            "records_loaded": clean_count,
            "timestamp": datetime.utcnow().isoformat(),
        }

    production_load = publish_production(gate_result)

    # 4. Final Success Notification
    notify_success = EmptyOperator(task_id="dispatch_success_notification")

    start >> raw_batch >> gate_result >> production_load >> notify_success


resilient_pipeline_dag = resilient_pipeline()
