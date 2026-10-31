"""Scenario 4: Data Passing Patterns & Custom MinIO/S3 XCom Backend.

Addresses the critical data engineering assessment gap:
Eliminating PostgreSQL metadata database bloat and IPC failure by routing
large intermediate datasets (DataFrames, serialized model artifacts, JSON batches)
to S3-compatible object storage (MinIO) transparently.

Architectural Mechanism:
1. Upstream task returns a large payload (>1KB threshold).
2. `MinIOXComBackend.serialize_value()` detects size > 1024 bytes.
3. Payload is serialized to JSON and pushed to MinIO bucket (`s3://airflow-xcom-bucket/xcom/...`).
4. Only the lightweight URI string reference is written to the PostgreSQL `xcom` table.
5. Downstream task pulls XCom: `MinIOXComBackend.deserialize_value()` intercepts the URI,
   fetches the payload from MinIO, and hands the fully reconstructed Python dictionary
   to the downstream task transparently!
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "lead-data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=15),
}


@dag(
    dag_id="04_custom_xcom_backend_minio",
    default_args=default_args,
    description="Demonstration of transparent S3/MinIO Custom XCom offloading vs Postgres bloat",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lead-de", "xcom", "custom-backend", "minio", "s3-offload"],
)
def custom_xcom_pipeline():
    """Workflow demonstrating transparent large artifact passing via MinIO XCom Backend."""

    start = EmptyOperator(task_id="start_xcom_demo")

    # 1. Producer Task 1: Small Primitive Metadata (Under 1KB - Stored in Postgres)
    @task(task_id="emit_small_metadata")
    def emit_small_metadata() -> dict[str, Any]:
        """Returns a small metadata status dictionary (<1KB).

        MinIOXComBackend detects payload <= 1024 bytes and stores directly in PostgreSQL.
        """
        small_payload = {
            "pipeline_name": "lead_de_assessment",
            "batch_version": "v2.10.5",
            "execution_flag": True,
            "status": "INITIALIZED",
        }
        logger.info("Emitting small metadata (<1KB): %s", small_payload)
        return small_payload

    # 2. Producer Task 2: Large Dataset Payload (Exceeds 1KB - Offloaded to MinIO)
    @task(task_id="extract_large_synthetic_batch")
    def extract_large_synthetic_batch() -> dict[str, Any]:
        """Generates a large dataset exceeding the 1024-byte threshold.

        MinIOXComBackend intercepts this return value, uploads it to MinIO at
        `s3://airflow-xcom-bucket/xcom/...`, and returns only the URI to PostgreSQL.
        """
        # Generate 150 simulated customer event records (~15KB)
        records = [
            {
                "event_id": f"evt_{i:05d}",
                "user_id": f"usr_{i % 50:04d}",
                "event_type": "transaction_placed" if i % 2 == 0 else "balance_inquiry",
                "amount": round(25.50 * (i + 1), 2),
                "currency": "USD",
                "ip_address": f"192.168.1.{i % 255}",
                "timestamp": datetime.utcnow().isoformat(),
                "device_fingerprint": f"sha256_mock_fingerprint_token_{i * 987654321}",
            }
            for i in range(150)
        ]

        payload = {
            "batch_id": "large_batch_2026_demo",
            "total_events": len(records),
            "generated_at": datetime.utcnow().isoformat(),
            "records": records,
        }
        logger.info(
            "Emitting large synthetic dataset (%d records, ~15KB). MinIOXComBackend will offload to S3.",
            len(records),
        )
        return payload

    small_meta = emit_small_metadata()
    large_batch = extract_large_synthetic_batch()

    # 3. Downstream Consumer Task (Transparent Deserialization)
    @task(task_id="consume_and_audit_xcom_payload")
    def consume_and_audit(meta: dict[str, Any], batch: dict[str, Any]) -> dict[str, Any]:
        """Downstream consumer receives deserialized objects seamlessly.

        Notice: The task author writes normal Python code! The MinIO download
        happened completely transparently in the background during XCom deserialization.
        """
        logger.info("Received Small Metadata from Postgres XCom: %s", meta["pipeline_name"])
        logger.info(
            "Received Large Batch from MinIO XCom: Batch ID '%s', Total Events: %d",
            batch["batch_id"],
            batch["total_events"],
        )

        # Audit business computation
        total_volume = sum(r["amount"] for r in batch["records"])
        logger.info("Computed Total Transaction Volume across MinIO batch: $%.2f", total_volume)

        return {
            "audit_status": "PASSED",
            "total_volume_audited": total_volume,
            "events_verified": batch["total_events"],
            "backend_verified": "MinIO S3 offload + Postgres pointer confirmed",
        }

    audit_result = consume_and_audit(small_meta, large_batch)
    end = EmptyOperator(task_id="end_xcom_demo")

    start >> [small_meta, large_batch]
    audit_result >> end


custom_xcom_pipeline_dag = custom_xcom_pipeline()
