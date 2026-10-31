"""Custom S3/MinIO XCom Backend for Apache Airflow.

Addresses the critical data engineering gap:
Standard Airflow XCom stores serialized task return values directly into the PostgreSQL
`xcom` table (`value` column as BYTEA / TEXT). When workflows pass medium-to-large payloads
(e.g., Pandas DataFrames, multi-MB JSON batches, feature vectors), this causes:
1. PostgreSQL table and WAL bloat, degrading scheduler query latency.
2. Worker memory spikes and serialization bottlenecks.
3. Transaction log saturation and task IPC timeout failures.

This backend routes payloads exceeding `XCOM_SIZE_THRESHOLD_BYTES` (default 1024 bytes)
to MinIO/S3 object storage, saving only a lightweight URI reference (`s3://...`) in Postgres.
"""

from __future__ import annotations

import json
import logging
import os
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import boto3
from airflow.models.xcom import BaseXCom
from botocore.config import Config

if TYPE_CHECKING:
    from airflow.models.xcom import XCom

logger = logging.getLogger("airflow.xcom.minio_backend")

# Configuration constants
S3_BUCKET = os.getenv("MINIO_BUCKET_NAME", "airflow-xcom-bucket")
S3_PREFIX = os.getenv("XCOM_S3_KEY_PREFIX", "xcom/")
SIZE_THRESHOLD_BYTES = int(os.getenv("XCOM_SIZE_THRESHOLD_BYTES", "1024"))
S3_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

URI_PREFIX = f"s3://{S3_BUCKET}/"


def _get_s3_client():
    """Build boto3 S3 client configured for local MinIO or AWS S3."""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


class MinIOXComBackend(BaseXCom):
    """Custom Airflow XCom Backend routing large payloads to MinIO / AWS S3."""

    @staticmethod
    def serialize_value(
        value: Any,
        *,
        key: str | None = None,
        task_id: str | None = None,
        dag_id: str | None = None,
        run_id: str | None = None,
        map_index: int = -1,
        **kwargs: Any,
    ) -> Any:
        """Serializes XCom value. If serialized size exceeds threshold, routes to S3/MinIO."""
        # Convert object to JSON representation for sizing
        try:
            raw_json = json.dumps(value, default=str)
            raw_bytes = raw_json.encode("utf-8")
        except (TypeError, ValueError):
            # Fall back to standard serializer for unpicklable types
            return BaseXCom.serialize_value(value)

        payload_size = len(raw_bytes)

        # Small primitives stay in PostgreSQL metadata DB
        if payload_size <= SIZE_THRESHOLD_BYTES:
            logger.debug(
                "XCom value for task '%s' (%d bytes) is below threshold (%d bytes); saving to Postgres.",
                task_id,
                payload_size,
                SIZE_THRESHOLD_BYTES,
            )
            return BaseXCom.serialize_value(value)

        # Large payload offloaded to MinIO/S3
        safe_key = key or "return_value"
        safe_run_id = (run_id or "adhoc_run").replace(":", "_").replace("+", "_")
        s3_key = (
            f"{S3_PREFIX.rstrip('/')}/{dag_id}/{safe_run_id}/{task_id}/{safe_key}_{map_index}.json"
        )

        logger.info(
            "📦 [MinIO XCom Backend] Offloading %d bytes for dag='%s' task='%s' to s3://%s/%s",
            payload_size,
            dag_id,
            task_id,
            S3_BUCKET,
            s3_key,
        )

        try:
            s3_client = _get_s3_client()
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=raw_bytes,
                ContentType="application/json",
                Metadata={
                    "dag_id": str(dag_id),
                    "task_id": str(task_id),
                    "key": str(key),
                    "payload_size": str(payload_size),
                },
            )
            s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
            # Return S3 reference pointer string to be saved in Postgres xcom table
            return BaseXCom.serialize_value(s3_uri)
        except Exception as exc:
            logger.error(
                "Failed to offload XCom to MinIO (s3://%s/%s). Falling back to BaseXCom: %s",
                S3_BUCKET,
                s3_key,
                exc,
            )
            return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result: XCom) -> Any:
        """Deserializes XCom value. If value is an S3 URI reference, fetches from MinIO/S3."""
        base_value = BaseXCom.deserialize_value(result)

        if isinstance(base_value, str) and base_value.startswith(URI_PREFIX):
            logger.info("📥 [MinIO XCom Backend] Fetching payload from S3 URI: %s", base_value)
            parsed = urlparse(base_value)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")

            try:
                s3_client = _get_s3_client()
                response = s3_client.get_object(Bucket=bucket, Key=key)
                content = response["Body"].read().decode("utf-8")
                return json.loads(content)
            except Exception as exc:
                logger.error(
                    "Failed to fetch XCom payload from MinIO (bucket=%s, key=%s): %s",
                    bucket,
                    key,
                    exc,
                )
                raise

        return base_value
