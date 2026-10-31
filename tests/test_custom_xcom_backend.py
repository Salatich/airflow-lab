"""Unit tests for MinIO / S3 Custom XCom Backend."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

from airflow.models.xcom import BaseXCom
from custom_xcom_backend import S3_BUCKET, URI_PREFIX, MinIOXComBackend


class MockXCom:
    """Mock Airflow XCom database model instance containing serialized value bytes."""

    def __init__(self, value: Any):
        # BaseXCom stores value as JSON-serialized bytes in the metadata DB
        if isinstance(value, bytes):
            self.value = value
        else:
            self.value = BaseXCom.serialize_value(value)


def test_serialize_small_payload_persists_in_postgres():
    """Small payloads under threshold (<=1024 bytes) must NOT be uploaded to S3."""
    small_data = {"status": "OK", "count": 42}

    with patch("custom_xcom_backend._get_s3_client") as mock_get_s3:
        serialized_bytes = MinIOXComBackend.serialize_value(
            small_data,
            key="test_key",
            task_id="task_1",
            dag_id="dag_test",
            run_id="run_1",
        )

        # S3 client must NOT be called for small payload
        mock_get_s3.assert_not_called()
        # Payload is standard JSON bytes
        assert isinstance(serialized_bytes, bytes)
        assert json.loads(serialized_bytes.decode("utf-8")) == small_data


def test_serialize_large_payload_offloaded_to_minio():
    """Large payloads exceeding threshold (>1024 bytes) MUST be offloaded to MinIO."""
    large_data = {"records": [{"id": i, "data": "a" * 50} for i in range(50)]}
    raw_json_len = len(json.dumps(large_data).encode("utf-8"))
    assert raw_json_len > 1024

    mock_s3 = MagicMock()
    with patch("custom_xcom_backend._get_s3_client", return_value=mock_s3):
        serialized_uri_bytes = MinIOXComBackend.serialize_value(
            large_data,
            key="large_records",
            task_id="extract_task",
            dag_id="data_pipeline",
            run_id="manual__2026-10-30",
        )

        # Assert S3 put_object was invoked
        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == S3_BUCKET
        assert "data_pipeline" in call_kwargs["Key"]
        assert call_kwargs["ContentType"] == "application/json"

        # Assert returned value is S3 URI string pointer (serialized into bytes)
        assert isinstance(serialized_uri_bytes, bytes)
        uri_str = json.loads(serialized_uri_bytes.decode("utf-8"))
        assert isinstance(uri_str, str)
        assert uri_str.startswith(URI_PREFIX)


def test_deserialize_fetches_from_s3_when_uri_detected():
    """When XCom value is an s3:// URI, deserialize_value must fetch and unpack from S3."""
    expected_data = {"customer_id": 999, "score": 0.98}
    s3_uri = f"{URI_PREFIX}xcom/my_dag/run_1/task_a/return_value_-1.json"

    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(expected_data).encode("utf-8")
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {"Body": mock_body}

    with patch("custom_xcom_backend._get_s3_client", return_value=mock_s3):
        mock_xcom_record = MockXCom(value=s3_uri)
        unpacked = MinIOXComBackend.deserialize_value(mock_xcom_record)

        mock_s3.get_object.assert_called_once()
        assert unpacked == expected_data


def test_deserialize_passthrough_normal_values():
    """Non-S3 values pass through deserialize_value unchanged."""
    normal_data = {"key": "standard_postgres_value"}
    mock_xcom_record = MockXCom(value=normal_data)

    with patch("custom_xcom_backend._get_s3_client") as mock_get_s3:
        unpacked = MinIOXComBackend.deserialize_value(mock_xcom_record)
        mock_get_s3.assert_not_called()
        assert unpacked == normal_data
