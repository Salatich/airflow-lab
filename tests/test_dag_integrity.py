"""Unit tests verifying Airflow DAG integrity, syntax, and absence of cycles."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.utils.dag_cycle_tester import check_cycle

DAGS_FOLDER = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="session")
def dag_bag():
    """Initializes DagBag pointing to the sandbox DAGs directory."""
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
    return DagBag(dag_folder=str(DAGS_FOLDER), include_examples=False)


def test_dags_load_without_import_errors(dag_bag: DagBag):
    """Asserts that no DAG files throw syntax, import, or parsing errors."""
    import_errors = dag_bag.import_errors
    assert not import_errors, f"DAG import errors detected: {import_errors}"


def test_all_expected_scenarios_present(dag_bag: DagBag):
    """Asserts that all 4 required assessment scenario DAGs are present."""
    expected_dag_ids = {
        "01_modern_operators_dynamic_mapping",
        "02_advanced_sensors_reschedule_deferrable",
        "03_retries_backoff_callbacks_gate",
        "04_custom_xcom_backend_minio",
    }
    loaded_dag_ids = set(dag_bag.dag_ids)
    assert expected_dag_ids.issubset(loaded_dag_ids), (
        f"Missing DAGs! Expected {expected_dag_ids}, but found {loaded_dag_ids}"
    )


def test_dags_have_no_cycles(dag_bag: DagBag):
    """Asserts that none of the DAGs contain cyclic dependencies."""
    for dag_id, dag in dag_bag.dags.items():
        check_cycle(dag)


def test_dynamic_task_mapping_dag_structure(dag_bag: DagBag):
    """Validates Scenario 1 task structure and dynamic mapping expand call."""
    dag = dag_bag.dags.get("01_modern_operators_dynamic_mapping")
    assert dag is not None
    assert "process_partition" in dag.task_dict
    assert "discover_partitions" in dag.task_dict
    assert "evaluate_data_volume" in dag.task_dict


def test_reschedule_sensor_dag_mode(dag_bag: DagBag):
    """Validates Scenario 2 sensor uses mode='reschedule' to prevent worker starvation."""
    dag = dag_bag.dags.get("02_advanced_sensors_reschedule_deferrable")
    assert dag is not None
    sensor_task = dag.get_task("wait_for_upstream_ingestion")
    assert sensor_task.mode == "reschedule", "ExternalTaskSensor MUST use mode='reschedule'!"


def test_retry_policy_configuration(dag_bag: DagBag):
    """Validates Scenario 3 enterprise retry policies and backoff."""
    dag = dag_bag.dags.get("03_retries_backoff_callbacks_gate")
    assert dag is not None
    task = dag.get_task("ingest_external_partner_data")
    assert task.retries == 3
    assert task.retry_exponential_backoff is True
    assert task.on_failure_callback is not None
