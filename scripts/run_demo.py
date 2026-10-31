"""Demonstration Runner for Apache Airflow Scenarios."""

from __future__ import annotations

import os
import time

import requests
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

console = Console()

AIRFLOW_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
AUTH = (
    os.getenv("AIRFLOW_WWW_USER_USERNAME", "airflow"),
    os.getenv("AIRFLOW_WWW_USER_PASSWORD", "airflow"),
)


def trigger_dag(dag_id: str):
    """Triggers a DAG run via Airflow REST API."""
    url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"
    try:
        resp = requests.post(url, json={}, auth=AUTH, timeout=10)
        if resp.status_code in (200, 201):
            data = resp.json()
            console.print(
                f"🚀 [bold green]Triggered {dag_id}:[/bold green] "
                f"Run ID: [yellow]{data.get('dag_run_id')}[/yellow] "
                f"(State: [cyan]{data.get('state')}[/cyan])"
            )
            return data.get("dag_run_id")
        else:
            console.print(
                f"[red]Failed to trigger {dag_id} (HTTP {resp.status_code}): {resp.text}[/red]"
            )
            return None
    except Exception as exc:
        console.print(f"[red]Error contacting Airflow API: {exc}[/red]")
        return None


def main():
    console.print(
        Panel.fit(
            "[bold cyan]Apache Airflow 2.10+ Scenario Demonstration Runner[/bold cyan]",
            border_style="cyan",
        )
    )

    scenarios = [
        ("01_modern_operators_dynamic_mapping", "Modern TaskFlow API & Dynamic Mapping expand()"),
        (
            "02_advanced_sensors_reschedule_deferrable",
            "Reschedule Sensors & Triggerer Async Deferral",
        ),
        ("03_retries_backoff_callbacks_gate", "Exponential Backoff & Data Quality Gate"),
        ("04_custom_xcom_backend_minio", "Custom MinIO S3 XCom Offloading vs DB Bloat"),
    ]

    table = Table(title="Demonstration Scenarios")
    table.add_column("DAG ID", style="green")
    table.add_column("Scenario Description", style="yellow")

    for s_id, s_desc in scenarios:
        table.add_row(s_id, s_desc)
    console.print(table)

    for s_id, s_desc in scenarios:
        console.print(f"\n[bold]Dispatching Scenario:[/bold] [cyan]{s_desc}[/cyan]")
        trigger_dag(s_id)
        time.sleep(1)


if __name__ == "__main__":
    main()
