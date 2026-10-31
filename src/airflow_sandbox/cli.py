"""CLI management tool for Apache Airflow Sandbox."""

from __future__ import annotations

import json
import subprocess
from typing import Optional

import requests
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from airflow_sandbox.config import settings

app = typer.Typer(
    name="airflow-lab",
    help="Production-Grade Apache Airflow Sandbox CLI for Lead Data Engineering Assessment",
    add_completion=False,
)
console = Console()


@app.command()
def info() -> None:
    """Display environment configuration, tuning parameters, and scenario index."""
    console.print(
        Panel.fit(
            "[bold cyan]Apache Airflow 2.10+ Production Sandbox[/bold cyan]\n"
            "[yellow]Lead Data Engineering Assessment Portfolio[/yellow]",
            border_style="cyan",
        )
    )

    tuning_table = Table(title="Airflow Deployment & Resource Configuration")
    tuning_table.add_column("Parameter", style="cyan", no_wrap=True)
    tuning_table.add_column("Configured Value", style="green")
    tuning_table.add_column("Production Impact / Rationale", style="dim")

    tuning_table.add_row(
        "AIRFLOW__CORE__EXECUTOR",
        "LocalExecutor",
        "Scale-up single node parallelism using multi-process worker pool without Celery/Redis overhead.",
    )
    tuning_table.add_row(
        "AIRFLOW__CORE__PARALLELISM",
        "32",
        "Maximum simultaneous task instances running across the entire Airflow instance.",
    )
    tuning_table.add_row(
        "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG",
        "16",
        "Prevents a single DAG run or mapping expansion from monopolizing all worker concurrency slots.",
    )
    tuning_table.add_row(
        "AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG",
        "16",
        "Controls backfill bursts and limits parallel DAG runs during catchup.",
    )
    tuning_table.add_row(
        "AIRFLOW__CORE__XCOM_BACKEND",
        "custom_xcom_backend.MinIOXComBackend",
        "Routes payloads >1KB to MinIO/S3 object storage, preventing Postgres DB bloat and IPC failure.",
    )

    console.print(tuning_table)

    scenarios_table = Table(title="Core Assessment Scenarios Implemented")
    scenarios_table.add_column("ID", style="bold magenta", width=6)
    scenarios_table.add_column("Scenario Name", style="bold green")
    scenarios_table.add_column("Key Operators / Patterns", style="cyan")
    scenarios_table.add_column("Targeted Assessment Gap", style="yellow")

    scenarios_table.add_row(
        "01",
        "Modern Operators & Dynamic Task Mapping",
        "@task, @task.branch, expand(), BashOperator, EmptyOperator",
        "Dynamic runtime fan-out without hardcoded task definitions.",
    )
    scenarios_table.add_row(
        "02",
        "Advanced Sensors & Resource Safety",
        "ExternalTaskSensor (mode='reschedule'), Deferrable Operators (Triggerer)",
        "Zero worker slot starvation during prolonged waits.",
    )
    scenarios_table.add_row(
        "03",
        "Retries, Backoff & Data Quality Gates",
        "retries=3, exponential backoff, on_failure_callback, quality gate",
        "Production resiliency, transient failure recovery, and alert routing.",
    )
    scenarios_table.add_row(
        "04",
        "Custom XCom Backend with S3 / MinIO",
        "MinIOXComBackend, serialize_value(), deserialize_value(), s3:// URI",
        "Safe passing of large DataFrames without metadata DB bloat.",
    )

    console.print(scenarios_table)


@app.command()
def verify() -> None:
    """Verify health of PostgreSQL metadata DB, MinIO storage, and Airflow services."""
    console.print("[bold blue]Checking Sandbox Infrastructure Services...[/bold blue]\n")

    # 1. Check Docker Containers
    try:
        ps_res = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            check=True,
        )
        containers = []
        for line in ps_res.stdout.strip().splitlines():
            if line.strip():
                try:
                    containers.append(json.loads(line))
                except Exception:
                    pass

        docker_table = Table(title="Docker Compose Service Health")
        docker_table.add_column("Service", style="cyan")
        docker_table.add_column("State", style="bold")
        docker_table.add_column("Status / Health", style="green")

        for c in containers:
            name = c.get("Service") or c.get("Name", "unknown")
            state = c.get("State", "unknown")
            health = c.get("Health", c.get("Status", ""))
            color = "green" if state == "running" else "red"
            docker_table.add_row(name, f"[{color}]{state}[/{color}]", health)

        console.print(docker_table)
    except Exception as exc:
        console.print(f"[yellow]Docker compose status check skipped/failed: {exc}[/yellow]")

    # 2. Check MinIO S3 API
    try:
        minio_url = f"{settings.minio_endpoint}/minio/health/live"
        resp = requests.get(minio_url, timeout=3)
        if resp.status_code == 200:
            console.print("✅ [green]MinIO S3 Service:[/green] Healthy (endpoint reachable)")
        else:
            console.print(f"⚠️ [yellow]MinIO S3 Service:[/yellow] HTTP status {resp.status_code}")
    except Exception as exc:
        console.print(f"❌ [red]MinIO S3 Service:[/red] Unreachable ({exc})")

    # 3. Check Airflow Webserver Health Endpoint
    try:
        web_url = f"http://localhost:{settings.airflow_webserver_port}/health"
        resp = requests.get(web_url, timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            metadb = data.get("metadatabase", {}).get("status", "unknown")
            scheduler = data.get("scheduler", {}).get("status", "unknown")
            triggerer = data.get("triggerer", {}).get("status", "unknown")
            console.print(
                f"✅ [green]Airflow Webserver API:[/green] Healthy\n"
                f"   - Metadata DB: [bold]{metadb}[/bold]\n"
                f"   - Scheduler: [bold]{scheduler}[/bold]\n"
                f"   - Triggerer: [bold]{triggerer}[/bold]"
            )
        else:
            console.print(f"⚠️ [yellow]Airflow Webserver:[/yellow] Status {resp.status_code}")
    except Exception as exc:
        console.print(f"❌ [red]Airflow Webserver:[/red] Unreachable ({exc})")


@app.command()
def trigger(
    dag_id: str = typer.Argument(..., help="DAG ID to trigger"),
    conf: Optional[str] = typer.Option(None, "--conf", "-c", help="JSON configuration payload"),
) -> None:
    """Trigger a DAG run using the Airflow REST API."""
    url = f"http://localhost:{settings.airflow_webserver_port}/api/v1/dags/{dag_id}/dagRuns"
    payload = {"conf": json.loads(conf)} if conf else {}

    auth = (settings.airflow_admin_user, settings.airflow_admin_password)
    try:
        console.print(f"[cyan]Triggering DAG '{dag_id}'...[/cyan]")
        resp = requests.post(url, json=payload, auth=auth, timeout=10)
        if resp.status_code in (200, 201):
            run_data = resp.json()
            console.print(
                f"🚀 [bold green]DAG Run Created Successfully![/bold green]\n"
                f"   DAG ID: [cyan]{run_data.get('dag_id')}[/cyan]\n"
                f"   Run ID: [yellow]{run_data.get('dag_run_id')}[/yellow]\n"
                f"   State:  [green]{run_data.get('state')}[/green]\n"
                f"   Logical Date: {run_data.get('logical_date')}"
            )
        else:
            console.print(
                f"[red]Failed to trigger DAG (HTTP {resp.status_code}): {resp.text}[/red]"
            )
    except Exception as exc:
        console.print(f"[red]Error triggering DAG: {exc}[/red]")


if __name__ == "__main__":
    app()
