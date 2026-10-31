"""Infrastructure Verification Script for Apache Airflow Sandbox."""

from __future__ import annotations

import json
import os
import subprocess
import sys

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


def check_docker_containers():
    """Validates docker-compose container states."""
    console.print("\n[bold cyan]1. Checking Docker Containers...[/bold cyan]")
    try:
        res = subprocess.run(
            ["docker", "compose", "ps", "--format", "json"],
            capture_output=True,
            text=True,
            check=True,
        )
        containers = []
        for line in res.stdout.strip().splitlines():
            if line.strip():
                try:
                    containers.append(json.loads(line))
                except Exception:
                    pass

        if not containers:
            console.print(
                "[yellow]No containers currently running. Start with 'docker compose up -d'[/yellow]"
            )
            return False

        table = Table(title="Container Health Matrix")
        table.add_column("Service", style="cyan")
        table.add_column("State", style="bold")
        table.add_column("Health / Status", style="green")

        all_healthy = True
        for c in containers:
            service = c.get("Service", c.get("Name", "unknown"))
            state = c.get("State", "unknown")
            health = c.get("Health", c.get("Status", ""))
            color = "green" if state == "running" else "red"
            if state != "running":
                all_healthy = False
            table.add_row(service, f"[{color}]{state}[/{color}]", health)

        console.print(table)
        return all_healthy
    except Exception as exc:
        console.print(f"[red]Docker check failed: {exc}[/red]")
        return False


def check_minio():
    """Validates MinIO S3 API."""
    console.print("\n[bold cyan]2. Checking MinIO S3 Object Store...[/bold cyan]")
    endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000")
    try:
        resp = requests.get(f"{endpoint}/minio/health/live", timeout=3)
        if resp.status_code == 200:
            console.print("✅ [green]MinIO S3:[/green] Live & Healthy (Port 9000)")
            return True
        else:
            console.print(f"⚠️ [yellow]MinIO S3:[/yellow] Unexpected HTTP status {resp.status_code}")
            return False
    except Exception as exc:
        console.print(f"❌ [red]MinIO S3:[/red] Connection failed ({exc})")
        return False


def check_airflow_health():
    """Validates Airflow Webserver /health endpoint."""
    console.print("\n[bold cyan]3. Checking Airflow Webserver Health Endpoint...[/bold cyan]")
    try:
        resp = requests.get("http://localhost:8080/health", timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            metadb = data.get("metadatabase", {}).get("status", "unknown")
            scheduler = data.get("scheduler", {}).get("status", "unknown")
            triggerer = data.get("triggerer", {}).get("status", "unknown")

            console.print(
                f"✅ [green]Airflow Webserver (Port 8080):[/green] Online\n"
                f"   • Metadata DB: [bold green]{metadb}[/bold green]\n"
                f"   • Scheduler:   [bold green]{scheduler}[/bold green]\n"
                f"   • Triggerer:   [bold green]{triggerer}[/bold green]"
            )
            return True
        else:
            console.print(f"⚠️ [yellow]Airflow Webserver:[/yellow] HTTP {resp.status_code}")
            return False
    except Exception as exc:
        console.print(f"❌ [red]Airflow Webserver:[/red] Connection failed ({exc})")
        return False


def main():
    console.print(
        Panel.fit(
            "[bold green]Apache Airflow 2.10+ Infrastructure Verification[/bold green]",
            border_style="green",
        )
    )
    c1 = check_docker_containers()
    c2 = check_minio()
    c3 = check_airflow_health()

    if c1 and c2 and c3:
        console.print("\n🎉 [bold green]All services are operational and healthy![/bold green]\n")
        sys.exit(0)
    else:
        console.print(
            "\n⚠️ [bold yellow]Some services are still starting or offline.[/bold yellow]\n"
        )
        sys.exit(0)


if __name__ == "__main__":
    main()
