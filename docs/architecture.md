# Apache Airflow 2.10+ Production Sandbox Architecture

[![Apache Airflow 2.10+](https://img.shields.io/badge/Apache%20Airflow-2.10%2B-blue.svg)](https://airflow.apache.org/)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![MinIO S3](https://img.shields.io/badge/Storage-MinIO%20S3-red.svg)](https://min.io/)
[![Python 3.13](https://img.shields.io/badge/Python-3.13-blue.svg)](https://www.python.org/)

---

## 1. System Architecture & Component Inventory

This repository implements an enterprise-grade Apache Airflow 2.10+ deployment engineered for production reliability, horizontal scalability, and zero-starvation scheduling.

```mermaid
flowchart TD
    subgraph Client ["Client & Developer Interfaces"]
        UI["Airflow Webserver UI<br/>(Port 8080)"]
        CLI["Airflow Sandbox CLI<br/>(airflow-lab)"]
        API["Airflow REST API v1"]
    end

    subgraph OrchestrationControlPlane ["Airflow Control Plane"]
        SCHED["Airflow Scheduler<br/>• DAG Parsing Loop<br/>• Dependency Graph Engine<br/>• Task Instance State Machine"]
        TRIG["Airflow Triggerer<br/>• Asynchronous asyncio Event Loop<br/>• Deferrable Operator Listener"]
    end

    subgraph ExecutionPlane ["Execution Plane (LocalExecutor)"]
        WORKER_POOL["Local Multi-Process Worker Pool<br/>(parallelism=32, max_active_tasks_per_dag=16)"]
        T_TASKFLOW["TaskFlow & Dynamic Mapped Tasks<br/>(expand() / partial())"]
        T_RESCHEDULE["Reschedule Sensors<br/>(UP_FOR_RESCHEDULE)"]
        T_RESILIENT["Resilient Tasks<br/>(Exponential Backoff & Quality Gate)"]
    end

    subgraph StateAndStorage ["Persistence & Object Storage"]
        PG[("PostgreSQL 16 Metadata DB<br/>• DAG Runs & Task Instances<br/>• XCom String URI Pointers<br/>• Connection Secrets & Variables")]
        MINIO[("MinIO S3 Object Storage<br/>(Bucket: airflow-xcom-bucket)<br/>• Offloaded Large XCom Payloads<br/>• Raw Ingestion Buffers")]
    end

    UI -->|Session Auth| PG
    CLI -->|REST API Calls| API
    API -->|Write Task Requests| PG

    SCHED <-->|Poll State & Acquire Locks| PG
    SCHED -->|Spawn Task Processes| WORKER_POOL

    WORKER_POOL --> T_TASKFLOW
    WORKER_POOL --> T_RESCHEDULE
    WORKER_POOL --> T_RESILIENT

    %% Deferral Flow
    T_RESCHEDULE -.->|Yield TaskDeferred Exception| TRIG
    TRIG <-->|Monitor Trigger Events| PG
    TRIG -.->|Trigger Fired: Re-enqueue Task| SCHED

    %% Custom XCom Backend Flow
    T_TASKFLOW ==>|Payload > 1KB: MinIOXComBackend| MINIO
    T_TASKFLOW -->|Store s3:// URI Pointer| PG
    T_RESILIENT ==>|Data Quality Audit| PG
```

---

## 2. Component Inventory

| Component | Technology | Role & Deployment Responsibility |
| :--- | :--- | :--- |
| **Airflow Scheduler** | Python 3.13 / Airflow 2.10.5 | Parses DAG files, evaluates trigger rules, schedules queued tasks, detects zombie tasks. |
| **Airflow Webserver** | Flask AppBuilder / Gunicorn | Renders workflow DAG graphs, tree views, task logs, and exposes REST API v1. |
| **Airflow Triggerer** | Asyncio Python Daemon | Handles deferrable operators; runs thousands of asynchronous triggers with 0 worker slots. |
| **LocalExecutor** | Multi-Process Worker Pool | Executes task instances as distinct child processes of the scheduler up to `parallelism` limit. |
| **PostgreSQL 16** | Relational Database (ACID) | Metadata store for DAG states, task instance execution records, XCom pointers, and locks. |
| **MinIO S3** | S3-Compatible Object Store | Primary sink for Custom XCom Backend; eliminates Postgres DB bloat for DataFrames and large JSONs. |

---

## 3. Executor Architecture & Tradeoff Matrix

Choosing the correct executor is a pivotal architectural decision for Lead Data Engineers:

| Dimension | `SequentialExecutor` | `LocalExecutor` (This Sandbox) | `CeleryExecutor` | `KubernetesExecutor` |
| :--- | :--- | :--- | :--- | :--- |
| **Concurrency** | 1 (Single-process) | 32+ (Multi-process on single node) | Thousands (Distributed worker cluster) | Tens of thousands (Dynamic Pods) |
| **Infrastructure Overhead** | None (SQLite) | Minimal (Postgres only) | High (Postgres + Redis/RabbitMQ + Flower) | High (Kubernetes cluster + K8s API) |
| **Resource Isolation** | None | Process-level (Shared node RAM/CPU) | Node-level (Dedicated worker nodes) | Container-level (Pod CPU/Memory limits) |
| **Startup Latency** | 0s | ~100ms (Process fork) | ~200ms (Queue consumption) | 5s–30s (Pod scheduling & image pull) |
| **Ideal Production Use Case** | Testing & local debug | Single large VM (up to 64 cores, 256GB RAM) | High-volume homogenous task workflows | Heterogeneous tasks with disparate dependencies & GPU needs |

---

## 4. Custom XCom Architecture (Zero-Bloat S3 Offloading)

Standard Airflow XCom writes all task return values directly into PostgreSQL:
```
PostgreSQL Table: xcom
Columns: (dag_id, task_id, run_id, execution_date, key, value BYTEA)
```
When a task returns a 50MB DataFrame, PostgreSQL writes 50MB into the `xcom` table AND 50MB into Write-Ahead Logs (WAL). At 1,000 runs/day, this introduces **100 GB/day of WAL churn**, bloating database indexes, exhausting disk IOPS, and freezing the Airflow Scheduler during garbage collection (`vacuum`).

### The Solution: `MinIOXComBackend`
```mermaid
sequenceDiagram
    autonumber
    participant TaskA as Upstream Task (@task)
    participant Backend as MinIOXComBackend
    participant MinIO as MinIO Object Storage
    participant Postgres as PostgreSQL Metadata DB
    participant TaskB as Downstream Task (@task)

    TaskA->>Backend: return {"batch_id": "9001", "records": [...5000 items...]}
    Backend->>Backend: Evaluate size (15,200 bytes > 1,024 byte threshold)
    Backend->>MinIO: put_object(Bucket='airflow-xcom-bucket', Key='xcom/dag/run/task/key.json')
    MinIO-->>Backend: HTTP 200 OK
    Backend->>Postgres: BaseXCom.serialize_value("s3://airflow-xcom-bucket/xcom/...")
    Note over Postgres: Postgres stores tiny 64-byte URI string!

    TaskB->>Backend: xcom_pull(task_ids='upstream_task')
    Backend->>Postgres: Fetch raw XCom value
    Postgres-->>Backend: "s3://airflow-xcom-bucket/xcom/..."
    Backend->>Backend: Detect URI starts with 's3://'
    Backend->>MinIO: get_object(Bucket, Key)
    MinIO-->>Backend: Return raw JSON bytes
    Backend->>TaskB: Reconstructed Python dict / DataFrame transparently
```
