# Apache Airflow 2.10+ Operational Runbook & Production Tuning Guide

This document provides definitive guidance for Lead Data Engineers operating, tuning, and troubleshooting Apache Airflow clusters at scale.

---

## 1. Concurrency Tuning & Resource Allocation

Airflow provides three hierarchical concurrency throttles. Misconfiguring these parameters leads to worker starvation, database lock contention, or scheduler stalls.

```
                    ┌────────────────────────────────────────────────────────┐
                    │               AIRFLOW__CORE__PARALLELISM               │
                    │               (Cluster-wide Task Limit)                │
                    │                   e.g., 32 slots                       │
                    └───────────────────────────┬────────────────────────────┘
                                                │
                 ┌──────────────────────────────┴──────────────────────────────┐
                 ▼                                                             ▼
┌─────────────────────────────────┐                           ┌─────────────────────────────────┐
│ AIRFLOW__CORE__MAX_ACTIVE_RUNS  │                           │ AIRFLOW__CORE__MAX_ACTIVE_RUNS  │
│          (DAG A = 16)           │                           │          (DAG B = 16)           │
└────────────────┬────────────────┘                           └────────────────┬────────────────┘
                 │                                                             │
                 ▼                                                             ▼
┌─────────────────────────────────┐                           ┌─────────────────────────────────┐
│ AIRFLOW__CORE__MAX_ACTIVE_TASKS │                           │ AIRFLOW__CORE__MAX_ACTIVE_TASKS │
│          (DAG A = 16)           │                           │          (DAG B = 16)           │
└─────────────────────────────────┘                           └─────────────────────────────────┘
```

### Parameter Reference & Formulas

| Environment Variable | Config Key | Default | Recommended Sandbox | Production Formula / Rationale |
| :--- | :--- | :--- | :--- | :--- |
| `AIRFLOW__CORE__PARALLELISM` | `[core] parallelism` | 32 | **32** | $\text{Parallelism} \le \text{Total CPU Cores} \times 4$. Sets the absolute ceiling of task instances executing simultaneously across the entire cluster. |
| `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG` | `[core] max_active_tasks_per_dag` | 16 | **16** | Formerly `dag_concurrency`. Prevents a single dynamic task mapping (`expand()`) DAG from monopolizing all cluster slots. Set $\le \frac{\text{Parallelism}}{2}$. |
| `AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG` | `[core] max_active_runs_per_dag` | 16 | **16** | Limits the number of concurrent execution dates running for a single DAG. Vital when backfilling historical data. |
| `AIRFLOW__CORE__DEFAULT_POOL_TASK_SLOT_COUNT` | `[core] default_pool_task_slot_count` | 128 | **32** | Governs the default task slot capacity in the scheduler pool. |

---

## 2. Scheduler & Zombie Task Management

### What is a Zombie Task?
A **Zombie Task** is a task instance that Airflow believes is running (`state="running"` in the metadata DB), but its corresponding process or container has died without updating the task state (e.g., due to OOM kill, node reboot, or SIGKILL).

### Detection & Heartbeat Settings:
```ini
[scheduler]
# Seconds before an unresponsive job heartbeat is marked a zombie
scheduler_zombie_task_threshold = 300

# How frequently scheduler sweeps for zombie tasks
zombie_detection_interval = 10.0

# Heartbeat interval emitted by running tasks to PostgreSQL
job_heartbeat_sec = 5
```

### Recovery Drill:
1. **Identify the zombie:** Check scheduler logs:
   ```bash
   docker compose logs airflow-scheduler | grep "Detected zombie job"
   ```
2. **Inspect OOM status:** If a task vanished, inspect Docker daemon kernel logs:
   ```bash
   docker inspect airflow-scheduler --format '{{.State.OOMKilled}}'
   ```
3. **Remediation:** Increase container memory limits or optimize the Python memory footprint (e.g. streaming chunks instead of loading entire tables into RAM).

---

## 3. Sensor Resource Safety: Poke vs Reschedule

### The Failure Mode: Sensor Starvation
Consider a cluster with `parallelism = 32`. A pipeline schedules 32 `ExternalTaskSensor` tasks using `mode="poke"` with `poke_interval = 60` waiting for upstream files.
- Each sensor occupies a full OS process / worker slot.
- During the 60-second sleep interval, the slot is locked and idle.
- **Result:** Concurrency is completely exhausted (32/32 slots occupied). Zero ETL tasks can run, causing a total cluster deadlock!

### The Solution:
Always enforce `mode="reschedule"` or migrate to **Deferrable Operators** via `airflow-triggerer`:
```python
# PROD PATTERN 1: Reschedule mode (frees slot between checks)
ExternalTaskSensor(
    task_id="wait_for_upstream",
    external_dag_id="upstream_pipeline",
    mode="reschedule",  # Relinquishes worker slot to the pool!
    poke_interval=30,
    timeout=600,
    soft_fail=True,
)

# PROD PATTERN 2: Deferrable Operator (Airflow Triggerer)
# Uses asyncio event loop. Consumes 0 worker slots!
TimeDeltaSensorAsync(
    task_id="async_window_sensor",
    delta=timedelta(minutes=5),
)
```

---

## 4. Connection & Secrets Management

In production, never store plaintext passwords in DAG code or `.env` files committed to source control.

### Option A: Environment Variable Secret Pattern
Airflow natively reads connections from environment variables with the prefix `AIRFLOW_CONN_<CONN_ID_UPPERCASE>`:
```bash
export AIRFLOW_CONN_AWS_MINIO='aws://minioadmin:minioadmin@?endpoint_url=http%3A%2F%2Fminio%3A9000'
```

### Option B: HashiCorp Vault / AWS Secrets Manager Backend
Configure Airflow's pluggable secrets backend in `airflow.cfg`:
```ini
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}
```

---

## 5. Log Diagnosis & Triage Cheatsheet

### 1. View Live Airflow Service Logs
```bash
# Webserver logs
docker compose logs -f airflow-webserver

# Scheduler loop & DAG parsing logs
docker compose logs -f airflow-scheduler

# Triggerer event loop logs
docker compose logs -f airflow-triggerer
```

### 2. Inspect Metadata DB Lock Contention
If the scheduler slows down, query PostgreSQL active connections:
```bash
docker compose exec postgres psql -U airflow -d airflow -c "
SELECT pid, age(clock_timestamp(), query_start), usename, query, state
FROM pg_stat_activity
WHERE state != 'idle' AND query NOT ILIKE '%pg_stat_activity%'
ORDER BY query_start ASC;
"
```

### 3. Clear Failed Tasks & Re-run via CLI
```bash
# Inside airflow-scheduler container:
airflow tasks clear 03_retries_backoff_callbacks_gate \
  --task-regex "ingest_.*" \
  --start-date 2026-10-30 \
  --yes
```
