# Background Job Specification: `metric_snapshot`

---

## 1. Overview & Objectives
- **Job Identifier:** `metric_snapshot`
- **Category:** Host, Runtime & Connection Pool Observability
- **Purpose:** Samples container-level and host-level operational vitals every 60 seconds (CPU 1-minute load, resident memory %, disk usage %, DuckDB connection pool wait times, DuckDB database file size, cron durations, ingest lag, active query count, DuckLake admission times, Celery queue depths, Celery worker counts, and ingest ledger status tallies).
- **Storage Target:** Unified PostgreSQL 16 `metric_snapshots` table (per ADR-22) to drive the System Health sparklines, live SSE stream, and Trends tab (`/admin/trends`).
- **Why It Runs:** Real-time visibility into internal application health is essential for diagnosing query bottlenecks, memory growth, and disk exhaustion. Without high-frequency sampling, resource contention or DuckDB connection starvation cannot be correlated with analytical query loads.

---

## 2. Scheduling & Cadence
- **Trigger Type:** High-frequency interval timer (`interval`)
- **Default Schedule:** Every 60 seconds (`seconds=60`).
- **Scope:** Process-global singleton job (runs once on the web backend pod).
- **Jitter & Misfire Policy:**
  - Jitter: 5 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Samples OS vitals via `os` / `/proc` / `shutil`, probes DuckDB pool status and per-service DuckDB file size, writes to PostgreSQL `metric_snapshots`. | PostgreSQL connection pool. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_scale`)** | Pod APScheduler (Web Pod Only) | Samples pod-local runtime vitals and connection pools, queries PostgreSQL ledger and Celery broker queues; writes to PostgreSQL `metric_snapshots`. | PostgreSQL connection pool. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `GET /api/admin/metric-history/batch` | Trends and System Health in Admin UI. |
| **Analyst (Remote Share)** | N/A | Blocked (403) | System-wide daemon on host server. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Connection Pool & Admission Probing:**
   - Probes `duckdb_pool.get_all_stats()` for `pool_wait_p95_ms` per service.
   - Probes `get_admission_stats()` for DuckLake lock wait and hold times (`ducklake_admission_wait_avg_ms`, `ducklake_admission_hold_avg_ms`, `ducklake_admission_timeouts`).
2. **DuckDB Database Size Probing:**
   - Probes per-service DuckDB database file size via `_sample_duckdb_size(service_id)` (`data/services/{service_id}.duckdb`) using direct `os.stat` (O(1) lookup without directory walk) and records `duckdb_file_bytes`.
3. **Cron Duration & Ingest Lag Probing:**
   - Probes latest terminal cron runs per task for `status IN ('success', 'warning', 'error')` and records `cron_duration_ms`.
   - Probes `get_latest_ingest_ts(service_id)` to calculate `ingest_lag_s`.
4. **Query Engine & Queue Probing:**
   - Queries `query_registry.summary()` for `active_query_count`.
   - Probes Celery queue depths (`celery_queue_depth_{queue}`, `celery_broker_reachable`) and inspects active workers and tasks with bounded `inspect(timeout=0.5)` to avoid network broadcast hangs.
   - In high-scale mode: probes PostgreSQL `ingest_ledger_summary()` for ledger status counts.
5. **OS Metric Probing:**
   - Reads 1-minute load average via `os.getloadavg()`.
   - Reads memory used percentage via `/proc/meminfo` (Linux).
   - Probes disk utilization for data mount (`/app/data`) and root (`/`).
6. **Metric Storage & Summarization:**
   - Every metric is recorded via `_safe_record` to PostgreSQL `metric_snapshots` (`backend.core.metric_snapshots.record()`) and registered with `operational_metrics`.
   - Returns a structured execution detail summary (e.g. `sampled=15 metrics`) recorded in `record_job_run`.
7. **Historical Pruning (Bounded Retention):**
   - Retention is 30 days, purged daily during `metadata_cleanup` via `backend.core.metric_snapshots.purge_old(retention_days=30)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local system probe and PostgreSQL write; zero cloud storage API calls.
  - **PostgreSQL Operations:** Every snapshot write executes via pooled connections (`psycopg_pool`) with primary key `(metric, service_id, task, ts)` and fast composite index lookups (`idx_metric_lookup`).
  - **Overhead Budget:** Total execution runtime must remain < 100ms per tick.
- **Timing & Resource Budgets:**
  - Sampling duration: < 50ms.
  - Postgres batch insert duration: < 15ms.

---

## 7. Failure Modes & Recovery Runbooks
- **PostgreSQL Connection Starvation:** Handled by pooled connection reuse and short autocommit transactions.
- **Celery Broker/Worker Hang:** Celery inspect calls are bounded by `timeout=0.5` seconds to prevent blocking the sampler thread if workers are partitioned or down.
- **Probe Failure Isolation:** `_safe_record` wraps each metric sample individually so a failure in one probe does not abort remaining vitals.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **System Health Snapshot:** `GET /api/admin/health-snapshot` (returns real-time system vitals, pool metrics, and latest snapshot values).
- **Inspect Trends (Batch):** `GET /api/admin/metric-history/batch?since=1h`.
- **Real-time Metrics SSE:** `GET /api/admin/events/stream?channels=system_metrics`.

---

## 9. AI Session Verification Checklist
- 2. Confirm operational vitals sampling across pool wait, active queries, celery queues, and ledger rows.
- 3. Confirm `duckdb_file_bytes` sampling per configured service without directory walks.
- 4. Confirm Celery worker inspection uses bounded `timeout=0.5` to prevent broadcast hangs.
- 5. Confirm `metric_snapshot` registration in dev-local-safe mode (`FLA_DEV_NO_CRONS=1`).
- 6. Confirm `cron_duration_ms` captures runs with `status='warning'` alongside `'success'` and `'error'`.
- 7. Confirm `_safe_record` handles exceptions gracefully without interrupting execution.
- 8. Confirm zero FOS Class A/B calls.
