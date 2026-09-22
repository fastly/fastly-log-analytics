# Background Job Specification: `metric_snapshot`

> [!NOTE]
> **Status: VERIFIED & OPERATIONAL (Observability & Cron Automation Audit)**
> Automated test suites verified: `tests/cron/test_metric_snapshot.py` (3 tests passing: operational vitals sampling across pool wait / queries / admission / celery, terminal duration extraction including status 'warning', and resilient error recovery).

---

## 1. Overview & Objectives
- **Job Identifier:** `metric_snapshot`
- **Category:** Host, Runtime & Connection Pool Observability
- **Purpose:** Samples container-level and host-level operational vitals every 60 seconds (CPU 1-minute load, resident memory %, disk usage %, DuckDB connection pool wait times, cron durations, ingest lag, active query count, DuckLake admission times, Celery queue depths, Celery worker counts, and ingest ledger status tallies).
- **Storage Target:** Stored into SQLite `data/system/system_metrics.db` (standard mode) or shared PostgreSQL `metric_snapshots` (multi-pod high-throughput mode) to drive the System Health sparklines and Trends tab (`/admin/trends`).
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
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Samples OS vitals via `os` / `/proc` / `shutil`, probes DuckDB pool status, writes to SQLite `data/system/system_metrics.db`. | Local SQLite write lock. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Samples pod-local runtime vitals and connection pools, queries PostgreSQL ledger and Celery broker queues; writes to PostgreSQL `metric_snapshots`. | PostgreSQL connection pool. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `GET /api/admin/metric-history/batch` | Trends and System Health in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Monitors local analyst process health. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | System-wide daemon on host server. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Connection Pool & Admission Probing:**
   - Probes `duckdb_pool.get_all_stats()` for `pool_wait_p95_ms` per service.
   - Probes `get_admission_stats()` for DuckLake lock wait and hold times (`ducklake_admission_wait_avg_ms`, `ducklake_admission_hold_avg_ms`, `ducklake_admission_timeouts`).
2. **Cron Duration & Ingest Lag Probing:**
   - Probes latest terminal cron runs per task for `status IN ('success', 'warning', 'error')` and records `cron_duration_ms`.
   - Probes `get_latest_ingest_ts(service_id)` to calculate `ingest_lag_s`.
3. **Query Engine & Queue Probing:**
   - Queries `query_registry.summary()` for `active_query_count`.
   - Probes Celery queue depths (`celery_queue_depth_{queue}`, `celery_broker_reachable`) and inspects active workers and tasks.
   - In high-throughput mode: probes PostgreSQL `ingest_ledger_summary()` for ledger status counts.
4. **OS Metric Probing:**
   - Reads 1-minute load average via `os.getloadavg()`.
   - Reads memory used percentage via `/proc/meminfo` (Linux).
   - Probes disk utilization for data mount (`/app/data`) and root (`/`).
5. **Metric Storage & Summarization:**
   - Every metric is recorded via `_safe_record` to `metric_snapshots` (SQLite or PostgreSQL) and registered with `operational_metrics`.
   - Returns a structured execution detail summary (e.g. `sampled=14 metrics`) recorded in `record_job_run`.
6. **Historical Pruning (Bounded Retention):**
   - Retention is 30 days, purged daily during `metadata_cleanup` via `metric_snapshots.purge_old(retention_days=30)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local system probe; zero cloud storage API calls.
  - **SQLite Operations:** Every snapshot write executes in WAL mode with fast composite index lookups (`idx_metric_lookup`).
  - **Overhead Budget:** Total execution runtime must remain < 100ms per tick.
- **Timing & Resource Budgets:**
  - Sampling duration: < 50ms.
  - SQLite/Postgres batch insert duration: < 15ms.

---

## 7. Failure Modes & Recovery Runbooks
- **SQLite Contention:** WAL mode handles readers without blocking sampler writer.
- **Probe Failure Isolation:** `_safe_record` wraps each metric sample individually so a failure in one probe does not abort remaining vitals.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Inspect Trends:** `GET /api/admin/metric-history/batch?since=1h`.
- **System Vitals:** `GET /api/admin/system/vitals`.

---

## 9. AI Session Automated Verification Checklist
- [x] 1. Automated unit tests verified in `tests/cron/test_metric_snapshot.py`.
- [x] 2. Verified operational vitals sampling across pool wait, active queries, celery queues, and ledger rows.
- [x] 3. Verified `cron_duration_ms` captures runs with `status='warning'` alongside `'success'` and `'error'`.
- [x] 4. Verified `_safe_record` handles exceptions gracefully without interrupting execution.
- [x] 5. Verified zero FOS Class A/B calls.
