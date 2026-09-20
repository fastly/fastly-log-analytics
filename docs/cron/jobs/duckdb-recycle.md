> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `duckdb_recycle` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `duckdb_recycle`

## 1. Overview & Objectives
- **Job Identifier:** `duckdb_recycle`
- **Category:** Memory Leak Prevention & Connection Lifecycle Management
- **Purpose:** Periodically drains and re-instantiates idle DuckDB database connections in the connection pool to reclaim resident memory accumulated by DuckDB's C++ Parquet metadata caching (`enable_object_cache`).
- **Why It Runs:** DuckDB's internal C++ object cache retains Parquet footer metadata across queries to accelerate subsequent scans. Over extended multi-week operational runs with millions of ingested files, this internal metadata cache can cause process resident memory (RSS) to grow continuously (500MB to > 4GB). Recycling connections drops the internal C++ caches and releases memory back to the operating system without service downtime or dropped in-flight queries.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Evaluated every `DUCKDB_RECYCLE_INTERVAL_MIN` minutes (default: 60 minutes).
- **Scope:** Process-global singleton job.
- **Activation Gate:** Registered **ONLY** if `DUCKDB_RECYCLE_INTERVAL_MIN > 0`. If set to 0 or negative, the job is disabled.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=120s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Cycles idle connections in `backend.core.duckdb_pool`; forces memory garbage collection. | Connection pool lock. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Cycles web serving pool connections; never scheduled to Celery workers. | Local pool lock. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/duckdb/recycle` | Memory and pool statistics in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Keeps analyst process memory bounded. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side memory management daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Gate Check:** Confirms `recycle_interval_min() > 0`.
2. **Pool Lock & Drain:**
   - Acquires the global DuckDB pool lock.
   - Identifies idle connections across all service connection pools.
   - Safely closes idle DuckDB connection handles (`con.close()`).
   - Active connections currently executing user queries are marked for recycling upon return to the pool (graceful drain; never forcibly terminates running queries).
3. **Explicit Memory Reclamation:**
   - Triggers `gc.collect()` in Python.
   - Where supported by the C allocator (glibc / jemalloc), issues memory trim calls (`malloc_trim(0)`).
4. **Pool Re-Instantiation:**
   - Pre-allocates fresh base connections to avoid latency spikes on subsequent incoming requests.
5. **Telemetry & Log Recording:**
   - Records memory RSS before and after recycling: `rss_before_mb`, `rss_after_mb`, `freed_mb`.
   - Logs execution summary in `cron_runs`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local memory and connection pool management; zero cloud API calls.
  - **Memory Metrics:** Resident memory RSS delta must be explicitly logged.
  - **Thread Wait Time:** Pool acquisition wait time (`app.thread_wait_ms`) during recycling must remain < 50ms.
- **Timing & Resource Budgets:**
  - Recycle execution: < 100ms.
  - Memory freed: Typically 100MB–1.5GB on busy services.
- **Audit Checklist:**
  - Confirm active user queries are NOT terminated or interrupted during recycling.
  - Verify fresh pool connections are healthy and accept incoming queries immediately.

---

## 7. Failure Modes & Recovery Runbooks
- **Re-Open Failure:** If opening a fresh DuckDB connection fails, logs error and falls back to lazy creation on next request.
- **Lock Contention:** Yields immediately if pool is under extreme burst demand.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Recycle:** `POST /api/admin/duckdb/recycle`.
- **Inspect Pool Status:** `GET /api/admin/system/pool-status`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/duckdb/recycle`; confirm HTTP 200.
- [ ] 2. Verify in logs: idle DuckDB connections closed and re-instantiated.
- [ ] 3. Verify in `cron_runs`: status `success` with recorded `rss_before_mb` and `rss_after_mb`.
- [ ] 4. Immediately execute an analytical query on `/api/dashboard/bundle`; confirm query executes successfully.
- [ ] 5. Under `FLA_DEV_NO_CRONS=1`, verify `duckdb_recycle` is registered and functions normally.
