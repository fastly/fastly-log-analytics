# Background Job Specification: `duckdb_recycle`

## 1. Overview & Objectives
- **Job Identifier:** `duckdb_recycle`
- **Category:** Memory Leak Prevention & Connection Lifecycle Management
- **Status:** Verified (Automated Unit Tests & Lifecycle Audited)
- **Purpose:** Periodically drains and re-instantiates idle DuckDB database connections in the connection pool to reclaim resident memory accumulated by DuckDB's C++ Parquet metadata caching (`enable_object_cache`).
- **Why It Runs:** DuckDB's internal C++ object cache retains Parquet footer metadata across queries to accelerate subsequent scans. Over extended multi-week operational runs with millions of ingested files, this internal metadata cache can cause process resident memory (RSS) to grow continuously (500MB to > 4GB). Recycling connections drops the internal C++ caches and releases memory back to the operating system without service downtime or dropped in-flight queries.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Evaluated every `DUCKDB_RECYCLE_INTERVAL_MIN` minutes (baseline default: 60 minutes; 5m in production environments).
- **Dynamic Load-Based Adaptation:**
  - **Memory Pressure Acceleration:** When process RSS approaches threshold (>= 80% of `DUCKDB_RECYCLE_RSS_THRESHOLD_MB`), or if a prior recycle cycle returned `incomplete`, the scheduler dynamically adjusts the interval down to 2 minutes (`interval_mins=2.0`) to aggressively reclaim memory before hitting OOM.
  - **Baseline Relaxation:** Once RSS drops below 50% of the threshold, the interval is automatically restored to its baseline setting.
- **Scope:** Process-global singleton job.
- **Activation Gate:** Registered **ONLY** if `DUCKDB_RECYCLE_INTERVAL_MIN > 0`. If set to 0 or negative, the job is disabled.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=120s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Cycles idle connections in `backend.core.duckdb_pool`; forces memory garbage collection and C-allocator heap trim. | Connection pool lock. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). |
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
1. **OOM Stopgap Check:** Evaluates `maybe_graceful_restart()` first. If RSS exceeds container limit, initiates graceful restart.
2. **RSS Gate Check:** Checks `current_rss_bytes()` against `_recycle_rss_threshold_bytes()`. If RSS is below threshold, skips recycle and dynamically evaluates schedule relaxation.
3. **Write Lock Acquisition:** Acquires per-service iceberg write locks in deterministic sorted order to prevent deadlock with concurrent sync/commit jobs.
4. **Connection Barrier & Drain:**
   - Raises fail-open recycle barrier so new connections pause for the duration of the drain.
   - Retires existing pools and waits up to `DUCKDB_RECYCLE_DRAIN_TIMEOUT_MS` for `in_use == 0`.
   - Actively executing queries are allowed to complete.
5. **Memory Reclamation:**
   - Calls `gc.collect()` to collect closed connection wrappers.
   - Issues `ctypes.CDLL("libc.so.6").malloc_trim(0)` on Linux to release free C++ heap pages back to the kernel.
   - Waits for weakref `live_connection_count` to reach zero.
6. **Barrier Release & Cleanup:**
   - Restores pools, drops recycle barrier, and releases service write locks.
7. **Telemetry & Log Recording:**
   - Measures `rss_before` and `rss_after`; records `freed_bytes`.
   - Dynamically adapts scheduler interval for the next tick based on post-recycle RSS.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local memory and connection pool management; zero cloud API calls.
  - **Memory Metrics:** Resident memory RSS delta explicitly logged (`freed_mb`).
  - **Thread Wait Time:** Pool acquisition wait time (`app.thread_wait_ms`) during recycling remains bounded by fail-open barrier timeout.
- **Timing & Resource Budgets:**
  - Recycle execution: < 100ms when idle; up to 3s when draining active requests.
  - Memory freed: Typically 100MB–1.5GB on busy services.
- **Audit Checklist:**
  - Active user queries are NOT terminated or interrupted during recycling.
  - Fresh pool connections are healthy and accept incoming queries immediately.

---

## 7. Failure Modes & Recovery Runbooks
- **Lock Contention / Drain Timeout:** Safely aborts cycle with status `incomplete`, releases all locks, and tightens schedule to retry in 2 minutes.
- **Re-Open Failure:** If opening a fresh DuckDB connection fails, falls back to lazy creation on next request.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Recycle:** `POST /api/admin/duckdb/recycle?force=false`.
- **Inspect Pool Status:** `GET /api/admin/duckdb/status`.

---

## 9. Automated Verification Checklist
- [x] 1. Unit tests verify `_trim_memory` calls `gc.collect()` and handles `malloc_trim(0)` gracefully.
- [x] 2. Unit tests verify `get_pool_status` and `is_recycle_barrier_active` return complete metrics.
- [x] 3. Unit tests verify `run_duckdb_recycle` skips when below RSS threshold and executes when exceeded.
- [x] 4. Unit tests verify adaptive dynamic scheduling tightens interval to 2m under elevated RSS and relaxes to baseline.
- [x] 5. Unit tests verify `POST /api/admin/duckdb/recycle` triggers recycle on demand.
- [x] 6. Unit tests verify `GET /api/admin/duckdb/status` returns memory metrics, barrier state, and connection pools.
- [x] 7. Confirm zero FOS Class A/B calls.
