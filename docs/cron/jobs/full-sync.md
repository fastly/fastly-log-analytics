# Background Job Specification: `full_sync_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `full_sync_{service_id}`
- **Category:** Full Cloud Bucket Sweep & Gap Reconciliation
- **Purpose:** Executes a deep, exhaustive LIST operation across the entire FOS raw log bucket prefix (`raw/request/**/*.gz`) to discover and ingest any log files that were delayed, dropped, or missed by high-frequency periodic sync ticks.
- **Why It Runs:** Real-time log streaming can experience transient edge delivery hiccups or network partitioning. Periodic sync ticks only inspect recent lookback windows to minimize FOS Class A LIST costs. The periodic full sweep guarantees 100% data completeness by auditing every raw log file deposited in FOS.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Every 6 hours at :30 (03:30, 09:30, 15:30, 21:30 UTC).
- **Timing Rationale:** 4x daily execution provides rapid recovery of late-arriving logs without waiting 24 hours. The 03:30 run sits after `metadata_cleanup` (03:15 UTC) and before `optimize` (04:00 UTC) so recovered files are merged into cloud compaction.
- **Configurable Overrides:**
  - `provisioning.cron_full_sweep.enabled` (default: true).
  - `provisioning.cron_full_sweep.cron_hours` (default: `"3,9,15,21"`).
  - `provisioning.cron_full_sweep.cron_minute` (default: `30`).
  - `provisioning.cron_full_sweep.max_files` (override default dynamic file budget).
  - `provisioning.cron_full_sweep.max_seconds` (override default dynamic duration budget).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Full S3 LIST on FOS raw prefix, cross-checks SQLite `ingested_files`, ingests missing `.gz` files into local buffer. | Exclusive per-service ingest lock. Active-request deferral (`should_defer_cron("full_sync", service_id)`). Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Workers / Web Pod | Full S3 LIST via `discover_prefix`, bulk inserts unrecorded keys into PostgreSQL `ingest_ledger`, dispatches worker conversion tasks. | Distributed PostgreSQL row locks. Celery queue-depth adaptive throttling. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/full-sweep/{service_id}` | Complete audit log in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Standard ingestion disabled for analysts. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side ingested state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Pre-Flight, Politeness Gate & Lock:**
   - Verifies configuration and acquires per-service ingest lock. Checks `FLA_DEV_NO_CRONS=1` (aborts if set).
   - Evaluates `should_defer_cron("full_sync", service_id)`. If active user requests are querying DuckDB, defers execution to preserve sub-second UI latency.
2. **Adaptive Queue-Depth & Backlog Budgeting:**
   - If not overridden by `_run_gap_heal`:
     - **High-Scale Mode:** Inspects broker queue depth via `celery_queue_depths()`. If queue depth > 5,000, scales `max_files` down to 5,000 to prevent worker starvation. If queue depth < 500, scales `max_files` up to 50,000.
     - **Standard Mode:** Inspects DuckDB buffer backlog via `buffer_backlog_stats()`. If uncommitted buffer file count > 2,000, scales `max_files` down to 5,000 and `max_seconds` to 300 to let commit drain first. If buffer is healthy (< 200 files), scales `max_files` up to 50,000 and `max_seconds` to 1200.
3. **Exhaustive FOS LIST & Ledger Diff:**
   - In High-Scale Mode: `discover_prefix(service_id)` executes full prefix LIST and inserts missing keys into `ingest_ledger` as `discovered`.
   - In Standard Mode: Issues paginated `list_fos_files` covering the entire service prefix, diffing against `ingested_files`.
4. **Targeted Ingestion & Quarantining:**
   - Downloads missing `.gz` log files and parses records into Parquet buffer.
   - Any corrupt or invalid lines use the same local-only, per-service quarantine contract as
     `log_discovery`: one exact-byte item per failed line or corrupt gzip, shared 1,000-item
     cap, immediate oldest-item eviction, and no age expiry. The source FOS object is always
     deleted after processing, including when evidence capture fails.
5. **View Update & State Recording:**
   - Updates DuckDB view to expose newly ingested historical rows.
   - Updates `ingested_files` table with newly captured keys.
6. **Telemetry, Timing & Progress Reporting:**
   - Emits real-time SSE progress events to `cron_progress`.
   - Records run status `error` when any log line, quarantine capture, or FOS deletion fails;
     otherwise `success`, with the shared zero-filled record-level and object-level outcome
     counters and full details in `cron_runs`.
   - A source object whose records ingest successfully but whose FOS deletion fails after
     bounded retries counts as `objects_failed`, not `objects_partial`.
   - Invokes `finalize_cron_duration` in `finally` block to record exact execution duration.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 LIST Calls:** Full paginated LIST must be attributed to `cron.full_sync` in `usage_log.db`.
  - **SQLite / Postgres Queries:** Diff check queries must use indexed lookups.
  - **Ingest Telemetry:** Number of discovered missing files must be explicitly logged in `cron_runs.details_json`.
  - **Corrupt Rows:** Valid rows continue, each failed line is stored separately when possible,
    and the run transitions to `error` with per-category counters.
- **Timing & Resource Budgets:**
  - S3 LIST throughput: > 5,000 keys/sec.
  - Ingestion processing: Matches standard ingestion rates (> 50,000 logs/sec per core).

---

## 7. Failure Modes & Recovery Runbooks
- **S3 504 / Gateway Timeout on Huge Buckets:** Implements delimiter and prefix-based subdirectory paging to prevent large LIST timeouts.
- **Lock Contention with Standard Sync:** Standard sync yields when full sweep holds the ingest lock.
- **Active Dashboard Queries:** `should_defer_cron` yields full sweep ticks while users are actively running queries.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Full Sweep:** `POST /api/admin/full-sweep/{service_id}`.
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. Automated Verification Matrix
- [x] 1. Active request deferral verified via `test_full_sweep_defers_when_active_requests_present`.
- [x] 2. Quarantined corrupt rows trigger `warning` status verified via `test_full_sweep_warning_status_on_corrupt_rows`.
- [x] 3. Dynamic budget scaling with buffer backlog verified via `test_full_sweep_adaptive_budget_scales_with_buffer_backlog` and `test_full_sweep_adaptive_budget_scales_up_when_buffer_clean`.
- [x] 4. Accurate duration finalization verified via `test_full_sweep_finalizes_duration`.
- [x] 5. Dynamic rescheduling and 6-hour interval configuration verified in `test_scheduler.py`.
