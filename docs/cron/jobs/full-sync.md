# Background Job Specification: `full_sync_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `full_sync_{service_id}`
- **Category:** Full Cloud Bucket Sweep & Gap Reconciliation
- **Purpose:** Executes a deep, exhaustive LIST operation across the entire FOS raw log bucket prefix (`raw/request/**/*.{zst,gz}`) to discover and enqueue any log files that were delayed, dropped, or missed by high-frequency periodic discovery ticks.
- **Why It Runs:** Real-time log streaming can experience transient edge delivery hiccups or network partitioning. Periodic discovery ticks only inspect recent lookback windows to minimize FOS Class A LIST costs. The periodic full sweep guarantees 100% data completeness by auditing every raw log file deposited in FOS.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Every 6 hours at :30 (03:30, 09:30, 15:30, 21:30 UTC).
- **Timing Rationale:** 4x daily execution provides rapid recovery of late-arriving logs without waiting 24 hours. The 03:30 run sits after `metadata_cleanup` (03:15 UTC).
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
| Component | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Unified ClickHouse Architecture** | APScheduler + Stateless Ingest Workers | Full S3 LIST via `discover_prefix`, bulk inserts unrecorded keys into PostgreSQL control-plane ledger, and processes batches via the stateless worker into FOS Archive Parquet and ClickHouse. | Distributed PostgreSQL row locks. Active-request deferral (`should_defer_cron("full_sync", service_id)`). Gated by `FLA_DEV_NO_CRONS=1`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/full-sweep/{service_id}` | Complete audit log in Admin UI. |
| **Analyst (Remote Share)** | N/A | Blocked (403) | Reads server-side ingested state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Pre-Flight, Politeness Gate & Lock:**
   - Verifies configuration and acquires per-service `full_sync` lease. Checks `FLA_DEV_NO_CRONS=1` (aborts if set).
   - Evaluates `should_defer_cron("full_sync", service_id)`. If active user requests are running, defers execution to preserve sub-second UI latency.
2. **Adaptive Queue-Depth & Backlog Budgeting:**
   - Inspects pending backlog / queue depth and scales `max_files` (between 5,000 and 50,000) to avoid overwhelming workers while draining existing backlog.
3. **Exhaustive FOS LIST & Ledger Diff:**
   - `discover_prefix(service_id)` executes a full prefix LIST and inserts missing keys into the PostgreSQL ingest ledger as `discovered` with `originating_task="full_sync"`.
4. **Batched Ingestion & Quarantining:**
   - Missing `.gz` / `.zst` log files are claimed, archived to FOS Parquet, and published into ClickHouse.
   - Any corrupt or invalid lines use the shared local-only, per-service quarantine contract: one exact-byte item per failed line or corrupt container, shared 1,000-item cap, immediate oldest-item eviction, and no age expiry.
5. **Telemetry, Timing & Progress Reporting:**
   - Emits real-time SSE progress events to `cron_progress`.
   - Records run status `error` when any log line, quarantine capture, or FOS deletion fails; otherwise `success`, with the shared zero-filled record-level and object-level outcome counters in `cron_runs`.
   - Invokes `finalize_cron_duration` in `finally` block to record exact execution duration.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 LIST Calls:** Full paginated LIST is attributed to `cron.full_sync` in PostgreSQL's `usage_log` table.
  - **PostgreSQL Queries:** Diff check and ledger insert queries use indexed lookups.
  - **Ingest Telemetry:** Number of discovered missing files is explicitly logged in `cron_runs.summary` and `outcome_counters`.
- **Timing & Resource Budgets:**
  - S3 LIST throughput: > 5,000 keys/sec.

---

## 7. Failure Modes & Recovery Runbooks
- **S3 504 / Gateway Timeout on Huge Buckets:** Implements delimiter and prefix-based subdirectory paging to prevent large LIST timeouts.
- **Active Dashboard Queries:** `should_defer_cron` yields full sweep ticks while users are actively running queries.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Full Sweep:** `POST /api/admin/full-sweep/{service_id}`.
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. Verification Checklist
- 1. Manual API trigger `POST /api/admin/full-sweep/{service_id}` returns HTTP 200 with `run_id`, requires admin authorization, and denies analysts with HTTP 403.
- 2. Execution records in `cron_runs` with status `success`, duration, non-zero `files_downloaded`, and structured `outcome_counters`.
- 3. Usage log attributes FOS Class A LIST calls to `cron.full_sync` under `process_context="cron.full_sync"`.
- 4. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
- 5. Active-request deferral when `should_defer_cron("full_sync", service_id)` is True, bypassed when `force=True`.
- 6. `discover_prefix(service_id)` executes full prefix LIST with `originating_task="full_sync"`.
- 7. Corrupt rows / quarantine failures transition status to `error` with structured outcome counters.
- 8. Scheduler registration, rescheduling, and disabling via `provisioning.cron_full_sweep`.
