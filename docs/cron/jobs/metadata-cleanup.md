# Background Job Specification: `metadata_cleanup_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `metadata_cleanup_{service_id}`
- **Category:** Operational Database Retention & Pruning
- **Purpose:** Trims historical operational records from per-service databases (`metadata.db`, `usage_log.db`, or PostgreSQL `METADATA_DSN`), including `usage_log`, `ingested_files`, `cron_runs`, `slow_queries`, expired quarantine files, and global `metric_snapshots` according to configured retention windows.
- **Why It Runs:** Continuous streaming writes hundreds of operational records per hour. Unbounded growth in SQLite databases causes WAL bloat, slower indexed lookups, and unneeded disk consumption. Scheduled pruning keeps SQLite databases small, fast, and cached in OS memory.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 03:15 UTC (`hour=3, minute=15`).
- **Timing Rationale:** Runs before `full_sync` (03:30 UTC) and `optimize` (04:00 UTC) so that the daily maintenance window stays single-threaded across heavy phases.
- **Configurable Overrides:**
  - `provisioning.cron_metadata_cleanup.enabled` (default: `true`).
  - `provisioning.cron_metadata_cleanup.cron_hour` (default: `3`).
  - `provisioning.cron_metadata_cleanup.cron_minute` (default: `15`).
  - `metadata_retention.usage_log_days` (default: 1 day).
  - `metadata_retention.ingested_files_days` (default: 1 day).
  - `metadata_retention.cron_runs_days` (default: 7 days).
  - `metadata_retention.slow_queries_days` (default: 30 days).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Connects to `data/services/{service_id}.metadata.db` and `usage_log.db`, issues bounded chunked `DELETE FROM` statements, commits WAL, and VACUUMs if rows were trimmed. | ThreadLocalPool connection lock. Politeness gate yields if active dashboard queries are running. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler or Worker | Connects to PostgreSQL `METADATA_DSN` or local usage log; executes SQL deletes in 5,000-row chunks. | PostgreSQL transaction lock. Politeness gate yields if active dashboard queries are running. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/metadata/cleanup/{service_id}` | Pruning statistics in Admin UI. Purges expired local quarantine evidence. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Prunes local analyst metadata databases. Quarantine writes and maintenance skipped. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side maintenance only; no direct interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Active Query Politeness Check:**
   - Calls `should_defer_cron("metadata_cleanup", service_id)`. If active user requests are running on the dashboard, gracefully exits to avoid locking SQLite tables during VACUUM.
2. **Progress Initialization:**
   - Calls `start_cron_run(src, "metadata_cleanup")` and registers execution in `cron_progress`.
3. **Config Resolution:**
   - Loads retention thresholds from `configs/{service_id}.json` under `metadata_retention`. Defaults to 1 day for `usage_log` and `ingested_files`, 7 days for `cron_runs`, and 30 days for `slow_queries`.
4. **Purge Ingested Files (with Dedup Guard):**
   - Verifies whether `cron_sync.delete_after` is active. If `delete_after` is `false`, `ingested_files` is preserved as the dedup gate against re-ingestion storms.
   - Otherwise, calculates cutoff and deletes rows in 5,000-row chunks.
5. **Purge Usage Log Records:**
   - Deletes raw `usage_log` rows older than `usage_log_days` in 5,000-row chunks.
   - *Note:* Hourly summary rollups in `usage_log_hourly_summary` are preserved indefinitely for billing history.
6. **Purge Cron Execution Logs & Slow Queries:**
   - Deletes `cron_runs` older than `cron_runs_days`.
   - Deletes `slow_queries` older than `slow_queries_days`.
7. **SQLite VACUUM:**
   - If any rows were deleted, issues `VACUUM` on SQLite database files to reclaim freed pages and compact physical storage.
8. **Global System Metrics & Local Quarantine Purge:**
   - Purges global `metric_snapshots` older than 30 days.
   - The owning serving/web process purges local quarantine evidence older than seven days and performs bounded oldest-first eviction when the 1,000-bad-line cap is exceeded. Evidence bytes are measured for warnings but do not independently trigger eviction. Celery workers and read-only Analyst Path A instances do not run this maintenance.
9. **Telemetry, Progress & Duration Finalization:**
   - Emits done event to `cron_progress`.
   - Records deleted row tallies and execution status in `cron_runs`.
   - In `finally:` block, calls `end_progress(run_id)` and `finalize_cron_duration(src, run_id, start_ts)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **FOS Calls:** Zero FOS calls for quarantine maintenance; evidence is local-only.
  - **SQLite Operations:** Every `DELETE FROM` statement executes in 5,000-row chunks via `ThreadLocalPool` with instrumented timings.
  - **Lock Wait Time:** Connection acquisition wait (`app.thread_wait_ms`) remains < 20ms.
- **Timing & Resource Budgets:**
  - Full cleanup execution: < 1.0 second (excluding VACUUM on large databases).
  - SQLite WAL truncate: < 100ms.
- **Audit Checklist:**
  - Confirm `usage_log_hourly_summary` is NOT deleted (billing history preserved).
  - Verify `ingested_files` deletion is suppressed when `cron_sync.delete_after=false`.
  - Confirm database files don't suffer from fragmentation.

---

## 7. Failure Modes & Recovery Runbooks
- **SQLite Database Locked (`sqlite3.OperationalError`):** Handled gracefully; politeness gate prevents contention with active dashboard users.
- **Corrupt SQLite File:** If corruption is detected, logs error and triggers automated `.dump` restore.
- **Read-Only Service (Analyst):** Quarantine writes and maintenance are skipped; only the instance's owned operational metadata is pruned.

### Local quarantine evidence
- One bad raw line is one capacity item. A source object with multiple malformed lines
  therefore contributes multiple items to the 1,000-item cap.
- Evidence is stored under `data/services/{service_id}/quarantine/`, with metadata in the
  service metadata database. The admin view groups those line items by source object and
  expands them for inspection.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Cleanup:** `POST /api/admin/metadata/cleanup/{service_id}`.
- **Inspect DB Sizes:** `GET /api/admin/system/disk-usage`.

---

## 9. Automated Verification Matrix
- [x] 1. Active request politeness deferral verified via `test_metadata_cleanup_defers_when_active_requests_present`.
- [x] 2. Progress events emitted to `cron_progress` verified via `test_metadata_cleanup_happy_path_emits_progress_and_finalizes`.
- [x] 3. Duration finalization in `finally:` block verified via `test_metadata_cleanup_happy_path_emits_progress_and_finalizes` and `test_metadata_cleanup_handles_failure_and_finalizes_duration`.
- [x] 4. Dynamic scheduler rescheduling on `cron_metadata_cleanup` config change verified via `test_sync_jobs_reschedules_metadata_cleanup_when_hour_changed`.
- [x] 5. Job disabled when `cron_metadata_cleanup.enabled = False` verified via `test_sync_jobs_skips_metadata_cleanup_when_disabled`.
- [x] 6. Streaming progress on manual trigger verified via `tests/routers/test_admin_compaction.py::test_metadata_cleanup_streams_done_event_on_success`.
