# Background Job Specification: `metadata_cleanup_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `metadata_cleanup_{service_id}`
- **Category:** Operational Database Retention & Pruning
- **Purpose:** Trims historical operational records from the unified PostgreSQL 16 `METADATA_DSN`, including `usage_log`, `ingested_files`, `cron_runs`, `slow_queries`, and global `metric_snapshots` according to configured retention windows. Quarantine-cap enforcement occurs during writes in log discovery; no separate quarantine cron or FOS deletion is executed.
- **Why It Runs:** Continuous streaming writes hundreds of operational records per hour. Scheduled pruning bounds PostgreSQL table growth and keeps indexed lookups efficient.

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
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Connects to PostgreSQL `METADATA_DSN` and issues bounded chunked `DELETE FROM` statements (`ctid IN (SELECT ctid ... LIMIT 5000)`). | PostgreSQL connection pool and transaction; politeness gate yields if active dashboard queries are running. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_scale`)** | Pod APScheduler | Connects to PostgreSQL `METADATA_DSN` and executes SQL deletes in bounded 5,000-row chunks. | PostgreSQL transaction; politeness gate yields if active dashboard queries are running. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/metadata-cleanup/{service_id}` (or `POST /api/admin/metadata-cleanup`) | Pruning statistics in Admin UI via SSE streaming. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Prunes local analyst metadata databases. Quarantine writes and maintenance skipped. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side maintenance only; no direct interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Kill-Switch Protection (`FLA_DEV_NO_CRONS=1`):**
   - Checks `dev_mode_no_crons()`. If active, immediately logs and exits without performing cleanup or allocating a run ID.
2. **Active Query Politeness Check:**
   - Calls `should_defer_cron("metadata_cleanup", service_id)`. If active user requests are running on the dashboard, gracefully exits to avoid contention with active analytical queries.
3. **Progress Initialization:**
   - Calls `start_cron_run(src, "metadata_cleanup")` and registers execution in `cron_progress`.
4. **Config Resolution:**
   - Loads retention thresholds from `configs/{service_id}.json` under `metadata_retention`. Defaults to 1 day for `usage_log` and `ingested_files`, 7 days for `cron_runs`, and 30 days for `slow_queries`.
5. **Purge Ingested Files (with Dedup Guard):**
   - Verifies whether `cron_sync.delete_after` is active via `is_ingested_files_dedup_active(service_id)`. If `delete_after` is `false`, `ingested_files` is preserved as the dedup gate against re-ingestion storms and `ingested_files_days` is force-overridden to 0.
   - Trims request rows older than `ingested_files_days` (default 1 day). Trims RUM rows (`client_vitals` / `client_errors`) older than a window guaranteed larger than any raw object's lifetime (`max(ingested_files_days, log_retention_days + 1)`), ensuring `ingested_files` does not grow without limit while preventing any re-ingest.
   - Deletes rows in 5,000-row chunks using `ctid IN (SELECT ctid FROM ... LIMIT 5000)`.
   - If any `ingested_files` rows were deleted, recomputes `recompute_ingested_files_summary(con, service_id)`.
6. **Purge Usage Log Records:**
   - Deletes raw `usage_log` rows older than `usage_log_days` in 5,000-row chunks.
   - *Note:* Hourly summary rollups in `usage_log_hourly_summary` are preserved indefinitely for billing and audit history.
7. **Purge Cron Execution Logs & Slow Queries:**
   - Deletes `cron_runs` older than `cron_runs_days` in 5,000-row chunks.
   - Deletes `slow_queries` older than `slow_queries_days` using unix-epoch cutoff (`started_at_utc < time.time() - days * 86400`).
8. **PostgreSQL Autovacuum & Storage Management:**
   - Under PostgreSQL 16 MVCC, dead tuples are managed by autovacuum without blocking concurrent operations or requiring exclusive table locks.
9. **Global System Metrics Purge:**
   - Purges global `metric_snapshots` older than 30 days via `metric_snapshots.purge_old(retention_days=30)`.
   - Performs zero FOS calls; local quarantine evidence adheres to the 1,000-item cap enforced during write discovery.
10. **Telemetry, Progress & Duration Finalization:**
    - Emits `done` event to `cron_progress`.
    - Records deleted row tallies (`rows_ingested=total_deleted`) and execution status in `cron_runs`.
    - In `finally:` block, calls `end_progress(run_id)` and `finalize_cron_duration(src, run_id, start_ts)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **FOS Calls:** Zero FOS calls for quarantine maintenance; evidence is local-only and capped at 1,000 items during write discovery.
  - **PostgreSQL Operations:** Every `DELETE FROM` statement executes in 5,000-row chunks via PostgreSQL connection pool (`psycopg_pool.ConnectionPool`) with instrumented timings.
  - **Lock Wait Time:** Connection acquisition wait (`app.thread_wait_ms`) remains < 20ms.
- **Timing & Resource Budgets:**
  - Full cleanup execution: < 1.0 second.
- **Audit Checklist:**
  - Confirm `usage_log_hourly_summary` is NOT deleted (billing history preserved).
  - Verify `ingested_files` deletion is suppressed when `cron_sync.delete_after=false`.
  - Verify RUM `ingested_files` rows are trimmed once past the raw retention window (`max(ingested_files_days, log_retention_days + 1)`).
  - Confirm zero FOS calls executed during cleanup.

---

## 7. Failure Modes & Recovery Runbooks
- **PostgreSQL Pool Contention / Statement Timeout:** Handled gracefully; politeness gate prevents contention with active dashboard users. Connection pool manages transaction lifecycle with automatic rollback on error.
- **Transient Failures:** Logged to `cron_runs` with `status="error"` and error summary; `finally:` block ensures `finalize_cron_duration` and `end_progress` always run.
- **Read-Only Service (Analyst):** Quarantine writes and maintenance are skipped; only the instance's owned operational metadata is pruned.

### Local Quarantine Evidence
- Each malformed line and corrupt gzip container is one item in a shared 1,000-item cap for that service. There is no age-based expiry or cloud deletion.
- Evidence is stored under `data/services/{service_id}/quarantine/`, outside static web roots, with metadata in the service metadata database.
- Cap eviction happens synchronously during quarantine writes in ingest.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Cleanup (with Service ID):** `POST /api/admin/metadata-cleanup/{service_id}`.
- **Trigger Cleanup (Default Service):** `POST /api/admin/metadata-cleanup`.
- **Streaming Response:** Returns Server-Sent Events (`status`, `progress`, `done`, `error`).
- **Inspect DB Storage Stats:** `GET /api/admin/metadata-storage`.

---

## 9. Automated Verification Matrix
- [x] 1. Active request politeness deferral verified via `test_metadata_cleanup_defers_when_active_requests_present`.
- [x] 2. Progress events emitted to `cron_progress` verified via `test_metadata_cleanup_happy_path_emits_progress_and_finalizes`.
- [x] 3. Duration finalization in `finally:` block verified via `test_metadata_cleanup_happy_path_emits_progress_and_finalizes` and `test_metadata_cleanup_handles_failure_and_finalizes_duration`.
- [x] 4. Dynamic scheduler rescheduling on `cron_metadata_cleanup` config change verified via `test_sync_jobs_reschedules_metadata_cleanup_when_hour_changed`.
- [x] 5. Job disabled when `cron_metadata_cleanup.enabled = False` verified via `test_sync_jobs_skips_metadata_cleanup_when_disabled`.
- [x] 6. Streaming progress on manual trigger verified via `tests/routers/test_admin_compaction.py::test_metadata_cleanup_streams_done_event_on_success`.
- [x] 7. Kill-switch protection under `FLA_DEV_NO_CRONS=1` verified via `test_metadata_cleanup_skips_under_dev_mode_no_crons` and scheduler registration check.
- [x] 8. Zero FOS calls contract verified via `test_metadata_cleanup_makes_zero_fos_calls`.
- [x] 9. Retention window pruning of `ingested_files` (dedup suppression on `delete_after=false` and RUM window `max(ingested_files_days, log_retention_days + 1)`) verified via contract test suite.
- [x] 10. `usage_log` chunked deletion in 5,000-row batches while preserving `usage_log_hourly_summary` verified via contract test suite.
- [x] 11. `cron_runs` (7d), `slow_queries` (30d unix-epoch), and global `metric_snapshots` (30d) retention verified via contract test suite.
- [x] 12. Manual trigger path parity (`POST /api/admin/metadata-cleanup/{service_id}` and `POST /api/admin/metadata-cleanup`) verified via contract test suite.
