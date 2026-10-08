# Background Job Specification: `expire_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `expire_{service_id}`
- **Category:** Snapshot Expiry, Data Retention & Cloud Cost Reclamation
- **Purpose:** Enforces data retention policies (`data_retention_days`, `rum_retention_days`) by deleting expired rows from DuckLake tables, expiring stale snapshots (`ducklake_expire_snapshots`), and purging unreferenced cloud Parquet files (`ducklake_cleanup_old_files`), along with local cache cleanup.
- **Why It Runs:** Continuous ingestion and snapshot commits accumulate metadata and historical Parquet files in FOS. Without periodic snapshot expiration and unreferenced file unlinking, storage costs grow unbounded and metadata overhead degrades commit speeds.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Hourly by default (`expire_interval_mins = 60`).
- **Timing Rationale:** Changed from historical weekly schedule to hourly to eliminate the "performance sawtooth" where thousands of snapshots accumulated and degraded commit latency from seconds to > 100s.
- **Configurable Overrides:**
  - `provisioning.cron_sync.expire_interval_mins` (default: 60, min: 5).
  - `provisioning.cron_sync.keep_snapshot_days` (default: 7 days).
  - `data_retention_days` (0 = retain forever).
  - `rum_retention_days` (0 = retain forever).
  - `cache_retention_days` (default: 90 days).
  - `rollup_retention_months` (default: 12 months).
- **Jitter & Misfire Policy:**
  - Jitter: 60 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Connects to DuckLake catalog, issues SQL deletes, expires snapshots, unlinks unreferenced cloud files, cleans local cache. | Atomic per-service/per-task `expire_snapshots` lease in PostgreSQL `job_runs`. Gated by `FLA_DEV_NO_CRONS=1` (writes/deletes FOS). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Serving-pod APScheduler | Executes snapshot expiry against the Postgres DuckLake catalog. It is not dispatched to RedBeat or Celery; the snapshot log is catalog-wide under shared Postgres. | Atomic per-service `expire_snapshots` lease in PostgreSQL `job_runs`; DuckLake write connection for catalog mutations. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/expire-snapshots/{service_id}` | Full retention and expiry status in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Cloud retention is managed exclusively by Admin. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side DuckLake state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
The job checks `should_defer_cron("expire_snapshots", service_id)` and initializes SSE progress (`start_progress`) before running five isolated maintenance steps. The atomic lease is per service and task, so concurrent Cron 8 invocations cannot overlap; it is not a lock shared with every other cron. Manual runs bypass politeness deferral but still use the same lease. Each maintenance failure is returned under its own `*_error` key and does not prevent later steps from being attempted:
1. **Step 1: Retention Delete (each retention knob gated independently):**
   - If `data_retention_days > 0`, delete expired rows from this service's DuckLake `logs` table.
   - If `rum_retention_days > 0`, delete expired rows from this service's `client_vitals` and `client_errors` tables.
   - *Crucial Rule:* A setting of `0` means **keep forever**.
2. **Step 2: Snapshot Expiry & File Cleanup:**
   - Calls `ducklake_expire_snapshots('lake', older_than => cutoff_date)` using `keep_snapshot_days`.
   - Calls `ducklake_cleanup_old_files('lake', older_than => cutoff_date)` on every run, including when there are no snapshots or snapshot expiry fails, to unlink queued unreferenced files in FOS.
   - *Never call `ducklake_delete_orphaned_files`* (it would destroy local compaction files).
3. **Step 3: Comprehensive Local Disk Cache & Temp Purge:**
   - Scans `cache/{bucket}/data`, RUM tables (`data_client_vitals`, `data_client_errors`), and `buffer/` for Parquet files older than `cache_retention_days` (default: 90 days) and deletes them.
   - Cleans orphaned temporary files (`*.tmp`, `*.part`, `*.bad.jsonl.tmp`) older than 2 hours across `cache/{bucket}/` left behind by aborted writes or worker crashes.
   - Temporary-file cleanup runs independently of the Parquet retention setting. Deletion and traversal failures are reported as `local_cache_error`; successful deletions are counted separately.
4. **Step 4: Rollup Retention Purge:**
   - Scans `cache/{bucket}/rollups/` for expired rollup Parquet files using `rollup_retention_months` (default: 12 months; the cutoff approximates a month as 30 days).
   - Reports deletions as `local_rollup_files_deleted` and failures as `local_rollup_error`.
5. **Step 5: Quarantine Consistency (Local Metadata Only):**
   - Quarantine evidence is local-only, has no age-based expiry, and is capped at 1,000
     items per service during ingestion writes. This job does not perform normal quarantine
     eviction or FOS quarantine deletion. Any bounded orphan-reconciliation responsibility
     belongs to `metadata_cleanup_{service_id}`, which removes stale metadata references
     for missing evidence and preserves/reports unexpected local evidence files.
6. **Telemetry & Log Recording:**
   - Records total duration, exact deletion counts in `summary`, FOS unlinks in `files_deleted_fos`, and isolated failures in `error_message` in `cron_runs`. The status is `warning` if any step failed and `success` when all steps completed cleanly.
   - The decorator sets `process_context="cron:expire_snapshots"` for query attribution and flushes FOS usage calls to PostgreSQL's `usage_log`. A `finally` block terminally finalizes any remaining cron lease and duration even if progress cleanup fails.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Deletes:** FOS delete calls must be tracked as Class A calls in PostgreSQL's `usage_log`, attributed to `cron:expire_snapshots`.
  - **DuckLake DDL/DML:** `DELETE FROM` and `CALL ducklake_*` queries must be attributed as Cron 8 in the Live Query Monitor; query-level records use the existing query registry/slow-query persistence.
  - **Isolated Result Keys:** `cron_runs.summary` records `retention_deleted_rows`, `snapshots_expired`, local deletion counts, and `files_unlinked`; `error_message` records isolated `*_error` details. `files_deleted_fos` stores the exact FOS unlink count. There is no `details_json` column.
- **Timing & Resource Budgets:**
  - Snapshot expiry: < 10 seconds.
  - S3 file unlinking: > 20 files/sec.
  - Local cache sweep: < 5 seconds.
- **Audit Checklist:**
  - Verify `data_retention_days = 0` never deletes historical logs.
  - Verify snapshots older than `keep_snapshot_days` are properly expired.
  - Confirm commit latency post-expiry drops due to reduced metadata overhead.

---

## 7. Failure Modes & Recovery Runbooks
- **Live File Anchor:** A snapshot cannot be expired while a live data file still references it. This is expected behavior; reclamation occurs after the daily `optimize` rewrite.
- **S3 403 / Access Denied on Delete:** Logs specific S3 error to `cron_runs` with warning; verifies IAM credentials.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Expiry:** `POST /api/admin/expire-snapshots/{service_id}`.
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. Verification Checklist
- [x] 1. Trigger `POST /api/admin/expire-snapshots/{service_id}`; verify HTTP 200. Manual runs returned `success` in Local Standard, Remote Standard, and Remote High-Scale.
- [x] 2. Confirm in `cron_runs`: status `success` (or `warning` if a non-fatal step failed), overall duration, exact summary counts, and `files_deleted_fos`.
- [x] 3. Verify in the Live Query Monitor that `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` executed.
- [x] 4. Confirm FOS Class A delete requests are recorded in PostgreSQL's `usage_log` under `cron:expire_snapshots`. `files_deleted_fos` counts unlinked objects, not API requests.
- [x] 5. Confirm local cache files older than retention policy are cleaned from disk; the contract test verifies deletion, while live runs found no eligible stale local files.
- [x] 6. Confirm maintenance SQL is attributed as `cron:expire_snapshots` in query telemetry in Standard and High-Scale modes.
- [x] 7. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute in Standard or High-Scale scheduler modes.
