> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `expire_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

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
  - `cache_retention_days` (default: 7 days).
  - `rollup_retention_months` (default: 12 months).
- **Jitter & Misfire Policy:**
  - Jitter: 60 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Connects to DuckLake catalog, issues SQL deletes, expires snapshots, unlinks unreferenced cloud files, cleans local cache. | Exclusive per-service maintenance lock. Gated by `FLA_DEV_NO_CRONS=1` (writes/deletes FOS). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler or Worker | Executes snapshot expiry across Postgres DuckLake catalog. Note: snapshot log is catalog-wide under shared Postgres. | PostgreSQL table/schema lock. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/expire-snapshots/{service_id}` | Full retention and expiry status in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Cloud retention is managed exclusively by Admin. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side DuckLake state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
The job executes 4 distinct, isolated steps. Each step captures its own errors so one failure does not abort the remaining steps:
1. **Step 1: Retention Delete (Gated on `data_retention_days > 0`):**
   - If `data_retention_days > 0`: `DELETE FROM lake.logs WHERE timestamp < ?` (cutoff = `NOW() - interval`).
   - If `rum_retention_days > 0`: `DELETE FROM lake.client_vitals WHERE timestamp < ?` and `DELETE FROM lake.client_errors WHERE timestamp < ?`.
   - *Crucial Rule:* A setting of `0` means **keep forever**.
2. **Step 2: Snapshot Expiry & File Cleanup:**
   - Calls `ducklake_expire_snapshots('lake', older_than => cutoff_date)` using `keep_snapshot_days`.
   - Calls `ducklake_cleanup_old_files('lake', older_than => cutoff_date)` to unlink queued unreferenced files in FOS.
   - *Never call `ducklake_delete_orphaned_files`* (it would destroy local compaction files).
3. **Step 3: Local Disk Cache Purge:**
   - Scans `cache/{bucket}/` and `data/parquet/` for files older than `cache_retention_days` and deletes them.
4. **Step 4: Rollup Retention Purge:**
   - Scans `rollups/{service_id}/` for day bundles older than `rollup_retention_months` and deletes them.
5. **Telemetry & Log Recording:**
   - Records step timings and deleted counts in `cron_runs` (marks `warning` if an isolated step failed, `success` if clean).
   - Records FOS Class A delete calls in `usage_log.db`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Deletes:** Every unlinked Parquet file must be tracked as a Class A call in `usage_log.db`.
  - **DuckLake DDL/DML:** All `DELETE FROM` and `CALL ducklake_*` statements must be recorded in `telemetry_queries`.
  - **Isolated Result Keys:** `cron_runs.details_json` must record `retention_deleted_rows`, `snapshots_expired`, `files_unlinked`, and any `*_error` details.
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

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/expire-snapshots/{service_id}`; verify HTTP 200.
- [ ] 2. Confirm in `cron_runs`: status `success` (or `warning` if non-fatal step error).
- [ ] 3. Verify in logs: `ducklake_expire_snapshots` and `ducklake_cleanup_old_files` executed.
- [ ] 4. Confirm FOS Class A delete calls recorded in `usage_log.db`.
- [ ] 5. Confirm local cache files older than retention policy are cleaned from disk.
- [ ] 6. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
