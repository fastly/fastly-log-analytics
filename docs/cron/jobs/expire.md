# Background Job Specification: `expire_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `expire_{service_id}`
- **Category:** Data Retention & Archive Cost Reclamation
- **Purpose:** Enforces data retention policies (`data_retention_days`, `rum_retention_days`) across ClickHouse serving tables (`request_facts`, `rum_vitals`, `rum_errors`) and Fastly Object Storage (FOS) Archive Parquet manifests (`high_scale_archive_manifests`).
- **Why It Runs:** Continuous ingestion accumulates MergeTree partitions in ClickHouse and canonical archive Parquet batches in FOS. Periodic retention enforcement keeps ClickHouse disk usage and FOS storage costs bounded to the configured retention window.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Hourly by default (`expire_interval_mins = 60`).
- **Configurable Overrides:**
  - `provisioning.cron_sync.expire_interval_mins` (default: 60, min: 5).
  - `data_retention_days` (0 = retain forever).
  - `rum_retention_days` (0 = retain forever).
- **Jitter & Misfire Policy:**
  - Jitter: 60 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Component | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Unified ClickHouse Architecture** | APScheduler / Stateless Worker Maintenance | Drops or prunes expired ClickHouse partitions/rows and unlinks expired FOS archive Parquet batches recorded in PostgreSQL `high_scale_archive_manifests`. | Per-service `expire` lease in PostgreSQL `cron_runs`. Gated by `FLA_DEV_NO_CRONS=1`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | Admin maintenance endpoints | Full retention and expiry status in Admin UI. |
| **Analyst (Remote Share)** | N/A | Blocked (403) | Reads server-side ClickHouse state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Retention Delete (each retention knob gated independently):**
   - If `data_retention_days > 0`, expires request-fact partitions/rows and archive manifests older than the cutoff.
   - If `rum_retention_days > 0`, expires `rum_vitals` and `rum_errors` partitions/rows and archive manifests older than the cutoff.
   - *Crucial Rule:* A setting of `0` means **keep forever**.
2. **FOS Archive Unlinking:**
   - Unlinks expired archive Parquet objects from `s3://{bucket}/{prefix}/archive/` and marks their manifest rows expired in PostgreSQL.
3. **Quarantine Consistency (Local Metadata Only):**
   - Quarantine evidence is local-only, has no age-based expiry, and is capped at 1,000 items per service during ingestion writes. This job does not perform quarantine eviction.
4. **Telemetry & Log Recording:**
   - Records total duration, deletion counts in `summary`, FOS unlinks in `files_deleted_fos`, and isolated failures in `error_message` in `cron_runs`.
   - Flushes FOS usage calls to PostgreSQL's `usage_log`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Deletes:** FOS delete calls are tracked as Class A calls in PostgreSQL's `usage_log`.
  - **ClickHouse DDL/DML:** Retention statements are registered in the Live Query Monitor (`query_registry`).
- **Timing & Resource Budgets:**
  - Retention pass: < 10 seconds.
  - S3 file unlinking: > 20 files/sec.
- **Audit Checklist:**
  - Verify `data_retention_days = 0` never deletes historical logs.
  - Verify expired archive objects are unlinked and recorded in `files_deleted_fos`.

---

## 7. Failure Modes & Recovery Runbooks
- **S3 403 / Access Denied on Delete:** Logs specific S3 error to `cron_runs` with warning; verifies FOS access key permissions.
- **ClickHouse Unavailable:** Recorded in `cron_runs`; retried on the next hourly tick.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}` and `/admin/clickhouse`.

---

## 9. Verification Checklist
- 1. Confirm in `cron_runs`: status `success` (or `warning` if a non-fatal step failed), overall duration, summary counts, and `files_deleted_fos`.
- 2. Confirm FOS Class A delete requests are recorded in PostgreSQL's `usage_log`.
- 3. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
