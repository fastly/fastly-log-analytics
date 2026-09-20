> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `optimize_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `optimize_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `optimize_{service_id}`
- **Category:** Cloud Lakehouse Optimization & Durability
- **Purpose:** Executes DuckLake-native cloud maintenance: (1) Durability flush of metadata-inlined commits to cloud storage (`CALL ducklake_flush_inlined_data('lake')`), and (2) Cloud Parquet file compaction and layout optimization (`CALL ducklake_rewrite_data_files('lake')`).
- **Why It Runs:** DuckLake inlines small commits directly into the metadata catalog for speed. Neither `ducklake_rewrite_data_files` nor `ducklake_merge_adjacent_files` will touch or promote inlined rows without an explicit flush. Without this job, a table could remain at `file_count = 0` indefinitely with all data residing solely in catalog memory/DB, and raw `.gz` files deleted after ingest. This job guarantees cloud data file materialization and eliminates fragmented small files in FOS.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 04:00 UTC (`hour=4, minute=0`).
- **Timing Rationale:** Runs after `rollup_compact` (02:00 UTC), `metadata_cleanup` (03:15 UTC), and `full_sync` (03:30 UTC) to ensure the previous day's commits and late logs have all landed.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Connects to DuckLake catalog, executes `ducklake_flush_inlined_data` then `ducklake_rewrite_data_files` against FOS `ducklake/`. | Exclusive per-service write lock. Gated by `FLA_DEV_NO_CRONS=1` (writes FOS). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat / Celery Worker | Dispatches cloud table rewrite task across Celery workers or executes via shared Postgres catalog. | PostgreSQL transaction lock. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/optimize/{service_id}` | Full cloud optimization history in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Cloud maintenance is strictly an Admin duty. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side DuckLake state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Pre-Flight & Lock Acquisition:** Verifies service config and acquires exclusive service maintenance lock. Checks `FLA_DEV_NO_CRONS=1` (skips if set).
2. **Durability Flush Step (Critical):**
   - Executes `CALL ducklake_flush_inlined_data('lake')`.
   - Forces all unmaterialized catalog commits to write physical Parquet data files to `s3://{bucket}/{prefix}/ducklake/data/`.
3. **Data File Rewrite & Compaction:**
   - Executes `CALL ducklake_rewrite_data_files('lake')`.
   - Merges small physical Parquet files into optimal 128MB–256MB chunks.
   - Preserves sort orders and zone maps for maximum partition pruning.
4. **Metadata Catalog Commit:** Commits rewritten data file references to the DuckLake catalog in a single atomic transaction.
5. **Usage Accounting & Telemetry:**
   - Logs FOS Class A PUT and Class B GET calls in `usage_log.db`.
   - Records run status, `bytes_rewritten`, and `files_compacted` in `cron_runs`.
6. **Lock Release:** Releases exclusive service lock.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** Rewrites generate Class A PUT and Class B GET/DELETE calls; all must be tracked and attributed in `usage_log.db`.
  - **DuckLake DDL/CALLs:** Every `CALL ducklake_*` procedure must be recorded in `telemetry_queries` with execution duration.
  - **SQLite Auditing:** Run state recorded in `metadata.db` via `ThreadLocalPool`.
- **Timing & Resource Budgets:**
  - Flush step: < 5s.
  - Rewrite throughput: > 50 MB/s.
  - Overall duration: < 10 minutes on large tables.
- **Audit Checklist:**
  - Verify that `file_count > 0` after the flush step.
  - Confirm table scan latency after rewrite decreases or remains optimal.
  - Verify zero orphaned files left in FOS without catalog references.

---

## 7. Failure Modes & Recovery Runbooks
- **S3 Upload Error Mid-Rewrite:** DuckLake transactions are atomic; uncommitted new files will be collected by `expire` cleanup.
- **Lock Contention:** If a commit is actively writing, `optimize` waits up to 60s before rescheduling.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Optimization:** `POST /api/admin/optimize/{service_id}`.
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/optimize/{service_id}`; verify HTTP 200 response.
- [ ] 2. Verify in logs: `ducklake_flush_inlined_data` executed successfully.
- [ ] 3. Verify in logs: `ducklake_rewrite_data_files` executed successfully.
- [ ] 4. Confirm in `cron_runs`: run status `success` with non-zero duration.
- [ ] 5. Confirm `usage_log.db` records FOS Class A/B calls attributed to `cron.optimize`.
- [ ] 6. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
