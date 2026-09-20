> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `full_sync_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `full_sync_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `full_sync_{service_id}`
- **Category:** Full Cloud Bucket Sweep & Gap Reconciliation
- **Purpose:** Executes a deep, exhaustive LIST operation across the entire FOS raw log bucket prefix (`raw/request/**/*.gz`) to discover and ingest any log files that were delayed, dropped, or missed by high-frequency periodic sync ticks.
- **Why It Runs:** Real-time log streaming can experience transient edge delivery hiccups or network partitioning. Periodic sync ticks only inspect recent lookback windows to minimize FOS Class A LIST costs. The daily full sweep guarantees 100% data completeness by auditing every raw log file deposited in FOS.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 03:30 UTC (`hour=3, minute=30`).
- **Timing Rationale:** Runs after `metadata_cleanup` (03:15 UTC) and before `optimize` (04:00 UTC) to ensure any recovered late files are incorporated into the daily cloud optimization.
- **Configurable Overrides:** `provisioning.cron_full_sweep.enabled` (default: true).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Full S3 LIST on FOS raw prefix, cross-checks SQLite `ingested_files`, ingests missing `.gz` files into local buffer. | Exclusive per-service ingest lock. Gated by `FLA_DEV_NO_CRONS=1` (skips execution). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Workers | Full S3 LIST, bulk inserts unrecorded keys into PostgreSQL `ingest_ledger`, dispatches worker conversion tasks. | Distributed PostgreSQL row locks. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/full-sweep/{service_id}` | Complete audit log in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Standard ingestion disabled for analysts. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side ingested state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Pre-Flight & Lock:** Verifies configuration and acquires per-service ingest lock. Checks `FLA_DEV_NO_CRONS=1` (aborts if set).
2. **Exhaustive FOS LIST:**
   - Issues paginated `FosS3FileSystem.ls(..., refresh=True)` covering the entire service prefix.
   - Collects all `.gz` object keys and metadata (sizes, mtimes).
3. **Difference Calculation against Ledger:**
   - Queries `ingested_files` (Standard) or `ingest_ledger` (High-Scale).
   - Identifies any cloud keys that are not registered as ingested or committed.
4. **Targeted Ingestion of Missing Files:**
   - Downloads missing `.gz` log files in parallel chunks.
   - Decompresses and transforms records into Parquet buffer.
5. **View Update & State Recording:**
   - Updates DuckDB view to expose the newly ingested historical rows.
   - Updates `ingested_files` table with newly captured keys.
6. **Telemetry & Log Progress:**
   - Emits SSE events to `cron_progress`.
   - Records run status, `missing_files_discovered`, and `duration_s` in `cron_runs`.
   - Records FOS Class A LIST/GET operations in `usage_log.db`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 LIST Calls:** Full paginated LIST must be attributed to `cron.full_sync` in `usage_log.db`.
  - **SQLite / Postgres Queries:** Diff check queries must use indexed lookups.
  - **Ingest Telemetry:** Number of discovered missing files must be explicitly logged in `cron_runs.details_json`.
- **Timing & Resource Budgets:**
  - S3 LIST throughput: > 5,000 keys/sec.
  - Ingestion processing: Matches standard ingestion rates (> 50,000 logs/sec per core).
- **Audit Checklist:**
  - Verify that keys already present in `ingested_files` are strictly filtered out without redundant downloads.
  - Confirm execution duration does not exceed the 30-minute maintenance window before `optimize` (04:00 UTC).

---

## 7. Failure Modes & Recovery Runbooks
- **S3 504 / Gateway Timeout on Huge Buckets:** Implements delimiter and prefix-based subdirectory paging to prevent large LIST timeouts.
- **Lock Contention with Standard Sync:** Standard sync yields when full sweep holds the ingest lock.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Full Sweep:** `POST /api/admin/full-sweep/{service_id}`.
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Artificially delete an entry from `ingested_files` while leaving the `.gz` in FOS.
- [ ] 2. Trigger `POST /api/admin/full-sweep/{service_id}`; confirm HTTP 200.
- [ ] 3. Verify in logs: the missing key is discovered and re-ingested.
- [ ] 4. Confirm `ingested_files` contains the restored key.
- [ ] 5. Confirm `usage_log.db` records Class A LIST calls attributed to `cron.full_sync`.
- [ ] 6. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
