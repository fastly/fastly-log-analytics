> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `log_commit_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `log_commit_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `log_commit_{service_id}`
- **Category:** Durable Lakehouse Storage Commit
- **Purpose:** Flushes transient Parquet buffer files from local disk (`cache/{bucket}/`) into the durable DuckLake table (`ducklake/` in FOS or Postgres catalog).
- **Why It Runs:** Prevents unbounded local disk growth and ensures data durability. Once committed to DuckLake, rows are preserved permanently in cloud object storage and discoverable by all analytical readers.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Configurable by the user per service via `provisioning.cron_sync.commit_interval_mins` (default: 5 minutes, minimum: 1 minute).
- **Service Configuration Binding:**
  - Standard mode: APScheduler runs every `commit_interval_mins`.
  - High-Scale mode: RedBeat schedules every `commit_interval_mins`.
- **Jitter & Misfire Policy:**
  - Jitter: 30 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=commit_interval_mins * 60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads local Parquet buffer, writes consolidated Parquet to FOS `ducklake/`, commits transaction to DuckLake metadata catalog, unlinks local buffer files. | Exclusive per-service commit lock. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Workers | Executes `CALL ducklake_flush_inlined_data('lake')` (durability flush to FOS Parquet), calls `ducklake_merge_adjacent_files('lake')`, and runs `finalize_committed_raw` with 10-minute deletion grace. | Distributed PostgreSQL transaction isolation. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/commit/{service_id}` | Full commit status and history in Admin UI. Displays prominent banner alert when commits fail. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Reads cloud DuckLake table; never commits. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side DuckLake state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
### Standard Mode:
1. **Pre-Flight Checks:** Verify service configuration. Exit immediately if `FLA_DEV_NO_CRONS=1`.
2. **Lock Acquisition:** Acquires the per-service commit lock. If a commit is already in progress, bail gracefully.
3. **Buffer Scan:** Identifies all uncommitted Parquet files in `cache/{bucket}/`.
4. **Data File Consolidation & Upload:**
   - Bundles small buffer files into size-capped Parquet data files.
   - Uploads data files to FOS under the DuckLake data prefix (`ducklake/data/`).
5. **Metadata Catalog Transaction:**
   - Commits new data file references to DuckLake metadata catalog (local `.ducklake` file or Postgres).
   - Atomically updates table snapshot metadata.
6. **Local Buffer Unlink:** Safely unlinks the uploaded local Parquet files.
7. **View Refresh:** Triggers `update_iceberg_view()` so analytical queries immediately read the new DuckLake snapshot.
8. **Logging, Quarantine & Banner Alerts:**
   - Records commit run in SQLite `cron_runs`.
   - If any unreadable buffer files are quarantined or a commit-side cleanup fails,
     `cron_runs.status` must be set to `error` (never `success`); the original raw-source
     ingestion outcome remains represented by the discovery/worker run counters.
   - If commit transaction fails, records `error` in `cron_runs` and triggers an Admin UI banner alert ("Commits to FOS failing").
   - Records FOS Class A PUT calls in `usage_log.db`.

### High-Scale Mode:
1. Queries PostgreSQL `ingest_ledger` for claimed batches where conversion is done.
2. Invokes DuckLake native merge functions on PostgreSQL catalog.
3. Updates status of ledger rows from `claimed` to `committed`.
4. Emits RedBeat completion metrics.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS PUT Calls:** Every committed Parquet file upload must record Class A PUT count and byte size in `usage_log.db`.
  - **DuckLake Catalog Operations:** SQL commit statements against the catalog must be instrumented.
  - **SQLite Updates:** Updates to `cron_runs` and `service_metadata` must flow through `ThreadLocalPool`.
- **Timing & Resource Budgets:**
  - Buffer scan & decision time: < 100ms.
  - S3 Upload throughput: > 50 MB/s.
  - Catalog commit duration: < 500ms.
- **Query & Execution Audit Checklist:**
  - Confirm uncommitted local buffer files are completely unlinked after commit to avoid double-counting.
  - Verify that the DuckDB view cache is invalidated so readers see the new commit without stale-view errors.
  - Confirm zero Class A API call waste.

---

## 7. Failure Modes & Recovery Runbooks
- **Catalog Commit Conflict (OCC):** In DuckLake, multi-writer conflicts trigger automatic retry with exponential backoff.
- **Upload Network Failure:** If FOS upload fails, the local buffer files remain intact on disk. The next commit tick retries upload automatically.
- **Dangling Local Buffer:** If unlinking fails, orphan file reconciliation identifies and purges duplicate buffer files.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Commit:** `POST /api/admin/commit/{service_id}`.
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/commit/{service_id}`; confirm HTTP 200 response.
- [ ] 2. Confirm execution logged in `cron_runs` with non-zero `committed_files` and `committed_rows`.
- [ ] 3. Verify local buffer files in `cache/{bucket}/` are unlinked.
- [ ] 4. Verify FOS `ducklake/data/` contains newly uploaded Parquet files.
- [ ] 5. Run an analytical query against `/api/dashboard/bundle`; confirm newly committed rows are returned.
- [ ] 6. Confirm FOS Class A PUT calls recorded in `usage_log.db`.
- [ ] 7. In High-Scale mode, confirm `ingest_ledger` rows transition to `committed`.
