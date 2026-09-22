# Background Job Specification: `sync_metadata_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `sync_metadata_{service_id}`
- **Category:** Analyst Path A Synchronization & Remote Catalog Discovery
- **Purpose:** Periodically pulls metadata and table snapshot pointers from Fastly Object Storage (FOS) into local standalone Analyst instances (`access_level: "read_only"`), keeping their local analytical views and admin state synchronized with commits performed by the Admin instance.
- **Why It Runs:** In the Analyst Path A deployment model, analysts run independent local copies of the application with read-only FOS bucket credentials. They do not ingest raw logs or commit snapshots. This job regularly discovers new DuckLake / Iceberg metadata snapshots committed to the shared FOS bucket by the Admin, updating the analyst's local DuckDB view so new data appears on their dashboard.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`) + Startup trigger
- **Default Schedule:** Evaluated every `interval_seconds` (derived from `log_period` or `cron_sync.interval_seconds`).
- **Startup Execution:** Fires immediately at application boot so the analyst's dashboard is populated upon launch.
- **Role Gate (Critical):** Registered **ONLY** for services configured with `access_level: "read_only"`. For Admin instances (`read_write`), this job is omitted because Admins update their local view immediately after each commit.
- **Configurable Overrides:**
  - `provisioning.cron_metadata_sync.enabled` (default: `true`).
  - `provisioning.cron_metadata_sync.interval_seconds` (overrides default sync interval).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process on Analyst) | Reads cloud catalog pointers from FOS `ducklake/` or `iceberg/meta/`, updates local DuckDB view. | Read/write connection to local DuckDB view. Active request politeness gate (`should_defer_cron`) defers scheduled runs during active dashboard queries. Permitted under `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | N/A (See ADR-17) | Known limitation in v3.0.0-beta1: DuckLake catalog requires Postgres discovery; see ADR-17 for proposed analyst cloud discovery. | N/A |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Disabled | N/A (Runs inline on commit) | Admins update view directly upon commit. |
| **Analyst Path A (Standalone Instance)** | Active | `POST /api/admin/rebuild-local-view` | Refreshes analyst dashboard from cloud. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Shares admin's live process; no metadata sync needed. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Role & Source Verification:**
   - Loads config via `svcconfig.load_config(service_id)` and source via `get_source_for_service(service_id)`.
   - Confirms `access_level == "read_only"`.
2. **Politeness Check:**
   - For scheduled runs (`run_id is None`), checks `should_defer_cron("metadata_sync", service_id)`. If active user requests are running on the dashboard, defers to prevent view rebuild lock contention.
   - Manual runs bypass the politeness gate.
3. **Execution & Progress Initialization:**
   - Calls `start_cron_run(src, "metadata_sync")`.
   - Initializes live tracking via `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="metadata_sync")`.
4. **Cloud Catalog Probe & Table Check:**
   - Attaches DuckLake catalog: `db_iceberg.init_iceberg_table(src, create=False)`.
   - Checks table existence: `db_iceberg.ducklake_table_exists(src)`. If no data committed yet, logs success with skip message and exits cleanly.
5. **Data File Synchronization:**
   - Calls `db_iceberg.sync_data(src, ...)` to download newly committed parquet files to the analyst's local cache directory.
   - Respects optional `time_range` boundary if configured.
6. **DuckDB View Rebuild:**
   - Opens connection `get_connection(source=src, read_only=False)` and executes `update_iceberg_view(con, src)` to bind local parquet files into the session `logs` view.
7. **Admin State Sync (Shared Views, Custom Fields, History):**
   - Calls `import_admin_state(service_id)`.
   - If `import_admin_state` fails (e.g. cloud network blip), captures `import_error`, emits warning progress event, and marks the cron run status as `"warning"`.
8. **Cache Invalidation & Telemetry Finalization:**
   - Refreshes config status (`refresh_config_status`) and invalidates dashboard cache (`invalidate_service`).
   - Logs `run_status` (`"success"` or `"warning"`) in `cron_runs` with file/row counts.
   - In guaranteed `finally:` block: calls `end_progress(run_id)` and `finalize_cron_duration(src, run_id, start_time_exec)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** Class B GET calls for metadata pointers must be attributed to `cron.sync_metadata` in `usage_log.db`.
  - **Zero FOS Class A PUTs:** Analysts must never execute PUT or DELETE operations against FOS.
  - **DuckDB DDL:** View refresh queries must be tracked in `telemetry_queries`.
- **Timing & Resource Budgets:**
  - Steady-state check (no new snapshot): < 250ms.
  - View reload execution: < 1.0s.
- **Audit Checklist:**
  - Verify that the analyst instance never modifies or overwrites cloud metadata.
  - Verify that custom fields and saved views from the admin are properly synchronized to the analyst.
  - Verify active dashboard queries cause scheduled metadata sync to defer cleanly.

---

## 7. Failure Modes & Recovery Runbooks
- **Active Dashboard Load:** Politeness gate defers metadata sync until interactive query window closes.
- **FOS Network Timeout:** Logs warning in `cron_runs`; local dashboard continues serving existing local view.
- **Stale View Error:** Analyst queries use `execute_with_stale_view_retry()` to force an immediate metadata reload if a buffer file was committed by the admin.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Rebuild Local View:** `POST /api/admin/rebuild-local-view?service_id={service_id}`.
- **Check Sync Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. Automated Verification Matrix
- [x] 1. Active request politeness deferral for scheduled runs verified via `test_run_metadata_sync_defers_when_active_requests_present`.
- [x] 2. Manual trigger bypass of politeness gate verified via `test_run_metadata_sync_does_not_defer_when_manual`.
- [x] 3. Warning status on `import_admin_state` failure verified via `test_run_metadata_sync_status_warning_when_import_admin_state_fails`.
- [x] 4. Dynamic rescheduling on `cron_metadata_sync.interval_seconds` change verified via `test_sync_jobs_reschedules_metadata_sync_when_interval_changed`.
- [x] 5. Job disabled when `cron_metadata_sync.enabled = False` verified via `test_sync_jobs_skips_metadata_sync_when_disabled`.
- [x] 6. Graceful handling of uncommitted table verified via `test_run_metadata_sync_skips_when_the_ducklake_table_does_not_exist_yet`.
- [x] 7. Time range persistence and clearing verified via `test_run_metadata_sync_persists_time_range_when_explicit_args_provided` and `test_run_metadata_sync_clears_time_range_on_manual_sync_all`.
