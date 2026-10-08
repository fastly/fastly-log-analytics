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
  - **FOS S3 Calls:** Class B GET calls for metadata pointers must be attributed to `cron.sync_metadata` in PostgreSQL's `usage_log` table.
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
Verified comprehensively via dedicated contract test suite `tests/cron/test_sync_metadata_contract.py` (20 contract tests across all operational boundaries) and `tests/test_scheduler.py`:
- [x] 1. **Role Gate Isolation:** `sync_metadata_{service_id}` registers strictly for Analyst Path A standalone instances (`access_level: "read_only"`), never for Admin (`read_write`) or Analyst Path B (`remote_sessions` / remote share). Verified via `test_contract_role_gate_analyst_only`.
- [x] 2. **Cadence & Derivation:** Interval timer evaluated every `interval_seconds` (derived from `log_period` or `cron_sync.interval_seconds` / `cron_metadata_sync.interval_seconds`) with `coalesce=True` and `misfire_grace_time=60`. Verified via `test_contract_cadence_and_derivation` and `test_contract_initial_startup_sync_registration`.
- [x] 3. **Dynamic Registration & Gating:** Dynamically reschedules when `interval_seconds` updates and unregisters when `cron_metadata_sync.enabled = False` or service is removed. Verified via `test_cron_metadata_sync_enabled_toggle`, `test_contract_dynamic_rescheduling_on_interval_change`, and `test_contract_teardown_unregisters_sync_metadata`.
- [x] 4. **Politeness Deferral:** Scheduled runs (`run_id is None`) defer via `should_defer_cron("metadata_sync", service_id)` when interactive dashboard queries are active; manual trigger (`POST /api/admin/rebuild-local-view`) explicitly bypasses politeness deferral. Verified via `test_contract_politeness_deferral_scheduled` and `test_contract_manual_trigger_bypasses_politeness`.
- [x] 5. **Graceful Uncommitted Table Handling:** When DuckLake/Iceberg table has not yet been committed by the admin (`ducklake_table_exists == False` or attach fails with not found / does not exist), logs `success` with skip summary, avoiding false alert errors on fresh services. In contrast, catalog attach failures (`init_iceberg_table` returning None) raise RuntimeError and log `status="error"`. Verified via `test_contract_graceful_uncommitted_table_handling` and `test_contract_attach_table_failure_raises`.
- [x] 6. **Time Range Bounds:** Pinned `time_range` boundary persisted to `provisioning.time_range` on scoped syncs, cleared on manual "Sync All" (`run_id is not None` with no start time). Verified via `test_contract_scoped_time_range_persisted` and `test_contract_sync_all_clears_time_range`.
- [x] 7. **Admin State Sync Resilience:** Captures `import_admin_state(service_id)` failures without failing the data sync, logging status `"warning"` in `cron_runs`. Verified via `test_contract_admin_state_sync_resilience`.
- [x] 8. **Zero Class A Cloud Mutations:** Analyst standalone instance executes only Class B GET operations (`s3.download_file`) attributed to `cron.sync_metadata` in `usage_log`, never executing FOS Class A PUT or DELETE operations. Verified via `test_contract_zero_class_a_cloud_mutations`.
- [x] 9. **Guaranteed Cleanup:** `end_progress(run_id)` and `finalize_cron_duration` executed in a guaranteed `finally` block on both success and error. Verified via `test_contract_guaranteed_cleanup_in_finally`.
- [x] 10. **Manual Trigger Endpoints:** `POST /api/admin/rebuild-local-view` returns 202 Accepted, clears caches, spawns background worker, and returns 503 on `cron_busy`. Analyst Path B sessions are blocked with 403 `{"error": "admin_only"}`. Verified via `test_contract_rebuild_local_view_endpoint_success`, `test_contract_rebuild_local_view_endpoint_busy`, and `test_contract_path_b_blocked_from_rebuild_local_view`.
