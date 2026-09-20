> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `sync_metadata_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `sync_metadata_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `sync_metadata_{service_id}` (also `initial_sync_{service_id}`)
- **Category:** Analyst Path A Synchronization & Remote Catalog Discovery
- **Purpose:** Periodically pulls metadata and table snapshot pointers from Fastly Object Storage (FOS) into local standalone Analyst instances (`access_level: "read_only"`), keeping their local analytical views synchronized with commits performed by the Admin instance.
- **Why It Runs:** In the Analyst Path A deployment model, analysts run independent local copies of the application with read-only FOS bucket credentials. They do not ingest raw logs or commit snapshots. This job regularly discovers new DuckLake / Iceberg metadata snapshots committed to the shared FOS bucket by the Admin, updating the analyst's local DuckDB view so new data appears on their dashboard.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`) + Startup trigger
- **Default Schedule:** Evaluated every `interval_seconds` (derived from `log_period` or `cron_sync.interval_seconds`).
- **Startup Execution:** Fires immediately at application boot (`initial_sync_{service_id}`) so the analyst's dashboard is populated upon launch.
- **Role Gate (Critical):** Registered **ONLY** for services configured with `access_level: "read_only"`. For Admin instances (`read_write`), this job is omitted because Admins update their local view immediately after each commit.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process on Analyst) | Reads cloud catalog pointers from FOS `ducklake/` or `iceberg/meta/`, updates local DuckDB view. | Read-only connection to local DuckDB. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | N/A (See ADR-17) | Known limitation in v3.0.0-beta1: DuckLake catalog requires Postgres discovery; see ADR-17 for proposed analyst cloud discovery. | N/A |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Disabled | N/A (Runs inline on commit) | Admins update view directly. |
| **Analyst Path A (Standalone Instance)** | Active | `POST /api/admin/rebuild-local-view` | Refreshes analyst dashboard from cloud. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Shares admin's live process; no metadata sync needed. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Role Verification:** Confirms `cfg["access_level"] == "read_only"`. If not read-only, safely exits.
2. **Cloud Metadata Probe:**
   - Issues FOS S3 GET for the latest metadata catalog pointer file (`version-hint.text` or DuckLake catalog pointer).
   - Compares remote snapshot version with current local snapshot version.
3. **Skip if Up-to-Date:** If the remote version matches the local view's snapshot ID, exits without reloading.
4. **DuckDB View Rebuild:**
   - If remote version is newer, reloads DuckLake table reference.
   - Executes `CREATE OR REPLACE VIEW logs AS SELECT * FROM ...` in the analyst's DuckDB session.
5. **Admin State Sync (Views & Custom Fields):**
   - Fetches `admin_state.json` from FOS `iceberg/meta/admin_state.json`.
   - Merges shared custom fields, saved views, and log format history into the analyst's local SQLite database.
6. **Telemetry & Log Recording:**
   - Records execution status and latest snapshot ID in `cron_runs`.
   - Records FOS Class B GET operations in `usage_log.db`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** Class B GET calls for metadata pointers must be attributed to `cron.sync_metadata` in `usage_log.db`.
  - **Zero FOS Class A PUTs:** Analysts must never execute PUT or DELETE operations.
  - **DuckDB DDL:** View refresh queries must be tracked in `telemetry_queries`.
- **Timing & Resource Budgets:**
  - Steady-state check (no new snapshot): < 150ms.
  - View reload execution: < 500ms.
- **Audit Checklist:**
  - Verify that the analyst instance never modifies or overwrites cloud metadata.
  - Verify that custom fields and saved views from the admin are properly synchronized to the analyst.

---

## 7. Failure Modes & Recovery Runbooks
- **FOS Network Timeout:** Logs warning to `cron_runs`; local dashboard continues serving existing local view.
- **Stale View Error:** Analyst queries use `execute_with_stale_view_retry()` to force an immediate metadata reload if a buffer file was committed by the admin.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Rebuild Local View:** `POST /api/admin/rebuild-local-view?service_id={service_id}`.
- **Check Sync Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Start a standalone analyst instance (`access_level: "read_only"`).
- [ ] 2. Verify `sync_metadata_{service_id}` is registered in APScheduler.
- [ ] 3. Commit a new snapshot from the Admin instance.
- [ ] 4. Wait for `sync_metadata` tick (or trigger `/api/admin/rebuild-local-view`); verify HTTP 200.
- [ ] 5. Query the analyst dashboard; confirm newly committed admin rows are visible.
- [ ] 6. Verify zero FOS Class A PUT/DELETE calls recorded in analyst's `usage_log.db`.
