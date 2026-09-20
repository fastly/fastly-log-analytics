> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `rum_commit_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `rum_commit_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `rum_commit_{service_id}`
- **Category:** Durable RUM Lakehouse Commit
- **Purpose:** Flushes transient RUM Parquet buffer files from local disk (`cache/{bucket}/rum/`) into the durable DuckLake tables `client_vitals` and `client_errors` in cloud storage.
- **Why It Runs:** Just like standard access logs, RUM telemetry must be durably stored in DuckLake to prevent local disk exhaustion and ensure multi-node / multi-analyst query discovery.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Configured via `rum.commit_interval_mins` (default matches `cron_sync.commit_interval_mins`, typically every 5 minutes).
- **Registration Gate:** Registered **ONLY** if `rum.enabled == true` AND `DEPLOYMENT_MODE == "standard"`.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads local RUM Parquet buffer, writes to FOS `ducklake/rum/`, commits to DuckLake catalog tables `client_vitals` and `client_errors`. | Exclusive per-service RUM commit lock. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Disabled | Handled automatically by high-scale batch publication pipelines. | N/A |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/commit/{service_id}` | Full RUM commit statistics in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Reads cloud DuckLake tables. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side RUM data; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Pre-Flight Checks:** Verifies RUM is enabled and acquires the per-service RUM commit lock. Checks `FLA_DEV_NO_CRONS=1`.
2. **Buffer Scan:** Scans `cache/{bucket}/rum/` for uncommitted vitals and error Parquet files.
3. **Table Commit Execution:**
   - Appends vitals files to DuckLake table `client_vitals`.
   - Appends error files to DuckLake table `client_errors`.
4. **Cloud Upload:** Writes consolidated Parquet data files to FOS under the DuckLake prefix.
5. **Catalog Transaction:** Commits snapshots to DuckLake catalog.
6. **Local Buffer Unlink:** Safely unlinks the committed local RUM Parquet files.
7. **Telemetry & Log Recording:**
   - Records commit metrics in `cron_runs` (`vitals_rows_committed`, `error_rows_committed`).
   - Logs FOS Class A PUT calls in `usage_log.db`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** S3 uploads must be logged in `usage_log.db` as Class A PUT calls.
  - **DuckLake Procedures:** DDL/DML commit procedures must be recorded in `telemetry_queries`.
  - **Lock Contention:** Wait time for RUM commit lock must remain < 50ms.
- **Timing & Resource Budgets:**
  - Commit transaction duration: < 1.0 second.
  - S3 upload throughput: > 50 MB/s.
- **Audit Checklist:**
  - Verify that committed local buffer files are unlinked after successful catalog commit.
  - Confirm analytical queries against `/api/rum/overview` immediately reflect the newly committed beacons.

---

## 7. Failure Modes & Recovery Runbooks
- **Catalog Commit Conflict:** Automatically retries with exponential backoff.
- **Upload Network Failure:** Local RUM buffer files are preserved; next interval tick retries upload cleanly.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Commit:** `POST /api/admin/rum/commit/{service_id}`.
- **Inspect Status:** `GET /api/admin/rum/status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/rum/commit/{service_id}`; confirm HTTP 200.
- [ ] 2. Confirm in `cron_runs`: status `success` with non-zero committed counts.
- [ ] 3. Verify local files in `cache/{bucket}/rum/` are unlinked.
- [ ] 4. Confirm in FOS: new Parquet files exist under `ducklake/`.
- [ ] 5. Query `/api/rum/overview`; confirm newly committed vitals are returned.
