> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `rum_sync_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `rum_sync_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `rum_sync_{service_id}`
- **Category:** Real User Monitoring (RUM) Ingest & Telemetry
- **Purpose:** Ingests client-side Core Web Vitals (LCP, INP, CLS, TTFB, FCP) and JavaScript error beacons streamed from Fastly to FOS under `raw/rum/**/*.gz`, parsing JSON payloads into DuckDB session tables and local buffer files.
- **Why It Runs:** RUM telemetry provides actual end-user browser performance data. These beacons are collected separately from server-side edge access logs. This job continuously ingests RUM beacons into the analytics pipeline, correlating client metrics with CDN requests via `cid` / `rum_cid`.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Evaluated every `rum_sync_interval_secs` (default matches `cron_sync.interval_seconds`, min: 5s).
- **Registration Gate (Critical):** Registered **ONLY** if `rum_enabled == true` AND the deployment mode is **Standard** (`DEPLOYMENT_MODE=standard`).
- **Mutual Exclusion Rule:** Must never be registered concurrently with `rum_discovery_{service_id}` to prevent duplicate ingestion into DuckLake `client_vitals` and `client_errors`.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads FOS `raw/rum/**/*.gz`, parses vitals and errors into local Parquet buffers `cache/{bucket}/rum/`. | Per-service RUM ingest lock. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Disabled | Handled by `rum_discovery_{service_id}` via Celery workers. | N/A |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/sync/{service_id}` | Full RUM status in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Reads cloud DuckLake tables; skips local RUM ingest. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side RUM data; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Check:** Confirms `rum.enabled == true`. Checks `FLA_DEV_NO_CRONS=1`.
2. **FOS LIST Call:** Executes `FosS3FileSystem.ls(f"{bucket}/{prefix}/raw/rum/")`.
3. **Filter Processed Files:** Compares discovered files against SQLite `ingested_rum_files`.
4. **Download & Parse Beacons:**
   - Downloads new `.gz` chunks in parallel.
   - Decompresses and extracts JSON beacon payloads:
     - Web Vitals: `lcp`, `inp`, `cls`, `ttfb`, `fcp`, `device_type`, `connection_type`, `effective_type`.
     - Errors: `message`, `source_file`, `lineno`, `colno`, `stack_trace`.
5. **Local Parquet Write:** Writes transformed records to `cache/{bucket}/rum/vitals_*.parquet` and `errors_*.parquet`.
6. **SQLite Tracking Update:** Inserts ingested filenames into SQLite `ingested_rum_files`.
7. **Telemetry & Log Recording:**
   - Emits progress event to `cron_progress`.
   - Records run status, `beacons_ingested`, and duration in `cron_runs`.
   - Logs FOS Class A LIST/GET calls in `usage_log.db`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** S3 operations must be recorded in `usage_log.db` under `cron.rum_sync`.
  - **DuckDB Operations:** Any intermediate DuckDB buffer appends must be tracked in `telemetry_queries`.
  - **SQLite Operations:** Ledger updates must flow through `ThreadLocalPool`.
- **Timing & Resource Budgets:**
  - Idle discovery tick (0 new files): < 200ms.
  - Beacon parsing throughput: > 25,000 beacons/sec per core.
- **Audit Checklist:**
  - Verify that standard access logs and RUM beacons are strictly segregated on disk.
  - Verify zero Class A API call waste on empty runs.

---

## 7. Failure Modes & Recovery Runbooks
- **Corrupt Beacon Payload:** Malformed JSON beacons are routed to error logs; valid beacons within the same batch are preserved.
- **FOS S3 Rate Limit (429):** Backs off exponentially; retries on subsequent interval tick.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Sync:** `POST /api/admin/rum/sync/{service_id}`.
- **Inspect Status:** `GET /api/admin/rum/status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Upload synthetic RUM beacon `.gz` files to FOS `raw/rum/`.
- [ ] 2. Trigger `POST /api/admin/rum/sync/{service_id}`; confirm HTTP 200.
- [ ] 3. Verify in logs: beacons are parsed and written to local Parquet buffer.
- [ ] 4. Confirm `ingested_rum_files` table records the new filenames.
- [ ] 5. Confirm in `cron_runs`: status `success` with non-zero beacon count.
- [ ] 6. Confirm FOS Class A LIST/GET calls logged in `usage_log.db`.
