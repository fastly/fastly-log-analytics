# Background Job Specification: `rum_sync_{service_id}`

> [!NOTE]
> **Status: VERIFIED & OPERATIONAL (Standard Mode / Ingest Pipeline Audit)**
> Automated test suites verified: `tests/cron/test_rum_sync.py` (25 tests passing), `tests/test_scheduler.py` (7 tests passing), and `tests/test_fastly_realtime_metrics.py` (2 tests passing).

---

## 1. Overview & Objectives
- **Job Identifier:** `rum_sync_{service_id}`
- **Category:** Real User Monitoring (RUM) Ingest & Telemetry
- **Purpose:** Ingests client-side Core Web Vitals (LCP, INP, CLS, TTFB, FCP) and JavaScript error beacons streamed from Fastly to FOS under `raw/rum/**/*.gz`, parsing JSON payloads into DuckDB session tables and local buffer files.
- **Why It Runs:** RUM telemetry provides actual end-user browser performance data. These beacons are collected separately from server-side edge access logs. This job continuously ingests RUM beacons into the analytics pipeline, correlating client metrics with CDN requests via `cid` / `rum_cid`.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Evaluated every `rum_sync_interval_secs` (configured via `rum.sync_interval_seconds` or falling back to `interval_seconds`, min: 5s).
- **Registration Gate (Critical):** Registered **ONLY** if `rum_enabled == true` AND the deployment mode is **Standard** (`DEPLOYMENT_MODE=standard`).
- **Active-Request Politeness Gate:** Evaluates `should_defer_cron("rum_sync", service_id)`. If active user/analyst queries are running on DuckDB, non-manual RUM sync ticks defer to protect query latency and avoid lock contention.
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
1. **Prerequisite & Politeness Check:** Confirms `rum.enabled == true`. Checks `FLA_DEV_NO_CRONS=1`. If not a manual run, checks `should_defer_cron("rum_sync", service_id)`.
2. **Progress Lifecycle Start:** Calls `start_cron_run(service_id, "rum_sync")` returning `run_id`, then calls `start_progress(run_id, service_id=service_id, task="rum_sync")`.
3. **Faro Bundle Integrity & Reconcile:**
   - Calls `_reconcile_faro_bundle(service_id, run_id)` to ensure pinned Faro SDK bundle is present in FOS and live VCL routes to it.
   - If bundle adoption, restore, or drift resync fails, logs warning and marks run as degraded.
4. **FOS LIST Call:** Executes `FosS3FileSystem.ls(f"{bucket}/{prefix}/raw/rum/")`.
5. **Filter Processed Files:** Compares discovered files against SQLite `ingested_rum_files`.
6. **Download & Parse Beacons in Chunks:**
   - Downloads new `.gz` chunks in parallel.
   - Decompresses and extracts JSON beacon payloads:
     - Web Vitals: `lcp`, `inp`, `cls`, `ttfb`, `fcp`, `device_type`, `connection_type`, `effective_type`.
     - Errors: `message`, `source_file`, `lineno`, `colno`, `stack_trace`.
   - On individual file download or parse errors, increments `error_count`, applies the
     shared local quarantine contract for malformed RUM lines, and continues the batch.
7. **Local Parquet Write:** Writes transformed records to `cache/{bucket}/rum/vitals_*.parquet` and `errors_*.parquet`.
8. **SQLite Tracking Update:** Inserts ingested filenames into SQLite `ingested_rum_files`.
9. **Telemetry, Status & Log Recording:**
   - Evaluates run health: if `error_count > 0`, records status `"error"`; a Faro reconcile
     degradation without ingestion failures remains `"warning"`; otherwise `"success"`.
   - Records the shared, zero-filled outcome counters `valid_records`, `malformed_records`,
     `corrupt_containers`, `quarantine_capture_failures`, `source_delete_failures`, and
     `cap_evictions`, plus `objects_processed`, `objects_successful`, `objects_partial`,
     and `objects_failed`.
   - If valid beacons are ingested but FOS deletion fails after bounded retries, the object
     is counted as `objects_failed` and `source_delete_failures` is incremented.
   - Updates `cron_runs` with `duration_s`, `files_downloaded`, `rows_ingested`, and detailed summary.
   - Guaranteed `finally:` block executes `end_progress(run_id)` and `cleanup_progress_and_reap()`.
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
- **Corrupt Beacon Payload:** Malformed JSON beacons are captured as individual local
  quarantine items when possible; valid beacons within the same batch are preserved and the
  run is marked `"error"`.
- **FOS S3 Rate Limit (429):** Backs off exponentially; retries on subsequent interval tick.
- **Faro Reconcile Failure:** Logged as non-fatal warning, preserving beacon ingest while alerting operator via `"warning"` status.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Sync:** `POST /api/admin/rum/sync/{service_id}`.
- **Inspect Status:** `GET /api/admin/rum/status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [x] 1. Upload/mock synthetic RUM beacon `.gz` files in FOS `raw/rum/`.
- [x] 2. Unit & Integration test suites verified: `tests/cron/test_rum_sync.py` (25 passed).
- [x] 3. Scheduler integration verified: `tests/test_scheduler.py` (7 passed).
- [x] 4. Metric recording verified: `tests/test_fastly_realtime_metrics.py` (2 passed).
- [x] 5. Progress tracking and duration finalization verified in `cron_runs`.
- [x] 6. Warning status transitions verified for partial file errors and reconcile failures.
