# Background Job Specification: `rum_sync_{service_id}`

> [!NOTE]
> **Status:** Standard-mode RUM ingestion runs as `rum_sync_{service_id}`.
> High-Scale RUM ingestion is handled by
> [`rum_discovery_{service_id}`](rum-discovery.md). Focused regression coverage
> lives in `tests/cron/test_rum_sync.py` and `tests/core/test_rum_ingest.py`.

---

## 1. Overview & Objectives
- **Job Identifier:** `rum_sync_{service_id}` (Standard mode only)
- **Category:** Real User Monitoring (RUM) Ingest & Telemetry
- **Purpose:** Ingests client-side Core Web Vitals (LCP, INP, CLS, TTFB, FCP) and JavaScript error beacons streamed from Fastly to FOS under `raw/rum/**/*.gz`, parsing JSON payloads into DuckDB session tables and local buffer files.
- **Why It Runs:** RUM telemetry provides actual end-user browser performance data. These beacons are collected separately from server-side edge access logs. This job continuously ingests RUM beacons into the analytics pipeline, correlating client metrics with CDN requests via `cid` / `rum_cid`.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Evaluated on the Standard RUM interval derived from `rum.sync_interval_seconds` or the service sync interval, with a five-second minimum.
- **Registration Gate:** Registered only when RUM is enabled and the deployment mode is Standard (`DEPLOYMENT_MODE=standard`).
- **Active-Request Politeness Gate:** Evaluates `should_defer_cron("rum_sync", service_id)`. Non-manual ticks defer while active queries are running.
- **Mutual Exclusion Rule:** Standard `rum_sync_{service_id}` is not registered in High-Scale mode; the distinct `rum_discovery_{service_id}` worker path owns that mode.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads FOS `raw/rum/**/*.gz`, parses vitals and errors into local Parquet buffers `cache/{bucket}/rum/`. | Per-service RUM ingest lock. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Disabled | Handled by `rum_discovery_{service_id}` through RedBeat and Celery workers; see the separate job specification. | N/A |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/sync/{service_id}` | Full RUM status in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Reads cloud DuckLake tables; skips local RUM ingest. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side RUM data; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite & Politeness Check:** Confirms RUM is enabled and Standard mode is active. Checks `FLA_DEV_NO_CRONS=1`. If not a manual run, checks `should_defer_cron("rum_sync", service_id)`.
2. **Progress Lifecycle Start:** Calls `start_cron_run(service_id, "rum_sync")` returning `run_id`, then calls `start_progress(run_id, service_id=service_id, task="rum_sync")`.
3. **Faro Bundle Integrity & Reconcile:**
   - Calls `_reconcile_faro_bundle(service_id, run_id)` to ensure pinned Faro SDK bundle is present in FOS and live VCL routes to it.
   - If bundle adoption, restore, or drift resync fails, logs a RUM-specific warning;
     this does not change the shared ingestion/quarantine contract.
4. **FOS LIST Call:** Lists the `raw/rum/` prefix through the shared FOS client.
5. **Filter Processed Files:** Compares discovered files against
   PostgreSQL `ingested_files` metadata for both RUM tables.
6. **Download & Parse Beacons in Chunks:**
   - Downloads new `.gz` chunks in parallel.
   - Decompresses and extracts JSON beacon payloads:
     - Web Vitals: `lcp`, `inp`, `cls`, `ttfb`, `fcp`, `device_type`, `connection_type`, `effective_type`.
     - Errors: `message`, `source_file`, `lineno`, `colno`, `stack_trace`.
   - Uses the same ingestion contract as request logs: valid beacons continue to their
     normal buffers, each malformed beacon becomes an individual local exact-byte
     quarantine item, corrupt gzip becomes one item, and processing continues.
7. **Local Parquet Write:** Writes transformed records to the Standard RUM buffer and refreshes the active DuckDB view.
8. **Ingest Tracking Update:** Inserts ingested filenames into PostgreSQL
   `ingested_files` metadata for `client_vitals` and `client_errors`.
9. **Retention Cleanup:** `rum.delete_after` is age-based cleanup, not
   acknowledgement of files processed by this run. Cleanup failures are logged
   and excluded from current-run object counters. Standard RUM does not
   immediately delete each processed source object; `source_delete_failures`
   applies only when a path explicitly schedules per-object deletion.
10. **Telemetry, Status & Log Recording:**
    - Persists the zero-filled request/RUM outcome counters in `cron_runs`.
      Malformed records, corrupt containers, evidence-capture failures, and
      other data-plane failures remain `"error"`.
    - Faro reconciliation is the only warning-only condition. A Faro warning
      may set an otherwise successful run to `"warning"`; it must never
      downgrade a data-plane `"error"`.
    - FOS operations are attributed to the PostgreSQL `usage_log` table.
    - Progress is finalized in the cron wrapper even when ingestion raises.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** S3 operations are attributed to the PostgreSQL `usage_log` table under the `cron.rum_sync` process context.
  - **DuckDB Operations:** Any intermediate DuckDB buffer appends must be tracked in `telemetry_queries`.
  - **PostgreSQL Operations:** Ingest metadata and cron-run state use the unified Postgres metadata store.
- **Timing & Resource Budgets:**
  - Idle discovery tick (0 new files): < 200ms.
  - Beacon parsing throughput: > 25,000 beacons/sec per core.
- **Audit Checklist:**
  - Verify that standard access logs and RUM beacons are strictly segregated on disk.
  - Verify zero Class A API call waste on empty runs.

---

## 7. Failure Modes & Recovery Runbooks
- **Corrupt Beacon Payload:** Uses the same malformed-record handling as request logs:
  individual local exact-byte quarantine items when possible, valid beacons preserved,
  and the run marked `"error"` for any malformed record, failed object, or evidence
  operation.
- **FOS S3 Rate Limit (429):** Backs off exponentially; retries on subsequent interval tick.
- **Faro Reconcile Failure:** Logged as a warning, preserving beacon ingest. A Faro-only issue yields `"warning"`; any RUM data-plane failure keeps the run at `"error"`.

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
