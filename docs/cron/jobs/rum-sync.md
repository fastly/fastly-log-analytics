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
- **Active-Request Politeness Gate:** Evaluates `should_defer_cron("rum_sync", service_id)`. Non-manual ticks defer while active queries are running. `log_discovery` no longer has this gate, so it is a parity gap slated for removal (§10, gap 5).
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
   Current behavior calls `list_fos_files(..., incremental_only=False)`, which
   lists the entire `raw/rum/` tree every tick. See
   [§10 Parity Gaps](#10-parity-with-request-ingestion-open-work) — this is the
   main source of `rum_sync` cost and host contention in Standard mode.
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
- [ ] 7. Incremental minute-prefix discovery: an idle tick issues ≤ 5 LIST calls regardless of `raw/rum/` size.
- [ ] 8. Raw RUM objects are deleted after ingest under `resolve_raw_delete_after`; stranded already-ingested objects are reclaimed.
- [ ] 9. A LIST failure marks the run `"error"` (never a silent `"success"` with 0 files).
- [ ] 10. `rum_sync` p95 duration < 5 s on GCE Standard under the 5 RPS seeder; request lag p90 < 20 s on the same host.

---

## 10. Parity with Request Ingestion (Open Work)

RUM and request ingestion are meant to share strategy and code. Standard
`rum_sync` has drifted from `log_discovery`. High-Scale `rum_discovery` already
matches the request path: it uses `rum_minute_list_prefix`, and
`finalize_committed_raw` deletes raw objects after commit. Standard mode is the
outlier.

### Observed impact (GCE Standard, 4 vCPU, 2026-10-07)

`rum_sync` runs every 30 s and takes 23–52 s per run, so it is effectively
always running. It contends with `log_discovery`, whose header
`refresh_config_status` varies from 1.3 s to 13 s. Request lag stays at a
median of about 22 s with a p90 of about 34 s, above the 20 s SLA. Local
Standard and High-Scale both pass.

### Gap table

| # | Concern | Request (`ingest` / `log_discovery`) | Standard RUM (`ingest_rum_logs` / `rum_sync`) | Target |
|---|---|---|---|---|
| 1 | Discovery scope | `incremental_only=True`: last 5 minute-prefixes, then a 4 h `StartAfter` fallback | `incremental_only=False`: full `raw/rum/` LIST every tick | Same incremental path. `list_fos_files` minute-prefix gate and `_compute_incremental_start_after` are hard-coded to `raw/request/`; parameterize them by prefix (reuse `rum_minute_list_prefix`). |
| 2 | Raw deletion after ingest | `delete_after` (`resolve_raw_delete_after`, default on) deletes inline after buffer write. Stranded already-ingested objects are reclaimed (capped). | Never deletes per-object. Only age-based `cleanup_old_rum_logs` (opt-in `rum.delete_after` days) runs, and it LISTs the whole prefix again. | Same `delete_after` contract and stranded-object reclaim. Keep the high-scale shared-source stand-down. |
| 3 | Time budget | `max_seconds`; first chunk always runs (Trap #41) | No budget | Same budget and first-chunk guarantee |
| 4 | LIST failure | Error event surfaced to the run | `{"type": "error"}` events are dropped; the run records `"success"` with 0 files | Record `"error"` |
| 5 | Politeness gate | Removed from `log_discovery` (`5a7541fd`) and absent from `commit` | `should_defer_cron` still gates `rum_sync` and `rum_commit` | Remove it, matching the request path |
| 6 | `ingested_files` growth | Trimmed by `metadata_cleanup` | Exempt from trimming (`11e17bff`), because raw objects outlive their rows and a full LIST would re-ingest them; grows without limit | After gap 2 lands, restore trimming with a window larger than raw retention |
| 7 | Code sharing | `ingest()` | Separate ~500-line `ingest_rum_logs` that re-implements chunking, download, in-flight and outcome handling | Converge on one chunk loop parameterized by a table/parse spec (follow-up, after 1–6) |

### Cleanup owed once the gaps close

- Duplicate `client_vitals` / `client_errors` rows written by the 2026-10-06
  re-ingest (Local and likely GCE). Dedupe on the beacon's natural key, using
  the same DuckLake delete path as retention, after a dry-run count.
- Already-ingested raw objects still in `raw/rum/`. These are reclaimed by the
  stranded-object sweep from gap 2, not by a one-off script.
