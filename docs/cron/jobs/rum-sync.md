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
- **Active-Request Politeness Gate:** Removed (mirroring commit `5a7541fd`): RUM freshness requires that automated ticks proceed without deferral by active queries.
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
1. **Prerequisite Check:** Confirms RUM is enabled and Standard mode is active. Checks `FLA_DEV_NO_CRONS=1`. No start-of-tick active-request deferral (mirroring commit `5a7541fd`): a deferred tick waits a full interval, and dashboard/SSE polling kept it tripped.
2. **Progress Lifecycle Start:** Calls `start_cron_run(service_id, "rum_sync")` returning `run_id`, then calls `start_progress(run_id, service_id=service_id, task="rum_sync")`.
3. **Faro Bundle Integrity & Reconcile:**
   - Calls `_reconcile_faro_bundle(service_id, run_id)` to ensure pinned Faro SDK bundle is present in FOS and live VCL routes to it.
   - If bundle adoption, restore, or drift resync fails, logs a RUM-specific warning;
     this does not change the shared ingestion/quarantine contract.
4. **FOS LIST Call (Incremental Minute-Prefix Discovery):** Lists through `list_fos_files(..., prefix_subpath="raw/rum/", incremental_only=True)`. Queries the recent minute-prefixes (`rum_minute_list_prefix`, last 5 minutes) first, with a 4-hour `StartAfter` lookback bound. An idle tick issues ≤ 5 LIST calls regardless of `raw/rum/` size. Any LIST error immediately surfaces as a run `"error"`.
5. **Filter Processed Files:** Compares discovered files against
   PostgreSQL `ingested_files` metadata for both RUM tables.
6. **Download & Parse Beacons in Chunks:**
   - Downloads new `.gz` chunks in parallel using bounded chunks (`CHUNK_SIZE = 50`) under `max_seconds` budget (Trap #41: first chunk is always allowed to execute).
   - Decompresses and extracts beacon payloads using the shared `_parse_rum_beacon_file` and `_parse_rum_line`:
     - Web Vitals: `lcp`, `inp`, `cls`, `ttfb`, `fcp`, `device`, `browser`, `os`, `cid`, `req_id`, `pathname`, `city`, `region`, `country`, `pop`, `tls`.
     - Errors: `error_message`, `error_file`, `error_line`, `error_col`, `pathname`, `browser`, `os`, `device`, `cid`, `req_id`, `city`, `region`, `country`, `pop`, `tls`, `ttfb`.
   - Every line that is invalid JSON, fails to parse, or parses to nothing that should have produced a row (including missing or unparseable timestamps) goes into `corrupt_lines` with a categorized reason and exact-byte local capture via `_quarantine_rum_corrupt_lines`.
   - Corrupt gzip containers go through `_capture_corrupt_container(service_id, "rum", ...)`.
   - On capture failure, marks object failed and adds to deletion-exclusion set.
7. **Local Parquet Write:** Writes transformed records to the Standard RUM buffer (`client_vitals`, `client_errors`) and refreshes the active DuckDB view.
8. **Ingest Tracking Update (Durable Bookkeeping):** Inserts ingested filenames into PostgreSQL `ingested_files` metadata with the *durable* rows actually written per table per file (`row_count = 0` for files that produced no rows for that table). Files that fail download or decompression are excluded so they are retried on the next tick.
9. **Inline Raw Deletion & Stranded Sweep:** Under `resolve_raw_delete_after` (default on; off for high-scale shared source), deletes raw objects inline per chunk right after buffer write and `ingested_files` insert succeed.
   - Excludes unreadable files and files whose quarantine capture failed (`exclude_from_delete = failed_paths | capture_failed_paths`).
   - Counts delete failures in `source_delete_failures`.
   - Reclaims stranded already-ingested objects discovered by `list_fos_files` up to `_STRANDED_DELETE_CAP`.
   - `cleanup_old_rum_logs` remains as an age-only backstop.
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
- [x] 2. Unit & Integration test suites verified: `tests/cron/test_rum_sync.py` (26 passed).
- [x] 3. Scheduler integration verified: `tests/test_scheduler.py` (7 passed).
- [x] 4. Metric recording verified: `tests/test_fastly_realtime_metrics.py` (2 passed).
- [x] 5. Progress tracking and duration finalization verified in `cron_runs`.
- [x] 6. Warning status transitions verified for partial file errors and reconcile failures.
- [x] 7. Incremental minute-prefix discovery: an idle tick issues ≤ 5 LIST calls regardless of `raw/rum/` size.
- [x] 8. Raw RUM objects are deleted after ingest under `resolve_raw_delete_after`; stranded already-ingested objects are reclaimed.
- [x] 9. A LIST failure marks the run `"error"` (never a silent `"success"` with 0 files).
- [x] 10. Time budget (`max_seconds`) with Trap #41 first chunk guarantee verified.
- [x] 11. Shared parser (`_parse_rum_beacon_file`) and quarantine flow per object (`_quarantine_rum_corrupt_lines`, exact bytes).
- [x] 12. Bookkeeping records durable rows written per table (0 for no-data files).
- [x] 13. `convert_rum_object` uses `_ducklake_detach` in `finally` (Trap #35).

---

## 10. Parity with Request Ingestion (Resolved)

RUM and request ingestion now share strategy, discovery, deletion, error handling, quarantine, and bookkeeping contracts.

### Resolved Parity Gaps (2026-10-07)

| # | Concern | Resolution |
|---|---|---|
| 1 | Discovery scope | `list_fos_files` and `_compute_incremental_start_after` parameterized by `prefix_subpath`, using `rum_minute_list_prefix` for `raw/rum/` with `incremental_only=True`. Idle ticks issue ≤ 5 LIST calls. |
| 2 | Raw deletion after ingest | Deleted inline per chunk after buffer write and `ingested_files` insert under `resolve_raw_delete_after`. Unreadable files and capture failures excluded. Stranded already-ingested files reclaimed. |
| 3 | Time budget | `max_seconds` enforced per tick with Trap #41 first chunk guarantee. |
| 4 | LIST failure | `{"type": "error"}` from `list_fos_files` marks the run `"error"`. |
| 5 | Politeness gate | `should_defer_cron` removed from `rum_sync` and `rum_commit` (mirroring commit `5a7541fd`). |
| 6 | `ingested_files` growth | Restored RUM `ingested_files` trimming in `metadata_cleanup` with window larger than raw retention (`log_retention_days + 1`). |
| 7 | Error handling & quarantine | Shared `_parse_rum_beacon_file` and `_parse_rum_line`. Missing/unparseable timestamps quarantined with categorized reasons. One summary warning per file. |
| 8 | Bookkeeping parity | Durable row counts recorded per file per table (0 for files with no rows for that table). Retries unreadable files. |
| 9 | DuckLake detach safety | `convert_rum_object` (and other convert functions) use `_ducklake_detach` in `finally` (Trap #35). |

### Completed Cleanup (2026-10-07)

- **Duplicate rows deduplicated:** 516,223 duplicate rows removed on Local Standard, 177,943 duplicate rows removed on GCE Standard using `scripts/dedupe_rum_tables.py` on the beacon's natural key through the DuckLake delete path.
- **Stranded raw objects:** Actively reclaimed by the inline stranded-object sweep from gap 2.
