> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `log_discovery_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `log_discovery_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `log_discovery_{service_id}` (historically `sync_{service_id}`)
- **Category:** Core Ingestion & Stream Ingestion Data Plane
- **Purpose:** Continuously discovers and ingests new `.gz` log files streamed from Fastly Real-Time Log Streaming to Fastly Object Storage (FOS) under `raw/request/**/*.gz`.
- **Why It Runs:** Log data is streamed into FOS continuously. Without discovery and ingestion, new CDN events never enter the analytics pipeline.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Derived from `log_period`:
  - `interval_seconds = max(5, log_period // 2)` if `log_period >= 60`, otherwise `max(5, log_period)`.
- **Configurable Overrides:**
  - `provisioning.cron_sync.interval_mins` (takes top UI priority).
  - `provisioning.cron_sync.interval_seconds` (written by provisioning scripts).
  - `provisioning.cron_sync.lookback_minutes` (controls High-Scale discovery lookback; default: `10`, min: `3`, max: `30`).
  - Minimum hardcoded clamp: 5 seconds.
- **Jitter & Misfire Policy:**
  - Jitter: 2 seconds (1s if interval < 5s).
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads FOS `raw/request/**/*.gz`, transforms to Parquet in local buffer `cache/{bucket}/`, updates session DuckDB `logs` view. | Acquires per-service ingest lock. Gated by `FLA_DEV_NO_CRONS=1` (skips execution). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Workers | Scans rolling 10-minute window (`minute_list_prefix`), inserts discovered keys into PostgreSQL `ingest_ledger` with `discovered` state, claims batches, and dispatches Celery conversion tasks (`convert_batch_files`). | Distributed PostgreSQL row locks (`FOR UPDATE SKIP LOCKED`). Never opens local DuckDB. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/sync/{service_id}` | Full access to raw ingestion status and logs. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Skips ingest; uses `sync_metadata_{service_id}` pull model instead. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side ingested state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
### Standard Mode:
1. **Pre-Flight Checks:** Verify service configuration and FOS credentials. Check `FLA_DEV_NO_CRONS=1` (exits immediately if set).
2. **FOS LIST Call:** Executes `FosS3FileSystem.ls(f"{bucket}/{prefix}/raw/request/")` with pagination.
3. **Filter Known Keys:** Checks discovered keys against SQLite `ingested_files` table (`SELECT filename FROM ingested_files WHERE filename IN (...)`).
4. **Download & Transform:**
   - Downloads new `.gz` chunks in parallel using thread pool.
   - Decompresses gzip stream in memory, extracts custom VCL expressions and standard log schema.
   - Converts rows to size-optimized Parquet files in `cache/{bucket}/`.
5. **View Update:** Calls `update_iceberg_view()` to stitch local Parquet buffer files into the DuckDB `logs` view.
6. **SQLite Ledger Commit:** Records successfully ingested files into SQLite `ingested_files`.
7. **Throttled Heavy Refresh:** If `_claim_heavy_refresh(service_id)` succeeds (at most once every 30s):
   - Triggers `update_top_values()` (100k reservoir sample backing autocomplete; short-circuits in <1ms via fingerprint cache if data has not changed).
   - Triggers `reconcile_fastly_stats()` (Fastly `/stats/aggregate` billing reconciliation).
8. **Progress & Status Update:** Emits `cron_progress` SSE event and records execution run in `cron_runs`. Any failed log line, quarantine-capture failure, or FOS deletion failure marks the run `error` (never `success` or `warning`). The run records a stable, zero-filled counter schema shared with RUM ingestion: `valid_records`, `malformed_records`, `corrupt_containers`, `quarantine_capture_failures`, `source_delete_failures`, and `cap_evictions`. It also records source-object counters for `objects_processed`, `objects_successful`, `objects_partial`, and `objects_failed`.
   - If records ingest successfully but FOS deletion fails after bounded retries, the
     object is counted as `objects_failed` and `source_delete_failures` is incremented.

### High-Scale Mode:
1. Issues FOS LIST on prefix (rolling 10-minute window).
2. Performs batch `INSERT INTO ingest_ledger (service_id, filename, status) VALUES (...) ON CONFLICT DO NOTHING`.
3. Selects batches using `UPDATE ingest_ledger SET status = 'claimed', worker_id = %s, claimed_at = NOW() WHERE status = 'discovered' ... RETURNING filename`.
4. Enqueues conversion tasks to Celery queue (`convert_batch_files.delay(...)`).
5. **Stateless Workers:** Worker processes skip local DuckDB heavy refresh to avoid file-lock contention with readers; autocomplete cache updates run on the serving web-pod.
6. **Quarantine Handling:** Valid rows continue ingesting when individual lines are malformed. Each bad line is captured as a separate exact-byte item under `data/services/{service_id}/quarantine/`; corrupt gzip containers are captured as one complete gzip item. Metadata records request/RUM source type, original FOS key, line ordinal, byte offset/length when known, normalized error category, bounded error text, and SHA-256. The source FOS object is always deleted after processing, even if capture fails; capture failures are recorded and the run is marked `error`. Quarantine is diagnostic evidence, not a re-ingest queue. High-Scale workers write to shared quarantine storage with bounded retries for transient storage failures, while the serving pod owns the admin surface and cap enforcement.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 API Calls:** Every `LIST` and `GET` operation must be attributed to `cron.log_discovery` and recorded in `usage_log.db` (Class A/B call tracking).
  - **SQLite Operations:** Every `SELECT` and `INSERT` against `ingested_files`, `cron_runs`, and `metadata.db` must flow through `ThreadLocalPool` with timings.
  - **DuckDB Operations:** View update DDL statements (`CREATE OR REPLACE VIEW logs AS ...`) must be tracked in `telemetry_queries`.
  - **Fastly API Calls:** Reconcile calls to Fastly `/stats/aggregate` must log latency and response codes.
- **Timing & Resource Budgets:**
  - Standard discovery tick (steady-state, 0 new files): < 250ms total execution.
  - Ingestion processing: > 50,000 logs/sec per core.
  - SQLite commit lock wait: < 50ms.
- **Audit Checklist:**
  - Confirm zero Class A API call proliferation (validate LIST pagination).
  - Verify `ingested_files` query uses primary index on `filename`.
  - Confirm heavy refresh is strictly clamped to the 30s throttle window.

---

## 7. Failure Modes & Recovery Runbooks
- **FOS Rate Limiting / 429:** Bounded exponential backoff with retry; exhausted failures mark the run `error`, record the source key and error, and continue other files where safe.
- **Corrupted `.gz` File / Bad Rows:** Captures exact local evidence when possible, records the error when capture fails, deletes the FOS source regardless, marks the run `error`, and continues the remaining batch. Every line receives a durable success/failure outcome.
- **FOS Deletion Failure:** Bounded retry is attempted. If deletion still fails, the run is marked `error`, the source key and deletion error are recorded, and other files continue.

### Quarantine retention and admin surface
- The cap is 1,000 items per service, shared by malformed lines and corrupt gzip items.
- There is no age-based expiry and no configurable override. New writes enforce the cap
  immediately by evicting oldest individual items; failures are recorded while other
  eligible evictions continue.
- Evidence is admin/read-write only. Both Analyst Path A and Analyst Path B are denied by
  backend authorization, regardless of UI visibility.
- The UI treats each line as an individual item, paginates results, supports source-type
  and error-category filters, shows readable previews by default, and provides authenticated
  exact-byte streaming downloads with size limits. Individual purge and service-scoped
  purge-all are supported. No separate quarantine cron is required.
- **Stale Buffer View Race:** Handled via `execute_with_stale_view_retry()` clearing view cache and rebuilding.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Ingestion:** `POST /api/admin/sync/{service_id}` (supports `?force=true`).
- **Inspect Live Progress:** `GET /api/admin/cron/progress/{service_id}` (SSE stream).
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/sync/{service_id}` with synthetic `.gz` files in FOS; verify HTTP 200 response.
- [ ] 2. Confirm execution records in `cron_runs` with status `success` and non-zero `files_ingested`.
- [ ] 3. Verify `usage_log.db` attributes FOS Class A LIST and Class B GET calls to `cron.log_discovery`.
- [ ] 4. Confirm new rows immediately queryable via `GET /api/dashboard/bundle`.
- [ ] 5. Confirm heavy refresh phases (`update_top_values`, `reconcile_fastly_stats`) run no more than once per 60s.
- [ ] 6. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
- [ ] 7. In High-Scale mode, verify rows transition properly in `ingest_ledger` (`discovered → claimed`).
- [ ] 8. Verify malformed-line and corrupt-gzip outcomes, per-category counters, `error` status,
  immediate per-service cap eviction, private evidence downloads, and Analyst Path A/B denial.
