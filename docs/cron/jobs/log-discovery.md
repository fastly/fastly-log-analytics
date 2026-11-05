# Background Job Specification: `log_discovery_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `log_discovery_{service_id}` (historically `sync_{service_id}`)
- **Category:** Core Ingestion & Stream Ingestion Data Plane
- **Purpose:** Continuously discovers and coordinates ingestion of new `.gz` / `.zst` log files streamed from Fastly Real-Time Log Streaming to Fastly Object Storage (FOS) under `raw/request/**/*.{zst,gz}`.
- **Why It Runs:** Log data is streamed into FOS continuously. Without discovery and ingestion, new CDN events never enter the analytics pipeline.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Derived from `log_period`:
  - `interval_seconds = max(5, log_period // 2)` if `log_period >= 60`, otherwise `max(5, log_period)`.
- **Configurable Overrides:**
  - `provisioning.cron_sync.interval_mins` (takes top UI priority).
  - `provisioning.cron_sync.interval_seconds` (written by provisioning scripts).
  - Minimum hardcoded clamp: 5 seconds.
- **Jitter & Misfire Policy:**
  - Jitter: 2 seconds (1s if interval < 5s).
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Component | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Unified ClickHouse Architecture** | APScheduler + Stateless Ingest Worker (`python -m backend.high_scale.worker`) | Scans recent minute prefixes on FOS `raw/request/`, records discovered keys in PostgreSQL control-plane ledger (`high_scale_source_ledger` / `ingest_ledger`), claims pages under fenced owner epochs, writes canonical Archive Parquet to FOS (`{prefix}/archive/`), and publishes verified batches to ClickHouse (`request_facts`). | Distributed PostgreSQL row locks & fenced owner epochs. Gated by `FLA_DEV_NO_CRONS=1`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/sync/{service_id}` | Full access to raw ingestion status and logs. |
| **Analyst (Remote Share)** | N/A | Blocked (403) | Reads server-side ingested state; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Pre-Flight Checks:** Verify service configuration and FOS credentials. Check `FLA_DEV_NO_CRONS=1` (exits immediately if set).
   - Unlike the maintenance crons, `log_discovery` has no start-of-tick active-request gate (`should_defer_cron`).
2. **FOS LIST & Discovery:**
   - Issues FOS LIST on the recent minute prefixes (`raw/request/`). The separate `ledger_sweep` job diffs a four-hour FOS lookback against the ledger to catch objects missed by the frequent discovery scan.
   - Records discovered keys in PostgreSQL with `service_id`, `object_key`, `size_bytes`, `status`, `discovered_at`, `originating_task`, and `originating_run_id`.
3. **Stateless Worker Page Pipeline (`backend/high_scale/worker.py`):**
   - Claims batches of objects (`HIGH_SCALE_MAX_BATCH_OBJECTS`, default 10 on lightweight workers, bounded by `HIGH_SCALE_MAX_BATCH_BYTES`, default 6 MiB) under fenced owner epochs.
   - Downloads and decodes objects concurrently (`HIGH_SCALE_OBJECT_CONCURRENCY`, default 4 per worker) in memory.
   - Writes a merged, checksum-verified Archive Parquet file to FOS (`{prefix}/archive/`) and registers the `ArchiveManifest` in PostgreSQL.
   - Bulk-inserts rows into ClickHouse `request_facts` as `pending`, verifies the count, flips the batch publication gate to `visible`, and acknowledges the batch sources in PostgreSQL.
4. **Throttled Heavy Refresh:** If `_claim_heavy_refresh(service_id)` succeeds (at most once every 60s, or on a forced run):
   - Triggers `update_top_values()` (reservoir sample backing autocomplete; short-circuits via fingerprint cache if data has not changed).
   - Runs the usage-log phase: `backfill_fastly_edge_writes()`, `reconcile_fastly_stats()` (Fastly `/stats/aggregate` billing reconciliation), and `run_usage_log_cleanup()`.
5. **Quarantine Handling:**
   - Valid rows continue ingesting when individual lines are malformed. Each bad line is captured as a separate exact-byte item under `data/services/{service_id}/quarantine/`; corrupt gzip containers are captured as one complete gzip item.
   - Metadata records request/RUM source type, original FOS key, line ordinal, byte offset/length when known, normalized error category, bounded error text, and SHA-256.
   - Quarantine is diagnostic evidence, not a re-ingest queue. PostgreSQL advisory locking serializes evidence writes and FIFO-cap enforcement (1,000 items per service).
6. **Progress & Status Update:**
   - Emits `cron_progress` SSE event and records execution run in `cron_runs`. Any failed log line, quarantine-capture failure, or FOS deletion failure marks the run `error` (never `success` or `warning`). The run records a stable, zero-filled counter schema shared with RUM ingestion: `valid_records`, `malformed_records`, `corrupt_containers`, `quarantine_capture_failures`, `source_delete_failures`, and `cap_evictions`, plus `objects_processed`, `objects_successful`, `objects_partial`, and `objects_failed`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 API Calls:** Every `LIST`, `GET`, `PUT`, and `DELETE` operation is attributed in the PostgreSQL `usage_log` table.
  - **PostgreSQL Operations:** Ledger, `ingested_files`, `cron_runs`, and outcome-counter writes use the shared instrumented metadata connection.
  - **ClickHouse Operations:** Publication and verification queries are registered through `query_registry.register("ClickHouse", ...)`.
  - **Fastly API Calls:** Reconcile calls to Fastly `/stats/aggregate` log latency and response codes.
- **Timing & Resource Budgets:**
  - Discovery tick (steady-state, 0 new files): < 250ms total execution.
  - Worker memory ceiling: <= 2 GiB under default lightweight batch settings (`HIGH_SCALE_WORKER_THREADS=1`, `HIGH_SCALE_MAX_BATCH_BYTES=6291456`, `HIGH_SCALE_MAX_BATCH_OBJECTS=10`).
- **Audit Checklist:**
  - Confirm zero Class A API call proliferation (validate LIST pagination).
  - Confirm heavy refresh is strictly clamped to the 60s throttle window.

---

## 7. Failure Modes & Recovery Runbooks
- **FOS Rate Limiting / 429:** Bounded exponential backoff with retry; exhausted failures mark the run `error`, record the source key and error, and continue other files where safe.
- **Corrupted `.gz` File / Bad Rows:** Captures exact local evidence when possible, records the error when capture fails, deletes the FOS source regardless, marks the run `error`, and continues the remaining batch. Every line receives a durable success/failure outcome.
- **FOS Deletion Failure:** Bounded retry is attempted. If deletion still fails, the run is marked `error`, the source key and deletion error are recorded, and other files continue.

### Quarantine retention and admin surface
- The cap is 1,000 items per service, shared by malformed lines and corrupt gzip items.
- There is no age-based expiry and no configurable override. New writes enforce the cap immediately by evicting oldest individual items.
- Evidence is admin-only. Analysts are denied by backend authorization, regardless of UI visibility.
- `GET /api/admin/quarantine/summary` reports item/byte counts, request/RUM counts, category counts, and time extents. `GET /api/admin/quarantine/download/{item_id}` streams exact bytes with a 50 MiB limit. `POST /api/admin/quarantine/purge` purges one item via `item_id` or all items when omitted.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Ingestion:** `POST /api/admin/sync/{service_id}` (supports `?force=true`).
- **Inspect Live Progress:** `GET /api/cron-runs/{run_id}/stream` (SSE stream).
- **Inspect Status:** `GET /api/admin/sync-status?service_id={service_id}` and `/admin/clickhouse`.

---

## 9. Verification Checklist
- 1. Trigger `POST /api/admin/sync/{service_id}` with synthetic `.gz` files in FOS; verify HTTP 200 response.
- 2. Confirm execution records in `cron_runs` with status `success` and non-zero `files_ingested`.
- 3. Verify the PostgreSQL `usage_log` table attributes FOS Class A LIST and Class B GET calls to `cron.log_discovery`.
- 4. Confirm new rows immediately queryable via `POST /api/dashboard/bundle`.
- 5. Confirm heavy refresh phases (`update_top_values` and the usage-log phase) run no more than once per 60s.
- 6. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
- 7. Verify source objects transition `discovered → claimed → acknowledged` in the PostgreSQL control plane and publish visibly in ClickHouse.
- 8. Verify malformed-line and corrupt-gzip outcomes, per-category counters, `error` status, immediate per-service cap eviction, private evidence downloads, and analyst denial.
