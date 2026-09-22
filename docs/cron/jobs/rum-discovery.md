# Background Job Specification: `rum_discovery_{service_id}`

> [!NOTE]
> **Status: VERIFIED & OPERATIONAL (High-Scale Architecture / Ingest Pipeline Audit)**
> Automated test suites verified: `tests/cron/test_rum_discovery.py` (4 tests passing: high-scale discovery success, faro bundle warning status, standard mode guard, broker config validation).

---

## 1. Overview & Objectives
- **Job Identifier:** `rum_discovery_{service_id}`
- **Category:** Distributed High-Scale RUM Discovery
- **Purpose:** In `DEPLOYMENT_MODE=high_throughput`, periodically issues FOS LIST calls on the raw RUM prefix (`raw/rum/**/*.gz`) across a sliding 5-minute window, records discovered keys into PostgreSQL `ingest_ledger`, dispatches batched Celery conversion tasks (`convert_batch_rum_files`), and reconciles the Faro client script bundle.
- **Why It Runs:** At high traffic volumes (tens of thousands of client beacons per second), single-pod synchronous beacon decompression and parsing exhausts CPU resources. Distributed discovery decouples S3 LIST operations from worker-tier conversion, allowing RUM ingestion to scale horizontally across worker nodes.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Evaluated every `rum_disc_interval_secs` (derived from `rum.sync_interval_seconds` or `log_period`, min: 5s).
- **Registration Gate:** Registered **ONLY** if `rum.enabled == true` AND `DEPLOYMENT_MODE == "high_throughput"`.
- **Worker Routing:** Evaluated and scheduled via RedBeat (`_REDBEAT_JOB_PREFIXES`) on the Celery worker fleet.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | Disabled | Synchronous mode uses `rum_discovery_{service_id}` instead. Cleanly returns if invoked. | N/A |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Workers | Discovers FOS `raw/rum/` keys, writes to PostgreSQL `ingest_ledger`, dispatches Celery conversion jobs. | PostgreSQL row-level locks on `ingest_ledger`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/discovery/{service_id}` | Distributed RUM queue metrics in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Server-side distributed infrastructure. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side background daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite & Mode Checks:**
   - Checks `dev_mode_no_crons()`; skips if `FLA_DEV_NO_CRONS=1`.
   - Confirms `is_high_throughput_mode(src)` and active RUM configuration (`rum_enabled`).
   - Confirms service is `read_write`.
2. **Progress Lifecycle Start:**
   - Calls `start_cron_run(src, "rum_discovery")` returning `run_id`.
   - Calls `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="rum_discovery")`.
3. **Faro Bundle Integrity & Upstream Drift Reconcile:**
   - Calls `_reconcile_faro_bundle(service_id, run_id)`.
   - If Faro reconciliation fails or reports an issue, tracks `faro_ok = False` and
     continues discovery. Faro is the only RUM-specific warning condition; ingestion,
     quarantine, counters, retries, and FOS deletion follow the same contract as request
     discovery.
4. **Broker Check:**
   - Confirms `CELERY_BROKER_URL` is set; records status `"error"` if missing.
5. **FOS LIST Call & Ledger Dispatch:**
   - For each minute in a 5-minute lookback window (`rum_minute_list_prefix`):
     - Executes `discover_rum_prefix(service_id, prefix_subpath=prefix)`.
     - Inserts unseen keys into PostgreSQL `ingest_ledger` with `status='discovered'`.
     - Dispatches batches to Celery via `convert_batch_rum_files.delay`.
6. **Telemetry & Finalization:**
   - Records the same shared zero-filled outcome counters and status rules as request
     discovery. A Faro-only failure adds warning details to `summary` and `error_message`;
     any data-plane failure records `"error"`.
   - If successful: records status `"success"` with `files_downloaded=discovered`.
   - Guaranteed `finally:` ends progress and calls `finalize_cron_run_if_running`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** LIST operations must be logged in `usage_log.db` under `cron.rum_discovery`.
  - **PostgreSQL DML:** All ledger operations must be timed and instrumented.
  - **Celery Enqueue Time:** Task dispatch latency must be < 10ms.
- **Timing & Resource Budgets:**
  - Discovery LIST execution: < 200ms.
  - PostgreSQL batch insert: < 50ms.
- **Audit Checklist:**
  - Verify PostgreSQL index on `(service_id, source_type, status)` is utilized.
  - Verify zero task queue congestion under normal traffic volumes.

---

## 7. Failure Modes & Recovery Runbooks
- **Celery Queue Saturation:** Checks queue depth before dispatching; defers claiming if worker queues are full.
- **Worker Crash:** Orphaned claimed items are reclaimed automatically by `ledger_rum_sweep_{service_id}`.
- **Faro Upstream Outage:** Discovery continues uninterrupted; run records status `"warning"`.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Discovery:** `POST /api/admin/rum/discovery/{service_id}`.
- **Inspect Ledger Status:** `GET /api/admin/ledger/status?service_id={service_id}&source_type=rum`.

---

## 9. AI Session Automated Verification Checklist
- [x] 1. Automated unit tests verified in `tests/cron/test_rum_discovery.py`.
- [x] 2. Verified high-scale RUM discovery across 5-minute sliding window with `status='success'`.
- [x] 3. Verified Faro bundle reconcile failure records `status='warning'` without halting beacon discovery.
- [x] 4. Verified standard-mode guard skips cleanly when invoked outside high-throughput deployments.
- [x] 5. Verified clean progress lifecycle (`start_progress`, `end_progress`) and `finalize_cron_run_if_running`.
