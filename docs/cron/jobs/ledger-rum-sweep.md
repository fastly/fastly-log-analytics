# Background Job Specification: `ledger_rum_sweep_{service_id}`

> [!NOTE]
> **Status: VERIFIED & OPERATIONAL (High-Scale Architecture / Ingest Pipeline Audit)**
> Automated test suites verified: `tests/cron/test_ledger_rum_sweep.py` (5 tests passing: high-scale recovery success, broker/dead-letter warning status, standard mode guard, read_only guard, dev-mode suppression).

---

## 1. Overview & Objectives
- **Job Identifier:** `ledger_rum_sweep_{service_id}`
- **Category:** Distributed State Machine Crash Recovery & RUM Dead-Letter Sweep
- **Purpose:** Acts as the automated crash-net for distributed RUM beacon ingestion in `DEPLOYMENT_MODE=high_throughput`. It scans PostgreSQL `ingest_ledger` for orphaned `rum` claims, resets timed-out items, re-dispatches worker tasks to Celery `q.ingest` with queue-depth safety, and tracks quarantined/dead-letter items.
- **Why It Runs:** RUM beacon conversion can fail due to malformed client telemetry, browser extensions corrupting JSON payloads, or Celery worker evictions. This sweeper guarantees that transient worker failures do not drop RUM beacons and that poison-pill beacons are quarantined without blocking the distributed pipeline.
- **Ownership boundary:** `rum_discovery_{service_id}` owns discovery and initial dispatch; RUM conversion workers own record validation, quarantine capture, durable publication, and source acknowledgement. This sweep only repairs ledger state and redispatches eligible work; it never re-parses payloads or performs a second commit.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 15 minutes (`minutes=15`).
- **Registration Gate:** Registered **ONLY** if `rum.enabled == true` AND `DEPLOYMENT_MODE == "high_throughput"`.
- **Worker Routing:** Evaluated via RedBeat on the Celery worker fleet (`_REDBEAT_JOB_PREFIXES`).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | Disabled | Not applicable in synchronous SQLite mode. Cleanly skips if called. | N/A |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Worker | Scans PostgreSQL `ingest_ledger` where `object_key LIKE '%raw/rum/%'`. | PostgreSQL row-level locks on `ingest_ledger`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/ledger/sweep/{service_id}` | RUM dead-letter visibility in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Server-side distributed infrastructure. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side background daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite & Mode Checks:**
   - Checks `dev_mode_no_crons()`; skips if `FLA_DEV_NO_CRONS=1`.
   - Confirms `is_high_throughput_mode(src)` and active RUM configuration (`rum_enabled`).
   - Confirms service is `read_write`.
2. **Progress Lifecycle Start:**
   - Calls `start_cron_run(src, "ledger_rum_sweep")` returning `run_id`.
   - Calls `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="ledger_rum_sweep")`.
3. **Ledger Recovery & Redispatch (`sweep_rum_ledger_once`):**
   - **Reclaims Stale Claims:** Resets orphaned `claimed` rows older than `LEDGER_RECLAIM_AFTER_S` back to `discovered`.
   - **Re-dispatches Stuck Rows:** Gathers stuck `discovered` items, checks Celery `q.ingest` queue depth via `celery_queue_depths()`, and re-dispatches up to batch limits if the queue has capacity.
   - **Lookback FOS LIST Diff:** Runs `discover_rum_prefix` for a 4-hour lookback window to catch any uncataloged files.
   - **Counts Dead Letters:** Queries PostgreSQL `ingest_ledger` for `status IN ('quarantined', 'dead_letter')` matching `raw/rum/%`.
4. **Warning & Status Evaluation:**
   - If broker probe failed or if dead-letter/quarantined RUM rows > 0: records status `"warning"` with warning details in `summary` and `error_message`.
   - If clean: records status `"success"` with reclaimed, redispatched, and discovered metrics.
   - A warning here reports recovery pressure or accumulated poison work. It does not downgrade a conversion worker's data-plane `"error"` outcome when record processing, quarantine capture, or source deletion fails.
5. **Finalization:**
   - Guaranteed `finally:` ends progress and calls `finalize_cron_run_if_running`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **PostgreSQL DML:** All ledger updates must be timed and instrumented.
  - **Queue Probes:** Valkey queue depth checks must be tracked.
  - **FOS Calls:** S3 LIST diff calls must be logged in `usage_log.db`.
- **Timing & Resource Budgets:**
  - Sweep execution: < 15 seconds.
  - PostgreSQL transaction duration: < 100ms.
- **Audit Checklist:**
  - Confirm lookback LIST does not scan older than 4 hours.
  - Verify that poison-pill RUM beacons transition to `quarantined` without worker crashes.

---

## 7. Failure Modes & Recovery Runbooks
- **Postgres Database Timeout:** Retries on subsequent tick; logs warning.
- **RUM Quarantine Spike:** If quarantined items accumulate, surfaces as `"warning"` status in `cron_runs` to alert operators.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Sweep:** `POST /api/admin/rum/ledger/sweep/{service_id}`.
- **Inspect Quarantine:** `GET /api/admin/ledger/quarantine?service_id={service_id}&source_type=rum`.

---

## 9. AI Session Automated Verification Checklist
- [x] 1. Automated unit tests verified in `tests/cron/test_ledger_rum_sweep.py`.
- [x] 2. Verified recovery of stale claims, re-dispatching of pending items, and discovery diff with `status='success'`.
- [x] 3. Verified Celery broker failure or dead-letter accumulation records `status='warning'` with error details.
- [x] 4. Verified standard-mode and read-only guards skip cleanly.
- [x] 5. Verified clean progress lifecycle (`start_progress`, `end_progress`) and `finalize_cron_run_if_running`.
