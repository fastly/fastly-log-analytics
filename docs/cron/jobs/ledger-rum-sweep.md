# Background Job Specification: `ledger_rum_sweep_{service_id}`

---

## 1. Overview & Objectives
- **Job Identifier:** `ledger_rum_sweep_{service_id}`
- **Category:** Distributed State Machine Crash Recovery & RUM Dead-Letter Sweep
- **Purpose:** Acts as the automated crash-net for RUM beacon ingestion. It scans the PostgreSQL ingest ledger for orphaned `rum` claims, resets timed-out items, re-queues eligible work, diffs FOS `raw/rum/` across a 4-hour lookback window, and tracks quarantined/dead-letter items.
- **Why It Runs:** RUM beacon ingestion can encounter malformed client telemetry, browser extensions corrupting JSON payloads, or worker evictions. This sweeper guarantees that transient worker failures do not drop RUM beacons and that poison-pill beacons are quarantined without blocking the pipeline.
- **Ownership boundary:** `rum_discovery_{service_id}` owns discovery; RUM ingest workers own record validation, quarantine capture, durable archive & ClickHouse publication, and source acknowledgement. This sweep only repairs ledger state and re-queues eligible work; it never re-parses payloads or performs a second commit.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 15 minutes (`minutes=15`).
- **Registration Gate:** Registered when `rum.enabled == true`.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Component | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Unified ClickHouse Architecture** | APScheduler + Stateless Worker / Control Plane | Scans PostgreSQL ingest ledger where `object_key LIKE '%raw/rum/%'`. | PostgreSQL row-level locks on `ingest_ledger`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/ledger/sweep/{service_id}` | RUM dead-letter visibility in Admin UI. |
| **Analyst (Remote Share)** | N/A | Blocked (403) | Server-side background daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Checks:**
   - Checks `dev_mode_no_crons()`; skips if `FLA_DEV_NO_CRONS=1`.
   - Confirms active RUM configuration (`rum_enabled`) and `read_write` service mode.
2. **Progress Lifecycle Start:**
   - Calls `start_cron_run(src, "ledger_rum_sweep")` returning `run_id`.
   - Calls `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="ledger_rum_sweep")`.
3. **Ledger Recovery & Redispatch (`sweep_rum_ledger_once`):**
   - **Reclaims Stale Claims:** Resets orphaned `claimed` rows older than `LEDGER_RECLAIM_AFTER_S` back to `discovered`.
   - **Re-dispatches Stuck Rows:** Gathers stuck `discovered` items and re-queues them within queue-depth bounds.
   - **Lookback FOS LIST Diff:** Runs `discover_rum_prefix` for a 4-hour lookback window to catch any uncataloged files.
   - **Counts Dead Letters:** Queries PostgreSQL `ingest_ledger` for `status IN ('quarantined', 'dead_letter')` matching `raw/rum/%`.
4. **Warning & Status Evaluation:**
   - If dead-letter/quarantined RUM rows > 0: records status `"warning"` with warning details in `summary` and `error_message`.
   - If clean: records status `"success"` with reclaimed, redispatched, and discovered metrics.
5. **Finalization:**
   - Guaranteed `finally:` ends progress and calls `finalize_cron_run_if_running`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **PostgreSQL DML:** All ledger updates are timed and instrumented.
  - **FOS Calls:** S3 LIST diff calls are logged in PostgreSQL's `usage_log` table.
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

## 9. Verification Checklist
- 1. Confirm recovery of stale claims, re-dispatching of pending items, and discovery diff with `status='success'`.
- 2. Confirm dead-letter accumulation records `status='warning'` with error details.
- 3. Confirm clean progress lifecycle (`start_progress`, `end_progress`) and `finalize_cron_run_if_running`.
- 4. Confirm manual trigger endpoint `POST /api/admin/rum/ledger/sweep/{service_id}` with re-entrancy 409, RUM-disabled 400, and RBAC 403 enforcement.
- 5. Confirm quarantine inspection with `source_type=rum` filtering and exact item counts across `/api/admin/ledger/quarantine` and `/api/admin/quarantine`.
- 6. Confirm dynamic scheduler registration, interval override via `cron_ledger_rum_sweep.interval_minutes`, and `enabled: false` clean unregistration.
- 7. Confirm duration tracking and telemetry finalization via `finalize_cron_duration`.
