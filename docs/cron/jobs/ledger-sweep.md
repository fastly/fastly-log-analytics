# Background Job Specification: `ledger_sweep_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `ledger_sweep_{service_id}`
- **Category:** Distributed State Machine Crash Recovery & Dead-Letter Management
- **Purpose:** Acts as the automated crash-net for High-Scale (`DEPLOYMENT_MODE=high_throughput`) distributed ingestion. It scans PostgreSQL `ingest_ledger` to reclaim stuck worker claims, re-dispatches stranded tasks with queue-depth guards, moves permanently failing items to `dead_letter` / `quarantined`, and diffs FOS to catch up on any unrecorded keys.
- **Why It Runs:** Distributed Celery workers can crash, lose network connectivity, or be killed by Kubernetes OOMKilled events mid-conversion. Without an autonomous ledger sweeper, claimed log batches would remain permanently stuck in `claimed` status, creating silent data holes in the lakehouse.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 15 minutes (`minutes=15`, `misfire_grace_time=300s`).
- **Timing Rationale:** Replaced the legacy `now.minute % 15 == 0` inline tick check with a dedicated standalone job to guarantee predictable execution regardless of discovery frequency.
- **Configurable Overrides:**
  - `provisioning.cron_ledger_sweep.enabled` (default: `true`).
  - `provisioning.cron_ledger_sweep.interval_minutes` (default: `15`).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | Disabled | Not applicable in synchronous SQLite mode. | N/A |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Worker | Connects to PostgreSQL `METADATA_DSN`; queries and mutates `ingest_ledger` state machine. | PostgreSQL row-level locks (`UPDATE ... WHERE status='claimed'`). |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/ledger/sweep/{service_id}` | Ingestion health, recovery metrics, and quarantine visibility in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Distributed ledger operates server-side only. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side background daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Check:**
   - Loads config and verifies `is_high_throughput_mode(src)`. Exits immediately if standard mode.
2. **Progress & Telemetry Initialization:**
   - Calls `start_cron_run(src, "ledger_sweep")`.
   - Initializes live tracking via `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="ledger_sweep")`.
   - Emits initial status event to `cron_progress`.
3. **Reclaim Stuck Claims:**
   - In `sweep_ledger_once(service_id)`:
     - Identifies rows in `ingest_ledger` where `status = 'claimed'` and `claimed_at < NOW() - LEDGER_RECLAIM_AFTER_S`.
     - Resets `status='discovered'`, `claimed_by=NULL`, `claimed_at=NULL`, and updates `next_attempt_at`.
     - Excludes `raw/rum/%` object keys (handled separately by `ledger_rum_sweep`).
4. **Queue-Depth Guarded Re-Dispatch:**
   - Queries `celery_queue_depths()` for `q.ingest`.
   - If broker is reachable and `queue_depth < pending_batches`: dispatches pending batches via `convert_batch_files.delay(...)`.
   - If queue is already full, skips re-dispatch to avoid message multiplication during drain.
5. **Lookback FOS Diff Sweep:**
   - Calls `discover_prefix(service_id, start_time=st)` for lookback window (default: 4 hours) to catch any objects missed by real-time discovery.
6. **Dead-Letter & Health Check:**
   - Queries count of rows in `quarantined` or `dead_letter` status in `ingest_ledger`.
   - If broker probe failed or `dead_letter > 0`: sets cron run status to `"warning"`.
7. **Telemetry & Log Recording:**
   - Logs `run_status` (`"success"` or `"warning"`) in `cron_runs` with summary string.
   - In guaranteed `finally:` block: calls `end_progress(run_id)` and `finalize_cron_duration(src, run_id, started)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **PostgreSQL DML:** All `UPDATE ingest_ledger` statements must complete within transaction boundaries.
  - **Valkey Queue Probes:** Queue length checks against Valkey must be timed.
  - **FOS S3 Calls:** Lookback LIST calls must be tracked in `usage_log.db`.
- **Timing & Resource Budgets:**
  - Reclaim query duration: < 100ms.
  - Redis queue probe: < 10ms.
  - Overall sweep duration: < 15 seconds.
- **Audit Checklist:**
  - Verify PostgreSQL index on `(service_id, status, claimed_at)` is utilized.
  - Verify that queue-depth guards prevent duplicate message storms during worker outages.
  - Verify warning status appears in UI when dead-letter items exist.

---

## 7. Failure Modes & Recovery Runbooks
- **PostgreSQL Database Connection Failure:** Caught in try/except; logs error in `cron_runs`.
- **Celery Broker Unreachable:** Flagged as warning in summary; skips re-dispatch until broker recovers.
- **Dead-Letter Accumulation:** Surface in System Jobs UI as warning; operator can inspect via `GET /api/admin/ledger/quarantine`.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Sweep:** `POST /api/admin/ledger/sweep/{service_id}`.
- **Inspect Quarantine:** `GET /api/admin/ledger/quarantine?service_id={service_id}`.

---

## 9. Automated Verification Matrix
- [x] 1. Live progress initialization and completion verified via `test_run_ledger_sweep_emits_progress_and_finalizes_duration`.
- [x] 2. Duration finalization in `finally` block verified via `test_run_ledger_sweep_emits_progress_and_finalizes_duration`.
- [x] 3. Status warning on broker issue or dead-letter rows verified via `test_run_ledger_sweep_status_warning_on_dead_letter_or_broker_issue`.
- [x] 4. Dynamic rescheduling on `cron_ledger_sweep.interval_minutes` change verified via `test_sync_jobs_reschedules_ledger_sweep_when_interval_changed`.
- [x] 5. Job disabled when `cron_ledger_sweep.enabled = False` verified via `test_sync_jobs_skips_ledger_sweep_when_disabled`.
- [x] 6. Stale worker claim reclamation verified via `tests/core/test_step4_sweeper.py`.
- [x] 7. Queue depth lost-message guard verified via `tests/core/test_step4_sweeper.py`.
