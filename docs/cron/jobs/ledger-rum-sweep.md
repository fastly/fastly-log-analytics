> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `ledger_rum_sweep_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `ledger_rum_sweep_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `ledger_rum_sweep_{service_id}`
- **Category:** Distributed State Machine Crash Recovery & RUM Dead-Letter Sweep
- **Purpose:** Acts as the automated crash-net for distributed RUM beacon ingestion in `DEPLOYMENT_MODE=high_throughput`. It scans PostgreSQL `ingest_ledger` for orphaned `rum` claims, resets timed-out items, re-dispatches worker tasks, and routes persistently unparseable beacons to quarantine.
- **Why It Runs:** RUM beacon conversion can fail due to malformed client telemetry, browser extensions corrupting JSON payloads, or Celery worker evictions. This sweeper guarantees that transient worker failures do not drop RUM beacons and that poison-pill beacons are quarantined without blocking the distributed pipeline.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 15 minutes (`minutes=15`).
- **Worker Routing:** Evaluated via RedBeat on the Celery worker fleet (`_REDBEAT_JOB_PREFIXES`).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | Disabled | Not applicable in synchronous SQLite mode. | N/A |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Worker | Scans PostgreSQL `ingest_ledger` where `source_type = 'rum'`. | PostgreSQL row-level locks. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/ledger/sweep/{service_id}` | RUM dead-letter visibility in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Server-side distributed infrastructure. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side background daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Check:** Confirms `DEPLOYMENT_MODE == "high_throughput"` and RUM is enabled.
2. **Reclaim Stale Claims:**
   - Queries `ingest_ledger` where `source_type = 'rum'`, `status = 'claimed'`, and `claimed_at < NOW() - INTERVAL '30 MINUTE'`.
   - If `retry_count < 3`: resets status to `discovered` and increments `retry_count`.
   - If `retry_count >= 3`: updates status to `quarantined` with error diagnostic.
3. **Queue-Depth Probing & Re-Dispatch:**
   - Probes Celery queue depth; if healthy, dispatches pending `discovered` RUM tasks.
4. **FOS Lookback Diff Sweep:**
   - Audits recent FOS `raw/rum/` keys to ensure no beacon files were missed during broker reloads.
5. **Telemetry & Log Recording:**
   - Records reclaimed counts and sweep duration in `cron_runs`.
   - Emits Prometheus metrics for RUM quarantine status.

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
  - Confirm lookback LIST does not scan older than 2 hours.
  - Verify that poison-pill RUM beacons transition to `quarantined` without worker crashes.

---

## 7. Failure Modes & Recovery Runbooks
- **Postgres Database Timeout:** Retries with exponential backoff; emits Prometheus alert.
- **RUM Quarantine Spike:** If quarantined items exceed 50, triggers warning for frontend telemetry bug investigation.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Sweep:** `POST /api/admin/rum/ledger/sweep/{service_id}`.
- **Inspect Quarantine:** `GET /api/admin/ledger/quarantine?service_id={service_id}&source_type=rum`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Artificially set an `ingest_ledger` RUM row to `claimed` with `claimed_at = NOW() - 40 min`.
- [ ] 2. Trigger `POST /api/admin/rum/ledger/sweep/{service_id}`; confirm HTTP 200.
- [ ] 3. Verify in PostgreSQL: row status is reset to `discovered`.
- [ ] 4. Force `retry_count = 3` and re-run sweep; verify row status transitions to `quarantined`.
- [ ] 5. Confirm `cron_runs` records execution status `success`.
