> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `ledger_sweep_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `ledger_sweep_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `ledger_sweep_{service_id}`
- **Category:** Distributed State Machine Crash Recovery & Dead-Letter Management
- **Purpose:** Acts as the automated crash-net for High-Scale (`DEPLOYMENT_MODE=high_throughput`) distributed ingestion. It scans PostgreSQL `ingest_ledger` to reclaim stuck worker claims, re-dispatches stranded tasks with queue-depth guards, moves permanently failing items to `dead_letter` / `quarantined`, and diffs FOS to catch up on any unrecorded keys.
- **Why It Runs:** Distributed Celery workers can crash, lose network connectivity, or be killed by Kubernetes OOMKilled events mid-conversion. Without an autonomous ledger sweeper, claimed log batches would remain permanently stuck in `claimed` status, creating silent data holes in the lakehouse.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 15 minutes (`minutes=15`).
- **Timing Rationale:** Replaced the legacy `now.minute % 15 == 0` inline tick check with a dedicated standalone job to guarantee predictable execution regardless of discovery frequency.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | Disabled | Not applicable in synchronous SQLite mode. | N/A |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Worker | Connects to PostgreSQL `METADATA_DSN`; queries and mutates `ingest_ledger` state machine. | PostgreSQL row-level locks (`FOR UPDATE SKIP LOCKED`). |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/ledger/sweep/{service_id}` | Ingestion health and quarantine visibility in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Distributed ledger operates server-side only. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side background daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Check:** Confirms `DEPLOYMENT_MODE == "high_throughput"`. If standard mode, exits immediately.
2. **Reclaim Stuck Claims:**
   - Identifies rows in `ingest_ledger` where `status = 'claimed'` and `claimed_at < NOW() - INTERVAL '30 MINUTE'`.
   - If `retry_count < max_retries` (default: 3): resets status to `discovered`, increments `retry_count`.
   - If `retry_count >= max_retries`: moves row to `quarantined` or `dead_letter` with failure diagnostic.
3. **Queue-Depth Guarded Re-Dispatch:**
   - Probes Valkey/Redis queue depth for the `fastly_ingest` task queue.
   - If queue depth is below congestion threshold (< 5,000 tasks): selects orphaned `discovered` batches and enqueues Celery conversion tasks.
4. **FOS Lookback Diff Sweep:**
   - Performs a bounded S3 LIST covering the last 2-4 hours of `raw/request/`.
   - Inserts any missing keys into `ingest_ledger` as `discovered` with `ON CONFLICT DO NOTHING`.
5. **Telemetry & Metric Emission:**
   - Emits Prometheus metrics: `app_ledger_reclaimed_claims_total`, `app_ledger_dead_letter_total`.
   - Logs execution summary in `cron_runs`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **PostgreSQL DML:** All `UPDATE ingest_ledger` and `SELECT ... FOR UPDATE` queries must record execution duration.
  - **Redis Queue Probes:** Queue length checks against Valkey must be timed.
  - **FOS S3 Calls:** Lookback LIST calls must be tracked in `usage_log.db`.
- **Timing & Resource Budgets:**
  - Reclaim query duration: < 100ms.
  - Redis queue probe: < 10ms.
  - Overall sweep duration: < 30 seconds.
- **Audit Checklist:**
  - Verify PostgreSQL index on `(service_id, status, claimed_at)` is utilized.
  - Verify that queue-depth guards prevent re-dispatch storms during worker outages.

---

## 7. Failure Modes & Recovery Runbooks
- **PostgreSQL Database Connection Failure:** Retries with exponential backoff; logs critical alert in Prometheus.
- **Dead-Letter Accumulation:** If dead-letter count exceeds 100, triggers a high-severity alert for operator investigation.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Sweep:** `POST /api/admin/ledger/sweep/{service_id}`.
- **Inspect Quarantine:** `GET /api/admin/ledger/quarantine?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Artificially set an `ingest_ledger` row to `claimed` with `claimed_at = NOW() - 45 min`.
- [ ] 2. Trigger `POST /api/admin/ledger/sweep/{service_id}`; confirm HTTP 200.
- [ ] 3. Verify in PostgreSQL: row status is reset to `discovered` and `retry_count` is incremented.
- [ ] 4. Force `retry_count = 3` and re-run sweep; verify row status transitions to `dead_letter`.
- [ ] 5. Confirm `cron_runs` records execution status `success`.
