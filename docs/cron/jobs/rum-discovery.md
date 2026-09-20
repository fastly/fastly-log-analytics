> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `rum_discovery_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `rum_discovery_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `rum_discovery_{service_id}`
- **Category:** Distributed High-Scale RUM Discovery
- **Purpose:** In `DEPLOYMENT_MODE=high_throughput`, periodically issues FOS LIST calls on the raw RUM prefix (`raw/rum/**/*.gz`), records discovered keys into PostgreSQL `ingest_ledger` with `source_type = 'rum'`, and enqueues distributed Celery worker tasks for beacon parsing and conversion.
- **Why It Runs:** At high traffic volumes (tens of thousands of client beacons per second), single-pod synchronous beacon decompression and parsing exhausts CPU resources. Distributed discovery decouples S3 LIST operations from worker-tier conversion, allowing RUM ingestion to scale horizontally across worker nodes.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Evaluated every `rum_disc_interval_secs` (derived from `rum.sync_interval_seconds` or `log_period`, min: 5s).
- **Registration Gate:** Registered **ONLY** if `rum.enabled == true` AND `DEPLOYMENT_MODE == "high_throughput"`.
- **Worker Routing:** Evaluated and scheduled via RedBeat (`_REDBEAT_JOB_PREFIXES`) on Celery worker fleet.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | Disabled | Synchronous mode uses `rum_sync_{service_id}` instead. | N/A |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | RedBeat + Celery Workers | Discovers FOS `raw/rum/` keys, writes to PostgreSQL `ingest_ledger`, dispatches Celery conversion jobs. | PostgreSQL row-level locks. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/discovery/{service_id}` | Distributed RUM queue metrics in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Server-side distributed infrastructure. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side background daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Check:** Confirms high-throughput mode and active RUM configuration.
2. **FOS LIST Call:** Executes bounded S3 LIST on `raw/rum/`.
3. **Ledger Batch Insertion:** Inserts newly discovered keys into PostgreSQL:
   ```sql
   INSERT INTO ingest_ledger (service_id, filename, source_type, status)
   VALUES (%s, %s, 'rum', 'discovered')
   ON CONFLICT DO NOTHING;
   ```
4. **Batch Claiming:**
   ```sql
   UPDATE ingest_ledger
   SET status = 'claimed', worker_id = %s, claimed_at = NOW()
   WHERE service_id = %s AND source_type = 'rum' AND status = 'discovered'
   RETURNING filename;
   ```
5. **Task Dispatch:** Enqueues Celery conversion task `convert_rum_batch.delay(service_id, filenames)`.
6. **Telemetry & Log Recording:**
   - Emits Prometheus metrics for discovered RUM keys.
   - Logs execution summary in `cron_runs`.

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

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Discovery:** `POST /api/admin/rum/discovery/{service_id}`.
- **Inspect Ledger Status:** `GET /api/admin/ledger/status?service_id={service_id}&source_type=rum`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Upload synthetic RUM files to FOS `raw/rum/`.
- [ ] 2. Trigger `POST /api/admin/rum/discovery/{service_id}`; confirm HTTP 200.
- [ ] 3. Verify in PostgreSQL: keys are inserted into `ingest_ledger` with `source_type = 'rum'`.
- [ ] 4. Confirm Celery worker consumes and converts the batch.
- [ ] 5. Verify `cron_runs` records execution status `success`.
