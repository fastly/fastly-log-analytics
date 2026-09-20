> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `metadata_cleanup_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `metadata_cleanup_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `metadata_cleanup_{service_id}`
- **Category:** Operational Database Retention & Pruning
- **Purpose:** Trims historical operational records from per-service SQLite databases (`metadata.db` and `usage_log.db`), including `usage_log`, `ingested_files`, and `cron_runs` according to configured retention windows.
- **Why It Runs:** Continuous streaming writes hundreds of operational records per hour. Unbounded growth in SQLite databases causes WAL bloat, slower indexed lookups, and unneeded disk consumption. Scheduled pruning keeps SQLite databases small, fast, and cached in OS memory.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 03:15 UTC (`hour=3, minute=15`).
- **Timing Rationale:** Runs before `full_sync` (03:30 UTC) and `optimize` (04:00 UTC) so that the daily maintenance window stays single-threaded across heavy phases.
- **Configurable Overrides:**
  - `metadata_retention.usage_log_days` (default: 1 day).
  - `metadata_retention.ingested_files_days` (default: 1 day).
  - `metadata_retention.cron_runs_days` (default: 7 days).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Connects to `data/services/{service_id}.metadata.db` and `usage_log.db`, issues bounded `DELETE FROM` statements, commits WAL. | ThreadLocalPool connection lock. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler or Worker | Connects to PostgreSQL `METADATA_DSN` or local usage log; executes SQL deletes. | PostgreSQL transaction lock. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/metadata/cleanup/{service_id}` | Pruning statistics in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Prunes local analyst metadata databases. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Server-side maintenance only; no direct interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Config Resolution:** Loads retention thresholds from `configs/{service_id}.json` under `metadata_retention`. Defaults to 1 day for `usage_log` and `ingested_files`, and 7 days for `cron_runs`.
2. **Purge Ingested Files:**
   - Calculates cutoff timestamp: `NOW() - ingested_files_days`.
   - Executes: `DELETE FROM ingested_files WHERE ingested_at < ?`.
3. **Purge Usage Log Records:**
   - Calculates cutoff: `NOW() - usage_log_days`.
   - Executes: `DELETE FROM usage_log WHERE timestamp < ?`.
   - *Note:* Hourly summary rollups in `usage_log_hourly_summary` are preserved for billing history.
4. **Purge Cron Execution Logs:**
   - Calculates cutoff: `NOW() - cron_runs_days`.
   - Executes: `DELETE FROM cron_runs WHERE started_at < ?`.
5. **SQLite WAL Checkpoint:**
   - Issues `PRAGMA wal_checkpoint(TRUNCATE)` to reclaim disk pages and keep WAL files at minimal size.
6. **Telemetry & Log Recording:**
   - Records deleted row tallies and execution duration in `cron_runs`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local database operations; zero cloud network calls.
  - **SQLite Operations:** Every `DELETE FROM` statement must execute through `ThreadLocalPool` with instrumented timings.
  - **Lock Wait Time:** Connection acquisition wait (`app.thread_wait_ms`) must remain < 20ms.
- **Timing & Resource Budgets:**
  - Full cleanup execution: < 1.0 second.
  - SQLite WAL truncate: < 100ms.
- **Audit Checklist:**
  - Confirm `usage_log_hourly_summary` is NOT deleted (billing history preserved).
  - Verify `ingested_files` deletion does not delete entries within the lookback window required by `full_sync`.
  - Confirm database files don't suffer from fragmentation.

---

## 7. Failure Modes & Recovery Runbooks
- **SQLite Database Locked (`sqlite3.OperationalError`):** Uses exponential backoff retry up to 5 attempts; WAL mode allows concurrent readers.
- **Corrupt SQLite File:** If corruption is detected, logs error and triggers automated `.dump` restore.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Cleanup:** `POST /api/admin/metadata/cleanup/{service_id}`.
- **Inspect DB Sizes:** `GET /api/admin/system/disk-usage`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Insert synthetic rows older than retention cutoffs into `usage_log`, `ingested_files`, and `cron_runs`.
- [ ] 2. Trigger `POST /api/admin/metadata/cleanup/{service_id}`; verify HTTP 200.
- [ ] 3. Verify expired rows are deleted from the target tables.
- [ ] 4. Verify rows within the retention window remain intact.
- [ ] 5. Verify `usage_log_hourly_summary` retains all historical records.
- [ ] 6. Confirm WAL file size is truncated.
- [ ] 7. Under `FLA_DEV_NO_CRONS=1`, verify job does not register or execute.
