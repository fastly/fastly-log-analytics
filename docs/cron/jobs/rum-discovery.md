# Background Job Specification: `rum_discovery_{service_id}`

---

## 1. Overview & Objectives
- **Job Identifier:** `rum_discovery_{service_id}`
- **Category:** RUM Discovery & Faro Bundle Reconciliation
- **Purpose:** Periodically issues FOS LIST calls on the raw RUM prefix (`raw/rum/**/*.{zst,gz}`) across a sliding 5-minute window, records discovered keys into the PostgreSQL ingest ledger for batched worker ingestion into ClickHouse (`rum_vitals` and `rum_errors`), and reconciles the Faro client script bundle.
- **Why It Runs:** Client-side Core Web Vitals and JS error beacons are streamed to `raw/rum/`. Discovery decouples S3 LIST operations from worker-tier batch decoding and archiving, allowing RUM ingestion to scale seamlessly alongside request ingestion.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Evaluated every `rum_disc_interval_secs` (derived from `rum.sync_interval_seconds` or `log_period`, min: 5s).
- **Registration Gate:** Registered when `rum.enabled == true`.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Component | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Unified ClickHouse Architecture** | APScheduler + Stateless Ingest Worker | Discovers FOS `raw/rum/` keys, writes to PostgreSQL ingest ledger, and publishes verified batches to FOS Archive Parquet and ClickHouse (`rum_vitals`, `rum_errors`). | PostgreSQL row-level locks on `ingest_ledger`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/discovery/{service_id}` | RUM queue and status metrics in Admin UI. |
| **Analyst (Remote Share)** | N/A | Blocked (403) | Server-side background daemon. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Checks:**
   - Checks `dev_mode_no_crons()`; skips if `FLA_DEV_NO_CRONS=1`.
   - Confirms active RUM configuration (`rum_enabled`) and `read_write` service mode.
2. **Progress Lifecycle Start:**
   - Calls `start_cron_run(src, "rum_discovery")` returning `run_id`.
   - Calls `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="rum_discovery")`.
3. **Faro Bundle Integrity & Upstream Drift Reconcile:**
   - Calls `_reconcile_faro_bundle(service_id, run_id)`.
   - If Faro reconciliation fails or reports an issue, tracks `faro_ok = False` and continues discovery. Faro is the only RUM-specific warning condition; ingestion, quarantine, counters, retries, and FOS deletion follow the same contract as request discovery.
4. **FOS LIST Call & Ledger Dispatch:**
   - For each minute in a 5-minute lookback window (`rum_minute_list_prefix`):
     - Executes `discover_rum_prefix(service_id, prefix_subpath=prefix)`.
     - Inserts unseen keys into PostgreSQL `ingest_ledger` with `status='discovered'`.
     - Stateless workers claim discovered RUM objects, decode both `rum_vitals` and `rum_errors` domains from shared objects (Trap #48), archive to FOS Parquet, and publish to ClickHouse.
5. **Telemetry & Finalization:**
   - Records the shared zero-filled outcome counters and status rules. A Faro-only failure adds warning details to `summary` and `error_message`; any data-plane failure records `"error"`.
   - If successful: records status `"success"` with `files_downloaded=discovered`.
   - Guaranteed `finally:` ends progress and calls `finalize_cron_run_if_running`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** LIST operations are attributed to the PostgreSQL `usage_log` table under `cron.rum_discovery`.
  - **PostgreSQL DML:** Ledger and originating-run outcome updates use the shared instrumented Postgres metadata connection.
- **Timing & Resource Budgets:**
  - Discovery LIST execution: < 200ms.
  - PostgreSQL batch insert: < 50ms.
- **Audit Checklist:**
  - Verify PostgreSQL index on `(service_id, source_type, status)` is utilized.

---

## 7. Failure Modes & Recovery Runbooks
- **Worker Crash:** Orphaned claimed items are reclaimed automatically by `ledger_rum_sweep_{service_id}`.
- **Faro Upstream Outage:** Discovery continues uninterrupted; run records status `"warning"`.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Discovery:** `POST /api/admin/rum/discovery/{service_id}`.
- **Inspect Ledger Status:** `GET /api/admin/celery/ledger?service_id={service_id}&status=all`.

---

## 9. Verification Checklist
- 1. Confirm RUM discovery across 5-minute sliding window with `status='success'`.
- 2. Confirm Faro bundle reconcile failure records `status='warning'` without halting beacon discovery.
- 3. Confirm clean progress lifecycle (`start_progress`, `end_progress`) and `finalize_cron_run_if_running`.
