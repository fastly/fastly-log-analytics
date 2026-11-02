# Background Job Specification: `rum_commit_{service_id}`

---

## 1. Overview & Objectives
- **Job Identifier:** `rum_commit_{service_id}`
- **Category:** Durable RUM Lakehouse Commit
- **Purpose:** Flushes transient RUM Parquet buffer files from local disk (`cache/{bucket}/rum/`) into the durable DuckLake tables `client_vitals` and `client_errors` in cloud storage.
- **Why It Runs:** Just like standard access logs, RUM telemetry must be durably stored in DuckLake to prevent local disk exhaustion and ensure multi-node / multi-analyst query discovery.
- **High-Scale boundary:** This is deliberately a Standard-mode-only cron. High-Scale RUM conversion workers publish facts through the ingest-ledger/publication pipeline during conversion; `ledger_rum_sweep_{service_id}` recovers stalled or dead-lettered work. Adding a second High-Scale `rum_commit` tick would duplicate ownership and could race ledger publication.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Configured via `rum.commit_interval_mins` (default 1 minute); `rum_sync` also triggers a commit as soon as new rows arrive.
- **Registration Gate:** Registered **ONLY** if `rum.enabled == true` AND `DEPLOYMENT_MODE == "standard"`.
- **Active-Request Gate:** Removed (matching request `commit_{id}`): RUM freshness requires that automated ticks proceed without deferral.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads local RUM Parquet buffer, writes to FOS `ducklake/rum/`, commits to DuckLake catalog tables `client_vitals` and `client_errors`. | Exclusive per-service RUM commit lock. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_scale`)** | Disabled | Handled by `rum_discovery_{service_id}` → Celery conversion/publication; recovery is owned by `ledger_rum_sweep_{service_id}`. | PostgreSQL ledger state and worker publication ownership. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/sync/{service_id}` (commits as soon as new rows arrive) | Full RUM commit statistics in Admin UI. |
| **Analyst (Remote Share)** | N/A | Blocked (403) | Reads server-side RUM data; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Pre-Flight Checks:**
   - Verifies RUM is enabled and service is `read_write`.
   - Checks `FLA_DEV_NO_CRONS=1`.
   - Automated ticks proceed without in-flight query deferral (matching the request commit path).
2. **Disk Space Safety Pre-Check:**
   - Executes `_check_disk_space(_commit_cache_dir(src), service_id, "rum_commit")`.
   - If disk space is below safety threshold (<500MB), aborts immediately with status `"error"` to avoid mid-commit corruption.
3. **Progress Lifecycle Start:**
   - Calls `start_cron_run(src, "rum_commit")` returning `run_id`.
   - Calls `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="rum_commit")`.
4. **Table Commit Execution (Dual-Table Isolated):**
   - **`client_vitals`**: Calls `commit_buffer(src, table_name="client_vitals")`.
   - **`client_errors`**: Calls `commit_buffer(src, table_name="client_errors")`.
5. **Ledger Publication:**
   - Calls `_mark_ledger_published(service_id, rum=True)` **only if both** tables committed without errors (prevents premature raw deletion).
6. **Local Compaction Asynchronous Triggers:**
   - Launches daemon threads for `compact_local_partitions` on `client_vitals` and `client_errors` with defensive try/except logging.
7. **Telemetry, Status & Finalization:**
   - If both tables fail: records status `"error"`.
   - If one table succeeds and one fails: records the commit-stage partial result while
     preserving the shared ingestion outcome already recorded by discovery. Commit-stage
     counters remain limited to files/rows/tables committed and failed.
   - If both succeed: records status `"success"` with total rows and files committed.
   - Guaranteed `finally:` resets Boto3 caller hint, ends progress, and calls `finalize_cron_run_if_running`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** S3 uploads must be logged in PostgreSQL's `usage_log` table as Class A PUT calls under `_BOTO3_CALLER_HINT="rum_commit"`.
  - **DuckLake Procedures:** DDL/DML commit procedures must be registered with the Live Query Monitor query registry.
  - **Lock Contention:** Wait time for RUM commit lock must remain < 50ms.
- **Timing & Resource Budgets:**
  - Commit transaction duration: < 1.0 second.
  - S3 upload throughput: > 50 MB/s.
- **Audit Checklist:**
  - Verify that committed local buffer files are unlinked after successful catalog commit.
  - Confirm analytical queries against `/api/services/{service_id}/rum/analytics` immediately reflect the newly committed beacons.

---

## 7. Failure Modes & Recovery Runbooks
- **Partial Table Failure:** If `client_errors` fails while `client_vitals` succeeds, status is `"warning"`, ledger is kept un-published so raw data is preserved, and next tick retries.
- **High-Scale misconception:** No High-Scale `rum_commit` run should be expected in `cron_runs`; inspect the RUM ingest ledger, worker publication state, and `ledger_rum_sweep_{service_id}` instead.
- **Catalog Commit Conflict:** Automatically retries with exponential backoff.
- **Upload Network Failure:** Local RUM buffer files are preserved; next interval tick retries upload cleanly.
- **Disk Full:** Aborts prior to catalog modification, emitting status `"error"` with disk alert message.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM ingest + commit:** `POST /api/admin/rum/sync/{service_id}` (there is no standalone commit endpoint; `rum_sync` triggers `rum_commit` when new rows arrive).
- **Inspect Status:** `GET /api/services/{service_id}/rum/status`.

---

## 9. Verification Checklist
- 2. Confirm full commit success with both tables: status `"success"`, row counts and files recorded.
- 3. Confirm partial commit failure: status `"warning"`, raw ledger kept safe.
- 4. Confirm active-requests politeness gate removal: automated ticks proceed without deferral to maintain RUM freshness parity with request logs.
- 5. Confirm disk space pre-check: aborts cleanly with status `"error"` on low disk.
- 6. Confirm progress tracking lifecycle: `start_progress` and `end_progress` clean teardown.
- 7. Confirm natural key deduplication across RUM tables (`client_vitals`, `client_errors`) in both DuckLake buffer commit and local parquet compaction.
