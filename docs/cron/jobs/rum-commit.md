# Background Job Specification: `rum_commit_{service_id}`

> [!NOTE]
> **Status: VERIFIED & OPERATIONAL (Standard Mode / Ingest Pipeline Audit)**
> Automated test suites verified: `tests/cron/test_rum_commit.py` (4 tests passing: full commit success, partial failure warning, politeness gate deferral, disk space abort).

---

## 1. Overview & Objectives
- **Job Identifier:** `rum_commit_{service_id}`
- **Category:** Durable RUM Lakehouse Commit
- **Purpose:** Flushes transient RUM Parquet buffer files from local disk (`cache/{bucket}/rum/`) into the durable DuckLake tables `client_vitals` and `client_errors` in cloud storage.
- **Why It Runs:** Just like standard access logs, RUM telemetry must be durably stored in DuckLake to prevent local disk exhaustion and ensure multi-node / multi-analyst query discovery.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Configured via `rum.commit_interval_mins` (default matches `cron_sync.commit_interval_mins`, typically every 5 minutes).
- **Registration Gate:** Registered **ONLY** if `rum.enabled == true` AND `DEPLOYMENT_MODE == "standard"`.
- **Active-Request Politeness Gate:** Evaluates `should_defer_cron("rum_commit", service_id)`. If active user/analyst queries are running on DuckDB, non-manual RUM commit ticks defer to protect query latency and avoid lock contention.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads local RUM Parquet buffer, writes to FOS `ducklake/rum/`, commits to DuckLake catalog tables `client_vitals` and `client_errors`. | Exclusive per-service RUM commit lock. Gated by `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Disabled | Handled automatically by high-scale batch publication pipelines. | N/A |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rum/commit/{service_id}` | Full RUM commit statistics in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Disabled | Disabled | Reads cloud DuckLake tables. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads server-side RUM data; no cron interaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Pre-Flight Checks & Politeness Gate:**
   - Verifies RUM is enabled and service is `read_write`.
   - Checks `FLA_DEV_NO_CRONS=1`.
   - Checks `should_defer_cron("rum_commit", service_id)`. If active queries are running and run is not manual/forced, defers tick.
2. **Disk Space Safety Pre-Check:**
   - Executes `_check_disk_space(_commit_cache_dir(src), service_id, "rum_commit")`.
   - If disk space is below safety threshold (<500MB), aborts immediately with status `"error"` to avoid mid-commit corruption.
3. **Progress Lifecycle Start:**
   - Calls `start_cron_run(src, "rum_commit")` returning `run_id`.
   - Calls `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="rum_commit")`.
4. **Table Commit Execution (Dual-Table Isolated):**
   - **`client_vitals`**: Calls `commit_buffer(src, table_name="client_vitals")` and `sync_data(src, table_name="client_vitals")`. Catches table-specific errors.
   - **`client_errors`**: Calls `commit_buffer(src, table_name="client_errors")` and `sync_data(src, table_name="client_errors")`. Catches table-specific errors.
5. **Ledger Publication:**
   - Calls `_mark_ledger_published(service_id, rum=True)` **only if both** tables committed without errors (prevents premature raw deletion).
6. **Local Compaction Asynchronous Triggers:**
   - Launches daemon threads for `compact_local_partitions` on `client_vitals` and `client_errors` with defensive try/except logging.
7. **Telemetry, Status & Finalization:**
   - If both tables fail: records status `"error"`.
   - If one table succeeds and one fails: records status `"warning"` with partial accounting (e.g. `Partial RUM commit: 2 vitals (50 rows) and 0 errors; errors failed: ...`).
   - If both succeed: records status `"success"` with total rows and files committed.
   - Guaranteed `finally:` resets Boto3 caller hint, ends progress, and calls `finalize_cron_run_if_running`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **FOS S3 Calls:** S3 uploads must be logged in `usage_log.db` as Class A PUT calls under `_BOTO3_CALLER_HINT="rum_commit"`.
  - **DuckLake Procedures:** DDL/DML commit procedures must be recorded in `telemetry_queries`.
  - **Lock Contention:** Wait time for RUM commit lock must remain < 50ms.
- **Timing & Resource Budgets:**
  - Commit transaction duration: < 1.0 second.
  - S3 upload throughput: > 50 MB/s.
- **Audit Checklist:**
  - Verify that committed local buffer files are unlinked after successful catalog commit.
  - Confirm analytical queries against `/api/rum/overview` immediately reflect the newly committed beacons.

---

## 7. Failure Modes & Recovery Runbooks
- **Partial Table Failure:** If `client_errors` fails while `client_vitals` succeeds, status is `"warning"`, ledger is kept un-published so raw data is preserved, and next tick retries.
- **Catalog Commit Conflict:** Automatically retries with exponential backoff.
- **Upload Network Failure:** Local RUM buffer files are preserved; next interval tick retries upload cleanly.
- **Disk Full:** Aborts prior to catalog modification, emitting status `"error"` with disk alert message.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger RUM Commit:** `POST /api/admin/rum/commit/{service_id}`.
- **Inspect Status:** `GET /api/admin/rum/status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [x] 1. Automated unit tests verified in `tests/cron/test_rum_commit.py`.
- [x] 2. Verified full commit success with both tables: status `"success"`, row counts and files recorded.
- [x] 3. Verified partial commit failure: status `"warning"`, raw ledger kept safe.
- [x] 4. Verified politeness gate: background run defers during active queries; manual/force bypasses.
- [x] 5. Verified disk space pre-check: aborts cleanly with status `"error"` on low disk.
- [x] 6. Verified progress tracking lifecycle: `start_progress` and `end_progress` clean teardown.
