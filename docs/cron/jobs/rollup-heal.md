# Background Job Specification: `rollup_heal_{service_id}`

> **Status:** VERIFIED (All 4 Environments Active)
> **Task Name in `cron_runs`:** `rollup_hour_heal`
> **Scheduler Job ID:** `rollup_heal_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `rollup_heal_{service_id}`
- **Task Identifier (`cron_runs`):** `rollup_hour_heal`
- **Category:** Rollup Self-Healing & Top-N Completeness
- **Purpose:** Idempotently scans the previous 48 hours (2 days) to discover, rebuild, and backfill missing closed-hour Top-N rollup bundles.
- **Why It Runs:** Fastly log delivery has variable latency (typically 1-3 minutes). Bursty services or delayed log delivery can result in an hour closing before all its data lands in the buffer. Without hourly self-healing, closed hours whose last logs arrived late would be permanently omitted from precomputed Top-N rollups, forcing the dashboard to return incomplete data or fall back to slow raw scans until the daily 02:00 UTC compaction. The 48-hour lookback guarantees that edge logs delayed across the UTC midnight boundary (00:00 UTC) are detected and healed prior to the daily 02:00 UTC compaction.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Hourly at 5 minutes past the hour (`minute=5`, e.g. 01:05, 02:05 UTC).
- **Startup Trigger:** Also scheduled with `next_run_time = NOW() + 30s` at scheduler boot.
- **Politeness Gate:** Evaluates `should_defer_cron("rollup_hour_heal", service_id)`. Yields execution if user API queries are actively in flight.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=900s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads committed/buffered logs for closed hours, writes missing bundles into `rollups/{service_id}/hour_bundles/`. | Exclusive per-service rollup lock. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). Yields via `should_defer_cron` if user queries are active. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Generates missing hour rollups from DuckLake view. Incremental startup catch-up capped via `max_missing_hours=1` until complete lookback coverage is verified. | Pod-local lock; never dispatched to Celery workers. Yields via `should_defer_cron` if user queries are active. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/backfill-bundle-rollups` | Complete Top-N rollup coverage. Live SSE progress updates in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Heals local analyst rollup bundles. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Benefits transparently from healed rollups. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Active-Request Gate:** Evaluates `should_defer_cron("rollup_hour_heal", service_id)`. If an analyst is currently running queries, the heal defers gracefully.
2. **Progress Initialization:** Calls `start_progress(run_id, task="rollup_hour_heal")` to stream real-time stage updates to the Admin UI via SSE.
3. **Window Identification:** Establishes lookback window: previous 48 hours (2 days) from current time (`lookback_days = 2`).
4. **Missing Bundle Detection:** Calls `backfill_missing_hour_bundles(service_id, source, lookback_days=2, max_missing_hours=...)`. Identifies any closed UTC hours that lack a completed bundle in `rollups/{service_id}/hour_bundles/`.
5. **Durable-Serving Throttle:** In durable serving mode, if lookback coverage has not yet been verified (`not coverage_ready`), bounds execution to `max_missing_hours = 1` per tick to avoid starving user queries, marking coverage ready once complete.
6. **Idempotent Bundle Computation:** For each missing hour:
   - Queries raw/committed log data bounded strictly by `timestamp >= hour_start AND timestamp < hour_end`.
   - Computes dimensional Top-N metrics: URLs, status codes, PoPs, ASNs, User-Agents, TLS versions, origins, and verified bots.
   - Writes consolidated `hour_bundle_YYYY-MM-DD_HH.parquet`.
7. **Empty Hour Sentinels:** For closed hours with genuinely zero log rows, stamps an empty sentinel bundle so reader queries do not treat the hour as a missing writer gap and re-scan raw parquet on every query.
8. **Validation & Atomic Publish:** Verifies file size and row counts; atomically moves into live rollup directory.
9. **Log & Telemetry:** Records execution summary in `cron_runs` (`status="success"`, `summary="Healed X missing hour(s): Y field rollup(s) rebuilt, Z hour(s) bundled, W empty hour(s) stamped"`, `duration_s`).

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Local-Only Safety:** No outbound S3/FOS calls; reads from local cache or DuckLake view.
  - **DuckDB Operations:** All aggregation queries are tracked via `telemetry_queries`.
  - **Budget:** Single-hour bundle generation must complete in < 1.5s on standard datasets.
- **Audit Checklist:**
  - Verify lookback window is bounded to 48 hours to catch cross-midnight edge delays while preventing runaway backfills.
  - Confirm bundle generation does not double-count events across hour boundaries.
  - Confirm zero Class A FOS PUTs generated by this job.

---

## 7. Failure Modes & Recovery Runbooks
- **Missing Raw Data:** If an hour has zero log rows, an empty marker bundle is written to prevent endless re-computation attempts.
- **Corrupted Existing Bundle:** Checksums and header validation detect truncated parquet files and trigger a rebuild.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Backfill / Heal:** `POST /api/admin/backfill-bundle-rollups`.
- **Inspect Rollup Status:** `GET /api/cron-runs?task=rollup_hour_heal`.

---

## 9. Verification Checklist
- [x] 1. Verified `_run_rollup_hour_heal` registration in `backend/cron/scheduler.py` at `minute=5` and startup (`NOW() + 30s`).
- [x] 2. Verified task mapping `"rollup_heal": "rollup_hour_heal"` in `backend/cron/schedule.py`.
- [x] 3. Verified `lookback_days = 2` (48-hour lookback window) in `backend/cron/jobs/compaction.py`.
- [x] 4. Verified live execution in running container (`fla-hs-backend-1` run id 12063 at 04:05:00Z and run id 12076).
- [x] 5. Verified `cron_runs` table records `task="rollup_hour_heal"` with `status="success"` and comprehensive summary metrics.
