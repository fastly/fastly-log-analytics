> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `partial_hour_merge_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `partial_hour_merge_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `partial_hour_merge_{service_id}`
- **Category:** Speed-Layer Rollup & Active-Hour Query Acceleration
- **Purpose:** Incrementally folds newly arrived buffer writes from the active hour into a pod-local partial-hour rollup file every 30 seconds.
- **Why It Runs:** Real-time dashboards (`/dashboard`, `/origin`, `/security`) frequently query the current in-progress hour. Without partial-hour rollups, every render must re-scan all raw buffer files written since the top of the hour. This job ensures live scans only need to process data written "since the last 30s tick", slashing dashboard p95 query latency from seconds to < 50ms.

---

## 2. Scheduling & Cadence
- **Trigger Type:** High-frequency interval timer (`interval`)
- **Default Schedule:** Every 30 seconds (`seconds=30`).
- **Configurable Overrides:**
  - `PARTIAL_HOUR_MERGE_INTERVAL_SEC` (env var integer, default: 30, min: 10, max: 120).
  - `PARTIAL_HOUR_MERGE_ENABLED=true` (env var toggle, default: true).
- **Jitter & Misfire Policy:**
  - Jitter: 5 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=60s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Scans local buffer files for current UTC hour in `cache/{bucket}/buffer/`, incrementally aggregates into `rollups/partial_hour/hour=<H>/all_fields.parquet`. | Pod-local rollup write lock. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). Yields via `should_defer_cron` if user queries are active. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Because Celery workers write directly into the shared DuckLake table, local buffer directories are empty. The job detects 0 local files and exits in < 1ms. Active-hour queries on the web pod query DuckLake directly with partition pruning (~150-250ms). | Pod-local execution; read-only DuckLake connection. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | Internal trigger | Fast active-hour dashboard renders. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Maintains local partial rollups. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Benefits transparently from speed layer. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Active Hour Timestamp Calculation:** Computes current UTC hour bucket (`toStartOfHour(NOW())`).
2. **Scan New Increments:** Detects buffer or partition files with timestamps newer than the last partial-hour rollup snapshot.
3. **Rollup Aggregation:** Executes lightweight DuckDB aggregation across dimensions (status, PoP, country, domain, content_type) for the incremental window.
4. **Merge with Existing Partial Rollup:** Unifies existing partial-hour accumulator with new increments into a fresh `partial_hour_current.parquet`.
5. **Atomic File Replace:** Replaces active partial-hour rollup file atomically.
6. **Top-of-Hour Rollover:** When the hour rolls over, the completed hour is handed off to `rollup_heal` / standard rollups, and a new partial-hour file initializes.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local-only operation; zero cloud API calls.
  - **DuckDB Statements:** Rollup aggregation queries are recorded in `telemetry_queries`.
  - **Duration Budget:** Execution must complete in < 200ms to prevent CPU contention with incoming dashboard requests.
- **Audit Checklist:**
  - Confirm partial-hour queries utilize columnar aggregation without full raw row scans.
  - Verify `app.thread_wait_ms` does not spike during 30s tick execution.
  - Confirm zero network I/O in `usage_log.db`.

---

## 7. Failure Modes & Recovery Runbooks
- **Corrupt Partial File:** If the partial-hour file is unreadable, it is deleted and re-accumulated from active-hour raw buffer files on the next tick.
- **High Concurrency Contention:** If serving queries hold DuckDB connections, `partial_hour_merge` yields via cooperative locking.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Toggle Enabled:** `PARTIAL_HOUR_MERGE_ENABLED=false` env var.
- **Inspect Rollup Status:** Surfaces in `/admin/trends` and Live Query Monitor.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Write fresh synthetic log rows for the current UTC hour.
- [ ] 2. Wait 30 seconds for `partial_hour_merge` to fire.
- [ ] 3. Verify `rollups/{service_id}/` contains an updated `partial_hour_*.parquet` file.
- [ ] 4. Query `/api/dashboard/bundle` for a 15-minute window; verify DuckDB queries hit the partial rollup.
- [ ] 5. Confirm query duration for the active-hour panel is < 50ms.
- [ ] 6. Verify zero FOS Class A/B calls recorded in `usage_log.db`.
