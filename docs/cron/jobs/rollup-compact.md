> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `rollup_compact_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `rollup_compact_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `rollup_compact_{service_id}`
- **Category:** Rollup Compaction & Multi-Day Query Accelerator
- **Purpose:** Daily consolidation of 24 hourly rollup bundle files into a single, highly compressed per-day Parquet file (`day_bundle_YYYY-MM-DD.parquet`) for closed days over a 30-day lookback window.
- **Why It Runs:** Long-range queries (7-day, 30-day views across Dashboard, Origin, Security, Network) would otherwise need to open and scan 168 to 720 individual hourly Parquet files. By merging closed days into single day bundles, filesystem open/seek overhead is reduced by 96%, ensuring 30-day multi-million row dashboard queries return in under 300ms.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 02:00 UTC (`hour=2, minute=0`).
- **Timing Rationale:** Runs before `full_sync` (03:30 UTC) and `optimize` (04:00 UTC) so that all rollups for previous days are finalized prior to cloud maintenance.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Reads 24 hourly bundles in `rollups/{service_id}/hour_bundles/`, writes `day_bundle_YYYY-MM-DD.parquet`, unlinks hourly bundles. | Exclusive per-service rollup lock. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). Yields via `should_defer_cron("rollup_compact")` if user queries are active. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Consolidates local day rollups on web serving pods across 11 subsystems. | Pod-local file lock; never runs on Celery workers. Yields via `should_defer_cron("rollup_compact")` if user queries are active. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rollups/compact/{service_id}` | Accelerated multi-day dashboard querying. Live SSE progress updates in Admin UI. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Keeps multi-day queries fast on analyst laptops. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Benefits transparently from server-side compaction. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Active-Request Gate:** Evaluates `should_defer_cron("rollup_compact", service_id)`. If an analyst is currently running queries, compaction defers gracefully.
2. **Progress Initialization:** Calls `start_progress(run_id, task="rollup_compact")` to stream real-time stage updates to the Admin UI via SSE.
3. **Candidate Day Discovery:** Scans `rollups/{service_id}/hour_bundles/` for completed calendar days (00:00 - 23:00 UTC) older than today that have not yet been compacted into a day bundle.
4. **Deep Pass Lookback (30 Days):** Evaluates up to 30 closed days across all 11 rollup subsystems:
   - Dashboard & Overview Top-N day bundles (`day_bundle_YYYY-MM-DD.parquet`)
   - IP-spread unique cardinality counters
   - Origin summary & Origin latency percentiles
   - Network RTT percentiles & Network speed aggregations
   - Network quality & Performance latency
   - Security dimensions & NGWAF / Verified bots
5. **DuckDB Merge & Aggregation:**
   - Issues vectorized in-memory DuckDB queries reading the 24 hour bundles for each day.
   - Merges Top-N arrays, recomputes unique cardinality approximations, and sums metric counters.
6. **Day Bundle Materialization:** Writes `day_bundle_YYYY-MM-DD.parquet` using ZSTD compression.
7. **Retirement of Hourly Bundles:** Safely unlinks the 24 hourly files once the day bundle is verified.
8. **Accurate Status Logging:**
   - Emits completion metrics to `cron_runs` (`days_compacted`, `bytes_reduced`, `duration_s`).
   - If ANY subsystem compaction encountered an error, records `status="warning"` in `cron_runs` (never `success`), populates `error_message` with the failed subsystems, and surfaces it in `/api/admin/health-snapshot`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Local-Only Safety:** Must never make outbound S3/FOS calls.
  - **DuckDB Statements:** Day bundle creation queries are recorded in `telemetry_queries`.
  - **Compression Efficiency:** Must achieve > 40% byte reduction compared to sum of 24 hourly files.
- **Timing & Resource Budgets:**
  - Day compaction throughput: > 50,000 rollup rows/sec.
  - Maximum daily run duration: < 60 seconds across 30 days.
- **Audit Checklist:**
  - Confirm day bundle preserves exact mathematical sums for count, bytes, and error tallies.
  - Verify that hourly files are deleted only AFTER the day bundle passes read validation.
  - Verify zero Class A FOS calls in `usage_log.db`.

---

## 7. Failure Modes & Recovery Runbooks
- **Incomplete Day Data:** If a day is missing some hours, compaction skips that day until `rollup_heal` backfills the missing hours.
- **Parquet Write Interruption:** Temporary files use `.tmp` extensions; corrupted temporary files are purged on boot.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Compaction:** `POST /api/admin/rollups/compact/{service_id}`.
- **Inspect Status:** `GET /api/admin/rollups/status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Generate 24 hourly synthetic rollup bundles for a past UTC day.
- [ ] 2. Trigger `POST /api/admin/rollups/compact/{service_id}`; confirm HTTP 200.
- [ ] 3. Verify `day_bundle_YYYY-MM-DD.parquet` is created.
- [ ] 4. Confirm the 24 individual hourly bundle files are retired/unlinked.
- [ ] 5. Run a 7-day query on `/api/dashboard/bundle`; confirm query reads the day bundle directly.
- [ ] 6. Confirm query response time is < 150ms.
