# Background Job Specification: `insights_prewarmer_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `insights_prewarmer_{service_id}`
- **Status:** **Verified** (`release/v3.0.0-beta3`)
- **Category:** Analytics Cache Prewarming & Anomaly Pre-Computation
- **Purpose:** Precomputes baseline statistical distributions and anomaly detection metrics for the Insights page (`/insights`), prewarming the in-memory cache every 240 seconds.
- **Why It Runs:** Calculating statistical anomaly baselines across up to 720 hours (30 days) of historical data requires multi-dimensional aggregations and percentile calculations that take 1.5–20 seconds on cold scans. The cache TTL is 300 seconds (`INSIGHTS_CACHE_TTL = 300`). By running at a 240s cadence, the prewarmer guarantees that the default Insights view (window/baseline derived adaptively from log history) is always cached, ensuring instantaneous UI loads (< 50ms) with zero wait time for users.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 240 seconds (`seconds=240`, `jitter=15`).
- **Timing Rationale:** Strictly runs inside the 300s cache TTL window (`240s < 300s`) so default insight queries never encounter a cold cache miss.
- **Configurable Overrides:**
  - `provisioning.cron_insights_prewarmer.enabled` (default: `true`).
  - `provisioning.cron_insights_prewarmer.interval_seconds` (default: `240`).
- **Jitter & Misfire Policy:**
  - Jitter: 15 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=360s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Queries session DuckDB `logs` view and precomputed rollups; populates module memory cache `_insights_cache`. | Read-only connection from DuckDB pool with `skip_view_update=True`. Active request politeness gate (`should_defer_cron`) defers when user queries are active. Permitted under `FLA_DEV_NO_CRONS=1`. |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Queries ephemeral DuckDB over DuckLake or ClickHouse dimension rollups; populates local web cache. | Pod-local memory cache; never dispatched to Celery workers. Yields if active dashboard queries are executing. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | Internal trigger / `GET /api/insights/anomalies` | Instantaneous `/insights` render. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger / `GET /api/insights/anomalies` | Prewarms local analyst instance cache against local parquet data. |
| **Analyst Path B (Remote Share)** | Active (Admin Process) | Server-side execution | Prewarms distinct active analyst clamp shapes (`analyst_clamp_cache_key`) while remote sharing is active. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Source & Politeness Check:**
   - Loads source via `get_source_for_service(service_id)`.
   - Checks `should_defer_cron("insights_prewarmer", service_id)`. If active user requests are running on the dashboard, defers to avoid DuckDB thread/CPU contention.
2. **Adaptive Parameter Resolution:**
   - Reads cached extent status via `svcconfig.get_status(src["name"])`.
   - Derives `(window_hours, baseline_hours)` adaptively from `history_hours_from_earliest(...)` using `pick_insights_default`. (e.g. >= 30d history defaults to 1h/720h, matching the frontend picker).
3. **Execution & Progress Initialization:**
   - Calls `start_cron_run(src, "insights_prewarmer")`.
   - Initializes live tracking via `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="insights_prewarmer")`.
4. **Connection Acquisition & Memory Tuning:**
   - Opens read-only DuckDB connection: `get_connection(source=src, max_wait=5, read_only=True, skip_view_update=True)`.
   - Applies unconstrained memory limit (`DUCKDB_MEMORY_LIMIT`) if configured to prevent OOM on 720h scans.
5. **Admin Prewarming:**
   - Calls `get_insights(con, src, window_hours, baseline_hours, service_id, force_refresh=True)`.
   - Recomputes and populates `_insights_cache` for the admin view.
6. **Analyst Shape Prewarming (Remote Share):**
   - If sharing is live (`is_sharing_active()`), extracts distinct clamp shapes from active invites (capped at `_MAX_ANALYST_SHAPES = 8`).
   - For each active clamp shape, computes clamp extents via `resolve_analyst_insights_clamp` and calls `get_insights(...)` with `clamp_cache_key`.
7. **Telemetry & Duration Finalization:**
   - Emits done event to `cron_progress`.
   - Logs `success` run to `cron_runs` with detailed summary of warmed shapes and hours.
   - In guaranteed `finally:` block: closes DuckDB connection, calls `end_progress(run_id)`, and calls `finalize_cron_duration(src, run_id, started)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local DuckDB/DuckLake execution; 0 cloud storage calls.
  - **DuckDB Statements:** Analytical queries instrumented via `track_query` under category `"insights"`.
  - **Connection Borrowing:** Uses `skip_view_update=True` to minimize lock overhead.
- **Timing & Resource Budgets:**
  - Prewarm query execution: < 2.0s on rollup-accelerated data.
  - Memory consumption: < 25MB cached payload across admin and analyst shapes.
- **Audit Checklist:**
  - Verify baseline queries leverage precomputed day/hour rollups where available.
  - Verify user request to `/api/insights/anomalies` returns `X-Cache: HIT` after prewarmer run.
  - Verify active dashboard queries cause prewarmer to defer cleanly without logging error.

---

## 7. Failure Modes & Recovery Runbooks
- **Active Dashboard Load:** Politeness gate defers prewarming cleanly.
- **DuckDB Lock Timeout:** Caught by `RuntimeError("database is locked")`; logs skip message and retries next tick.
- **Prewarmer Exception:** Logged as `"error"` in `cron_runs`; dashboard users fall back gracefully to on-demand query execution upon loading `/insights`.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Interactive Warm Trigger:** `GET /api/insights/anomalies?service_id={service_id}&window_hours=1&baseline_hours=168` (forces execution and populates cache).

---

## 9. Automated Verification Matrix
Verified with 30 passing automated tests across the dedicated contract suite (`tests/cron/test_insights_prewarmer_contract.py` — 12 tests) and the operational test suite (`tests/cron/test_insights_prewarmer.py` — 18 tests):

### Dedicated Contract Test Suite (`tests/cron/test_insights_prewarmer_contract.py`)
- [x] **Requirement 1 — Cadence and Trigger:** Interval timer running every 240 seconds (`seconds=240`, `jitter=15`) strictly within the 300s cache TTL (`INSIGHTS_CACHE_TTL = 300`). Verified via `test_contract_cadence_and_cache_ttl_safety`.
- [x] **Requirement 2 — Dynamic Registration Gating & Rescheduling:** Job disabled when `cron_insights_prewarmer.enabled=False`; rescheduled with custom interval on `cron_insights_prewarmer.interval_seconds` update. Verified via `test_contract_dynamic_registration_gating_and_rescheduling`.
- [x] **Requirement 3 — Dual-Role Registration:** Registered for both Admin (`read_write`) and Analyst Path A (`read_only`). Verified via `test_contract_dual_role_registration_admin_and_analyst_a`.
- [x] **Requirement 4 — Pod Safety:** In distributed High-Scale mode (`DEPLOYMENT_MODE=high_throughput` / `mode=external`), runs strictly on the web serving pod's APScheduler, never routed to RedBeat or Celery workers. Verified via `test_contract_pod_safety_never_routes_to_redbeat`.
- [x] **Requirement 5 — Local Safety under `FLA_DEV_NO_CRONS=1`:** Registered and executed as a local-safe read-only cache prewarmer during dev mode via `_register_dev_local_safe_jobs`. Verified via `test_contract_local_safety_under_fla_dev_no_crons`.
- [x] **Requirement 6 — Politeness Deferral:** Cleanly defers execution via `should_defer_cron("insights_prewarmer", service_id)` when active user queries are running on the dashboard. Verified via `test_contract_politeness_deferral_when_active_queries`.
- [x] **Requirement 7 — Adaptive Parameter Resolution:** Adaptively derives `(window_hours, baseline_hours)` based on service log history extents via `pick_insights_default`. Verified via `test_contract_adaptive_parameter_resolution`.
- [x] **Requirement 8 — Read-Only DuckDB Connection & Memory Tuning:** Acquires read-only DuckDB connection with `skip_view_update=True` and enforces `DUCKDB_MEMORY_LIMIT` without mutating thread pools. Verified via `test_contract_read_only_connection_and_memory_tuning`.
- [x] **Requirement 9 — Admin Cache Prewarming:** Populates in-memory `_insights_cache` via `get_insights` with `force_refresh=True`. Verified via `test_contract_admin_cache_prewarming`.
- [x] **Requirement 10 — Remote Share Analyst Clamp Shapes:** When remote sharing is active (`is_sharing_active()`), extracts distinct active clamp shapes (capped at `_MAX_ANALYST_SHAPES = 8`) and prewarms each clamp cache key. Verified via `test_contract_remote_share_analyst_clamp_shapes`.
- [x] **Requirement 11 — Telemetry, Progress & Outcome Logging:** Emits `cron_progress` SSE events, attributes analytical queries to category `"insights"`, and records `success` run in PostgreSQL `cron_runs`. Verified via `test_contract_telemetry_progress_and_cron_runs_logging`.
- [x] **Requirement 12 — Guaranteed Duration Finalization:** Ensures `finalize_cron_duration`, `end_progress`, and connection close are executed in a guaranteed `finally` block even when prewarming errors. Verified via `test_contract_guaranteed_finalize_cron_duration_in_finally`.
