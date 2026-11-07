# Background Job Specification: `insights_prewarmer_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `insights_prewarmer_{service_id}`
- **Category:** Analytics Cache Prewarming & Anomaly Pre-Computation
- **Purpose:** Precomputes baseline statistical distributions and anomaly detection metrics for the Insights page (`/insights`) against ClickHouse (`backend/high_scale/insights/`), prewarming the in-memory cache every 240 seconds.
- **Why It Runs:** Calculating statistical anomaly baselines across 51 detectors requires multi-dimensional aggregations and percentile calculations. The cache TTL is 300 seconds (`INSIGHTS_CACHE_TTL = 300`). The 240s cadence keeps the adaptive default warm for instant page loads.
- **Resource Policy:** Warm only the adaptive default for admin and eligible analyst shapes, using `max_workers=1`. Do not automatically warm the additional 24h/720h month comparison; it remains available on demand.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 240 seconds (`seconds=240`, `jitter=15`).
- **Timing Rationale:** Strictly runs inside the 300s cache TTL window (`240s < 300s`) so default insight queries never encounter a cold cache miss. On High-Scale a prewarm joins any evaluation already running for the same shape instead of starting a second scan, and page requests between refreshes are served the last result while it refreshes (see [Insights](../../pages/insights.md#backend)).
- **Configurable Overrides:**
  - `provisioning.cron_insights_prewarmer.enabled` (default: `true`).
  - `provisioning.cron_insights_prewarmer.interval_seconds` (default: `240`).
- **Jitter & Misfire Policy:**
  - Jitter: 15 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=360s`.

---

## 3. Architecture Execution Matrix
| Component | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Unified ClickHouse Architecture** | APScheduler (Backend Pod) | Queries ClickHouse via `backend/high_scale/insights/` and populates module memory cache `_insights_cache`. | Active request politeness gate (`should_defer_cron`) defers when user queries are active. Permitted under `FLA_DEV_NO_CRONS=1`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | Internal trigger / `POST /api/insights` | Instantaneous `/insights` render. |
| **Analyst (Remote Share)** | Active (Admin Process) | Server-side execution | Prewarms distinct active analyst clamp shapes (`analyst_clamp_cache_key`) while remote sharing is active. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Source & Politeness Check:**
   - Loads source via `get_source_for_service(service_id)`.
   - Checks `should_defer_cron("insights_prewarmer", service_id)`. If active user requests are running on the dashboard, defers to avoid query contention.
2. **Adaptive Parameter Resolution:**
   - Reads cached extent status via `svcconfig.get_status(src["name"])`.
   - Derives `(window_hours, baseline_hours)` adaptively from `history_hours_from_earliest(...)` using `pick_insights_default` (e.g. >= 7d history defaults to 24h/168h, matching the frontend picker).
3. **Execution & Progress Initialization:**
   - Calls `start_cron_run(src, "insights_prewarmer")`.
   - Initializes live tracking via `cleanup_progress_and_reap()` and `start_progress(run_id, service_id=service_id, task="insights_prewarmer")`.
4. **Admin Prewarming:**
   - Calls `hs_insights.get_high_scale_insights(...)` with `force_refresh=True` and `max_workers=1`.
   - Recomputes and populates `_insights_cache` for the admin view.
5. **Analyst Shape Prewarming (Remote Share):**
   - If sharing is live (`is_sharing_active()`), extracts distinct clamp shapes from active invites (capped at `_MAX_ANALYST_SHAPES = 8`).
   - For each active clamp shape, computes clamp extents via `resolve_analyst_insights_clamp` and warms the cache with `clamp_cache_key` and `max_workers=1`.
6. **Telemetry & Duration Finalization:**
   - Emits done event to `cron_progress`.
   - Logs `success` run to `cron_runs` with detailed summary of warmed shapes and hours.
   - In guaranteed `finally:` block: calls `end_progress(run_id)` and `finalize_cron_duration(src, run_id, started)`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly ClickHouse execution; 0 cloud storage calls.
  - **ClickHouse Statements:** Analytical queries registered via `query_registry` under category `"insights"`.
- **Timing & Resource Budgets:**
  - Prewarm query execution: < 2.0s.
  - Memory consumption: < 25MB cached payload across admin and analyst shapes.
- **Audit Checklist:**
  - Verify user request to `/api/insights` returns `is_cached=True` after prewarmer run.
  - Verify active dashboard queries cause prewarmer to defer cleanly without logging error.

---

## 7. Failure Modes & Recovery Runbooks
- **Active Dashboard Load:** Politeness gate defers prewarming cleanly.
- **Prewarmer Exception:** Logged as `"error"` in `cron_runs`; dashboard users fall back gracefully to on-demand query execution upon loading `/insights`.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Interactive Warm Trigger:** `POST /api/insights` with the service and `window_hours` / `baseline_hours` in the request body (forces execution and populates cache).

---

## 9. Verification Checklist
- **Requirement 1 — Cadence and Trigger:** Interval timer running every 240 seconds (`seconds=240`, `jitter=15`) strictly within the 300s cache TTL (`INSIGHTS_CACHE_TTL = 300`).
- **Requirement 2 — Dynamic Registration Gating, Rescheduling & Unregistration:** Job disabled when `cron_insights_prewarmer.enabled=False`; rescheduled with custom interval on `cron_insights_prewarmer.interval_seconds` update; cleanly unregistered and removed from scheduler when disabled.
- **Requirement 3 — Pod Safety:** Runs on the web serving pod's APScheduler to warm the pod-local `_insights_cache`.
- **Requirement 4 — Local Safety under `FLA_DEV_NO_CRONS=1`:** Registered and executed as a local-safe read-only cache prewarmer during dev mode via `_register_dev_local_safe_jobs`.
- **Requirement 5 — Politeness Deferral:** Cleanly defers execution via `should_defer_cron("insights_prewarmer", service_id)` when active user queries are running on the dashboard.
- **Requirement 6 — Adaptive Parameter Resolution:** Adaptively derives `(window_hours, baseline_hours)` based on service log history extents via `pick_insights_default`.
- **Requirement 7 — Admin Cache Prewarming & Subsequent Reuse:** Populates in-memory `_insights_cache` via ClickHouse insights with `force_refresh=True`, ensuring subsequent dashboard requests hit the cache (`is_cached=True`).
- **Requirement 8 — Remote Share Analyst Clamp Shapes:** When remote sharing is active (`is_sharing_active()`), extracts distinct active clamp shapes (capped at `_MAX_ANALYST_SHAPES = 8`) and prewarms each clamp cache key.
- **Requirement 9 — Telemetry, Progress & Outcome Logging:** Emits `cron_progress` SSE events, attributes analytical queries to category `"insights"`, and records `success` run in PostgreSQL `cron_runs`.
- **Requirement 10 — Guaranteed Duration Finalization:** Ensures `finalize_cron_duration` and `end_progress` are executed in a guaranteed `finally` block even when prewarming errors.
