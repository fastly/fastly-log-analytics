> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `insights_prewarmer_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `insights_prewarmer_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `insights_prewarmer_{service_id}`
- **Category:** Analytics Cache Prewarming & Anomaly Pre-Computation
- **Purpose:** Precomputes baseline statistical distributions and anomaly detection metrics for the Insights page (`/insights`), prewarming the in-memory cache every 240 seconds.
- **Why It Runs:** Calculating statistical anomaly baselines across 168 hours of historical data requires multi-dimensional aggregations and percentile calculations that take 1.5–3.0 seconds on cold scans. The cache TTL is 300 seconds (`INSIGHTS_CACHE_TTL = 300`). By running at a 240s cadence, the prewarmer guarantees that the default Insights view (1h active window vs 168h baseline) is always cached, ensuring instantaneous UI loads (< 50ms) with zero wait time for users.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 240 seconds (`seconds=240`).
- **Timing Rationale:** Strictly runs inside the 300s cache TTL window (`240s < 300s`) so default insight queries never encounter a cold cache miss.
- **Jitter & Misfire Policy:**
  - Jitter: 15 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=360s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Queries session DuckDB `logs` view and precomputed rollups; populates module memory cache `_insights_cache`. | Read-only connection from DuckDB pool. Permitted under `FLA_DEV_NO_CRONS=1` (local-safe). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Queries ephemeral DuckDB over DuckLake or ClickHouse dimension rollups; populates local web cache. | Pod-local memory cache; never dispatched to Celery workers. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | Internal trigger | Instantaneous `/insights` render. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Prewarms local analyst instance cache. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reaps cached benefits on shared server. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Parameter Resolution:** Sets default window parameters: `window_hours = 1`, `baseline_hours = 168` (7-day seasonal baseline).
2. **Pre-Check Cache Freshness:** Checks whether the active cache entry for `(service_id, 1, 168)` has > 60s remaining TTL; skips redundant calculation if cache was recently refreshed by an interactive user request.
3. **Multi-Dimension Baseline Query:**
   - Evaluates metrics: error rates, status code distribution, bandwidth, latency p95, top failing URLs, top failing ASNs, bot anomalies.
   - Computes statistical deviations (z-scores and interquartile ranges) between active hour and baseline.
4. **Cache Materialization:** Stores the precomputed payload into the in-memory insights cache with a 300s expiration timestamp.
5. **Telemetry & Log Recording:** Records cache warming duration and metric count in `cron_runs` and `telemetry_queries`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & Resource Capture:**
  - **Zero FOS Calls:** Strictly local cache/DuckDB execution; zero cloud API calls.
  - **DuckDB Statements:** Baseline SQL queries must be recorded in `telemetry_queries`.
  - **Pool Contention:** Must borrow connection with low priority so active user HTTP requests are never queued behind prewarming.
- **Timing & Resource Budgets:**
  - Prewarm query execution: < 1.5s on rollup-accelerated data.
  - Memory consumption: < 15MB cached payload.
- **Audit Checklist:**
  - Verify queries leverage precomputed day/hour rollups for the 168h baseline rather than scanning raw logs.
  - Verify `/api/insights/anomalies` response header returns `X-Cache: HIT`.

---

## 7. Failure Modes & Recovery Runbooks
- **Prewarmer Timeout / Error:** Logs failure to `cron_runs`; the frontend gracefully falls back to executing the query on-demand upon page load.
- **DuckDB Pool Exhaustion:** If all pool connections are busy with user queries, prewarmer aborts immediately rather than queueing.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Interactive Warm Trigger:** `GET /api/insights/anomalies?service_id={service_id}&window_hours=1&baseline_hours=168` (populates cache).

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Allow `insights_prewarmer` to fire or trigger an initial run.
- [ ] 2. Inspect logs: confirm prewarmer completed and populated the cache.
- [ ] 3. Call `GET /api/insights/anomalies?service_id={service_id}`; confirm response time is < 50ms (Cache HIT).
- [ ] 4. Confirm in `cron_runs`: status `success` with non-zero duration.
- [ ] 5. Verify zero FOS Class A/B calls in `usage_log.db`.
