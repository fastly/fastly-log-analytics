# Background Job Specification: `rdns_enrichment`

## 1. Overview & Objectives
- **Job Identifier:** `rdns_enrichment`
- **Category:** IP Intelligence & Asynchronous Network Enrichment
- **Status:** Verified (Automated Unit Tests & Lifecycle Audited)
- **Purpose:** Discovers client IP addresses from recent traffic in DuckDB/DuckLake that lack reverse DNS hostname mappings and executes asynchronous PTR queries with Forward-Confirmed reverse DNS (FCrDNS) verification, caching the resolved hostnames in SQLite `rdns_cache.db`.
- **Why It Runs:** Fastly access logs contain client IP addresses, but reverse DNS hostnames (e.g. `*.googlebot.com`, `*.crawl.yahoo.net`) provide vital human-readable context for security audits, bot detection, and traffic classification. Performing synchronous DNS lookups during request serving is prohibitively slow (> 100ms per IP). Running background asynchronous resolution enriches IP records ahead of time with zero serving-path latency penalty.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 5 minutes (`minutes=5`).
- **Scope:** Process-global singleton job registered on the Scheduler (`system_jobs`).
- **Jitter & Misfire Policy:**
  - Jitter: 15 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process Web Pod) | Discovers distinct IPs across active services using `execute_with_stale_view_retry()`, resolves PTR records asynchronously via `aiodns`, and writes to `data/cache/rdns_cache.db`. | Local SQLite thread-local pool write lock (`_write_lock`). Gated by `FLA_DEV_NO_CRONS=1` (network outbound suppressed). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Queries distinct IPs from DuckLake views; populates local/shared rDNS cache. | Process-global singleton; never scheduled to Celery workers. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rdns/enrich` | Reverse DNS hostnames in Network and Security tables; full cache stats via `GET /api/admin/rdns/stats` and `GET /api/admin/bot-sources`. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Enriches hostnames locally on analyst instance. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads enriched hostnames from shared server cache without scheduling independent jobs. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Dev Mode Gate:**
   - Evaluates `dev_mode_no_crons()`. If active (`FLA_DEV_NO_CRONS=1`), immediately skips outbound DNS queries and view scans, recording `status="skipped"` and `"skipped (dev_mode_no_crons)"` in `system_jobs`.
2. **Dynamic Queue-Depth Sizing:**
   - Probes the count of unresolved (`status='pending'`) IPs in `rdns_cache.db`:
     - Empty / minimal backlog (`pending == 0`): Defaults to baseline 200 IPs.
     - Small backlog (`0 < pending <= 200`): Drains up to `max(100, pending)` IPs.
     - Medium backlog (`200 < pending <= 2000`): Scales up to 500 IPs per tick.
     - High backlog (`pending > 2000`): Caps at safety ceiling of 1,000 IPs per tick to preserve scheduler responsiveness.
   - An explicit `limit` parameter can also be supplied via manual API trigger to bypass dynamic scaling.
3. **Asynchronous PTR Resolution:**
   - Dispatches non-blocking PTR queries via `aiodns` with a concurrency limit of 50 and a strict 2.0s timeout per lookup.
   - Performs FCrDNS (Forward-Confirmed reverse DNS) validation: ensures the returned PTR hostname resolves forward back to the original IP address.
4. **Stale Record Refresh:**
   - Re-resolves stale IP records older than 48 hours (proportional to `batch_limit // 4`).
5. **Unresolved IP Discovery:**
   - Queries distinct client IPs from the last 30 days across all configured services with an `ip` column using `execute_with_stale_view_retry()`:
     ```sql
     SELECT DISTINCT ip FROM logs
     WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
       AND ip IS NOT NULL
     LIMIT 500;
     ```
   - Filters out already-cached IPs and enqueues up to 500 new IPs into `rdns_cache.db` as `status='pending'`.
6. **Reaping & Retention Pruning:**
   - Runs daily (`_maybe_reap_stale_rows`):
     - Deletes `nxdomain` and `error` records older than 30 days.
     - Enforces `_MAX_TOTAL_ROWS = 250,000` row cap by pruning oldest negative cache entries if cache size exceeds limit.
7. **Status & Telemetry Recording:**
   - Decorated with `@global_job("rdns_enrichment")`.
   - Records outcome into `system_jobs._status["rdns_enrichment"]`:
     - `status="success"`: Normal run with resolved, error, and discovered counts.
     - `status="warning"`: Resolution errors encountered with 0 successful resolutions (indicating upstream DNS resolver timeout or failure).
     - `status="skipped"`: Bypassed by `FLA_DEV_NO_CRONS=1`.
     - `status="error"`: Unhandled exception caught and logged.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **DuckDB Discovery:** Uses `execute_with_stale_view_retry()` to prevent failures during concurrent Parquet buffer rotation.
  - **Zero FOS Calls:** Strictly local DuckDB + external DNS calls; zero cloud FOS calls.
- **Timing & Resource Budgets:**
  - Discovery query duration: < 500ms.
  - DNS resolution batch: Typically 1.0–4.0s for 100–500 IPs across 50 concurrent async workers.
  - Wall-clock runtime captured in `system_jobs._status["rdns_enrichment"]["duration_s"]`.
- **Audit Checklist:**
  - Verified `rdns_cache` TTL expiration and daily reap prevents stale hostname pollution.
  - Confirmed DNS timeouts do not block scheduler threads (uses asyncio event loop with 2.0s per-lookup timeout).

---

## 7. Failure Modes & Recovery Runbooks
- **Upstream DNS Resolver Down / Degraded:**
  - Per-address resolver timeouts and network failures are counted in `errors`; other lookups continue and pending addresses remain eligible for a later tick.
  - If `errors > 0` and `resolved == 0`, the job records `status="warning"` with `(DNS lookup failures detected)`. A mixed batch with at least one successful resolution remains `status="success"` and exposes the error count in its summary.
  - An unhandled batch or storage exception records `status="error"`; this is distinct from ordinary per-address lookup failures.
  - Next scheduled tick automatically retries remaining pending IPs.
- **Stale DuckDB Buffer View Race:**
  - Automatically caught and healed via `execute_with_stale_view_retry()`.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Enrichment:** `POST /api/admin/rdns/enrich` (accepts optional `limit` query param).
- **Inspect rDNS Cache Stats:** `GET /api/admin/rdns/stats` and `GET /api/admin/bot-sources`.
- **System Job Status:** `GET /api/admin/system-jobs`.

---

## 9. Automated Verification Checklist
- [x] 1. Unit tests verify `_run_rdns_enrichment` records `success` with resolved, error, and discovered counts.
- [x] 2. Unit tests verify `_run_rdns_enrichment` records `warning` when DNS resolution fails with zero successes.
- [x] 3. Unit tests verify `_run_rdns_enrichment` skips when `FLA_DEV_NO_CRONS=1` is active (`status="skipped"`).
- [x] 4. Unit tests verify dynamic queue-depth batch sizing (100 baseline, 500 medium, 1,000 safety ceiling).
- [x] 5. Unit tests verify manual `POST /api/admin/rdns/enrich` and `GET /api/admin/rdns/stats` endpoints.
- [x] 6. Regression suite verifies backward compatibility with existing scheduler tests.
