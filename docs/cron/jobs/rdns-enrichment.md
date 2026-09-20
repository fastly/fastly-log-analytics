> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `rdns_enrichment` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `rdns_enrichment`

## 1. Overview & Objectives
- **Job Identifier:** `rdns_enrichment`
- **Category:** IP Intelligence & Asynchronous Network Enrichment
- **Purpose:** Discovers top client IP addresses from recent traffic in DuckDB/DuckLake that lack reverse DNS hostname mappings and executes asynchronous PTR queries, caching the resolved hostnames in SQLite `rdns_cache.db`.
- **Why It Runs:** Fastly access logs contain client IP addresses, but reverse DNS hostnames (e.g. `*.googlebot.com`, `*.crawl.yahoo.net`) provide vital human-readable context for security audits, bot detection, and traffic classification. Performing synchronous DNS lookups during request serving is prohibitively slow (> 100ms per IP). Running background asynchronous resolution enriches IP records ahead of time with zero serving-path latency penalty.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 5 minutes (`minutes=5`).
- **Scope:** Process-global singleton job.
- **Jitter & Misfire Policy:**
  - Jitter: 15 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Queries top IPs from active services using `execute_with_stale_view_retry()`, resolves PTR records asynchronously, writes to `data/cache/rdns_cache.db`. | Local SQLite thread-local pool lock. Gated by `FLA_DEV_NO_CRONS=1` (network outbound). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Queries top IPs from ClickHouse/DuckLake; populates shared/local rDNS cache. | Local SQLite lock; never scheduled to Celery workers. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/rdns/enrich` | Reverse DNS hostnames in Network and Security tables. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Enriches hostnames locally on analyst instance. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads enriched hostnames from shared server cache. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Unresolved IP Discovery:**
   - Queries recent log traffic (last 1-2 hours) across configured services for high-volume client IPs:
     ```sql
     SELECT client_ip, COUNT(*) as hits
     FROM logs
     WHERE timestamp >= NOW() - INTERVAL '1 HOUR'
     GROUP BY client_ip
     ORDER BY hits DESC
     LIMIT 500;
     ```
   - Checks candidates against SQLite `rdns_cache` to filter out already resolved or recently attempted IPs.
2. **Asynchronous PTR Resolution:**
   - Batches up to 100 unresolved IPs per tick.
   - Dispatches non-blocking PTR queries via `asyncio` / thread pool with strict 2.0s timeouts per lookup.
3. **Cache Storage:**
   - Stores resolved hostname or NXDOMAIN/timeout marker into SQLite `rdns_cache` with a 7-day TTL.
4. **Telemetry & Log Recording:**
   - Emits resolved count and resolution failure count in `cron_runs`.
   - Records DNS query latency in `telemetry_queries`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **DNS Query Timings:** Average and p95 DNS PTR lookup latency must be recorded.
  - **DuckDB Discovery:** Uses `execute_with_stale_view_retry()` to prevent crashes during concurrent Parquet buffer rotation.
  - **Zero FOS Calls:** Strictly local DuckDB + external DNS calls; zero cloud FOS calls.
- **Timing & Resource Budgets:**
  - Discovery query duration: < 300ms.
  - DNS resolution batch: < 3.0 seconds total across all 100 IPs.
- **Audit Checklist:**
  - Verify `rdns_cache` TTL expiration prevents stale hostname pollution.
  - Confirm DNS timeouts do not block the scheduler executor threads.

---

## 7. Failure Modes & Recovery Runbooks
- **Upstream DNS Resolver Down:** Backs off gracefully, caches temporary failure markers for 1 hour to prevent hammering the resolver.
- **Stale DuckDB Buffer Error:** Automatically self-heals via `execute_with_stale_view_retry()`.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Enrichment:** `POST /api/admin/rdns/enrich`.
- **Inspect rDNS Cache Size:** `GET /api/admin/system/cache-stats`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Insert synthetic log rows with well-known public IPs (e.g. `8.8.8.8`).
- [ ] 2. Trigger `POST /api/admin/rdns/enrich`; confirm HTTP 200.
- [ ] 3. Verify in SQLite `rdns_cache.db`: `8.8.8.8` resolves to `dns.google`.
- [ ] 4. Confirm subsequent queries for `8.8.8.8` return cached hostname without outbound DNS lookup.
- [ ] 5. Confirm `cron_runs` records execution status `success`.
