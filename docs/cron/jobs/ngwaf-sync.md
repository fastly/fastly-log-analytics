> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `ngwaf_sync_{service_id}` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `ngwaf_sync_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `ngwaf_sync_{service_id}`
- **Category:** Fastly Next-Gen WAF (Signal Sciences) Intelligence Sync
- **Purpose:** Synchronizes security signals, attack indicators, blocked IP addresses, and rate-limit triggers from the Fastly Next-Gen WAF (NGWAF) API into local SQLite `ngwaf_bot_cache.db`.
- **Why It Runs:** Fastly edge logs include NGWAF security tags (e.g. `SIGSCI-TAGS`, `x-sigsci-tags`). To cross-correlate edge log hits with WAF decisions and display comprehensive threat actor profiles on the Security and WAF dashboards, the system periodically imports active attack signals directly from the Fastly NGWAF API.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 5 minutes (`minutes=5`).
- **Configurable Overrides:** `provisioning.cron_ngwaf.interval_mins` (default: 5).
- **Registration Gate:** Registered **ONLY** if `get_ngwaf_workspace_id(service_id)` resolves to an active NGWAF workspace.
- **Jitter & Misfire Policy:**
  - Jitter: 15 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Calls Fastly NGWAF API, parses security events, writes to `data/ngwaf/ngwaf_bot_cache.db`. | Local SQLite thread-local pool lock. Gated by `FLA_DEV_NO_CRONS=1` (outbound API). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Calls NGWAF API and populates shared/local SQLite bot cache. | Local SQLite lock; never scheduled to Celery workers. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/ngwaf/sync/{service_id}` | WAF signals and threat intelligence on Security pages. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Synchronizes WAF signals locally if credentials provided. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads enriched WAF intelligence from shared server. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Prerequisite Check:** Confirms NGWAF API access token and workspace ID are configured. Checks `FLA_DEV_NO_CRONS=1`.
2. **NGWAF API Request:**
   - Issues authenticated GET request to Fastly NGWAF API:
     `https://dashboard.signalsciences.net/api/v0/corps/{corp}/sites/{site}/activity`
   - Requests recent suspicious IP events and active attack tags.
3. **Event Extraction & Filtering:**
   - Extracts: `ip`, `signals`, `reasons`, `action` (block, allow), `request_count`, `window_start`.
4. **SQLite Cache Upsert:**
   - Performs batch upsert into `ngwaf_bot_cache.db` table `ngwaf_activity`:
     ```sql
     INSERT INTO ngwaf_activity (ip, signals, action, last_seen)
     VALUES (?, ?, ?, ?)
     ON CONFLICT(ip) DO UPDATE SET signals = excluded.signals, last_seen = excluded.last_seen;
     ```
5. **Telemetry & Log Recording:**
   - Records synced IP count, API response time, and HTTP status in `cron_runs`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **Fastly NGWAF API Calls:** Latency, rate-limit headers (`X-RateLimit-Remaining`), and HTTP status must be recorded.
  - **SQLite Operations:** Every batch upsert must flow through `ThreadLocalPool` with instrumented timings.
  - **Zero FOS Calls:** Strictly external NGWAF API and local SQLite; zero cloud FOS calls.
- **Timing & Resource Budgets:**
  - API call latency: < 1.0 second.
  - SQLite upsert duration: < 50ms.
- **Audit Checklist:**
  - Verify that NGWAF API rate limits are not exceeded.
  - Verify expired WAF events (> 7 days) are pruned automatically.

---

## 7. Failure Modes & Recovery Runbooks
- **NGWAF 401 / Invalid Token:** Logs clear authentication error in `cron_runs`; suppresses continuous retry until credentials updated.
- **NGWAF 429 / Rate Limited:** Exponential backoff with jitter; retries on next 5m tick.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger NGWAF Sync:** `POST /api/admin/ngwaf/sync/{service_id}`.
- **Inspect WAF Cache:** `GET /api/admin/ngwaf/status?service_id={service_id}`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/ngwaf/sync/{service_id}`; confirm HTTP 200.
- [ ] 2. Call `GET /api/admin/ngwaf/status?service_id={service_id}`; verify non-zero synced records.
- [ ] 3. Query `ngwaf_bot_cache.db`; confirm `ngwaf_activity` table contains recent IPs.
- [ ] 4. Confirm in `cron_runs`: status `success` with non-zero duration.
- [ ] 5. Confirm zero FOS Class A/B calls in `usage_log.db`.
