> [!TODO]
> **Cron Specification Status: PENDING AI SESSION VERIFICATION**
> This specification defines the target execution lifecycle, role/architecture behaviors, telemetry attribution, query audits, and testing checklist for the `bot_data_refresh` background job.
> An AI testing session has not yet verified this background job against a running system. When executing the dedicated verification session, follow the checklist in Section 9, remove this callout, and mark the status as verified.

# Background Job Specification: `bot_data_refresh`

## 1. Overview & Objectives
- **Job Identifier:** `bot_data_refresh`
- **Category:** Threat Intelligence & Verified Bot CIDR Synchronization
- **Purpose:** Synchronizes official published IP CIDR ranges for major search engine crawlers and verified bots (Googlebot, Bingbot, Applebot, DuckDuckBot, Baiduspider, YandexBot) from public JSON feeds into shared SQLite `ngwaf_bot_cache.db`.
- **Why It Runs:** Fastly logs tag incoming requests with User-Agents, but malicious actors frequently spoof legitimate crawler User-Agents. To provide authentic Verified Bot analytics on the Security and Bots dashboards, the system cross-references client IP addresses against official published search engine IP CIDRs. These IP ranges update periodically and must be kept in sync.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 02:00 UTC (`hour=2, minute=0`).
- **Scope:** Process-global singleton job.
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process) | Fetches public HTTP JSON feeds, parses IP subnets, writes to `data/ngwaf/ngwaf_bot_cache.db`. | Local SQLite thread-local pool lock. Gated by `FLA_DEV_NO_CRONS=1` (outbound HTTP). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Fetches feeds and populates shared/local bot cache on web serving pods. | Local SQLite lock; never scheduled to Celery workers. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/bots/refresh` | Verified bot metrics on Security dashboard. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Maintains local bot CIDR cache on analyst instance. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads enriched bot metrics from shared server. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Public Feed Retrieval:**
   - Issues HTTP GET requests with a 10s timeout to official IP feed endpoints:
     - Google: `https://developers.google.com/search/apis/ipranges/googlebot.json`
     - Microsoft/Bing: `https://www.bing.com/toolbox/bingbot.json`
     - Apple: `https://help.apple.com/crawlers/applebot.json`
     - DuckDuckGo: Official CIDR list.
2. **Subnet Parsing & Validation:**
   - Parses IPv4 and IPv6 CIDR prefix strings using `ipaddress.ip_network`.
   - Validates format and filters out malformed blocks.
3. **Cache Storage in SQLite:**
   - Replaces cached prefixes in `ngwaf_bot_cache.db` table `verified_bot_cidrs`.
   - Records `bot_name`, `cidr_prefix`, `ip_version`, and `updated_at`.
4. **Telemetry & Log Recording:**
   - Emits total prefixes updated and HTTP latencies in `cron_runs`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **HTTP Feed Calls:** Outbound HTTP request latency, status code, and payload sizes must be logged.
  - **SQLite Operations:** Database transactions must flow through `ThreadLocalPool` with instrumented timings.
  - **Zero FOS Calls:** Strictly external HTTP feeds and local SQLite; zero cloud FOS calls.
- **Timing & Resource Budgets:**
  - HTTP feed downloads: < 2.5s total across all endpoints.
  - SQLite transaction duration: < 50ms.
- **Audit Checklist:**
  - Verify feed endpoints use caching headers (`ETag`, `If-Modified-Since`) where supported.
  - Confirm partial feed failures (e.g. one provider down) do not purge existing cached ranges for other providers.

---

## 7. Failure Modes & Recovery Runbooks
- **Feed Endpoint Down / 503:** Preserves existing cached CIDRs; logs warning in `cron_runs` and retries next day.
- **Malformed Feed Payload:** JSON schema validation rejects unexpected schemas to protect against corrupted external feeds.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger Bot Sync:** `POST /api/admin/bots/refresh`.
- **Inspect Bot Sources:** `GET /api/admin/bot-sources`.

---

## 9. AI Session Automated Verification Checklist
- [ ] 1. Trigger `POST /api/admin/bots/refresh`; confirm HTTP 200.
- [ ] 2. Call `GET /api/admin/bot-sources`; confirm Google, Bing, Apple show recent sync timestamps.
- [ ] 3. Query `ngwaf_bot_cache.db`; confirm `verified_bot_cidrs` table has > 100 entries.
- [ ] 4. Verify in `cron_runs`: status `success` with non-zero duration.
- [ ] 5. Confirm zero FOS Class A/B calls in `usage_log.db`.
