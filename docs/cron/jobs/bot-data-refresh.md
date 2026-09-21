# Background Job Specification: `bot_data_refresh`

## 1. Overview & Objectives
- **Job Identifier:** `bot_data_refresh`
- **Category:** Threat Intelligence & Verified Bot Registry Synchronization
- **Status:** Verified (Automated Unit Tests & Lifecycle Audited)
- **Purpose:** Synchronizes official published bot registries, malicious IP lists, Tor exit nodes, and cloud provider IP ranges (Arcjet Well-Known Bots, Tor bulk exit list, IPsum threat feed, AWS IP ranges, GCP IP ranges) from public feeds into local JSON cache files under `data/cache/bot_sources/`.
- **Why It Runs:** Fastly logs tag incoming requests with User-Agents, but malicious actors frequently spoof legitimate crawler User-Agents. To provide authentic Verified Bot analytics on the Security and Bots dashboards, the system cross-references client IP addresses and User-Agents against official published feeds. These feeds update periodically and must be kept in sync without penalizing query-time latency.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Cron trigger (`cron`)
- **Default Schedule:** Daily at 02:00 UTC (`hour=2, minute=0`).
- **Scope:** Process-global singleton job registered on the Scheduler (`system_jobs`).
- **Jitter & Misfire Policy:**
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=3600s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process Web Pod) | Fetches public HTTP/JSON feeds, normalizes format, writes to `data/cache/bot_sources/{source_id}.json`. | Local in-process locks (`_matcher_lock`, `_source_cache_lock`). Gated by `FLA_DEV_NO_CRONS=1` (outbound HTTP suppressed). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Fetches feeds and populates local bot cache on web serving pods. | Process-global singleton; never scheduled to Celery workers. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/bot-sources/refresh`<br/>`POST /api/admin/bots/refresh` | Full bot source management via `GET /api/admin/bot-sources`, per-source refresh `POST /api/admin/bot-sources/{source_id}/refresh`. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Maintains local bot source cache on analyst instance. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads enriched bot metrics from shared server. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Dev Mode Gate:**
   - Evaluates `dev_mode_no_crons()`. If active (`FLA_DEV_NO_CRONS=1`), immediately skips outbound HTTP requests, recording `status="skipped"` and `"skipped (dev_mode_no_crons)"` in `system_jobs`.
2. **Public Feed Retrieval & Parsing:**
   - Iterates through enabled sources in `BOT_SOURCES` (`well-known-bots`, `tor-exit-nodes`, `ipsum-blocklist`, `aws-ip-ranges`, `gcp-ip-ranges`).
   - Issues HTTP GET request with a 30s timeout and User-Agent `fastly-log-analysis/1.0`.
   - Normalizes payload format:
     - Arcjet JSON format: extracts domains and dynamic CIDRs.
     - Cloud ranges (AWS/GCP JSON): parses IPv4 and IPv6 prefixes.
     - Plain-text lists (Tor exit nodes, IPsum): extracts IP blocks and scores.
3. **Atomic Cache File Storage:**
   - Writes normalized envelope to `data/cache/bot_sources/{source_id}.json` containing:
     - `last_updated`: ISO timestamp in UTC (`iso_z_now()`).
     - `entry_count`: count of prefixes or bot entries.
     - `entries`: parsed entry list.
4. **In-Memory Cache Invalidation:**
   - Acquires `_matcher_lock` and invalidates `_matcher_cache`.
   - Invalidates `_source_cache` via file mtime checks, ensuring analytical query paths immediately pick up the latest definitions.
5. **Status & Telemetry Recording:**
   - Decorated with `@global_job("bot_data_refresh")`.
   - Records outcome into `system_jobs._status["bot_data_refresh"]`:
     - `status="success"`: Normal run where all enabled sources refresh cleanly.
     - `status="warning"`: Partial failure where some sources succeed but one or more fail.
     - `status="error"`: Total failure where all enabled sources fail to refresh.
     - `status="skipped"`: Bypassed by `FLA_DEV_NO_CRONS=1`.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **HTTP Feed Calls:** Outbound HTTP request status and exceptions logged with source ID.
  - **Zero FOS Calls:** Strictly external HTTP feeds and local JSON cache; zero cloud FOS calls.
- **Timing & Resource Budgets:**
  - Full refresh runtime: < 10.0s total across all enabled feeds.
  - Wall-clock runtime captured in `system_jobs._status["bot_data_refresh"]["duration_s"]`.
- **Audit Checklist:**
  - Verify failed individual feeds do not corrupt or delete existing cache files on disk.
  - Confirm in-process memory structures invalidate safely without race conditions.

---

## 7. Failure Modes & Recovery Runbooks
- **Upstream Feed Downtime / 503:**
  - Preserves existing cache file on disk (`load_source` continues serving cached entries).
  - Logs error, records `warning` (or `error` if all fail) in `system_jobs`.
  - Next daily run retries automatically, or operator triggers manual refresh via `POST /api/admin/bot-sources/refresh`.
- **Malformed Feed Payload:**
  - `fetch_and_cache_source` catches JSON/format errors and records failure without overwriting valid cache.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Refresh All Sources:** `POST /api/admin/bot-sources/refresh` (alias: `POST /api/admin/bots/refresh`).
- **Refresh Single Source:** `POST /api/admin/bot-sources/{source_id}/refresh`.
- **Inspect Bot Sources:** `GET /api/admin/bot-sources`.
- **System Job Status:** `GET /api/admin/system-jobs`.

---

## 9. Automated Verification Checklist
- [x] 1. Unit tests verify `_run_bot_data_refresh` records `success` with total entry count.
- [x] 2. Unit tests verify `_run_bot_data_refresh` records `warning` on partial feed failures.
- [x] 3. Unit tests verify `_run_bot_data_refresh` records `error` on total feed failures.
- [x] 4. Unit tests verify `_run_bot_data_refresh` skips when `FLA_DEV_NO_CRONS=1` is active (`status="skipped"`).
- [x] 5. Unit tests verify manual `POST /api/admin/bot-sources/refresh` and alias `/api/admin/bots/refresh` endpoints.
- [x] 6. Regression suite verifies backward compatibility with existing scheduler tests.
