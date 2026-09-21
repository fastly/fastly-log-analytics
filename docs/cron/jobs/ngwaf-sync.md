# Background Job Specification: `ngwaf_sync_{service_id}`

## 1. Overview & Objectives
- **Job Identifier:** `ngwaf_sync_{service_id}`
- **Category:** Fastly Next-Gen WAF (Signal Sciences) Intelligence Sync
- **Status:** Verified (Automated Unit Tests & Lifecycle Audited)
- **Purpose:** Synchronizes verified bot requests and security signals from the Fastly Next-Gen WAF (NGWAF) API into local SQLite `ngwaf_bot_cache.db`.
- **Why It Runs:** Fastly edge logs include NGWAF security tags (e.g. `SIGSCI-TAGS`, `x-sigsci-tags`). To cross-correlate edge log hits with WAF decisions and display comprehensive threat actor profiles on the Security and WAF dashboards, the system periodically imports active attack and verified bot signals directly from the Fastly NGWAF API.

---

## 2. Scheduling & Cadence
- **Trigger Type:** Interval timer (`interval`)
- **Default Schedule:** Every 5 minutes (`minutes=5`), with **Adaptive Load Rescheduling**:
  - Under normal volume (< 500 records/sync): maintains baseline configured interval (default 5m).
  - Under high bot volume or backlog (>= 500 records or 4-minute runtime budget reached): dynamically tightens interval to **2 minutes** (`minutes=2`) to rapidly drain backlogs without falling behind.
  - Automatically relaxes back to baseline (5m) once the backlog clears.
- **Configurable Overrides:** `provisioning.cron_ngwaf.interval_mins` (default: 5).
- **Registration Gate:** Registered **ONLY** if `get_ngwaf_workspace_id(service_id)` resolves to an active NGWAF workspace.
- **Jitter & Misfire Policy:**
  - Jitter: 15 seconds.
  - `max_instances=1`, `coalesce=True`, `misfire_grace_time=300s`.

---

## 3. Architecture Execution Matrix
| Architecture / Mode | Execution Engine | Data Path | Concurrency & Locks |
|---|---|---|---|
| **Standard Mode (`DEPLOYMENT_MODE=standard`)** | APScheduler (In-Process Web Pod) | Calls Fastly NGWAF API, enriches with known bot signatures, upserts into `data/ngwaf_bot_cache.db`. | Local SQLite thread-local pool lock. Gated by `FLA_DEV_NO_CRONS=1` (outbound API suppressed). |
| **High-Scale Mode (`DEPLOYMENT_MODE=high_throughput`)** | Pod APScheduler (Web Pod Only) | Calls NGWAF API and populates shared/local SQLite bot cache. | Local SQLite lock; decorated with `@cron_task("sync_ngwaf_bots", job_name="ngwaf_sync")`. |

---

## 4. Role & Permissions Matrix
| Role | Job State | Manual API Trigger | Data Visibility |
|---|---|---|---|
| **Admin (`read_write`)** | Active | `POST /api/admin/ngwaf/sync/{service_id}` | Full NGWAF cache management via `GET /api/admin/ngwaf/status`, verified bot analytics on Security pages. |
| **Analyst Path A (Standalone Instance)** | Active | Internal trigger | Synchronizes WAF signals locally if credentials provided. |
| **Analyst Path B (Remote Share)** | N/A | Blocked (403) | Reads enriched WAF intelligence from shared server. |

---

## 5. Execution Lifecycle & Step-by-Step Logic
1. **Dev Mode Gate:**
   - Evaluates `dev_mode_no_crons()`. If active (`FLA_DEV_NO_CRONS=1`), immediately skips outbound API calls.
2. **Prerequisite Validation:**
   - Verifies `ngwaf_workspace_id` and `fastly_api_key` are configured. Skips gracefully if unconfigured.
   - Ensures local SQLite schema exists (`ensure_schema()`).
3. **Watermark Management:**
   - Inspects `ngwaf_sync_state.last_timestamp_synced` for the workspace.
   - On the very first run (watermark is None), seeds the watermark with the current UTC timestamp (`iso_z_now()`), records initial success in `cron_runs`, and begins incremental sync on the next cycle.
4. **Paged API Fetch & Enrichment:**
   - Iterates paged events from `fetch_verified_bots_paged(api_key, workspace_id, from_ts)`.
   - Filters by optional `server_name_filter`.
   - Cross-references user agents against `build_matcher()` to assign `wellknown_bot_id` and `wellknown_bot_name`.
   - Upserts records in batches into `ngwaf_bots` with advanced high-water mark timestamp.
   - Enforces a 4-minute execution budget (`max_runtime_secs = 240`) per run to preserve process concurrency.
5. **Retention Cleanup:**
   - Calls `cleanup_old_bots(retention_days)` (default: 30 days) to purge stale cache records.
6. **Adaptive Rescheduling:**
   - If `budget_exceeded` is True or `total_records >= 500`, reschedules the APScheduler job to 2 minutes.
   - Otherwise, restores the baseline interval (5 minutes).
7. **Telemetry & Log Recording:**
   - Logs execution outcome into `cron_runs` table via `log_cron_run`.
   - Distinguishes authentication errors (HTTP 401/403) with explicit guidance for Fastly API key or workspace permissions.

---

## 6. Telemetry, Timing & Query Audit Contract
- **100% Query & API Call Capture:**
  - **Fastly NGWAF API Calls:** Outbound HTTP request status and exceptions logged with service display name.
  - **SQLite Operations:** Every batch upsert and schema check flows through `open_small_cache_db` / thread-local pool.
  - **Zero FOS Calls:** Strictly external NGWAF API and local SQLite; zero cloud FOS calls.
- **Timing & Resource Budgets:**
  - Max per-tick runtime: 240 seconds budget ceiling.
  - Normal tick runtime: < 2.0s when caught up.
- **Audit Checklist:**
  - Confirm partial batch commits persist so crashes/interruptions do not lose data.
  - Verify expired records (> retention_days) are regularly purged without table locks.

---

## 7. Failure Modes & Recovery Runbooks
- **NGWAF 401 / Invalid API Key:**
  - Recorded as `error` with message: `"NGWAF sync failed: authentication error (check Fastly API key / workspace permissions)"`.
  - Operator checks `GET /api/admin/ngwaf/status` and updates credentials in service config.
- **NGWAF API Rate Limiting / 429:**
  - Logs rate limit notice; next tick retries automatically from last committed watermark.
- **Network Timeout / Upstream 503:**
  - Catches transient error and retries safely on next tick without state corruption.

---

## 8. Manual Trigger Endpoints & Admin Controls
- **Trigger NGWAF Sync:** `POST /api/admin/ngwaf/sync/{service_id}`.
- **Inspect WAF Cache Status:** `GET /api/admin/ngwaf/status?service_id={service_id}`.
- **Inspect Service Cron History:** `GET /api/admin/sync-status?service_id={service_id}`.

---

## 9. Automated Verification Checklist
- [x] 1. Unit tests verify `_run_ngwaf_bot_sync` skips when `FLA_DEV_NO_CRONS=1` is active.
- [x] 2. Unit tests verify first-time sync watermark initialization.
- [x] 3. Unit tests verify paged bot fetch, enrichment, upsert, and cleanup.
- [x] 4. Unit tests verify adaptive rescheduling to 2 minutes under heavy bot volume (>= 500 records).
- [x] 5. Unit tests verify authentication error (401/403) classification.
- [x] 6. Unit tests verify manual `POST /api/admin/ngwaf/sync/{service_id}` and `GET /api/admin/ngwaf/status` endpoints.
