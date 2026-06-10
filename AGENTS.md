# AGENTS.md — AI Agent Guide

The canonical reference for any AI agent working on this project. **Read this end-to-end before your first non-trivial change; re-read the [Traps & Gotchas](#traps--gotchas) section before every change.**

## How to use this file

- **First-time agent in this repo:** read in order — Overview → Architecture → Traps & Gotchas → Directives. Skim the rest.
- **Returning agent:** re-skim Traps & Gotchas every session. Most regressions in this codebase are re-discoveries of a documented trap.
- **Companion docs:**
  - [README.md](README.md) — user-facing overview, features, install. Don't duplicate it here.
  - [MONKEYPATCHES.md](MONKEYPATCHES.md) — every import-time patch we apply to s3fs/PyIceberg, with motivating incident and cleanup path. Update in the same commit when you add/remove a patch.
- **When to update this file:** any non-trivial structural change — new endpoints, new background jobs, schema changes, new traps discovered. See [Keeping This File Current](#keeping-this-file-current).

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Ingest Pipeline](#ingest-pipeline)
4. [VCL Log Format & Custom Fields](#vcl-log-format--custom-fields)
5. [Key Systems](#key-systems)
6. [Provisioning](#provisioning)
7. [Frontend Patterns](#frontend-patterns)
8. [Testing](#testing)
9. [Traps & Gotchas](#traps--gotchas)
10. [AI Agent Directives](#ai-agent-directives)
11. [Keeping This File Current](#keeping-this-file-current)

---

## Overview

FastAPI + Next.js dashboard for Fastly Real-Time VCL logs streamed to Fastly Object Storage (FOS). Continuously ingests `.gz` log files into DuckDB + Apache Iceberg and exposes per-service analytics, alerts, NGWAF bot detection, custom log fields, and a live-share feature for read-only analyst access.

User-facing pitch + features list lives in [README.md](README.md). This file documents *how it works internally*.

**Stack:**
- Backend: FastAPI, DuckDB, PyIceberg, APScheduler, boto3 (S3-compatible FOS), `uv`, `ruff`, `mypy`, `pytest`
- Frontend: Next.js 16, React 19, TanStack Query v5, Zustand, shadcn/ui, openapi-fetch, vitest
- Storage: FOS (S3-compatible), per-service DuckDB + SQLite (operational metadata), global SQLite for NGWAF bot cache + live-share
- Optional: [`falco`](https://github.com/ysugimoto/falco) VCL linter — detected via `shutil.which("falco")`, degrades gracefully to regex checks when absent

**VCL editing:** when you write or edit a log format string, a custom field `vcl_log_expression`, or any VCL snippet, it must pass `falco lint`. Use `+` for string concatenation; wrap literals in heredoc strings (`{"literal"}`). Call sites: [backend/utils/vcl_utils.py](backend/utils/vcl_utils.py), [backend/provision.py](backend/provision.py), [backend/routers/services/core.py](backend/routers/services/core.py).

## Architecture

### Data layout

| Layer | Location | Purpose |
|---|---|---|
| Raw logs | `s3://{bucket}/{prefix}/raw/**/*.gz` | Immutable gzipped JSON from Fastly |
| Local buffer | `cache/{bucket}/` | Transient Parquet between ingest and Iceberg commit |
| Iceberg table | `s3://{bucket}/{prefix}/iceberg/` | Durable long-term storage, hour-partitioned |
| Admin state | `s3://{bucket}/{prefix}/iceberg/meta/admin_state.json` | log_format_history, audit_logs, views, custom_fields (no alerts — alerts are per-instance) |
| DuckDB | `data/services/{service_id}.duckdb` | Per-service analytical engine **only**: session-scoped `logs` view + temp tables |
| Service metadata DB | `data/services/{service_id}.metadata.db` | Per-service SQLite (WAL): `alerts`, `views`, `audit_logs`, `cron_runs`, `sources`, `ingested_files`, `asn_names`, `usage_log` |
| NGWAF bot cache | `data/ngwaf/ngwaf_bot_cache.db` | Shared SQLite for VERIFIED-BOT enrichment |
| Live-share DB | `data/system/remote_share.db` | Singleton SQLite (WAL): invites, sessions, audit, TOS, lockouts |
| Service configs | `configs/{logging_service_id}.json` | Credentials, settings, log_fields config |

The DuckDB `logs` view stitches the Iceberg table and the local Parquet buffer so queries always see all data without callers caring which layer holds which row.

### Package layout (post v2.0 carve-ups)

Several historical monoliths were split into cohesive packages with thin re-export shims at the old paths so existing imports keep working:

| Old path | New package | Shim status |
|---|---|---|
| `backend/core/iceberg.py` | [`backend/core/iceberg/`](backend/core/iceberg/) (`_core.py` + `fs.py`) | package `__init__.py` re-exports the historical public surface; the monkeypatched s3fs methods are now `FosS3FileSystem` / `CachedS3FileSystem` subclasses in `fs.py` |
| `backend/core/metadata_db.py` | [`backend/core/metadata/`](backend/core/metadata/) (`base`, `alerts`, `views`, `ingest_log`, `cron_log`, `asn_cache`, `usage_log`, `reconciliation`, `state`) | thin shim at [`backend/core/metadata_db.py`](backend/core/metadata_db.py) re-exports the full surface plus a `_ShimModule` proxy so `monkeypatch.setattr(metadata_db, "_DATA_DIR", ...)` still flips the live binding inside `metadata.base` |
| `backend/core/share_db.py` | [`backend/core/share_db/`](backend/core/share_db/) (`connection`, `schema`, `invites`, `sessions`, `audit`, `passcode`, `tos`, `settings`, `validation`) | package `__init__.py` re-exports the historical public surface; passcode hashing is argon2id (legacy scrypt verify branch stays for transparent rehash-on-login) |
| `backend/utils/tunnel.py` | [`backend/utils/tunnel/`](backend/utils/tunnel/) (`manager`, `session`, `rate_limiter`, `state`, `fingerprint`) | package `__init__.py` re-exports `get_tunnel_manager`, `AnalystSession`, etc. SSH-to-localhost.run code path (`_TUNNEL_URL_RE`, sleep listener, reconnect logic, `use_tunnel=True` branches) was deleted in v2.0 — only direct-mode (HTTPS public_endpoint) is supported. The `use_tunnel=True` kwarg still exists as a back-compat keyword that raises a clear error |
| `backend/scheduler.py` | [`backend/cron/`](backend/cron/) (`scheduler.py`, `decorators.py`, `jobs/{sync,commit,compaction,optimize,expire,metadata}.py`) | thin shim at [`backend/scheduler.py`](backend/scheduler.py) re-exports `get_scheduler`, `Scheduler`, `cron_task`, every `_run_*` job body, and the watchdog constants |

Other new modules introduced by the cleanup:

- [`backend/repositories/_sql/`](backend/repositories/_sql/) — named, parameterized SQL templates extracted out of inline repo strings (one file per repo concern: `dashboard`, `security`, `network`, `origin`, etc.). Repository functions keep their names and signatures; they call into the templates instead of carrying SQL inline.
- [`backend/core/field_registry.py`](backend/core/field_registry.py) — Phase 7 (in progress) typed registry that owns per-field declarations (code, display name, type, valid aggregations, valid filter ops, derivations, security-regex hooks). Migration of readers (dashboard CTE generator, rollup spec builder, top_n logic, SQL validator, scoring matrix labels) is incremental.
- [`backend/core/request_context.py`](backend/core/request_context.py) — Phase 2 single FastAPI dependency that bundles `service_id`, `source`, `con`, `telemetry`, `analyst_session`, `cached_temps`. Replaces the `AnalyticsDeps` bundle and folds `require_service_access` into context construction (there is no path that builds a context without enforcing tenancy).
- [`backend/core/request_telemetry.py`](backend/core/request_telemetry.py) — Phase 1 thin wrapper around the OTel tracer that owns section spans, query attribution, call log, cache state, and the `app.thread_wait_ms` custom metric instrumented at `_Pool.acquire`. Lives on `RequestContext`.
- [`backend/core/settings.py`](backend/core/settings.py) — Phase 3.5 `Settings(BaseSettings)` class (pydantic-settings) that owns every env var. Required-in-prod settings are pydantic validators.

### Personas (where the two onboarding paths live)

The README explains the two collaboration modes for end users. Implementation pointers:

- **Admin** (`access_level: "read_write"`) — full ingest/management surface. Config: `configs/{logging_service_id}.json`.
- **Analyst Path A — independent instance** (durable, JSON-config join). Read-only FOS credentials, runs its own copy of the app. Components: `POST /api/services/{service_id}/generate-viewer-key` → [`api_invite_analyst()`](backend/routers/services/core.py), `GET /api/provision/join` (SSE), [`InviteAnalystDialog`](frontend/components/InviteAnalystDialog/), ProvisionWizard "join" mode.
- **Analyst Path B — live shared instance** (SSH-tunnelled). No FOS credentials, uses admin's running process. See [Live Dashboard Sharing](#live-dashboard-sharing) below for components.

**Both paths must keep working.** Don't remove either. Don't introduce a "unified" replacement without keeping the JSON-config flow intact — it's the only option when the admin's instance can't stay running.

## Ingest Pipeline

APScheduler runs six job types per service:

| Job | Schedule | Function |
|---|---|---|
| `sync_{id}` | every `log_period` sec | LIST FOS raw/, download new `.gz`, transform to Parquet, update DuckDB view, flush usage log, run cleanup |
| `commit_{id}` | every `commit_interval_mins` (default 5) | Commit local Parquet buffer → Iceberg table, flush usage log |
| `local_compact_{id}` | every 2 min | Compact local-only hourly/daily Parquet files, flush usage log |
| `optimize_{id}` | daily 03:00 UTC | Compact small Iceberg data files, flush usage log |
| `expire_{id}` | weekly Sun 04:00 UTC | Expire old Iceberg snapshots, flush usage log |
| `metadata_sync_{id}` | varies | Sync admin state to FOS, flush usage log |

Teardown removes jobs on the next `_sync_jobs()` reload. The `config not found, skipping` warning during teardown is normal — a job fired after the config was deleted; harmless.

### Local-Only Parquet Compaction (Dashboard Performance)

To maintain top-tier dashboard querying speeds over long periods without generating massive FOS write costs or massive file bottlenecks, we employ sequential size-capped bin-packing local compaction (implemented in `backend/core/local_compaction.py`):
1. **Periodic Job (`local_compact_{id}`):** Runs every 2 minutes. It scans local cache directories, identifies any hourly partitions containing multiple small files, and merges them sequentially into size-capped compacted Parquet files (default <= 256MB) to maintain DuckDB query parallelism.
2. **Compact-on-Sync Thread:** Triggered immediately after a raw sync completes. If multiple new files are detected, a background thread merges them immediately.
3. **Daily & Weekly Tier Rollup:** Partitions older than 7 days (customizable via `LOCAL_COMPACT_DAILY_TIER_DAYS`) are sequentially bin-packed by day into daily files (e.g. `daily_YYYY-MM-DD_<uuid>.parquet`), with single-file bins correctly migrated to retire empty hourly dirs. Daily files older than 30 days are further bin-packed into weekly files (e.g. `weekly_YYYY-WXX_<uuid>.parquet`) under `weekly/`. All files are capped at `_MAX_PARTITION_BYTES` to prevent huge file bottlenecks and preserve maximum parallelism.

*Note: Use `local_compaction` for hot-tier ongoing dashboard performance. Use the global `optimize_{id}` / `optimize_table` path when you want compaction reflected in FOS too.*

### Atomic ingest (in-flight manifest + deterministic buffer names)

The window between `iceberg.write_to_buffer` and `metadata_db.insert_ingested_files` is crash-protected by a per-service `ingest_in_flight` table:

1. `metadata_db.record_in_flight(source_name, buf_filename, file_rows)` persists `(file_name, row_count, file_size)` tuples **before** the Parquet is written.
2. `iceberg.write_to_buffer(src, arrow_table, buf_filename)` writes Parquet. Filename is `batch_{sha256(sorted_chunk)[:16]}.parquet` — deterministic so a re-ingest of the same chunk overwrites the same file instead of stacking duplicates.
3. `metadata_db.insert_ingested_files(...)` commits the row_count/size tuples.
4. `metadata_db.clear_in_flight(...)` drops the row.

`_recover_in_flight(src)` runs first on every `ingest()` and reconciles any leftover rows: buffer present → promote to `ingested_files` + clear; buffer missing → drop without touching `ingested_files` (next LIST tick re-ingests cleanly). Both halves are idempotent.

**Known intentional limitation:** if ingest crashes mid-LIST, the next tick re-LISTs from the same `StartAfter` marker. Wastes ≤10 Class A ops per crash but is correct — every file is dedup'd before download.

### Health probe

`GET /api/health` is cheap liveness. `GET /api/health?deep=1` also verifies per-service ingest freshness: reads `max(ingested_at) FROM ingested_files` and the latest terminal `sync` cron run per service; returns 503 when any service is `degraded` (last ingest older than `stale_minutes` — default 30 — or last sync errored). SQLite-only, never FOS or Fastly. Safe to wire into a load balancer.

## VCL Log Format & Custom Fields

Generated by `generate_log_format()` in [backend/core/log_fields.py](backend/core/log_fields.py). Single-line JSON VCL string.

**Field groups (A–L):**

| Group | Name | Dependency |
|---|---|---|
| (always) | Core HTTP | — |
| A | Request Identity | — |
| B | Cache Deep-Dive | — |
| C | Infrastructure | — |
| D | Geolocation Basic | — |
| E | Geolocation Precision | Requires D |
| F | Network Quality Core | — |
| G | Network Quality Deep | Requires F |
| H | Security: TLS Fingerprinting | — |
| I | Security: Proxy & Anonymization | — |
| J | WAF / NGWAF | (UI only shown if NGWAF linked) |
| K | QUIC / HTTP3 | — |
| L | Origin Metrics | — |

Key concepts:
- `format_hash` — SHA-256 of generated format; detects drift between deployed VCL and local config.
- `FASTLY_LOG_FORMAT_SAFE_MAX` ≈ 8,000 chars. Enforced before deployment.
- `generate_capture_vcl()` in [backend/provision.py](backend/provision.py) injects per-hook code (recv, miss, pass, fetch, error, deliver) to populate log variables.
- `log_format_history` tracks format changes with before/after group lists, added/removed fields, actor.

### Custom fields

Admins define arbitrary VCL fields appended to the log format. Storage: `configs/{service_id}.json` → `log_fields.custom_fields[]`. API: `/api/services/{service_id}/custom-fields` (CRUD + lint + export/import) in [backend/routers/services/core.py](backend/routers/services/core.py).

Per-field schema (`name`, `label`, `vcl_log_expression`, `snippets`, `duckdb_type`, `value_type`, `collection_stage`, `bytes_estimate`, `enabled`, `show_in_dashboard`, `show_in_logs`, `filterable`) lives in [backend/models/custom_fields.py](backend/models/custom_fields.py).

**All configs are schema v2.** Always load `log_fields` with the v2 default:

```python
lf = cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []}
```

## Key Systems

Brief summaries; click through to source for details.

### Scheduler ([backend/cron/](backend/cron/))
Single `BackgroundScheduler` owned by [backend/cron/scheduler.py](backend/cron/scheduler.py). `_sync_jobs()` adds/removes per-service jobs on `reload()`. The `@cron_task` decorator (telemetry context + usage-log flush + watchdog hard-cap) lives in [backend/cron/decorators.py](backend/cron/decorators.py). Per-job bodies live under [backend/cron/jobs/](backend/cron/jobs/) (`sync`, `commit`, `compaction`, `optimize`, `expire`, `metadata`). Per-run progress events tracked in [backend/cron_progress.py](backend/cron_progress.py) and streamed via SSE. [backend/scheduler.py](backend/scheduler.py) is a thin compat shim that re-exports the same public symbols.

### NGWAF Bot Detection ([backend/utils/ngwaf.py](backend/utils/ngwaf.py), [backend/utils/ngwaf_bot_cache.py](backend/utils/ngwaf_bot_cache.py))
Syncs VERIFIED-BOT requests from `GET https://api.fastly.com/ngwaf/v1/workspaces/{id}/requests`. JSON:API pagination via `meta.next_cursor`. Shared SQLite cache at `data/ngwaf/ngwaf_bot_cache.db`. Enriches log rows with `waf_req_id` + `waf_sig LIKE '%VERIFIED-BOT%'`.

NGWAF workspace listing (`GET /api/provision/ngwaf-workspaces`): response key is `"data"`. **Don't `or`-chain** with `data.get("workspaces", [])` — an empty list is falsy and falls through. Use `if "data" in data` explicitly. (See Trap #3.)

### Alerts / Saved Views ([backend/routers/alerts.py](backend/routers/alerts.py), [backend/routers/views.py](backend/routers/views.py))
Both stored in per-service `metadata.db` (SQLite). Alerts are threshold-based with webhook fire. Views capture filter set + time range.

### State Sync ([backend/state_sync.py](backend/state_sync.py))
`export_admin_state` writes `audit_logs` + `views` from per-service SQLite, plus `log_format_history` + `custom_fields` from the config JSON, to `{prefix}/iceberg/meta/admin_state.json`. **Alerts are not synced** — each instance maintains its own. Only `read_write` services export.

### FOS Usage Logging ([backend/utils/usage_logger.py](backend/utils/usage_logger.py), [backend/core/metadata/usage_log.py](backend/core/metadata/usage_log.py))
Every FOS Class A/B op and CDN download recorded to per-service `usage_log` SQLite for cost analysis.
- Global toggle: `data/system/usage_logging.json`
- Process-context tagging via `set_process_context()` in [backend/utils/telemetry.py](backend/utils/telemetry.py) — tags entries with `cron:sync:svc1` or `api:GET /api/...`
- Each cron handler calls `flush_usage_log(service_id)` at completion (the `@cron_task` decorator wires this).
- Costs computed at query time from rate config — changing rates recomputes history.
- Admin endpoints: `GET/PATCH /api/admin/usage-logging`, `GET/DELETE /api/admin/usage-log`, `GET /api/admin/usage-log/export`. Frontend: `/admin/usage-log`.

### Log-Line Accounting ([backend/routers/admin.py](backend/routers/admin.py) `api_log_accounting`)
Per-bucket reconciliation between Fastly's `/stats/service/{id}` log-emission counter and our `sum(row_count) FROM ingested_files`.
- Field probe order: `log → log_records → log_entries → logging_requests`; first non-zero wins. All-zero logs a warning.
- In-flight clamp: current bucket is in totals but excluded from sustained-loss scan (Fastly Stats lags ingest).
- Sustained-loss alert: ≥2 consecutive completed buckets with `gap_pct ≥ 0.05`.
- Frontend cadence: `staleTime 30s`, `refetchInterval 60s` → ≤1 Fastly Stats call/min per open admin tab.

### Iceberg Pointer + Summary Hash-Throttle ([backend/core/iceberg/_core.py](backend/core/iceberg/_core.py))
Every commit writes `metadata_location.txt` (unavoidable) and `table_summary.json` (skippable). The latter is content-hashed against `_table_summary_hash_cache`; identical payloads skip the PUT. Saves one FOS PUT per no-op commit in steady state. Cache is module-scope, process-lifetime.

### DuckDB Connection Pool ([backend/core/duckdb_pool.py](backend/core/duckdb_pool.py))
Per-service LIFO pool replaces per-request `duckdb.connect()` + S3 / iceberg setup + view rebind (~50ms steady-state). Pool size is `DUCKDB_POOL_MAX_SIZE` (default 8). All pool connections open with `read_only=False` — `get_connection` forces this so cron writers and pool readers don't trip DuckDB's "different configuration" error on the same file. Optional per-connection tuning: `DUCKDB_POOL_CONN_MEMORY_LIMIT` (e.g. `256MB`) caps RSS growth under concurrent large scans; `DUCKDB_POOL_CONN_THREADS` reduces context-switching when `pool_size × per_conn_threads` exceeds physical cores. View-binding happens outside the pool lock to avoid deadlocking the FastAPI thread pool when an Iceberg snapshot reload blocks.

### Hourly Top-N Rollups ([backend/core/rollups.py](backend/core/rollups.py), [scripts/backfill_rollups.py](scripts/backfill_rollups.py))
Precomputes per-hour Top-N aggregates for the dashboard's most-asked fields (ip, country, url, custom fields) and writes them under `<cache>/data/rollups/`. Closed hours read from the rollup; the current ("live") hour merges the rollup with a fast scan of the buffer. Plus a per-minute time-series bundle (`rollups/timeseries/...`) used by the dashboard chart to skip the wide Iceberg scan. Skipped buckets fall back to the raw scan path. Generated by `local_compact_{id}` after each compaction pass; the global `optimize_{id}` job rebuilds the day's worth on each run.

### Response Telemetry Middleware ([backend/utils/telemetry_response_middleware.py](backend/utils/telemetry_response_middleware.py))
Backstop for endpoints that return a plain `dict` instead of going through `BaseResponse.with_telemetry`. Inspects JSON object responses, injects `_debug_queries` / `_debug_calls` / `_is_cached` from the contextvar collectors if missing. **Must be added INNER to `CompressMiddleware`** (i.e. `add_middleware(TelemetryResponseBodyMiddleware)` BEFORE `add_middleware(CompressMiddleware)`) so it sees the raw JSON, not br/zstd/gzip-encoded bytes. Skips streaming responses, non-dict bodies, and already-instrumented responses. Gated on `DEBUG_RESPONSES`; failure modes are silent + non-blocking.

### CDN-Fronted Log Delivery
FOS reads are fronted by a Fastly CDN VCL service (`cdn_service_id`, `cdn_url`, `cdn_secret`). The CDN validates a shared-secret query param to gate access; rate-limited to blunt brute-force. Separate from the logging service ID.

### Live Dashboard Sharing
Components for the live-shared-instance remote-analyst feature (Path B). Two direct-mode sharing modes are exposed to the admin (the SSH-reverse-tunnel via localhost.run was deleted in v2.0):

1. **Admin-provided hostname** (e.g. `https://logs.example.com`)
2. **Admin-provided IP** (e.g. `https://203.0.113.42:8443`)

Both share a single backend code path: `ShareStartPayload.use_tunnel=False` + `public_endpoint=<https URL>`. The mode selector in the UI is presentational — the backend only cares that `public_endpoint` starts with `https://` (cookies need `secure=true`). `use_tunnel=True` still exists as a back-compat keyword and now raises a clear error.

Components:

- [backend/utils/tunnel/](backend/utils/tunnel/) — package split: `manager.py` owns the `TunnelManager` singleton (direct-mode lifecycle, sever-all panic), `session.py` holds `AnalystSession`, `rate_limiter.py` is the sliding-window `_LoginRateLimiter`, `state.py` persists `tunnel_state.json`, `fingerprint.py` computes the session fingerprint hash. Process singleton via `get_tunnel_manager()`; `reset_for_tests()` for pytest.
- [backend/utils/remote_access.py](backend/utils/remote_access.py) — `RemoteAccessMiddleware` does DNS-rebinding gate (Host/Origin allow-lists, including `testclient`/`testserver` for pytest), blocks admin paths on remote requests, applies response hardening (CSP, X-Frame-Options DENY, no-store, no-referrer). `_StaticAssetLimiter` rate-limits static assets to blunt scrapes.
- [backend/core/share_db/](backend/core/share_db/) — package split: `connection.py` (pool + corruption self-heal with quarantine), `schema.py` (own MIGRATIONS dict + `apply_pending` + `PRAGMA user_version`), `invites.py`, `sessions.py`, `audit.py`, `passcode.py` (argon2id current default; scrypt verify branch stays for transparent rehash-on-login upgrade), `tos.py`, `settings.py`, `validation.py`. Singleton SQLite at `data/system/remote_share.db`: `remote_invites`, `invite_services`, `remote_sessions`, `remote_share_audit_logs`, `share_settings`, `remote_invite_claim_tokens`, `share_tos_versions`. WAL mode, per-IP/per-email lockout.
- [backend/routers/share_auth.py](backend/routers/share_auth.py) (`/api/share/*`) — analyst-facing: `login`, `logout`, `acknowledge`, `heartbeat`, `claim/{token}`. Tagged so middleware lets them through the tunnel.
- [backend/routers/share_admin.py](backend/routers/share_admin.py) (`/api/admin/share/*`, **blocked over tunnel**) — admin-facing: tunnel lifecycle, invite CRUD, session evict, panic/sever-all, backup export/import, GDPR erase, settings.
- Frontend: [ShareDashboardDialog](frontend/components/ShareDashboardDialog/), [/share-login](frontend/app/share-login/) (TOS-gated), [useAnalystHeartbeat](frontend/hooks/useAnalystHeartbeat.ts), [useShareStatusBanner](frontend/hooks/useShareStatusBanner.tsx). Watermark mounts in `AppLayout` when `bootstrap.settings.is_remote_analyst === true`.

When adding an endpoint that analysts must reach over the tunnel, **register under `/api/share/*`** (auto-allowed) or update `_is_blocked_path()` — don't punch a hole somewhere obvious. (Trap #20.)

## Provisioning

### UI Wizard ([frontend/components/ProvisionWizard/ProvisionWizard.tsx](frontend/components/ProvisionWizard/ProvisionWizard.tsx))
Step order: `mode → token → service → storage → ngwaf → fields → execute`. Token entered in step 2 must be threaded into every Fastly-credentialed API call (including the NGWAF fetch). `execute` streams SSE.

### CLI ([backend/provision.py](backend/provision.py))
- `python backend/provision.py` — interactive
- `python backend/provision.py --teardown --service-id {id}` — teardown

CLI supports provisioning and teardown only. There is no analyst join command — that path is web-only.

### Teardown
Removes the FOS logging endpoint from the Fastly service, the CDN VCL service, the FOS access key, local config, local DuckDB, local cache. APScheduler cleans stale jobs on the next `reload()`.

## Frontend Patterns

> **REQUIRED READING before any frontend work:**
> [`frontend/node_modules/next/dist/docs/`](frontend/node_modules/next/dist/docs/)
> — the Next.js 16 App Router docs are vendored locally. Read the relevant
> sections (loading.tsx, prefetching, streaming, instant-navigation, caching,
> linking-and-navigating) BEFORE proposing or implementing changes to
> components / pages / hooks. **Click-feel bugs are almost always a Next
> conventions violation that the docs would have flagged.** Past failures
> from skipping this: shipping pages without `loading.tsx`, blocking
> layouts on uncached data, per-instance `setInterval` storms, missing
> `signal` cancellation, polling intervals tuned for "live feel" not
> backend cost. The conventions section below distills the rules but
> defer to the docs for any pattern not listed.

**Stack:** Next.js 16 app router, React 19, TanStack Query v5, Zustand, shadcn/ui, Recharts, openapi-fetch.

**Type-safe client:**
```typescript
// frontend/lib/api.ts
import createClient from "openapi-fetch";
import type { paths } from "@/types/api.generated";
const client = createClient<paths>({ baseUrl: "" });
```

A global middleware in [frontend/lib/api.ts](frontend/lib/api.ts) checks `response.ok` and **throws a JS Error** on 4xx/5xx. FastAPI wraps errors as `{"detail": {"error": "..."}}` — extract with `r.error?.detail?.error || r.error?.error`. Use try/catch or TanStack Query's `isError`.

**Generated types:** `cd frontend && npm run gen:types`. Run after backend endpoint/model changes. `make ci` does this automatically via `make typecheck-frontend`.

**Streaming/binary endpoints** (SSE, blobs) use raw `fetch()` — leave a comment so future readers don't "fix" it.

### Canonical patterns (May 2026 DRY refactor — use these in new code)

1. **`response_model=` on every router handler.** Without it the OpenAPI emits `Record<string, unknown>`. Routes using `Depends(get_source)` should also lift `service_id: str` into the signature so it appears as a path parameter.
2. **`usePageContext()`** — single store read for `activeServiceId + start/end + timezone`. Don't read `useServiceStore + useFilterStore + useTimezoneStore` separately.
3. **`ReportLayout`** for analytics pages — bundles `usePageContext + useReportConfig + useFilterPayload + useUrlFilterSync + useServiceQuery + ChartIntervalButtons + ReportShell`. Fall back to `ReportShell` only for multi-query or non-standard chrome pages.
4. **`HelpDialog`** from [components/ui/help-dialog.tsx](frontend/components/ui/help-dialog.tsx) — don't compose `Dialog + DialogHeader + DialogTitle` by hand for help content.
5. **`useBaseMap`** for any MapLibre setup. Don't duplicate the world-layer + theming inline.
6. **`metadata.record_audit(service_id, event_type=..., details=...)`** — direct (or via the `metadata_db` shim; both resolve to the same `metadata.audit` impl). The `duckdb.log_audit_event` shim and `repositories/audit.py` pass-through were removed.
7. **`date_utils.parse_iso_utc` / `iso_z` / `iso_z_now`** — don't hand-roll `datetime.fromisoformat(s.replace("Z", "+00:00"))`.
8. **`@cron_task` decorator** in [backend/cron/decorators.py](backend/cron/decorators.py) — handles `start_call_tracking`, `set_process_context`, `flush_usage_log` finally-block, watchdog hard-cap. Re-exported from [backend/scheduler.py](backend/scheduler.py) for compat.
9. **`empty_schema_response(runner)`** in [_base.py](backend/repositories/_base.py) — return this when a repo function hits a service with no logs.
10. **`origin_latency_us_expr(actual_cols)`** in `_base.py` — don't hand-roll the `COALESCE("ottfb", "ttfb" * 1000000.0)` fragment.

### Next.js navigation + loading conventions (READ BEFORE TOUCHING FRONTEND)

Distilled from `frontend/node_modules/next/dist/docs/` — these are the
rules to follow so click-to-render feels instant. Failure modes I've shipped
before and you should not repeat:

**1. Every navigable route MUST have a `loading.tsx`.** Without it, dynamic
routes (all our `'use client'` pages) get NO prefetched fallback — the
browser sits on the previous page until the destination's JS is ready and
its useQueries have settled. With it, Next.js renders the skeleton the
instant the user clicks. Use a variant from
[components/skeletons/PageSkeleton.tsx](frontend/components/skeletons/PageSkeleton.tsx)
— don't hand-roll Array.from + Skeleton inline.

**2. Layouts MUST NOT block on uncached data.** If `app/layout.tsx` or any
shared layout awaits a fetch / accesses cookies / etc. before rendering
children, **`loading.tsx` will not show a fallback at all** — Next.js waits
for the layout to settle first. The previous fix to `AppLayout` removed an
`isLoading ? <Spinner /> : children` gate that was doing exactly this; any
new layout-level data must use `useQuery` with `staleTime` so re-renders
are cheap, and the layout must never short-circuit children behind a
loading boolean.

**3. Cancel in-flight queries on every route change.** AppLayout's
`useEffect([pathname])` calls `queryClient.cancelQueries({ type: 'active' })`
so the old page's leftover polls (e.g. SystemHealthCard's 10s health-snapshot
poll) don't compete with the new page's mount work. **Always thread `signal`
through queryFns** so cancellation actually aborts the network request —
this hasn't been done universally yet, but new queryFns should follow:
```typescript
queryFn: async ({ signal }) => {
  const { data } = await client.GET(..., { signal })
  return data
}
```

**4. Poll intervals must respect backend cost.** Default is 10s+. The
SystemHealthCard fix bumped a 2s poll to 10s because the endpoint took 1-1.7s
under load — at 2s polling that was constant backend pressure. If real-time
updates matter, add a manual Refresh button, don't poll faster than 5s.
Always set `refetchIntervalInBackground: false` so background tabs don't
keep hammering.

**5. NEVER spawn per-instance `setInterval` for visible-tick state.** If
multiple components need a 1Hz "now" value (countdowns, "X seconds ago"
displays), they share the single
[useNowMs](frontend/hooks/useNowSeconds.ts) hook — one `setInterval` for
the whole tree. Past offenders: SystemJobBox (10 instances × 1s tick on
/admin), CronScheduleBox (5+ on /logs), useElapsedTime (per-consumer
ticker). All now consume `useNowMs`. If a new component needs a ticker,
use this hook; do not roll your own.

**6. Async buttons need IMMEDIATE feedback.** Every button whose `onClick`
does async work must render `<Loader2 className="h-3 w-3 mr-1 animate-spin" />`
+ a pending label (`Stopping…`, `Saving…`, `Severing…`) while pending.
`disabled={busy}` ALONE looks dead. Pattern lives in
[ExcludeRegexCard](frontend/components/SessionScoring/ExcludeRegexCard.tsx);
share-dashboard buttons follow the same shape after the recent fix.

**7. Prefetch behavior:**
   - Static routes → full route prefetched on Link viewport entry
   - Dynamic routes (all our `'use client'` pages) → **partially prefetched
     only if `loading.tsx` exists** (covers the shell to the loading
     boundary). Without loading.tsx, NO prefetch happens.
   - `<Link prefetch={true}>` is the default; use `prefetch={false}` only
     in dense lists (infinite-scroll tables) where the link cardinality
     would balloon the prefetch traffic.
   - **Hover-prefetch data, not just bundle:** when a Link target needs an
     API call to render meaningfully, add `onMouseEnter` that calls
     `queryClient.prefetchQuery(...)`. Example: the Admin → Share Dashboard
     link in [admin/page.tsx](frontend/app/admin/page.tsx#L791) warms the
     share-status query so the destination renders real content
     immediately instead of skeleton-then-swap.

**8. Wrap `router.replace()` inside effects in `startTransition`.** A
synchronous `router.replace()` inside `useEffect` causes a render cascade
that blocks paint. Examples:
[useUrlServiceSync](frontend/hooks/useUrlServiceSync.ts),
[AppLayout redirect block](frontend/components/AppLayout.tsx#L163). All
existing call sites are wrapped; new ones must follow.

**9. React Query defaults are set in
[QueryProvider](frontend/components/QueryProvider.tsx):** `staleTime: 30s`,
`gcTime: 5min`, `refetchOnWindowFocus: false`. Don't override per-query
unless you need to — and when you do, document why.

**10. When a click feels slow, MEASURE before guessing.** I have a working
playwright reproducer at `/tmp/nav-perf-test2.mjs` that times each phase
of a click (URL change, DOM ready, network idle, individual API requests).
Run it against the live tunnel (`localhost:3001`) BEFORE proposing a fix.
Click-feedback bugs are almost always about: (a) polls running while
navigation is in flight, (b) heavy useQuery fan-out on mount, (c) layout
re-renders triggered by store subscriptions. The trace shows which.

### Removed modules — don't recreate

- `backend/utils/audit_helpers.py` (referenced the long-removed DuckDB `_ingested_files` table)
- `backend/repositories/audit.py` (was a 27-line pass-through)
- `scripts/validate_logs.py` / `.sh` (depended on removed bits)
- `backend/core/duckdb.log_audit_event` shim (call `metadata.record_audit` directly; test patches must target `backend.core.metadata.audit.record_audit` — or `backend.core.metadata_db.record_audit` via the shim, which the `_ShimModule` proxy mirrors onto the live binding)
- `QueryRunner.safe_select` / `safe_select_list` (use `actual_cols` directly)

## Testing

**The Rule:** before committing, run `make ci`. It runs ruff check → ruff format check → mypy → pytest → typecheck-frontend → vitest → osv-scanner. Add or update tests for every change; if a change is not testable in isolation, document why.

### Backend (`tests/`, mirrors source tree)

Fixtures in [tests/conftest.py](tests/conftest.py):
- `in_memory_duckdb` — real DuckDB in memory, `_alerts` and `_views` pre-created
- `test_service_source` — minimal source dict
- `client` — FastAPI `TestClient` with DuckDB deps overridden

Patterns:
- Router test with mocked config: `@patch("config.load_config")`, `@patch("config.save_config")`, hit `TestClient`
- Router test with in-memory DuckDB: use the `client` fixture, seed via `generate_mock_logs` + `insert_mock_logs` from [tests/utils/mock_data.py](tests/utils/mock_data.py)
- Mock external HTTP: `@patch("urllib.request.urlopen")` with a `MagicMock` context manager
- Provision tests: see [tests/routers/test_provision.py](tests/routers/test_provision.py) — `@patch("backend.core.iceberg.init_iceberg_table")` etc.

**What to test:** happy path + error cases (missing params, invalid data, external 4xx/5xx), state-persistence invariants (e.g. custom fields not overwritten on unrelated saves), config schema invariants, any DuckDB query using dynamic table names (SQL-injection coverage via `_safe_table()`).

### Frontend (`frontend/__tests__/`, vitest + jsdom)

Setup: [frontend/vitest.setup.ts](frontend/vitest.setup.ts) — global mocks for Next router, `localStorage`, `matchMedia`. `globals: false`, so the setup file explicitly registers `afterEach(cleanup)` and MSW's `server.listen()` at module top-level (NOT inside a hook — see Traps #17/#18).

Patterns: `render` + `screen.getBy*` for components; `renderHook` for hooks; direct calls for pure utils.

**What to test:** pure utilities exhaustively (filters, formatters, URL builders), hook state transitions, component key states (loading/error/empty/populated), navigation/URL helpers.

## Traps & Gotchas

This is the single most valuable section. Re-read it.

### 1. `openapi-fetch` semantics
The global middleware throws on 4xx/5xx. Handle with try/catch or TanStack Query's `isError`. Extract error message with `r.error?.detail?.error || r.error?.error`.

### 2. Python `UnboundLocalError` from conditional imports
An `import x` *anywhere* in a function makes `x` local for the entire function. Any earlier use raises `UnboundLocalError`. Keep imports at module level or at the very top of the function.

### 3. Falsy empty list in `or` chains
```python
# WRONG: data["data"] == [] falls through despite key existing
raw = data.get("data") or data.get("workspaces") or []
# RIGHT:
raw = data.get("data") if "data" in data else data.get("workspaces", [])
```

### 4. `_safe_table()` is mandatory
All DuckDB table names derived from user-controlled values (service IDs, field names) must go through `_safe_table()` from [backend/repositories/_base.py](backend/repositories/_base.py). Never interpolate into SQL strings.

### 5. DuckDB write connections are serialized
`get_connection()` in [backend/core/duckdb.py](backend/core/duckdb.py) locks. Never hold a write connection across requests. (Operational metadata writes hit per-service SQLite via `metadata_db`, which is unaffected — see Trap #15.)

### 6. Configs are keyed by LOGGING service ID, not CDN service ID
`configs/{logging_service_id}.json`. When looking up a stored API key by service ID, it's the logging service ID. `cdn_service_id` is a different Fastly service.

### 7. APScheduler "config not found, skipping" during teardown is normal
A job fired after the config was deleted. The next `reload()` evicts the stale job.

### 8. Ruff format must pass
`make ci` fails the `format-check` step on any unformatted file. Run `make format` before committing.

### 9. OpenAPI types must be regenerated after backend schema changes
`cd frontend && npm run gen:types`. `make ci` does this automatically; running `tsc` manually does not.

### 10. The web-based analyst invite/join flow is live — preserve it
`api_invite_analyst()` in [backend/routers/services/core.py](backend/routers/services/core.py), `GET /api/provision/join` (SSE), `InviteAnalystDialog`, ProvisionWizard "join" mode. The invite is **JSON-only** — no encrypted invite code, no `/api/provision/decrypt-invite`.

### 11. All configs are schema v2
`lf = cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []}` — always.

### 12. NGWAF bot cache is SQLite, ATTACHed to DuckDB
`data/ngwaf/ngwaf_bot_cache.db` is SQLite, accessed via DuckDB `ATTACH ... TYPE SQLITE`. Shared across services (unlike per-service `.metadata.db`). Cross-engine bridges via `attach_ngwaf_cache` / `attach_metadata_db` in [backend/repositories/_base.py](backend/repositories/_base.py).

### 13. Next.js startup crash on restricted macOS
`ERR_SYSTEM_ERROR: uv_interface_addresses returned Unknown system error 1` on some hardened systems. `frontend/package.json` `dev` script binds `-H 127.0.0.1` to bypass interface enumeration. Don't drop the flag.

### 14. VCL regex right-hand side must be a string literal
The RHS of `~` or `!~` must be a literal. No variables, no concatenation. Use `regsub()` / `regsuball()` for dynamic logic.

### 15. Operational metadata lives in per-service SQLite, not DuckDB
Alerts, views, audit, cron history, ingested-file dedup, ASN names, source registration, usage telemetry → `data/services/{id}.metadata.db` (WAL). Read/write via [backend/core/metadata/](backend/core/metadata/) (or the [backend/core/metadata_db.py](backend/core/metadata_db.py) shim for old import paths) — never via DuckDB. JOINs against log data: ATTACH the SQLite read-only as `meta` via `attach_metadata_db()`, or pre-fetch and inline as a parameterised IN list (see `dashboard.py` ASN search). New write paths use the `@sync_db_retry` (tenacity-backed) decorator to handle SQLite `OperationalError` busy/locked under WAL contention.

### 16. Monkeypatches → catalog in [MONKEYPATCHES.md](MONKEYPATCHES.md)
Historically we patched six s3fs methods + one PyIceberg `SqlCatalog.load_table` at import time. Phase 4 of the v2.0 carve-up replaced the s3fs patches with `FosS3FileSystem` / `CachedS3FileSystem` subclasses in [backend/core/iceberg/fs.py](backend/core/iceberg/fs.py) registered as a pyiceberg `FileIO`. Whatever remains is documented in MONKEYPATCHES.md with site, motivating incident, and cleanup path. Update that file in the same commit when you add/modify/remove a patch.

### 17. MSW + openapi-fetch ordering — `server.listen()` must run at module load
`openapi-fetch` captures `globalThis.fetch` at `createClient` time. [frontend/lib/api.ts](frontend/lib/api.ts) creates its client at module load, so MSW's `server.listen()` MUST execute at the top of [frontend/vitest.setup.ts](frontend/vitest.setup.ts) — **not inside `beforeAll`**. If listen runs after lib/api.ts is imported, the captured fetch is the unpatched original and every test silently bypasses MSW. Symptom: handlers never fire, requests hit real loopback. Don't move that call into a hook.

### 18. `@testing-library/react` auto-cleanup is OFF when `globals: false`
Our [frontend/vitest.config.ts](frontend/vitest.config.ts) sets `globals: false`, so RTL doesn't register its own `afterEach(cleanup)`. [frontend/vitest.setup.ts](frontend/vitest.setup.ts) has an explicit one. Without it, earlier-test components stay mounted and pollute `screen.getBy*` (symptom: input values accumulate, e.g. `value="jane@example.comjane@example.com..."`).

### 19. `extractApiError` Error-instance trap
`Error.message` is non-enumerable, so `JSON.stringify(new Error('boom'))` returns `"{}"`. [frontend/lib/api.ts](frontend/lib/api.ts) `extractApiError` early-returns `error.message` for `instanceof Error`. Keep that check at the top if you refactor.

### 20. `RemoteAccessMiddleware` blocks admin routes over the live-share tunnel
The tunnel exposes the same FastAPI app to the public internet. Middleware classifies by `Host` and blocks remote requests from admin paths — including `/api/admin/share/*`. When you add an endpoint analysts must reach, register under `/api/share/*` or update `_is_blocked_path()`. Don't remove the `testclient`/`testserver` allow-list entries — they're what let pytest hit admin routes.

### 21. `sync_data` orphan-cleanup vs local-compaction outputs
Local compaction writes merged rollups to three places: `<cache>/data/daily/`, `<cache>/data/weekly/`, and `<cache>/data/timestamp_hour=*/compacted_*.parquet`. None of these are tracked by the iceberg snapshot, so they are NOT in `cloud_files`/`active_paths`. The orphan-cleanup loop in [backend/core/iceberg/_core.py](backend/core/iceberg/_core.py) `sync_data()` walks the cache and deletes anything not in `active_paths`; without explicit allow-rules it nukes every compacted output, and the [`local_compacted_files` registry](backend/core/metadata/ingest_log.py) then blocks re-download of the source files — silently dropping rows from the view (production: 1.65M → 302K on 2026-05-31, then 1.66M → 1.62M on 2026-06-01 from the per-partition `compacted_*` variant). The fix is two-pronged: orphan-cleanup restricts its walk to `timestamp_hour=*` dirs AND skips `compacted_*.parquet` filenames. **If you add a new local-only output pattern, add it to both the dir skip and the file skip.** Integration coverage in [tests/core/test_local_compaction.py](tests/core/test_local_compaction.py)::`test_compaction_outputs_survive_iceberg_sync_orphan_cleanup` exercises the round-trip with real `compact_local_partitions` + real `sync_data`.

### 22. `unattended-upgrades` can OOM a memory-tight VM
A 16 GB Linux VM running backend + frontend + caddy holds a steady-state working set in the 10-13 GB range. The Debian/Ubuntu nightly `apt-daily-upgrade.timer` forks a transient 1-2 GB downloader on top of that, which can trip an OOM kill that wedges the kernel (sshd dies; needs a VM reset). The mitigation is to `systemctl mask apt-daily.timer apt-daily-upgrade.timer unattended-upgrades.service` on the host and re-assert it on every restart so a re-image / apt-reinstall can't silently re-enable them. Trade-off: no automatic security patching — patch manually on a planned maintenance window with the backend container stopped. **If you provision a VM with more RAM, you may safely re-enable upgrades.**

## AI Agent Directives

These apply to every change, regardless of scope.

### Testing

1. **Run `make ci` after every code change.** Fix all errors and warnings. Never report success without running CI.
2. **Add tests for every non-trivial change.** New endpoint → router test. New utility → unit test. Bug fix → regression test that would have caught it.
3. **Prefer integration tests over pure mocks** for backend behavior. The `in_memory_duckdb` + `client` fixture pattern tests real SQL while staying fast.
4. **Test error paths.** Missing config, external 4xx/5xx, empty DB.
5. **Frontend tests live in `frontend/__tests__/`** mirroring source structure (`app/`, `components/`, `hooks/`, `lib/`).
6. **Verify in the real app when you can.** Start the server, drive the UI, watch the logs (we log every query and FOS call). Don't rely on green tests alone for feature correctness.

### Code Changes

7. **No backward-compatibility shims.** Fields like `stats_token` and `cdn_domain` do not exist in the schema — do not add fallbacks for absent fields.
8. **Never interpolate user-controlled values into SQL.** `_safe_table()` for table names, parameterised queries (`con.execute("... WHERE x = ?", [value])`) for filter values.
9. **Handle `openapi-fetch` errors explicitly.** Never `.then((r) => r.data?.x || fallback)` without checking `r.error`.
10. **Keep Python imports at module level.** Conditional mid-function imports trigger `UnboundLocalError` (Trap #2).
11. **Run `ruff format` before committing** (or rely on `make ci`).

### Secrets & sensitive data

12. **Scan for committed secrets BEFORE every commit.** The repo has a `secret-scan` Makefile target (gitleaks) that's wired into both `make ci` and the pre-commit hook (`.pre-commit-config.yaml`). Either run pre-commit (`uv run pre-commit run --all-files`) or `make secret-scan` before pushing. CI also runs it (`.github/workflows/ci.yml`) and will fail the build, but catching it locally is faster.
13. **Allowlist suppression order** when a legitimate placeholder trips the scanner:
    - **Inline** (single line): append `# gitleaks:allow` to the offending line. Cheapest for a one-off test fixture.
    - **Fingerprint** (one-off historical): add the finding's `{file}:{rule-id}:{commit}:{secret-hash}` line to `.gitleaksignore` at repo root.
    - **Path** (entire file or directory): add a regex to the `[allowlist] paths` array in `.gitleaks.toml`. Use this when adding a new directory of test fixtures.
14. **Never commit a real credential to suppress the scanner.** The point of the gate is exactly this. If a legitimate secret needs to live in the tree (e.g. an SSH public key used as a trust anchor), document why in a comment adjacent to the allowlist entry and explain why exposure is intentional.
15. **Never put real customer values in code, scripts, tests, or docs.** This includes Fastly service IDs (use `<service-id>` or `${FASTLY_SERVICE_ID:?}` env vars in scripts), bucket names, real domains, real IPs (Fastly edge ranges are fine — they're published), real email addresses (use `you@example.com`), or screenshots that show the above. Test fixtures use placeholders (`TestLogSvcABC123`, `FAKE_TOKEN`, `"FROM_CONFIG"`). Real deployment values come from env vars / per-host config that's gitignored.
16. **Files that must never be committed** (covered by `.gitignore` — verify before any new directory of generated content lands):
    - `.env` (real env), `configs/*.json` except `configs/ssh_known_hosts`, `data/system/` (real SSH key + share DB), `.scoring/` (per-deployment AES keys), `tests/fixtures/scoring/` (real prod traces). The `.gitleaks.toml` allowlist also covers these so a working-tree (`--no-git`) scan stays clean for ad-hoc local runs.

### Provisioning Wizard

12. The token entered in step 2 must be threaded to any API call needing Fastly credentials (including the NGWAF workspace fetch). Don't rely on stored-config fallback alone.
13. Step order: `mode → token → service → storage → ngwaf → fields → execute`. Document changes here.
14. The `execute` step streams SSE. Never poll it with a regular `GET`.

### New Analytics Pages

15. Pattern: `backend/routers/{name}.py` + `backend/repositories/{name}.py` + `backend/models/{name}.py` + `frontend/app/{name}/page.tsx`.
16. All DuckDB queries in repos use `_safe_table()`.
17. All new endpoints get at least one test in `tests/routers/`.
18. Regenerate OpenAPI types after the endpoint lands: `cd frontend && npm run gen:types`.

## Keeping This File Current

Update this file in the same commit that introduces:
- New or removed API endpoints
- New background job types
- Config schema changes
- New traps or gotchas you fixed (other developers and agents will hit them again)
- Workflow changes that affect the user personas

If a section here describes code or behavior that no longer exists, fix or delete it immediately. Stale docs are worse than missing docs — they actively mislead.
