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
| `backend/routers/session_scoring.py` (was 2442) | [`backend/routers/session_scoring.py`](backend/routers/session_scoring.py) (1327) + [`backend/routers/session_scoring_admin.py`](backend/routers/session_scoring_admin.py) (1193) | sidecar holds retrain + admin-config endpoints (enforce-threshold, exclude-regex, enforce-status-code, matrix-versions, rotate-key, audit, threshold GET/PUT, evaluation/per-reason, dashboard composite); registers on the shared router via import-for-side-effects at the bottom of `session_scoring.py` |
| `backend/routers/admin.py` (was 1739) | [`backend/routers/admin.py`](backend/routers/admin.py) (1491) + [`backend/routers/admin_usage.py`](backend/routers/admin_usage.py) (302) | usage-logging endpoints (settings GET/POST/PATCH, usage-log GET/DELETE/export, system-jobs) live in the sidecar; same import-for-side-effects pattern |
| `backend/core/log_fields.py` (was 1904) | [`backend/core/log_fields.py`](backend/core/log_fields.py) (659) + [`backend/core/_log_fields_data.py`](backend/core/_log_fields_data.py) (1277) | data-only carve: `LOG_FIELD_CATALOG`, `GROUP_INFO`, `GROUP_DEPENDENCIES`, `PRESETS`, `INSIGHT_DEFINITIONS` moved to the sidecar and re-imported. Zero behaviour change |
| `backend/core/duckdb.py` (was 2110) | [`backend/core/duckdb.py`](backend/core/duckdb.py) (1099) + [`backend/core/_duckdb_status.py`](backend/core/_duckdb_status.py) (1119) | `get_sync_status`, `refresh_config_status`, `update_top_values`, `get_ingested_files`, `delete_ingested_files`, `get_schema`, `_clear_schema_cache`, `get_asn_names` / `format_asn_label` / `enrich_asn_labels`, `update_cron_duration`, `log_usage_calls`, `backfill_fastly_edge_writes`, `reconcile_fastly_stats`, `purge_usage_log` move to the sidecar. Re-exported back into `backend.core.duckdb`. Sidecar late-binds shared helpers from the main module via `_db_main` to dodge the circular import |

Other new modules introduced by the cleanup:

- [`backend/repositories/_sql/`](backend/repositories/_sql/) — named, parameterized SQL templates extracted out of inline repo strings (one file per repo concern: `dashboard`, `security`, `network`, `origin`, etc.). Repository functions keep their names and signatures; they call into the templates instead of carrying SQL inline.
- [`backend/core/field_registry.py`](backend/core/field_registry.py) — Phase 7 (shipped, including step 13) typed registry that owns per-field declarations (code, display name, type, valid aggregations, valid filter ops, derivations, security-regex hooks). All readers migrated (dashboard CTE generator, rollup spec builder, top_n logic, SQL validator, scoring matrix labels, plus 8 step-13 callers: `services/core.py`, `provision/orchestrator.py`, `provision/fastly_api.py`, `provision/cli.py`, `iceberg/_core.py`, `ingest.py`, `models/custom_fields.py`, `state_sync.py`). Same-identity re-exports of every helper + constant preserve `from log_fields import X` callers.
- [`backend/core/request_context.py`](backend/core/request_context.py) — Phase 2 single FastAPI dependency that bundles `service_id`, `source`, `con`, `telemetry`, `analyst_session`, `cached_temps`. Replaces the v1 `AnalyticsDeps` bundle (deleted at the v2.0 cut — Phase 8.1/8.2) and folds `require_service_access` into context construction (there is no path that builds a context without enforcing tenancy). 23 analytics endpoints across 8 routers (dashboard / query / sessions / security / network / origin / performance / insights) now take `ctx: RequestContext = Depends(build_request_context)` directly.
- [`backend/core/request_telemetry.py`](backend/core/request_telemetry.py) — Phase 1 thin wrapper around the OTel tracer that owns section spans, query attribution, call log, cache state, and the `app.thread_wait_ms` custom metric instrumented at `_Pool.acquire`. Lives on `RequestContext`.
- [`backend/core/settings.py`](backend/core/settings.py) — Phase 3.5 `Settings(BaseSettings)` class (pydantic-settings) that owns every env var. Required-in-prod settings are pydantic validators.
- [`backend/core/iceberg/_core.py`](backend/core/iceberg/_core.py) `execute_with_stale_view_retry(con, src, fn)` — self-heal wrapper for code paths that open raw DuckDB connections instead of going through `QueryRunner`. On stale-buffer "No files found" errors, busts `_view_cache` via `clear_source_caches(keep_snapshot_cache=True)` + `update_iceberg_view(force=True)` then retries `fn` once. Used by `rdns_cache` discovery, `rollups` DESCRIBE sites, and `/api/query`. Pre-fix prod incidents: ~8h of 100%-failing rdns runs + analyst-visible query errors on the same buffer-deletion race.

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

**Pool wait observability** — `_Pool.acquire` records every checkout's wall-clock wait time to (a) the OTel `app.thread_wait_ms` histogram tagged `{outcome: reused | created | timeout, waited: true | false, service}` for off-box analysis via `docker logs app-backend-1 | grep app.thread_wait_ms`, AND (b) a bounded in-process ring buffer (~1024 samples per service) consumed by `Pool.stats().wait` (p50/p95/p99/max/mean). `GET /api/admin/health-snapshot` exposes the per-service stats; the `SystemHealthCard` on `/admin` renders top-level Pool wait p95 / Pool in-use / idle cards plus an expandable per-service table. ADR-03 escalation rule: p95 > 50ms ⇒ consider separate-process cron isolation; > 200ms flags red. Both paths are non-blocking (try/except around the recorder) so instrumentation can never break a checkout.

### Hourly Top-N Rollups ([backend/core/rollups.py](backend/core/rollups.py), [scripts/backfill_rollups.py](scripts/backfill_rollups.py))
Precomputes per-hour Top-N aggregates for the dashboard's most-asked fields (ip, country, url, custom fields) and writes them under `<cache>/rollups/`. Closed hours read from the rollup; the current ("live") hour merges the rollup with a fast scan of the buffer. Plus a per-minute time-series bundle (`rollups/hour_bundled/hour=H/time_series.parquet`) used by the dashboard chart to skip the wide Iceberg scan. Skipped buckets fall back to the raw scan path. Generated by `local_compact_{id}` after each compaction pass; the global `optimize_{id}` job rebuilds the day's worth on each run.

**Bundle tiers** (cheapest first wins in the reader):
- `rollups/day_bundled/day=D/all_fields.parquet` — one parquet per day, all fields. Reader prefers this for fully-in-window closed days.
- `rollups/hour_bundled/hour=H/all_fields.parquet` — one parquet per hour, all fields. Reader uses for partial-day boundary hours + any day without a day-bundle.
- `rollups/hour/field=F/hour=H/*.parquet` — per-(field, hour). Original source of truth; the bundle writers read from here.
- `rollups/day/field=F/day=D/*.parquet` — per-(field, day). Source for the day-bundler.

**Virtual fields** (`waf_sig_ind`, `edge_score_reason_ind` — see `_VIRTUAL_FIELD_BACKING` in rollups.py) are CSV-unnested at WRITE time so the dashboard reader serves them through the standard rollup path instead of paying a 30-day unnest-during-query each request. Wired in `_run_per_field_copy` via `_build_virtual_field_copy_query`. Adding a new virtual field requires (a) appending to `_VIRTUAL_FIELD_BACKING`, (b) ensuring its `backing` column is on the schema, (c) a one-shot rebundle migration so existing hour/day bundles pick it up (see next point).

**Stale-bundle hazard.** `bundle_hours` / `bundle_days` use mtime to skip up-to-date bundles, and the cron only re-bundles HOURS THAT JUST RECEIVED DATA. Closed historical hours never get re-touched. If you add a new field to the rollup writer (real or virtual), the per-(field, hour) parquets land but the bundled `all_fields.parquet` for closed hours stays without them — the dashboard's bundled-rollup reader returns 0 rows for the new field and the runtime fallback fires. Fix: add a data migration that deletes the closed bundles and runs `backfill_*_bundles` (canonical pattern: `_rollups_virtual_field_rebundle` in [backend/core/data_migrations.py](backend/core/data_migrations.py)).

**Live-hour batch must filter virtual fields out** before `execute_top_n_batch` (in `_base.py`'s `execute_top_n_rollups`): the SQL projects `field_name AS value` and virtual names aren't real columns on the live temp table. Passing them through BinderException's the whole UNION ALL and silently drops the live-hour merge for real fields too. See `live_fields = [f for f in fields if f in actual_cols]` at the merge site.

**`live_temp` narrow projection** ([backend/repositories/dashboard.py](backend/repositories/dashboard.py)): only `conn_requests` + `timestamp` on the `chart_metric == "requests"` path. The runtime CSV-unnest fallback for virtual fields (`_exploded_top_n`) queries the BASE table via stashed `orig_table_name` / `orig_where_clause` / `orig_params`, not the temp, so the temp doesn't need to carry `waf_sig` / `edge_score_reason`. Map_data is derived from `all_top_res` instead of a separate query on the temp, so `country` isn't needed either. If you add a new consumer that reads from the temp, add its columns to `narrow_col_set` AND verify the chart_metric branches.

**`get_top_bots` rollup-served UAs** ([backend/repositories/security.py](backend/repositories/security.py)): on the unfiltered path (`not filters`), top UAs come from `execute_top_n_rollups(["ua"], ..., limit=50000)` instead of scanning the iceberg view for the `ua` column. The NGWAF JOIN still needs the raw temp because `waf_req_id` is high-cardinality and not rollup-served — but the temp is single-column (`waf_req_id` only) when the rollup path serves UAs. Filtered requests fall back to the original combined `(ua, waf_req_id)` temp.

### Response Telemetry Middleware ([backend/utils/telemetry_response_middleware.py](backend/utils/telemetry_response_middleware.py))
Backstop for endpoints that return a plain `dict` instead of going through `BaseResponse.with_telemetry`. Inspects JSON object responses, injects `_debug_queries` / `_debug_calls` / `_is_cached` from the contextvar collectors if missing. **Must be added INNER to `CompressMiddleware`** (i.e. `add_middleware(TelemetryResponseBodyMiddleware)` BEFORE `add_middleware(CompressMiddleware)`) so it sees the raw JSON, not br/zstd/gzip-encoded bytes. Skips streaming responses, non-dict bodies, and already-instrumented responses. Gated on `DEBUG_RESPONSES`; failure modes are silent + non-blocking.

### Live Query Monitor ([backend/core/query_registry.py](backend/core/query_registry.py), [backend/routers/admin_queries.py](backend/routers/admin_queries.py), [frontend/app/admin/queries/](frontend/app/admin/queries/))
Real-time view of every executing DuckDB + SQLite query — attribution (analyst / admin / cron / system), caller `file:line`, pool slot, duration ticking up live, kind-aware Kill button that calls `con.interrupt()`. Page at `/admin/queries`, admin-only via `RemoteAccessMiddleware`. Polling at 300 ms; the Active panel promotes "completed in the last 10 s" rows as faded entries with an outcome badge so typical-traffic (p50 ≈ 0.2 ms, max ≈ 29 ms) queries are visible. Notable Slow Queries panel filters the completed-history ring buffer by threshold (100ms / 500ms / 1s / 2s / 5s), sorted slowest first.

Instrumentation lives at two seams: SQLite `InstrumentedCursor` ([backend/utils/sqlite_profiler.py](backend/utils/sqlite_profiler.py)) registers/deregisters around `execute*`; DuckDB `InstrumentedDuckDBConnection` + `_InstrumentedResult` ([backend/core/query_instrumentation.py](backend/core/query_instrumentation.py)) wraps the connection returned from `checkout_connection` so deregistration happens at terminal-fetch time (fetchdf, arrow, etc.) rather than at `execute()` — DuckDB's execute returns in ~ms while fetch can run for seconds. Per-query overhead measured ~21 µs (~0.3% of dashboard bundle wall time). Cancel path is safe under pool reuse: a stamped `_conn_to_query[id(con)]` is verified under lock before `interrupt()` so a stale UI click never cancels a different query that's checked out the same physical connection later.

Audit log fires on every successful cancel (`audit_log` in [backend/utils/structlog_config.py](backend/utils/structlog_config.py)) with the actor + full target attribution. OTel histograms: `app.active_queries.count`, `app.query_duration_ms`, `app.queries_cancelled_total`. Kill switches: `QUERY_MONITOR_ENABLED=0` hides the endpoints (404), `QUERY_REGISTRY_DISABLED=1` bypasses the hot path entirely for zero overhead. Design + post-spec polish history in [pending-docs/design_live_query_monitoring.md](pending-docs/design_live_query_monitoring.md).

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

### Server-side bootstrap pre-fetch ([frontend/lib/ssr/bootstrap.ts](frontend/lib/ssr/bootstrap.ts), [frontend/app/layout.tsx](frontend/app/layout.tsx))

The root layout SSR-fetches `/api/bootstrap`, dehydrates it into the React Query cache (via a new `HydrationBoundary` in `QueryProvider`), and ships the JSON inline in the first HTML paint. `useBootstrap` and every hook that reads `bootstrap.*` via `queryClient.getQueryData(['bootstrap'])` find the data already cached on first render — no client-side bootstrap RTT, no `'No service selected'` flash, share banner in the initial paint.

Adding a new SSR pre-fetch (e.g., for a per-page endpoint):

1. **Use `node:http.request`, NOT `fetch()`.** Node's `fetch()` always overrides the `Host` header from the URL. The backend's `_remote_host_allowed` gate rejects remote-classified requests whose Host isn't the public endpoint — so without preserved Host, the SSR fetch returns 400 host_not_allowed and silently falls through to the client.
2. **Trust topology is `X-Remote-Analyst: 1`, not `X-Proxied-By-Caddy`.** The SSR runtime hits the backend over loopback. `is_request_remote` ([backend/utils/remote_access.py](backend/utils/remote_access.py)) classifies based on `request.client.host` first, so a forwarded Caddy marker is IGNORED. `X-Remote-Analyst: 1` is the loopback-honored primitive (gated on `tunnel_manager.is_sharing_active()`). Forward it ONLY when the inbound request carries `X-Proxied-By-Caddy` — otherwise the admin SSH-tunnel path is mis-classified as analyst and 400'd. (See history: the 2026-06-11 SSR-leak incident reverted in `f3d8dd7` / `546c279` was the previous-attempt version that forwarded `X-Proxied-By-Caddy` directly. Backend ignored it, returned admin payload, dehydration leaked admin fields into public HTML.)
3. **Always wrap in try/catch + bounded timeout, return `null` on any failure.** SSR errors must NEVER propagate into a broken page — the layout falls back to client fetch when the helper returns null. 5s is generous for prod cron contention; never block SSR longer.
4. **`force-dynamic` is REQUIRED** in any layout/page that does a per-request SSR fetch via `cookies()` / `headers()` from an imported helper. Next.js's static-analysis pass only detects direct `cookies()` calls in the component file itself — calls from an imported module won't flip the route to dynamic. Without `export const dynamic = "force-dynamic"` the layout gets SSG'd at build time (when the backend isn't reachable) and the dehydrated state is permanently empty.
5. **Adversarial test required:** before deploying, hit the prod public URL anonymous AND the admin tunnel and verify the dehydrated state shape. Anonymous public must contain only the `needs_login` stub (NO `sharing_active`, NO `ngwaf_workspace_id`, NO `sync_status`). Admin must contain the full payload.

The `serviceStore` Zustand slice hydrates from the SSR-cached bootstrap in `useBootstrap`'s post-mount `useEffect` — for the one-render window before that effect fires, use [`useEffectiveServiceId`](frontend/hooks/useIsDataReady.ts) which falls back to `bootstrap.active_service_id` from the React Query cache. Direct reads of `useServiceStore(s => s.activeServiceId)` flash "No service selected" on first paint.

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
11. **`useEffectiveServiceId`** in [hooks/useIsDataReady.ts](frontend/hooks/useIsDataReady.ts) — read this instead of `useServiceStore(s => s.activeServiceId)` whenever the answer matters on FIRST PAINT (gating views, building cache keys, "no service selected" branches). It falls back to `bootstrap.active_service_id` from the SSR-hydrated React Query cache so the page doesn't flash empty before the persisted Zustand store catches up.

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

### 23. SSR upstream fetch must use `node:http`, not `fetch()`
Node's `fetch()` always rewrites the `Host` header from the URL — there's no way to override it. The backend's `_remote_host_allowed` gate ([backend/utils/remote_access.py](backend/utils/remote_access.py)) rejects remote-classified requests whose `Host` isn't the public endpoint. SSR helpers like [frontend/lib/ssr/bootstrap.ts](frontend/lib/ssr/bootstrap.ts) use `node:http.request` which preserves arbitrary headers verbatim. If you write a new SSR helper, do NOT reach for `fetch()` — copy the `rawRequest` pattern. The 2026-06-11 SSR-leak incident (reverts `f3d8dd7` / `546c279`) was the first version using `fetch()`; the `Host` got rewritten to `127.0.0.1:8000`, the backend classified as admin-from-loopback, and the full admin bootstrap dehydrated into anonymous public HTML.

### 24. Rollup writers must rebundle bundles after adding a field
`bundle_hours` / `bundle_days` use mtime to skip up-to-date bundles. The cron only re-bundles HOURS THAT JUST RECEIVED DATA. Closed historical hours never re-touch. So a new field added to the rollup writer (real or virtual) lands as a per-(field, hour) parquet but the bundled `all_fields.parquet` for closed hours stays without it — the dashboard's bundled-rollup reader returns 0 rows for the new field and the runtime fallback fires (defeats the perf win). Fix: ship a one-shot data migration that deletes the closed `all_fields.parquet` files and runs `backfill_*_bundles` so they get rewritten with the new field. Canonical pattern: `_rollups_virtual_field_rebundle` in [backend/core/data_migrations.py](backend/core/data_migrations.py).

### 25. Virtual fields blow up the live-hour batch if not filtered out
`execute_top_n_rollups` in [_base.py](backend/repositories/_base.py) needs the active-hour merge to include real fields' new rows. The live-hour SQL projects `field_name AS value` and BinderExceptions on any name that's not a column on the live temp. Virtual fields like `waf_sig_ind` don't exist as real columns — passing them through silently kills the whole UNION ALL (the outer `except Exception: pass` swallows it) and drops the live-hour merge for REAL fields too. Always filter to `actual_cols` before the batch:
```python
live_fields = [f for f in fields if f in actual_cols]
if live_fields:
    live_res, _ = self.execute_top_n_batch(live_fields, tmp_name, ...)
```

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

### Architectural choices to preserve

The 2026-06 retrospective surfaced several structural decisions the audit specifically validated. Don't rewrite these in a future reimagining:

- **ADR-driven architecture with decisions captured AFTER the lesson lands.** This is the velocity strategy, not a debt. Continue the cadence — write the ADR after a phase ships, not before.
- **[MONKEYPATCHES.md](MONKEYPATCHES.md) as a living inventory** with root-cause attribution per patch (incident date, why upstream can't fix, removal criteria).
- **Property-based testing** (Hypothesis) for filter/query roundtrips. Catches drift without hand-written matrices.
- **RequestContext** making tenancy structurally impossible to bypass — can't construct without `_enforce_service_access`.
- **Modular package carves with re-export shims** for backward compat during refactor (the `metadata_db.py` / `scheduler.py` pattern).
- **Named exception classes + explicit retry policies** (vs. generic `except Exception`).
- **Three-tier docs scheme** (pending-docs / local-docs / docs) — intentional and works for a public-repo solo project.
- **MVP-then-iterate cadence with phase-based cleanup.** Don't propose "spike before shipping" rewrites — solo bandwidth and information-unavailability at v1.0 time make iterate-then-cleanup the right trade-off.

### Anti-patterns explicitly rejected

If a refactor proposal matches one of these, push back. Each was investigated and rejected during the 2026-06 audit; the rationale is preserved here so future-you / future-agent doesn't relitigate:

- **Generic "schema codegen" infrastructure** for FilterSpec — `openapi-typescript` already handles the 80% case; codegen can't express the procedural collision-handling logic that's the actual duplication.
- **Premature `usePagination` / `PaginationConfig` context** when there are only 2 paginated endpoints with genuinely different sort semantics.
- **Centralized `RoleProvider` context** — role is 2 orthogonal flags (`analyst_session` × `is_remote_analyst`), not a hierarchy; an enum would have locked in a false model when SHARE-INVITED was added.
- **Multi-language scoring codegen** (Python ↔ Rust) — parity is enforced cheaply by fixture tests; codegen adds versioned-schema overhead and constrains schema evolution.
- **Pre-formatted server-side response values** — `TopTenTable` needs raw values for click handlers and map ops; pre-formatting forces double payload and locks display format into the API contract.
- **Cache-coherence "state machine" abstractions** — the bottleneck is DuckDB view rebuild time, not cache layer policy; a state machine wouldn't have prevented the 2026-06-09 transient-empty-result incident.
- **Unified `QueryExecutor`** for retry — stale-view and compaction-race are different error classes with different recovery costs; collapsing them creates a leaky abstraction.
- **Tentacle-parameter threading** through repository signatures (e.g., passing `RequestContext.cached_temps` to every repo function) — couples request scope to data layer.
- **Custom `FsspecFileIO` subclass to "fix" the s3fs monkeypatches** — investigated 2026-05-21 and rejected; pyiceberg instantiates `S3FileSystem` directly inside its `_s3()` builder, bypassing the FileIO layer entirely. Wait for upstream `supply-your-own-FileSystem-class` hook (tracked in [MONKEYPATCHES.md](MONKEYPATCHES.md)).

## Keeping This File Current

Update this file in the same commit that introduces:
- New or removed API endpoints
- New background job types
- Config schema changes
- New traps or gotchas you fixed (other developers and agents will hit them again)
- Workflow changes that affect the user personas

If a section here describes code or behavior that no longer exists, fix or delete it immediately. Stale docs are worse than missing docs — they actively mislead.
