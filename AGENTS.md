# AGENTS.md — AI Agent Guide

The canonical reference for any contributor or AI agent working on this project. **Read this end-to-end before your first non-trivial change; re-read the [Traps & Gotchas](#traps--gotchas) section before every change.** New here? Start with [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the system design, then come back here for the patterns and traps.

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

FastAPI + Next.js dashboard for Fastly Real-Time VCL logs streamed to Fastly Object Storage (FOS). Continuously ingests `.gz` log files into DuckDB + DuckLake (formerly PyIceberg) using a Celery/Valkey architecture and exposes per-service analytics, alerts, NGWAF bot detection, custom log fields, and a live-share feature for read-only analyst access.

User-facing pitch + features list lives in [README.md](README.md). This file documents *how it works internally*.

**Stack:**
- Backend: FastAPI, DuckDB, DuckLake, Celery, Valkey, APScheduler, boto3 (S3-compatible FOS), `uv`, `ruff`, `mypy`, `pytest`
- Frontend: Next.js 16, React 19, TanStack Query v5, Zustand, shadcn/ui, openapi-fetch, vitest
- Storage: FOS (S3-compatible), per-service DuckDB + SQLite (operational metadata), global SQLite for NGWAF bot cache + live-share
- Optional: [`falco`](https://github.com/ysugimoto/falco) VCL linter — detected via `shutil.which("falco")`, degrades gracefully to regex checks when absent

**VCL editing:** when you write or edit a log format string, a custom field `vcl_log_expression`, or any VCL snippet, it must pass `falco lint`. Use `+` for string concatenation; wrap literals in heredoc strings (`{"literal"}`). Call sites: [backend/utils/vcl_utils.py](backend/utils/vcl_utils.py), [backend/provision/fastly_api.py](backend/provision/fastly_api.py), [backend/routers/services/core.py](backend/routers/services/core.py).

## Architecture

### Data layout

| Layer | Location | Purpose |
|---|---|---|
| Raw logs | `s3://{bucket}/{prefix}/raw/**/*.gz` | Immutable gzipped JSON from Fastly |
| Local buffer | `cache/{bucket}/` | Transient Parquet between ingest and commit |
| DuckLake table | Catalog: local `.ducklake` file or Postgres DSN (multi-pod/celery-mode requires Postgres); data: `s3://{bucket}/{prefix}/ducklake/` for cloud-backed sources | Durable long-term storage. Replaced Apache Iceberg/pyiceberg as the commit-path catalog in v3.0.0 — see [ADR-14](docs/adr/14-ducklake-replacement.md). `buffer.py`/`sync.py` still carry pyiceberg-era code for the commit machinery itself; not yet fully retired. |
| Admin state | `s3://{bucket}/{prefix}/iceberg/meta/admin_state.json` | log_format_history, audit_logs, views, custom_fields (no alerts — alerts are per-instance). Path predates the DuckLake cutover; unaffected since it was never part of the Iceberg/DuckLake commit-path catalog. |
| DuckDB | `data/services/{service_id}.duckdb` | Per-service analytical engine **only**: session-scoped `logs` view + temp tables |
| Service metadata DB | `data/services/{service_id}.metadata.db`, or a shared Postgres database (`METADATA_DSN`, required for multi-pod — see [ADR-15](docs/adr/15-multi-writer-topology.md)) | Per-service SQLite (WAL): `alerts`, `views`, `audit_logs`, `cron_runs`, `sources`, `ingested_files`, `asn_names`, `slow_queries`, `ingest_ledger` (celery mode — see [ADR-16](docs/adr/16-ingest-ledger.md)) |
| Usage-log DB | `data/services/{service_id}.usage_log.db` | Per-service SQLite (WAL): `usage_log` + `usage_log_hourly_summary`, split out of `metadata.db` so the cron writer's lock never blocks admin readers |
| NGWAF bot cache | `data/ngwaf/ngwaf_bot_cache.db` | Shared SQLite for VERIFIED-BOT enrichment |
| Live-share DB | `data/system/remote_share.db` | Singleton SQLite (WAL): invites, sessions, audit, TOS, lockouts |
| Service configs | `configs/{logging_service_id}.json` | Credentials, settings, log_fields config |

The DuckDB `logs` view stitches the DuckLake table and the local Parquet buffer so queries always see all data without callers caring which layer holds which row.

### Package layout (post v2.0 carve-ups)

Several historical monoliths were split into cohesive packages with thin re-export shims at the old paths so existing imports keep working:

| Old path | New package | Shim status |
|---|---|---|
| `backend/core/iceberg.py` | [`backend/core/iceberg/`](backend/core/iceberg/) (`_core.py` + `fs.py`) | package `__init__.py` re-exports the historical public surface; the monkeypatched s3fs methods are now `FosS3FileSystem` / `CachedS3FileSystem` subclasses in `fs.py` |
| `backend/core/metadata_db.py` | [`backend/core/metadata/`](backend/core/metadata/) (`base`, `alerts`, `views`, `ingest_log`, `cron_log`, `asn_cache`, `usage_log`, `usage_log_db`, `reconciliation`, `slow_queries`, `state`) | package `__init__.py` re-exports the full surface and installs a `_ShimModule` proxy so `monkeypatch.setattr(metadata, "_DATA_DIR", ...)` still flips the live binding inside `metadata.base` (legacy callers alias the package as `metadata_db`). No separate `metadata_db.py` file remains — the proxy lives on the package |
| `backend/core/share_db.py` | [`backend/core/share_db/`](backend/core/share_db/) (`connection`, `schema`, `invites`, `sessions`, `audit`, `passcode`, `tos`, `settings`, `validation`) | package `__init__.py` re-exports the historical public surface; passcode hashing is argon2id (legacy scrypt verify branch stays for transparent rehash-on-login) |
| `backend/utils/tunnel.py` | [`backend/utils/tunnel/`](backend/utils/tunnel/) (`manager`, `session`, `rate_limiter`, `state`, `fingerprint`) | package `__init__.py` re-exports `get_tunnel_manager`, `AnalystSession`, etc. SSH-to-localhost.run code path (`_TUNNEL_URL_RE`, sleep listener, reconnect logic, `use_tunnel=True` branches) was deleted in v2.0 — only direct-mode (HTTPS public_endpoint) is supported. The `use_tunnel=True` kwarg still exists as a back-compat keyword that raises a clear error |
| `backend/scheduler.py` (removed 2026-07-06) | [`backend/cron/`](backend/cron/) (`scheduler.py`, `decorators.py`, `jobs/{sync,commit,compaction,optimize,expire,metadata,duckdb_recycle,insights_prewarmer,metric_snapshot}.py`) | none — import `get_scheduler`/`Scheduler` from `backend.cron.scheduler`, `cron_task` from `backend.cron.decorators`, and each `_run_*` body from its `backend.cron.jobs.*` module |
| `backend/routers/session_scoring.py` (was 2442) | [`backend/routers/session_scoring.py`](backend/routers/session_scoring.py) (~1.7k) + [`backend/routers/session_scoring_admin.py`](backend/routers/session_scoring_admin.py) (~1.6k) | sidecar holds retrain + admin-config endpoints (enforce-threshold, exclude-regex, enforce-status-code, matrix-versions, rotate-key, audit, threshold GET/PUT, L2-enforce GET/PUT, evaluation/per-reason, dashboard composite); registers on the shared router via import-for-side-effects at the bottom of `session_scoring.py` |
| `backend/routers/admin.py` (was 1650) | [`backend/routers/admin/`](backend/routers/admin/) (`pop_locations`, `ingest`, `trees`, `downloads`, `sync_status`, `compaction`, `health`, `log_accounting`, `iceberg`, `bot_sources`, `system_metrics`, `metric_history`, `_helpers`, `_dir_size`, `_router`) + [`backend/routers/admin_usage.py`](backend/routers/admin_usage.py) (sidecar) | v2.0 carve: 15 sub-modules each < 350 lines (`system_metrics` serves the system-vitals snapshot, `metric_history` the admin metric-history trend lines). `admin/__init__.py` re-exports the historical public surface (`router`, `compute_sync_status_cached`, `compute_log_accounting`, `LOG_ACCOUNTING_*`, `SustainedLossAlert`, `_QueueFile`, `_stream_from_worker`, `_fetch_file_to_zip`, `_resolve_source`, `_get_dir_size`, `ClientDisconnected`). `admin_usage.py` still attaches its endpoints to the shared `router` via `importlib.import_module` from the package init |
| `backend/core/rollups.py` (was 2045) | [`backend/core/rollups/`](backend/core/rollups/) (`_common`, `time_series`, `sessions`, `hour_bundles`, `day_bundles`, `recompute`, `wellknown_bots`, plus the per-dimension rollup writers `slow_urls`, `network_rtt`, `network_speed`, `origin_summary`, `origin_dims`, `origin_latency_ts`, `perf_dims`, `perf_latency`, `security_dims`, `verified_bots_ts`, `ngwaf_bots`, `network_health_heatmap`, `network_health_geo`) | v2.0 carve, writers added through v2.1. `rollups/__init__.py` re-exports the rollup surface so `from backend.core.rollups import X` (or `from backend.core import rollups; rollups.X`) keeps working unchanged. Shared bits — constants, ident validators, path helpers, query builders, `_VIRTUAL_FIELD_BACKING`, and the shared per-hour bundle writer `build_per_hour_bundles` (writer-side mirror of `compact_closed_days`) — live in `_common.py` |
| `backend/core/log_fields.py` (was 1904) | [`backend/core/log_fields.py`](backend/core/log_fields.py) (659) + [`backend/core/_log_fields_data.py`](backend/core/_log_fields_data.py) (1277) | data-only carve: `LOG_FIELD_CATALOG`, `GROUP_INFO`, `GROUP_DEPENDENCIES`, `PRESETS`, `INSIGHT_DEFINITIONS` moved to the sidecar and re-imported. Zero behaviour change |
| `backend/core/duckdb.py` (was 2110) | [`backend/core/duckdb.py`](backend/core/duckdb.py) (1099) + [`backend/core/_duckdb_status.py`](backend/core/_duckdb_status.py) (1119) | `get_sync_status`, `refresh_config_status`, `update_top_values`, `get_ingested_files`, `delete_ingested_files`, `get_schema`, `_clear_schema_cache`, `get_asn_names` / `format_asn_label` / `enrich_asn_labels`, `update_cron_duration`, `log_usage_calls`, `backfill_fastly_edge_writes`, `reconcile_fastly_stats`, `purge_usage_log` move to the sidecar. Re-exported back into `backend.core.duckdb`. Sidecar late-binds shared helpers from the main module via `_db_main` to dodge the circular import |

Other new modules introduced by the cleanup:

- [`backend/repositories/_sql/`](backend/repositories/_sql/) — named, parameterized SQL templates extracted out of inline repo strings (one file per repo concern: `dashboard`, `security`, `network`, `origin`, etc.). Repository functions keep their names and signatures; they call into the templates instead of carrying SQL inline.
- [`backend/core/field_registry.py`](backend/core/field_registry.py) — Phase 7 (shipped, including step 13) typed registry that owns per-field declarations (code, display name, type, valid aggregations, valid filter ops, derivations, security-regex hooks). All readers migrated (dashboard CTE generator, rollup spec builder, top_n logic, SQL validator, scoring matrix labels, plus 8 step-13 callers: `services/core.py`, `provision/orchestrator.py`, `provision/fastly_api.py`, `provision/cli.py`, `iceberg/_core.py`, `ingest.py`, `models/custom_fields.py`, `state_sync.py`). Same-identity re-exports of every helper + constant preserve `from log_fields import X` callers.
- [`backend/core/request_context.py`](backend/core/request_context.py) — Phase 2 single FastAPI dependency that bundles `service_id`, `source`, `con`, `telemetry`, `analyst_session`, `cached_temps`. Replaces the v1 `AnalyticsDeps` bundle (deleted at the v2.0 cut — Phase 8.1/8.2) and folds `require_service_access` into context construction (there is no path that builds a context without enforcing tenancy). 23 analytics endpoints across 8 routers (dashboard / query / sessions / security / network / origin / performance / insights) now take `ctx: RequestContext = Depends(build_request_context)` directly.
- [`backend/core/request_telemetry.py`](backend/core/request_telemetry.py) — Phase 1 thin wrapper around the OTel tracer that owns section spans, query attribution, call log, cache state, and the `app.thread_wait_ms` custom metric instrumented at `_Pool.acquire`. Lives on `RequestContext`.
- [`backend/core/sqlite_pool.py`](backend/core/sqlite_pool.py) — `ThreadLocalPool`, the generic thread-local SQLite pool extracted from the three previously-duplicated pools in `metadata/base.py`, `metadata/usage_log_db.py`, and `share_db/connection.py`. Each is now a thin wrapper configuring `path_fn` / `schema_fn` / `connect_fn` / `on_borrow_fn` around the one shared implementation; `share_db` queries flow through `InstrumentedConnection` for the first time and appear in the Live Query Monitor under `service=__global_share__`.
- **Env / config handling** — there is no central settings class. App-level env vars are read via `os.getenv` at their use sites (e.g. `OTEL_EXPORTER` in [request_telemetry.py](backend/core/request_telemetry.py), `STRUCTLOG_FORMAT` in [structlog_config.py](backend/utils/structlog_config.py), pool tuning in [duckdb_pool.py](backend/core/duckdb_pool.py)); per-service credentials/settings live in `configs/{id}.json` loaded by [backend/config.py](backend/config.py).
- [`backend/core/iceberg/_core.py`](backend/core/iceberg/_core.py) `execute_with_stale_view_retry(con, src, fn)` — self-heal wrapper for code paths that open raw DuckDB connections instead of going through `QueryRunner`. On stale-buffer "No files found" errors, busts `_view_cache` via `clear_source_caches(keep_snapshot_cache=True)` + `update_iceberg_view(force=True)` then retries `fn` once. Used by `rdns_cache` discovery, `rollups` DESCRIBE sites, and `/api/query`. Pre-fix prod incidents: ~8h of 100%-failing rdns runs + analyst-visible query errors on the same buffer-deletion race.

### Personas (where the two onboarding paths live)

The README explains the two collaboration modes for end users. Implementation pointers:

- **Admin** (`access_level: "read_write"`) — full ingest/management surface. Config: `configs/{logging_service_id}.json`.
- **Analyst Path A — independent instance** (durable, JSON-config join). Read-only FOS credentials, runs its own copy of the app. Components: `POST /api/services/{service_id}/generate-viewer-key` → [`api_invite_analyst()`](backend/routers/services/core.py), `GET /api/provision/join` (SSE), [`InviteAnalystDialog`](frontend/components/InviteAnalystDialog/), ProvisionWizard "join" mode. **Known gap as of v3.0.0**: this flow was never updated for the DuckLake cutover — pyiceberg's catalog was reconstructible purely from FOS-resident `metadata.json` pointers, which is what let a Path-A instance with only bucket credentials work standalone; DuckLake's catalog (Postgres or a local file) is not FOS-resident, so a Path-A analyst against a DuckLake/celery-mode service currently has no way to discover committed table state. See [ADR-17](docs/adr/17-analyst-path-a-ducklake.md) (Proposed, unimplemented) before touching this flow for such a service.
- **Analyst Path B — live shared instance** (direct-mode against an HTTPS public_endpoint; the SSH-tunnel-to-localhost.run option was deleted in v2.0). No FOS credentials, uses admin's running process. See [Live Dashboard Sharing](#live-dashboard-sharing) below for components.

**Both paths must keep working.** Don't remove either. Don't introduce a "unified" replacement without keeping the JSON-config flow intact — it's the only option when the admin's instance can't stay running.

## Ingest Pipeline

Two data planes, selected by `INGEST_MODE` (`local` default vs `celery`) — see [ADR-14](docs/adr/14-ducklake-replacement.md)/[ADR-15](docs/adr/15-multi-writer-topology.md)/[ADR-16](docs/adr/16-ingest-ledger.md) for the full design. Both commit to the same DuckLake table and the same unified `logs` view.

**Default (`local`) mode** — APScheduler runs these per-service (plus per-service `alerts` evaluation + `insights_prewarmer`, and process-global maintenance jobs — see the [Scheduler](#scheduler-backendcron) note). Job names were renamed from `sync_{id}`/`commit_{id}` to the pair below during the v3.0.0 rework — grep history for the old names if you're reading pre-v3 code or logs:

| Job | Schedule | Function |
|---|---|---|
| `log_discovery_{id}` | every `log_period` sec | LIST FOS raw/, download new `.gz`, transform to Parquet, update DuckDB view, flush usage log, run cleanup |
| `log_ingest_{id}` | every `commit_interval_mins` (default 5) | Commit local Parquet buffer → DuckLake table, flush usage log |
| `local_compact_{id}` | every 2 min | Compact local-only hourly/daily Parquet files, flush usage log |
| `rollup_heal_{id}` | hourly at :05 | Re-run the idempotent `backfill_missing_hour_bundles` (1-day lookback) so closed hours the per-sync recompute missed get their top-N rollups within ~1 h; local-only writes |
| `rollup_compact_{id}` | daily 02:00 UTC | Consolidate closed-day per-hour rollup parquet into per-day files (30-day deep pass); local-only writes |
| `optimize_{id}` | daily 03:00 UTC | DuckLake-native (`db_iceberg.optimize_table` → `_optimize_table_impl`): `CALL ducklake_flush_inlined_data('lake')` **then** `CALL ducklake_rewrite_data_files('lake')`. The flush is a DURABILITY step, not an optimization — DuckLake inlines small commits into the metadata catalog, and neither `ducklake_rewrite_data_files` nor `ducklake_merge_adjacent_files` promotes inlined rows (both only touch already-materialized files), so without it a table stays at `file_count = 0` forever and the catalog DB holds the only copy of the data (the raw `.gz` is deleted after ingest). Complements celery mode's `ducklake_merge_adjacent_files` (called from `commit_batch`). |
| `expire_{id}` | weekly Sun 04:00 UTC | DuckLake-native since v3.0.0 (`db_iceberg.run_cloud_maintenance` → `_run_cloud_maintenance_impl`). Four steps, each isolated so one failure records its own `*_error` result key (→ `warning` cron run) and the rest proceed: **(1)** retention delete — `DELETE FROM lake.<table> WHERE timestamp < ?` for `data_retention_days`, plus `rum_retention_days` over `client_vitals`/`client_errors` (RUM telemetry lives in those tables keyed by `cid`; `logs.rum_cid` is only the CDN-side correlation key, present solely when RUM provisioning injected that log field). **`0` means keep forever for BOTH knobs** — the pyiceberg original would have resolved `data_retention_days=0` + `rum_retention_days>0` to a "delete everything from now backwards" cutoff; that is now gated on `data_retention_days > 0`. **(2)** `ducklake_expire_snapshots('lake', older_than => ?)` for `keep_snapshot_days`, then a `ducklake_cleanup_old_files('lake', older_than => ?)` sweep. Expiry reclaims NO bytes on its own; it queues unreferenced parquet, and because a file is queued at expiry time a LATER run unlinks it — which is why the sweep is unconditional, not gated on "this run expired something". Never call `ducklake_delete_orphaned_files` here: it sweeps the data path by listing and would eat local-compaction output. Also note a snapshot cannot be expired while a live data file still anchors to it, so reclamation mostly follows the daily `optimize` rewrite. **(3)/(4)** filesystem-only purges of the local `data/` cache and `rollups/` (`cache_retention_days`, `rollup_retention_months`) — untouched by the DuckLake port. Under a shared Postgres `DUCKLAKE_CATALOG` the snapshot log is catalog-WIDE, so `snapshots_before`/`snapshots_after` and the expiry itself span every tenant, not just the one service. No CAS-retry loop: there is no metadata-pointer race to lose, the job holds the per-service write lock, and the pyiceberg exception shapes it keyed on cannot be raised. |
| `metadata_sync_{id}` | varies | Sync admin state to FOS, flush usage log |

**`INGEST_MODE=celery` mode** — discovery and conversion fan out across Celery workers instead of running in one pod's scheduler loop, backed by the `ingest_ledger` state machine (`discovered → claimed → committed`/`quarantined`/`dead_letter`). `log_discovery_{id}`/`log_ingest_{id}` still exist as job names but run inline on a RedBeat-scheduled worker rather than the backend's APScheduler, plus a new `ledger_sweep_{id}` job (crash-net recovery: reclaims stuck claims, re-dispatches lost messages with a queue-depth guard, catches up via an FOS-diff). Requires a Postgres `DUCKLAKE_CATALOG` and `METADATA_DSN` (enforced at boot by `config.validate_ingest_mode()`) — a file-based catalog cannot serve concurrent worker writers. RUM beacon ingest is ported to this mode too (`rum_discovery_{id}` + `ledger_rum_sweep_{id}` in [backend/cron/jobs/rum_ledger.py](backend/cron/jobs/rum_ledger.py), both in `_REDBEAT_JOB_PREFIXES`); the v2 `rum_sync_{id}`/`rum_commit_{id}` pair remains the sync-mode path. **Ingest scales horizontally; the SERVING tier does not — it is single-pod, see [ADR-18](docs/adr/18-serving-tier-single-pod.md).**

Teardown removes jobs on the next `_sync_jobs()` reload. The `config not found, skipping` warning during teardown is normal — a job fired after the config was deleted; harmless.

### Local-Only Parquet Compaction (Dashboard Performance)

To maintain top-tier dashboard querying speeds over long periods without generating massive FOS write costs or massive file bottlenecks, we employ sequential size-capped bin-packing local compaction (implemented in `backend/core/local_compaction.py`):
1. **Periodic Job (`local_compact_{id}`):** Runs every 2 minutes. It scans local cache directories, identifies any hourly partitions containing multiple small files, and merges them sequentially into size-capped compacted Parquet files (default <= 256MB) to maintain DuckDB query parallelism.
2. **Compact-on-Sync Thread:** Triggered immediately after a raw sync completes. If multiple new files are detected, a background thread merges them immediately.
3. **Daily & Weekly Tier Rollup:** Partitions older than 1 day (customizable via `LOCAL_COMPACT_DAILY_TIER_DAYS`) are sequentially bin-packed by day into daily files (e.g. `daily_YYYY-MM-DD_<uuid>.parquet`), with single-file bins correctly migrated to retire empty hourly dirs. Daily files older than 30 days are further bin-packed into weekly files (e.g. `weekly_YYYY-WXX_<uuid>.parquet`) under `weekly/`. All files are capped at `_MAX_PARTITION_BYTES` to prevent huge file bottlenecks and preserve maximum parallelism.

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

`GET /api/health` is cheap liveness. `GET /api/health?deep=1` also verifies per-service ingest freshness: reads `max(ingested_at) FROM ingested_files` and the latest terminal `log_discovery` cron run per service (queries match both `'log_discovery'` and the pre-v3.0.0 `'sync'` name — `cron_runs` history predates the rename); returns 503 when any service is `degraded` (last ingest older than `stale_minutes` — default 30, but SRE-22 widens it per-service to that service's own historical p95 gap between non-empty ingests before degrading, so a low-traffic service's organic quiet periods don't false-positive — or last discovery errored, or a discovery row is stuck in `status='running'` past `_STUCK_SYNC_RUNNING_MINS` — the orphaned-sync-row condition, name unchanged since it's an internal constant, not a job name — or the latest `log_ingest` / `metadata_sync` cron errored). Celery mode additionally derives freshness straight from `ingest_ledger` (`max(committed_at)`, oldest non-committed row age, worker count vs. queue depth) rather than relying on `cron_runs` timing alone — see [ADR-16](docs/adr/16-ingest-ledger.md). SQLite-only, never FOS or Fastly. Safe to wire into a load balancer.

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
- `default_off` — catalog fields flagged `default_off: True` (e.g. `cookie_session`, group H) are EXCLUDED from `resolve_enabled_fields` even when their group is enabled; they require explicit per-field opt-in. Don't "fix" a default_off field's absence by enabling its group.
- `generate_capture_vcl()` in [backend/provision/fastly_api.py](backend/provision/fastly_api.py) injects per-hook code (recv, miss, pass, fetch, error, deliver) to populate log variables. The captured `ip` field prefers a non-empty operator-set `req.http.Fastly-Client-IP` (the real source behind a fronting proxy) and falls back to `client.ip`; a priority `-100` recv snippet scrubs a client-forged `Fastly-Client-IP` at the true edge so it can't poison the log. Edge-hop detection in generated VCL is `fastly.ff.visits_this_service == 0` (a genuine per-hop salted hash a client can't forge), which replaced the earlier shield-auth secret.
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
Single `BackgroundScheduler` owned by [backend/cron/scheduler.py](backend/cron/scheduler.py). `_sync_jobs()` adds/removes per-service jobs on `reload()`. The `@cron_task` decorator (telemetry context + usage-log flush + watchdog hard-cap) lives in [backend/cron/decorators.py](backend/cron/decorators.py). Per-job bodies live under [backend/cron/jobs/](backend/cron/jobs/) (`sync`, `commit`, `compaction`, `optimize`, `expire`, `metadata`, `insights_prewarmer`, plus the process-global `duckdb_recycle` and `metric_snapshot`). `duckdb_recycle` (bounds the DuckDB object-cache leak) and `metric_snapshot` (SRE sampler feeding `metric_history`) register once for the process, not per service. Per-run progress events tracked in [backend/cron_progress.py](backend/cron_progress.py) and streamed via SSE. (The flat `backend/scheduler.py` compat shim was retired 2026-07-06; import from the `backend.cron.*` homes directly.)

### NGWAF Bot Detection ([backend/utils/ngwaf.py](backend/utils/ngwaf.py), [backend/utils/ngwaf_bot_cache.py](backend/utils/ngwaf_bot_cache.py))
Syncs VERIFIED-BOT requests from `GET https://api.fastly.com/ngwaf/v1/workspaces/{id}/requests`. JSON:API pagination via `meta.next_cursor`. Shared SQLite cache at `data/ngwaf/ngwaf_bot_cache.db`. Enriches log rows with `waf_req_id` + `waf_sig LIKE '%VERIFIED-BOT%'`.

NGWAF workspace listing (`GET /api/provision/ngwaf-workspaces`): response key is `"data"`. **Don't `or`-chain** with `data.get("workspaces", [])` — an empty list is falsy and falls through. Use `if "data" in data` explicitly. (See Trap #3.)

### Alerts / Saved Views ([backend/routers/alerts.py](backend/routers/alerts.py), [backend/routers/views.py](backend/routers/views.py))
Both stored in per-service `metadata.db` (SQLite). Alerts are threshold-based with webhook fire. Views capture filter set + time range.

### Insights ([backend/repositories/insights/](backend/repositories/insights/), `INSIGHT_DEFINITIONS` in [backend/core/_log_fields_data.py](backend/core/_log_fields_data.py))
Availability is declared in `INSIGHT_DEFINITIONS` (`required_fields` + `required_groups`); the Insights page renders each card generically. Every definition MUST declare a `category` ([backend/repositories/insights/registry.py](backend/repositories/insights/registry.py) `InsightCategory`: security/origin/edge/network/traffic — a registration without one fails Pydantic at import time); the page tabs by that key with labels/icons/triage order in [frontend/lib/insight-sections.ts](frontend/lib/insight-sections.ts), so a new insight in an EXISTING category needs no frontend change, but a new category needs a section entry there. Set `required_groups=[]` when the gate should be only core fields — declaring a group grays the card out in LogSettings / ProvisionWizard previews even if it would run (e.g. `repeated_patterns` needs only client IP + timestamp). Definitions, row processors, and severity logic live in [backend/repositories/insights/definitions.py](backend/repositories/insights/definitions.py); SQL templates in [backend/repositories/_sql/insights.py](backend/repositories/_sql/insights.py). The repository's `sql.count("?")` placeholder heuristic means any regex literal in a template must be `?`-free (no `(?i)` — pass the `'i'` flag to `regexp_matches` instead). The default window/baseline adapts to the available history (a young service compares the last hour against the previous hour instead of showing "not enough data"). Analyst insight results are warmed via a stable invite-keyed cache + the `insights_prewarmer` cron. PARITY CONTRACT: the prewarmer must warm EXACTLY the adaptive default (window, baseline) pair the page will pick — [backend/utils/insights_defaults.py](backend/utils/insights_defaults.py) mirrors `pickInsightsDefault` in [frontend/lib/insights-defaults.ts](frontend/lib/insights-defaults.ts) band-for-band; the `/api/insights` cache key folds in both hour values, so warming any other pair is a guaranteed cold compute (~20 s on a long-history service). `tests/utils/test_insights_defaults.py` parses the TS source so the two sides can't drift silently.

### State Sync ([backend/state_sync.py](backend/state_sync.py))
`export_admin_state` writes `audit_logs` + `views` from per-service SQLite, plus `log_format_history` + `custom_fields` from the config JSON, to `{prefix}/iceberg/meta/admin_state.json`. **Alerts are not synced** — each instance maintains its own. Only `read_write` services export.

### FOS Usage Logging ([backend/utils/usage_logger.py](backend/utils/usage_logger.py), [backend/core/metadata/usage_log.py](backend/core/metadata/usage_log.py))
Every FOS Class A/B op and CDN download recorded to per-service `usage_log` SQLite for cost analysis.
- Global toggle: `data/system/usage_logging.json`
- Process-context tagging via `set_process_context()` in [backend/utils/telemetry.py](backend/utils/telemetry.py) — tags entries with `cron:sync:svc1` or `api:GET /api/...`
- Each cron handler calls `flush_usage_log(service_id)` at completion (the `@cron_task` decorator wires this).
- Costs computed at query time from rate config — changing rates recomputes history.
- Admin endpoints: `GET/PATCH /api/admin/usage-logging`, `GET/DELETE /api/admin/usage-log`, `GET /api/admin/usage-log/export`. Frontend: `/admin/usage-log`.

### Log-Line Accounting ([backend/routers/admin/log_accounting.py](backend/routers/admin/log_accounting.py) `api_log_accounting`)
Per-bucket reconciliation between Fastly's `/stats/service/{id}` log-emission counter and our `sum(row_count) FROM ingested_files`.
- Field probe order: `log → log_records → log_entries → logging_requests`; first non-zero wins. All-zero logs a warning.
- In-flight clamp: current bucket is in totals but excluded from sustained-loss scan (Fastly Stats lags ingest).
- Sustained-loss alert: ≥2 consecutive completed buckets with `gap_pct ≥ 0.05`.
- Frontend cadence: `staleTime 30s`, `refetchInterval 60s` → ≤1 Fastly Stats call/min per open admin tab.

### Iceberg Pointer + Summary Hash-Throttle ([backend/core/iceberg/_core.py](backend/core/iceberg/_core.py))
Every commit writes `metadata_location.txt` (unavoidable) and `table_summary.json` (skippable). The latter is content-hashed against `_table_summary_hash_cache`; identical payloads skip the PUT. Saves one FOS PUT per no-op commit in steady state. Cache is module-scope, process-lifetime.

### DuckDB Connection Pool ([backend/core/duckdb_pool.py](backend/core/duckdb_pool.py))
Per-service LIFO pool replaces per-request `duckdb.connect()` + S3 / iceberg setup + view rebind (~50ms steady-state). Pool size is `DUCKDB_POOL_MAX_SIZE` (default 8). All pool connections open with `read_only=False` — `get_connection` forces this so cron writers and pool readers don't trip DuckDB's "different configuration" error on the same file. Optional per-connection tuning: `DUCKDB_POOL_CONN_MEMORY_LIMIT` (e.g. `256MB`) caps RSS growth under concurrent large scans; `DUCKDB_POOL_CONN_THREADS` reduces context-switching when `pool_size × per_conn_threads` exceeds physical cores. View-binding happens outside the pool lock to avoid deadlocking the FastAPI thread pool when an Iceberg snapshot reload blocks.

**Pool wait observability** — `_Pool.acquire` records every checkout's wall-clock wait time to (a) the OTel `app.thread_wait_ms` histogram tagged `{outcome: reused | created | timeout, waited: true | false, service}` for off-box analysis via `docker compose logs backend | grep app.thread_wait_ms`, AND (b) a bounded in-process ring buffer (~1024 samples per service) consumed by `Pool.stats().wait` (p50/p95/p99/max/mean). `GET /api/admin/health-snapshot` exposes the per-service stats (plus `saturated_rejects_total` / `drain_rejects_total` pool-reject counters and the last-warmed timestamp); the `SystemHealthCard` on `/admin` renders top-level Pool wait p95 / Pool in-use / idle cards plus an expandable per-service table. ADR-03 escalation rule: p95 > 50ms ⇒ consider separate-process cron isolation; > 200ms flags red. Both paths are non-blocking (try/except around the recorder) so instrumentation can never break a checkout.

**OOM safeguards.** Two backstops bound process RSS independently of the pool. (1) [backend/core/memory_guard.py](backend/core/memory_guard.py) — an opt-in process-level guard (`BACKEND_GRACEFUL_RESTART_RSS_MB`) that, on crossing the ceiling, converts a destructive cgroup OOM-`SIGKILL` into a graceful self-restart (SIGTERM → uvicorn drains → `restart: unless-stopped`). (2) The `duckdb_recycle` cron drains + recycles the pool to free DuckDB's object cache (`DUCKDB_RECYCLE_INTERVAL_MIN`, off by default). The historical unbounded-RSS leak that motivated both was ultimately a **SQLite** connection leak — the per-tick cron watchdog built a throwaway `ThreadPoolExecutor` each tick, orphaning the `check_same_thread` `ThreadLocalPool` connection it opened; [backend/core/sqlite_pool.py](backend/core/sqlite_pool.py) now reaps connections whose owning thread has exited (swept on cold-open), and the watchdog reuses one bounded executor.

### Hourly Top-N Rollups ([backend/core/rollups/](backend/core/rollups/), [scripts/backfill_rollups.py](scripts/backfill_rollups.py))
Precomputes per-hour Top-N aggregates for the dashboard's most-asked fields (ip, country, url, custom fields) and writes them under `<cache>/rollups/`. Closed hours read from the rollup; the current ("live") hour merges the rollup with a fast scan of the buffer. Plus a per-minute time-series bundle (`rollups/hour_bundled/hour=H/time_series.parquet`) used by the dashboard chart to skip the wide Iceberg scan. Skipped buckets fall back to the raw scan path. Written by `recompute_touched_hours` after each `local_compact_{id}` pass (only hours that just received data). Bursty services can close an hour without ever re-touching it, so the hourly `rollup_heal_{id}` cron (:05 — `_run_rollup_hour_heal` in [backend/cron/jobs/compaction.py](backend/cron/jobs/compaction.py)) re-runs the idempotent `backfill_missing_hour_bundles` with a 1-day lookback; `rollup_compact_{id}` (daily 02:00 UTC) is the 30-day deep pass + per-day compaction. Verified-empty closed hours get an empty sentinel bundle (covered-and-empty, distinguishable from a writer gap), and the reader (`execute_top_n_rollups` in [_base.py](backend/repositories/_base.py)) live-queries any still-uncovered closed hours of not-yet-day-compacted days instead of silently under-counting (the 2026-07-06 "total_rows disagrees with every card's field_total" bug).

**Bundle tiers** (cheapest first wins in the reader):
- `rollups/day_bundled/day=D/all_fields.parquet` — one parquet per day, all fields. Reader prefers this for fully-in-window closed days.
- `rollups/hour_bundled/hour=H/all_fields.parquet` — one parquet per hour, all fields. Reader uses for partial-day boundary hours + any day without a day-bundle.
- `rollups/hour/field=F/hour=H/*.parquet` — per-(field, hour). Original source of truth; the bundle writers read from here.
- `rollups/day/field=F/day=D/*.parquet` — per-(field, day). Source for the day-bundler.

**Virtual fields** (`waf_sig_ind`, `edge_score_reason_ind` — see `_VIRTUAL_FIELD_BACKING` in `rollups/_common.py`) are CSV-unnested at WRITE time so the dashboard reader serves them through the standard rollup path instead of paying a 30-day unnest-during-query each request. Wired in `_run_per_field_copy` (rollups/recompute.py) via `_build_virtual_field_copy_query` (rollups/_common.py). Adding a new virtual field requires (a) appending to `_VIRTUAL_FIELD_BACKING`, (b) ensuring its `backing` column is on the schema, (c) a one-shot rebundle migration so existing hour/day bundles pick it up (see next point).

**Stale-bundle hazard.** `bundle_hours` / `bundle_days` use mtime to skip up-to-date bundles, and the cron only re-bundles HOURS THAT JUST RECEIVED DATA. Closed historical hours never get re-touched. If you add a new field to the rollup writer (real or virtual), the per-(field, hour) parquets land but the bundled `all_fields.parquet` for closed hours stays without them — the dashboard's bundled-rollup reader returns 0 rows for the new field and the runtime fallback fires. Fix: delete the stale closed bundles and re-run the backfill — `backfill_missing_bundles` / `backfill_day_bundles` in [backend/core/rollups/](backend/core/rollups/), or the [`POST /api/admin/backfill-bundle-rollups`](backend/routers/admin/compaction.py) endpoint.

**Live-hour batch must filter virtual fields out** before `execute_top_n_batch` (in `_base.py`'s `execute_top_n_rollups`): the SQL projects `field_name AS value` and virtual names aren't real columns on the live temp table. Passing them through BinderException's the whole UNION ALL and silently drops the live-hour merge for real fields too. See `live_fields = [f for f in fields if f in actual_cols]` at the merge site.

**Dashboard rollup path builds NO per-request temp** ([backend/repositories/dashboard.py](backend/repositories/dashboard.py) `_build_query_target`): every window-scan section is rollup-served — top-N/map via `execute_top_n_rollups`, chart via `try_time_series_from_rollup`, `conn_requests` histogram via `try_conn_requests_hist_from_rollup` (reads the `security_conn_reuse` rollup; 2h min-window override + live active-hour top-up + '21-100'/'>100'→'21+' label fold), signal unnests via the virtual-field rollups — and each section's rollup-miss fallback runs its ONE query against the base table via the stashed `orig_table_name` / `orig_where_clause` / `orig_params` (the old eager narrow temp cost ~391 ms for ~12 ms of temp reads). The non-rollup (filtered) path still materializes the wide temp. `map_data` derives from `all_top_res`, not a separate query. New dashboard consumers: serve from a rollup with a direct base-table fallback; don't reintroduce a shared per-request temp.

**`get_top_bots` rollup-served UAs** ([backend/repositories/security.py](backend/repositories/security.py)): on the unfiltered path (`not filters`), top UAs come from `execute_top_n_rollups(["ua"], ..., limit=50000)` instead of scanning the iceberg view for the `ua` column. The NGWAF bots panel is served from the per-hour `ngwaf_bots` rollup ([backend/core/rollups/ngwaf_bots.py](backend/core/rollups/ngwaf_bots.py) — the `waf_req_id ⨝ ngwaf_bot_cache` join done ONCE per closed hour at write time via `sqlite_scan`; exact SUM across hours, no `_approx`; a zero-bot closed hour writes an EMPTY parquet as a covered-and-empty sentinel so quiet services don't fall back forever) via `runner.try_ngwaf_top_bots_from_rollup`. On a rollup miss it falls back to a single direct base-table join (`SQL.NGWAF_TOP_BOTS_JOIN_DIRECT`) — NO temp at all on the rollup-UA path. Filtered requests fall back to the original combined `(ua, waf_req_id)` temp.

**`/api/network-health` skip-temp seam** ([backend/repositories/network.py](backend/repositories/network.py)): the heatmap/geo sections are hoisted to `try_network_heatmap_from_rollup` / `try_network_geo_from_rollup` BEFORE building the filtered temp; when every requested scan-bound section hits a rollup, `create_filtered_temp_table` is skipped entirely (mirrors origin.py's skip-temp guard — `network:temp_skipped` in `_section_timings`).

**When adding a new analytics panel (or a new field rendered on an existing panel), consider a rollup at PR time.** Any panel reading from the per-request temp table on 30 d windows is a candidate. Workflow:

1. **Measure first.** Hit the endpoint on prod via the admin tunnel with a 30 d window + empty `filters: {}` and inspect `_section_timings` in the JSON response. Any per-panel section > ~1 s on 30 d is rollup-worthy. (Audit JSONs under `performance-report/` usually don't carry section_timing — prefer a live curl.)
2. **Pick the shape.** Three reference templates already cover the common cases:
   - Per-dimension percentiles (weighted-average across hours) — copy [backend/core/rollups/slow_urls.py](backend/core/rollups/slow_urls.py) (per-URL p50/p95/p99) or [backend/core/rollups/network_rtt.py](backend/core/rollups/network_rtt.py) (per-ASN p95/p99). Reader returns `{..., "_approx": True}`; the FE surfaces an "Approximate" badge on the affected panel.
   - Exact GROUP BY counts — copy [backend/core/rollups/network_speed.py](backend/core/rollups/network_speed.py). Math is associative across hours; no `_approx` flag.
   - Exact time series (re-bucketable) — copy [backend/core/rollups/verified_bots_ts.py](backend/core/rollups/verified_bots_ts.py). Store at MINUTE granularity (`date_trunc('minute', timestamp)`); the reader re-buckets via `time_bucket` to any caller `bucket_seconds` that's a multiple of 60 (gate `% 60 == 0` — non-multiples are inexact). Unlike the leaderboard shapes, the **day compactor PRESERVES the bucket_ts dimension** (`GROUP BY bucket_ts, dim`) so a series can still be produced over the window, and the reader is **hybrid**: `UNION ALL` of the closed-hour rollup + a scoped live query for the in-progress active hour from the temp table (the writer never rolls up the active hour), merged by an outer `GROUP BY (bucket, dim) SUM`. No `_approx` flag.
   - Single-row-per-hour summary (multiple aggregates) — copy [backend/core/rollups/origin_summary.py](backend/core/rollups/origin_summary.py).
3. **Wire the 10 seams.** Writer module → constants in `_common.py` → exports in `__init__.py` → `recompute.py` hook (best-effort `try/except`) → daily compactor in `day_bundles.py` (mirror `compact_network_rtt_closed_days_to_daily`) → cron hook in `backend/cron/jobs/compaction.py` → reader method on `QueryRunner` in `backend/repositories/_base.py` (with the standard eligibility gates: `not has_filters` + window ≥ 48 h + ≥ 50% closed-hour coverage + day-prefer/hour-fallback walk) → dispatcher at the live-SQL call site (try rollup, fall through to live on `None`) → admin backfill endpoint in `backend/routers/admin/compaction.py` → tests + extend `tests/core/test_rollups_recompute.py` to patch the new `build_*` call in both `_swallows_downstream_bundle_errors` and `_malformed_hour_skipped`.
4. **After deploy, run the post-deploy backfill.** `POST /api/admin/backfill-bundle-rollups` walks the bundle tree and produces both the per-hour and per-day files for historical hours in one shot — without this, the new rollup only covers hours touched after deploy.
5. **Re-measure on prod.** Confirm `_section_timings` shows `<name>_query_rollup` with sub-100 ms instead of `<name>_query` with seconds. If the dispatcher routes but timing is unchanged, the rollup file wasn't built (check backfill counts).

**Don't try these — they've been declined for documented reasons:**
- Pre-aggregating percentile sketches for cross-hour combine — DuckDB has no sketch combine. Use request-weighted averages with the count carried alongside (see `network_rtt.py` reader SQL + the no-sketch-combine comment in [backend/repositories/_base.py](backend/repositories/_base.py)).
- Collapsing rollup parquets into fewer-larger files for SCAN-bound queries — DuckDB parallelises across files. Daily-compaction for ROLLUPS is different (those are file-open-overhead-dominated, not scan-bound).
- Response-caching as the perf lever — new logs are always being ingested so the cache TTL has to be sub-minute. Real wins live on the cold-path SQL.
- `temp_table_create` itself — it's materialize-bound; prior CTE/view replacements REGRESSED downstream scans 5×. Add more rollups so fewer panels read from the temp at all instead of trying to make the temp faster.

### Response Telemetry Middleware ([backend/utils/telemetry_response_middleware.py](backend/utils/telemetry_response_middleware.py))
Backstop for endpoints that return a plain `dict` instead of going through `BaseResponse.with_telemetry`. Inspects JSON object responses, injects `_debug_queries` / `_debug_calls` / `_debug_sqlite` / `_is_cached` from the contextvar collectors if missing (`_debug_sqlite` is snapshot-copied BEFORE `get_tracked_calls()`, which can itself run a SQLite SELECT that would append mid-injection). **Must be added INNER to `CompressMiddleware`** (i.e. `add_middleware(TelemetryResponseBodyMiddleware)` BEFORE `add_middleware(CompressMiddleware)`) so it sees the raw JSON, not br/zstd/gzip-encoded bytes. Skips streaming responses, non-dict bodies, and already-instrumented responses. Debug keys are per-request opt-in: the frontend API client sends `x-debug-responses: 1` when the DiagnosticsPanel toggle is on (keys are STRIPPED otherwise), and SSR's own upstream fetch mirrors the toggle via the `fla.debugResponses` cookie ([frontend/lib/debug-cookie.ts](frontend/lib/debug-cookie.ts), read in [frontend/lib/ssr/_transport.ts](frontend/lib/ssr/_transport.ts)) — the Debug Panel's SQLite/DuckDB views are page-scoped per-response captures, not global ring buffers. Gated on `DEBUG_RESPONSES`; failure modes are silent + non-blocking.

### Live Query Monitor ([backend/core/query_registry.py](backend/core/query_registry.py), [backend/routers/admin_queries.py](backend/routers/admin_queries.py), [frontend/app/admin/queries/](frontend/app/admin/queries/))
Real-time view of every executing DuckDB + SQLite query — attribution (analyst / admin / cron / system), caller `file:line`, pool slot, duration ticking up live, kind-aware Kill button that calls `con.interrupt()`. Page at `/admin/queries`, admin-only via `RemoteAccessMiddleware`. Polling at 300 ms; the Active panel promotes "completed in the last 10 s" rows as faded entries with an outcome badge so typical-traffic (p50 ≈ 0.2 ms, max ≈ 29 ms) queries are visible. Notable Slow Queries panel filters the completed-history ring buffer by threshold (100ms / 500ms / 1s / 2s / 5s), sorted slowest first. Queries above the persistence threshold are also written to a per-service `slow_queries` table ([backend/core/metadata/slow_queries.py](backend/core/metadata/slow_queries.py), in `metadata.db`) stamped with the request correlation id (`rid`, also emitted in the access log), so the panel can answer "what was slow yesterday?" across restarts.

Instrumentation lives at two seams: SQLite `InstrumentedCursor` ([backend/utils/sqlite_profiler.py](backend/utils/sqlite_profiler.py)) registers/deregisters around `execute*`; DuckDB `InstrumentedDuckDBConnection` + `_InstrumentedResult` ([backend/core/query_instrumentation.py](backend/core/query_instrumentation.py)) wraps the connection returned from `checkout_connection` so deregistration happens at terminal-fetch time (fetchdf, arrow, etc.) rather than at `execute()` — DuckDB's execute returns in ~ms while fetch can run for seconds. Per-query overhead measured ~21 µs (~0.3% of dashboard bundle wall time). Cancel path is safe under pool reuse: a stamped `_conn_to_query[id(con)]` is verified under lock before `interrupt()` so a stale UI click never cancels a different query that's checked out the same physical connection later.

Audit log fires on every successful cancel (`audit_log` in [backend/utils/structlog_config.py](backend/utils/structlog_config.py)) with the actor + full target attribution. OTel histograms: `app.active_queries.count`, `app.query_duration_ms`, `app.queries_cancelled_total`. Kill switches: `QUERY_MONITOR_ENABLED=0` hides the endpoints (404), `QUERY_REGISTRY_DISABLED=1` bypasses the hot path entirely for zero overhead.

### Multiplexed Admin SSE ([backend/routers/admin/events.py](backend/routers/admin/events.py), [frontend/hooks/useAdminEventStream.ts](frontend/hooks/useAdminEventStream.ts))
The admin shell mounts ONE SSE connection: `GET /api/admin/events/stream?channels=sync_status,cron_runs,system_metrics,share`. It fans the publisher subscriptions + the metrics poll loop into a single typed-envelope stream; [frontend/lib/admin-stream-apply.ts](frontend/lib/admin-stream-apply.ts) demuxes each envelope back to the same React Query cache the old per-stream hooks wrote. This replaced four always-on streams (`sync-status`, `cron-runs`, `system-metrics`, `share`) that starved bootstrap/panel fetches over the HTTP/1.1 admin tunnel (~6 conns/origin). The `system_metrics` channel is deduped server-side ([backend/system_metrics_sampler.py](backend/system_metrics_sampler.py), short per-service TTL + per-key lock) so N admin tabs collapse to one recompute per window. The analyst `/api/log-extents/stream` is separate and untouched. **When adding an admin real-time feed, add a channel here — don't mount a new dedicated SSE endpoint.** `build_share_live_payload` lives in `backend.utils.tunnel` (not `share_admin`) so the events router imports it without a routers→routers edge.

The remaining HTTP/1.1-tunnel connection-limit exposure (v2.2.1) is addressed one layer down, in the Caddy config itself: a second site block in the [Caddyfile](Caddyfile) listens on `https://localhost:8443` / `https://127.0.0.1:8443` (loopback-pinned via an explicit `bind`, since a bare hostname like `localhost` doesn't restrict Caddy's listener the way a literal IP does), terminating TLS with Caddy's internal self-signed CA so the browser negotiates HTTP/2 (real multiplexing) over the SSH tunnel instead of being capped at ~6 connections/origin under HTTP/1.1. `ssh -L 8443:127.0.0.1:8443 <host>` (see `docs/deploy/*.md`) is the recommended tunnel for any multi-tab admin session; the plain `:3001`/`:8000` HTTP tunnel still works and is what the rest of this doc's tunnel examples assume.

### Control Room ([backend/routers/control_room.py](backend/routers/control_room.py), [frontend/app/control-room/](frontend/app/control-room/))
Admin-only operational dashboard with nine fully-live tabs: Overview, Performance, Origin, Security, Network, Sessions, Cost, Insights, and Admin Health. All tabs are backed by a real-time SSE stream polling `rt.fastly.com` at 1-second cadence (`/api/services/{id}/realtime-stream`). The poller runs on a dedicated daemon thread ([backend/core/realtime/poller.py](backend/core/realtime/poller.py)) with httpx connection pooling, immune to event-loop starvation. The transform layer ([backend/core/realtime/transform.py](backend/core/realtime/transform.py)) extracts 50+ metrics from the RT response into typed SSE envelopes. Charts seed with 60 bars of historical data on page load, render with rolling-average smoothing and SI-suffix Y-axis formatting (k, M, G), and use progressive rendering with lazy-loaded Plotly. A PoP traffic heatmap shows geographic request distribution. Contextual help icons explain each panel. Backgrounded tabs stop polling. Mutation endpoints (mitigations, rules, allowlist, big-red-button, cost-governor) remain admin-gated 501 stubs. Log-field audit and correlator endpoints provide DuckDB-driven data. The Admin Health tab surfaces log-field audit state and ingest health.

### CMCD Streaming Analytics ([backend/routers/cmcd.py](backend/routers/cmcd.py), [backend/routers/cmcd_admin.py](backend/routers/cmcd_admin.py), [frontend/app/streaming/](frontend/app/streaming/))
Services with Common Media Client Data (CMCD) logging enabled get a `/streaming` page and per-session `/sessions/stream` detail page. CMCD field extraction supports v1 (query-string) and v2 (request-header) formats via generated VCL snippets ([backend/provision/cmcd_vcl.py](backend/provision/cmcd_vcl.py)). The admin settings UI ([backend/routers/cmcd_admin.py](backend/routers/cmcd_admin.py)) selects the CMCD version and manages the field catalog. The streaming page shows buffer-health distributions, bitrate/throughput time series, content-type breakdowns, top streaming URLs, and concurrent-session counts with 30-second auto-refresh. The nav item hides automatically when a service lacks CMCD logging (`hasCmcd` derived from service status schema). Repository SQL lives in [backend/repositories/cmcd.py](backend/repositories/cmcd.py); queries use a temp table with combined time-series and no self-joins.

### Service Summary / Fastly Value ([backend/routers/value.py](backend/routers/value.py), [frontend/app/fastly-value/](frontend/app/fastly-value/))
Executive summary page consolidating the measurable value Fastly delivers across tabbed sections: Summary, CDN & Caching, Security, Bot Management, Network & Performance, and Image Optimizer. Backed by a precomputed overview rollup ([backend/core/rollups/overview.py](backend/core/rollups/overview.py)) for fast loads. The IO tab shows format distribution, bandwidth savings from format conversion, per-request `Fastly-Io-Transform-Stats` metrics, and optimization-opportunity tables with drill-down links. Services without IO enabled see an upsell tab instead. Repository SQL in [backend/repositories/value.py](backend/repositories/value.py).

### Ingest Error Quarantine ([backend/core/metadata/quarantine.py](backend/core/metadata/quarantine.py), [backend/routers/admin/quarantine.py](backend/routers/admin/quarantine.py))
Corrupt or unparseable log lines are saved to an `errors/` prefix in FOS, timestamped and classified by failure reason (malformed JSON, schema mismatch, encoding error). Quarantine storage is included in usage cost calculations. Admin endpoints support export, download, and purge (purge deletes all files, not just expired). The admin UI renders a Quarantine section on the admin page ([frontend/app/admin/_sections/QuarantineSection.tsx](frontend/app/admin/_sections/QuarantineSection.tsx)).

### CDN-Fronted Log Delivery
FOS reads are fronted by a Fastly CDN VCL service (`cdn_service_id`, `cdn_url`, `cdn_secret`). The CDN validates a shared-secret query param to gate access; rate-limited to blunt brute-force. Separate from the logging service ID.

### Session Scoring (edge L2) ([backend/routers/session_scoring.py](backend/routers/session_scoring.py), [compute/scorer/](compute/scorer/))
Edge-computed 0–100 risk score per request, combining cookie/timing signals (L1) with a PageRank route-transition matrix (L2). The Rust scorer at [compute/scorer/](compute/scorer/) builds to Wasm (`make scorer-package`) and runs on Fastly Compute as an instance-per-request sub-fetch; its native unit tests gate via `make scorer-test`. The matrix is **not** embedded — it's served from the `scoring_matrix` KV Store at runtime. Backend surface: read/retrain/admin-config endpoints in [session_scoring.py](backend/routers/session_scoring.py) + [session_scoring_admin.py](backend/routers/session_scoring_admin.py); deploy/teardown orchestration in [backend/provision/session_scoring_orchestrator.py](backend/provision/session_scoring_orchestrator.py); VCL generation in [backend/provision/session_scoring_vcl.py](backend/provision/session_scoring_vcl.py).

- **L2 enforcement is explicit operator opt-in** — `GET`/`PUT /scoring/l2-enforce` (`L2EnforcementCard` in the UI). L2 is always computed and logged but contributes to the *enforced* combined score only after an operator enables it; enabling fades it in over three days, disabling returns it to observe-only. There is no clock-driven auto-ramp. Deployment age is only an advisory readiness gauge.
- **NGWAF skip-inspection on the sub-fetch** — the internal scoring sub-fetch carries `x-sigsci-skip-inspection-once` so NGWAF doesn't inspect (and 406) the internal call; it's set on the compute route and unset on the scrub + restart paths so the real-origin WAF path is never bypassed. (See [session_scoring_vcl.py](backend/provision/session_scoring_vcl.py).)
- **No VCL retry of the scoring sub-fetch** — only 2 restarts (score→origin spends them); fails open on timeout/error by design.

### Live Dashboard Sharing
Components for the live-shared-instance remote-analyst feature (Path B). Two direct-mode sharing modes are exposed to the admin (the SSH-reverse-tunnel via localhost.run was deleted in v2.0):

1. **Admin-provided hostname** (e.g. `https://logs.example.com`)
2. **Admin-provided IP** (e.g. `https://203.0.113.42:8443`)

Both share a single backend code path: `ShareStartPayload.use_tunnel=False` + `public_endpoint=<https URL>`. The mode selector in the UI is presentational — the backend only cares that `public_endpoint` starts with `https://` (cookies need `secure=true`). `use_tunnel=True` still exists as a back-compat keyword and now raises a clear error.

Components:

- [backend/utils/tunnel/](backend/utils/tunnel/) — package split: `manager.py` owns the `TunnelManager` singleton (direct-mode lifecycle, sever-all panic), `session.py` holds `AnalystSession`, `rate_limiter.py` is the sliding-window `_LoginRateLimiter`, `state.py` persists `tunnel_state.json`, `fingerprint.py` computes the session fingerprint hash. Process singleton via `get_tunnel_manager()`; `reset_for_tests()` for pytest.
- [backend/utils/remote_access.py](backend/utils/remote_access.py) — `RemoteAccessMiddleware` does DNS-rebinding gate (Host/Origin allow-lists, including `testclient`/`testserver` for pytest), blocks admin paths on remote requests, applies response hardening (CSP, X-Frame-Options DENY, no-store, no-referrer). `_StaticAssetLimiter` rate-limits static assets to blunt scrapes. **Idle-timeout activity model:** the middleware refreshes an analyst session's 2-hour idle deadline only on genuine interaction — requests carry an `X-User-Active: 1|0` header (set from real DOM activity, not background react-query refetches), SSE streams (`/api/log-extents/stream`) are exempt so a backgrounded tab can't keep itself alive, an IP change updates the session's recorded address without bumping the clock, and the `/api/share/heartbeat` beat doubles as the activity channel for an otherwise-quiet dashboard. The access log records `act` + `idle_touch` per request for observability.
- [backend/core/share_db/](backend/core/share_db/) — package split: `connection.py` (pool + corruption self-heal with quarantine), `schema.py` (own MIGRATIONS dict + `apply_pending` + `PRAGMA user_version`, plus `_reconcile_additive_columns` — self-heal that re-asserts additive columns when `user_version` is ahead of the actual DDL; `remote_invites.allow_concurrent_sessions`, migration 003, backs the per-invite "allow shared logins" toggle via `PATCH /api/admin/share/invites/{invite_id}/sharing`), `invites.py`, `sessions.py`, `audit.py`, `passcode.py` (argon2id current default; scrypt verify branch stays for transparent rehash-on-login upgrade), `tos.py`, `settings.py`, `validation.py`. Singleton SQLite at `data/system/remote_share.db`: `remote_invites`, `invite_services`, `remote_sessions`, `remote_share_audit_logs`, `share_settings`, `remote_invite_claim_tokens`, `share_tos_versions`. WAL mode, per-IP/per-email lockout.
- [backend/routers/share_auth.py](backend/routers/share_auth.py) (`/api/share/*`) — analyst-facing: `auth-config` (unauth — tells /share-login which login modes to render; the frontend fails OPEN to passcode on fetch error), `login`, `logout`, `acknowledge`, `heartbeat`, `claim/{token}`. Tagged so middleware lets them through the tunnel.
- **Analyst OAuth/OIDC login** (opt-in alternative to passcode): [backend/routers/share_oauth.py](backend/routers/share_oauth.py) — `GET /api/share/oauth/authorize` + `GET /api/share/oauth/callback` are TOP-LEVEL browser navigations, every outcome a 302 (never JSON); converges on the SAME `TunnelManager` session as passcode so RBAC/masking/TOS/boot are inherited. Provider registry in [backend/core/oauth/](backend/core/oauth/): gitignored `data/system/oauth_providers.json` (`OAUTH_PROVIDERS_CONFIG_PATH` override), creds via `OAUTH_<KEY>_CLIENT_ID/_CLIENT_SECRET`, inert unless `OAUTH_FLOW_STATE_SECRET` is set; `SHARE_PASSCODE_LOGIN_ENABLED=0` disables passcode for SSO-exclusive deployments (enforced at the endpoint, not just the UI); prod env passthrough in docker-compose.prod.yml. Invites carry `auth_method` / `oauth_provider` / `oauth_subject` (migration 004); optional JIT invite auto-provisioning for trusted org-restricted providers. E2E/dev mock IdP [backend/routers/mock_idp.py](backend/routers/mock_idp.py) mounts ONLY when `OAUTH_MOCK_IDP=1`.
- [backend/routers/share_admin.py](backend/routers/share_admin.py) (`/api/admin/share/*`, **blocked over tunnel**) — admin-facing: tunnel lifecycle, invite CRUD, session evict, panic/sever-all, backup export/import, GDPR erase, settings.
- Frontend: [share-dashboard components](frontend/components/share-dashboard/) (sharing control, invites, sessions, audit panels), [/share-login](frontend/app/share-login/) (TOS-gated), [useAnalystHeartbeat](frontend/hooks/useAnalystHeartbeat.ts), [useShareStatusBanner](frontend/hooks/useShareStatusBanner.tsx). Watermark mounts in `AppLayout` when `bootstrap.settings.is_remote_analyst === true`.

When adding an endpoint that analysts must reach over the tunnel, **register under `/api/share/*`** (auto-allowed) or update `_is_blocked_path()` — don't punch a hole somewhere obvious. (Trap #20.)

### Session-detail lookup ([backend/routers/sessions.py](backend/routers/sessions.py), [backend/core/session_token.py](backend/core/session_token.py))
The session list attaches an opaque AES-GCM `session_token` per row, sealing the real `(ip, ja4, start, end)` tuple (service-bound via AAD), minted BEFORE response masking runs. The detail endpoint unseals it, runs the exact-match lookup, then re-masks on the way out. **Never key session detail on the displayed `ip`** — for a masking analyst it's rewritten to `1.2.3.xxx` and will never match a stored IP (the "No results" bug). A masking analyst supplying a raw top-level `ip` (no token) is rejected; admin + non-masking analysts keep the raw-ip path. The key is a process-ephemeral default; the `SESSION_TOKEN_SECRET` env override gives restart-stable tokens. Zero/negative session windows (single-request sessions, a large share of traffic) are widened ~1s each side before the analyst time-clamp, else they trip `time_range_empty`.

## Provisioning

### UI Wizard ([frontend/components/ProvisionWizard/ProvisionWizard.tsx](frontend/components/ProvisionWizard/ProvisionWizard.tsx))
Step order: `mode → token → service → storage → ngwaf → fields → execute`. Token entered in step 2 must be threaded into every Fastly-credentialed API call (including the NGWAF fetch). `execute` streams SSE.

### CLI ([backend/provision/cli.py](backend/provision/cli.py))
- `python -m backend.provision.cli provision` — interactive wizard
- `python -m backend.provision.cli teardown --service-id {id}` — teardown
- `python -m backend.provision.cli invite-analyst --service-id {id}` — generate a read-only analyst invite
- `python -m backend.provision.cli enable-scoring --service-id {id}` — enable (or redeploy) session scoring on a service
- `python -m backend.provision.cli disable-scoring --service-id {id}` — disable session scoring on a service

Subcommands: `provision` / `teardown` / `invite-analyst` / `update-logs` / `update-cdn` / `enable-scoring` / `disable-scoring` / `list-groups` / `list-fields`.

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

Per-page SSR prefetch is now real and covers dashboard / origin / security / performance / insights (fetchers in [frontend/lib/ssr/](frontend/lib/ssr/), shared POST caller in `_transport.ts`, seeding in `seed.ts`). Adding another:

1. **Use `node:http.request`, NOT `fetch()`.** Node's `fetch()` always overrides the `Host` header from the URL. The backend's `_remote_host_allowed` gate rejects remote-classified requests whose Host isn't the public endpoint — so without preserved Host, the SSR fetch returns 400 host_not_allowed and silently falls through to the client.
2. **Trust topology is `X-Remote-Analyst: 1`, not `X-Proxied-By-Caddy`.** The SSR runtime hits the backend over loopback. `is_request_remote` ([backend/utils/remote_access.py](backend/utils/remote_access.py)) classifies based on `request.client.host` first, so a forwarded Caddy marker is IGNORED. `X-Remote-Analyst: 1` is the loopback-honored primitive (gated on `tunnel_manager.is_sharing_active()`). Forward it ONLY when the inbound request carries `X-Proxied-By-Caddy` — otherwise the admin SSH-tunnel path is mis-classified as analyst and 400'd. (See history: the 2026-06-11 SSR-leak incident reverted in `f3d8dd7` / `546c279` was the previous-attempt version that forwarded `X-Proxied-By-Caddy` directly. Backend ignored it, returned admin payload, dehydration leaked admin fields into public HTML.)
3. **Always wrap in try/catch + bounded timeout, return `null` on any failure.** SSR errors must NEVER propagate into a broken page — the layout falls back to client fetch when the helper returns null. 5s is generous for prod cron contention; never block SSR longer.
4. **`force-dynamic` is REQUIRED** in any layout/page that does a per-request SSR fetch via `cookies()` / `headers()` from an imported helper. Next.js's static-analysis pass only detects direct `cookies()` calls in the component file itself — calls from an imported module won't flip the route to dynamic. Without `export const dynamic = "force-dynamic"` the layout gets SSG'd at build time (when the backend isn't reachable) and the dehydrated state is permanently empty.
5. **Adversarial test required:** before deploying, hit the prod public URL anonymous AND the admin tunnel and verify the dehydrated state shape. Anonymous public must contain only the `needs_login` stub (NO `sharing_active`, NO `ngwaf_workspace_id`, NO `sync_status`). Admin must contain the full payload.
6. **The SSR seed key MUST byte-match the first-paint client key.** Re-key the page query on a server-reproducible `(rangeToken, anchor)` relative-range pair, NOT a client-`now()`-anchored `start/end` — otherwise the seed lands under a different cache key and the client refetches anyway (no cold-load win). Pin it with a `__tests__/ssr/<page>.test.ts` key-match + transport-trust test (see `dashboard.test.ts`). The anchor must ALSO make the identical snap-to-stale-extents decision as the client — both sides share [frontend/lib/log-extents-snap.ts](frontend/lib/log-extents-snap.ts) (extents >15 min stale snap the window to real data); a naive now()-anchored seed lands under a different key.
7. **Honor the `?service=` URL param over `bootstrap.active_service_id`.** A cold load of a non-default service's URL otherwise seeds — and paints — the wrong service's data. Diagnose via curl + grep of the dehydrated payload (e.g. `total_rows_total`), not browser timing.

The `serviceStore` Zustand slice hydrates from the SSR-cached bootstrap in `useBootstrap`'s post-mount `useEffect` — for the one-render window before that effect fires, use [`useEffectiveServiceId`](frontend/hooks/useIsDataReady.ts) which falls back to `bootstrap.active_service_id` from the React Query cache. Direct reads of `useServiceStore(s => s.activeServiceId)` flash "No service selected" on first paint.

### Canonical patterns (May 2026 DRY refactor — use these in new code)

1. **`response_model=` on every router handler.** Without it the OpenAPI emits `Record<string, unknown>`. Routes using `Depends(get_source)` should also lift `service_id: str` into the signature so it appears as a path parameter. Wire-safe recipe for dict-shaped endpoints: model with `extra="allow"` + `response_model_exclude_unset=True` + string (`.isoformat()`) timestamps, fields derived from the PRODUCER's actual payload (see [backend/models/common.py](backend/models/common.py)) — byte-identical wire output, now typed in the OpenAPI. SSE/file/binary routes carry an explicit exemption comment instead. The whole admin surface follows this as of 2026-07-06.
2. **`useActiveService` + `useTimeRange` + `useTimezone`** for page context (`activeServiceId`, `start/end`, `timezone`). Don't read `useServiceStore + useFilterStore + useTimezoneStore` directly; on first paint prefer `useEffectiveServiceId` (item 11) so the page doesn't flash "No service selected".
3. **`ReportLayout`** for analytics pages — bundles `useActiveService/useTimeRange/useTimezone + useReportConfig + useDebouncedFilterPayload + useViewMetricUrlSync + useServiceQuery + ChartIntervalButtons + ReportShell` (the URL-sync hook pair was renamed 2026-07-06: view/metric→URL is [useViewMetricUrlSync](frontend/hooks/useViewMetricUrlSync.ts), filters→URL is [useFilterUrlWriteback](frontend/hooks/useFilterUrlWriteback.ts)). Fall back to `ReportShell` only for multi-query or non-standard chrome pages.
4. **`HelpDialog`** from [components/ui/help-dialog.tsx](frontend/components/ui/help-dialog.tsx) — don't compose `Dialog + DialogHeader + DialogTitle` by hand for help content.
5. **`useBaseMap`** for any MapLibre setup. Don't duplicate the world-layer + theming inline.
6. **`metadata.record_audit(service_id, event_type=..., details=...)`** — direct (or via the `metadata_db` shim; both resolve to the same `metadata.audit` impl). The `duckdb.log_audit_event` shim and `repositories/audit.py` pass-through were removed.
7. **`date_utils.parse_iso_utc` / `iso_z` / `iso_z_now`** — don't hand-roll `datetime.fromisoformat(s.replace("Z", "+00:00"))`.
8. **`@cron_task` decorator** in [backend/cron/decorators.py](backend/cron/decorators.py) — handles `start_call_tracking`, `set_process_context`, `flush_usage_log` finally-block, watchdog hard-cap.
9. **`empty_schema_response(runner)`** in [_base.py](backend/repositories/_base.py) — return this when a repo function hits a service with no logs.
10. **`origin_latency_us_expr(actual_cols)`** in `_base.py` — don't hand-roll the `COALESCE("ottfb", "ttfb" * 1000000.0)` fragment.
11. **`useEffectiveServiceId`** in [hooks/useIsDataReady.ts](frontend/hooks/useIsDataReady.ts) — read this instead of `useServiceStore(s => s.activeServiceId)` whenever the answer matters on FIRST PAINT (gating views, building cache keys, "no service selected" branches). It falls back to `bootstrap.active_service_id` from the SSR-hydrated React Query cache so the page doesn't flash empty before the persisted Zustand store catches up.
12. **`analystFetch`** in [frontend/lib/analystFetch.ts](frontend/lib/analystFetch.ts) — shared analyst-facing fetch + response-envelope helper. Don't hand-roll the analyst fetch/error-unwrap per consumer.
13. **`CardErrorState`** — the shared inline card-error component (alert + Retry) for any dashboard/analytics card whose query can fail. Don't fabricate zeros or leave a spinner on a 5xx; render this.
14. **Shared PoP label** in [frontend/lib/pop.ts](frontend/lib/pop.ts) — render PoP codes as `DEN (Denver, CO - USA)` via the shared helper, seeded from `bootstrap.pop_geo`. Keep the raw code for click-to-filter; the label is display-only.

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
     `queryClient.prefetchQuery(...)`. Example: the Admin prefetch links in
     [AdminPrefetchLinks.tsx](frontend/app/admin/AdminPrefetchLinks.tsx)
     warm the share-status (and other destination) queries on hover so the
     destination renders real content immediately instead of
     skeleton-then-swap.

**8. Wrap `router.replace()` inside effects in `startTransition`.** A
synchronous `router.replace()` inside `useEffect` causes a render cascade
that blocks paint. Examples:
[useUrlServiceSync](frontend/hooks/useUrlServiceSync.ts),
[AppLayout redirect block](frontend/components/AppLayout.tsx). All
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
- `backend/core/duckdb.log_audit_event` shim (call `metadata.record_audit` directly; test patches must target `backend.core.metadata.audit.record_audit` — or `backend.core.metadata.record_audit` via the package re-export, which the `_ShimModule` proxy mirrors onto the live binding)
- `QueryRunner.safe_select` / `safe_select_list` (use `actual_cols` directly)

## Testing

**The Rule:** before committing, run `make ci`. It runs the full gate in parallel (`-j2`): backend pytest + frontend vitest + frontend typecheck (with OpenAPI type regen) + frontend ESLint ceiling (`lint-frontend`) + ruff check + ruff format check + mypy + import-contracts + VCL lint tests (`vcl-test`) + Rust scorer cargo tests (`scorer-test`) + frontend dep resolution (`verify-deps`) + secret scan + OSV scan + OTEL console-exporter guard (`otel-guard`). Add or update tests for every change; if a change is not testable in isolation, document why.

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

### E2E ([frontend/e2e/](frontend/e2e/), Playwright)

Cross-browser matrix: chromium + firefox + webkit, all blocking on PR. Mock backend booted by [frontend/e2e/global-setup.ts](frontend/e2e/global-setup.ts) under `FASTLY_MOCK_MODE=1`. Run locally with `cd frontend && npx playwright test --project=chromium` (or `--project=firefox` / `webkit`). Trace + screenshot auto-uploaded on failure.

SSE-stream specs MUST split on the multi-separator regex `/\r\n\r\n|\n\n|\r\r/` — `sse-starlette` emits `\r\n\r\n`, so the naive `\n\n` parser returns zero messages (see commit `0368868`). Mirrors the production `useServiceStream` regex.

### Visual regression (opt-in, [frontend/e2e/visual-regression.spec.ts](frontend/e2e/visual-regression.spec.ts))

Gated behind `RUN_VISUAL_REGRESSION=1` so default CI stays at ~20 cross-browser tests (including the `a11y-routes`, `a11y-admin-routes`, and `keyboard-navigation` specs). Baselines for chromium-darwin committed under `e2e/visual-regression.spec.ts-snapshots/`. To bootstrap a new platform:

```bash
RUN_VISUAL_REGRESSION=1 npx playwright test --project=chromium \
    e2e/visual-regression.spec.ts --update-snapshots
```

Snapshots embed `{browser}-{platform}` — a darwin baseline fails strict-pixel comparison on linux. CI baselines for linux need a one-time `--update-snapshots` run when the env var is flipped on in workflow.

### Hot-path micro-benchmarks ([tests/perf/test_benchmarks_micro.py](tests/perf/test_benchmarks_micro.py))

Per-call cost benches for HyperLogLog + SQL utility paths. Auto-disabled under `xdist` (assertions still run as smoke); for real numbers:

```bash
uv run pytest tests/perf/test_benchmarks_micro.py \
    -o 'addopts=-q' --benchmark-only
```

### Perf gate scales

Three tiers in [tests/perf/baseline.json](tests/perf/baseline.json), driven by `PERF_NUM_ROWS`:

- `smoke_100k` (default, PR-blocking) — `make ci` runs this on every push
- `mid_500k` (opt-in via `PERF_NUM_ROWS=500000`) — closes the 10× inflection gap; wire as a label-triggered PR job for query-shape-touching changes
- `nightly_1m` — cron-scheduled in [.github/workflows/perf-nightly.yml](.github/workflows/perf-nightly.yml)

Refresh after legitimate perf improvements:
```bash
PERF_NUM_ROWS=500000 uv run python scripts/emit_perf_latest.py
bash scripts/perf_gate.sh
# then update the relevant scenario in baseline.json with headroom
```

### Stateful + property tests

Hypothesis `RuleBasedStateMachine` pattern at [tests/core/test_ingest_stateful.py](tests/core/test_ingest_stateful.py) — first example in the repo. The `@initialize()` rule MUST `metadata_db.teardown(service_id)` because Hypothesis runs many instances per pytest function and the per-test SQLite file is shared across instances (otherwise: `FlakyStrategyDefinition`).

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
Alerts, views, audit, cron history, ingested-file dedup, ASN names, source registration, slow-query history → `data/services/{id}.metadata.db` (WAL); usage telemetry (`usage_log` + `usage_log_hourly_summary`) now lives in the separate `data/services/{id}.usage_log.db` file so the cron writer's lock can't block admin readers. Read/write via [backend/core/metadata/](backend/core/metadata/) (legacy `from backend.core import metadata as metadata_db` call sites resolve through the package's `_ShimModule` proxy) — never via DuckDB. JOINs against log data: ATTACH the SQLite read-only as `meta` via `attach_metadata_db()`, or pre-fetch and inline as a parameterised IN list (see `dashboard.py` ASN search). SQLite connections open in WAL mode with `synchronous=NORMAL`, which lets writers and readers proceed without blocking each other under contention.

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
`bundle_hours` / `bundle_days` use mtime to skip up-to-date bundles. The cron only re-bundles HOURS THAT JUST RECEIVED DATA. Closed historical hours never re-touch. So a new field added to the rollup writer (real or virtual) lands as a per-(field, hour) parquet but the bundled `all_fields.parquet` for closed hours stays without it — the dashboard's bundled-rollup reader returns 0 rows for the new field and the runtime fallback fires (defeats the perf win). Fix: delete the closed `all_fields.parquet` files and re-run the backfill (`backfill_missing_bundles` / `backfill_day_bundles` in [backend/core/rollups/](backend/core/rollups/), or the [`POST /api/admin/backfill-bundle-rollups`](backend/routers/admin/compaction.py) endpoint) so they get rewritten with the new field.

### 25. Virtual fields blow up the live-hour batch if not filtered out
`execute_top_n_rollups` in [_base.py](backend/repositories/_base.py) needs the active-hour merge to include real fields' new rows. The live-hour SQL projects `field_name AS value` and BinderExceptions on any name that's not a column on the live temp. Virtual fields like `waf_sig_ind` don't exist as real columns — passing them through silently kills the whole UNION ALL (the outer `except Exception: pass` swallows it) and drops the live-hour merge for REAL fields too. Always filter to `actual_cols` before the batch:
```python
live_fields = [f for f in fields if f in actual_cols]
if live_fields:
    live_res, _ = self.execute_top_n_batch(live_fields, tmp_name, ...)
```

### 26. Tombstoned buffer parquets must be excluded from DIRECT buffer reads
Post-commit, buffer parquets are not unlinked — `tombstone_buffer_files` ([backend/core/iceberg/buffer.py](backend/core/iceberg/buffer.py)) writes a `.parquet.consumed-<ts>` sidecar and the file lingers for a grace window so views bound BEFORE the commit stay readable. Its rows are ALREADY in the hourly partitions, so any code reading the buffer dir directly (not via `buffer_files()`, which filters) must skip `_tombstoned_parquet_paths(buffer_dir)` or it double-counts — the dashboard's direct active-hour read (`_create_active_hour_temp_direct` in [_base.py](backend/repositories/_base.py), the ~6 ms replacement for the ~700 ms view path on the live slice) double-counted up to ~10 min of the freshest rows before the fix (prod 2026-07-07: 40 of 55 buffer files were tombstoned). New consumers of the live slice should reuse the per-request shared temp via `begin_shared_active_hour_temps()` / `end_shared_active_hour_temps()` rather than rolling their own read.

### 27. System-managed custom fields must be re-asserted on every `log_fields` write
Session scoring and CMCD each inject a canonical set of `log_fields.custom_fields` generated from code. `_is_system_field` ([backend/core/field_registry.py](backend/core/field_registry.py)) hides them from the user-editable list, so **every writer that persists `log_fields` from a list it did not author omits them** — the log-fields UI round-trip, a `state_sync` pull of a remote `admin_state.json`, and a provisioning reconcile whose `log_fields` was built from groups alone.

The omission is not cosmetic: `reconcile_vcl_state` regenerates the Fastly log format from the persisted config, so a dropped field leaves its extraction VCL installed and running while nothing writes its output to the log line. The column then ingests **empty string, not NULL** (the field is absent from the format, so ingest fills the schema default), the feature's `available` probe still passes because the DuckDB column exists, and the page renders all zeros with no error to explain why.

Two incidents, same root cause:
- **2026-06-02** — `state_sync` overwrote scoring's 8 fields on every ~30 s metadata_sync tick.
- **2026-08-12** — the SE-demo service lost all 14 `cmcd_*` fields. CMCD had been enabled since 2026-07-13 and never collected a single value; the cache-key probe (`?CMCD=` vs a control param) proved the snippet was still stripping CMCD from `req.url` the whole time. The trigger was `update_logging_endpoint` assigning `cfg["log_fields"]` **wholesale** with no merge guard, unlike its `cli.py` / `api_service_log_fields_set` siblings.

Route every such write through `reconcile_system_custom_fields` / `reconcile_cfg_system_custom_fields` in [backend/provision/system_fields.py](backend/provision/system_fields.py), and key it on the **current** feature state, never on a state transition — a reconcile that says nothing about the feature must still re-assert its fields, and a disable must strip them. The transition-only guard in `update_service_config` is exactly what let the SE-demo config stay broken across a month of reconciles. **If you add a third system-managed feature, add its flag to `system_feature_flags` — do not add a fourth re-injection block.**

### 28. Never resolve the Iceberg metadata pointer from an unpaginated listing
`list_objects_v2` caps a single response at **1000 keys**, and pyiceberg names metadata files `<zero-padded-version>-<uuid>.metadata.json`. So the lexicographically-first page holds the **oldest** versions — and `sorted(metadata_files)[-1]` over that page resolves an ancient snapshot while presenting as "latest".

**2026-08 SE-demo incident.** The service's `metadata/` held 9,314 metadata.json objects. The truncated first page ended at `00952-…`; current was `08999-…`. `_read_metadata_pointer`'s discovery fallback resolved **v952**, the table committed forward from that stale base (reaching v1247), and every data file referenced only by v953…v8999 became unreachable — **41 days, 2026-07-01 → 2026-08-10, ~1.05 GB of July parquet alone**. Ingest kept reporting success the whole time because the buffer→commit path was healthy; only the *base* was wrong. The parquet was never deleted, just dereferenced, so it stayed fully recoverable.

The trigger was cheap: both pointer-key candidates in `_read_metadata_pointer` are wrapped in `except Exception: continue`, so **one transient CDN 5xx or timeout** on `metadata_location.txt` is enough to drop into discovery. Note the pointer is CDN-fronted with a 10 s TTL + `stale_while_revalidate` (see the iceberg-metadata snippet), so transient misses are normal operation, not an exotic failure.

Three independent defenses now exist in [backend/core/iceberg/_core.py](backend/core/iceberg/_core.py) — keep all three:
1. **Paginate** — `_list_metadata_json_keys` uses a paginator. Never call `list_objects_v2` directly for metadata discovery.
2. **Order by parsed version** — `_newest_metadata_key` / `metadata_version`, not a raw string sort (which is only accidentally correct while digit widths match).
3. **Refuse to regress** — both `_read_metadata_pointer` and `_refresh_local_catalog_metadata` reject a resolution older than the known-good location and log at ERROR. This one alone would have prevented the incident; it is the backstop, not the fix.

Recovery for an already-rolled-back table is [scripts/recover_orphaned_iceberg_metadata.py](scripts/recover_orphaned_iceberg_metadata.py) (dry-run by default). It reattaches the abandoned branch by moving the pointer to the newest version, and reports any data on the *current* branch that the recovered branch lacks — reattaching without handling that list trades one gap for another. **If you add another metadata-resolution path, paginate it and guard it, or this recurs silently.**

### 29. Feature extraction VCL must be emitted BEFORE field capture
Generated `vcl_recv` puts two related blocks in one snippet: **extraction** parses a source into `req.http.x-<feature>:*`, and **capture** promotes it into `req.http.x-fos-edge-data:<field>` — and only the promoted header is what the log format reads. Emit capture first and it copies empty strings; the extraction then runs too late to matter.

**2026-08 CMCD outage.** `generate_capture_vcl` appended the `cmcd_enabled` block *after* the `get_capture_vcl_statements` loop (the deployed snippet had capture at lines 186-199 and extraction at 209-225). Every `cmcd_*` column logged empty. It hid for a month because the failure is completely silent in all three places you'd look:

- Nothing errors — the VCL is valid and Fastly's `validate` passes.
- The DuckDB columns exist, so the feature's `available` probe returns true and /streaming renders zeros rather than "not enabled".
- **The extraction genuinely works.** It still runs `querystring.filter(req.url, "CMCD")`, so an edge-side cache-key probe (`?CMCD=A` vs `?CMCD=B` vs a control param) proves CMCD is being handled — and that proof is real but says nothing about whether the value reached the log line.

The invariant is pinned by [tests/utils/test_cmcd_capture_ordering.py](tests/utils/test_cmcd_capture_ordering.py) for all 14 fields, in BOTH generators (`fastly_api.generate_capture_vcl` and `declarative/generators.generate_consolidated_snippet`). **When you add a feature that extracts into a `req.http.x-*` header for logging, order it before capture and add it to that test.** To verify a capture-stage field end-to-end, don't infer from edge behaviour — send a uniquely-tagged request and query for the tag.

### 30. The log-line budget silently truncates late custom fields
`generate_log_format` tracks an aggregate `budget` (`FASTLY_LOG_LINE_DELIVER_MAX`) and clamps each variable-length field to what's left: `cf_limit = max(0, min(cf_limit, budget))`, allocated greedily in `custom_fields` list order. Once the budget runs dry, later fields get `substr(x, 0, 0)` and **log null forever** with no warning.

On the SE-demo service (groups A–M + 23 custom fields) this squeezed `cmcd_sid` to `substr(..., 0, 26)` — truncating 36-char UUID session ids to 26 chars and breaking session-level joins. Values logged before the squeeze are full length, so the same column holds both 26- and 40-char ids, which reads like a client quirk rather than a config effect.

**Do not "fix" this by adding `byte_limit` to individual fields without measuring.** `byte_limit` only ever *lowers* a cap — it cannot create budget — and shifting the allocation moved other fields to zero when tried. Check the real generated format first:
```python
generate_log_format(cfg)  # then grep for substr(..., 0, 0) and per-field caps
```
The genuine levers are reducing enabled groups/custom fields or raising the line budget, both of which trade against Fastly silently dropping over-long log lines.

### 31. Teardown: dereference before delete, and ENUMERATE what you remove
Teardown touches a service the customer still serves traffic from, so ordering is a correctness property, not a preference. **Every step that strips VCL from the customer's service must complete before any step that deletes a service that VCL points at.** Otherwise their active version routes to a deleted host and their site breaks — caused by our teardown. Order in `perform_teardown`: scoring VCL strip + Compute delete → logging endpoint + all owned VCL off the customer service → FOS keys → FOS bucket → analytics-owned CDN service last (the logging endpoint is what referenced it).

Two 2026-08-13 defects from a real teardown, both now pinned by [tests/utils/test_teardown_dereference_order.py](tests/utils/test_teardown_dereference_order.py):

- **Blind DELETE by hardcoded name = silent no-op.** `remove_logging_endpoint` DELETEd a hardcoded list of snippet names and swallowed every 404. The names on the service didn't match, so it logged *"removed 0 active snippets"* and reported success, leaving the entire capture VCL live on a "torn down" service. Endpoints, conditions and dictionaries were removed correctly because those three already enumerated-then-matched. The list was also missing `- vcl_pass`. **Always GET the list and delete what you can attribute; never guess names.** Attribution must be narrow: the `Fastly Log Analytics` prefix plus the canonical `scoring_snippet_names()` / `cmcd_snippet_names()` / RUM sets. Do NOT prefix-match `Session ` — a customer's own `Session Tracking - *` snippets sit beside our `Session Scoring - *` ones.
- **Best-effort dereference + unconditional delete.** `teardown_scoring_resources` treated the VCL strip as best-effort ("the operator cares most about not paying for an orphaned Compute service") and deleted the Compute service anyway. That trade is backwards: an orphaned Compute service is pennies and re-deletable, a dangling backend on live traffic is an incident — and Fastly 500s on these calls are real. It now aborts and leaves the Compute service in place.

Also: **never label a destructive log line with a name you didn't resolve from the thing being deleted.** The teardown passed the customer's service display name as `cdn_service_name`, so the log read ``Deleting CDN service '<customer-domain>'`` while actually deleting a different service id entirely — indistinguishable, to the operator watching, from destroying their production site. Log the id alongside every delete.

### 32. DuckLake inlines small commits — only `ducklake_flush_inlined_data` makes them durable
DuckLake does not write parquet for a small INSERT. It **inlines** the rows straight into the metadata catalog (Postgres, or the `.ducklake` file), visible as `changes: {'inlined_insert': [...]}` in `ducklake_snapshots`. `ducklake_table_info` then honestly reports `file_count = 0` for a table holding real committed rows.

**Neither compaction primitive promotes inlined rows.** Verified empirically on a throwaway catalog: `ducklake_merge_adjacent_files` (celery path) and `ducklake_rewrite_data_files` (default path, via `optimize_table`) both leave `file_count = 0` with zero parquet on disk, because both operate on already-materialized files. A table whose every commit was inlined stays inlined forever no matter how often compaction runs. `ducklake_flush_inlined_data` is the ONLY primitive that promotes them.

This was a live bug on this branch and it is a **durability** bug, not a layout preference: `finalize_committed_raw` deletes the raw `.gz` once its ledger row has been committed for `RAW_DELETE_GRACE_S`, so with no flush the only copy of every ingested row is the catalog itself. Observed on the live test service before the fix: 27,613 committed files, 27,615 catalog snapshots, FOS `ducklake/` prefix **empty**, and 4 raw files left in the bucket. Losing the Postgres volume would have been total data loss with nothing to re-ingest.

Both compaction paths now flush first — `merge_lake_files` (celery) and `_optimize_table_impl` (default). **If you add a third write path, it needs the flush too**, and setting `DATA_PATH` to FOS is not a substitute: DATA_PATH only says where parquet goes *once written*. Pinned by `test_merge_lake_files_flushes_inlined_rows_to_parquet` and `test_optimize_table_flushes_inlined_rows_to_parquet` — both assert the durability property (`file_count > 0` after a run that starts at 0), never just that the call happens.

Corollary for readers: any code inferring "is there data?" from file count alone is wrong under DuckLake. Count rows, or consult inlined state. This is what broke `get_table_info` (see [ADR-14](docs/adr/14-ducklake-replacement.md)).

### 33. A catalog swap fails SILENTLY on the read side
When the v3 DuckLake cutover moved the write path off pyiceberg, every reader still built on pyiceberg's Table API kept working — it just reported an empty, frozen catalog forever, with no error. `get_table_info` / `get_snapshot_calendar`, the post-commit `table_summary.json` writer, `lake_info.py`'s fallback, and `_run_cloud_maintenance_impl`'s retention deletion were all found in this state at different times, each discovered separately.

The retention one was the worst: it is the ONLY enforcement of `data_retention_days` / `rum_retention_days`, so customer data retention silently never ran against DuckLake data. It also concealed a latent wipe — the original entered its conditional-prune branch whenever `rum_retention_days > data_retention_days` *including when `data_retention_days == 0`*, which resolves the cutoff to *now* and deletes every non-RUM row. That never fired only because the whole function was dead; making it live without gating on `> 0` would have shipped a data wipe.

**When you migrate a storage backend, audit every caller of the OLD backend's read API before calling the cutover done — not just the write path.** A grep of the cron job file is not enough; read the function it calls. `optimize_{id}` and `expire_{id}` sit side by side, look identical from the scheduler, and only one of them had been migrated.

## AI Agent Directives

These apply to every change, regardless of scope.

### Testing

1. **Run `make ci` after every code change.** Fix all errors and warnings. Never report success without running CI.
2. **Add tests for every non-trivial change.** New endpoint → router test. New utility → unit test. Bug fix → regression test that would have caught it.
3. **Prefer integration tests over pure mocks** for backend behavior. The `in_memory_duckdb` + `client` fixture pattern tests real SQL while staying fast.
4. **Test error paths.** Missing config, external 4xx/5xx, empty DB.
5. **Frontend tests live in `frontend/__tests__/`** mirroring source structure (`app/`, `components/`, `hooks/`, `lib/`).
6. **Verify in the real app when you can.** Start the server, drive the UI, watch the logs (we log every query and FOS call). Don't rely on green tests alone for feature correctness.
7. **Run the Playwright suite as part of the dev-verify checklist.** Alongside the `verify-dev-first` flow (`./run.sh --dev` on 18002/13002), run `cd frontend && npx playwright test --project=chromium` for any change touching the admin shell, dashboard, provision wizard, custom-field drawer, or share-login. The suite spawns its own backend on 18004 + frontend on 13004 via [frontend/playwright.config.ts](frontend/playwright.config.ts) so it doesn't collide with the dev shell on 18002/13002. Use `--project=chromium,firefox,webkit` before pushing if the change touches browser-only interactions (DnD, popovers, chart hover).

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
    - `.env` (real env), `configs/*.json`, `data/system/` (real SSH key + share DB), `.scoring/` (per-deployment AES keys), `tests/fixtures/scoring/` (real prod traces). The `.gitleaks.toml` allowlist also covers these so a working-tree (`--no-git`) scan stays clean for ad-hoc local runs.

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
- **Modular package carves with re-export shims** for backward compat during refactor (the `metadata` package `_ShimModule` proxy + the `scheduler.py` re-export shim).
- **Named exception classes + explicit retry policies** (vs. generic `except Exception`).
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
