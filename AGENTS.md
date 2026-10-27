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
9. [Shared Helpers](#shared-helpers)
10. [Traps & Gotchas](#traps--gotchas)
11. [AI Agent Directives](#ai-agent-directives)
12. [Keeping This File Current](#keeping-this-file-current)

---

## Overview

FastAPI + Next.js dashboard for Fastly Real-Time VCL logs streamed to Fastly Object Storage (FOS). Continuously ingests `.gz` log files into DuckDB + DuckLake (formerly PyIceberg) using a Celery/Valkey architecture and exposes per-service analytics, alerts, NGWAF bot detection, custom log fields, and a live-share feature for read-only analyst access.

User-facing pitch + features list lives in [README.md](README.md). This file documents *how it works internally*.

**Stack:**
- Backend: FastAPI, DuckDB, DuckLake, PostgreSQL 16, Celery, Valkey, APScheduler, boto3 (S3-compatible FOS), `uv`, `ruff`, `mypy`, `pytest`
- Frontend: Next.js 16, React 19, TanStack Query v5, Zustand, shadcn/ui, openapi-fetch, vitest
- Storage: FOS (S3-compatible), per-service DuckDB (analytical querying), unified PostgreSQL 16 (operational metadata, state tracking, and DuckLake catalogs per ADR-22)
- Optional: [`falco`](https://github.com/ysugimoto/falco) VCL linter — detected via `shutil.which("falco")`, degrades gracefully to regex checks when absent

**VCL editing:** when you write or edit a log format string, a custom field `vcl_log_expression`, or any VCL snippet, it must pass `falco lint`. Use `+` for string concatenation; wrap literals in heredoc strings (`{"literal"}`). Call sites: [backend/utils/vcl_utils.py](backend/utils/vcl_utils.py), [backend/provision/fastly_api.py](backend/provision/fastly_api.py), [backend/routers/services/core.py](backend/routers/services/core.py).

## Architecture

### Data layout

| Layer | Location | Purpose |
|---|---|---|
| Raw logs | `s3://{bucket}/{prefix}/raw/request/**/*.gz` and `.../raw/rum/**/*.gz` | Immutable gzipped request and RUM JSON from Fastly; v3 services use these roots exclusively |
| Local buffer | `cache/{bucket}/` | Transient Parquet between ingest and commit |
| DuckLake table | Catalog: Postgres DSN (`DUCKLAKE_CATALOG`, required across all deployment modes — see [ADR-22](docs/adr/22-postgres-only-metadata.md)); data: local disk under `/app/data/services/{service_id}/ducklake/` | Long-term, transactional storage powered by DuckDB's DuckLake extension. All queried parquet data is stored locally on NVMe/SSD disk for instant analytical queries with 0 network latency. FOS / S3 serves as the edge ingress destination for raw `.gz` logs and as the durable cloud copy of committed data: in Standard, `backend/core/storage/backup.py` pushes the catalog-live Parquet files to `{prefix}/ducklake/<path under the data root>` after every commit (every `commit_interval_mins`), compaction and retention run, and `python -m backend.provision.cli restore-data --service-id <id>` rebuilds local data from it; in High-Scale the data lives there directly. Archive Parquet and synchronized admin state also live in FOS. |
| Admin state | `s3://{bucket}/{prefix}/meta/admin_state.json` | log_format_history, audit_logs, views, custom_fields (no alerts — alerts are per-instance). Moved from `iceberg/meta/` in v3.0.0-beta4 (no read fallback; re-provision). The scoring matrix and its history live beside it under `meta/`. |
| DuckDB | `data/services/{service_id}.duckdb` | Per-service analytical engine **only**: session-scoped `logs` view + temp tables |
| Metadata & State DB | PostgreSQL 16 (`METADATA_DSN`, mandatory across all modes — see [ADR-22](docs/adr/22-postgres-only-metadata.md)) | Unified relational store: `alerts`, `views`, `audit_logs`, `cron_runs`, `sources`, `ingested_files`, `asn_names`, `slow_queries`, `usage_log`, `usage_log_hourly_summary`, `remote_invites`, `remote_sessions`, `remote_share_audit_logs`, `share_settings`, `share_tos_versions`, `ngwaf_bots`, and `rdns` |
| Service configs | `configs/{logging_service_id}.json` | Credentials, settings, log_fields config |

The DuckDB `logs` view stitches the DuckLake table and the local Parquet buffer
so queries always see all data without callers caring which layer holds which
row. In the explicit `DEPLOYMENT_MODE=high_scale` topology, serving
connections use ClickHouse or ephemeral in-memory DuckDB instances reading
durable state; native service files and local buffers are not
opened by the serving path. Sync/file mode retains the local-buffer behavior.

### Package layout (post v2.0 carve-ups)

Several historical monoliths were split into cohesive packages with thin re-export shims at the old paths so existing imports keep working:

| Old path | New package | Shim status |
|---|---|---|
| `backend/core/iceberg/` (renamed `backend/core/storage/` in v3.0.0-beta4) | [`backend/core/storage/`](backend/core/storage/) (`_core.py`) | package `__init__.py` re-exports the DuckLake-backed public surface (`view.py`, `manifest.py`, `_ducklake.py`, `buffer.py`, and `core/local_compaction.py` folded into `_core.py` with `sys.modules` aliases; `fs.py` and `pyiceberg` were removed in v3.0) |
| `backend/core/metadata_db.py` | [`backend/core/metadata/`](backend/core/metadata/) (`base`, `ingest_log`, `usage_log`, `ingest_ledger`) | package `__init__.py` re-exports the full surface (`pg_connection`, `pg_schema`, `reconciliation`, `state`, `alerts`, and `state_sync` folded into `base`; `cron_log`, `usage_log_db`, and `slow_queries` folded into `usage_log`; `quarantine` folded into `ingest_log`, with `sys.modules` aliases) and installs a `_ShimModule` proxy so `monkeypatch.setattr(metadata, "_DATA_DIR", ...)` still flips the live binding inside `metadata.base` (legacy callers alias the package as `metadata_db`). No separate `metadata_db.py` file remains — the proxy lives on the package |
| `backend/core/share_db.py` | [`backend/core/share_db/__init__.py`](backend/core/share_db/__init__.py) | package `__init__.py` owns connection/schema/tos/settings/sessions/audit/invites/validation (`invites.py` and `validation.py` folded in with `sys.modules` aliases) and re-exports the historical public surface; passcode hashing is argon2id (legacy scrypt verify branch stays for transparent rehash-on-login) |
| `backend/utils/tunnel.py` | [`backend/utils/tunnel/`](backend/utils/tunnel/) (`__init__` [including `manager`], `session`, `rate_limiter`, `state`, `fingerprint`) | package `__init__.py` owns `TunnelManager` (`manager.py` folded in) and re-exports `get_tunnel_manager`, `AnalystSession`, etc. SSH-to-localhost.run code path (`_TUNNEL_URL_RE`, sleep listener, reconnect logic, `use_tunnel=True` branches) was deleted in v2.0 — only direct-mode (HTTPS public_endpoint) is supported. The `use_tunnel=True` kwarg still exists as a back-compat keyword that raises a clear error |
| `backend/scheduler.py` (removed 2026-07-06) | [`backend/cron/`](backend/cron/) (`scheduler.py`, `jobs/{sync,metadata}.py`) | `scheduler.py` consolidates `decorators.py` (`@cron_task`), `lifecycle.py` (`cron_run`, `record_skip_row`), and `cron_progress.py`; `jobs/sync.py` consolidates `_common` + `commit` + `rum_sync` + `rum_commit` + `rum_ledger`; and `jobs/metadata.py` consolidates `compaction` + `maintenance` (`optimize` + `expire`) + `duckdb_recycle` + `metric_snapshots` (plus `core/metric_snapshot.py`) + `insights_prewarmer` (with `sys.modules` aliases in `jobs/__init__.py` and `backend/__init__.py`) — import `get_scheduler`/`Scheduler`/`cron_task` from `backend.cron.scheduler` (or `backend.cron.decorators` / `backend.cron.lifecycle` aliases), and each `_run_*` body from its `backend.cron.jobs.*` module |
| `backend/routers/session_scoring.py` | [`backend/routers/session_scoring.py`](backend/routers/session_scoring.py) | `session_scoring_admin.py` (retrain + admin-config endpoints: enforce-threshold, exclude-regex, enforce-status-code, matrix-versions, rotate-key, audit, threshold GET/PUT, L2-enforce GET/PUT, evaluation/per-reason, dashboard composite) is consolidated into `session_scoring.py` (with `sys.modules` alias in `backend/__init__.py`) |
| `backend/routers/admin.py` | [`backend/routers/admin/`](backend/routers/admin/) (`compaction`, `health`, `celery_status`, `rum`) | `admin/__init__.py` re-exports the historical public surface (`router`, `compute_sync_status_cached`, `compute_log_accounting`, `LOG_ACCOUNTING_*`, `SustainedLossAlert`, `_QueueFile`, `_stream_from_worker`, `_fetch_file_to_zip`, `_resolve_source`, `ClientDisconnected`) and aliases folded submodules (`_router`/`_helpers`/`clickhouse`/`events`/`sync_status`/`system_metrics`/`log_accounting`/`backend.routers.admin_queries`/`backend.routers.admin_usage` → `health`, `ingest`/`quarantine`/`downloads`/`iceberg`/`bot_sources`/`high_scale`/`backend.routers.high_scale` → `compaction`; top-level `backend/celery_status.py` folded into `backend/celery_app.py`) |
| `backend/core/rollups.py` | [`backend/core/rollups/`](backend/core/rollups/) (`_common`, `day_bundles`, `recompute`, `spec`, plus the domain rollup writers `origin_dims`, `security_dims`) | `rollups/__init__.py` re-exports the rollup surface (`overview`/`sessions`/`time_series`/`rum`/`repositories.rollup_coverage` folded into `recompute`, `hour_bundles` and `partial_hour` folded into `day_bundles`, `slow_urls`/`origin_latency_ts`/`perf_latency`/`network` folded into `origin_dims`, `ngwaf_bots`, `wellknown_bots`, and `utils/ngwaf` folded into `security_dims`, with `sys.modules` aliases) so `from backend.core.rollups import X` (or `from backend.core import rollups; rollups.X`) keeps working unchanged. Shared bits — constants, ident validators, path helpers, query builders, `_VIRTUAL_FIELD_BACKING`, and the shared per-hour bundle writer `build_per_hour_bundles` (writer-side mirror of `compact_closed_days`) — live in `_common.py` |
| `backend/core/log_fields.py` | [`backend/core/log_fields.py`](backend/core/log_fields.py) | `LOG_FIELD_CATALOG`, `GROUP_INFO`, `GROUP_DEPENDENCIES`, `PRESETS`, `INSIGHT_DEFINITIONS` (`_log_fields_data.py`, `core/field_registry.py`, and `utils/vcl_utils.py` folded in with `sys.modules` aliases) and VCL log format generation live in `log_fields.py` |
| `backend/core/duckdb.py` | [`backend/core/duckdb.py`](backend/core/duckdb.py) | Analytical DuckDB connection management plus `utils/sql_validator.py`, `core/reset.py`, and `_duckdb_status.py` (`get_sync_status`, `refresh_config_status`, `update_top_values`, `get_ingested_files`, `delete_ingested_files`, `get_schema`, `_clear_schema_cache`, `get_asn_names` / `format_asn_label` / `enrich_asn_labels`, `update_cron_duration`, `log_usage_calls`, `backfill_fastly_edge_writes`, `reconcile_fastly_stats`, `purge_usage_log`, folded in with `sys.modules` aliases) |

Other new modules introduced by the cleanup:

- [`backend/repositories/_sql/`](backend/repositories/_sql/) — Pure re-export shim package exposing parameterized SQL templates co-located in their canonical domain repository modules (`network.py`, `origin.py` [including `performance` and `cmcd`], `rum.py`, `security.py`, `dashboard.py`, `sessions.py` [including `session_scoring`], `assets.py`, `insights/definitions.py`, etc.). Repository functions keep their names and signatures.
- [`backend/core/log_fields.py`](backend/core/log_fields.py) (`backend.core.field_registry` alias) — Phase 7 (shipped, including step 13) typed registry that owns per-field declarations (code, display name, type, valid aggregations, valid filter ops, derivations, security-regex hooks) as well as metric SQL expressions (`origin_latency_us`, `sum_or_zero`, `column_or_default`, `coalesce`, `HIT_STATES`; aliased as `backend.core.metric_expressions`) and VCL helpers (`backend.utils.vcl_utils` alias). All readers migrated (dashboard CTE generator, rollup spec builder, top_n logic, SQL validator, scoring matrix labels, plus 8 step-13 callers: `services/__init__.py`, `provision/orchestrator.py`, `provision/fastly_api.py`, `provision/cli.py`, `storage/_core.py`, `ingest.py`, `models/custom_fields.py`, `core/metadata/state.py`). Same-identity re-exports of every helper + constant preserve `from log_fields import X` callers.
- [`backend/core/request_context.py`](backend/core/request_context.py) — Phase 2 single FastAPI dependency (`backend.deps` folded in with `sys.modules` alias) that bundles `service_id`, `source`, `con`, `telemetry`, `analyst_session`, `cached_temps`, plus the opaque AES-GCM session-detail token helpers (`seal_session_token`, `unseal_session_token`, aliased as `backend.core.session_token`). Replaces the v1 `AnalyticsDeps` bundle (deleted at the v2.0 cut — Phase 8.1/8.2) and folds `require_service_access` into context construction (there is no path that builds a context without enforcing tenancy). 23 analytics endpoints across 8 routers (dashboard / query / sessions / security / network / origin / performance / insights) now take `ctx: RequestContext = Depends(build_request_context)` directly.
- [`backend/core/request_telemetry.py`](backend/core/request_telemetry.py) — Phase 1 thin wrapper around the OTel tracer that owns section spans, `QueryRegistry` (`backend.core.query_registry` alias), query instrumentation (`backend.core.query_instrumentation` alias), query attribution (`backend.core.query_attribution` alias), durable request metrics (`backend.core.request_metrics` / `backend.request_metrics_observer` aliases), `TelemetryResponseBodyMiddleware` (`backend.utils.telemetry_response_middleware` alias), call log, cache state, High-Scale task scopes (aliased as `backend.core.high_scale_sections`), and the `app.thread_wait_ms` custom metric instrumented at `_Pool.acquire`. Lives on `RequestContext`.
- [`backend/core/metadata/base.py`](backend/core/metadata/base.py) (`backend.core.metadata.pg_connection` alias) — PostgreSQL 16 connection pooling (`psycopg_pool.ConnectionPool`), thread-local connection reuse, autocommit statement execution, and instrumented profiling via `telemetry.record_statement` (aliased as `sqlite_profiler.record_statement`; see [ADR-22](docs/adr/22-postgres-only-metadata.md)).
- **Env / config handling** — there is no central settings class. App-level env vars are read via `os.getenv` at their use sites (e.g. `OTEL_EXPORTER` in [request_telemetry.py](backend/core/request_telemetry.py), `STRUCTLOG_FORMAT` in [telemetry.py](backend/utils/telemetry.py), pool tuning in [duckdb_pool.py](backend/core/duckdb_pool.py)); per-service credentials/settings live in `configs/{id}.json` loaded by [backend/config.py](backend/config.py). `DEPLOYMENT_MODE=standard` preserves synchronous ingest and file-backed serving; `DEPLOYMENT_MODE=high_scale` requires ClickHouse, Postgres, and durable serving. Both require `METADATA_DSN` and `DUCKLAKE_CATALOG`.
- [`backend/core/storage/_core.py`](backend/core/storage/_core.py) `execute_with_stale_view_retry(con, src, fn)` — self-heal wrapper for code paths that open raw DuckDB connections instead of going through `QueryRunner`. On stale-buffer "No files found" errors, busts `_view_cache` via `clear_source_caches(keep_snapshot_cache=True)` + `update_storage_view(force=True)` then retries `fn` once. Used by `rdns_cache` discovery, `rollups` DESCRIBE sites, and `/api/query`. Pre-fix prod incidents: ~8h of 100%-failing rdns runs + analyst-visible query errors on the same buffer-deletion race.
- **Consolidated domain modules (Rounds 25–78)** — `<1,100`-line leaf modules are consolidated into cohesive domain homes with `_AliasLoader` and `sys.modules` shims in [`backend/__init__.py`](backend/__init__.py): `scoring/{features,labels,fixtures,matrix,evaluate}.py` → [`backend/scoring/__init__.py`](backend/scoring/__init__.py); `core/field_registry.py`, `utils/vcl_utils.py`, and `utils/vcl_validator.py` → [`backend/core/log_fields.py`](backend/core/log_fields.py); `utils/sql_validator.py` and `core/reset.py` → [`backend/core/duckdb.py`](backend/core/duckdb.py); `core/query_registry.py`, `utils/telemetry_response_middleware.py`, and `core/query_instrumentation.py` → [`backend/core/request_telemetry.py`](backend/core/request_telemetry.py); `utils/rdns_cache.py` → [`backend/utils/bot_sources.py`](backend/utils/bot_sources.py); `utils/hll.py`, `utils/time_window.py`, `utils/structlog_config.py`, `utils/bounded_cache.py`, `utils/http.py`, `core/retry.py`, `utils/date_utils.py`, and `utils/telemetry_proxy.py` → [`backend/utils/telemetry.py`](backend/utils/telemetry.py); `utils/countries.py` and `utils/remote_access.py` → [`backend/utils/router_utils.py`](backend/utils/router_utils.py); `deps.py` → [`backend/core/request_context.py`](backend/core/request_context.py); `core/realtime/transform.py` → [`backend/core/realtime/__init__.py`](backend/core/realtime/__init__.py); `utils/tunnel/manager.py` → [`backend/utils/tunnel/__init__.py`](backend/utils/tunnel/__init__.py); `core/oauth/{registry,client}.py` → [`backend/core/oauth/__init__.py`](backend/core/oauth/__init__.py); `routers/{share_auth,share_oauth}.py` → [`backend/routers/share_admin.py`](backend/routers/share_admin.py); `routers/{alerts,usage}.py` → [`backend/routers/control_room.py`](backend/routers/control_room.py); `routers/services/core.py` and `routers/{provision,bootstrap}.py` → [`backend/routers/services/__init__.py`](backend/routers/services/__init__.py); `state_sync.py` and `core/metadata/state.py` → [`backend/core/metadata/base.py`](backend/core/metadata/base.py); `core/metadata/cron_log.py` → [`backend/core/metadata/usage_log.py`](backend/core/metadata/usage_log.py); `high_scale/{orchestration,rum,rum_recovery,_common,aggregate_batch_adapter,registry_bootstrap,assets,worker,recovery}.py` → [`backend/high_scale/ingest_controller.py`](backend/high_scale/ingest_controller.py); `high_scale/{console_access,deletion}.py` → [`backend/high_scale/postgres_control.py`](backend/high_scale/postgres_control.py); `high_scale/{dashboard,origin,security,decoder,query,insights,insight_queries,network,sessions,aggregate_query,performance,cmcd}.py` → [`backend/high_scale/query_service.py`](backend/high_scale/query_service.py); `high_scale/{historical_jobs,publication,archive_publication,archive_models}.py` → [`backend/high_scale/archive.py`](backend/high_scale/archive.py); `core/faro_versions.py` and `core/rum_ingest.py` → [`backend/core/ingest.py`](backend/core/ingest.py); `core/{clickhouse_client,clickhouse_manifest,clickhouse_publication}.py` → [`backend/core/clickhouse.py`](backend/core/clickhouse.py); `core/storage/{view,_ducklake,buffer}.py` and `core/local_compaction.py` → [`backend/core/storage/_core.py`](backend/core/storage/_core.py); `models/{insights,admin_actions,origin,network,performance,session_scoring,dashboard}.py` → [`backend/models/analytics.py`](backend/models/analytics.py); `models/share_admin.py` → [`backend/models/admin.py`](backend/models/admin.py); `models/{services,provision}.py` → [`backend/models/common.py`](backend/models/common.py); `routers/{query,performance,origin,network,rum}.py` → [`backend/routers/dashboard.py`](backend/routers/dashboard.py); `provision/utils.py` → [`backend/provision/cli.py`](backend/provision/cli.py); `provision/declarative/{iac_reconciler,state,fastly_integration,generators}.py` → [`backend/provision/declarative/reconciler.py`](backend/provision/declarative/reconciler.py); `provision/{cmcd_fields,fos_setup,rum_orchestrator_v2,rum_assets}.py` and `utils/terraform_gen.py` → [`backend/provision/orchestrator.py`](backend/provision/orchestrator.py); `provision/{session_scoring_vcl,session_scoring_setup}.py` → [`backend/provision/session_scoring_orchestrator.py`](backend/provision/session_scoring_orchestrator.py); `core/fastly/{rum_provisioning,utils}.py` → [`backend/core/fastly/client.py`](backend/core/fastly/client.py); `repositories/{usage,alerts}.py` → [`backend/repositories/assets.py`](backend/repositories/assets.py); `repositories/query.py` → [`backend/repositories/dashboard.py`](backend/repositories/dashboard.py); `repositories/{performance,cmcd,rum}.py` → [`backend/repositories/origin.py`](backend/repositories/origin.py); `repositories/session_scoring.py` → [`backend/repositories/sessions.py`](backend/repositories/sessions.py); `repositories/rollup_coverage.py` → [`backend/core/rollups/recompute.py`](backend/core/rollups/recompute.py); `repositories/top_n_rollups.py` and `repositories/utils/{response_cache,filters}.py` → [`backend/repositories/_base.py`](backend/repositories/_base.py).

### Personas

- **Admin** — full ingest/management surface. Config: `configs/{logging_service_id}.json`.
- **Analyst — live shared instance** (remote sharing; direct-mode against an HTTPS public_endpoint; the SSH-tunnel-to-localhost.run option was deleted in v2.0). No FOS credentials, uses the admin's running process. Detected on the frontend by `bootstrap.settings.is_remote_analyst`. See [Live Dashboard Sharing](#live-dashboard-sharing) below for components.

The v2 "independent instance" analyst (Path A: read-only FOS credentials, `generate-viewer-key`, the wizard "join" mode, the `metadata_sync` cron job, the read-only `access_level` service setting) was removed in v3: a DuckLake catalog is not bucket-resident, so an analyst holding only bucket credentials cannot reconstruct the lake. It stays in v2 only; do not reintroduce it.

## Ingest Pipeline

Two data planes are selected by `DEPLOYMENT_MODE`: `standard` uses synchronous DuckDB + DuckLake ingest, while `high_scale` uses ClickHouse ingest and micro-batching. Both provide analytical querying over logs; see [docs/runbooks/high-scale-capacity-and-recovery.md](docs/runbooks/high-scale-capacity-and-recovery.md) for the high-scale design.

**Default (`sync`) mode** — APScheduler runs these per-service (plus per-service `alerts` evaluation + `insights_prewarmer`, and process-global maintenance jobs — see the [Scheduler](#scheduler-backendcron) note). Job names were renamed from `sync_{id}`/`commit_{id}` to the pair below during the v3.0.0-beta1 rework — grep history for the old names if you're reading pre-v3 code or logs:

| Job | Schedule | Function |
|---|---|---|
| `log_discovery_{id}` | every `log_period` sec | LIST FOS raw/, download new `.gz`, transform to Parquet, update DuckDB view, flush usage log, run cleanup |
| `commit_{id}` | every `commit_interval_mins` (default 5) | Commit local Parquet buffer → DuckLake table, flush usage log, then (Standard) push the new data files to FOS on a single-flight background thread (`schedule_cloud_push`; `optimize`, `expire` and the RUM commit trigger it too) |
| `local_compact_{id}` | every 2 min | Compact local-only hourly/daily Parquet files, flush usage log |
| `rollup_heal_{id}` | hourly at :05 | Re-run the idempotent `backfill_missing_hour_bundles` (48-hour lookback, `lookback_days = 2`) so closed hours the per-sync recompute missed get their top-N rollups within ~1 h; local-only writes |
| `rollup_compact_{id}` | daily 02:00 UTC | Consolidate closed-day per-hour rollup parquet into per-day files (30-day deep pass); local-only writes |
| `optimize_{id}` | daily 04:00 UTC | DuckLake-native (`db_storage.optimize_table` → `_optimize_table_impl`): `CALL ducklake_flush_inlined_data('lake')` **then** multi-table bin-packing `CALL ducklake_merge_adjacent_files('lake', '{tbl}')` and compaction `CALL ducklake_rewrite_data_files('lake', '{tbl}')` across all lake tables (`logs`, `client_vitals`, `client_errors`). The flush is a DURABILITY step, not an optimization — DuckLake inlines small commits into the metadata catalog, and neither `ducklake_rewrite_data_files` nor `ducklake_merge_adjacent_files` promotes inlined rows (both only touch already-materialized files), so without it a table stays at `file_count = 0` forever and the catalog DB holds the only copy of the data (the raw `.gz` is deleted after ingest). Gated by `FLA_DEV_NO_CRONS=1` and `should_defer_cron("optimize")`. S3 calls recorded in `usage_log` under `process_context="cron.optimize"`. Manual trigger via `POST /api/admin/optimize/{service_id}`. |
| `expire_{id}` | Hourly (default `expire_interval_mins=60`) | DuckLake-native (`run_cloud_maintenance`): (1) independently gate request/RUM retention deletes on `data_retention_days > 0` / `rum_retention_days > 0` (`0` keeps forever); (2) attempt snapshot expiry and queued-file cleanup independently and unconditionally, including empty catalogs; (3) purge old local cache Parquet plus orphaned temp files older than 2h; (4) purge old local rollup Parquet; (5) preserve quarantine evidence without age-based eviction. Each failure produces a `*_error` result and a `warning` run while later steps proceed. `start_cron_run` holds the atomic per-service/per-task `expire_snapshots` lease in PostgreSQL `job_runs`; the run outcome is recorded in `cron_runs`. This is not a lock shared by all cron tasks. High-Scale keeps the job on the single serving pod, never a Celery worker. The decorator tags query/FOS telemetry with `process_context="cron:expire_snapshots"`; the cron row stores exact deletion counts in `summary` and FOS unlinks in `files_deleted_fos`. A `finally` block terminally finalizes the lease and duration even if progress cleanup fails. Manual trigger: `POST /api/admin/expire-snapshots/{service_id}`. `FLA_DEV_NO_CRONS=1` prevents registration and execution. |
| `full_sync_{id}` | every 6h (:30 UTC) | Periodic full bucket sweep: exhaustive S3 LIST on FOS raw/ logs prefix to reconcile missing logs with PostgreSQL `ingested_files` (ADR-22). Attributes Class A LIST ops to `usage_log` as `cron.full_sync`; transitions to `error` on corrupt rows with `outcome_counters` recorded in `cron_runs`. Manual trigger: `POST /api/admin/full-sweep/{service_id}`. |
| `gap_heal_{id}` | every 30m (configurable) | Periodic gap detector that evaluates Fastly log accounting metrics against actual ingested events (24h lookback). If sustained loss is observed (≥2 consecutive completed hourly buckets with ≥5% deficit), triggers a targeted full sweep (`_run_full_sweep`) with adaptive severity bands (`mild`, `elevated`, `severe`, `critical`) and expanded sweep budgets. Manual trigger: `POST /api/admin/gap-heal/{service_id}`. |
| `rum_sync_{id}` | every `log_period` sec | Standard mode RUM beacon ingest: downloads raw beacons, parses, buffers locally, and immediately triggers `rum_commit_{id}` when new rows arrive (`total > 0`) for live recency parity with request logs. |
| `rum_commit_{id}` | post-sync immediate + 1m fallback | Drains `client_vitals` and `client_errors` buffers into DuckLake, recomputes aggregates, persists RUM extents, and emits `sync_status` SSE. |
| `metadata_cleanup_{id}` | daily 03:15 UTC (configurable) | PostgreSQL operational metadata retention pruning: prunes expired `ingested_files` (guarded by `delete_after` retention and RUM safety window `max(ingested_files_days, log_retention_days + 1)`), chunked 5,000-row purge of detailed `usage_log` while preserving `usage_log_hourly_summary`, `cron_runs` (7d), `slow_queries` (30d), and global `metric_snapshots` (30d). Zero FOS calls contract. Manual trigger: `POST /api/admin/metadata-cleanup/{service_id}` or `POST /api/admin/metadata-cleanup`. |
| `alerts_evaluation_{id}` | every `log_period` sec (configurable) | Evaluates user-configured alert threshold rules against recent log windows in DuckDB/DuckLake; dispatches webhook/channel notifications (Slack, PagerDuty, generic webhooks). Gated on `_service_has_alerts(service_id)` and `should_defer_cron("alerts")`. |

**`DEPLOYMENT_MODE=high_scale` mode** — ingest is handled by dedicated `high-scale-worker` micro-batching directly to ClickHouse and FOS archive Parquet for K8s clusters, backed by partitioned cursors and multi-source ArchiveManifests. ClickHouse serves as the high-throughput analytical engine. Requires ClickHouse cluster and PostgreSQL metadata. **Ingest scales horizontally across worker replicas; the backend serving tier is single-pod.**

RUM worker discovery keeps `high_scale_source_cursors.source_cursor` as the existing forward position and adds per-domain `reinspection_cursor` plus `forward_pages_since_reinspection` state. Existing schemas acquire the two columns additively with `ADD COLUMN IF NOT EXISTS`. Every ten successful forward pages, the next RUM listing inspects one bounded page through the same ledger/claim/archive path; multi-page reinspection resumes with `StartAfter` at the last key and does not rewind the forward cursor. This provides eventual late-arrival reconciliation, **not a 20-second guarantee** (a large prefix takes multiple ticks). Request discovery is unchanged. See [High-Scale Capacity and Recovery](docs/runbooks/high-scale-capacity-and-recovery.md#late-arriving-rum-source-objects) before changing cursor handling or reconciling missed objects.

Teardown removes jobs on the next `_sync_jobs()` reload. The `config not found, skipping` warning during teardown is normal — a job fired after the config was deleted; harmless.

### Local-Only Parquet Compaction (Dashboard Performance)

To maintain top-tier dashboard querying speeds over long periods without generating massive FOS write costs or massive file bottlenecks, we employ sequential size-capped bin-packing local compaction (implemented in `backend/core/local_compaction.py`):
1. **Periodic Job (`local_compact_{id}`):** Runs every 2 minutes. It scans local cache directories, identifies any hourly partitions containing multiple small files, and merges them sequentially into size-capped compacted Parquet files (default <= 256MB) to maintain DuckDB query parallelism.
2. **Compact-on-Sync Thread:** Triggered immediately after a raw sync completes. If multiple new files are detected, a background thread merges them immediately.
3. **Daily & Weekly Tier Rollup:** Partitions older than 1 day (customizable via `LOCAL_COMPACT_DAILY_TIER_DAYS`) are sequentially bin-packed by day into daily files (e.g. `daily_YYYY-MM-DD_<uuid>.parquet`), with single-file bins correctly migrated to retire empty hourly dirs. Daily files older than 30 days are further bin-packed into weekly files (e.g. `weekly_YYYY-WXX_<uuid>.parquet`) under `weekly/`. All files are capped at `_MAX_PARTITION_BYTES` to prevent huge file bottlenecks and preserve maximum parallelism.

*Note: Use `local_compaction` for hot-tier ongoing dashboard performance. Use the global `optimize_{id}` / `optimize_table` path when you want compaction reflected in FOS too.*

### Atomic ingest (in-flight manifest + deterministic buffer names)

The window between `storage.write_to_buffer` and `metadata_db.insert_ingested_files` is crash-protected by a per-service `ingest_in_flight` table:

1. `metadata_db.record_in_flight(source_name, buf_filename, file_rows)` persists `(file_name, row_count, file_size)` tuples **before** the Parquet is written.
2. `storage.write_to_buffer(src, arrow_table, buf_filename)` writes Parquet. Filename is `batch_{sha256(sorted_chunk)[:16]}.parquet` — deterministic so a re-ingest of the same chunk overwrites the same file instead of stacking duplicates.
3. `metadata_db.insert_ingested_files(...)` commits the row_count/size tuples.
4. `metadata_db.clear_in_flight(...)` drops the row.

`_recover_in_flight(src)` runs first on every `ingest()` and reconciles any leftover rows: buffer present → promote to `ingested_files` + clear; buffer missing → drop without touching `ingested_files` (next LIST tick re-ingests cleanly). Both halves are idempotent.

**Known intentional limitation:** if ingest crashes mid-LIST, the next tick re-LISTs from the same `StartAfter` marker. Wastes ≤10 Class A ops per crash but is correct — every file is dedup'd before download.

### Health probe

`GET /api/health` is cheap liveness. `GET /api/health?deep=1` also verifies per-service ingest freshness: reads `max(ingested_at) FROM ingested_files` and the latest terminal `log_discovery` cron run per service (queries match both `'log_discovery'` and the pre-v3.0.0-beta1 `'sync'` name — `cron_runs` history predates the rename); returns 503 when any service is `degraded` (last ingest older than `stale_minutes` — default 30, but SRE-22 widens it per-service to that service's own historical p95 gap between non-empty ingests before degrading, so a low-traffic service's organic quiet periods don't false-positive — or last discovery errored, or a discovery row is stuck in `status='running'` past `_STUCK_SYNC_RUNNING_MINS` — the orphaned-sync-row condition, name unchanged since it's an internal constant, not a job name — or the latest `commit` cron errored). Celery mode additionally derives freshness straight from `ingest_ledger` (`max(committed_at)`, oldest non-committed row age, worker count vs. queue depth) rather than relying on `cron_runs` timing alone — see [ADR-16](docs/adr/16-ingest-ledger.md). Postgres-only, never FOS or Fastly. Safe to wire into a load balancer.

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
Single `BackgroundScheduler` owned by [backend/cron/scheduler.py](backend/cron/scheduler.py). `_sync_jobs()` adds/removes per-service jobs on `reload()`. The `@cron_task` decorator (telemetry context + usage-log flush + watchdog hard-cap) also lives in [backend/cron/scheduler.py](backend/cron/scheduler.py) (aliased as `backend.cron.decorators`). Per-job bodies live under [backend/cron/jobs/](backend/cron/jobs/) (`sync`, `rum_sync`, `compaction`, `maintenance`, `metadata`, `insights_prewarmer`; `sync` consolidates `commit`, `rum_sync` consolidates `rum_commit` + `rum_ledger`, `maintenance` consolidates `optimize` + `expire`, and `metadata` also houses the process-global `duckdb_recycle` and `metric_snapshot` jobs). `duckdb_recycle` (bounds the DuckDB object-cache leak) and `metric_snapshot` (SRE sampler feeding `metric_history`) register once for the process, not per service. Per-run progress events tracked in [backend/cron_progress.py](backend/cron_progress.py) and streamed via SSE. Every job body owns its per-run lease/progress/terminal-row cycle through `cron_run` ([backend/cron/lifecycle.py](backend/cron/lifecycle.py)) — see [Shared Helpers](#shared-helpers). (The flat `backend/scheduler.py` compat shim was retired 2026-07-06; import from the `backend.cron.*` homes directly.)

### NGWAF Bot Detection ([backend/utils/ngwaf.py](backend/utils/ngwaf.py), [backend/utils/ngwaf_bot_cache.py](backend/utils/ngwaf_bot_cache.py))
Syncs VERIFIED-BOT requests from `GET https://api.fastly.com/ngwaf/v1/workspaces/{id}/requests`. JSON:API pagination via `meta.next_cursor`. Stored in PostgreSQL `ngwaf_bots` table (ADR-22). Enriches log rows with `waf_req_id` + `waf_sig LIKE '%VERIFIED-BOT%'`.

NGWAF workspace listing (`GET /api/provision/ngwaf-workspaces`): response key is `"data"`. **Don't `or`-chain** with `data.get("workspaces", [])` — an empty list is falsy and falls through. Use `if "data" in data` explicitly. (See Trap #3.)

### Alerts / Saved Views ([backend/routers/alerts.py](backend/routers/alerts.py), [backend/routers/views.py](backend/routers/views.py))
Both stored in PostgreSQL metadata schemas (`alerts`, `views`). Alerts are threshold-based with webhook fire. Views capture filter set + time range.

### Insights ([backend/repositories/insights/](backend/repositories/insights/), `INSIGHT_DEFINITIONS` in [backend/core/_log_fields_data.py](backend/core/_log_fields_data.py))
Availability is declared in `INSIGHT_DEFINITIONS` (`required_fields` + `required_groups`); the Insights page renders each card generically. Every definition MUST declare a `category` ([backend/repositories/insights/registry.py](backend/repositories/insights/registry.py) `InsightCategory`: security/origin/edge/network/traffic — a registration without one fails Pydantic at import time); the page tabs by that key with labels/icons/triage order in [frontend/lib/insight-sections.ts](frontend/lib/insight-sections.ts), so a new insight in an EXISTING category needs no frontend change, but a new category needs a section entry there. Set `required_groups=[]` when the gate should be only core fields — declaring a group grays the card out in LogSettings / ProvisionWizard previews even if it would run (e.g. `repeated_patterns` needs only client IP + timestamp). Definitions, row processors, severity logic, and SQL templates live in [backend/repositories/insights/definitions.py](backend/repositories/insights/definitions.py) (re-aliased via `backend/repositories/_sql/insights.py`). The repository's `sql.count("?")` placeholder heuristic means any regex literal in a template must be `?`-free (no `(?i)` — pass the `'i'` flag to `regexp_matches` instead). The default window/baseline adapts to the available history (a young service compares the last hour against the previous hour instead of showing "not enough data"). Analyst insight results are warmed via a stable invite-keyed cache + the `insights_prewarmer` cron. PARITY CONTRACT: the prewarmer must warm EXACTLY the adaptive default (window, baseline) pair the page will pick — [backend/utils/insights_defaults.py](backend/utils/insights_defaults.py) mirrors `pickInsightsDefault` in [frontend/lib/insights-defaults.ts](frontend/lib/insights-defaults.ts) band-for-band; the `/api/insights` cache key folds in both hour values, so warming any other pair is a guaranteed cold compute (~20 s on a long-history service). `tests/utils/test_insights_defaults.py` parses the TS source so the two sides can't drift silently.

The insights prewarmer runs insight queries serially (`max_workers=1`) and warms
only the adaptive default plus eligible analyst clamp shapes. Additional
comparisons, including 24h/720h, stay on demand: automatically warming the month
option on each tick caused 91–95s background runs and dashboard contention on
Standard mode. Never compensate with per-job DuckDB thread or memory overrides.

The prewarmer **yields to ingest**: a tick (~55 s avg on the 4-vCPU VM) running alongside `log_discovery` / `rum_sync` for the same service stretches their tick time and so ingest freshness. The signal is the in-process progress registry (`backend.cron_progress.active_task_for_service`, no DB query). Ingest in flight at tick start defers the tick (INFO log, no lease, no `cron_runs` row, same shape as the active-request gate); ingest starting mid-tick skips the remaining shape recomputes and the run records success with "stopped early". The yield is **bounded by warmth**: `_last_full_warm` must be younger than `INSIGHTS_CACHE_TTL` (300 s), otherwise the tick runs to completion regardless of ingest. With interval 240 s < TTL 300 s < 2 x interval, at most one tick in a row is deferred or cut short and an entry is cold for at most 180 s extra. In `high_scale` discovery runs on a Celery worker in another process, so the signal never fires there. Do not remove the warmth bound — without it a continuously busy ingest would starve the cache indefinitely.

The prewarmer is **demand-driven**: `/api/insights` and `/api/insights/cache-collapse-detail` record the last request monotonic timestamp per service (`record_insights_request(service_id)` in [backend/cron/jobs/insights_prewarmer.py](backend/cron/jobs/insights_prewarmer.py)). When no request has arrived within the idle window (`provisioning.cron_insights_prewarmer.idle_minutes`, falling back to `INSIGHTS_PREWARM_IDLE_MINUTES`, default `30` minutes; `<= 0` disables the gate), `_run_insights_prewarmer` skips before acquiring a lease or opening a DuckDB connection. The first visit after idle does a cold compute and re-enables background warming for the next 30 minutes.

**Insights SQL & Scratch-Table Performance ([backend/repositories/insights/definitions.py](backend/repositories/insights/definitions.py), [backend/repositories/insights/repository.py](backend/repositories/insights/repository.py)):**
- **Window-first `AS NOT MATERIALIZED` CTEs**: Any CTE (`base`, `raw_base`, `fingerprint_asn_maps`, `deep_requests`, `ua_parsed`) referenced by both the window filter (`win`) and the baseline comparison (`base_b`) MUST use `AS NOT MATERIALIZED`. Without it, DuckDB automatically materializes the entire 30-day CTE before applying `WHERE is_w` or the `JOIN win` hash pushdown. With `AS NOT MATERIALIZED`, DuckDB binds the same `?` parameter count at binder time, inlines the CTE, scans only the 1h window for `win`, and pushes `HASH_JOIN` filters into `base_b` so baseline aggregations (`COUNT(DISTINCT ...)`, `PERCENTILE_CONT`) run only on candidate keys that passed the window `HAVING` threshold.
- **Distinct-dimension regex/string pre-evaluation**: For regex/string-heavy detections (`COALESCED_IP_SECURITY_AGGREGATES`, `CRAWLER_DISPARITY`, `STALE_BROWSER_VERSION`), evaluate `regexp_matches` / `regexp_extract` / `contains(lower(...))` once per distinct `url`, `ua`, or `referer` instead of per row across the 30-day table.
- **Schema-driven scratch columns**: `_required_temp_cols(actual_cols)` materializes only columns present on the service's table schema into `scratch_N.t_logs`.
- **Reproducible local profiler**: Run `uv run python scripts/dev/profile_insights.py` to benchmark cold/warm p50/p95 wall + process CPU times and per-phase/per-detection timings against a deterministic synthetic 30-day dataset (`tests/repositories/test_insights_golden.py` enforces zero behavior drift across all 51 registered definitions).

### State Sync ([backend/state_sync.py](backend/state_sync.py))
`export_admin_state` writes `audit_logs` + `views` from PostgreSQL metadata, plus `log_format_history` + `custom_fields` from the config JSON, to `{prefix}/meta/admin_state.json`. **Alerts are not synced** — each instance maintains its own. Only `read_write` services export.

### FOS Usage Logging ([backend/utils/usage_logger.py](backend/utils/usage_logger.py), [backend/core/metadata/usage_log.py](backend/core/metadata/usage_log.py))
Every FOS Class A/B op and CDN download recorded to PostgreSQL `usage_log` table for cost analysis.
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

### DuckDB Connection Pool ([backend/core/duckdb_pool.py](backend/core/duckdb_pool.py))
Per-service LIFO pool replaces per-request `duckdb.connect()` + S3 / DuckLake setup + view rebind (~50ms steady-state). Pool size is `DUCKDB_POOL_MAX_SIZE` (default 8). All pool connections open with `read_only=False` — `get_connection` forces this so cron writers and pool readers don't trip DuckDB's "different configuration" error on the same file. Optional per-connection tuning: `DUCKDB_POOL_CONN_MEMORY_LIMIT` (e.g. `256MB`) caps RSS growth under concurrent large scans. DuckDB `threads` is instance-global, not per-connection — it is set once from `DUCKDB_THREADS` and must never change at runtime (Trap #42). View-binding happens outside the pool lock to avoid deadlocking the FastAPI thread pool when an Iceberg snapshot reload blocks.

**Pool wait observability** — `_Pool.acquire` records every checkout's wall-clock wait time to (a) the OTel `app.thread_wait_ms` histogram tagged `{outcome: reused | created | timeout, waited: true | false, service}` for off-box analysis via `docker compose logs backend | grep app.thread_wait_ms`, AND (b) a bounded in-process ring buffer (~1024 samples per service) consumed by `Pool.stats().wait` (p50/p95/p99/max/mean). `GET /api/admin/health-snapshot` exposes the per-service stats (plus `saturated_rejects_total` / `drain_rejects_total` pool-reject counters and the last-warmed timestamp); the `SystemHealthCard` on `/admin` renders top-level Pool wait p95 / Pool in-use / idle cards plus an expandable per-service table. ADR-03 escalation rule: p95 > 50ms ⇒ consider separate-process cron isolation; > 200ms flags red. Both paths are non-blocking (try/except around the recorder) so instrumentation can never break a checkout.

**OOM safeguards.** Two backstops bound process RSS independently of the pool. (1) [backend/core/memory_guard.py](backend/core/memory_guard.py) — an opt-in process-level guard (`BACKEND_GRACEFUL_RESTART_RSS_MB`) that, on crossing the ceiling, converts a destructive cgroup OOM-`SIGKILL` into a graceful self-restart (SIGTERM → uvicorn drains → `restart: unless-stopped`). (2) The `duckdb_recycle` cron drains + recycles the pool to free DuckDB's object cache (`DUCKDB_RECYCLE_INTERVAL_MIN`, off by default). The historical unbounded-RSS leak that motivated both was ultimately a **SQLite** connection leak — the per-tick cron watchdog built a throwaway `ThreadPoolExecutor` each tick, orphaning the `check_same_thread` `ThreadLocalPool` connection it opened; [backend/core/sqlite_pool.py](backend/core/sqlite_pool.py) now reaps connections whose owning thread has exited (swept on cold-open), and the watchdog reuses one bounded executor.

### Hourly Top-N Rollups ([backend/core/rollups/](backend/core/rollups/), [scripts/backfill_rollups.py](scripts/backfill_rollups.py))
Precomputes per-hour Top-N aggregates for the dashboard's most-asked fields (ip, country, url, custom fields) and writes them under `<cache>/rollups/`. Closed hours read from the rollup; the current ("live") hour merges the rollup with a fast scan of the buffer. Plus a per-minute time-series bundle (`rollups/hour_bundled/hour=H/time_series.parquet`) used by the dashboard chart to skip the wide storage scan. Skipped buckets fall back to the raw scan path. Written by `recompute_touched_hours` after each `local_compact_{id}` pass (only hours that just received data). Bursty services can close an hour without ever re-touching it, so the hourly `rollup_heal_{id}` cron (:05 — `_run_rollup_hour_heal` in [backend/cron/jobs/compaction.py](backend/cron/jobs/compaction.py)) re-runs the idempotent `backfill_missing_hour_bundles` with a 48-hour lookback (`lookback_days = 2`); `rollup_compact_{id}` (daily 02:00 UTC) is the 30-day deep pass + per-day compaction. Verified-empty closed hours get an empty sentinel bundle (covered-and-empty, distinguishable from a writer gap), and the reader (`execute_top_n_rollups` in [_base.py](backend/repositories/_base.py)) live-queries any still-uncovered closed hours of not-yet-day-compacted days instead of silently under-counting (the 2026-07-06 "total_rows disagrees with every card's field_total" bug).

**Bundle tiers** (cheapest first wins in the reader):
- `rollups/day_bundled/day=D/all_fields.parquet` — one parquet per day, all fields. Reader prefers this for fully-in-window closed days.
- `rollups/hour_bundled/hour=H/all_fields.parquet` — one parquet per hour, all fields. Reader uses for partial-day boundary hours + any day without a day-bundle.
- `rollups/hour/field=F/hour=H/*.parquet` — per-(field, hour). Original source of truth; the bundle writers read from here.
- `rollups/day/field=F/day=D/*.parquet` — per-(field, day). Source for the day-bundler.

**Virtual fields** (`waf_sig_ind`, `edge_score_reason_ind` — see `_VIRTUAL_FIELD_BACKING` in `rollups/_common.py`) are CSV-unnested at WRITE time so the dashboard reader serves them through the standard rollup path instead of paying a 30-day unnest-during-query each request. Wired in `_run_per_field_copy` (rollups/recompute.py) via `_build_virtual_field_copy_query` (rollups/_common.py). Adding a new virtual field requires (a) appending to `_VIRTUAL_FIELD_BACKING`, (b) ensuring its `backing` column is on the schema, (c) a one-shot rebundle migration so existing hour/day bundles pick it up (see next point).

**Stale-bundle hazard.** `bundle_hours` / `bundle_days` use mtime to skip up-to-date bundles, and the cron only re-bundles HOURS THAT JUST RECEIVED DATA. Closed historical hours never get re-touched. If you add a new field to the rollup writer (real or virtual), the per-(field, hour) parquets land but the bundled `all_fields.parquet` for closed hours stays without them — the dashboard's bundled-rollup reader returns 0 rows for the new field and the runtime fallback fires. Fix: delete the stale closed bundles and re-run the backfill — `backfill_missing_bundles` / `backfill_day_bundles` in [backend/core/rollups/](backend/core/rollups/), or the [`POST /api/admin/backfill-bundle-rollups`](backend/routers/admin/compaction.py) endpoint.

**Live-hour batch must filter virtual fields out** before `execute_top_n_batch` (in `_base.py`'s `execute_top_n_rollups`): the SQL projects `field_name AS value` and virtual names aren't real columns on the live temp table. Passing them through BinderException's the whole UNION ALL and silently drops the live-hour merge for real fields too. See `live_fields = [f for f in fields if f in actual_cols]` at the merge site.

**Dashboard rollup path builds NO per-request temp** ([backend/repositories/dashboard.py](backend/repositories/dashboard.py) `_build_query_target`): every window-scan section is rollup-served — top-N/map via `execute_top_n_rollups`, chart via `try_time_series_from_rollup`, `conn_requests` histogram via `try_conn_requests_hist_from_rollup` (reads the `security_conn_reuse` rollup; 2h min-window override + live active-hour top-up + '21-100'/'>100'→'21+' label fold), signal unnests via the virtual-field rollups — and each section's rollup-miss fallback runs its ONE query against the base table via the stashed `orig_table_name` / `orig_where_clause` / `orig_params` (the old eager narrow temp cost ~391 ms for ~12 ms of temp reads). The non-rollup (filtered) path still materializes the wide temp. `map_data` derives from `all_top_res`, not a separate query. New dashboard consumers: serve from a rollup with a direct base-table fallback; don't reintroduce a shared per-request temp.

**`get_top_bots` rollup-served UAs** ([backend/repositories/security.py](backend/repositories/security.py)): on the unfiltered path (`not filters`), top UAs come from `execute_top_n_rollups(["ua"], ..., limit=50000)` instead of scanning the storage view for the `ua` column. The NGWAF bots panel is served from the per-hour `ngwaf_bots` rollup ([backend/core/rollups/ngwaf_bots.py](backend/core/rollups/ngwaf_bots.py) — the `waf_req_id ⨝ ngwaf_bot_cache` join done ONCE per closed hour at write time via `sqlite_scan`; exact SUM across hours, no `_approx`; a zero-bot closed hour writes an EMPTY parquet as a covered-and-empty sentinel so quiet services don't fall back forever) via `runner.try_ngwaf_top_bots_from_rollup`. On a rollup miss it falls back to a single direct base-table join (`SQL.NGWAF_TOP_BOTS_JOIN_DIRECT`) — NO temp at all on the rollup-UA path. Filtered requests fall back to the original combined `(ua, waf_req_id)` temp.

**`/api/network-health` skip-temp seam** ([backend/repositories/network.py](backend/repositories/network.py)): the heatmap/geo sections are hoisted to `try_network_heatmap_from_rollup` / `try_network_geo_from_rollup` BEFORE building the filtered temp; when every requested scan-bound section hits a rollup, `create_filtered_temp_table` is skipped entirely (mirrors origin.py's skip-temp guard — `network:temp_skipped` in `_section_timings`).

**When adding a new analytics panel (or a new field rendered on an existing panel), consider a rollup at PR time.** Any panel reading from the per-request temp table on 30 d windows is a candidate. Workflow:

1. **Measure first.** Hit the endpoint on prod via the admin tunnel with a 30 d window + empty `filters: {}` and inspect `_section_timings` in the JSON response. Any per-panel section > ~1 s on 30 d is rollup-worthy. (Audit JSONs under `performance-report/` usually don't carry section_timing — prefer a live curl.)
2. **Pick the shape.** Three reference templates already cover the common cases:
   - Per-dimension percentiles (weighted-average across hours) — copy [backend/core/rollups/origin_dims.py](backend/core/rollups/origin_dims.py) (`slow_urls` per-URL p50/p95/p99) or [backend/core/rollups/network.py](backend/core/rollups/network.py) (`network_rtt` per-ASN p95/p99). Reader returns `{..., "_approx": True}`; the FE surfaces an "Approximate" badge on the affected panel.
   - Exact GROUP BY counts — copy [backend/core/rollups/network_speed.py](backend/core/rollups/network_speed.py). Math is associative across hours; no `_approx` flag.
   - Exact time series (re-bucketable) — copy [backend/core/rollups/verified_bots_ts.py](backend/core/rollups/verified_bots_ts.py). Store at MINUTE granularity (`date_trunc('minute', timestamp)`); the reader re-buckets via `time_bucket` to any caller `bucket_seconds` that's a multiple of 60 (gate `% 60 == 0` — non-multiples are inexact). Unlike the leaderboard shapes, the **day compactor PRESERVES the bucket_ts dimension** (`GROUP BY bucket_ts, dim`) so a series can still be produced over the window, and the reader is **hybrid**: `UNION ALL` of the closed-hour rollup + a scoped live query for the in-progress active hour from the temp table (the writer never rolls up the active hour), merged by an outer `GROUP BY (bucket, dim) SUM`. No `_approx` flag.
   - Single-row-per-hour summary (multiple aggregates) — copy [backend/core/rollups/origin_summary.py](backend/core/rollups/origin_summary.py).
3. **Wire the 10 seams.** Writer module → constants in `_common.py` → exports in `__init__.py` → a `RollupSpec` entry in `spec.HOUR_WRITERS` (`recompute.py` iterates it best-effort; import the writer into `recompute.py` too — it is dispatched by name) → daily compactor in `day_bundles.py` (mirror `compact_network_rtt_closed_days_to_daily`) → cron hook in `backend/cron/jobs/compaction.py` → reader method on `QueryRunner` in `backend/repositories/_base.py` (with the standard eligibility gates: `not has_filters` + window ≥ 48 h + ≥ 50% closed-hour coverage + day-prefer/hour-fallback walk) — coverage decisions live in `backend/repositories/rollup_coverage.py` (`CoverageIndex`, `plan_hourly_read`, `collect_day_prefer_paths`); extend `tests/repositories/test_rollup_reader_coverage_golden.py` for the new reader → dispatcher at the live-SQL call site (try rollup, fall through to live on `None`) → admin backfill endpoint in `backend/routers/admin/compaction.py` → tests + extend `tests/core/test_rollups_recompute.py` to patch the new `build_*` call in both `_swallows_downstream_bundle_errors` and `_malformed_hour_skipped`.
4. **After deploy, run the post-deploy backfill.** `POST /api/admin/backfill-bundle-rollups` walks the bundle tree and produces both the per-hour and per-day files for historical hours in one shot — without this, the new rollup only covers hours touched after deploy.
5. **Re-measure on prod.** Confirm `_section_timings` shows `<name>_query_rollup` with sub-100 ms instead of `<name>_query` with seconds. If the dispatcher routes but timing is unchanged, the rollup file wasn't built (check backfill counts).

**Don't try these — they've been declined for documented reasons:**
- Pre-aggregating percentile sketches for cross-hour combine — DuckDB has no sketch combine. Use request-weighted averages with the count carried alongside (see `network_rtt.py` reader SQL + the no-sketch-combine comment in [backend/repositories/_base.py](backend/repositories/_base.py)).
- Collapsing rollup parquets into fewer-larger files for SCAN-bound queries — DuckDB parallelises across files. Daily-compaction for ROLLUPS is different (those are file-open-overhead-dominated, not scan-bound).
- Response-caching as the perf lever — new logs are always being ingested so the cache TTL has to be sub-minute. Real wins live on the cold-path SQL.
- `temp_table_create` itself — it's materialize-bound; prior CTE/view replacements REGRESSED downstream scans 5×. Add more rollups so fewer panels read from the temp at all instead of trying to make the temp faster.

### Response Telemetry Middleware ([backend/utils/telemetry_response_middleware.py](backend/utils/telemetry_response_middleware.py))
Backstop for endpoints that return a plain `dict` instead of going through `BaseResponse.with_telemetry`. Inspects JSON object responses, injects `_debug_queries` / `_debug_calls` / `_debug_sqlite` / `_is_cached` from the contextvar collectors if missing (`_debug_sqlite` is snapshot-copied BEFORE `get_tracked_calls()`, which can itself run a SQLite SELECT that would append mid-injection). **Must be added INNER to `CompressMiddleware`** (i.e. `add_middleware(TelemetryResponseBodyMiddleware)` BEFORE `add_middleware(CompressMiddleware)`) so it sees the raw JSON, not br/zstd/gzip-encoded bytes. Skips streaming responses, non-dict bodies, and already-instrumented responses. Debug keys are per-request opt-in: the frontend API client sends `x-debug-responses: 1` when the DiagnosticsPanel toggle is on (keys are STRIPPED otherwise), and SSR's own upstream fetch mirrors the toggle via the `fla.debugResponses` cookie ([frontend/lib/debug-cookie.ts](frontend/lib/debug-cookie.ts), read in [frontend/lib/ssr/_transport.ts](frontend/lib/ssr/_transport.ts)) — the Debug Panel's SQLite/DuckDB views are page-scoped per-response captures, not global ring buffers. Gated on `DEBUG_RESPONSES`; failure modes are silent + non-blocking.

### Isolated High-Scale Request Facts ([backend/routers/high_scale.py](backend/routers/high_scale.py))
`POST /api/high-scale/services/{service_id}/request-facts` is an explicit
high-scale-only read surface backed by `backend/high_scale/query_service.py`.
It is not selected by `DEPLOYMENT_MODE` and does not alter standard
routing. A service must be explicitly bound through the
injectable `HighScaleServiceRegistry`; unregistered services are refused.
The route uses `RequestContext` tenancy and analyst time clamping, masks
`client_ip` under the existing invite PII policy, and returns signed keyset
pagination plus `QueryResponseMetadata`.

### High-scale helper modules (wired)
The six capacity helpers each have a production caller, all behind the high-scale topology (`config.is_high_scale_mode()` for backend code; the worker and ingest controller are high-scale-only). Standard mode runs none of this and its responses are unchanged. Full design, gates and non-goals: [docs/runbooks/high-scale-helper-wiring.md](docs/runbooks/high-scale-helper-wiring.md).

| Module | Caller |
|---|---|
| `disk_guard.py` | 60 s `metric_snapshot` sampler (`_sample_high_scale_disk` via `disk_sampler.py`) reads the real serving volume from ClickHouse `system.disks`; mode is logged on transition, exported as gauges, and surfaced as `high_scale.disk` in `GET /api/admin/health-snapshot`. Evaluate-and-surface only: no documented optional work exists to gate, so nothing is suspended |
| `retention.py` | `GET /api/admin/high-scale/retention-recommendation` (admin-only via the `/api/admin/` prefix). Advisory; an explicit `provisioning.cron_sync.data_retention_days` always wins and nothing is persisted |
| `fairness.py` | `HighScaleWorkerLoop.run_once` / `_MultiServiceWorkerLoop.run_once` select (service, domain) pages through `FairPageSelector`. `HIGH_SCALE_PAGES_PER_TICK` (default uncapped, original order) and `HIGH_SCALE_SERVICE_WEIGHTS` tune it |
| `projections.py` | `HighScaleIngestController._publish_cmcd_projection` publishes `project_cmcd` rows (CMCD map plus the request's `country`/`asn`) to `cmcd_projection_facts` after request ingest (best-effort, idempotent batch id) |
| `recovery_capacity.py` | `rebuild_serving_state_from_fos` / `rebuild_origin_projections_from_fos` take an optional measurement and report `capacity` + `recovery_claim_valid`; recovery is never blocked. `scripts/backfill_high_scale_origin.py --capacity-json` exits 3 when the rebuild completes but the claim is invalid |
| `topology.py` | `HighScaleConfig.validate()` / `topology_profile()` (worker startup) and the `high_scale.topology` health block |

Do not wire a new caller that disables ingest, compaction, replay or deletion on a disk mode without a design that names the work to suspend.

High-scale Performance (`/api/performance/aggregates`) has no stub sections: waterfall reads `performance_minute_summary`, scatter reads `performance_scatter_samples`, and any active filter reroutes every section to `request_facts`. A new projection table needs all of: `sql/*.sql` schema, `CLICKHOUSE_HIGH_SCALE_TABLES`/`_COLUMNS` allowlists in `core/clickhouse_client.py`, an `AggregateBatchAdapter._ORIGIN_COLUMNS` entry, both publish tuples in `ingest_controller.py`, and a replay/backfill path (`scripts/backfill_high_scale_performance.py --until <worker deploy time>`; replaying past that time double-counts the sums). The stored `performance_minute_dimensions.latency_*_ms` columns hold raw microseconds; the reader divides by 1000.

### High-scale Security reads
`backend/high_scale/security.py` mirrors the repository contract (bucket labels, `time`/`pct` series, row keys) and never returns a hardcoded empty section; a ClickHouse failure propagates to the error envelope. Every card reads `request_facts` with the request `filters` bound, except the unfiltered NGWAF bot cards, which read the `ngwaf_bot` rows of `security_minute_dimensions`. `request_facts` has no `waf_req_id`, so NGWAF names are resolved at ingest (`_enrich_high_scale_ngwaf_bots`, before the facts publish) and persisted as `ngwaf_bot` / `ngwaf_bot_category` / `ngwaf_wellknown_bot` for filtered reads. `mask_ips` masks `active_clients[].ip` and `top_ips_header[].ip`; the router must pass it, and must clamp `threat-intel` / `proxies/export` windows before dispatching. Rebuild the projection from the archive with `scripts/backfill_high_scale_security.py` (adds rows; clear the window first).

### High-scale readers return real data or fail loudly
No `DEPLOYMENT_MODE=high_scale` reader may return hardcoded empty lists/dicts, a swallowed query failure, or a silently ignored filter/parameter. When ClickHouse lacks the data, add it to a projection and rebuild; do not approximate. Runbook for the existing additions: [docs/runbooks/high-scale-reader-parity-backfill.md](docs/runbooks/high-scale-reader-parity-backfill.md).

- **Percentiles**: never average per-minute percentiles. Origin projection rows carry a log-histogram sketch (`_sketch.py`, `*_sk_v/_sk_w`) merged at read time with `quantilesExactWeightedArray`; responses set `_approx` + `_approx_note` (1% bound) and counts stay exact.
- **Filters**: `_filters.request_filter_clauses` translates request filters (keys normalised like the analyst filter-lock, all typed params); unsupported filters raise `ValueError` (HTTP 400), never get dropped. RUM and CMCD have their own narrower allow-lists for their tables.
- **Dashboard window boundaries**: with both bounds supplied, a sub-minute start or end uses the existing scoped facts reader; whole-minute projections cannot represent partial minutes. Aggregate response-cache keys retain seconds and fractions so different event windows cannot share results. Minute-aligned unfiltered windows keep the projection path, and existing aligned-end semantics are unchanged.
- **Visibility fence**: fact readers require the batch's publication row to be `visible` (`_scope.visible_fact_clauses`).
- **Origin filters**: the Origin projections are per-minute with no filterable dimensions, so a request with any active filter is served by `origin_facts.py` straight from `request_facts` with exact quantiles (`_approx: false`), using the projection's per-row semantics (latency = `ottfb` else `ttfb`, status = `ost`, overhead = `elapsed - ottlb`). Unfiltered requests keep the sketch-backed projection path. A filter field with no facts mapping raises a 400; it is never ignored. `tests/high_scale/test_clickhouse_readers_integration.py` covers the filtered path on a real engine.
- **SQL console (`high_scale/query.py`) trust boundary**: never splice scope into the user's SQL. Service, publication-fence and window scope are sent as the `additional_table_filters` HTTP setting through `ClickHouseClient.execute(settings=...)` (allow-listed names only), the result cap is applied by wrapping the user SQL as `SELECT * FROM (\n<sql>\n) LIMIT n` plus `max_result_rows`, so a trailing `--` comment or an `OR` in the user's own `WHERE` cannot undo either. Function calls are checked against `_ALLOWED_FUNCTIONS` (ClickHouse table/server functions such as `remote`, `url`, `file`, `getSetting` are rejected; add a function only after confirming it cannot reach another host or disclose server state), and the first query per client probes `system.one` to fail closed if the server does not enforce the filter. `tests/high_scale/test_query_console_scope.py` and the `sql_console` cases in the real-engine integration module pin all of it. **Analysts get the console only through per-service, per-mask-mode least-privilege ClickHouse identities** (`backend/high_scale/console_access.py`, [runbook](docs/runbooks/high-scale-analyst-console.md)): each user is granted `SELECT` on its own `fla_cv_*` views only (no base table, no publication table), the views fix `service_id` and the publication fence under `SQL SECURITY DEFINER`, and the masked views replace `client_ip` and the `custom_fields` / `cmcd` session and IP keys with `'[redacted]'` in the data, so no string function can recover them. Five independent reviews each found a way DuckDB's name-based validation and ClickHouse's name resolution disagree (comment nesting, `"a'"` quote bridging, `WITH RECURSIVE <table>`, a CTE over a `UNION`, `x IN <table>`), which is why the database, not the guard, holds the boundary. Without `CLICKHOUSE_CONSOLE_SECRET` the router returns 403 `console_unavailable_for_analysts`; a view that leaks another service or fails to mask fails the first-use identity check closed. Provision order is load-bearing: views are created and verified before any `GRANT`. Do not widen the analyst grants, and re-run `RUN_HS_CONSOLE_ACCESS_INTEGRATION=1 uv run pytest tests/high_scale/test_console_access_integration.py` after any change to them or to the ClickHouse image.
- **Insights**: `insights.py` runs every registered detection natively (`insight_queries.py`, same row layout as the standard processors); a new standard insight needs a ClickHouse query there or `test_every_registered_detection_runs_and_the_response_validates` fails.
- **Adding a column to a projection or fact table** touches: the `.sql` schema (`CREATE` + `ADD COLUMN IF NOT EXISTS`), `CLICKHOUSE_HIGH_SCALE_COLUMNS`, the writer's `_DOMAIN_COLUMNS` / `_ORIGIN_COLUMNS`, the projection builder, a `purge_*` + `rebuild_*` path in `recovery.py` wired into `scripts/backfill_high_scale_origin.py --projection`, the reader, and the runbook. A rebuild publishes under new batch ids, so replacing rows requires the purge first.
- **ClickHouse dialect traps** (all found only against a real engine): `formatDateTime` minutes are `%i` (`%M` is the month name); a trailing `ORDER BY`/`LIMIT` after `UNION ALL` binds to the last `SELECT` only (wrap the union); `greatest(UInt64, Float64)` is illegal (`toFloat64`); a `LEFT JOIN` miss yields defaults, not NULL (use `ifNull`); 64-bit ints are unquoted only because the client sets `output_format_json_quote_64bit_integers=0`. Fake clients cannot catch these: run `RUN_HS_READERS_CLICKHOUSE_INTEGRATION=1 uv run pytest tests/high_scale/test_clickhouse_readers_integration.py` (and `RUN_RUM_CLICKHOUSE_INTEGRATION=1 ... test_rum_integration.py`) when touching reader SQL.

### Certificate-authenticated administrator connection

The optional mTLS gateway is a separate administrator access surface for Compose
and Kubernetes. Its dedicated client CA is not the analyst login system or a
platform-wide client CA. See [docs/deploy/admin-mtls.md](docs/deploy/admin-mtls.md).

`ADMIN_GATEWAY_SECRET` is a server-only credential injected into
`X-Admin-Gateway-Token` after client-certificate authentication. Backend
`backend/utils/admin_gateway.py` and frontend SSR independently verify it.
Never put it in bootstrap, browser code or logs. Public proxies must strip it.
Requests carrying the public `X-Proxied-By-Caddy` marker cannot be promoted with
this credential.

`ADMIN_GATEWAY_REQUIRED=1` removes implicit administrator access from loopback
and `LOCAL_ADMIN_CIDRS`; unauthenticated requests keep analyst restrictions.
Cheap liveness remains available, but deep health requires authentication.
Set required mode on both backend and frontend runtimes. SSR bootstrap caching
uses the shared transport trust decision and bypasses shared caching whenever
an `analyst_session_id` cookie is present; even a loopback request can produce
an analyst-scoped bootstrap. Never cache a response marked `is_remote_analyst`.
Legacy configurations without required mode retain their existing behavior.
Do not remove host-management SSH when retiring dashboard tunnel scaffolding.

### Live Query Monitor ([backend/core/query_registry.py](backend/core/query_registry.py), [backend/routers/admin_queries.py](backend/routers/admin_queries.py), [frontend/app/admin/queries/](frontend/app/admin/queries/))
Real-time view of every executing DuckDB + PostgreSQL query — attribution (analyst / admin / cron / system), caller `file:line`, pool slot, duration ticking up live, kind-aware Kill button that calls `con.interrupt()`. Page at `/admin/queries`, admin-only via `RemoteAccessMiddleware`. Polling at 300 ms; the Active panel promotes "completed in the last 10 s" rows as faded entries with an outcome badge so typical-traffic (p50 ≈ 0.2 ms, max ≈ 29 ms) queries are visible. Notable Slow Queries panel filters the completed-history ring buffer by threshold (100ms / 500ms / 1s / 2s / 5s), sorted slowest first. Queries above the persistence threshold are also written to a `slow_queries` table ([backend/core/metadata/usage_log.py](backend/core/metadata/usage_log.py), aliased as `backend.core.metadata.slow_queries`, in PostgreSQL metadata) stamped with the request correlation id (`rid`, also emitted in the access log), so the panel can answer "what was slow yesterday?" across restarts.

Instrumentation lives at two seams: PostgreSQL `PgCursorWrapper` / profiler ([backend/utils/telemetry.py](backend/utils/telemetry.py), aliased as `backend.utils.sqlite_profiler`) registers/deregisters around `execute*`; DuckDB `InstrumentedDuckDBConnection` + `_InstrumentedResult` ([backend/core/query_instrumentation.py](backend/core/query_instrumentation.py)) wraps the connection returned from `checkout_connection` so deregistration happens at terminal-fetch time (fetchdf, arrow, etc.) rather than at `execute()` — DuckDB's execute returns in ~ms while fetch can run for seconds. Per-query overhead measured ~21 µs (~0.3% of dashboard bundle wall time). Cancel path is safe under pool reuse: a stamped `_conn_to_query[id(con)]` is verified under lock before `interrupt()` so a stale UI click never cancels a different query that's checked out the same physical connection later.

Audit log fires on every successful cancel (`audit_log` in [backend/utils/structlog_config.py](backend/utils/structlog_config.py)) with the actor + full target attribution. OTel histograms: `app.active_queries.count`, `app.query_duration_ms`, `app.queries_cancelled_total`. Kill switches: `QUERY_MONITOR_ENABLED=0` hides the endpoints (404), `QUERY_REGISTRY_DISABLED=1` bypasses the hot path entirely for zero overhead.

### Multiplexed Admin SSE ([backend/routers/admin/high_scale.py](backend/routers/admin/high_scale.py), [frontend/hooks/useAdminEventStream.ts](frontend/hooks/useAdminEventStream.ts))
The admin shell mounts ONE SSE connection: `GET /api/admin/events/stream?channels=sync_status,cron_runs,system_metrics,share`. It fans the publisher subscriptions + the metrics poll loop into a single typed-envelope stream; [frontend/lib/admin-stream-apply.ts](frontend/lib/admin-stream-apply.ts) demuxes each envelope back to the same React Query cache the old per-stream hooks wrote. This replaced four always-on streams (`sync-status`, `cron-runs`, `system-metrics`, `share`) that starved bootstrap/panel fetches over the HTTP/1.1 admin tunnel (~6 conns/origin). The `system_metrics` channel is deduped server-side ([backend/routers/admin/health.py](backend/routers/admin/health.py), aliased as `backend.system_metrics_sampler` and `backend.routers.admin.system_metrics`, short per-service TTL + per-key lock) so N admin tabs collapse to one recompute per window. The analyst `/api/log-extents/stream` is separate and untouched. **When adding an admin real-time feed, add a channel here — don't mount a new dedicated SSE endpoint.** `build_share_live_payload` lives in `backend.utils.tunnel` (not `share_admin`) so the events router imports it without a routers→routers edge.

The remaining HTTP/1.1-tunnel connection-limit exposure (v2.2.1) is addressed one layer down, in the Caddy config itself: a second site block in the [Caddyfile](Caddyfile) listens on `https://localhost:8443` / `https://127.0.0.1:8443` (loopback-pinned via an explicit `bind`, since a bare hostname like `localhost` doesn't restrict Caddy's listener the way a literal IP does), terminating TLS with Caddy's internal self-signed CA so the browser negotiates HTTP/2 (real multiplexing) over the SSH tunnel instead of being capped at ~6 connections/origin under HTTP/1.1. `ssh -L 8443:127.0.0.1:8443 <host>` (see `docs/deploy/*.md`) is the recommended tunnel for any multi-tab admin session; the plain `:3001`/`:8000` HTTP tunnel still works and is what the rest of this doc's tunnel examples assume.

### Control Room ([backend/routers/control_room.py](backend/routers/control_room.py), [frontend/app/control-room/](frontend/app/control-room/))
Admin-only operational dashboard with nine fully-live tabs: Overview, Performance, Origin, Security, Network, Sessions, Cost, Insights, and Admin Health. All tabs are backed by a real-time SSE stream polling `rt.fastly.com` at 1-second cadence (`/api/services/{id}/realtime-stream`). The poller runs on a dedicated daemon thread ([backend/core/realtime/transform.py](backend/core/realtime/transform.py), aliased as `backend.core.realtime.poller` and `backend.core.realtime.manager`) with httpx connection pooling, immune to event-loop starvation. The transform layer ([backend/core/realtime/transform.py](backend/core/realtime/transform.py)) extracts 50+ metrics from the RT response into typed SSE envelopes. Charts seed with 60 bars of historical data on page load, render with rolling-average smoothing and SI-suffix Y-axis formatting (k, M, G), and use progressive rendering with lazy-loaded Plotly. A PoP traffic heatmap shows geographic request distribution. Contextual help icons explain each panel. Backgrounded tabs stop polling. Mutation endpoints (mitigations, rules, allowlist, big-red-button, cost-governor) remain admin-gated 501 stubs. Log-field audit and correlator endpoints provide DuckDB-driven data. The Admin Health tab surfaces log-field audit state and ingest health.

### CMCD Streaming Analytics ([backend/routers/cmcd.py](backend/routers/cmcd.py), [backend/routers/cmcd_admin.py](backend/routers/cmcd_admin.py), [frontend/app/streaming/](frontend/app/streaming/))
Services with Common Media Client Data (CMCD) logging enabled get a `/streaming` page and per-session `/sessions/stream` detail page. CMCD field extraction supports v1 (query-string) and v2 (request-header) formats via generated VCL snippets ([backend/provision/cmcd_vcl.py](backend/provision/cmcd_vcl.py)). The admin settings UI ([backend/routers/cmcd_admin.py](backend/routers/cmcd_admin.py)) selects the CMCD version and manages the field catalog. The streaming page shows buffer-health distributions, bitrate/throughput time series, content-type breakdowns, top streaming URLs, and concurrent-session counts with 30-second auto-refresh. The nav item hides automatically when a service lacks CMCD logging (`hasCmcd` derived from service status schema). Repository SQL lives in [backend/repositories/cmcd.py](backend/repositories/cmcd.py); queries use a temp table with combined time-series and no self-joins.

### Service Summary / Fastly Value ([backend/routers/value.py](backend/routers/value.py), [frontend/app/fastly-value/](frontend/app/fastly-value/))
Executive summary page consolidating the measurable value Fastly delivers across tabbed sections: Summary, CDN & Caching, Security, Bot Management, Network & Performance, and Image Optimizer. Backed by a precomputed overview rollup ([backend/core/rollups/time_series.py](backend/core/rollups/time_series.py), aliased as `backend.core.rollups.overview`) for fast loads. The IO tab shows format distribution, bandwidth savings from format conversion, per-request `Fastly-Io-Transform-Stats` metrics, and optimization-opportunity tables with drill-down links. Services without IO enabled see an upsell tab instead. Repository SQL in [backend/repositories/value.py](backend/repositories/value.py).

### Ingest Error Quarantine ([backend/core/metadata/ingest_log.py](backend/core/metadata/ingest_log.py), [backend/routers/admin/ingest.py](backend/routers/admin/ingest.py))
Corrupt or unparseable log lines are saved to an `errors/` prefix in FOS, timestamped and classified by failure reason (malformed JSON, schema mismatch, encoding error). Quarantine storage is included in usage cost calculations. Admin endpoints support export, download, and purge (purge deletes all files, not just expired). The admin UI renders a Quarantine section on the admin page ([frontend/app/admin/_sections/QuarantineSection.tsx](frontend/app/admin/_sections/QuarantineSection.tsx)).

### CDN-Fronted Log Delivery
FOS reads are fronted by a Fastly CDN VCL service (`cdn_service_id`, `cdn_url`, `cdn_secret`). The CDN validates a shared-secret query param to gate access; rate-limited to blunt brute-force. Separate from the logging service ID.

### Session Scoring (edge L2) ([backend/routers/session_scoring.py](backend/routers/session_scoring.py), [compute/scorer/](compute/scorer/))
Edge-computed 0–100 risk score per request, combining cookie/timing signals (L1) with a PageRank route-transition matrix (L2). The Rust scorer at [compute/scorer/](compute/scorer/) builds to Wasm (`make scorer-package`) and runs on Fastly Compute as an instance-per-request sub-fetch; its native unit tests gate via `make scorer-test`. The matrix is **not** embedded — it's served from the `scoring_matrix` KV Store at runtime. Backend surface: read/retrain/admin-config endpoints in [session_scoring.py](backend/routers/session_scoring.py) + [session_scoring_admin.py](backend/routers/session_scoring_admin.py); deploy/teardown orchestration in [backend/provision/session_scoring_orchestrator.py](backend/provision/session_scoring_orchestrator.py); VCL generation in [backend/provision/session_scoring_vcl.py](backend/provision/session_scoring_vcl.py).

- **L2 enforcement is explicit operator opt-in** — `GET`/`PUT /scoring/l2-enforce` (`L2EnforcementCard` in the UI). L2 is always computed and logged but contributes to the *enforced* combined score only after an operator enables it; enabling fades it in over three days, disabling returns it to observe-only. There is no clock-driven auto-ramp. Deployment age is only an advisory readiness gauge.
- **NGWAF skip-inspection on the sub-fetch** — the internal scoring sub-fetch carries `x-sigsci-skip-inspection-once` so NGWAF doesn't inspect (and 406) the internal call; it's set on the compute route and unset on the scrub + restart paths so the real-origin WAF path is never bypassed. (See [session_scoring_vcl.py](backend/provision/session_scoring_vcl.py).)
- **No VCL retry of the scoring sub-fetch** — only 2 restarts (score→origin spends them); fails open on timeout/error by design.

### Live Dashboard Sharing
Components for the live-shared-instance remote-analyst feature. Two direct-mode sharing modes are exposed to the admin (the SSH-reverse-tunnel via localhost.run was deleted in v2.0):

1. **Admin-provided hostname** (e.g. `https://logs.example.com`)
2. **Admin-provided IP** (e.g. `https://203.0.113.42:8443`)

Both share a single backend code path: `ShareStartPayload.use_tunnel=False` + `public_endpoint=<https URL>`. The mode selector in the UI is presentational — the backend only cares that `public_endpoint` starts with `https://` (cookies need `secure=true`). `use_tunnel=True` still exists as a back-compat keyword and now raises a clear error.

Components:

- [backend/utils/tunnel/](backend/utils/tunnel/) — package split: `manager.py` owns the `TunnelManager` singleton (direct-mode lifecycle, sever-all panic), `session.py` holds `AnalystSession`, `rate_limiter.py` is the sliding-window `_LoginRateLimiter`, `state.py` persists `tunnel_state.json`, `fingerprint.py` computes the session fingerprint hash. Process singleton via `get_tunnel_manager()`; `reset_for_tests()` for pytest.
- [backend/utils/remote_access.py](backend/utils/remote_access.py) — `RemoteAccessMiddleware` does DNS-rebinding gate (Host/Origin allow-lists, including `testclient`/`testserver` for pytest), blocks admin paths on remote requests, applies response hardening (CSP, X-Frame-Options DENY, no-store, no-referrer). `_StaticAssetLimiter` rate-limits static assets to blunt scrapes. **Idle-timeout activity model:** the middleware refreshes an analyst session's 2-hour idle deadline only on genuine interaction — requests carry an `X-User-Active: 1|0` header (set from real DOM activity, not background react-query refetches), SSE streams (`/api/log-extents/stream`) are exempt so a backgrounded tab can't keep itself alive, an IP change updates the session's recorded address without bumping the clock, and the `/api/share/heartbeat` beat doubles as the activity channel for an otherwise-quiet dashboard. The access log records `act` + `idle_touch` per request for observability.
- [backend/core/share_db/](backend/core/share_db/) — package split: `connection.py` (PostgreSQL connection and session management), `schema.py` (table DDL and versioning), `invites.py`, `sessions.py`, `audit.py`, `passcode.py` (argon2id current default; scrypt verify branch stays for transparent rehash-on-login upgrade), `tos.py`, `settings.py`, `validation.py`. Stored in PostgreSQL metadata schemas (`pg_schema.py`): `remote_invites`, `invite_services`, `remote_sessions`, `remote_share_audit_logs`, `share_settings`, `remote_invite_claim_tokens`, `share_tos_versions`. Per-IP/per-email lockout.
- [backend/routers/share_auth.py](backend/routers/share_auth.py) (`/api/share/*`) — analyst-facing: `auth-config` (unauth — tells /share-login which login modes to render; the frontend fails OPEN to passcode on fetch error), `login`, `logout`, `acknowledge`, `heartbeat`, `claim/{token}`. Tagged so middleware lets them through the tunnel.
- **Analyst OAuth/OIDC login** (opt-in alternative to passcode): [backend/routers/share_oauth.py](backend/routers/share_oauth.py) — `GET /api/share/oauth/authorize` + `GET /api/share/oauth/callback` are TOP-LEVEL browser navigations, every outcome a 302 (never JSON); converges on the SAME `TunnelManager` session as passcode so RBAC/masking/TOS/boot are inherited. Provider registry in [backend/core/oauth/](backend/core/oauth/): gitignored `data/system/oauth_providers.json` (`OAUTH_PROVIDERS_CONFIG_PATH` override), creds via `OAUTH_<KEY>_CLIENT_ID/_CLIENT_SECRET`, inert unless `OAUTH_FLOW_STATE_SECRET` is set; `SHARE_PASSCODE_LOGIN_ENABLED=0` disables passcode for SSO-exclusive deployments (enforced at the endpoint, not just the UI); prod env passthrough in docker-compose.prod.yml. Invites carry `auth_method` / `oauth_provider` / `oauth_subject` (migration 004); optional JIT invite auto-provisioning for trusted org-restricted providers. E2E/dev mock IdP [backend/routers/mock_idp.py](backend/routers/mock_idp.py) mounts ONLY when `OAUTH_MOCK_IDP=1`.
- [backend/routers/share_admin.py](backend/routers/share_admin.py) (`/api/admin/share/*`, **blocked over tunnel**) — admin-facing: tunnel lifecycle, invite CRUD, session evict, panic/sever-all, backup export/import, GDPR erase, settings.
- Frontend: [share-dashboard components](frontend/components/share-dashboard/) (sharing control, invites, sessions, audit panels), [/share-login](frontend/app/share-login/) (TOS-gated), `useAnalystHeartbeat` & [useShareStatusBanner](frontend/hooks/useShareStatusBanner.tsx). Watermark mounts in `AppLayout` when `bootstrap.settings.is_remote_analyst === true`.

When adding an endpoint that analysts must reach over the tunnel, **register under `/api/share/*`** (auto-allowed) or update `_is_blocked_path()` — don't punch a hole somewhere obvious. (Trap #20.)

### Session-detail lookup ([backend/routers/session_scoring.py](backend/routers/session_scoring.py), [backend/core/request_context.py](backend/core/request_context.py))
The session list attaches an opaque AES-GCM `session_token` per row, sealing the real `(ip, ja4, start, end)` tuple (service-bound via AAD), minted BEFORE response masking runs. The detail endpoint unseals it, runs the exact-match lookup, then re-masks on the way out. **Never key session detail on the displayed `ip`** — for a masking analyst it's rewritten to `1.2.3.xxx` and will never match a stored IP (the "No results" bug). A masking analyst supplying a raw top-level `ip` (no token) is rejected; admin + non-masking analysts keep the raw-ip path. The key is a process-ephemeral default; the `SESSION_TOKEN_SECRET` env override gives restart-stable tokens. Zero/negative session windows (single-request sessions, a large share of traffic) are widened ~1s each side before the analyst time-clamp, else they trip `time_range_empty`.

## Provisioning

### UI Wizard ([frontend/components/ProvisionWizard/ProvisionWizard.tsx](frontend/components/ProvisionWizard/ProvisionWizard.tsx))
Step order: `mode → token → service → storage → ngwaf → fields → execute`. Token entered in step 2 must be threaded into every Fastly-credentialed API call (including the NGWAF fetch). `execute` streams SSE.

### CLI ([backend/provision/cli.py](backend/provision/cli.py))
- `python -m backend.provision.cli provision` — interactive wizard
- `python -m backend.provision.cli teardown --service-id {id}` — teardown
- `python -m backend.provision.cli enable-scoring --service-id {id}` — enable (or redeploy) session scoring on a service
- `python -m backend.provision.cli disable-scoring --service-id {id}` — disable session scoring on a service

Subcommands: `provision` / `teardown` / `update-logs` / `update-cdn` / `enable-scoring` / `disable-scoring` / `list-groups` / `list-fields`.

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

Per-page SSR prefetch is real for origin / security / performance / insights (the dashboard is NOT SSR-seeded — `app/dashboard/page.tsx` is a plain client wrapper and its bundle is fetched on mount) (fetchers in [frontend/lib/ssr/](frontend/lib/ssr/), shared POST caller in `_transport.ts`, seeding in `seed.ts`). Adding another:

1. **Use `node:http.request`, NOT `fetch()`.** Node's `fetch()` always overrides the `Host` header from the URL. The backend's `_remote_host_allowed` gate rejects remote-classified requests whose Host isn't the public endpoint — so without preserved Host, the SSR fetch returns 400 host_not_allowed and silently falls through to the client.
2. **Trust topology is `X-Remote-Analyst: 1`, not `X-Proxied-By-Caddy`.** The SSR runtime hits the backend over loopback. `is_request_remote` ([backend/utils/remote_access.py](backend/utils/remote_access.py)) classifies based on `request.client.host` first, so a forwarded Caddy marker is IGNORED. `X-Remote-Analyst: 1` is the loopback-honored primitive (gated on `tunnel_manager.is_sharing_active()`). Forward it ONLY when the inbound request carries `X-Proxied-By-Caddy` — otherwise the admin SSH-tunnel path is mis-classified as analyst and 400'd. (See history: the 2026-06-11 SSR-leak incident reverted in `f3d8dd7` / `546c279` was the previous-attempt version that forwarded `X-Proxied-By-Caddy` directly. Backend ignored it, returned admin payload, dehydration leaked admin fields into public HTML.)
3. **Always wrap in try/catch + bounded timeout, return `null` on any failure.** SSR errors must NEVER propagate into a broken page — the layout falls back to client fetch when the helper returns null. 5s is generous for prod cron contention; never block SSR longer.
4. **`force-dynamic` is REQUIRED** in any layout/page that does a per-request SSR fetch via `cookies()` / `headers()` from an imported helper. Next.js's static-analysis pass only detects direct `cookies()` calls in the component file itself — calls from an imported module won't flip the route to dynamic. Without `export const dynamic = "force-dynamic"` the layout gets SSG'd at build time (when the backend isn't reachable) and the dehydrated state is permanently empty.
5. **Adversarial test required:** before deploying, hit the prod public URL anonymous AND the admin tunnel and verify the dehydrated state shape. Anonymous public must contain only the `needs_login` stub (NO `sharing_active`, NO `ngwaf_workspace_id`, NO `sync_status`). Admin must contain the full payload.
6. **The SSR seed key MUST byte-match the first-paint client key.** Re-key the page query on a server-reproducible `(rangeToken, anchor)` relative-range pair, NOT a client-`now()`-anchored `start/end` — otherwise the seed lands under a different cache key and the client refetches anyway (no cold-load win). Pin it with a `__tests__/ssr/<page>.test.ts` key-match + transport-trust test (see `origin.test.ts`). The anchor must ALSO make the identical snap-to-stale-extents decision as the client — both sides share [frontend/lib/log-extents-snap.ts](frontend/lib/log-extents-snap.ts) (extents >15 min stale snap the window to real data); a naive now()-anchored seed lands under a different key.
7. **Honor the `?service=` URL param over `bootstrap.active_service_id`.** A cold load of a non-default service's URL otherwise seeds — and paints — the wrong service's data. Diagnose via curl + grep of the dehydrated payload (e.g. `total_rows_total`), not browser timing.

The `serviceStore` Zustand slice hydrates from the SSR-cached bootstrap in `useBootstrap`'s post-mount `useEffect` — for the one-render window before that effect fires, use [`useEffectiveServiceId`](frontend/hooks/useIsDataReady.ts) which falls back to `bootstrap.active_service_id` from the React Query cache. Direct reads of `useServiceStore(s => s.activeServiceId)` flash "No service selected" on first paint.

### Canonical patterns (May 2026 DRY refactor — use these in new code)

1. **`response_model=` on every router handler.** Without it the OpenAPI emits `Record<string, unknown>`. Routes using `Depends(get_source)` should also lift `service_id: str` into the signature so it appears as a path parameter. Wire-safe recipe for dict-shaped endpoints: model with `extra="allow"` + `response_model_exclude_unset=True` + string (`.isoformat()`) timestamps, fields derived from the PRODUCER's actual payload (see [backend/models/common.py](backend/models/common.py)) — byte-identical wire output, now typed in the OpenAPI. SSE/file/binary routes carry an explicit exemption comment instead. The whole admin surface follows this as of 2026-07-06.
2. **`useActiveService` + `useTimeRange` + `useTimezone`** for page context (`activeServiceId`, `start/end`, `timezone`). Don't read `useServiceStore + useFilterStore + useTimezoneStore` directly; on first paint prefer `useEffectiveServiceId` (item 11) so the page doesn't flash "No service selected".
3. **`ReportLayout`** for analytics pages — bundles `useActiveService/useTimeRange/useTimezone + useReportConfig + useDebouncedFilterPayload + useViewMetricUrlSync + useServiceQuery + ChartIntervalButtons + ReportShell` (the URL-sync hook pair was renamed 2026-07-06: view/metric→URL is [useViewMetricUrlSync](frontend/hooks/useViewMetricUrlSync.ts), filters→URL is [useFilterUrlWriteback](frontend/hooks/useFilterUrlWriteback.ts)). Fall back to `ReportShell` only for multi-query or non-standard chrome pages.
4. **`HelpDialog`** from [components/ui/help-dialog.tsx](frontend/components/ui/help-dialog.tsx) — don't compose `Dialog + DialogHeader + DialogTitle` by hand for help content.
5. **`createBaseMap` + `addCountryBaseLayer`** ([components/Map/baseLayers.ts](frontend/components/Map/baseLayers.ts)) for any MapLibre setup. Don't duplicate the map constructor, world-layer + theming inline.
6. **`metadata.record_audit(service_id, event_type=..., details=...)`** — direct (or via the `metadata_db` shim; both resolve to the same `metadata.audit` impl). The `duckdb.log_audit_event` shim and `repositories/audit.py` pass-through were removed.
7. **`date_utils.parse_iso_utc` / `iso_z` / `iso_z_now`** — don't hand-roll `datetime.fromisoformat(s.replace("Z", "+00:00"))`.
8. **`@cron_task` decorator** in [backend/cron/scheduler.py](backend/cron/scheduler.py) (aliased as `backend.cron.decorators`) — handles `start_call_tracking`, `set_process_context`, `flush_usage_log` finally-block, watchdog hard-cap.
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

### Testing Philosophy & Speed (The 3-Tier Workflow)

The project tests ~8,000 backend test cases, ~1,400 frontend test cases, Playwright E2E suites, and 18 security/static gates. Running the entire suite takes several minutes. **Never run `make ci` on every incremental code edit during active development.** Instead, adopt the 3-tier workflow:

1. **Inner Loop (Active Coding — 1 to 3 seconds):**
   Run only the specific test file or test selector matching the code you are touching:
   ```bash
   # Backend: target a single test file or function name
   uv run pytest tests/routers/test_dashboard_router.py
   uv run pytest -k "test_specific_feature"

   # Frontend: target a single test spec
   cd frontend && npx vitest hooks/useDashboardData.test.ts
   ```
2. **Intermediate Smoke Loop (`make fast-ci` — ~1 to 2 minutes):**
   A fast local smoke check run before switching tasks, context switching, or taking a break. It runs OpenAPI drift checks, typechecks, linters, frontend contract tests, and core unit tests across backend and frontend in parallel (`-j4`).
   *Note: `fast-ci` is strictly a developer convenience check. It skips >80% of backend tests (all DB engines, rollups, API routers, and cron tasks), skips all UI component tests, and skips Playwright E2E. It is NOT a substitute for `make ci`.*
3. **Outer Loop Gate (`make ci` — Final Pre-Push / Pre-Deploy Gate):**
   Mirrors every gating GitHub Actions workflow (`ci.yml` and `e2e.yml`). Run once when feature work is complete before pushing to origin or opening a PR:
   - **Stage 1 (Fail-Fast Static Gates, ~10–15s):** Runs `gen-types` followed by the static and security checks in parallel (`-j4`: `typecheck-frontend`, `lint-frontend`, `lint`, `format-check`, `typecheck`, `import-contracts`, `vcl-test`, `scorer-test`, `scorer-audit`, `verify-deps`, `secret-scan`, `osv`, `otel-guard`, `urllib-guard`, `dateparse-guard`, `retry-guard`, `coalesce-guard`, `security-regression`, `openapi-drift`, `deploy-validate`, `stray-file-gate`). Syntax errors, lint/format issues, and type mismatches fail immediately.
   - **Stage 2 (Backend Suite):** Runs `test-ci` (`pytest -n auto` + `--cov-fail-under=85`).
   - **Stage 3 (Frontend Suite):** Runs `test-frontend-ci` (`vitest` with 4-way coverage gates). Pytest and Vitest run sequentially to prevent CPU-core exhaustion and memory swap thrashing.
   - **Stage 4 (Load Gate):** Runs `perf-ci` (100k synthetic load benchmark).
   - **Stage 5 (E2E):** Runs `e2e` (Playwright cross-browser matrix).

### Test Preservation Policy (Never Delete Tests)

**Do NOT remove or delete tests to speed up CI runs.**
- **The test count is not the bottleneck:** Profiling proves that individual test assertions take negligible time (<10% of total runtime). Over 70% of Vitest's runtime is runner infrastructure overhead (JSDOM environment spinning up and tear down, plus module bundling per worker thread). Pytest's collection alone takes ~33s. Deleting tests buys virtually no speedup while introducing severe regression blind spots.
- **Coverage ratchets and floors:** Strict coverage gates fail CI if coverage drops:
  - Backend: `--cov-fail-under=85` in `test-ci` and CI.
  - Frontend: strict 4-way thresholds for lines (66%), statements (65%), functions (54%), and branches (52%).
  - Security regressions: monotonic floor of dedicated regression tests, never lowered (`scripts/check_security_regression_count.sh` holds the current `FLOOR`).
- **Production safety:** Tests protect critical invariants (multi-tenant isolation, real-time log ingestion, DuckLake commits, Rust session scoring, and edge VCL generation). Add or update tests for every change; if a change is not testable in isolation, document why.

### Backend (`tests/`, mirrors source tree)

Fixtures in [tests/conftest.py](tests/conftest.py):
- `in_memory_duckdb` — real DuckDB in memory, `_alerts` and `_views` pre-created
- `test_service_source` — minimal source dict
- `client` — FastAPI `TestClient` with DuckDB deps overridden

**Per-test isolation is fast on purpose.** The autouse `isolate_metadata_db` fixture ([tests/conftest.py](tests/conftest.py)) probes `_PER_TEST_RESET_TABLES` with one `UNION ALL ... WHERE EXISTS` query and `TRUNCATE`s only the tables that actually hold rows (a TRUNCATE recreates every listed relation; truncating all of them per test across N xdist workers made each reset take seconds). **When you add a Postgres metadata table that tests write to, add it to `_PER_TEST_RESET_TABLES`** or rows leak between tests. Module-level caches drain through `CacheRegistry.clear_all()` (see [Shared Helpers](#shared-helpers)); parallel runs share one Postgres, so never run two pytest sessions against the same DSN at once (the symptom is random failures that pass in isolation).

Patterns:
- Router test with mocked config: `@patch("config.load_config")`, `@patch("config.save_config")`, hit `TestClient`
- Router test with in-memory DuckDB: use the `client` fixture, seed via `generate_mock_logs` + `insert_mock_logs` from [tests/utils/mock_data.py](tests/utils/mock_data.py)
- Mock external HTTP: `@patch("urllib.request.urlopen")` with a `MagicMock` context manager
- Provision tests: see [tests/routers/test_provision.py](tests/routers/test_provision.py) — `@patch("backend.core.storage.init_storage_table")` etc.

**What to test:** happy path + error cases (missing params, invalid data, external 4xx/5xx), state-persistence invariants (e.g. custom fields not overwritten on unrelated saves), config schema invariants, any DuckDB query using dynamic table names (SQL-injection coverage via `_safe_table()`).

### Frontend (`frontend/__tests__/`, vitest + jsdom)

Setup: [frontend/vitest.setup.ts](frontend/vitest.setup.ts) — global mocks for Next router, `localStorage`, `matchMedia`. `globals: false`, so the setup file explicitly registers `afterEach(cleanup)` and MSW's `server.listen()` at module top-level (NOT inside a hook — see Traps #17/#18).

Patterns: `render` + `screen.getBy*` for components; `renderHook` for hooks; direct calls for pure utils.

**What to test:** pure utilities exhaustively (filters, formatters, URL builders), hook state transitions, component key states (loading/error/empty/populated), navigation/URL helpers.

### E2E ([frontend/e2e/](frontend/e2e/), Playwright)

Cross-browser matrix: chromium + firefox + webkit, all blocking on PR. Mock backend booted by [frontend/e2e/global-setup.ts](frontend/e2e/global-setup.ts) under `FASTLY_MOCK_MODE=1`. Run locally with `cd frontend && npx playwright test --project=chromium` (or `--project=firefox` / `webkit`). Trace + screenshot auto-uploaded on failure.

SSE-stream specs MUST split on the multi-separator regex `/\r\n\r\n|\n\n|\r\r/` — `sse-starlette` emits `\r\n\r\n`, so the naive `\n\n` parser returns zero messages (see commit `0368868`). Mirrors the production `useServiceStream` regex.

SSR-seeded route fixtures must cover server and browser requests. Streaming's
isolated contract backend uses a per-browser cookie fixture, enabled only by
`CONTRACT_STREAMING_FIXTURES=1` with sandbox paths in `run_contract_backend.py`;
the production app never installs it. Browser `page.route` cannot override a
fresh SSR seed. Keep fixture cleanup per test and preserve populated assertions.

### Hot-path micro-benchmarks ([tests/perf/test_benchmarks_micro.py](tests/perf/test_benchmarks_micro.py))

Per-call cost benches for HyperLogLog + SQL utility paths. Auto-disabled under `xdist` (assertions still run as smoke); for real numbers:

```bash
uv run pytest tests/perf/test_benchmarks_micro.py \
    -o 'addopts=-q' --benchmark-only
```

### Section-level performance evidence

Network's list-shaped `pop_health` keeps its data shape and exposes owner metadata
through the debug-gated `_component_observations.pop_health` sidecar. Do not infer
its cache state from the enclosing bundle or promote historical child timings
under an ancestor cache hit or single-flight follower. Standard reader-dispatch
stamps describe actual attempts; they do not establish missing coverage/history.

Standard dashboard counts preserve the raw query's inclusive end. The count
rollup reader covers half-open, whole-minute windows; partial-minute bounds
fall back to raw counts. The dashboard adds exact-end rows from the scoped
base table after a successful rollup read. Keep these contracts separate.

[`docs/runbooks/section-sweep-measurements.md`](docs/runbooks/section-sweep-measurements.md) defines the Phase 4b reporting contract. Shared visible-results budgets live in `scripts/lib/performance_budgets.json`. `section_sweep.py` requires reviewed prerequisite evidence before any live request; its HTTP-only reports remain incomplete rather than certifying a page. First/repeat request order is not cache state, cached enclosing payloads cannot supply current child timings, and overlapping spans must not be summed into request compute. Response-cache observations are stamped by `repositories/_base.py` (aliased as `repositories/utils/response_cache.py`) and direct-cache owners, with single-flight followers explicitly distinguished from computed misses. Network health's `scope="self"` excludes separately assembled shielding; an enclosing subtree hit still makes both historical. Controlled sampling requires TTL expiry before a verified miss/hit pair; its optional CLI flag never bypasses the prerequisite gate. The browser verifier reports both aggregate and bundle observations with explicit unknowns. Do not run profiling while backend instrumentation, history/coverage, real filter verification, analyst lifecycle or browser evidence is missing. Reader-local `_reader_coverage` and `_history` distinguish actual policies, any-field presence and count/extent provenance; they are not complete-history claims. High-Scale task scopes live in `core/request_telemetry.py` (aliased as `core/high_scale_sections.py`); ClickHouse admission/HTTP/decode phases are client wall observations, never server CPU estimates. Preserve existing debug gates and historical cache/follower semantics. Disposable Chromium analysts use `scripts/lib/analyst_fixture.cjs` only against already-active, exact verified HTTPS origins; ownership journals and unresolved creates are not proof of cleanup until reconciled. Do not reuse the old auto-sharing `setupAnalystSession` helper for qualification.

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

### Unified Synthetic Traffic & Log Generation ([scripts/load_test/generate_synthetic_traffic.py](scripts/load_test/generate_synthetic_traffic.py))

For all synthetic test data generation, traffic simulation, and performance benchmarking, use the single authoritative unified generator script:

```bash
uv run python scripts/load_test/generate_synthetic_traffic.py [args]
```

It supports the following multi-tier targets:
- `--target local` (default): Writes synthetic Parquet batches directly into `cache/{bucket}/buffer/` and calls `commit_buffer` to materialize local DuckLake snapshots immediately. Extremely fast, zero cost, and 100% reproducible for local test runs.
- `--target fos`: Generates and uploads gzipped NDJSON chunks directly to Fastly Object Storage to test the full discovery, ledger, and commit crons.
- `--target clickhouse` (or `--with-clickhouse` alongside another target): Directly seeds ClickHouse fact tables (`request_facts`) and registers publication batches for high-scale serving verification.

Scenario Profiles:
- `--scenario diurnal` (default): Standard 24h day/night curve with normal CDN performance (~82% CHR).
- `--scenario ddos-spike`: A 15-minute attack spike with concentrated IPs, JA3 signatures, and 429/503 errors.
- `--scenario origin-5xx-outage`: A 30-minute backend database outage (502/503/504 MISSes, high OTTFB).
- `--scenario bot-scrape`: High-volume automated crawler activity simulating Googlebot, SEO tools, and scrapers.
- `--scenario slow-network`: High RTT, elevated TLS handshakes, and slow mobile connections.

Examples:
```bash
# Generate 100k rows over 24 hours directly to local DuckLake buffer and commit it:
uv run python scripts/load_test/generate_synthetic_traffic.py --target local --scenario diurnal --rows 100000 --commit

# Directly seed ClickHouse with 50k rows of diurnal traffic for high-scale:
uv run python scripts/load_test/generate_synthetic_traffic.py --target clickhouse --scenario diurnal --rows 50000

# Full E2E Pipeline (Upload 50k rows of diurnal logs to FOS and seed ClickHouse in parallel):
uv run python scripts/load_test/generate_synthetic_traffic.py --config configs/<logging-service-id>.json --target fos --with-clickhouse --rows 50000
```

### Edge-Load Client Traffic Simulation & Pipeline Lag Testing ([scripts/dev/scale_harness.py](scripts/dev/scale_harness.py))

While the synthetic traffic generator (`generate_synthetic_traffic.py`) seeds logs directly to bypass CDN fees, you must use `scripts/dev/scale_harness.py` to send real concurrency-controlled HTTP client requests to a deployed CDN edge endpoint to test the full live **VCL -> Fastly CDN -> FOS -> Discovery/Ingest -> Serving** pipeline.

It performs the following:
1. **Client Concurrency-Staged Requests:** Fires concurrent async requests using `aiohttp` to the specified `--url` based on stage profiles (defaults to stages of `100:10,500:10,1000:10` target RPS:seconds). Rates over 1,000 RPS require `--allow-high-rate`.
2. **Telemetry Checkpoints:** Queries Prometheus to measure p95 serving and pool wait latencies.
3. **Freshness Lag Sampling:** Periodically queries the backend's `/api/log-extents` to calculate the exact freshness lag (delay between the edge request event time and backend ingestion completion).
4. **Settle Window:** Settles for 60 seconds (customizable via `--settle-seconds`) after the load stages complete to allow delayed FOS log delivery to catch up before taking the final checkpoint.

Run the scale harness from the project root:
```bash
uv run python scripts/dev/scale_harness.py run \
  --url "$LOADTEST_URL" \
  --service-id "$FASTLY_SERVICE_ID" \
  --stages 100:30,500:30,1000:30 \
  --start-time "$START_UTC" --end-time "$END_UTC" \
  --output performance-report/scale-harness.json
```

### Multi-Environment Health Verification ([scripts/check_environment_health.py](scripts/check_environment_health.py))

To quickly check the three active target environments (Local Standard, GCE VM Standard, and Elevation K8s High-Scale) are up and listening correctly on their forwarded ports, use the `scripts/check_environment_health.py` script. Local High-Scale is retired; High-Scale verification runs in Elevation. The script is verified continuously by the `tests/test_environment_connectivity.py` integration test.

It performs the following checks:
1. **Connectivity:** Asserts that each environment is reachable on its designated ports.
2. **Health Endpoints:** Verifies that `/api/health` returns HTTP 200 within a 50ms latency budget and correct application version (`3.0.0-beta1`).
3. **Tenancy:** Resolves each environment's designated Fastly service ID and ensures multi-tenant isolation (i.e. invalid or unauthorized service IDs are strictly rejected).
4. **RBAC:** Verifies that unauthenticated requests to protected analytics endpoints are correctly blocked (returning HTTP 401).

Run it from the repo root:
```bash
uv run python scripts/check_environment_health.py
```

### Parallel Multi-Environment Deployment

Optional Standard Docker telemetry uses
`docker-compose.observability.standard.yml` plus its `.standard.prod.yml`
host-network overlay, **not** the existing multipod observability overlay.
It adds only private Prometheus/Tempo receivers. Export settings remain
operator-owned and default off. See [the deployment guide](docs/deploy/observability.md)
for the canonical runner's explicit opt-in and public
`python -m scripts.dev.verify_observability` delivery checks. The SDK probe proves
container transport, not running-worker coverage. A separate chosen-traceparent
health request proves normal HTTP telemetry from the running API instance.
The minimal Helm profile is `global.privateObservability.enabled=true`, not
the legacy full `observability.enabled` bundle. Verified private Prometheus
UI links in the canonical report expire within one hour; Tempo is API-only.
Neither proof is full qualification, all-endpoint coverage or durability.

`scripts/dev/deploy_test_all.sh` is a **local-only** script: it is gitignored (it embeds environment credentials) and is not in the public repo, so a fresh clone will not have it. The rules for using it, and what each run does, live in one place: [Canonical Multi-Tier Deployment Mandate](#canonical-multi-tier-deployment-mandate-never-deploy-or-test-by-hand).

```bash
bash scripts/dev/deploy_test_all.sh
# Or skip the 5-minute audit watch for fast iterations:
bash scripts/dev/deploy_test_all.sh --no-monitor
```

### Multi-Environment Auditing & Pipeline Monitoring ([scripts/dev/audit_environments.py](scripts/dev/audit_environments.py))

Comprehensive system, resource, and pipeline diagnostic tool designed to verify operational health across the three active deployment tiers (**Local Standard**, **GCE Standard**, and **Elevation High-Scale**). Local High-Scale is retired; High-Scale verification runs in Elevation.

**Audited Dimensions:**
- **System Vitals:** CPU load averages (1m / 5m / 15m) normalized against total vCPUs, RAM usage percentages, and root/data mount disk saturation.
- **Scheduler Health:** APScheduler heartbeat tick age, flagging schedulers lagging beyond threshold (>30s) or in-flight job freezes.
- **Cron History & Failures:** Scans the last 24h of task execution logs in metadata DBs, alerting on failed jobs (e.g. read-only constraint violations, sync timeouts).
- **Ingestion Currency vs FOS State:** Compares latest request/RUM timestamps to wall clock. Accurately distinguishes between genuine bucket idle states (0 raw `.gz` files in FOS bucket) and stalled ingest pipelines.
- **Parquet Storage & Compaction:** Monitors hourly partition sprawl in local cache, daily/weekly rollup tier status, and durable DuckLake data file count and byte footprint.
- **Pool Latency & Saturation:** Tracks DuckDB connection pool p95/p99 wait latencies and rejected acquisitions.
- **High-Scale Worker & Broker Health:** Audits Celery worker node counts, active queue depths, and ClickHouse ingestion pipeline status.

**Usage:**
```bash
# Point-in-time snapshot check across all 3 active environments:
python3 scripts/dev/audit_environments.py
# Or via Makefile:
make audit

# Continuous real-time monitoring watch (e.g. 5 minutes, 10s interval):
python3 scripts/dev/audit_environments.py --watch --duration 5 --interval 10
# Or via Makefile:
make audit-watch

# Deep audit chained after connectivity & tenancy verification:
python3 scripts/check_environment_health.py --audit --watch
```

### Stateful + property tests

Hypothesis `RuleBasedStateMachine` pattern at [tests/core/test_ingest_stateful.py](tests/core/test_ingest_stateful.py) — first example in the repo. The `@initialize()` rule MUST `metadata_db.teardown(service_id)` because Hypothesis runs many instances per pytest function and the per-test SQLite file is shared across instances (otherwise: `FlakyStrategyDefinition`).

## Shared Helpers

Single owners for patterns that were hand-rolled per call site until the 2026-10 consolidation. **Reach for these before writing a lock, loop, regex, cache, envelope or SQL fragment.** Ratchet gates (`make retry-guard`, `coalesce-guard`, `dateparse-guard`, `urllib-guard`; baselines only go down) fail CI on new bespoke copies. Tests monkeypatch the primitives at their canonical homes, so keep call-time imports as written in each module's docstring.

### Cron lifecycle ([backend/cron/scheduler.py](backend/cron/scheduler.py), aliased as `backend.cron.lifecycle`)
- `cron_run(service_id, task, ...)` is the per-run context manager every job body uses: resolve the source, acquire the `(service_id, task)` lease via `start_cron_run`, start/end progress, write a terminal `cron_runs` row with duration/status/exception, and run `finalize_*` backstops in a `finally`. Use `run.skipped`, `run.success(...)`, `run.warn(...)`, `run.fail(...)`, `run.record(...)`, `run.log_progress(...)`. Body exceptions are recorded and swallowed by default (`suppress_exceptions=True`); `error_summary` / `error_result` shape the auto-`error` row.
- `CronLeaseHeldError` ([backend/core/metadata/cron_log.py](backend/core/metadata/cron_log.py), a `RuntimeError` subclass) means the lease is held by another run: a **clean skip**, never an error (`cron_run` turns it into `run.skipped`). Do not catch bare `RuntimeError` around `start_cron_run`.
- `record_skip_row(source, task, summary, status="skipped", ...)` writes the single terminal row for a pre-flight skip that never takes the lease (nothing to evaluate, disk too full). Used by the compaction and metadata jobs.
- Sanctioned second lifecycle: HTTP-triggered thread spawns (`routers/admin/compaction.py`, `utils/router_utils.py`) own a run id and hand it in with `cron_run(run_id=...)`. A job that skips writing a terminal status leaves a `running` row that freezes ingestion (the deep health check flags it past `_STUCK_SYNC_RUNNING_MINS`; see Health probe).

### Fastly draft versions ([backend/provision/draft_version.py](backend/provision/draft_version.py))
`with draft_version(service_id, token, base_version=..., call=fastly, ...) as new_ver:` owns clone -> (your mutations) -> validate -> activate, and on any failure re-activates `base_version` (`rollback=True`; a failed rollback is logged at WARNING, never raised, so the original error surfaces). Validation that returns anything but `status == "ok"` raises. The Fastly client is passed in as `call` so each orchestrator keeps its own module-level `fastly` seam for tests. Do not hand-write clone/validate/activate again; `on_failure=` runs before the rollback, `rollback=False` + `rollback_to_version` is for callers that own the rollback.

### Service config writes ([backend/config.py](backend/config.py), [backend/utils/router_utils.py](backend/utils/router_utils.py))
- Every read-modify-write goes through `mutate_config(service_id, fn)`: `config_lock` (cross-process `flock` sidecar, bounded 5 s wait, re-entrant per thread, degrades to unlocked with `config_lock_degraded`), a fresh read that bypasses the in-process cache, `fn(cfg)` mutating in place, then an atomic save. Returns the saved config, or `None` when the service has no config. Routers use `mutate_service_config(service_id, fn)`, which maps `None` to the standard 404 `{"error": "Service not found"}`; `load_service_config` is the read counterpart.
- Multi-step changes that must roll back use `snapshot_config(service_id)` (raw persisted text or `None`) and `restore_config(service_id, snapshot)` under the same lock. Never `config_path.write_text(...)` a config.
- **cid-salt rules:** `cfg["rum"]["cid_salt"]` is write-once. Every save re-adopts the persisted salt (a stale writer cannot re-key pseudonymous cids); `restore_config` re-adopts the *current* salt so a rollback never undoes a rotation; an unreadable existing config aborts the save (`CidSaltReadError`) instead of minting a new salt. Only `backend.core.duckdb.rotate_cid_salt` may pass `allow_cid_salt_change=True`. A callback that wants to replace the whole dict must `clear()` + `update()`. Anything touching this path needs the security-reviewer pass.

### SQL helpers
- `escape_sql_literal(value)` ([backend/utils/sql_validator.py](backend/utils/sql_validator.py)) is the only way to put a string inside a single-quoted DuckDB literal (doubles `'`; S3 object keys are attacker-controlled). Do not write `.replace("'", "''")`.
- `backend.core.sql_ident` (`is_safe_ident`, `require_safe_ident(name, label)`) validates bare identifiers; the table-name slugger stays `_safe_table()` (Trap #4).
- [backend/core/field_registry.py](backend/core/field_registry.py) (aliased as `backend.core.metric_expressions`) owns null-default and metric fragments with golden-string tests: `origin_latency_us(cols, absent=...)`, `sum_or_zero(expr, zero=, where=)`, `column_or_default(cols, col, default, cast=)`, `coalesce(expr, default)`, `HIT_STATES`. Only expressions that are byte-identical in meaning are consolidated; a change to a fragment changes served SQL and rollup schemas, so update the golden tests in the same commit. New inline `COALESCE(` in repositories/rollup writers fails `make coalesce-guard`.

### Retry ([backend/core/retry.py](backend/core/retry.py))
`retry_call(fn, retry_on=pred, attempts=N, base_delay=, max_delay=, multiplier=, jitter=, max_elapsed=, before_retry=, sleep=)` is the one bounded-backoff primitive (tenacity underneath). The predicate decides retryable-vs-fatal; `before_retry` is the recovery seam (e.g. bust a view cache); `sleep=` is the test seam. A `for`/`while` + `try/except` + `sleep` loop is a hand-rolled retry and fails `make retry-guard` unless it is in the baseline. **Loops intentionally left bespoke** because `retry_call` cannot reproduce their timing or shape: `config.py` VirtioFS read loop (fixed 50 ms sleep that also runs after the final attempt), `fos_setup` bucket delete (step schedule 2 s then 5 s, not exponential), `orchestrator` storage init and the scorer readiness probe (`yield` status events / `status_cb` between attempts, custom linear/1.5x schedules), and the polling/SSE loops in routers (those poll, they do not retry). `core/fastly/client.py` uses tenacity directly (method-blind, documented).

### Caches ([backend/utils/telemetry.py](backend/utils/telemetry.py), aliased as `backend.utils.time_window` and `backend.utils.bounded_cache`)
`BoundedTTLCache(maxsize=, ttl_seconds=)` is the thread-safe LRU+TTL dict (`cachetools.TTLCache` behind a `threading.RLock`) for time-keyed caches. Register every module-level cache with `CacheRegistry.register("pkg.name", cache)` in the same change so the test harness drains it (`CacheRegistry.clear_all()`); clear in place, never rebind the module global (other modules import it by name — `duckdb._schema_cache` is the worked example). **Do not migrate snapshot-keyed caches** (storage `_view_cache`, `_snapshot_files_cache`, pointer/manifest caches — Trap #28): they invalidate on snapshot identity, not time.

### Dates ([backend/utils/date_utils.py](backend/utils/date_utils.py))
`parse_iso_utc`, `parse_window`, `parse_hour_key[_strict]` (`YYYY-MM-DD-HH`), `parse_day_key[_strict]`, `floor_to_hour`, `floor_to_day`, `ensure_utc` (naive = UTC, aware -> `astimezone`; never `.replace(tzinfo=UTC)` on an aware value), `iso_z`. Raw `strptime` / `fromisoformat` / `.replace(tzinfo=UTC)` in `backend/core` and `backend/repositories` is ratcheted by `make dateparse-guard` (`scripts/check_no_raw_dateparse.sh`: per-file baseline, counts only go down; lower the row when you migrate a site). Remaining sites are intentional (naive handling, documented parse bugs).

### HTTP layer ([backend/utils/router_utils.py](backend/utils/router_utils.py), [backend/utils/telemetry.py](backend/utils/telemetry.py), [backend/models/common.py](backend/models/common.py))
- **Errors:** `make_error(code, message=None, **extras)` builds the `{"error": code, ...}` detail; use it with `HTTPException`. At an `except` site whose text could leak internals (DuckDB paths, upstream Fastly bodies, cid salt) call `raise_internal(logger, exc, code=..., request=...)`: logs the full exception with an `error_id`/`request_id` and returns only those ids. `redact_error_text` / `scrub_secrets` (in `backend.utils.telemetry`, aliased as `backend.utils.structlog_config` and `backend.utils.error_redaction`; strip absolute server paths and registered cid salts) must stay on every analyst-visible error path. `@query_errors` is the decorator for query-backed handlers. Never `detail={"error": str(e)}`.
- `require_service_id(service_id)` is the only answer to a missing service id: **400 `service_id_required`** via `make_error`, never FastAPI's 422 body. The frontend and e2e key on that code.
- **SSE:** build every SSE route with `sse_response(stream)` (shared 5 s ping + passthrough headers, in `backend.utils.router_utils`, aliased as `backend.utils.sse`). `tests/utils/test_sse.py::test_routers_build_sse_only_via_sse_response` fails on a direct `EventSourceResponse(` under `backend/routers`.
- **Log throttling:** `log_once_per(key, interval_s=60.0)` ([backend/utils/log_throttle.py](backend/utils/log_throttle.py)) returns True at most once per interval per key; wrap any log line that can fire per request/row while a failure persists.
- **Typed responses:** every route has a `response_model` (`tests/test_openapi_typed_responses.py` keeps the untyped allowlist empty) defined in `backend/models/` (never inline in routers). Use `WireSafeModel` (`extra="allow"` and omits never-set fields, so the wire stays byte-identical to the dict the producer built) or `PassthroughModel` (`extra="allow"` only) as bases (in `backend.models.common`, aliased as `backend.models._base`), and `JsonRows` / `JsonDict` / `JsonAny` for DB-native values (Decimal, tz-aware datetimes, bytes) so they keep the `jsonable_encoder` encoding (`Z`, not `+00:00`) while OpenAPI advertises the row shape. Run `make gen-types` after changing a model, and alias generated types on the frontend via `Schema<'ModelName'>` in [frontend/types/api.ts](frontend/types/api.ts) rather than hand-writing duplicate TypeScript interfaces.
- **Admin SSE & cron triggers ([backend/routers/admin/high_scale.py](backend/routers/admin/high_scale.py), aliased as `backend.routers.admin._helpers`):** reuse `_stream_from_worker(work)` for queue-backed SSE streams and `run_admin_cron_trigger(service_id, task, job_fn, ...)` for manual cron trigger endpoints.

### Rollup & High-Scale DRY helpers ([backend/repositories/_rollup_planner.py](backend/repositories/_rollup_planner.py), [backend/high_scale/_filters.py](backend/high_scale/_filters.py), [backend/utils/countries.py](backend/utils/countries.py))
- `plan_rollup_window(...)` and `resolve_rollup_bounds(...)` in [backend/repositories/_rollup_planner.py](backend/repositories/_rollup_planner.py) own the `can_use_rollup` gate, time-bound resolution, and hourly/daily rollup path planning across repository queries.
- `build_multi_per_hour_bundles(...)` and `clean_multi_bundle_sentinels(...)` in [backend/core/rollups/_common.py](backend/core/rollups/_common.py) own multi-bundle rollup writing and sentinel cleanup.
- `get_pop_coords_sql_case(...)` in [backend/utils/countries.py](backend/utils/countries.py) (aliased as `backend.utils.pop_utils`) dynamically generates SQL `CASE` expressions for Fastly POP coordinates from `POP_LOCATIONS` (never hardcode coordinate `CASE` blocks in SQL).
- `backend/high_scale/_filters.py` centralizes ClickHouse filter/time-window SQL helpers (`_ch_parse_iso`, `_ch_escape`, `_normalize_filter_clauses`, `_compile_filter_clauses_sql`), `BaseClickHouseBatchAdapter` in [backend/high_scale/ingest_runner.py](backend/high_scale/ingest_runner.py) owns shared batch-adapter plumbing, and `_MultiServiceWorkerLoop` in [backend/high_scale/worker.py](backend/high_scale/worker.py) runs multi-service worker loops.

### Frontend DRY helpers ([frontend/components/RouteErrorBoundary.tsx](frontend/components/RouteErrorBoundary.tsx), [frontend/lib/log-fields.ts](frontend/lib/log-fields.ts), [frontend/lib/ssr/upstream.ts](frontend/lib/ssr/upstream.ts))
- `createRouteErrorBoundary({ logTag, title, description })` in [frontend/components/RouteErrorBoundary.tsx](frontend/components/RouteErrorBoundary.tsx) builds route `error.tsx` components.
- [frontend/lib/log-fields.ts](frontend/lib/log-fields.ts) owns pure state transitions (`applyToggleField`, `applyToggleGroup`, `applySelectAllInGroup`, `applyPreset`, `computeSelectedCount`) shared between the Provision Wizard and Log Field Manager.
- `resolveDefaultSsrRangeKey(...)` in [frontend/lib/ssr/upstream.ts](frontend/lib/ssr/upstream.ts) resolves default SSR time-range keys from `LogExtentsResponse`, and `getPageLoadId()` in [frontend/lib/api.ts](frontend/lib/api.ts) is the single page-load correlation ID source across `api.ts` and `analystFetch.ts`.

### Postgres metadata connections ([backend/core/metadata/pg_connection.py](backend/core/metadata/pg_connection.py))
Each thread holds one long-lived pooled connection (`get_pg_thread_connection`); `PgConnectionWrapper.execute` holds the wrapper's `_busy` lock while a statement is in flight. **Busy-lock rule (Trap #45):** returning a connection to the pool from another thread is only legal when that lock can be taken without blocking. Threads that end for good call `release_pg_thread_connection()` themselves (the cron heartbeat thread does).

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
All DuckDB table names derived from user-controlled values (service IDs, field names) must go through `_safe_table()` from [backend/repositories/_base.py](backend/repositories/_base.py). Never interpolate into SQL strings. Bare identifiers that are not table names (DuckLake aliases, rollup field names, lake tables) validate through `backend.core.sql_ident` (`is_safe_ident` / `require_safe_ident`) — one `fullmatch` owner; do not add a private identifier regex (`tests/core/test_sql_ident.py` fails on one). String literals go through `escape_sql_literal` (see [Shared Helpers](#shared-helpers)).

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

### 10. (Retired) The web-based analyst invite/join flow
Removed in v3 (see Personas). There is no `generate-viewer-key`, `/api/provision/join`, `InviteAnalystDialog` or wizard "join" mode, and no `/api/provision/decrypt-invite`.

### 11. All configs are schema v2
`lf = cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []}` — always.

### 12. NGWAF bot cache lives in PostgreSQL, materialized into DuckDB temp table
`ngwaf_bots` is stored in PostgreSQL (`pg_schema.py`) and materialized in DuckDB via `CREATE OR REPLACE TEMP TABLE {alias}_ngwaf_bots` (see `attach_ngwaf_cache` in [backend/repositories/_base.py](backend/repositories/_base.py)).

### 13. Next.js startup crash on restricted macOS
`ERR_SYSTEM_ERROR: uv_interface_addresses returned Unknown system error 1` on some hardened systems. `frontend/package.json` `dev` script binds `-H 127.0.0.1` to bypass interface enumeration. Don't drop the flag.

### 14. VCL regex right-hand side must be a string literal
The RHS of `~` or `!~` must be a literal. No variables, no concatenation. Use `regsub()` / `regsuball()` for dynamic logic.

### 15. Operational metadata lives in PostgreSQL 16, not DuckDB
Alerts, views, audit, cron history, ingested-file dedup, ASN names, source registration, slow-query history, usage telemetry, live share, and NGWAF bot cache live in PostgreSQL 16 via `METADATA_DSN` ([ADR-22](docs/adr/22-postgres-only-metadata.md)). Read/write via [backend/core/metadata/](backend/core/metadata/) (legacy `from backend.core import metadata as metadata_db` call sites resolve through the package's `_ShimModule` proxy) — never via DuckDB. PostgreSQL MVCC guarantees that background cron writers never block reader queries or cause file locking deadlocks.

### 16. Monkeypatches → catalog in [MONKEYPATCHES.md](MONKEYPATCHES.md)
No import-time patches remain as of v3.0: the s3fs and PyIceberg patches were retired together with `pyiceberg` and `s3fs`. If you add one, document it in MONKEYPATCHES.md with site, motivating incident, and cleanup path in the same commit.

### 17. MSW + openapi-fetch ordering — `server.listen()` must run at module load
`openapi-fetch` captures `globalThis.fetch` at `createClient` time. [frontend/lib/api.ts](frontend/lib/api.ts) creates its client at module load, so MSW's `server.listen()` MUST execute at the top of [frontend/vitest.setup.ts](frontend/vitest.setup.ts) — **not inside `beforeAll`**. If listen runs after lib/api.ts is imported, the captured fetch is the unpatched original and every test silently bypasses MSW. Symptom: handlers never fire, requests hit real loopback. Don't move that call into a hook.

### 18. `@testing-library/react` auto-cleanup is OFF when `globals: false`
Our [frontend/vitest.config.ts](frontend/vitest.config.ts) sets `globals: false`, so RTL doesn't register its own `afterEach(cleanup)`. [frontend/vitest.setup.ts](frontend/vitest.setup.ts) has an explicit one. Without it, earlier-test components stay mounted and pollute `screen.getBy*` (symptom: input values accumulate, e.g. `value="jane@example.comjane@example.com..."`).

### 19. `extractApiError` Error-instance trap
`Error.message` is non-enumerable, so `JSON.stringify(new Error('boom'))` returns `"{}"`. [frontend/lib/api.ts](frontend/lib/api.ts) `extractApiError` early-returns `error.message` for `instanceof Error`. Keep that check at the top if you refactor.

### 20. `RemoteAccessMiddleware` blocks admin routes over the live-share tunnel
The tunnel exposes the same FastAPI app to the public internet. Middleware classifies by `Host` and blocks remote requests from admin paths — including `/api/admin/share/*`. When you add an endpoint analysts must reach, register under `/api/share/*` or update `_is_blocked_path()`. Don't remove the `testclient`/`testserver` allow-list entries — they're what let pytest hit admin routes.

### 21. (Retired) `sync_data` orphan-cleanup vs local-compaction outputs
`sync_data` (a no-op under DuckLake) has been deleted, and with it the orphan-cleanup loop this trap described no longer exists. The incident it recorded (2026-05-31, 1.65M → 302K rows from deleting locally compacted outputs) is cited by [ADR-18](docs/adr/18-serving-tier-single-pod.md). If you add any code that walks the local cache and deletes files not in a catalog, it must skip `compacted_*.parquet`, `daily/` and `weekly/`.

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
Post-commit, buffer parquets are not unlinked — `tombstone_buffer_files` ([backend/core/storage/buffer.py](backend/core/storage/buffer.py)) writes a `.parquet.consumed-<ts>` sidecar and the file lingers for a grace window so views bound BEFORE the commit stay readable. Its rows are ALREADY in the hourly partitions, so any code reading the buffer dir directly (not via `buffer_files()`, which filters) must skip `_tombstoned_parquet_paths(buffer_dir)` or it double-counts — the dashboard's direct active-hour read (`_create_active_hour_temp_direct` in [_base.py](backend/repositories/_base.py), the ~6 ms replacement for the ~700 ms view path on the live slice) double-counted up to ~10 min of the freshest rows before the fix (prod 2026-07-07: 40 of 55 buffer files were tombstoned). New consumers of the live slice should reuse the per-request shared temp via `begin_shared_active_hour_temps()` / `end_shared_active_hour_temps()` rather than rolling their own read.

### 27. System-managed custom fields must be re-asserted on every `log_fields` write
Session scoring and CMCD each inject a canonical set of `log_fields.custom_fields` generated from code. `_is_system_field` ([backend/core/field_registry.py](backend/core/field_registry.py)) hides them from the user-editable list, so **every writer that persists `log_fields` from a list it did not author omits them** — the log-fields UI round-trip, a `state_sync` pull of a remote `admin_state.json`, and a provisioning reconcile whose `log_fields` was built from groups alone.

The omission is not cosmetic: `reconcile_vcl_state` regenerates the Fastly log format from the persisted config, so a dropped field leaves its extraction VCL installed and running while nothing writes its output to the log line. The column then ingests **empty string, not NULL** (the field is absent from the format, so ingest fills the schema default), the feature's `available` probe still passes because the DuckDB column exists, and the page renders all zeros with no error to explain why.

Two incidents, same root cause:
- **2026-06-02** — a `state_sync` pull (since removed with Path A) overwrote scoring's 8 fields on every ~30 s sync tick.
- **2026-08-12** — a production demo service lost all 14 `cmcd_*` fields. CMCD had been enabled since 2026-07-13 and never collected a single value; the cache-key probe (`?CMCD=` vs a control param) proved the snippet was still stripping CMCD from `req.url` the whole time. The trigger was `update_logging_endpoint` assigning `cfg["log_fields"]` **wholesale** with no merge guard, unlike its `cli.py` / `api_service_log_fields_set` siblings.

Route every such write through `reconcile_system_custom_fields` / `reconcile_cfg_system_custom_fields` in [backend/provision/system_fields.py](backend/provision/system_fields.py), and key it on the **current** feature state, never on a state transition — a reconcile that says nothing about the feature must still re-assert its fields, and a disable must strip them. The transition-only guard in `update_service_config` is exactly what let the affected config stay broken across a month of reconciles. **If you add a third system-managed feature, add its flag to `system_feature_flags` — do not add a fourth re-injection block.**

### 28. (Retired) Never resolve the Iceberg metadata pointer from an unpaginated listing
The pointer-resolution code this trap guarded (`_read_metadata_pointer`, `_list_metadata_json_keys`) was removed with PyIceberg. The general lesson stands: never take "latest" from the first page of a `list_objects_v2` result (it caps at 1000 keys and returns oldest first for zero-padded names). Paginate and order by parsed version.

### 29. Feature extraction VCL must be emitted BEFORE field capture
Generated `vcl_recv` puts two related blocks in one snippet: **extraction** parses a source into `req.http.x-<feature>:*`, and **capture** promotes it into `req.http.x-fos-edge-data:<field>` — and only the promoted header is what the log format reads. Emit capture first and it copies empty strings; the extraction then runs too late to matter.

**2026-08 CMCD outage.** `generate_capture_vcl` appended the `cmcd_enabled` block *after* the `get_capture_vcl_statements` loop (the deployed snippet had capture at lines 186-199 and extraction at 209-225). Every `cmcd_*` column logged empty. It hid for a month because the failure is completely silent in all three places you'd look:

- Nothing errors — the VCL is valid and Fastly's `validate` passes.
- The DuckDB columns exist, so the feature's `available` probe returns true and /streaming renders zeros rather than "not enabled".
- **The extraction genuinely works.** It still runs `querystring.filter(req.url, "CMCD")`, so an edge-side cache-key probe (`?CMCD=A` vs `?CMCD=B` vs a control param) proves CMCD is being handled — and that proof is real but says nothing about whether the value reached the log line.

The invariant is pinned by [tests/utils/test_cmcd_capture_ordering.py](tests/utils/test_cmcd_capture_ordering.py) for all 14 fields, in BOTH generators (`fastly_api.generate_capture_vcl` and `declarative/generators.generate_consolidated_snippet`). **When you add a feature that extracts into a `req.http.x-*` header for logging, order it before capture and add it to that test.** To verify a capture-stage field end-to-end, don't infer from edge behaviour — send a uniquely-tagged request and query for the tag.

### 30. The log-line budget silently truncates late custom fields
`generate_log_format` tracks an aggregate `budget` (`FASTLY_LOG_LINE_DELIVER_MAX`) and clamps each variable-length field to what's left: `cf_limit = max(0, min(cf_limit, budget))`, allocated greedily in `custom_fields` list order. Once the budget runs dry, later fields get `substr(x, 0, 0)` and **log null forever** with no warning.

On the affected service (groups A–M + 23 custom fields) this squeezed `cmcd_sid` to `substr(..., 0, 26)` — truncating 36-char UUID session ids to 26 chars and breaking session-level joins. Values logged before the squeeze are full length, so the same column holds both 26- and 40-char ids, which reads like a client quirk rather than a config effect.

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

All DuckLake attachments configure `DATA_INLINING_ROW_LIMIT 0` (`_ducklake_attach()`
in `backend/core/storage/_ducklake.py`). By default, DuckLake inlines small
inserts (≤10 rows) into the metadata catalog rather than writing parquet files
immediately. Setting `DATA_INLINING_ROW_LIMIT 0` ensures every insert writes
directly to Parquet data files at commit time. This guarantees immediate
durability, prevents rows from being trapped in the metadata catalog, and natively
eliminates upstream bug duckdb/ducklake#1495 (stale cached inlined tables across
multiple attaches) without requiring custom binaries or unverified extension patches.

DuckLake does not write parquet for a small INSERT. It **inlines** the rows straight into the metadata catalog (Postgres, or the `.ducklake` file), visible as `changes: {'inlined_insert': [...]}` in `ducklake_snapshots`. `ducklake_table_info` then honestly reports `file_count = 0` for a table holding real committed rows.

**Neither compaction primitive promotes inlined rows.** Verified empirically on a throwaway catalog: `ducklake_merge_adjacent_files` (celery path) and `ducklake_rewrite_data_files` (default path, via `optimize_table`) both leave `file_count = 0` with zero parquet on disk, because both operate on already-materialized files. A table whose every commit was inlined stays inlined forever no matter how often compaction runs. `ducklake_flush_inlined_data` is the ONLY primitive that promotes them.

This was a live bug on this branch and it is a **durability** bug, not a layout preference: `finalize_committed_raw` deletes the raw `.gz` once its ledger row has been committed for `RAW_DELETE_GRACE_S`, so with no flush the only copy of every ingested row is the catalog itself. Observed on the live test service before the fix: 27,613 committed files, 27,615 catalog snapshots, FOS `ducklake/` prefix **empty**, and 4 raw files left in the bucket. Losing the Postgres volume would have been total data loss with nothing to re-ingest.

Both compaction paths now flush first — `merge_lake_files` (celery) and `_optimize_table_impl` (default). **If you add a third write path, it needs the flush too**, and setting `DATA_PATH` to FOS is not a substitute: DATA_PATH only says where parquet goes *once written*. Pinned by `test_merge_lake_files_flushes_inlined_rows_to_parquet` and `test_optimize_table_flushes_inlined_rows_to_parquet` — both assert the durability property (`file_count > 0` after a run that starts at 0), never just that the call happens.

Corollary for readers: any code inferring "is there data?" from file count alone is wrong under DuckLake. Count rows, or consult inlined state. This is what broke `get_table_info` (see [ADR-14](docs/adr/14-ducklake-replacement.md)).

### 33. A catalog swap fails SILENTLY on the read side
When the v3 DuckLake cutover moved the write path off pyiceberg, every reader still built on pyiceberg's Table API kept working — it just reported an empty, frozen catalog forever, with no error. `get_table_info` / `get_snapshot_calendar`, the post-commit `table_summary.json` writer, `lake_info.py`'s fallback, and `_run_cloud_maintenance_impl`'s retention deletion were all found in this state at different times, each discovered separately.

The retention one was the worst: it is the ONLY enforcement of `data_retention_days` / `rum_retention_days`, so customer data retention silently never ran against DuckLake data. It also concealed a latent wipe — the original entered its conditional-prune branch whenever `rum_retention_days > data_retention_days` *including when `data_retention_days == 0`*, which resolves the cutoff to *now* and deletes every non-RUM row. That never fired only because the whole function was dead; making it live without gating on `> 0` would have shipped a data wipe.

**When you migrate a storage backend, audit every caller of the OLD backend's read API before calling the cutover done — not just the write path.** A grep of the cron job file is not enough; read the function it calls. `optimize_{id}` and `expire_{id}` sit side by side, look identical from the scheduler, and only one of them had been migrated.

### 34. (Retired) `cache/.../data/` is a MIRROR — never adopt it alongside the FOS manifests
The legacy-parquet adopter and the `ducklake_adopt` cron row were removed. v2 users tear down, upgrade, and provision fresh; nothing adopts a v2 Iceberg table or local cache. Two lessons survive for any future import code: `ducklake_add_data_files` is not idempotent (dedupe against `ducklake_list_files`), and `ducklake_list_files` echoes object-storage URIs verbatim (normalise by scheme, not `abspath`).

### 35. Concurrent `ATTACH ... AS lake` on different connections silently corrupts the catalog
`_ducklake_attach` had no synchronization. Two or more connections in the *same process* independently attaching the DuckLake catalog under the alias `lake` — which happens routinely, not just at boot: any burst of concurrent requests that need a fresh pooled connection (`duckdb_pool._Pool.acquire`'s "build fresh" branch) triggers this — race and corrupt each other's local catalog metadata. Every racing connection's `ATTACH` statement reports success (no exception), but `ducklake_snapshots('lake')` — and therefore the Iceberg-compat view rebuild, and therefore `earliest_log_at`/`latest_log_at`/`total_rows_total`/the Traffic-over-Time chart — then fails FOREVER on that connection with `Catalog "__ducklake_metadata_lake" does not exist`. Because pooled connections are long-lived, one bad race at connection-build time poisons that connection for its entire life in the pool; nothing ever retries or re-verifies the attach on reuse.

Verified empirically (`tests/core/test_ducklake_attach_concurrency.py`): 6-8 threads each opening their own connection and calling `_ducklake_attach` concurrently reproduced the corruption on 100% of the racing connections, every run — including against a purely local (non-Postgres) DuckLake catalog, so this is not a Postgres-catalog-specific race. A single ad-hoc connection, or connections serialized with a lock, never reproduced it.

**The fix is a process-wide lock around the ENTIRE `_ducklake_attach` body, including `INSTALL/LOAD ducklake`.** Locking only the `ATTACH` statements (not the extension load) fixed the corruption but introduced an intermittent hang under repeated concurrent test runs — extension loading isn't safe to race either. `INSTALL ducklake; LOAD ducklake;` must be inside the same critical section as the `ATTACH`, not run outside it.

A companion bug fed the same race: `_update_storage_view_locked` unconditionally called `_ducklake_attach(con, source, read_only=False)` even when `lake` was already attached READ_ONLY on that connection (the common case — `get_connection()` always attaches `lake` read-only for pooled connections; this function also runs on those SAME read-only request connections via the slow-path view rebuild, not only on cron's dedicated write connections). The mode-mismatched re-attach throws `database with name "lake" already exists`, which `_ducklake_attach`'s broad "already exists → return True" match swallows as a harmless no-op — but the re-attach ATTEMPT itself is what triggers the corruption. **Check whether `lake` is already attached (`SELECT 1 FROM duckdb_databases() WHERE database_name = 'lake'`) before calling `_ducklake_attach` again on the same connection; if it must attach, match the connection's actual read-only mode** — don't hardcode either value.

Trap #33's shape again: a swallowed "already exists"/"already attached" is not proof the catalog is functional. If you add a new caller that re-attaches `lake` on a connection that might already have it, verify with a real query (`ducklake_snapshots('lake')`), not by trusting that `ATTACH` didn't raise.

**A third instance of the same race, found in production (2026-09-22 GCE Remote Standard incident): raw, unlocked `con.execute("DETACH lake")` calls.** DETACH mutates the exact same shared/racy catalog state ATTACH does, but three call sites issued it directly instead of through the locked path: `duckdb_pool.py`'s `release()` (detaches `lake` from every connection returned to the pool — the highest-traffic of the three), `buffer.py`'s `_ducklake_write_connection()` (detaches the pool's read-only attach before re-attaching read-write for a commit), and `_ducklake_migration.py`'s legacy adoption. A raw DETACH from any one of these can rip `lake` out from under another connection concurrently inside a locked `_ducklake_attach` call, which observed production as `Catalog Error: Schema with name lake does not exist!` on connections that had attached and were already mid-transaction — dozens of per-file commit retries failing identically in the same batch, since they all shared the one now-poisoned connection. **Every DETACH of `lake` (or `__ducklake_metadata_lake`) must go through `_ducklake_detach()` in `_ducklake.py`, which wraps the same `_attach_lock`** — never call `con.execute("DETACH lake")` directly. Pinned by `test_ducklake_detach_is_serialized_by_the_attach_lock` in `tests/core/test_ducklake_attach_concurrency.py`.

**Pool release must not detach at all (2026-10-07 GCE dashboard 400s).** Routing `release()`'s DETACH through the lock serialized it but did not make it safe: connections to the same service file share one DuckDB instance, so a locked DETACH on release still evicts `lake` from every sibling connection mid-query. The dashboard bundle's parallel aggregates branch failed with `schema "lake" does not exist`, and the stale-view retry failed too. `release()` no longer detaches; `acquire()` still verifies or restores the attach on reuse. Pinned by `test_pool_release_keeps_lake_attached_for_sibling_in_flight_connection`.

**A fourth instance (2026-09-22 GCE cold-start incident): a read-only reader evicting a live writer's read-write attach on mode mismatch.** `_ducklake_attach`'s existing-attach handling used to detach unconditionally on ANY mode mismatch, symmetric in both directions. But a read-write attach already satisfies a read-only caller's needs (SELECT works fine against it) — there is no reason for a `get_connection(read_only=True)` reader that races a live writer (ingest/buffer/commit, or the startup Iceberg-view pre-warm, both of which need `read_only=False`) to force a downgrade. Doing so ripped the writer's in-flight attach out from under it mid-use, reproducing DuckDB core's `ResourceInUseException`: "Unique file handle conflict ... in the process of being detached" — and, once the writer's attach was gone, the follow-on `Binder Error: Failed to find attached database "lake"` seen in `ducklake_current_snapshot_id()`'s probe (that probe's `except Exception: return None` fallback is why the symptom self-heals rather than hard-failing — see `_ducklake_view_token()`'s "return None on failure, caller rebuilds" contract in `view.py`). This is not a transient timing race the existing `_attach_lock` retry budget can absorb — it's the reader *itself* destroying the writer's still-needed attach, deterministically, on any mode mismatch.

Fix: a read-only caller finding `lake` already attached read-write now reuses it as-is (verified live via `ducklake_snapshots('lake')`) instead of detaching it. Only a genuine write caller finding a read-only attach still needs to force the upgrade — DuckDB has no in-place "upgrade" primitive, so that direction still detaches+reattaches. Pinned by `test_readonly_attach_reuses_live_readwrite_attach_without_detaching` in `tests/core/test_ducklake_attach_concurrency.py`, using `_ducklake_write_connection()` (the real commit-path mechanism) to hold a genuine read-write attach open while a concurrent reader races it.

**A fifth instance (2026-09-23 GCE ~40-minute stall + live-recurring "Unique file handle conflict"): the fourth-instance fix didn't help because the writer itself was holding the attach far longer than any retry budget assumes.** `_commit_buffer_impl` (`backend/core/storage/buffer.py`) defined `_BUFFER_COMMIT_CHUNK_SIZE = 50` with a docstring claiming buffer files are committed in bounded chunks "to avoid OOM" — but the constant was dead code, never referenced anywhere in the function. The real implementation opened ONE `_ducklake_write_connection` (an exclusive DuckLake write-attach) and passed the ENTIRE buffered file list — potentially thousands of files representing hours of backlog — into a single `read_parquet([...])` commit, all inside that one continuously-held attach. Under a large backlog this single write-hold ran for minutes, far past the `_attach_lock`'s 20-attempt/~30s retry budget every other connection depends on (pooled readers via `get_connection()`, RUM's own commit/aggregate connections, other services' ticks) — so those callers exhausted their retries and failed with "Unique file handle conflict" / "retries exhausted" / "zombie lock timeout", cascading into 300s cron watchdog timeouts across `log_discovery`, `insights_prewarmer`, `commit`, `local_compact`, and `gap_heal`.

Fix: `_commit_buffer_impl` now iterates `files` in `_BUFFER_COMMIT_CHUNK_SIZE`-sized batches via a new `_commit_one_chunk()` helper, opening and detaching a **fresh** `_ducklake_write_connection` per chunk instead of one spanning the whole backlog — restoring the write-hold duration to roughly what the retry budget was designed to tolerate. The existing full-batch-then-per-file-fallback-with-quarantine logic is preserved, applied per chunk. If a later chunk's attach fails (transient contention), rows already committed by earlier chunks are kept — the function stops rather than discarding prior progress. Pinned by `TestCommitBufferChunking` in `tests/core/test_ducklake_layer.py` (asserts N/chunk_size attach cycles for a large backlog, and that partial progress survives a later-chunk attach failure). **Any future "read/process/commit the whole buffer in one shot" code path needs the same bounded-chunk treatment — an unbounded single write-hold defeats every lock-fairness mechanism above it, no matter how well-tuned the retry budget is.**

Separately investigated and NOT fixed: a writer's `finally: _ducklake_detach()` in `_ducklake_write_connection()` can still evict `lake` out from under a concurrently-open reader at commit-end (reproduced empirically, ~1/16 runs under 15 racing readers). This is intentionally left as a self-healing degradation, not a bug — the probe that would fail returns `None` and its caller rebuilds the view. Don't "fix" this with a broader lock; the existing graceful-fallback contract already covers it, and the observed production symptom (a single non-recurring INFO log line at cold start) confirms it self-heals in practice. Also verified separately: DuckDB's own `duckdb.connect()` never races on a brand-new file path in isolation (0 failures across repeated 8-thread tests with no ATTACH involved) — the race is specific to the ducklake extension's ATTACH/catalog-creation path, not general file-open contention, and is already fully covered by `_attach_lock`'s existing retry loop.

### 36. Single-host deploys: three compose files, and host bind-mounts must be owned `1000:1000`
On a single-VM Docker Compose deployment, bring-up must run **all three** compose files, in order: `docker compose -f docker-compose.yml -f docker-compose.prod.yml -f docker-compose.deploy.yml up -d --build`. Omitting `docker-compose.prod.yml` silently drops `network_mode: host`, the loopback-only binds, and the anti-XFF-spoofing hardening (`TRUSTED_PROXY_IPS`, `REQUIRE_PROXY_HEADERS`) — the stack still comes up, just unhardened and without port 443 published (site becomes unreachable over HTTPS while looking "fine" in `docker compose ps`).

Separately: the backend Dockerfile creates and runs as `app` (UID/GID 1000) and documents that bind-mount targets must be owned by that user (`RUN mkdir -p /app/configs /app/data /app/cache ... && chown -R app:app /app`), but this is only enforced **inside** the image — nothing enforces it on the **host-side** bind-mount sources (`data/`, `configs/`, `cache/` next to the compose files). If those directories drift to a different UID:GID, the container starts, then crash-loops on the first write with `PermissionError` / `OperationalError: attempt to write a readonly database` — it can sit like this for weeks if no write path is exercised until a restart forces one. Before restarting containers, verify `stat -c '%u:%g' data configs cache` reads `1000:1000`; `sudo chown -R 1000:1000` them if not.

### 37. Map prewarming must configure the worker before constructing any map

MapLibre shares its worker pool across map instances. `MapPrewarm` can run
before any visible map module, so it must call
`setWorkerUrl('/maplibre-gl-worker.mjs')` immediately after its lazy import and
before constructing a map. Otherwise it creates a worker with an empty URL:
the browser fetches the current page's HTML as a module, fails MIME checking,
and subsequent maps inherit the broken pool. Configuring the visible map later
does not repair workers already created.

Do not treat an attached canvas as proof that a map works. The hidden prewarm
canvas is 1px and can exist even when no country geometry is loaded. The map
E2E test supplies country data and requires rendered `countries` features on
the full-sized dashboard map; the unit test runs prewarm without another map
component to pin configuration-before-construction.

### 38. NGWAF dashboard cards require a configured workspace

`bootstrap.ngwaf_configured` is the active service's attachment flag, available
to both admin and analyst without exposing the workspace ID. `useDashboardCards`
excludes NGWAF Verified Bots and NGWAF Signals from the available card list when
it is false. Virtual fields must not bypass this gate: persisted card selections,
"Show all", or leftover WAF log fields do not establish that NGWAF is configured.
The header counts only available selected cards; saved preferences remain intact
when switching back to a service where NGWAF is configured.

### 39. Header freshness is request-event time, not a log-file timestamp

The REQUEST header uses `request.latest_log_at`, the request table's event-time
extent. A raw object's filename timestamp belongs in `latest_ingested_file_at`
or `latest_available_file_at`; it must never overwrite that extent in a cached
status overlay. Otherwise REST and SSE can report an older request than
`log-extents` and the persisted request metrics, and the header falsely looks
stalled. Bootstrap cache hits and misses must preserve the same meaning.

Use the current snapshot's request metrics, with a flat extent fallback only
when the request group is absent. Keep RUM separate. Explicit nulls clear
metrics; omitted fields in a partial SSE update retain their prior values.
Never choose the maximum of old and new timestamps: a reset or replacement
can legitimately move the extent backward.

In durable/Celery mode, `request_metrics_observer` owns a single backend
reconciliation loop: observe the durable serving view, persist, then publish
changed status through SSE. It waits 15 seconds after each completed pass;
connection/query time is additional. This is serving work and remains enabled
with development ingestion crons paused. It never starts an ingestion writer.
Sync/file mode keeps its post-batch refresh. See ADR-18 for the ownership rule.

### 40. METRICS-group catalog fields have no real column — exclude them from every per-field rollup writer

`LOG_FIELD_CATALOG` entries in the `METRICS` group (`requests`, `hit_rate`,
`5xx`, `4xx`, `p50_latency`, `p95_latency`, `p99_latency`, `throughput`,
`req_size`, `ttfb_ms`) all have `vcl=None` — they are chart-metric-only
synthetic concepts (`LogField.is_derived`), never a real per-row ingested
column. `FIELDS` in `backend/repositories/dashboard.py` includes them anyway
(no group-based filter), so anything that iterates `FIELDS` to build a
per-field rollup `SELECT <field>` ends up asking DuckDB to bind a column
that doesn't exist in the raw/buffer parquet.

Live incident (2026-09-14): `backend/cron/jobs/partial_hour.py`'s 30-second
merge tick hit this — `Binder Error: Referenced column "requests" not found
in FROM clause!` on every tick, for every service, immediately after a
deploy. `backend/core/rollups/_common.py::_get_fields` (the standard hourly
per-field rollup writer's field list) had the identical gap and would have
failed the same way on its next touched-hour tick; it just hadn't fired yet.

Fix: derive the exclusion from the registry itself
(`{f.code for f in in_group(Group.METRICS)}`), not a hand-maintained literal
list — `_LIVE_TOPN_SKIP_FIELDS` (`backend/repositories/_base.py`) and
`_get_fields` both fold this set in now. **Any new per-field rollup/merge
writer that iterates `FIELDS` directly must exclude this same set** — a
hand-curated skip-list will drift the moment a new METRICS field is added to
the catalog, which is exactly how this incident happened.

### 41. Incremental ingest must attempt one batch after an expensive LIST

The standard-mode `log_discovery` budget includes both FOS discovery and file
processing. On a high-object-count bucket, the four-hour incremental lookback
can consume the full 240-second budget before `ingest()` reaches its chunk loop.
Checking `max_seconds` before the first chunk then creates a permanent stall:
every tick lists the same backlog, processes zero files, and reports "No new log
files found" even while newer objects exist in FOS.

Always allow the first bounded chunk to run, then enforce the time limit before
subsequent chunks. This guarantees forward progress without removing the
per-tick bound. The regression is pinned in
`tests/core/test_ingest_timing.py::TestIngestMaxSeconds::test_expired_listing_budget_still_attempts_first_batch`.

### 42. DuckDB `threads` is instance-global — changing it at runtime deadlocks the process

`SET threads` on any connection changes the thread count for every connection to
that database instance. Each change makes DuckDB's `TaskScheduler::RelaunchThreads`
join worker threads while holding the scheduler mutex. On 2026-10-07 the GCE
backend froze: `get_connection` set `threads=DUCKDB_THREADS` (2), while the pool
fresh-build path (`DUCKDB_POOL_CONN_THREADS`, default 1) and the insights
prewarmer (`threads=1`) set it back. In gdb, one thread held the scheduler mutex
and joined a worker. That worker waited on a DuckLake multi-file scan lock. The
lock holder's nested metadata query was waiting on the scheduler mutex. Every
later query, `SET`, and `LOAD ducklake` hung behind it, so the 60s attach-lock
timeouts, `unhandled_error` on the dashboard, and permanently skipped
`log_discovery`/`commit` ticks were all downstream symptoms.

Set threads only through `_ensure_threads` in `backend/core/duckdb.py`, which
issues `SET` only when the value differs. Never add per-connection or per-job
thread overrides on a file-backed service connection. Throttle background work
with worker counts instead. Separate `:memory:` connections are separate
instances and may set their own value before running queries. `memory_limit` is
also instance-global. Pinned by `tests/core/test_duckdb_global_threads.py`.

### 43. Request lag comes from serial work on the ingest tick, not the poll interval

Freshness SLA is 10–20 s. Both regressions that broke it came from slow serial work
hidden inside an ingest loop.

**Standard.** `log_discovery` used to run `recompute_touched_hours` and
`recompute_wellknown_bots_rollup` inline. On a busy service those take 90–170 s, so
with `max_instances=1` most ticks were skipped, and lag rose past a minute. Rollups
now go through `schedule_post_ingest_rollups` in `backend/cron/jobs/sync.py`, a
per-service single-flight daemon worker that coalesces hours queued while it runs.
**Do not move rollup or other heavy refresh work back onto the discovery tick.**

**High-scale.** The historical per-object worker loop processed ~3 s of round trips per 1–3 row object: FOS GET/PUT, ~15 Postgres control transitions, and ~7 ClickHouse publish+verify pairs. Under heavy load, serial per-object processing starved ingest and drove up lag. The batched page ingest pipeline overhaul (`backend/high_scale/orchestration.py`, `archive_publication.py` (`archive_writer` alias), `ingest_controller.py`, `postgres_control.py`) resolves this via four core architectural optimizations:
1. **Batched Page Ingest & Archive Amortization:** Ingest operates on pages of 50–100 objects (up to `HIGH_SCALE_BATCH_SIZE_RECORDS`), decompressing and decoding concurrently in Arrow memory, and writing a single merged Parquet archive artifact to FOS per page. This amortizes the ~1.2 s FOS PUT and checksum verification round-trip across the whole batch rather than paying it per object.
2. **Single Batch Manifest per Page:** Each page generates one `ArchiveManifest` (`batch_{manifest_id}`) registered in Postgres, tracking all member source keys, SHA-256 digests, and per-source replay byte offsets. Replay and retention deletions remain granular per source while control metadata transactions scale per page.
3. **Collapsed Publish Handshake:** Replaced ~15 per-object control transitions with 3–4 batch transactions per page: `claim_sources_batch`, `register_batch_manifest`, `mark_batch_published`, and `acknowledge_sources_batch`. Bulk multi-row INSERTs into ClickHouse per table are verified in a single pending→visible publication gate, drastically cutting Postgres and ClickHouse round-trips.
4. **Key-Range Partitioned Cursors:** Sources are partitioned by key hash (`hash(source_key) % partition_count`) under a single owner epoch in `control_cursors`. Worker replicas acquire exclusive claims per partition range without contending or locking each other out on the `sources` table, enabling linear scaling across worker replicas.
Page objects run on a bounded thread pool (`HIGH_SCALE_OBJECT_CONCURRENCY`, default 4) with pool-backed Postgres connection reuse. Size worker counts against Postgres `max_connections`.


### 44. RUM and request ingest must share the parse, quarantine, and deletion contract

Standard-mode RUM ingest (`rum_sync` / `ingest_rum_logs`) drifted from request
ingest (`log_discovery` / `ingest`), creating several severe failure modes:
1. Discovery scanned the full `raw/rum/` prefix every tick instead of using bounded
   incremental minute-prefixes (`rum_minute_list_prefix`), burning host CPU and
   causing 20–50s ticks that starved request discovery.
2. Raw objects were never deleted inline upon buffer commit; `cleanup_old_rum_logs`
   only deleted after 90 days. Because `metadata_cleanup` trimmed `ingested_files` after
   1 day, subsequent full scans re-ingested hundreds of thousands of aged raw beacons
   as duplicate rows.
3. Errors and corrupt lines used an inline parser that silently dropped rows missing
   timestamps, rather than quarantining them with exact bytes via
   `_quarantine_rum_corrupt_lines`.
4. Deletion exclusion sets were missing, risking data loss on unreadable or capture-failed objects.

Always enforce the unified contract across both streams:
- Parameterize minute-prefix discovery and `StartAfter` bounds by prefix subpath.
- Delete each raw object inline per chunk under `resolve_raw_delete_after`, excluding
  unreadable objects and objects whose quarantine capture failed.
- Share the parser (`_parse_rum_beacon_file` / `_parse_rum_line`) and quarantine flow.
- Record durable rows in `ingested_files` (0 for files that produced no rows for that table).
- Enforce the `max_seconds` budget with the first chunk always allowed to run (Trap #41).

### 45. Never return a busy Postgres connection to the pool from another thread

`close_all_pg_connections()` drains every tracked thread connection (test teardown, shutdown). Handing a libpq connection back to the pool while its owner thread has a statement in flight frees it under the running query and segfaults or cuts off the owner (the 2026-10 background-query shutdown failures). Each `PgConnectionWrapper` therefore carries a `_busy` `RLock` held for the duration of `execute`; the drain takes it with `acquire(blocking=False)` and **skips** (leaves tracked, for a later drain) any connection it cannot take. Rules: never `putconn` / `_return_to_pool_once` a connection you do not own without taking `_busy` non-blocking first; never block on `_busy` from the drain path (a hung query would hang shutdown); a thread that is ending calls `release_pg_thread_connection()` for its own connection. Test: `tests/core/test_pg_thread_connection_stale.py`. The same shape applies to the `_checked_out` WeakSet: do not replace it with a strong list (it pinned dead threads' connections until the pool timed out).

### 46. Gate non-SSR-seeded store/query state on `useMounted()` in route chunks (`React #418`)

In Next.js App Router, `<RootLayout>` (`StoreHydrator`, `useBootstrap`, `useAdminEventStream`) mounts and executes effects **before** late-loaded route chunks (such as `app/admin/page.js`) finish hydrating. During that gap:
1. `StoreHydrator` / `useBootstrap` sets `useServiceStore.activeServiceId` (which is `null` during SSR when no `?service=` URL param is present).
2. `useAdminEventStream` (`SSE_SYNC_STATUS`, `SSE_USAGE_COST`) or fast local queries (`useSystemMetrics`, `useBotSources`, `useQuarantineSummary`, `useShareSettings`, `useSyncStatus`) can populate React Query / Zustand caches before the route's first client render.
If a route component reads `activeServiceId` or non-SSR-seeded query data directly during its initial render, the client's first hydration pass will render populated DOM (e.g. `?service=xyz` hrefs, populated tables, or live metrics) where SSR rendered `null` or a skeleton, triggering `Minified React error #418` in `e2e/hydration-smoke.spec.ts`. Always gate `activeServiceId` (when not backed by a URL param) and non-SSR-seeded query/SSE data on `const mounted = useMounted()` so the first client pass matches SSR HTML byte-for-byte.

### 47. Pin both halves of a DuckLake-plus-buffer view to the same commit boundary

The buffer branch captures fixed Parquet paths, so the committed DuckLake branch
must use `AT (VERSION => snapshot_id)`, not the advancing current table. Otherwise
a buffer commit makes an already-bound view read those rows twice: once from
DuckLake and once from the retained buffer files. A shared rollup session can
persist those duplicates into closed-hour bundles.

Construct the view under the existing per-service lock and cache the snapshot
actually pinned, never a later snapshot probe. If the committed snapshot cannot
be verified, fail rather than publishing a partial or unpinned view. Do not hold
a transaction or lock across rollup execution, or unlink buffers early.
`test_rollup_count_preserves_rows_when_buffer_commits_mid_recompute` in
`tests/core/test_ducklake_layer.py` checks exact counts during a real commit and
freshness on the next checkout. Previously corrupted bundles still require an
explicit operational rebuild; changing the view does not repair them.

Daily derivatives need independent repair when a reader selects the daily tier.
Never rebuild a daily file from whichever hourly files remain after retirement:
that can replace a complete day with a partial day. A scoped day repair must
recompute all 24 closed UTC hours from retained events and publish the daily
replacement only after complete success. Keep intermediate repair outputs away
from the normal compactor; a failed repair must leave the existing day intact.

### 48. A shared `raw/rum/` object belongs to one domain but carries both

High-scale `rum_vitals` and `rum_errors` discovery both list `raw/rum/`. The
source ledger is `UNIQUE(service_id, object_key)`, so the first domain to see an
object claims it. Decoding only the claimed domain silently dropped the other
domain's rows, and the raw object was then deleted. `ingest_controller.py` now
decodes the sibling domain from the same payload, keeps only rows passing
`row_matches_serving_domain`, and publishes them under
`build_batch_id_from_manifest_id(f"{manifest_id}:{sibling}")` before the
source is acknowledged. Sibling rows are not counted, archived or dead-lettered
as the claimed domain's rows. Do not "fix" this by giving each domain its own
ledger row or by resetting cursors. Pinned by
`test_rum_errors_claim_also_publishes_sibling_vitals_from_the_shared_object`.

**An error-owned archive is not an error-only archive.** Inspect its exact
`sources_json` slices and deduplicate expanded copies of each `raw_json` by
source version and original line ordinal before diagnosing loss. Empty
`error_message` rows can be genuine vitals; archive row counts minus error-fact
counts do not prove missing errors. Faro's exception discriminator is authoritative,
not the edge envelope's metric fields or query string. `_rum_kind` prevents an
exception from becoming a false vital when those fields are repopulated by the
fallback. Message-less exceptions retain the standard parser's `Unknown error`;
flat errors need an actual discriminator (stack, file or exception kind), not
merely a schema column containing an empty string. Invalid metrics and flat
records with neither kind are dead-lettered with their original bytes.
Mixed bodies can have a valid claimed-domain row and an invalid sibling;
count sibling rejections separately rather than silently dropping them.
Custom Faro events have the standard `event_<name>=1.0` projection, while
navigation attributes and measurements can expand to multiple metrics.
Input-event counts alone do not prove valid-vital counts.

For historical repair, `scripts/recover_high_scale_rum.py` defaults to a
read-only dry run for exactly one unambiguous terminal manifest. Reconstruct
both domains from retained original lines; preserve version, ordinal and
transform. Check physical facts across **all** batch IDs and their visible
publication receipts, including semantic equivalents with changed event IDs.
Hidden, duplicate, conflicting, expired or unprovable identities block apply.
Review the plan digest and counts before `--apply --approve-plan`; serialize
repairs and hold the terminal-source/current-owner fence throughout apply.
Generation is the manifest's **owner epoch**, not its archive epoch. Stable
missing-set batch IDs plus pre/post identity checks make retries safe beyond
ClickHouse's block-dedup window. Never substitute a generic rebuild, blind
replay, cursor reset or source-ledger edit. The recovery runbook documents the
bounds and the post-deploy procedure.

### 49. PoP health cannot drop a partial leading hour

Standard PoP health's whole-hour rollup collector skips a partial leading hour,
but its live merge restores only the active-hour tail. A window starting partway
through an hour must use the existing scoped raw fallback, not serve a truncated
rollup result. Keep hour-aligned starts eligible. The real-DuckDB regression in
`tests/repositories/test_rollup_reader_coverage_golden.py` checks both dispatch
paths against retained events over the same 24-hour bounds.

### 50. The Standard-mode cloud copy is the only off-box copy of committed data

Standard keeps DuckLake Parquet on local disk and the catalog in Postgres. The cloud copy under `{prefix}/ducklake/` (`backend/core/storage/backup.py`) is the only thing that survives losing both, because the raw `.gz` are deleted after ingest when `delete_after` is on. The v3 cutover silently dropped the old "push to the cloud after commit" step (the commit interval kept its "Cloud Commit Interval" label while only committing locally); this restores it.

- **The FOS listing is the only state.** Nothing about the copy is recorded in Postgres, so it stays usable when the catalog is lost. Each push lists `ducklake/main/<table>/`, uploads catalog-live files that are missing or differ in size, and deletes keys the catalog no longer references.
- **Never delete when the live set is empty.** An empty `ducklake_list_files` means a wiped or unreadable catalog, not "everything was compacted away"; deleting then would destroy the backup. Pinned by `test_push_to_cloud_never_deletes_when_the_catalog_reports_no_live_files`.
- **Hook every writer of the file set.** `schedule_cloud_push` runs after a commit that committed files, after the RUM commit, and after `optimize` and `expire`. A new job that adds, rewrites or removes DuckLake data files needs the same call. It is single-flight per service and skipped in High-Scale, without a bucket and under `FLA_DEV_NO_CRONS=1` (so a dev checkout never pushes to a real bucket).
- **Reset and teardown must purge the copy**, or a restore would bring deleted data back: `reset_service_logs` / `reset_service_rum` delete `ducklake/main/<table>/` through `_purge_ducklake_remote_dirs`. Teardown relies on its bucket or prefix deletion options.
- **Restore** (`restore-data` CLI, `restore_from_cloud`) downloads missing files, creates absent tables from the Parquet schema and registers files with `ducklake_add_data_files`, which is not idempotent: it skips files `ducklake_list_files` already lists. It also rebuilds the Postgres `ingested_files` index from each restored row's `_source_file`, so raw `.gz` still in the bucket are not ingested twice; other operational metadata (cron history, usage log) is not restored, and raw objects that produced no rows are simply read again.
- **Window of exposure:** data ingested since the last successful push exists only locally. The push cost model is in the Usage page calculator (Class A per file, 30-day minimum billing on rewritten files).

### Bounded ClickHouse diagnostic index (ADR-20)

`backend/core/clickhouse_{manifest,publication,rows,export}.py` (`clickhouse_schema` folded into `clickhouse_publication.py`) implements
explicit pinned DuckLake snapshot export to immutable Fastly Object Storage
artifacts, with six Postgres control relations. It is not an ingest mirror.
Task 12 rejected this prototype for dashboard serving after Task 10's measured
latency/error/saturation failures; Task 11 Kubernetes packaging was not executed.
Both dashboard endpoints call the existing DuckLake repository directly,
regardless of `CLICKHOUSE_ENABLED`. That flag enables diagnostic index/replay
tooling, not dashboard routing; do not add a separate serving flag.
`repositories/clickhouse_dashboard.py` is an archived experimental evaluator
for direct offline library comparisons, never imported by a live endpoint.
Run `make clickhouse-prototype-schema`
and `python -m scripts.clickhouse_replay`; see
[the runbook](docs/runbooks/clickhouse-prototype.md).

Use real psycopg pool transactions, not the SQLite-shaped metadata wrapper
whose `commit()` is a no-op. All fact reads require `FINAL`. Only complete,
content-verified generations may activate; rebuilds allocate a new generation
and load every artifact, including previously published ones. Expired datasets
and unverified targets are ineligible. The archived hybrid evaluator must pass
the snapshot actually pinned to its DuckLake view into `readiness`; a separate
current-version probe is not proof. The adapter builds a unique request view
using `AT VERSION` and the existing normalization helper, never replaces the
pooled service view, and rolls back its read transaction on exit. Readiness
re-verifies the entire target on every request (intentionally costly).
Unsupported requests log a DuckLake fallback reason; transport failures
raise `HybridUnavailable` in direct evaluation. No dashboard HTTP route handles
that exception anymore. Raw deletion and the ingest ledger remain unchanged.

Task 8 instrumentation lives in `clickhouse_metrics.py` and
`clickhouse_client.py` (`clickhouse_observer` alias). Use multipod + observability + ClickHouse overlays
together: the existing OTLP exporter feeds Prometheus, with a recording alias
for its byte-counter naming difference. Enabled backend callbacks share a
bounded authenticated disk probe and a Postgres actionable-backlog aggregate;
never run full generation readiness in a metric callback. Unknown lag/disk
is not zero. Physical inserted-row counters include acknowledged retries.
Query IDs belong in logs, never metric labels; Active Queries entries have
`con=None` and cannot interrupt the shared HTTP client. Replay commands
force-flush before exit; SDK resource instance IDs isolate concurrent
cumulative streams. See the runbook for live names and verification evidence.

Task 9 adds API-only admin controls in `routers/admin/high_scale.py` (aliased as `routers/admin/clickhouse.py`):
`GET /api/admin/clickhouse/status?service_id=...` and
`POST /api/admin/clickhouse/replay`. Both remain behind the existing admin
prefix block; never allow them through the analyst middleware. Status reports
service-scoped publication history and actionable backlog age. Disabled or
unavailable measurements are null, not zero; enabled dependency failures return
503. An expired active generation remains visible with `expired=true`; health
is connectivity/schema health, not a fresh-data guarantee.

Replay requires a configured read-write service and exactly one `dataset_id`
(new generation) or `generation` (resume). `dry_run=true` validates retained
metadata references without FOS access, audit writes, or manifest mutations.
The HTTP cap is 100 artifacts per request **and per dataset**, since activation
verifies the whole dataset even after a small final replay batch. Work runs
synchronously in a request thread with bounded Postgres waits; larger datasets
use the explicit CLI. Apply uses only `FosArtifacts.load`, never export, and
audits requested/completed/failed operations with safe counts. Responses,
including debug-opted-in errors, must not disclose artifact URLs or credentials.

## AI Agent Directives

These apply to every change, regardless of scope.

### Operational Protocol: Work Autonomously Within the Requested Scope

This project overrides the global requirement to keep asking questions until every detail is confirmed. Once the goal is clear, carry the task through research, planning, implementation, review, testing, and routine fixes without approval checkpoints between steps.

1. **Read & Synthesize First:** Read the relevant page or cron specification (`docs/pages/{page}.md`, `docs/cron/jobs/{cron}.md`), architecture documents, deployment runbooks, and tests. Resolve questions from those sources before asking the operator.
2. **Make Routine Decisions:** Choose the simplest approach consistent with existing patterns. Make reasonable, reversible assumptions and keep moving. Do not stop for plan approval, implementation approval, routine test runs, reviewer dispatch, or ordinary fixes to findings within scope. Skill workflows requiring those checkpoints yield to this rule.
3. **Ask Only When Blocked:** Ask when missing information materially changes the requested outcome and cannot be resolved from the repo, or when an action needs explicit authorization. Do not expand the task to unrelated work.
4. **Document Decisions:** Keep authoritative documentation current with material behavior and design decisions. Routine research and tests do not need a documentation or approval checkpoint.
5. **Verify the Result:** Run relevant tests, lint, and formatting; verify affected deployment-mode and role behavior and telemetry attribution. Report actual blockers rather than waiting for approval to run ordinary checks.
6. **Keep Explicit Boundaries:** Commit only your own changes and push feature/release branches with `git push origin HEAD` without asking. Never push main/master, force-push, create or merge PRs, publish, deploy, or perform destructive operational actions without fresh explicit approval. Autonomous development does not authorize deployments; the deployment procedure below applies only after approval.

### Canonical Multi-Tier Deployment Mandate (Never Deploy or Test by Hand)

1. **Always use `scripts/dev/deploy_test_all.sh` after committing and pushing**: NEVER deploy, restart, refresh port-forwards, or test environments manually by hand (e.g. running ad-hoc `docker compose up`, `kubectl set image`, or manual background port-forwards). The project provides `scripts/dev/deploy_test_all.sh` as the single canonical, robust, and repeatable deployment script. You MUST craft an intentional commit for your change and push it to upstream origin (`git push origin HEAD`) BEFORE invoking `deploy_test_all.sh`. The script will NEVER auto-commit or auto-push, and will leave any other uncommitted work-in-progress files in the working directory untouched.
2. **Three-Tier Parallelism & Drift Prevention**: The script first verifies that HEAD is already pushed to upstream origin, then concurrently builds, updates, establishes tunnels/port-forwards, and verifiably tests across the 3 active environments:
   - **Local Standard** (Docker Compose, `http://localhost:3000` / `http://127.0.0.1/dashboard`): rebuilds and relaunches via `docker compose up -d --build`.
   - **GCE Standard** (Remote VM via SSH tunnel, `http://localhost:3001`): pulls the latest commit over SSH, runs `~/restart.sh --no-wait`, and forwards ports `3001`/`8001`.
   - **Remote High-Scale** (Kubernetes cluster via port-forward, `http://localhost:3002` / operator-configured analyst URL): deploys the frontend, backend, and worker images with the Jenkins tags, injects the current `COMMIT_HASH` into the frontend pods, waits for rollouts, and forwards ports `3002`/`8002`.
3. **No Interactive Blocking in Automation**: The script automatically defaults Jenkins tags to the short 12-char commit hash (`$COMMIT_HASH`) and bypasses interactive `read` prompts when running in non-interactive/automated agent shells (`! -t 0`) or when `BACKEND_TAG` / `FRONTEND_TAG` are pre-set in the environment.
4. **Mandatory Post-Deploy Verification**: Every run executes `scripts/verify_dashboard.js` against all 3 active endpoints to guarantee commit parity and live rendering before work is deemed complete.
5. **Mandatory 5-Minute Stability Monitoring**: Following successful dashboard verification, `deploy_test_all.sh` invokes `scripts/dev/audit_environments.py` to continuously monitor CPU, memory, scheduler tick rate, DuckDB connection pool latency, and ingestion health across the 3 active environments in real time for 5 minutes (`--watch --duration 5 --interval 10`) to confirm zero post-rollout regressions (customizable via `MONITOR_MINUTES=N`, skippable for rapid iterations via `--no-monitor` / `MONITOR_MINUTES=0`).

### Testing

1. **Use the 3-tier workflow** (focused test while coding, `make fast-ci` smoke, `make ci` once before pushing) — details in [Testing Philosophy & Speed](#testing-philosophy--speed-the-3-tier-workflow).
2. **Never delete or weaken existing tests** to speed up CI; coverage, vitest and security-regression floors are ratchets — see [Test Preservation Policy](#test-preservation-policy-never-delete-tests).
3. **Add tests for every non-trivial change:** New endpoint → router test. New utility → unit test. Bug fix → regression test that would have caught it.
4. **Extend canonical domain test suites instead of creating one-off test files:** Always add new unit, integration, or E2E test cases to the existing canonical domain file (e.g. `tests/core/test_clickhouse.py`, `tests/repositories/test_security.py`, `frontend/__tests__/app/dashboard.test.tsx`, `frontend/e2e/pages/dashboard.spec.ts`) rather than spawning a new `test_<feature_or_ticket>.py` / `.spec.ts` file per PR.
5. **Prefer integration tests over pure mocks** for backend behavior. The `in_memory_duckdb` + `client` fixture pattern tests real SQL while staying fast.
6. **Test error paths:** Missing config, external 4xx/5xx, empty DB.
7. **Frontend tests live in `frontend/__tests__/`** mirroring source structure (`app/`, `components/`, `hooks/`, `lib/`).
8. **Verify in the real app when you can.** Start the server, drive the UI, watch the logs (we log every query and FOS call). Don't rely on green tests alone for feature correctness.
9. **Run the Playwright suite as part of the dev-verify checklist.** Alongside the `verify-dev-first` flow (`./run.sh --dev` on 18002/13002), run `cd frontend && npx playwright test --project=chromium` for any change touching the admin shell, dashboard, provision wizard, custom-field drawer, or share-login. The suite spawns its own backend on 18004 + frontend on 13004 via [frontend/playwright.config.ts](frontend/playwright.config.ts) so it doesn't collide with the dev shell on 18002/13002. Use `--project=chromium,firefox,webkit` before pushing if the change touches browser-only interactions (DnD, popovers, chart hover).

### Code Changes

10. **No backward-compatibility shims.** Fields like `stats_token` and `cdn_domain` do not exist in the schema — do not add fallbacks for absent fields.
11. **Rule of 150 lines & ≥2 callers for sub-modules:** Do not split code into a new `_sections/`, `_sql/`, rollup writer, model file, or helper module unless it is reused by 2+ independent callers OR exceeds ~150–200 lines of cohesive logic on its own. Prefer data-driven config tables/loops and shared helpers in existing domain modules (`_base.py`, `network.py`, `router_utils.py`, etc.) over copy-pasting boilerplate across files.
12. **Never interpolate user-controlled values into SQL.** `_safe_table()` for table names, `sql_ident.require_safe_ident` for other bare identifiers, `escape_sql_literal` where a literal cannot be bound, parameterised queries (`con.execute("... WHERE x = ?", [value])`) for filter values.
13. **Handle `openapi-fetch` errors explicitly.** Never `.then((r) => r.data?.x || fallback)` without checking `r.error`.
14. **Keep Python imports at module level.** Conditional mid-function imports trigger `UnboundLocalError` (Trap #2).
15. **Run `ruff format` before committing** (or rely on `make ci`).

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
- **Unified `QueryExecutor`** for retry — stale-view and compaction-race are different error classes with different recovery costs; collapsing them creates a leaky abstraction. `backend.core.retry.retry_call` is deliberately *mechanics only* (attempts, backoff, budget, predicate); error classification and recovery stay at each call site.
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

<!-- graft:start -->
## Graft — repo context graph

This repo is indexed in `graft/`: small linked markdown nodes that explain each
system and carry exact file:line spans, kept in sync with the code through git.

For ANY task here — understanding how something works, finding where code lives,
or scoping a change — get context from the graph before grepping or opening
source files. Re-ask freely (it's cheap) and reuse literal identifiers you
already have (symbol, error string, file name) as the query. New to this repo?
Run `graft map` first — a token-budgeted orientation (dir clusters, hubs,
hotspots), no LLM, no key.

- Run `graft ask "<your question>" --source` → ranked nodes with the relevant
  code spans inlined (each hit's ≤8-line crux by default; `--full` for whole
  definitions when the crux isn't enough). Match the tool to the task shape:
  for understanding or editing, the top node IS the answer — cite its
  `covers:` file:line spans and edit straight from `--source`. For
  exhaustive tasks ("every occurrence / every caller of this pattern"), ranked
  results are top-N, not complete — run `graft grep "<literal>"` instead
  (exhaustive over indexed files, grouped by enclosing symbol), falling back
  to raw `grep -rn` only for unindexed files.
- `graft skeleton <file>` → every definition's signature + span, ~10× cheaper
  than reading the file; use it to skim an API surface.
- `graft callers <symbol>` gives precomputed, exact edges — who calls this.
  Add `--direction out` for what it calls, or `--depth N` to walk
  transitively for the full blast radius. For structural questions, skip
  ranking and use this directly.
- Or browse: `graft/INDEX.md` lists every node; follow the links.
- Monorepos and folders of multiple repos rank fairly across sub-projects —
  hits carry `[scope/]` labels naming which one they're from. Narrow with
  `graft ask "<task>" --in <scope>/` once you know where you're working.

If a returned span is truncated ("+N more lines"), open the file at that exact
range before finalizing. Only open source files when a node genuinely lacks a
needed detail, and then at the exact file:line the node points to — never
re-read whole files.

After big code changes, refresh the graph with `graft build` (deterministic,
no API key, $0).
<!-- graft:end -->
