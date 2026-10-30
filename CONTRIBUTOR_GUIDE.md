# Contributor Reference

Architecture, implementation patterns and numbered correctness traps. Read the sections relevant to the task, not this entire document on every session. [AGENTS.md](AGENTS.md) is the authoritative operating policy; it takes precedence over workflow examples here.

## How to use this file

- **First time in this repo:** start with Overview and Architecture, then read the relevant subsystem and numbered traps.
- **Returning:** read the relevant traps before changing their subsystem.
- **Companion docs:**
  - [README.md](README.md) — user-facing overview, features, install. Not duplicated here.
  - [docs/adr/](docs/adr/) — architecture decision records (ADR-14 DuckLake, ADR-16 ingest ledger, ADR-18 serving tier, ADR-21 high-scale, ADR-22 Postgres-only metadata).
  - [docs/pages/](docs/pages/) and [docs/cron/jobs/](docs/cron/jobs/) — per-page and per-cron specifications.
  - [docs/deploy/](docs/deploy/) — deployment runbooks; [docs/runbooks/](docs/runbooks/) — high-scale operations.
  - [MONKEYPATCHES.md](MONKEYPATCHES.md) — catalog of import-time patches (update it in the same commit if you add or remove one).
- **Trap numbers are stable identifiers** (code comments and docs cite them). Gaps in the numbering are removed entries; never renumber.
- **When to update this file:** any structural change — new endpoints, background jobs, schema changes, newly discovered traps. See [Keeping This File Current](#keeping-this-file-current).

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

FastAPI + Next.js dashboard for Fastly Real-Time VCL logs streamed to Fastly Object Storage (FOS). It continuously ingests `.gz` log files into DuckDB + DuckLake (Standard mode) or ClickHouse (High-Scale mode) and exposes per-service analytics, alerts, NGWAF bot detection, custom log fields, edge session scoring, RUM, and a live-share feature for read-only analyst access.

**Stack:**
- Backend: FastAPI, DuckDB, DuckLake, PostgreSQL 16, Celery/Valkey (High-Scale and optional Standard broker), APScheduler, boto3 (S3-compatible FOS), `uv`, `ruff`, `mypy`, `pytest`
- Frontend: Next.js 16, React 19, TanStack Query v5, Zustand, shadcn/ui, openapi-fetch, vitest, Playwright
- Storage: FOS (raw logs + durable cloud copy), local-disk DuckLake Parquet (Standard), ClickHouse (High-Scale), PostgreSQL 16 for all operational metadata and DuckLake catalogs ([ADR-22](docs/adr/22-postgres-only-metadata.md))
- Optional: [`falco`](https://github.com/ysugimoto/falco) VCL linter — detected via `shutil.which("falco")`, degrades to regex checks when absent

**VCL editing:** a log format string, custom field `vcl_log_expression`, or any VCL snippet must pass `falco lint`. Use `+` for string concatenation; wrap literals in heredoc strings (`{"literal"}`). Call sites: [backend/core/vcl_generator.py](backend/core/vcl_generator.py), [backend/provision/fastly_api.py](backend/provision/fastly_api.py), [backend/routers/services/custom_fields.py](backend/routers/services/custom_fields.py).

**Deploying:** see [docs/deploy/](docs/deploy/). Single-host production runs `docker-compose.yml` plus the `docker-compose.prod.yml` overlay (never a plain `docker compose up` in production); Kubernetes uses the Helm chart in [deploy/chart/](deploy/chart/).

## Architecture

### Data layout

| Layer | Location | Purpose |
|---|---|---|
| Raw logs | `s3://{bucket}/{prefix}/raw/request/**/*.gz` and `.../raw/rum/**/*.gz` | Immutable gzipped request and RUM JSON from Fastly |
| Local buffer | `cache/{bucket}/` | Transient Parquet between ingest and commit |
| DuckLake table | Catalog in Postgres (`DUCKLAKE_CATALOG`, required in all modes); data Parquet on local disk under `/app/data/services/{service_id}/ducklake/` | Long-term transactional storage; all queried Parquet is local for zero-latency scans. FOS is the raw-log ingress and the durable cloud copy: in Standard, `backend/core/storage/backup.py` pushes catalog-live Parquet to `{prefix}/ducklake/<path>` after every commit, and `python -m backend.provision.cli restore-data --service-id <id>` rebuilds local data from it ([Trap #50](#50-the-standard-mode-cloud-copy-is-the-only-off-box-copy-of-committed-data)). |
| Admin state | `s3://{bucket}/{prefix}/meta/admin_state.json` | log_format_history, audit_logs, views, custom_fields (not alerts — alerts are per-instance). Scoring matrix and history live beside it under `meta/`. |
| DuckDB | `data/services/{service_id}.duckdb` | Per-service analytical engine only: session-scoped `logs` view + temp tables |
| Metadata DB | PostgreSQL 16 (`METADATA_DSN`, mandatory) | `alerts`, `views`, `audit_logs`, `cron_runs`, `sources`, `ingested_files`, `asn_names`, `slow_queries`, `usage_log`, `usage_log_hourly_summary`, `remote_invites`, `remote_sessions`, `remote_share_audit_logs`, `share_settings`, `share_tos_versions`, `ngwaf_bots`, `rdns`, … (schema in [backend/core/metadata/pg_schema.py](backend/core/metadata/pg_schema.py)) |
| Service configs | `configs/{logging_service_id}.json` | Credentials, settings, `log_fields` config (gitignored) |

The DuckDB `logs` view stitches the DuckLake table and the local Parquet buffer so queries see all data without caring which layer holds a row. With `DEPLOYMENT_MODE=high_scale`, serving uses ClickHouse (or ephemeral in-memory DuckDB over durable state) and never opens native service files or local buffers. `DEPLOYMENT_MODE=standard` (default) keeps synchronous ingest and file-backed serving. Both modes require `METADATA_DSN` and `DUCKLAKE_CATALOG`.

### Package map

Large modules are split into packages whose `__init__.py` re-exports the historical public surface via `install_shim_module` ([backend/utils/module_shim.py](backend/utils/module_shim.py)); the proxy forwards `monkeypatch.setattr` / `delattr` to the owning submodule so tests patching the package path still work. Import from the package; patch where the code under test looks the name up.

| Package | Contents |
|---|---|
| [`backend/core/storage/`](backend/core/storage/) | `catalog.py` (DuckLake attach/detach, secrets, DDL), `view.py` (`logs` view stitching, `execute_with_stale_view_retry`), `buffer.py` (local Parquet buffer, commit, tombstones), `compaction.py` (local tiered compaction, `optimize_table`, `run_cloud_maintenance`), `backup.py` (FOS cloud push/restore) |
| [`backend/core/metadata/`](backend/core/metadata/) | `pg_pool.py` (pooled psycopg connections), `pg_schema.py` (schema bootstrap), `admin_state.py`, `alerts_db.py`, `state_sync.py`, `usage_log.py`, `fos_usage.py`, `cron_runs.py`, `slow_queries_db.py`, `ingest_log.py`, `quarantine_evidence.py`; `base.py` re-exports the surface |
| [`backend/core/ingest/`](backend/core/ingest/) | `discovery.py` (FOS LIST, watermarks), `parser.py` (JSON parsing, schema coercion, quarantine routing), `writer.py` (Parquet staging, ledger, post-sync rollups), `batch_worker.py` (Celery batch conversion, `finalize_committed_raw`, `merge_lake_files`) |
| [`backend/core/rollups/`](backend/core/rollups/) | `_common.py`, `recompute.py` (`HOUR_WRITERS`), `bundle_builder.py`, `day_bundles.py`, `day_compaction_sql.py`, `partial_hour.py`, `rollup_coverage.py`, `overview_rollups.py`, `time_series_rollups.py`, `session_rollups.py`, and domain writers `origin_dims.py`, `network_dims.py`, `security_dims.py`, `security_bots.py` |
| [`backend/core/clickhouse/`](backend/core/clickhouse/) | `constants.py`, `params.py`, `client.py`, `health.py` |
| [`backend/core/request_telemetry/`](backend/core/request_telemetry/) | `instruments.py` (OTel), `query_registry.py` (live query registry + instrumented DuckDB connection), `tracker.py` (`RequestTelemetry`, request-metrics observer), `middleware.py` |
| [`backend/core/`](backend/core/) singles | `duckdb.py` (connections, `_ensure_threads`), `duckdb_pool.py`, `duckdb_status.py`, `reset.py`, `catalog.py` (`LOG_FIELD_CATALOG`), `log_fields.py`, `presets.py`, `vcl_generator.py`, `metric_expressions.py`, `insight_definitions.py`, `request_context.py`, `retry.py`, `sql_validator.py`, `rum_ingest.py`, `lifespan.py`; `fastly/`, `oauth/`, `realtime/`, `share_db/` |
| [`backend/cron/`](backend/cron/) | `scheduler/{progress,lifecycle,runner}.py` (`Scheduler`, `@cron_task`, `cron_run`, `record_skip_row`), `jobs/{_common,sync,commit,rum_sync,rum_commit,rum_ledger,metadata,compaction,maintenance,duckdb_recycle,metric_snapshots,insights_prewarmer}.py`. Import `get_scheduler`/`Scheduler`/`cron_task`/`cron_run` from `backend.cron.scheduler` and each `_run_*` body from its `backend.cron.jobs.*` module |
| [`backend/routers/`](backend/routers/) | One module per domain; packages `admin/`, `services/` (`core`, `crud`, `vcl`, `custom_fields`, `views`, `storage`), `session_scoring/`, `share_admin/`, `rum/`; `root_system.py` (middleware, exception handlers, health/debug) |
| [`backend/repositories/`](backend/repositories/) | `_base.py` (`QueryRunner`, temps, telemetry), `rollup_executor/` (top-N, time-series, scalar readers), `top_n_rollups.py`, `utils/` (filters, response cache, `response_shaping/`), per-domain modules; `insights/` (`definitions/`, `rollup_fastpath.py`, `repository.py`, `shaping.py`), `security/` (`sql`, `watchdog`, `aggregates`), `sessions/` (`rollup_reader`, `queries`, `scoring`) |
| [`backend/models/`](backend/models/) | Pydantic schemas: `analytics/`, `common/`, `admin/` |
| [`backend/provision/`](backend/provision/) | `orchestrator.py`, `fastly_api.py`, `fos_setup.py`, `cli.py`, `cmcd_fields.py`, `rum_assets.py`, `rum_orchestrator_v2.py`, `declarative/` (reconciler), `session_scoring/` |
| [`backend/high_scale/`](backend/high_scale/) | `archive/`, `postgres_control/`, `ingest_controller/`, `insights/`, `worker.py`, `query.py`, `query_service.py`, `recovery.py`, domain readers (`dashboard`, `origin`, `performance`, `security`, `network`, `sessions`, `cmcd`, `rum`), `sql/` schemas |
| [`backend/utils/`](backend/utils/) | `telemetry/` (structlog config, Prometheus metrics, log flusher, proxy, system stats), `bot_sources/`, `tunnel/`, `remote_access.py`, `router_utils.py`, `error_utils.py`, `sse.py`, `date_utils.py`, `bounded_cache.py`, `time_window.py`, `hll.py`, `pii_masking.py`, `countries.py`, `pop_utils.py` |

Request-scoped context: [`backend/core/request_context.py`](backend/core/request_context.py) bundles `service_id`, `source`, `con`, `telemetry`, `analyst_session`, `cached_temps` and the opaque AES-GCM session-detail token helpers (`seal_session_token`, `open_session_token`). It folds `require_service_access` into construction, so no path builds a context without enforcing tenancy.

**Env / config handling.** There is no central settings class. App-level env vars are read via `os.getenv` at their use sites (e.g. `OTEL_EXPORTER` in `request_telemetry/`, `STRUCTLOG_FORMAT` in `utils/telemetry/`, pool tuning in `duckdb_pool.py`). Per-service credentials/settings live in `configs/{id}.json`, loaded by [backend/config.py](backend/config.py).

### Personas

- **Admin** — full ingest/management surface. Config: `configs/{logging_service_id}.json`.
- **Analyst — live shared instance.** Remote read-only access through the admin's running process over an HTTPS `public_endpoint`; no FOS credentials. Detected on the frontend by `bootstrap.settings.is_remote_analyst`. See [Live Dashboard Sharing](#live-dashboard-sharing). An analyst holding only bucket credentials cannot reconstruct a DuckLake catalog, so there is no "independent analyst instance" mode; do not add one.

## Ingest Pipeline

Two data planes are selected by `DEPLOYMENT_MODE`: `standard` (synchronous DuckDB + DuckLake ingest) and `high_scale` (ClickHouse ingest with micro-batching workers; see [docs/runbooks/high-scale-capacity-and-recovery.md](docs/runbooks/high-scale-capacity-and-recovery.md)).

**Standard mode.** APScheduler runs these per service, plus process-global maintenance jobs (see [Scheduler](#scheduler-backendcron)). Job ids are `<name>_{service_id}`:

| Job | Schedule | Function |
|---|---|---|
| `log_discovery` | every `log_period` sec (floored at 5 s) | LIST FOS `raw/`, download new `.gz`, transform to Parquet, update the DuckDB view, flush usage log, run cleanup |
| `commit` | every `commit_interval_mins` (default 5) | Commit the local Parquet buffer into DuckLake, flush usage log, then push new data files to FOS on a single-flight background thread (`schedule_cloud_push`; `optimize`, `expire` and the RUM commit also trigger it) |
| `local_compact` | every 2 min (`LOCAL_COMPACT_INTERVAL_MIN`) | Compact local-only hourly/daily Parquet |
| `partial_hour_merge` | every few seconds (`partial_hour_merge_interval_sec()`), when enabled | Merge the active hour's rollup slice; local-only |
| `rollup_heal` | hourly at :05 | Re-run idempotent `backfill_missing_hour_bundles` (48 h lookback) so closed hours missed by the per-sync recompute get rollups; local-only |
| `rollup_compact` | daily 02:00 UTC | Consolidate closed-day per-hour rollup Parquet into per-day files (30-day deep pass); local-only |
| `optimize` | daily 04:00 UTC | `CALL ducklake_flush_inlined_data` **then** `ducklake_merge_adjacent_files` and `ducklake_rewrite_data_files` for all lake tables (`logs`, `client_vitals`, `client_errors`). The flush is a durability step ([Trap #32](#32-ducklake-inlines-small-commits--only-ducklake_flush_inlined_data-makes-them-durable)). Manual: `POST /api/admin/optimize/{service_id}` |
| `expire` | hourly (`expire_interval_mins`, default 60) | `run_cloud_maintenance`: (1) retention deletes gated independently on `data_retention_days > 0` / `rum_retention_days > 0` (`0` keeps forever; an unguarded `0` resolves the cutoff to *now* and would delete everything); (2) snapshot expiry and queued-file cleanup, unconditionally; (3) purge old local cache Parquet and orphaned temps older than 2 h; (4) purge old local rollup Parquet; (5) quarantine evidence is never age-evicted. Each step's failure is recorded (`*_error`, `warning` run) and later steps still run. The per-service/per-task `expire_snapshots` lease lives in `job_runs`. Manual: `POST /api/admin/expire-snapshots/{service_id}` |
| `full_sync` | every 6 h (:30 UTC) | Exhaustive FOS LIST of `raw/` reconciled against `ingested_files`; LIST ops are attributed to `usage_log` as `cron.full_sync`. Manual: `POST /api/admin/full-sweep/{service_id}` |
| `gap_heal` | every 30 min (configurable) | Compares Fastly log-accounting metrics to ingested events (24 h lookback); on sustained loss (≥2 consecutive completed hourly buckets with ≥5 % deficit) triggers a targeted full sweep with severity bands (`mild`…`critical`). Manual: `POST /api/admin/gap-heal/{service_id}` |
| `rum_sync` | every `log_period` sec | RUM beacon ingest (see [Trap #44](#44-rum-and-request-ingest-must-share-the-parse-quarantine-and-deletion-contract)); triggers `rum_commit` immediately when new rows arrive |
| `rum_commit` | post-sync + 1 min fallback | Drain `client_vitals` / `client_errors` buffers into DuckLake, recompute aggregates, persist RUM extents, emit `sync_status` SSE |
| `metadata_cleanup` | daily 03:15 UTC (configurable) | Postgres retention pruning: expired `ingested_files` (guarded by `delete_after` and the RUM safety window `max(ingested_files_days, log_retention_days + 1)`), chunked purge of detailed `usage_log` (hourly summary kept), `cron_runs` (7 d), `slow_queries` (30 d), global `metric_snapshots` (30 d). Zero FOS calls. Manual: `POST /api/admin/metadata-cleanup[/{service_id}]` |
| `alerts_evaluation` | every `log_period` sec | Evaluate alert rules against recent windows; dispatch Slack / PagerDuty / webhook notifications. Gated on `_service_has_alerts` and `should_defer_cron("alerts")` |
| `insights_prewarmer` | ~240 s | Warm Insights caches (see [Insights](#insights)) |
| `ngwaf_sync` | `cron_ngwaf.interval_mins` (default 5), when an NGWAF workspace is linked | Sync VERIFIED-BOT requests |

Process-global jobs: `duckdb_recycle`, `metric_snapshot` (SRE sampler), `bot_data_refresh`, `rdns_enrichment`, `share_audit_purge`. Per-job specs: [docs/cron/jobs/](docs/cron/jobs/). `FLA_DEV_NO_CRONS=1` prevents registration and execution of ingest crons; `FLA_DEV_LOCAL_CRONS=0` also removes the local-safe jobs.

**High-Scale mode.** Ingest runs in dedicated `high-scale-worker` processes micro-batching directly to ClickHouse and FOS archive Parquet, backed by partitioned cursors and multi-source `ArchiveManifest`s. The backend serving tier is single-pod; ingest scales horizontally across worker replicas. The scheduler additionally registers `ledger_sweep`, `rum_discovery` and `ledger_rum_sweep` jobs in this mode.

RUM worker discovery keeps `high_scale_source_cursors.source_cursor` as the forward position and adds per-domain `reinspection_cursor` / `forward_pages_since_reinspection`. Every ten successful forward pages the next RUM listing inspects one bounded page through the same ledger/claim/archive path (resumed with `StartAfter`, never rewinding the forward cursor). This is eventual late-arrival reconciliation, **not a 20-second guarantee**. See the capacity runbook before changing cursor handling.

Teardown removes jobs on the next `_sync_jobs()` reload; a `config not found, skipping` warning during teardown is a job that fired after its config was deleted and is harmless.

### Local-only Parquet compaction

Implemented in [backend/core/storage/compaction.py](backend/core/storage/compaction.py) to keep dashboard scans fast without FOS write cost:
1. `local_compact` scans local cache dirs and merges small hourly files into size-capped Parquet (default ≤ 256 MB) to preserve DuckDB parallelism.
2. A compact-on-sync thread merges immediately after a raw sync that produced multiple files.
3. Partitions older than 1 day (`LOCAL_COMPACT_DAILY_TIER_DAYS`) bin-pack into `daily_YYYY-MM-DD_<uuid>.parquet`; daily files older than 30 days bin-pack into `weekly/weekly_YYYY-WXX_<uuid>.parquet`. All files are capped at `_MAX_PARTITION_BYTES`.

Use local compaction for hot-tier dashboard performance; use `optimize` when compaction must also be reflected in FOS. Code that walks the local cache and deletes files not in a catalog must skip `compacted_*.parquet`, `daily/` and `weekly/`.

### Atomic ingest (in-flight manifest + deterministic buffer names)

The window between `write_to_buffer` and `insert_ingested_files` is crash-protected by an `ingest_in_flight` table: `record_in_flight(source_name, buf_filename, file_rows)` persists `(file_name, row_count, file_size)` **before** the Parquet is written; the Parquet filename is `batch_{sha256(sorted_chunk)[:16]}.parquet` (deterministic, so re-ingesting a chunk overwrites instead of stacking duplicates); `insert_ingested_files` commits the tuples; `clear_in_flight` drops the row. `_recover_in_flight(src)` runs first on every `ingest()`: buffer present → promote to `ingested_files` and clear; buffer missing → drop without touching `ingested_files` (the next LIST re-ingests). Both halves are idempotent. If ingest crashes mid-LIST the next tick re-LISTs from the same `StartAfter` marker, wasting at most ~10 Class A ops, which is intentional.

### Health probe

`GET /api/health` is cheap liveness. `GET /api/health?deep=1` also verifies per-service ingest freshness (`max(ingested_at) FROM ingested_files` and the latest terminal `log_discovery` cron run) and returns 503 when any service is `degraded`: last ingest older than `stale_minutes` (default 30, widened per service to its own historical p95 gap between non-empty ingests so a low-traffic service's quiet periods don't false-positive), last discovery errored, a discovery row stuck in `status='running'` past `_STUCK_SYNC_RUNNING_MINS`, or the latest `commit` errored. Celery mode additionally derives freshness from `ingest_ledger` (see [ADR-16](docs/adr/16-ingest-ledger.md)). Postgres-only, never FOS or Fastly; safe for a load balancer. Deep health requires authentication when `ADMIN_GATEWAY_REQUIRED=1`.

## VCL Log Format & Custom Fields

Generated by `generate_log_format()` in [backend/core/vcl_generator.py](backend/core/vcl_generator.py) from the registry in [backend/core/catalog.py](backend/core/catalog.py) (`LOG_FIELD_CATALOG`, groups; presets in `presets.py`). Output is a single-line JSON VCL string.

**Field groups:** always-on Core HTTP; A Request Identity; B Cache Deep-Dive; C Infrastructure; D Geolocation Basic; E Geolocation Precision (requires D); F Network Quality Core; G Network Quality Deep (requires F); H Security: TLS Fingerprinting; I Security: Proxy & Anonymization; J WAF/NGWAF (UI only when NGWAF is linked); K QUIC/HTTP3; L Origin Metrics; M Image Optimizer Metrics. A `METRICS` pseudo-group holds chart-metric-only synthetic fields (see [Trap #40](#40-metrics-group-catalog-fields-have-no-real-column--exclude-them-from-every-per-field-rollup-writer)).

- `format_hash` — SHA-256 of the generated format; detects drift between deployed VCL and local config.
- `FASTLY_LOG_FORMAT_SAFE_MAX` (default 12,000 chars, env-overridable; Fastly's own limit is ~16 KiB) is enforced before deployment.
- `default_off` fields (e.g. `cookie_session`, group H) are excluded from `resolve_enabled_fields` even when their group is enabled; they need explicit per-field opt-in. Don't "fix" their absence by enabling the group.
- `generate_capture_vcl()` in [backend/provision/fastly_api.py](backend/provision/fastly_api.py) injects per-hook code (recv, miss, pass, fetch, error, deliver) populating log variables. The captured `ip` prefers a non-empty operator-set `req.http.Fastly-Client-IP` and falls back to `client.ip`; a priority `-100` recv snippet scrubs a client-forged `Fastly-Client-IP` at the true edge. Edge-hop detection is `fastly.ff.visits_this_service == 0`, a per-hop value a client cannot forge.
- `log_format_history` records format changes with before/after group lists, added/removed fields and actor.

### Custom fields

Admins define VCL fields appended to the log format. Storage: `configs/{service_id}.json` → `log_fields.custom_fields[]`. API: `/api/services/{service_id}/custom-fields` (CRUD, lint, export/import) in [backend/routers/services/custom_fields.py](backend/routers/services/custom_fields.py). Per-field schema (`name`, `label`, `vcl_log_expression`, `snippets`, `duckdb_type`, `value_type`, `collection_stage`, `bytes_estimate`, `enabled`, `show_in_dashboard`, `show_in_logs`, `filterable`) is defined in the `backend/models/` custom-field models.

All configs are schema v2. Always load with the v2 default:

```python
lf = cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []}
```

## Key Systems

Brief summaries; click through to source.

### Scheduler ([backend/cron/](backend/cron/))
A single `BackgroundScheduler` owned by the `Scheduler` class in [backend/cron/scheduler/runner.py](backend/cron/scheduler/runner.py); `_sync_jobs()` adds/removes per-service jobs on `reload()`. The `@cron_task` decorator (telemetry context, usage-log flush, watchdog hard-cap) and `cron_run` live in [backend/cron/scheduler/lifecycle.py](backend/cron/scheduler/lifecycle.py); per-run progress in `progress.py`, streamed over SSE. Job bodies live in `backend/cron/jobs/` (`sync` holds discovery, full sweep and gap heal; `maintenance` holds `optimize`, `expire`, `metadata_cleanup`; `metadata` holds NGWAF sync, alerts, bot-data refresh, rDNS, share-audit purge and the process-global recycle/snapshot entry points). `duckdb_recycle` (bounds DuckDB's object-cache growth) and `metric_snapshot` register once per process. Every job body owns its lease/progress/terminal-row cycle through `cron_run` — see [Shared Helpers](#shared-helpers).

### NGWAF bot detection
`GET https://api.fastly.com/ngwaf/v1/workspaces/{id}/requests` is synced by `_run_ngwaf_bot_sync` ([backend/cron/jobs/metadata.py](backend/cron/jobs/metadata.py)) using the writers/readers in [backend/core/rollups/security_dims.py](backend/core/rollups/security_dims.py) and [security_bots.py](backend/core/rollups/security_bots.py) (JSON:API pagination via `meta.next_cursor`). Rows land in the Postgres `ngwaf_bots` table and enrich log rows by `waf_req_id` + `waf_sig LIKE '%VERIFIED-BOT%'`. The workspace-listing response key is `"data"`; never `or`-chain it ([Trap #3](#3-falsy-empty-list-in-or-chains)).

### Alerts / saved views ([backend/routers/alerts.py](backend/routers/alerts.py))
Both live in Postgres (`alerts`, `views`). Alerts are threshold-based with webhook fire. Views capture a filter set + time range.

### Insights
Availability is declared in `INSIGHT_DEFINITIONS` ([backend/core/insight_definitions.py](backend/core/insight_definitions.py): `required_fields` + `required_groups`); the page renders each card generically. Every definition must declare a `category` (`InsightCategory` in [backend/repositories/insights/__init__.py](backend/repositories/insights/__init__.py); a registration without one fails Pydantic at import). A new insight in an existing category needs no frontend change; a new category needs a tab entry in [frontend/components/Insights/InsightTabsAndControls.tsx](frontend/components/Insights/InsightTabsAndControls.tsx). Set `required_groups=[]` when the gate should be only core fields — declaring a group grays the card out in previews even if it would run. Definitions, row processors, severity logic and SQL templates live in [`backend/repositories/insights/definitions/`](backend/repositories/insights/definitions/). The placeholder heuristic `sql.count("?")` means regex literals in templates must be `?`-free (no `(?i)`; pass the `'i'` flag to `regexp_matches`). The default window/baseline adapts to available history (a young service compares the last hour to the previous hour).

**Prewarmer contract.** The prewarmer must warm exactly the adaptive default (window, baseline) pair the page will pick: `pick_insights_default` in [backend/utils/time_window.py](backend/utils/time_window.py) mirrors `pickInsightsDefault` in [frontend/lib/chartUtils.ts](frontend/lib/chartUtils.ts) band-for-band; `/api/insights` cache keys fold in both hour values, so warming another pair is a guaranteed cold compute. It runs insight queries serially (`max_workers=1`) and warms only the adaptive default plus eligible analyst clamp shapes; other comparisons (24 h/720 h) stay on demand. Never compensate with per-job DuckDB thread or memory overrides. It **yields to ingest**: ingest in flight at tick start defers the tick (no lease, no `cron_runs` row); ingest starting mid-tick stops early and records success. The yield is **bounded by warmth** — `_last_full_warm` younger than `INSIGHTS_CACHE_TTL` (300 s) or the tick runs regardless — so a continuously busy ingest can't starve the cache; do not remove that bound. In `high_scale` discovery runs in another process, so the yield signal never fires. It is **demand-driven**: `/api/insights` records the last request per service (`record_insights_request`); with no request within the idle window (`provisioning.cron_insights_prewarmer.idle_minutes`, else `INSIGHTS_PREWARM_IDLE_MINUTES`, default 30; `<= 0` disables) the tick skips before taking a lease or opening DuckDB.

**Insights SQL.** `tests/repositories/test_insights_golden.py` enforces zero behavior drift across all registered definitions; run it after any SQL template change. Evaluate regex/string predicates once per distinct `url`, `ua` or `referer` rather than per row, and keep CTEs referenced by both the window and baseline comparison from being materialized over the full 30-day table.

### State sync ([backend/core/metadata/state_sync.py](backend/core/metadata/state_sync.py))
`export_admin_state` writes `audit_logs` + `views` (from Postgres) plus `log_format_history` + `custom_fields` (from the config JSON) to `{prefix}/meta/admin_state.json`. Alerts are not synced; only `read_write` services export.

### FOS usage logging ([backend/core/metadata/usage_log.py](backend/core/metadata/usage_log.py), [fos_usage.py](backend/core/metadata/fos_usage.py))
Every FOS Class A/B op and CDN download is recorded to the Postgres `usage_log` for cost analysis. Global toggle: `data/system/usage_logging.json`. Entries are tagged via `set_process_context()` (`backend/utils/telemetry/`) such as `cron:sync:svc1` or `api:GET /api/...`; each cron handler flushes via `flush_usage_log(service_id)` (the `@cron_task` decorator wires this). Costs are computed at query time from the rate config, so changing rates recomputes history. Admin endpoints: `GET/PATCH /api/admin/usage-logging`, `GET/DELETE /api/admin/usage-log`, `GET /api/admin/usage-log/export`; frontend `/admin/usage-log`.

### Log-line accounting ([backend/routers/admin/log_accounting.py](backend/routers/admin/log_accounting.py))
Per-bucket reconciliation between Fastly's `/stats/service/{id}` log-emission counter and `sum(row_count) FROM ingested_files`. Field probe order `log → log_records → log_entries → logging_requests`, first non-zero wins (all-zero logs a warning). The current bucket is in totals but excluded from the sustained-loss scan (Fastly Stats lags ingest). Sustained loss = ≥2 consecutive completed buckets with `gap_pct ≥ 0.05`. Frontend polls at most once per minute per open admin tab.

### DuckDB connection pool ([backend/core/duckdb_pool.py](backend/core/duckdb_pool.py))
Per-service LIFO pool replaces per-request `duckdb.connect()` + DuckLake setup + view rebind. Size `DUCKDB_POOL_MAX_SIZE` (default 8). All pool connections open `read_only=False` so cron writers and pool readers never trip "different configuration". `DUCKDB_POOL_CONN_MEMORY_LIMIT` optionally caps RSS under concurrent large scans. DuckDB `threads` is instance-global ([Trap #42](#42-duckdb-threads-is-instance-global--changing-it-at-runtime-deadlocks-the-process)). View binding happens outside the pool lock so a DuckLake view rebuild can't deadlock the FastAPI thread pool.

**Wait observability.** `_Pool.acquire` records every checkout wait to the OTel `app.thread_wait_ms` histogram (`{outcome: reused|created|timeout, waited, service}`) and to a bounded per-service ring buffer (~1024 samples) read by `Pool.stats().wait` (p50/p95/p99/max/mean). `GET /api/admin/health-snapshot` exposes per-service stats and `saturated_rejects_total` / `drain_rejects_total`; `SystemHealthCard` on `/admin` renders them. Escalation rule (ADR-03): p95 > 50 ms ⇒ consider separate-process cron isolation; > 200 ms flags red. Recording is non-blocking (try/except) so instrumentation can't break a checkout. The slow-checkout log separates `queue_admission_ms` from `reused_attach_ms` / `view_prepare_ms` / `fresh_build_ms`; `acquire_total_ms` includes setup and is not evidence of queue saturation. `DUCKDB_POOL_SLOW_CHECKOUT_MS=0` disables the phase collection.

**OOM safeguards.** (1) `BACKEND_GRACEFUL_RESTART_RSS_MB` (read by the DuckDB-recycle job module) converts a destructive cgroup OOM-kill into a graceful self-restart (SIGTERM → uvicorn drains → `restart: unless-stopped`). (2) The `duckdb_recycle` cron drains and recycles the pool to free DuckDB's object cache (`DUCKDB_RECYCLE_INTERVAL_MIN`, off by default). Per-tick cron watchdogs must reuse one bounded executor; a throwaway executor per tick leaks thread-local connections.

### Hourly Top-N rollups ([backend/core/rollups/](backend/core/rollups/))
Precomputes per-hour Top-N aggregates for the most-asked dashboard fields (ip, country, url, custom fields) under `<cache>/rollups/`. Closed hours read from the rollup; the live hour merges the rollup with a fast buffer scan. A per-minute time-series bundle (`rollups/hour_bundled/hour=H/time_series.parquet`) feeds the dashboard chart. Skipped buckets fall back to the raw scan. Written by `recompute_touched_hours` after each `local_compact` pass (only hours that just received data). Bursty services can close an hour without re-touching it, so `rollup_heal` re-runs the idempotent backfill (48 h lookback, `_run_rollup_hour_heal` in [backend/cron/jobs/compaction.py](backend/cron/jobs/compaction.py)); `rollup_compact` is the 30-day deep pass plus per-day compaction. Verified-empty closed hours get an empty sentinel bundle (covered-and-empty, distinguishable from a writer gap); `execute_top_n_rollups` in [backend/repositories/_base.py](backend/repositories/_base.py) live-queries still-uncovered closed hours of not-yet-day-compacted days instead of silently under-counting.

**Bundle tiers** (cheapest first wins in the reader):
- `rollups/day_bundled/day=D/all_fields.parquet` — one per day, all fields; preferred for fully-in-window closed days.
- `rollups/hour_bundled/hour=H/all_fields.parquet` — one per hour; partial-day boundary hours and days without a day bundle.
- `rollups/hour/field=F/hour=H/*.parquet` — per-(field, hour), source of truth for the bundle writers.
- `rollups/day/field=F/day=D/*.parquet` — per-(field, day), source for the day bundler.

**Virtual fields** (`waf_sig_ind`, `edge_score_reason_ind`; `_VIRTUAL_FIELD_BACKING` in `rollups/_common.py`) are CSV-unnested at write time so the reader serves them through the standard rollup path. Adding one requires (a) appending to `_VIRTUAL_FIELD_BACKING`, (b) ensuring its backing column is on the schema, (c) a one-shot rebundle ([Trap #24](#24-rollup-writers-must-rebundle-bundles-after-adding-a-field), [Trap #25](#25-virtual-fields-blow-up-the-live-hour-batch-if-not-filtered-out)).

**Dashboard rollup path builds no per-request temp** ([backend/repositories/dashboard.py](backend/repositories/dashboard.py)): every window-scan section is rollup-served — top-N/map via `execute_top_n_rollups`, chart via `try_time_series_from_rollup`, the `conn_requests` histogram via `try_conn_requests_hist_from_rollup`, signal unnests via the virtual-field rollups — and each section's rollup-miss fallback runs one query against the base table via the stashed `orig_table_name` / `orig_where_clause` / `orig_params`. The non-rollup (filtered) path still materializes the wide temp. New dashboard consumers: serve from a rollup with a direct base-table fallback; don't reintroduce a shared per-request temp.

**`get_top_bots`** ([backend/repositories/security/watchdog.py](backend/repositories/security/watchdog.py)): on the unfiltered path, top UAs come from `execute_top_n_rollups(["ua"], …)` and the NGWAF bots panel from the per-hour `ngwaf_bots` rollup via `try_ngwaf_top_bots_from_rollup` (the `waf_req_id ⨝ ngwaf_bots` join done once per closed hour at write time; zero-bot closed hours write an empty sentinel). A rollup miss falls back to a single direct join; filtered requests use the original `(ua, waf_req_id)` temp.

**`/api/network-health`** hoists the heatmap/geo sections to `try_network_heatmap_from_rollup` / `try_network_geo_from_rollup` before building the filtered temp; when every requested scan-bound section hits a rollup the temp is skipped (`network:temp_skipped` in `_section_timings`).

**Adding a panel or field: consider a rollup at PR time.** Any panel reading the per-request temp on 30-day windows is a candidate.
1. **Measure first.** Request the endpoint with a 30 d window and empty `filters: {}` and read `_section_timings`; any section > ~1 s is rollup-worthy.
2. **Pick the shape.** Per-dimension percentiles as request-weighted averages (`origin_dims.py` `slow_urls`, `network_dims.py` RTT; reader returns `_approx: True`, UI shows an "Approximate" badge); exact GROUP BY counts (associative across hours, no `_approx`); exact minute-granularity time series (store `date_trunc('minute', timestamp)`, re-bucket via `time_bucket` for multiples of 60 s, day compactor **preserves** `bucket_ts`, reader is hybrid closed-hour rollup + live active-hour query); single-row-per-hour summaries (`origin_dims.py` summary writers).
3. **Wire the seams.** Writer in the matching `*_dims.py` / `*_rollups.py` module → constants in `rollups/_common.py` → export in `rollups/__init__.py` → a `RollupSpec` entry in `HOUR_WRITERS` (`recompute.py`; the writer is dispatched by name) → daily compactor in `day_bundles.py` → cron hook in `backend/cron/jobs/compaction.py` → reader on `QueryRunner` / `rollup_executor/` (gates: `not has_filters`, window ≥ 48 h, ≥ 50 % closed-hour coverage, day-prefer/hour-fallback walk; coverage decisions live in [backend/core/rollups/rollup_coverage.py](backend/core/rollups/rollup_coverage.py): `CoverageIndex`, `plan_hourly_read`, `collect_day_prefer_paths`) → dispatcher at the live-SQL call site (try rollup, fall through on `None`) → admin backfill in [backend/routers/admin/compaction.py](backend/routers/admin/compaction.py) → tests (extend `tests/repositories/test_rollup_reader_coverage_golden.py` and `tests/core/test_rollups_recompute.py`).
4. **After deploy, run `POST /api/admin/backfill-bundle-rollups`** — without it the rollup covers only hours touched after deploy.
5. **Re-measure.** `_section_timings` should show `<name>_query_rollup` sub-100 ms; if not, the rollup file wasn't built.

**Rules that are not negotiable:** don't pre-aggregate percentile sketches for cross-hour combine in Standard (DuckDB has no sketch combine; use request-weighted averages with counts); don't collapse scan-bound rollup Parquet into fewer files (DuckDB parallelises across files; rollup *day* compaction is different — file-open-overhead-dominated); don't use response caching as the perf lever (new logs arrive constantly, so TTLs must be sub-minute); don't try to speed up `temp_table_create` itself (materialize-bound; add rollups so fewer panels read the temp).

### Response telemetry middleware ([backend/utils/telemetry/metrics.py](backend/utils/telemetry/metrics.py), [backend/core/request_telemetry/middleware.py](backend/core/request_telemetry/middleware.py))
`TelemetryResponseBodyMiddleware` is a backstop for endpoints returning a plain `dict` instead of `BaseResponse.with_telemetry`. It injects `_debug_queries` / `_debug_calls` / `_debug_sqlite` / `_is_cached` from the contextvar collectors when missing (`_debug_sqlite` is snapshot-copied before `get_tracked_calls()`, which can itself append). **It must be added INNER to `CompressMiddleware`** (`add_middleware(TelemetryResponseBodyMiddleware)` before `add_middleware(CompressMiddleware)`, see [backend/routers/root_system.py](backend/routers/root_system.py) and ADR-04) so it sees raw JSON. It skips streaming responses, non-dict bodies and already-instrumented responses. Debug keys are per-request opt-in: the frontend client sends `x-debug-responses: 1` when the DiagnosticsPanel toggle is on (keys are stripped otherwise) and SSR mirrors it via the `fla.debugResponses` cookie. Gated on `DEBUG_RESPONSES`; failures are silent and non-blocking.

### Isolated High-Scale request facts ([backend/routers/admin/high_scale.py](backend/routers/admin/high_scale.py))
`POST /api/high-scale/services/{service_id}/request-facts` is an explicit high-scale-only read surface backed by [backend/high_scale/query_service.py](backend/high_scale/query_service.py); it is not selected by `DEPLOYMENT_MODE`. A service must be bound through the injectable `HighScaleServiceRegistry`; unregistered services are refused. It uses `RequestContext` tenancy and analyst time clamping, masks `client_ip` under the invite PII policy, and returns signed keyset pagination plus `QueryResponseMetadata`. Capacity helpers (disk sampling, retention recommendation, fair page selection, CMCD projection, recovery capacity, topology profile) are wired only behind the high-scale topology and Standard responses are unchanged. Do not wire a caller that disables ingest, compaction, replay or deletion on a disk mode without a design naming the work to suspend.

High-Scale Performance (`/api/performance/aggregates`) has no stub sections: waterfall reads `performance_minute_summary`, scatter reads `performance_scatter_samples`, and any active filter reroutes every section to `request_facts`. Stored `performance_minute_dimensions.latency_*_ms` columns hold raw microseconds; the reader divides by 1000. Backfill with [scripts/backfill_high_scale.py](scripts/backfill_high_scale.py) (replaying past the worker deploy time double-counts sums).

### High-Scale readers return real data or fail loudly
No `high_scale` reader may return hardcoded empty lists/dicts, a swallowed query failure, or a silently ignored filter/parameter. When ClickHouse lacks data, add it to a projection and rebuild; don't approximate. Runbook: [docs/runbooks/high-scale-reader-parity-backfill.md](docs/runbooks/high-scale-reader-parity-backfill.md).

- **Percentiles:** never average per-minute percentiles. Origin projection rows carry a log-histogram sketch (`*_sk_v/_sk_w`) merged at read time with `quantilesExactWeightedArray`; responses set `_approx` + `_approx_note` (1 % bound); counts stay exact.
- **Filters:** `request_filter_clauses` (in [backend/high_scale/_common.py](backend/high_scale/_common.py)) translates request filters with typed params; unsupported filters raise `ValueError` (HTTP 400), never get dropped. RUM and CMCD have narrower allow-lists for their tables.
- **Dashboard window boundaries:** a sub-minute start or end uses the scoped facts reader (whole-minute projections can't represent partial minutes); aggregate cache keys retain seconds and fractions; minute-aligned unfiltered windows keep the projection path.
- **Visibility fence:** fact readers require the batch's publication row to be `visible` (`visible_fact_clauses`).
- **Filtered Origin:** projections are per-minute with no filterable dimensions, so any active filter is served straight from `request_facts` with exact quantiles (`_approx: false`) and the projection's per-row semantics (latency = `ottfb` else `ttfb`, status = `ost`, overhead = `elapsed - ottlb`). A filter field with no facts mapping raises 400.
- **Security reads:** [backend/high_scale/security.py](backend/high_scale/security.py) mirrors the repository contract and never returns a hardcoded empty section. `request_facts` has no `waf_req_id`, so NGWAF names are resolved at ingest (`_enrich_high_scale_ngwaf_bots`, before the facts publish) and persisted as `ngwaf_bot` / `ngwaf_bot_category` / `ngwaf_wellknown_bot`. `mask_ips` masks `active_clients[].ip` and `top_ips_header[].ip`; the router must pass it and clamp `threat-intel` / `proxies/export` windows.
- **SQL console trust boundary** ([backend/high_scale/query.py](backend/high_scale/query.py)): never splice scope into the user's SQL. Service, publication-fence and window scope are sent as the `additional_table_filters` HTTP setting via `ClickHouseClient.execute(settings=...)` (allow-listed names only); the row cap wraps the user SQL as `SELECT * FROM (\n<sql>\n) LIMIT n` plus `max_result_rows`, so a trailing `--` or an `OR` in the user's `WHERE` cannot undo either. Function calls are checked against `_ALLOWED_FUNCTIONS` (table/server functions such as `remote`, `url`, `file`, `getSetting` are rejected; add one only after confirming it cannot reach another host or disclose server state), and the first query per client probes `system.one` to fail closed if the server does not enforce the filter. **Analysts get the console only through per-service, per-mask-mode least-privilege ClickHouse identities** ([backend/high_scale/postgres_control/console_access.py](backend/high_scale/postgres_control/console_access.py), [runbook](docs/runbooks/high-scale-analyst-console.md)): each user is granted `SELECT` on its own `fla_cv_*` views only; views fix `service_id` and the publication fence under `SQL SECURITY DEFINER`; masked views replace `client_ip` and `custom_fields` / `cmcd` session and IP keys with `'[redacted]'` in the data. Name-based SQL validation and ClickHouse's name resolution can disagree (comment nesting, quote bridging, `WITH RECURSIVE <table>`, CTE over `UNION`, `x IN <table>`), which is why the database — not the guard — holds the boundary. Without `CLICKHOUSE_CONSOLE_SECRET` the router returns 403 `console_unavailable_for_analysts`. Provision order is load-bearing: views are created and verified before any `GRANT`. Don't widen analyst grants; re-run the console-access integration tests in `tests/high_scale/` (gated by `RUN_HS_*` env vars) after changing them or the ClickHouse image.
- **Insights:** [backend/high_scale/insights/](backend/high_scale/insights/) runs every registered detection natively; a new standard insight needs a ClickHouse query there.
- **Adding a column to a projection or fact table** touches: the `.sql` schema (`CREATE` + `ADD COLUMN IF NOT EXISTS`), `CLICKHOUSE_HIGH_SCALE_COLUMNS` / `CLICKHOUSE_HIGH_SCALE_TABLES` in `core/clickhouse/constants.py`, the writer's column lists, the projection builder, a purge + rebuild path in `recovery.py` wired into [scripts/backfill_high_scale.py](scripts/backfill_high_scale.py), the reader, and the runbook. A rebuild publishes under new batch ids, so replacing rows requires the purge first.
- **ClickHouse dialect traps** (only visible against a real engine): `formatDateTime` minutes are `%i` (`%M` is the month name); a trailing `ORDER BY`/`LIMIT` after `UNION ALL` binds to the last `SELECT` only (wrap the union); `greatest(UInt64, Float64)` is illegal (`toFloat64`); a `LEFT JOIN` miss yields defaults, not NULL (`ifNull`); 64-bit ints are unquoted only because the client sets `output_format_json_quote_64bit_integers=0`. Fake clients can't catch these; run the real-engine integration tests (gated by `RUN_HS_*` env vars) when touching reader SQL.

### High-scale archive retention
Raw `.gz` objects are deleted once archived, so the FOS archive (`high-scale/archive/`) is the only copy that can rebuild ClickHouse after a volume loss, needed only while ClickHouse still holds the data. `HighScaleArchiveExpirer` ([backend/high_scale/postgres_control/deletion.py](backend/high_scale/postgres_control/deletion.py), run from `HighScaleDeletionSweeper.sweep`) deletes a manifest's artifact, `.json` manifest and `.commit` marker (commit marker first) once the manifest is `source_deleted` and its coverage is older than the domain's retention plus a day (`data_retention_days` for `request`, `rum_retention_days` for RUM, 0 keeps forever), then sets `archive_expired_at`; `manifests_covering` skips expired manifests so replay never reads a missing artifact. **`retention_seconds` / `retention_deadline` on a manifest are not this retention** — they only delay raw-source deletion; wiring `data_retention_days` into them would keep raw logs for the whole window. `/api/usage/prefill` returns the service's real `data_retention_days` / `rum_retention_days` and `deployment_mode`; `/api/usage/current-storage` adds the live archive (`PostgresControlPlane.archive_storage_summary`) and recently expired artifacts that still bill. Cost-calculator archive constants are measured; re-measure from `high_scale_archive_manifests` (`regr_slope` of `artifact_size` on `row_count`) if the archive schema changes.

### Certificate-authenticated administrator connection
The optional mTLS gateway is a separate administrator access surface for Compose and Kubernetes; its client CA is not the analyst login system. See [docs/deploy/admin-mtls.md](docs/deploy/admin-mtls.md). `ADMIN_GATEWAY_SECRET` is a server-only credential injected into `X-Admin-Gateway-Token` after client-certificate authentication; backend ([backend/utils/remote_access.py](backend/utils/remote_access.py)) and frontend SSR verify it independently. Never put it in bootstrap, browser code or logs; public proxies must strip it. Requests carrying the public `X-Proxied-By-Caddy` marker cannot be promoted with it. `ADMIN_GATEWAY_REQUIRED=1` removes implicit administrator access from loopback and `LOCAL_ADMIN_CIDRS` (unauthenticated requests keep analyst restrictions; cheap liveness stays open, deep health requires authentication); set it on both backend and frontend runtimes. SSR bootstrap caching uses the shared transport trust decision and bypasses shared caching whenever an `analyst_session_id` cookie is present; never cache a response marked `is_remote_analyst`. Don't remove host-management SSH when retiring dashboard tunnel scaffolding.

### Live Query Monitor ([backend/core/request_telemetry/query_registry.py](backend/core/request_telemetry/query_registry.py), [backend/routers/admin/admin_queries.py](backend/routers/admin/admin_queries.py), [frontend/app/admin/queries/](frontend/app/admin/queries/))
Real-time view of every executing DuckDB and PostgreSQL query with attribution (analyst/admin/cron/system), caller `file:line`, pool slot, live duration and a kind-aware Kill button calling `con.interrupt()`. Page `/admin/queries` (admin-only), polled at 300 ms. Queries above the persistence threshold are written to `slow_queries` with the request correlation id (`rid`, also in the access log). Instrumentation seams: the Postgres cursor wrapper/profiler, and `InstrumentedDuckDBConnection` (deregisters at terminal-fetch time, not `execute()`, since fetch can run for seconds). Cancel is safe under pool reuse: a stamped `_conn_to_query[id(con)]` is verified under lock before `interrupt()`. Every successful cancel is audit-logged. Kill switches: `QUERY_MONITOR_ENABLED=0` (endpoints 404), `QUERY_REGISTRY_DISABLED=1` (zero overhead).

### Multiplexed admin SSE ([backend/routers/admin/events.py](backend/routers/admin/events.py), [frontend/components/SyncStatusBadge/](frontend/components/SyncStatusBadge/))
The admin shell mounts one SSE connection: `GET /api/admin/events/stream?channels=sync_status,cron_runs,system_metrics,share`, a single typed-envelope stream demuxed on the frontend into the same React Query caches. It replaced four always-on streams that starved bootstrap/panel fetches over HTTP/1.1 (~6 connections per origin). `system_metrics` is deduped server-side (short per-service TTL + per-key lock). The analyst `/api/log-extents/stream` is separate. **When adding an admin real-time feed, add a channel here; don't mount a new dedicated SSE endpoint.** `build_share_live_payload` lives in `backend.utils.tunnel` (not `share_admin`) so routers don't import each other. For multi-tab admin sessions over an SSH tunnel, the Caddyfile's loopback-only `https://127.0.0.1:8443` block terminates TLS with Caddy's internal CA so the browser negotiates HTTP/2 (`ssh -L 8443:127.0.0.1:8443 <host>`); the plain `:3001`/`:8000` HTTP tunnel still works.

### Control Room ([backend/routers/control_room.py](backend/routers/control_room.py), [frontend/app/control-room/](frontend/app/control-room/))
Operational dashboard (analyst-visible; the Admin tab is hidden from analysts) with six tabs (Summary, CDN & Caching, Security, Performance, Network, Admin), backed by an SSE stream polling `rt.fastly.com` at 1 s cadence (`/api/services/{id}/realtime-stream`). The poller runs on a dedicated daemon thread in [backend/core/realtime/](backend/core/realtime/) with httpx pooling, immune to event-loop starvation; the transform layer extracts 50+ metrics into typed SSE envelopes. Charts seed with 60 bars of history, use rolling-average smoothing and lazy-loaded Plotly; backgrounded tabs stop polling. Mutation endpoints (mitigations, rules, allowlist, big-red-button, cost-governor) remain admin-gated 501 stubs. Log-field audit and correlator endpoints are DuckDB-driven.

### CMCD streaming analytics ([backend/repositories/cmcd.py](backend/repositories/cmcd.py), [backend/routers/performance.py](backend/routers/performance.py), [frontend/app/streaming/](frontend/app/streaming/))
Services with Common Media Client Data logging get a `/streaming` page and per-session detail. Field extraction supports v1 (query-string) and v2 (request-header) via generated VCL ([backend/provision/cmcd_fields.py](backend/provision/cmcd_fields.py)); the admin settings (`/api/services/{id}/cmcd/{status,enable,disable}`) select the version and field catalog. The nav item hides when a service lacks CMCD logging. Repository queries use a temp table with combined time series and no self-joins.

### Service Summary / Fastly Value ([backend/repositories/value.py](backend/repositories/value.py), [frontend/app/fastly-value/](frontend/app/fastly-value/))
Executive summary page (`POST /api/value/summary`) across Summary, CDN & Caching, Security, Bot Management, Network & Performance and Image Optimizer tabs, backed by the precomputed overview rollup ([backend/core/rollups/overview_rollups.py](backend/core/rollups/overview_rollups.py)). Services without Image Optimizer see an upsell tab.

### Ingest error quarantine ([backend/core/metadata/ingest_log.py](backend/core/metadata/ingest_log.py), [backend/routers/admin/quarantine.py](backend/routers/admin/quarantine.py))
Corrupt or unparseable lines are saved under an `errors/` prefix in FOS, timestamped and classified (malformed JSON, schema mismatch, encoding error). Quarantine storage is included in usage cost. Admin endpoints export, download and purge (purge deletes all files, not just expired).

### CDN-fronted log delivery
FOS reads are fronted by a Fastly CDN VCL service (`cdn_service_id`, `cdn_url`, `cdn_secret`) that validates a shared-secret query param (rate-limited against brute force). It is a different service from the logging service ID.

### Session scoring (edge L2) ([backend/routers/session_scoring/](backend/routers/session_scoring/), [compute/scorer/](compute/scorer/), [backend/provision/session_scoring/](backend/provision/session_scoring/))
Edge-computed 0–100 risk score per request combining cookie/timing signals (L1) with a PageRank route-transition matrix (L2). The Rust scorer builds to Wasm (`make scorer-package`) and runs on Fastly Compute as an instance-per-request sub-fetch; `make scorer-test` gates its native tests. The matrix is not embedded — it is served from the `scoring_matrix` KV Store at runtime. Orchestration: `enable_scoring` / teardown in `provision/session_scoring/orchestrate.py`, KV/config/secret stores in `stores.py`, Compute deploy and VCL snippet builders in `wasm_deploy.py`.
- **L2 enforcement is explicit operator opt-in** (`GET`/`PUT /scoring/l2-enforce`): L2 is always computed and logged but contributes to the enforced score only after enabling, fading in over three days; disabling returns to observe-only. No clock-driven auto-ramp; deployment age is an advisory readiness gauge only.
- **NGWAF skip-inspection** — the internal scoring sub-fetch carries `x-sigsci-skip-inspection-once` so NGWAF doesn't inspect (and 406) the internal call; it is set on the compute route and unset on the scrub and restart paths so the real-origin WAF path is never bypassed.
- **No VCL retry of the scoring sub-fetch** — only 2 restarts (score→origin spends them); fail open on timeout/error by design.
- **Session-detail lookup:** the session list attaches an opaque AES-GCM `session_token` per row, sealing `(ip, ja4, start, end)` (service-bound via AAD), minted before response masking. The detail endpoint unseals it, runs the exact-match lookup, then re-masks. **Never key session detail on the displayed `ip`** (rewritten to `1.2.3.xxx` for a masking analyst, it never matches). A masking analyst supplying a raw top-level `ip` is rejected. `SESSION_TOKEN_SECRET` gives restart-stable tokens. Zero/negative session windows (single-request sessions) are widened ~1 s each side before the analyst time clamp, else they trip `time_range_empty`.

### Live Dashboard Sharing
Two direct-mode sharing modes: an admin-provided hostname (`https://logs.example.com`) or IP (`https://203.0.113.42:8443`). Both share one code path: `public_endpoint` must start with `https://` (cookies need `secure=true`); the UI mode selector is presentational.
- [backend/utils/tunnel/](backend/utils/tunnel/) — `TunnelManager` singleton (`get_tunnel_manager()`, direct-mode lifecycle, sever-all panic), `AnalystSession`, `_LoginRateLimiter`, `tunnel_state.json` persistence, session fingerprint hash; `reset_for_tests()` for pytest.
- [backend/utils/remote_access.py](backend/utils/remote_access.py) — `RemoteAccessMiddleware`: DNS-rebinding gate (Host/Origin allow-lists incl. `testclient`/`testserver`), blocks admin paths on remote requests, response hardening (CSP, X-Frame-Options DENY, no-store, no-referrer), `_StaticAssetLimiter`. **Idle-timeout activity model:** the 2-hour idle deadline is refreshed only on genuine interaction — requests carry `X-User-Active: 1|0` (from real DOM activity, not background refetches); SSE streams (`/api/log-extents/stream`) are exempt; an IP change updates the recorded address without bumping the clock; `/api/share/heartbeat` doubles as the activity channel.
- [backend/core/share_db/](backend/core/share_db/) — Postgres-backed invites, sessions, audit, passcode (argon2id; rehashed on login when the cost parameters change), ToS, settings; tables `remote_invites`, `invite_services`, `remote_sessions`, `remote_share_audit_logs`, `share_settings`, `remote_invite_claim_tokens`, `share_tos_versions`. Per-IP/per-email lockout.
- [backend/routers/share_admin/](backend/routers/share_admin/) — admin routes under `/api/admin/share/*` (**blocked over the tunnel**): `tunnel_routes.py` (lifecycle, status, mock IdP), `invites.py` (invite CRUD, passcodes, OAuth), `sessions_and_tos.py` (sessions, evict, panic, backup export/import, GDPR erase, settings, ToS, and the analyst-facing `/api/share/*` routes: `auth-config` (unauthenticated; the frontend fails open to passcode on fetch error), `login`, `logout`, `acknowledge`, `heartbeat`, `claim/{token}`).
- **Analyst OAuth/OIDC login** (opt-in alternative to passcode): `GET /api/share/oauth/authorize` and `/callback` are top-level browser navigations where every outcome is a 302 (never JSON); they converge on the same `TunnelManager` session so RBAC/masking/ToS/boot are inherited. Provider registry in [backend/core/oauth/](backend/core/oauth/): gitignored `data/system/oauth_providers.json` (`OAUTH_PROVIDERS_CONFIG_PATH` override), credentials via `OAUTH_<KEY>_CLIENT_ID` / `_CLIENT_SECRET`, inert unless `OAUTH_FLOW_STATE_SECRET` is set; `SHARE_PASSCODE_LOGIN_ENABLED=0` disables passcode (enforced at the endpoint, not just the UI). Invites carry `auth_method` / `oauth_provider` / `oauth_subject`; optional JIT invite provisioning for trusted org-restricted providers. The mock IdP mounts only when `OAUTH_MOCK_IDP=1`.
- Frontend: [components/share-dashboard/](frontend/components/share-dashboard/), [/share-login](frontend/app/share-login/) (ToS-gated), `useAnalystHeartbeat`. A watermark mounts in `AppLayout` when `bootstrap.settings.is_remote_analyst === true`.

An endpoint analysts must reach over the tunnel must be registered under `/api/share/*` (auto-allowed) or added to `_is_blocked_path()` ([Trap #20](#20-remoteaccessmiddleware-blocks-admin-routes-over-the-live-share-tunnel)).

## Provisioning

### UI wizard ([frontend/components/ProvisionWizard/](frontend/components/ProvisionWizard/))
Step order: `mode → token → service → storage → ngwaf → fields → execute`. The token entered in step 2 must be threaded into every Fastly-credentialed API call (including the NGWAF workspace fetch); don't rely on stored-config fallback alone. `execute` streams SSE — never poll it with a plain `GET`. Document step-order changes here.

### CLI ([backend/provision/cli.py](backend/provision/cli.py))
`python -m backend.provision.cli <subcommand>`: `provision` (interactive wizard), `teardown --service-id {id}`, `update-logs`, `update-cdn`, `enable-scoring`, `disable-scoring`, `list-groups`, `list-fields`, `restore-data --service-id {id}`.

### Teardown (`perform_teardown` in [backend/provision/orchestrator.py](backend/provision/orchestrator.py))
Removes the FOS logging endpoint from the Fastly service, the CDN VCL service, the FOS access key, local config, local DuckDB and local cache. See [Trap #31](#31-teardown-dereference-before-delete-and-enumerate-what-you-remove) for ordering.

## Frontend Patterns

> **Before any frontend work** read the relevant guide in `frontend/node_modules/next/dist/docs/` (see [frontend/AGENTS.md](frontend/AGENTS.md)); the vendored Next.js 16 App Router docs differ from older knowledge. Click-feel bugs are almost always a Next conventions violation (missing `loading.tsx`, layouts blocking on uncached data, per-instance `setInterval` storms, missing `signal` cancellation, polling tuned for "live feel" instead of backend cost).

**Stack:** Next.js 16 App Router, React 19, TanStack Query v5, Zustand, shadcn/ui, Recharts/Plotly, openapi-fetch.

**Type-safe client** ([frontend/lib/api.ts](frontend/lib/api.ts)):
```typescript
import createClient from "openapi-fetch";
import type { paths } from "@/types/api.generated";
const client = createClient<paths>({ baseUrl: "" });
```
A global middleware checks `response.ok` and **throws a JS Error** on 4xx/5xx. FastAPI wraps errors as `{"detail": {"error": "..."}}`; extract with `r.error?.detail?.error || r.error?.error` or use `extractApiError`. Use try/catch or TanStack Query's `isError`; never `.then((r) => r.data?.x || fallback)` without checking `r.error`. **Generated types:** `cd frontend && npm run gen:types` after backend endpoint/model changes (`make gen-types`; `make ci` runs it). **Streaming/binary endpoints** (SSE, blobs) use raw `fetch()` — leave a comment so nobody "fixes" it.

### Server-side bootstrap pre-fetch ([frontend/app/layout.tsx](frontend/app/layout.tsx), [frontend/lib/apiSsrProxy.ts](frontend/lib/apiSsrProxy.ts), [frontend/lib/ssr/upstream.ts](frontend/lib/ssr/upstream.ts))

The root layout SSR-fetches `/api/bootstrap`, dehydrates it into the React Query cache (`HydrationBoundary` in `QueryProvider`) and ships the JSON inline in the first HTML paint, so `useBootstrap` and `getQueryData(['bootstrap'])` consumers find data on first render. Several pages (origin, security, performance, insights, assets-shield, streaming, alerts, admin) also SSR-seed their primary query; the main dashboard is a client wrapper whose bundle is fetched on mount. Rules for adding another:

1. **Use `node:http.request`, not `fetch()`.** Node's `fetch()` always rewrites `Host` from the URL; the backend's `_remote_host_allowed` gate rejects remote-classified requests whose Host isn't the public endpoint, so the SSR fetch would silently fall through to the client. `apiSsrProxy.ts` has the `rawRequest` pattern to copy.
2. **Trust topology is `X-Remote-Analyst: 1`, not `X-Proxied-By-Caddy`.** SSR reaches the backend over loopback, and `is_request_remote` classifies on `request.client.host` first, so a forwarded Caddy marker is ignored and the backend returns the **admin** payload — which would then dehydrate into public HTML. Forward `X-Remote-Analyst: 1` (honored on loopback, gated on `tunnel_manager.is_sharing_active()`) only when the inbound request carries `X-Proxied-By-Caddy`; otherwise the admin SSH-tunnel path is mis-classified as analyst.
3. **Always wrap in try/catch + a bounded timeout (5 s) and return `null` on failure.** SSR errors must never break a page; the layout falls back to client fetch.
4. **`export const dynamic = "force-dynamic"` is required** in any layout/page doing a per-request SSR fetch through an imported helper that calls `cookies()`/`headers()`. Next's static analysis detects only direct calls in the component file; otherwise the route is built statically when the backend isn't reachable and the dehydrated state is permanently empty.
5. **Adversarial test before deploy:** hit the public URL anonymously and the admin path and verify the dehydrated state. Anonymous must contain only the `needs_login` stub (no `sharing_active`, `ngwaf_workspace_id`, `sync_status`); admin must contain the full payload.
6. **The SSR seed key must byte-match the first-paint client key.** Key the page query on a server-reproducible `(rangeToken, anchor)` relative-range pair, not a client-`now()`-anchored `start/end`, and make the identical snap-to-stale-extents decision on both sides (extents > 15 min stale snap the window to real data), otherwise the seed lands under a different key and the client refetches. Pin it with a key-match + transport-trust test.
7. **Honor the `?service=` URL param over `bootstrap.active_service_id`**, or a cold load of a non-default service seeds and paints the wrong service. Diagnose via curl + grep of the dehydrated payload, not browser timing.

**Keep server route wrappers when consolidating client modules.** Replacing a server `page.tsx` with its client component removes prefetch and dehydration even if the fetch helper remains, regressing populated initial HTML and causing layout shift. SSR-seeded pages need no initial browser API call, but manual refresh must still carry request attribution.

The `serviceStore` slice hydrates from the SSR-cached bootstrap in `useBootstrap`'s post-mount effect. For the one-render window before it fires, use `useEffectiveServiceId` ([frontend/hooks/useBootstrap.ts](frontend/hooks/useBootstrap.ts)), which falls back to `bootstrap.active_service_id` from the React Query cache; reading `useServiceStore(s => s.activeServiceId)` directly flashes "No service selected".

### Canonical patterns (use in new code)

1. **`response_model=` on every router handler.** Without it OpenAPI emits `Record<string, unknown>`. Routes using `Depends(get_source)` should also lift `service_id: str` into the signature. For dict-shaped endpoints use a model with `extra="allow"` + `response_model_exclude_unset=True`, string (`.isoformat()`) timestamps, fields derived from the producer's actual payload — byte-identical wire output, typed OpenAPI. SSE/file/binary routes carry an explicit exemption comment.
2. **Page context hooks** — `useTimeRange`, `useFilterUrlWriteback`, `useServiceQuery`, `useAnalyticsReportQuery`, `useReportRangeAnchor` ([frontend/hooks/useDashboardCards.ts](frontend/hooks/useDashboardCards.ts)); `useTimezone`, `useNowMs` ([frontend/hooks/usePersistentPreferences.ts](frontend/hooks/usePersistentPreferences.ts)); `useMounted`, `useEffectiveServiceId`, `useIsDataReady` (`useBootstrap.ts`). Don't read multiple stores directly.
3. **`ReportLayout`** ([frontend/components/ReportLayout.tsx](frontend/components/ReportLayout.tsx)) for analytics pages — bundles page context + report config + debounced filter payload + `useViewMetricUrlSync` + `useServiceQuery` + `ChartIntervalButtons` + `ReportShell`. Fall back to `ReportShell` only for multi-query or non-standard chrome.
4. **`HelpDialog`** for help content (don't hand-compose `Dialog` + header). **`createBaseMap` + `addCountryBaseLayer`** ([frontend/components/Map/NetworkMap/types.ts](frontend/components/Map/NetworkMap/types.ts)) for any MapLibre setup. **`CardErrorState`** (alert + Retry) for any card whose query can fail — don't fabricate zeros or leave a spinner on a 5xx.
5. **`analystFetch`** ([frontend/hooks/useAnalystAndServiceHooks.ts](frontend/hooks/useAnalystAndServiceHooks.ts)) — shared analyst fetch + envelope unwrapping; don't hand-roll per consumer. **`getPageLoadId()`** ([frontend/lib/api.ts](frontend/lib/api.ts)) is the single page-load correlation id source.
6. **PoP labels** render as `DEN (Denver, CO - USA)` via the shared helper seeded from `bootstrap.pop_geo`; keep the raw code for click-to-filter.
7. **Route decomposition** — route pages keep `page.tsx` as a thin server/wrapper entry and split UI into `_sections/<Route>Client.tsx` plus focused section panels, sharing `useReportPageState` ([frontend/hooks/useReportPageState.ts](frontend/hooks/useReportPageState.ts)). Complex modals follow the same sub-component pattern. `createRouteErrorBoundary` ([frontend/app/error.tsx](frontend/app/error.tsx)) builds route `error.tsx` components. `resolveDefaultSsrRangeKey` ([frontend/lib/ssr/upstream.ts](frontend/lib/ssr/upstream.ts)) resolves default SSR range keys.
8. **Backend:** `metadata.record_audit(service_id, event_type=..., details=...)`; `date_utils.parse_iso_utc` / `iso_z` (don't hand-roll `fromisoformat(s.replace("Z", ...))`); `@cron_task`; `empty_schema_response(runner)` and `origin_latency_us_expr(actual_cols)` in `repositories/_base.py` (don't hand-roll the `COALESCE("ottfb", "ttfb" * 1000000.0)` fragment).

### Next.js navigation + loading conventions

1. **Every navigable route needs a `loading.tsx`.** Without it, dynamic (`'use client'`) routes get no prefetched fallback and the browser sits on the previous page until the destination settles. Use `RouteSkeleton` ([frontend/components/ui/skeleton.tsx](frontend/components/ui/skeleton.tsx)) variants; don't hand-roll skeleton arrays.
2. **Layouts must not block on uncached data.** If a shared layout awaits a fetch or short-circuits children behind a loading boolean, `loading.tsx` never shows. Layout-level data uses `useQuery` with `staleTime`; never gate children on a loading flag.
3. **Cancel in-flight queries on route change** (`queryClient.cancelQueries({ type: 'active' })` in `AppLayout`'s `useEffect([pathname])`) and **thread `signal` through queryFns** (`queryFn: async ({ signal }) => client.GET(..., { signal })`) so cancellation aborts the request.
4. **Poll intervals respect backend cost.** Default ≥ 10 s; never poll faster than 5 s (add a manual Refresh instead); always `refetchIntervalInBackground: false`.
5. **Never spawn per-instance `setInterval` for visible-tick state.** Components needing a shared 1 Hz "now" use the single `useNowMs` hook.
6. **Async buttons need immediate feedback:** `<Loader2 className="h-3 w-3 mr-1 animate-spin" />` plus a pending label (`Saving…`); `disabled={busy}` alone looks dead.
7. **Prefetch:** static routes prefetch fully; dynamic routes prefetch partially only if `loading.tsx` exists. `<Link prefetch>` is the default; use `prefetch={false}` only in dense lists. When a link target needs an API call to render meaningfully, add `onMouseEnter` → `queryClient.prefetchQuery(...)` (see `AdminPrefetchLinks`).
8. **Wrap `router.replace()` inside effects in `startTransition`** — a synchronous call in `useEffect` causes a render cascade that blocks paint.
9. **React Query defaults** (`QueryProvider`): `staleTime: 30s`, `gcTime: 5min`, `refetchOnWindowFocus: false`. Override per query only with a documented reason.
10. **When a click feels slow, measure before guessing** with Playwright timing of URL change, DOM ready, network idle and individual API requests. Usual causes: polls running during navigation, heavy `useQuery` fan-out on mount, layout re-renders from store subscriptions.

## Testing

### Validation workflow

Use local development for basic checks, hosted CI for full gates, and a deployed Standard or High-Scale environment for realistic verification. Do not start heavy local suites or builds by default. See [AGENTS.md](AGENTS.md) for publication and deployment requirements.

1. **Inner loop (seconds):** run only the file or selector you are touching.
   ```bash
   uv run pytest tests/routers/test_dashboard_router.py
   uv run pytest -k "test_specific_feature"
   cd frontend && npx vitest __tests__/hooks/useBootstrap.test.ts
   ```
2. **Smoke (`make fast-ci`, opt-in locally):** OpenAPI drift, typechecks, linters, frontend contract tests and core unit directories. This skips most backend tests, UI component tests and E2E; it is not a full gate.
3. **Full gates (hosted):** push the intended release/feature branch with normal hooks, then require successful CI and Playwright runs for that exact SHA before deployment. `make ci` remains the full workflow-equivalent command for a suitable runner, not a mandatory workstation run.

### Test preservation

**Never delete or weaken tests to speed up CI.** Test count isn't the bottleneck (collection and vitest/JSDOM startup dominate), and coverage ratchets fail the build if coverage drops: backend `--cov-fail-under=85`; frontend lines 66 / statements 65 / functions 54 / branches 52; security regressions have a monotonic floor (`scripts/check_security_regression_count.sh`, never lowered); the ESLint count ceiling only goes down. Add or update tests for every change; if a change isn't testable in isolation, document why.

### Backend (`tests/`)

Fixtures in [tests/conftest.py](tests/conftest.py): `in_memory_duckdb` (real DuckDB, `_alerts`/`_views` pre-created), `test_service_source`, `client` (FastAPI `TestClient` with DuckDB deps overridden); mock data via `generate_mock_logs` in [tests/_helpers/](tests/_helpers/__init__.py).

**Per-test isolation is fast on purpose.** The autouse `isolate_metadata_db` fixture probes `_PER_TEST_RESET_TABLES` with one `UNION ALL … WHERE EXISTS` query and `TRUNCATE`s only tables that hold rows (truncating all per test across xdist workers took seconds). **When you add a Postgres metadata table that tests write to, add it to `_PER_TEST_RESET_TABLES`** or rows leak between tests. Module-level caches drain through `CacheRegistry.clear_all()`. Parallel runs share one Postgres: never run two pytest sessions against the same DSN (symptom: random failures that pass in isolation).

Patterns: router tests with mocked config (`@patch("config.load_config")`); router tests on in-memory DuckDB via `client`; mock external HTTP with `@patch("urllib.request.urlopen")`; VCR cassettes under `tests/cassettes/`; provisioning tests in `tests/routers/test_provision.py` and `tests/provision/`. **What to test:** happy path + error cases (missing params, invalid data, external 4xx/5xx), state-persistence invariants (custom fields not overwritten on unrelated saves), config schema invariants, and SQL-injection coverage for any query with dynamic table names (`_safe_table()`). Extend the canonical domain test file instead of creating a one-off per feature. Prefer integration tests over pure mocks. Hypothesis `RuleBasedStateMachine` tests must `metadata_db.teardown(service_id)` in `@initialize()` (instances share per-test state; otherwise `FlakyStrategyDefinition`).

### Frontend (`frontend/__tests__/`, vitest + jsdom)

[frontend/vitest.setup.ts](frontend/vitest.setup.ts) provides global mocks (Next router, `localStorage`, `matchMedia`). `globals: false`, so the setup file explicitly registers `afterEach(cleanup)` and MSW's `server.listen()` at module top level — see [Traps #17/#18](#17-msw--openapi-fetch-ordering--serverlisten-must-run-at-module-load). Patterns: `render` + `screen.getBy*` for components; `renderHook` for hooks; direct calls for pure utilities. Test pure utilities exhaustively, hook state transitions, component key states (loading/error/empty/populated) and URL helpers.

### E2E ([frontend/e2e/](frontend/e2e/), Playwright)

Chromium, Firefox and WebKit all block PRs. A mock backend boots from `frontend/e2e/global-setup.ts` under `FASTLY_MOCK_MODE=1` on port 18004 (frontend 13004, via `playwright.config.ts`), so it doesn't collide with a dev shell. Run locally: `cd frontend && npx playwright test --project=chromium`. Run the suite for any change touching the admin shell, dashboard, provision wizard, custom-field drawer or share-login; use all three browsers for browser-only interactions (DnD, popovers, chart hover). SSE specs must split on `/\r\n\r\n|\n\n|\r\r/` (`sse-starlette` emits `\r\n\r\n`). SSR-seeded route fixtures must cover server and browser requests: browser `page.route` cannot override a fresh SSR seed, so use a server-side fixture backend ([scripts/run_contract_backend.py](scripts/run_contract_backend.py)) that the production app never installs.

### Performance gates and tooling

- **Perf gate.** `PERF_NUM_ROWS` selects a scale in [tests/perf/baseline.json](tests/perf/baseline.json): `smoke_100k` (default, PR-blocking), `mid_500k` (opt-in), `nightly_1m` ([.github/workflows/perf-nightly.yml](.github/workflows/perf-nightly.yml)). Refresh after a legitimate improvement: `PERF_NUM_ROWS=500000 uv run python scripts/emit_perf_latest.py && bash scripts/perf_gate.sh`, then update the scenario in `baseline.json` with headroom.
- **Synthetic data:** use the one generator, `uv run python scripts/load_test/generate_synthetic_traffic.py`, with `--target local` (Parquet batches into the local buffer, `--commit` to materialize DuckLake snapshots), `--target fos` (gzipped NDJSON to FOS, exercising discovery/ledger/commit), or `--target clickhouse` / `--with-clickhouse` (seeds `request_facts` + publication batches). Scenarios: `diurnal` (default), `ddos-spike`, `origin-5xx-outage`, `bot-scrape`, `slow-network`. Example: `--target local --scenario diurnal --rows 100000 --commit`.
- **Live pipeline lag:** [scripts/dev/scale_harness.py](scripts/dev/scale_harness.py) sends staged concurrent HTTP load to a deployed edge URL and samples `/api/log-extents` for ingest freshness lag (rates above 1,000 RPS need `--allow-high-rate`). It costs real CDN requests; use it deliberately.
- **Section-level evidence:** `scripts/lib/performance_budgets.json` defines the section-level performance budgets. First/repeat request order is not cache state; cached enclosing payloads can't supply current child timings; overlapping spans must not be summed into request compute; ClickHouse admission/HTTP/decode phases are client wall observations, not server CPU.

## Shared Helpers

Single owners for patterns that were once hand-rolled per call site. **Reach for these before writing a lock, loop, regex, cache, envelope or SQL fragment.** Ratchet gates (`make retry-guard`, `coalesce-guard`, `dateparse-guard`, `urllib-guard`; baselines only go down) fail CI on new bespoke copies. Tests monkeypatch the primitives at their canonical homes, so keep call-time imports as written in each module.

### Cron lifecycle ([backend/cron/scheduler/lifecycle.py](backend/cron/scheduler/lifecycle.py))
- `cron_run(service_id, task, ...)` is the per-run context manager every job body uses: resolve the source, acquire the `(service_id, task)` lease via `start_cron_run`, start/end progress, write a terminal `cron_runs` row (duration/status/exception), and run `finalize_*` backstops in a `finally`. Use `run.skipped`, `run.success(...)`, `run.warn(...)`, `run.fail(...)`, `run.record(...)`, `run.log_progress(...)`. Body exceptions are recorded and swallowed by default (`suppress_exceptions=True`); `error_summary` / `error_result` shape the auto-`error` row.
- `CronLeaseHeldError` ([backend/core/metadata/cron_runs.py](backend/core/metadata/cron_runs.py), a `RuntimeError` subclass) means another run holds the lease: a clean skip, never an error. Don't catch bare `RuntimeError` around `start_cron_run`.
- `record_skip_row(source, task, summary, status="skipped", ...)` writes the single terminal row for a pre-flight skip that never takes the lease (nothing to evaluate, disk too full).
- HTTP-triggered thread spawns (`routers/admin/compaction.py`, `utils/router_utils.py`) own a run id and pass it as `cron_run(run_id=...)`. A job that skips writing a terminal status leaves a `running` row that freezes ingestion (the deep health check flags it past `_STUCK_SYNC_RUNNING_MINS`).

### Fastly draft versions ([backend/provision/declarative/fastly_ops.py](backend/provision/declarative/fastly_ops.py), [backend/provision/cli.py](backend/provision/cli.py))
Service changes go through the clone → mutate → validate → activate flow with rollback to the previous active version on any failure (a failed rollback is logged at WARNING and never masks the original error); validation that returns anything but `status == "ok"` raises. Don't hand-write clone/validate/activate elsewhere; reuse the existing draft-version context manager and pass the Fastly client in so each orchestrator keeps its own module-level `fastly` seam for tests.

### Service config writes ([backend/config.py](backend/config.py), [backend/utils/router_utils.py](backend/utils/router_utils.py))
- Every read-modify-write goes through `mutate_config(service_id, fn)`: `config_lock` (cross-process `flock` sidecar, bounded 5 s wait, re-entrant per thread, degrades to unlocked with `config_lock_degraded`), a fresh read bypassing the in-process cache, `fn(cfg)` mutating in place, atomic save. Returns the saved config or `None` when the service has no config. Routers use `mutate_service_config(service_id, fn)`, mapping `None` to the standard 404 `{"error": "Service not found"}`; `load_service_config` is the read counterpart.
- Multi-step changes that must roll back use `snapshot_config(service_id)` (raw persisted text or `None`) and restore it under the same lock. Never `config_path.write_text(...)` a config.
- **cid-salt rules:** `cfg["rum"]["cid_salt"]` is write-once. Every save re-adopts the persisted salt (a stale writer can't re-key pseudonymous cids); an unreadable existing config aborts the save (`CidSaltReadError`) instead of minting a new salt. `allow_cid_salt_change=True` is reserved for an explicit rotation helper (none exists in this tree). A callback that replaces the whole dict must `clear()` + `update()`. Anything touching this path needs a security review.

### SQL helpers
- `escape_sql_literal(value)` ([backend/core/sql_validator.py](backend/core/sql_validator.py)) is the only way to put a string inside a single-quoted DuckDB literal (S3 object keys are attacker-controlled). Don't write `.replace("'", "''")`.
- `is_safe_ident` / `require_safe_ident(name, label)` (same module) validate bare identifiers; the table-name slugger stays `_safe_table()` ([Trap #4](#4-_safe_table-is-mandatory)). Don't add a private identifier regex.
- [backend/core/metric_expressions.py](backend/core/metric_expressions.py) owns null-default and metric fragments with golden-string tests: `origin_latency_us(cols, absent=...)`, `sum_or_zero(expr, zero=, where=)`, `column_or_default(cols, col, default, cast=)`, `coalesce(expr, default)`, `HIT_STATES`. A change to a fragment changes served SQL and rollup schemas, so update the golden tests in the same commit. New inline `COALESCE(` in repositories or rollup writers fails `make coalesce-guard`.

### Retry ([backend/core/retry.py](backend/core/retry.py))
`retry_call(fn, retry_on=pred, attempts=N, base_delay=, max_delay=, multiplier=, jitter=, max_elapsed=, before_retry=, sleep=)` is the one bounded-backoff primitive (tenacity underneath). The predicate decides retryable vs fatal; `before_retry` is the recovery seam (e.g. bust a view cache); `sleep=` is the test seam. A `for`/`while` + `try/except` + `sleep` loop is a hand-rolled retry and fails `make retry-guard` unless baselined. Error classification and recovery stay at each call site — `retry_call` is mechanics only. Intentionally bespoke loops (timing `retry_call` can't reproduce): the VirtioFS config read loop in `config.py`, the FOS bucket-delete step schedule, orchestrator storage init and scorer readiness probes that yield status events between attempts, and polling/SSE loops (those poll, they don't retry). `core/fastly/client.py` uses tenacity directly (method-blind).

### Caches ([backend/utils/bounded_cache.py](backend/utils/bounded_cache.py))
`BoundedTTLCache(maxsize=, ttl_seconds=)` is the thread-safe LRU+TTL dict for time-keyed caches. Register every module-level cache with `CacheRegistry.register("pkg.name", cache)` in the same change so the test harness drains it; clear in place, never rebind the module global (other modules import it by name). **Don't migrate snapshot-keyed caches** (storage `_view_cache` and the snapshot-file caches): they invalidate on snapshot identity, not time.

### Dates ([backend/utils/date_utils.py](backend/utils/date_utils.py))
`parse_iso_utc`, `parse_window`, `parse_hour_key[_strict]` (`YYYY-MM-DD-HH`), `parse_day_key[_strict]`, `iso_z_now`, `ensure_utc` (naive = UTC, aware → `astimezone`; never `.replace(tzinfo=UTC)` on an aware value), `iso_z`. Raw `strptime` / `fromisoformat` / `.replace(tzinfo=UTC)` in `backend/core` and `backend/repositories` is ratcheted by `make dateparse-guard` (per-file baseline in `scripts/check_no_raw_dateparse.sh`; lower the row when you migrate a site).

### HTTP layer ([backend/utils/router_utils.py](backend/utils/router_utils.py), [backend/utils/error_utils.py](backend/utils/error_utils.py), [backend/models/common/base.py](backend/models/common/base.py))
- **Errors:** `make_error(code, message=None, **extras)` builds the `{"error": code, ...}` detail for `HTTPException`. At an `except` site whose text could leak internals (DuckDB paths, upstream Fastly bodies, cid salt) call `raise_internal(logger, exc, code=..., request=...)`: it logs the full exception with an `error_id`/`request_id` and returns only those ids. `redact_error_text` / `scrub_secrets` (strip absolute server paths and registered cid salts) must stay on every analyst-visible error path. `@query_errors` decorates query-backed handlers. Never `detail={"error": str(e)}`.
- `require_service_id(service_id)` is the only answer to a missing service id: **400 `service_id_required`** via `make_error`, never FastAPI's 422 body; the frontend and e2e key on that code.
- **SSE:** build every SSE route with `sse_response(stream)` ([backend/utils/sse.py](backend/utils/sse.py): shared 5 s ping + passthrough headers). A test fails on a direct `EventSourceResponse(` under `backend/routers`.
- **Log throttling:** `log_once_per(key, interval_s=60.0)` (`backend/utils/telemetry/logging_config.py`) returns True at most once per interval per key; wrap any log line that can fire per request/row while a failure persists.
- **Typed responses:** every route has a `response_model` defined in `backend/models/` (never inline in routers; a test keeps the untyped allowlist empty). Use `WireSafeModel` (`extra="allow"`, omits never-set fields, wire byte-identical to the producer's dict) or `PassthroughModel` (`extra="allow"` only) as bases, and `JsonRows` / `JsonDict` / `JsonAny` for DB-native values (Decimal, tz-aware datetimes, bytes) so they keep the `jsonable_encoder` encoding (`Z`, not `+00:00`). Run `make gen-types` after changing a model and alias generated types on the frontend via `Schema<'ModelName'>` ([frontend/types/api.ts](frontend/types/api.ts)) instead of hand-writing interfaces.
- **Admin SSE & cron triggers:** reuse `_stream_from_worker(work)` and `run_admin_cron_trigger(service_id, task, job_fn, ...)` ([backend/routers/admin/_helpers.py](backend/routers/admin/_helpers.py)).

### High-Scale DRY helpers
[backend/high_scale/_common.py](backend/high_scale/_common.py) centralizes ClickHouse filter/time-window SQL helpers; `BaseClickHouseBatchAdapter` ([backend/high_scale/archive/clickhouse_batch.py](backend/high_scale/archive/clickhouse_batch.py)) owns shared batch-adapter plumbing; `_MultiServiceWorkerLoop` ([backend/high_scale/worker.py](backend/high_scale/worker.py)) runs multi-service worker loops. Fastly POP coordinates come from `get_pop_locations()` / `get_pop_lat_lon_map()` in [backend/utils/pop_utils.py](backend/utils/pop_utils.py); never hardcode coordinate `CASE` blocks in SQL.

### Postgres metadata connections ([backend/core/metadata/pg_pool.py](backend/core/metadata/pg_pool.py))
Each thread holds one long-lived pooled connection (`get_pg_thread_connection`); `PgConnectionWrapper.execute` holds the wrapper's `_busy` lock while a statement is in flight. See [Trap #45](#45-never-return-a-busy-postgres-connection-to-the-pool-from-another-thread). Threads that end for good call `release_pg_thread_connection()` themselves.

### Frontend helpers
- `resolveDefaultSsrRangeKey(...)` in [frontend/lib/ssr/upstream.ts](frontend/lib/ssr/upstream.ts) resolves default SSR time-range keys from `LogExtentsResponse`; `getPageLoadId()` in [frontend/lib/api.ts](frontend/lib/api.ts) is the page-load correlation id source.
- Analytics route pages follow `page.tsx` + `_sections/<Route>Client.tsx` and share state through `useReportPageState` ([frontend/hooks/useReportPageState.ts](frontend/hooks/useReportPageState.ts)).

## Traps & Gotchas

The single most valuable section. Re-read it.

### 1. `openapi-fetch` semantics
The global middleware throws on 4xx/5xx. Handle with try/catch or TanStack Query's `isError`; extract the message with `r.error?.detail?.error || r.error?.error`.

### 2. Python `UnboundLocalError` from conditional imports
An `import x` anywhere in a function makes `x` local for the whole function; any earlier use raises `UnboundLocalError`. Keep imports at module level or at the very top of the function.

### 3. Falsy empty list in `or` chains
```python
# WRONG: data["data"] == [] falls through despite the key existing
raw = data.get("data") or data.get("workspaces") or []
# RIGHT:
raw = data.get("data") if "data" in data else data.get("workspaces", [])
```
(NGWAF workspace listing: `GET /api/provision/ngwaf-workspaces`, response key `"data"`.)

### 4. `_safe_table()` is mandatory
All DuckDB table names derived from user-controlled values (service IDs, field names) go through `_safe_table()` ([backend/core/duckdb.py](backend/core/duckdb.py) and `repositories/_base.py`). Never interpolate into SQL. Other bare identifiers use `require_safe_ident`; string literals use `escape_sql_literal`; filter values use parameters.

### 5. DuckDB write connections are serialized
`get_connection()` in [backend/core/duckdb.py](backend/core/duckdb.py) locks. Never hold a write connection across requests. Operational metadata lives in Postgres and is unaffected ([Trap #15](#15-operational-metadata-lives-in-postgresql-16-not-duckdb)).

### 6. Configs are keyed by LOGGING service ID, not CDN service ID
`configs/{logging_service_id}.json`. When looking up a stored API key, use the logging service ID; `cdn_service_id` is a different Fastly service.

### 7. APScheduler "config not found, skipping" during teardown is normal
A job fired after its config was deleted; the next `reload()` evicts the stale job.

### 8. Ruff format must pass
`make ci` fails `format-check` on unformatted files. Format only the files you changed before committing.

### 9. OpenAPI types must be regenerated after backend schema changes
`make gen-types` (or `cd frontend && npm run gen:types`). `make ci` does it automatically; running `tsc` by hand does not.

### 11. All configs are schema v2
`lf = cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []}` — always.

### 12. NGWAF bot cache lives in PostgreSQL, materialized into a DuckDB temp table
`ngwaf_bots` is stored in Postgres (`pg_schema.py`) and materialized in DuckDB via `CREATE OR REPLACE TEMP TABLE {alias}_ngwaf_bots` (`attach_ngwaf_cache` in `repositories/_base.py`).

### 13. Next.js startup crash on restricted macOS
`ERR_SYSTEM_ERROR: uv_interface_addresses returned Unknown system error 1` on some hardened systems. The `dev` script in `frontend/package.json` binds `-H 127.0.0.1` to bypass interface enumeration. Don't drop the flag.

### 14. VCL regex right-hand side must be a string literal
The RHS of `~` / `!~` must be a literal — no variables or concatenation. Use `regsub()` / `regsuball()` for dynamic logic.

### 15. Operational metadata lives in PostgreSQL 16, not DuckDB
Alerts, views, audit, cron history, ingested-file dedup, ASN names, source registration, slow-query history, usage telemetry, live share and the NGWAF bot cache live in Postgres via `METADATA_DSN` ([ADR-22](docs/adr/22-postgres-only-metadata.md)). Read/write through [backend/core/metadata/](backend/core/metadata/) (`from backend.core import metadata as metadata_db` resolves through the package's `_ShimModule`), never via DuckDB. MVCC means background cron writers never block readers or cause file-lock deadlocks.

### 17. MSW + openapi-fetch ordering — `server.listen()` must run at module load
`openapi-fetch` captures `globalThis.fetch` at `createClient` time and `frontend/lib/api.ts` creates its client at module load, so MSW's `server.listen()` must execute at the top of `vitest.setup.ts`, **not inside `beforeAll`**. Otherwise the captured fetch is the unpatched original and tests silently bypass MSW (symptom: handlers never fire, requests hit real loopback).

### 18. `@testing-library/react` auto-cleanup is OFF when `globals: false`
`vitest.config.ts` sets `globals: false`, so RTL registers no `afterEach(cleanup)`; `vitest.setup.ts` has an explicit one. Without it, earlier components stay mounted and pollute `screen.getBy*` (input values accumulate).

### 19. `extractApiError` Error-instance trap
`Error.message` is non-enumerable, so `JSON.stringify(new Error('boom'))` is `"{}"`. `extractApiError` in `frontend/lib/api.ts` early-returns `error.message` for `instanceof Error`; keep that check first.

### 20. `RemoteAccessMiddleware` blocks admin routes over the live-share tunnel
The tunnel exposes the same FastAPI app to the internet. The middleware classifies by `Host` and blocks remote requests to admin paths, including `/api/admin/share/*`. Register analyst-reachable endpoints under `/api/share/*` or update `_is_blocked_path()`. Don't remove the `testclient`/`testserver` allow-list entries; they let pytest hit admin routes.

### 23. SSR upstream fetch must use `node:http`, not `fetch()`
See rule 1 of [Server-side bootstrap pre-fetch](#server-side-bootstrap-pre-fetch-frontendapplayouttsx-frontendlibapissrproxyts-frontendlibssrupstreamts). `fetch()` rewrites `Host`, the backend classifies the request as admin-from-loopback, and the full admin bootstrap dehydrates into anonymous public HTML. Copy the `rawRequest` pattern in `apiSsrProxy.ts`.

### 24. Rollup writers must rebundle bundles after adding a field
`bundle_hours` / `bundle_days` use mtime to skip up-to-date bundles, and the cron only re-bundles hours that just received data. A new real or virtual field therefore lands as a per-(field, hour) Parquet while the bundled `all_fields.parquet` for closed hours stays without it; the bundled reader returns 0 rows for the new field and the runtime fallback fires (defeating the perf win). Fix: delete the stale closed bundles and re-run the backfill (`backfill_missing_bundles` / `backfill_day_bundles`, or `POST /api/admin/backfill-bundle-rollups`). **Never per-field-backfill and rebundle into `all_fields`** — that can clobber existing fields with the partial set.

### 25. Virtual fields blow up the live-hour batch if not filtered out
The live-hour SQL in `execute_top_n_rollups` projects `field_name AS value` and BinderExceptions on any name that isn't a real column on the live temp. Virtual fields (`waf_sig_ind`) aren't; passing them through kills the whole `UNION ALL` (the outer `except` swallows it) and drops the live-hour merge for real fields too. Always filter first:
```python
live_fields = [f for f in fields if f in actual_cols]
if live_fields:
    live_res, _ = self.execute_top_n_batch(live_fields, tmp_name, ...)
```

### 26. Tombstoned buffer Parquets must be excluded from direct buffer reads
Post-commit, buffer Parquets aren't unlinked: `tombstone_buffer_files` ([backend/core/storage/buffer.py](backend/core/storage/buffer.py)) writes a `.parquet.consumed-<ts>` sidecar and the file lingers for a grace window so views bound before the commit stay readable. Their rows are already in the hourly partitions, so any code reading the buffer dir directly (not via `buffer_files()`, which filters) must skip `_tombstoned_parquet_paths(buffer_dir)` or it double-counts — `_create_active_hour_temp_direct` in `_base.py` is the reference. New live-slice consumers reuse the per-request shared temp via `begin_shared_active_hour_temps()` / `end_shared_active_hour_temps()`.

### 27. System-managed custom fields must be re-asserted on every `log_fields` write
Session scoring and CMCD each inject a canonical set of `log_fields.custom_fields` from code; `_is_system_field` ([backend/core/catalog.py](backend/core/catalog.py)) hides them from the user-editable list, so any writer persisting `log_fields` from a list it didn't author omits them: the UI round-trip, an admin-state pull, or a provisioning reconcile built from groups alone. The omission isn't cosmetic — `reconcile_vcl_state` regenerates the Fastly log format from the persisted config, so a dropped field leaves its extraction VCL running while nothing writes its output; the column then ingests empty string (not NULL), the feature's `available` probe still passes because the DuckDB column exists, and the page renders zeros with no error. Route every such write through `reconcile_system_custom_fields` / `reconcile_cfg_system_custom_fields` ([backend/provision/cmcd_fields.py](backend/provision/cmcd_fields.py)) and key it on the **current** feature state (`system_feature_flags`), never on a state transition: a reconcile that says nothing about the feature must still re-assert its fields, and a disable must strip them. **A third system-managed feature adds its flag to `system_feature_flags`; do not add another re-injection block.** Anything on this path needs a security/correctness review.

### 29. Feature extraction VCL must be emitted BEFORE field capture
Generated `vcl_recv` has two related blocks: **extraction** parses a source into `req.http.x-<feature>:*`, and **capture** promotes it into `req.http.x-fos-edge-data:<field>`, which the log format reads. If capture is emitted first it copies empty strings and extraction runs too late. The failure is silent everywhere you'd look: the VCL is valid and `validate` passes; the DuckDB columns exist so the feature's `available` probe is true and the page shows zeros instead of "not enabled"; and the extraction itself genuinely works, so an edge cache-key probe proves CMCD is handled while saying nothing about whether the value reached the log line. Both generators (`generate_capture_vcl` in `fastly_api.py` and the consolidated snippet in `declarative/vcl_builder.py`) must order extraction before capture; a feature that extracts into a `req.http.x-*` header for logging needs ordering coverage in the VCL tests. To verify a capture-stage field end to end, send a uniquely tagged request and query for the tag rather than inferring from edge behaviour.

### 30. The log-line budget silently truncates late custom fields
`generate_log_format` tracks an aggregate `budget` (`FASTLY_LOG_LINE_DELIVER_MAX`) and clamps each variable-length field to what's left (`cf_limit = max(0, min(cf_limit, budget))`), allocated greedily in `custom_fields` order. Once the budget is dry, later fields get `substr(x, 0, 0)` and log null with no warning (e.g. a 36-char UUID squeezed to 26 chars, breaking session joins). Values logged before the squeeze are full length, so one column can hold mixed lengths, which reads like a client quirk. **Don't "fix" this by adding `byte_limit` to individual fields without measuring**: `byte_limit` only lowers a cap, can't create budget, and shifting allocation can zero other fields. Inspect the real generated format (`generate_log_format(cfg)`; grep for `substr(..., 0, 0)` and per-field caps). The real levers are fewer enabled groups/custom fields or a higher line budget, which trade against Fastly silently dropping over-long lines.

### 31. Teardown: dereference before delete, and ENUMERATE what you remove
Teardown touches a service the customer still serves traffic from, so ordering is a correctness property: **every step that strips VCL from the customer's service must complete before any step that deletes a service that VCL points at**, or their active version routes to a deleted host. Order in `perform_teardown`: scoring VCL strip + Compute delete → logging endpoint and all owned VCL off the customer service → FOS keys → FOS bucket → analytics-owned CDN service last.
- **Never delete by guessed name.** Blind DELETEs of hardcoded snippet names that swallow 404s report success while leaving the capture VCL live. GET the list and delete what you can attribute, narrowly: the `Fastly Log Analytics` prefix plus the canonical `scoring_snippet_names()` / `cmcd_snippet_names()` / RUM sets. Do **not** prefix-match `Session ` — a customer's `Session Tracking - *` snippets sit beside our `Session Scoring - *` ones.
- **No best-effort dereference followed by unconditional delete.** If the VCL strip fails, abort and leave the Compute service in place: an orphaned Compute service is cheap and re-deletable, a dangling backend on live traffic is an incident (Fastly 500s on these calls are real).
- **Never label a destructive log line with a name you didn't resolve from the thing being deleted;** log the id alongside every delete.
Cover ordering changes with the provisioning/teardown tests in `tests/provision/`.

### 32. DuckLake inlines small commits — only `ducklake_flush_inlined_data` makes them durable
DuckLake can inline small inserts into the metadata catalog instead of writing Parquet (visible as `inlined_insert` in `ducklake_snapshots`), and `ducklake_table_info` then honestly reports `file_count = 0` for a table holding real rows. All attachments configure `DATA_INLINING_ROW_LIMIT 0` (`_ducklake_attach()` in [backend/core/storage/catalog.py](backend/core/storage/catalog.py)) so every insert writes Parquet at commit, which also avoids upstream duckdb/ducklake#1495 (stale cached inlined tables across attaches). Still: **neither `ducklake_merge_adjacent_files` nor `ducklake_rewrite_data_files` promotes inlined rows** (both touch only materialized files); `ducklake_flush_inlined_data` is the only primitive that does. This is a durability invariant, not a layout preference: `finalize_committed_raw` deletes the raw `.gz` once its ledger row has been committed for `RAW_DELETE_GRACE_S`, so without the flush the catalog holds the only copy. Both compaction paths flush first (`merge_lake_files` for Celery, `_optimize_table_impl` for the default path); **a new write path needs the flush too**, and pointing `DATA_PATH` at FOS is not a substitute. Corollary for readers: code inferring "is there data?" from file count is wrong under DuckLake — count rows or consult inlined state (see [ADR-14](docs/adr/14-ducklake-replacement.md)).

### 35. Concurrent `ATTACH … AS lake` on different connections silently corrupts the catalog
Several rules, enforced in [backend/core/storage/catalog.py](backend/core/storage/catalog.py), [backend/core/storage/buffer.py](backend/core/storage/buffer.py) and `duckdb_pool.py`:
- **One process-wide lock (`_attach_lock`) wraps the entire `_ducklake_attach` body, including `INSTALL/LOAD ducklake`.** Unsynchronized concurrent attaches under the alias `lake` — routine whenever a burst of requests builds fresh pooled connections — report success but leave the connection permanently failing `ducklake_snapshots('lake')` with `Catalog "__ducklake_metadata_lake" does not exist`, poisoning that pooled connection for its life. Locking only the `ATTACH` (not the extension load) still hangs intermittently; extension loading isn't safe to race either.
- **Check before re-attaching.** If `lake` is already attached on the connection (`SELECT 1 FROM duckdb_databases() WHERE database_name = 'lake'`), don't call `_ducklake_attach` again blindly; if you must, match the connection's actual read-only mode. A swallowed "already exists" is not proof the catalog is functional — verify with a real query (`ducklake_snapshots('lake')`).
- **Every DETACH of `lake` (or `__ducklake_metadata_lake`) goes through `_ducklake_detach()`**, which takes the same lock; never `con.execute("DETACH lake")` directly.
- **Pool `release()` must not detach.** Connections to the same service file share one DuckDB instance, so a (even locked) DETACH on release evicts `lake` from sibling connections mid-query. `acquire()` verifies or restores the attach on reuse.
- **A read-only caller finding `lake` attached read-write reuses it** (verified live via `ducklake_snapshots('lake')`) instead of detaching; reader-forced downgrades rip a live writer's attach away mid-use (`Unique file handle conflict … in the process of being detached`). Only a write caller finding a read-only attach forces the detach + re-attach upgrade.
- **Bounded write-holds.** The commit path must not hold one exclusive write attach across an unbounded backlog: `_commit_buffer_impl` commits in `_BUFFER_COMMIT_CHUNK_SIZE`-file batches, each with a fresh `_ducklake_write_connection`, so no write-hold outlasts the `_attach_lock` retry budget (~20 attempts/~30 s) that every other connection depends on; a later chunk's attach failure keeps earlier chunks' progress. Any "process the whole buffer in one shot" path needs the same treatment.
- A writer's `finally: _ducklake_detach()` can still evict `lake` under a concurrent reader at commit end; this is an intentional self-healing degradation (the view-token probe returns `None` and the caller rebuilds the view). Don't "fix" it with a broader lock.

### 37. Map prewarming must configure the worker before constructing any map
MapLibre shares its worker pool across map instances, and `MapPrewarm` can run before any visible map module, so it must call `setWorkerUrl('/maplibre-gl-worker.mjs')` immediately after its lazy import and before constructing a map. Otherwise it creates a worker with an empty URL, the browser fetches the page HTML as a module, fails MIME checking, and every later map inherits the broken pool. Configuring the visible map later does not repair workers already created. An attached canvas is not proof a map works (the 1 px prewarm canvas exists even with no geometry loaded); map E2E tests must supply country data and require rendered `countries` features on the full-size map.

### 38. NGWAF dashboard cards require a configured workspace
`bootstrap.ngwaf_configured` is the active service's attachment flag, available to admin and analyst without exposing the workspace id. `useDashboardCards` excludes NGWAF Verified Bots and NGWAF Signals from the available card list when it is false. Virtual fields must not bypass this gate: persisted card selections, "Show all" or leftover WAF log fields do not establish NGWAF is configured. The header counts only available selected cards; saved preferences stay intact when switching to a service where NGWAF is configured.

### 39. Header freshness is request-event time, not a log-file timestamp
The REQUEST header uses `request.latest_log_at`, the request table's event-time extent. A raw object's filename timestamp belongs in `latest_ingested_file_at` / `latest_available_file_at` and must never overwrite that extent in a cached status overlay, or REST and SSE report an older request than `log-extents` and the header falsely looks stalled. Bootstrap cache hits and misses must preserve the same meaning. Use the current snapshot's request metrics, falling back to the flat extent only when the request group is absent; keep RUM separate. Explicit nulls clear metrics; omitted fields in a partial SSE update retain prior values. Never take the max of old and new timestamps: a reset or replacement can legitimately move the extent backward. In durable/Celery mode `request_metrics_observer` (started in `backend/core/lifespan.py`, implemented in `request_telemetry/tracker.py`) owns a single backend reconciliation loop — observe the durable serving view, persist, publish changed status via SSE — waiting 15 s after each completed pass. It is serving work, stays enabled with development ingest crons paused, and never starts an ingest writer. Sync/file mode keeps its post-batch refresh (ADR-18).

### 40. METRICS-group catalog fields have no real column — exclude them from every per-field rollup writer
`LOG_FIELD_CATALOG` entries in the `METRICS` group (`requests`, `hit_rate`, `5xx`, `4xx`, `p50_latency`, `p95_latency`, `p99_latency`, `throughput`, `req_size`, `ttfb_ms`) have `vcl=None`: chart-metric-only synthetic concepts, never an ingested column (`LogField.is_derived`). `FIELDS` in `repositories/dashboard.py` includes them anyway, so anything iterating `FIELDS` to build a per-field `SELECT <field>` fails with `Binder Error: Referenced column "requests" not found`. Derive the exclusion from the registry (`{f.code for f in in_group(Group.METRICS)}`), never a hand-maintained list: `_LIVE_TOPN_SKIP_FIELDS` (`repositories/_base.py`) and `_get_fields` (`rollups/_common.py`) both fold it in. **Any new per-field rollup/merge writer that iterates `FIELDS` must exclude the same set.**

### 41. Incremental ingest must attempt one batch after an expensive LIST
The `log_discovery` budget covers both FOS discovery and file processing. On a high-object-count bucket the four-hour incremental lookback can consume the whole 240 s budget before the chunk loop; checking `max_seconds` before the first chunk then stalls permanently (every tick lists the same backlog, processes zero files, reports "No new log files found"). Always allow the first bounded chunk to run, then enforce the limit before subsequent chunks. Pinned by the `max_seconds` tests in `tests/core/test_convert_batch.py`.

### 42. DuckDB `threads` is instance-global — changing it at runtime deadlocks the process
`SET threads` on any connection changes the count for every connection to that database instance, and each change makes DuckDB's `TaskScheduler::RelaunchThreads` join workers while holding the scheduler mutex. If a worker is waiting on a DuckLake scan lock whose holder's nested metadata query needs the scheduler mutex, everything deadlocks and all later queries, `SET`s and `LOAD ducklake` hang. Set threads only through `_ensure_threads` in `backend/core/duckdb.py` (issues `SET` only when the value differs, from `DUCKDB_THREADS`). Never add per-connection or per-job thread overrides on a file-backed service connection; throttle background work with worker counts. Separate `:memory:` connections are separate instances and may set their own value before running queries. `memory_limit` is also instance-global.

### 43. Request lag comes from serial work on the ingest tick, not the poll interval
The freshness SLA is 10–20 s.
- **Standard:** `log_discovery` must not run `recompute_touched_hours` / `recompute_wellknown_bots_rollup` inline (90–170 s on a busy service; with `max_instances=1` most ticks would be skipped). Rollups go through `schedule_post_ingest_rollups` in [backend/cron/jobs/sync.py](backend/cron/jobs/sync.py), a per-service single-flight daemon worker that coalesces hours queued while it runs. **Don't move rollup or other heavy refresh work back onto the discovery tick.**
- **High-Scale:** serial per-object processing (FOS GET/PUT, ~15 Postgres control transitions, ~7 ClickHouse publish+verify pairs) starves ingest. The page-batched pipeline instead (1) ingests pages of 50–100 objects, decoding concurrently in Arrow memory and writing one merged archive Parquet per page to amortize the FOS PUT; (2) registers one `ArchiveManifest` (`batch_{manifest_id}`) per page with member source keys, SHA-256 digests and per-source replay offsets, so replay and retention stay granular per source; (3) collapses control traffic into batch transactions (`claim_sources_batch`, a batch-manifest put, publish marking, `acknowledge_sources_batch`) with bulk ClickHouse inserts verified in one pending→visible gate; (4) partitions sources by key hash under a single owner epoch, so replicas claim exclusive ranges without contending. Page objects run on a bounded thread pool (`HIGH_SCALE_OBJECT_CONCURRENCY`, default 4) with pooled Postgres connections; size worker counts against Postgres `max_connections`.

### 44. RUM and request ingest must share the parse, quarantine, and deletion contract
Standard-mode RUM ingest (`rum_sync` / `ingest_rum_logs`) must follow the same contract as request ingest: (1) discovery uses bounded incremental minute prefixes (`rum_minute_list_prefix`) with `StartAfter` bounds, never a full `raw/rum/` scan per tick (which burns CPU and starves request discovery); (2) raw objects are deleted inline per chunk under `resolve_raw_delete_after`, excluding unreadable objects and objects whose quarantine capture failed (otherwise `metadata_cleanup` trims `ingested_files` and a full scan re-ingests aged raw beacons as duplicates); (3) one shared parser (`_parse_rum_beacon_file` / `_parse_rum_line`) and quarantine flow, so rows missing timestamps are quarantined with exact bytes via `_quarantine_rum_corrupt_lines` ([backend/core/rum_ingest.py](backend/core/rum_ingest.py)), never silently dropped; (4) durable rows in `ingested_files` (0 for files that produced no rows for that table); (5) the `max_seconds` budget with the first chunk always allowed ([Trap #41](#41-incremental-ingest-must-attempt-one-batch-after-an-expensive-list)).

### 45. Never return a busy Postgres connection to the pool from another thread
`close_all_pg_connections()` drains every tracked thread connection (teardown, shutdown). Handing a libpq connection back to the pool while its owner has a statement in flight frees it under the running query (segfault or cut-off). Each `PgConnectionWrapper` carries a `_busy` `RLock` held for the duration of `execute`; the drain takes it with `acquire(blocking=False)` and **skips** (leaves tracked) any connection it can't take. Never `putconn` / `_return_to_pool_once` a connection you don't own without taking `_busy` non-blocking first; never block on `_busy` from the drain path (a hung query would hang shutdown); an ending thread calls `release_pg_thread_connection()` for its own connection. The `_checked_out` WeakSet must not become a strong list (it would pin dead threads' connections until the pool times out).

### 46. Gate non-SSR-seeded store/query state on `useMounted()` in route chunks (React #418)
In the App Router, the root layout (`StoreHydrator`, `useBootstrap`, admin SSE) mounts and runs effects before late-loaded route chunks hydrate. In that gap `useServiceStore.activeServiceId` (null during SSR without a `?service=` param) is set and fast local queries/SSE can populate caches before the route's first client render. A route component that reads `activeServiceId` or non-SSR-seeded query data directly in its first render renders populated DOM where SSR rendered `null`/a skeleton, producing `Minified React error #418`. Gate those reads on `const mounted = useMounted()` so the first client pass matches SSR byte for byte. Clock-derived chart labels (`Date.now()`) need the same gate, since a second can pass between server render and hydration — including in screen-reader tables even when the chart itself is client-only.

### 47. Pin both halves of a DuckLake-plus-buffer view to the same commit boundary
The buffer branch captures fixed Parquet paths, so the committed DuckLake branch must use `AT (VERSION => snapshot_id)`, not the advancing current table; otherwise a buffer commit makes an already-bound view read the rows twice (DuckLake and the retained buffer files), and a shared rollup session can persist those duplicates into closed-hour bundles. Build the view under the existing per-service lock and cache the snapshot actually pinned, never a later probe; if the snapshot can't be verified, fail rather than publish a partial or unpinned view. Don't hold a transaction or lock across rollup execution, or unlink buffers early. Changing the view does not repair already-corrupted bundles; that needs an explicit rebuild. Daily derivatives need independent repair when a reader selects the daily tier: never rebuild a daily file from whichever hourly files remain after retirement (a partial day would replace a complete one); a scoped day repair recomputes all 24 closed UTC hours from retained events and publishes the replacement only after full success, with intermediates kept away from the normal compactor.

Country-only daily repair uses admin-only `POST /api/admin/rebuild-country-daily-rollup`, not the time-series repair endpoint. It requires independently verified full-day raw totals and the complete nonempty-country vector, reconciles every hour against retained time-series references, and replaces only the country field in the daily bundle. Publication and later maintenance must preserve other fields and reject stale generations; missing retired per-field files are not permission to overwrite an existing daily field from partial hourly remnants.

`POST /api/admin/rebuild-daily-rollup-fields` uses the same repair engine for selected country, status, method and PoP fields. Supply an independent complete vector for each selected field; canonical NULL/empty exclusions mean a vector can sum to less than the request total, including zero. The engine pins retained raw once, verifies all 24 hours and exact daily-minute references when hourly references have retired, and atomically replaces only the selected fields. Source, reference, vector or generation drift must refuse publication, not trigger a partial-day rebuild.

Per-field hourly files and all-fields hourly bundles are alternative copies of
the same `(field, hour)`, not additive inputs. They can coexist during a refresh
or after cleanup fails. Daily top-N compaction selects the newer complete copy
by mtime, with the bundle winning ties, rather than summing both. Keep every
partition of a selected per-field hour. A bundle covers only the fields it
actually contains; its presence must not discard another field's hourly files.
The overlap and source-selection regressions live in `tests/core/test_rollups.py`.
This prevents new double-counting but does not repair previously saved days.

### 48. A shared `raw/rum/` object belongs to one domain but carries both
High-scale `rum_vitals` and `rum_errors` discovery both list `raw/rum/`, and the source ledger is `UNIQUE(service_id, object_key)`, so the first domain to see an object claims it. Decoding only the claimed domain silently drops the sibling's rows, and the raw object is then deleted. `ingest_controller` therefore decodes the sibling domain from the same payload, keeps only rows passing `row_matches_serving_domain`, and publishes them under `build_batch_id_from_manifest_id(f"{manifest_id}:{sibling}")` before the source is acknowledged; sibling rows are not counted, archived or dead-lettered as the claimed domain's rows. Don't "fix" this with separate ledger rows per domain or cursor resets. Regression: `test_rum_errors_claim_also_publishes_sibling_vitals_from_the_shared_object` (`tests/high_scale/test_ingest_controller.py`).
- **An error-owned archive is not an error-only archive.** Inspect exact `sources_json` slices and deduplicate expanded copies of each `raw_json` by source version and original line ordinal before diagnosing loss; empty `error_message` rows can be genuine vitals. Faro's exception discriminator is authoritative, not the envelope's metric fields or query string; `_rum_kind` prevents an exception from becoming a false vital. Message-less exceptions keep `Unknown error`; flat errors need an actual discriminator (stack, file or exception kind). Invalid metrics and flat records with neither kind are dead-lettered with their original bytes; sibling rejections are counted separately. Custom Faro events project as `event_<name>=1.0`, and navigation attributes/measurements can expand to multiple metrics, so input-event counts don't prove valid-vital counts.
- **Historical repair:** a recovery tool must default to a read-only dry run for exactly one unambiguous terminal manifest, reconstruct both domains from retained original lines, check physical facts across all batch ids and visible publication receipts (including semantic equivalents with changed event ids), and block apply on hidden, duplicate, conflicting, expired or unprovable identities. Require an explicit plan-digest approval before apply; serialize repairs and hold the terminal-source/current-owner fence throughout. Generation is the manifest's **owner epoch**, not its archive epoch. Stable missing-set batch ids plus pre/post identity checks make retries safe beyond ClickHouse's block-dedup window. Never substitute a generic rebuild, blind replay, cursor reset or source-ledger edit; see the capacity runbook.

### 49. PoP health cannot drop a partial leading hour
Standard PoP health's whole-hour rollup collector skips a partial leading hour but its live merge restores only the active-hour tail, so a window starting partway through an hour must use the scoped raw fallback instead of serving a truncated rollup. Hour-aligned starts stay eligible. `tests/repositories/test_rollup_reader_coverage_golden.py` checks both dispatch paths against retained events.

### 50. The Standard-mode cloud copy is the only off-box copy of committed data
Standard keeps DuckLake Parquet on local disk and the catalog in Postgres. The copy under `{prefix}/ducklake/` ([backend/core/storage/backup.py](backend/core/storage/backup.py)) is the only thing surviving loss of both, since raw `.gz` are deleted after ingest when `delete_after` is on.
- **The FOS listing is the only state.** Nothing about the copy is recorded in Postgres, so it stays usable when the catalog is lost. Each push lists `ducklake/main/<table>/`, uploads catalog-live files that are missing or differ in size, and deletes keys the catalog no longer references.
- **Never delete when the live set is empty.** An empty `ducklake_list_files` means a wiped or unreadable catalog, not "everything compacted away".
- **A fresh catalog must never delete an old backup (adoption).** After a full loss the new catalog is empty or nearly so, so "not live in this catalog" describes the whole old copy. A push deletes only keys uploaded after the catalog's oldest retained snapshot (`_catalog_identity`: the `main` schema uuid and `min(snapshot_time)` from the Postgres catalog) until the catalog has adopted the copy via the `{prefix}/ducklake/_catalog.json` marker holding its schema uuid. The marker is written when no foreign key exists (new install, or an established deployment whose copy holds only its own files) and by `restore_from_cloud`; another catalog's marker doesn't count. A foreign copy is kept untouched, new files still upload beside it, and a throttled warning says to run `restore-data`. Push reconciles all three tables every time so adoption never rests on a partial listing.
- **Hook every writer of the file set.** `schedule_cloud_push` runs after a commit that committed files, after the RUM commit, and after `optimize` and `expire`; a new job that adds, rewrites or removes DuckLake data files needs the same call. It is single-flight per service and skipped in High-Scale, without a bucket, and under `FLA_DEV_NO_CRONS=1`.
- **Reset and teardown must purge the copy**, or a restore would bring deleted data back: `reset_service_logs` / `reset_service_rum` delete `ducklake/main/<table>/` via `_purge_ducklake_remote_dirs`; teardown relies on its bucket/prefix deletion options.
- **Restore** (`restore-data` CLI, `restore_from_cloud`) downloads missing files, creates absent tables from the Parquet schema, registers files with `ducklake_add_data_files` (not idempotent — it skips files `ducklake_list_files` already lists) and rebuilds the Postgres `ingested_files` index from each row's `_source_file` so raw `.gz` still in the bucket aren't ingested twice. Other operational metadata (cron history, usage log) is not restored.
- **Exposure window:** data ingested since the last successful push exists only locally. Push cost (Class A per file, 30-day minimum billing on rewritten files) is modeled in the Usage page calculator.
Regression tests: `test_push_to_cloud_never_deletes_when_the_catalog_reports_no_live_files` and `test_push_never_deletes_a_copy_that_predates_the_catalog` in `tests/core/test_storage.py`.

### 51. Field-picker reservoir samples have no request scope
`top_values.json` is an unbounded reservoir sample. `get_field_values` may use it only with no search, time bounds or active filters; scoped suggestions query the scoped data, keeping the own-field filter exclusion. Cases live in `tests/repositories/test_top_n_rollups_golden.py`.

### 52. Dashboard top-N must clip partial hours and include the exact endpoint
Whole-hour aggregates can't represent a partial leading or trailing hour. The dashboard's bounded top-N read selects only fully contained closed hours, merges scoped raw boundary slices and the inclusive endpoint, and keeps the direct active-hour path; virtual CSV fields need the same boundaries. Other callers keep the half-open contract. Read failures must not become a successful partial result on this exact-window path. Existing any-field coverage and bounded gap-healing limits still apply (this doesn't establish complete history). A stored rollup file can disappear between listing and execution, and refreshing the storage view can't repair SQL with explicit paths: on a missing-file error naming a selected rollup, discard the whole stored plan and read the complete scoped raw window (including virtual fields and the inclusive endpoint) — never merge a surviving partial plan. Unrelated I/O failures, permissions, cancellation and raw-fallback failures still propagate. Standard dashboard counts likewise preserve the raw query's inclusive end: the count rollup reader covers half-open whole-minute windows, partial-minute bounds fall back to raw counts, and the dashboard adds exact-end rows from the scoped base table after a successful rollup read. Real-DuckDB cases in `tests/repositories/test_top_n_rollups_golden.py` compare country/map/virtual counts across active/closed transitions, a missing bundle, stale listings and removal after selection.

### 53. Dashboard component cache keys must preserve exact window bounds
Standard dashboard aggregate and top-bot cache keys retain seconds and fractions in both bounds; minute-truncating explicit bounds lets different event windows reuse each other's results even when the SQL is correctly scoped. Relative-window quantization belongs to request-window resolution, not the component cache key. Identical-window hits, TTL and ingest invalidation are unchanged.

### 54. RUM fetch flushes must survive document dismissal
The generated tracker unrolls queued Faro events into empty-body POSTs, and its fetch path needs `keepalive: true` (see [backend/provision/rum_assets.py](backend/provision/rum_assets.py)); otherwise navigation can cancel the old document's requests before they reach the server, and can leave Chromium Fetch interception ids invalid, failing a guarded browser capture without an origin rejection. Keep disposable-analyst origin guards fail-closed — don't ignore cancelled interception commands. Both fetch- and XHR-triggered flushes must retain keepalive; the native `sendBeacon` path is separate.

### 55. Filtered dashboard signals must reuse the materialized target
The filtered dashboard already copies requested CSV backing columns into its connection-local temp table. WAF-signal and scoring-reason expansions must read that current query target, not rescan the original storage view with the same filters. The unfiltered rollup path creates no temp, so its missing-rollup fallback uses the original table with exact inclusive bounds. Keep duplicate-token counts, blank/null handling and masking unchanged; don't share these temps with the parallel bots connection. **Projection follows enabled downstream consumers, not source predicates:** requested card/backing columns only for top-N, country independently for the map, connection counts for the histogram, timestamp plus the selected chart SQL's inputs for time series (resolve chart inputs alongside its expression so fallback metrics can't drift from the projection). Count-only requests project a constant; never restore `SELECT *` when no downstream column is needed. The canonical dashboard golden suite covers section demand, metric fallbacks and exact counts.

### 56. Single-host deploys: host bind-mounts must be owned `1000:1000`
Production runs `docker-compose.yml` plus `docker-compose.prod.yml` ([docs/deploy/](docs/deploy/)); omitting the prod overlay silently drops `network_mode: host`, loopback-only binds and the anti-XFF-spoofing hardening (`TRUSTED_PROXY_IPS`, `REQUIRE_PROXY_HEADERS`) while the stack still comes up. The backend image runs as `app` (UID/GID 1000) and the Dockerfile chowns the in-image paths, but nothing enforces ownership of the **host-side** bind-mount sources (`data/`, `configs/`, `cache/`). If those drift to another UID:GID the container starts and then crash-loops on its first write with `PermissionError` / `attempt to write a readonly database`, possibly long after the change. Before restarting containers verify `stat -c '%u:%g' data configs cache` reads `1000:1000`.

### ClickHouse diagnostic replay (ADR-20, ADR-21)
`scripts/clickhouse_replay.py` and the admin endpoints in [backend/routers/admin/clickhouse.py](backend/routers/admin/clickhouse.py) (`GET /api/admin/clickhouse/status?service_id=...`, `POST /api/admin/clickhouse/replay`) are diagnostic index/replay tooling, not dashboard serving: dashboard endpoints call the existing DuckLake repository directly regardless of `CLICKHOUSE_ENABLED`, and there is no separate serving flag. Both endpoints stay behind the admin prefix block (never allow them through the analyst middleware). Use real psycopg pool transactions, not a wrapper whose `commit()` is a no-op. All fact reads require `FINAL`. Only complete, content-verified generations may activate; rebuilds allocate a new generation and load every artifact. Status reports service-scoped publication history and actionable backlog age; disabled/unavailable measurements are null, not zero; enabled dependency failures return 503; an expired active generation stays visible with `expired=true`. Replay requires a read-write service and exactly one `dataset_id` (new generation) or `generation` (resume); `dry_run=true` validates retained metadata references without FOS access, audit writes or manifest mutations. The HTTP cap is 100 artifacts per request **and per dataset** (activation verifies the whole dataset); larger datasets use the CLI. Apply uses only `FosArtifacts.load`, never export, and audits requested/completed/failed operations with safe counts; responses (including debug-opted-in errors) must not disclose artifact URLs or credentials. Metrics: query ids belong in logs, never metric labels; unknown lag/disk is not zero; enabled callbacks share a bounded authenticated disk probe and never run full generation readiness; replay commands force-flush telemetry before exit.

## AI Agent Directives

The operating policy is [AGENTS.md](AGENTS.md). The domain-specific rules below supplement it.

### Working agreement

1. **Read first.** Read the relevant page or cron spec (`docs/pages/{page}.md`, `docs/cron/jobs/{cron}.md`), the architecture docs, deployment runbooks and tests before changing code. Resolve questions from those sources before asking.
2. **Choose the simplest approach consistent with existing patterns,** and keep authoritative documentation (this file, ADRs, page/cron specs) current with material behavior changes.
3. **Verify the result:** run the relevant tests, lint and formatting; verify affected deployment-mode and role behavior and telemetry attribution.
4. **Keep explicit boundaries:** commit only your own changes (by pathspec); push feature/release branches only; never push `main`, force-push, create or merge PRs, publish or deploy without explicit approval from the maintainer.

### Testing

1. **Use focused local checks and full hosted gates** — see [Testing](#testing).
2. **Never delete or weaken existing tests**; coverage, vitest and security-regression floors are ratchets.
3. **Add tests for every non-trivial change:** new endpoint → router test; new utility → unit test; bug fix → a regression test that would have caught it.
4. **Extend the canonical domain test file** (e.g. `tests/core/test_clickhouse.py`, `tests/repositories/test_security.py`, `frontend/__tests__/app/dashboard.test.tsx`, `frontend/e2e/pages/dashboard.spec.ts`) instead of creating a one-off file per feature.
5. **Prefer integration tests over pure mocks** (`in_memory_duckdb` + `client` test real SQL while staying fast). Test error paths: missing config, external 4xx/5xx, empty DB.
6. **Verify in the real app when you can:** start the server, drive the UI, watch the logs (every query and FOS call is logged). Run the Playwright suite for changes touching the admin shell, dashboard, provision wizard, custom-field drawer or share-login.

### Code changes

1. **No backward-compatibility shims** for fields that don't exist in the schema (e.g. no `stats_token` or `cdn_domain` fallbacks).
2. **Rule of 150 lines & ≥ 2 callers for sub-modules:** don't split code into a new `_sections/`, rollup writer, model file or helper module unless 2+ independent callers reuse it or it exceeds ~150–200 lines of cohesive logic. Prefer data-driven tables/loops and shared helpers in existing domain modules over copy-pasted boilerplate.
3. **Never interpolate user-controlled values into SQL** — `_safe_table()`, `require_safe_ident`, `escape_sql_literal`, or parameters (`con.execute("... WHERE x = ?", [value])`).
4. **Handle `openapi-fetch` errors explicitly.**
5. **Keep Python imports at module level** (Trap #2). Run `ruff format` before committing.
6. **Don't reintroduce removed code:** audit-helper/audit-repository pass-throughs, `QueryRunner.safe_select` (use `actual_cols` directly), and a `duckdb.log_audit_event` shim (call `metadata.record_audit` directly; test patches target `backend.core.metadata.audit.record_audit` or the package re-export).
7. **Architecture rules to preserve:** `backend/core/*` must not import schedulers or routers (inject callables; enforced by `make import-contracts`); tenancy stays structurally unbypassable through `RequestContext`; split packages keep re-export shims via `install_shim_module`; use named exception classes and explicit retry policies instead of generic `except Exception`; keep `retry_call` mechanics-only.
8. **Pushback list (investigated and rejected):** generic schema codegen for filter specs; a centralized `RoleProvider` (role is two orthogonal flags, `analyst_session` × `is_remote_analyst`); pre-formatted server-side response values (tables need raw values for click handlers); cache-coherence "state machines" (the bottleneck is DuckDB view rebuild time, not cache policy); a unified `QueryExecutor` for retry (stale-view and compaction-race recoveries differ); threading `RequestContext.cached_temps` through repository signatures.

### Secrets & sensitive data

1. **Scan for secrets before every commit.** `make secret-scan` (gitleaks) is wired into `make ci` and the pre-commit hook (`uv run pre-commit run --all-files`); CI also runs it.
2. **Allowlist suppression order** for a legitimate placeholder: inline `# gitleaks:allow` on the line; fingerprint line in `.gitleaksignore`; path regex in `.gitleaks.toml` `[allowlist] paths` for a whole fixture directory.
3. **Never commit a real credential to suppress the scanner.** If a legitimate secret must live in the tree (e.g. an SSH public key as a trust anchor), document why next to the allowlist entry.
4. **Never put real customer values in code, scripts, tests or docs:** Fastly service IDs (use `<service-id>` or `${FASTLY_SERVICE_ID:?}`), bucket names, real domains/IPs (published Fastly edge ranges are fine), real emails (use `you@example.com`), or screenshots showing them. Fixtures use placeholders (`TestLogSvcABC123`, `FAKE_TOKEN`, `"FROM_CONFIG"`); real values come from env vars or gitignored per-host config.
5. **Never commit:** `.env`, `configs/*.json`, `data/system/` (SSH key, share state, OAuth provider config), `.scoring/` (per-deployment AES keys), or real production traces. Verify `.gitignore` covers any new directory of generated content.

### Provisioning wizard

Thread the step-2 token into every Fastly-credentialed call; keep step order `mode → token → service → storage → ngwaf → fields → execute`; `execute` streams SSE.

### New analytics pages

1. Pattern: `backend/routers/{name}.py` + `backend/repositories/{name}.py` + `backend/models/` schema + `frontend/app/{name}/page.tsx` (+ `loading.tsx`).
2. All DuckDB queries in repositories use `_safe_table()`.
3. Every new endpoint gets at least one test in `tests/routers/`.
4. Regenerate OpenAPI types after the endpoint lands (`make gen-types`).

## Keeping This File Current

Update this file in the same commit that introduces:
- new or removed API endpoints
- new background job types or schedule changes (verify the [Ingest Pipeline](#ingest-pipeline) table against `backend/cron/scheduler/runner.py`)
- config schema changes
- new traps or gotchas you fixed (other contributors and agents will hit them again), stated as an invariant — "do X because Y" — plus the test that pins it, not as a dated story
- workflow changes that affect the personas

If a section describes code or behavior that no longer exists, fix or delete it immediately. Stale docs are worse than missing docs. Verify any path you add with `git ls-files`.
