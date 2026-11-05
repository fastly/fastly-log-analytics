# Project Reference for AI Agents

Architecture, subsystem reference, shared helpers, and numbered correctness traps for the Fastly Log Analytics codebase.

## Companion Docs

- [README.md](README.md) — user-facing overview, features, installation, and local commands.
- [CONTRIBUTING.md](CONTRIBUTING.md) — contributor setup, test commands, and PR checklist.
- [MONKEYPATCHES.md](MONKEYPATCHES.md) — active third-party runtime monkeypatches and removal criteria.
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — storage layout, ClickHouse analytical store, stateless ingest worker & batch publication pipeline, health probe, and VCL log format.
- [docs/features.md](docs/features.md) — feature reference and VCL field groups (`A`–`M`).
- [docs/cron/README.md](docs/cron/README.md) and [docs/cron/jobs/](docs/cron/jobs/) — background task architecture, 3-executor thread pool, and per-cron specifications.
- [docs/pages/README.md](docs/pages/README.md) and [docs/pages/](docs/pages/) — per-page frontend/backend specifications and role access matrix.
- [frontend/docs/routing.md](frontend/docs/routing.md) — Next.js App Router topology, SSR bootstrap pre-fetch invariants, and navigation/loading conventions.
- [docs/adr/](docs/adr/) — architecture decision records (ADR-16 ingest ledger, ADR-21 ClickHouse serving & recovery architecture, ADR-22 Postgres-only metadata).
- [docs/deploy/](docs/deploy/) — deployment runbooks; [docs/runbooks/](docs/runbooks/) — capacity, recovery, reader parity, and analyst SQL console runbooks.
- [docs/session_scoring_runbook.md](docs/session_scoring_runbook.md) — edge L1/L2 session scoring and Compute Wasm deployment.

Trap numbers (`#1`–`#56`) are stable identifiers cited by code comments and docs; gaps in the numbering are removed entries.

## Table of Contents

1. [Overview](#overview)
2. [Architecture & Package Map](#architecture)
3. [Key Systems](#key-systems)
4. [Provisioning](#provisioning)
5. [Frontend Patterns](#frontend-patterns)
6. [Testing](#testing)
7. [Shared Helpers](#shared-helpers)
8. [Traps & Gotchas](#traps--gotchas)

---

## Overview

FastAPI + Next.js dashboard for Fastly Real-Time VCL logs streamed to Fastly Object Storage (FOS). It continuously ingests `.gz` / `.zst` log files via a stateless ingest worker (`python -m backend.high_scale.worker`), checkpoints canonical Archive Parquet to FOS, publishes verified batches into ClickHouse, and exposes per-service analytics, alerts, NGWAF bot detection, custom log fields, edge session scoring, RUM, and a live-share feature for read-only analyst access. The same unified architecture scales seamlessly from a 1-worker laptop Docker Compose setup (~2–4 GiB RAM with `HIGH_SCALE_WORKER_THREADS=1`, `HIGH_SCALE_MAX_BATCH_BYTES=6291456`, `HIGH_SCALE_MAX_BATCH_OBJECTS=10`) up to 1,000,000 RPS on Kubernetes.

**Stack:**
- Backend: FastAPI, ClickHouse, PostgreSQL 16, stateless ingest worker (`python -m backend.high_scale.worker`), APScheduler, boto3 (S3-compatible FOS), `uv`, `ruff`, `mypy`, `pytest`
- Frontend: Next.js 16, React 19, TanStack Query v5, Zustand, shadcn/ui, openapi-fetch, vitest, Playwright
- Storage: FOS (raw logs + canonical Archive Parquet under `{prefix}/archive/`), ClickHouse (analytical MergeTree tables & projections), PostgreSQL 16 for all operational metadata and ingest control state ([ADR-22](docs/adr/22-postgres-only-metadata.md))
- Optional: [`falco`](https://github.com/ysugimoto/falco) VCL linter — detected via `shutil.which("falco")`, degrades to regex checks when absent

**VCL editing:** a log format string, custom field `vcl_log_expression`, or any VCL snippet must pass `falco lint`. Use `+` for string concatenation; wrap literals in heredoc strings (`{"literal"}`). Call sites: [backend/core/vcl_generator.py](backend/core/vcl_generator.py), [backend/provision/fastly_api.py](backend/provision/fastly_api.py), [backend/routers/services/custom_fields.py](backend/routers/services/custom_fields.py). See [docs/ARCHITECTURE.md §3](docs/ARCHITECTURE.md#3-vcl-log-format--custom-fields).

**Deploying:** see [docs/deploy/](docs/deploy/). Single-host production runs `docker-compose.yml` plus the `docker-compose.prod.yml` overlay; Kubernetes uses the Helm chart in [deploy/chart/](deploy/chart/).

## Architecture

For the layered storage layout (FOS raw logs, FOS canonical Archive Parquet, ClickHouse serving tables, PostgreSQL 16 metadata and control plane), batched ingest publication pipeline, and deep health probe (`GET /api/health?deep=1`), see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

### Package map

Large modules are split into packages whose `__init__.py` re-exports the historical public surface via `install_shim_module` ([backend/utils/module_shim.py](backend/utils/module_shim.py)); the proxy forwards `monkeypatch.setattr` / `delattr` to the owning submodule so tests patching the package path still work. `backend/core/*` never imports `backend/cron` or `backend/routers` (enforced by `make import-contracts`).

| Package | Contents |
|---|---|
| [`backend/core/clickhouse/`](backend/core/clickhouse/) | `constants.py`, `params.py`, `client.py`, `health.py` |
| [`backend/core/metadata/`](backend/core/metadata/) | `pg_pool.py` (pooled psycopg connections), `pg_schema.py` (schema bootstrap), `admin_state.py`, `alerts_db.py`, `state_sync.py`, `usage_log.py`, `fos_usage.py`, `cron_runs.py`, `slow_queries_db.py`, `ingest_log.py`, `quarantine_evidence.py`; `base.py` re-exports the surface |
| [`backend/core/ingest/`](backend/core/ingest/) | `parser.py` (pure-Python JSON parsing, schema coercion, quarantine routing) |
| [`backend/core/request_telemetry/`](backend/core/request_telemetry/) | `instruments.py` (OTel), `query_registry.py` (live ClickHouse & Postgres query registry), `tracker.py` (`RequestTelemetry`, request-metrics observer), `middleware.py` |
| [`backend/core/`](backend/core/) singles | `fos_client.py` (FOS S3 client, CDN helpers, service source resolution, geo/DMA & ASN enrichment, cid-salt & metadata passthroughs), `reset.py`, `catalog.py` (`LOG_FIELD_CATALOG`), `log_fields.py`, `presets.py`, `vcl_generator.py`, `metric_expressions.py`, `insight_definitions.py`, `request_context.py`, `retry.py`, `sql_validator.py`, `rum_ingest.py`, `lifespan.py`; `fastly/`, `oauth/`, `realtime/`, `share_db/` |
| [`backend/high_scale/`](backend/high_scale/) | Sole analytical reader & ingest engine: `archive/`, `postgres_control/`, `ingest_controller/`, `insights/`, `worker.py`, `query.py`, `query_service.py`, `recovery.py`, domain readers (`dashboard`, `origin`, `performance`, `security`, `network`, `sessions`, `cmcd`, `rum`), `sql/` schemas |
| [`backend/repositories/`](backend/repositories/) | Pure-Python response shaping, caching, filter helpers, bot classification, value bucket shapers, insight definitions/cache (`utils/response_cache.py`, `utils/response_shaping/`, `utils/filters.py`, `security/watchdog.py`, `value.py`, `insights/`, `query.py`, `assets.py`) |
| [`backend/cron/`](backend/cron/) | `scheduler/{progress,lifecycle,runner}.py` (`Scheduler`, `@cron_task`, `cron_run`, `record_skip_row`), `jobs/{_common,sync,rum_sync,rum_ledger,metadata,maintenance,insights_prewarmer}.py`. Import `get_scheduler`/`Scheduler`/`cron_task`/`cron_run` from `backend.cron.scheduler` and each `_run_*` body from its `backend.cron.jobs.*` module |
| [`backend/routers/`](backend/routers/) | One module per domain; packages `admin/`, `services/` (`core`, `crud`, `vcl`, `custom_fields`, `views`, `storage`), `session_scoring/`, `share_admin/`, `rum/`; `root_system.py` (middleware, exception handlers, health/debug) |
| [`backend/models/`](backend/models/) | Pydantic schemas: `analytics/`, `common/`, `admin/` |
| [`backend/provision/`](backend/provision/) | `orchestrator.py`, `fastly_api.py`, `fos_setup.py`, `cli.py`, `cmcd_fields.py`, `rum_assets.py`, `rum_orchestrator.py`, `declarative/` (reconciler), `session_scoring/` |
| [`backend/utils/`](backend/utils/) | `telemetry/` (structlog config, Prometheus metrics, log flusher, proxy, system stats), `bot_sources/`, `tunnel/`, `remote_access.py`, `router_utils.py`, `error_utils.py`, `sse.py`, `date_utils.py`, `bounded_cache.py`, `time_window.py`, `hll.py`, `pii_masking.py`, `countries.py`, `pop_utils.py` |

Request-scoped context: [`backend/core/request_context.py`](backend/core/request_context.py) bundles `service_id`, `source`, `high_scale`, `telemetry`, `analyst_session`, and the opaque AES-GCM session-detail token helpers (`seal_session_token`, `open_session_token`). It folds `require_service_access` into construction, so no path builds a context without enforcing tenancy.

**Env / config handling.** There is no central settings class. App-level env vars are read via `os.getenv` at their use sites (e.g. `OTEL_EXPORTER` in `request_telemetry/`, `STRUCTLOG_FORMAT` in `utils/telemetry/`, `CLICKHOUSE_URL` in `config.py`). Per-service credentials/settings live in `configs/{id}.json`, loaded by [backend/config.py](backend/config.py) (`lf = cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []}`).

### Personas

- **Admin** — full ingest/management surface. Config: `configs/{logging_service_id}.json`.
- **Analyst — live shared instance.** Remote read-only access through the admin's running process over an HTTPS `public_endpoint`; no FOS credentials. Detected on the frontend by `bootstrap.settings.is_remote_analyst`. See [docs/pages/admin/share.md](docs/pages/admin/share.md) and [docs/pages/share-login.md](docs/pages/share-login.md).

## Key Systems

### Background tasks & cron scheduler ([backend/cron/](backend/cron/))
See [docs/cron/README.md](docs/cron/README.md) for the job inventory, 3-executor thread pool (`streaming`, `maintenance`, `default`), `FLA_DEV_NO_CRONS=1` / `FLA_DEV_LOCAL_CRONS=0` safety gates, and per-job specs in [docs/cron/jobs/](docs/cron/jobs/). Every job body owns its lease/progress/terminal-row cycle through `cron_run` — see [Shared Helpers](#shared-helpers).

### Subsystem & page reference
Each user-facing and admin subsystem has a dedicated specification:
- **Insights & Prewarmer:** [docs/pages/insights.md](docs/pages/insights.md) and [docs/cron/jobs/insights-prewarmer.md](docs/cron/jobs/insights-prewarmer.md).
- **ClickHouse Data Plane & Stateless Ingest Worker:** [docs/runbooks/high-scale-capacity-and-recovery.md](docs/runbooks/high-scale-capacity-and-recovery.md) (batched ingest, RUM cursor reinspection, recovery math), [docs/runbooks/high-scale-reader-parity-backfill.md](docs/runbooks/high-scale-reader-parity-backfill.md) (reader/schema contracts, percentiles, filters, ClickHouse dialect traps, archive retention, backfill CLI), [docs/runbooks/high-scale-analyst-console.md](docs/runbooks/high-scale-analyst-console.md) (least-privilege `fla_cv_*` SQL console boundary), [docs/pages/high-scale-request-facts.md](docs/pages/high-scale-request-facts.md), and [docs/pages/admin/clickhouse.md](docs/pages/admin/clickhouse.md) (diagnostic index & replay).
- **Session Scoring (Edge L1/L2 + Compute Wasm):** [docs/session_scoring_runbook.md](docs/session_scoring_runbook.md), [docs/pages/admin/session-scoring.md](docs/pages/admin/session-scoring.md), and [docs/pages/sessions.md](docs/pages/sessions.md) (including opaque AES-GCM `session_token` lookup and explicit L2 enforcement opt-in).
- **Live Dashboard Sharing & OAuth/OIDC:** [docs/pages/admin/share.md](docs/pages/admin/share.md) and [docs/pages/share-login.md](docs/pages/share-login.md).
- **Admin mTLS Gateway:** [docs/deploy/admin-mtls.md](docs/deploy/admin-mtls.md) (`ADMIN_GATEWAY_SECRET` / `X-Admin-Gateway-Token` / `ADMIN_GATEWAY_REQUIRED=1`).
- **Live Query Monitor:** [docs/pages/admin/queries.md](docs/pages/admin/queries.md) (`query_registry.py`, live ClickHouse and PostgreSQL query tracking).
- **FOS Usage Logging, Log-Line Accounting & Quarantine:** [docs/pages/admin/usage-log.md](docs/pages/admin/usage-log.md), [docs/pages/admin/overview.md](docs/pages/admin/overview.md), and [docs/cron/jobs/gap-heal.md](docs/cron/jobs/gap-heal.md).
- **Control Room, CMCD Streaming & Fastly Value:** [docs/pages/control-room.md](docs/pages/control-room.md), [docs/pages/streaming.md](docs/pages/streaming.md), and [docs/pages/fastly-value.md](docs/pages/fastly-value.md).
- **Alerts & Saved Views:** [docs/pages/alerts.md](docs/pages/alerts.md) (`alerts` and `views` tables in Postgres).
- **NGWAF Bot Detection:** [docs/cron/jobs/ngwaf-sync.md](docs/cron/jobs/ngwaf-sync.md) (`_run_ngwaf_bot_sync` in [backend/cron/jobs/metadata.py](backend/cron/jobs/metadata.py), bot classifiers in [backend/repositories/security/watchdog.py](backend/repositories/security/watchdog.py), and ClickHouse security readers in [backend/high_scale/security.py](backend/high_scale/security.py); workspace listing key is `"data"`, see [Trap #3](#3-falsy-empty-list-in-or-chains)).
- **State Sync & CDN-Fronted Log Delivery:** `export_admin_state` ([backend/core/metadata/state_sync.py](backend/core/metadata/state_sync.py)) writes `audit_logs`, `views`, `log_format_history`, and `custom_fields` to `{prefix}/meta/admin_state.json` (`read_write` services only; alerts are not synced). FOS reads are fronted by a dedicated Fastly CDN VCL service (`cdn_service_id`, `cdn_url`, `cdn_secret`), separate from the logging service ID ([Trap #6](#6-configs-are-keyed-by-logging-service-id-not-cdn-service-id)).

### ClickHouse analytical store & stateless ingest pipeline ([backend/high_scale/](backend/high_scale/))
- **Analytical serving:** All dashboard, domain analytics, session scoring, RUM, insights, alerts, and SQL console queries execute against ClickHouse (`request_facts`, `rum_vitals`, `rum_errors`, and least-privilege `fla_cv_*` analyst views) via `backend/high_scale/` readers and `backend/core/clickhouse/client.py`.
- **Stateless ingest worker (`python -m backend.high_scale.worker`):** Claims fenced source batches from the PostgreSQL control plane (`backend/high_scale/postgres_control/`), decodes compressed logs concurrently in memory, writes canonical Archive Parquet batches to Fastly Object Storage (`{prefix}/archive/`), inserts rows into ClickHouse as `pending`, verifies counts, flips the batch publication gate to `visible`, and acknowledges the batch in PostgreSQL.
- **Lightweight default footprint:** Runs on a laptop or single VM with `HIGH_SCALE_WORKER_THREADS=1`, `HIGH_SCALE_MAX_BATCH_BYTES=6291456` (6 MiB), and `HIGH_SCALE_MAX_BATCH_OBJECTS=10` inside a 2 GiB worker container alongside a 2–4 GiB ClickHouse container, and scales horizontally across Kubernetes worker pods up to 1,000,000 RPS bursts.

### PostgreSQL 16 operational metadata & control plane ([backend/core/metadata/](backend/core/metadata/), [backend/high_scale/postgres_control/](backend/high_scale/postgres_control/))
PostgreSQL 16 (`METADATA_DSN`) is the single transactional store for operational state (`alerts`, `views`, `audit_logs`, `cron_runs`, `sources`, `ingested_files`, `quarantine_evidence`, `usage_log`, `remote_invites`, `remote_sessions`, `ngwaf_bots`, `rdns`) and ingest control-plane tables (`high_scale_source_ledger`, `high_scale_source_cursors`, `high_scale_partition_cursors`, `high_scale_archive_manifests`, `high_scale_publication_batches`). Pooled thread-local connections are managed by `pg_pool.py` (`METADATA_PG_POOL_MAX`, default 64 on the API backend and 6 on stateless workers).

### Response telemetry middleware & multiplexed admin SSE
- **`TelemetryResponseBodyMiddleware`** ([backend/core/request_telemetry/middleware.py](backend/core/request_telemetry/middleware.py)): backstop for endpoints returning a plain `dict` instead of `BaseResponse.with_telemetry`; injects `_debug_queries` / `_debug_calls` / `_is_cached` when `x-debug-responses: 1` (or `fla.debugResponses` cookie on SSR) is set under `DEBUG_RESPONSES`. **Must be added INNER to `CompressMiddleware`** (`add_middleware(TelemetryResponseBodyMiddleware)` before `add_middleware(CompressMiddleware)` in [backend/routers/root_system.py](backend/routers/root_system.py)) so it sees raw JSON.
- **Multiplexed admin SSE** ([backend/routers/admin/events.py](backend/routers/admin/events.py), [frontend/components/SyncStatusBadge/](frontend/components/SyncStatusBadge/)): `GET /api/admin/events/stream?channels=sync_status,cron_runs,system_metrics,share` mounts a single typed-envelope stream demuxed on the frontend into React Query caches. Admin real-time feeds belong on this multiplexed stream rather than new dedicated SSE endpoints. `build_share_live_payload` lives in `backend.utils.tunnel` so routers don't import each other.

## Provisioning

- **UI wizard** ([frontend/components/ProvisionWizard/](frontend/components/ProvisionWizard/)): step order `token → service → storage → ngwaf → fields → execute` ([docs/pages/README.md](docs/pages/README.md)).
- **CLI** ([backend/provision/cli.py](backend/provision/cli.py)): `python -m backend.provision.cli <subcommand>` (`provision`, `teardown --service-id {id}`, `update-logs`, `update-cdn`, `enable-scoring`, `disable-scoring`, `list-groups`, `list-fields`).
- **Teardown** (`perform_teardown` in [backend/provision/orchestrator.py](backend/provision/orchestrator.py)): removes the FOS logging endpoint, CDN VCL service, FOS access key, local config, and service data/metadata. See [Trap #31](#31-teardown-dereference-before-delete-and-enumerate-what-you-remove) for strict dereference-before-delete ordering.

## Frontend Patterns

Next.js 16 App Router docs are vendored in `frontend/node_modules/next/dist/docs/` (referenced by [frontend/AGENTS.md](frontend/AGENTS.md)). For the route rendering table, SSR bootstrap pre-fetch invariants (`node:http.request`, `X-Remote-Analyst: 1`, `dynamic = "force-dynamic"`, seed key matching), and Next.js navigation/loading conventions, see [frontend/docs/routing.md](frontend/docs/routing.md).

**Type-safe client** ([frontend/lib/api.ts](frontend/lib/api.ts)):
```typescript
import createClient from "openapi-fetch";
import type { paths } from "@/types/api.generated";
const client = createClient<paths>({ baseUrl: "" });
```
A global middleware checks `response.ok` and **throws a JS Error** on 4xx/5xx. FastAPI wraps errors as `{"detail": {"error": "..."}}`; extract with `r.error?.detail?.error || r.error?.error` or use `extractApiError`. Use try/catch or TanStack Query's `isError`; never `.then((r) => r.data?.x || fallback)` without checking `r.error`. **Generated types:** `cd frontend && npm run gen:types` after backend endpoint/model changes (`make gen-types`; `make ci` runs it). **Streaming/binary endpoints** (SSE, blobs) use raw `fetch()`.

### Canonical patterns

1. **`response_model=` on every router handler.** Without it OpenAPI emits `Record<string, unknown>`. Routes using `Depends(get_source)` also lift `service_id: str` into the signature. For dict-shaped endpoints use a model with `extra="allow"` + `response_model_exclude_unset=True`, string (`.isoformat()`) timestamps, fields derived from the producer's actual payload — byte-identical wire output, typed OpenAPI. SSE/file/binary routes carry an explicit exemption comment.
2. **Page context hooks** — `useTimeRange`, `useFilterUrlWriteback`, `useServiceQuery`, `useAnalyticsReportQuery`, `useReportRangeAnchor` ([frontend/hooks/useDashboardCards.ts](frontend/hooks/useDashboardCards.ts)); `useTimezone`, `useNowMs` ([frontend/hooks/usePersistentPreferences.ts](frontend/hooks/usePersistentPreferences.ts)); `useMounted`, `useEffectiveServiceId`, `useIsDataReady` ([frontend/hooks/useBootstrap.ts](frontend/hooks/useBootstrap.ts)).
3. **`ReportLayout`** ([frontend/components/ReportLayout.tsx](frontend/components/ReportLayout.tsx)) for analytics pages — bundles page context + report config + debounced filter payload + `useViewMetricUrlSync` + `useServiceQuery` + `ChartIntervalButtons` + `ReportShell`. Fall back to `ReportShell` only for multi-query or non-standard chrome.
4. **`HelpDialog`** for help content. **`createBaseMap` + `addCountryBaseLayer`** ([frontend/components/Map/NetworkMap/types.ts](frontend/components/Map/NetworkMap/types.ts)) for MapLibre setup. **`CardErrorState`** (alert + Retry) for cards whose query can fail.
5. **`analystFetch`** ([frontend/hooks/useAnalystAndServiceHooks.ts](frontend/hooks/useAnalystAndServiceHooks.ts)) — shared analyst fetch + envelope unwrapping. **`getPageLoadId()`** ([frontend/lib/api.ts](frontend/lib/api.ts)) is the single page-load correlation id source.
6. **PoP labels** render as `DEN (Denver, CO - USA)` via the shared helper seeded from `bootstrap.pop_geo`; keep the raw code for click-to-filter.
7. **Route decomposition** — route pages keep `page.tsx` as a thin server/wrapper entry and split UI into `_sections/<Route>Client.tsx` plus focused section panels, sharing `useReportPageState` ([frontend/hooks/useReportPageState.ts](frontend/hooks/useReportPageState.ts)). `createRouteErrorBoundary` ([frontend/app/error.tsx](frontend/app/error.tsx)) builds route `error.tsx` components. `resolveDefaultSsrRangeKey` ([frontend/lib/ssr/upstream.ts](frontend/lib/ssr/upstream.ts)) resolves default SSR range keys.
8. **Backend:** `metadata.record_audit(service_id, event_type=..., details=...)`; `date_utils.parse_iso_utc` / `iso_z`; `@cron_task`; `SectionTimer`, `_compact_sql_for_debug`, and `_safe_table` in `backend/repositories/utils/response_shaping/`.

## Testing

### Test commands & CI gates

```bash
uv run pytest tests/routers/test_dashboard_router.py
uv run pytest -k "test_specific_feature"
cd frontend && npx vitest __tests__/hooks/useBootstrap.test.ts
cd frontend && npx playwright test --project=chromium
make fast-ci   # OpenAPI drift, typechecks, linters, frontend contract tests, core unit dirs
make ci        # Full CI gate
```

CI enforces coverage and code-quality ratchets:
- Backend coverage floor: `--cov-fail-under=85`
- Frontend vitest coverage floors: lines 66 / statements 65 / functions 54 / branches 52
- Security regression count floor: `scripts/check_security_regression_count.sh`
- Pattern ratchet guards: `make retry-guard`, `make coalesce-guard`, `make dateparse-guard`, `make urllib-guard`, `make import-contracts`, `make secret-scan`

### Backend (`tests/`)

Fixtures in [tests/conftest.py](tests/conftest.py): `service_source`, `client` (FastAPI `TestClient`); mock data and ClickHouse test doubles in [tests/_helpers/](tests/_helpers/__init__.py).

**Per-test isolation:** The autouse `isolate_metadata_db` fixture probes `_PER_TEST_RESET_TABLES` with one `UNION ALL … WHERE EXISTS` query and `TRUNCATE`s only tables that hold rows. **Any new Postgres metadata table that tests write to must be added to `_PER_TEST_RESET_TABLES`** or rows leak between tests. Module-level caches drain through `CacheRegistry.clear_all()`. Parallel runs share one Postgres DSN (`_pg_worker_schema` per xdist worker). Hypothesis `RuleBasedStateMachine` tests call `metadata_db.teardown(service_id)` in `@initialize()`.

### Frontend (`frontend/__tests__/`, vitest + jsdom)

[frontend/vitest.setup.ts](frontend/vitest.setup.ts) provides global mocks (Next router, `localStorage`, `matchMedia`). `globals: false`, so the setup file explicitly registers `afterEach(cleanup)` and MSW's `server.listen()` at module top level — see [Traps #17/#18](#17-msw--openapi-fetch-ordering--serverlisten-must-run-at-module-load).

### E2E ([frontend/e2e/](frontend/e2e/), Playwright)

Chromium, Firefox and WebKit run in CI. A mock backend boots from `frontend/e2e/global-setup.ts` under `FASTLY_MOCK_MODE=1` on port 18004 (frontend 13004, via `playwright.config.ts`). SSE specs split on `/\r\n\r\n|\n\n|\r\r/` (`sse-starlette` emits `\r\n\r\n`). SSR-seeded route fixtures cover both server and browser requests via the contract fixture backend ([scripts/run_contract_backend.py](scripts/run_contract_backend.py)).

### Performance gates and synthetic data tooling

- **Perf gate:** `PERF_NUM_ROWS` selects a scale in [tests/perf/baseline.json](tests/perf/baseline.json): `smoke_100k` (default, PR-blocking), `mid_500k` (opt-in), `nightly_1m` ([.github/workflows/perf-nightly.yml](.github/workflows/perf-nightly.yml)). Refresh: `PERF_NUM_ROWS=500000 uv run python scripts/emit_perf_latest.py && bash scripts/perf_gate.sh`.
- **Synthetic data:** `uv run python scripts/load_test/generate_synthetic_traffic.py` with `--target fos` (gzipped NDJSON to FOS, exercising discovery/ledger/worker ingest) or `--target clickhouse` / `--with-clickhouse` (seeds `request_facts` + publication batches). Scenarios: `diurnal` (default), `ddos-spike`, `origin-5xx-outage`, `bot-scrape`, `slow-network`.
- **Live pipeline lag:** [scripts/dev/scale_harness.py](scripts/dev/scale_harness.py) sends staged concurrent HTTP load to a deployed edge URL and samples `/api/log-extents` for ingest freshness lag (rates above 1,000 RPS require `--allow-high-rate`).
- **Section-level budgets:** `scripts/lib/performance_budgets.json` defines per-page section-level performance budgets.

## Shared Helpers

Single owners for cross-cutting patterns. Ratchet gates (`make retry-guard`, `coalesce-guard`, `dateparse-guard`, `urllib-guard`; baselines only go down) fail CI on new bespoke copies. Tests monkeypatch the primitives at their canonical homes, so keep call-time imports as written in each module.

### Cron lifecycle ([backend/cron/scheduler/lifecycle.py](backend/cron/scheduler/lifecycle.py))
- `cron_run(service_id, task, ...)` is the per-run context manager every job body uses: resolve the source, acquire the `(service_id, task)` lease via `start_cron_run`, start/end progress, write a terminal `cron_runs` row (duration/status/exception), and run `finalize_*` backstops in a `finally`. Use `run.skipped`, `run.success(...)`, `run.warn(...)`, `run.fail(...)`, `run.record(...)`, `run.log_progress(...)`. Body exceptions are recorded and swallowed by default (`suppress_exceptions=True`); `error_summary` / `error_result` shape the auto-`error` row.
- `CronLeaseHeldError` ([backend/core/metadata/cron_runs.py](backend/core/metadata/cron_runs.py), a `RuntimeError` subclass) means another run holds the lease: a clean skip, never an error. Don't catch bare `RuntimeError` around `start_cron_run`.
- `record_skip_row(source, task, summary, status="skipped", ...)` writes the single terminal row for a pre-flight skip that never takes the lease (nothing to evaluate, disk too full).
- HTTP-triggered thread spawns (`routers/admin/compaction.py`, `utils/router_utils.py`) own a run id and pass it as `cron_run(run_id=...)`. A job that skips writing a terminal status leaves a `running` row that freezes ingestion (the deep health check flags it past `_STUCK_SYNC_RUNNING_MINS`).

### Fastly draft versions ([backend/provision/declarative/fastly_ops.py](backend/provision/declarative/fastly_ops.py), [backend/provision/cli.py](backend/provision/cli.py))
Service changes go through the clone → mutate → validate → activate flow with rollback to the previous active version on any failure (a failed rollback is logged at WARNING and never masks the original error); validation that returns anything but `status == "ok"` raises. Reuse the draft-version context manager and pass the Fastly client in so each orchestrator keeps its own module-level `fastly` seam for tests.

### Service config writes ([backend/config.py](backend/config.py), [backend/utils/router_utils.py](backend/utils/router_utils.py))
- Every read-modify-write goes through `mutate_config(service_id, fn)`: `config_lock` (cross-process `flock` sidecar, bounded 5 s wait, re-entrant per thread, degrades to unlocked with `config_lock_degraded`), a fresh read bypassing the in-process cache, `fn(cfg)` mutating in place, atomic save. Returns the saved config or `None` when the service has no config. Routers use `mutate_service_config(service_id, fn)`, mapping `None` to the standard 404 `{"error": "Service not found"}`; `load_service_config` is the read counterpart.
- Multi-step changes that must roll back use `snapshot_config(service_id)` (raw persisted text or `None`) and restore it under the same lock. Never `config_path.write_text(...)` a config.
- **cid-salt rules:** `cfg["rum"]["cid_salt"]` is write-once. Every save re-adopts the persisted salt (a stale writer can't re-key pseudonymous cids); an unreadable existing config aborts the save (`CidSaltReadError`) instead of minting a new salt. `allow_cid_salt_change=True` is reserved for an explicit rotation helper (none exists in this tree). A callback that replaces the whole dict must `clear()` + `update()`.

### SQL helpers
- `escape_sql_literal(value)` ([backend/core/sql_validator.py](backend/core/sql_validator.py)) is the only way to put a string inside a single-quoted SQL literal (S3 object keys are attacker-controlled). Don't write `.replace("'", "''")`.
- `is_safe_ident` / `require_safe_ident(name, label)` (same module) validate bare identifiers; the table-name slugger stays `_safe_table()` ([Trap #4](#4-_safe_table-is-mandatory)).
- [backend/core/metric_expressions.py](backend/core/metric_expressions.py) owns null-default and metric fragments with golden-string tests: `origin_latency_us(cols, absent=...)`, `sum_or_zero(expr, zero=, where=)`, `column_or_default(cols, col, default, cast=)`, `coalesce(expr, default)`, `HIT_STATES`. New inline `COALESCE(` in repositories fails `make coalesce-guard`.

### Retry ([backend/core/retry.py](backend/core/retry.py))
`retry_call(fn, retry_on=pred, attempts=N, base_delay=, max_delay=, multiplier=, jitter=, max_elapsed=, before_retry=, sleep=)` is the one bounded-backoff primitive (tenacity underneath). The predicate decides retryable vs fatal; `before_retry` is the recovery seam; `sleep=` is the test seam. A `for`/`while` + `try/except` + `sleep` loop is a hand-rolled retry and fails `make retry-guard` unless baselined. Error classification and recovery stay at each call site — `retry_call` is mechanics only. Intentionally bespoke loops (timing `retry_call` can't reproduce): the VirtioFS config read loop in `config.py`, the FOS bucket-delete step schedule, orchestrator storage init and scorer readiness probes that yield status events between attempts, and polling/SSE loops. `core/fastly/client.py` uses tenacity directly (method-blind).

### Caches ([backend/utils/bounded_cache.py](backend/utils/bounded_cache.py))
`BoundedTTLCache(maxsize=, ttl_seconds=)` is the thread-safe LRU+TTL dict for time-keyed caches. Register every module-level cache with `CacheRegistry.register("pkg.name", cache)` so the test harness drains it; clear in place, never rebind the module global (other modules import it by name).

### Dates ([backend/utils/date_utils.py](backend/utils/date_utils.py))
`parse_iso_utc`, `parse_window`, `parse_hour_key[_strict]` (`YYYY-MM-DD-HH`), `parse_day_key[_strict]`, `iso_z_now`, `ensure_utc` (naive = UTC, aware → `astimezone`; never `.replace(tzinfo=UTC)` on an aware value), `iso_z`. Raw `strptime` / `fromisoformat` / `.replace(tzinfo=UTC)` in `backend/core` and `backend/repositories` is ratcheted by `make dateparse-guard` (per-file baseline in `scripts/check_no_raw_dateparse.sh`).

### HTTP layer ([backend/utils/router_utils.py](backend/utils/router_utils.py), [backend/utils/error_utils.py](backend/utils/error_utils.py), [backend/models/common/base.py](backend/models/common/base.py))
- **Errors:** `make_error(code, message=None, **extras)` builds the `{"error": code, ...}` detail for `HTTPException`. At an `except` site whose text could leak internals (server paths, upstream Fastly bodies, cid salt) call `raise_internal(logger, exc, code=..., request=...)`: it logs the full exception with an `error_id`/`request_id` and returns only those ids. `redact_error_text` / `scrub_secrets` (strip absolute server paths and registered cid salts) stay on every analyst-visible error path. `@query_errors` decorates query-backed handlers. Never `detail={"error": str(e)}`.
- `require_service_id(service_id)` returns **400 `service_id_required`** via `make_error` for a missing service id (never FastAPI's 422 body); the frontend and e2e key on that code.
- **SSE:** build every SSE route with `sse_response(stream)` ([backend/utils/sse.py](backend/utils/sse.py): shared 5 s ping + passthrough headers). A test fails on a direct `EventSourceResponse(` under `backend/routers`.
- **Log throttling:** `log_once_per(key, interval_s=60.0)` (`backend/utils/telemetry/logging_config.py`) returns True at most once per interval per key; wrap any log line that can fire per request/row while a failure persists.
- **Typed responses:** every route has a `response_model` defined in `backend/models/` (never inline in routers; a test keeps the untyped allowlist empty). Use `WireSafeModel` (`extra="allow"`, omits never-set fields, wire byte-identical to the producer's dict) or `PassthroughModel` (`extra="allow"` only) as bases, and `JsonRows` / `JsonDict` / `JsonAny` for DB-native values (Decimal, tz-aware datetimes, bytes) so they keep the `jsonable_encoder` encoding (`Z`, not `+00:00`). Alias generated types on the frontend via `Schema<'ModelName'>` ([frontend/types/api.ts](frontend/types/api.ts)).
- **Admin SSE & cron triggers:** reuse `_stream_from_worker(work)` and `run_admin_cron_trigger(service_id, task, job_fn, ...)` ([backend/routers/admin/_helpers.py](backend/routers/admin/_helpers.py)).

### ClickHouse & Worker DRY helpers
[backend/high_scale/_common.py](backend/high_scale/_common.py) centralizes ClickHouse filter/time-window SQL helpers; `BaseClickHouseBatchAdapter` ([backend/high_scale/archive/clickhouse_batch.py](backend/high_scale/archive/clickhouse_batch.py)) owns shared batch-adapter plumbing; `_MultiServiceWorkerLoop` ([backend/high_scale/worker.py](backend/high_scale/worker.py)) runs multi-service worker loops. Fastly POP coordinates come from `get_pop_locations()` / `get_pop_lat_lon_map()` in [backend/utils/pop_utils.py](backend/utils/pop_utils.py); never hardcode coordinate `CASE` blocks in SQL.

### Postgres metadata connections ([backend/core/metadata/pg_pool.py](backend/core/metadata/pg_pool.py))
Each thread holds one long-lived pooled connection (`get_pg_thread_connection`); `PgConnectionWrapper.execute` holds the wrapper's `_busy` lock while a statement is in flight. See [Trap #45](#45-never-return-a-busy-postgres-connection-to-the-pool-from-another-thread). Threads that end for good call `release_pg_thread_connection()` themselves.

### Frontend helpers
- `resolveDefaultSsrRangeKey(...)` in [frontend/lib/ssr/upstream.ts](frontend/lib/ssr/upstream.ts) resolves default SSR time-range keys from `LogExtentsResponse`; `getPageLoadId()` in [frontend/lib/api.ts](frontend/lib/api.ts) is the page-load correlation id source.
- Analytics route pages follow `page.tsx` + `_sections/<Route>Client.tsx` and share state through `useReportPageState` ([frontend/hooks/useReportPageState.ts](frontend/hooks/useReportPageState.ts)).

## Traps & Gotchas

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
All SQL table/view names derived from user-controlled values (service IDs, field names) go through `_safe_table_name()` / `_safe_table()` ([backend/core/fos_client.py](backend/core/fos_client.py) and `backend/repositories/utils/response_shaping/`). Never interpolate into SQL. Other bare identifiers use `require_safe_ident`; string literals use `escape_sql_literal`; filter values use parameters.

### 6. Configs are keyed by LOGGING service ID, not CDN service ID
`configs/{logging_service_id}.json`. When looking up a stored API key, use the logging service ID; `cdn_service_id` is a different Fastly service.

### 7. APScheduler "config not found, skipping" during teardown is normal
A job fired after its config was deleted; the next `reload()` evicts the stale job.

### 8. Ruff format must pass
`make ci` fails `format-check` on unformatted files.

### 9. OpenAPI types must be regenerated after backend schema changes
`make gen-types` (or `cd frontend && npm run gen:types`). `make ci` does it automatically; running `tsc` by hand does not.

### 11. All configs are schema v2
`lf = cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []}` — always.

### 12. NGWAF bot cache lives in PostgreSQL
`ngwaf_bots` is stored in Postgres (`pg_schema.py`) and populated by `_run_ngwaf_bot_sync` ([backend/cron/jobs/metadata.py](backend/cron/jobs/metadata.py)), providing verified-bot enrichment for ClickHouse security readers.

### 13. Next.js startup crash on restricted macOS
`ERR_SYSTEM_ERROR: uv_interface_addresses returned Unknown system error 1` on some hardened systems. The `dev` script in `frontend/package.json` binds `-H 127.0.0.1` to bypass interface enumeration. Don't drop the flag.

### 14. VCL regex right-hand side must be a string literal
The RHS of `~` / `!~` must be a literal — no variables or concatenation. Use `regsub()` / `regsuball()` for dynamic logic.

### 15. Operational metadata lives in PostgreSQL 16
Alerts, views, audit, cron history, ingested-file dedup, ASN names, source registration, slow-query history, usage telemetry, live share, the NGWAF bot cache, and the ingest control plane live in Postgres via `METADATA_DSN` ([ADR-22](docs/adr/22-postgres-only-metadata.md)). Read/write through [backend/core/metadata/](backend/core/metadata/) (`from backend.core import metadata as metadata_db` resolves through the package's `_ShimModule`) and [backend/high_scale/postgres_control/](backend/high_scale/postgres_control/). MVCC means background cron writers never block readers or cause file-lock deadlocks.

### 17. MSW + openapi-fetch ordering — `server.listen()` must run at module load
`openapi-fetch` captures `globalThis.fetch` at `createClient` time and `frontend/lib/api.ts` creates its client at module load, so MSW's `server.listen()` must execute at the top of `vitest.setup.ts`, **not inside `beforeAll`**. Otherwise the captured fetch is the unpatched original and tests silently bypass MSW (symptom: handlers never fire, requests hit real loopback).

### 18. `@testing-library/react` auto-cleanup is OFF when `globals: false`
`vitest.config.ts` sets `globals: false`, so RTL registers no `afterEach(cleanup)`; `vitest.setup.ts` has an explicit one. Without it, earlier components stay mounted and pollute `screen.getBy*` (input values accumulate).

### 19. `extractApiError` Error-instance trap
`Error.message` is non-enumerable, so `JSON.stringify(new Error('boom'))` is `"{}"`. `extractApiError` in `frontend/lib/api.ts` early-returns `error.message` for `instanceof Error`; keep that check first.

### 20. `RemoteAccessMiddleware` blocks admin routes over the live-share tunnel
The tunnel exposes the same FastAPI app to the internet. The middleware classifies by `Host` and blocks remote requests to admin paths, including `/api/admin/share/*`. Register analyst-reachable endpoints under `/api/share/*` or update `_is_blocked_path()`. Don't remove the `testclient`/`testserver` allow-list entries; they let pytest hit admin routes.

### 23. SSR upstream fetch must use `node:http`, not `fetch()`
See rule 1 of [frontend/docs/routing.md](frontend/docs/routing.md#ssr-bootstrap-pre-fetch--hydration-invariants). `fetch()` rewrites `Host`, the backend classifies the request as admin-from-loopback, and the full admin bootstrap dehydrates into anonymous public HTML. Copy the `rawRequest` pattern in `apiSsrProxy.ts`.

### 27. System-managed custom fields must be re-asserted on every `log_fields` write
Session scoring and CMCD each inject a canonical set of `log_fields.custom_fields` from code; `_is_system_field` ([backend/core/catalog.py](backend/core/catalog.py)) hides them from the user-editable list, so any writer persisting `log_fields` from a list it didn't author omits them: the UI round-trip, an admin-state pull, or a provisioning reconcile built from groups alone. The omission isn't cosmetic — `reconcile_vcl_state` regenerates the Fastly log format from the persisted config, so a dropped field leaves its extraction VCL running while nothing writes its output; the column then ingests empty string (not NULL), the feature's `available` probe still passes because the schema column exists, and the page renders zeros with no error. Route every such write through `reconcile_system_custom_fields` / `reconcile_cfg_system_custom_fields` ([backend/provision/cmcd_fields.py](backend/provision/cmcd_fields.py)) and key it on the **current** feature state (`system_feature_flags`), never on a state transition: a reconcile that says nothing about the feature must still re-assert its fields, and a disable must strip them. **A third system-managed feature adds its flag to `system_feature_flags`; do not add another re-injection block.**

### 29. Feature extraction VCL must be emitted BEFORE field capture
Generated `vcl_recv` has two related blocks: **extraction** parses a source into `req.http.x-<feature>:*`, and **capture** promotes it into `req.http.x-fos-edge-data:<field>`, which the log format reads. If capture is emitted first it copies empty strings and extraction runs too late. The failure is silent everywhere you'd look: the VCL is valid and `validate` passes; the schema columns exist so the feature's `available` probe is true and the page shows zeros instead of "not enabled"; and the extraction itself genuinely works, so an edge cache-key probe proves CMCD is handled while saying nothing about whether the value reached the log line. Both generators (`generate_capture_vcl` in `fastly_api.py` and the consolidated snippet in `declarative/vcl_builder.py`) must order extraction before capture; a feature that extracts into a `req.http.x-*` header for logging needs ordering coverage in the VCL tests.

### 30. The log-line budget silently truncates late custom fields
`generate_log_format` tracks an aggregate `budget` (`FASTLY_LOG_LINE_DELIVER_MAX`) and clamps each variable-length field to what's left (`cf_limit = max(0, min(cf_limit, budget))`), allocated greedily in `custom_fields` order. Once the budget is dry, later fields get `substr(x, 0, 0)` and log null with no warning (e.g. a 36-char UUID squeezed to 26 chars, breaking session joins). Values logged before the squeeze are full length, so one column can hold mixed lengths, which reads like a client quirk. `byte_limit` only lowers a cap, can't create budget, and shifting allocation can zero other fields. Inspect the real generated format (`generate_log_format(cfg)`; grep for `substr(..., 0, 0)` and per-field caps). The real levers are fewer enabled groups/custom fields or a higher line budget, which trade against Fastly silently dropping over-long lines.

### 31. Teardown: dereference before delete, and ENUMERATE what you remove
Teardown touches a service the customer still serves traffic from, so ordering is a correctness property: **every step that strips VCL from the customer's service must complete before any step that deletes a service that VCL points at**, or their active version routes to a deleted host. Order in `perform_teardown`: scoring VCL strip + Compute delete → logging endpoint and all owned VCL off the customer service → FOS keys → FOS bucket → analytics-owned CDN service last.
- **Never delete by guessed name.** Blind DELETEs of hardcoded snippet names that swallow 404s report success while leaving the capture VCL live. GET the list and delete what you can attribute, narrowly: the `Fastly Log Analytics` prefix plus the canonical `scoring_snippet_names()` / `cmcd_snippet_names()` / RUM sets. Do **not** prefix-match `Session ` — a customer's `Session Tracking - *` snippets sit beside our `Session Scoring - *` ones.
- **No best-effort dereference followed by unconditional delete.** If the VCL strip fails, abort and leave the Compute service in place: an orphaned Compute service is cheap and re-deletable, a dangling backend on live traffic is an incident (Fastly 500s on these calls are real).
- **Never label a destructive log line with a name you didn't resolve from the thing being deleted;** log the id alongside every delete.
Cover ordering changes with the provisioning/teardown tests in `tests/provision/`.

### 37. Map prewarming must configure the worker before constructing any map
MapLibre shares its worker pool across map instances, and `MapPrewarm` can run before any visible map module, so it must call `setWorkerUrl('/maplibre-gl-worker.mjs')` immediately after its lazy import and before constructing a map. Otherwise it creates a worker with an empty URL, the browser fetches the page HTML as a module, fails MIME checking, and every later map inherits the broken pool. Configuring the visible map later does not repair workers already created. An attached canvas is not proof a map works (the 1 px prewarm canvas exists even with no geometry loaded); map E2E tests must supply country data and require rendered `countries` features on the full-size map.

### 38. NGWAF dashboard cards require a configured workspace
`bootstrap.ngwaf_configured` is the active service's attachment flag, available to admin and analyst without exposing the workspace id. `useDashboardCards` excludes NGWAF Verified Bots and NGWAF Signals from the available card list when it is false. Virtual fields must not bypass this gate: persisted card selections, "Show all" or leftover WAF log fields do not establish NGWAF is configured. The header counts only available selected cards; saved preferences stay intact when switching to a service where NGWAF is configured.

### 39. Header freshness is request-event time, not a log-file timestamp
The REQUEST header uses `request.latest_log_at`, the request table's event-time extent. A raw object's filename timestamp belongs in `latest_ingested_file_at` / `latest_available_file_at` and must never overwrite that extent in a cached status overlay, or REST and SSE report an older request than `log-extents` and the header falsely looks stalled. Bootstrap cache hits and misses must preserve the same meaning. Use the current snapshot's request metrics, falling back to the flat extent only when the request group is absent; keep RUM separate. Explicit nulls clear metrics; omitted fields in a partial SSE update retain prior values. Never take the max of old and new timestamps: a reset or replacement can legitimately move the extent backward. `request_metrics_observer` (started in `backend/core/lifespan.py`, implemented in `request_telemetry/tracker.py`) owns a single backend reconciliation loop — observe the durable serving view, persist, publish changed status via SSE — waiting 15 s after each completed pass. It is serving work, stays enabled with development ingest crons paused, and never starts an ingest writer.

### 40. METRICS-group catalog fields have no real column
`LOG_FIELD_CATALOG` entries in the `METRICS` group (`requests`, `hit_rate`, `5xx`, `4xx`, `p50_latency`, `p95_latency`, `p99_latency`, `throughput`, `req_size`, `ttfb_ms`) have `vcl=None`: chart-metric-only synthetic concepts, never an ingested column (`LogField.is_derived`). Derive the exclusion from the registry (`{f.code for f in in_group(Group.METRICS)}`), never a hand-maintained list, whenever iterating catalog fields to build per-field SQL projections.

### 41. Incremental ingest must attempt one batch after an expensive LIST
The `log_discovery` budget covers both FOS discovery and batch claiming/processing. On a high-object-count bucket the four-hour incremental lookback can consume the whole 240 s budget before the first batch; checking `max_seconds` before the first batch then stalls permanently (every tick lists the same backlog, processes zero files, reports "No new log files found"). Always allow the first bounded batch to run, then enforce the limit before subsequent batches.

### 43. Request lag comes from serial work on the ingest tick, not the poll interval
The freshness SLA is 10–20 s. Serial per-object processing (FOS GET/PUT, ~15 Postgres control transitions, ~7 ClickHouse publish+verify pairs) starves ingest. The page-batched pipeline instead (1) ingests pages of objects (`HIGH_SCALE_MAX_BATCH_OBJECTS`, default 10 on lightweight workers, up to 50–100), decoding concurrently in Arrow memory and writing one merged archive Parquet per page to amortize the FOS PUT; (2) registers one `ArchiveManifest` (`batch_{manifest_id}`) per page with member source keys, SHA-256 digests and per-source replay offsets, so replay and retention stay granular per source; (3) collapses control traffic into batch transactions (`claim_sources_batch`, a batch-manifest put, publish marking, `acknowledge_sources_batch`) with bulk ClickHouse inserts verified in one pending→visible gate; (4) partitions sources by key hash under a single owner epoch, so replicas claim exclusive ranges without contending. Page objects run on a bounded thread pool (`HIGH_SCALE_OBJECT_CONCURRENCY`, default 4) with pooled Postgres connections; size worker counts against Postgres `max_connections`.

### 44. RUM and request ingest must share the parse, quarantine, and deletion contract
RUM ingest (`rum_discovery` / `ingest_controller`) must follow the same contract as request ingest: (1) discovery uses bounded incremental minute prefixes (`rum_minute_list_prefix`) with `StartAfter` bounds, never a full `raw/rum/` scan per tick (which burns CPU and starves request discovery); (2) raw objects are deleted after archive verification under `resolve_raw_delete_after`, excluding unreadable objects and objects whose quarantine capture failed; (3) one shared parser (`_parse_rum_beacon_file` / `_parse_rum_line`) and quarantine flow, so rows missing timestamps are quarantined with exact bytes via `_quarantine_rum_corrupt_lines` ([backend/core/rum_ingest.py](backend/core/rum_ingest.py)), never silently dropped; (4) the `max_seconds` budget with the first batch always allowed ([Trap #41](#41-incremental-ingest-must-attempt-one-batch-after-an-expensive-list)).

### 45. Never return a busy Postgres connection to the pool from another thread
`close_all_pg_connections()` drains every tracked thread connection (teardown, shutdown). Handing a libpq connection back to the pool while its owner has a statement in flight frees it under the running query (segfault or cut-off). Each `PgConnectionWrapper` carries a `_busy` `RLock` held for the duration of `execute`; the drain takes it with `acquire(blocking=False)` and **skips** (leaves tracked) any connection it can't take. Never `putconn` / `_return_to_pool_once` a connection you don't own without taking `_busy` non-blocking first; never block on `_busy` from the drain path (a hung query would hang shutdown); an ending thread calls `release_pg_thread_connection()` for its own connection. The `_checked_out` WeakSet must not become a strong list (it would pin dead threads' connections until the pool times out).

### 46. Gate non-SSR-seeded store/query state on `useMounted()` in route chunks (React #418)
In the App Router, the root layout (`StoreHydrator`, `useBootstrap`, admin SSE) mounts and runs effects before late-loaded route chunks hydrate. In that gap `useServiceStore.activeServiceId` (null during SSR without a `?service=` param) is set and fast local queries/SSE can populate caches before the route's first client render. A route component that reads `activeServiceId` or non-SSR-seeded query data directly in its first render renders populated DOM where SSR rendered `null`/a skeleton, producing `Minified React error #418`. Gate those reads on `const mounted = useMounted()` so the first client pass matches SSR byte for byte. Clock-derived chart labels (`Date.now()`) need the same gate, since a second can pass between server render and hydration — including in screen-reader tables even when the chart itself is client-only.

### 48. A shared `raw/rum/` object belongs to one domain but carries both
`rum_vitals` and `rum_errors` discovery both list `raw/rum/`, and the source ledger is `UNIQUE(service_id, object_key)`, so the first domain to see an object claims it. Decoding only the claimed domain silently drops the sibling's rows, and the raw object is then deleted. `ingest_controller` therefore decodes the sibling domain from the same payload, keeps only rows passing `row_matches_serving_domain`, and publishes them under `build_batch_id_from_manifest_id(f"{manifest_id}:{sibling}")` before the source is acknowledged; sibling rows are not counted, archived or dead-lettered as the claimed domain's rows. Don't "fix" this with separate ledger rows per domain or cursor resets. Regression: `test_rum_errors_claim_also_publishes_sibling_vitals_from_the_shared_object` (`tests/high_scale/test_ingest_controller.py`).
- **An error-owned archive is not an error-only archive.** Inspect exact `sources_json` slices and deduplicate expanded copies of each `raw_json` by source version and original line ordinal before diagnosing loss; empty `error_message` rows can be genuine vitals. Faro's exception discriminator is authoritative, not the envelope's metric fields or query string; `_rum_kind` prevents an exception from becoming a false vital. Message-less exceptions keep `Unknown error`; flat errors need an actual discriminator (stack, file or exception kind). Invalid metrics and flat records with neither kind are dead-lettered with their original bytes; sibling rejections are counted separately. Custom Faro events project as `event_<name>=1.0`, and navigation attributes/measurements can expand to multiple metrics, so input-event counts don't prove valid-vital counts.
- **Historical repair:** a recovery tool must default to a read-only dry run for exactly one unambiguous terminal manifest, reconstruct both domains from retained original lines, check physical facts across all batch ids and visible publication receipts (including semantic equivalents with changed event ids), and block apply on hidden, duplicate, conflicting, expired or unprovable identities. Require an explicit plan-digest approval before apply; serialize repairs and hold the terminal-source/current-owner fence throughout. Generation is the manifest's **owner epoch**, not its archive epoch. Stable missing-set batch ids plus pre/post identity checks make retries safe beyond ClickHouse's block-dedup window. Never substitute a generic rebuild, blind replay, cursor reset or source-ledger edit; see the capacity runbook.

### 51. Field-picker reservoir samples have no request scope
`top_values.json` is an unbounded reservoir sample. `get_field_values` may use it only with no search, time bounds or active filters; scoped suggestions query the scoped data, keeping the own-field filter exclusion.

### 53. Dashboard component cache keys must preserve exact window bounds
Dashboard aggregate and top-bot cache keys retain seconds and fractions in both bounds; minute-truncating explicit bounds lets different event windows reuse each other's results even when the SQL is correctly scoped. Relative-window quantization belongs to request-window resolution, not the component cache key. Identical-window hits, TTL and ingest invalidation are unchanged.

### 54. RUM fetch flushes must survive document dismissal
The generated tracker unrolls queued Faro events into empty-body POSTs, and its fetch path needs `keepalive: true` (see [backend/provision/rum_assets.py](backend/provision/rum_assets.py)); otherwise navigation can cancel the old document's requests before they reach the server, and can leave Chromium Fetch interception ids invalid, failing a guarded browser capture without an origin rejection. Keep disposable-analyst origin guards fail-closed — don't ignore cancelled interception commands. Both fetch- and XHR-triggered flushes must retain keepalive; the native `sendBeacon` path is separate.

### 56. Single-host deploys: host bind-mounts must be owned `1000:1000`
Production runs `docker-compose.yml` plus `docker-compose.prod.yml` ([docs/deploy/](docs/deploy/)); omitting the prod overlay silently drops `network_mode: host`, loopback-only binds and the anti-XFF-spoofing hardening (`TRUSTED_PROXY_IPS`, `REQUIRE_PROXY_HEADERS`) while the stack still comes up. The backend image runs as `app` (UID/GID 1000) and the Dockerfile chowns the in-image paths, but nothing enforces ownership of the **host-side** bind-mount sources (`data/`, `configs/`). If those drift to another UID:GID the container starts and then crash-loops on its first write with `PermissionError`, possibly long after the change. Before restarting containers verify `stat -c '%u:%g' data configs` (Linux) or `stat -f '%u:%g' data configs` (macOS) reads `1000:1000`.

<!-- trail:start -->
## Trail — repo context graph

This repo is indexed in `.trail/cache/`: small linked markdown nodes that explain each
system and carry exact file:line spans, kept in sync with the code through git.

For ANY task here — understanding how something works, finding where code lives,
or scoping a change — get context from the graph before grepping or opening
source files. Re-ask freely (it's cheap) and reuse literal identifiers you
already have (symbol, error string, file name) as the query. New to this repo?
Run `trail map` first — a token-budgeted orientation (dir clusters, hubs,
hotspots), no LLM, no key.

- Run `trail ask "<your question>" --source` → ranked nodes with the relevant
  code spans inlined (each hit's ≤8-line crux by default; `--full` for whole
  definitions when the crux isn't enough). Match the tool to the task shape:
  for understanding or editing, the top node IS the answer — cite its
  `covers:` file:line spans and edit straight from `--source`. For
  exhaustive tasks ("every occurrence / every caller of this pattern"), ranked
  results are top-N, not complete — run `trail grep "<literal>"` instead
  (exhaustive over indexed files, grouped by enclosing symbol), falling back
  to raw `grep -rn` only for unindexed files.
- `trail skeleton <file>` → every definition's signature + span, ~10× cheaper
  than reading the file; use it to skim an API surface.
- `trail callers <symbol>` gives precomputed, exact edges — who calls this.
  Add `--direction out` for what it calls, or `--depth N` to walk
  transitively for the full blast radius. For structural questions, skip
  ranking and use this directly.
- Or browse: `.trail/cache/INDEX.md` lists every node; follow the links.
- Monorepos and folders of multiple repos rank fairly across sub-projects —
  hits carry `[scope/]` labels naming which one they're from. Narrow with
  `trail ask "<task>" --in <scope>/` once you know where you're working.

If a returned span is truncated ("+N more lines"), open the file at that exact
range before finalizing. Only open source files when a node genuinely lacks a
needed detail, and then at the exact file:line the node points to — never
re-read whole files.

After big code changes, refresh the graph with `trail build` (deterministic,
no API key, $0).
<!-- trail:end -->
