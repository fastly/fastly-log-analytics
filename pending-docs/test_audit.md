# Test Audit — Phase 0 Triage

182 test files surveyed against the per-phase carve-up plan. Each file is classified as:

- **keep** — load-bearing, will only need cosmetic updates (import paths, fixture shape)
- **rewrite** — covers something real but tests the old shape (must be refactored when its target is)
- **delete** — asserts against removed/renamed surfaces, redundant, or asserts an implementation detail that the refactor eliminates
- **flaky** — needs fix before it becomes useful (none identified at audit time, will surface during phase execution)

**Default is `keep`** — pre-existing test coverage is valuable. Aggressive `delete` only for tests that assert workarounds the refactor structurally eliminates (e.g., `_read_only` private-attribute tests once `RequestContext` makes that trick impossible).

This map is **advisory** — each phase re-confirms its slice during execution and updates the map. Phase 10.5 / 10.12 do a final sweep and any test still tagged `delete`/`flaky` that survived must be resolved.

---

## By phase ownership

### Phase 1 — Telemetry / OTel / structlog / aiodns

| Test file | Plan disposition | Notes |
|---|---|---|
| `tests/utils/test_telemetry.py` | **rewrite** | replace contextvar-mirror assertions with OTel span-exporter assertions |
| `tests/utils/test_telemetry_proxy.py` | **keep** | proxy logic unchanged; OTel only wraps it |
| `tests/utils/test_telemetry_proxy_phase2.py` | **keep** | same — proxy internals |
| `tests/utils/test_telemetry_proxy_phase3a.py` | **keep** | same |
| `tests/utils/test_telemetry_proxy_phase3b.py` | **keep** | same |
| `tests/utils/test_telemetry_proxy_phase4.py` | **keep** | same |
| `tests/utils/test_telemetry_response_middleware.py` | **rewrite** | renderer now reads spans, not contextvars; assert on rendered JSON shape, not internals |
| `tests/utils/test_rdns_cache.py` | **rewrite** | new aiodns + asyncio.gather + aiosqlite path needs concurrency tests, mock DNS resolver, mocked SQLite |

### Phase 2 — RequestContext + tenancy

| Test file | Plan disposition | Notes |
|---|---|---|
| `tests/test_deps.py` | **rewrite** | replace `AnalyticsDeps`-shape assertions with `RequestContext`; **delete** `_read_only` private-attribute regression test (structurally impossible after Phase 2) |
| `tests/routers/test_cross_tenant_scope.py` | **keep** | tag all assertions with `@pytest.mark.security_regression` |
| `tests/routers/test_pages.py` | **keep** | router-level smoke |
| `tests/routers/test_endpoints.py` | **keep** | same |
| `tests/routers/test_route_methods.py` | **keep** | structural |
| `tests/routers/test_invite_analyst.py` | **keep** | analyst-session path |

### Phase 3 — Middleware invariants + pydantic-settings + tenacity

| Test file | Plan disposition | Notes |
|---|---|---|
| `tests/test_main.py` | **keep + add** | add middleware-order snapshot test (Phase 3.3) |
| `tests/test_proxy_headers_regression.py` | **keep** | load-bearing trust topology test; predates plan, stays |
| `tests/test_config.py` | **rewrite** | swap to `pydantic-settings` `Settings` validator tests |
| **NEW** `tests/test_trust_topology.py` | **add** | Caddyfile + docker-compose.prod.yml snapshot tests (Phase 3.4) |
| `tests/utils/test_fastly_api.py` | **rewrite** | tenacity retry behaviors replace custom try/except loops; rewrite mock-network-failure cases |
| `tests/utils/test_ngwaf.py` | **rewrite** | same — tenacity replaces ad-hoc retry |
| `tests/utils/test_fastly_api_orchestrators.py` | **keep** | orchestration logic unchanged |
| `tests/utils/test_fastly_client.py` (core/) | **keep** | client-level, untouched by tenacity wrapper |

### Phase 4 — Iceberg carve-up + monkeypatch elimination

| Test file | Plan disposition | Notes |
|---|---|---|
| `tests/test_e2e_pyiceberg_s3.py` | **rewrite** | imports become `from backend.core.iceberg.view import ...` etc.; assertions stay |
| `tests/core/test_iceberg.py` | **rewrite** | same — split into per-module test files: `test_iceberg_view.py`, `test_iceberg_catalog.py`, `test_iceberg_warehouse.py`, `test_iceberg_manifest.py`, `test_iceberg_fs.py` |
| `tests/core/test_iceberg_helpers.py` | **rewrite** | helpers move to new modules |
| `tests/core/test_endpoint_routing.py` | **keep** | uses `pyiceberg.catalog.sql.SqlCatalog` stub; verify `FosSqlCatalog` subclass still composes with stub |
| `tests/core/test_duckdb_pool.py` | **keep + add** | add F3-wedge stress test (Phase 4.2); 20-VU read burst during catalog churn |
| `tests/core/test_duckdb_concurrency.py` | **keep** | concurrency model unchanged |
| `tests/core/test_local_compaction.py` | **keep** | `test_compaction_outputs_survive_iceberg_sync_orphan_cleanup` is the load-bearing Trap-#21 invariant |
| **NEW** `tests/core/test_query_planner.py` | **add** | new query-rewriter in Phase 4.4 needs unit coverage |
| **NEW** `tests/core/test_storage_reader.py` | **add** | new StorageReader protocol implementations |
| **NEW** `tests/perf/test_wedge_stress.py` | **add** | F3-wedge dedicated stress harness |

### Phase 5a — SQL extraction

| Test file | Plan disposition | Notes |
|---|---|---|
| Every `tests/repositories/test_*.py` | **keep** | function signatures preserved; templates rendered by helper; existing tests continue to pass |
| **NEW** `tests/repositories/_sql/test_*.py` | **add** | per-template render test (one per file under `backend/repositories/_sql/`) |

### Phase 5b — Repository splits + metadata carve-up + Terraform JSON + cachetools

| Test file | Plan disposition | Notes |
|---|---|---|
| `tests/repositories/test_dashboard.py` | **rewrite** | split mirrors source: `test_dashboard_aggregates.py`, `_raw.py`, `_field_values.py`, `_csv.py` |
| `tests/repositories/test_origin.py` | **rewrite** | split by section, matching source carve |
| `tests/repositories/test_base.py` | **rewrite** | `_compact_sql_for_debug` moves to a render module; test moves with it |
| `tests/repositories/test_base_helpers.py` | **rewrite** | helpers may relocate |
| `tests/repositories/test_base_metrics.py` | **keep** | metrics surface stays |
| `tests/repositories/test_insights.py` | **keep or rewrite** | depends on Phase 5b.5 data-driven decision (open Q4) |
| `tests/repositories/test_insights_registry.py` | **keep** | registry concept survives |
| `tests/repositories/test_insights_processors.py` | **keep** | processors stay |
| `tests/repositories/test_filters_properties.py` | **keep** | property-based test of filter rendering |
| `tests/repositories/test_aggregates_timeseries_properties.py` | **keep** | property-based |
| `tests/repositories/test_all_repos_properties.py` | **keep** | smoke property-based across repos |
| `tests/core/test_metadata_db_*.py` (6 files) | **rewrite** | every test now imports from `backend.core.metadata.<concern>`; structure mirrors mixin split |
| `tests/utils/test_terraform_gen.py` | **rewrite** | assert `.tf.json` dict shape, not HCL string output |
| `tests/utils/test_terraform_resource_graph.py` | **keep** | resource graph logic unchanged |
| `tests/utils/test_bounded_cache.py` | **rewrite** | now backed by `cachetools.LRUCache`; behavior-preserving rewrite |
| `tests/utils/test_ngwaf_bot_cache.py` | **rewrite** | now backed by `cachetools.TTLCache` |

### Phase 6 — Scheduler carve-up + cron isolation

| Test file | Plan disposition | Notes |
|---|---|---|
| `tests/test_scheduler.py` | **rewrite** | imports now `backend.cron.scheduler` etc.; **delete** any assertion of the deferred-invalidation hack (commit 8364335, gone after Phase 6.3) |
| `tests/test_scheduler_watchdog.py` | **rewrite** | watchdog logic moves to `backend/cron/scheduler.py`; rewrite imports |
| `tests/test_scheduler_apscheduler_stress.py` | **keep** | APScheduler stress unchanged unless v4 adopted (Phase 6.4 spike) |
| `tests/core/test_scheduler_timing.py` | **keep** | timing logic unchanged |
| `tests/test_cron_progress.py` | **rewrite** | if 6.2 migrates cron progress to SQLite, schema test needs the migration; otherwise minor |
| **NEW** `tests/cron/test_jobs_*.py` | **add** | one per job module under `backend/cron/jobs/` (sync, commit, compaction, optimize, expire, metadata) |
| **NEW** `tests/cron/test_isolation.py` | **add** | concurrency test: 20-VU read burst during active cron sync; reads must not 503 (Phase 6.6) |

### Phase 7 — Field registry + Fastly SDK spike + scoring parity

| Test file | Plan disposition | Notes |
|---|---|---|
| `tests/core/test_log_fields.py` | **rewrite** | swap constants-based assertions for registry queries; add property-based test (hypothesis) that every field round-trips through validator |
| `tests/utils/test_sql_validator.py` | **rewrite** | derived from registry; rewrite to assert registry-driven validation |
| `tests/scoring/*` (10 files) | **keep** | fixture-byte-pinned parity with `compute/`; phase 7.3 re-runs full suite |
| `tests/core/test_vcl_semantics.py` | **keep** | falco-tagged; phase 7.4 gates on it |
| `tests/utils/test_fastly_api.py` | **(see Phase 3)** | if Fastly SDK adopted, additional rewrite; otherwise unchanged |

### Phase 8 — Composite-first API + hard cutover

| Test file | Plan disposition | Notes |
|---|---|---|
| Any test importing the dropped granular endpoints | **delete** | per Phase 8.7 dead-code grep |
| `tests/test_response_contract.py` | **keep + add** | extend with composite payloads |
| `tests/test_openapi_snapshot.py` | **keep** | regenerates snapshot post-Phase 8 |
| Frontend hook tests (Vitest) | **rewrite** | now hit composite endpoints; **tag tenancy-relevant ones** `@pytest.mark.security_regression` |

### Phase 9a — Frontend rendering boundary + nuqs

| Test file | Plan disposition | Notes |
|---|---|---|
| `frontend/__tests__/*PlotlyChart*.test.tsx` | **rewrite** | drop the `visible=false` warm-up workaround assertion; assert on new lazy-load utility |
| `frontend/__tests__/*LazyMount*.test.tsx` | **rewrite** | same |
| **NEW** Vitest tests for nuqs-synced components | **add** | URL query-string change triggers correct API requests + filter updates |
| **NEW** axe-a11y per route's first paint | **add** | one per route in the RSC/CSR table |

### Phase 9b — Frontend large-file splits

| Test file | Plan disposition | Notes |
|---|---|---|
| Existing component tests | **keep** | imports update as files move |
| **NEW** wizard state-machine tests | **add** | the extracted `ProvisionWizard/state.ts` gets unit coverage |
| **NEW** DataTable column-picker logic tests | **add** | extracted utility module |
| All form-submission integration tests | **rewrite** | standardize on `msw` (Mock Service Worker) per plan |

### Phase 10 — Tunnel carve-up + share_db carve-up + final sweep

| Test file | Plan disposition | Notes |
|---|---|---|
| `tests/remote_access/test_tunnel.py` | **rewrite + shrink** | SSH-to-localhost.run path deleted; ~half of tests go with it; remaining direct-mode tests reorganize under `tests/utils/tunnel/test_*.py` |
| `tests/remote_access/test_share_db.py` | **rewrite** | split alongside `share_db/{connection,schema,invites,sessions,audit,passcode,tos}.py` |
| `tests/remote_access/test_share_admin_routes.py` | **keep** | admin endpoint behavior unchanged |
| `tests/remote_access/test_share_auth_routes.py` | **keep** | auth flow unchanged |
| `tests/remote_access/test_middleware.py` | **keep** | middleware unchanged in Phase 10 |
| `tests/utils/test_state_sync.py` | **keep** | state-sync logic unchanged |
| `tests/utils/test_pop_utils.py` | **keep** | pop cache unchanged |
| `tests/utils/test_geo.py` | **keep** | geo logic unchanged |
| `tests/utils/test_cdn*.py` | **keep** | CDN miss tracking unchanged |
| `tests/utils/test_provision_utils.py` | **rewrite** | rich.console replaces custom printers; assert on output where printers are exercised |

### Phase-agnostic — keep as-is

These tests cover surfaces the cleanup doesn't touch:

- `tests/models/test_common.py`, `test_schema_sync.py`
- `tests/services/test_service_manager.py`
- `tests/utils/test_date_utils.py`, `test_bot_sources.py`, `test_router_utils.py`, `test_sqlite_profiler.py`, `test_system_jobs.py`, `test_lint_log_format_degraded_mode.py`, `test_vcl_utils.py`, `test_vcl_validator.py`, `test_usage_logger.py`, `test_fos_setup.py`, `test_fastly_utils.py`, `test_ngwaf_vcr.py`, `test_fastly_api_orchestrators.py`
- `tests/core/test_*` not listed above (custom field lifecycle, type roundtrip, ingest variants, lake info, common models, fastly client / fastly service, fastly edge writes backfill, metrics, reconcile, rollups, log line budget)
- `tests/routers/test_*` not listed above (catalog, debug, alerts_and_views, scoring exclude regex, session scoring router, query router, security insights, custom fields validation, custom field count limits / vcl lint, audit router, cron router, core get endpoints, etc.)
- All scoring tests (10 files) — fixture-pinned parity with `compute/`
- `tests/test_smoke_end_to_end.py`, `tests/test_e2e_pipeline.py`, `tests/test_performance_smoke.py`, `tests/test_no_trace_leakage_sweep.py`

---

## Summary counts

| Disposition | Approx. count | Notes |
|---|---|---|
| keep | ~120 | the bulk — pre-existing coverage is load-bearing |
| rewrite | ~40 | mostly per-phase import-path updates + assertion-shape changes |
| delete | ~5-10 | tests that asserted workarounds the refactor eliminates (`_read_only` trick, deferred-invalidation hack, granular endpoint hits) |
| flaky | 0 known | will surface during execution; log to `pending-docs/surprises.md` |
| add (NEW) | ~25 | per-phase new modules need new tests |

Total post-refactor: 207–212 test files (down from 182 only if `delete` outpaces `add`, which it doesn't — net up by ~20).

---

## Per-phase test-cleanup checklist

Each phase's verification step §1 re-runs against this map. The map is updated in the same PR if the phase surfaces a test we missed or one whose disposition changes.

Phase 10.5 / 10.12 final sweep: any test still tagged `delete`/`flaky` here must be resolved. Coverage delta on every touched module ≥ 80%.
