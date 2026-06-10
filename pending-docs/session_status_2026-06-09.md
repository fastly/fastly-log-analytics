# v2.0 Cleanup — Session Status (2026-06-09)

## Branch state

Branch: `refactor/cleanup` (pushed to origin)
Baseline tag: `refactor/cleanup-baseline` at `78f23d1` (post-merge main)
HEAD: `e326137`

```
e326137 Phase 7 migration: insights/repository.py + dashboard.py → field_registry
e7c9d42 Phase 5a tail + Phase 7 partial migration
076a3e7 Phase 10 finishing: rich+typer, CIDR refresh script, deploy runbooks
1b53dcf RBAC + UX audit fixes: H-1, H-2, H-3, H-4, M-1, L-1, L-2
5c2b013 Phase 4a + Phase 6 + Phase 9b: iceberg / scheduler / frontend carves
f6a7a50 Phase 5b + Phase 10a: metadata_db / share_db / tunnel package carves
83ca9ca Sessions/raw-logs redesign + Phase 7 FieldRegistry scaffold + lint sweep
6f07be4 Phase 5a + Phase 4 design: 7 repos SQL-extracted, iceberg carve doc
db1cfa4 Fix 6 failing tests: aiohttp 3.14 / vcrpy / HEAD body / dashboard cache
3c1a4e9 Session status doc — captures what shipped in this run
cfbcf47 Phase 5a: extract performance.ORIGIN_TIMESERIES SQL template
f6caf9d Phase 5a: SQL ownership audit + _sql/ scaffolding + usage extraction
5d7a1f4 Phase 2 + 3.5a: RequestContext dependency + centralised retry policies
1c9baf8 Phase 1+3 scaffolding: structlog/rdns_async tests, Settings, trust topology
b5ab51b Phase 1: rDNS aiodns refactor + RequestTelemetry scaffolding
dc11e02 v2.0 cleanup Phase 0: ADRs, audits, gates, baselines
78f23d1 ← baseline
```

**18 commits ahead of baseline. +42,018 / −23,121 across the branch.**

## Quality gates

| Gate | Result |
|---|---|
| `uv run pytest` | **3583 passed, 4 skipped, 0 failed** |
| `uv run ruff check backend/ tests/ scripts/` | clean |
| `cd frontend && npx tsc --noEmit` | clean |
| `bash scripts/check_security_regression_count.sh` | **98** ≥ 24 floor (up from 24 → 52 → 71 → 98) |
| Backend tests added this branch | +1300 (baseline 2268 → 3583) |
| Frontend tsc errors | 0 |

## Phases shipped (✅)

### Phase 0 — Baselines, ADRs, audits, gates (dc11e02)
5 ADRs, 7 pending-docs audits, `baseline_metrics.sh`, `check_security_regression_count.sh`, `perf_gate.sh`, `check_no_router_core_imports.sh`. Makefile targets (`perf`, `verify`, `ratchet`, `baseline`, `security-regression`). CI: security_regression + perf-gate steps. ARCHITECTURE.md `[v2.0-pending]` banners. 6 GCE comment scrubs.

### Phase 1 — Telemetry + rDNS (b5ab51b, 1c9baf8)
OpenTelemetry + structlog + aiodns + aiosqlite + tenacity. `RequestTelemetry` facade with `thread_wait_histogram()` instrument. structlog config + OTel trace_id/span_id processor. rdns_cache rewritten: concurrent aiodns + single-transaction aiosqlite bulk write.

### Phase 2 — RequestContext (5d7a1f4)
`backend/core/request_context.py` + `build_request_context` dep + inline `_enforce_service_access`. 13 tests, 7 tagged `security_regression`.

### Phase 3 + 3.5a — Middleware invariants + Settings + retry (multiple)
`MIDDLEWARE_ORDER` tuple + boot-time `assert_middleware_order()`. `tests/test_trust_topology.py` (11 tests). `backend/core/settings.py` pydantic-settings module. `backend/utils/retry.py` centralised tenacity retry policies.

### Phase 5a (full) — SQL extraction (f6caf9d, cfbcf47, 6f07be4, e7c9d42)
93 templates across 12 `_sql/` modules:
- usage (1), performance (1) — early extractions
- alerts (4), sessions (3), query (5), network (8), security (12), origin (17), dashboard (7) — bulk extraction workflow
- base (7), insights (30) — tail extraction
Skipped fragments documented (one-liners, runtime-built SQL, parquet glob paths).

### Phase 5b — metadata_db carve (f6a7a50)
`backend/core/metadata_db.py` (3168 → 83 line shim) → `backend/core/metadata/` (9 mixin modules: base, alerts, views, ingest_log, cron_log, asn_cache, usage_log, reconciliation, state). `_ShimModule` ModuleType subclass mirrors monkeypatch.setattr writes onto the carved modules — every existing test fixture keeps working.

### Phase 4a — iceberg fs carve (5c2b013)
`backend/core/iceberg.py` (4232 → 173 shim + 3812 _core + 490 fs). All 6 monkeypatches preserved verbatim in fs.py. `__init__.py` imports fs FIRST so patches install before any pyiceberg/s3fs import.

### Phase 6 — scheduler carve (5c2b013)
`backend/scheduler.py` (2843 → thin shim) → `backend/cron/` package (scheduler, decorators, jobs/{sync, commit, compaction, optimize, expire, metadata}). Pool-vs-process isolation decision deferred (needs Phase 1 thread-wait telemetry data).

### Phase 7 — FieldRegistry scaffold + 5 caller migrations (83ca9ca, e7c9d42, e326137)
`backend/core/field_registry.py` (527 lines): frozen-dataclass REGISTRY (81 fields, 15 derived) + BY_CODE + BY_GROUP + WIRE_ORDER + helpers. Derived from LOG_FIELD_CATALOG at import time. Migrated callers: bootstrap.py, usage.py, insights/repository.py, dashboard.py. Adversarial parity check survived 9 attack vectors (JSON injection, wire-order, dep-closure, security-hook completeness, etc.).

Deferred migrations (require step-13 helper ports first):
- services/core.py, orchestrator.py, fastly_api.py (PRESETS, generate_log_format, format_hash, render_vcl)
- iceberg/_core.py, ingest.py (loggable() / duck_type call sites)
- custom_fields.py, state_sync.py
- cli.py (GROUP_INFO not yet re-exported)

### Phase 9b — Frontend file splits (5c2b013)
16 files >500 lines split in parallel:
- ProvisionWizard.tsx (**3582 → 57** shell + 21 sub-files: steps/, hooks, types, helpers)
- logs/page.tsx 2136, admin/page.tsx 1438, alerts/page.tsx 959, etc.
Pure structural splits — no runtime behavior change.

### Phase 10a — tunnel + share_db carves (f6a7a50)
- tunnel.py (1022 → 5 modules), SSH-to-localhost.run path DELETED (~285 lines) per the v2.0 direct-mode-only decision.
- share_db.py (1312 → 9 modules), argon2-cffi added, transparent scrypt → argon2id rehash-on-login.

### Phase 10 finishing (076a3e7)
- rich + typer adopted in `provision/utils.py` + `provision/cli.py` (7 typer subcommands).
- httpx-everywhere sweep: no-op (telemetry_proxy.py is the sole legitimate aiohttp caller).
- `scripts/refresh_fastly_cidrs.py` + 6 tests (stdlib + httpx, idempotent, --dry-run / --check / --help, IPv4-only since the Caddyfile matcher is v4-only).
- 4 deploy runbooks under `docs/deploy/` (aws_ec2, gce, azure_vm, generic_linux) — same 7 sections each for side-by-side diff.

### Sessions/raw-logs redesign (83ca9ca)
Per `pending-docs/sessions_raw_logs_redesign_plan.md`: edge_sid added to Session model + sessions repo (`MAX(edge_sid)` aggregation + has_edge_sid flag), FlagSessionPopover supports un-flagging, `/sessions` page gets flag column + modal integration, dashboard raw-logs section removed → CTA banner to `/query`, AppLayout filter-bar conditional on `?mode=raw`, `/query` becomes dual-mode (Structured + Raw SQL).

### RBAC + UX audit fixes (1b53dcf)
Per `pending-docs/qa_ux_and_rbac_security_audit.md`:
- **H-1**: /api/usage/ added to `_ANALYST_BLOCKED_PREFIXES`
- **H-2**: /api/download* added to `_ANALYST_BLOCKED_SUBPATHS`
- **H-3**: /api/services/{id}/lake-info + /api/cron-schedule blocked
- **H-4**: scoring admin suffix gate (/config, /status, /audit, /threshold, /exclude-regex, /enforce-status-code)
- Trailing-slash bypass on H-3/H-4 (found by adversarial reviewer) — closed via path normalization
- **M-1**: alerts page hides Create Alert + actions for analysts
- **L-1**: rAF-throttled MapLibre mousemove handlers
- **L-2**: world.geojson preload moved out of `/share-login`
27 new security_regression tests (15 base + 12 adversarial bypass guards).

## Carve-up sizes (post)

| File | Before | After |
|---|---|---|
| `backend/core/iceberg.py` (4232) | monolith | `iceberg/_core.py 3812 + fs.py 490 + __init__.py 173` |
| `backend/scheduler.py` (2843) | monolith | thin shim + `backend/cron/` package |
| `backend/core/metadata_db.py` (3168) | monolith | `metadata_db.py 83-line shim` + `backend/core/metadata/` 9-module package |
| `backend/core/share_db.py` (1312) | monolith | `backend/core/share_db/` 9-module package |
| `backend/utils/tunnel.py` (1022) | monolith | `backend/utils/tunnel/` 5-module package, SSH-to-localhost.run path deleted (-285 lines) |
| `frontend/components/ProvisionWizard/ProvisionWizard.tsx` (3582) | monolith | 57-line shell + 21 sub-files |
| 15 other frontend files >500 lines | various | split |

## Frontend file-size scorecard

| Bucket | Baseline | Now |
|---|---|---|
| Frontend files > 500 lines | 16 | **1** (dashboard/page.tsx at 960 — the post-raw-logs-removal residue, a candidate for follow-up split) |

## Backend file-size scorecard

Top 5 backend files now:
1. `backend/core/iceberg/_core.py` 3812 (was 4232; -420 + carved fs)
2. `backend/routers/session_scoring.py` 2442 (untouched; future Phase 10b candidate)
3. `backend/core/duckdb.py` 2110 (untouched; future candidate)
4. `backend/core/log_fields.py` 1904 (FieldRegistry derives from it; final source-of-truth flip is step 13)
5. `backend/routers/admin.py` 1739 (Phase 10 explicit out-of-scope)

## Not yet done

### Phase 8 — Hard cutover (needs user coordination)
Drop the deprecated aliases: `AnalyticsDeps = RequestContext`, `is_cached`/`_is_cached` alias, `_meta_con` parallel path, `process_context_scope` vs `set_process_context` distinction. Requires 24–48h advance notice (CHANGELOG + README) per the plan. Don't ship without user.

### Phase 7 — Remaining 7 callers
Per the migration doc:
- step 5 (services/core.py), step 7 (orchestrator.py), step 8 (fastly_api.py) — need `PRESETS`, `generate_log_format`, `format_hash`, `render_vcl` ported to the registry first
- step 9 (iceberg/_core.py), step 10 (ingest.py) — need `loggable()` / `duck_type` callable surface
- step 11 (custom_fields.py), step 12 (state_sync.py)
- step 13 (log_fields.py internal cutover) — flips the source of truth from `LOG_FIELD_CATALOG` to `REGISTRY`
- cli.py — needs `GROUP_INFO` re-exported

### Phase 6 — Cron isolation decision (pool vs process)
Per the plan, this requires Phase 1 thread-wait-time telemetry data to choose. The Phase 6 carve is done; the isolation decision waits on telemetry from prod.

### Phase 1.4 — Full emitter migration
The OTel + structlog scaffolding is in place. Per-call-site review across the ~20 telemetry-emitting modules is still pending (the iothread mirror `process_context_scope`/`set_process_context` distinction depends on this).

### Frontend `app/dashboard/page.tsx` 960 lines
The post-raw-logs-removal residue. Could be split into `_sections/{Aggregates,Timeseries,TopN,GeoMap}.tsx` per the Phase 9b pattern.

## How to verify locally

```
make verify           # full pre-deploy gate
make security-regression   # 98 ≥ 24 floor
uv run pytest -q
```

## Memory worth noting

Per `verify-dev-first`: every code-affecting commit should be smoke-tested at `13002/18002` before GCE deploy. The audit fixes (1b53dcf) and the sessions redesign (83ca9ca) are the most user-visible — both deserve a dev pass before prod.
