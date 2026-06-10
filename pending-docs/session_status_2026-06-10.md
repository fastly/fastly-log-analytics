# v2.0 Cleanup — Session Status (2026-06-10)

Supersedes [session_status_2026-06-09.md](session_status_2026-06-09.md).

## Branch state

Branch: `refactor/cleanup` (pushed)
Baseline tag: `refactor/cleanup-baseline` at `78f23d1` (post-merge main)
HEAD: `43b4326`

```
43b4326 Drop AnalyticsDeps; route 8 analytics routers via RequestContext (v2.0 cut Phase 8.1/8.2)
c79efe3 Drop get_meta_con (v2.0 cut Phase 8.3)
3ba45da Fix: dashboard Reset restores 24h window on fresh data too
eebc672 Fix: dashboard Reset also restores per-chart granularity to 1h
4722e1d Pre-cutover: snapshot prod → dev scripts + rollback runbook update
0b88f51 Phase 7 step 13 + Phase 9b dashboard residue + Phase 10 wrap
90b3d7e Session status doc refresh — full branch scorecard
e326137 Phase 7 migration: insights/repository.py + dashboard.py → field_registry
…
dc11e02 v2.0 cleanup Phase 0: ADRs, audits, gates, baselines
78f23d1 ← baseline
```

**24 commits ahead of baseline. ALL DEPLOYED TO PROD in 4 deploy cycles tonight.**

## Quality gates

| Gate | Result |
|---|---|
| `uv run pytest` | passes (exit 0). One historically-flaky test (`test_concurrent_readers_against_held_writer`) intermittently fails in isolation; passes in the parallel suite. Not caused by tonight's changes. |
| ruff backend/tests/scripts | clean (last verified before tonight's edits) |
| frontend tsc | clean |
| Backend tests added since refactor start | +1300 |

## Prod deploys tonight (all clean)

| Time (UTC) | HEAD | Change |
|---|---|---|
| 05:42:16 | eebc672 | Initial cleanup-branch deploy (Phase 0–7 + 9b + 10 wrap + Reset granularity fix) |
| 05:55:07 | 3ba45da | Reset 24h-window restore on fresh data (clearFilters bug) |
| 06:34:28 | c79efe3 | Drop `get_meta_con` (Phase 8.3) |
| 06:49:44 | 43b4326 | Drop `AnalyticsDeps`; 23 routes on `RequestContext` (Phase 8.1/8.2) |

Pre-deploy: `~/snapshots/pre-v2.0-cutover-20260610T050529Z/` (496 MB tar, sha256-verified manifest). Rollback runbook: [pending-docs/rollback_runbook.md](rollback_runbook.md).

## What shipped tonight

### Reset button fix on Dashboard
[3ba45da](../) + [eebc672](../) — pre-deploy prod browser-verify caught that `clearFilters()` only flipped flags; on fresh-data services the snap effect's `keep current range` branch made Reset a no-op for any narrow window. `clearFilters` now restores `startTime`/`endTime` to last-24h-from-now defaults before re-arming the auto-snap flags. Stale-data path still snaps to extents via `autoSetRange` (which overwrites the defaults). Regression test pins both branches.

### Phase 8.3 — `get_meta_con` removed
[c79efe3](../) — the skip-view-update parallel path was a perf hack for `/api/schema` + `/api/insight-availability`. After Phase 4 + `duckdb_pool.checkout_connection`'s fingerprint check (view-cache identity + buffer mtime) it's structurally unnecessary. `bootstrap.py` now uses `get_con`. Test pin (`test_get_meta_con_symbol_removed`) prevents silent re-introduction.

### Phase 8.1/8.2 — `AnalyticsDeps` deleted; 23 routes on `RequestContext`
[43b4326](../) — every analytics router (dashboard / query / sessions / security / network / origin / performance / insights, 23 endpoints) now takes `ctx: RequestContext = Depends(build_request_context)` instead of `deps: AnalyticsDeps = Depends()`. `AnalyticsDeps` class removed from `backend/deps.py`. Test pin (`test_analytics_deps_symbol_removed`) prevents re-introduction.

**Security implication (positive)**: `AnalyticsDeps` did NOT enforce analyst tenancy — `require_service_access` was defined but never wired in as a sibling dep. `build_request_context` calls `_enforce_service_access` inline, so any analyst session passing `?service_id=` for a service outside their scope now correctly 403s instead of returning data.

`tests/conftest.py` gained a `build_request_context` override on the shared `client` fixture so router tests still get a usable in-memory context.

## Phases shipped (cumulative; ✅)

Same list as [session_status_2026-06-09.md §Phases shipped](session_status_2026-06-09.md) — **PLUS**:

### Phase 7 step 13 — log_fields source-of-truth flip + 8 callers
[0b88f51](../) (deployed tonight as part of the initial cleanup-branch deploy). `field_registry.py` gained same-identity re-exports of every helper + constant (`generate_log_format`, `format_hash`, `get_lf_config`, `LOG_FIELD_CATALOG`, `PRESETS`, etc.). 8 callers migrated (`services/core.py`, `provision/orchestrator.py`, `provision/fastly_api.py`, `provision/cli.py`, `iceberg/_core.py`, `ingest.py`, `models/custom_fields.py`, `state_sync.py`). Adversarial verify across 9 attack vectors (object identity, byte-for-byte VCL, scoring fixtures, etc.) — all survived. Parity test pins re-export identity.

### Phase 9b dashboard residue split
[0b88f51](../). `frontend/app/dashboard/page.tsx` 960 → 748-line shell + 5 `_sections/*` files (`CardGrid`, `GeoMap`, `TrafficChart`, `categories.ts`, `chartHelpers.ts`, `types.ts`).

### Phase 10 wrap
[0b88f51](../). `AGENTS.md`, `docs/ARCHITECTURE.md`, `pending-docs/tech_debt_audit.md`, `pending-docs/library_evaluation.md` refreshed. New baseline snapshot at `pending-docs/baseline/20260610T034649Z/`.

### Phase 8.3 — get_meta_con removed
See [c79efe3](../) — described above.

### Phase 8.1/8.2 — AnalyticsDeps → RequestContext
See [43b4326](../) — described above.

## Backend file-size scorecard

Top 5 backend files now (post-Phase-7-step-13 + 9b):
1. `backend/core/iceberg/_core.py` — **3812** (was 4232; carved fs.py off in Phase 4a; further split blocked by 6-monkeypatch contract — see `pending-docs/design_iceberg_carveup.md`)
2. `backend/routers/session_scoring.py` — **2442** (untouched; future Phase 10b candidate per session_status_2026-06-09)
3. `backend/core/duckdb.py` — **2110** (untouched; future candidate)
4. `backend/core/log_fields.py` — **1904** (FieldRegistry derives from it; full source-of-truth flip would require dropping LOG_FIELD_CATALOG entirely — too deep to swap tonight)
5. `backend/routers/admin.py` — **1739** (Phase 10 explicit out-of-scope per cleanup_plan.md)

Success-criteria target was `0` files > 1500 lines. Still **5**. Each has a documented reason for not-yet-split; per the plan's "zero tech debt" rule, those reasons need to be acknowledged in the doc (this file) OR each file split.

## Frontend file-size scorecard

`frontend/app/dashboard/page.tsx`: 960 → **748** (Phase 9b residue split landed). No frontend file > 500 lines per the latest baseline.

## Not yet done

### Phase 8.4 — `_is_cached` alias
DEFERRED tonight. The Pydantic 2 `Field(alias=…)` pattern is a clean, idiomatic alias — not actually tech debt. Removing it churns the wire format (frontend reads `_is_cached`) for zero functional benefit. Worth a coordinated rename only if the project decides to standardize on no-underscore wire names (which would also affect `_debug_queries`, `_debug_calls`).

### Phase 6 — Cron isolation decision (pool vs process)
Per the plan, needs Phase 1 thread-wait telemetry data from prod to choose. The Phase 6 carve is done; the isolation decision is data-gated.

### Phase 1.4 — Full OTel/structlog emitter migration
~20 call sites. The scaffolding (`RequestTelemetry` + `thread_wait_histogram`) is in place. Per-call-site migration is deferred — the iothread `process_context_scope`/`set_process_context` distinction in Phase 10.1 depends on this.

### Phase 10.1 — `process_context_scope` vs `set_process_context` distinction
Two functions in `backend/utils/telemetry.py`, both load-bearing for cron + iothread mirror. Plan options: formalize as `RequestScope` / `BackgroundScope` typed scopes, or eliminate by making iothread mirror read from `RequestContext` directly. Either route is risky to ship at night — defer.

### File-size sweep (5 files > 1500 lines)
See scorecard above. Each requires its own carve design + tests; not safe to split without runway.

## How to verify locally

```
make verify           # full pre-deploy gate
make security-regression
uv run pytest -q
```

## Memory worth noting (unchanged)

Per `verify-dev-first`: every code-affecting commit was smoke-tested at `13002/18002` before each GCE deploy tonight.
Per `gce-deploy-rebuild`: deploys went via `gcloud compute ssh fastly-log-analysis --zone=us-central1-a` → `~/restart.sh`; browser hard-refreshed after each frontend rebuild.
Per `dev-sandbox-scrub`: after snapshot restore, the config was scrubbed (cdn_url cleared top + provisioning; cron_compact/sync/ngwaf disabled under provisioning.*).
