# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# fastly-log-analytics

Interactive log analytics for Fastly logs stored in Fastly Object Storage, powered by DuckDB + Next.js + FastAPI.

## Before any non-trivial change

Read [AGENTS.md](AGENTS.md) end-to-end. Most regressions in this codebase are re-discoveries of a documented trap. Re-read the Traps & Gotchas section before every change. See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for system design.

## Commands

- `make dev` — start full stack (backend + frontend) with hot reload via `./run.sh --dev`
- `make ci` — full CI pipeline locally (lint, typecheck, test, security, e2e)
- `make test` — backend tests quick loop: `uv run pytest`
- `make test-ci` — backend tests as CI runs them (`-n auto`, coverage floor 86%, includes terraform tests)
- `make test-frontend` — frontend tests: vitest
- `make test-frontend-ci` — frontend tests with coverage floors (CI gate)
- `make e2e` — Playwright end-to-end tests (chromium + firefox + webkit)
- `make lint` — ruff check + ruff format --check
- `make import-contracts` — enforce no cross-router imports + core ↛ routers (import-linter)
- `make typecheck` — mypy on backend
- `make gen-types` — regenerate OpenAPI spec + frontend TypeScript types
- `make openapi-drift` — verify frontend openapi.json matches backend
- `make scorer-package` — rebuild Rust/Wasm scorer (needs rustup + Fastly CLI, run off-VM)
- `make scorer-test` — `cargo test` on the scorer
- `make security-regression` — security regression suite (floor=206, never lower)
- `make verify` — full CI + ratchet checks

Single test: `uv run pytest tests/path/to/test_file.py::test_name -v`

## Architecture

FastAPI backend in `backend/`, Next.js App Router frontend in `frontend/`, Rust Compute@Edge scorer in `compute/scorer/`. Data plane: Fastly logs land in FOS (Fastly Object Storage), the backend ingests .gz files into DuckDB via Iceberg-like local tables with parquet storage, pre-computed rollups for performance, and per-service SQLite metadata DBs for state tracking. The frontend talks to the backend API; types are generated from the OpenAPI spec.

### Key directories

- `backend/routers/` — FastAPI route handlers (one per analytics domain)
- `backend/repositories/` — DuckDB query layer (all SQL lives here, never inline in routers)
- `backend/core/` — ingest pipeline, DuckDB pool, field registry, compaction, scheduling
- `backend/provision/` — Fastly service provisioning (FOS buckets, CDN, logging endpoints)
- `backend/scoring/` — Python mirror of the Rust scorer (parity contract)
- `frontend/app/` — Next.js App Router pages
- `frontend/components/` — shared UI (ReportLayout, AnalyticsCard, PlotlyChart, etc.)
- `frontend/lib/` — api client, SSR helpers, log-extents snap
- `frontend/stores/` — Zustand stores (serviceStore, filterStore)
- `frontend/hooks/` — React hooks (SSE, bootstrap, URL sync)
- `compute/scorer/` — Rust session scoring Compute service

## Conventions

### Backend
- Use `uv run pytest` not bare `pytest` — system pip drifts off `uv.lock`
- All DuckDB queries go through repository functions in `backend/repositories/`, never inline SQL in routers
- `RequestContext` (from `backend/deps.py`) provides `service_id`, DuckDB connection, and config to route handlers
- Error responses use `make_error()` / `@query_errors` decorator
- Route tests assert via `app.openapi()["paths"]` not `app.routes` walk
- Response models: use `extra="allow"` + `exclude_unset=True` for wire safety; timestamps need `.isoformat()`
- Two roles: admin (trusted, full access) and analyst (adversary model, PII-masked, RBAC-gated via `remote_access` middleware)

### Frontend
- State: Zustand stores for client state, React Query for server state, nuqs for URL state
- SSR: force-dynamic + client-only live query pattern (useMounted/initialData to avoid React #418)
- API client: `lib/api.ts` with `getApiBase()` — SSE hooks concat string, never `new URL()`
- Type generation: `npm run gen:types` from OpenAPI spec (`frontend/openapi.json` -> `frontend/types/api.generated.ts`)
- UI primitives: ReportLayout/ReportShell for page structure, AnalyticsCard for cards, PlotlyChart + ChartA11yTable for charts
- Design tokens: OKLCH in `globals.css`
- ESLint ceiling ratchet (832) — drive DOWN, never raise
- `SERVICELESS_PATH_PREFIXES` in api-client: routes that work without an active service

### Scorer (Rust/Python parity)
- `compute/scorer/` (Rust) and `backend/scoring/` (Python) must stay in sync
- FSM1 matrix wire format, AES-GCM session cookie codec, URL normalizer
- Changes to scoring logic require updating both sides + cross-language fixtures

### CI ratchets
- ESLint ceiling: 832 (check_eslint_count.sh) — drive down
- Security regression floor: 206 — never lower
- Backend coverage floor: 86%
- Frontend coverage floors: defined in Makefile `test-frontend-ci` target
- OpenAPI drift: frontend/openapi.json must match backend

### Testing
- Backend: `uv run pytest` (pytest9, strict markers, frozen_clock seam)
- Frontend: `npm run test` from `frontend/` (not bare `npx vitest` — stale cache)
- E2e: Playwright with axe a11y + keyboard nav gates
- Derive test fixture keys from the PRODUCER (the code that writes), not the consumer
- `gc_collect_iterations=0` in conftest to suppress pytest9 "unclosed database" spam

### Deployment
- Single GCE VM: Docker Compose + Caddy reverse proxy
- Prod binds 127.0.0.1; Caddy:80 is sole ingress (stamps X-Proxied-By-Caddy)
- Admin access via SSH tunnel to :3001; analyst access via `/share-login`
- Deploy: `gcloud compute ssh <INSTANCE> --zone=<ZONE> --command="~/restart.sh"`
- Verify on dev (ports 13002/18002) before GCE deploy
- `local-docs/` and `pending-docs/` are local-only; `docs/` is PUBLIC
- Repo is PUBLIC — never commit GCE/bucket/service-ID strings; use `infra-leak-sweep` skill

### OpenAPI contract
- After editing backend routes or models: run `make gen-types` to regenerate `frontend/openapi.json` + `frontend/types/api.generated.ts`
- A PostToolUse hook reminds you when edits touch backend files

## Critical traps (abridged — full list in AGENTS.md)

**SQL injection** — All DuckDB table names from user-controlled values (service IDs, field names) must go through `_safe_table()` from `backend/repositories/_base.py`. Never interpolate into SQL strings.

**DuckDB write connections are serialized** — `get_connection()` in `backend/core/duckdb.py` locks. Never hold a write connection across requests. Operational metadata goes to per-service SQLite via `backend/core/metadata/`, not DuckDB.

**Operational metadata vs DuckDB** — Alerts, views, cron history, ingested-file dedup, ASN names → `data/services/{id}.metadata.db` (WAL). Usage telemetry → `data/services/{id}.usage_log.db`. Read/write via `backend/core/metadata/`; never via DuckDB directly. JOINs against log data: ATTACH SQLite read-only as `meta` via `attach_metadata_db()`.

**Configs are keyed by LOGGING service ID**, not CDN service ID. `cdn_service_id` is a separate Fastly service.

**Falsy empty list in `or` chains** — `data.get("data") or []` falls through when `data["data"]` is `[]`. Use `data.get("data") if "data" in data else data.get("workspaces", [])`.

**All configs are schema v2** — always load: `lf = cfg.get("log_fields") or {"schema_version": 2, "custom_fields": []}`.

**SSR upstream fetch must use `node:http`**, not `fetch()` — Node's `fetch()` rewrites the `Host` header. SSR helpers (e.g. `frontend/lib/ssr/bootstrap.ts`) use `node:http.request` to preserve arbitrary headers. New SSR helpers must copy the `rawRequest` pattern; using `fetch()` causes the backend to classify the request wrong and return admin data to anonymous users.

**MSW + openapi-fetch ordering** — `server.listen()` in `frontend/vitest.setup.ts` must run at module load, NOT inside `beforeAll`. `openapi-fetch` captures `globalThis.fetch` at `createClient` time (module load), so a later `listen()` leaves the captured fetch unpatched and MSW handlers silently never fire.

**RTL auto-cleanup is off** — `frontend/vitest.config.ts` sets `globals: false`, so RTL doesn't register its own `afterEach(cleanup)`. The setup file has an explicit one; without it, earlier-test components stay mounted and pollute `screen.getBy*`.

**Live-slice consumers** — new code reading the buffer dir directly must skip tombstoned parquets via `_tombstoned_parquet_paths()`, or reuse the per-request shared temp via `begin_shared_active_hour_temps()` / `end_shared_active_hour_temps()`. Reading tombstoned files double-counts rows already committed to Iceberg.

**Virtual fields in top-N batch** — filter to `actual_cols` before the live-hour UNION ALL; virtual fields (e.g. `waf_sig_ind`) don't exist as real columns and silently kill the whole batch for real fields too.

**RemoteAccessMiddleware blocks admin routes over live-share** — when you add an endpoint analysts must reach, register under `/api/share/*` or update `_is_blocked_path()`. The `testclient`/`testserver` allow-list entries in that middleware must not be removed.

**Rollup bundle backfill after adding a field** — `bundle_hours`/`bundle_days` skip up-to-date bundles by mtime. Closed historical `all_fields.parquet` files don't update automatically. After adding a field to a rollup writer, delete those files and run `backfill_missing_bundles` (or `POST /api/admin/backfill-bundle-rollups`) so historical hours include the new field.

**Local compaction outputs survive orphan-cleanup** — orphan-cleanup in `sync_data()` must skip `cache/data/daily/`, `cache/data/weekly/`, and `compacted_*.parquet` in timestamp-hour dirs. If you add a new local-only output pattern, add it to both the dir skip and the file skip.

**VCL regex RHS must be a string literal** — the RHS of `~` / `!~` must be a literal. No variables, no concatenation. Use `regsub()` / `regsuball()` for dynamic logic.
