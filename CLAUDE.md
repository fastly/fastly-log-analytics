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

Major feature areas: interactive analytics (dashboard, origin, security, network, performance), Insights (45 anomaly detections), Control Room (real-time RT API at 1 s cadence), Streaming (CMCD analytics), Service Summary (Fastly value executive view), Session Scoring (Rust/Wasm edge scorer), live analyst sharing with OAuth/OIDC, and ingest error quarantine.

### Key directories

- `backend/routers/` — FastAPI route handlers (one per analytics domain); `admin/` is now a package with sub-modules (health, events, ingest, quarantine, etc.)
- `backend/routers/control_room.py` — real-time RT API poller + SSE fan-out (Control Room)
- `backend/routers/cmcd.py` / `cmcd_admin.py` — CMCD streaming analytics + field admin
- `backend/routers/value.py` — Service Summary (Fastly value executive view)
- `backend/routers/session_scoring.py` / `session_scoring_admin.py` — edge scorer management
- `backend/routers/share_oauth.py` — OAuth/OIDC analyst sign-in
- `backend/repositories/` — DuckDB query layer (all SQL lives here, never inline in routers)
- `backend/core/` — ingest pipeline, DuckDB pool, field registry, compaction, scheduling
- `backend/provision/` — Fastly service provisioning (FOS buckets, CDN, logging endpoints)
- `backend/scoring/` — Python mirror of the Rust scorer (parity contract)
- `frontend/app/` — Next.js App Router pages; includes `control-room/`, `fastly-value/`, `streaming/`
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
- **DuckDB tuning env vars** (set in compose env or `.env`): `DUCKDB_MEMORY_LIMIT` (default `4GB`) caps buffer pool; `DUCKDB_THREADS` (default `min(cpu_count, 8)`) caps parallelism — both take effect at connection open. `DUCKDB_POOL_MAX_SIZE` caps concurrent connections per service (prod default: 4). Prefer lowering these on memory-constrained VMs before adding swap.

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
