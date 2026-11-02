# Page Specifications (`docs/pages/`)

One document per route. Each describes what the page shows, the backend endpoints it calls, who can use it, and the tests that cover it. For system design see [ARCHITECTURE.md](../ARCHITECTURE.md) and [AGENTS.md](../../AGENTS.md); background jobs are in [docs/cron](../cron/README.md).

## Pages

| Route | Spec | Focus |
|---|---|---|
| `/dashboard` | [dashboard.md](dashboard.md) | Traffic chart, country map, Top-N cards, global filters |
| `/origin` | [origin.md](origin.md) | Origin latency, errors, status codes, slow URLs, per-IP health |
| `/performance` | [performance.md](performance.md) | Latency waterfall, slowest URLs and networks, cache TTL, origin vs edge |
| `/security` | [security.md](security.md) | Bots, TLS fingerprints, header anomalies, proxy watchdog |
| `/network` | [network.md](network.md) | ASN and PoP health, transport metrics, RTT breakdowns, shielding |
| `/insights` | [insights.md](insights.md) | 51 anomaly detectors in five categories |
| `/streaming` | [streaming.md](streaming.md) | CMCD video quality across all viewers |
| `/rum` | [rum.md](rum.md) | Core Web Vitals, JS errors, environments, live events |
| `/sessions` | [sessions.md](sessions.md) | Reconstructed request sessions, flagging, inspector |
| `/sessions/stream` | [sessions-stream.md](sessions-stream.md) | Per-session video QoE |
| `/fastly-value` | [fastly-value.md](fastly-value.md) | Offload, cache, bandwidth, threats, Image Optimizer |
| `/assets-shield` | [assets-shield.md](assets-shield.md) | Cache and compression by asset type |
| `/query` | [query.md](query.md) | Read-only SQL pad and structured log browser |
| `/logs` | [logs.md](logs.md) | Data management: crons, audit, ingestion, storage, schema |
| `/alerts` | [alerts.md](alerts.md) | Alert rules and notifications |
| `/usage` | [usage-and-cost.md](usage-and-cost.md) | Storage and API usage, cost estimator |
| `/control-room` | [control-room.md](control-room.md) | Real-time metrics and recent errors |
| `/high-scale/request-facts` | [high-scale-request-facts.md](high-scale-request-facts.md) | Paged request-fact explorer |
| `/share-login` | [share-login.md](share-login.md) | Analyst sign-in and terms gate |
| `/admin` | [admin/overview.md](admin/overview.md) | Status cards, services, health, quarantine, settings |
| `/admin/queries` | [admin/queries.md](admin/queries.md) | Live query monitor |
| `/admin/queue` | [admin/queue.md](admin/queue.md) | Ingest ledger, Celery queues, scheduler |
| `/admin/trends` | [admin/trends.md](admin/trends.md) | Sampled system vitals |
| `/admin/usage-log` | [admin/usage-log.md](admin/usage-log.md) | Per-operation FOS and CDN ledger |
| `/admin/rum` | [admin/rum.md](admin/rum.md) | RUM enablement and settings |
| `/admin/session-scoring` | [admin/session-scoring.md](admin/session-scoring.md) | Edge session scoring console |
| `/admin/share` | [admin/share.md](admin/share.md) | Remote sharing, invites, sessions, audit |
| `/admin/clickhouse` | [admin/clickhouse.md](admin/clickhouse.md) | ClickHouse status and bounded replay |

## Conventions shared by the pages

**Layout and state.** Analytics pages are built on `ReportLayout` ([`frontend/components/ReportLayout.tsx`](../../frontend/components/ReportLayout.tsx)), which provides the service selector, time range, filter bar, saved views and compare mode. Global state lives in [`frontend/stores/filterStore.ts`](../../frontend/stores/filterStore.ts) (filters, time range, compare, edge-only) and the service store; per-page hooks are in [`frontend/hooks/`](../../frontend/hooks/). Multi-panel pages fetch one composite endpoint (for example `POST /api/dashboard/bundle`, `/api/origin/aggregates`) instead of one request per panel.

**Time range.** The default window is the last 24 hours. A service with less history uses its full extent (`isAutoRange`). Requests can carry a relative `range_token` plus an `anchor`; the server resolves the window from them (`backend/utils/time_window.py`) and clamps it to the caller's ceiling. Real-time pages (`/control-room`, `/sessions/stream`) and the admin trend pages use their own windows.

**Loading.** Every route has a `loading.tsx`. Cards reserve their height and show a per-panel loading message; refetches dim existing content rather than replacing it with a skeleton. Pages whose first-paint query key is reproducible on the server (`/security`, `/insights`, `/streaming`, `/assets-shield`, `/alerts`, `/logs`, `/admin/trends`) prefetch and dehydrate their default data; see the frontend section of [AGENTS.md](../../AGENTS.md).

**Roles.** There are two personas. The admin has full access. The analyst is a remote share session: read-only, limited to the invited services and time ceiling, with client IPs and session IDs masked according to the invite's PII policy. Enforcement is server-side: `RemoteAccessMiddleware` ([`backend/utils/remote_access.py`](../../backend/utils/remote_access.py)) blocks admin prefixes and non-allow-listed writes for analysts, and `RequestContext` ([`backend/core/request_context.py`](../../backend/core/request_context.py)) enforces service access and time clamping. Pages that are admin-only for analysts: `/alerts`, `/usage`, `/logs`, `/high-scale/*` and everything under `/admin`.

**Deployment modes.** `DEPLOYMENT_MODE=standard` serves from DuckDB over DuckLake plus the local Parquet buffer, using rollups where eligible. `DEPLOYMENT_MODE=high_scale` serves from ClickHouse readers in `backend/high_scale/`, which return real data or fail loudly. Operational metadata is PostgreSQL in both ([ADR-22](../adr/22-postgres-only-metadata.md)). Each page document notes where the two modes differ.

**Telemetry.** The frontend API client sends `X-Page-Load-ID` on every request; when debug responses are enabled it also sends `x-debug-responses: 1`, and responses then include `_debug_queries` / `_debug_calls` that the Debug Panel renders. `GET /api/debug/page-telemetry` returns the telemetry recorded for a page load (admin only).

## Test layout

- Frontend unit tests: [`frontend/__tests__/`](../../frontend/__tests__/) (vitest, MSW)
- E2E: [`frontend/e2e/`](../../frontend/e2e/) (Playwright); page contracts are in `pages/dashboard.spec.ts`, `pages/core-analytics-pages.spec.ts`, `pages/admin-overview.spec.ts` and `zz-analyst-share-login-lockout.spec.ts`
- Backend: [`tests/`](../../tests/) mirrors the source tree (`routers/`, `repositories/`, `core/`, `cron/`, `high_scale/`, `remote_access/`)

## Writing or updating a page spec

State only what exists: the route and its source files, URL parameters, panels, roles, endpoints with request and response fields taken from the Pydantic models, deployment-mode differences, and tests that exist in the repository. Keep it short.
