# Control Room (`/control-room`)

Real-time operations console. Live metrics come from Fastly's real-time analytics API (`rt.fastly.com`, one tick per second) and a rolling stream of recent 4xx/5xx requests from the ingested logs. It is not a historical, time-windowed report, so it uses `ReportShell` rather than `ReportLayout`.

Source: [`frontend/app/control-room/`](../../frontend/app/control-room/) (`page.tsx`, `_sections/ControlRoomClient.tsx`, per-tab files, `useControlRoomStream.ts`).

## UI

- Header: a connection badge (SSE state) and a freshness badge (time since the last tick).
- Tabs: Summary, CDN & Caching, Security, Performance, Network and Admin. Each historical tab links to its report page (`/dashboard`, `/origin`, `/security`, `/performance`, `/network`). The Network tab includes a PoP traffic map. The Admin tab is hidden from analysts and shows log-field audit and operations panels.
- The chart is seeded with the last 60 ticks on load, then updated from the stream; a recent-errors card lists 4xx/5xx events.
- Narrow viewports switch from tabs to a condensed `MobileOverview`.
- Background tabs stop polling.

## Roles

| Capability | Admin | Analyst (remote share) |
|---|---|---|
| Seed and metrics stream | yes | yes (`/realtime-stream` and `/control-room/error-stream` are in the analyst SSE allowlist) |
| Error stream, correlator | yes | yes, client IPs masked per invite |
| Log-field audit, Admin tab | yes | no (`/log-field-audit` is blocked for analysts) |
| Mutation endpoints | `501 not_implemented` | rejected before reaching the handler |

## Backend

All in [`backend/routers/control_room.py`](../../backend/routers/control_room.py) except the audit route in [`backend/routers/audit.py`](../../backend/routers/audit.py). Every route is scoped by `service_id` and checked with `require_service_access`.

- `GET /api/services/{service_id}/realtime-seed`: last 60 ticks (`RealtimeSeedResponse`) from the in-process publisher, or a fresh fetch from the Fastly real-time API; it also starts the poller for the service.
- `GET /api/services/{service_id}/realtime-stream` (SSE): live metric ticks. The poller (`backend/core/realtime`) runs on a dedicated daemon thread with a pooled HTTP client and transforms each real-time response into a typed tick.
- `GET /api/services/{service_id}/control-room/error-stream` (SSE): polls every 2 s for recent 4xx/5xx requests (`error_event` events with an `event_id`, de-duplicated). Standard mode queries DuckDB; high-scale mode queries ClickHouse `request_facts`. `client_ip` is masked for masking analysts.
- `POST /api/services/{service_id}/control-room/correlate` (`CorrelateRequest`: `dimension`, `window_minutes` 1 to 60 default 5, `limit` 1 to 100 default 10): top values of a dimension among recent errors; the `ip` dimension is masked for masking analysts.
- `GET /api/services/{service_id}/control-room/{tab}`: static tab payloads (`overview`, ...); unknown tabs are `404 unknown_tab`.
- `GET /api/services/{service_id}/log-field-audit`: which log fields are enabled and which features they support.
- `POST /api/services/{service_id}/control-room/{mitigations,rules,allowlist,big-red-button,cost-governor}`: admin-only placeholders that always return `501 not_implemented`.

## Tests

- E2E: `Control Room` in [`frontend/e2e/pages/admin-overview.spec.ts`](../../frontend/e2e/pages/admin-overview.spec.ts) (sidebar link, desktop tabs, SSE badge, mobile layout, accessibility).
