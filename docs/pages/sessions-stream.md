# Stream Session Detail (`/sessions/stream`)

Per-session video QoE inspector for a session that carries CMCD data. It is reached from the Sessions table through an opaque token; the fleet-level view is [streaming.md](streaming.md).

Source: [`frontend/app/sessions/stream/page.tsx`](../../frontend/app/sessions/stream/page.tsx) (client page with a `loading.tsx`). Aggregation happens client-side in `useStreamAggregates` over the returned request rows.

## URL parameters

- `token`: the `session_token` from `POST /api/sessions` (see [sessions.md](sessions.md)). Without it the page shows a "No session token" alert with a link back to `/sessions`.
- `service`: handled by the shared service resolution.

## Panels

- Summary cards: Duration, Avg Bitrate, Top Bitrate, Utilization, Rebuffers, Startup Time, Buffer Health.
- Session Timeline, Bitrate Distribution (with upshift/downshift counts), Buffer Depth Distribution, Network Conditions, Content Breakdown, Buffer Starvation Events.
- Alerts: "Partial data" when exactly 500 rows come back (the analysis covers the first 500 requests); "No video segment requests" for manifest-only sessions; "Failed to load session" on error.

## Roles

Admin and analyst both load the page. An analyst's IPs are masked per invite; the sealed token is what lets a masking analyst open the detail. `POST /api/sessions/detail` is the only data call.

## Backend

`POST /api/sessions/detail` with `{session_token}` returns `SessionDetailResponse` (`columns`, `data`) from [`backend/routers/session_scoring/scoring_analytics.py`](../../backend/routers/session_scoring/scoring_analytics.py). Standard mode reads the request view for the unsealed session extent; high-scale mode reads `request_facts` for it.

## Tests

Covered by [`frontend/__tests__/app/sessions-stream.test.tsx`](../../frontend/__tests__/app/sessions-stream.test.tsx), [`tests/core/test_session_token.py`](../../tests/core/test_session_token.py), and [`tests/repositories/test_sessions.py`](../../tests/repositories/test_sessions.py).
