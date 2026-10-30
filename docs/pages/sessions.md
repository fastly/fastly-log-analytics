# User Sessions (`/sessions`)

Request-level sessions reconstructed from the logs, with filtering, operator flagging and a per-session inspector.

Source: [`frontend/app/sessions/`](../../frontend/app/sessions/) (`page.tsx` re-exports `_sections/SessionsClient.tsx` and `SessionsTable.tsx`), inspector in [`frontend/components/SessionDetailModal.tsx`](../../frontend/components/SessionDetailModal.tsx), flagging UI in [`frontend/components/SessionScoring/`](../../frontend/components/SessionScoring/). Built on `ReportLayout`; not SSR-seeded.

## How sessions are formed

[`backend/repositories/sessions/queries.py`](../../backend/repositories/sessions/queries.py) partitions requests by client IP (plus JA4 when the field is logged) and starts a new session after a gap of more than 30 minutes. Each row reports start/end, request count, unique URLs, 4xx/5xx counts, bytes, median RTT, edge/shield counts, `edge_sid` and, when CMCD fields exist, `is_streaming`. The page is limited to a 7-day window; a wider range shows a notice asking the user to narrow it. Single-request sessions are widened by one second each side before the analyst time clamp.

## URL parameters

Shared range and filter parameters (see [dashboard.md](dashboard.md)) plus `flagged` (boolean), `streaming` (boolean, only when CMCD is enabled), `min_reqs` and `min_4xx_pct` (integer thresholds that highlight sessions).

## Panels

- Controls: flagged-only, streaming-only, minimum requests, minimum 4xx %.
- Sessions table (newest first): IP, country, started, requests, unique URLs, ASN, 4xx%, median RTT, JA4, edge/shield, user agent, a streaming icon, and a Flag action. Optional columns appear only if the underlying fields are logged (`has_rtt`, `has_ja4`, `has_edge`, `has_edge_sid`, `has_cmcd`).
- Row click opens the session inspector dialog with summary fields and the session's individual request rows; streaming sessions link to `/sessions/stream?token=...` ([sessions-stream.md](sessions-stream.md)).
- Flagging records a scoring label through `/api/services/{service_id}/scoring/labels` (see [admin/session-scoring.md](admin/session-scoring.md)); labelled sessions show their label.

## Opaque session tokens

Every row carries `session_token`, an AES-GCM sealed `(ip, ja4, start, end)` tuple bound to the service (`seal_session_token` in [`backend/core/request_context.py`](../../backend/core/request_context.py)). The detail endpoint unseals it, so a masking analyst, whose displayed IP is rewritten, can still drill into a session. A masking analyst sending a raw `ip` without a token is rejected. The key is process-ephemeral unless `SESSION_TOKEN_SECRET` is set.

## Roles

| Role | Access |
|---|---|
| Admin | Full, raw IPs, flagging. |
| Analyst (remote share) | Read-only; `/api/sessions` is in the analyst POST allowlist; IPs masked per invite; flagging writes are rejected (`read_only`). |

## Backend

Router: `sessions_router` (prefix `/api/sessions`) in [`backend/routers/session_scoring/scoring_analytics.py`](../../backend/routers/session_scoring/scoring_analytics.py); models in [`backend/models/analytics/sessions.py`](../../backend/models/analytics/sessions.py).

- `POST /api/sessions` (`SessionsRequest`: range fields, `filters`, `page`, `limit` up to 500, `sort_by`, `sort_dir`, `flagged_only`, `streaming_only`, `min_reqs_flag`, `min_4xx_pct_flag`) returns `SessionsResponse`: `sessions[]`, `asn_names`, `total`, `page`, `limit` and the `has_*` flags.
- `POST /api/sessions/detail` (`SessionDetailRequest`: `session_token`, or `ip`/`ja4`/`start_time`/`end_time` for admins) returns `SessionDetailResponse` (`columns`, `data`).

Standard mode reads the sessions rollup where eligible (`rollup_reader.py`), otherwise the request view. High-scale mode ([`backend/high_scale/sessions.py`](../../backend/high_scale/sessions.py)) applies the shared filters, `sort_by`/`sort_dir`, real pagination and a real window `total`; `is_streaming` and `streaming_only` read the stored `cmcd` map.

## Tests

- Frontend: [`frontend/__tests__/app/usage-sessions-streaming.test.tsx`](../../frontend/__tests__/app/usage-sessions-streaming.test.tsx)
- Backend: [`tests/repositories/test_sessions.py`](../../tests/repositories/test_sessions.py), [`tests/core/test_session_token.py`](../../tests/core/test_session_token.py), [`tests/routers/test_session_scoring_router.py`](../../tests/routers/test_session_scoring_router.py)
- E2E: `User Sessions Explorer Page Contract` in [`frontend/e2e/pages/core-analytics-pages.spec.ts`](../../frontend/e2e/pages/core-analytics-pages.spec.ts)
