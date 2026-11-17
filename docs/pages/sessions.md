# User Sessions (`/sessions`)

Request-level sessions reconstructed from the logs, with filtering, operator flagging and a per-session inspector.

Source: [`frontend/app/sessions/`](../../frontend/app/sessions/) (`page.tsx` re-exports `_sections/SessionsClient.tsx` and `SessionsTable.tsx`), inspector in [`frontend/components/SessionDetailModal.tsx`](../../frontend/components/SessionDetailModal.tsx), flagging UI in [`frontend/components/SessionScoring/`](../../frontend/components/SessionScoring/). Built on `ReportLayout`; not SSR-seeded.

## How sessions are formed

[`backend/high_scale/sessions.py`](../../backend/high_scale/sessions.py) partitions requests in ClickHouse `request_facts` by client IP (plus JA4 when the field is logged) and starts a new session after a gap of more than 30 minutes. Each row reports start/end, request count, unique URLs, 4xx/5xx counts, bytes, median RTT, edge/shield counts, `edge_sid` and, when CMCD fields exist, `is_streaming`. The page is limited to a 7-day window; a wider range shows a notice asking the user to narrow it. Single-request sessions are widened by one second each side before the analyst time clamp.

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

The ClickHouse reader ([`backend/high_scale/sessions.py`](../../backend/high_scale/sessions.py)) applies the shared filters, `sort_by`/`sort_dir`, real pagination and a real window `total`; `is_streaming` and `streaming_only` read the stored `cmcd` map.

## Cloud

Cloud sessionizes raw Parquet in local DuckDB ([`frontend/lib/cloud-engine/sessions.ts`](../../frontend/lib/cloud-engine/sessions.ts), [`sessions-window.ts`](../../frontend/lib/cloud-engine/sessions-window.ts)). The browser's DuckDB has 256 MB and can't spill, so the list never loads the whole range with every column.

The default newest-first page loads only the files from the last 45 minutes: sessions that start in the last 15 minutes, plus the 30 minutes before them, because a session that starts at t follows 30 minutes of silence. If that slice holds fewer sessions than the page, the slice widens 4× (1 h, 4 h, …, then the whole range) and loads only the older files it needs, releasing the narrower read. Other sorts load the whole range. Either way the ranking pass skips `ua` and `url` (except `url` when sorting by unique URLs). A second pass then reads `ua` and `url` only from the files that the page's sessions overlap (at most 8 loads), and fills in the user agent and unique URLs for those sessions. Results equal a whole-range scan exactly. One client's sessions never overlap, so its rows inside a session's span are exactly that session's rows.

On `session-scans.test.ts`'s day of 5-minute files (57,691 rows, 1.65 MiB of Parquet), page 1 reads 2,214 rows and 47 KiB instead of the whole day.

A filter or `flagged_only` that matches few sessions can widen to the whole range. The 256-file and 64 MiB compressed scan limits still apply to every load, and going over them returns their 413 (`… choose a shorter time range`). `total` is the page's row count, not a window total.

## Tests

- Frontend: [`frontend/__tests__/app/usage-sessions-streaming.test.tsx`](../../frontend/__tests__/app/usage-sessions-streaming.test.tsx), Cloud: [`frontend/lib/cloud-engine/session-scans.test.ts`](../../frontend/lib/cloud-engine/session-scans.test.ts)
- Backend: [`tests/core/test_session_token.py`](../../tests/core/test_session_token.py), [`tests/routers/test_session_scoring_router.py`](../../tests/routers/test_session_scoring_router.py)
- E2E: `User Sessions Explorer Page Contract` in [`frontend/e2e/pages/core-analytics-pages.spec.ts`](../../frontend/e2e/pages/core-analytics-pages.spec.ts)
