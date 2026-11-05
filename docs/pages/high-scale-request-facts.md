# Request Facts (`/high-scale/request-facts`)

Paged explorer over the raw ClickHouse `request_facts` table for one service, with filters, response metadata (status, exactness, coverage, freshness) and cursor pagination.

Source: [`frontend/app/high-scale/request-facts/page.tsx`](../../frontend/app/high-scale/request-facts/page.tsx) (the page itself; `loading.tsx` alongside). The component is re-exported by [`frontend/components/high-scale/HighScaleRequestFactsPanel.tsx`](../../frontend/components/high-scale/HighScaleRequestFactsPanel.tsx).

## UI

- Metadata bar: status, exact or approximate, coverage, freshness lag and watermark.
- Mode badge (`data-testid="high-scale-mode-badge"`): displays the active ClickHouse query engine status from `metadata.mode`.
- Filters: `status`, `domain`, `pop`, `country`, `batch_id`, with Clear filters. The time window comes from the shared time range.
- Read-only table with previous/next cursor paging; "No request facts in this time range." when empty; loading card "Loading request facts…" while fetching; refetches dim the table.

## Roles

Admin. The UI route is blocked for share analysts in `AppLayout` (`/high-scale` is in the analyst-blocked prefixes). The API (`/api/high-scale/` is in the analyst POST allowlist) is scoped with `RequestContext`, clamped to the invite's time bounds, and masks `client_ip` per the invite policy.

## Backend

Routes in [`backend/routers/admin/high_scale.py`](../../backend/routers/admin/high_scale.py) (router prefix `/api/high-scale`; models in [`backend/models/admin/high_scale.py`](../../backend/models/admin/high_scale.py)); query logic in [`backend/high_scale/query_service.py`](../../backend/high_scale/query_service.py).

- `POST /api/high-scale/services/{service_id}/request-facts` (used by the page): `HighScaleRequestFactRequest` (`start_time`, `end_time`, `limit` up to the page maximum, `cursor`, `status`, `domain`, `pop`, `country`, `batch_id`). Returns `HighScaleRequestFactResponse`: `rows`, `next_cursor`, and `metadata` (`status`, `exact`, `coverage`, `freshness_lag_seconds`, `watermark`, `approximation_error`, `error`, `mode` = `clickhouse`).
- `GET` and `POST /api/high-scale/request-facts`: aliases that resolve the active service from the request context and default to the last 24 hours.
- Other routes in the same router (not used by this page): `rum-facts/{domain}`, `cmcd-facts`, `aggregates`, `queries` and `exports` per service.

A service must be bound through `HighScaleServiceRegistry`. Pagination uses HMAC-signed opaque cursors; ClickHouse readers require the batch's publication row to be `visible`. Cursor signing uses `HIGH_SCALE_CURSOR_SECRET` when set.

## Tests

- Backend: [`tests/high_scale/test_query_service.py`](../../tests/high_scale/test_query_service.py)
- E2E: `High-Scale Request Facts Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
