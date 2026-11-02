# Real User Monitoring (`/rum`)

Browser-side telemetry (Grafana Faro SDK beacons delivered through the Fastly edge): Core Web Vitals, JS errors, environments, worst pages and a live event stream. The page also hosts RUM enablement for admins.

Source: [`frontend/app/rum/`](../../frontend/app/rum/) (`page.tsx` re-exports `_sections/RumClient.tsx`, `RumStatusSection.tsx`, `RumVitalsSection.tsx`, `RumTablesSection.tsx`), dialogs in [`frontend/components/Rum/`](../../frontend/components/Rum/). Built on `ReportLayout`; not SSR-seeded. The admin-side ingestion and settings page is [admin/rum.md](admin/rum.md).

## Panels

- **RUM Status** card: enabled state and enable time, Faro SDK version card (admin), Enable RUM / Disable RUM dialogs (admin, streamed provisioning), installation snippet dialog. Analytics and live events are only requested when RUM is enabled; when no beacons exist in the window the page shows a no-data or empty-window card.
- **Vitals**: LCP, CLS, INP and FID p75 cards with a good / needs-improvement / poor distribution bar, a Web Vitals trend, a JS Error Rate trend.
- **Environments** (browser, OS, device donuts) and **Beacons Over Time** (pageviews, interactions, errors).
- **Worst Performing Pages** and **JavaScript Runtime Exceptions** tables.
- **Live Activity Monitor**: newest events with a detail dialog showing extracted client fields and the raw payload.

Standard filters apply; supported filter columns are `browser`, `os`, `device`, `country`, `pathname`, `city`, `region`, `pop`, `tls`. An unsupported column or malformed JSON is a `400 rum_filters_unsupported`; a filter is never silently dropped.

## Roles

| Capability | Admin | Analyst (remote share) |
|---|---|---|
| Analytics, live events, beacon health | yes | yes, read-only, window clamped; IP masking per invite |
| `GET /api/services/{id}/rum/status` | full status incl. drift | minimal `{enabled, enabled_at}` |
| Enable, disable, settings, versions, upgrade | yes (`require_admin`) | blocked (`403 admin_only`, `_ANALYST_BLOCKED_RUM_SUFFIXES`) |

## Backend

Routes under `/api/services/{service_id}/rum/...` in [`backend/routers/rum/`](../../backend/routers/rum/): `analytics.py`, `provisioning.py`, `beacon_ingest.py`. Models in [`backend/models/analytics/rum.py`](../../backend/models/analytics/rum.py).

- `GET .../rum/analytics` (`start_time`, `end_time`, `range_token`, `anchor`, `filters` as a JSON object `{column: {mode, values}}`): `RumAnalyticsResponse` with `no_data`, `beacon_count`, `pageview_count`, `interaction_count`, `error_count`, `vitals` (`lcp`, `cls`, `inp`, `fid`), `worst_pages`, `errors`, `trends` (aligned `timestamps`, `lcp`, `cls`, `error_rate`, `pageviews`, `interactions`, `errors`) and `environments`.
- `GET .../rum/live-events`: newest events (`time`, `type`, `path`, `desc`, `browser`, `os`, `raw_log`).
- `GET .../rum/beacon-health`: setup validation (beacons arriving).
- `GET .../rum/status`, `GET .../rum/versions`, `POST .../rum/enable`, `.../disable`, `.../upgrade`, `.../settings` (admin provisioning; see [admin/rum.md](admin/rum.md)).
- Asset routes `/js/rum.js` and `/js/faro-sdk.js` (and the beacon ingest route) in `beacon_ingest.py`.

Standard mode reads DuckLake `client_vitals` and `client_errors`, fed by the `rum_sync` / `rum_commit` crons ([rum-sync](../cron/jobs/rum-sync.md), [rum-commit](../cron/jobs/rum-commit.md)).

High-scale mode (`backend/high_scale/rum.py::rum_analytics`) reads the service's visible `rum_vitals_facts` and `rum_error_facts` through the instrumented ClickHouse client:

- Percentiles are continuous p75 over facts (counts and sums cannot supply them). Query failures propagate; they are never turned into zero-count responses.
- Bounds are inclusive UTC event times; missing bounds default to the last 24 hours. Windows up to 48 hours use hourly buckets, longer windows daily buckets including both endpoint buckets. Empty buckets have null measurements and zero counts.
- A beacon is identified by the request event ID, else client ID plus event time rounded to seconds; several metrics from one beacon count once. Pageviews exclude `event_%` metric names, interactions include them. Bucket error rate is distinct errors / (distinct vitals + distinct errors) x 100.
- LCP/FCP/TTFB values above 20 are divided by 1000 (ms to s) and rounded to 2 places, CLS to 3, INP is integer ms.
- `no_data` means neither fact table holds data for the service; an empty window over older data still returns the full empty axis.
- Environments (browser/OS/device) come from the vitals facts; beacons that reported nothing count as `Unknown`. Rows written before the environment columns existed read `Unknown` until rebuilt ([reader-parity backfill runbook](../runbooks/high-scale-reader-parity-backfill.md)).
- Every read requires the batch's publication row to be `visible`. Raw `raw/rum/` objects are shared by both RUM domains; see [AGENTS.md](../../AGENTS.md) trap 48.

## Tests

- Frontend: [`frontend/__tests__/app/rum.test.tsx`](../../frontend/__tests__/app/rum.test.tsx)
- Backend: [`tests/high_scale/test_rum.py`](../../tests/high_scale/test_rum.py), [`tests/cron/test_rum_sync.py`](../../tests/cron/test_rum_sync.py), [`tests/routers/test_analytics_routers.py`](../../tests/routers/test_analytics_routers.py)
