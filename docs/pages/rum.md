# Real User Monitoring (`/rum`)

Browser-side telemetry (Grafana Faro SDK beacons delivered through the Fastly edge): Core Web Vitals, JS errors, environments, worst pages and a live event stream. The page also hosts RUM enablement for admins.

Source: [`frontend/app/rum/`](../../frontend/app/rum/) (`page.tsx` re-exports `_sections/RumClient.tsx`, `RumStatusSection.tsx`, `RumVitalsSection.tsx`, `RumTablesSection.tsx`), dialogs in [`frontend/components/Rum/`](../../frontend/components/Rum/). Built on `ReportLayout`; not SSR-seeded. The admin-side ingestion and settings page is [admin/rum.md](admin/rum.md).

## Panels

- **RUM Status** card: enabled state and enable time, Faro SDK version card (admin), Enable RUM / Disable RUM dialogs (admin, streamed provisioning), installation snippet dialog. Analytics and live events are only requested when RUM is enabled; when no beacons exist in the window the page shows a no-data or empty-window card.
- **Vitals**: LCP, CLS, INP and FID p75 cards with a good / needs-improvement / poor distribution bar, a Web Vitals trend, a JS Error Rate trend.
- **Environments** (browser, OS, device donuts) and **Beacons Over Time** (pageviews, interactions, errors).
- **Worst Performing Pages** and **JavaScript Runtime Exceptions** tables.
- **Slowest URLs (Client TTLB vs Edge)** and **Slowest Networks (Client TTLB vs Edge)** tables (client-side resource TTLB percentiles `avg`, `p50`, `p95`, `p99` paired with matching edge elapsed times via `req_id`, sortable by `P99`, `P95`, `P50`, or `Avg`).
- **Live Activity Monitor**: newest page, resource timing (`ttlb`), and error events with a detail dialog showing extracted client fields, raw payload, and—when a `req_id` is present—the **Matching Edge Request Log** line fetched on demand (`GET .../rum/request-log`).

Standard filters apply; supported filter columns are `browser`, `os`, `device`, `country`, `pathname`, `city`, `region`, `pop`, `tls`. An unsupported column or malformed JSON is a `400 rum_filters_unsupported`; a filter is never silently dropped.

Beacons sampled below 100% (`sample_rate`, `error_sample_rate`, `resource_sample_rate`) store their active sampling rate per row so counts in analytics, live breakdowns, and Slowest URLs/Networks are inverse-probability weighted (`1 / sample_rate`) and flagged `estimated: true` when sampled rows are present (percentiles remain unweighted over observed timings).

Cloud accepts fields present in its RUM tables, including `browser_name`,
`metric_name`, `metric_value`, `error_type`, `req_id`, `resource_url`, and `asn` (stored as text in Parquet and safely coerced via `TRY_CAST` in DuckDB), plus the `browser`, `os`, `device`
and `pathname` aliases. Request-only custom fields are unsupported even after
visiting request analytics. A field absent from one RUM dataset has NULL semantics:
an include filter excludes those rows; an exclude filter keeps them.
Cloud keeps vitals and errors in separate typed Parquet views over cached
compressed files. Request-schema changes do not change their SQL types, and
stale range reads fail rather than returning success against a replacement dataset.

## Roles

| Capability | Admin | Analyst (remote share) |
|---|---|---|
| Analytics, live events, beacon health | yes | yes, read-only, window clamped; IP masking per invite |
| `GET /api/services/{id}/rum/status` | full status incl. drift | minimal `{enabled, enabled_at}` |
| Enable, disable, settings, versions, upgrade | yes (`require_admin`) | blocked (`403 admin_only`, `_ANALYST_BLOCKED_RUM_SUFFIXES`) |

## Backend

Routes under `/api/services/{service_id}/rum/...` in [`backend/routers/rum/`](../../backend/routers/rum/): `analytics.py`, `provisioning.py`, `beacon_ingest.py`. Models in [`backend/models/analytics/rum.py`](../../backend/models/analytics/rum.py).

- `GET .../rum/analytics` (`start_time`, `end_time`, `range_token`, `anchor`, `filters` as a JSON object `{column: {mode, values}}`): `RumAnalyticsResponse` with `no_data`, `estimated`, `beacon_count`, `pageview_count`, `interaction_count`, `error_count`, `vitals` (`lcp`, `cls`, `inp`, `fid`, `fcp`, `ttfb`), `worst_pages`, `slowest_urls`, `slowest_networks`, `errors`, `trends` (aligned `timestamps`, `lcp`, `cls`, `fcp`, `ttfb`, `error_rate`, `pageviews`, `interactions`, `errors`) and `environments`.
- `GET .../rum/live-events`: newest events (`time`, `type`, `path`, `desc`, `browser`, `os`, `req_id`, `resource_url`, `metric_name`, `metric_value`, `raw_log`).
- `GET .../rum/request-log` (`req_id`, optional `timestamp`): looks up the matching edge request log line for a RUM resource timing or beacon `req_id` (`found`, `req_id`, `request`), enforcing analyst IP masking / PII redaction.
- `GET .../rum/beacon-health`: setup validation (beacons arriving).
- `GET .../rum/status`, `GET .../rum/versions`, `POST .../rum/enable`, `.../disable`, `.../upgrade`, `.../settings` (admin provisioning; see [admin/rum.md](admin/rum.md)).
- Asset routes `/js/rum.js` and `/js/faro-sdk.js` (and the beacon ingest route) in `beacon_ingest.py`.

The RUM analytics reader (`backend/high_scale/rum.py::rum_analytics`) reads the service's visible `rum_vitals_facts` and `rum_error_facts` in ClickHouse through the instrumented ClickHouse client (ingested from `raw/rum/` via the [`rum_discovery`](../cron/jobs/rum-discovery.md) cron and the stateless ingest worker):

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
