# Dashboard (`/dashboard`)

Landing analytics page: a traffic time series, a country map and a grid of Top-N cards over one service, one time window and one global filter set. Clicking any value adds a filter that re-queries every panel.

Source: [`frontend/app/dashboard/`](../../frontend/app/dashboard/) (`page.tsx`, `_sections/DashboardClient.tsx`, `_sections/DashboardWidgets.tsx`, `loading.tsx`). The page is a client component built inside `ReportLayout` ([`frontend/components/ReportLayout.tsx`](../../frontend/components/ReportLayout.tsx)); it is not SSR-seeded.

## URL parameters

| Parameter | Meaning |
|---|---|
| `service` | Active service (logging service ID). |
| `range` | Relative preset label (`1h`, `3h`, `6h`, `12h`, `24h`, `3d`, `7d`, `30d`). |
| `start_time`, `end_time` | Absolute window (ISO). |
| `filters` | JSON filter object keyed by column with `{mode, values}`; legacy `filter_<column>=value` is also read. |
| `metric` | Chart metric (see below). Consumed once and stripped from the URL by `useViewMetricUrlSync`. |
| `view` | Saved-view ID to apply, then stripped. |

`range`, `start_time`, `end_time` and `filters` are read once on load by `hydrateFilterStoreFromUrl` ([`frontend/lib/urlFilter.ts`](../../frontend/lib/urlFilter.ts)) and removed from the address bar. The default window is the last 24 hours; a service with less history uses its full extent (`isAutoRange`, see `useReportRangeAnchor`).

## Panels

- **Traffic over Time**: metric buttons come from the log-field catalog's `METRICS` group (`requests`, `5xx`, `4xx`, `hit_rate`, `throughput`, `req_size`, `ttfb`) plus a Latency menu (`p50_latency`, `p95_latency`, `p99_latency`). Interval buttons `1s / 1m / 1h / 1d`, trend-overlay buttons, a clickable series legend for stacked bars, and drag-to-zoom on the chart (sets a custom range). Series transforms run in a Web Worker above 2,000 points.
- **Map**: country choropleth (`map_data`); clicking a country adds a `country` filter.
- **Card grid**: Top-N cards in collapsible categories Request, Cache, Geography, Network & Connection, Edge Infrastructure, Security, Origin, QUIC / HTTP3, Image Optimization, plus Custom for user-defined fields (definitions in [`useDashboardCardDefinitions.ts`](../../frontend/hooks/useDashboardCardDefinitions.ts)). Cards for fields absent from the active log format show an `EyeOff` marker in the Cards popover. NGWAF cards (`_ngwaf_bot_name`, `waf_sig_ind`) are hidden unless `bootstrap.ngwaf_configured` is true.
- **Cards popover** (header): per-card checkboxes, Show all, Reset. Visibility is stored in `localStorage` key `dashboard_cards`; collapsed categories in `dashboard_collapsed_sections`. Both are per browser, so an analyst's layout is independent of the admin's.
- **Raw Request Log Inspector**: "See Raw Logs" navigates to `/query` with `service`, `start_time`, `end_time` and `filters`.

Filter bar (shared, from `ReportLayout`): time presets and custom range, filter pills with include/exclude, an "Edge only" switch, a Compare toggle with offset presets (`1w`, `4w`, `1y`; the default compare window is the preceding window of equal length), and saved views (select and save). A row click adds an include filter; a pill can be negated.

States: skeletons reserve fixed 300px boxes ("Crunching logs...", "Loading map…", "Loading..."); while refetching, panels dim (`opacity-40`) and an "Updating" badge appears. A stale-view error shows an amber "Preparing your data" notice and retries; other errors show a Retry banner.

## Roles

| Capability | Admin | Analyst (remote share) |
|---|---|---|
| Analytics, filters, drill-down | yes | yes, within the invite's services and time ceiling (`RequestContext.clamp`) |
| Client IP / `cookie_session` | raw | masked or redacted when the invite masks IPs (`mask_ip`, `[redacted]`) |
| Save / delete views | yes | no (`POST`/`DELETE` on `/api/views` return 403 `read_only`) |
| `/query` handoff | yes | yes (analyst SQL is validated and masked server-side) |

## Backend

Routes are in [`backend/routers/dashboard.py`](../../backend/routers/dashboard.py); models in [`backend/models/analytics/dashboard.py`](../../backend/models/analytics/dashboard.py).

- `POST /api/dashboard/bundle` (`BundleResponse`): one round trip for the page. The request is `AggregatesRequest` (`start_time`, `end_time`, `range_token`, `anchor`, `filters`, `chart_metric`, `chart_interval`, `fields`, `sections` from `core | topten | bots`, plus include flags). Response: `aggregates` (`data`: field -> `{top: [{value, count, label}], total}`, `time_series`, `map_data`, `where_clause`, `interval`, `metric`, `total_rows`, `total_rows_total`, optional `reader_coverage` / `history` observations) and `top_bots` (`bots`, `ngwaf_bots`). The page requests all three sections.
- `POST /api/dashboard/aggregates`: same request/response without bots; used only for the Compare window.
- `POST /api/dashboard/field-values`: value suggestions for the filter editor.
- `POST /api/dashboard/raw/csv`: streaming CSV (limit capped at 50,000 rows). Analyst exports mask IP columns and redact session IDs. No page component calls it today.
- Saved views use `GET /api/views/{service_id}`, `POST /api/views/`, `DELETE /api/views/{view_id}` ([`backend/routers/alerts.py`](../../backend/routers/alerts.py)); the log-field catalog comes from bootstrap.

The bundle runs `aggregates` on the request connection and `top_bots` on a second pooled connection when one is idle, otherwise sequentially. Standard mode serves unfiltered requests from rollups where coverage allows and falls back to scoped raw SQL; filtered requests materialize one connection-local temp table. In `high_scale` mode the request is dispatched to the ClickHouse reader ([`backend/high_scale/dashboard.py`](../../backend/high_scale/dashboard.py)); failures surface as errors, never as empty data. See [AGENTS.md](../../AGENTS.md) traps 47, 49, 52, 53 and 55 for window-boundary and cache-key rules.

## Tests

- Frontend unit: [`frontend/__tests__/app/dashboard.test.tsx`](../../frontend/__tests__/app/dashboard.test.tsx)
- E2E: `Dashboard Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts) (single bundle call, metric switching, compare query, click-to-filter, map, card/collapse persistence, NGWAF card gating)
- Backend: [`tests/routers/test_dashboard_router.py`](../../tests/routers/test_dashboard_router.py), [`tests/repositories/test_top_n_rollups_golden.py`](../../tests/repositories/test_top_n_rollups_golden.py), [`tests/repositories/test_rollup_reader_coverage_golden.py`](../../tests/repositories/test_rollup_reader_coverage_golden.py)
