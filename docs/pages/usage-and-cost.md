# Usage & Cost (`/usage`)

Object-storage and delivery usage for a service, with an interactive cost estimator. It shows FOS storage, Class A/B operations, CDN bandwidth, log and RUM activity, and projected cost.

Source: [`frontend/app/usage/`](../../frontend/app/usage/) (`page.tsx` re-exports `_sections/UsageClient.tsx`, `UsageStatCards.tsx`, `UsageChartsGrid.tsx`) and [`frontend/components/CostCalculator/`](../../frontend/components/CostCalculator/) (`CostCalculator.tsx`, `costCalculatorModel.ts`). Built on `ReportLayout`.

## Panels

- Stat cards: Storage Impact (period), Class A Ops (period), Class B Ops (period), Estimated Incurred Cost.
- Charts: FOS Operations, CDN Bandwidth, Estimated Cost, Log Activity (Processed), and Real User Monitoring (RUM) Operations.
- Cost Estimator: inputs for storage, Class A and Class B rates, retention, volumes and a delivery billing zone selector (`MBZ100` $0.08/GB, `MBZ200` $0.12/GB default, `Global` $0.18/GB; `costCalculatorModel.ts`). Calculations run in the browser from the prefilled values. Storage sizing follows the service's data and RUM retention (0 keeps forever, modelled as 365 days; deleted objects floor at the 30-day FOS minimum); for high-scale services the archive footprint is included.

## Roles

Admin only. `/api/usage/` is in the analyst blocklist (`_ANALYST_BLOCKED_PREFIXES`) and `AppLayout` redirects analysts away from `/usage`.

## Backend

All routes are in [`backend/routers/usage.py`](../../backend/routers/usage.py), prefix `/api/usage`. The range endpoints take `start`, `end` and `by` (`hour` or `day`).

| Endpoint | Purpose |
|---|---|
| `GET /api/usage/prefill/rates` | Configured rates and `billing_zone` for the stat cards without a data scan. |
| `GET /api/usage/prefill` | Estimator prefill: historical log volumes, retention (`data_retention_days`, `rum_retention_days`), `deployment_mode`, RUM beacon rates. |
| `GET /api/usage/current-storage` | Live FOS object count and bytes; adds the high-scale archive and recently expired artifacts, which still bill. |
| `GET /api/usage/operations` | Class A / Class B operations over time. |
| `GET /api/usage/bandwidth` | CDN bandwidth over time from Fastly stats. |
| `GET /api/usage/log-activity` | Processed log activity over time. |
| `GET /api/usage/rum-breakdown` | RUM operations breakdown. |

Operation counts come from the PostgreSQL `usage_log` and `usage_log_hourly_summary` tables (every FOS call is attributed by process context); Fastly `/stats/service` calls need the service's stored Fastly token. Rates and the billing zone are managed with `GET`/`PATCH /api/admin/usage-logging` ([`backend/routers/admin/admin_queries.py`](../../backend/routers/admin/admin_queries.py)); the per-operation ledger page is [admin/usage-log.md](admin/usage-log.md).

## Tests

- Frontend: [`frontend/__tests__/app/usage-sessions-streaming.test.tsx`](../../frontend/__tests__/app/usage-sessions-streaming.test.tsx)
- E2E: `Usage & Cost Estimator Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
