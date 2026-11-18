# Usage & Cost (`/usage`)

Object-storage and delivery usage for a service, with an interactive cost estimator. It shows FOS storage, Class A/B operations, CDN bandwidth, log and RUM activity, and projected cost.

Source: [`frontend/app/usage/`](../../frontend/app/usage/) (`page.tsx` re-exports `_sections/UsageClient.tsx`, `UsageStatCards.tsx`, `UsageChartsGrid.tsx`) and [`frontend/components/CostCalculator/`](../../frontend/components/CostCalculator/) (`CostCalculator.tsx`, `costCalculatorModel.ts`). Built on `ReportLayout`.

## Panels

- Stat cards: Storage Impact (period), Class A Ops (period), Class B Ops (period), Estimated Incurred Cost.
- Charts: FOS Operations, CDN Bandwidth, Estimated Cost, Log Activity (Processed), and Real User Monitoring (RUM) Operations. Cloud adds Compute Requests and Compute vCPU (below).

On Cloud, the Estimated Incurred Cost card's **Compute & vCPU** row and the **Compute Requests** and **Compute vCPU** charts come from one read of `/api/usage/compute` per range, which the card's Incurred Cost lines share. That route reads Fastly Platform Metrics (`/api/admin/usage/compute-billing`) for the app and worker Compute services. The row shows Fastly's requests (app plus worker), vCPU (`compute_execution_time_ms`) and billed time (`compute_request_time_billed_ms`), labelled "Fastly metrics". KV operations on that row are still logged. The charts stack the app service against the worker service: requests per period, and vCPU in seconds. They use hourly buckets, summed into UTC days at the 1d interval. The charts show only the hours Platform Metrics returned, so the latest hour can be partial and later hours are left empty rather than shown as zero. When Platform Metrics is unavailable, the row falls back to the logged figures from `/api/usage/current-storage`, labelled "logged", and the charts show their empty state. That happens when the read fails, on the shell runtime, or when the range is longer than 31 days. High-Scale has neither the row nor the charts.
- Cost Estimator: inputs for storage, Class A and Class B rates, retention, volumes and a delivery billing zone selector (`MBZ100` $0.08/GB, `MBZ200` $0.12/GB default, `Global` $0.18/GB; `costCalculatorModel.ts`). Calculations run in the browser from the prefilled values. Storage sizing follows the service's data and RUM retention (0 keeps forever, modelled as 365 days; deleted objects floor at the 30-day FOS minimum) and includes the batch archive footprint.

Cloud uses `CloudCostCalculator.tsx` and the canonical `lib/cloud-engine/usage/estimate.ts` model instead of the High-Scale archive model. It has three parts:

- **Your Traffic & Config:** editable total and edge requests per day, log sample rate, log file period, raw files per period, compressed bytes per line, Parquet bytes per row and log retention, each labelled Measured, Reference or What-if (after an edit); switches for edge-only logging and deleting raw logs after ingest; and RUM pageviews, sample rate and retention when RUM is enabled.
- **Contract Rates:** the configured rates, read-only, with list-price fallbacks. There is no billing zone selector.
- **Monthly Estimate Breakdown:** Compute requests and vCPU, KV operations, secrets, CDN requests and delivery, storage, and the model's summary/RUM/compaction/high-RPS lines, each with its monthly quantities under the label, and the total.

There is no capacity panel and no compaction toggle: compaction follows the logged service's setting from the prefill (off otherwise), and the ingest capacity model ([cost-methodology.md](../cloud/cost-methodology.md#ingest-capacity-task-253)) is not shown. The Cloud adapter supplies measured inputs, unit costs and configured rates through `/api/usage/prefill`; unavailable measurements remain labelled reference assumptions. Editing an input recalculates the estimate locally.

On Cloud, `/api/usage/current-storage` reports Deleted (Still Billed) as the `bytes_deleted` from `fos_usage` records in the last `min_billed_days` (30 by default) before now, whatever range is selected, matching High-Scale's 30-day window.

## Roles

Admin only. `/api/usage/` is in the analyst blocklist (`_ANALYST_BLOCKED_PREFIXES`) and `AppLayout` redirects analysts away from `/usage`.

## Backend

All routes are in [`backend/routers/usage.py`](../../backend/routers/usage.py), prefix `/api/usage`. The range endpoints take `start`, `end` and `by` (`hour` or `day`).

| Endpoint | Purpose |
|---|---|
| `GET /api/usage/prefill/rates` | Configured rates and `billing_zone` for the stat cards without a data scan. |
| `GET /api/usage/prefill` | Estimator prefill: historical log volumes, retention (`data_retention_days`, `rum_retention_days`), RUM beacon rates. |
| `GET /api/usage/current-storage` | Live FOS object count and bytes; adds the batch archive and recently expired artifacts, which still bill. |
| `GET /api/usage/operations` | Class A / Class B operations over time. |
| `GET /api/usage/bandwidth` | CDN bandwidth over time from Fastly stats. |
| `GET /api/usage/log-activity` | Processed log activity over time. |
| `GET /api/usage/rum-breakdown` | RUM operations breakdown. |
| `GET /api/usage/platform` | Observed Object Storage, Compute, and NGWAF usage from Fastly Platform Metrics. |
| `GET /api/usage/geography` | Delivery-service request and bandwidth totals grouped by region or datacenter. |

Operation reconciliation and usage charts use Fastly Platform Metrics with the service's stored token. Local operation attribution comes from the PostgreSQL `usage_log` and `usage_log_hourly_summary` tables, where every FOS call is attributed by process context. Rates and the billing zone are managed with `GET`/`PATCH /api/admin/usage-logging` ([`backend/routers/admin/admin_queries.py`](../../backend/routers/admin/admin_queries.py)); the per-operation ledger page is [admin/usage-log.md](admin/usage-log.md).

When Platform Metrics is available, its Object Storage byte and bucket gauges are the authoritative Fastly-observed source for the period storage cost and estimator prefill. The bytes are a gauge, so the UI uses the latest account-wide snapshot and integrates daily snapshots into GB-hours; it never sums gauge points as counters. Local FOS inventory remains visible as an install-scoped comparison and is used only when the Platform query is unavailable. The account-wide scope can make this measured total include storage outside the selected install.

Cloud also shows measured Compute requests, billed request time, backend/cache/resource totals for its app and worker services. When an NGWAF workspace is configured, the page shows its allowed, blocked, challenged, logged, timeout, and unknown request counters. These optional sections fail independently; unavailable metrics do not replace real zero values with modeled data or make the rest of the page fail.

The Traffic Geography card uses daily Platform Metrics counters for the same delivery service as the bandwidth chart. It defaults to region and fetches datacenter detail only after that toggle is selected. Group totals and shares are informational: they never feed the storage, operation, egress, Compute, or estimator cost totals.

High-Scale estimator traffic comes from the selected monitored logging service, not the separate archive-delivery CDN service. Missing Fastly edge-request counters remain unavailable; an explicit zero remains measured zero.

## Tests

- Frontend: [`frontend/__tests__/app/usage-sessions-streaming.test.tsx`](../../frontend/__tests__/app/usage-sessions-streaming.test.tsx)
- Calculator regressions: [`frontend/__tests__/components/CloudCostCalculator.test.tsx`](../../frontend/__tests__/components/CloudCostCalculator.test.tsx), [`frontend/lib/cloud-engine/usage/estimate.test.ts`](../../frontend/lib/cloud-engine/usage/estimate.test.ts), [`tests/routers/test_usage_prefill.py`](../../tests/routers/test_usage_prefill.py).
- E2E: `Usage & Cost Estimator Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
