# Usage & Cost (`/usage`)

Object-storage and delivery usage for a service, with an interactive cost estimator. It shows FOS storage, Class A/B operations, CDN bandwidth, log and RUM activity, and projected cost.

Source: [`frontend/app/usage/`](../../frontend/app/usage/) (`page.tsx` re-exports `_sections/UsageClient.tsx`, `UsageStatCards.tsx`, `UsageChartsGrid.tsx`) and [`frontend/components/CostCalculator/`](../../frontend/components/CostCalculator/) (`CostCalculator.tsx`, `costCalculatorModel.ts`). Built on `ReportLayout`.

## Panels

- Stat cards: Storage Impact (period), Class A Ops (period), Class B Ops (period), Estimated Incurred Cost. On Cloud the cost card lists every line of `incurred` from `/api/usage/current-storage` (Object Storage, storage operations, CDN delivery, with its egress and request costs shown under its total, Compute requests and vCPU, KV operations, Secret Store, Config Store) at the service's configured rates, and its total is their sum. CDN delivery includes the front service's billed requests and bytes, clock probes among them; a line whose quantity is unavailable shows n/a and marks the total partial. High-Scale's card still totals storage, operations and egress.
- Charts: FOS Operations, CDN Bandwidth, Estimated Cost, Log Activity (Processed), and Real User Monitoring (RUM) Operations. Cloud adds Compute Requests and Compute vCPU (below).

On Cloud, the Estimated Incurred Cost card's **Compute & vCPU** row and the **Compute Requests** and **Compute vCPU** charts come from one read of `/api/usage/compute` per range, which the card's Incurred Cost lines share. That route reads Fastly Platform Metrics (`/api/admin/usage/compute-billing`) for the app and worker Compute services. The row shows Fastly's requests (app plus worker), vCPU (`compute_execution_time_ms`) and billed time (`compute_request_time_billed_ms`), labelled "Fastly metrics". KV operations on that row are still logged. The charts stack the app service against the worker service: requests per period, and vCPU in seconds. They use hourly buckets, summed into UTC days at the 1d interval. The charts show only the hours Platform Metrics returned, so the latest hour can be partial and later hours are left empty rather than shown as zero. When Platform Metrics is unavailable, the row falls back to the logged figures from `/api/usage/current-storage`, labelled "logged", and the charts show their empty state. That happens when the read fails, on the shell runtime, or when the range is longer than 31 days. High-Scale has neither the row nor the charts.
- Cost Estimator: inputs for storage, Class A and Class B rates, retention, volumes and a delivery billing zone selector (`MBZ100` $0.08/GB, `MBZ200` $0.12/GB default, `Global` $0.18/GB; `costCalculatorModel.ts`). Calculations run in the browser from the prefilled values. On Cloud the delivery line also models the clock probes: about 53 POPs probing `/_tick` and `/_flush` at half the shortest log period, clamped to 1–30 s (every 30 s on Cloud, about 305K requests a day), so the probe cost is flat for any log period of a minute or more. Storage sizing follows the service's data and RUM retention (0 keeps forever, modelled as 365 days; deleted objects floor at the 30-day FOS minimum) and includes the batch archive footprint.

Cloud uses `CloudCostCalculator.tsx` and the canonical `lib/cloud-engine/usage/estimate.ts` model instead of the High-Scale archive model. It has three parts:

- **Your Traffic & Config:** editable total and edge requests per day, log sample rate, log file period, raw files per period, compressed bytes per line, Parquet bytes per row and log retention, each labelled Measured, Reference or What-if (after an edit); switches for edge-only logging and deleting raw logs after ingest; and RUM pageviews, sample rate and retention when RUM is enabled.
- **Contract Rates:** the configured rates, read-only, with list-price fallbacks. There is no billing zone selector.
- **Monthly Estimate Breakdown:** Compute requests and vCPU, KV operations, secrets, CDN requests and delivery, storage, and the model's summary/RUM/compaction/high-RPS lines, each with its monthly quantities under the label, and the total.

There is no capacity panel and no compaction toggle: compaction follows the logged service's setting from the prefill (off otherwise), and the ingest capacity model ([cost-methodology.md](../cloud/cost-methodology.md#ingest-capacity-task-253)) is not shown. The Cloud adapter supplies measured inputs, unit costs and configured rates through `/api/usage/prefill`; unavailable measurements remain labelled reference assumptions. Editing an input recalculates the estimate locally.

On both Cloud and High-Scale, `/api/usage/current-storage` computes selected-period storage GB-hours from service-local lifecycle intervals (`StorageBillingInterval`): every live owned object plus every locally recorded deletion still inside its minimum billing window (30 days by default), integrated over the selected `[start, end)` range. Cloud reads the latest complete background `ops:storage_inventory` snapshot (never performing an on-demand full-bucket LIST on page load) together with `ops:ingest_files` and `fos_usage` deletion records; High-Scale reads Postgres `high_scale_source_objects`, `high_scale_archive_manifests`, and local cloud-copy deletion records. Both editions share the same response semantics (`total_billed_gb_hours`, `live_billed_gb_hours`, `deleted_billed_gb_hours`, `minimum_billed_gb_hours`, `storage_billing_complete`, `storage_billing_provenance`) and frontend calculator. Incomplete lifecycle coverage is marked `storage_billing_complete: false` and shown as partial, never fabricated as an exact zero.

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

Incurred Class A/B operations in `/api/usage/operations` use the service-local usage log in both editions (`usage_log_hourly_summary` plus boundary `usage_log` rows on High-Scale; `fos_usage` on Cloud) and expose `source`, `complete`, and `provenance`. Unavailable or incomplete operation telemetry is shown as `Unavailable` and excluded from the incurred cost total rather than rendered as `$0.00`. The monthly Cost Estimator remains a modeled projection and may legitimately differ from selected-period incurred cost. Rates and the billing zone are managed with `GET`/`PATCH /api/admin/usage-logging` ([`backend/routers/admin/admin_queries.py`](../../backend/routers/admin/admin_queries.py)); the per-operation ledger page is [admin/usage-log.md](admin/usage-log.md).

Service-local selected-period storage (`total_billed_gb_hours`, labeled `Measured · this install`) is authoritative for Storage Impact, Estimated Incurred Cost, and both Cost Estimators. Fastly Platform Object Storage metrics remain visible on the separately labeled `Account-wide · Fastly Platform` informational card and never override local storage costs.

The Cost Estimators turn that measurement into a month at the range's average size: billed GB-hours divided by the range's hours is the average stored GB, shown as that many GB-months (for example 24 GB-hours over a 1-day range is 1 GB-month). Dividing by 720 instead would price only the hours in the range.

Cloud's measured Compute figures appear only in the cost card's Compute row and the two Compute charts; the top Platform and Traffic Geography cards were removed. Unavailable metrics do not replace real zero values with modeled data or make the rest of the page fail.

High-Scale estimator traffic comes from the selected monitored logging service, not the separate archive-delivery CDN service. Missing Fastly edge-request counters remain unavailable; an explicit zero remains measured zero.

## Tests

- Frontend: [`frontend/__tests__/app/usage-sessions-streaming.test.tsx`](../../frontend/__tests__/app/usage-sessions-streaming.test.tsx)
- Calculator regressions: [`frontend/__tests__/components/CloudCostCalculator.test.tsx`](../../frontend/__tests__/components/CloudCostCalculator.test.tsx), [`frontend/lib/cloud-engine/usage/estimate.test.ts`](../../frontend/lib/cloud-engine/usage/estimate.test.ts), [`tests/routers/test_usage_prefill.py`](../../tests/routers/test_usage_prefill.py).
- E2E: `Usage & Cost Estimator Page Contract` in [`frontend/e2e/pages/dashboard.spec.ts`](../../frontend/e2e/pages/dashboard.spec.ts)
