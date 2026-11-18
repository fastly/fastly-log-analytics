# Usage Geographic Attribution

## Goal

Add regional and datacenter request/bandwidth attribution to the Usage & Cost page using Platform Metrics `group_by`, without changing any billing calculation.

## User experience

Add a "Fastly Traffic Geography" card below Observed Fastly Platform usage. It includes:

- a Region / Datacenter segmented control,
- a horizontal Plotly bar chart for requests or bandwidth,
- the top ten groups with request, bandwidth, and share columns,
- a total line matching the ungrouped delivery-service totals.

Region is the default. Datacenter is queried only when selected. Rows are sorted by the selected measure. The card is informational and explicitly labeled "Delivery service · Platform Metrics"; it is never included in the cost model.

## Data source

Use the delivery service that already supplies the Usage page's CDN bandwidth:

- High-Scale: `cdn_service_id`, falling back to `logging_service_id`.
- Cloud: the configured front VCL service.

Query one entity endpoint at daily granularity with:

- `metric=requests,bandwidth`
- `group_by=region` or `group_by=datacenter`
- RFC 3339 `from`/`to`
- cursor pagination.

Sum only counter rows across days. Reject duplicate `(timestamp, group)` rows, missing group names, out-of-window timestamps, and malformed values. The API returns no rows if the service or location data is unavailable; the card reports unavailable rather than zero.

## Contract

Expose `GET /api/usage/geography?start&end&group_by=region|datacenter`:

```text
PlatformGeographyResponse
  source: fastly | unavailable
  scope: delivery_service
  group_by: region | datacenter
  requests: integer
  bandwidth_bytes: integer
  data:
    - location
    - requests
    - bandwidth_bytes
    - request_share
    - bandwidth_share
```

Service IDs are never returned. Tracked debug calls must not leak the delivery entity path; use the same safe response strategy as `/api/usage/platform`.

## High-Scale

Add the endpoint to `backend/routers/usage.py` and use the shared `fastly_platform_metrics` helper with grouped rows. Preserve the row-level `region` or `datacenter` string while the helper normalizes `timestamp` to `start_time`.

## Cloud

Add `GET /api/admin/usage/geography` to the Compute admin runtime. Reuse the Platform Metrics entity helper and front service ID. Validate group selection before making a request. The browser adapter exposes the response at `/api/usage/geography`.

## Frontend

Reuse TanStack Query, the existing Plotly chart wrapper, `AnalyticsCard`, and shadcn buttons. Region and datacenter are client state; switching tabs creates a separate query key. Use no map dependency despite existing MapLibre support because Platform Metrics does not include coordinates and a bar/table preserves exact values and accessibility.

## Failure behavior

- Missing token or delivery service: unavailable card.
- `group_by` unsupported by the backend: 400.
- Platform authorization/rate-limit/parser/pagination failure: unavailable card, no partial rows.
- One location with zero counters remains visible only when the API returned that row; no invented zero for absent groups.
- A region-level total may differ from account billing due to service scope; label and tooltip state this.

## Non-goals

- Changing cost totals or rates.
- POP coordinate maps.
- Account-wide location aggregation.
- DDoS/NGWAF or Compute geographic metrics.
- Real-Time geographic data.

## Testing

- Python and Rust tests for group validation, sparse values, duplicate group/timestamp rows, cursor paging, total/share aggregation, out-of-window rows, and no ID leakage.
- Adapter contract test for pass-through and query-string preservation.
- Frontend tests for region default, datacenter toggle query, zero/empty/unavailable states, share totals, chart/table accessibility, and no cost callback.
