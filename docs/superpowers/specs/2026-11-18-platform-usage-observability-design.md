# Observed Fastly Platform Usage

## Goal

Add an "Observed Fastly Platform" section to the Usage & Cost page and use returned real usage as the primary measured source wherever the metric applies. Existing local observations and modeled values remain fallback only when Fastly Platform Metrics is unavailable.

The first slice covers:

- account-level Object Storage capacity gauges,
- Compute billing and reliability totals for Cloud,
- configured NGWAF workspace outcome totals.

POP/region attribution, DDoS actions, and additional product metric sets remain follow-up work.

## User experience

Place three compact cards below the existing stat cards and above the current charts:

1. **Fastly Object Storage**
   - Latest `object_storage_bytes_count`
   - Latest `object_storage_buckets_count`
   - Label: "Account-wide snapshot"
   - Included in the page's observed storage period cost and monthly storage prefill when available.
   - Existing local FOS inventory remains visible as an install-scoped comparison and is used only when Platform data is unavailable.
2. **Cloud Compute**
   - Requests, billed request time, backend errors, sandbox count, and limit/memory/runtime errors for the selected range.
   - Cloud only; absent on High-Scale.
   - Label: "App + worker services"
3. **Next-Gen WAF**
   - Total, allowed, blocked, challenged, logged, timeout, and unknown requests for the selected range.
   - Shown only when `ngwaf_workspace_id` is configured.
   - Label: "Configured workspace"

Each card has independent loading, empty, unavailable, and error states. One unsupported or unauthorized metric cannot hide the other cards or fail the page. Missing optional products use "Not configured" or "Unavailable", not zero.

## Data semantics

### Object Storage gauges

`object_storage_bytes_count` and `object_storage_buckets_count` are gauges. Query them at daily granularity, ordered by timestamp. For each metric, use the newest value in the requested range. Never sum them and never include them in the existing period or monthly cost estimate.

These are account-wide metrics, so the card must always state that scope. The returned daily gauge series is not a counter: each point is a maximum observed snapshot for its day. Use the series to form the best available GB-hour coverage for the selected period; retain the latest point as the current snapshot. Platform storage drives the displayed measured storage cost whenever the query succeeds. Local inventory is not allowed to silently replace successful Platform data.

### Compute counters

Cloud already obtains Compute usage from one Platform Metrics request per owned service for the selected range. Extend that result to include:

- `compute_requests`
- `compute_execution_time_ms`
- `compute_request_time_billed_ms`
- `compute_bereqs`
- `compute_cache_operations_count`
- `compute_sandboxes`
- `compute_service_bereq_error`
- `compute_service_limits_error`
- `compute_service_memory_exceeded_error`
- `compute_service_runtime_error`
- `compute_service_timeout_error`
- `compute_service_vcpu_exceeded_error`

Sum counters across complete and overlapping buckets in the selected range. Keep the existing per-service request and vCPU chart behavior. The new reliability totals are app + worker totals only.

### NGWAF counters

Query the configured workspace entity with:

- `ngwaf_requests_total_count`
- `ngwaf_requests_allowed_count`
- `ngwaf_requests_blocked_count`
- `ngwaf_requests_challenged_count`
- `ngwaf_requests_logged_count`
- `ngwaf_requests_timeout_count`
- `ngwaf_requests_unknown_count`

Sum those counters over the selected range. Account and workspace rule gauges are excluded from this slice because they are configuration snapshots, not request usage.

## Architecture

### Shared backend contract

Add a backend response representing three independent optional sections:

```text
PlatformUsageResponse
  source: "fastly" | "partial" | "unavailable"
  object_storage:
    status: "ok" | "not_configured" | "unavailable"
    observed_at: string | null
    bytes: integer | null
    buckets: integer | null
  compute:
    status: "ok" | "not_configured" | "unavailable"
    requests: integer
    billed_time_ms: number
    backend_errors: integer
    cache_operations: integer
    sandboxes: integer
    resource_errors: integer
  ngwaf:
    status: "ok" | "not_configured" | "unavailable"
    total: integer
    allowed: integer
    blocked: integer
    challenged: integer
    logged: integer
    timeout: integer
    unknown: integer
```

The response never includes tokens, workspace IDs, service IDs, raw API bodies, or pagination cursors.

### High-Scale

Add one read-only usage endpoint in `backend/routers/usage.py`. It resolves the active service configuration, calls the aggregate Platform Metrics endpoint for Object Storage, and calls the configured NGWAF workspace when present. It omits Compute because this install has no owned Compute services.

Each source is queried independently. A failed source produces an `unavailable` section and a sanitized reason code; other sections still return.

### Cloud

Add a `GET /api/admin/usage/platform` route to the Compute admin surface. The runtime:

- queries Object Storage gauges from `/metrics/v2/platform/aggregate/daily`,
- queries NGWAF metrics from the workspace entity when `ngwaf_workspace_id` is configured,
- reuses the existing batched app/worker Compute request path for the selected range.

The browser adapter exposes this as `/api/usage/platform`, matching the High-Scale frontend call. The Cloud implementation must preserve the Compute admin call budget and return partial data if only one optional product is unavailable.

### Frontend

Add a `useQuery` for `/api/usage/platform` in the Usage page. Render one reusable card component with:

- card title and scope label,
- primary value,
- compact supporting totals,
- tooltip/help text for account-wide gauges and NGWAF scope,
- independent loading/error/unavailable presentation.

When Platform Object Storage data is available:

- use its GB-hours for the Usage card's storage cost,
- expose the latest account bytes to the estimator as measured storage,
- mark local FOS inventory values as an install-scoped comparison.

Use local values as fallback only when Platform data is unavailable. The estimator retains the explicit account-wide scope note and permits user what-if edits.

## Failure and compatibility behavior

- No Fastly token: Object Storage and NGWAF are unavailable; the page remains functional.
- No NGWAF workspace: the NGWAF card is "Not configured".
- No Compute ownership/configuration: the Compute card is absent on High-Scale.
- A Platform Metrics authorization, timeout, malformed timestamp, or pagination error affects only its source.
- Sparse counters in a returned interval mean zero.
- Storage gauges are selected as snapshots and never aggregated as counters.
- Existing `/api/usage/operations`, `/api/usage/bandwidth`, `/api/usage/compute`, cost estimates, and Real-Time Stats remain unchanged.

## Testing

- Python helper tests for daily gauge selection, independent section failure, account/workspace scope, NGWAF sparse counters, and no secret leakage.
- Rust tests for one batched Cloud Compute call with expanded metric set, optional Object Storage/NGWAF calls, partial failures, cursor validation, and bounded admin calls.
- Frontend component tests for all three scopes, loading/error/not-configured states, and proof that none of the observed values enter cost totals.
- Focused API/adapter contract tests for both deployment modes.

## Non-goals

- Changing the cost model or invoice estimate.
- Attributing account-level Object Storage bytes to a single install.
- Querying or pricing NGWAF rule configuration.
- Adding POP/region charts, DDoS actions, KV store inventory, or historical migration work.
- Replacing existing Real-Time charts.

This slice still does not invent per-install attribution for account-wide gauges. If a user has multiple installs or unrelated buckets in the account, the UI's source label explains why the authoritative Platform cost can exceed the selected install's local inventory.
