# Historical DDoS Usage

## Goal

Add a delivery-service DDoS history chart to Usage & Cost without duplicating the Control Room's Real-Time incident status or changing any billing calculation.

## User experience

Extend Observed Fastly Platform with a fourth card and a compact historical trend:

- Detected requests
- Mitigated requests
- Allowed requests
- Dominant actions: blackhole, close, limit streams, downgrade, tarpit

The card shows the selected period total. The chart below it is daily for long ranges and hourly for ranges up to 48 hours. Labels say "Platform Metrics · delivery service".

## Data source

Query the same delivery service used by Bandwidth and Geography:

- High-Scale: `cdn_service_id`
- Cloud: front VCL service

Use the `ddos` metric set. The implementation may request explicit counters to keep the payload small:

- `ddos_protection_requests_allow_count`
- `ddos_protection_requests_detect_count`
- `ddos_protection_requests_mitigate_count`
- `ddos_action_blackhole`
- `ddos_action_close`
- `ddos_action_downgrade`
- `ddos_action_limit_streams_connections`
- `ddos_action_limit_streams_requests`
- `ddos_action_tarpit`
- `ddos_action_tarpit_accept`

These are counters. A successful 200 response with no rows is measured zero because Platform Metrics omits zero-valued sparse keys/rows. An HTTP, parser, pagination, or auth failure remains unavailable and is not rendered as zero.

## Contract

Extend `PlatformUsageResponse`:

```text
ddos:
  status: ok | not_configured | unavailable
  scope: delivery_service | null
  detected: integer
  mitigated: integer
  allowed: integer
  actions: {
    blackhole, close, downgrade,
    limit_streams_connections, limit_streams_requests,
    tarpit, tarpit_accept
  }
  data:
    - time
    - detected
    - mitigated
    - allowed
```

No service ID, token, raw body, or cursor is returned. The response still uses the existing overall `fastly | partial | unavailable` source classification.

## Semantics

- Detected and mitigated are request counters and may exceed one another for overlapping categories only if Fastly reports them that way.
- Action counters count decisions/connections, not requests; display them as actions, never sum them into detected/mitigated request totals.
- `not_configured` is not treated as degradation.
- Missing delivery service is `not_configured`.
- The chart never feeds cost, FOS, Compute, or NGWAF calculations.

## Architecture

### High-Scale

Extend `/api/usage/platform` to query the delivery entity independently. Storage, Compute (`not_configured`), NGWAF, and DDoS each retain isolated failures.

### Cloud

Extend `/api/admin/usage/platform` to query the front VCL service for DDoS counters independently. Keep the existing app/worker Compute batching.

### Frontend

Extend `PlatformUsageCards` with a DDoS card and add a compact Plotly area/bar chart for the returned daily/hourly points. Use the existing Platform scope styles. The Control Room remains the Real-Time incident/status view.

## Testing

- Successful no-row response is `ok` with zero totals/empty chart.
- Explicit zero values stay measured zero.
- Sparse positive rows sum correctly.
- Action counters stay separate from request counters.
- Missing delivery service is not configured.
- HTTP/parser/cursor failures are unavailable, not zero.
- No service ID appears in normal/debug HTTP responses.
- Existing Platform storage/Compute/NGWAF responses and costs remain unchanged.

## Non-goals

- Replacing the Control Room Real-Time DDoS panel.
- Inferring mitigation cost savings.
- Mapping attack severity to billing.
- Account-wide DDoS aggregation.
