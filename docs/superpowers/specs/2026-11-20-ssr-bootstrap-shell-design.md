# SSR Bootstrap Shell and Page-Seed Audit

## Goal

Render feature-gated navigation and shell controls from the request's canonical bootstrap
snapshot on the first paint. RUM, CMCD/Streaming, service selection, analyst state, and
resolved log-field capability must not appear several render frames after the rest of the
navigation.

The app should also audit report pages for safe server-seeded default data without moving
Cloud DuckDB/WASM processing, arbitrary filters, live streams, searches, or mutations out of
the browser.

## Architecture

`fetchBootstrapServerSide()` remains the single source for the request-scoped bootstrap
payload. The root layout hydrates that payload into React Query and derives explicit shell
gates from it:

- active service ID and service list
- RUM enabled
- CMCD enabled
- analyst and share-analyst display/authorization state
- resolved active log-field IDs
- system settings and capability flags consumed by the shell

`AppLayout` consumes those gates synchronously from the hydrated snapshot, with SSR-derived
props used for the first render. Client bootstrap refreshes remain a fallback and update the
same query key after the initial render. Browser-only store reconciliation, service URL
writeback, local cache identity, live status, and mutation effects remain client-side.

Capability checks must come from saved service configuration:

- High-Scale: `rum_enabled` / `cmcd_enabled` in service bootstrap data
- Cloud: normalized `service.cmcd.enabled` and normalized service RUM config
- Do not infer shell visibility from whether a field happened to appear in a historical
  Parquet file

## Page audit

For each report page, classify its initial requests:

1. **Safe SSR seed:** request-scoped, stable, cacheable configuration or the default,
   unfiltered aggregate already represented by a server helper and client query key.
2. **Client-only:** browser DuckDB/WASM, file-range planning, filters, searches, live
   events, streams, polling, mutations, or data that requires browser-local cache state.
3. **Existing SSR seed:** no change, but verify the server and client query keys match.

Candidate SSR work is limited to existing bootstrap-dependent shell data and existing
upstream helpers for default analytics. Pages with worker-backed or range-dependent Cloud
reads must not receive speculative SSR work unless the exact initial query key and request
scope can be proven.

## Cache and invalidation

The Cloud metadata cache remains short-lived:

- 15-second sliding reuse
- 60-second hard maximum
- keyed by authorized cache identity
- cleared on local mutations through the central analytical request adapter

Server-rendered seeds are request-scoped and must not be shared across users, analyst
sessions, services, or hosts. No credentials or privileged upstream headers are serialized.
Config mutations invalidate the corresponding React Query/bootstrap cache; the shell updates
from the refreshed canonical service config.

## Error handling

If SSR bootstrap fails, render a stable shell from server-known values without assuming
capabilities are disabled. The existing client bootstrap request remains the fallback. A
missing or failed capability check must not reveal an authenticated route or privileged
control; UI visibility is not authorization.

## Tests

- AppLayout test: RUM and Streaming are present in the first render when SSR bootstrap says
  enabled, with no later client update required.
- AppLayout test: feature links remain absent when the canonical config says disabled.
- Cloud/Elevation parity test for the same canonical service capability shape.
- Mutation test: changing CMCD/RUM config invalidates bootstrap metadata and updates nav.
- Focused SSR tests for each newly seeded page verify exact client/server query-key parity.
- Existing Cloud browser, security, type, and lint suites remain green.

## Scope exclusions

- No arbitrary URL/filter/server computation to SSR.
- No Cloud Parquet/WASM work moved server-side.
- No polling, SSE, live status, or mutation flow moved server-side.
- No new auth model; bootstrap authorization remains the existing source.
