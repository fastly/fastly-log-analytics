# Cloud Dashboard XHR Design

## Goal

Reduce Cloud dashboard startup and background request waterfalls without moving
range-specific analytics away from the browser. One authenticated bootstrap
request should establish the shell, service metadata, capability data, catalog,
saved views, and build identity. Background catalog-change detection should use
the catalog event endpoint rather than scanning operational Parquet files.

## Request ownership

The Cloud frontend is a static export, so its "SSR" bootstrap cannot call Compute
at build time. Today the browser asks for `/api/bootstrap`, the client-side Cloud
adapter synthesizes it with `/api/auth/session` and `/api/services`, and hooks
then issue additional metadata requests. This saves no network request because
the synthesized response is assembled in the browser.

Analytics remain intentionally client-side:

- `/api/files` discovers raw and summary files for the current service, range,
  dataset, and filter path.
- `/api/file` serves immutable Parquet bytes.
- DuckDB-WASM computes raw fallbacks in the browser.
- The dashboard worker decodes and merges Cloud summaries.
- The persistent browser file cache reuses Parquet bytes after reload.

Moving those endpoints into the shell bootstrap would either precompute a
window that may not match the user's eventual range or require serializing a
large, private manifest into page state. Neither is the default best practice.

## Native bootstrap

Cloud Compute will own `GET /api/bootstrap`.

The endpoint must return the existing `BootstrapResponse` shape needed by the
frontend, including:

- `user` and debug visibility flags
- `services` with the existing role-specific redaction
- `active_service_id`
- `active_log_field_ids`
- `log_fields_catalog`
- `views` for the active service
- `compute_service_id` and `compute_service_version`
- `build_sha`
- capability flags (`runtime_mode`, `ngwaf_configured`, `fastly_bots_available`,
  remote analyst state, and mask policy)

The active service comes from `service_id` in the request query or
`x-service-id`, then falls back to the existing active/first service rule.
Signed-out or invalid-session requests return the same anonymous bootstrap used
today and clear stale session/CDN cookies where applicable.

The response is private and request-identity scoped. It must never be shared
across users, analyst roles, services, or hosts. Existing service redaction is
mandatory for non-admin users. A failed mandatory metadata read produces an
explicit non-2xx response; the frontend must not turn a partial or malformed
bootstrap into a success-shaped object.

The frontend adapter will treat `/api/bootstrap` as a native Cloud route instead
of synthesizing it. `seedBootstrapDependentCaches` will continue to seed the
services, catalog, and views query keys, so their consuming hooks do not issue
follow-up calls for already-hydrated data. `CloudVersionFooter` will read
`build_sha` from the bootstrap cache and stop issuing `/version`.

`/api/sync-status` and the last `log_discovery` cron run remain separate. Their
payloads are refreshed independently every five minutes and should not delay the
critical shell bootstrap.

## Catalog change events

Cloud Compute already exposes `GET /api/ingest/events`, which reads the catalog
head and emits SSE frames with:

- `event: ready` for the initial head
- `event: commit` when a retained generation changed
- `event: resync` when the browser cursor fell behind retained history
- `id: <service-id>:<generation>`
- `retry: <milliseconds>`

`useHeaderBadgeStream` will use this endpoint for Cloud instead of
`readIngestEvents`'s 15-second browser scan of:

- `/api/logs/cron_runs/live`
- `/api/files?dataset=ops:cron_runs`
- the corresponding operational Parquet files

The hook will remember the last event ID, retain it in the reconnect query, and
honor the server retry interval. On `ready`, it updates the local cursor without
invalidating page queries. On `commit`, it calls the existing
`observeCatalogCommit` path and invalidates sync status. On `resync`, it clears
the manifest cache and triggers a fresh service catalog observation.

The stream remains best-effort: visibility-hidden tabs pause work, aborting the
page cancels the request, and transient transport failures reconnect with the
existing bounded retry behavior. Authentication failures stop reconnecting and
surface the existing signed-out/session path; operational failures do not
interrupt analytics already loaded in the browser.

## Static and API delivery

This change does not combine hashed JavaScript or CSS chunks. Next.js chunking
preserves route-level loading and immutable-cache reuse; Brotli and one-year
static caching are already active. Production readiness remains a separate
audit for static asset bundle budgets, response headers, HTML cache isolation,
and effective Fastly policies.

## Tests

Add focused backend tests for:

1. signed-out native bootstrap
2. admin native bootstrap
3. analyst service redaction and role-scoped views
4. active service selection by query/header
5. catalog and view seed shapes
6. build SHA in bootstrap
7. stale-session cookie clearing

Add focused frontend tests for:

1. Cloud `/api/bootstrap` being forwarded once as a native route
2. no browser synthesis calls to `/api/auth/session` or `/api/services`
3. dependent service/catalog/views caches being seeded
4. footer rendering from bootstrap without `/version`
5. initial `ready` ingest event not invalidating caches
6. `commit` and `resync` events updating the cursor and invalidating correctly
7. visibility pause and abort cleanup

Preserve and rerun the existing metadata-cache identity tests, Cloud endpoint
contract tests, ingest event tests, browser-source cache tests, and CMCD raw/
summary fallback tests.

## Success criteria

- Cloud startup makes one `/api/bootstrap` request instead of the browser-side
  auth/session/services/catalog/views/version waterfall.
- Dashboard background updates stop reading cron operational Parquet files merely
  to discover catalog commits.
- Sync status and data-range analytics preserve their current freshness and
  authorization behavior.
- Raw and summary analytics continue to use the identity-scoped browser cache
  and execute in the browser.
- Static chunk splitting and compression remain unchanged.
