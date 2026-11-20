# Shared Report Loading Design

## Goal

Give all report pages one consistent, accessible loading experience. A report must
show that data is loading from the moment its prerequisite service state is not ready
until the initial query completes, rather than briefly showing cards with an empty
state and no visible explanation.

## Problem

`useServiceQuery` disables data queries until the active service and field metadata
are ready. TanStack Query reports a disabled, pending query as not actively loading,
because `isLoading` only means pending and fetching. Some pages compensate with
local checks such as `!data && !error`; Streaming does not. Its cards can therefore
look loaded-but-empty while Cloud CMCD files or summaries are still being read.

`AnalyticsCard` already owns the visual loading overlay and accessible status
announcement, but most cards rely on its generic `Loading data...` fallback.
Neighboring pages that supply contextual messages feel clearer and more intentional.

## Design

### Shared query readiness

`useServiceQuery` will continue to prevent network work until its prerequisites are
ready. It will additionally expose that prerequisite wait as `isLoading` when the
query is otherwise enabled and has no data.

This state is:

- loading while app/service data is not ready and the caller did not explicitly
  disable the query
- loading while TanStack is performing the initial fetch
- not loading when the caller explicitly disables the query
- not loading during ordinary background refreshes when prior data is retained

The hook must preserve TanStack Query's other properties and existing
`placeholderData` behavior.

### Shared card announcement

`AnalyticsCard` will keep the same overlay, spinner, status role, and precedence:
loading, error, empty, then content. Its default copy will become
`Loading ${title}...`. Explicit `loadingMessage` values remain authoritative so
pages can use more precise copy where they already do.

This makes Streaming's cards announce messages such as `Loading Active Viewers...`
and `Loading Buffer Health...` without changing its chart layout or adding a
one-off banner. Stat-card skeleton behavior remains unchanged because those cards
already show that data is loading.

### Scope

This is a shared behavior correction for analytics queries and cards. It does not:

- add polling or retries
- change CMCD rollup/raw fallback decisions
- move Cloud analytics out of the browser
- bypass the identity/service-scoped browser file cache on reload
- alter server-side seeds
- change filter, cancellation, or caching behavior
- require page-specific copy for every card

For Cloud reads, cached Parquet or summary bytes remain available through the shared
browser `CacheStorage` source after a page reload. Summary decoding and aggregation
continue in the dashboard Web Worker, while raw fallback analytics continue in the
browser's DuckDB-WASM read context.

## Error and refresh behavior

Initial prerequisite waits and initial fetches use the loading overlay. Background
refreshes continue to use the existing updating badge because retained data should
remain visible. Errors continue to replace loading with the current error state;
the shared query state must not turn an errored disabled query into an endless
spinner.

## Tests

Add focused coverage for:

1. A service query that is waiting for data readiness reports `isLoading` while no
   data exists.
2. An explicitly disabled service query does not report `isLoading`.
3. An `AnalyticsCard` with no `loadingMessage` announces the contextual title-based
   message.
4. A card with an explicit `loadingMessage` keeps that custom message.
5. Streaming passes its initial query state through the existing cards, including
   the prerequisite-wait state.

Run the focused hook/card tests, Streaming tests, frontend typecheck, and frontend
lint on changed files. Use the existing full CI workflows as the integration gate.

## Success criteria

- Streaming immediately communicates that data is loading, even before the CMCD
  request begins.
- All `AnalyticsCard` users receive the same accessible contextual default unless
  they already define a more precise message.
- Reloading a Cloud page still reuses its identity/service-scoped files, and its
  analytics still execute in the browser.
- No visible loading regression appears during background refreshes or when a query
  is intentionally disabled.
- The implementation requires no page-by-page audit patches beyond a regression
  test for Streaming.
