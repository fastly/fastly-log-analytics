# Cloud Manifest Startup

## Goal

When a signed-in browser reloads a Cloud dashboard with a valid persisted manifest, dashboard panels start reading Parquet without waiting for another complete `/api/files` listing.

The browser may reuse only a complete, generation-scoped manifest for the same browser cache scope, service, signed-in identity, dataset, and view. Its saved time range becomes the incremental base, not a requirement that a relative request has identical timestamps. The open tail remains fresh: a reused snapshot is advanced through the existing incremental catalog delta before new work depends on it.

## Current behavior

Cloud dashboards already:

- coalesce concurrent in-flight manifest requests;
- reuse a same-range snapshot for two seconds in memory;
- keep generation-scoped complete snapshots in memory for incremental requests;
- cache closed raw-request hour segments for at most one hour; and
- persist Parquet files and derived rollups in CacheStorage.

The first manifest request still pauses after a hard reload because both snapshot maps and the two-second reuse map are process-local. Each panel must wait for a fresh complete `/api/files` enumeration before it can prove that its Parquet coverage is complete.

## Chosen design

Persist complete raw-request manifests in a dedicated CacheStorage cache. Hydrate an eligible record into the existing incremental manifest machinery before the first panel scan.

The persisted record is an optimization, not an authority. A missing, stale, malformed, oversized, or unavailable record falls back to the current network snapshot path.

### Cache record

One current snapshot is stored per:

- the existing `manifestCacheScope()`;
- the normalized dataset and view; and
- a versioned cache format tag.

The stored JSON contains:

- `formatVersion`;
- `scopeKey`;
- `from`;
- `to`;
- `catalogGeneration`;
- `savedAt`;
- `manifest`;

Only complete raw-request manifests are persisted. Other datasets, summary views, and incomplete manifests are not persisted under this design. A completed full snapshot caused by a required resync is complete and versioned, so it replaces the stale persisted record.

### Validation

Hydration accepts a record only when:

- its format version matches the current reader;
- `scopeKey`, dataset, and view exactly match the request;
- `catalogGeneration` is a safe non-negative integer;
- `manifest.complete` is true and its `catalogGeneration` matches the record;
- dates and file intervals are valid and overlap the stored range;
- every file has a valid identity;
- the metadata remains below `MAX_MANIFEST_BYTES`; and
- the identity set remains below `MAX_IDENTITIES`.

Rejected records are ignored. Best-effort deletion is attempted for malformed, incompatible, or expired records; deletion failure does not turn a valid fallback listing into an error.

The persisted record stores its original range as the incremental base; the new request does not have to have identical timestamps. This matters because a relative range's `to` advances after a reload. Hydration reuses a record only when its base can be advanced to the new requested range through the existing delta contract.

The persisted record expires after 24 hours. A catalog generation delta and full resync remain responsible for freshness even when the age window accepts the record. Expiry is a retention guard, not a substitute for generation validation.

### Startup flow

1. `fetchManifest` checks the two-second in-memory snapshot first.
2. For an eligible raw-request range, it hydrates one matching persisted record and installs it as the base snapshot.
3. The existing incremental path requests `since_generation` plus the previous range.
4. If the delta completes, the resulting manifest is authoritative for the requested range and is saved back to CacheStorage.
5. If the server returns `catalog_resync_required`, the browser performs the existing full snapshot and replaces the persisted record only after that snapshot is complete and versioned.
6. If hydration or CacheStorage access fails, the browser performs the existing complete listing path.

The dashboard does not begin Parquet reads from a hydrated record before the generation delta completes. The "no manifest pause" acceptance criterion applies to a hard reload whose closed-range snapshot and catalog generation are valid; in that case, startup performs one lightweight delta and no full listing.

### Invalidation

- A `commit` notification advances the minimum required generation.
- A `resync` notification invalidates persisted snapshots for the affected identity.
- `clearManifestCache` and `observeManifestCommit` clear the corresponding in-memory and persisted eligibility.
- Identity or service scope changes use a different key, so a prior user's or service's snapshot cannot be returned.
- Sign-out and explicit identity updates rotate the existing dashboard cache scope; persisted records for the old scope remain isolated until normal browser-cache retention removes them.

Deletes and writes are best effort. They do not block dashboard correctness.

### Error handling

CacheStorage reads, writes, and deletes are instrumented through the existing browser store-error reporting. A failure falls back to network listing. It is not reported as a manifest request failure and must never cause an incomplete manifest to be used.

Network status, generation conflicts, duplicate cursors, page limits, byte limits, identity limits, and catalog resync behavior retain their current errors and recovery paths.

## Boundaries

This design does not:

- persist non-request datasets or summary listings;
- allow Parquet reads before complete coverage is established;
- change `/api/files` server semantics;
- remove the existing in-memory coalescing or delta machinery;
- introduce a background service worker; or
- turn the one-time delta into a page-data API guarantee.

## Tests

Add focused tests for:

1. a cold complete listing being persisted;
2. a hard-reload-style hydration performing an incremental request instead of a full listing;
3. a valid closed-range snapshot satisfying the warm-reload startup criterion;
4. unchanged open-tail results after a catalog commit;
5. service, identity, dataset, view, and range isolation;
6. `catalog_resync_required` replacing the stale persisted record;
7. malformed, incompatible, oversized, and identity-overflow records falling back;
8. CacheStorage read/write/delete rejection falling back without hiding the original network behavior;
9. explicit clear and resync invalidation; and
10. preserving cursor, page-limit, byte-limit, identity-limit, and generation-conflict errors.

Existing manifest, engine, source-cache, TypeScript, and lint checks must pass.

## Acceptance

The todo is complete when:

- a browser startup with an eligible persisted snapshot performs no complete `/api/files` listing;
- its generation delta returns a complete authoritative manifest before Parquet reads begin;
- commit/resync and identity isolation remain correct;
- CacheStorage failures preserve the current startup behavior; and
- the focused tests and project type/lint checks pass.
