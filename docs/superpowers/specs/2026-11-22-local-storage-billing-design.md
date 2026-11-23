# Local storage billing design

## Problem

The Usage page labels storage as an incurred cost for the selected period, but
it does not consistently measure that value from this install's storage state.
The UI prefers Fastly Platform's account-wide `object_storage_bytes_count`,
which can include unrelated buckets and does not represent this service's
deleted-but-still-billed bytes. Both editions also reduce local state to a
current snapshot and multiply it by the 30-day minimum, rather than integrating
billable object lifetimes across the selected period.

## Goal

Compute selected-period storage GB-hours from service-local lifecycle data:

- every object currently owned by this install;
- every locally recorded object deletion that remains in the minimum billing
  window;
- the overlap of each billable interval with the selected `[start, end)`;
- no Fastly Platform storage metric in the Storage Impact, incurred-cost, or
  Cost Estimator storage result.

Fastly Platform storage remains available as a separately labeled account-wide
informational card.

The same selected-period authority applies to operations: Estimated Incurred
Cost uses the service-local FOS operation ledger in both editions. The monthly
Cost Estimator may continue to model future/reference operation volume, so it
can differ from incurred operations for the selected day. A missing local
ledger is unavailable/partial, not an exact zero and not silently replaced by
an account-wide Platform count.

The complete Usage page should finish within 20 seconds from navigation on the
normal supported data set. The page must read a locally persisted inventory
snapshot and apply newer lifecycle deltas; it must not perform an on-demand
full bucket LIST before rendering storage cost.

## Shared contract

Add one internal `StorageBillingInterval` model. Every interval carries:

- `kind`: `live`, `deleted`, or `minimum-billed`;
- `dataset` and storage source;
- `bytes`;
- `created_at`;
- `deleted_at` when applicable;
- `billed_from`;
- `billed_until`;
- `complete`;
- provenance describing the lifecycle ledger.

The calculator sums:

```text
GB-hours = sum(bytes / GiB
  * overlap_seconds([billed_from, billed_until), [start, end))
  / 3600)
```

For an object deleted during its minimum term, `billed_until` extends through
the remaining minimum-billed interval. A live object remains billed through
the selected period end. Where a source knows a deleted object's size and
deletion time but not its creation time, it may contribute only through an
explicitly conservative minimum-billing interval and marks that portion
incomplete; it must not claim an exact incurred cost.

## Edition adapters

### High-Scale

Use the existing Postgres control plane as the lifecycle source of truth:

- `high_scale_source_objects` supplies source size, `discovered_at`, status,
  and the transition to `source_deleted`.
- `high_scale_archive_manifests` supplies archive artifact size, creation, and
  `archive_expired_at`.
- Existing local cloud-copy deletion records supply deleted bytes when
  applicable.

The adapter does not infer intervals from ClickHouse row counts or Fastly
Platform metrics.

### Cloud

Build the same intervals from Cloud-owned local state:

- `storage_reconcile` supplies the complete inventory of live owned objects.
- The background reconcile persists a versioned, per-object inventory snapshot
  with size and storage timestamp needed for byte-time calculation, not just
  aggregate dataset totals. The browser reads the latest complete snapshot.
- `ops:ingest_files` supplies exact raw request object creation/deletion state
  and size.
- `fos_usage` and the local cloud-copy deletion ledger supply processed and
  compaction/retention deletion events.

Ledger reads are paginated and bounded. If all owned object lifecycles cannot
be established, the response succeeds with the exact portion and an explicit
incomplete flag rather than silently substituting an estimate.

## API and UI behavior

`/api/usage/current-storage` returns:

- local live and deleted object counts/bytes;
- selected-period live, deleted, and minimum-billed GB-hours;
- `total_billed_gb_hours` as the sum of those local components;
- completeness and provenance per source;
- period bounds used by the calculation.

The Usage page:

- removes `platformStorage` from `UsageStatCards`' storage calculation;
- labels local values `Measured · this install`;
- uses `total_billed_gb_hours` for the Storage Impact and incurred storage
  line;
- passes the same local GB-hours into the High-Scale and Cloud Cost
  Calculators;
- shows a partial-cost warning when any required storage lifecycle source is
  incomplete;
- keeps the Platform storage card as `Account-wide · Fastly Platform` for
  visibility, never as an override.
- uses service-local A/B operation totals and buckets for incurred operations;
  an unavailable local ledger removes the operation component from the exact
  total instead of showing `$0.00`.

## Completeness and errors

- Missing or failed lifecycle reads are explicit. Do not render exact zero.
- Incomplete deletion coverage marks storage cost partial and identifies the
  missing source.
- Empty selected periods are valid and return zero GB-hours.
- All calculations stay in `bytes`, GiB-hours, and contract rate units; no
  floating-point account-wide substitute is used.
- A local inventory can be stale only within a documented reconciliation
  window; the response reports its `observed_at` timestamp.
- The cold Usage page completes within 20 seconds after the local inventory
  snapshot is current. A stale/missing snapshot is explicit and may trigger an
  advisory, never an on-demand full-bucket preflight.

## Testing

Add focused tests that fail on the old behavior:

- a live object crossing only part of the selected window contributes only
  that overlap;
- a deletion inside its minimum term contributes through the billed floor;
- a deletion after the minimum term stops at deletion;
- Platform storage being present must not replace local storage on the stat
  card or either calculator;
- Cloud and High-Scale adapters produce equal shared-contract output for
  equivalent lifecycle records;
- incomplete Cloud deletion history produces `Incomplete`/partial cost, not
  a false exact value;
- aggregate live inventory and exact deletion records do not double count
  bytes.

Run focused backend usage/router tests and frontend Usage, storage, calculator,
Cloud adapter, and High-Scale Postgres control tests; then typecheck and lint
only the touched files.

## Out of scope

- Replacing Fastly Platform metrics for Compute or NGWAF.
- Repricing contract rates.
- Rebuilding historical data solely to populate the UI.
- Counting unrelated account buckets as service storage.
