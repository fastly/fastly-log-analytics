# High-Scale Vertical Slice Evidence Plan

The vertical slice must prove one complete request/RUM/CMCD path locally before
portable Kubernetes packaging or runtime mode registration.

## Required flow

1. Read versioned source objects from an FOS-compatible test store.
2. Assign deterministic event identities containing service, domain, source
   key/version, record ordinal, and transform version.
3. Normalize request events, RUM vitals, and RUM errors independently.
4. Derive CMCD exactly once from request-event identity.
5. Write full-fidelity Parquet artifacts and verify checksum, row count, byte
   count, and canonical digest.
6. Commit the archive manifest and visibility fence.
7. Publish ClickHouse facts and aggregates with idempotent batch identity.
8. Prove readers exclude unpublished batches.
9. Delete the source only through the fenced deletion controller.
10. Empty the serving target and rebuild solely from the archive.

## Failure matrix

The evidence suite must cover duplicate retries, lost insert acknowledgements,
partial inserts, worker termination, manifest replay, replica replacement,
recovery leases, stale owner epochs, malformed records, and a source object
whose legitimate rows are identical except for record ordinal.

Each case records source, decoded, accepted, quarantined, archived, and
serving-visible counts plus canonical digests. Any mismatch blocks deletion and
keeps the service on its prior owner.

## Exit criteria

The slice is complete only when the replay budget is measured end-to-end,
freshness and coverage watermarks are exposed, the exact/approximate status is
reported, and the migration fence can abort without leaving two authoritative
pipelines. Local validation must not mutate Elevation or customer services.
