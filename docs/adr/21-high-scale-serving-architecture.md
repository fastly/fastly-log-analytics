# ADR-21 - High-Scale Ownership and Recovery Gate

**Status:** Phase 0 gate accepted; runtime implementation not enabled
**Date:** 2026-09-11

## Decision

The repository will add a separate, explicitly fenced `high_scale` data plane.
This ADR defines its contracts but does not add `high_scale` to the supported
runtime modes. `standard` and `high_throughput` remain the only active modes
until the migration and recovery evidence described below exists.

| Operation | standard | high_throughput | high_scale |
|---|---|---|---|
| Source discovery | synchronous ingest | high-throughput ledger | high-scale ledger |
| Durable event store | DuckLake | DuckLake | verified FOS archive |
| Serving store | DuckDB/DuckLake | DuckDB/DuckLake | ClickHouse |
| Source deletion | existing ingest path | existing ingest path | archive deletion controller |
| RUM ingest | existing mode path | RUM ledger | high-scale RUM pipeline |
| CMCD | request-row analytics | request-row analytics | request-event projection |
| DuckLake writers | allowed | allowed | prohibited |

The high-scale plane owns one source cursor and one owner epoch per service.
Cutover requires a drained old owner, a durable cursor handoff, and an
atomic cutover record. Rollback is allowed only when the replacement archive
is replayable and the prior owner has not lost source coverage.

## Archive and deletion protocol

Archive state is monotonic and adjacent:

```text
artifact_uploading -> artifact_verified -> manifest_prepared
  -> manifest_committed -> deletion_eligible -> source_deleted
```

A manifest records service and domain, exact source identity and version,
artifact identity, row and byte counts, per-domain counts, schema and
transform versions, canonical event digest, retention and deletion deadlines,
and the archive epoch. Source deletion requires a committed verified manifest,
the current owner epoch, an expired grace period, and no replay lease.

FOS is the recovery authority. ClickHouse publication or local ingestion alone
never authorizes source deletion. The deletion controller is the only owner
permitted to perform that operation.

## Recovery capacity

Recovery capacity must be measured as a pipeline, not inferred from one
component. The executable budget requires archive read, decode, ClickHouse
insert, replication factor, live-ingest reservation, and recovery window. A
budget is feasible only when the slowest measured stage can sustain the
required replay rate after reserving live-ingest capacity.

For 2,000,000 request events/sec and a 15-minute recovery window, the minimum
replay rate is 192,000,000 events/sec before live-ingest reservation. With a
25% reservation, the pipeline must sustain 256,000,000 events/sec.

## Consequences

- The existing ClickHouse prototype remains diagnostic tooling under ADR-20;
  `CLICKHOUSE_ENABLED` does not become a serving switch.
- High-scale code must not import the current DuckLake commit/admission path
  as its durable or deletion authority.
- RUM vitals, RUM errors, request facts, and CMCD projections require separate
  identities, watermarks, retention, and conservation checks.
- A selected ClickHouse/Keeper operator, continuous-ingest vertical slice,
  archive-only recovery test, differential canary, and rollback evidence are
  required before enabling the runtime mode.
