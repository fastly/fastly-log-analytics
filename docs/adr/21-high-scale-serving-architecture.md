# ADR-21 - Unified ClickHouse Serving, Ownership, and Recovery Gate

**Status:** Accepted (Unified as the single architecture in `v3.0.0-rc3`, superseding [ADR-01](01-storage-model.md), [ADR-06](06-view-warming.md), [ADR-14](14-ducklake-replacement.md), [ADR-18](18-serving-tier-single-pod.md), and [ADR-19](19-fos-native-v3-topology.md)).
**Date:** 2026-09-11 (Updated 2026-10-09, Unified 2026-11-04)

## Decision

The repository uses a single, unified ClickHouse + PostgreSQL 16 + Fastly Object Storage (FOS) data plane across all deployments (from a 1-worker laptop Docker Compose stack up to multi-node Kubernetes clusters).

| Operation | Unified Architecture (`high_scale`) |
|---|---|
| Source discovery | PostgreSQL ingest ledger (`high_scale_source_cursors`, `high_scale_partition_cursors`) |
| Durable event store | Verified FOS batch archive (`{prefix}/high-scale/archive/`) |
| Serving store | ClickHouse (`request_facts`, `rum_vitals_facts`, `rum_error_facts`, `cmcd_projection_facts`, minute projections) |
| Source deletion | Archive deletion controller (`HighScaleDeletionSweeper`, `HighScaleArchiveExpirer`) |
| RUM ingest | Stateless worker RUM pipeline (`rum_vitals` + `rum_errors` sibling publication) |
| CMCD | Request-event projection (`cmcd_projection_facts`) |

The data plane owns one owner epoch per service and, per the 2026-10-09 amendment below, one source cursor per key-range partition of that service.
Cutover requires a drained old owner, a durable cursor handoff, and an
atomic cutover record. Rollback is allowed only when the replacement archive
is replayable and the prior owner has not lost source coverage.

## Archive and deletion protocol

Archive state is monotonic and adjacent:

```text
artifact_uploading -> artifact_verified -> manifest_prepared
  -> manifest_committed -> deletion_eligible -> source_deleted
```

> **Amendment (2026-10-09):** a manifest may cover many source objects. It holds
> one artifact and a list of source entries (key, version, checksum, row range,
> deletion deadline); replay and deletion remain per source.

> **Amendment (2026-10-09):** a service may have several source cursors, one per key-range partition, all under the single owner epoch, so one hot service can be ingested in parallel. Cutover and handoff apply to all partition cursors together.

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
