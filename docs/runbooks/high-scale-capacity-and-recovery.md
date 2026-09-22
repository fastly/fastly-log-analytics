# High-Scale Capacity and Recovery Contract

The high-scale data plane must sustain 2M request events/sec, absorb 5M/sec
bursts for five and thirty minutes, sustain 500k RUM events/sec, and absorb
1M/sec RUM bursts without dropping data. Overload increases lag and backlog;
it never silently samples or discards events.

The replacement-instance target is ingest acceptance within two minutes and
usable triage for the latest 24 hours within 15 minutes. The 15-minute target
is decomposed into ingest acceptance, triage availability, exact raw-query
availability, and full rebuild completion. RPO is zero for acknowledged
archive writes.

## Watermarks and tiers

Every response carries request, RUM vitals, RUM errors, and CMCD watermarks
independently. A watermark identifies the service, domain, owner epoch,
coverage start/end, last accepted source cursor, last archived event, and last
serving-visible event. Query responses also declare whether results are exact
or approximate, the coverage fraction, freshness age, and any error state.

Recent data is the hot tier and targets interactive dashboard latency. Warm
history uses ClickHouse facts and incremental aggregates. Cold history is
queued, bounded, cancellable, progress-aware, and exportable. Interactive raw
pages are capped at 500 rows and use keyset cursors; clients never fetch a
large result set to paginate locally.

## Recovery qualification

Qualification measures FOS read/decode, normalization, archive verification,
ClickHouse insert, replication, aggregate rebuild, and live-ingest reservation
separately. A recovery claim is valid only when the slowest stage sustains the
required replay rate for the complete target window while the live reservation
remains available.

FOS is the durable source of truth and recovery authority. ClickHouse backups
may accelerate recovery but cannot replace the verified archive. Source
deletion remains blocked until the archive manifest is committed, the grace
period expires, the owner epoch matches, and no replay lease is active.

## Capacity profiles

The portable production profile uses two ClickHouse replicas per shard and
three Keeper members. Development profiles may use one replica and a reduced
Keeper topology, but must not claim production availability. Per-service
fairness guarantees minimum progress while allowing idle capacity to be
borrowed by active services.
