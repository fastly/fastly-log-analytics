# ADR-19: Fastly Object Storage-native v3 topology

- **Status:** Accepted for v3.0 design
- **Date:** 2026-09-04
- **Decision owners:** Platform and analytics maintainers

## Context

Fastly Log Analytics receives immutable log objects in Fastly Object Storage
(FOS), which exposes an S3-compatible API. The application currently has two
execution modes:

- single-node synchronous ingest using APScheduler, local Parquet buffers,
  local DuckLake metadata, and a per-service DuckDB file;
- distributed Celery ingest using shared Postgres coordination and a
  Postgres-backed DuckLake catalog, while dashboard serving still depends on
  pod-local DuckDB and Parquet state.

The first mode is a valid small single-node deployment, but neither mode may
be presented as horizontally scalable dashboard serving. A native DuckDB file
is process-local writer state, and local Parquet/rollup files cannot be
referenced by shared metadata unless every serving process can read the same
filesystem. A recent local commit failure reported
`Database is locked by another process`, demonstrating why this boundary must
be explicit.

## Decision

v3.0 will use four explicit planes:

1. **FOS landing plane:** Fastly logging writes immutable raw objects to FOS.
   FOS notifications, where enabled, are at-least-once latency hints. Periodic
   FOS listing/reconciliation remains the recovery source of truth.
2. **Distributed ingest plane:** a Postgres ingest ledger owns object identity,
   leases, retries, quarantine, and replay. Celery/Valkey workers download and
   normalize bounded batches. Commit concurrency is independently bounded.
3. **Durable catalog/data plane:** a Postgres-backed DuckLake catalog publishes
   Parquet to FOS. A batch is not durable for raw-deletion purposes until
   required DuckLake inline data is flushed and publication is recorded.
4. **Serving plane:** distributed serving uses read-only or ephemeral DuckDB
   connections against durable shared catalog/data state. Local DuckDB files,
   buffers, and rollups may be disposable accelerators only; shared metadata
   must never point at pod-local correctness state.

The synchronous mode remains available for development and explicitly
single-node deployments. Celery mode is the supported scalable production
ingest topology and requires Postgres-backed metadata and DuckLake catalog
configuration at boot.

## Consequences

### Positive

- FOS is treated as the durable source of truth rather than as a queue.
- Duplicate, delayed, and missed discovery events are recoverable.
- Ingest workers can scale independently from dashboard serving.
- Catalog contention can be measured and bounded instead of hidden behind
  unbounded writer concurrency.
- A new serving replica can answer from durable state without inheriting a
  private filesystem.

### Negative

- Postgres is required for scalable ingest coordination and catalog operation.
- Serving directly from FOS-backed Parquet requires object-store request,
  memory, and query-admission benchmarks.
- Rollups and prewarming must move to shared durable storage or become
  explicitly disposable.
- The v3 migration is larger than increasing worker counts or retry limits.

## Rejected alternatives

- **More writers against a shared native DuckDB file:** DuckDB supports
  multiple writer threads in one process, not independent backend/worker
  processes sharing a writable database file.
- **Treating FOS notifications as exactly-once:** object notifications are
  at-least-once and can be duplicated or delayed.
- **Keeping shared bookkeeping for pod-local replacement files:** this can
  silently suppress re-downloads on a replica that does not have the file.
- **Scaling the current synchronous scheduler horizontally:** it would create
  competing file/catalog writers and does not solve serving-state ownership.

## Validation gates

Before v3.0 deployment:

- lock diagnostics identify process, service, path, ingest mode, and catalog
  mode;
- duplicate FOS delivery produces one logical object;
- replay after a worker or catalog failure is idempotent;
- raw deletion follows durable DuckLake publication and inline-data flush;
- a serving replica works with an empty local cache;
- bounded 1x/2x/4x FOS load tests measure delivery, discovery, ingest,
  publication, dashboard freshness, latency, and errors separately;
- the full repository CI gate passes;
- production v2.4.1 remains unchanged during this validation.
