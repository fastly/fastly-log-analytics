# ADR-01 — Storage Model

**Status:** Partially Superseded (Amended by [ADR-14](14-ducklake-replacement.md) & [ADR-21](21-high-scale-serving-architecture.md))
**Decided by:** Engineering retrospective
**Supersedes:** implicit storage model that grew over the perf-improvement branch

> [!NOTE]
> **Architectural Amendments (v3.0):**
> - **DuckLake Cutover ([ADR-14](14-ducklake-replacement.md)):** The commit-path durable catalog replaced PyIceberg with DuckLake. Raw logs stream to Fastly Object Storage (FOS), are transformed into Parquet in the local buffer, and commit to DuckLake.
> - **High-Scale Data Plane ([ADR-21](21-high-scale-serving-architecture.md)):** In `DEPLOYMENT_MODE=high_scale`, ingest micro-batches directly to ClickHouse and writes durable FOS archive Parquet. Serving queries ClickHouse directly or uses ephemeral in-memory DuckDB reading durable DuckLake state. The live-buffer tier and rollups-as-query-optimization pattern remain active in standard mode.

## Context

The perf-improvement branch left us with five storage tiers stitched together:

1. **Live buffer** — in-memory ring + local parquet shards held until the next sync
2. **Local Parquet (`/mnt/app-data/raw/...`)** — landed-and-compacted files on the VM disk
3. **Apache Iceberg on Fastly Object Storage** — committed long-term store
4. **Local-compaction outputs** — `compacted_*.parquet` artifacts the compaction job emits before Iceberg commit
5. **Rollups** — pre-aggregated parquets for dashboard / top-N queries

This sprawl created the F3 wedge bug (Iceberg view-rebuild holding `_Pool.acquire`'s `_cond`), confusing recovery semantics, and the periodic "what tier owns this row right now" question.

## Decision

The persistence model is **live-buffer → Iceberg**, full stop.

- **Live buffer** is the only writer-side tier. Its job: capture raw events safely until the next commit window.
- **Iceberg on Fastly Object Storage** is the only durable store. Everything else is derived.
- **Rollups are a query optimization, not a tier.** Phase 4's query planner rewrites read SQL to point at rollup parquets when the request shape is rollup-eligible. Routers do not know rollups exist. If rollups are missing, queries fall back to the raw view (slower, correct).
- **Local-compaction outputs are an Iceberg implementation detail.** The compaction job produces `compacted_*.parquet`, commits them through pyiceberg, and removes them once the commit lands. They are NOT a separate tier you can query.

Storage backend is **Fastly Object Storage** (S3-compatible). Storage portability across clouds is explicitly out of scope (see ADR for VM portability). No fsspec abstraction, no gcsfs/adlfs adapters.

## Local-warehouse fallback rule

For local development, Iceberg writes go to a `file://` warehouse on the dev machine when `cdn_url` is cleared on a service config. The same Iceberg code paths run in both modes — only the warehouse URL changes. This guarantees dev exercises the same commit semantics as prod.

## Consequences

- Phase 4 carves `backend/core/iceberg.py` (removed) along this decision: separate `view`, `catalog`, `warehouse`, `manifest`, `fs` modules. The "what tier" confusion goes away because there are only two tiers.
- `local-compaction outputs survive Iceberg orphan-cleanup` (existing trap) stays a load-bearing invariant. Phase 4.6 re-asserts it after the carve-up.
- Orphan-file cleanup for Iceberg/FOS stays out of scope pending the upstream pyiceberg work (PR #3361).
- Rollup catch-up is a query-rewrite concern, not a tier-promotion concern.

## Out of scope

- Migrating to Iceberg table-format v3 (deferred — see plan §"Out of scope")
- Non-Fastly storage backends
