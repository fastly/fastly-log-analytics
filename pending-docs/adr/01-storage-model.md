# ADR-01 — Storage Model

**Status:** Accepted (Phase 0)
**Decided by:** v2.0 cleanup planning
**Supersedes:** implicit storage model that grew over the perf-improvement branch

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

For local development (per `dev-sandbox-scrub` memory), Iceberg writes go to a `file://` warehouse on the dev machine when `cdn_url` is cleared on a service config. The same Iceberg code paths run in both modes — only the warehouse URL changes. This guarantees dev exercises the same commit semantics as prod.

## Consequences

- Phase 4 carves `backend/core/iceberg.py` along this decision: separate `view`, `catalog`, `warehouse`, `manifest`, `fs` modules. The "what tier" confusion goes away because there are only two tiers.
- `local-compaction outputs survive Iceberg orphan-cleanup` (existing trap, verified by `tests/core/test_local_compaction.py::test_compaction_outputs_survive_iceberg_sync_orphan_cleanup`) stays a load-bearing invariant. Phase 4.6 re-asserts it after the carve-up.
- Orphan-file cleanup for Iceberg/FOS stays out of scope (per `orphan-files-defer` memory — wait for pyiceberg PR #3361).
- Rollup catch-up is a query-rewrite concern, not a tier-promotion concern.

## Out of scope

- Migrating to Iceberg table-format v3 (deferred — see plan §"Out of scope")
- Non-Fastly storage backends
