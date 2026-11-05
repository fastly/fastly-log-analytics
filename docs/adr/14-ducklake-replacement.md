# ADR-14 — DuckLake Replaces PyIceberg as the Commit-Path Catalog

**Status:** Superseded by [ADR-21](21-high-scale-serving-architecture.md) (historical)
**Decided by:** v3 scalability rework
**Supersedes:** [ADR-01](01-storage-model.md)'s "live-buffer → Iceberg (pyiceberg)" decision, for the commit/durable-store tier only

> [!NOTE]
> **Superseded in v3.0.0-rc3 ([ADR-21](21-high-scale-serving-architecture.md)):**
> The DuckLake extension, local Parquet buffer, and DuckDB analytical storage engine have been retired in favor of the unified ClickHouse + PostgreSQL 16 + FOS batch archive architecture. This ADR is retained as historical record only.

## Context

ADR-01 fixed the persistence model as live-buffer → Apache Iceberg on Fastly Object Storage, written and read through pyiceberg's `SqlCatalog` (a local SQLite catalog file per process, rehydrated from `metadata.json` pointers discoverable in the bucket itself). That model had a hard ceiling: pyiceberg's SqlCatalog was not safe for concurrent writers, and the manifest-scan-based UI metadata path was showing multi-second cost per scan on a 4-core VM.

## Historical Decision Summary

During the early v3 transition, DuckDB's **DuckLake** extension temporarily replaced pyiceberg as the catalog and write path before being superseded by ClickHouse in `v3.0.0-rc3` ([ADR-21](21-high-scale-serving-architecture.md)).

## Out of scope

- Removing pyiceberg from `buffer.py`/`sync.py` (separate task, needs its own audit of whether those code paths are still reachable).
- A full DuckLake-vs-Iceberg-table-format-v3 comparison — DuckLake was chosen for its multi-writer transactional catalog, not evaluated against every alternative.
