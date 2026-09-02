# ADR-17 — Analyst Path A Under DuckLake (Open Gap)

**Status:** Proposed — not yet implemented or validated. This ADR documents a real gap opened by [ADR-14](14-ducklake-replacement.md), not a shipped decision.
**Decided by:** v3.0.0 scalability review, 2026-08-31/09-01

## Context

AGENTS.md defines two analyst collaboration modes. Path A — "independent instance" — is the durable one: an analyst is issued **read-only FOS credentials** (`generate-viewer-key`) and runs their *own copy of the application*, joining via a JSON-config flow (`GET /api/provision/join`, `InviteAnalystDialog`, ProvisionWizard "join" mode). It is explicitly designed to work even when the admin's own process isn't running — the analyst's instance is self-sufficient once it has bucket credentials.

That self-sufficiency was a property of the pyiceberg catalog, not an accident. Pyiceberg's `SqlCatalog` here is a *local* SQLite file, rehydrated by reading `metadata.json` pointer files that live inside the FOS bucket itself (`_refresh_local_catalog_metadata`). Anyone with read access to the bucket can reconstruct the full catalog state — table schema, snapshot history, manifest/data-file listing — with no dependency on any service the admin runs. That is exactly what Path A needs.

DuckLake does not have this property. Its catalog metadata — schema, snapshot chain, file listing, and (per [ADR-14](14-ducklake-replacement.md)) inlined rows not yet flushed to parquet — lives in whichever backend `DUCKLAKE_CATALOG` points at: a local `.ducklake` file, or (required for multi-writer / celery mode, per [ADR-15](15-multi-writer-topology.md)) Postgres. Neither is discoverable from FOS bucket contents alone. **A Path-A analyst instance holding only read-only FOS credentials today has no way to discover DuckLake table or snapshot state at all.** This was verified by grep, not assumed: nothing in `backend/routers/services/core.py`, `backend/routers/provision.py`, or `backend/provision/*.py` references `DUCKLAKE_CATALOG` or DuckLake in any form — the join flow was never touched by the DuckLake migration.

This is a genuine regression for any service running celery mode / DuckLake with an active Path-A analyst, not a cosmetic gap: an independent-instance analyst against such a service would see an empty or permanently-stale lake.

## Options considered

1. **Grant Path-A analysts a scoped read-only Postgres role**, delivered alongside FOS credentials in the join payload (extend `generate-viewer-key` to also mint a Postgres role scoped to that service's rows). Rejected as the primary recommendation: it reintroduces exactly the liveness dependency Path A exists to avoid — "works even when the admin's instance can't stay running" implicitly assumed a runnable, admin-independent *catalog*, not just admin-independent *storage*. A live Postgres control-plane database becomes a new single point of failure for a mode whose whole purpose is not having one. It's also a materially larger security surface: today a leaked Path-A credential exposes bucket-read on log data; a leaked Postgres role would expose the shared metadata database directly.
2. **A periodically-exported, FOS-resident, file-based DuckLake catalog per service**, refreshed on some cadence (e.g. after every N commits or every compaction pass) and attached read-only by the Path-A instance directly from its S3 path. This restores the "everything the analyst needs is discoverable from the bucket" property Path A depends on, at the cost of the analyst seeing data as-of the last export rather than live. **This is the direction this ADR recommends**, but it is unvalidated: it is not yet confirmed that DuckDB's `ducklake` extension can attach a file-based catalog directly from an `s3://` path in read-only mode while a separate writer (Postgres-backed) is actively committing against the *same underlying data files* — the catalog and the data path are two different concerns, and the export would need to reference the same parquet files the live catalog already wrote, not a copy. This needs a spike against a real multi-writer setup before it's treated as validated.
3. **Do nothing; Path A is admin-only until this is resolved.** Not desirable long-term, but is the honest current state, and better documented than left silent.

## Decision

No implementation ships with this ADR. The decision is to record the gap precisely (so it isn't silently rediscovered later as a mystery bug report) and to name option 2 as the preferred direction for whoever picks this up, with its unresolved technical risk called out explicitly rather than assumed away. Until it's implemented and validated, Path A analysts against a DuckLake/celery-mode service should be treated as **unsupported** — this should be surfaced to users at invite time (the join flow should probably refuse or warn, rather than silently producing an empty dashboard; that UI/API change is itself unimplemented and is part of the follow-up work, not this ADR).

## Consequences

- Anyone re-enabling or documenting Path A for a celery-mode service must first resolve this ADR's open question, not assume pyiceberg-era behavior still holds.
- The join flow (`generate-viewer-key`, `/api/provision/join`) should eventually branch on `INGEST_MODE`/`DUCKLAKE_CATALOG` shape to either provision option 2's export mechanism or explicitly reject the join with a clear error — currently it does neither.
- Path B (live shared instance, direct-mode against the admin's running process) is unaffected — it never had its own catalog dependency; it reads through the admin's already-running backend, which already has a valid DuckLake attach.

## Out of scope

- Implementing option 2's export mechanism (separate task; needs the validation spike described above first).
- Any change to Path B.
