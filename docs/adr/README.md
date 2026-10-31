# Architecture Decision Records

Each ADR captures one significant design decision — the context, the choice, and its consequences. They explain *why* the system is shaped the way it is, and are the place to look before changing a load-bearing invariant.

| # | Title | Status | Topic |
|---|---|---|---|
| [ADR-01](01-storage-model.md) | Storage Model | Partially superseded (ADR-14/21) | Raw logs → local Parquet buffer → DuckLake/archive; layered storage layout (amended by ADR-14/21) |
| [ADR-02](02-request-lifecycle.md) | Request Lifecycle | Accepted | How an API request flows through RequestContext |
| [ADR-03](03-tenancy.md) | Tenancy | Accepted | Per-service isolation (config, DuckDB engine, metadata, DuckLake table) |
| [ADR-04](04-middleware-order.md) | Middleware Order | Accepted | The required ordering of FastAPI middleware asserted at boot |
| [ADR-05](05-frontend-rendering-boundary.md) | Frontend Rendering Boundary | Accepted | Server vs. client rendering split in the Next.js app |
| [ADR-06](06-view-warming.md) | Writer-Driven View Warming | Accepted (Standard mode only) | Keeping the unified `logs` view warm for fast reads in DuckDB pool |
| [ADR-07](07-feature-budgets.md) | Per-Feature Performance & Cost Budgets | Accepted | The p95 / storage / scale budget every new endpoint declares |
| [ADR-08](08-observability.md) | Observability Strategy | Accepted (storage amended by ADR-22) | Logs, metrics, and traces; structlog + OpenTelemetry + Postgres usage_log |
| [ADR-09](09-error-handling.md) | Error Handling, Retry, and Idempotency | Accepted | Failure semantics across the ingest and API paths |
| [ADR-10](10-schema-evolution.md) | Schema Evolution Contract | Accepted (field rules); storage superseded by ADR-14/22 | How new/missing/changed log fields are absorbed without breaking Rust Scorer |
| [ADR-11](11-secret-rotation.md) | Secret Rotation Policy | Accepted | Rotating credentials and keys (FOS keys, scoring AES key, etc.) |
| [ADR-12](12-api-versioning.md) | API Versioning Doctrine | Accepted | Flat `/api/*` surface, no URL versioning, breaking change rules |
| [ADR-13](13-backup-dr.md) | Backup, Disaster Recovery, and Data Replay | Partially superseded (ADR-14/21/22) | Recovering state and replaying data; FOS is source of truth (amended by ADR-14/22) |
| [ADR-14](14-ducklake-replacement.md) | DuckLake Replaces PyIceberg | Accepted | Commit-path catalog swap; supersedes ADR-01's Iceberg decision |
| [ADR-15](15-multi-writer-topology.md) | Multi-Writer Topology | **Superseded by ADR-22** (historical) | Postgres metadata backend, split cron scheduling; dual SQLite/Postgres mode retired |
| [ADR-16](16-ingest-ledger.md) | Ingest Ledger | Accepted | discovered/claimed/committed/quarantined/dead_letter state machine for celery/high-scale ingest |
| [ADR-17](17-analyst-path-a-ducklake.md) | Analyst Path A Under DuckLake | **Withdrawn** | Path A removed in v3; remote sharing is the only analyst access model |
| [ADR-18](18-serving-tier-single-pod.md) | Durable Serving Mode Removes Native DuckDB File Ownership | Accepted | Ingest scales horizontally across Celery workers; backend serving tier pinned to single replica |
| [ADR-19](19-fos-native-v3-topology.md) | FOS-native v3 Topology | Accepted | Formalizes the 4 explicit planes (landing, ingest, durable catalog, serving) and FOS as truth |
| [ADR-20](20-clickhouse-serving-plane.md) | Reject ClickHouse Dashboard Serving Prototype | Rejected prototype (superseded by ADR-21) | Measured latency/saturation gates failed on early hybrid prototype; evolved into ADR-21 |
| [ADR-21](21-high-scale-serving-architecture.md) | High-Scale Ownership and Recovery Gate | Accepted | Standard vs High-Scale two-mode architecture; ClickHouse serving engine + FOS archive |
| [ADR-22](22-postgres-only-metadata.md) | PostgreSQL-Only Unified Metadata & Catalog | Accepted | Mandatory PostgreSQL 16 across all modes; eliminates SQLite, `sqlite_pool.py`, and dual-mode rewriting |

File paths and module names inside an ADR describe the code at decision time. In particular, `backend/core/iceberg/` was replaced by `backend/core/storage/` (ADR-14), and the SQLite pool and per-service SQLite metadata modules (`sqlite_pool.py`, `sqlite_migrations.py`) were removed (ADR-22). The ADRs are historical decisions, not a map of the current tree; see [ARCHITECTURE.md](../ARCHITECTURE.md) for that.

Status values: **Accepted** decisions describe current behavior; **Partially superseded** and **Scoped** ADRs still hold for the parts named; **Superseded**, **Withdrawn** and **Rejected prototype** ADRs are kept as historical record only and must not be implemented.

For the broader system overview see [../ARCHITECTURE.md](../ARCHITECTURE.md); for implementation patterns and known traps see [../../AGENTS.md](../../AGENTS.md).
