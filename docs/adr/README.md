# Architecture Decision Records

Each ADR captures one significant design decision — the context, the choice, and its consequences. They explain *why* the system is shaped the way it is, and are the place to look before changing a load-bearing invariant.

| # | Title | Status | Topic |
|---|---|---|---|
| [ADR-01](01-storage-model.md) | Storage Model | **Superseded by ADR-21** (historical) | Early buffer → lakehouse storage layout; superseded by ClickHouse + FOS batch archive |
| [ADR-02](02-request-lifecycle.md) | Request Lifecycle | Accepted | How an API request flows through RequestContext |
| [ADR-03](03-tenancy.md) | Tenancy | Accepted | Per-service isolation (config, metadata, ClickHouse partition/service_id fence) |
| [ADR-04](04-middleware-order.md) | Middleware Order | Accepted | The required ordering of FastAPI middleware asserted at boot |
| [ADR-05](05-frontend-rendering-boundary.md) | Frontend Rendering Boundary | Accepted | Server vs. client rendering split in the Next.js app |
| [ADR-06](06-view-warming.md) | Writer-Driven View Warming | **Superseded by ADR-21** (historical) | Legacy DuckDB view warming; retired with unified ClickHouse serving |
| [ADR-07](07-feature-budgets.md) | Per-Feature Performance & Cost Budgets | Accepted | The p95 / storage / scale budget every new endpoint declares |
| [ADR-08](08-observability.md) | Observability Strategy | Accepted (storage amended by ADR-22) | Logs, metrics, and traces; structlog + OpenTelemetry + Postgres usage_log |
| [ADR-09](09-error-handling.md) | Error Handling, Retry, and Idempotency | Accepted | Failure semantics across the ingest and API paths |
| [ADR-10](10-schema-evolution.md) | Schema Evolution Contract | Accepted (field rules); storage superseded by ADR-21/22 | How new/missing/changed log fields are absorbed without breaking Rust Scorer |
| [ADR-11](11-secret-rotation.md) | Secret Rotation Policy | Accepted | Rotating credentials and keys (FOS keys, scoring AES key, etc.) |
| [ADR-12](12-api-versioning.md) | API Versioning Doctrine | Accepted | Flat `/api/*` surface, no URL versioning, breaking change rules |
| [ADR-13](13-backup-dr.md) | Backup, Disaster Recovery, and Data Replay | Partially superseded (ADR-21/22) | Recovering state and replaying data; FOS is source of truth |
| [ADR-14](14-ducklake-replacement.md) | DuckLake Replaces PyIceberg | **Superseded by ADR-21** (historical) | Legacy DuckLake catalog swap; retired in v3.0.0-rc3 |
| [ADR-15](15-multi-writer-topology.md) | Multi-Writer Topology | **Superseded by ADR-22** (historical) | Postgres metadata backend, split cron scheduling; dual SQLite/Postgres mode retired |
| [ADR-16](16-ingest-ledger.md) | Ingest Ledger | **Superseded by ADR-21/22** (historical) | discovered/claimed/committed/quarantined/dead_letter state machine for ingest |
| [ADR-17](17-analyst-path-a-ducklake.md) | Analyst Path A Under DuckLake | **Withdrawn** | Path A removed in v3; remote sharing is the only analyst access model |
| [ADR-18](18-serving-tier-single-pod.md) | Durable Serving Mode Removes Native DuckDB File Ownership | **Superseded by ADR-21** (historical) | Transitional serving tier analysis; superseded by unified ClickHouse serving |
| [ADR-19](19-fos-native-v3-topology.md) | FOS-native v3 Topology | **Superseded by ADR-21** (historical) | Transitional v3 4-plane topology; superseded by unified ClickHouse + FOS archive |
| [ADR-20](20-clickhouse-serving-plane.md) | Reject ClickHouse Dashboard Serving Prototype | Rejected prototype (superseded by ADR-21) | Measured latency/saturation gates failed on early hybrid prototype; evolved into ADR-21 |
| [ADR-21](21-high-scale-serving-architecture.md) | Unified ClickHouse Serving, Ownership, and Recovery Gate | Accepted | Unified ClickHouse serving engine + PostgreSQL 16 control plane + FOS batch archive |
| [ADR-22](22-postgres-only-metadata.md) | PostgreSQL-Only Unified Metadata Store | Accepted | Mandatory PostgreSQL 16 across all deployments; eliminates SQLite and dual-mode rewriting |

File paths and module names inside an ADR describe the code at decision time. In particular, `backend/core/iceberg/`, `backend/core/storage/`, `backend/core/duckdb_pool.py`, `backend/core/rollups/`, and the SQLite pool (`sqlite_pool.py`, `sqlite_migrations.py`) were removed in `v3.0.0-rc3` when the project standardized on ClickHouse + PostgreSQL 16 (ADR-21, ADR-22). The ADRs are historical decisions, not a map of the current tree; see [ARCHITECTURE.md](../ARCHITECTURE.md) for that.

Status values: **Accepted** decisions describe current behavior; **Partially superseded** and **Scoped** ADRs still hold for the parts named; **Superseded**, **Withdrawn** and **Rejected prototype** ADRs are kept as historical record only and must not be implemented.

For the broader system overview see [../ARCHITECTURE.md](../ARCHITECTURE.md); for implementation patterns and known traps see [../../AGENTS.md](../../AGENTS.md).
