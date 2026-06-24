# Architecture Decision Records

Each ADR captures one significant design decision — the context, the choice, and its consequences. They explain *why* the system is shaped the way it is, and are the place to look before changing a load-bearing invariant.

| # | Title | Topic |
|---|---|---|
| [ADR-01](01-storage-model.md) | Storage Model | Raw logs → local Parquet buffer → Iceberg table; the layered storage layout |
| [ADR-02](02-request-lifecycle.md) | Request Lifecycle | How an API request flows through the backend |
| [ADR-03](03-tenancy.md) | Tenancy | Per-service isolation (config, DuckDB engine, metadata, Iceberg table) |
| [ADR-04](04-middleware-order.md) | Middleware Order | The required ordering of FastAPI middleware and why |
| [ADR-05](05-frontend-rendering-boundary.md) | Frontend Rendering Boundary | Server vs. client rendering split in the Next.js app |
| [ADR-06](06-view-warming.md) | Writer-Driven View Warming | Keeping the unified `logs` view warm for fast reads |
| [ADR-07](07-feature-budgets.md) | Per-Feature Performance & Cost Budgets | The p95 / storage / scale budget every new endpoint declares |
| [ADR-08](08-observability.md) | Observability Strategy | Logs, metrics, and traces; structlog + OpenTelemetry wiring |
| [ADR-09](09-error-handling.md) | Error Handling, Retry, and Idempotency | Failure semantics across the ingest and API paths |
| [ADR-10](10-schema-evolution.md) | Schema Evolution Contract | How new/missing/changed log fields are absorbed |
| [ADR-11](11-secret-rotation.md) | Secret Rotation Policy | Rotating credentials and keys (FOS keys, scoring AES key, etc.) |
| [ADR-12](12-api-versioning.md) | API Versioning Doctrine | How the API surface evolves without breaking clients |
| [ADR-13](13-backup-dr.md) | Backup, Disaster Recovery, and Data Replay | Recovering state and replaying data after loss |

For the broader system overview see [../ARCHITECTURE.md](../ARCHITECTURE.md); for implementation patterns and known traps see [../../AGENTS.md](../../AGENTS.md).
