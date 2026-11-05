# ADR-18 — Durable Serving Mode Removes Native DuckDB File Ownership

**Status:** Superseded by [ADR-21](21-high-scale-serving-architecture.md) (historical)
**Decided by:** v3 scalability review

> [!NOTE]
> **Superseded in v3.0.0-rc3 ([ADR-21](21-high-scale-serving-architecture.md)):**
> Fastly Log Analytics now uses ClickHouse 24.8 + PostgreSQL 16 + stateless ingest workers (`python -m backend.high_scale.worker`) across all deployments. Per-service `.duckdb` files, local Parquet buffers, and pod-local Top-N rollup files have been removed. This ADR is retained as historical record of why file-backed DuckDB and pod-local rollups could not scale horizontally.

## Context

[ADR-15](15-multi-writer-topology.md) and [ADR-16](16-ingest-ledger.md) made the **ingest** tier horizontally scalable. This ADR documented the structural blockers that prevented scaling the legacy DuckDB/Parquet serving tier (`process-exclusive .duckdb files`, `pod-local compacted parquet`, and `fleet-wide cron leases over pod-local rollup files`) and motivated the cutover to ClickHouse ([ADR-21](21-high-scale-serving-architecture.md)).

### ClickHouse prototype decision (2026-09-08 UTC)

[ADR-20](20-clickhouse-serving-plane.md) rejected the measured bounded hybrid
prototype for dashboard serving. Serial p95 regressed from 1.055 s to 6.629 s;
higher-concurrency hybrid attempts hit request deadlines and memory aborts.
Correctness/rebuild evidence did not satisfy the Docker serving gate, so
Kubernetes packaging was not executed. Production dashboard routing remains
DuckLake-only; retained ClickHouse diagnostic/replay tooling does not resolve
this ADR's constraints or authorize additional backend replicas. This result
does not rule out a different ClickHouse implementation.

### Request-status observation ownership

Celery discovery and conversion do not own the backend's cached request
metrics. A raw object's name is not a request-event timestamp, and successful
discovery does not establish that rows are queryable.

The single serving backend owns observation, persistence, and SSE publication
of request count/min/max. In durable mode it must query the same durable-only
view as analytics, including committed inlined rows, without consulting local
parquet or opening a native service database. An authoritative empty result
replaces prior counts/extents with zero/null; a query failure preserves the
last successful observation and is logged, never presented as fresh zero.

Use one non-overlapping backend observer with a 15-second reconciliation
interval, starting at application startup and stopping with its lifespan.
This deliberately coalesces ingestion batches and also catches retention,
reset, missed events, and backend restarts. The interval is a refresh target,
not a hard latency guarantee: query duration or dependency failure can delay
an observation. No per-file metric query or worker-side config write is added.
Successful refresh must persist before publishing through the existing status
channel; heavy schema/top-value refresh retains its separate cadence.

The observer is serving work, not ingestion: disabling development crons must
not disable it when explicitly using the durable serving topology. Sync/file
mode retains its existing batch refresh. Additional worker dirty notifications
are deferred until measurements justify them; they are not required for
correctness or recovery with this bounded reconciliation.

### Local lifecycle enforcement

The single-process rule still applies to sync/file mode and to the local
development stack. Uvicorn's
reload mode uses a Python multiprocessing child for the server worker. Killing
only the listener or the uvicorn command can leave that child alive, still
holding a service `.duckdb` file and its WAL. The next worker then reports a
database-lock failure even though the lock holder is an orphaned process from a
previous reload.

`run.sh --kill` therefore enumerates the checkout's
`data/services/*.duckdb` files and terminates only Python/uvicorn processes
holding those files, after stopping the known listeners. This is a local
lifecycle cleanup, not a distributed-lock mechanism; durable mode is the
production path that avoids the native file for the logs view.

The September 2026 incident confirmed the distinction: PID `96313` was an
orphaned reload worker with the service database and WAL open, while the
current worker reported its own PID (`5957`) in the timeout context. The error
message's PID is the requester, not a reliable lock-holder identity; operators
must use `lsof`/process inspection to identify the actual holder.

## Consequences

- `replicaCount` in the chart applies to the stateless frontend only. Scaling the backend requires resolving this ADR first, not editing a value.
- Any future work that says "scale out the backend" must address blockers 1–3 together. Fixing one in isolation makes the failure quieter, not rarer.
- New cron jobs must be classified by **where their output lands**. A job taking the shared lease while writing pod-local output is the bug in blocker 3, and it is currently easy to write by copying any existing job body.

## Lower-severity documented gaps (not fixed here)

These are recorded so they are not rediscovered as mysteries. None is worth fixing while the serving tier is single-pod, and each becomes load-bearing the moment it is not.

- **Cron progress is in-process memory.** `backend/cron/scheduler/progress.py` is a bare module-level dict (`_progress`/`_last_update`/`_run_metadata` guarded by a `threading.Lock`). In external mode the producers are Celery workers and the consumers are backend pods, in different processes entirely — so the admin active-runs panel and the sync-status activity text are blind to all ingest work. They are not wrong, they are empty, which reads as "nothing is running" during an active sync. The durable `cron_runs` rows are unaffected; only the live progress stream is lost.
- **Two stores bypass the Postgres seam entirely.** `backend/cron/jobs/metric_snapshots.py` (`data/system/system_metrics.db`) and `backend/core/metadata/usage_log.py` are plain SQLite with no `pg_connection.is_postgres()` routing, so unlike every other metadata store they stay pod-local under `METADATA_DSN`. Admin System Health trends and the usage log therefore describe whichever pod served the request.
- **`metric_snapshot` takes no lease.** `backend/cron/jobs/metric_snapshots.py` never calls `start_cron_run`, so all N pods sample every 60s and write into a key with no pod dimension — N interleaved series stored as one. Harmless at N=1, meaningless above it. Note this is the *correct* behavior for a pod-local sampler and the wrong storage shape for it; it is blocker 3's mirror image.
- **`committed_buffers` was missing from the Postgres schema.** It is created by SQLite migration 004, not by `_SCHEMA`, and `scripts/setup_pg_schema.py` only translates `_SCHEMA` plus explicit DDL — so Postgres metadata mode had no `committed_buffers` table at all, silently losing the durable checkpoint that stops a crash between `table.append()` and `tombstone_buffer_files()` from re-appending the same rows. `pg_connection._IGNORE_TABLES` already listed the table, i.e. the dialect shim was written assuming it existed. DDL added to `setup_pg_schema.py` alongside this ADR, since `docker-compose.multipod.yml` now runs on Postgres metadata and would otherwise have regressed.
- **`service_id` is not on every metadata table.** `pg_connection`'s module docstring claimed migration 015 added `service_id` to every per-service table; it added it to `cron_runs` and `local_compacted_files` only. `committed_buffers` has no `service_id` and `filter_uncommitted_buffers` applies no such predicate, so that table is cross-tenant by buffer basename under a shared database — safe only because those basenames are uuid-derived. Docstring corrected; the schema was not changed.
- **Chart deployment defaults CrashLoop — CLOSED.** `values.yaml` shipped `config.deploymentMode: high_scale` with `config.ducklakeCatalog: ""` and `secrets.metadataDsn: ""`, so a default `helm install` booted a backend, worker and beat that `validate_deployment_mode()` correctly refused to start. The gate was behaving as designed; the defaults were wrong. Resolved after this ADR was written: the chart now defaults to `config.deploymentMode: standard` (single-node, no external datastores, so a bare `helm install` is deployable), renders the worker/beat/KEDA objects only in high-throughput mode, and validates the high-throughput prerequisites in `templates/validate.yaml` — so selecting high-throughput without a Postgres `ducklakeCatalog`/`metadataDsn`/broker is a `helm template` error naming the missing value, not three CrashLoopBackOffs. Pinned by `tests/chart/test_helm.py`.

## Out of scope

- Making local rollups, prewarming, metric snapshots, and usage logs shared
  across serving replicas.
- Making sync/file mode multi-pod safe, or making pod-local job leases
  replica-aware.
- Anything about the ingest tier, which scales as ADR-15/ADR-16 describe.

(The chart's `deploymentMode`/`ducklakeCatalog` default mismatch was out of scope when this ADR was written and has since been fixed — see the gap list above.)
