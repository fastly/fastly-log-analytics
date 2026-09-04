# ADR-15 — Multi-Writer Topology: Postgres Metadata + Split Scheduling

**Status:** Accepted
**Decided by:** v3.0.0 scalability rework, `release/v3.0.0` commits `3c3df695` (Postgres metadata seam), `4487be3f` (scheduler split), `debfaba3`/`8fa0dfd1` (Postgres DuckLake catalog requirement)

## Context

ADR-03 (Tenancy) and the original ingest design assume one backend process per service, each with its own per-service SQLite metadata database (`configs/`, cron state, ingest bookkeeping) and its own DuckDB file. That model cannot scale past one pod: SQLite files are pod-local, so a second backend pod (or a fleet of Celery workers) has no shared view of cron leases, ingest state, or the commit catalog. The 100k–1M RPS target requires horizontally scaled ingestion — many Celery workers pulling from FOS concurrently — which is impossible with a pod-local metadata store.

## Decision

### 1. Postgres is the metadata backend for multi-pod deployment

`METADATA_DSN` (env var) switches every metadata read/write (`backend/core/metadata/base.py::get_con`/`get_con_readonly`/`close_all_connections`) from the SQLite `ThreadLocalPool` to a Postgres connection, routed through `backend/core/metadata/pg_connection.py`. This is gated by `pg_connection.is_postgres()` — SQLite remains the default for single-pod/dev deployments; nothing about the API surface changes for callers.

- `get_pg_thread_connection()`: one long-lived connection per thread (mirrors SQLite's per-thread pool semantics), shared across **all** services on that thread since rows are scoped by a `service_id` column rather than by file.
- `get_pg_readonly_connection()`: fresh checkout-and-return per call, for `contextlib.closing()` callers.
- `PgConnectionWrapper`/`PgCursorWrapper` translate SQLite-shaped SQL to Postgres dialect at the query layer (`?` → `%s` placeholders, `INSERT OR REPLACE/IGNORE` → `ON CONFLICT`, `cursor.lastrowid` emulated via `RETURNING id` for `cron_runs`) so the large existing body of SQLite-flavored queries across the metadata layer did not need a parallel Postgres rewrite.
- Both wrappers register with the Live Query Monitor (`backend.utils.sqlite_profiler._live_register`/`_live_deregister`) — Postgres metadata queries are not a blind spot in `/admin/queries`.

### 2. DuckLake's catalog must also be Postgres in multi-writer mode

Independently of metadata, `DUCKLAKE_CATALOG` must be a Postgres DSN whenever `INGEST_MODE=celery` (enforced by `config.validate_ingest_mode()`, see [ADR-14](14-ducklake-replacement.md)). These are two separate Postgres roles serving two separate concerns — cron/ingest bookkeeping vs. the commit-path catalog — that happen to both require a real multi-writer database once you have more than one writer process. They may point at the same Postgres instance (and even the same database — every DuckLake table is `ducklake_`-prefixed, so it does not collide with the metadata schema; `docker-compose.multipod.yml` shares one), but the code does not assume they do: `_ducklake_attach` reads `DUCKLAKE_CATALOG` and nothing else. It briefly fell back to `METADATA_DSN`, which made this claim false in `INGEST_MODE=sync`; the fallback was removed rather than the claim weakened, because in sync mode it would have silently planted the catalog inside the metadata database and abandoned the `.ducklake` file holding the real table state.

### 3. Cron work is split by write target, not by "internal vs. external" mode

A naive multi-pod cutover would route every scheduled job through Celery/RedBeat once "external" mode is on. That's wrong: jobs like rollup computation, local compaction, alert evaluation, and snapshot maintenance read and write the **pod-local** DuckDB file and cache — routing them to a worker pool would have them fight the backend's own readers for that file's single-writer lock, or simply operate on the wrong pod's data.

`backend/cron/scheduler.py` encodes the split explicitly:

```python
_REDBEAT_JOB_PREFIXES = (
    "log_discovery_", "commit_", "ledger_sweep_", "full_sync_", "gap_heal_",
    "rum_discovery_", "ledger_rum_sweep_",   # added by 587d0b5f (RUM ledger port)
)
```

Jobs whose name starts with one of these prefixes route to RedBeat (Celery's Redis-backed periodic scheduler) and run on worker processes — they only ever touch the ingest ledger and FOS, never a pod-local file. Everything else stays on the backend's own in-process APScheduler, in every mode, including "external." `_add_job()` enforces this at registration time; `get_job()` only returns `None` for RedBeat-routed job IDs (APScheduler-routed jobs are always locally introspectable).

### 4. Cron leases must be atomic under Postgres autocommit

SQLite's single-writer-per-file serialization made a check-then-insert lease acquisition (`SELECT count(*) ... WHERE status='running'`, then `INSERT`) accidentally race-free — the file lock made the two statements atomic in practice. Postgres, running in autocommit mode with genuinely concurrent connections, does not provide that for free: two pods can both pass the `SELECT count(*) == 0` check before either commits its `INSERT`, and both start the same job.

The fix is a partial unique index plus a single atomic statement, not a documented caveat:

```sql
CREATE UNIQUE INDEX idx_job_runs_running_lease ON job_runs(service_id, job_name) WHERE status = 'running'
```

`start_cron_run()` issues `INSERT ... ON CONFLICT (service_id, job_name) WHERE status = 'running' DO NOTHING` and checks `cursor.rowcount` to determine which caller actually won the lease. This is verified by a real multi-threaded concurrency test (`tests/core/test_cron_log_lease.py`), not just a unit test of the SQL string — two real threads race for the same lease and exactly one must win.

## Consequences

- A deployment can run single-pod/SQLite (default), or multi-pod with `METADATA_DSN` + `DUCKLAKE_CATALOG` both pointed at Postgres and `INGEST_MODE=celery`. There is no partial state where only one of the two is Postgres-backed and celery mode is enabled — `validate_ingest_mode()` refuses to boot in that configuration. (This was asserted here before the `METADATA_DSN` half of the gate existed; it is now enforced and covered by `tests/test_validate_ingest_mode.py`.)
- **"Multi-pod" in this ADR means the INGEST tier.** The serving tier — the backend Deployment that answers API requests — remains single-pod for reasons this ADR does not address: the per-service `.duckdb` file is process-exclusive, and several cron jobs write pod-local output under a cross-pod lease. See [ADR-18](18-serving-tier-single-pod.md) before scaling backend replicas past 1.
- The Postgres metadata schema is created at startup, by every process that needs it. `metadata/base.py` wires `_init_schema` as the SQLite pool's `schema_fn` only, so SQLite self-initializes and Postgres did not — making `scripts/setup_pg_schema.py` a mandatory deploy step, which, when skipped, produced a stack that booted clean and then failed every metadata query with `relation "cron_runs" does not exist`. `backend/core/metadata/pg_schema.py::ensure_pg_schema()` now closes that gap: the backend lifespan and each Celery worker's `worker_process_init` call it, it runs once per process, and it tolerates the `duplicate_table` / `duplicate_object` / `unique_violation` SQLSTATEs that concurrent pods can still produce from `CREATE ... IF NOT EXISTS`. The DDL lives in that module so the script and the boot path can never drift; the script remains the explicit ops entry point. Any table added by a SQLite migration rather than by `_SCHEMA` still needs an explicit DDL entry there (`slow_queries` and `committed_buffers` both do).
- Every future scheduled job must be classified into the RedBeat/APScheduler split at the point it's added — a job that touches the pod-local DuckDB file or cache but gets accidentally prefixed into `_REDBEAT_JOB_PREFIXES` will silently run on the wrong pod's data.
- Any future lease/dedup mechanism added to `job_runs` or a similar table must default to "assume concurrent writers," not "assume SQLite's file lock will save us" — that assumption already broke once here.

## Out of scope

- Read replicas / connection-pool sizing tuning for the Postgres metadata database in production (deployment-specific, not an architectural decision).
- Automatic failover between SQLite and Postgres metadata backends at runtime — the backend selects one at boot and does not switch.
