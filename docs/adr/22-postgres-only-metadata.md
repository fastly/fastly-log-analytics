# ADR-22 — PostgreSQL-Only Unified Metadata and Catalog Storage

**Status:** Accepted  
**Decided by:** v3.0.0 PostgreSQL unification, superseding [ADR-15](15-multi-writer-topology.md) and amending [ADR-14](14-ducklake-replacement.md)  
**Date:** 2026-09-24  

---

## Context

[ADR-15](15-multi-writer-topology.md) introduced a dual-mode storage topology: single-pod "standard" deployments used per-service SQLite files (`data/services/<id>.metadata.db`, `usage_log.db`, `remote_share.db`, `system_metrics.db`), while multi-pod "high_throughput" deployments used PostgreSQL via `METADATA_DSN` and a Postgres DuckLake catalog.

In practice, maintaining dual-mode branching across every persistence layer caused severe operational and architectural liabilities:
1. **Concurrency and Lock Contention:** SQLite's file-level locking (`SQLITE_BUSY`, WAL writer locks) caused cron sync writers to block reader endpoints for seconds at a time.
2. **Dialect Divergence and Hidden Bugs:** Complex runtime SQL rewriting (`_rewrite_sql`, `_rewrite_insert_or`) was required to bridge SQLite and PostgreSQL semantics. Code tested under SQLite frequently encountered subtle PostgreSQL dialect gaps (such as `DOUBLE PRECISION` vs `REAL`, `rowid` semantics, or datetime string functions) in production.
3. **Data Integrity Risks:** SQLite triggers maintaining summary rollups (`usage_log_hourly_summary`) were absent under PostgreSQL, requiring duplicate maintenance paths.
4. **Maintenance Burden:** A custom connection pooling layer (`backend/core/sqlite_pool.py`), file-corruption quarantine logic, and ad-hoc WAL checkpointing permeated seven different packages.

As part of the breaking `v3.0.0` major release, we decided to eliminate SQLite entirely and standardize all runtime environments, automated tests, and CI pipelines exclusively on PostgreSQL 16.

---

## Decision

PostgreSQL 16 is now the sole, mandatory metadata and catalog storage engine for all deployment modes (`DEPLOYMENT_MODE=standard` and `DEPLOYMENT_MODE=high_throughput`). Every deployment requires valid `METADATA_DSN` and `DUCKLAKE_CATALOG` environment variables.

### 1. File-by-File Migration Summary

- **`backend/core/metadata/base.py` & `cron_log.py`:**
  - Deleted the SQLite connection pool, `ThreadLocalPool` references, and PRAGMA configuration.
  - `get_con()`, `get_con_readonly()`, `release_thread_connection()`, and `close_all_connections()` route directly and unconditionally to `backend.core.metadata.pg_connection`.
  - SQLite retry-on-locked error handlers were made passthroughs or updated to handle `psycopg.Error`.
- **`backend/core/metadata/usage_log_db.py` & `usage_log.py`:**
  - Removed SQLite connection machinery from `usage_log_db.py`.
  - Replicated the historical SQLite `AFTER INSERT` and `AFTER DELETE` rollup triggers in Python:
    - `log_usage_calls` and `log_synthetic_usage` compute hourly rollups in memory and execute atomic `ON CONFLICT (service_id, hour, operation_class, operation_type) DO UPDATE SET count = ...` upserts in the same transaction as raw row inserts.
    - `reconcile_fastly_stats` calculates prior reconciliation rows, decrements them from `usage_log_hourly_summary`, inserts new reconciliation rows, and updates the summary.
  - Removed all `PRAGMA wal_checkpoint` calls.
- **`backend/core/share_db/`:**
  - Completely migrated `connection.py`, `schema.py`, `audit.py`, `invites.py`, `passcode.py`, `sessions.py`, `settings.py`, and `tos.py` to PostgreSQL.
  - Removed the SQLite file-quarantine recovery mechanism (`get_safe_share_db_connection`, `_recovery_marker`).
  - Retained `_SCHEMA` definitions so `pg_schema.py` continues to bootstrap live-share tables in PostgreSQL.
- **`backend/core/metric_snapshots.py`:**
  - Deleted SQLite file connection logic (`_get_con`, `_open_readonly`, `_DDL`).
  - Standardized all snapshot writes, history queries, and retention purging directly on PostgreSQL.
  - Updated `teardown()` to truncate the table for test isolation.
- **`backend/utils/ngwaf_bot_cache.py` & `backend/utils/rdns_cache.py`:**
  - Added table and index DDL for `ngwaf_bots`, `ngwaf_sync_state`, and `rdns` to `backend/core/metadata/pg_schema.py`.
  - Replaced `sqlite_pool.open_small_cache_db` with `get_pg_thread_connection()`.
  - Replaced `aiosqlite` in `rdns_cache.py` with threaded batch execution (`asyncio.to_thread(_bulk_update_sync)`).
- **`backend/core/sqlite_pool.py`:**
  - Completely deleted `sqlite_pool.py` and its unit tests (`tests/core/test_sqlite_pool.py`).
- **`backend/core/iceberg/_ducklake.py` & `backend/config.py`:**
  - Deleted local `.ducklake` file fallback in `_ducklake_attach()`; `DUCKLAKE_CATALOG` must be a Postgres DSN in all deployment modes.
  - `config.validate_deployment_mode()` now requires `METADATA_DSN` and `DUCKLAKE_CATALOG` in both `standard` and `high_throughput` modes.
- **Infrastructure (`docker-compose.yml`, `docker-compose.prod.yml`):**
  - Added a `postgres:16-alpine` service to local `docker-compose.yml` (on bridge network `app-network`, healthy dependency for `backend`).
  - Added a hardened `postgres:16-alpine` service to `docker-compose.prod.yml` using `network_mode: host`, loopback-only binding (`listen_addresses=127.0.0.1`), persistent storage on `/mnt/app-data/postgres`, and no default fallback password.

---

## Architectural Rulings: Retained Concurrency Mechanisms

During implementation, two mechanisms originally proposed for deletion were evaluated and **deliberately retained**:

### 1. DuckLake Attach-Conflict Retry Loop (`_ATTACH_CONFLICT_RETRY_ATTEMPTS` in `_ducklake.py`)
- **Plan proposed:** Delete the retry loop as an artifact of SQLite file locks.
- **Ruling:** **RETAINED.**
- **Rationale:** The retry loop guards against DuckDB's in-process single-alias catalog conflict (`"unique file handle conflict"`, `"already attached by database"`). This conflict occurs when concurrent connections in the same process attach or re-attach the `lake` alias. Because this is a DuckDB engine constraint rather than a storage backend constraint, the retry loop is required under PostgreSQL catalogs as well.

### 2. DuckDB Pool-Release Eager Detach (`_ducklake_detach()` in `duckdb_pool.py::release()`)
- **Plan proposed:** Remove `_ducklake_detach()` on connection release.
- **Ruling:** **RETAINED.**
- **Rationale:** DuckDB does not allow concurrent connections in the same process to attach the same catalog alias in conflicting modes (e.g., one connection attached `read_only=True` while another attaches `read_only=False`). If an idle pooled reader connection retains `lake` attached, an incoming background writer attempting a read-write attach triggers a mode conflict. Detaching `lake` during `release()` ensures idle pool connections never block active writers.

---

## Consequences

### Positive
- **Complete Elimination of Database Lock Contention:** Crons and API readers run concurrently without `SQLITE_BUSY` deadlocks.
- **Unified Codebase:** Eliminated dual-mode code branches, SQLite connection pools, and WAL pragmas.
- **Dialect Consistency:** All metadata queries run against identical PostgreSQL schema and semantics across local dev, CI, and production.
- **Stateless Application Containers:** `/app/data` is no longer mounted to the host on macOS/Colima, eliminating cross-VM filesystem synchronization issues.

### Operational Changes
- **PostgreSQL Dependency:** Local development now requires a running PostgreSQL container (`docker compose up postgres` or `make start-dev`). Zero-dependency SQLite execution is no longer supported.
- **Test Isolation:** Pytest suites (`tests/conftest.py`) automatically isolate test execution by provisioning dedicated per-worker PostgreSQL databases (`ducklake_test_<worker_id>`).

---

## Out of Scope & Follow-ups

1. **Typing Refinements:** Several functions currently accept `Any = None` for connection parameters; a future refactor can standardize these on `PgConnectionWrapper`.
2. **Test Shims Deprecation:** Compatibility attributes (`_DATA_DIR`, `_initialized`, `_local`) retained on modules to avoid breaking legacy fixtures can be pruned in a future cleanup cycle.
