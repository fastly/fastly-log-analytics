"""Postgres metadata schema: the DDL, and the boot-time "ensure" that applies it.

``backend.core.metadata.base`` wires ``_init_schema`` as the *SQLite*
pool's ``schema_fn``, so a SQLite metadata store creates itself on first
use and a Postgres one never did. Skipping ``scripts/setup_pg_schema.py``
therefore produced a stack that booted clean and then failed every
metadata query with ``relation "cron_runs" does not exist``.
:func:`ensure_pg_schema` closes that gap: it runs once per process at
startup (API lifespan and Celery worker init) and is a cheap no-op when
``METADATA_DSN`` is unset.

The DDL lives here rather than in the script so both entry points share
one definition — ``scripts/setup_pg_schema.py`` is still the explicit ops
command and still exits nonzero on a real failure.

Concurrency: several pods can boot at once, and concurrent
``CREATE TABLE IF NOT EXISTS`` / ``CREATE INDEX IF NOT EXISTS`` can still
collide on the shared catalog (``duplicate_table`` / ``duplicate_object``
/ a ``pg_type`` unique violation) rather than being silently skipped.
Those specific SQLSTATEs mean "another pod won the race", which is
success; everything else is a genuine error and propagates. The pool runs
``autocommit=True``, so a failed statement does not poison the rest.
"""

from __future__ import annotations

import logging
import threading

logger = logging.getLogger(__name__)

# slow_queries is created by a SQLite migration, not _SCHEMA, so it needs an
# explicit PG definition. CREATE IF NOT EXISTS — never DROP: re-running setup
# against a populated database must be non-destructive.
_SLOW_QUERIES_DDL = """
CREATE TABLE IF NOT EXISTS slow_queries (
    id SERIAL PRIMARY KEY,
    query_id TEXT NOT NULL,
    db_type TEXT NOT NULL,
    service_id TEXT,
    started_at_utc REAL NOT NULL,
    ended_at_utc REAL NOT NULL,
    duration_ms REAL NOT NULL,
    outcome TEXT NOT NULL,
    sql_preview TEXT NOT NULL,
    sql_full TEXT,
    sql_len INTEGER NOT NULL,
    attr_kind TEXT NOT NULL,
    attr_label TEXT NOT NULL,
    attr_principal_id TEXT,
    attr_caller_qualname TEXT NOT NULL,
    attr_caller_file TEXT NOT NULL,
    attr_request_path TEXT,
    attr_request_id TEXT,
    attr_cron_job TEXT,
    attr_cron_run_id TEXT,
    attr_pool_slot TEXT,
    error_type TEXT,
    error_message TEXT,
    peak_memory_mb REAL
)
"""


# committed_buffers is created by SQLite migration 004, not _SCHEMA, so the
# Postgres path never had it — while pg_connection._IGNORE_TABLES already
# rewrites its INSERT OR IGNORE, i.e. the dialect seam was built assuming the
# table exists. Without this DDL, Postgres metadata mode loses the durable
# buffer-commit checkpoint that exists to stop a crash between
# table.append() and tombstone_buffer_files() re-appending the same rows
# (~2x row duplication for the affected hour). CREATE IF NOT EXISTS — never
# DROP.
#
# NOTE: no service_id column, mirroring SQLite. Migration 015 added
# service_id to cron_runs and local_compacted_files ONLY, and
# filter_uncommitted_buffers() queries this table with no service_id
# predicate, so under one shared database this table is cross-tenant by
# basename. Safe only because buffer basenames are uuid-derived. See ADR-18.
_COMMITTED_BUFFERS_DDL = """
CREATE TABLE IF NOT EXISTS committed_buffers (
    buffer_filename TEXT PRIMARY KEY,
    committed_at TEXT NOT NULL DEFAULT (to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))
)
"""

# SQLSTATEs that mean "a concurrently-booting pod created this first".
# 42P07 duplicate_table (covers indexes too), 42710 duplicate_object,
# 23505 unique_violation (the pg_type/pg_class catalog race).
_RACE_SQLSTATES = frozenset({"42P07", "42710", "23505"})

_ensured = False
_ensure_lock = threading.Lock()


def _to_postgres(sql: str) -> str:
    """Rewrite a SQLite DDL statement into Postgres dialect."""
    pg_sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    pg_sql = pg_sql.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
    pg_sql = pg_sql.replace("SERIAL PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    pg_sql = pg_sql.replace("DEFAULT (datetime('now'))", "DEFAULT (current_timestamp AT TIME ZONE 'UTC')")
    pg_sql = pg_sql.replace(
        "DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
        "DEFAULT (to_char(current_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"'))",
    )
    # Drop SQLite's STRICT table qualifier.
    pg_sql = pg_sql.replace(", STRICT", "")
    pg_sql = pg_sql.replace(" STRICT", "")
    return pg_sql


def pg_schema_statements() -> list[str]:
    """Every DDL statement the Postgres metadata store needs, in order."""
    from backend.core.metadata.base import _SCHEMA
    from backend.core.metadata.usage_log_db import _SCHEMA as _USAGE_SCHEMA
    from backend.core.share_db.schema import _SCHEMA as _SHARE_DB_SCHEMA

    statements = [_to_postgres(sql) for sql in _SCHEMA + _USAGE_SCHEMA + _SHARE_DB_SCHEMA]
    # SQLite triggers (usage_log rollup maintenance) don't translate to PG
    # and the PG write path maintains the rollup itself.
    statements = [s for s in statements if not s.strip().startswith("CREATE TRIGGER")]
    statements.append(_SLOW_QUERIES_DDL)
    statements.append(_COMMITTED_BUFFERS_DDL)
    return statements


def _is_concurrent_create(exc: Exception) -> bool:
    return getattr(exc, "sqlstate", None) in _RACE_SQLSTATES


def apply_pg_schema(cur) -> tuple[int, int]:
    """Apply the schema through ``cur``. Returns ``(applied, raced)``.

    Statements that lost a race against a concurrently-booting pod are
    counted in ``raced`` and skipped; every other failure raises with the
    offending statement attached so the caller can print it.
    """
    applied = 0
    raced = 0
    for pg_sql in pg_schema_statements():
        try:
            cur.execute(pg_sql)
            applied += 1
        except Exception as e:
            if _is_concurrent_create(e):
                raced += 1
                continue
            raise RuntimeError(
                f"failed to apply schema statement:\n{pg_sql.strip()}\n-> {type(e).__name__}: {e}"
            ) from e
    return applied, raced


def ensure_pg_schema(*, force: bool = False) -> bool:
    """Idempotently create the Postgres metadata schema. Once per process.

    Returns True when the DDL was applied on this call, False when it was
    skipped (no ``METADATA_DSN``, or already ensured in this process).
    Failures are logged at CRITICAL and swallowed: crash-looping a pod on
    a transient Postgres blip during boot is worse than booting degraded,
    and ``_ensured`` is only latched on success so a later caller retries.
    """
    global _ensured

    from backend.core.metadata import pg_connection

    if not pg_connection.is_postgres():
        return False
    if _ensured and not force:
        return False

    with _ensure_lock:
        if _ensured and not force:
            return False
        try:
            pool = pg_connection.get_pg_pool()
            with pool.connection() as conn, conn.cursor() as cur:
                applied, raced = apply_pg_schema(cur)
        except Exception as e:
            logger.critical(
                "[metadata] Postgres schema bootstrap FAILED — metadata queries will fail until this is "
                "resolved. Fix the database and restart, or run `uv run python scripts/setup_pg_schema.py`. "
                "Cause: %s",
                e,
            )
            return False
        _ensured = True

    logger.info(
        "[metadata] Postgres metadata schema ensured (%d statements applied, %d already created by another pod).",
        applied,
        raced,
    )
    return True


def reset_ensure_flag_for_tests() -> None:
    """Drop the once-per-process latch so a test can exercise the boot path."""
    global _ensured
    _ensured = False
