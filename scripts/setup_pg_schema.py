"""Create the unified Postgres metadata schema (METADATA_DSN).

Rewrites the SQLite ``_SCHEMA`` statements (metadata + usage_log) into
Postgres dialect and applies them. Safe to re-run: every statement is
``IF NOT EXISTS``-style and nothing is dropped — an existing, populated
database is left intact. Any DDL failure is printed with the offending
statement and the script exits nonzero (errors are never swallowed).
"""

import os
import sys

from backend.core.metadata.base import _SCHEMA
from backend.core.metadata.pg_connection import get_pg_pool
from backend.core.metadata.usage_log_db import _SCHEMA as _USAGE_SCHEMA
from backend.core.share_db.schema import _SCHEMA as _SHARE_DB_SCHEMA

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


def setup() -> None:
    if not os.environ.get("METADATA_DSN"):
        print("Error: METADATA_DSN must be set.")
        sys.exit(1)

    statements = [_to_postgres(sql) for sql in _SCHEMA + _USAGE_SCHEMA + _SHARE_DB_SCHEMA]
    # SQLite triggers (usage_log rollup maintenance) don't translate to PG
    # and the PG write path maintains the rollup itself.
    statements = [s for s in statements if not s.strip().startswith("CREATE TRIGGER")]
    statements.append(_SLOW_QUERIES_DDL)
    statements.append(_COMMITTED_BUFFERS_DDL)

    pool = get_pg_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            for pg_sql in statements:
                try:
                    cur.execute(pg_sql)
                except Exception as e:
                    print("Error applying schema statement:")
                    print(pg_sql.strip())
                    print(f"-> {type(e).__name__}: {e}")
                    sys.exit(1)

    print(f"Schema applied ({len(statements)} statements).")


if __name__ == "__main__":
    setup()
