"""Roll unified Postgres metadata back out into per-service SQLite files.

Partitions service-scoped rows (by ``service_id``, or ``source_name`` for
audit_logs) into ``<service_id>.metadata.db`` / ``<service_id>.usage_log.db``
files in the output directory. Tables with neither scoping column are
exported whole into ``_global.metadata.db`` / ``_global.usage_log.db`` so no
data is silently left behind in Postgres.

Refuses to write into a target DB file that already exists unless ``--force``
is passed (a partial earlier rollback mixed with a fresh one is worse than
either).
"""

import argparse
import os
import sqlite3
import sys

from backend.core.metadata.base import _SCHEMA
from backend.core.metadata.pg_connection import get_pg_pool
from backend.core.metadata.usage_log_db import _SCHEMA as _USAGE_SCHEMA

GLOBAL_BUCKET = "_global"

# Tables that live in <service_id>.metadata.db.
METADATA_TABLES = [
    "sources",
    "ingested_files",
    "ingested_files_summary",
    "ingest_in_flight",
    "ingest_ledger",
    "job_runs",
    "quarantined_files",
    "alerts",
    "cron_runs",
    "local_compacted_files",
    "slow_queries",
    "scoring_labels",
    "scoring_audit",
    "views",
    "audit_logs",
    "asn_names",
]

# Tables that live in the dedicated <service_id>.usage_log.db file.
USAGE_LOG_TABLES = [
    "usage_log",
    "usage_log_hourly_summary",
    "telemetry_queries",
    "telemetry_sections",
]

# slow_queries is created by a SQLite migration, not _SCHEMA.
_SLOW_QUERIES_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS slow_queries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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


def _filter_column(pg_cur, table: str) -> str | None:
    """service_id / source_name if the PG table carries one, else None (global)."""
    pg_cur.execute(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_name = %s AND column_name IN ('service_id', 'source_name')",
        (table,),
    )
    cols = {r["column_name"] for r in pg_cur.fetchall()}
    if "service_id" in cols:
        return "service_id"
    if "source_name" in cols:
        return "source_name"
    return None


def _open_target(db_path: str, schema: list[str], extra_ddl: str | None, force: bool) -> sqlite3.Connection:
    if os.path.exists(db_path):
        if not force:
            print(f"Error: target DB already exists: {db_path}. Re-run with --force to overwrite it.")
            sys.exit(1)
        os.remove(db_path)

    con = sqlite3.connect(db_path)
    cur = con.cursor()
    for sql in schema:
        cur.execute(sql)
    if extra_ddl:
        cur.execute(extra_ddl)
    # Drop the schema's triggers for the duration of the restore: the
    # usage_log INSERT trigger maintains usage_log_hourly_summary, and we
    # restore the summary rows verbatim — leaving the trigger active would
    # double-count. The app re-creates its triggers on the next connection
    # (the schema is applied on every init).
    cur.execute("SELECT name FROM sqlite_master WHERE type = 'trigger'")
    for (trigger_name,) in cur.fetchall():
        cur.execute(f'DROP TRIGGER IF EXISTS "{trigger_name}"')
    return con


def _sqlite_columns(sqlite_cur, table: str) -> list[str]:
    sqlite_cur.execute(f"PRAGMA table_info({table})")
    return [r[1] for r in sqlite_cur.fetchall()]


def _export_rows(pg_rows, table: str, sqlite_con: sqlite3.Connection) -> None:
    if not pg_rows:
        return
    sqlite_cur = sqlite_con.cursor()
    target_cols = set(_sqlite_columns(sqlite_cur, table))
    if not target_cols:
        print(f"  {table}: SKIPPED — table missing from the SQLite schema")
        return

    src_cols = list(pg_rows[0].keys())
    out_cols = [c for c in src_cols if c in target_cols]
    dropped = [c for c in src_cols if c not in target_cols]
    if dropped:
        print(f"  {table}: NOTE — PG-only columns not exported: {dropped}")

    cols_str = ", ".join(out_cols)
    placeholders = ", ".join(["?"] * len(out_cols))
    insert_sql = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"  # noqa: S608
    sqlite_cur.executemany(insert_sql, [tuple(row[c] for c in out_cols) for row in pg_rows])
    print(f"  {table}: {len(pg_rows)} rows exported.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output_dir", help="Directory to save the resulting .metadata.db / .usage_log.db files")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite target DB files that already exist in output_dir",
    )
    args = parser.parse_args()

    if not os.environ.get("METADATA_DSN"):
        print("Error: METADATA_DSN must be set.")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    pool = get_pg_pool()
    with pool.connection() as pg_con:
        with pg_con.cursor() as pg_cur:
            # Discover the set of services across the major scoped tables.
            services: set[str] = set()
            for t in ["cron_runs", "ingested_files", "audit_logs", "slow_queries", "job_runs", "ingest_ledger"]:
                col = _filter_column(pg_cur, t)
                if col is None:
                    continue
                pg_cur.execute(f"SELECT DISTINCT {col} AS svc FROM {t} WHERE {col} IS NOT NULL")  # noqa: S608
                for row in pg_cur.fetchall():
                    services.add(row["svc"])

            print(f"Found services: {sorted(services)}")

            # Classify tables: scoped ones partition per service; the rest
            # export whole into the _global files.
            scoped: dict[str, str] = {}
            global_tables: list[str] = []
            for table in METADATA_TABLES + USAGE_LOG_TABLES:
                col = _filter_column(pg_cur, table)
                if col:
                    scoped[table] = col
                else:
                    global_tables.append(table)

            buckets = sorted(services) + [GLOBAL_BUCKET]
            for svc in buckets:
                is_global = svc == GLOBAL_BUCKET

                for suffix, tables, schema, extra in (
                    (".metadata.db", METADATA_TABLES, _SCHEMA, _SLOW_QUERIES_SQLITE_DDL),
                    (".usage_log.db", USAGE_LOG_TABLES, list(_USAGE_SCHEMA), None),
                ):
                    export: list[tuple[str, list]] = []
                    for table in tables:
                        if is_global:
                            if table not in global_tables:
                                continue
                            pg_cur.execute(f"SELECT * FROM {table}")  # noqa: S608
                        else:
                            if table not in scoped:
                                continue
                            pg_cur.execute(
                                f"SELECT * FROM {table} WHERE {scoped[table]} = %s",  # noqa: S608
                                (svc,),
                            )
                        rows = pg_cur.fetchall()
                        if rows:
                            export.append((table, rows))

                    if not export:
                        continue

                    db_path = os.path.join(args.output_dir, f"{svc}{suffix}")
                    label = "global (non-service-scoped) tables" if is_global else svc
                    print(f"Rolling back {label} to {db_path}...")
                    sqlite_con = _open_target(db_path, schema, extra, args.force)
                    try:
                        for table, rows in export:
                            _export_rows(rows, table, sqlite_con)
                        sqlite_con.commit()
                    finally:
                        sqlite_con.close()

    print("Rollback complete.")


if __name__ == "__main__":
    main()
