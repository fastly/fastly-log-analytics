"""Migrate per-service SQLite metadata into a unified Postgres database.

Reads every ``<service_id>.metadata.db`` and ``<service_id>.usage_log.db``
in the given backup directory and inserts their rows into Postgres
(``METADATA_DSN``), stamping ``service_id`` on every row so the unified
schema can partition per-service state back apart.

Behavioral guarantees:

- ``service_id`` is stamped on every inserted row (derived from the source
  filename; a row's own non-NULL service_id wins) for every PG table that
  carries the column.
- The SQLite surrogate ``id`` is dropped on insert so Postgres assigns its
  own sequence values — per-service SQLite ids collide across services and
  would desync the PG sequences.
- ``--dry-run`` executes the full migration inside a transaction, prints the
  per-table verification counts, then rolls everything back.
- After each table, the SQLite source row count and the resulting PG row
  count (scoped to the service where the table carries service_id) are
  printed side by side.
"""

import argparse
import glob
import os
import sqlite3
import sys

import psycopg

from backend.core.metadata.pg_connection import get_pg_pool

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

# Tables that live in the dedicated <service_id>.usage_log.db file
# (see backend/core/metadata/usage_log_db.py).
USAGE_LOG_TABLES = [
    "usage_log",
    "usage_log_hourly_summary",
    "telemetry_queries",
    "telemetry_sections",
]


def _pg_columns(pg_cur, table: str) -> set[str]:
    pg_cur.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
        (table,),
    )
    return {r["column_name"] for r in pg_cur.fetchall()}


def _migrate_table(sqlite_cur, pg_cur, table: str, service_id: str) -> None:
    try:
        sqlite_cur.execute(f"SELECT * FROM {table}")  # noqa: S608 - table from static allowlist
        rows = sqlite_cur.fetchall()
    except sqlite3.OperationalError:
        return  # table absent in this (older) source DB

    if not rows:
        return

    pg_cols = _pg_columns(pg_cur, table)
    if not pg_cols:
        print(f"  {table}: SKIPPED — table does not exist in Postgres (run setup_pg_schema.py)")
        return

    src_cols = list(rows[0].keys())
    # Drop the SQLite surrogate id: per-service ids collide across services
    # and inserting them explicitly desyncs the PG sequence.
    out_cols = [c for c in src_cols if c != "id" and c in pg_cols]
    stamp_service_id = "service_id" in pg_cols
    if stamp_service_id and "service_id" not in out_cols:
        out_cols.append("service_id")

    dropped = [c for c in src_cols if c != "id" and c not in pg_cols]
    if dropped:
        print(f"  {table}: WARNING — columns missing in Postgres, not migrated: {dropped}")

    def _row_values(row: sqlite3.Row) -> tuple:
        vals = []
        for col in out_cols:
            if col == "service_id":
                own = row["service_id"] if "service_id" in row.keys() else None
                vals.append(own if own is not None else service_id)
            else:
                vals.append(row[col])
        return tuple(vals)

    cols_str = ", ".join(out_cols)
    placeholders = ", ".join(["%s"] * len(out_cols))
    insert_sql = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"  # noqa: S608
    pg_cur.executemany(insert_sql, [_row_values(r) for r in rows])

    # Per-table verification: source count vs resulting PG count.
    if stamp_service_id:
        pg_cur.execute(f"SELECT count(*) AS n FROM {table} WHERE service_id = %s", (service_id,))  # noqa: S608
        scope = f"service_id={service_id}"
    else:
        pg_cur.execute(f"SELECT count(*) AS n FROM {table}")  # noqa: S608
        scope = "all rows"
    pg_count = pg_cur.fetchone()["n"]
    print(f"  {table}: sqlite={len(rows)} pg({scope})={pg_count}")


def migrate_db(db_path: str, suffix: str, tables: list[str], pg_cur) -> None:
    service_id = os.path.basename(db_path).removesuffix(suffix)
    print(f"Migrating {service_id} from {db_path}...")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    try:
        for table in tables:
            _migrate_table(cur, pg_cur, table, service_id)
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backup_dir", help="Directory containing .metadata.db / .usage_log.db files")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the full migration + verification inside a transaction, then roll back",
    )
    args = parser.parse_args()

    if not os.environ.get("METADATA_DSN"):
        print("Error: METADATA_DSN must be set.")
        sys.exit(1)

    metadata_dbs = sorted(glob.glob(os.path.join(args.backup_dir, "*.metadata.db")))
    usage_dbs = sorted(glob.glob(os.path.join(args.backup_dir, "*.usage_log.db")))
    if not metadata_dbs and not usage_dbs:
        print(f"Error: no *.metadata.db or *.usage_log.db files found in {args.backup_dir}")
        sys.exit(1)

    pool = get_pg_pool()
    with pool.connection() as pg_con:
        # The pool is autocommit; wrap the whole migration in one explicit
        # transaction so a failure (or --dry-run) leaves Postgres untouched.
        with pg_con.transaction() as tx:
            with pg_con.cursor() as pg_cur:
                for db_file in metadata_dbs:
                    migrate_db(db_file, ".metadata.db", METADATA_TABLES, pg_cur)
                for db_file in usage_dbs:
                    migrate_db(db_file, ".usage_log.db", USAGE_LOG_TABLES, pg_cur)
            if args.dry_run:
                print("Dry run: rolling back all inserts.")
                raise psycopg.Rollback(tx)

    print("Dry run complete (no changes applied)." if args.dry_run else "Migration complete.")


if __name__ == "__main__":
    main()
