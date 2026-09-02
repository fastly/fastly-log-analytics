"""Lightweight schema migration framework for per-service SQLite metadata DBs.

Background — why not Alembic?
    Alembic assumes a single canonical DB with a shared linear history. Our
    operational metadata lives in ``data/services/{service_id}.metadata.db``
    — one file per service, created lazily on first ingest. Alembic's
    ``alembic upgrade head`` workflow doesn't map onto "open this file for
    the first time today; bring it to current". A handful of Python
    callbacks keyed by version number is the right size.

Design:
    * ``PRAGMA user_version`` (a 32-bit integer baked into the SQLite file
      header) tracks the current schema version. Zero overhead, no extra
      table, atomic with the migration that bumps it.
    * Migrations are Python callbacks keyed by version number in
      ``MIGRATIONS``. Each must be IDEMPOTENT — fresh DBs created from the
      latest ``_SCHEMA`` already have everything, so re-running a migration
      after init must be a no-op.
    * Migrations run inside a transaction. Failure rolls back; the version
      is NOT bumped, so the next open retries.

Adding a migration:
    1. Write a function ``_migration_{NNN}_{description}(con)`` that mutates
       the schema. Use ``_has_column`` / ``_has_table`` for idempotency.
    2. Add a corresponding ``CREATE TABLE`` / ``ALTER TABLE ... ADD COLUMN``
       to ``backend.core.metadata._SCHEMA`` so fresh DBs already have it.
    3. Register the function in ``MIGRATIONS`` with the next available
       integer version.
    4. Add a test in ``tests/core/test_metadata_db_migrations.py`` that
       seeds a pre-migration DB and asserts the post-migration shape.

Pinned by ``tests/core/test_metadata_db_migrations.py``.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Callable

logger = logging.getLogger(__name__)


# ── Introspection helpers ────────────────────────────────────────────────────


def _has_column(con: sqlite3.Connection, table: str, column: str) -> bool:
    """True iff ``table.column`` exists. Returns False if the table itself
    does not exist (so callers don't need a separate table-existence check
    before adding a column to a freshly-renamed table).
    """
    try:
        cols = con.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.OperationalError:
        return False
    return any((c[1] if isinstance(c, tuple) else c["name"]) == column for c in cols)


def _has_table(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return row is not None


# ── Migrations ───────────────────────────────────────────────────────────────


def _migration_001_add_ingested_files_error_count(con: sqlite3.Connection) -> None:
    """Add ``ingested_files.error_count`` (NULL-fill counter per file).

    Closes the second half of the TESTING_PLAN_3 item-3 type-mismatch
    contract: when ``read_json_auto(..., ignore_errors=true)`` NULLs a
    type-mismatched cell, surface that drift as an operational metric
    rather than a silent data-quality issue.
    """
    if _has_column(con, "ingested_files", "error_count"):
        return
    con.execute("ALTER TABLE ingested_files ADD COLUMN error_count INTEGER DEFAULT 0")


def _migration_002_add_ingested_files_file_date(con: sqlite3.Connection) -> None:
    """Add ``ingested_files.file_date`` (DATE parsed from filename) + index.

    Backfills via the same GLOB-validated substr/instr pattern used at
    runtime by ``get_log_accounting_counts``: locate the first 'T' in the
    filename (the Fastly emit-time marker) and use the 10 chars before it
    when they match YYYY-MM-DD. Filenames that don't match the canonical
    Fastly basename get NULL — callers must treat the column as optional.

    The composite index ``(source_name, file_date)`` lets per-day usage
    queries scan only the date range they need instead of walking every
    row for the source and computing the date per-row via substr — which
    the existing ``(source_name, ingested_at)`` index can't help with
    because the bucket extraction wraps the column in a function.
    """
    if not _has_column(con, "ingested_files", "file_date"):
        con.execute("ALTER TABLE ingested_files ADD COLUMN file_date DATE")
    con.execute(
        """
        UPDATE ingested_files
        SET file_date = substr(file_name, instr(file_name, 'T') - 10, 10)
        WHERE file_date IS NULL
          AND instr(file_name, 'T') >= 11
          AND substr(file_name, instr(file_name, 'T') - 10, 10)
              GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_ingested_files_source_date ON ingested_files(source_name, file_date)")


def _migration_004_committed_buffers(con: sqlite3.Connection) -> None:
    """Create ``committed_buffers`` — durable checkpoint that a buffer
    parquet was successfully appended to Iceberg.

    Closes the dup-creating race in ``backend.core.iceberg.buffer
    .commit_buffer`` between ``table.append(combined)`` (writes Iceberg
    snapshot) and ``tombstone_buffer_files(...)`` (marks the buffer file
    as consumed). A crash between those two steps used to leave the
    buffer file active, causing the next commit tick to re-append the
    same rows — observable as ~2× row duplication for the affected
    hour. With this checkpoint, the next tick sees the
    ``committed_buffers`` row, skips the re-append, and tombstones the
    buffer to close the loop.

    Why SQLite, not a sidecar marker file on disk: a single fsync on a
    SQLite WAL commit beats N marker files written/synced individually,
    and bulk lookups (`WHERE buffer_filename IN (...)`) at the start of
    every commit tick are cheap. Per-service DB (same place as
    ``ingested_files``), so the bucket-scoped lifecycle matches.

    ``filename`` is the BASENAME only (e.g. ``batch_abc123def456.parquet``)
    — the parent directory is implicit per the per-service buffer dir.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS committed_buffers (
            buffer_filename TEXT PRIMARY KEY,
            committed_at    TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )


def _migration_005_slow_queries(con: sqlite3.Connection) -> None:
    """Create ``slow_queries`` — durable per-service history of SQL queries
    whose ``duration_ms`` exceeded the persistence threshold.

    Why: the live ``query_registry`` only holds the most recent 2000
    completed queries (in-memory ring buffer). That's ~10-30 minutes of
    history on a busy service and zero history across restarts. The
    Notable Slow Queries panel becomes empty every restart and can't
    answer "what was slow yesterday?". This table is the persistent
    backing store; the registry continues to serve live + most-recent
    reads (cheap memory deque), while this SQLite table answers any
    query past that window.

    Writer: ``query_registry.deregister`` calls ``insert_slow_query``
    inline ONLY when ``duration_ms >= _SLOW_QUERY_PERSIST_THRESHOLD_MS``
    (default 100 ms). Filtering at the hot path means most queries (the
    sub-100ms majority) pay zero SQLite cost; the ones we DO persist
    are already slow enough that a 1-2 ms WAL append is invisible.

    Reader: ``GET /api/admin/slow-queries?since_hours=...&threshold_ms=...``.

    Retention: 7 days by default, governed by ``metadata_cleanup``.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS slow_queries (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id             INTEGER NOT NULL,
            db_type              TEXT    NOT NULL,
            service_id           TEXT,
            started_at_utc       REAL    NOT NULL,
            ended_at_utc         REAL    NOT NULL,
            duration_ms          REAL    NOT NULL,
            outcome              TEXT    NOT NULL,
            sql_preview          TEXT    NOT NULL,
            sql_full             TEXT,
            sql_len              INTEGER NOT NULL DEFAULT 0,

            attr_kind            TEXT    NOT NULL,
            attr_label           TEXT    NOT NULL,
            attr_principal_id    TEXT,
            attr_caller_qualname TEXT    NOT NULL,
            attr_caller_file     TEXT    NOT NULL,
            attr_request_path    TEXT,
            attr_request_id      TEXT,
            attr_cron_job        TEXT,
            attr_cron_run_id     TEXT,
            attr_pool_slot       TEXT,

            error_type           TEXT,
            error_message        TEXT,
            peak_memory_mb       REAL
        )
        """
    )
    # Time-descending lookups dominate the read pattern. A descending
    # index on ``started_at_utc`` backs the WHERE range filter without
    # a sort step.
    con.execute("CREATE INDEX IF NOT EXISTS idx_slow_queries_started_at ON slow_queries(started_at_utc DESC)")
    # Secondary index for the "slowest of the last 7d" query — the panel
    # also offers a duration-DESC sort variant.
    con.execute("CREATE INDEX IF NOT EXISTS idx_slow_queries_duration ON slow_queries(duration_ms DESC)")


def _migration_006_slow_queries_count_covering_index(con: sqlite3.Connection) -> None:
    """Covering index for ``count_slow_queries`` (admin-nav badge SELECT).

    Today ``idx_slow_queries_started_at(started_at_utc DESC)`` backs the
    ``WHERE started_at_utc >= ?`` range, but SQLite then walks each
    candidate row to apply the ``duration_ms >= ?`` predicate. Adding
    ``duration_ms`` to the index lets the COUNT resolve from the index
    alone — index-only scan, no row reads.

    IF NOT EXISTS so it's safe on re-run; the existing two indexes from
    v5 remain (different sort variants), so this is purely additive.
    """
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_slow_queries_count_filter ON slow_queries(started_at_utc DESC, duration_ms)"
    )


def _migration_007_quarantined_files(con: sqlite3.Connection) -> None:
    """Create ``quarantined_files`` — tracks raw .gz files copied to the
    ``errors/`` FOS prefix because they contained corrupt/invalid lines.

    Files with any corrupt lines are copied before deletion so operators
    can diagnose patterns in log corruption (edge misconfig, encoding
    issues, truncated writes). A sidecar ``.meta.json`` in FOS carries
    the bad-line samples; this table indexes the quarantined files for
    admin listing and 14-day auto-purge.
    """
    if _has_table(con, "quarantined_files"):
        return
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quarantined_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            source_name TEXT NOT NULL,
            fos_key TEXT NOT NULL,
            error_key TEXT NOT NULL,
            meta_key TEXT NOT NULL,
            valid_rows INTEGER NOT NULL DEFAULT 0,
            corrupt_rows INTEGER NOT NULL DEFAULT 0,
            file_size_bytes INTEGER,
            corrupt_samples TEXT,
            quarantined_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(file_name, source_name)
        )
        """
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_quarantined_at ON quarantined_files(source_name, quarantined_at)")


def _migration_008_quarantined_reason_counts(con: sqlite3.Connection) -> None:
    """Add ``reason_counts`` column to ``quarantined_files``.

    JSON dict mapping corruption reason → count per file, e.g.
    ``{"invalid_json": 3, "missing_timestamp": 1}``.
    """
    if not _has_table(con, "quarantined_files"):
        return
    cols = {row[1] for row in con.execute("PRAGMA table_info(quarantined_files)").fetchall()}
    if "reason_counts" in cols:
        return
    con.execute("ALTER TABLE quarantined_files ADD COLUMN reason_counts TEXT DEFAULT '{}'")


def _migration_009_quarantined_error_size(con: sqlite3.Connection) -> None:
    """Add ``error_size_bytes`` to track actual FOS quarantine object size."""
    if not _has_table(con, "quarantined_files"):
        return
    cols = {row[1] for row in con.execute("PRAGMA table_info(quarantined_files)").fetchall()}
    if "error_size_bytes" in cols:
        return
    con.execute("ALTER TABLE quarantined_files ADD COLUMN error_size_bytes INTEGER")


def _migration_010_rum_duckdb_iceberg(con: sqlite3.Connection) -> None:
    """Add table_name column to ingested_files and migrate ingest_in_flight to composite PK."""
    schema_sql_ingested = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='ingested_files'"
    ).fetchone()
    if schema_sql_ingested and "PRIMARY KEY (file_name, source_name, table_name)" not in schema_sql_ingested[0]:
        con.execute(
            """
            CREATE TABLE ingested_files_new (
                file_name TEXT,
                source_name TEXT,
                ingested_at TEXT DEFAULT (datetime('now')),
                row_count INTEGER,
                file_size_bytes INTEGER,
                error_count INTEGER DEFAULT 0,
                file_date DATE,
                table_name TEXT NOT NULL DEFAULT 'logs',
                PRIMARY KEY (file_name, source_name, table_name)
            )
            """
        )
        # Check if table_name column exists in old table
        has_table_name = "table_name" in schema_sql_ingested[0]
        select_cols = "file_name, source_name, ingested_at, row_count, file_size_bytes, error_count, file_date, "
        if has_table_name:
            select_cols += "table_name"
        else:
            select_cols += "'logs'"

        con.execute(
            f"""
            INSERT INTO ingested_files_new (file_name, source_name, ingested_at, row_count, file_size_bytes, error_count, file_date, table_name)
            SELECT {select_cols} FROM ingested_files
            """
        )
        con.execute("DROP TABLE ingested_files")
        con.execute("ALTER TABLE ingested_files_new RENAME TO ingested_files")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_ingested_files_source_ingested_at ON ingested_files(source_name, ingested_at)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_ingested_files_table_source ON ingested_files(table_name, source_name)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_ingested_files_source_date ON ingested_files(source_name, file_date)"
        )
    elif not _has_column(con, "ingested_files", "table_name"):
        con.execute("ALTER TABLE ingested_files ADD COLUMN table_name TEXT NOT NULL DEFAULT 'logs'")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_ingested_files_table_source ON ingested_files(table_name, source_name)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_ingested_files_source_date ON ingested_files(source_name, file_date)"
        )

    schema_sql = con.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='ingest_in_flight'").fetchone()
    if schema_sql and "PRIMARY KEY (buffer_filename, table_name)" not in schema_sql[0]:
        con.execute(
            """
            CREATE TABLE ingest_in_flight_new (
                buffer_filename TEXT NOT NULL,
                source_name     TEXT NOT NULL,
                files_json      TEXT,
                started_at      TEXT,
                table_name      TEXT NOT NULL DEFAULT 'logs',
                PRIMARY KEY (buffer_filename, table_name)
            )
            """
        )
        con.execute(
            """
            INSERT INTO ingest_in_flight_new (buffer_filename, source_name, files_json, started_at, table_name)
            SELECT buffer_filename, source_name, files_json, started_at, 'logs' FROM ingest_in_flight
            """
        )
        con.execute("DROP TABLE ingest_in_flight")
        con.execute("ALTER TABLE ingest_in_flight_new RENAME TO ingest_in_flight")
        con.execute("CREATE INDEX IF NOT EXISTS idx_in_flight_source ON ingest_in_flight(source_name)")
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_in_flight_table_source ON ingest_in_flight(table_name, source_name)"
        )


def _migration_011_drop_rum_beacons(con: sqlite3.Connection) -> None:
    """Drop the deprecated SQLite-prototype rum_beacons table and its indices."""
    con.execute("DROP TABLE IF EXISTS rum_beacons")


def _migration_012_rum_file_date(con: sqlite3.Connection) -> None:
    """Backfill ``ingested_files.file_date`` for RUM logs with YYYY/MM/DD directory structure."""
    if not _has_table(con, "ingested_files"):
        return
    import re

    # Find all rows in ingested_files where file_date is NULL and process them
    rows = con.execute("SELECT file_name FROM ingested_files WHERE file_date IS NULL").fetchall()

    rum_pat = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")
    loose_pat = re.compile(r"(\d{4})-(\d{2})-(\d{2})")

    updates = []
    for row in rows:
        fn = row[0] if isinstance(row, tuple) else row["file_name"]
        m_rum = rum_pat.search(fn)
        if m_rum:
            dt = f"{m_rum.group(1)}-{m_rum.group(2)}-{m_rum.group(3)}"
            updates.append((dt, fn))
        else:
            m_loose = loose_pat.search(fn)
            if m_loose:
                dt = f"{m_loose.group(1)}-{m_loose.group(2)}-{m_loose.group(3)}"
                updates.append((dt, fn))

    if updates:
        con.executemany("UPDATE ingested_files SET file_date = ? WHERE file_name = ?", updates)


def _migration_013_add_alert_anomaly_and_channel_fields(con: sqlite3.Connection) -> None:
    """Add channels_json, zscore_threshold, and baseline_period_days columns to alerts table."""
    if not _has_table(con, "alerts"):
        return
    if not _has_column(con, "alerts", "channels_json"):
        con.execute("ALTER TABLE alerts ADD COLUMN channels_json TEXT DEFAULT '[]'")
    if not _has_column(con, "alerts", "zscore_threshold"):
        con.execute("ALTER TABLE alerts ADD COLUMN zscore_threshold REAL DEFAULT 3.0")
    if not _has_column(con, "alerts", "baseline_period_days"):
        con.execute("ALTER TABLE alerts ADD COLUMN baseline_period_days INTEGER DEFAULT 7")


def _migration_014_cron_task_status_started_composite(con: sqlite3.Connection) -> None:
    """Drop index idx_cron_task_status and rebuild it as a composite index covering
    (task, status, started_at) to avoid table-scans / sorting on health check probes."""
    if not _has_table(con, "cron_runs"):
        return
    con.execute("DROP INDEX IF EXISTS idx_cron_task_status")
    con.execute("CREATE INDEX IF NOT EXISTS idx_cron_task_status ON cron_runs(task, status, started_at)")


def _migration_015_add_service_ids(con: sqlite3.Connection) -> None:
    """Add service_id column to cron_runs and local_compacted_files for Postgres seam compatibility."""
    import os

    # In SQLite, the service ID is the basename of the file before .metadata.db
    # We can fetch it by inspecting the attached database file.
    db_file = con.execute("PRAGMA database_list").fetchone()[2]
    service_id = ""
    if db_file and os.path.basename(db_file).endswith(".metadata.db"):
        service_id = os.path.basename(db_file).replace(".metadata.db", "")

    if _has_table(con, "cron_runs") and not _has_column(con, "cron_runs", "service_id"):
        con.execute("ALTER TABLE cron_runs ADD COLUMN service_id TEXT")
        if service_id:
            con.execute("UPDATE cron_runs SET service_id = ?", (service_id,))

    if _has_table(con, "local_compacted_files") and not _has_column(con, "local_compacted_files", "service_id"):
        con.execute("ALTER TABLE local_compacted_files ADD COLUMN service_id TEXT")
        if service_id:
            con.execute("UPDATE local_compacted_files SET service_id = ?", (service_id,))


def _migration_016_ingest_ledger_retry_columns(con: sqlite3.Connection) -> None:
    """Add attempts/last_error to ingest_ledger so convert failures are
    visible and bounded (quarantine after N attempts) instead of an
    invisible infinite reclaim loop."""
    if _has_table(con, "ingest_ledger"):
        if not _has_column(con, "ingest_ledger", "attempts"):
            con.execute("ALTER TABLE ingest_ledger ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0")
        if not _has_column(con, "ingest_ledger", "last_error"):
            con.execute("ALTER TABLE ingest_ledger ADD COLUMN last_error TEXT")
        if not _has_column(con, "ingest_ledger", "discovered_at"):
            con.execute("ALTER TABLE ingest_ledger ADD COLUMN discovered_at REAL")


def _migration_017_ingest_ledger_epoch_timestamps(con: sqlite3.Connection) -> None:
    """Normalize legacy TEXT datetimes in ingest_ledger to epoch floats.

    The first ledger writer stored ``datetime('now')`` TEXT into the REAL
    claimed_at/committed_at columns; the sweeper now compares epoch floats,
    and SQLite orders every number BEFORE every text value — so legacy text
    claims would never satisfy ``claimed_at < <cutoff>`` and stuck rows from
    the pre-fix era would never be reclaimed."""
    if not _has_table(con, "ingest_ledger"):
        return
    for col in ("claimed_at", "committed_at"):
        con.execute(
            f"UPDATE ingest_ledger SET {col} = CAST(strftime('%s', {col}) AS REAL) WHERE typeof({col}) = 'text'"
        )


def _migration_018_ingest_ledger_raw_deleted_at(con: sqlite3.Connection) -> None:
    """Track raw .gz deletion per ledger row so the celery-mode log_ingest
    cron can delete durable-committed raw files (delete_after) without a
    second bookkeeping table, and re-runs stay idempotent."""
    if _has_table(con, "ingest_ledger") and not _has_column(con, "ingest_ledger", "raw_deleted_at"):
        con.execute("ALTER TABLE ingest_ledger ADD COLUMN raw_deleted_at REAL")


def _migration_019_job_runs_running_lease_unique_index(con: sqlite3.Connection) -> None:
    """Add the partial unique index that makes lease acquisition atomic
    under concurrent writers (multiple pods against one Postgres metadata
    backend): at most one 'running' row per (service_id, job_name). Also
    in ``base.py``'s ``_SCHEMA`` for brand-new databases — this migration
    covers every already-provisioned service's existing metadata.db.

    Defensively dedupes first: a pre-existing DB could (rarely — the
    orphaned-lease trap this same index exists to close) already hold two
    literal 'running' rows for the same (service_id, job_name), which would
    make CREATE UNIQUE INDEX fail outright and wedge every future migration
    on this DB. Keep the most recently heartbeated row; mark the rest
    'reaped' — they are, definitionally, the losers of a race that
    predates this fix.
    """
    if not _has_table(con, "job_runs"):
        return
    con.execute(
        """
        UPDATE job_runs SET status = 'reaped', detail = COALESCE(detail, 'superseded — pre-dates lease uniqueness fix')
        WHERE status = 'running' AND id NOT IN (
            SELECT id FROM (
                SELECT id, ROW_NUMBER() OVER (
                    PARTITION BY service_id, job_name ORDER BY heartbeat_at DESC, id DESC
                ) AS rn
                FROM job_runs WHERE status = 'running'
            ) WHERE rn = 1
        )
        """
    )
    con.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_job_runs_running_lease "
        "ON job_runs(service_id, job_name) WHERE status = 'running'"
    )


# Insertion order = application order. Use integer keys. The key=3 slot
# (a rebuild of usage_log_hourly_summary) was retired alongside the
# legacy usage_log schema; the gap is intentional and apply_pending
# tolerates it (the iterator just skips missing keys).


def _migration_020_quarantined_files_service_id(con: sqlite3.Connection) -> None:
    """Make ``service_id`` the tenant discriminator on ``quarantined_files``.

    Five readers (``list_quarantined_files``, ``get_quarantine_summary``,
    ``get_expired_quarantined_files``, ``get_quarantine_storage_total``,
    ``delete_quarantined_rows``) filtered ``WHERE source_name = ?`` while
    binding the *service_id*. That worked only because the two coincide
    today (``src["name"]`` is the service id). Under a shared Postgres
    metadata database every other table scopes rows by a ``service_id``
    column (ADR-15), so this one is brought in line rather than left
    depending on the coincidence — otherwise a source named differently
    from its service makes its quarantined files invisible to the admin
    view AND to the retention sweeper, while they keep occupying FOS.

    The UNIQUE constraint moves with it: ``INSERT OR REPLACE`` dedupes on
    it, so it must be ``(service_id, file_name)`` to match the Postgres
    ``ON CONFLICT`` target. SQLite cannot alter a UNIQUE in place, hence
    the rebuild-and-copy.
    """
    import os

    if not _has_table(con, "quarantined_files"):
        return
    if _has_column(con, "quarantined_files", "service_id"):
        return

    # Same derivation migration 015 uses: the service id is the metadata
    # filename stem. Empty for an in-memory/atypical DB — rows then carry
    # NULL and are healed by the next insert for that file.
    db_file = con.execute("PRAGMA database_list").fetchone()[2]
    service_id = ""
    if db_file and os.path.basename(db_file).endswith(".metadata.db"):
        service_id = os.path.basename(db_file).replace(".metadata.db", "")

    con.execute(
        """CREATE TABLE quarantined_files_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_id TEXT,
            file_name TEXT NOT NULL,
            source_name TEXT NOT NULL,
            fos_key TEXT NOT NULL,
            error_key TEXT NOT NULL,
            meta_key TEXT NOT NULL,
            valid_rows INTEGER NOT NULL DEFAULT 0,
            corrupt_rows INTEGER NOT NULL DEFAULT 0,
            file_size_bytes INTEGER,
            error_size_bytes INTEGER,
            corrupt_samples TEXT,
            reason_counts TEXT,
            quarantined_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(service_id, file_name)
        )"""
    )
    # Column-by-column copy: migrations 008/009 added reason_counts /
    # error_size_bytes, so an older DB may lack them.
    has_reason = _has_column(con, "quarantined_files", "reason_counts")
    has_err_size = _has_column(con, "quarantined_files", "error_size_bytes")
    reason_sel = "reason_counts" if has_reason else "NULL"
    err_size_sel = "error_size_bytes" if has_err_size else "NULL"
    # GROUP BY collapses any pre-existing duplicate (service_id, file_name)
    # pairs the looser old constraint allowed, keeping the newest row —
    # without it the INSERT would trip the new UNIQUE and abort the upgrade.
    con.execute(
        f"""INSERT INTO quarantined_files_new
            (service_id, file_name, source_name, fos_key, error_key, meta_key,
             valid_rows, corrupt_rows, file_size_bytes, error_size_bytes,
             corrupt_samples, reason_counts, quarantined_at)
        SELECT ?, file_name, source_name, fos_key, error_key, meta_key,
               valid_rows, corrupt_rows, file_size_bytes, {err_size_sel},
               corrupt_samples, {reason_sel}, max(quarantined_at)
        FROM quarantined_files
        GROUP BY file_name""",
        (service_id or None,),
    )
    con.execute("DROP TABLE quarantined_files")
    con.execute("ALTER TABLE quarantined_files_new RENAME TO quarantined_files")
    con.execute("DROP INDEX IF EXISTS idx_quarantined_at")
    con.execute("CREATE INDEX IF NOT EXISTS idx_quarantined_at ON quarantined_files(service_id, quarantined_at)")


MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _migration_001_add_ingested_files_error_count,
    2: _migration_002_add_ingested_files_file_date,
    4: _migration_004_committed_buffers,
    5: _migration_005_slow_queries,
    6: _migration_006_slow_queries_count_covering_index,
    7: _migration_007_quarantined_files,
    8: _migration_008_quarantined_reason_counts,
    9: _migration_009_quarantined_error_size,
    10: _migration_010_rum_duckdb_iceberg,
    11: _migration_011_drop_rum_beacons,
    12: _migration_012_rum_file_date,
    13: _migration_013_add_alert_anomaly_and_channel_fields,
    14: _migration_014_cron_task_status_started_composite,
    15: _migration_015_add_service_ids,
    16: _migration_016_ingest_ledger_retry_columns,
    17: _migration_017_ingest_ledger_epoch_timestamps,
    18: _migration_018_ingest_ledger_raw_deleted_at,
    19: _migration_019_job_runs_running_lease_unique_index,
    20: _migration_020_quarantined_files_service_id,
}

LATEST_VERSION = max(MIGRATIONS) if MIGRATIONS else 0


# ── Loader ───────────────────────────────────────────────────────────────────


def get_current_version(con: sqlite3.Connection) -> int:
    return con.execute("PRAGMA user_version").fetchone()[0]


def run_pending_migrations(
    con: sqlite3.Connection,
    migrations: dict[int, Callable[[sqlite3.Connection], None]],
    *,
    log_prefix: str = "sqlite_migrations",
) -> int:
    """Apply every callback in ``migrations`` whose version is greater than
    the DB's ``user_version``.

    Shared by :func:`apply_pending` (per-service metadata.db) and
    :func:`backend.core.share_db.schema.apply_pending` (global share DB) —
    the two used to be near-identical handwritten loops. Each migration
    runs inside a transaction; the ``PRAGMA user_version`` bump is the
    last statement, so a failure leaves the DB at the previous version
    and the next open retries.
    """
    current = con.execute("PRAGMA user_version").fetchone()[0]
    applied = 0
    for version in sorted(migrations):
        if version <= current:
            continue
        func = migrations[version]
        logger.info("[%s] applying v%d (%s)", log_prefix, version, func.__name__)
        try:
            with con:
                func(con)
                con.execute(f"PRAGMA user_version = {version}")
            applied += 1
        except Exception:
            logger.exception("[%s] v%d failed — aborting", log_prefix, version)
            raise
    return applied


def apply_pending(con: sqlite3.Connection) -> int:
    """Apply every per-service metadata.db migration past ``user_version``.

    Safe to call on every open — no-op when the DB is already current.
    Delegates to :func:`run_pending_migrations`.
    """
    return run_pending_migrations(con, MIGRATIONS, log_prefix="sqlite_migrations")
