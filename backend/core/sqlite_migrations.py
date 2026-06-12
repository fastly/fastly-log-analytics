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
       to ``backend.core.metadata_db._SCHEMA`` so fresh DBs already have it.
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


def _migration_003_rebuild_usage_log_hourly_summary(con: sqlite3.Connection) -> None:
    """Rebuild ``usage_log_hourly_summary`` from raw ``usage_log``.

    The v0-v2 rollup is corrupted on any DB that has run
    ``reconcile_fastly_stats``: the INSERT-only trigger never accounted for
    the per-hour DELETE+INSERT refresh cycle, so RECONCILE_A/B contributions
    accumulated across passes — 30-60x inflation observed in prod. The
    matching DELETE/UPDATE triggers ship in ``_SCHEMA`` and are already
    present by the time this migration runs (``_init_schema`` runs the
    schema pass before ``apply_pending``).
    """
    if not _has_table(con, "usage_log_hourly_summary") or not _has_table(con, "usage_log"):
        return
    con.execute("DELETE FROM usage_log_hourly_summary")
    con.execute(
        """
        INSERT INTO usage_log_hourly_summary
            (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
        SELECT service_id,
               substr(timestamp, 1, 13),
               COALESCE(operation_class, ''),
               COALESCE(operation_type, ''),
               SUM(COALESCE(count, 1)),
               SUM(COALESCE(bytes, 0)),
               datetime('now')
        FROM usage_log
        WHERE service_id IS NOT NULL
          AND timestamp IS NOT NULL
          AND length(timestamp) >= 13
        GROUP BY 1, 2, 3, 4
        """
    )


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


# Insertion order = application order. Use integer keys; gaps are not
# allowed (`apply_pending` iterates sorted keys and stops on failure).
MIGRATIONS: dict[int, Callable[[sqlite3.Connection], None]] = {
    1: _migration_001_add_ingested_files_error_count,
    2: _migration_002_add_ingested_files_file_date,
    3: _migration_003_rebuild_usage_log_hourly_summary,
    4: _migration_004_committed_buffers,
}

LATEST_VERSION = max(MIGRATIONS) if MIGRATIONS else 0


# ── Loader ───────────────────────────────────────────────────────────────────


def get_current_version(con: sqlite3.Connection) -> int:
    return con.execute("PRAGMA user_version").fetchone()[0]


def apply_pending(con: sqlite3.Connection) -> int:
    """Apply every migration whose version is greater than ``user_version``.

    Returns the number of migrations applied. Safe to call on every open —
    no-op when the DB is already at the latest version.

    Each migration runs inside a transaction. The version bump is the last
    statement in that transaction, so a failure leaves the DB at the
    previous version and the next open retries.
    """
    current = get_current_version(con)
    applied = 0
    for version in sorted(MIGRATIONS):
        if version <= current:
            continue
        func = MIGRATIONS[version]
        logger.info("[sqlite_migrations] applying v%d (%s)", version, func.__name__)
        try:
            with con:
                func(con)
                con.execute(f"PRAGMA user_version = {version}")
            applied += 1
        except Exception:
            logger.exception("[sqlite_migrations] v%d failed — aborting", version)
            raise
    return applied
