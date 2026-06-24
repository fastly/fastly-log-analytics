"""Shared connection management + schema for the per-service metadata SQLite store.

This module owns the process-wide thread-local pool, the init lock, the dedup
filename cache, and the schema bootstrap. Every concern-specific module in
``backend.core.metadata`` (alerts, views, ingest_log, cron_log, asn_cache,
usage_log, reconciliation, state) imports ``get_con`` from here and writes
through it.

Carved out of the historical ``backend.core.metadata`` monolith — callers
should import from ``backend.core.metadata`` (or its concern-specific
submodules). The package's ``__init__`` installs a ``_ShimModule`` proxy that
mirrors writes to shared-state attributes here, so monkeypatch sites in tests
work without addressing the submodule directly.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import sys
import threading

from backend.core.sqlite_pool import ThreadLocalPool

logger = logging.getLogger(__name__)

# These four module globals are part of the long-standing test surface.
# - ``_DATA_DIR`` is read by ``db_path`` (below) on every call so the
#   ``tests/conftest.py:isolate_metadata_db`` monkeypatch keeps taking
#   effect after the ThreadLocalPool extraction.
# - ``_init_lock`` / ``_initialized`` / ``_local`` are surfaced to the
#   pool through providers (see ``_pool`` further down) so the same
#   conftest patches plus ``tests/core/test_metadata_db_concurrency.py``
#   continue to swap them in fresh per-test.
# - ``_all_connections`` is owned by the pool itself; the module-level
#   name is retained as a passthrough alias used only by retrospective
#   helpers that walked it directly.
_DATA_DIR = "data/services"
_local = threading.local()
_init_lock = threading.Lock()
_initialized: set[str] = set()

# Process-wide cache of {service_id: set[file_name]} for ingest dedup.
# ``get_ingested_filenames`` populates lazily on the first bounded read
# (cron hot path passes ``limit=200_000``); ``insert_ingested_files`` keeps
# it in sync. Unbounded reads (admin teardown / repair tools) bypass and
# invalidate the cache. Eliminates the ~640 ms SQL fetchall on every ~5 s
# sync tick for services with >1 M ingested_files.
_ingested_filenames_cache: dict[str, set[str]] = {}
_ingested_filenames_cache_lock = threading.Lock()


# Pre-compiled for the per-insert file_date parse. The canonical Fastly
# basename is `...<YYYY-MM-DD>T<HH:MM:SS>.<ms>-<rand>.log.gz`; locate the
# first 'T' and use the 10 chars before it when they look like a date.
# Matches the GLOB in _migration_002 / get_log_accounting_counts so legacy
# and runtime parsing agree.
_FILE_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})T")


def _parse_file_date(file_name: str) -> str | None:
    """Return 'YYYY-MM-DD' parsed from filename or None if no match.

    Cheap regex on the basename — runs per-insert, called from the bulk
    INSERT in `insert_ingested_files`. Same semantics as the SQL backfill
    in `_migration_002_add_ingested_files_file_date`.
    """
    if not file_name:
        return None
    m = _FILE_DATE_RE.search(file_name)
    return m.group(1) if m else None


def _clear_ingested_filenames_cache(service_id: str | None = None) -> None:
    """Drop the dedup cache for one service or all services.

    Called from the pytest ``isolate_metadata_db`` fixture (every test gets a
    clean slate) and from ``teardown`` so deleted services don't keep
    phantom dedup state.
    """
    with _ingested_filenames_cache_lock:
        if service_id is None:
            _ingested_filenames_cache.clear()
        else:
            _ingested_filenames_cache.pop(service_id, None)


_ORPHAN_THRESHOLD_MINS = 60

# Per-task override of the orphan cutoff used by ``start_cron_run``. A row of
# the same task still ``running`` past this many minutes is treated as dead and
# reaped before the guard check, so the next tick can start fresh. ``sync`` is
# idempotent + incremental (it re-discovers un-ingested files via ``StartAfter``
# next tick), so a short cutoff just means a stuck sync "dies and picks back up"
# quickly instead of freezing ingestion for the full default hour. Keep each
# value safely ABOVE the task's worst-case legit runtime to avoid reaping a
# healthy slow run (sync's ingest budget is max_seconds=240 → 10 min is ~2.5×).
# Incident 2026-06-19: a leaked sync row blocked ingestion ~20 min at the old
# 60-min cutoff. Tasks not listed here fall back to _ORPHAN_THRESHOLD_MINS.
_TASK_ORPHAN_THRESHOLD_MINS = {"sync": 10}


class InvalidServiceIdError(ValueError):
    """Raised by ``db_path`` when ``service_id`` fails format validation.

    Fastly service IDs are 22-character lowercase alphanumeric strings, but
    legacy fixtures and Admin-provisioned identifiers also use hyphens and
    mixed case, so we accept the union (``[A-Za-z0-9_-]{1,64}``). Anything
    outside that — non-ASCII characters, path separators, null bytes — would
    either traverse the data directory or hit macOS APFS / strict Linux
    filesystems with ``OSError(Errno 92): Illegal byte sequence`` and bubble
    up as an opaque ``sqlite3.OperationalError: unable to open database
    file``. Reject at the data-layer chokepoint so every caller is safe.
    The shared FastAPI exception handler in ``backend.main`` converts this
    into a 422 instead of a 500.
    """


# Anchored, length-bounded. Hyphens and underscores allowed for legacy
# fixtures (e.g. "test-service-id"). 1-64 chars covers Fastly's 22-char
# native IDs with headroom for Admin-assigned suffixes.
_SERVICE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _validate_service_id_or_raise(service_id: str) -> None:
    if not isinstance(service_id, str):
        raise TypeError(f"service_id must be a string, got {type(service_id).__name__}: {service_id!r}")
    if not _SERVICE_ID_RE.match(service_id):
        raise InvalidServiceIdError(f"service_id must match {_SERVICE_ID_RE.pattern!r}; got {service_id!r}")


# ── Connection management ─────────────────────────────────────────────────────


def db_path(service_id: str) -> str:
    """Absolute path to the per-service metadata SQLite file.

    A non-string ``service_id`` would silently produce a junk path
    containing the object's repr (e.g. ``<...0x...>.metadata.db``) and
    leak files on disk. Reject at the boundary so the bad caller is
    pinpointed immediately. A malformed-string ``service_id`` raises
    :class:`InvalidServiceIdError` for the same reason — see that class's
    docstring for the threat model.
    """
    _validate_service_id_or_raise(service_id)
    return os.path.join(_DATA_DIR, f"{service_id}.metadata.db")


# Resolve through ``sys.modules`` so a ``monkeypatch.setattr(metadata_db,
# "_init_lock", ...)`` (used by tests/core/test_metadata_db_concurrency.py
# to force-time-out the cold path's lock) actually takes effect on every
# subsequent call — the providers re-read the module attribute each time.
_module = sys.modules[__name__]
_pool = ThreadLocalPool(
    name="metadata_db",
    path_fn=lambda sid: db_path(sid),
    schema_fn=lambda con: _init_schema(con),
    init_lock_provider=lambda: _module._init_lock,
    initialized_provider=lambda: _module._initialized,
    local_provider=lambda: _module._local,
    local_attr="conns",
)

# Exposed for the small handful of legacy spots (and the metadata_db shim's
# _MIRRORED_TO_BASE list) that walked the connection registry directly.
_all_connections = _pool._all_connections
_all_connections_lock = _pool._all_connections_lock


def get_con(service_id: str) -> sqlite3.Connection:
    """Return a thread-local SQLite connection for the given service.

    Lazily initialises the file (creating ``data/services/`` and the schema)
    on first use per (thread, service_id) pair.

    Concurrency: ``PRAGMA journal_mode=WAL`` requires an exclusive writer
    lock to switch from the default (delete) journal mode. If N threads
    open a brand-new service file simultaneously, they collide on that
    PRAGMA and one raises ``OperationalError: database is locked`` despite
    the connection's 30s timeout. The pool holds ``_init_lock`` across the
    connect+PRAGMA window so cold-start is serialised once per process;
    subsequent calls hit the thread-local pool early and pay nothing.
    """
    return _pool.get(service_id)


def close_all_connections() -> None:
    """Close every connection opened by ``get_con`` in any thread.

    Used by the pytest fixture in tests/conftest.py to drain connections
    opened on FastAPI TestClient worker threads — the fixture only has
    access to its own thread's ``_local`` and would otherwise leak those.
    """
    _pool.close_all()


def teardown(service_id: str) -> None:
    """Close any thread-local connection and delete the SQLite file.

    Called from ``backend/provision.py`` during service teardown. Safe to call
    even if the file does not exist or other threads still hold connections —
    other threads will reopen lazily and re-init schema if the file is missing.
    """
    _pool.teardown(service_id)
    _clear_ingested_filenames_cache(service_id)

    from backend.core.sqlite_pool import remove_sqlite_db_files

    remove_sqlite_db_files(db_path(service_id), name="metadata_db")


# ── Schema ────────────────────────────────────────────────────────────────────


_SCHEMA = [
    """CREATE TABLE IF NOT EXISTS sources (
        name TEXT PRIMARY KEY,
        config TEXT,
        table_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS ingested_files (
        file_name TEXT,
        source_name TEXT,
        ingested_at TEXT DEFAULT (datetime('now')),
        row_count INTEGER,
        file_size_bytes INTEGER,
        error_count INTEGER DEFAULT 0,
        file_date DATE,
        PRIMARY KEY (file_name, source_name)
    )""",
    # Covers `/usage/prefill`'s source+range narrowing
    # (`WHERE source_name = ? AND ingested_at BETWEEN ? AND ?`) and the
    # bounded `list_unbackfilled_fastly_edge_files` scan (see :1128). The
    # previous `idx_ingested_files_source` indexed source_name alone — SQLite
    # had to walk every row for the matching source and filter ingested_at
    # in memory (~250ms per query on populated services). The composite
    # satisfies the range scan directly and is a strict superset for
    # source_name-only lookups (SQLite uses leading-column prefixes), so the
    # old index is redundant and dropped here. Index name matches the
    # by-name reference in `list_unbackfilled_fastly_edge_files`'s docstring.
    "CREATE INDEX IF NOT EXISTS idx_ingested_files_source_ingested_at ON ingested_files(source_name, ingested_at)",
    # Note: idx_ingested_files_source_date (companion index for per-day
    # usage queries) is created by _migration_002_add_ingested_files_file_date,
    # not here — _SCHEMA runs before migrations and a legacy DB upgrading
    # would fail on this CREATE INDEX (the file_date column doesn't exist
    # yet at that point). The migration is idempotent + runs for fresh DBs
    # too (apply_pending walks v1..LATEST on every init), so the index
    # always lands without _SCHEMA carrying it.
    "DROP INDEX IF EXISTS idx_ingested_files_source",
    # Earlier in this branch a redundant `idx_ingested_files_source_ts` was
    # added under a different name before discovering the existing
    # by-name reference above; clean it up so no service ends up with two
    # functionally identical composites.
    "DROP INDEX IF EXISTS idx_ingested_files_source_ts",
    # Single-row-per-service rollup maintained by ``insert_ingested_files``.
    # Without it, ``get_ingested_files_status_summary`` had to SUM(row_count)
    # + SUM(file_size_bytes) across the whole table on every cron tick —
    # ~4 s on services with >1 M rows since SQLite couldn't satisfy the SUMs
    # from any existing index. Lazy-bootstrapped from the full scan on first
    # read after upgrade; transactional delta updates after that.
    """CREATE TABLE IF NOT EXISTS ingested_files_summary (
        source_name TEXT PRIMARY KEY,
        file_count INTEGER NOT NULL DEFAULT 0,
        total_rows INTEGER NOT NULL DEFAULT 0,
        total_bytes INTEGER NOT NULL DEFAULT 0,
        count_with_bytes INTEGER NOT NULL DEFAULT 0,
        latest_file_name TEXT,
        last_ingested TEXT
    )""",
    # Atomic ingest manifest. A row is written BEFORE the buffer Parquet
    # appears on disk and deleted AFTER ingested_files is updated. On startup
    # the ingest loop sweeps this table: if the buffer file exists the row is
    # promoted (commit ingested_files, drop the in_flight row); if it is
    # missing the row is dropped without touching ingested_files (the buffer
    # write itself crashed — files will re-LIST on the next tick). Combined
    # with deterministic buffer filenames (sha256 of sorted source filenames)
    # this makes the ingest → buffer → metadata commit sequence crash-safe
    # without ever double-committing a row to Iceberg.
    """CREATE TABLE IF NOT EXISTS ingest_in_flight (
        buffer_filename TEXT PRIMARY KEY,
        source_name TEXT NOT NULL,
        files_json TEXT NOT NULL,
        started_at TEXT DEFAULT (datetime('now'))
    )""",
    "CREATE INDEX IF NOT EXISTS idx_in_flight_source ON ingest_in_flight(source_name)",
    """CREATE TABLE IF NOT EXISTS cron_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task TEXT NOT NULL,
        started_at TEXT NOT NULL,
        duration_s REAL,
        status TEXT,
        error_message TEXT,
        files_downloaded INTEGER DEFAULT 0,
        files_deleted_fos INTEGER DEFAULT 0,
        rows_ingested INTEGER DEFAULT 0,
        corrupt_rows INTEGER DEFAULT 0,
        parquet_files_created INTEGER DEFAULT 0,
        parquet_files_optimized INTEGER DEFAULT 0,
        parquet_keys TEXT DEFAULT '[]',
        summary TEXT,
        log_output TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_cron_task_started ON cron_runs(task, started_at)",
    # Covers `/logs`'s unfiltered pagination
    # (`ORDER BY started_at DESC LIMIT ? OFFSET ?` with no `WHERE task`) and
    # `main.py`'s sync-status probe (`WHERE task='sync' AND status != 'running'
    # ORDER BY started_at DESC LIMIT 1`). Without it, SQLite falls back to a
    # TEMP B-TREE sort over the full table because `idx_cron_task_started`
    # requires a leading-`task` predicate to satisfy the ORDER BY.
    "CREATE INDEX IF NOT EXISTS idx_cron_started ON cron_runs(started_at DESC)",
    """CREATE TABLE IF NOT EXISTS asn_names (
        asn INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        fetched_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
    )""",
    """CREATE TABLE IF NOT EXISTS audit_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        source_name TEXT,
        event_type TEXT NOT NULL,
        details TEXT,
        actor TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_audit_source ON audit_logs(source_name)",
    """CREATE TABLE IF NOT EXISTS views (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        filters_json TEXT NOT NULL,
        time_range_type TEXT,
        start_time TEXT,
        end_time TEXT,
        page TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS alerts (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        name TEXT NOT NULL,
        category TEXT DEFAULT 'reliability',
        metric TEXT NOT NULL,
        evaluation_type TEXT DEFAULT 'absolute',
        evaluation_scope TEXT DEFAULT 'all',
        operator TEXT NOT NULL,
        threshold REAL NOT NULL,
        window_min REAL NOT NULL,
        comparison_period_min REAL,
        status_codes TEXT,
        webhook_url TEXT,
        enabled INTEGER DEFAULT 1,
        last_triggered_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""",
    # Admin-flagged sessions for the edge session-scoring system. Each row
    # is one (service, sid) tuple labeled good/bad/neutral by the admin.
    # Feeds backend.scoring.evaluate.evaluate() for matrix ROC-AUC; the
    # neutral label is captured for UI completeness but excluded from the
    # AUC computation (intentionally uncertain).
    """CREATE TABLE IF NOT EXISTS scoring_labels (
        id TEXT PRIMARY KEY,
        service_id TEXT NOT NULL,
        sid TEXT NOT NULL,
        label TEXT NOT NULL CHECK (label IN ('good', 'bad', 'neutral')),
        notes TEXT DEFAULT '',
        flagged_by TEXT,
        sample_ip TEXT,
        sample_ua TEXT,
        sample_url TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )""",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_scoring_labels_svc_sid ON scoring_labels(service_id, sid)",
    "CREATE INDEX IF NOT EXISTS idx_scoring_labels_svc_label ON scoring_labels(service_id, label)",
    # Operator audit log specifically for scoring-config mutations.
    # Separate from audit_logs (which gets state_sync'd) because scoring-
    # audit is per-host operator-attribution data that should NOT mirror
    # to read_only analyst replicas.
    """CREATE TABLE IF NOT EXISTS scoring_audit (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL DEFAULT (datetime('now')),
        service_id TEXT NOT NULL,
        action TEXT NOT NULL,
        actor TEXT NOT NULL,
        details TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_scoring_audit_svc_ts ON scoring_audit(service_id, timestamp DESC)",
    # Plain timestamp index for the list_scoring_audit ORDER BY timestamp DESC
    # path when the service_id predicate is already satisfied — keeps the sort
    # itself indexed instead of falling back to a TEMP B-TREE on large audit
    # tables.
    "CREATE INDEX IF NOT EXISTS idx_scoring_audit_ts ON scoring_audit(timestamp DESC)",
    # Tracks Iceberg parquet basenames that local_compaction merged into a
    # bigger local file and then deleted from disk. WITHOUT this table the
    # sync_data fast-path check sees the deletions as "missing local files"
    # → falls into the slow path → re-downloads the same files from FOS →
    # local_compaction merges + deletes them again → infinite loop draining
    # FOS bandwidth. With this table, sync_data treats basenames in the
    # registry as "intentionally absent locally, do not re-fetch".
    """CREATE TABLE IF NOT EXISTS local_compacted_files (
        file_name TEXT PRIMARY KEY,
        compacted_at TEXT DEFAULT (datetime('now'))
    )""",
]


def _init_schema(con: sqlite3.Connection) -> None:
    from backend.core import sqlite_migrations

    for stmt in _SCHEMA:
        con.execute(stmt)
    con.commit()
    sqlite_migrations.apply_pending(con)
