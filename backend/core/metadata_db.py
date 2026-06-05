"""Per-service operational metadata store, backed by SQLite.

DuckDB is reserved for analytical queries over Iceberg log data. Everything
else — alerts, saved views, audit logs, ingested-file dedup tracking, cron run
history, ASN name cache, source registration, FOS/CDN usage telemetry — lives
here, in a per-service SQLite file at ``data/services/{service_id}.metadata.db``.

Why per-service: SQLite's writer lock is per-file even in WAL mode. With many
services ingesting concurrently, a single global file would serialise every
ingest's `ingested_files` write. Per-file isolation also makes service
teardown a single ``rm`` and bounds blast radius on corruption.

Concurrency model: thread-local connections (sqlite3 connections are not
thread-safe) keyed by ``(thread, service_id)``. WAL + ``synchronous=NORMAL``
gives readers freedom from writer locks within a single file.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import iso_z, iso_z_now

logger = logging.getLogger(__name__)

_DATA_DIR = "data/services"
_local = threading.local()
_init_lock = threading.Lock()
_initialized: set[str] = set()

# Process-global registry of every connection handed out by ``get_con``,
# regardless of which thread opened it. ``_local.conns`` is the fast path for
# per-thread reuse; ``_all_connections`` exists so cleanup code (notably the
# pytest fixture in tests/conftest.py) can close connections opened on
# FastAPI TestClient worker threads, which are otherwise invisible to the
# main thread's ``_local``. Without it, those connections live until GC and
# emit ``ResourceWarning: unclosed database`` during interpreter shutdown.
_all_connections: list[sqlite3.Connection] = []
_all_connections_lock = threading.Lock()

# Process-wide cache of {service_id: set[file_name]} for ingest dedup.
# ``get_ingested_filenames`` populates lazily on the first bounded read
# (cron hot path passes ``limit=200_000``); ``insert_ingested_files`` keeps
# it in sync. Unbounded reads (admin teardown / repair tools) bypass and
# invalidate the cache. Eliminates the ~640 ms SQL fetchall on every ~5 s
# sync tick for services with >1 M ingested_files.
_ingested_filenames_cache: dict[str, set[str]] = {}
_ingested_filenames_cache_lock = threading.Lock()


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


# ── Connection management ─────────────────────────────────────────────────────


def db_path(service_id: str) -> str:
    """Absolute path to the per-service metadata SQLite file.

    A non-string ``service_id`` would silently produce a junk path
    containing the object's repr (e.g. ``<...0x...>.metadata.db``) and
    leak files on disk. Reject at the boundary so the bad caller is
    pinpointed immediately.
    """
    if not isinstance(service_id, str):
        raise TypeError(f"service_id must be a string, got {type(service_id).__name__}: {service_id!r}")
    return os.path.join(_DATA_DIR, f"{service_id}.metadata.db")


def _connections() -> dict[str, sqlite3.Connection]:
    if not hasattr(_local, "conns"):
        _local.conns = {}
    return _local.conns


def get_con(service_id: str) -> sqlite3.Connection:
    """Return a thread-local SQLite connection for the given service.

    Lazily initialises the file (creating ``data/services/`` and the schema)
    on first use per (thread, service_id) pair.

    Concurrency: ``PRAGMA journal_mode=WAL`` requires an exclusive writer
    lock to switch from the default (delete) journal mode. If N threads
    open a brand-new service file simultaneously, they collide on that
    PRAGMA and one raises ``OperationalError: database is locked`` despite
    the connection's 30s timeout. We hold ``_init_lock`` across the
    connect+PRAGMA window so cold-start is serialised once per process;
    subsequent calls hit the thread-local pool early and pay nothing.
    """
    pool = _connections()
    con = pool.get(service_id)
    if con is not None:
        return con

    path = db_path(service_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not _init_lock.acquire(timeout=10):
        raise sqlite3.OperationalError(
            f"metadata_db._init_lock contended >10s for {service_id} — another thread is stuck inside connect+PRAGMA"
        )
    try:
        # InstrumentedConnection subclasses sqlite3.Connection to time and
        # capture every statement into a process-global ring buffer for the
        # Debug Panel. ~5us per statement; bounded ring buffer. See
        # backend/utils/sqlite_profiler.py for the capture/read API.
        from backend.utils.sqlite_profiler import InstrumentedConnection

        con = sqlite3.connect(path, timeout=30.0, factory=InstrumentedConnection)
        # Register the raw connection IMMEDIATELY so any exception below
        # (e.g. a concurrent teardown deletes the file mid-PRAGMA) doesn't
        # leak an unclosed SQLite handle. Production sees this rarely; the
        # test suite hits it under ``test_metadata_db_concurrency``.
        with _all_connections_lock:
            _all_connections.append(con)
        try:
            con.row_factory = sqlite3.Row
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.execute("PRAGMA foreign_keys=ON")
            # 64MB page cache. Default is 2MB which forces the per-service
            # cron's repeated SUM/COUNT scans (usage_log, ingested_files)
            # to repeatedly re-read pages from disk. 64MB fits the largest
            # tables we currently maintain in-memory and is a single-digit
            # MB cost per connection. Architecture-review Dimension 2.
            con.execute("PRAGMA cache_size=-64000")
            # Belt-and-suspenders alongside Python's timeout=30.0 above:
            # busy_timeout is the kernel-level wait that gets honored when
            # WAL writers are committing; the Python timeout is a wrapper
            # around it but the explicit PRAGMA ensures consistent behavior
            # across the Python and C call paths.
            con.execute("PRAGMA busy_timeout=30000")

            if path not in _initialized:
                _init_schema(con)
                _initialized.add(path)
        except Exception:
            try:
                con.close()
            except Exception:
                pass
            raise
    finally:
        _init_lock.release()

    pool[service_id] = con
    return con


def close_all_connections() -> None:
    """Close every connection opened by ``get_con`` in any thread.

    Used by the pytest fixture in tests/conftest.py to drain connections
    opened on FastAPI TestClient worker threads — the fixture only has
    access to its own thread's ``_local`` and would otherwise leak those.
    """
    with _all_connections_lock:
        for con in _all_connections:
            try:
                con.close()
            except Exception:
                pass
        _all_connections.clear()


def teardown(service_id: str) -> None:
    """Close any thread-local connection and delete the SQLite file.

    Called from ``backend/provision.py`` during service teardown. Safe to call
    even if the file does not exist or other threads still hold connections —
    other threads will reopen lazily and re-init schema if the file is missing.
    """
    pool = _connections()
    con = pool.pop(service_id, None)
    if con is not None:
        try:
            con.close()
        except Exception:
            pass

    path = db_path(service_id)
    _initialized.discard(path)
    _clear_ingested_filenames_cache(service_id)

    for suffix in ("", "-wal", "-shm", "-journal"):
        target = path + suffix
        try:
            if os.path.exists(target):
                os.remove(target)
        except OSError as e:
            logger.debug("[metadata_db] could not remove %s: %s", target, e)


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
    """CREATE TABLE IF NOT EXISTS usage_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        service_id TEXT,
        operation_class TEXT,
        operation_type TEXT,
        url TEXT,
        status TEXT,
        duration_ms REAL,
        function_name TEXT,
        process_context TEXT,
        bytes INTEGER,
        -- Rolled-up op count. Normal rows = 1. Reconciliation rows from
        -- reconcile_fastly_stats() use this to compactly represent the gap
        -- between locally-observed ops and Fastly's authoritative
        -- /stats/aggregate count (e.g. one row with count=200000 for an
        -- hour where Fastly's multipart-upload pattern produced 3 A ops
        -- per file while our backfill counted 1). Aggregators must use
        -- SUM(count), not COUNT(*).
        count INTEGER NOT NULL DEFAULT 1
    )""",
    "CREATE INDEX IF NOT EXISTS idx_usage_timestamp ON usage_log(timestamp)",
    # Covering index for log_synthetic_usage's chunked dedup query
    # (SELECT url ... WHERE service_id=? AND function_name=? AND url IN (...)).
    # Without it, the dedup falls back to a full scan of usage_log and the
    # cron's usage_log phase blows past its 30s budget once usage_log grows
    # into the millions of rows.
    "CREATE INDEX IF NOT EXISTS idx_usage_dedup ON usage_log(service_id, function_name, url)",
    # Covering index for reconcile_fastly_stats' per-hour SUM(count) probe
    # over the last N hours. Without it, the per-class hourly GROUP BY
    # falls back to a service_id-only scan (via idx_usage_dedup) and the
    # cron's usage_log phase blows past 30s on a multi-million-row table.
    "CREATE INDEX IF NOT EXISTS idx_usage_reconcile ON usage_log(service_id, operation_class, timestamp)",
    # Covering index for telemetry._query_iothread_calls_from_usage_log
    # (Debug Panel: pull iothread/pool FOS+CDN rows tagged with the current
    # request's process_context). Without it, every API request full-scans
    # usage_log (3-5s on multi-million-row tables) — and the query fires
    # once per parallel endpoint in a dashboard load, so a single page open
    # serialized 30+s of SQLite. process_context is the high-cardinality
    # primary filter; timestamp is included so the WHERE timestamp >= ?
    # narrowing rides the same index walk.
    "CREATE INDEX IF NOT EXISTS idx_usage_process_context_ts ON usage_log(process_context, timestamp)",
    # Covering index for get_usage_logs (Admin Usage Log page). The page issues
    # three queries per render — count(*), aggregate SUM(CASE...), and
    # SELECT * ORDER BY timestamp DESC LIMIT 500 — all keyed on
    # (service_id, timestamp). Without this, the ORDER BY DESC LIMIT pattern
    # falls back to idx_usage_dedup (service_id only) + TEMP B-TREE sort over
    # millions of rows: a 24h window on a 5M-row table took 16s to fetch a
    # 500-row page. Including (operation_class, count, bytes) makes the
    # aggregate covering too (5× faster than non-covering on the same query).
    "CREATE INDEX IF NOT EXISTS idx_usage_service_ts ON usage_log(service_id, timestamp, operation_class, count, bytes)",
    # Hourly rollup of usage_log keyed by (service, hour-prefix of timestamp,
    # operation_class, operation_type). Powers the /admin/usage-log aggregate
    # GROUP BY which used to scan millions of usage_log rows (~600 ms steady
    # state). With the rollup the aggregate becomes a small indexed sum over
    # at most 24 hours × a few op-class/type pairs. Maintained by the
    # AFTER INSERT trigger below (incremental, always-consistent) plus a
    # backfill helper for services upgrading from a pre-rollup install.
    """CREATE TABLE IF NOT EXISTS usage_log_hourly_summary (
        service_id TEXT NOT NULL,
        hour TEXT NOT NULL,
        operation_class TEXT NOT NULL DEFAULT '',
        operation_type TEXT NOT NULL DEFAULT '',
        count INTEGER NOT NULL DEFAULT 0,
        bytes INTEGER NOT NULL DEFAULT 0,
        last_updated TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (service_id, hour, operation_class, operation_type)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_usage_hourly_svc_hour ON usage_log_hourly_summary(service_id, hour)",
    # AFTER INSERT trigger: every row added to usage_log bumps its hour bucket
    # in the summary. Hour key = first 13 chars of timestamp ("YYYY-MM-DDTHH").
    # Coalesce on empty operation_class/operation_type because rows can have
    # NULLs; the rollup uses '' as a normalised sentinel. ON CONFLICT path
    # supports the reconcile_fastly_stats compaction pattern where multiple
    # rows for the same (hour, class, type) accumulate.
    """CREATE TRIGGER IF NOT EXISTS trg_usage_log_summary_insert
    AFTER INSERT ON usage_log
    WHEN NEW.timestamp IS NOT NULL AND length(NEW.timestamp) >= 13 AND NEW.service_id IS NOT NULL
    BEGIN
        INSERT INTO usage_log_hourly_summary
            (service_id, hour, operation_class, operation_type, count, bytes, last_updated)
        VALUES (NEW.service_id, substr(NEW.timestamp, 1, 13),
                COALESCE(NEW.operation_class, ''), COALESCE(NEW.operation_type, ''),
                COALESCE(NEW.count, 1), COALESCE(NEW.bytes, 0), datetime('now'))
        ON CONFLICT(service_id, hour, operation_class, operation_type)
        DO UPDATE SET count = count + excluded.count,
                      bytes = bytes + excluded.bytes,
                      last_updated = excluded.last_updated;
    END""",
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
    # Tracking table for the data-migration framework
    # (``backend.core.data_migrations``). Each row records one applied
    # data-migration: long-running, one-time data setup tasks (e.g. the
    # rollups initial backfill) that are NOT schema DDL changes. Schema
    # migrations use ``PRAGMA user_version`` via ``sqlite_migrations.py``
    # — these two systems are intentionally separate because schema
    # changes must block startup, while data migrations run async on a
    # daemon thread so a multi-hour backfill can't wedge the boot loop.
    """CREATE TABLE IF NOT EXISTS applied_data_migrations (
        name TEXT PRIMARY KEY,
        applied_at TEXT NOT NULL DEFAULT (datetime('now')),
        duration_s REAL,
        status TEXT NOT NULL DEFAULT 'success',
        notes TEXT
    )""",
]


def _init_schema(con: sqlite3.Connection) -> None:
    from backend.core import sqlite_migrations

    for stmt in _SCHEMA:
        con.execute(stmt)
    con.commit()
    sqlite_migrations.apply_pending(con)


# ── alerts ────────────────────────────────────────────────────────────────────


def list_alerts(service_id: str, filter_service_id: str | None = None) -> list[dict]:
    """Return all alerts, optionally filtered by service_id."""
    con = get_con(service_id)
    where = "WHERE service_id = ? " if filter_service_id else ""
    params: list = [filter_service_id] if filter_service_id else []
    rows = con.execute(
        "SELECT id, service_id, name, category, metric, evaluation_type, operator, threshold, "
        "window_min, comparison_period_min, status_codes, webhook_url, enabled, "
        "last_triggered_at, created_at, evaluation_scope "
        f"FROM alerts {where}ORDER BY created_at DESC",
        params,
    ).fetchall()

    return [
        {
            "id": r["id"],
            "service_id": r["service_id"],
            "name": r["name"],
            "category": r["category"],
            "metric": r["metric"],
            "evaluation_type": r["evaluation_type"],
            "operator": r["operator"],
            "threshold": r["threshold"],
            "window_min": r["window_min"],
            "comparison_period_min": r["comparison_period_min"],
            "status_codes": json.loads(r["status_codes"]) if r["status_codes"] else None,
            "webhook_url": r["webhook_url"],
            "enabled": bool(r["enabled"]),
            "last_triggered_at": r["last_triggered_at"],
            "created_at": r["created_at"],
            "evaluation_scope": r["evaluation_scope"] or "all",
        }
        for r in rows
    ]


def count_alerts(service_id: str) -> int:
    """Return total number of alerts (enabled + disabled) for a service.

    Used by the scheduler to gate the alerts evaluation cron: when zero, the
    cron is not registered at all so we don't waste a tick per ``log_period``
    producing "skipped — no alerts configured" entries in cron_runs.
    """
    con = get_con(service_id)
    row = con.execute("SELECT count(*) AS n FROM alerts WHERE service_id = ?", (service_id,)).fetchone()
    return int(row["n"]) if row else 0


def save_alert(service_id: str, alert) -> dict:
    """Insert or update an alert. Returns {id, status}."""
    import uuid

    con = get_con(service_id)
    alert_id = alert.id or str(uuid.uuid4())
    con.execute(
        """
        INSERT INTO alerts (id, service_id, name, category, metric, evaluation_type,
            operator, threshold, window_min, comparison_period_min, status_codes,
            webhook_url, enabled, evaluation_scope)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            service_id = excluded.service_id,
            name = excluded.name,
            category = excluded.category,
            metric = excluded.metric,
            evaluation_type = excluded.evaluation_type,
            operator = excluded.operator,
            threshold = excluded.threshold,
            window_min = excluded.window_min,
            comparison_period_min = excluded.comparison_period_min,
            status_codes = excluded.status_codes,
            webhook_url = excluded.webhook_url,
            enabled = excluded.enabled,
            evaluation_scope = excluded.evaluation_scope
        """,
        (
            alert_id,
            alert.service_id,
            alert.name,
            alert.category,
            alert.metric,
            alert.evaluation_type,
            alert.operator,
            alert.threshold,
            alert.window_min,
            alert.comparison_period_min,
            json.dumps(alert.status_codes) if alert.status_codes else None,
            alert.webhook_url,
            1 if alert.enabled else 0,
            alert.evaluation_scope,
        ),
    )
    con.commit()
    return {"id": alert_id, "status": "success"}


def toggle_alert(service_id: str, alert_id: str, enabled: bool) -> dict:
    con = get_con(service_id)
    cur = con.execute(
        "SELECT service_id FROM alerts WHERE id = ?",
        (alert_id,),
    )
    row = cur.fetchone()
    con.execute(
        "UPDATE alerts SET enabled = ? WHERE id = ?",
        (1 if enabled else 0, alert_id),
    )
    con.commit()
    return {"id": alert_id, "status": "success", "service_id": row["service_id"] if row else None}


def delete_alert(service_id: str, alert_id: str) -> dict:
    con = get_con(service_id)
    cur = con.execute("SELECT service_id FROM alerts WHERE id = ?", (alert_id,))
    row = cur.fetchone()
    con.execute("DELETE FROM alerts WHERE id = ?", (alert_id,))
    con.commit()
    return {"status": "success", "service_id": row["service_id"] if row else None}


def update_alert_last_triggered(service_id: str, alert_id: str, triggered_ts: str | None = None) -> None:
    con = get_con(service_id)
    if triggered_ts:
        con.execute(
            "UPDATE alerts SET last_triggered_at = ? WHERE id = ?",
            (triggered_ts, alert_id),
        )
    else:
        con.execute(
            "UPDATE alerts SET last_triggered_at = datetime('now') WHERE id = ?",
            (alert_id,),
        )
    con.commit()


# ── views ─────────────────────────────────────────────────────────────────────


def list_views(service_id: str) -> list[dict]:
    con = get_con(service_id)
    rows = con.execute(
        "SELECT id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at "
        "FROM views WHERE service_id = ? ORDER BY created_at DESC",
        (service_id,),
    ).fetchall()
    return [
        {
            "id": r["id"],
            "service_id": r["service_id"],
            "name": r["name"],
            "filters_json": r["filters_json"],
            "time_range_type": r["time_range_type"],
            "start_time": r["start_time"],
            "end_time": r["end_time"],
            "page": r["page"],
            "created_at": str(r["created_at"]) if r["created_at"] is not None else "",
        }
        for r in rows
    ]


def save_view(service_id: str, view) -> dict:
    import uuid

    con = get_con(service_id)
    view_id = view.id or str(uuid.uuid4())
    con.execute(
        "INSERT OR REPLACE INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            view_id,
            view.service_id,
            view.name,
            view.filters_json,
            view.time_range_type,
            view.start_time,
            view.end_time,
            view.page,
        ),
    )
    con.commit()
    return {"id": view_id, "status": "success"}


def delete_view(service_id: str, view_id: str) -> dict:
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE id = ?", (view_id,))
    con.commit()
    return {"status": "success"}


def replace_views_for_service(service_id: str, views: list[dict]) -> None:
    """Replace all saved views for a service. Used by state_sync.import_admin_state."""
    con = get_con(service_id)
    con.execute("DELETE FROM views WHERE service_id = ?", (service_id,))
    if views:
        con.executemany(
            "INSERT INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    v.get("id"),
                    v.get("service_id"),
                    v.get("name"),
                    v.get("filters_json"),
                    v.get("time_range_type"),
                    v.get("start_time"),
                    v.get("end_time"),
                    v.get("page"),
                    v.get("created_at"),
                )
                for v in views
            ],
        )
    con.commit()


def upsert_views_for_service(service_id: str, views: list[dict]) -> None:
    """Upsert saved views by id WITHOUT deleting local-only rows.

    Used by state_sync.import_admin_state on read_only analyst hosts so
    locally-created views (which the analyst created on their own pod) are
    preserved through every metadata_sync cron tick. Without this, the
    cron's wholesale DELETE+INSERT silently wiped any analyst-side view
    that hadn't been mirrored back to FOS — and ``export_admin_state``
    refuses to push from read_only hosts, so the loss was permanent.
    """
    if not views:
        return
    con = get_con(service_id)
    con.executemany(
        "INSERT INTO views (id, service_id, name, filters_json, time_range_type, start_time, end_time, page, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
        "ON CONFLICT(id) DO UPDATE SET "
        "name=excluded.name, filters_json=excluded.filters_json, "
        "time_range_type=excluded.time_range_type, start_time=excluded.start_time, "
        "end_time=excluded.end_time, page=excluded.page, created_at=excluded.created_at",
        [
            (
                v.get("id"),
                v.get("service_id"),
                v.get("name"),
                v.get("filters_json"),
                v.get("time_range_type"),
                v.get("start_time"),
                v.get("end_time"),
                v.get("page"),
                v.get("created_at"),
            )
            for v in views
        ],
    )
    con.commit()


# ── audit_logs ────────────────────────────────────────────────────────────────


def record_audit(service_id: str, event_type: str, details: dict, actor: str = "ui") -> None:
    con = get_con(service_id)
    con.execute(
        "INSERT INTO audit_logs (source_name, event_type, details, actor) VALUES (?, ?, ?, ?)",
        (service_id, event_type, json.dumps(details), actor),
    )
    con.commit()


def list_audit(service_id: str, limit: int = 200, since: str | None = None) -> list[dict]:
    """List audit log entries for a service, most recent first."""
    con = get_con(service_id)
    if since:
        rows = con.execute(
            "SELECT timestamp, source_name, event_type, details, actor FROM audit_logs "
            "WHERE source_name = ? AND timestamp >= ? ORDER BY timestamp DESC LIMIT ?",
            (service_id, since, limit),
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT timestamp, source_name, event_type, details, actor FROM audit_logs "
            "WHERE source_name = ? ORDER BY timestamp DESC LIMIT ?",
            (service_id, limit),
        ).fetchall()
    return [
        {
            "timestamp": str(r["timestamp"]) if r["timestamp"] is not None else "",
            "source_name": r["source_name"],
            "event_type": r["event_type"],
            "details": r["details"],
            "actor": r["actor"],
        }
        for r in rows
    ]


def get_audit_logs(
    service_id: str,
    *,
    event_type: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "timestamp",
    sort_dir: str = "DESC",
) -> tuple[int, list[dict]]:
    """Paginated audit log query with optional event_type filter."""
    con = get_con(service_id)
    where = ["source_name = ?"]
    params: list = [service_id]
    if event_type and event_type != "all":
        where.append("event_type = ?")
        params.append(event_type)
    where_sql = "WHERE " + " AND ".join(where)

    total = int(con.execute(f"SELECT count(*) FROM audit_logs {where_sql}", params).fetchone()[0])

    valid_sort_cols = {"timestamp", "event_type", "actor"}
    sort_col_safe = sort_col if sort_col in valid_sort_cols else "timestamp"
    sort_dir_safe = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    offset = (page - 1) * per_page

    rows = con.execute(
        f"""SELECT id, timestamp, event_type, details, actor
            FROM audit_logs {where_sql}
            ORDER BY {sort_col_safe} {sort_dir_safe}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    import json as _json

    entries = [
        {
            "id": r["id"],
            "timestamp": str(r["timestamp"]) if r["timestamp"] is not None else "",
            "event_type": r["event_type"],
            "details": _json.loads(r["details"] or "{}"),
            "actor": r["actor"],
            "source": "local",
        }
        for r in rows
    ]
    return total, entries


def export_audit(service_id: str, limit: int = 200) -> list[dict]:
    """Used by state_sync.export_admin_state — same as list_audit but with a stable column shape."""
    return list_audit(service_id, limit=limit)


def replace_audit_for_service(service_id: str, rows: list[dict]) -> None:
    """Replace all audit logs for a service. Used by state_sync.import_admin_state."""
    con = get_con(service_id)
    con.execute("DELETE FROM audit_logs WHERE source_name = ?", (service_id,))
    if rows:
        con.executemany(
            "INSERT INTO audit_logs (timestamp, source_name, event_type, details, actor) VALUES (?, ?, ?, ?, ?)",
            [
                (
                    r.get("timestamp"),
                    r.get("source_name"),
                    r.get("event_type"),
                    r.get("details"),
                    r.get("actor"),
                )
                for r in rows
            ],
        )
    con.commit()


def merge_audit_for_service(service_id: str, rows: list[dict]) -> None:
    """Insert audit log entries from remote without deleting local ones.

    Used by state_sync.import_admin_state on read_only analyst hosts to
    preserve local audit entries created by the analyst's own actions
    (which the wholesale ``replace_audit_for_service`` would have wiped on
    every cron tick).

    Dedup key: (timestamp, source_name, event_type, actor) — a row with
    those four fields equal to an existing row is considered the same
    event and skipped. ``timestamp`` has second precision so collisions
    between distinct events are improbable, and even if they happen the
    audit log tolerates the missed insert.
    """
    if not rows:
        return
    con = get_con(service_id)
    for r in rows:
        existing = con.execute(
            "SELECT 1 FROM audit_logs WHERE source_name = ? AND timestamp = ? AND event_type = ? AND actor = ? LIMIT 1",
            (r.get("source_name"), r.get("timestamp"), r.get("event_type"), r.get("actor")),
        ).fetchone()
        if existing:
            continue
        con.execute(
            "INSERT INTO audit_logs (timestamp, source_name, event_type, details, actor) VALUES (?, ?, ?, ?, ?)",
            (r.get("timestamp"), r.get("source_name"), r.get("event_type"), r.get("details"), r.get("actor")),
        )
    con.commit()


# ── ingested_files ────────────────────────────────────────────────────────────


def get_ingested_filenames(service_id: str, limit: int | None = None) -> set[str]:
    """Return the set of file_names already ingested for a service. Used by ingest dedup.

    ``limit`` (when set) caps the result to the N most-recently ingested files.
    Cron ingest passes a small limit (a few hundred k) so the 4s+ full-table
    fetchall on busy services doesn't dominate the per-tick wall time —
    incremental LIST only returns files within the lookback window, so older
    rows can't appear in dedup checks anyway. ``None`` preserves the legacy
    full-load behaviour for manual/full-sweep imports that scan the whole
    bucket.

    Bounded calls (``limit`` is not ``None``) read from a process-wide
    in-memory cache populated on first call and kept in sync by
    ``insert_ingested_files``. Cuts per-tick wall time by ~640 ms on
    services with >1 M ingested_files (1.66 s sync tick → ~1.0 s).
    Unbounded calls always hit SQLite for ground truth and invalidate the
    cache.
    """
    if limit is None:
        with _ingested_filenames_cache_lock:
            _ingested_filenames_cache.pop(service_id, None)
        con = get_con(service_id)
        rows = con.execute(
            "SELECT file_name FROM ingested_files WHERE source_name = ?",
            (service_id,),
        ).fetchall()
        return {r["file_name"] for r in rows}

    with _ingested_filenames_cache_lock:
        cached = _ingested_filenames_cache.get(service_id)
        if cached is not None:
            return cached.copy()

    con = get_con(service_id)
    rows = con.execute(
        "SELECT file_name FROM ingested_files WHERE source_name = ? ORDER BY ingested_at DESC LIMIT ?",
        (service_id, limit),
    ).fetchall()
    fresh = {r["file_name"] for r in rows}
    with _ingested_filenames_cache_lock:
        _ingested_filenames_cache[service_id] = fresh
    return fresh.copy()


def list_ingested_files(service_id: str, limit: int = 10000) -> list[dict]:
    """Return up to ``limit`` most-recent ingested files for a service.

    Capped at 10000 by default because the admin Ingestion-History DataTable
    renders client-side — pulling millions of rows over HTTP just to paginate
    them in JS was the 5s+ load time on busy services. 10000 rows still covers
    weeks of normal ingestion volume; admins who need older data can drop the
    cap explicitly.
    """
    con = get_con(service_id)
    rows = con.execute(
        "SELECT file_name, ingested_at, row_count, file_size_bytes FROM ingested_files "
        "WHERE source_name = ? ORDER BY ingested_at DESC LIMIT ?",
        (service_id, limit),
    ).fetchall()
    return [
        {
            "file_name": r["file_name"],
            "ingested_at": str(r["ingested_at"]) if r["ingested_at"] is not None else "",
            "row_count": r["row_count"],
            "file_size_bytes": r["file_size_bytes"],
        }
        for r in rows
    ]


def list_ingested_files_for_status(service_id: str) -> list[tuple[str, str, int | None, int | None]]:
    """Tuple-form variant used by refresh_config_status — avoids dict overhead in hot path."""
    con = get_con(service_id)
    rows = con.execute(
        "SELECT file_name, ingested_at, row_count, file_size_bytes FROM ingested_files WHERE source_name = ?",
        (service_id,),
    ).fetchall()
    return [(r["file_name"], r["ingested_at"], r["row_count"], r["file_size_bytes"]) for r in rows]


def _bootstrap_ingested_files_summary(con: sqlite3.Connection, service_id: str) -> dict:
    """One-time SQL aggregate to seed ``ingested_files_summary`` from existing rows.

    Pays the full ~4 s scan ONCE per service per app lifetime so subsequent
    ``get_ingested_files_status_summary`` calls are O(1) lookups against the
    rollup row. Called from the summary getter when the rollup is missing.
    """
    agg = con.execute(
        """
        SELECT
            COUNT(*)               AS file_count,
            COALESCE(SUM(row_count), 0)        AS total_rows,
            COALESCE(SUM(file_size_bytes), 0)  AS total_bytes,
            COUNT(file_size_bytes) AS count_with_bytes,
            MAX(ingested_at)       AS last_ingested
        FROM ingested_files
        WHERE source_name = ?
        """,
        (service_id,),
    ).fetchone()
    latest_fn_row = con.execute(
        "SELECT file_name FROM ingested_files WHERE source_name = ? ORDER BY ingested_at DESC LIMIT 1",
        (service_id,),
    ).fetchone()
    summary = {
        "file_count": (agg["file_count"] if agg else 0) or 0,
        "total_rows": (agg["total_rows"] if agg else 0) or 0,
        "total_bytes": (agg["total_bytes"] if agg else 0) or 0,
        "count_with_bytes": (agg["count_with_bytes"] if agg else 0) or 0,
        "last_ingested": (agg["last_ingested"] if agg else None),
        "latest_file_name": (latest_fn_row["file_name"] if latest_fn_row else None),
    }
    con.execute(
        """INSERT INTO ingested_files_summary
               (source_name, file_count, total_rows, total_bytes,
                count_with_bytes, latest_file_name, last_ingested)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(source_name) DO UPDATE SET
               file_count = excluded.file_count,
               total_rows = excluded.total_rows,
               total_bytes = excluded.total_bytes,
               count_with_bytes = excluded.count_with_bytes,
               latest_file_name = excluded.latest_file_name,
               last_ingested = excluded.last_ingested""",
        (
            service_id,
            summary["file_count"],
            summary["total_rows"],
            summary["total_bytes"],
            summary["count_with_bytes"],
            summary["latest_file_name"],
            summary["last_ingested"],
        ),
    )
    con.commit()
    return summary


def get_ingested_files_status_summary(service_id: str) -> dict:
    """O(1) rollup read for ``get_sync_status`` header fields.

    Replaces the per-tick ``list_ingested_files_for_status`` fetchall + Python
    sum/max loop that scaled with table size and hit ~5 s on services with
    >1 M ingested files. Maintained transactionally by
    ``insert_ingested_files``; bootstrapped lazily from a one-time aggregate
    scan if the rollup row is missing (e.g. first read after upgrade).

    Returns ``{file_count, total_rows, total_bytes, count_with_bytes,
    last_ingested, latest_file_name}`` with zero/None defaults when no files
    are ingested yet.
    """
    con = get_con(service_id)
    row = con.execute(
        "SELECT file_count, total_rows, total_bytes, count_with_bytes, "
        "       latest_file_name, last_ingested "
        "FROM ingested_files_summary WHERE source_name = ?",
        (service_id,),
    ).fetchone()
    if row is None:
        return _bootstrap_ingested_files_summary(con, service_id)
    return {
        "file_count": row["file_count"] or 0,
        "total_rows": row["total_rows"] or 0,
        "total_bytes": row["total_bytes"] or 0,
        "count_with_bytes": row["count_with_bytes"] or 0,
        "last_ingested": row["last_ingested"],
        "latest_file_name": row["latest_file_name"],
    }


def get_log_accounting_counts(
    service_id: str,
    sql_start: str,
    sql_end: str,
    width: int,
    start_bucket: str,
    end_bucket: str,
) -> dict[str, tuple[int, int]]:
    """Return ``{bucket: (rows, files)}`` for log-accounting reconciliation.

    The compute_log_accounting endpoint used to pull every row in the padded
    ±2h window into Python and run a per-row regex to extract the emission
    bucket from the filename — ~100K rows × regex/dict ops per render of the
    log-accounting panel. Pushing the bucket extraction and group-by into
    SQLite returns ~N rows where N is the bucket count (24-72 for a typical
    window), letting the index do the heavy lifting.

    The CASE matches the Python ``_bucket_for_file`` fallback chain: if the
    full path contains a 'T' preceded by a YYYY-MM-DD prefix we slice the
    emission bucket out of the filename; otherwise we fall back to
    ``ingested_at`` (covers legacy/test files without an ISO basename).
    """
    con = get_con(service_id)
    rows = con.execute(
        """
        SELECT
          CASE
            WHEN instr(file_name, 'T') >= 11
             AND substr(file_name, instr(file_name, 'T') - 10, 10)
                 GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
            THEN substr(file_name, instr(file_name, 'T') - 10, ?)
            WHEN ingested_at IS NOT NULL
            THEN substr(replace(ingested_at, ' ', 'T'), 1, ?)
            ELSE NULL
          END AS bucket,
          sum(row_count) AS rows,
          count(*)       AS files
        FROM ingested_files
        WHERE source_name = ?
          AND datetime(ingested_at) >= datetime(?)
          AND datetime(ingested_at) <= datetime(?)
          AND file_name != '__seeding_attempted__'
        GROUP BY 1
        HAVING bucket IS NOT NULL AND bucket >= ? AND bucket <= ?
        """,
        (width, width, service_id, sql_start, sql_end, start_bucket, end_bucket),
    ).fetchall()
    return {r["bucket"]: (int(r["rows"] or 0), int(r["files"] or 0)) for r in rows}


def get_storage_stats_window(service_id: str, start_str: str, end_str: str) -> tuple[int, int]:
    """Return (file_count, total_bytes) for ingested_files in [start, end].

    Cost panel previously pulled every row (`list_ingested_files_for_status`)
    and filtered/summed in Python — millions of rows per service over HTTP +
    O(N) loop. Pushing COUNT/SUM into SQL lets it run against the source_name
    index and return two integers.
    """
    con = get_con(service_id)
    row = con.execute(
        """SELECT count(*) AS n, coalesce(sum(file_size_bytes), 0) AS bytes
           FROM ingested_files
           WHERE source_name = ?
             AND ingested_at >= ?
             AND ingested_at <= ?""",
        (service_id, start_str, end_str),
    ).fetchone()
    if not row:
        return 0, 0
    return int(row["n"] or 0), int(row["bytes"] or 0)


def list_unbackfilled_fastly_edge_files(
    service_id: str,
    since: str | None = None,
) -> list[tuple[str, str, int | None, int | None]]:
    """Return ingested_files rows that DON'T yet have a matching ``fastly.edge``
    row in ``usage_log``. Powers the incremental fast path in
    ``backfill_fastly_edge_writes`` so we stop re-checking ~7500 already-
    backfilled files on every cron tick.

    ``since`` (ISO timestamp string) bounds the outer scan via
    ``ingested_at >= since`` so the cron hot path doesn't pay the N×NOT EXISTS
    cost on million-row services where every file is already backfilled
    (steady-state: returns 0 rows but the scan itself was ~7 s). The bounded
    query uses ``idx_ingested_files_source_ingested_at`` for an indexed range
    scan and the inner ``NOT EXISTS`` continues to use ``idx_usage_dedup``.
    Pass ``None`` for an unbounded scan (rare — admin sweep, repair tools).
    """
    con = get_con(service_id)
    if since is None:
        rows = con.execute(
            """
            SELECT file_name, ingested_at, row_count, file_size_bytes
            FROM ingested_files
            WHERE source_name = ?
              AND file_name != '__seeding_attempted__'
              AND NOT EXISTS (
                SELECT 1 FROM usage_log
                WHERE service_id = ingested_files.source_name
                  AND function_name = 'fastly.edge'
                  AND url = ingested_files.file_name
              )
            """,
            (service_id,),
        ).fetchall()
    else:
        rows = con.execute(
            """
            SELECT file_name, ingested_at, row_count, file_size_bytes
            FROM ingested_files
            WHERE source_name = ?
              AND ingested_at >= ?
              AND file_name != '__seeding_attempted__'
              AND NOT EXISTS (
                SELECT 1 FROM usage_log
                WHERE service_id = ingested_files.source_name
                  AND function_name = 'fastly.edge'
                  AND url = ingested_files.file_name
              )
            """,
            (service_id, since),
        ).fetchall()
    return [(r["file_name"], r["ingested_at"], r["row_count"], r["file_size_bytes"]) for r in rows]


def get_latest_reconciliation_ts(service_id: str) -> str | None:
    """Return ISO timestamp of the most recent ``fastly.reconciliation`` row
    for the service, or ``None`` if none exist. Used by
    ``reconcile_fastly_stats`` to gate hourly so we don't burn Fastly API
    quota + run the per-class SUBSTR scans on every cron tick."""
    con = get_con(service_id)
    row = con.execute(
        """
        SELECT max(timestamp) AS latest
        FROM usage_log
        WHERE service_id = ? AND function_name = 'fastly.reconciliation'
        """,
        (service_id,),
    ).fetchone()
    if not row:
        return None
    return row["latest"] if row["latest"] else None


def register_locally_compacted(service_id: str, file_names: list[str]) -> None:
    """Record parquet basenames that local_compaction merged + deleted.

    sync_data uses this to distinguish "intentionally absent locally"
    (merged into a bigger local file) from "missing, needs re-fetch".
    """
    if not file_names:
        return
    con = get_con(service_id)
    con.executemany(
        "INSERT OR IGNORE INTO local_compacted_files (file_name) VALUES (?)",
        [(n,) for n in file_names],
    )
    con.commit()


def get_locally_compacted_basenames(service_id: str) -> set[str]:
    """Return the set of parquet basenames that local_compaction has
    intentionally removed (so sync_data should skip re-downloading them).
    Cached at the call site if used in a hot loop.
    """
    con = get_con(service_id)
    return {row[0] for row in con.execute("SELECT file_name FROM local_compacted_files").fetchall()}


def insert_ingested_files(service_id: str, rows: list[tuple[str, int, int | None]]) -> None:
    """Bulk-insert/upsert (file_name, row_count, file_size_bytes) rows for a service.

    Also maintains the ``ingested_files_summary`` rollup in the same
    transaction so dashboard refresh stays O(1) instead of scanning the full
    1M+ row table on every cron tick. Reads existing values for any rows that
    would upsert so the delta is correct (re-ingest of the same file must not
    double-count its bytes).
    """
    if not rows:
        return
    con = get_con(service_id)

    # Bootstrap the rollup if missing — without this, the delta UPSERT below
    # would seed the rollup with only THIS batch's counts when ingested_files
    # already had a million rows (first insert after upgrade on a populated
    # service). The bootstrap commits in its own statement; the delta update
    # below then correctly adds this batch on top.
    if (
        con.execute(
            "SELECT 1 FROM ingested_files_summary WHERE source_name = ?",
            (service_id,),
        ).fetchone()
        is None
    ):
        _bootstrap_ingested_files_summary(con, service_id)

    # Snapshot existing values for rows that already exist, so we can compute
    # accurate (new - old) deltas for the rollup even when this batch upserts.
    file_names = [fn for fn, _, _ in rows]
    existing: dict[str, tuple[int | None, int | None]] = {}
    chunk = 500  # SQLite default expression-tree depth allows ~1000 params
    for i in range(0, len(file_names), chunk):
        batch = file_names[i : i + chunk]
        placeholders = ",".join(["?"] * len(batch))
        for r in con.execute(
            f"SELECT file_name, row_count, file_size_bytes FROM ingested_files "
            f"WHERE source_name = ? AND file_name IN ({placeholders})",
            (service_id, *batch),
        ).fetchall():
            existing[r["file_name"]] = (r["row_count"], r["file_size_bytes"])

    file_count_delta = 0
    rows_delta = 0
    bytes_delta = 0
    count_with_bytes_delta = 0
    latest_file_name = max(file_names)  # lexicographic; filenames embed timestamp
    for fn, rc, sz in rows:
        if fn in existing:
            old_rc, old_sz = existing[fn]
            rows_delta += (rc or 0) - (old_rc or 0)
            bytes_delta += (sz or 0) - (old_sz or 0)
            had_size = old_sz is not None
            has_size = sz is not None
            if has_size and not had_size:
                count_with_bytes_delta += 1
            elif had_size and not has_size:
                count_with_bytes_delta -= 1
        else:
            file_count_delta += 1
            rows_delta += rc or 0
            bytes_delta += sz or 0
            if sz is not None:
                count_with_bytes_delta += 1

    con.executemany(
        """INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(file_name, source_name) DO UPDATE SET
               row_count = excluded.row_count,
               file_size_bytes = excluded.file_size_bytes""",
        [(fn, service_id, rc, sz) for (fn, rc, sz) in rows],
    )
    # Use the just-applied DB clock so last_ingested matches the row's
    # ingested_at default (datetime('now')) — keeps the rollup honest.
    now_str = con.execute("SELECT datetime('now')").fetchone()[0]
    con.execute(
        """INSERT INTO ingested_files_summary
               (source_name, file_count, total_rows, total_bytes,
                count_with_bytes, latest_file_name, last_ingested)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(source_name) DO UPDATE SET
               file_count       = file_count + excluded.file_count,
               total_rows       = total_rows + excluded.total_rows,
               total_bytes      = total_bytes + excluded.total_bytes,
               count_with_bytes = count_with_bytes + excluded.count_with_bytes,
               latest_file_name = CASE
                   WHEN latest_file_name IS NULL OR excluded.latest_file_name > latest_file_name
                       THEN excluded.latest_file_name
                   ELSE latest_file_name
               END,
               last_ingested = CASE
                   WHEN last_ingested IS NULL OR excluded.last_ingested > last_ingested
                       THEN excluded.last_ingested
                   ELSE last_ingested
               END""",
        (
            service_id,
            file_count_delta,
            rows_delta,
            bytes_delta,
            count_with_bytes_delta,
            latest_file_name,
            now_str,
        ),
    )
    con.commit()

    # Keep the dedup cache in sync. Only extend if the cache is already
    # populated — seeding it here would prematurely cap a fresh process's
    # cache to just this batch when ingested_files already had millions of
    # rows.
    with _ingested_filenames_cache_lock:
        cached = _ingested_filenames_cache.get(service_id)
        if cached is not None:
            cached.update(file_names)


def record_in_flight(
    service_id: str,
    buffer_filename: str,
    rows: list[tuple[str, int, int | None]],
) -> None:
    """Persist the (file_name, row_count, file_size) tuples that BELONG to a
    buffer Parquet, BEFORE the Parquet is written.

    On crash recovery, ``list_in_flight`` returns these tuples so the sweep
    can promote them into ``ingested_files`` without re-parsing the buffer.
    Upsert semantics: a re-run of the same chunk (same deterministic
    buffer filename) overwrites the prior manifest — never raises.
    """
    con = get_con(service_id)
    con.execute(
        """INSERT INTO ingest_in_flight (buffer_filename, source_name, files_json, started_at)
           VALUES (?, ?, ?, datetime('now'))
           ON CONFLICT(buffer_filename) DO UPDATE SET
               source_name = excluded.source_name,
               files_json = excluded.files_json,
               started_at = excluded.started_at""",
        (buffer_filename, service_id, json.dumps(rows)),
    )
    con.commit()


def clear_in_flight(service_id: str, buffer_filename: str) -> None:
    """Drop the in_flight row for ``buffer_filename`` after its files have
    been committed to ``ingested_files``. Idempotent."""
    con = get_con(service_id)
    con.execute(
        "DELETE FROM ingest_in_flight WHERE source_name = ? AND buffer_filename = ?",
        (service_id, buffer_filename),
    )
    con.commit()


def list_in_flight(service_id: str) -> list[tuple[str, list[tuple[str, int, int | None]]]]:
    """Return [(buffer_filename, [(file_name, row_count, file_size), ...]), ...]
    for every pending row belonging to this service. Used by the crash-
    recovery sweep at the start of every ingest tick."""
    con = get_con(service_id)
    rows = con.execute(
        "SELECT buffer_filename, files_json FROM ingest_in_flight WHERE source_name = ?",
        (service_id,),
    ).fetchall()
    out: list[tuple[str, list[tuple[str, int, int | None]]]] = []
    for r in rows:
        try:
            tuples = [tuple(t) for t in json.loads(r["files_json"] or "[]")]
        except (json.JSONDecodeError, TypeError):
            tuples = []
        out.append((r["buffer_filename"], tuples))
    return out


def get_log_activity(service_id: str, start_iso: str, end_iso: str, by: str) -> dict:
    """Return time-bucketed log activity (rows + bytes ingested per bucket).

    SQLite has no DATE_TRUNC, so we bucket via SUBSTR on the ISO timestamp.
    Used by /api/usage/log-activity.
    """
    width_map = {
        "second": 19,  # YYYY-MM-DDTHH:MM:SS
        "minute": 16,  # YYYY-MM-DDTHH:MM
        "hour": 13,  # YYYY-MM-DDTHH
        "day": 10,  # YYYY-MM-DD
    }
    width = width_map.get(by, 13)

    con = get_con(service_id)
    rows = con.execute(
        f"""
        SELECT substr(replace(ingested_at, ' ', 'T'), 1, {width}) AS bucket,
               sum(row_count) AS rc,
               sum(file_size_bytes) AS bs
        FROM ingested_files
        WHERE source_name = ?
          AND file_name != '__seeding_attempted__'
          AND ingested_at >= ?
          AND ingested_at <= ?
        GROUP BY bucket ORDER BY bucket
        """,
        (service_id, start_iso, end_iso),
    ).fetchall()

    def _normalize(bucket: str) -> str:
        if by == "hour":
            return bucket + ":00"
        if by == "minute" and len(bucket) == 16:
            return bucket
        if by == "day":
            return bucket
        return bucket

    points: list[dict] = []
    total_rows = 0
    total_bytes = 0
    for r in rows:
        if r["bucket"] is None:
            continue
        rc = int(r["rc"] or 0)
        bs = int(r["bs"] or 0)
        points.append({"time": _normalize(str(r["bucket"])), "row_count": rc, "bytes": bs})
        total_rows += rc
        total_bytes += bs
    return {
        "data": points,
        "total_rows": total_rows,
        "total_bytes": total_bytes,
        "granularity": by,
    }


def get_node_count_avg(service_id: str) -> float | None:
    """Average number of files-per-flush, derived from the basename timestamp.

    Used by routers/usage.py prefill estimator. The basename always starts with
    YYYY-MM-DDTHH:MM:SS — the first 'T' in the path is always the timestamp T
    (bucket/prefix segments are lowercase + numeric). Grouping by that 19-char
    substring is equivalent to the prior Python regex over file_name, but runs
    entirely in SQLite instead of dragging every row across the boundary.
    """
    con = get_con(service_id)
    row = con.execute(
        """SELECT avg(c) AS avg_c FROM (
               SELECT count(*) AS c
               FROM ingested_files
               WHERE source_name = ?
                 AND instr(file_name, 'T') >= 11
               GROUP BY substr(file_name, instr(file_name, 'T') - 10, 19)
           )""",
        (service_id,),
    ).fetchone()
    if not row or row["avg_c"] is None:
        return None
    return float(row["avg_c"])


# ── cron_runs ─────────────────────────────────────────────────────────────────


def start_cron_run(service_id: str, task: str) -> int:
    """Create a 'running' cron run row, reaping orphans first.

    Raises RuntimeError if a run of the same task is already in progress
    (within the orphan threshold). Returns the new row id.
    """
    con = get_con(service_id)
    started_at = iso_z_now()
    time_cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=_ORPHAN_THRESHOLD_MINS))

    # Reap orphans first (rows still 'running' but older than the threshold).
    con.execute(
        "UPDATE cron_runs SET status = 'error', "
        "error_message = COALESCE(error_message, 'Process interrupted') "
        "WHERE task = ? AND status = 'running' AND started_at < ?",
        (task, time_cutoff),
    )

    busy = con.execute(
        "SELECT count(*) AS n FROM cron_runs WHERE task = ? AND status = 'running'",
        (task,),
    ).fetchone()
    if busy and busy["n"] > 0:
        con.commit()
        raise RuntimeError(f"Task '{task}' is already running for this service.")

    cur = con.execute(
        "INSERT INTO cron_runs (task, started_at, duration_s, status, parquet_keys) "
        "VALUES (?, ?, 0.0, 'running', '[]')",
        (task, started_at),
    )
    con.commit()
    return int(cur.lastrowid or 0)


def log_cron_run(
    service_id: str,
    task: str,
    duration_s: float,
    status: str,
    *,
    error_message: str | None = None,
    files_downloaded: int = 0,
    files_deleted_fos: int = 0,
    rows_ingested: int = 0,
    corrupt_rows: int = 0,
    parquet_files_created: int = 0,
    parquet_files_optimized: int = 0,
    parquet_keys: list | None = None,
    summary: str | None = None,
    log_output: str | None = None,
    run_id: int | None = None,
) -> None:
    """Update an existing cron_run row by id, or insert a new completed one.

    When ``run_id`` is provided (the common case — start_cron_run created the
    row), this UPDATEs in place. Otherwise INSERTs a fresh terminal row
    (used by paths that didn't go through start_cron_run, e.g. retries).
    """
    con = get_con(service_id)
    started_at = iso_z(datetime.now(UTC) - timedelta(seconds=max(duration_s, 0)))
    keys_json = json.dumps(parquet_keys or [])
    if run_id is not None:
        con.execute(
            """UPDATE cron_runs SET
                duration_s = ?, status = ?, error_message = ?,
                files_downloaded = ?, files_deleted_fos = ?, rows_ingested = ?, corrupt_rows = ?,
                parquet_files_created = ?, parquet_files_optimized = ?,
                parquet_keys = ?, summary = ?, log_output = ?
               WHERE id = ?""",
            (
                duration_s,
                status,
                error_message,
                files_downloaded,
                files_deleted_fos,
                rows_ingested,
                corrupt_rows,
                parquet_files_created,
                parquet_files_optimized,
                keys_json,
                summary,
                log_output,
                run_id,
            ),
        )
    else:
        con.execute(
            """INSERT INTO cron_runs (task, started_at, duration_s, status, error_message,
                files_downloaded, files_deleted_fos, rows_ingested, corrupt_rows,
                parquet_files_created, parquet_files_optimized, parquet_keys, summary, log_output)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                task,
                started_at,
                duration_s,
                status,
                error_message,
                files_downloaded,
                files_deleted_fos,
                rows_ingested,
                corrupt_rows,
                parquet_files_created,
                parquet_files_optimized,
                keys_json,
                summary,
                log_output,
            ),
        )
    con.commit()


def update_cron_duration(
    service_id: str,
    run_id: int,
    duration_s: float,
    log_output: str | None = None,
) -> None:
    con = get_con(service_id)
    if log_output is None:
        con.execute(
            "UPDATE cron_runs SET duration_s = ? WHERE id = ?",
            (duration_s, run_id),
        )
    else:
        con.execute(
            "UPDATE cron_runs SET duration_s = ?, log_output = ? WHERE id = ?",
            (duration_s, log_output, run_id),
        )
    con.commit()


def delete_cron_run(service_id: str, run_id: int) -> None:
    con = get_con(service_id)
    con.execute("DELETE FROM cron_runs WHERE id = ?", (run_id,))
    con.commit()


def purge_cron_runs(
    service_id: str,
    *,
    task: str | None = None,
    days: int | None = None,
) -> None:
    con = get_con(service_id)
    where: list[str] = []
    params: list = []
    if task and task != "all":
        where.append("task = ?")
        params.append(task)
    if days is not None:
        cutoff = iso_z(datetime.now(UTC) - timedelta(days=days))
        where.append("started_at < ?")
        params.append(cutoff)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    con.execute(f"DELETE FROM cron_runs {where_sql}", params)
    con.commit()


def record_scoring_audit(
    service_id: str,
    action: str,
    *,
    actor: str = "operator",
    details: dict | None = None,
) -> None:
    """Append an operator-attribution row to the scoring_audit log.

    Called from every scoring-config-mutating endpoint (enable, disable,
    threshold commit + enforce, retrain, rotate-key, matrix-rollback).
    Best-effort: any SQLite failure is logged at DEBUG and swallowed so
    a busy WAL doesn't block the actual operator action.
    """
    try:
        con = get_con(service_id)
        con.execute(
            "INSERT INTO scoring_audit (service_id, action, actor, details) VALUES (?, ?, ?, ?)",
            (service_id, action, actor, json.dumps(details) if details else None),
        )
        con.commit()
    except sqlite3.Error as e:
        logger.debug("[metadata_db] record_scoring_audit(%s, %s) failed: %s", service_id, action, e)


def list_scoring_audit(
    service_id: str,
    *,
    limit: int = 100,
    since: str | None = None,
) -> list[dict]:
    """Most-recent first. Optional ISO ``since`` timestamp lower bound."""
    try:
        con = get_con(service_id)
        if since:
            rows = con.execute(
                "SELECT id, timestamp, action, actor, details FROM scoring_audit "
                "WHERE service_id = ? AND timestamp >= ? ORDER BY id DESC LIMIT ?",
                (service_id, since, limit),
            ).fetchall()
        else:
            rows = con.execute(
                "SELECT id, timestamp, action, actor, details FROM scoring_audit "
                "WHERE service_id = ? ORDER BY id DESC LIMIT ?",
                (service_id, limit),
            ).fetchall()
        out = []
        for r in rows:
            row = dict(r)
            if row.get("details"):
                try:
                    row["details"] = json.loads(row["details"])
                except (ValueError, TypeError):
                    pass
            out.append(row)
        return out
    except sqlite3.Error as e:
        logger.debug("[metadata_db] list_scoring_audit(%s) failed: %s", service_id, e)
        return []


def prune_scoring_audit(service_id: str, *, keep_last: int = 10000) -> None:
    """Trim scoring_audit to the most recent ``keep_last`` rows per service.

    Cheap unbounded growth guard — every scoring-config mutation appends
    one row, and the table is only ever read by the admin UI / state_sync
    export which already caps its own page size. Best-effort: any SQLite
    failure is logged at DEBUG and swallowed so trimming never blocks the
    caller (typically a maintenance cron, not the operator hot path).
    """
    try:
        con = get_con(service_id)
        # Tiebreak on id DESC so concurrent inserts that landed in the same
        # `datetime('now')` second are deterministically ordered (otherwise
        # SQLite is free to pick any row from the tied group, which makes
        # prune flaky under burst workloads and breaks reproducibility tests).
        con.execute(
            "DELETE FROM scoring_audit WHERE service_id = ? AND id NOT IN ("
            "SELECT id FROM scoring_audit WHERE service_id = ? ORDER BY timestamp DESC, id DESC LIMIT ?)",
            (service_id, service_id, keep_last),
        )
        con.commit()
    except sqlite3.Error as e:
        logger.debug("[metadata_db] prune_scoring_audit(%s) failed: %s", service_id, e)


def get_cron_run_status(service_id: str, run_id: int) -> str | None:
    """Return the status string for a single cron_runs row, or None if
    the row doesn't exist. Used by cron_progress.list_active_runs to
    cross-check the in-memory state against the DB-of-truth (catches
    abandoned-worker-thread zombies that completed log_cron_run but
    never fired end_progress).

    Narrowed exception scope: catches sqlite3.Error (DB unreachable,
    table missing, locked) and logs at DEBUG so the next 'why isn't
    the cross-check firing?' triage isn't flying blind. Returns None
    on any DB failure so list_active_runs falls back to the in-memory
    signal (we'd rather show a false in-flight than miss a real one).
    """
    try:
        con = get_con(service_id)
        row = con.execute("SELECT status FROM cron_runs WHERE id = ?", (run_id,)).fetchone()
        return row["status"] if row else None
    except sqlite3.Error as e:
        logger.debug("[metadata_db] get_cron_run_status(%s, %s) failed: %s", service_id, run_id, e)
        return None


def get_cron_runs(
    service_id: str,
    *,
    task: str | None = None,
    status: str | None = None,
    page: int = 1,
    per_page: int = 50,
    sort_col: str = "started_at",
    sort_dir: str = "DESC",
) -> tuple[int, list[dict]]:
    """Paginated cron run history. Used by repositories/cron.py."""
    con = get_con(service_id)
    where: list[str] = []
    params: list = []
    if task and task != "all":
        where.append("task = ?")
        params.append(task)
    if status and status != "all":
        where.append("status = ?")
        params.append(status)
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    total_row = con.execute(f"SELECT count(*) AS n FROM cron_runs {where_sql}", params).fetchone()
    total = int(total_row["n"]) if total_row else 0

    valid_sort_cols = {"started_at", "duration_s", "task", "status"}
    sort_col_safe = sort_col if sort_col in valid_sort_cols else "started_at"
    sort_dir_safe = "ASC" if sort_dir.upper() == "ASC" else "DESC"
    offset = (page - 1) * per_page

    rows = con.execute(
        f"""SELECT id, task, started_at, duration_s, status, error_message,
                   files_downloaded, files_deleted_fos, rows_ingested, corrupt_rows,
                   parquet_files_created, parquet_files_optimized, parquet_keys, summary
            FROM cron_runs {where_sql}
            ORDER BY {sort_col_safe} {sort_dir_safe}
            LIMIT ? OFFSET ?""",
        params + [per_page, offset],
    ).fetchall()

    entries = [
        {
            "id": r["id"],
            "task": r["task"],
            "started_at": r["started_at"],
            "duration_s": r["duration_s"],
            "status": r["status"],
            "error_message": r["error_message"],
            "files_downloaded": r["files_downloaded"],
            "files_deleted_fos": r["files_deleted_fos"],
            "rows_ingested": r["rows_ingested"],
            "corrupt_rows": r["corrupt_rows"],
            "parquet_files_created": r["parquet_files_created"],
            "parquet_files_optimized": r["parquet_files_optimized"],
            "parquet_keys": json.loads(r["parquet_keys"] or "[]"),
            "summary": r["summary"],
        }
        for r in rows
    ]
    return total, entries


def latest_cron_per_task(service_id: str) -> dict[str, dict]:
    """Return {task: latest_completed_run_dict} for the sync-status endpoint.

    The original `id IN (SELECT max(id) GROUP BY task)` form forced a full
    scan + GROUP BY across cron_runs (210ms / 44K rows on prod). This rewrite
    pulls the distinct task list (cheap — usually <10 tasks) and does one
    btree-seek per task into `idx_cron_task_started(task, started_at)` to find
    the latest non-`running` row, taking ~25ms. Result is identical because
    ids and started_at are co-monotonic for the same task.
    """
    con = get_con(service_id)
    rows = con.execute(
        """WITH tasks AS (SELECT DISTINCT task FROM cron_runs),
                latest AS (
                    SELECT t.task, (
                        SELECT c2.id FROM cron_runs c2
                        WHERE c2.task = t.task AND c2.status != 'running'
                        ORDER BY c2.started_at DESC LIMIT 1
                    ) AS lid
                    FROM tasks t
                )
            SELECT c.task, c.started_at, c.status, c.duration_s, c.summary, c.error_message
            FROM cron_runs c JOIN latest l ON c.id = l.lid"""
    ).fetchall()
    return {
        r["task"]: {
            "started_at": r["started_at"],
            "status": r["status"],
            "duration_s": r["duration_s"],
            "summary": r["summary"],
            "error_message": r["error_message"],
        }
        for r in rows
    }


def reap_running_cron_runs(service_id: str, reason: str = "Process interrupted by server restart") -> int:
    """Mark every ``running`` cron row as ``error``, regardless of age.

    Called at backend startup: in-memory progress dicts (``backend.cron_progress``)
    are wiped on every restart, so any row still marked ``running`` in SQLite is
    by definition an orphan — its event stream is gone and the worker thread
    that owned it died with the previous process. Without this reap, the run
    sits in the DB until the next sync of the *same task* triggers
    ``start_cron_run``'s 60-minute orphan cutoff — and in the meantime the UI
    polls ``/api/cron-runs?status=running``, sees the stale row, and mounts a
    ``CronLiveLog`` that hangs on "Loading logs..." until the SSE endpoint
    times out 30 s later.

    Returns the number of rows reaped (0 if none).
    """
    con = get_con(service_id)
    cur = con.execute(
        "UPDATE cron_runs SET status = 'error', error_message = COALESCE(error_message, ?) WHERE status = 'running'",
        (reason,),
    )
    con.commit()
    return int(cur.rowcount or 0)


def cron_busy(service_id: str) -> bool:
    """True if any cron run is currently 'running' within the orphan threshold."""
    con = get_con(service_id)
    time_cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=_ORPHAN_THRESHOLD_MINS))
    row = con.execute(
        "SELECT count(*) AS n FROM cron_runs WHERE status = 'running' AND started_at > ?",
        (time_cutoff,),
    ).fetchone()
    return bool(row and row["n"] > 0)


def cron_summary_for_tasks(service_id: str, tasks: tuple[str, ...] = ("sync", "commit")) -> dict[str, dict]:
    """For each named task, return the latest run's summary fields. Used by refresh_config_status."""
    if not tasks:
        return {}
    con = get_con(service_id)
    placeholders = ",".join("?" * len(tasks))
    rows = con.execute(
        f"""
        SELECT task, started_at, duration_s, status, error_message, summary
        FROM (
            SELECT task, started_at, duration_s, status, error_message, summary,
                   ROW_NUMBER() OVER (PARTITION BY task ORDER BY started_at DESC) AS rn
            FROM cron_runs
            WHERE task IN ({placeholders})
        )
        WHERE rn = 1
        """,
        tasks,
    ).fetchall()
    return {
        row["task"]: {
            "last_run": row["started_at"],
            "duration_s": row["duration_s"],
            "status": row["status"],
            "error_message": row["error_message"],
            "summary": row["summary"],
        }
        for row in rows
    }


# ── asn_names ─────────────────────────────────────────────────────────────────


def lookup_asn_names(service_id: str, asns: list[int], max_age_days: int = 30) -> dict[int, str]:
    """Return cached {asn: name} for the requested ASNs that are still fresh."""
    if not asns:
        return {}
    con = get_con(service_id)
    fresh_cutoff = (datetime.now(UTC) - timedelta(days=max_age_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    placeholders = ",".join("?" * len(asns))
    rows = con.execute(
        f"SELECT asn, name FROM asn_names WHERE asn IN ({placeholders}) AND fetched_at >= ?",
        list(asns) + [fresh_cutoff],
    ).fetchall()
    return {int(r["asn"]): r["name"] for r in rows}


def upsert_asn_names(service_id: str, mapping: dict[int, str]) -> None:
    if not mapping:
        return
    con = get_con(service_id)
    now = iso_z_now()
    con.executemany(
        "INSERT INTO asn_names (asn, name, fetched_at) VALUES (?, ?, ?) "
        "ON CONFLICT(asn) DO UPDATE SET name = excluded.name, fetched_at = excluded.fetched_at",
        [(int(asn), name, now) for asn, name in mapping.items()],
    )
    con.commit()


def asn_ints_for_search(service_id: str, name_ilike: str) -> list[int]:
    """Return ASN integers whose cached name matches the given LIKE pattern.

    Used by the dashboard ASN search to pre-fetch matching ASNs and inline them
    into a DuckDB IN clause (avoids cross-engine JOINs).
    """
    con = get_con(service_id)
    rows = con.execute(
        "SELECT asn FROM asn_names WHERE name LIKE ? COLLATE NOCASE",
        (name_ilike,),
    ).fetchall()
    return [int(r["asn"]) for r in rows]


# ── sources ───────────────────────────────────────────────────────────────────


def register_source(service_id: str, name: str, config_json: str, table_name: str) -> None:
    """Idempotently register a source. Returns nothing (callers compute table_name themselves)."""
    con = get_con(service_id)
    con.execute(
        "INSERT OR IGNORE INTO sources (name, config, table_name) VALUES (?, ?, ?)",
        (name, config_json, table_name),
    )
    con.commit()


def get_source_by_name(service_id: str, name: str) -> dict | None:
    con = get_con(service_id)
    row = con.execute(
        "SELECT name, config, table_name FROM sources WHERE name = ?",
        (name,),
    ).fetchone()
    if not row:
        return None
    return {"name": row["name"], "config": row["config"], "table_name": row["table_name"]}


# ── usage_log ─────────────────────────────────────────────────────────────────


def log_usage_calls(service_id: str, calls: list[dict], process_context: str | None = None) -> None:
    if not calls:
        return
    con = get_con(service_id)
    now = iso_z_now()
    rows = []
    for c in calls:
        op_type = (c.get("method") or "").upper()
        details = c.get("details") or ""
        svc = c.get("service", "FOS")

        # FOS classification:
        #   Class A: PUT/POST/COPY/LIST family (mutating writes, multi-object delete via POST ?delete).
        #     Canonical S3 op names land here; so do raw HTTP verbs PUT/POST/COPY,
        #     which is what the telemetry proxy emits via request.method.
        #   Class B: GET/HEAD/single-object DELETE (the default).
        # Note: single-object DELETE (`DELETE /key`) is Class B in Fastly billing;
        # the DeleteObjects batch endpoint arrives as POST and is therefore A.
        op_class = "B"
        if svc == "FOS" and op_type in (
            "PUT_OBJECT",
            "POST_OBJECT",
            "COPY_OBJECT",
            "LIST_OBJECTS_V2",
            "DELETE_OBJECTS",
            "PUT",
            "POST",
            "COPY",
        ):
            op_class = "A"
        elif svc == "CDN":
            op_class = "CDN"
        elif "Class A" in details:
            op_class = "A"

        # Apply shield egress multiplier for CDN operations
        op_bytes = c.get("bytes")
        if op_class == "CDN" and op_bytes is not None:
            # X-Cache values are stored at the beginning of details: "HIT, MISS · duckdb httpfs"
            # Fastly X-Cache order is: Shield POP first, Edge POP second.
            # If there's a comma (multiple POPs) AND the Edge POP (the last value)
            # is MISS or PASS, the Edge fetched the payload from the Shield.
            # This doubles the egress cost (Shield -> Edge -> Client).
            x_cache_part = details.split(" · ")[0] if " · " in details else details
            parts = [p.strip().upper() for p in x_cache_part.split(",") if p.strip()]
            if len(parts) > 1 and parts[-1] in ("MISS", "PASS"):
                op_bytes = op_bytes * 2

        rows.append(
            (
                now,
                service_id,
                op_class,
                c.get("method"),
                c.get("path"),
                str(c.get("status", "OK")),
                c.get("time_ms"),
                c.get("caller"),
                process_context,
                op_bytes,
            )
        )
    try:
        con.executemany(
            "INSERT INTO usage_log "
            "(timestamp, service_id, operation_class, operation_type, url, status, "
            " duration_ms, function_name, process_context, bytes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        con.commit()
    except Exception as e:
        logger.error("[metadata_db] Failed to log usage calls: %s", e)


def log_synthetic_usage(service_id: str, calls: list[dict]) -> int:
    """Idempotently log synthetic usage rows (e.g. Fastly-edge backfill).

    Dedupes against existing rows where function_name = 'fastly.edge' AND url IN (incoming).
    Returns the number of newly inserted rows.
    """
    if not calls:
        return 0
    con = get_con(service_id)

    urls = [c.get("path") for c in calls if c.get("path")]
    if not urls:
        return 0

    existing: set[str] = set()
    for i in range(0, len(urls), 500):
        chunk = urls[i : i + 500]
        placeholders = ", ".join("?" for _ in chunk)
        cur = con.execute(
            f"SELECT url FROM usage_log WHERE service_id = ? AND function_name = 'fastly.edge' AND url IN ({placeholders})",
            [service_id] + chunk,
        )
        existing.update(r["url"] for r in cur.fetchall())

    new_rows = []
    now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    for c in calls:
        url = c.get("path")
        if not url or url in existing:
            continue
        ts = c.get("_timestamp_override") or now_iso
        new_rows.append(
            (
                ts,
                service_id,
                "A",
                c.get("method", "PUT_OBJECT"),
                url,
                str(c.get("status", "OK")),
                0.0,
                c.get("caller", "fastly.edge"),
                c.get("process_context", "fastly:log_write"),
                c.get("bytes"),
            )
        )

    if not new_rows:
        return 0
    try:
        con.executemany(
            "INSERT INTO usage_log "
            "(timestamp, service_id, operation_class, operation_type, url, status, "
            " duration_ms, function_name, process_context, bytes) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            new_rows,
        )
        con.commit()
        return len(new_rows)
    except Exception as e:
        logger.error("[metadata_db] Synthetic usage log failed: %s", e)
        return 0


def reconcile_fastly_stats(
    service_id: str,
    hourly_records: list[dict],
) -> int:
    """Upsert per-hour reconciliation rows to align local usage_log with Fastly's
    authoritative /stats/aggregate counts.

    Each record in ``hourly_records`` is a dict with::

        {
            "hour_iso": "2026-05-22T13:00:00Z",  # bucket start (UTC, hour-aligned)
            "class_a": <int>,                     # Fastly's reported Class A ops for the hour
            "class_b": <int>,                     # Fastly's reported Class B ops for the hour
        }

    For each (hour, class) pair we compute ``gap = fastly_count - local_sum``
    where ``local_sum`` is SUM(count) over rows in that hour excluding prior
    reconciliation rows. We then DELETE any existing reconciliation rows for
    that hour/class and INSERT one row with ``count = gap`` when gap > 0.

    Reconciliation rows are tagged ``function_name='fastly.reconciliation'`` and
    ``process_context='fastly:reconciliation'`` so they're trivially separable
    from observed rows in queries and excluded from future ``local_sum`` math.

    Returns the number of reconciliation rows written (one per non-zero gap).
    """
    if not hourly_records:
        return 0
    con = get_con(service_id)

    # Normalise the incoming records into {hour_start_iso: {"A": int, "B": int}}.
    by_hour: dict[str, dict[str, int]] = {}
    earliest: datetime | None = None
    latest: datetime | None = None
    for rec in hourly_records:
        hour_iso = rec.get("hour_iso")
        if not hour_iso:
            continue
        try:
            start_dt = datetime.strptime(hour_iso.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z")
        except (ValueError, AttributeError):
            continue
        start_str = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        by_hour[start_str] = {
            "A": int(rec.get("class_a") or 0),
            "B": int(rec.get("class_b") or 0),
        }
        if earliest is None or start_dt < earliest:
            earliest = start_dt
        if latest is None or start_dt > latest:
            latest = start_dt

    if not by_hour or earliest is None or latest is None:
        return 0

    window_start = earliest.strftime("%Y-%m-%dT%H:%M:%SZ")
    window_end = (latest + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Single scan covering both classes — substr() truncates the ISO
    # timestamp to its hour prefix; SQLite groups by string equality,
    # which works because we write all rows in the same "%Y-%m-%dT%H:%M:%SZ"
    # format. The supporting index is idx_usage_reconcile (service_id,
    # operation_class, timestamp), so the IN-list still uses the index.
    local_sums: dict[tuple[str, str], int] = {}
    for r in con.execute(
        """
        SELECT operation_class, substr(timestamp, 1, 13), coalesce(sum(count), 0)
        FROM usage_log
        WHERE service_id = ? AND operation_class IN ('A', 'B')
          AND timestamp >= ? AND timestamp < ?
          AND function_name != 'fastly.reconciliation'
        GROUP BY operation_class, 2
        """,
        (service_id, window_start, window_end),
    ):
        local_sums[(r[0], r[1])] = int(r[2] or 0)

    # Wipe prior reconciliation rows in the window in a single range delete
    # spanning both classes, then insert one row per (hour, class) gap > 0.
    con.execute(
        """
        DELETE FROM usage_log
        WHERE service_id = ? AND operation_class IN ('A', 'B')
          AND timestamp >= ? AND timestamp < ?
          AND function_name = 'fastly.reconciliation'
        """,
        (service_id, window_start, window_end),
    )

    written = 0
    insert_rows: list[tuple] = []
    for hour_start, classes in by_hour.items():
        hour_prefix = hour_start[:13]  # "YYYY-MM-DDTHH"
        for op_class, fastly_count in classes.items():
            local_sum = local_sums.get((op_class, hour_prefix), 0)
            gap = fastly_count - local_sum
            if gap > 0:
                insert_rows.append(
                    (
                        hour_start,
                        service_id,
                        op_class,
                        f"RECONCILE_{op_class}",
                        f"fastly://stats/aggregate/{hour_start}",
                        "OK",
                        0.0,
                        "fastly.reconciliation",
                        "fastly:reconciliation",
                        None,
                        gap,
                    )
                )
                written += 1

    if insert_rows:
        con.executemany(
            """
            INSERT INTO usage_log
            (timestamp, service_id, operation_class, operation_type, url, status,
             duration_ms, function_name, process_context, bytes, count)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_rows,
        )
    con.commit()
    return written


def purge_usage_log(service_id: str, retention_days: int) -> None:
    if retention_days <= 0:
        return
    con = get_con(service_id)
    cutoff = iso_z(datetime.now(UTC) - timedelta(days=retention_days))
    con.execute("DELETE FROM usage_log WHERE timestamp < ?", (cutoff,))
    con.commit()


def clear_usage_log(service_id: str) -> None:
    con = get_con(service_id)
    con.execute("DELETE FROM usage_log WHERE service_id = ?", (service_id,))
    con.commit()


USAGE_LOG_HOURLY_BACKFILL_NAME = "2026-06-04_usage_log_hourly_summary_backfill"

# Per-process guard so the in-process check doesn't hit SQLite on every read.
# The DB-level marker (applied_data_migrations) is the source of truth across
# restarts; this cache just trims redundant lookups within one process.
_usage_log_backfilled: set[str] = set()
_usage_log_backfill_lock = threading.Lock()


def _ensure_usage_log_hourly_backfilled(con: sqlite3.Connection, service_id: str) -> None:
    """Populate usage_log_hourly_summary for services upgrading from a
    pre-trigger install. Idempotent; runs at most once per service.

    Detection: presence of the named row in ``applied_data_migrations``. The
    trigger handles all NEW inserts; this backfill catches the rows that
    existed before the trigger was added. Synchronous so /admin/usage-log
    returns correct data on first access (typically <1 s for ~1 M rows).
    """
    if service_id in _usage_log_backfilled:
        return
    with _usage_log_backfill_lock:
        if service_id in _usage_log_backfilled:
            return
        try:
            applied = con.execute(
                "SELECT 1 FROM applied_data_migrations WHERE name = ?",
                (USAGE_LOG_HOURLY_BACKFILL_NAME,),
            ).fetchone()
            if applied is None:
                t0 = time.time()
                logger.info("[usage_log] backfilling hourly summary for %s", service_id)
                # Wipe any partial summary rows the trigger may have written
                # for this service since boot — we're rebuilding from raw so
                # the GROUP BY sum is exact, not double-counted on top of
                # trigger-written rows.
                con.execute("DELETE FROM usage_log_hourly_summary WHERE service_id = ?", (service_id,))
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
                    WHERE service_id = ?
                      AND timestamp IS NOT NULL
                      AND length(timestamp) >= 13
                    GROUP BY 1, 2, 3, 4
                    """,
                    (service_id,),
                )
                con.execute(
                    "INSERT OR REPLACE INTO applied_data_migrations "
                    "(name, applied_at, duration_s, status, notes) VALUES (?, ?, ?, ?, ?)",
                    (USAGE_LOG_HOURLY_BACKFILL_NAME, iso_z_now(), time.time() - t0, "success",
                     "rebuilt usage_log_hourly_summary from raw"),
                )
                con.commit()
                logger.info("[usage_log] hourly backfill complete for %s in %.2fs", service_id, time.time() - t0)
        except Exception as e:
            logger.warning("[usage_log] hourly summary backfill failed for %s: %s", service_id, e)
        _usage_log_backfilled.add(service_id)


def _query_usage_log_aggregate_rollup(
    con: sqlite3.Connection,
    service_id: str,
    start: str,
    end: str,
    usage_type: str,
) -> list[sqlite3.Row]:
    """Compute the (operation_class, operation_type) totals exactly using the
    hourly rollup for fully-contained hours plus raw usage_log for the two
    boundary hours (which usually aren't hour-aligned).

    The rollup PK lookup is sub-millisecond; the boundary raw scans cover at
    most 2 hours of data (~80 k rows in a busy service) and ride the
    idx_usage_service_ts index. Combined cost is typically ~1-2 ms vs the
    600 ms full-window GROUP BY this replaces.
    """
    # Hour bucket prefix is "YYYY-MM-DDTHH" (13 chars). Timestamps in
    # usage_log are stored as ISO strings, so prefix comparison is correct.
    start_hour = (start or "")[:13]
    end_hour = (end or "")[:13]

    class_filter = ""
    class_params: list = []
    if usage_type:
        if usage_type == "CDN":
            class_filter = "AND operation_class = 'CDN'"
        elif usage_type == "FOS-A":
            class_filter = "AND operation_class = 'A'"
        elif usage_type == "FOS-B":
            class_filter = "AND operation_class = 'B'"
        elif usage_type == "FOS":
            class_filter = "AND operation_class IN ('A', 'B')"
        else:
            class_filter = "AND operation_class = ?"
            class_params = [usage_type]

    # Sub-hour range collapses to a single raw scan — no hour bucket fully
    # contained, both boundary parts would target the same hour anyway.
    if start_hour == end_hour:
        rows = con.execute(
            f"""
            SELECT operation_class, operation_type,
                   SUM(count) AS c, SUM(COALESCE(bytes, 0)) AS b
            FROM usage_log
            WHERE service_id = ? AND timestamp >= ? AND timestamp <= ? {class_filter}
            GROUP BY operation_class, operation_type
            """,
            [service_id, start, end] + class_params,
        ).fetchall()
        return rows

    # Boundary range comparisons keyed on timestamp directly (not
    # `substr(timestamp, 1, 13)`) so SQLite can ride idx_usage_service_ts
    # as a pure range scan — substr() forces per-row evaluation, ~5x slower
    # on the end-of-day boundary (18k rows: 90ms with substr vs ~15ms with
    # pure range). The hour boundary is the start of the FOLLOWING hour, so
    # we strip any " " or "T" between date/time and use the ISO Z form to
    # match what writers store.
    def _next_hour_start(hour_prefix: str) -> str:
        # "2026-06-04T23" → "2026-06-05T00:00:00.000Z"
        try:
            dt = datetime.strptime(hour_prefix, "%Y-%m-%dT%H").replace(tzinfo=UTC)
        except ValueError:
            return hour_prefix + ":59:59.999Z"
        nxt = dt + timedelta(hours=1)
        return nxt.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def _hour_start(hour_prefix: str) -> str:
        return hour_prefix + ":00:00.000Z"

    start_hour_end = _next_hour_start(start_hour)
    end_hour_start = _hour_start(end_hour)

    # Three-part UNION ALL: interior hours from rollup, boundary hours from
    # raw usage_log. SUM(SUM(...)) collapses the two sources into a single
    # (op_class, op_type) tuple per group.
    rollup_class_filter = class_filter  # same syntax works against the rollup
    rows = con.execute(
        f"""
        SELECT operation_class, operation_type,
               SUM(c) AS c, SUM(b) AS b
        FROM (
            SELECT operation_class, operation_type, count AS c, bytes AS b
            FROM usage_log_hourly_summary
            WHERE service_id = ? AND hour > ? AND hour < ? {rollup_class_filter}
            UNION ALL
            SELECT operation_class, operation_type, count AS c, COALESCE(bytes, 0) AS b
            FROM usage_log
            WHERE service_id = ? AND timestamp >= ? AND timestamp < ? {class_filter}
            UNION ALL
            SELECT operation_class, operation_type, count AS c, COALESCE(bytes, 0) AS b
            FROM usage_log
            WHERE service_id = ? AND timestamp >= ? AND timestamp <= ? {class_filter}
        )
        GROUP BY operation_class, operation_type
        """,
        # Interior rollup params
        [service_id, start_hour, end_hour] + class_params
        # Start-boundary raw params: [start, next_hour_after_start_hour)
        + [service_id, start, start_hour_end] + class_params
        # End-boundary raw params: [start_of_end_hour, end]
        + [service_id, end_hour_start, end] + class_params,
    ).fetchall()
    return rows


def get_usage_logs(
    service_id: str,
    start: str,
    end: str,
    *,
    usage_type: str = "",
    process_context: str = "",
    operation_type: str = "",
    page: int = 1,
    page_size: int = 100,
) -> tuple[list[dict], int, dict]:
    """Paginated usage log query with aggregates. Used by the Usage Log page."""
    con = get_con(service_id)
    conditions = ["service_id = ?", "timestamp >= ?", "timestamp <= ?"]
    params: list = [service_id, start, end]

    if usage_type:
        if usage_type == "CDN":
            conditions.append("operation_class = 'CDN'")
        elif usage_type == "FOS-A":
            conditions.append("operation_class = 'A'")
        elif usage_type == "FOS-B":
            conditions.append("operation_class = 'B'")
        elif usage_type == "FOS":
            conditions.append("operation_class IN ('A', 'B')")
        else:
            conditions.append("operation_class = ?")
            params.append(usage_type)

    if process_context:
        conditions.append("process_context LIKE ?")
        params.append(f"%{process_context}%")
    if operation_type:
        conditions.append("operation_type LIKE ?")
        params.append(f"%{operation_type}%")

    where = " AND ".join(conditions)
    total = con.execute(f"SELECT count(*) FROM usage_log WHERE {where}", params).fetchone()[0]

    offset = (page - 1) * page_size
    cur = con.execute(
        f"SELECT * FROM usage_log WHERE {where} ORDER BY timestamp DESC LIMIT ? OFFSET ?",
        params + [page_size, offset],
    )
    entries = [dict(r) for r in cur.fetchall()]

    # Aggregate path: prefer the usage_log_hourly_summary rollup when only the
    # service+timestamp predicates are active (the common admin-page case). The
    # rollup is maintained incrementally by trg_usage_log_summary_insert, so
    # it's always consistent — no scheduler needed. We can only use it when no
    # process_context / operation_type LIKE filters are present (the rollup
    # doesn't carry those columns); the operation_class filter IS supported
    # because the rollup stores it as a normalised key. Backfill of any
    # service that predates the trigger happens lazily on first read.
    rollup_eligible = not process_context and not operation_type
    if rollup_eligible:
        _ensure_usage_log_hourly_backfilled(con, service_id)
        grouped = _query_usage_log_aggregate_rollup(con, service_id, start, end, usage_type)
    else:
        # One GROUP BY (operation_class, operation_type) does the work of both the
        # 5-CASE-WHEN totals query AND the per-class breakdown — they're the same
        # 800K-row scan over usage_log, just shaped differently. Doing both in
        # one query saves a full pass per Usage Log page load (~1s on prod).
        grouped = con.execute(
            f"""
            SELECT operation_class, operation_type,
                   sum(count) AS c, sum(coalesce(bytes, 0)) AS b
            FROM usage_log
            WHERE {where}
            GROUP BY 1, 2
            """,
            params,
        ).fetchall()

    totals = {"A": 0, "B": 0, "CDN": 0}
    bytes_by_class = {"A": 0, "B": 0, "CDN": 0}
    class_a_breakdown: dict[str, int] = {}
    class_b_breakdown: dict[str, int] = {}
    for r in grouped:
        cls, otype, c, b = r["operation_class"], r["operation_type"], int(r["c"] or 0), int(r["b"] or 0)
        if cls in totals:
            totals[cls] += c
            bytes_by_class[cls] += b
        if cls == "A":
            class_a_breakdown[otype] = c
        elif cls == "B":
            class_b_breakdown[otype] = c

    res_agg = {
        "total_class_a": totals["A"],
        "total_class_b": totals["B"],
        "total_cdn_downloads": totals["CDN"],
        "total_cdn_bytes": bytes_by_class["CDN"],
        "total_fos_bytes": bytes_by_class["A"] + bytes_by_class["B"],
        "class_a_breakdown": class_a_breakdown,
        "class_b_breakdown": class_b_breakdown,
    }

    return entries, total, res_agg


# ── Metadata retention / cleanup ──────────────────────────────────────────────
# usage_log and ingested_files are append-only and unbounded by default.
# On a long-running deploy they grow without limit (witnessed: 5.7 GB
# metadata.db with 8.25M usage_log rows + 2.35M ingested_files rows). The
# UI doesn't need that history beyond a short window — Usage & Cost pages
# query a configurable window; Data Management shows recent files; cron_runs
# is a short audit trail. Trim by age; keep VACUUM gated to actual deletions
# because a no-op VACUUM still rewrites the whole file.

# Per-table retention windows (days). Override via cfg["metadata_retention"]
# per service. 0 (or negative) disables cleanup for that table / artefact.
#
# rollups_days is not a SQLite table but a per-hour parquet tree under
# ``<cache>/rollups/hour/field=X/hour=Y/``. The cleanup helper deletes
# hour-dirs older than this window. Default 90d gives broad dashboard
# query coverage while bounding disk; set to 0 to keep all history.
DEFAULT_METADATA_RETENTION = {
    "usage_log_days": 1,
    "ingested_files_days": 1,
    "cron_runs_days": 7,
    "rollups_days": 90,
}

# Tables surfaced in the storage stats endpoint. Order matters for the UI.
_STATS_TABLES = (
    "usage_log",
    "ingested_files",
    "cron_runs",
    "alerts",
    "saved_views",
    "audit_log",
    "in_flight_buffers",
    "locally_compacted_files",
)

# (table, retention_key, timestamp_column) for each trimmable table.
_CLEANUP_TABLES = (
    ("usage_log", "usage_log_days", "timestamp"),
    ("ingested_files", "ingested_files_days", "ingested_at"),
    ("cron_runs", "cron_runs_days", "started_at"),
)


def get_metadata_storage_stats(service_id: str) -> dict:
    """Per-table row count + estimated bytes for this service's metadata.db.

    Bytes come from SQLite's ``dbstat`` virtual table (compiled into stock
    Python sqlite3 ≥3.31). If a table doesn't exist (older schema), it's
    omitted rather than erroring. Total ``db_bytes`` is the sum across the
    whole file — including indexes, free pages, and tables not in
    ``_STATS_TABLES``, so it won't equal sum-of-per-table-bytes.
    """
    con = get_con(service_id)
    out: dict[str, dict] = {}
    for t in _STATS_TABLES:
        try:
            rows = con.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
        except sqlite3.OperationalError:
            continue
        try:
            row = con.execute("SELECT sum(pgsize) FROM dbstat WHERE name = ?", (t,)).fetchone()
            bytes_ = int(row[0]) if row and row[0] is not None else 0
        except sqlite3.OperationalError:
            bytes_ = None
        out[t] = {"rows": int(rows or 0), "bytes": bytes_}

    db_bytes: int | None
    try:
        row = con.execute("SELECT sum(pgsize) FROM dbstat").fetchone()
        db_bytes = int(row[0]) if row and row[0] is not None else 0
    except sqlite3.OperationalError:
        db_bytes = None

    return {
        "tables": out,
        "db_bytes": db_bytes,
        "db_path": db_path(service_id),
    }


def is_ingested_files_dedup_active(service_id: str) -> bool:
    """Return True when the ``ingested_files`` table is the active dedup gate.

    The sync's ``delete_after`` flag (default True) makes ingest a destructive
    op: a successfully-ingested .gz is DELETEd from FOS, so a future LIST
    can never re-discover it — the ``ingested_files`` row is vestigial
    after that point. When ``delete_after`` is set to False, the raw files
    stay in FOS forever and the daily ``full_sync`` (cron) does a complete
    LIST; the only thing stopping it from re-ingesting every prior file is
    a matching entry in ``ingested_files``. In that mode the table CANNOT
    be trimmed without causing re-ingestion storms.
    """
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id) or {}
    delete_after = cfg.get("provisioning", {}).get("cron_sync", {}).get("delete_after", True)
    # Treat anything other than an explicit False as safe-to-trim. None,
    # missing, truthy strings — all default to the safe path.
    return delete_after is not False


def cleanup_metadata(
    service_id: str,
    retention: dict | None = None,
    on_event=None,
) -> dict:
    """Delete rows older than the per-table retention window. VACUUM if any were deleted.

    retention shape: ``{"usage_log_days": int, "ingested_files_days": int,
    "cron_runs_days": int}``. Missing keys fall back to
    ``DEFAULT_METADATA_RETENTION``. A value of 0 (or negative) disables
    cleanup for that table — useful for an analyst-only service that wants
    to retain the full audit trail.

    ``ingested_files_days`` is **force-overridden to 0** when
    ``cron_sync.delete_after`` is False on this service — see
    ``is_ingested_files_dedup_active``. The override is announced via an
    ``on_event`` status message so the operator knows the configured
    retention is being ignored.

    ``on_event``: optional callable receiving event dicts at each milestone
    (status messages, per-table delete results, VACUUM start/end). The
    callback is invoked synchronously from the worker — the manual-cleanup
    endpoint uses a thread-safe queue to bridge to SSE. Event shapes:

        {"type": "status", "message": str}
        {"type": "progress", "current": int, "total": int, "message": str}

    The scheduled cron passes ``on_event=None`` and gets silent operation
    (events still arrive in the function's return dict for logging).

    Returns ``{"deleted": {table: count}, "before": {table: rows},
    "after": {table: rows}, "vacuumed": bool, "duration_s": float}``.
    """
    import time as _t

    def _emit(event: dict) -> None:
        if on_event is None:
            return
        try:
            on_event(event)
        except Exception:
            # Never let an event-sink failure abort the cleanup itself.
            pass

    cfg = {**DEFAULT_METADATA_RETENTION, **(retention or {})}

    # Safety override: when cron_sync.delete_after is False, ingested_files
    # is the dedup gate against re-LIST → re-ingest by the daily full_sync.
    # Trimming it would re-ingest every aged-out file. Force-disable the
    # ingested_files retention regardless of what cfg / caller passed,
    # and surface the override so the operator sees why it didn't apply.
    if not is_ingested_files_dedup_active(service_id):
        configured = int(cfg.get("ingested_files_days") or 0)
        if configured > 0:
            _emit(
                {
                    "type": "status",
                    "message": (
                        f"ingested_files retention ({configured}d) ignored — "
                        "cron_sync.delete_after=false makes this table the dedup gate. "
                        "Trimming would cause full_sync to re-ingest aged-out files."
                    ),
                }
            )
        cfg["ingested_files_days"] = 0

    con = get_con(service_id)
    t0 = _t.time()

    # Steps: 3 deletes + 1 vacuum + 1 post-count = 5. Set up the progress
    # framing so the modal can render a determinate bar.
    total_steps = len(_CLEANUP_TABLES) + 2

    _emit({"type": "status", "message": "Reading current row counts…"})
    before: dict[str, int] = {}
    for table, _, _ in _CLEANUP_TABLES:
        try:
            before[table] = int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] or 0)
        except sqlite3.OperationalError:
            before[table] = 0

    deleted: dict[str, int] = {}
    for idx, (table, key, ts_col) in enumerate(_CLEANUP_TABLES, start=1):
        days = cfg.get(key)
        try:
            days_int = int(days) if days is not None else 0
        except (TypeError, ValueError):
            days_int = 0
        if days_int <= 0:
            deleted[table] = 0
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: retention disabled (0 days) — skipped",
                }
            )
            continue
        _emit({"type": "status", "message": f"Trimming {table} (older than {days_int}d)…"})
        try:
            cur = con.execute(
                f"DELETE FROM {table} WHERE {ts_col} < datetime('now', ?)",
                (f"-{days_int} days",),
            )
            deleted[table] = int(cur.rowcount or 0)
            con.commit()
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: deleted {deleted[table]:,} rows (kept rows ≤{days_int}d old)",
                }
            )
        except sqlite3.OperationalError as e:
            logger.warning("[metadata_cleanup] %s: skip %s — %s", service_id, table, e)
            deleted[table] = 0
            _emit(
                {
                    "type": "progress",
                    "current": idx,
                    "total": total_steps,
                    "message": f"{table}: skipped ({e})",
                }
            )

    vacuumed = False
    if any(deleted.values()):
        # VACUUM cannot run inside an open transaction. Commit + drop the
        # Python wrapper's auto-BEGIN so the next execute() autocommits.
        _emit(
            {
                "type": "status",
                "message": "VACUUMing — rewrites the whole file, may take minutes on large DBs…",
            }
        )
        con.commit()
        old_iso = con.isolation_level
        con.isolation_level = None
        try:
            con.execute("VACUUM")
            vacuumed = True
            _emit(
                {
                    "type": "progress",
                    "current": len(_CLEANUP_TABLES) + 1,
                    "total": total_steps,
                    "message": "VACUUM complete — file shrunk to reflect deletions",
                }
            )
        except sqlite3.OperationalError as e:
            # Locked / busy — not fatal, the delete already shrank the row count.
            logger.warning("[metadata_cleanup] %s: VACUUM skipped — %s", service_id, e)
            _emit(
                {
                    "type": "progress",
                    "current": len(_CLEANUP_TABLES) + 1,
                    "total": total_steps,
                    "message": f"VACUUM skipped ({e}) — row counts already reduced",
                }
            )
        finally:
            con.isolation_level = old_iso
    else:
        _emit(
            {
                "type": "progress",
                "current": len(_CLEANUP_TABLES) + 1,
                "total": total_steps,
                "message": "Nothing deleted — VACUUM skipped (no-op rewrite would waste cycles)",
            }
        )

    after: dict[str, int] = {}
    for table, _, _ in _CLEANUP_TABLES:
        try:
            after[table] = int(con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] or 0)
        except sqlite3.OperationalError:
            after[table] = 0
    _emit(
        {
            "type": "progress",
            "current": total_steps,
            "total": total_steps,
            "message": f"Final counts: {', '.join(f'{t}={n:,}' for t, n in after.items())}",
        }
    )

    # Rollup parquet tree cleanup — independent of the SQLite tables. Skip
    # silently when the rollups module / source aren't available; rollups
    # are an optimisation, never a correctness dependency.
    rollups_deleted = 0
    try:
        rollups_days = int(cfg.get("rollups_days") or 0)
    except (TypeError, ValueError):
        rollups_days = 0
    if rollups_days > 0:
        try:
            from backend.core import rollups as _rollups
            from backend.core.duckdb import get_source_for_service

            src = get_source_for_service(service_id)
            if src is not None:
                rollups_deleted = _rollups.cleanup_old_rollups(service_id, src, rollups_days)
                if rollups_deleted:
                    _emit(
                        {
                            "type": "status",
                            "message": f"Rollups: dropped {rollups_deleted} hour-dir(s) older than {rollups_days}d",
                        }
                    )
        except Exception as e:
            logger.warning("[metadata_cleanup] %s: rollups cleanup skipped — %s", service_id, e)

    return {
        "deleted": deleted,
        "before": before,
        "after": after,
        "vacuumed": vacuumed,
        "rollups_deleted": rollups_deleted,
        "duration_s": round(_t.time() - t0, 3),
    }


# ── Data-migration tracking ───────────────────────────────────────────────────
# See backend/core/data_migrations.py for the runner. These helpers exist here
# (not in the runner module) so the runner can stay free of sqlite imports —
# the per-service connection lifecycle lives entirely in this module.


def list_applied_data_migrations(service_id: str) -> set[str]:
    """Return the set of applied data-migration names for a service.

    Used by the runner to diff against the registered MIGRATIONS list and
    determine which still need to run. Returns an empty set for a fresh DB.
    """
    con = get_con(service_id)
    try:
        rows = con.execute("SELECT name FROM applied_data_migrations").fetchall()
        return {r["name"] for r in rows}
    except sqlite3.OperationalError:
        # Schema not yet initialised — caller will hit this on its first
        # successful query path; treat as "nothing applied yet".
        return set()


def record_applied_data_migration(
    service_id: str,
    name: str,
    *,
    duration_s: float,
    status: str = "success",
    notes: str | None = None,
) -> None:
    """Persist a successful (or failed) migration completion."""
    con = get_con(service_id)
    con.execute(
        "INSERT OR REPLACE INTO applied_data_migrations (name, applied_at, duration_s, status, notes) "
        "VALUES (?, ?, ?, ?, ?)",
        (name, iso_z_now(), float(duration_s), status, notes),
    )
    con.commit()
