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
    "CREATE INDEX IF NOT EXISTS idx_ingested_files_source ON ingested_files(source_name)",
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
    for stmt in _SCHEMA:
        con.execute(stmt)
    con.commit()
    # Bring pre-migration-framework service DBs up to current. Migrations
    # are idempotent (each checks before mutating) so this is also safe to
    # call on fresh DBs that already have everything from ``_SCHEMA``.
    # On a healthy fresh install the loop exits on the first version check.
    from backend.core import sqlite_migrations

    applied = sqlite_migrations.apply_pending(con)
    if applied:
        logger.info("[metadata_db] applied %d pending migration(s)", applied)
    # New DBs leap straight to LATEST so the migration loop doesn't waste
    # a check on every open. Idempotency means doing the work first is
    # harmless, but skipping the inspection is cheaper at scale.
    if sqlite_migrations.get_current_version(con) < sqlite_migrations.LATEST_VERSION:
        con.execute(f"PRAGMA user_version = {sqlite_migrations.LATEST_VERSION}")
        con.commit()


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
