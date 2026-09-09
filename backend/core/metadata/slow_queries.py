"""Persistent slow-query history.

The live ``query_registry`` only holds the most recent 2000 completed
queries (in-memory ring). That window is ~10–30 min on a busy service
and zero across restarts. This module persists any completed query
above a threshold so the Notable Slow Queries panel can answer "what
was slow yesterday?" — see ``_migration_005_slow_queries`` for the
full design notes.

The helpers in here are intentionally narrow: a write path used by
``query_registry.deregister``, two read paths used by the admin
router, and a retention purge used by ``cleanup_metadata``.
"""

from __future__ import annotations

import atexit
import threading
from typing import Any

from backend.core.metadata.base import get_con, release_thread_connection

_INSERT_SQL = """
    INSERT INTO slow_queries (
        query_id, db_type, service_id, started_at_utc, ended_at_utc,
        duration_ms, outcome, sql_preview, sql_full, sql_len,
        attr_kind, attr_label, attr_principal_id,
        attr_caller_qualname, attr_caller_file,
        attr_request_path, attr_request_id,
        attr_cron_job, attr_cron_run_id, attr_pool_slot,
        error_type, error_message, peak_memory_mb
    ) VALUES (
        :query_id, :db_type, :service_id, :started_at_utc, :ended_at_utc,
        :duration_ms, :outcome, :sql_preview, :sql_full, :sql_len,
        :attr_kind, :attr_label, :attr_principal_id,
        :attr_caller_qualname, :attr_caller_file,
        :attr_request_path, :attr_request_id,
        :attr_cron_job, :attr_cron_run_id, :attr_pool_slot,
        :error_type, :error_message, :peak_memory_mb
    )
"""

_FLUSH_INTERVAL_S = 5.0
_FLUSH_BATCH_SIZE = 50

# Keyed by service_id → list of param dicts
_buffer: dict[str, list[dict[str, Any]]] = {}
_buffer_lock = threading.Lock()
_flush_timer: threading.Timer | None = None


def _normalise_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "query_id": row["query_id"],
        "db_type": row["db_type"],
        "service_id": row.get("service_id"),
        "started_at_utc": row["started_at_utc"],
        "ended_at_utc": row["ended_at_utc"],
        "duration_ms": row["duration_ms"],
        "outcome": row["outcome"],
        "sql_preview": row.get("sql_preview") or "",
        "sql_full": row.get("sql_full"),
        "sql_len": row.get("sql_len") or 0,
        "attr_kind": row.get("attr_kind") or "system",
        "attr_label": row.get("attr_label") or "",
        "attr_principal_id": row.get("attr_principal_id"),
        "attr_caller_qualname": row.get("attr_caller_qualname") or "",
        "attr_caller_file": row.get("attr_caller_file") or "",
        "attr_request_path": row.get("attr_request_path"),
        "attr_request_id": row.get("attr_request_id"),
        "attr_cron_job": row.get("attr_cron_job"),
        "attr_cron_run_id": row.get("attr_cron_run_id"),
        "attr_pool_slot": row.get("attr_pool_slot"),
        "error_type": row.get("error_type"),
        "error_message": row.get("error_message"),
        "peak_memory_mb": row.get("peak_memory_mb"),
    }


def _schedule_flush() -> None:
    global _flush_timer
    if _flush_timer is not None:
        return
    _flush_timer = threading.Timer(_FLUSH_INTERVAL_S, _flush_all)
    _flush_timer.daemon = True
    _flush_timer.start()


def _flush_all(*, only_service: str | None = None) -> None:
    """Flush buffered slow-query rows to SQLite.

    If *only_service* is given, flush just that service's buffer (used by
    read functions to ensure consistency). Otherwise flush everything.
    """
    global _flush_timer
    with _buffer_lock:
        if only_service is not None:
            rows = _buffer.pop(only_service, None)
            if rows is None:
                return
            pending = {only_service: rows}
        else:
            _flush_timer = None
            if not _buffer:
                return
            pending = dict(_buffer)
            _buffer.clear()

    for service_id, rows in pending.items():
        try:
            con = get_con(service_id)
            con.executemany(_INSERT_SQL, rows)
            con.commit()
        except Exception:
            pass
        finally:
            release_thread_connection()


def insert_slow_query(service_id: str, row: dict[str, Any]) -> None:
    """Buffer a completed-query row for batch insertion.

    Called inline from ``query_registry.deregister`` on the hot path.
    Rows are buffered in memory and flushed every few seconds or when
    the buffer reaches a size threshold, replacing the prior synchronous
    per-row INSERT+COMMIT that caused ~2,700 contention events/week.

    The batch-threshold flush runs on a background thread rather than
    inline: a disk/fsync stall on the flush would otherwise block
    whatever just finished the query being recorded — and since flushing
    a slow-query batch is itself now a slow query, an inline call risked
    a feedback loop (recording slow queries makes more of them slow).
    """
    params = _normalise_row(row)
    with _buffer_lock:
        _buffer.setdefault(service_id, []).append(params)
        total = sum(len(v) for v in _buffer.values())
        if total >= _FLUSH_BATCH_SIZE:
            needs_immediate = True
        else:
            needs_immediate = False
            _schedule_flush()

    if needs_immediate:
        threading.Thread(target=_flush_all, daemon=True).start()


def flush_slow_query_buffer() -> None:
    """Force-flush the buffer. Called at shutdown."""
    _flush_all()


atexit.register(flush_slow_query_buffer)


def list_slow_queries(
    service_id: str,
    *,
    since_utc: float,
    until_utc: float | None = None,
    threshold_ms: float = 0.0,
    kind: str | None = None,
    db_type: str | None = None,
    limit: int = 200,
    sort_by_duration: bool = False,
) -> list[dict[str, Any]]:
    """Return slow-query rows for a service in a time window.

    Defaults to time-DESC ordering (most recent first) since that's the
    panel's main view. ``sort_by_duration=True`` switches to
    duration-DESC for the "slowest of the period" variant.

    ``kind`` / ``db_type`` are optional filters; both index-friendly
    because they're equality on small cardinality columns.

    ``limit`` is capped at the call site — pass user input through a
    server-side clamp before reaching this function.
    """
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    sql = ["SELECT * FROM slow_queries WHERE service_id = ? AND started_at_utc >= ?"]
    args: list[Any] = [service_id, since_utc]
    if until_utc is not None:
        sql.append("AND started_at_utc < ?")
        args.append(until_utc)
    if threshold_ms > 0:
        sql.append("AND duration_ms >= ?")
        args.append(threshold_ms)
    if kind is not None:
        sql.append("AND attr_kind = ?")
        args.append(kind)
    if db_type is not None:
        sql.append("AND db_type = ?")
        args.append(db_type)
    sql.append("ORDER BY duration_ms DESC" if sort_by_duration else "ORDER BY started_at_utc DESC")
    sql.append("LIMIT ?")
    args.append(limit)
    rows = con.execute(" ".join(sql), args).fetchall()
    return [dict(r) for r in rows]


def count_slow_queries(
    service_id: str,
    *,
    since_utc: float,
    threshold_ms: float = 0.0,
) -> int:
    """Cheap count of persisted slow queries in a window. Used by the
    operations-overview card to render an at-a-glance badge without
    pulling the full row set."""
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    row = con.execute(
        "SELECT COUNT(*) AS n FROM slow_queries WHERE service_id = ? AND started_at_utc >= ? AND duration_ms >= ?",
        (service_id, since_utc, threshold_ms),
    ).fetchone()
    return int(row["n"] or 0)


def purge_old_slow_queries(service_id: str, *, older_than_utc: float) -> int:
    """Delete rows whose ``started_at_utc`` is below the cutoff. Called
    from ``cleanup_metadata`` on the retention cadence. Returns the
    number of rows removed."""
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    cur = con.execute(
        "DELETE FROM slow_queries WHERE service_id = ? AND started_at_utc < ?",
        (service_id, older_than_utc),
    )
    con.commit()
    return cur.rowcount or 0


def slow_queries_storage_stats(service_id: str) -> dict[str, Any]:
    """Cheap row-count + oldest/newest timestamps for the storage
    inspection page. ``None`` timestamps mean the table is empty."""
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    row = con.execute(
        "SELECT COUNT(*) AS n, MIN(started_at_utc) AS oldest, MAX(started_at_utc) AS newest FROM slow_queries WHERE service_id = ?",
        (service_id,),
    ).fetchone()
    return {
        "row_count": int(row["n"] or 0),
        "oldest_utc": row["oldest"],
        "newest_utc": row["newest"],
    }
