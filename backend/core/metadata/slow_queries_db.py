"""Buffered slow-query persistence and history retrieval against metadata Postgres."""

from __future__ import annotations

import atexit
import logging
import sys
import threading
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict

from backend.core.metadata.pg_pool import get_con, release_thread_connection

logger = logging.getLogger(__name__)


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
        ?, ?, ?, ?, ?,
        ?, ?, ?, ?, ?,
        ?, ?, ?,
        ?, ?,
        ?, ?,
        ?, ?, ?,
        ?, ?, ?
    )
"""

_FLUSH_INTERVAL_S = 5.0
_FLUSH_BATCH_SIZE = 50

# Keyed by service_id → list of param dicts
_buffer: dict[str, list[dict[str, Any]]] = {}
_buffer_lock = threading.Lock()
_flush_timer: threading.Timer | None = None
_flush_in_progress = False


_OrUnknown = Annotated[Any, BeforeValidator(lambda v: v or "unknown")]
_OrSystem = Annotated[Any, BeforeValidator(lambda v: v or "system")]
_OrZeroFloat = Annotated[Any, BeforeValidator(lambda v: v or 0.0)]
_OrZeroInt = Annotated[Any, BeforeValidator(lambda v: v or 0)]
_OrEmptyStr = Annotated[Any, BeforeValidator(lambda v: v or "")]
_OptStr = Annotated[str | None, BeforeValidator(lambda v: str(v) if v is not None else None)]


class _SlowQueryRowModel(BaseModel):
    model_config = ConfigDict(extra="ignore")

    query_id: Any
    db_type: _OrUnknown = "unknown"
    service_id: Any = None
    started_at_utc: _OrZeroFloat = 0.0
    ended_at_utc: _OrZeroFloat = 0.0
    duration_ms: _OrZeroFloat = 0.0
    outcome: _OrUnknown = "unknown"
    sql_preview: _OrEmptyStr = ""
    sql_full: Any = None
    sql_len: _OrZeroInt = 0
    attr_kind: _OrSystem = "system"
    attr_label: _OrEmptyStr = ""
    attr_principal_id: Any = None
    attr_caller_qualname: _OrEmptyStr = ""
    attr_caller_file: _OrEmptyStr = ""
    attr_request_path: Any = None
    attr_request_id: Any = None
    attr_cron_job: Any = None
    attr_cron_run_id: _OptStr = None
    attr_pool_slot: Any = None
    error_type: Any = None
    error_message: Any = None
    peak_memory_mb: Any = None


def _normalise_row(row: dict[str, Any]) -> dict[str, Any]:
    return _SlowQueryRowModel.model_validate(row).model_dump()


def _insert_params(row: dict[str, Any]) -> tuple[Any, ...]:
    return tuple(_SlowQueryRowModel.model_validate(row).model_dump().values())


def _schedule_flush() -> None:
    global _flush_timer
    if _flush_timer is not None:
        return
    _flush_timer = threading.Timer(_FLUSH_INTERVAL_S, _flush_all)
    setattr(sys.modules[__name__], "_flush_timer", _flush_timer)
    _flush_timer.daemon = True
    _flush_timer.start()


def _flush_all(*, only_service: str | None = None) -> None:
    """Flush buffered slow-query rows to PostgreSQL (METADATA_DSN).

    If *only_service* is given, flush just that service's buffer (used by
    read functions to ensure consistency). Otherwise flush everything.
    """
    global _flush_timer, _flush_in_progress
    with _buffer_lock:
        if only_service is not None:
            rows = _buffer.pop(only_service, None)
            if rows is None:
                return
            pending = {only_service: rows}
        else:
            _flush_timer = None
            setattr(sys.modules[__name__], "_flush_timer", None)
            if not _buffer:
                _flush_in_progress = False
                setattr(sys.modules[__name__], "_flush_in_progress", False)
                return
            pending = dict(_buffer)
            _buffer.clear()

    try:
        for service_id, rows in pending.items():
            try:
                con = get_con(service_id)
                con.executemany(_INSERT_SQL, [_insert_params(_normalise_row(row)) for row in rows])
                con.commit()
            except Exception:
                logger.debug("_flush_all: con.commit failed (ignored)", exc_info=True)
            finally:
                release_thread_connection()
    finally:
        if only_service is None:
            with _buffer_lock:
                _flush_in_progress = False
                setattr(sys.modules[__name__], "_flush_in_progress", False)
                if _buffer:
                    _schedule_flush()


def insert_slow_query(service_id: str, row: dict[str, Any]) -> None:
    """Buffer a completed-query row for batch insertion."""
    global _flush_in_progress
    params = _normalise_row(row)
    with _buffer_lock:
        _buffer.setdefault(service_id, []).append(params)
        total = sum(len(v) for v in _buffer.values())
        if total >= _FLUSH_BATCH_SIZE and not _flush_in_progress:
            _flush_in_progress = True
            setattr(sys.modules[__name__], "_flush_in_progress", True)
            needs_immediate = True
        else:
            needs_immediate = False
            if not _flush_in_progress:
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
    """Return slow-query rows for a service in a time window."""
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
    """Cheap count of persisted slow queries in a window."""
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    row = con.execute(
        "SELECT COUNT(*) AS n FROM slow_queries WHERE service_id = ? AND started_at_utc >= ? AND duration_ms >= ?",
        (service_id, since_utc, threshold_ms),
    ).fetchone()
    return int(row["n"] or 0)


def clear_slow_queries(service_id: str) -> int:
    """Delete all persisted slow-query rows for *service_id*."""
    global _flush_timer
    _flush_all(only_service=service_id)
    con = get_con(service_id)
    cur = con.execute(
        "DELETE FROM slow_queries WHERE service_id = ?",
        (service_id,),
    )
    con.commit()
    with _buffer_lock:
        _buffer.pop(service_id, None)
        if not _buffer and _flush_timer is not None:
            _flush_timer.cancel()
            _flush_timer = None
            setattr(sys.modules[__name__], "_flush_timer", None)
    return int(cur.rowcount or 0)
