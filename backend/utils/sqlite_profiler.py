"""Always-on SQLite query profiler — captures every SQL statement issued
through ``metadata_db.get_con()`` into a process-global ring buffer so the
Debug Panel can profile metadata reads/writes alongside DuckDB queries.

Why a ring buffer (not per-request capture): the high-value SQLite queries
to surface are the ones inside cron jobs (sync, alerts, full_sweep,
gap_heal). Those run outside any HTTP request, so a per-request middleware
buffer would never see them. A process-global deque captures everything and
the Debug Panel polls a /api/debug/recent-sqlite endpoint to render it.

Why subclass Connection/Cursor (not ``set_trace_callback``): trace callback
gets the SQL text only — no timing, no rowcount. Subclassing intercepts
``execute``/``executemany`` and times the call itself, returning the
unmodified cursor result. Negligible overhead: ~5us per statement
(perf_counter + dict append).

Thread-safety: the deque is implicitly thread-safe for append/iterate. The
sequence counter uses an atomic ``itertools.count``. No global lock.
"""

from __future__ import annotations

import itertools
import logging
import sqlite3
import sys
import time
from collections import deque
from datetime import UTC, datetime
from typing import Any

import structlog

logger = logging.getLogger(__name__)

# Ring buffer cap. ~500B per entry × 1000 = ~500KB worst case — bounded.
_BUFFER_CAP = 1000

# SQL text truncation. Few SQLite statements in this codebase exceed 2KB; cap
# defends against pathological IN(...) expansions.
_SQL_TRUNCATE = 4096

# Process-global capture state.
_buffer: deque[dict[str, Any]] = deque(maxlen=_BUFFER_CAP)
_seq = itertools.count(1)
_dropped = 0  # count of entries pushed out of the ring


def _record(sql: str, params: Any, duration_ms: float, rowcount: int, op: str) -> None:
    """Append one profiled statement to the ring buffer. Best-effort: any
    failure here MUST NOT propagate into the calling SQL path. The profiler
    is observability, not control flow.
    """
    global _dropped
    try:
        if _buffer.maxlen and len(_buffer) == _buffer.maxlen:
            _dropped += 1
        _buffer.append(
            {
                "seq": next(_seq),
                "ts": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "sql": _summarize_sql(sql),
                "params_kind": _describe_params(params),
                "time_ms": round(duration_ms, 3),
                "rows": rowcount,
                "op": op,
            }
        )
    except Exception:
        # Profiler errors must never surface to the caller. Log at debug so a
        # broken record doesn't flood the logs either.
        logger.debug("sqlite_profiler record failed", exc_info=True)


def _summarize_sql(sql: str) -> str:
    if not isinstance(sql, str):
        sql = str(sql)
    if len(sql) > _SQL_TRUNCATE:
        return sql[:_SQL_TRUNCATE] + f"… [+{len(sql) - _SQL_TRUNCATE} chars]"
    return sql


def _describe_params(params: Any) -> str:
    """Short shape descriptor — we do NOT capture parameter values (PII risk
    in usage_log / url columns). Just the shape, for debugging IN(...) blow-ups.
    """
    if params is None:
        return "none"
    if isinstance(params, (list, tuple)):
        return f"seq[{len(params)}]"
    if isinstance(params, dict):
        return f"map[{len(params)}]"
    return type(params).__name__


def _live_register(db_type: str, sql: Any, con: Any) -> int:
    """Register the executing statement with the Live Query Monitor's
    registry and bind ``query_id`` into the structlog context. Mirrors the
    profiler's contract: any failure here is swallowed at DEBUG and the SQL
    path continues unaffected.

    Reads ``con._service_id`` (stashed by
    :func:`backend.core.metadata.base.get_con`) so the live monitor can
    tag SQLite rows with the service whose metadata.db they're hitting.
    Connections opened by code that bypasses ``get_con`` (test fixtures,
    introspection scripts) have no such attribute and surface as
    ``service: null`` rather than crashing."""
    try:
        from backend.core.query_registry import query_registry

        service_id = getattr(con, "_service_id", None)
        qid = query_registry.register(db_type, str(sql), service_id=service_id, con=con)
        if qid >= 0:
            structlog.contextvars.bind_contextvars(query_id=qid)
        return qid
    except Exception:
        logger.debug("live-registry register failed", exc_info=True)
        return -1


def _live_deregister(qid: int, error: BaseException | None) -> None:
    if qid < 0:
        return
    try:
        from backend.core.query_registry import query_registry

        query_registry.deregister(qid, error=error)
    except Exception:
        logger.debug("live-registry deregister failed", exc_info=True)
    finally:
        try:
            structlog.contextvars.unbind_contextvars("query_id")
        except Exception:
            pass


class InstrumentedCursor(sqlite3.Cursor):
    """Cursor subclass that times every execute/executemany/executescript.

    Inline timing (rather than a helper) so we can read ``self.rowcount``
    directly after the call. The cursor's rowcount is only meaningful for
    DML (INSERT/UPDATE/DELETE) — SELECT returns -1 until rows are fetched,
    which we accept rather than triggering an implicit fetchall().
    """

    def execute(self, sql: str, parameters: Any = (), /) -> sqlite3.Cursor:  # type: ignore[override]
        t0 = time.perf_counter()
        qid = _live_register("SQLite", sql, self.connection)
        err: BaseException | None = None
        try:
            return super().execute(sql, parameters)
        except BaseException as e:
            err = e
            raise
        finally:
            _live_deregister(qid, err)
            _record(sql, parameters, (time.perf_counter() - t0) * 1000.0, self.rowcount, "execute")

    def executemany(self, sql: str, seq_of_parameters: Any, /) -> sqlite3.Cursor:  # type: ignore[override]
        t0 = time.perf_counter()
        qid = _live_register("SQLite", sql, self.connection)
        err: BaseException | None = None
        try:
            return super().executemany(sql, seq_of_parameters)
        except BaseException as e:
            err = e
            raise
        finally:
            _live_deregister(qid, err)
            _record(
                sql,
                seq_of_parameters,
                (time.perf_counter() - t0) * 1000.0,
                self.rowcount,
                "executemany",
            )

    def executescript(self, sql_script: str, /) -> sqlite3.Cursor:  # type: ignore[override]
        t0 = time.perf_counter()
        qid = _live_register("SQLite", sql_script, self.connection)
        err: BaseException | None = None
        try:
            return super().executescript(sql_script)
        except BaseException as e:
            err = e
            raise
        finally:
            _live_deregister(qid, err)
            _record(sql_script, None, (time.perf_counter() - t0) * 1000.0, self.rowcount, "executescript")


class InstrumentedConnection(sqlite3.Connection):
    """Connection subclass that hands out InstrumentedCursor and also
    intercepts Connection.execute / executemany (shorthand calls that bypass
    the explicit cursor() path)."""

    def cursor(self, factory=InstrumentedCursor):  # type: ignore[override]
        # Always return InstrumentedCursor unless caller asks for another
        # explicit factory.
        return super().cursor(factory)

    def execute(self, sql, parameters=(), /):  # type: ignore[override]
        # Connection.execute() implicitly creates a cursor. We can't reuse
        # super().execute() because that uses the default Cursor, not our
        # subclass. Materialise a cursor explicitly so we get instrumentation.
        cur = self.cursor()
        return cur.execute(sql, parameters)

    def executemany(self, sql, parameters, /):  # type: ignore[override]
        cur = self.cursor()
        return cur.executemany(sql, parameters)

    def executescript(self, sql_script, /):  # type: ignore[override]
        cur = self.cursor()
        return cur.executescript(sql_script)


# ── Public read API (used by the /api/debug/recent-sqlite endpoint) ────────────


def get_recent(limit: int = 200, since_seq: int = 0) -> dict[str, Any]:
    """Return up to ``limit`` most-recent profiled statements.

    ``since_seq``: caller's last-seen seq value; we return only entries with
    seq > since_seq. Lets the Debug Panel poll incrementally without
    re-rendering identical rows. Pass 0 to fetch from the head of the buffer.
    """
    # Snapshot the deque under a list comprehension — deque iteration is safe
    # under concurrent append.
    snapshot = list(_buffer)
    if since_seq > 0:
        snapshot = [e for e in snapshot if e["seq"] > since_seq]
    if limit and len(snapshot) > limit:
        snapshot = snapshot[-limit:]
    return {
        "queries": snapshot,
        "buffer_size": len(_buffer),
        "buffer_cap": _BUFFER_CAP,
        "dropped": _dropped,
        "last_seq": _buffer[-1]["seq"] if _buffer else 0,
    }


def clear() -> None:
    """Drain the buffer. Intended for tests and the /api/debug clear button.
    Does NOT reset the seq counter — clients can still tell what's new.
    """
    global _dropped
    _buffer.clear()
    _dropped = 0


def buffer_stats() -> dict[str, int]:
    """Lightweight stats for telemetry / status pages."""
    return {
        "buffer_size": len(_buffer),
        "buffer_cap": _BUFFER_CAP,
        "dropped": _dropped,
    }


# Defensive: emit one INFO at first import noting that capture is on so
# operators see this in logs and don't think it's silent overhead.
if "pytest" not in sys.modules:
    logger.info("sqlite_profiler active — capturing up to %d recent statements", _BUFFER_CAP)
