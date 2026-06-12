"""In-memory registry of currently-executing SQL queries.

The Live Query Monitor's backend half. Tracks active queries across DuckDB
and SQLite, keeps a bounded ring buffer of recently-completed ones (incl.
errors), and exposes a safe ``cancel_query`` that interrupts the right
connection even when the underlying pool reuses connections aggressively.

Design notes:

- **Hot path is lock-free.** Register/deregister rely on CPython's
  GIL-protected dict ``__setitem__``/``pop``, matching the pattern used by
  :mod:`backend.utils.sqlite_profiler`. The cancel path takes one short
  lock to validate the per-connection stamp before calling ``interrupt()``.

- **Per-connection stamp** (``_conn_to_query``). Pooled connections execute
  many queries over their lifetime. To cancel safely we must verify that
  the query we want to kill is *still* the one bound to the connection. We
  stamp ``id(con) → query_id`` on register and refuse to interrupt if the
  stamp has moved on. This is the regression-test bait described in the
  design doc §13.10.

- **Weak references to connections.** A strong ref would resurrect closed
  connections or stop the pool from freeing them on error
  ([duckdb_pool.py:338]). DuckDB connections support ``weakref.ref()``;
  sqlite3.Connection does too.

- **Completed-history ring buffer.** Most-investigated case post-incident
  is "what did that query do" or "why did it fail". Bounded ``deque`` mirrors
  :mod:`backend.utils.sqlite_profiler` and stores ``outcome`` +
  ``error_type``/``error_message`` (truncated).

- **OTel hooks.** Mirrors the existing ``app.thread_wait_ms`` histogram
  pattern at ``duckdb_pool.py:221``. Lazy meter creation avoids importing
  ``opentelemetry`` at module-load time (tests run without the SDK).

- **Best-effort.** Any exception inside register/deregister/cancel is
  swallowed at the registry boundary — instrumentation is observability,
  not control flow. Same contract as :func:`sqlite_profiler._record`.
"""

from __future__ import annotations

import collections
import itertools
import logging
import os
import threading
import time
import weakref
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from backend.core.query_attribution import (
    Attribution,
    _capture_caller,
    current_attribution,
)

logger = logging.getLogger(__name__)

_SQL_TRUNCATE = 4096
_ERR_TRUNCATE = 512
_HISTORY_CAP = 200
_seq = itertools.count(1)

# Hot-path kill switch. Read once at module load — flipping requires a
# restart, but it's the kind of thing you'd flip during an incident
# anyway. When True, register() / deregister() return immediately so the
# SQL hot path takes ZERO instrumentation cost. Default off (registry on);
# flip to "1" to surgically disable if you suspect the live monitor is
# contributing to slowness.
_REGISTRY_DISABLED = os.environ.get("QUERY_REGISTRY_DISABLED", "").lower() in (
    "1",
    "true",
    "yes",
    "on",
)
if _REGISTRY_DISABLED:
    logger.warning(
        "query_registry hot-path DISABLED via QUERY_REGISTRY_DISABLED env. "
        "Live Query Monitor will show no queries until you unset it and restart."
    )

# Identity → query_id map. Validates that interrupt() targets the right
# query. id(con) is stable for the connection's lifetime and we clear the
# entry on deregister, so a reused pool slot can't be confused with a prior
# query. Short lock on read+write because the cancel path needs a consistent
# multi-step view.
_conn_to_query: dict[int, int] = {}
_conn_to_query_lock = threading.Lock()


@dataclass(slots=True)
class ActiveQuery:
    query_id: int
    db_type: str  # "DuckDB" | "SQLite"
    sql: str  # truncated to _SQL_TRUNCATE
    attribution: Attribution
    service_id: str | None
    started_at_mono: float
    started_at_utc: float
    # weakref so a closed connection auto-clears; None when we can't hold a
    # reference (e.g. the SQLite cursor path passes ``self.connection``).
    _con_ref: Callable[[], Any] | None = None
    _con_id: int | None = None
    cancelled_at: float | None = None


@dataclass(slots=True)
class CompletedQuery:
    """Snapshot pushed into the history ring buffer on deregister."""

    query_id: int
    db_type: str
    sql: str
    attribution: Attribution
    service_id: str | None
    started_at_utc: float
    ended_at_utc: float
    duration_ms: float
    outcome: str  # "ok" | "error" | "cancelled"
    error_type: str | None = None
    error_message: str | None = None


def _truncate(text: str, cap: int) -> str:
    if len(text) <= cap:
        return text
    return text[:cap] + f"… [+{len(text) - cap} chars]"


# ── OTel metrics (lazy; the SDK is optional under tests) ────────────────────

_metric_lock = threading.Lock()
_metric_active_count: Any = None
_metric_duration_ms: Any = None
_metric_cancelled_total: Any = None


def _ensure_metrics() -> None:
    global _metric_active_count, _metric_duration_ms, _metric_cancelled_total
    if _metric_active_count is not None:
        return
    with _metric_lock:
        if _metric_active_count is not None:
            return
        try:
            from opentelemetry import metrics

            meter = metrics.get_meter("backend.query_registry")
            _metric_active_count = meter.create_up_down_counter(
                "app.active_queries.count",
                description="Currently-executing SQL queries by db/kind.",
            )
            _metric_duration_ms = meter.create_histogram(
                "app.query_duration_ms",
                unit="ms",
                description="Wall-clock duration of completed SQL queries.",
            )
            _metric_cancelled_total = meter.create_counter(
                "app.queries_cancelled_total",
                description="Admin-initiated query cancellations.",
            )
        except Exception:
            logger.debug("OTel meter creation failed; metrics disabled", exc_info=True)


def _metric_safe(emit: Callable[[], None]) -> None:
    try:
        _ensure_metrics()
        if _metric_active_count is None:
            return
        emit()
    except Exception:
        logger.debug("query_registry metric emit failed", exc_info=True)


# ── Registry ────────────────────────────────────────────────────────────────


class QueryRegistry:
    def __init__(self) -> None:
        self._queries: dict[int, ActiveQuery] = {}
        # deque is thread-safe for single appends/iter under CPython GIL.
        self._history: collections.deque[CompletedQuery] = collections.deque(maxlen=_HISTORY_CAP)

    # ── register / deregister ────────────────────────────────────────────────

    def register(
        self,
        db_type: str,
        sql: str,
        *,
        service_id: str | None = None,
        con: Any | None = None,
        pool_slot: str | None = None,
    ) -> int:
        """Insert an :class:`ActiveQuery` and return its query_id.

        Returns ``-1`` on internal failure (so callers can blindly pass it
        to :meth:`deregister` without branching). Instrumentation never
        raises into the SQL hot path."""
        if _REGISTRY_DISABLED:
            return -1
        try:
            qid = next(_seq)
            qualname, file_line = _capture_caller()
            base = current_attribution.get() or Attribution.system()
            attribution = base.with_caller(qualname, file_line).with_pool_slot(pool_slot)
            con_id = id(con) if con is not None else None
            active = ActiveQuery(
                query_id=qid,
                db_type=db_type,
                sql=_truncate(sql if isinstance(sql, str) else str(sql), _SQL_TRUNCATE),
                attribution=attribution,
                service_id=service_id,
                started_at_mono=time.monotonic(),
                started_at_utc=time.time(),
                _con_ref=_safe_weakref(con) if con is not None else None,
                _con_id=con_id,
            )
            self._queries[qid] = active
            if con_id is not None:
                with _conn_to_query_lock:
                    _conn_to_query[con_id] = qid
            _metric_safe(lambda: _metric_active_count.add(1, {"db": db_type, "kind": attribution.kind}))
            return qid
        except Exception:
            logger.debug("query_registry.register failed", exc_info=True)
            return -1

    def deregister(self, qid: int, *, error: BaseException | None = None) -> None:
        if qid < 0:
            return
        try:
            active = self._queries.pop(qid, None)
            if active is None:
                return
            if active._con_id is not None:
                with _conn_to_query_lock:
                    if _conn_to_query.get(active._con_id) == qid:
                        del _conn_to_query[active._con_id]

            ended = time.time()
            duration_ms = round((time.monotonic() - active.started_at_mono) * 1000, 2)
            if active.cancelled_at is not None:
                outcome = "cancelled"
            elif error is not None:
                outcome = "error"
            else:
                outcome = "ok"
            err_type = type(error).__name__ if error is not None else None
            err_msg: str | None = None
            if error is not None:
                err_msg = _truncate(str(error), _ERR_TRUNCATE)

            self._history.append(
                CompletedQuery(
                    query_id=active.query_id,
                    db_type=active.db_type,
                    sql=active.sql,
                    attribution=active.attribution,
                    service_id=active.service_id,
                    started_at_utc=active.started_at_utc,
                    ended_at_utc=ended,
                    duration_ms=duration_ms,
                    outcome=outcome,
                    error_type=err_type,
                    error_message=err_msg,
                )
            )

            _metric_safe(lambda: _metric_active_count.add(-1, {"db": active.db_type, "kind": active.attribution.kind}))
            _metric_safe(
                lambda: _metric_duration_ms.record(
                    duration_ms,
                    {
                        "db": active.db_type,
                        "kind": active.attribution.kind,
                        "outcome": outcome,
                    },
                )
            )
        except Exception:
            logger.debug("query_registry.deregister failed", exc_info=True)

    # ── cancel ───────────────────────────────────────────────────────────────

    def cancel_query(self, qid: int, *, admin_id: str | None = None) -> str:
        """Interrupt the targeted query if it's still on its connection.

        Returns a structured state string for the API to surface:
        ``"cancelled" | "not_found" | "already_finished" | "connection_gone"``.
        Always idempotent — admins re-click."""
        active = self._queries.get(qid)
        if active is None:
            return "not_found"
        if active._con_ref is None:
            return "already_finished"
        target_kind: str
        target_principal: str | None
        target_caller: str
        target_service: str | None
        target_db: str
        target_duration_ms: float
        with _conn_to_query_lock:
            if active._con_id is None or _conn_to_query.get(active._con_id) != qid:
                # Connection moved on to a different query — refuse.
                return "already_finished"
            con = active._con_ref()
            if con is None:
                return "connection_gone"
            try:
                con.interrupt()  # supported by both duckdb + sqlite3
            except Exception:
                logger.debug("interrupt() raised", exc_info=True)
                return "connection_gone"
            active.cancelled_at = time.time()
            target_kind = active.attribution.kind
            target_principal = active.attribution.principal_id()
            target_caller = active.attribution.caller_file
            target_service = active.service_id
            target_db = active.db_type
            target_duration_ms = round((time.monotonic() - active.started_at_mono) * 1000, 2)

        # Audit log outside the lock.
        try:
            from backend.utils.structlog_config import audit_log

            audit_log.warning(
                "query_cancel",
                admin_id=admin_id,
                query_id=qid,
                target_kind=target_kind,
                target_principal=target_principal,
                target_caller=target_caller,
                target_service=target_service,
                target_db=target_db,
                target_duration_ms=target_duration_ms,
            )
        except Exception:
            logger.debug("query_cancel audit log failed", exc_info=True)
        _metric_safe(lambda: _metric_cancelled_total.add(1, {"db": target_db, "kind": target_kind}))
        return "cancelled"

    # ── reads ────────────────────────────────────────────────────────────────

    def snapshot(
        self,
        *,
        since_seq: int = 0,
        full_sql: bool = False,
        include_completed: bool = False,
    ) -> dict[str, Any]:
        """Return active + (optionally) recently-completed rows newer than
        ``since_seq``. Snapshots by copying the dict/deque under a list
        comprehension — safe under concurrent writes."""
        now_mono = time.monotonic()
        active_rows: list[dict] = [
            _row_for_active(q, now_mono, full_sql) for q in list(self._queries.values()) if q.query_id > since_seq
        ]
        completed_rows: list[dict] = []
        if include_completed:
            completed_rows = [_row_for_completed(c, full_sql) for c in list(self._history) if c.query_id > since_seq]
        last_seq_active = max((r["query_id"] for r in active_rows), default=since_seq)
        last_seq_completed = max((r["query_id"] for r in completed_rows), default=since_seq)
        return {
            "last_seq": max(last_seq_active, last_seq_completed, since_seq),
            "active": active_rows,
            "completed": completed_rows,
        }

    def get(self, qid: int) -> ActiveQuery | None:
        return self._queries.get(qid)

    def summary(self) -> dict[str, Any]:
        """Cheap top-line counts for the tab badge."""
        active = list(self._queries.values())
        by_db: dict[str, int] = collections.Counter(q.db_type for q in active)
        longest_ms = 0.0
        if active:
            now = time.monotonic()
            longest_ms = round(max((now - q.started_at_mono) * 1000.0 for q in active), 2)
        return {
            "active_total": len(active),
            "by_db_type": dict(by_db),
            "longest_ms": longest_ms,
        }


def _safe_weakref(obj: Any) -> Callable[[], Any] | None:
    """Return a no-arg callable that dereferences to ``obj`` (or ``None``
    once ``obj`` is gone).

    Tries ``weakref.ref(obj)`` first — preferred so the registry never
    prevents the pool from freeing a connection on error
    ([duckdb_pool.py:338]). DuckDB connections support weakref; sqlite3
    connections do not (they have no ``__weakref__`` slot — verified
    against sqlite3 from CPython 3.13). For non-weakref-able objects we
    fall back to a strong-reference closure: as long as the
    :class:`ActiveQuery` is in ``_queries``, the connection lives; the
    moment ``deregister`` pops the row, the closure (and the strong ref)
    are collected. This matches the caller's own lifecycle — code calling
    ``cursor.execute()`` is already holding the connection during the
    query, so the registry's parallel strong ref doesn't change observable
    behavior. Returns ``None`` only if both paths fail (defensive)."""
    try:
        return weakref.ref(obj)
    except TypeError:
        try:
            ref = obj  # closure captures a strong reference

            def _strong_ref() -> Any:
                return ref

            return _strong_ref
        except Exception:
            return None


def _attribution_payload(attr: Attribution) -> dict[str, Any]:
    return {
        "kind": attr.kind,
        "label": attr.display_label(),
        "principal_id": attr.principal_id(),
        "caller_qualname": attr.caller_qualname,
        "caller_file": attr.caller_file,
        "request_path": attr.request_path,
        "request_id": attr.request_id,
        "cron_job": attr.cron_job,
        "cron_run_id": attr.cron_run_id,
        "pool_slot": attr.pool_slot,
    }


def _row_for_active(q: ActiveQuery, now_mono: float, full_sql: bool) -> dict[str, Any]:
    return {
        "query_id": q.query_id,
        "db_type": q.db_type,
        "sql_preview": q.sql[:200],
        "sql": q.sql if full_sql else None,
        "sql_len": len(q.sql),
        "attribution": _attribution_payload(q.attribution),
        "service_id": q.service_id,
        "started_at_utc": q.started_at_utc,
        "duration_ms": round((now_mono - q.started_at_mono) * 1000, 2),
        "cancellable": q._con_ref is not None,
        "cancelled_at": q.cancelled_at,
    }


def _row_for_completed(c: CompletedQuery, full_sql: bool) -> dict[str, Any]:
    return {
        "query_id": c.query_id,
        "db_type": c.db_type,
        "sql_preview": c.sql[:200],
        "sql": c.sql if full_sql else None,
        "sql_len": len(c.sql),
        "attribution": _attribution_payload(c.attribution),
        "service_id": c.service_id,
        "started_at_utc": c.started_at_utc,
        "ended_at_utc": c.ended_at_utc,
        "duration_ms": c.duration_ms,
        "outcome": c.outcome,
        "error_type": c.error_type,
        "error_message": c.error_message,
    }


# Process-wide singleton — every instrumentation site imports this.
query_registry = QueryRegistry()
