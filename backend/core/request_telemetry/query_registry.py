"""In-flight and completed query registry, attribution, and DuckDB connection instrumentation."""

from __future__ import annotations

import collections
import itertools
import logging
import os
import sys
import threading
import time
import types
import weakref
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, replace
from typing import Any, Literal

import cachetools
import duckdb
import structlog
from opentelemetry import metrics
from opentelemetry.trace import Span

from backend.core.request_telemetry.instruments import (
    _end_db_span,
    _start_db_span,
)
from backend.core.request_telemetry.middleware import audit_log, scrub_secrets

logger = logging.getLogger("backend.core.request_telemetry")

# ── Query Attribution (folded from backend.core.query_attribution) ───────────

_INSTRUMENTATION_PREFIXES: tuple[str, ...] = (
    "backend/core/query_registry",
    "backend/core/query_attribution",
    "backend/core/request_telemetry",
    "backend/core/query_instrumentation",
    "backend/utils/telemetry",
    "backend/core/duckdb_pool",
    "/sqlite3/",
    "/duckdb/",
)


def _capture_caller(skip_frames: int = 2) -> tuple[str, str]:
    """Return ``(qualname, "<rel-path>:<lineno>")`` of the first non-instrumentation caller frame."""
    try:
        frame: Any = sys._getframe(skip_frames)
    except ValueError:
        return ("<unknown>", "<unknown>")
    while frame is not None:
        path = frame.f_code.co_filename
        if not any(p in path for p in _INSTRUMENTATION_PREFIXES):
            qual = getattr(frame.f_code, "co_qualname", frame.f_code.co_name)
            display_path = path
            for marker in ("backend/", "frontend/"):
                idx = path.rfind(marker)
                if idx != -1:
                    display_path = path[idx:]
                    break
            return (qual, f"{display_path}:{frame.f_lineno}")
        frame = frame.f_back
    return ("<unknown>", "<unknown>")


@dataclass(slots=True)
class Attribution:
    """Structured attribution for a single in-flight or completed query."""

    kind: str  # "analyst" | "admin" | "cron" | "system"
    analyst_id: str | None = None
    analyst_name: str | None = None
    admin_id: str | None = None
    cron_job: str | None = None
    cron_run_id: str | None = None

    caller_qualname: str = "<unknown>"
    caller_file: str = "<unknown>"
    request_path: str | None = None
    request_id: str | None = None
    cancel_key: str | None = None
    pool_slot: str | None = None

    @classmethod
    def analyst(
        cls,
        *,
        analyst_id: str,
        analyst_name: str | None,
        request_path: str | None,
        request_id: str | None,
        cancel_key: str | None = None,
    ) -> Attribution:
        return cls(
            kind="analyst",
            analyst_id=analyst_id,
            analyst_name=analyst_name,
            request_path=request_path,
            request_id=request_id,
            cancel_key=cancel_key,
        )

    @classmethod
    def admin(
        cls,
        *,
        admin_id: str,
        request_path: str | None,
        request_id: str | None,
        cancel_key: str | None = None,
    ) -> Attribution:
        return cls(
            kind="admin",
            admin_id=admin_id,
            request_path=request_path,
            request_id=request_id,
            cancel_key=cancel_key,
        )

    @classmethod
    def cron(cls, *, cron_job: str, cron_run_id: str | None = None) -> Attribution:
        return cls(kind="cron", cron_job=cron_job, cron_run_id=cron_run_id)

    @classmethod
    def system(cls, *, hint: str | None = None) -> Attribution:
        return cls(
            kind="system",
            caller_qualname=hint or f"thread:{threading.current_thread().name}",
            caller_file="<system>",
        )

    def principal_id(self) -> str | None:
        if self.kind == "analyst":
            return self.analyst_id
        if self.kind == "admin":
            return self.admin_id
        if self.kind == "cron":
            return self.cron_run_id or self.cron_job
        return None

    def display_label(self) -> str:
        tail = f" — {self.request_path}" if self.request_path else ""
        if self.kind == "analyst":
            who = self.analyst_name or f"Guest ({self.analyst_id[-4:] if self.analyst_id else '?'})"
            return f"Analyst: {who}{tail}"
        if self.kind == "admin":
            return f"Admin: {self.admin_id or 'admin'}{tail}"
        if self.kind == "cron":
            run = f" (run {self.cron_run_id})" if self.cron_run_id else ""
            return f"Cron: {self.cron_job}{run}"
        return f"System: {self.caller_qualname}"

    def with_caller(self, qualname: str, file_line: str) -> Attribution:
        return replace(self, caller_qualname=qualname, caller_file=file_line)

    def with_pool_slot(self, slot: str | None) -> Attribution:
        if slot is None or self.pool_slot == slot:
            return self
        return replace(self, pool_slot=slot)


current_attribution: ContextVar[Attribution | None] = ContextVar("current_attribution", default=None)


def derive_from_process_context(process_ctx: str | None) -> Attribution | None:
    """Build an Attribution for ``cron:`` / ``startup:`` / ``shutdown:`` process contexts."""
    if not process_ctx:
        return None
    head, _, tail = process_ctx.partition(":")
    if head == "cron":
        return Attribution.cron(cron_job=tail or process_ctx)
    if head in ("startup", "shutdown"):
        return Attribution.system(hint=process_ctx)
    return None


# ── Query Instrumentation (folded from backend.core.query_instrumentation) ──

_MEMORY_PROBE_THRESHOLD_S = 0.5

# Methods that return a streaming reader rather than a materialised result.
# The reader's iteration is the real work; deregistration must wait for
# the reader to be exhausted (or garbage collected), not for the method to
# return.
_READER_METHODS: frozenset[str] = frozenset({"arrow", "fetch_record_batch", "fetch_record_batches"})

# Terminal methods on a DuckDB result that actually materialise data.
# Deregistration happens after these complete (success or error).
# Streaming reader methods are handled separately by :data:`_READER_METHODS`
# below and intentionally NOT listed here.
_TERMINAL_METHODS: tuple[str, ...] = (
    "fetchall",
    "fetchone",
    "fetchmany",
    "fetchnumpy",
    "fetchdf",
    "fetch_df",
    "df",
    "fetch_df_chunk",
    "fetch_arrow_table",
    "to_arrow_table",
    "pl",
    "to_df",
    "torch",
    "tf",
    "close",
)


class InstrumentedDuckDBConnection:
    """Thin proxy around a raw ``duckdb.DuckDBPyConnection``.

    Constructed by :func:`backend.core.duckdb_pool._instrument`; lives only
    inside the request's ``with checkout_connection(...)`` scope.
    """

    __slots__ = ("_con", "_service_id")

    def __init__(self, raw_con: Any, *, service_id: str | None):
        self._con = raw_con
        self._service_id = service_id

    # ── instrumented exec entry-points ──────────────────────────────────────

    def execute(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        return self._invoke("execute", query, args, kwargs)

    def executemany(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        return self._invoke("executemany", query, args, kwargs)

    def sql(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        return self._invoke("sql", query, args, kwargs)

    def query(self, query: Any, *args: Any, **kwargs: Any) -> Any:
        return self._invoke("query", query, args, kwargs)

    # ── delegation for everything else ──────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        # __slots__ omits __dict__, so anything not in the slot list (or
        # explicitly defined above) lands here. Pass through to the raw
        # connection.
        return getattr(self._con, name)

    def __enter__(self) -> InstrumentedDuckDBConnection:
        # DuckDB connections support context-manager use; preserve it.
        self._con.__enter__()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
        return self._con.__exit__(exc_type, exc_val, exc_tb)

    # ── internals ───────────────────────────────────────────────────────────

    def _invoke(self, method_name: str, query: Any, args: tuple, kwargs: dict) -> Any:
        sql_text = str(query)
        slot = None
        if self._service_id is not None:
            slot = f"{self._service_id}#{id(self._con) % 10000:04d}"
        qid = query_registry.register(
            "DuckDB",
            sql_text,
            service_id=self._service_id,
            con=self._con,
            pool_slot=slot,
        )
        if qid >= 0:
            try:
                structlog.contextvars.bind_contextvars(query_id=qid)
            except Exception:
                logger.debug("_invoke: structlog.contextvars.bind_contextvars failed (ignored)", exc_info=True)

        con_ref = _safe_weakref(self._con)
        bound = getattr(self._con, method_name)
        _is_cleanup_sql = sql_text.lstrip().upper().startswith("DROP ")
        if not _is_cleanup_sql and query_registry.is_query_cancelled(qid):
            err = duckdb.InterruptException("Interrupt Error: Interrupted!")
            _deregister(qid, err, peak_memory_mb=None)
            raise err
        span = _start_db_span("duckdb", sql_text, self._service_id)
        try:
            result = bound(query, *args, **kwargs)
        except BaseException as err:
            peak = _probe_duckdb_memory(self._con)
            _deregister(qid, err, peak_memory_mb=peak)
            _end_db_span(span, err)
            raise
        return _InstrumentedResult(result, qid, con_ref, span)


class _InstrumentedResult:
    """Proxy over a DuckDB result object that delays registry deregistration
    until a terminal fetch completes — or until garbage collection runs.
    """

    __slots__ = ("_raw", "_qid", "_done", "_con_ref", "_start_mono", "_span")

    def __init__(
        self,
        raw: Any,
        qid: int,
        con_ref: Callable[[], Any] | None = None,
        span: Span | None = None,
    ):
        self._raw = raw
        self._qid = qid
        self._done = False
        self._con_ref = con_ref
        self._start_mono = time.monotonic()
        self._span = span

    def _finish(self, error: BaseException | None = None, *, probe_memory: bool = True) -> None:
        if self._done:
            return
        self._done = True
        peak_mb: float | None = None
        elapsed = time.monotonic() - self._start_mono
        if probe_memory and elapsed >= _MEMORY_PROBE_THRESHOLD_S and self._con_ref is not None:
            con = self._con_ref()
            if con is not None:
                peak_mb = _probe_duckdb_memory(con)
        _deregister(self._qid, error, peak_memory_mb=peak_mb)
        _end_db_span(self._span, error)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._raw, name)
        if name in _READER_METHODS and callable(attr):
            finish = self._finish
            mark_done = self._mark_done
            qid = self._qid
            con_ref = self._con_ref
            start_mono = self._start_mono
            span = self._span

            def _reader_finish(error: BaseException | None = None, *, probe_memory: bool = True) -> None:
                peak_mb: float | None = None
                elapsed = time.monotonic() - start_mono
                if probe_memory and elapsed >= _MEMORY_PROBE_THRESHOLD_S and con_ref is not None:
                    con = con_ref()
                    if con is not None:
                        peak_mb = _probe_duckdb_memory(con)
                _deregister(qid, error, peak_memory_mb=peak_mb)
                _end_db_span(span, error)

            def _reader_wrapped(*args: Any, **kwargs: Any) -> Any:
                try:
                    reader = attr(*args, **kwargs)
                except BaseException as e:
                    finish(e)
                    raise
                mark_done()
                return _InstrumentedRecordReader(reader, _reader_finish)

            return _reader_wrapped
        if name in _TERMINAL_METHODS and callable(attr):
            finish = self._finish

            def _wrapped(*args: Any, **kwargs: Any) -> Any:
                err: BaseException | None = None
                try:
                    return attr(*args, **kwargs)
                except BaseException as e:
                    err = e
                    raise
                finally:
                    finish(err)

            return _wrapped
        return attr

    def _mark_done(self) -> None:
        self._done = True

    def __iter__(self) -> Any:
        err: BaseException | None = None
        try:
            yield from iter(self._raw)
        except BaseException as e:
            err = e
            raise
        finally:
            if not self._done:
                self._finish(err)

    def __del__(self) -> None:
        try:
            if not self._done:
                self._finish(None, probe_memory=False)
        except Exception:
            logger.debug("__del__: step failed (ignored)", exc_info=True)


class _InstrumentedRecordReader:
    """Proxy over ``pyarrow.RecordBatchReader`` that defers registry
    deregistration until iteration completes.
    """

    __slots__ = ("_raw", "_finish", "_done")

    def __init__(self, raw: Any, finish: Callable[..., None]):
        self._raw = raw
        self._finish = finish
        self._done = False

    def _complete(self, error: BaseException | None = None, *, probe_memory: bool = True) -> None:
        if self._done:
            return
        self._done = True
        try:
            self._finish(error, probe_memory=probe_memory)
        except Exception:
            logger.debug("_complete: self._finish failed (ignored)", exc_info=True)

    def __iter__(self) -> Any:
        err: BaseException | None = None
        try:
            yield from iter(self._raw)
        except BaseException as e:
            err = e
            raise
        finally:
            self._complete(err)

    def read_next_batch(self) -> Any:
        try:
            return self._raw.read_next_batch()
        except StopIteration:
            self._complete()
            raise
        except BaseException as e:
            self._complete(e)
            raise

    def read_all(self) -> Any:
        err: BaseException | None = None
        try:
            return self._raw.read_all()
        except BaseException as e:
            err = e
            raise
        finally:
            self._complete(err)

    def close(self) -> Any:
        try:
            return self._raw.close()
        finally:
            self._complete()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw, name)

    def __del__(self) -> None:
        try:
            self._complete(None, probe_memory=False)
        except Exception:
            logger.debug("__del__: self._complete failed (ignored)", exc_info=True)


def _safe_weakref(obj: Any) -> Callable[[], Any] | None:
    try:
        return weakref.ref(obj)
    except TypeError:
        try:
            ref = obj

            def _strong_ref() -> Any:
                return ref

            return _strong_ref
        except Exception:
            return None


def _parse_memory_mb(value: Any) -> float | None:
    """Parse a DuckDB byte count into MB (float). Returns ``None`` if the
    value can't be interpreted.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        bytes_val = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        units = {
            "b": 1.0,
            "bytes": 1.0,
            "kb": 1_000.0,
            "kib": 1024.0,
            "mb": 1_000_000.0,
            "mib": 1024.0**2,
            "gb": 1_000_000_000.0,
            "gib": 1024.0**3,
            "tb": 1_000_000_000_000.0,
            "tib": 1024.0**4,
        }
        import re as _re

        m = _re.match(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)?\s*$", text)
        if m is None:
            return None
        num = float(m.group(1))
        unit = (m.group(2) or "b").lower()
        if unit not in units:
            return None
        bytes_val = num * units[unit]
    else:
        return None
    return round(bytes_val / (1024.0 * 1024.0), 2)


def _probe_duckdb_memory(con: Any) -> float | None:
    """Best-effort read of the connection's currently-held memory, in MB."""
    try:
        cursor = con.cursor()
        try:
            row = cursor.execute(
                "SELECT sum(memory_usage_bytes) + sum(temporary_storage_bytes) FROM duckdb_memory()"
            ).fetchone()
        finally:
            try:
                cursor.close()
            except Exception:
                logger.debug("_probe_duckdb_memory: cursor.close failed (ignored)", exc_info=True)
        if row is None or row[0] is None:
            return None
        return _parse_memory_mb(row[0])
    except Exception:
        return None


def _deregister(qid: int, error: BaseException | None, *, peak_memory_mb: float | None = None) -> None:
    if qid < 0:
        return
    try:
        query_registry.deregister(qid, error=error, peak_memory_mb=peak_memory_mb)
    except Exception:
        logger.debug("live-registry deregister failed", exc_info=True)
    finally:
        try:
            structlog.contextvars.unbind_contextvars("query_id")
        except Exception:
            logger.debug("_deregister: structlog.contextvars.unbind_contextvars failed (ignored)", exc_info=True)


# ── Query Registry (folded from backend.core.query_registry) ─────────────────

_SQL_TRUNCATE = 4096
_ERR_TRUNCATE = 512
_HISTORY_CAP = 400
_SLOW_QUERY_PERSIST_THRESHOLD_MS = float(os.environ.get("QUERY_REGISTRY_PERSIST_THRESHOLD_MS", "100"))
_SLOW_QUERY_PERSIST_DISABLED = os.environ.get("QUERY_REGISTRY_PERSIST_DISABLED", "").lower() in (
    "1",
    "true",
    "yes",
)
try:
    _POOL_QUERY_TIMEOUT_S = max(1.0, float(os.environ.get("DUCKDB_POOL_QUERY_TIMEOUT_S", "120")))
except (TypeError, ValueError):
    _POOL_QUERY_TIMEOUT_S = 120.0
_POOL_QUERY_WATCH_INTERVAL_S = 1.0
_seq = itertools.count(1)
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

_conn_to_query: dict[int, int] = {}
_conn_to_query_lock = threading.Lock()


@dataclass(slots=True)
class ActiveQuery:
    query_id: int
    db_type: str
    sql: str
    attribution: Attribution
    service_id: str | None
    started_at_mono: float
    started_at_utc: float
    _con_ref: Callable[[], Any] | None = None
    _con_id: int | None = None
    cancelled_at: float | None = None


@dataclass(slots=True)
class CompletedQuery:
    query_id: int
    db_type: str
    sql: str
    attribution: Attribution
    service_id: str | None
    started_at_utc: float
    ended_at_utc: float
    duration_ms: float
    outcome: str
    error_type: str | None = None
    error_message: str | None = None
    peak_memory_mb: float | None = None


_scrub_secrets = scrub_secrets


def _truncate(text: str, cap: int) -> str:
    if len(text) <= cap:
        return text
    return text[:cap] + f"… [+{len(text) - cap} chars]"


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


class _CancelledKeyCache(cachetools.LRUCache[str, bool]):
    """Bounded LRU set/cache of cancelled request keys using cachetools."""

    def append(self, key: str) -> None:
        self[key] = True

    def extend(self, keys: Any) -> None:
        for key in keys:
            self[key] = True


class QueryRegistry:
    def __init__(self) -> None:
        self._queries: dict[int, ActiveQuery] = {}
        self._history: collections.deque[CompletedQuery] = collections.deque(maxlen=_HISTORY_CAP)
        self._cancelled_keys: _CancelledKeyCache = _CancelledKeyCache(maxsize=512)

    def register(
        self,
        db_type: str,
        sql: str,
        *,
        service_id: str | None = None,
        con: Any | None = None,
        pool_slot: str | None = None,
    ) -> int:
        if _REGISTRY_DISABLED:
            return -1
        try:
            qid = next(_seq)
            qualname, file_line = _capture_caller()
            base = current_attribution.get() or Attribution.system()
            attribution = base.with_caller(qualname, file_line).with_pool_slot(pool_slot)
            con_id = id(con) if con is not None else None
            sql_str = sql if isinstance(sql, str) else str(sql)
            active = ActiveQuery(
                query_id=qid,
                db_type=db_type,
                sql=_truncate(_scrub_secrets(sql_str), _SQL_TRUNCATE),
                attribution=attribution,
                service_id=service_id,
                started_at_mono=time.monotonic(),
                started_at_utc=time.time(),
                _con_ref=_safe_weakref(con) if con is not None else None,
                _con_id=con_id,
            )
            if (
                db_type == "DuckDB"
                and attribution.cancel_key
                and attribution.cancel_key in self._cancelled_keys
                and not sql_str.lstrip().upper().startswith("DROP ")
            ):
                active.cancelled_at = time.time()
            self._queries[qid] = active
            if con_id is not None:
                with _conn_to_query_lock:
                    _conn_to_query[con_id] = qid
            _metric_safe(lambda: _metric_active_count.add(1, {"db": db_type, "kind": attribution.kind}))
            return qid
        except Exception:
            logger.debug("query_registry.register failed", exc_info=True)
            return -1

    def deregister(
        self,
        qid: int,
        *,
        error: BaseException | None = None,
        peak_memory_mb: float | None = None,
    ) -> None:
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
                err_msg = _truncate(_scrub_secrets(str(error)), _ERR_TRUNCATE)

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
                    peak_memory_mb=peak_memory_mb,
                )
            )

            if (
                not _SLOW_QUERY_PERSIST_DISABLED
                and duration_ms >= _SLOW_QUERY_PERSIST_THRESHOLD_MS
                and active.service_id
            ):
                try:
                    _persist_slow_query(active, ended, duration_ms, outcome, err_type, err_msg, peak_memory_mb)
                except Exception:
                    logger.debug("query_registry slow-query persist failed", exc_info=True)

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

    def expire_pool_queries(self, *, now_mono: float | None = None) -> int:
        now = time.monotonic() if now_mono is None else now_mono
        now_wall = time.time()
        for active in tuple(self._queries.values()):
            if active.cancelled_at is not None and (now_wall - active.cancelled_at) >= 5.0:
                self.deregister(active.query_id)

        expired = [
            active.query_id
            for active in tuple(self._queries.values())
            if active.db_type == "DuckDB"
            and active.attribution.pool_slot is not None
            and active.cancelled_at is None
            and now - active.started_at_mono >= _POOL_QUERY_TIMEOUT_S
        ]
        interrupted = 0
        for qid in expired:
            if self.cancel_query(qid, admin_id="pool-timeout") == "cancelled":
                interrupted += 1
                logger.warning("interrupting pooled DuckDB query after %.1fs", _POOL_QUERY_TIMEOUT_S)
        return interrupted

    def cancel_query(
        self, qid: int, *, admin_id: str | None = None
    ) -> Literal["cancelled", "not_found", "already_finished", "connection_gone"]:
        active = self._queries.get(qid)
        if active is None:
            return "not_found"
        if active._con_ref is None:
            return "already_finished"
        with _conn_to_query_lock:
            if active._con_id is None or _conn_to_query.get(active._con_id) != qid:
                return "already_finished"
            con = active._con_ref()
            if con is None:
                return "connection_gone"
            try:
                con.interrupt()
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

        try:
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

    def is_query_cancelled(self, qid: int) -> bool:
        if qid < 0:
            return False
        active = self._queries.get(qid)
        return bool(active is not None and active.cancelled_at is not None)

    def cancel_for_key(self, cancel_key: str | None, *, admin_id: str = "client-disconnect") -> int:
        if not cancel_key:
            return 0
        self._cancelled_keys[cancel_key] = True
        interrupted = 0
        for active in tuple(self._queries.values()):
            if (
                active.db_type == "DuckDB"
                and active.cancelled_at is None
                and active.attribution.cancel_key == cancel_key
                and self.cancel_query(active.query_id, admin_id=admin_id) == "cancelled"
            ):
                interrupted += 1
        return interrupted

    def snapshot(
        self,
        *,
        since_seq: int = 0,
        full_sql: bool = False,
        include_completed: bool = False,
        all_active: bool = False,
        cold_limit: int | None = None,
    ) -> dict[str, Any]:
        now_mono = time.monotonic()
        active_rows: list[dict] = [
            _row_for_active(q, now_mono, full_sql)
            for q in list(self._queries.values())
            if all_active or q.query_id > since_seq
        ]
        completed_rows: list[dict] = []
        if include_completed:
            completed_rows = [_row_for_completed(c, full_sql) for c in list(self._history) if c.query_id > since_seq]
            if cold_limit is not None and cold_limit > 0 and since_seq == 0 and len(completed_rows) > cold_limit:
                completed_rows = completed_rows[-cold_limit:]
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


def _persist_slow_query(
    active: ActiveQuery,
    ended: float,
    duration_ms: float,
    outcome: str,
    err_type: str | None,
    err_msg: str | None,
    peak_memory_mb: float | None,
) -> None:
    from backend.core import metadata as _meta_mod

    attr = active.attribution
    _meta_mod.insert_slow_query(
        active.service_id or "",
        {
            "query_id": active.query_id,
            "db_type": active.db_type,
            "service_id": active.service_id,
            "started_at_utc": active.started_at_utc,
            "ended_at_utc": ended,
            "duration_ms": duration_ms,
            "outcome": outcome,
            "sql_preview": active.sql[:200],
            "sql_full": active.sql,
            "sql_len": len(active.sql),
            "attr_kind": attr.kind,
            "attr_label": attr.display_label(),
            "attr_principal_id": attr.principal_id(),
            "attr_caller_qualname": attr.caller_qualname,
            "attr_caller_file": attr.caller_file,
            "attr_request_path": attr.request_path,
            "attr_request_id": attr.request_id,
            "attr_cron_job": attr.cron_job,
            "attr_cron_run_id": attr.cron_run_id,
            "attr_pool_slot": attr.pool_slot,
            "error_type": err_type,
            "error_message": err_msg,
            "peak_memory_mb": peak_memory_mb,
        },
    )


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
        "peak_memory_mb": c.peak_memory_mb,
    }


query_registry = QueryRegistry()


def _pool_query_watchdog() -> None:
    while True:
        try:
            query_registry.expire_pool_queries()
        except Exception:
            logger.debug("pooled-query watchdog failed", exc_info=True)
        time.sleep(_POOL_QUERY_WATCH_INTERVAL_S)


threading.Thread(
    target=_pool_query_watchdog,
    name="duckdb-pool-query-watchdog",
    daemon=True,
).start()


class _QueryRegistryModule(types.ModuleType):
    """Allow `patch('backend.core.request_telemetry.query_registry.<attr>')` to target `query_registry`."""

    def __getattr__(self, name: str) -> Any:
        return getattr(query_registry, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name != "query_registry" and hasattr(QueryRegistry, name):
            setattr(query_registry, name, value)
            return
        super().__setattr__(name, value)

    def __delattr__(self, name: str) -> None:
        if name != "query_registry" and hasattr(QueryRegistry, name):
            delattr(query_registry, name)
            return
        super().__delattr__(name)


sys.modules[__name__].__class__ = _QueryRegistryModule
