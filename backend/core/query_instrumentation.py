"""DuckDB connection + result proxies for the Live Query Monitor.

DuckDB ``con.execute()`` returns nearly instantly (~0.3ms for a 50M-row
SELECT in the verification test); the actual work happens in the result
object's terminal methods — ``fetchall``, ``fetchdf``, ``arrow``, etc.
Wrapping only ``execute()`` would lie by ~4 orders of magnitude. The proxy
therefore registers at execute-start, hands back a wrapped result, and
deregisters on the result's terminal fetch (or on garbage collection as a
safety net).

Lives in its own module so :mod:`backend.core.duckdb_pool` can import it
lazily and so :mod:`backend.core.query_attribution`'s frame-walk can list
this file in :data:`_INSTRUMENTATION_PREFIXES` without a circular import.

Caveats:

- ``__getattr__`` proxies break ``isinstance(con, DuckDBPyConnection)`` —
  but a grep of the backend shows the only ``isinstance`` test against the
  ``duckdb`` module checks ``IOException``, not the connection class, so
  no call sites need changing.
- ``__del__`` is best-effort but reliable under CPython refcount: the
  wrapper lives only inside the request's ``with checkout_connection`` block.
- DuckDB 1.5's ``.arrow()`` returns a ``RecordBatchReader`` that streams
  lazily. We deregister at the ``.arrow()`` call boundary, which may be
  slightly early for callers that iterate the reader after the call.
  Acceptable for v1: the only ``.arrow()`` use in this codebase is in
  :mod:`backend.core.iceberg.buffer` where the call site materialises the
  table immediately.
"""

from __future__ import annotations

import logging
from typing import Any

import structlog

logger = logging.getLogger(__name__)


# Terminal methods on a DuckDB result that actually materialise data.
# Deregistration happens after these complete (success or error).
_TERMINAL_METHODS: tuple[str, ...] = (
    "fetchall",
    "fetchone",
    "fetchmany",
    "fetchnumpy",
    "fetchdf",
    "fetch_df",
    "df",
    "fetch_df_chunk",
    "arrow",
    "fetch_arrow_table",
    "fetch_record_batch",
    "fetch_record_batches",
    "pl",
    "to_df",
    "torch",
    "tf",
    "close",
)

# Connection methods that issue a SQL statement. ``execute`` is the primary;
# the relational-API methods (``sql``, ``query``) also accept SQL text.
_EXEC_METHODS: tuple[str, ...] = ("execute", "executemany", "sql", "query")


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

    def execute(self, query, *args, **kwargs):
        return self._invoke("execute", query, args, kwargs)

    def executemany(self, query, *args, **kwargs):
        return self._invoke("executemany", query, args, kwargs)

    def sql(self, query, *args, **kwargs):
        return self._invoke("sql", query, args, kwargs)

    def query(self, query, *args, **kwargs):
        return self._invoke("query", query, args, kwargs)

    # ── delegation for everything else ──────────────────────────────────────

    def __getattr__(self, name: str) -> Any:
        # __slots__ omits __dict__, so anything not in the slot list (or
        # explicitly defined above) lands here. Pass through to the raw
        # connection.
        return getattr(self._con, name)

    def __enter__(self):
        # DuckDB connections support context-manager use; preserve it.
        self._con.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._con.__exit__(exc_type, exc_val, exc_tb)

    # Cursor-style attribute access — DuckDB doesn't have a cursor() method
    # but some callers chain .description and similar on the connection
    # directly. __getattr__ handles those.

    # ── internals ───────────────────────────────────────────────────────────

    def _invoke(self, method_name: str, query: Any, args: tuple, kwargs: dict) -> Any:
        from backend.core.query_registry import query_registry

        sql_text = str(query)
        # Per-connection short id so ops can correlate two queries on the
        # same physical pool slot. id() % 10000 is stable for the
        # connection's lifetime and never exposed across processes.
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
                pass

        bound = getattr(self._con, method_name)
        try:
            result = bound(query, *args, **kwargs)
        except BaseException as err:
            _deregister(qid, err)
            raise
        # The result is a relation / cursor — wrap so terminal fetch
        # methods drive deregistration with the right timing.
        return _InstrumentedResult(result, qid)


class _InstrumentedResult:
    """Proxy over a DuckDB result object that delays registry deregistration
    until a terminal fetch completes — or until garbage collection runs.

    Wraps the result so its ``__getattr__``-delegated terminal methods
    capture exceptions (so the registry records ``outcome="error"`` with
    the exception type) and so iteration via ``for row in result`` is
    covered too.
    """

    __slots__ = ("_raw", "_qid", "_done")

    def __init__(self, raw: Any, qid: int):
        self._raw = raw
        self._qid = qid
        self._done = False

    def _finish(self, error: BaseException | None = None) -> None:
        if self._done:
            return
        self._done = True
        _deregister(self._qid, error)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._raw, name)
        if name in _TERMINAL_METHODS and callable(attr):
            qid = self._qid
            done_setter = self._mark_done

            def _wrapped(*args: Any, **kwargs: Any) -> Any:
                err: BaseException | None = None
                try:
                    return attr(*args, **kwargs)
                except BaseException as e:
                    err = e
                    raise
                finally:
                    done_setter()
                    _deregister(qid, err)

            return _wrapped
        return attr

    def _mark_done(self) -> None:
        self._done = True

    def __iter__(self) -> Any:
        qid = self._qid
        err: BaseException | None = None
        try:
            yield from iter(self._raw)
        except BaseException as e:
            err = e
            raise
        finally:
            if not self._done:
                self._done = True
                _deregister(qid, err)

    def __del__(self):
        # Safety net for callers that never reach a terminal method. Under
        # CPython refcount this fires deterministically when the wrapper
        # goes out of scope.
        try:
            if not self._done:
                self._done = True
                _deregister(self._qid, None)
        except Exception:
            pass


def _deregister(qid: int, error: BaseException | None) -> None:
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
