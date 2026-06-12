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
- ``RecordBatchReader`` paths (``.arrow()`` / ``fetch_record_batch``)
  return a streaming reader. We wrap the reader so deregistration waits
  for iteration to complete; the safety net :meth:`_InstrumentedResult.__del__`
  catches readers that are never iterated.
"""

from __future__ import annotations

import logging
import weakref
from collections.abc import Callable
from typing import Any

import structlog

logger = logging.getLogger(__name__)


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

        con_ref = _safe_weakref(self._con)
        bound = getattr(self._con, method_name)
        try:
            result = bound(query, *args, **kwargs)
        except BaseException as err:
            peak = _probe_duckdb_memory(self._con)
            _deregister(qid, err, peak_memory_mb=peak)
            raise
        # The result is a relation / cursor — wrap so terminal fetch
        # methods drive deregistration with the right timing.
        return _InstrumentedResult(result, qid, con_ref)


class _InstrumentedResult:
    """Proxy over a DuckDB result object that delays registry deregistration
    until a terminal fetch completes — or until garbage collection runs.

    Wraps the result so its ``__getattr__``-delegated terminal methods
    capture exceptions (so the registry records ``outcome="error"`` with
    the exception type) and so iteration via ``for row in result`` is
    covered too. Streaming reader methods (``.arrow()``,
    ``.fetch_record_batch()``) return a :class:`_InstrumentedRecordReader`
    that defers deregistration until the reader is exhausted.
    """

    __slots__ = ("_raw", "_qid", "_done", "_con_ref")

    def __init__(self, raw: Any, qid: int, con_ref: Callable[[], Any] | None = None):
        self._raw = raw
        self._qid = qid
        self._done = False
        self._con_ref = con_ref

    def _finish(self, error: BaseException | None = None, *, probe_memory: bool = True) -> None:
        if self._done:
            return
        self._done = True
        peak_mb: float | None = None
        if probe_memory and self._con_ref is not None:
            con = self._con_ref()
            if con is not None:
                peak_mb = _probe_duckdb_memory(con)
        _deregister(self._qid, error, peak_memory_mb=peak_mb)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._raw, name)
        if name in _READER_METHODS and callable(attr):
            # Return value is a streaming reader; deregistration must wait
            # for iteration. Hand ownership to the reader: build a finish
            # callable that captures qid + con_ref directly so the reader
            # can deregister even after we mark this instance done.
            finish = self._finish
            mark_done = self._mark_done
            qid = self._qid
            con_ref = self._con_ref

            def _reader_finish(error: BaseException | None = None, *, probe_memory: bool = True) -> None:
                peak_mb: float | None = None
                if probe_memory and con_ref is not None:
                    con = con_ref()
                    if con is not None:
                        peak_mb = _probe_duckdb_memory(con)
                _deregister(qid, error, peak_memory_mb=peak_mb)

            def _reader_wrapped(*args: Any, **kwargs: Any) -> Any:
                try:
                    reader = attr(*args, **kwargs)
                except BaseException as e:
                    finish(e)
                    raise
                # Hand ownership of deregistration to the reader. Mark
                # this instance done so its __del__ doesn't double-fire.
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
            # Iteration is a terminal completion; probe memory.
            if not self._done:
                self._finish(err)

    def __del__(self):
        # Safety net for callers that never reach a terminal method. Under
        # CPython refcount this fires deterministically when the wrapper
        # goes out of scope. Skip the memory probe — running SQL during
        # __del__ on a possibly-closed connection is unsafe.
        try:
            if not self._done:
                self._finish(None, probe_memory=False)
        except Exception:
            pass


class _InstrumentedRecordReader:
    """Proxy over ``pyarrow.RecordBatchReader`` that defers registry
    deregistration until iteration completes.

    Without this wrapper, ``.arrow()`` would deregister at the call site
    even though the consumer iterates batches lazily afterwards — the
    monitor would show ~0ms duration for what's actually a long stream.
    """

    __slots__ = ("_raw", "_finish", "_done")

    def __init__(self, raw: Any, finish: Callable[..., None]):
        self._raw = raw
        # `finish` is _InstrumentedResult._finish — a bound method that
        # carries the qid + con_ref. Keeping the bound method (rather than
        # the result) lets the wrapped result instance be collected as
        # soon as the caller drops it.
        self._finish = finish
        self._done = False

    def _complete(self, error: BaseException | None = None, *, probe_memory: bool = True) -> None:
        if self._done:
            return
        self._done = True
        try:
            self._finish(error, probe_memory=probe_memory)
        except Exception:
            pass

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
        # Pass-through for schema, read_pandas, etc. that don't mark
        # completion. Iteration / read_all / close / read_next_batch above
        # are the deterministic completion points.
        return getattr(self._raw, name)

    def __del__(self):
        try:
            self._complete(None, probe_memory=False)
        except Exception:
            pass


def _safe_weakref(obj: Any) -> Callable[[], Any] | None:
    """Return a weakref to ``obj`` if possible. Mirrors the same fallback
    pattern as :func:`backend.core.query_registry._safe_weakref` — see
    that docstring for the rationale. Returns ``None`` if the object can't
    be weakref'd and we don't want to hold a strong reference."""
    try:
        return weakref.ref(obj)
    except TypeError:
        return None


def _parse_memory_mb(value: Any) -> float | None:
    """Parse a DuckDB byte count into MB (float). Returns ``None`` if the
    value can't be interpreted.

    Accepts:
    - integers / floats (bytes)
    - strings with binary or decimal suffixes (``"512.5 MiB"``, ``"1.2 GB"``)

    The byte-count path is what :func:`_probe_duckdb_memory` uses today;
    the string path exists so a future probe that reads ``current_setting``
    can reuse this parser without bespoke handling.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        bytes_val = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        # Strip suffix, parse number.
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
        # Split on the first letter character.
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
    """Best-effort read of the connection's currently-held memory, in MB.

    Calls ``SELECT sum(memory_usage_bytes) + sum(temporary_storage_bytes)
    FROM duckdb_memory()`` on a fresh cursor — safe to invoke when the
    main query is already done (which is when ``_finish`` runs). Returns
    ``None`` if anything goes wrong — instrumentation is observability,
    not control flow.

    Note: this is "memory still held by the connection right after the
    query finished", not a true peak. For materialising queries
    (``CREATE TABLE AS``, persistent tables, registered DataFrames) this
    reflects the resident size; for transient SELECTs whose results have
    been consumed by the caller it can read low. We expose it as
    ``peak_memory_mb`` on the completed row because it's the most
    operationally useful single number we can capture without in-flight
    probing (deferred per design doc §13.4).
    """
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
                pass
        if row is None or row[0] is None:
            return None
        return _parse_memory_mb(row[0])
    except Exception:
        # DuckDB versions before ~1.0 don't have duckdb_memory(); also
        # could fail if the connection is mid-transaction in a weird state.
        # Either way: silently skip the field.
        return None


def _deregister(qid: int, error: BaseException | None, *, peak_memory_mb: float | None = None) -> None:
    if qid < 0:
        return
    try:
        from backend.core.query_registry import query_registry

        query_registry.deregister(qid, error=error, peak_memory_mb=peak_memory_mb)
    except Exception:
        logger.debug("live-registry deregister failed", exc_info=True)
    finally:
        try:
            structlog.contextvars.unbind_contextvars("query_id")
        except Exception:
            pass
