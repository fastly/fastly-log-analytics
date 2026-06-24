"""Branch coverage for backend/core/query_instrumentation.py.

Audit finding: the proxy + memory-probe paths in
:mod:`backend.core.query_instrumentation` carry several "best effort"
contracts that, if regressed, would silently double-count registry
deregistrations, leak active rows for streaming readers, or crash the
SQL hot path when DuckDB returns a malformed PRAGMA row. These tests
pin those contracts using real in-memory DuckDB connections (faster +
more realistic than mocking the DuckDB API) and only mock the
``query_registry`` symbol to count register/deregister calls.

Covers:
- ``_InstrumentedResult._finish`` idempotency (no double-deregister)
- Streaming readers (``arrow`` / ``fetch_record_batch``) defer
  deregistration until the reader is exhausted
- Terminal methods (``fetchall``, ``fetchone``, ``fetchnumpy``)
  deregister immediately
- ``.execute`` raising still deregisters with an error flag
- Weakref-after-GC fallback: probe is safe when underlying con is gone
- ``_probe_duckdb_memory`` swallows malformed PRAGMA output without
  crashing the caller
"""

from __future__ import annotations

import gc
import weakref
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from backend.core.query_instrumentation import (
    InstrumentedDuckDBConnection,
    _InstrumentedRecordReader,
    _InstrumentedResult,
    _probe_duckdb_memory,
    _safe_weakref,
)


@pytest.fixture
def duck() -> duckdb.DuckDBPyConnection:
    """A throwaway in-memory DuckDB connection per test."""
    con = duckdb.connect(":memory:")
    yield con
    try:
        con.close()
    except Exception:
        pass


# ── _InstrumentedResult._finish idempotency ─────────────────────────────────


def test_finish_is_idempotent_no_double_deregister():
    """Calling ``_finish`` twice must deregister exactly once. Without
    the ``self._done`` guard, a terminal method followed by ``__del__``
    (or two terminal methods in a row) would double-pop the registry
    and corrupt by-db-type counts."""
    fake_raw = MagicMock()
    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        result = _InstrumentedResult(fake_raw, qid=7, con_ref=None)
        result._finish(None, probe_memory=False)
        result._finish(None, probe_memory=False)
        # And a third call (the safety-net __del__ path) is also a no-op.
        result._finish(None, probe_memory=False)
    assert mock_dereg.call_count == 1
    assert mock_dereg.call_args.args == (7, None)


# ── Streaming reader defers deregistration ──────────────────────────────────


def test_arrow_reader_defers_deregister_until_exhausted(duck):
    """``.arrow()`` returns a streaming reader; the proxy must NOT
    deregister at the method-call boundary. Without _InstrumentedRecordReader
    the monitor would attribute ~0ms to long streams."""
    duck.execute("CREATE TABLE t AS SELECT i FROM range(50_000) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="defer_svc")

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        reader = con.execute("SELECT * FROM t").arrow()
        # Returning the reader does NOT deregister.
        assert mock_dereg.call_count == 0
        assert isinstance(reader, _InstrumentedRecordReader)
        # Iterate — deregister fires exactly once at the end.
        total = sum(b.num_rows for b in reader)
        assert total == 50_000
        assert mock_dereg.call_count == 1


def test_fetch_record_batch_defers_until_drained(duck):
    """``fetch_record_batch`` is in _READER_METHODS too; deregistration
    waits for the consumer to drain via read_next_batch()."""
    duck.execute("CREATE TABLE t AS SELECT i FROM range(1_000) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="frb_svc")

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        reader = con.execute("SELECT * FROM t").fetch_record_batch(100)
        assert mock_dereg.call_count == 0
        # Drain by iterating (RecordBatchReader supports iteration).
        rows = sum(b.num_rows for b in reader)
        assert rows == 1_000
        assert mock_dereg.call_count == 1


# ── Terminal methods deregister immediately ─────────────────────────────────


@pytest.mark.parametrize("method", ["fetchall", "fetchone", "fetchnumpy"])
def test_terminal_method_deregisters_immediately(duck, method):
    """fetchall / fetchone / fetchnumpy are in _TERMINAL_METHODS — they
    must deregister at the call boundary (not after some later GC)."""
    duck.execute("CREATE TABLE t AS SELECT i FROM range(10) tbl(i)")
    con = InstrumentedDuckDBConnection(duck, service_id="term_svc")

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        result = con.execute("SELECT i FROM t")
        getattr(result, method)()
        assert mock_dereg.call_count == 1
        # The single deregister call carried no error.
        assert mock_dereg.call_args.args[1] is None


# ── Execute-time exception still deregisters with error ─────────────────────


def test_execute_exception_deregisters_with_error(duck):
    """When the underlying ``.execute`` raises, the proxy must still
    deregister AND pass the exception so the registry records
    ``outcome="error"`` rather than leaking the row as active forever."""
    con = InstrumentedDuckDBConnection(duck, service_id="err_svc")

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        with pytest.raises(duckdb.Error):
            con.execute("SELECT * FROM table_that_does_not_exist_xyz")
        assert mock_dereg.call_count == 1
        # Second positional arg is the error instance.
        passed_err = mock_dereg.call_args.args[1]
        assert isinstance(passed_err, BaseException)


# ── Weakref-after-GC: _finish does not raise when con is gone ───────────────


def test_finish_safe_when_underlying_con_gc_collected():
    """The proxy keeps only a weakref to the raw connection (so the
    pool can free it on error). When the raw con is GC'd before
    ``_finish`` runs, the probe must short-circuit on ``con is None``
    rather than crashing — instrumentation must never crash the caller."""

    class _WeakRefable:
        def cursor(self):  # never called — weakref dies before probe runs
            raise AssertionError("probe ran on a dead connection")

    raw = _WeakRefable()
    ref = _safe_weakref(raw)
    assert ref is not None and ref() is raw  # weakref worked

    fake_inner = MagicMock()
    result = _InstrumentedResult(fake_inner, qid=42, con_ref=ref)

    del raw
    gc.collect()
    assert ref() is None  # underlying con collected

    with patch("backend.core.query_instrumentation._deregister") as mock_dereg:
        # probe_memory=True forces the con_ref deref branch; must not raise.
        result._finish(None, probe_memory=True)
    assert mock_dereg.call_count == 1
    # peak_memory_mb kwarg defaults to None when the con is gone.
    assert mock_dereg.call_args.kwargs.get("peak_memory_mb") is None


def test_safe_weakref_falls_back_to_strong_ref_for_non_weakrefable():
    """Some objects (e.g. plain ints, sqlite3 connections in some
    builds) reject weakref. The helper must return a callable that
    yields the object rather than ``None`` — otherwise the probe path
    silently no-ops for whole classes of connections."""
    not_weakrefable = 12345  # int rejects weakref.ref
    with pytest.raises(TypeError):
        weakref.ref(not_weakrefable)
    ref = _safe_weakref(not_weakrefable)
    assert ref is not None
    assert ref() == 12345


# ── _probe_duckdb_memory handles malformed PRAGMA output ────────────────────


def test_probe_returns_none_on_malformed_pragma_row():
    """If the PRAGMA row comes back with an unparseable byte value the
    probe must return ``None`` rather than propagate ValueError out of
    the SQL hot path (the finally-block on .execute calls this)."""
    fake_con = MagicMock()
    fake_cursor = MagicMock()
    fake_con.cursor.return_value = fake_cursor
    # Row exists but the byte value is garbage that _parse_memory_mb rejects.
    fake_cursor.execute.return_value.fetchone.return_value = ("not-a-number",)
    assert _probe_duckdb_memory(fake_con) is None


def test_probe_returns_none_on_none_row():
    """fetchone() returning None (empty result set) must short-circuit
    to None, not raise IndexError."""
    fake_con = MagicMock()
    fake_cursor = MagicMock()
    fake_con.cursor.return_value = fake_cursor
    fake_cursor.execute.return_value.fetchone.return_value = None
    assert _probe_duckdb_memory(fake_con) is None


def test_probe_returns_none_on_null_byte_value():
    """duckdb_memory() may report a NULL sum (empty connection state) —
    treat as 'no number to record' instead of crashing on float(None)."""
    fake_con = MagicMock()
    fake_cursor = MagicMock()
    fake_con.cursor.return_value = fake_cursor
    fake_cursor.execute.return_value.fetchone.return_value = (None,)
    assert _probe_duckdb_memory(fake_con) is None


def test_probe_returns_none_when_cursor_raises():
    """Pre-1.0 DuckDB has no duckdb_memory() view — cursor.execute()
    raises. The probe must swallow and return None so older builds
    don't break instrumentation."""
    fake_con = MagicMock()
    fake_con.cursor.side_effect = RuntimeError("no such function: duckdb_memory")
    assert _probe_duckdb_memory(fake_con) is None
