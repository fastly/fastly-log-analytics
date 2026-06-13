"""Unit tests for the un-covered branches in ``backend.utils.telemetry``.

The bulk of this module is exercised incidentally through router tests
(the request-context middleware uses it on every request). This file
covers the small slices that don't run on the happy path —
specifically the iothread / usage_log query branch and the OTel-span
emission branches inside ``record_call`` / ``track_query``. Those
slices matter: if a refactor breaks them silently, the debug panel
loses its iothread visibility AND OTel loses the in-request span
events, neither of which has any other test pinning it.
"""

from __future__ import annotations

import time

import pytest

from backend.utils import telemetry as _t


@pytest.fixture(autouse=True)
def _isolate_telemetry_state():
    """Each test starts and ends with a clean process_context + tracked-
    calls list — these are module-level ContextVars and would otherwise
    pollute later tests that read from them (the scheduler-progress
    helpers, for instance, are randomly ordered with us and trip when
    a context leaks). Restore in ``finally`` so test failures don't
    leave stale state behind."""
    prior_ctx = _t.get_process_context()
    _t.start_call_tracking()
    try:
        yield
    finally:
        _t._set_process_context_for_tests(prior_ctx)
        _t.start_call_tracking()  # reset tracked-calls to empty


def test_query_iothread_calls_returns_empty_when_no_context():
    """The iothread path early-returns when no process context is set —
    no SQLite open, no exception. This is the most common case (a test
    or a request that hasn't passed through the context middleware
    yet)."""
    _t._set_process_context_for_tests(None)
    assert _t._query_iothread_calls_from_usage_log() == []


def test_query_iothread_calls_returns_empty_when_debug_off(monkeypatch):
    """The whole iothread query path is gated on ``DEBUG_RESPONSES`` —
    when off, ``BaseResponse`` strips ``_debug_calls`` anyway so the
    SQLite scan is pure overhead. Pin this early-return so a refactor
    can't accidentally drop the guard and add per-request DB load."""
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: False)
    assert _t._query_iothread_calls_from_usage_log() == []


def test_query_iothread_calls_returns_empty_when_no_start_ts(monkeypatch):
    """Without a request-start timestamp the helper can't bound the
    SQL window, so it returns early — defensive: the context-tagging
    middleware sets ``_REQUEST_START_TS`` per request but unit tests
    or background tasks bypass that middleware."""
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: True)
    _t._REQUEST_START_TS.set(None)
    assert _t._query_iothread_calls_from_usage_log() == []


def test_query_iothread_calls_returns_empty_for_non_api_context(monkeypatch):
    """Only contexts beginning with ``api:`` get the iothread query —
    cron/system contexts don't render debug panels so the SQL scan
    would be pure waste."""
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: True)
    _t._REQUEST_START_TS.set(time.time())
    _t._set_process_context_for_tests("cron:sync")  # not "api:"
    assert _t._query_iothread_calls_from_usage_log() == []


def test_record_call_swallows_otel_failure(monkeypatch):
    """The OTel-span emission branch inside ``record_call`` is best-effort
    — an import or recording failure must not break the caller. Force
    the import to raise and verify the call still records into the
    tracked-calls list."""
    # Capture the tracked-calls write path so we can assert it ran.
    _t.start_call_tracking()
    # Sabotage opentelemetry.trace.get_current_span by injecting a
    # broken module via sys.modules.
    import sys
    import types

    fake = types.ModuleType("opentelemetry.trace")

    def _boom(*_a, **_kw):
        raise RuntimeError("otel sabotage")

    fake.get_current_span = _boom  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", fake)
    # Must not raise even though the OTel branch explodes.
    _t.record_call(
        method="GET",
        path="/x",
        time_ms=5.0,
        service="CDN",
        status=200,
        caller="tests.unit",
    )
    # Call still landed in the tracked-calls list.
    calls = _t.get_tracked_calls()
    assert any(c.get("path") == "/x" for c in calls)


def test_is_full_miss_chain_variants():
    """Header-chain parsing — pin the four canonical states so a future
    change to the parser can't silently break cache-hit accounting."""
    assert _t._is_full_miss("HIT, HIT") is False
    assert _t._is_full_miss("MISS, HIT") is False
    assert _t._is_full_miss("MISS, MISS") is True
    assert _t._is_full_miss("PASS") is True
    assert _t._is_full_miss(None) is False
    assert _t._is_full_miss("") is False


def test_process_context_scope_pops_on_exit():
    """``process_context_scope`` must restore the prior context even on
    exception. The OTel mirror at ``_ACTIVE_CONTEXTS`` is a stack — a
    bug that fails to pop would leak the context to subsequent
    requests on the same thread."""
    _t._set_process_context_for_tests("outer")
    try:
        with _t.process_context_scope("inner"):
            assert _t.get_process_context() == "inner"
            raise ValueError("boom")
    except ValueError:
        pass
    assert _t.get_process_context() == "outer"


def test_get_process_context_with_fallback_returns_contextvar_value():
    """When ``_PROCESS_CONTEXT`` is set, the fallback helper prefers it
    over the ``_ACTIVE_CONTEXTS`` stack mirror — the ContextVar is the
    authoritative source within a single thread."""
    _t._set_process_context_for_tests("ctx-direct")
    assert _t.get_process_context_with_fallback() == "ctx-direct"


def test_query_iothread_calls_returns_shaped_rows(monkeypatch):
    """When the gates pass and the usage_log query returns rows, the
    helper must shape them into the dashboard's debug-panel format
    (CDN vs FOS service tag, fixed key set). This is the success
    branch of the iothread surface — the panel falls back to silence
    if anything in the chain raises."""
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: True)
    monkeypatch.setattr("backend.config.is_usage_logging_enabled", lambda: True)
    monkeypatch.setattr("backend.config.get_active_service_id", lambda: "svc-x")
    _t._REQUEST_START_TS.set(time.time() - 1)
    _t._set_process_context_for_tests("api:/foo")

    class _Cur:
        def fetchall(self):
            # 7-tuple: operation_type, url, status, duration_ms,
            # function_name, bytes, operation_class
            return [
                ("GET", "/object/a", 200, 12.3, "fastly.client", 1024, "B"),
                ("GET", "/object/b", 200, 4.5, "fastly.cdn", 512, "CDN"),
            ]

    class _Con:
        def execute(self, *_a, **_kw):
            return _Cur()

        def close(self):
            pass

    class _FakeDB:
        @staticmethod
        def open_readonly(_sid):
            return _Con()

    monkeypatch.setattr("backend.core.metadata.usage_log_db", _FakeDB, raising=False)
    # The function imports usage_log_db inline; patch the sys.modules
    # entry it pulls from.
    import sys

    monkeypatch.setitem(sys.modules, "backend.core.metadata.usage_log_db", _FakeDB)

    rows = _t._query_iothread_calls_from_usage_log()
    assert len(rows) == 2
    services = {r["service"] for r in rows}
    assert services == {"FOS", "CDN"}
    assert rows[0]["method"] == "GET"
    assert rows[0]["details"] == "iothread (via usage_log)"


def test_get_process_context_with_fallback_falls_back_to_stack():
    """When the ContextVar is None (fsspec iothread / pyiceberg writer
    thread that didn't inherit the parent's ContextVar), the helper
    pulls the top of the ``_ACTIVE_CONTEXTS`` stack mirror — this is
    the load-bearing path documented in cleanup_plan §10.3."""
    _t._set_process_context_for_tests(None)
    # Push something onto the mirror by entering a scope, then clear
    # the ContextVar to simulate the cross-thread inheritance gap.
    with _t.process_context_scope("ctx-via-mirror"):
        _t._PROCESS_CONTEXT.set(None)
        assert _t.get_process_context_with_fallback() == "ctx-via-mirror"
