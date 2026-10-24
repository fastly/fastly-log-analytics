"""Unit tests for backend.utils.telemetry primitives.

Focus: the process-context propagation primitives that the proxy/iceberg
_inject hooks rely on. The cross-thread fallback test pins behavior that
the ContextVar-only path silently regressed: rows landing as NULL in
usage_log when _inject ran on fsspec's iothread or pyiceberg's executor
threads (which don't inherit ContextVar state from the cron thread).
"""

from __future__ import annotations

import threading

from backend.utils.telemetry import (
    _LATEST_PROCESS_CONTEXT_LOCK,
    _set_process_context_for_tests,
    get_process_context,
    get_process_context_with_fallback,
    process_context_scope,
)


def _reset_global_fallback() -> None:
    """Tests share process state — keep them order-independent."""
    import backend.utils.telemetry as _tel

    with _LATEST_PROCESS_CONTEXT_LOCK:
        _tel._LATEST_PROCESS_CONTEXT = None
        _tel._ACTIVE_CONTEXTS.clear()


def test_set_process_context_visible_in_same_thread():
    _reset_global_fallback()
    _set_process_context_for_tests("cron_alpha")
    assert get_process_context() == "cron_alpha"
    assert get_process_context_with_fallback() == "cron_alpha"


def test_get_process_context_with_fallback_returns_last_set_in_unrelated_thread():
    """A raw threading.Thread does NOT inherit ContextVar state from the
    spawning thread. get_process_context() returns None there; the fallback
    returns the last value set process-wide.

    This is exactly the fsspecIO / pyiceberg ExecutorFactory situation that
    caused 86% of pyiceberg.s3fs telemetry rows to land with NULL
    process_context in production."""
    _reset_global_fallback()
    _set_process_context_for_tests("cron_sync_main_thread")

    captured: dict[str, object] = {}

    def worker() -> None:
        captured["ctxvar"] = get_process_context()
        captured["fallback"] = get_process_context_with_fallback()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert captured["ctxvar"] is None, (
        "ContextVar must NOT propagate to a raw thread — if this changes, the "
        "fallback is no longer load-bearing and the global mirror can be removed."
    )
    assert captured["fallback"] == "cron_sync_main_thread"


def test_fallback_reflects_most_recent_setter_across_threads():
    """Last-writer-wins: thread B's _set_process_context_for_tests overwrites the
    fallback for thread C's reader. Documents the known limitation that
    concurrent crons can misattribute (worst case) — but never NULL."""
    _reset_global_fallback()
    _set_process_context_for_tests("cron_A")

    barrier = threading.Barrier(3)
    fallback_reads: list[str | None] = []

    def setter_b() -> None:
        barrier.wait()
        _set_process_context_for_tests("cron_B")

    def reader_c() -> None:
        barrier.wait()
        # Tiny race buffer so setter_b's write is observable; the test does
        # not depend on perfect ordering, just that the fallback returns
        # *some* non-None value (proves the cross-thread mirror works).
        import time

        time.sleep(0.05)
        fallback_reads.append(get_process_context_with_fallback())

    tb = threading.Thread(target=setter_b)
    tc = threading.Thread(target=reader_c)
    tb.start()
    tc.start()
    barrier.wait()
    tb.join()
    tc.join()

    assert fallback_reads == ["cron_B"], f"expected reader to see most recent setter's value; got {fallback_reads}"


def test_fallback_returns_none_when_never_set():
    """Before any _set_process_context_for_tests call, both readers return None — the
    fallback must not invent a value out of thin air."""
    _reset_global_fallback()

    captured: dict[str, object] = {}

    def worker() -> None:
        captured["ctxvar"] = get_process_context()
        captured["fallback"] = get_process_context_with_fallback()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert captured["ctxvar"] is None
    assert captured["fallback"] is None


def test_process_context_scope_resets_contextvar_and_global_on_exit():
    """The cron_task decorator wraps every job in process_context_scope.
    On exit, both the ContextVar and the process-global mirror must be
    cleared — otherwise APScheduler's reused worker threads carry the
    stale ContextVar into the next job, and the fsspec iothread keeps
    reading the stale global. Symptom (2026-05-20): sync_ngwaf_bots
    accumulated 1,100+ misattributed pyiceberg.s3fs rows.

    Run in a fresh thread so the ContextVar starts at its default (None),
    matching how APScheduler's worker threads see things on first job."""
    _reset_global_fallback()

    captured: dict[str, object] = {}

    def worker() -> None:
        with process_context_scope("cron_sync"):
            captured["in_ctxvar"] = get_process_context()
            captured["in_fallback"] = get_process_context_with_fallback()

        captured["out_ctxvar"] = get_process_context()
        captured["out_fallback"] = get_process_context_with_fallback()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert captured["in_ctxvar"] == "cron_sync"
    assert captured["in_fallback"] == "cron_sync"
    assert captured["out_ctxvar"] is None, (
        "ContextVar must be reset on scope exit; otherwise APScheduler reuses "
        "the worker thread with stale context for the next job."
    )
    assert captured["out_fallback"] is None, (
        "Process-global mirror must be cleared on scope exit; otherwise the "
        "fsspec iothread reads stale context as fallback and mis-attributes "
        "subsequent crons' I/O."
    )


def test_process_context_scope_stack_preserves_outer_when_inner_exits():
    """The active-context stack semantics: when a quick inner cron B exits
    while a long-running outer cron A is still active, the fallback must
    revert to A — NOT None. Pre-fix (CAS-clear) telemetry showed 80% of
    pyiceberg.s3fs rows tagged 'untagged:fsspecIO' because the fsspec
    iothread read None after B's exit during A's continuation."""
    _reset_global_fallback()

    a_started = threading.Event()
    b_done = threading.Event()
    captured: dict[str, str | None] = {}

    def long_running_a() -> None:
        with process_context_scope("cron_A_long"):
            a_started.set()
            b_done.wait(timeout=2.0)
            # After B exited, the fsspec iothread (simulated by a fresh
            # thread reading the fallback) must see cron_A again.
            fallback_reads: dict[str, str | None] = {}

            def iothread_reader() -> None:
                fallback_reads["v"] = get_process_context_with_fallback()

            t = threading.Thread(target=iothread_reader)
            t.start()
            t.join()
            captured["after_b_exit"] = fallback_reads["v"]

    def quick_b() -> None:
        a_started.wait(timeout=2.0)
        with process_context_scope("cron_B_quick"):
            # While B is active, fallback reflects most recent (B).
            captured["during_b"] = _read_fallback_from_iothread()
        b_done.set()

    ta = threading.Thread(target=long_running_a)
    tb = threading.Thread(target=quick_b)
    ta.start()
    tb.start()
    ta.join()
    tb.join()

    assert captured["during_b"] == "cron_B_quick", (
        "While B's scope is active, the stack top (= B) should be the fallback value, not A."
    )
    assert captured["after_b_exit"] == "cron_A_long", (
        "After B exits, the stack pops back to A. The fsspec iothread must "
        "see A — not None (the CAS-clear regression that landed 80% of "
        "rows as untagged:fsspecIO)."
    )


def _read_fallback_from_iothread() -> str | None:
    """Helper: read the fallback from a fresh thread (no ContextVar inherit)
    so the test exercises the iothread path that motivates the global mirror."""
    holder: dict[str, str | None] = {}

    def _r() -> None:
        holder["v"] = get_process_context_with_fallback()

    t = threading.Thread(target=_r)
    t.start()
    t.join()
    return holder["v"]


def test_process_context_scope_handles_concurrent_setter_outside_scope():
    """If something calls _set_process_context_for_tests() OUTSIDE the scope (legacy
    code path or test helper), the scope's exit pops its own name from
    the stack and falls back to the prior stack top — not to the value
    the rogue setter wrote. The rogue value only sticks if the stack is
    otherwise empty."""
    _reset_global_fallback()

    captured: dict[str, str | None] = {}

    def worker() -> None:
        with process_context_scope("cron_A"):
            # Legacy code path that doesn't use the scope.
            _set_process_context_for_tests("rogue_value")
            captured["during"] = _read_fallback_from_iothread()

        captured["after"] = _read_fallback_from_iothread()

    t = threading.Thread(target=worker)
    t.start()
    t.join()

    assert captured["during"] == "rogue_value"
    # After scope exit, A is popped → stack empty → mirror = None.
    # (The rogue setter overwrote the mirror but didn't touch the stack.)
    assert captured["after"] is None, (
        "Scope exit should restore from the stack, not preserve whatever a rogue setter happened to write last."
    )


def test_query_iothread_calls_does_not_synchronously_flush_proxy(monkeypatch):
    """Regression for M5 (item 24): the request-path
    `_query_iothread_calls_from_usage_log` MUST NOT block on
    `telemetry_proxy._flush_log_writes_for_tests`. Under cron contention
    that wait routinely hit its 250 ms ceiling and stacked across
    every admin nav request, dragging total wait to 5 s+ during a
    cron sync tick. The fix accepts up to one batch interval (~100 ms)
    of visibility lag in the debug panel as the trade-off.

    Test asserts the flusher is never called from this function. If a
    future refactor reintroduces a synchronous wait, this fails."""
    _reset_global_fallback()
    _set_process_context_for_tests("api:GET /api/test")

    from backend import config as svcconfig
    from backend.models import common as common_models
    from backend.utils import telemetry, telemetry_proxy

    flush_calls: list[float] = []

    def _track_flush(timeout: float = 2.0) -> None:
        flush_calls.append(timeout)

    monkeypatch.setattr(telemetry_proxy, "_flush_log_writes_for_tests", _track_flush)
    monkeypatch.setattr(common_models, "_debug_responses_enabled", lambda: True)
    monkeypatch.setattr(svcconfig, "is_usage_logging_enabled", lambda: True)
    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "svc1")
    telemetry._REQUEST_START_TS.set(0.0)

    # Make get_con raise so we don't actually touch SQLite — we only
    # care that the proxy flusher isn't called before that.
    from backend.core import metadata as metadata_db

    def _boom(*_args, **_kwargs):
        raise RuntimeError("intentionally unreachable in test")

    monkeypatch.setattr(metadata_db, "get_con", _boom)

    result = telemetry._query_iothread_calls_from_usage_log()
    assert result == [], "Function should swallow get_con errors and return empty"
    assert flush_calls == [], (
        f"_query_iothread_calls_from_usage_log must NOT block on the proxy "
        f"flusher (M5 regression). Got flush calls with timeouts: {flush_calls}"
    )


# ── Merged from test_telemetry_unit.py ──────────────────────────────────────

import time

import pytest

from backend.utils import telemetry as _t


@pytest.fixture(autouse=True)
def _isolate_telemetry_state():
    """Each test starts and ends with a clean process_context + tracked-calls list."""
    prior_ctx = _t.get_process_context()
    _t.start_call_tracking()
    try:
        yield
    finally:
        _t._set_process_context_for_tests(prior_ctx)
        _t.start_call_tracking()


def test_query_iothread_calls_returns_empty_when_no_context():
    _t._set_process_context_for_tests(None)
    assert _t._query_iothread_calls_from_usage_log() == []


def test_query_iothread_calls_returns_empty_when_debug_off(monkeypatch):
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: False)
    assert _t._query_iothread_calls_from_usage_log() == []


def test_query_iothread_calls_returns_empty_when_no_start_ts(monkeypatch):
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: True)
    _t._REQUEST_START_TS.set(None)
    assert _t._query_iothread_calls_from_usage_log() == []


def test_query_iothread_calls_returns_empty_for_non_api_context(monkeypatch):
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: True)
    _t._REQUEST_START_TS.set(time.time())
    _t._set_process_context_for_tests("cron:sync")
    assert _t._query_iothread_calls_from_usage_log() == []


def test_record_call_swallows_otel_failure(monkeypatch):
    _t.start_call_tracking()
    import sys
    import types

    fake = types.ModuleType("opentelemetry.trace")

    def _boom(*_a, **_kw):
        raise RuntimeError("otel sabotage")

    fake.get_current_span = _boom  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", fake)
    _t.record_call(
        method="GET",
        path="/x",
        time_ms=5.0,
        service="CDN",
        status=200,
        caller="tests.unit",
    )
    calls = _t.get_tracked_calls()
    assert any(c.get("path") == "/x" for c in calls)


def test_is_full_miss_chain_variants():
    assert _t.is_full_miss("HIT, HIT") is False
    assert _t.is_full_miss("MISS, HIT") is False
    assert _t.is_full_miss("MISS, MISS") is True
    assert _t.is_full_miss("PASS") is True
    assert _t.is_full_miss(None) is False
    assert _t.is_full_miss("") is False


def test_process_context_scope_pops_on_exit():
    _t._set_process_context_for_tests("outer")
    try:
        with _t.process_context_scope("inner"):
            assert _t.get_process_context() == "inner"
            raise ValueError("boom")
    except ValueError:
        pass
    assert _t.get_process_context() == "outer"


def test_get_process_context_with_fallback_returns_contextvar_value():
    _t._set_process_context_for_tests("ctx-direct")
    assert _t.get_process_context_with_fallback() == "ctx-direct"


def test_query_iothread_calls_returns_shaped_rows(monkeypatch):
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: True)
    monkeypatch.setattr("backend.config.is_usage_logging_enabled", lambda: True)
    monkeypatch.setattr("backend.config.get_active_service_id", lambda: "svc-x")
    _t._REQUEST_START_TS.set(time.time() - 1)
    _t._set_process_context_for_tests("api:/foo")

    class _Cur:
        def fetchall(self):
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
    import sys

    monkeypatch.setitem(sys.modules, "backend.core.metadata.usage_log_db", _FakeDB)

    rows = _t._query_iothread_calls_from_usage_log()
    assert len(rows) == 2
    services = {r["service"] for r in rows}
    assert services == {"FOS", "CDN"}
    assert rows[0]["method"] == "GET"
    assert rows[0]["details"] == "iothread (via usage_log)"


def test_get_process_context_with_fallback_falls_back_to_stack():
    _t._set_process_context_for_tests(None)
    with _t.process_context_scope("ctx-via-mirror"):
        _t._PROCESS_CONTEXT.set(None)
        assert _t.get_process_context_with_fallback() == "ctx-via-mirror"


# ── debug_envelope ───────────────────────────────────────────────────────────


def test_ungated_always_injects_envelope_in_place(monkeypatch):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
    res = {"ok": True}
    out = _t.debug_envelope(res, gated=False)
    assert out is res
    assert set(res) == {"ok", "_debug_queries", "_debug_sqlite", "_debug_calls", "_is_cached"}
    assert res["_is_cached"] is False


def test_gated_strips_debug_keys_when_debug_responses_off(monkeypatch):
    monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
    src = {"a": 1, "_debug_queries": [1], "_debug_calls": [2], "_debug_sqlite": [3]}
    out = _t.debug_envelope(src, gated=True, is_cached=True)
    assert out == {"a": 1, "_debug_sqlite": [3], "_is_cached": True}
    assert "_is_cached" not in src


def test_gated_keeps_debug_keys_when_debug_responses_on(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    out = _t.debug_envelope({"a": 1, "_debug_queries": [1], "_debug_calls": [2]}, gated=True)
    assert out == {"a": 1, "_debug_queries": [1], "_debug_calls": [2], "_is_cached": False}


def test_gated_never_injects_collectors(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    assert _t.debug_envelope({"a": 1}, gated=True) == {"a": 1, "_is_cached": False}
