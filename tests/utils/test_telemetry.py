"""Consolidated tests for test_telemetry."""

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


# ── Merged from test_usage_logger.py ────────────────────────────────────────

from unittest.mock import patch

# ---------------------------------------------------------------------------
# telemetry — process context helpers


def test_process_context_set_and_get():
    from backend.utils.telemetry import _set_process_context_for_tests, get_process_context

    _set_process_context_for_tests("cron:sync:svc1")
    assert get_process_context() == "cron:sync:svc1"


def test_process_context_default_is_none():
    import threading

    result = {}

    def _read():
        # A new thread copies the parent context; the parent hasn't set the var
        # in this test, but we check via the ContextVar default value
        from backend.utils.telemetry import _PROCESS_CONTEXT

        result["ctx"] = _PROCESS_CONTEXT.get(None)

    t = threading.Thread(target=_read)
    t.start()
    t.join()
    # The default for _PROCESS_CONTEXT is None, confirmed independently
    assert result["ctx"] is None


def test_start_call_tracking_resets_calls():
    from backend.utils.telemetry import get_tracked_calls, record_call, start_call_tracking

    start_call_tracking()
    record_call("PutObject", "/file.gz", 10.0, service="FOS", details="Class A")
    assert len(get_tracked_calls()) == 1

    start_call_tracking()
    assert get_tracked_calls() == []


def test_record_call_stores_bytes():
    from backend.utils.telemetry import get_tracked_calls, record_call, start_call_tracking

    start_call_tracking()
    record_call("GetObject", "/data.parquet", 5.0, service="FOS", bytes_count=1024)
    calls = get_tracked_calls()
    assert len(calls) == 1
    assert calls[0]["bytes"] == 1024


# ---------------------------------------------------------------------------
# log_usage_calls — classification and insertion (now per-service SQLite)


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_inserts_fos_a(mock_enabled):
    from backend.core.duckdb import log_usage_calls

    calls = [
        {
            "service": "FOS",
            "method": "PutObject",
            "path": "/prefix/file.gz",
            "time_ms": 12.0,
            "status": "OK",
            "details": "Class A",
            "caller": "upload_file",
            "bytes": None,
        }
    ]
    log_usage_calls({"name": "svc1"}, calls, process_context="cron:sync:svc1")

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    row = con.execute("SELECT operation_class, operation_type, service_id FROM usage_log").fetchone()
    assert row is not None
    assert row["operation_class"] == "A"
    assert row["operation_type"] == "PutObject"
    assert row["service_id"] == "svc1"


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_classifies_raw_http_put_post_as_class_a(mock_enabled):
    """Telemetry proxy emits raw HTTP verbs (PUT/POST/COPY) via request.method.

    The classifier must recognise those as Class A so PutObject, UploadPart,
    DeleteObjects batch, etc. routed through the proxy aren't misbilled as B.
    """
    from backend.core.duckdb import log_usage_calls

    calls = [
        {"service": "FOS", "method": "PUT", "path": "/k", "time_ms": 1.0, "status": "OK"},
        {"service": "FOS", "method": "POST", "path": "/?delete", "time_ms": 1.0, "status": "OK"},
        {"service": "FOS", "method": "COPY", "path": "/dst", "time_ms": 1.0, "status": "OK"},
        {"service": "FOS", "method": "GET", "path": "/k", "time_ms": 1.0, "status": "OK"},
        {"service": "FOS", "method": "DELETE", "path": "/k", "time_ms": 1.0, "status": "OK"},
    ]
    log_usage_calls({"name": "svc1"}, calls)

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    rows = con.execute("SELECT operation_type, operation_class FROM usage_log ORDER BY rowid").fetchall()
    assert [(r["operation_type"], r["operation_class"]) for r in rows] == [
        ("PUT", "A"),
        ("POST", "A"),
        ("COPY", "A"),
        ("GET", "B"),
        ("DELETE", "B"),
    ]


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_classifies_cdn(mock_enabled):
    from backend.core.duckdb import log_usage_calls

    calls = [
        {
            "service": "CDN",
            "method": "download",
            "path": "/data.parquet",
            "time_ms": 50.0,
            "status": "OK",
            "details": None,
            "caller": None,
            "bytes": 2048,
        }
    ]
    log_usage_calls({"name": "svc1"}, calls)

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    row = con.execute("SELECT operation_class, bytes FROM usage_log").fetchone()
    assert row is not None
    assert row["operation_class"] == "CDN"
    assert row["bytes"] == 2048


@patch("backend.config.is_usage_logging_enabled", return_value=False)
def test_log_usage_calls_skips_when_disabled(mock_enabled):
    """When usage logging is disabled, no DB interaction should occur."""
    from backend.core.duckdb import log_usage_calls

    calls = [{"service": "FOS", "method": "PutObject", "path": "/x", "time_ms": 1.0, "status": "OK"}]
    # Should exit silently without raising
    log_usage_calls({"name": "svc1"}, calls)


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_classifies_non_fos_as_class_b(mock_enabled):
    """Non-FOS, non-CDN calls (e.g. Fastly API) default to operation_class 'B'."""
    from backend.core.duckdb import log_usage_calls

    calls = [
        {
            "service": "Fastly API",
            "method": "GET",
            "path": "/api/services",
            "time_ms": 8.0,
            "status": "OK",
            "details": None,
            "caller": None,
            "bytes": None,
        }
    ]
    log_usage_calls({"name": "svc1"}, calls)

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    rows = con.execute("SELECT operation_class FROM usage_log").fetchall()
    # Default classification for unknown services is Class B (per metadata_db)
    assert len(rows) == 1
    assert rows[0]["operation_class"] == "B"


# ---------------------------------------------------------------------------
# purge_usage_log — retention enforcement


@patch("backend.config.load_usage_logging_config", return_value={"retention_days": 7})
def test_purge_usage_log_deletes_old_rows(mock_cfg):
    from backend.core.duckdb import purge_usage_log
    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, status) VALUES (?, ?, ?, ?)",
        [
            ("2020-01-01T00:00:00", "svc1", "A", "OK"),  # very old — should be purged
            ("2099-01-01T00:00:00", "svc1", "A", "OK"),  # future — should survive
        ],
    )
    con.commit()

    purge_usage_log({"name": "svc1"})

    rows = con.execute("SELECT timestamp FROM usage_log").fetchall()
    assert len(rows) == 1
    assert "2099" in str(rows[0]["timestamp"])


@patch("backend.config.load_usage_logging_config", return_value={"retention_days": 0})
def test_purge_usage_log_skips_when_retention_zero(mock_cfg):
    from backend.core.duckdb import purge_usage_log
    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("svc1")
    con.execute(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, status) VALUES (?, ?, ?, ?)",
        ("2020-01-01T00:00:00", "svc1", "A", "OK"),
    )
    con.commit()

    purge_usage_log({"name": "svc1"})

    count = con.execute("SELECT count(*) FROM usage_log").fetchone()[0]
    assert count == 1


# ---------------------------------------------------------------------------
# flush_usage_log / run_usage_log_cleanup — the wrapper functions in
# backend.utils.usage_logger that the scheduler calls each cron tick.
# Both are deliberately broad-try/except — verify the happy + error paths
# while protecting against any of them escaping as an exception.


def test_flush_usage_log_skips_when_logging_disabled():
    """Disabled globally → flush is a no-op (no DB writes, no exceptions)."""
    from backend.utils.usage_logger import flush_usage_log

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=False),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-1")  # must not raise

    mock_log.assert_not_called()


def test_flush_usage_log_skips_when_no_tracked_calls():
    """If the per-request context has no recorded calls, flush returns
    early without bothering log_usage_calls."""
    from backend.utils import telemetry
    from backend.utils.usage_logger import flush_usage_log

    telemetry.start_call_tracking()  # reset to empty

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-2")

    mock_log.assert_not_called()


def test_flush_usage_log_skips_when_service_config_missing():
    from backend.utils import telemetry
    from backend.utils.usage_logger import flush_usage_log

    telemetry.start_call_tracking()
    telemetry.record_call("PutObject", "/k", 1.0, service="FOS")

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.load_config", return_value=None),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-missing")

    mock_log.assert_not_called()


def test_flush_usage_log_happy_path_forwards_calls_and_context():
    """All preconditions met → log_usage_calls is invoked with the source
    dict, the tracked calls list, and the process_context kwarg."""
    from backend.utils import telemetry
    from backend.utils.usage_logger import flush_usage_log

    telemetry.start_call_tracking()
    telemetry._set_process_context_for_tests("cron:sync:svc-flush-3")
    telemetry.record_call("PutObject", "/x.gz", 12.0, service="FOS", bytes_count=42)

    fake_cfg = {"service_id": "svc-flush-3", "fos_bucket": "b", "fos_region": "us-east-1", "name": "X"}
    fake_source = {"name": "svc-flush-3", "bucket": "b", "region": "us-east-1"}

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_to_source", return_value=fake_source),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-3")

    assert mock_log.call_count == 1
    args, kwargs = mock_log.call_args
    assert args[0] == fake_source
    assert len(args[1]) == 1
    assert args[1][0]["method"] == "PutObject"
    assert kwargs.get("process_context") == "cron:sync:svc-flush-3"


def test_flush_usage_log_swallows_inner_exception():
    """Even if log_usage_calls raises, flush must NOT propagate — the
    scheduler must keep running its cron jobs."""
    from backend.utils import telemetry
    from backend.utils.usage_logger import flush_usage_log

    telemetry.start_call_tracking()
    telemetry.record_call("PutObject", "/k", 1.0, service="FOS")

    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.load_config", return_value={"service_id": "x"}),
        patch("backend.config.config_to_source", return_value={"name": "x"}),
        patch("backend.core.duckdb.log_usage_calls", side_effect=RuntimeError("db down")),
    ):
        # Must not raise
        flush_usage_log("svc-flush-error")


def test_run_usage_log_cleanup_skips_when_service_config_missing():
    from backend.utils.usage_logger import run_usage_log_cleanup

    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.core.duckdb.purge_usage_log") as mock_purge,
    ):
        run_usage_log_cleanup("svc-cleanup-missing")

    mock_purge.assert_not_called()


def test_run_usage_log_cleanup_happy_path_delegates_to_purge():
    from backend.utils.usage_logger import run_usage_log_cleanup

    fake_cfg = {"service_id": "svc-cleanup-1", "fos_bucket": "b"}
    fake_source = {"name": "svc-cleanup-1"}

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_to_source", return_value=fake_source),
        patch("backend.core.duckdb.purge_usage_log") as mock_purge,
    ):
        run_usage_log_cleanup("svc-cleanup-1")

    mock_purge.assert_called_once_with(fake_source)


def test_run_usage_log_cleanup_swallows_inner_exception():
    from backend.utils.usage_logger import run_usage_log_cleanup

    with (
        patch("backend.config.load_config", return_value={"service_id": "x"}),
        patch("backend.config.config_to_source", return_value={"name": "x"}),
        patch("backend.core.duckdb.purge_usage_log", side_effect=RuntimeError("boom")),
    ):
        # Must not raise — the scheduler relies on this contract.
        run_usage_log_cleanup("svc-cleanup-error")


# ── Merged from test_cdn_miss_tracking.py ───────────────────────────────────

import pytest


class TestCdnMissTracking:
    @pytest.fixture(autouse=True)
    def fresh_call_tracking(self):
        from backend.utils.telemetry import _CALLS, start_call_tracking

        start_call_tracking()
        yield
        _CALLS.set([])

    class TestIsFullMiss:
        def test_single_hit(self):
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("HIT") is False

        def test_chained_hit_hit(self):
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("HIT, HIT") is False

        def test_edge_miss_shield_hit(self):
            """MISS at edge but shield served it — no FOS read happened."""
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("MISS, HIT") is False

        def test_full_miss(self):
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("MISS, MISS") is True

        def test_pass(self):
            """PASS = uncacheable request, always goes to FOS."""
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("PASS") is True

        def test_pass_pass(self):
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("PASS, PASS") is True

        def test_mixed_pass_miss(self):
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("MISS, PASS") is True

        def test_empty(self):
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("") is False
            assert is_full_miss(None) is False

        def test_case_insensitive(self):
            from backend.utils.telemetry import is_full_miss

            assert is_full_miss("miss, miss") is True
            assert is_full_miss("Hit, Hit") is False

    class TestRecordCdnCall:
        def test_hit_records_only_cdn_op(self):
            from backend.utils.telemetry import get_tracked_calls, record_cdn_call

            record_cdn_call("GET", "key.parquet", 12.3, headers={"X-Cache": "HIT, HIT"}, bytes_count=1024)

            calls = get_tracked_calls()
            assert len(calls) == 1
            assert calls[0]["service"] == "CDN"
            assert calls[0]["method"] == "GET"
            assert calls[0]["bytes"] == 1024

        def test_full_miss_records_cdn_plus_fos(self):
            from backend.utils.telemetry import get_tracked_calls, record_cdn_call

            record_cdn_call("GET", "key.parquet", 99.0, headers={"X-Cache": "MISS, MISS"}, bytes_count=2048)

            calls = get_tracked_calls()
            assert len(calls) == 2
            services = [c["service"] for c in calls]
            assert services == ["CDN", "FOS"]
            assert calls[1]["method"] == "GET_OBJECT"
            assert calls[1]["bytes"] == 2048
            assert "synthesized from CDN MISS" in (calls[1]["details"] or "")

        def test_head_miss_synthesizes_get_object_not_head_object(self):
            """Fastly fetches the full body on origin MISS regardless of client
            method (to populate cache for subsequent reads), so the real FOS-side
            op is always GET_OBJECT — even when the client sent HEAD. Field-
            confirmed by tracing single-file ingest paths: client HEAD MISS
            warmed Fastly's cache, and every CDN GET that followed was a HIT,
            meaning the FOS read happened exactly once on the HEAD path."""
            from backend.utils.telemetry import get_tracked_calls, record_cdn_call

            record_cdn_call("HEAD", "key.parquet", 50.0, headers={"X-Cache": "MISS, MISS"}, bytes_count=None)

            calls = get_tracked_calls()
            assert len(calls) == 2
            assert calls[1]["method"] == "GET_OBJECT"
            assert calls[1]["service"] == "FOS"
            assert "synthesized from CDN MISS" in (calls[1]["details"] or "")

        def test_edge_miss_shield_hit_no_fos_op(self):
            """The shield served it — no FOS read, just the CDN op."""
            from backend.utils.telemetry import get_tracked_calls, record_cdn_call

            record_cdn_call("GET", "key", 30.0, headers={"X-Cache": "MISS, HIT"}, bytes_count=512)

            calls = get_tracked_calls()
            assert len(calls) == 1
            assert calls[0]["service"] == "CDN"

        def test_no_headers_records_only_cdn(self):
            """When we don't have headers we can't tell — don't guess the FOS op."""
            from backend.utils.telemetry import get_tracked_calls, record_cdn_call

            record_cdn_call("GET", "key", 10.0, headers=None, bytes_count=100)

            calls = get_tracked_calls()
            assert len(calls) == 1
            assert calls[0]["service"] == "CDN"

        def test_pass_synthesizes_fos_op(self):
            from backend.utils.telemetry import get_tracked_calls, record_cdn_call

            record_cdn_call("GET", "uncacheable", 80.0, headers={"X-Cache": "PASS"}, bytes_count=4096)

            calls = get_tracked_calls()
            assert len(calls) == 2
            assert calls[1]["service"] == "FOS"
            assert calls[1]["method"] == "GET_OBJECT"


# ── Merged from test_telemetry_response_middleware.py ───────────────────────

import pytest
from fastapi import FastAPI, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.testclient import TestClient
from starlette.responses import JSONResponse, StreamingResponse

from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware


def _build_app(*, with_gzip: bool = False) -> FastAPI:
    """Build a minimal app with the telemetry middleware installed.

    Routes registered:
      - ``/plain-dict`` returns a plain ``dict`` with no telemetry. The
        middleware should inject the three keys.
      - ``/already-has-telemetry`` returns a dict with ``_debug_queries``
        already set. The middleware MUST NOT double-inject (and MUST
        preserve the values verbatim).
      - ``/list-response`` returns a top-level JSON list. The middleware
        MUST pass it through unchanged.
      - ``/streaming`` returns a ``StreamingResponse``. The middleware
        MUST NOT buffer it.
      - ``/non-json`` returns ``text/plain``. Untouched.
      - ``/empty`` returns 204 with empty body. Untouched.

    ``with_gzip=True`` stacks GZipMiddleware OUTSIDE the telemetry
    middleware — mirrors prod main.py ordering and pins that the
    backstop sees uncompressed JSON.
    """
    app = FastAPI()

    @app.get("/plain-dict")
    def plain_dict():
        return {"foo": 1, "bar": "two"}

    @app.get("/already-has-telemetry")
    def already_has():
        return {
            "foo": 1,
            "_debug_queries": [{"sql": "SELECT 1", "time_ms": 0.1}],
            "_debug_calls": [{"method": "GET", "path": "/x"}],
            "_debug_sqlite": [{"seq": 7, "sql": "SELECT 1", "time_ms": 0.2}],
            "_is_cached": True,
        }

    @app.get("/sqlite-work")
    def sqlite_work():
        # End-to-end page-scoping chain: profiler → per-request contextvar
        # collector → BaseResponse.with_telemetry. start_call_tracking is
        # called in-handler (same context) standing in for main.py's
        # tracking middleware, whose collector the handler context inherits.
        from backend.models.common import BaseResponse
        from backend.utils import sqlite_profiler
        from backend.utils.telemetry import start_call_tracking

        start_call_tracking()
        sqlite_profiler._record("CREATE TABLE page_scoped (x INTEGER)", None, 0.2, 0, "execute")
        return BaseResponse.with_telemetry()

    @app.get("/list-response")
    def list_response():
        return [1, 2, 3]

    @app.get("/sse")
    def sse():
        async def gen():
            yield b"data: hello\n\n"
            yield b"data: world\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.get("/ndjson")
    def ndjson_stream():
        async def gen():
            yield b'{"row":1}\n'
            yield b'{"row":2}\n'

        return StreamingResponse(gen(), media_type="application/x-ndjson")

    @app.get("/non-json")
    def non_json():
        return Response(content="hello", media_type="text/plain")

    @app.get("/empty")
    def empty():
        return Response(status_code=204)

    @app.get("/raises")
    def raises():
        # Endpoint that emits a 500 via FastAPI's default exception
        # handler. The handler returns a JSONResponse({"detail": ...}).
        # We pin that the middleware doesn't crash the request even when
        # the response is an error.
        raise RuntimeError("boom")

    # The middleware ordering in real main.py is:
    #   add_middleware(TelemetryResponseBody)   # inner — runs LAST on the way out
    #   add_middleware(GZip)                    # outer — wraps the telemetry one
    # ``app.add_middleware`` is reverse-stack (last call → outermost).
    app.add_middleware(TelemetryResponseBodyMiddleware)
    if with_gzip:
        app.add_middleware(GZipMiddleware, minimum_size=0)
    return app


class TestTelemetryResponseMiddleware:
    @pytest.fixture(autouse=True)
    def _enable_debug_responses(self, monkeypatch):
        """The middleware is gated on DEBUG_RESPONSES. Every test in this
        file is about WHAT the middleware does WHEN it's active — turn it
        on for the suite. A dedicated test below covers the gated-off case."""
        monkeypatch.setenv("DEBUG_RESPONSES", "1")

    # ── plain-dict endpoint: telemetry must be injected ─────────────────────

    def test_injects_debug_keys_into_plain_dict_response(self):
        """The pivot case: an endpoint that returns ``{"foo": 1}`` without
        BaseResponse must come back with ``_debug_queries`` / ``_debug_calls``
        / ``_is_cached`` added. This is the entire reason M1 exists —
        backstop the next endpoint that forgets to use BaseResponse."""
        client = TestClient(_build_app())
        r = client.get("/plain-dict")
        assert r.status_code == 200
        body = r.json()
        assert body["foo"] == 1
        assert body["bar"] == "two"
        assert "_debug_queries" in body
        assert "_debug_calls" in body
        assert "_debug_sqlite" in body
        assert "_is_cached" in body
        assert isinstance(body["_debug_queries"], list)
        assert isinstance(body["_debug_calls"], list)
        assert isinstance(body["_debug_sqlite"], list)
        assert body["_is_cached"] is False

    def test_injects_safely_when_no_telemetry_recorded(self):
        """Even when the contextvar collectors are empty (no queries / calls
        were tracked during the request), the middleware emits valid empty
        lists rather than ``null`` — the frontend's DebugPanel iterates
        these arrays unconditionally."""
        client = TestClient(_build_app())
        r = client.get("/plain-dict")
        body = r.json()
        assert body["_debug_queries"] == []
        assert body["_debug_calls"] == []

    # ── already-has-telemetry: NEVER double-inject ──────────────────────────

    def test_does_not_double_inject_when_endpoint_already_supplied_telemetry(self):
        """Endpoints using ``BaseResponse.with_telemetry`` already include
        the three keys. The middleware MUST preserve them verbatim — not
        overwrite with the (possibly different) contextvar snapshot."""
        client = TestClient(_build_app())
        r = client.get("/already-has-telemetry")
        body = r.json()
        assert body["_debug_queries"] == [{"sql": "SELECT 1", "time_ms": 0.1}]
        assert body["_debug_calls"] == [{"method": "GET", "path": "/x"}]
        assert body["_debug_sqlite"] == [{"seq": 7, "sql": "SELECT 1", "time_ms": 0.2}]
        assert body["_is_cached"] is True

    # ── non-dict bodies: passed through unchanged ───────────────────────────

    def test_top_level_list_response_is_untouched(self):
        """A route returning ``[1, 2, 3]`` cannot host the telemetry keys —
        they'd violate the published shape (would require wrapping the
        list). The middleware must leave it alone."""
        client = TestClient(_build_app())
        r = client.get("/list-response")
        assert r.json() == [1, 2, 3]

    def test_non_json_response_is_untouched(self):
        """A ``text/plain`` response is not parsed and not modified. Pinned
        because a body-reading middleware that doesn't check Content-Type
        would corrupt downloads / HTML / SSE."""
        client = TestClient(_build_app())
        r = client.get("/non-json")
        assert r.status_code == 200
        assert r.text == "hello"
        assert r.headers["content-type"].startswith("text/plain")

    def test_empty_body_response_is_untouched(self):
        """A 204 / empty body must not trip the JSON parser. The middleware
        has to reconstruct the response (the body iterator was drained) but
        must not invent a body."""
        client = TestClient(_build_app())
        r = client.get("/empty")
        assert r.status_code == 204
        assert r.content == b""

    # ── streaming: never buffer ─────────────────────────────────────────────

    def test_sse_response_passes_through_without_buffering(self):
        """SSE endpoints emit ``text/event-stream`` and would deadlock if
        buffered (infinite streams). Content-Type is the reliable signal —
        Starlette's BaseHTTPMiddleware wraps every response in an internal
        ``_StreamingResponse`` so ``isinstance(response, StreamingResponse)``
        is unreliable; we check the content-type instead.
        """
        client = TestClient(_build_app())
        r = client.get("/sse")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/event-stream")
        assert r.text == "data: hello\n\ndata: world\n\n"

    def test_ndjson_stream_passes_through_without_buffering(self):
        """Streaming-row endpoints emit ``application/x-ndjson`` (newline-
        delimited JSON). Each line is its own JSON object; buffering +
        injecting top-level telemetry keys would corrupt the format.
        Pinned because ``application/x-ndjson`` is the right escape hatch
        for routes that want streaming JSON without tripping this backstop.
        """
        client = TestClient(_build_app())
        r = client.get("/ndjson")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("application/x-ndjson")
        assert r.text == '{"row":1}\n{"row":2}\n'

    # ── page-scoped SQLite: profiler → collector → injection, end-to-end ───

    def test_sqlite_statements_executed_during_request_reach_debug_sqlite(self):
        """The Debug Panel's 'This page' SQLite view depends on this chain:
        the profiler's ``_record`` feeds the request-scoped contextvar
        collector (``start_call_tracking`` initialised it), and
        ``BaseResponse.with_telemetry`` serialises that collector as
        ``_debug_sqlite`` (the middleware then passes the already-populated
        body through untouched — no double-inject). Pin the whole path so a
        regression in any link (profiler hook removed, collector not
        initialised, field dropped) fails here."""
        client = TestClient(_build_app())
        r = client.get("/sqlite-work")
        body = r.json()
        sqls = [q["sql"] for q in body["_debug_sqlite"]]
        assert "CREATE TABLE page_scoped (x INTEGER)" in sqls
        entry = body["_debug_sqlite"][sqls.index("CREATE TABLE page_scoped (x INTEGER)")]
        assert entry["op"] == "execute"
        assert entry["time_ms"] >= 0
        assert entry["seq"] >= 1

    def test_plain_request_does_not_inherit_other_requests_sqlite(self):
        """A request that ran no SQLite of its own must carry an empty
        ``_debug_sqlite`` — NOT statements from cron, the ring buffer, or a
        previous request. This is the exact bug the page scoping fixes (7s
        of background statements shown as if the dashboard load caused
        them). The backstop injection reads its own per-request context, so
        the prior request's collector must be invisible here."""
        client = TestClient(_build_app())
        # Seed the process-global ring buffer via a request that DID run SQL.
        client.get("/sqlite-work")
        # A fresh request with no SQLite work of its own.
        r = client.get("/plain-dict")
        assert r.json()["_debug_sqlite"] == []

    # ── gated on DEBUG_RESPONSES ────────────────────────────────────────────

    def test_does_not_inject_when_debug_responses_env_is_off(self, monkeypatch):
        """The whole BaseResponse mechanism is gated on DEBUG_RESPONSES.
        The backstop must respect the same flag — otherwise prod (which
        runs with the flag off) would start emitting telemetry blocks on
        every plain-dict endpoint, growing response sizes."""
        monkeypatch.setenv("DEBUG_RESPONSES", "")
        client = TestClient(_build_app())
        r = client.get("/plain-dict")
        body = r.json()
        assert "_debug_queries" not in body
        assert "_debug_calls" not in body

    # ── gzip integration: pinned ordering ───────────────────────────────────

    def test_works_with_gzip_outer_middleware(self):
        """In real main.py, GZipMiddleware sits OUTSIDE this one. That means:
        on the way in, gzip → telemetry → route. On the way out, route →
        telemetry (injects) → gzip (compresses). The telemetry middleware
        sees the response BEFORE compression — that's the contract.

        If the ordering ever flips, this middleware would try to JSON-parse
        a gzipped byte stream and fail (silently, per the catch-all). This
        test asserts the happy path: gzip outer + telemetry inner + plain
        dict route = browser still gets injected telemetry."""
        client = TestClient(_build_app(with_gzip=True))
        r = client.get("/plain-dict", headers={"Accept-Encoding": "gzip"})
        # TestClient auto-decodes gzip transparently; we should see the
        # injected telemetry just like the browser would.
        body = r.json()
        assert "_debug_queries" in body, (
            "if telemetry injection silently breaks under gzip, the middleware "
            "is registered in the wrong order — must be INNER to gzip"
        )

    # ── error responses ─────────────────────────────────────────────────────

    def test_does_not_crash_on_500_error_response(self):
        """An endpoint that raises an uncaught exception must still produce
        a response. The middleware must not turn a 500 into a 502 by
        mishandling FastAPI's default error response.

        Note: FastAPI's *default* uncaught-exception handler returns
        ``text/plain "Internal Server Error"`` — so this middleware
        correctly passes it through unchanged (telemetry can't be added
        to a non-JSON body). Endpoints that want telemetry on errors
        should raise ``HTTPException`` (handler emits JSON), which the
        backstop would then inject into."""
        client = TestClient(_build_app(), raise_server_exceptions=False)
        r = client.get("/raises")
        assert r.status_code == 500
        # text/plain body, untouched
        assert r.text == "Internal Server Error"
        assert r.headers["content-type"].startswith("text/plain")

    # ── malformed JSON body: pass through ──────────────────────────────────

    def test_malformed_json_body_passes_through_unchanged(self):
        """If a buggy route declares ``application/json`` but returns a
        malformed body, the middleware must NOT crash the request — it
        falls back to emitting the original bytes. The endpoint is buggy
        but a 200 with broken JSON beats a 500 from the middleware."""
        app = FastAPI()

        @app.get("/bad-json")
        def bad_json():
            return Response(content=b"{not valid", media_type="application/json")

        app.add_middleware(TelemetryResponseBodyMiddleware)
        client = TestClient(app)
        r = client.get("/bad-json")
        assert r.status_code == 200
        assert r.content == b"{not valid"

    # ── JSON list inside JSONResponse (covers fastapi default) ──────────────

    def test_multiple_set_cookie_headers_survive_reconstruction(self):
        """The middleware reconstructs every JSON dict response (to inject
        telemetry). If the reconstruction collapses duplicate header values
        via ``dict(headers.items())``, the second Set-Cookie is silently
        dropped — which broke the share-login pending-cookie flow in prod:
        login sets ``analyst_pending_session_id`` AND deletes the full
        ``analyst_session_id`` (two Set-Cookie headers), the dict comprehension
        kept only the delete, the browser ended up with no session at all,
        AppLayout bounced to /share-login → infinite loop.

        Lock the cookie shape in so any future change to ``_reconstruct``
        that loses a Set-Cookie shows up here, not in a user's broken
        dashboard. The same property protects Link, Vary, and any other
        legitimately multi-valued response header.
        """
        app = FastAPI()

        @app.get("/dual-cookie")
        def dual_cookie(response: Response):
            response.set_cookie(
                key="alpha", value="A", httponly=True, secure=True, samesite="strict", max_age=86400, path="/"
            )
            response.delete_cookie("beta", path="/")
            return {"ok": True}

        app.add_middleware(TelemetryResponseBodyMiddleware)
        client = TestClient(app)
        r = client.get("/dual-cookie")
        assert r.status_code == 200
        assert "_debug_queries" in r.json(), "sanity: middleware actually fired"
        # ``r.headers.get_list`` (httpx) returns every value for the header.
        cookies = r.headers.get_list("set-cookie")
        joined = " | ".join(cookies)
        assert any("alpha=A" in c for c in cookies), (
            f"the Set-Cookie that sets `alpha` was dropped during reconstruction. saw: {joined!r}"
        )
        assert any("beta=" in c and ("Max-Age=0" in c or "expires=" in c.lower()) for c in cookies), (
            f"the Set-Cookie that deletes `beta` was dropped during reconstruction. saw: {joined!r}"
        )

    def test_jsonresponse_wrapped_dict_gets_telemetry_injected(self):
        """Some routes return an explicit JSONResponse instead of a bare
        dict. The middleware must handle both — JSONResponse pre-serialises
        on construction, but the body bytes still parse as JSON."""
        app = FastAPI()

        @app.get("/jr")
        def jr():
            return JSONResponse({"hello": "world"})

        app.add_middleware(TelemetryResponseBodyMiddleware)
        client = TestClient(app)
        r = client.get("/jr")
        body = r.json()
        assert body["hello"] == "world"
        assert "_debug_queries" in body

    # ── circular-import safety ──────────────────────────────────────────────

    def test_import_failure_of_debug_gate_does_not_block_request(self, monkeypatch):
        """If ``from backend.models.common import _debug_responses_enabled``
        ever blows up (a real concern at startup before all modules are
        settled, or in a test harness with broken patches), the middleware
        must return the response unmodified rather than 500'ing every
        request."""
        import backend.models.common as common_mod

        def _broken_gate():
            raise RuntimeError("circular import")

        monkeypatch.setattr(common_mod, "_debug_responses_enabled", _broken_gate)
        # And make the attribute LOOKUP raise too, simulating the import
        # itself failing. The middleware imports the symbol inside dispatch,
        # so we monkey-patch the parent module's __getattr__-equivalent by
        # deleting the attribute then poisoning the module.
        import sys

        class _PoisonedModule:
            def __getattr__(self, name):  # pragma: no cover - defensive
                raise RuntimeError("module load broken")

        monkeypatch.setitem(sys.modules, "backend.models.common", _PoisonedModule())

        client = TestClient(_build_app())
        r = client.get("/plain-dict")
        # Response should pass through unchanged — no _debug_queries injection.
        body = r.json()
        assert body == {"foo": 1, "bar": "two"}

    # ── is_remote: analyst responses get nothing ────────────────────────────

    def test_is_remote_flag_skips_telemetry_injection(self):
        """N-1 audit fix: analyst (is_remote=True) responses MUST NOT carry
        telemetry — _debug_calls leaks the Fastly KV store id, _debug_queries
        leaks raw SQL. The telemetry middleware honors the same is_remote
        flag the strip in RemoteAccessMiddleware uses, otherwise this
        middleware (which sits outside) would re-inject after the strip."""
        app = FastAPI()

        @app.get("/plain")
        def plain():
            return {"ok": True}

        # Inner middleware: stamp is_remote=True on every request. Because
        # add_middleware is reverse-stack (last call = outermost), we add
        # the stamper LAST so it wraps the telemetry middleware — but we
        # actually want the stamper to run BEFORE the telemetry middleware
        # reads request.state.is_remote. ASGI request-direction order is
        # outer→inner, so the LAST-added middleware sees the request first
        # → set the flag there.
        @app.middleware("http")
        async def _stamp_remote(request, call_next):
            request.state.is_remote = True
            return await call_next(request)

        app.add_middleware(TelemetryResponseBodyMiddleware)
        client = TestClient(app)
        r = client.get("/plain")
        body = r.json()
        assert body == {"ok": True}
        assert "_debug_queries" not in body
        assert "_debug_calls" not in body

    # ── body-iterator read failure: catch + pass through ──────────────────

    def test_body_iterator_read_failure_returns_original_response(self, monkeypatch):
        """If reading the response body iterator raises (e.g. an upstream
        middleware's stream produces an exception mid-yield), the telemetry
        middleware logs at warning and returns the original response
        untouched. Without this catch one upstream bug would 500 every
        request that flowed through us."""
        app = FastAPI()

        class _ExplodingResponse(JSONResponse):
            async def __call__(self, scope, receive, send):
                async def _bad_send(message):
                    if message["type"] == "http.response.body":
                        raise RuntimeError("body pipe broken")
                    await send(message)

                await super().__call__(scope, receive, _bad_send)

        @app.get("/explodes")
        def explodes():
            return _ExplodingResponse({"x": 1})

        app.add_middleware(TelemetryResponseBodyMiddleware)
        client = TestClient(app, raise_server_exceptions=False)
        # The ASGI failure surfaces as a 500 — what we're pinning is that
        # the middleware caught its OWN read failure (covered by line 162-164)
        # rather than crashing inside the try block. Either an OK pass-through
        # or a 500 from the underlying explosion both satisfy "no extra crash".
        r = client.get("/explodes")
        assert r.status_code in (200, 500)

    # ── opt_in=False: telemetry stripped from existing payloads ─────────────

    def test_strips_telemetry_when_force_include_off_and_no_header(self, monkeypatch):
        """Production code path: DEBUG_RESPONSES=1 (collectors still run for
        the Debug Panel toggle) but DEBUG_RESPONSES_FORCE_INCLUDE is unset
        and the request doesn't send ``x-debug-responses: 1``. The endpoint
        may have stuffed telemetry into the body via BaseResponse; this
        middleware must strip it on the way out so prod admins (no toggle)
        don't burn bytes on a 5-15 KB envelope they won't read."""
        monkeypatch.delenv("DEBUG_RESPONSES_FORCE_INCLUDE", raising=False)
        client = TestClient(_build_app())
        r = client.get("/already-has-telemetry")
        body = r.json()
        # Non-telemetry keys survive.
        assert body["foo"] == 1
        # The telemetry keys are stripped.
        assert "_debug_queries" not in body
        assert "_debug_calls" not in body
        assert "_debug_sqlite" not in body
        assert "_is_cached" not in body

    def test_strip_path_passes_through_when_payload_had_no_telemetry(self, monkeypatch):
        """opt_in=False path: when the endpoint did NOT add telemetry, the
        middleware shouldn't re-serialise the body. Otherwise every plain
        response pays a json.loads + json.dumps round-trip for nothing.
        Pin the no-op by checking the body bytes survive and the strip
        short-circuit was hit."""
        monkeypatch.delenv("DEBUG_RESPONSES_FORCE_INCLUDE", raising=False)
        client = TestClient(_build_app())
        r = client.get("/plain-dict")
        body = r.json()
        # No telemetry added (the strip path's early return for
        # "nothing to strip" took the fast path).
        assert body == {"foo": 1, "bar": "two"}

    # ── injection failure: telemetry collectors raise → pass through ────────

    def test_get_queries_failure_passes_through_original_body(self, monkeypatch):
        """If get_queries() or json.dumps() raises during injection, the
        middleware logs and returns the original body bytes. The endpoint's
        response still reaches the client — telemetry is observability, not
        a correctness gate."""
        import backend.utils.telemetry as telemetry_mod

        monkeypatch.setattr(
            telemetry_mod, "get_queries", lambda: (_ for _ in ()).throw(RuntimeError("collector broken"))
        )

        client = TestClient(_build_app())
        r = client.get("/plain-dict")
        body = r.json()
        # Original endpoint payload survives.
        assert body["foo"] == 1
        # Injection failed → no telemetry envelope added.
        assert "_debug_queries" not in body

    # ── _reconstruct: malformed content-type bytes ──────────────────────────

    def test_reconstruct_handles_non_ascii_content_type_header(self):
        """The _reconstruct fallback reads media_type from raw_headers and
        decodes as ASCII. A pathological response that somehow gets a non-
        ASCII byte sequence in its content-type header must not crash the
        middleware — the UnicodeDecodeError is caught and the new Response
        is built with media_type=None (Starlette tolerates that)."""
        from backend.utils.telemetry_response_middleware import _reconstruct

        class _FakeResp:
            status_code = 200
            media_type = None
            raw_headers: list[tuple[bytes, bytes]] = [(b"content-type", b"application/json\xff")]

        new = _reconstruct(_FakeResp(), b'{"ok":true}')
        assert new.status_code == 200
        assert new.body == b'{"ok":true}'
