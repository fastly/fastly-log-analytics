"""Consolidated tests for backend.utils.telemetry, usage_logger, CDN miss tracking, middleware, and HLL."""

from __future__ import annotations

import sys
import threading
import time
import types
from collections.abc import Callable
from unittest.mock import patch

import pytest
from backend.utils.hll import DEFAULT_PRECISION, SERIALIZATION_VERSION, HyperLogLog, _alpha
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware, _reconstruct
from fastapi import FastAPI, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.testclient import TestClient
from starlette.responses import JSONResponse, StreamingResponse

from backend import config as svcconfig
from backend.core import metadata as metadata_db
from backend.core.duckdb import log_usage_calls, purge_usage_log
from backend.core.metadata import usage_log_db
from backend.models import common as common_models
from backend.models.common import BaseResponse
from backend.utils import sqlite_profiler, telemetry_proxy
from backend.utils import telemetry as _t
from backend.utils.telemetry import (
    _CALLS,
    _LATEST_PROCESS_CONTEXT_LOCK,
    _PROCESS_CONTEXT,
    _set_process_context_for_tests,
    flush_usage_log,
    get_process_context,
    get_process_context_with_fallback,
    get_tracked_calls,
    is_full_miss,
    process_context_scope,
    record_call,
    record_cdn_call,
    run_usage_log_cleanup,
    start_call_tracking,
)

telemetry = _t


def _reset_global_fallback() -> None:
    """Tests share process state — keep them order-independent."""
    with _LATEST_PROCESS_CONTEXT_LOCK:
        _t._LATEST_PROCESS_CONTEXT = None
        _t._ACTIVE_CONTEXTS.clear()


def _run_in_thread(fn: Callable[[], None]) -> None:
    """Run ``fn`` in a fresh OS thread and wait for completion."""
    t = threading.Thread(target=fn)
    t.start()
    t.join()


def _read_fallback_from_iothread() -> str | None:
    """Read the fallback from a fresh thread (no ContextVar inheritance)."""
    holder: dict[str, str | None] = {}

    def _r() -> None:
        holder["v"] = get_process_context_with_fallback()

    _run_in_thread(_r)
    return holder["v"]


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


# ── Process context & cross-thread fallback ──────────────────────────────────


def test_set_process_context_visible_in_same_thread():
    """Setting process context updates both the thread-local ContextVar and fallback."""
    _reset_global_fallback()
    _set_process_context_for_tests("cron_alpha")
    assert get_process_context() == "cron_alpha"
    assert get_process_context_with_fallback() == "cron_alpha"


def test_get_process_context_with_fallback_returns_last_set_in_unrelated_thread():
    """A raw threading.Thread does NOT inherit ContextVar state from the
    spawning thread. get_process_context() returns None there; the fallback
    returns the last value set process-wide."""
    _reset_global_fallback()
    _set_process_context_for_tests("cron_sync_main_thread")
    captured: dict[str, object] = {}

    def worker() -> None:
        captured["ctxvar"] = get_process_context()
        captured["fallback"] = get_process_context_with_fallback()

    _run_in_thread(worker)
    assert captured["ctxvar"] is None, (
        "ContextVar must NOT propagate to a raw thread — if this changes, the "
        "fallback is no longer load-bearing and the global mirror can be removed."
    )
    assert captured["fallback"] == "cron_sync_main_thread"


def test_fallback_reflects_most_recent_setter_across_threads():
    """Last-writer-wins: thread B's _set_process_context_for_tests overwrites the
    fallback for thread C's reader."""
    _reset_global_fallback()
    _set_process_context_for_tests("cron_A")
    barrier = threading.Barrier(3)
    fallback_reads: list[str | None] = []

    def setter_b() -> None:
        barrier.wait()
        _set_process_context_for_tests("cron_B")

    def reader_c() -> None:
        barrier.wait()
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
    """Before any _set_process_context_for_tests call, both readers return None."""
    _reset_global_fallback()
    captured: dict[str, object] = {}

    def worker() -> None:
        captured["ctxvar"] = get_process_context()
        captured["fallback"] = get_process_context_with_fallback()

    _run_in_thread(worker)
    assert captured["ctxvar"] is None
    assert captured["fallback"] is None


def test_process_context_scope_resets_contextvar_and_global_on_exit():
    """On exit, both the ContextVar and the process-global mirror must be cleared."""
    _reset_global_fallback()
    captured: dict[str, object] = {}

    def worker() -> None:
        with process_context_scope("cron_sync"):
            captured["in_ctxvar"] = get_process_context()
            captured["in_fallback"] = get_process_context_with_fallback()
        captured["out_ctxvar"] = get_process_context()
        captured["out_fallback"] = get_process_context_with_fallback()

    _run_in_thread(worker)
    assert captured["in_ctxvar"] == "cron_sync"
    assert captured["in_fallback"] == "cron_sync"
    assert captured["out_ctxvar"] is None
    assert captured["out_fallback"] is None


def test_process_context_scope_stack_preserves_outer_when_inner_exits():
    """When a quick inner cron B exits while a long-running outer cron A is still
    active, the fallback must revert to A — NOT None."""
    _reset_global_fallback()
    a_started = threading.Event()
    b_done = threading.Event()
    captured: dict[str, str | None] = {}

    def long_running_a() -> None:
        with process_context_scope("cron_A_long"):
            a_started.set()
            b_done.wait(timeout=2.0)
            captured["after_b_exit"] = _read_fallback_from_iothread()

    def quick_b() -> None:
        a_started.wait(timeout=2.0)
        with process_context_scope("cron_B_quick"):
            captured["during_b"] = _read_fallback_from_iothread()
        b_done.set()

    ta = threading.Thread(target=long_running_a)
    tb = threading.Thread(target=quick_b)
    ta.start()
    tb.start()
    ta.join()
    tb.join()

    assert captured["during_b"] == "cron_B_quick"
    assert captured["after_b_exit"] == "cron_A_long"


def test_process_context_scope_handles_concurrent_setter_outside_scope():
    """If something calls _set_process_context_for_tests() OUTSIDE the scope,
    the scope's exit pops its own name from the stack and restores from the stack."""
    _reset_global_fallback()
    captured: dict[str, str | None] = {}

    def worker() -> None:
        with process_context_scope("cron_A"):
            _set_process_context_for_tests("rogue_value")
            captured["during"] = _read_fallback_from_iothread()
        captured["after"] = _read_fallback_from_iothread()

    _run_in_thread(worker)
    assert captured["during"] == "rogue_value"
    assert captured["after"] is None


def test_query_iothread_calls_does_not_synchronously_flush_proxy(monkeypatch):
    """Regression for M5 (item 24): _query_iothread_calls_from_usage_log MUST NOT
    block on telemetry_proxy._flush_log_writes_for_tests."""
    _reset_global_fallback()
    _set_process_context_for_tests("api:GET /api/test")
    flush_calls: list[float] = []

    monkeypatch.setattr(telemetry_proxy, "_flush_log_writes_for_tests", lambda timeout=2.0: flush_calls.append(timeout))
    monkeypatch.setattr(common_models, "_debug_responses_enabled", lambda: True)
    monkeypatch.setattr(svcconfig, "is_usage_logging_enabled", lambda: True)
    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "svc1")
    telemetry._REQUEST_START_TS.set(0.0)
    monkeypatch.setattr(metadata_db, "get_con", lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("unreachable")))

    assert telemetry._query_iothread_calls_from_usage_log() == []
    assert flush_calls == []


# ── Merged from test_telemetry_unit.py ──────────────────────────────────────


@pytest.mark.parametrize(
    ("debug_on", "start_ts", "ctx"),
    [
        (True, 1.0, None),
        (False, 1.0, "api:/x"),
        (True, None, "api:/x"),
        (True, 1.0, "cron:sync"),
    ],
    ids=["no_context", "debug_off", "no_start_ts", "non_api_context"],
)
def test_query_iothread_calls_returns_empty_guards(
    monkeypatch, debug_on: bool, start_ts: float | None, ctx: str | None
):
    monkeypatch.setattr("backend.models.common._debug_responses_enabled", lambda: debug_on)
    _t._REQUEST_START_TS.set(time.time() if start_ts is not None else None)
    _t._set_process_context_for_tests(ctx)
    assert _t._query_iothread_calls_from_usage_log() == []


def test_record_call_swallows_otel_failure(monkeypatch):
    _t.start_call_tracking()
    fake = types.ModuleType("opentelemetry.trace")
    fake.get_current_span = lambda *_a, **_kw: (_ for _ in ()).throw(RuntimeError("otel sabotage"))  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "opentelemetry.trace", fake)
    _t.record_call(method="GET", path="/x", time_ms=5.0, service="CDN", status=200, caller="tests.unit")
    assert any(c.get("path") == "/x" for c in _t.get_tracked_calls())


def test_is_full_miss_chain_variants():
    for val, expected in [
        ("HIT, HIT", False),
        ("MISS, HIT", False),
        ("MISS, MISS", True),
        ("PASS", True),
        (None, False),
        ("", False),
    ]:
        assert _t.is_full_miss(val) is expected


def test_process_context_scope_pops_on_exit():
    _t._set_process_context_for_tests("outer")
    with pytest.raises(ValueError, match="boom"), _t.process_context_scope("inner"):
        assert _t.get_process_context() == "inner"
        raise ValueError("boom")
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
    monkeypatch.setitem(sys.modules, "backend.core.metadata.usage_log_db", _FakeDB)

    rows = _t._query_iothread_calls_from_usage_log()
    assert len(rows) == 2
    assert {r["service"] for r in rows} == {"FOS", "CDN"}
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
    assert set(res) == {"ok", "_debug_queries", "_debug_calls", "_debug_sqlite", "_is_cached"}
    assert res["_is_cached"] is False


@pytest.mark.parametrize(
    ("env_val", "src", "is_cached", "expected"),
    [
        (
            None,
            {"a": 1, "_debug_queries": [1], "_debug_calls": [2], "_debug_sqlite": [3]},
            True,
            {"a": 1, "_debug_sqlite": [3], "_is_cached": True},
        ),
        (
            "1",
            {"a": 1, "_debug_queries": [1], "_debug_calls": [2]},
            False,
            {"a": 1, "_debug_queries": [1], "_debug_calls": [2], "_is_cached": False},
        ),
        ("1", {"a": 1}, False, {"a": 1, "_is_cached": False}),
    ],
    ids=["strips_when_off", "keeps_when_on", "never_injects_collectors"],
)
def test_gated_debug_envelope_cases(monkeypatch, env_val: str | None, src: dict, is_cached: bool, expected: dict):
    if env_val is None:
        monkeypatch.delenv("DEBUG_RESPONSES", raising=False)
    else:
        monkeypatch.setenv("DEBUG_RESPONSES", env_val)
    src_copy = dict(src)
    out = _t.debug_envelope(src_copy, gated=True, is_cached=is_cached)
    assert out == expected
    assert "_is_cached" not in src_copy


# ── Merged from test_usage_logger.py ────────────────────────────────────────


def test_process_context_set_and_get():
    _set_process_context_for_tests("cron:sync:svc1")
    assert get_process_context() == "cron:sync:svc1"


def test_process_context_default_is_none():
    result: dict[str, object] = {}
    _run_in_thread(lambda: result.__setitem__("ctx", _PROCESS_CONTEXT.get(None)))
    assert result["ctx"] is None


def test_start_call_tracking_resets_calls():
    start_call_tracking()
    record_call("PutObject", "/file.gz", 10.0, service="FOS", details="Class A")
    assert len(get_tracked_calls()) == 1
    start_call_tracking()
    assert get_tracked_calls() == []


def test_record_call_stores_bytes():
    start_call_tracking()
    record_call("GetObject", "/data.parquet", 5.0, service="FOS", bytes_count=1024)
    calls = get_tracked_calls()
    assert len(calls) == 1 and calls[0]["bytes"] == 1024


# ---------------------------------------------------------------------------
# log_usage_calls — classification and insertion (per-service metadata DB)


@pytest.mark.parametrize(
    ("call", "ctx", "expected_class", "expected_type", "expected_bytes"),
    [
        (
            {
                "service": "FOS",
                "method": "PutObject",
                "path": "/prefix/file.gz",
                "time_ms": 12.0,
                "status": "OK",
                "details": "Class A",
                "caller": "upload_file",
                "bytes": None,
            },
            "cron:sync:svc1",
            "A",
            "PutObject",
            None,
        ),
        (
            {
                "service": "CDN",
                "method": "download",
                "path": "/data.parquet",
                "time_ms": 50.0,
                "status": "OK",
                "bytes": 2048,
            },
            None,
            "CDN",
            "download",
            2048,
        ),
        (
            {"service": "Fastly API", "method": "GET", "path": "/api/services", "time_ms": 8.0, "status": "OK"},
            None,
            "B",
            "GET",
            None,
        ),
    ],
    ids=["fos_put_object_class_a", "cdn_download_class_cdn", "non_fos_fastly_api_class_b"],
)
@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_single_call_classification(
    mock_enabled, call: dict, ctx: str | None, expected_class: str, expected_type: str, expected_bytes: int | None
):
    log_usage_calls({"name": "svc1"}, [call], process_context=ctx)
    row = (
        usage_log_db.get_con("svc1")
        .execute("SELECT operation_class, operation_type, service_id, bytes FROM usage_log")
        .fetchone()
    )
    assert row is not None
    assert (row["operation_class"], row["operation_type"], row["service_id"], row["bytes"]) == (
        expected_class,
        expected_type,
        "svc1",
        expected_bytes,
    )


@patch("backend.config.is_usage_logging_enabled", return_value=True)
def test_log_usage_calls_classifies_raw_http_put_post_as_class_a(mock_enabled):
    """Telemetry proxy emits raw HTTP verbs (PUT/POST/COPY) via request.method;
    classifier must recognise those as Class A."""
    calls = [
        {"service": "FOS", "method": m, "path": "/k", "time_ms": 1.0, "status": "OK"}
        for m in ("PUT", "POST", "COPY", "GET", "DELETE")
    ]
    log_usage_calls({"name": "svc1"}, calls)
    con = usage_log_db.get_con("svc1")
    rows = con.execute("SELECT operation_type, operation_class FROM usage_log ORDER BY rowid").fetchall()
    assert [(r["operation_type"], r["operation_class"]) for r in rows] == [
        ("PUT", "A"),
        ("POST", "A"),
        ("COPY", "A"),
        ("GET", "B"),
        ("DELETE", "B"),
    ]


@patch("backend.config.is_usage_logging_enabled", return_value=False)
def test_log_usage_calls_skips_when_disabled(mock_enabled):
    """When usage logging is disabled, no DB interaction should occur."""
    calls = [{"service": "FOS", "method": "PutObject", "path": "/x", "time_ms": 1.0, "status": "OK"}]
    log_usage_calls({"name": "svc1"}, calls)


# ---------------------------------------------------------------------------
# purge_usage_log — retention enforcement


@patch("backend.config.load_usage_logging_config", return_value={"retention_days": 7})
def test_purge_usage_log_deletes_old_rows(mock_cfg):
    con = usage_log_db.get_con("svc1")
    con.executemany(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, status) VALUES (?, ?, ?, ?)",
        [("2020-01-01T00:00:00", "svc1", "A", "OK"), ("2099-01-01T00:00:00", "svc1", "A", "OK")],
    )
    con.commit()
    purge_usage_log({"name": "svc1"})
    rows = con.execute("SELECT timestamp FROM usage_log").fetchall()
    assert len(rows) == 1 and "2099" in str(rows[0]["timestamp"])


@patch("backend.config.load_usage_logging_config", return_value={"retention_days": 0})
def test_purge_usage_log_skips_when_retention_zero(mock_cfg):
    con = usage_log_db.get_con("svc1")
    con.execute(
        "INSERT INTO usage_log (timestamp, service_id, operation_class, status) VALUES (?, ?, ?, ?)",
        ("2020-01-01T00:00:00", "svc1", "A", "OK"),
    )
    con.commit()
    purge_usage_log({"name": "svc1"})
    assert con.execute("SELECT count(*) FROM usage_log").fetchone()[0] == 1


# ---------------------------------------------------------------------------
# flush_usage_log / run_usage_log_cleanup


@pytest.mark.parametrize(
    ("enabled", "has_call", "cfg"),
    [
        (False, False, {"service_id": "x"}),
        (True, False, {"service_id": "x"}),
        (True, True, None),
    ],
    ids=["logging_disabled", "no_tracked_calls", "service_config_missing"],
)
def test_flush_usage_log_skip_conditions(enabled: bool, has_call: bool, cfg: dict | None):
    telemetry.start_call_tracking()
    if has_call:
        telemetry.record_call("PutObject", "/k", 1.0, service="FOS")
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=enabled),
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.core.duckdb.log_usage_calls") as mock_log,
    ):
        flush_usage_log("svc-flush-skip")
    mock_log.assert_not_called()


def test_flush_usage_log_happy_path_forwards_calls_and_context():
    """All preconditions met → log_usage_calls is invoked with source, calls, and process_context."""
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
    assert args[0] == fake_source and len(args[1]) == 1 and args[1][0]["method"] == "PutObject"
    assert kwargs.get("process_context") == "cron:sync:svc-flush-3"


def test_flush_usage_log_swallows_inner_exception():
    """Even if log_usage_calls raises, flush must NOT propagate."""
    telemetry.start_call_tracking()
    telemetry.record_call("PutObject", "/k", 1.0, service="FOS")
    with (
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch("backend.config.load_config", return_value={"service_id": "x"}),
        patch("backend.config.config_to_source", return_value={"name": "x"}),
        patch("backend.core.duckdb.log_usage_calls", side_effect=RuntimeError("db down")),
    ):
        flush_usage_log("svc-flush-error")


def test_run_usage_log_cleanup_skips_when_service_config_missing():
    with (
        patch("backend.config.load_config", return_value=None),
        patch("backend.core.duckdb.purge_usage_log") as mock_purge,
    ):
        run_usage_log_cleanup("svc-cleanup-missing")
    mock_purge.assert_not_called()


def test_run_usage_log_cleanup_happy_path_delegates_to_purge():
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
    with (
        patch("backend.config.load_config", return_value={"service_id": "x"}),
        patch("backend.config.config_to_source", return_value={"name": "x"}),
        patch("backend.core.duckdb.purge_usage_log", side_effect=RuntimeError("boom")),
    ):
        run_usage_log_cleanup("svc-cleanup-error")


# ── Merged from test_cdn_miss_tracking.py ───────────────────────────────────


class TestCdnMissTracking:
    @pytest.fixture(autouse=True)
    def fresh_call_tracking(self):
        start_call_tracking()
        yield
        _CALLS.set([])

    class TestIsFullMiss:
        @pytest.mark.parametrize(
            ("header", "expected"),
            [
                ("HIT", False),
                ("HIT, HIT", False),
                ("MISS, HIT", False),
                ("MISS, MISS", True),
                ("PASS", True),
                ("PASS, PASS", True),
                ("MISS, PASS", True),
                ("", False),
                (None, False),
                ("miss, miss", True),
                ("Hit, Hit", False),
            ],
        )
        def test_is_full_miss_cases(self, header: str | None, expected: bool):
            assert is_full_miss(header) is expected

    class TestRecordCdnCall:
        @pytest.mark.parametrize(
            ("headers", "bytes_count"),
            [
                ({"X-Cache": "HIT, HIT"}, 1024),
                ({"X-Cache": "MISS, HIT"}, 512),
                (None, 100),
            ],
            ids=["hit_records_only_cdn", "edge_miss_shield_hit_no_fos", "no_headers_records_only_cdn"],
        )
        def test_non_miss_records_only_cdn_op(self, headers: dict | None, bytes_count: int):
            record_cdn_call("GET", "key.parquet", 12.3, headers=headers, bytes_count=bytes_count)
            calls = get_tracked_calls()
            assert len(calls) == 1
            assert calls[0]["service"] == "CDN"
            assert calls[0]["method"] == "GET"
            assert calls[0]["bytes"] == bytes_count

        @pytest.mark.parametrize(
            ("method", "key", "headers", "bytes_count"),
            [
                ("GET", "key.parquet", {"X-Cache": "MISS, MISS"}, 2048),
                ("HEAD", "key.parquet", {"X-Cache": "MISS, MISS"}, None),
                ("GET", "uncacheable", {"X-Cache": "PASS"}, 4096),
            ],
            ids=["full_miss_records_cdn_plus_fos", "head_miss_synthesizes_get_object", "pass_synthesizes_fos_op"],
        )
        def test_miss_or_pass_synthesizes_fos_get_object(
            self, method: str, key: str, headers: dict, bytes_count: int | None
        ):
            record_cdn_call(method, key, 50.0, headers=headers, bytes_count=bytes_count)
            calls = get_tracked_calls()
            assert len(calls) == 2
            assert [c["service"] for c in calls] == ["CDN", "FOS"]
            assert calls[1]["method"] == "GET_OBJECT"
            assert calls[1]["bytes"] == bytes_count
            assert "synthesized from CDN MISS" in (calls[1]["details"] or "")


# ── Merged from test_telemetry_response_middleware.py ───────────────────────


def _build_app(*, with_gzip: bool = False) -> FastAPI:
    """Build a minimal app with the telemetry middleware installed."""
    app = FastAPI()
    app.get("/plain-dict")(lambda: {"foo": 1, "bar": "two"})
    app.get("/already-has-telemetry")(
        lambda: {
            "foo": 1,
            "_debug_queries": [{"sql": "SELECT 1", "time_ms": 0.1}],
            "_debug_calls": [{"method": "GET", "path": "/x"}],
            "_debug_sqlite": [{"seq": 7, "sql": "SELECT 1", "time_ms": 0.2}],
            "_is_cached": True,
        }
    )

    @app.get("/sqlite-work")
    def sqlite_work():
        start_call_tracking()
        sqlite_profiler._record("CREATE TABLE page_scoped (x INTEGER)", None, 0.2, 0, "execute")
        return BaseResponse.with_telemetry()

    app.get("/list-response")(lambda: [1, 2, 3])

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

    app.get("/non-json")(lambda: Response(content="hello", media_type="text/plain"))
    app.get("/empty")(lambda: Response(status_code=204))
    app.get("/raises")(lambda: (_ for _ in ()).throw(RuntimeError("boom")))
    app.get("/bad-json")(lambda: Response(content=b"{not valid", media_type="application/json"))
    app.get("/jr")(lambda: JSONResponse({"hello": "world"}))

    @app.get("/dual-cookie")
    def dual_cookie(response: Response):
        response.set_cookie(
            key="alpha", value="A", httponly=True, secure=True, samesite="strict", max_age=86400, path="/"
        )
        response.delete_cookie("beta", path="/")
        return {"ok": True}

    app.add_middleware(TelemetryResponseBodyMiddleware)
    if with_gzip:
        app.add_middleware(GZipMiddleware, minimum_size=0)
    return app


class TestTelemetryResponseMiddleware:
    @pytest.fixture(autouse=True)
    def _enable_debug_responses(self, monkeypatch):
        """Enable DEBUG_RESPONSES by default for the middleware suite."""
        monkeypatch.setenv("DEBUG_RESPONSES", "1")

    def test_injects_debug_keys_into_plain_dict_response(self):
        """Plain dict responses get _debug_queries, _debug_calls, _debug_sqlite, and _is_cached injected."""
        r = TestClient(_build_app()).get("/plain-dict")
        assert r.status_code == 200
        body = r.json()
        assert body["foo"] == 1 and body["bar"] == "two"
        for key in ("_debug_queries", "_debug_calls", "_debug_sqlite"):
            assert key in body and isinstance(body[key], list)
        assert body["_is_cached"] is False

    def test_injects_safely_when_no_telemetry_recorded(self):
        """When collectors are empty, middleware emits valid empty lists."""
        body = TestClient(_build_app()).get("/plain-dict").json()
        assert body["_debug_queries"] == [] and body["_debug_calls"] == []

    def test_does_not_double_inject_when_endpoint_already_supplied_telemetry(self):
        """Endpoints using BaseResponse.with_telemetry keep their keys verbatim."""
        body = TestClient(_build_app()).get("/already-has-telemetry").json()
        assert body["_debug_queries"] == [{"sql": "SELECT 1", "time_ms": 0.1}]
        assert body["_debug_calls"] == [{"method": "GET", "path": "/x"}]
        assert body["_debug_sqlite"] == [{"seq": 7, "sql": "SELECT 1", "time_ms": 0.2}]
        assert body["_is_cached"] is True

    @pytest.mark.parametrize(
        ("path", "status", "ctype_prefix", "expected_text"),
        [
            ("/list-response", 200, "application/json", "[1,2,3]"),
            ("/non-json", 200, "text/plain", "hello"),
            ("/empty", 204, None, ""),
            ("/sse", 200, "text/event-stream", "data: hello\n\ndata: world\n\n"),
            ("/ndjson", 200, "application/x-ndjson", '{"row":1}\n{"row":2}\n'),
        ],
    )
    def test_passthrough_responses_untouched(
        self, path: str, status: int, ctype_prefix: str | None, expected_text: str
    ):
        r = TestClient(_build_app()).get(path)
        assert r.status_code == status
        assert r.text == expected_text
        if ctype_prefix:
            assert r.headers["content-type"].startswith(ctype_prefix)

    def test_sqlite_statements_executed_during_request_reach_debug_sqlite(self):
        """Profiler _record feeds request-scoped collector serialized as _debug_sqlite."""
        body = TestClient(_build_app()).get("/sqlite-work").json()
        sqls = [q["sql"] for q in body["_debug_sqlite"]]
        assert "CREATE TABLE page_scoped (x INTEGER)" in sqls
        entry = body["_debug_sqlite"][sqls.index("CREATE TABLE page_scoped (x INTEGER)")]
        assert entry["op"] == "execute" and entry["time_ms"] >= 0 and entry["seq"] >= 1

    def test_plain_request_does_not_inherit_other_requests_sqlite(self):
        """A request that ran no SQLite of its own carries an empty _debug_sqlite."""
        client = TestClient(_build_app())
        client.get("/sqlite-work")
        assert client.get("/plain-dict").json()["_debug_sqlite"] == []

    def test_does_not_inject_when_debug_responses_env_is_off(self, monkeypatch):
        monkeypatch.setenv("DEBUG_RESPONSES", "")
        body = TestClient(_build_app()).get("/plain-dict").json()
        assert "_debug_queries" not in body and "_debug_calls" not in body

    @pytest.mark.parametrize("master_enabled", [False, True])
    def test_no_envelope_preserves_original_wire_bytes(self, monkeypatch, master_enabled: bool) -> None:
        monkeypatch.setenv("DEBUG_RESPONSES", "1" if master_enabled else "")
        monkeypatch.delenv("DEBUG_RESPONSES_FORCE_INCLUDE", raising=False)
        app = FastAPI()
        body = b'{ "value": 1.00, "_section_timings": [], "_response_cache": {"state": "hit"} }\n'

        @app.get("/wire")
        def wire() -> Response:
            resp = Response(body, status_code=202, media_type="application/json", headers={"X-Example": "kept"})
            resp.set_cookie("alpha", "A")
            resp.set_cookie("beta", "B")
            return resp

        app.add_middleware(TelemetryResponseBodyMiddleware)
        response = TestClient(app).get("/wire")
        assert response.status_code == 202 and response.content == body
        assert response.headers["content-type"] == "application/json" and response.headers["X-Example"] == "kept"
        assert (
            int(response.headers["content-length"]) == len(body) and len(response.headers.get_list("set-cookie")) == 2
        )

    def test_master_off_strips_pre_attached_debug_data_and_preserves_cache_marker(self, monkeypatch) -> None:
        monkeypatch.setenv("DEBUG_RESPONSES", "0")
        monkeypatch.setenv("DEBUG_RESPONSES_FORCE_INCLUDE", "1")
        for name in ("get_queries", "get_sqlite_queries", "get_tracked_calls"):
            monkeypatch.setattr(telemetry, name, lambda: pytest.fail("Master-off must not read telemetry collectors"))
        payload = {
            "value": {"count": 7, "rows": [], "optional": None},
            "_section_timings": [{"name": "query", "ms": 0.1}],
            "_response_cache": {"state": "hit"},
            "_debug_queries": [{"sql": "SELECT 1"}],
            "_debug_calls": [{"path": "/example"}],
            "_debug_sqlite": [{"sql": "SELECT 2"}],
            "_debug_postgres": [{"sql": "SELECT 3"}],
            "_is_cached": True,
        }
        app = FastAPI()

        @app.get("/pre-attached")
        def pre_attached() -> Response:
            extra = {
                "debug_queries": [{"sql": "SELECT 4"}],
                "debug_calls": [{"path": "/bare"}],
                "debug_sqlite": [{"sql": "SELECT 5"}],
                "debug_postgres": [{"sql": "SELECT 6"}],
            }
            resp = JSONResponse({**payload, **extra}, status_code=202, headers={"X-Example": "kept"})
            resp.set_cookie("alpha", "A")
            resp.set_cookie("beta", "B")
            return resp

        app.add_middleware(TelemetryResponseBodyMiddleware)
        response = TestClient(app).get("/pre-attached", headers={"x-debug-responses": "1"})
        assert response.status_code == 202
        assert response.json() == {
            "value": payload["value"],
            "_section_timings": payload["_section_timings"],
            "_response_cache": payload["_response_cache"],
            "_is_cached": True,
        }
        assert response.headers["content-type"] == "application/json" and response.headers["x-example"] == "kept"
        assert int(response.headers["content-length"]) == len(response.content)
        assert len(response.headers.get_list("set-cookie")) == 2

    def test_works_with_gzip_outer_middleware(self):
        """GZipMiddleware outer + TelemetryResponseBodyMiddleware inner preserves injection."""
        r = TestClient(_build_app(with_gzip=True)).get("/plain-dict", headers={"Accept-Encoding": "gzip"})
        assert "_debug_queries" in r.json()

    def test_does_not_crash_on_500_error_response(self):
        r = TestClient(_build_app(), raise_server_exceptions=False).get("/raises")
        assert r.status_code == 500 and r.text == "Internal Server Error"
        assert r.headers["content-type"].startswith("text/plain")

    def test_malformed_json_body_passes_through_unchanged(self):
        r = TestClient(_build_app()).get("/bad-json")
        assert r.status_code == 200 and r.content == b"{not valid"

    def test_multiple_set_cookie_headers_survive_reconstruction(self):
        """Multiple Set-Cookie headers survive response reconstruction."""
        r = TestClient(_build_app()).get("/dual-cookie")
        assert r.status_code == 200 and "_debug_queries" in r.json()
        cookies = r.headers.get_list("set-cookie")
        assert any("alpha=A" in c for c in cookies)
        assert any("beta=" in c and ("Max-Age=0" in c or "expires=" in c.lower()) for c in cookies)

    def test_jsonresponse_wrapped_dict_gets_telemetry_injected(self):
        body = TestClient(_build_app()).get("/jr").json()
        assert body["hello"] == "world" and "_debug_queries" in body

    def test_import_failure_of_debug_gate_does_not_block_request(self, monkeypatch):
        """If importing _debug_responses_enabled fails, pass response through unmodified."""
        monkeypatch.setattr(
            common_models, "_debug_responses_enabled", lambda: (_ for _ in ()).throw(RuntimeError("circular import"))
        )

        class _PoisonedModule:
            def __getattr__(self, name):  # pragma: no cover - defensive
                raise RuntimeError("module load broken")

        monkeypatch.setitem(sys.modules, "backend.models.common", _PoisonedModule())
        assert TestClient(_build_app()).get("/plain-dict").json() == {"foo": 1, "bar": "two"}

    def test_is_remote_flag_skips_telemetry_injection(self):
        """N-1 audit fix: analyst (is_remote=True) responses MUST NOT carry telemetry."""
        app = FastAPI()
        app.get("/plain")(lambda: {"ok": True})

        @app.middleware("http")
        async def _stamp_remote(request, call_next):
            request.state.is_remote = True
            return await call_next(request)

        app.add_middleware(TelemetryResponseBodyMiddleware)
        body = TestClient(app).get("/plain").json()
        assert body == {"ok": True} and "_debug_queries" not in body and "_debug_calls" not in body

    def test_body_iterator_read_failure_returns_original_response(self):
        app = FastAPI()

        class _ExplodingResponse(JSONResponse):
            async def __call__(self, scope, receive, send):
                async def _bad_send(message):
                    if message["type"] == "http.response.body":
                        raise RuntimeError("body pipe broken")
                    await send(message)

                await super().__call__(scope, receive, _bad_send)

        app.get("/explodes")(lambda: _ExplodingResponse({"x": 1}))
        app.add_middleware(TelemetryResponseBodyMiddleware)
        r = TestClient(app, raise_server_exceptions=False).get("/explodes")
        assert r.status_code in (200, 500)

    def test_strips_telemetry_when_force_include_off_and_no_header(self, monkeypatch):
        """When DEBUG_RESPONSES_FORCE_INCLUDE is unset and no header is sent, strip telemetry."""
        monkeypatch.delenv("DEBUG_RESPONSES_FORCE_INCLUDE", raising=False)
        body = TestClient(_build_app()).get("/already-has-telemetry").json()
        assert body["foo"] == 1
        for key in ("_debug_queries", "_debug_calls", "_debug_sqlite", "_is_cached"):
            assert key not in body

    def test_strip_path_passes_through_when_payload_had_no_telemetry(self, monkeypatch):
        monkeypatch.delenv("DEBUG_RESPONSES_FORCE_INCLUDE", raising=False)
        assert TestClient(_build_app()).get("/plain-dict").json() == {"foo": 1, "bar": "two"}

    def test_get_queries_failure_passes_through_original_body(self, monkeypatch):
        monkeypatch.setattr(telemetry, "get_queries", lambda: (_ for _ in ()).throw(RuntimeError("collector broken")))
        body = TestClient(_build_app()).get("/plain-dict").json()
        assert body["foo"] == 1 and "_debug_queries" not in body

    def test_reconstruct_handles_non_ascii_content_type_header(self):
        class _FakeResp:
            status_code = 200
            media_type = None
            raw_headers: list[tuple[bytes, bytes]] = [(b"content-type", b"application/json\xff")]

        new = _reconstruct(_FakeResp(), b'{"ok":true}')
        assert new.status_code == 200 and new.body == b'{"ok":true}'


# ── Merged from tests/utils/test_hll.py ─────────────────────────────────────


def test_construction_default_precision():
    hll = HyperLogLog()
    assert hll.precision == DEFAULT_PRECISION and hll.m == 256 and hll.count() == 0


def test_construction_explicit_precision():
    for p in (4, 6, 8, 10, 12, 14, 16):
        hll = HyperLogLog(precision=p)
        assert hll.precision == p and hll.m == 1 << p


def test_construction_rejects_invalid_precision():
    for bad in (-1, 0, 3, 17, 32):
        with pytest.raises(ValueError, match="precision must be in"):
            HyperLogLog(precision=bad)


@pytest.mark.parametrize(
    ("n", "tol"),
    [
        (10, 0.20),
        (50, 0.20),
        (500, 0.10),
        (5000, 0.10),
        (50_000, 0.10),
        (100_000, 0.10),
    ],
)
def test_count_accuracy_within_tolerance(n: int, tol: float):
    hll = HyperLogLog(precision=DEFAULT_PRECISION)
    for i in range(n):
        hll.add(f"item-{i}")
    estimate = hll.count()
    rel_err = abs(estimate - n) / n
    assert rel_err <= tol, f"n={n}: estimate={estimate}, expected≈{n}, rel_err={rel_err:.3f}"


def test_add_is_idempotent():
    hll_once, hll_many = HyperLogLog(), HyperLogLog()
    items = [f"ip-{i}" for i in range(100)]
    for ip in items:
        hll_once.add(ip)
    for _ in range(5):
        for ip in items:
            hll_many.add(ip)
    assert hll_once.to_bytes() == hll_many.to_bytes() and hll_once.count() == hll_many.count()


def test_update_iterable_matches_add_loop():
    items = [f"x-{i}" for i in range(200)]
    hll_a, hll_b = HyperLogLog(), HyperLogLog()
    for item in items:
        hll_a.add(item)
    hll_b.update(items)
    assert hll_a.to_bytes() == hll_b.to_bytes()


def test_merge_equivalent_to_single_sketch():
    left_items = [f"a-{i}" for i in range(300)]
    right_items = [f"b-{i}" for i in range(300)]
    overlapping = [f"both-{i}" for i in range(100)]

    hll_left, hll_right = HyperLogLog(), HyperLogLog()
    hll_left.update(left_items + overlapping)
    hll_right.update(right_items + overlapping)

    hll_merged = HyperLogLog()
    hll_merged.merge(hll_left)
    hll_merged.merge(hll_right)

    hll_combined = HyperLogLog()
    hll_combined.update(left_items + right_items + overlapping)
    assert hll_merged.to_bytes() == hll_combined.to_bytes()


def test_merge_is_associative():
    sketches = [HyperLogLog() for _ in range(3)]
    for prefix, s in zip(("a", "b", "c"), sketches, strict=True):
        s.update(f"{prefix}-{i}" for i in range(50))
    a, b, c = sketches
    left, bc, right = HyperLogLog(), HyperLogLog(), HyperLogLog()
    for s in (a, b, c):
        left.merge(s)
    bc.merge(b)
    bc.merge(c)
    right.merge(a)
    right.merge(bc)
    assert left.to_bytes() == right.to_bytes()


def test_merge_rejects_precision_mismatch():
    with pytest.raises(ValueError, match="merge precision mismatch"):
        HyperLogLog(precision=8).merge(HyperLogLog(precision=10))


def test_to_bytes_layout_pinned():
    hll = HyperLogLog(precision=8)
    hll.update(f"x-{i}" for i in range(100))
    blob = hll.to_bytes()
    assert blob[0] == SERIALIZATION_VERSION and blob[1] == 8 and len(blob) == 258


def test_round_trip_serialization_lossless():
    hll = HyperLogLog()
    hll.update(f"ip-{i}" for i in range(2500))
    blob = hll.to_bytes()
    restored = HyperLogLog.from_bytes(blob)
    assert restored.to_bytes() == blob and restored.count() == hll.count()


def test_round_trip_across_precisions():
    for p in (4, 8, 12, 16):
        hll = HyperLogLog(precision=p)
        hll.update(f"k-{p}-{i}" for i in range(200))
        blob = hll.to_bytes()
        assert len(blob) == 2 + (1 << p)
        restored = HyperLogLog.from_bytes(blob)
        assert restored.precision == p and restored.to_bytes() == blob


@pytest.mark.parametrize(
    ("blob", "match"),
    [
        (bytes([99, 8]) + bytes(256), "unknown HLL serialization version"),
        (bytes([SERIALIZATION_VERSION, 8]) + bytes(100), "sketch blob length mismatch"),
        (bytes([SERIALIZATION_VERSION, 3]), "invalid precision in blob"),
        (b"", "sketch blob too short"),
        (b"\x01", "sketch blob too short"),
    ],
)
def test_from_bytes_rejects_invalid_blobs(blob: bytes, match: str):
    with pytest.raises(ValueError, match=match):
        HyperLogLog.from_bytes(blob)


def test_hash_is_stable_across_calls():
    items = [f"ip-{i}" for i in range(500)]
    sketches = [HyperLogLog() for _ in range(3)]
    for s in sketches:
        s.update(items)
    a = sketches[0].to_bytes()
    for s in sketches[1:]:
        assert s.to_bytes() == a


def test_count_empty_sketch_is_zero():
    assert HyperLogLog().count() == 0


def test_count_single_item():
    hll = HyperLogLog()
    hll.add("solo-ip")
    assert 0 < hll.count() <= 5


def test_len_returns_count():
    hll = HyperLogLog()
    hll.update(f"x-{i}" for i in range(100))
    assert len(hll) == hll.count()


def test_add_accepts_bytes_and_str():
    hll_str, hll_bytes = HyperLogLog(), HyperLogLog()
    items = ["1.2.3.4", "5.6.7.8", "ffff::1"]
    hll_str.update(items)
    hll_bytes.update(s.encode("utf-8") for s in items)
    assert hll_str.to_bytes() == hll_bytes.to_bytes()


def test_documented_error_bound_holds_on_average():
    relative_errors = []
    for n in (100, 500, 1_000, 5_000, 10_000, 50_000):
        hll = HyperLogLog()
        hll.update(f"item-{i}" for i in range(n))
        relative_errors.append(abs(hll.count() - n) / n)
    assert sum(relative_errors) / len(relative_errors) < 0.13


def test_alpha_constant_matches_documented_values():
    assert _alpha(16) == pytest.approx(0.673)
    assert _alpha(32) == pytest.approx(0.697)
    assert _alpha(64) == pytest.approx(0.709)
    assert _alpha(128) == pytest.approx(0.7213 / (1.0 + 1.079 / 128))
    assert _alpha(256) == pytest.approx(0.7213 / (1.0 + 1.079 / 256), rel=1e-12)


def test_count_does_not_raise_math_domain_error_under_extreme_bucket_ranks():
    hll = HyperLogLog(precision=8)
    hll._buckets = [25 for _ in hll._buckets]
    n = hll.count()
    assert isinstance(n, int) and n >= 0
