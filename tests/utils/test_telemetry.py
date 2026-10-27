"""Consolidated tests for backend.utils.telemetry, usage_logger, CDN miss tracking, middleware, and HLL."""

from __future__ import annotations

import asyncio
import importlib
import io
import logging
import os
import socket
import sys
import threading
import time
import tracemalloc
import types
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import aiohttp
import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yarl
from aiohttp import web
from backend.utils.telemetry_response_middleware import TelemetryResponseBodyMiddleware, _reconstruct
from botocore import UNSIGNED
from botocore.config import Config
from fastapi import FastAPI, Response
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.testclient import TestClient
from moto.server import ThreadedMotoServer
from starlette.responses import JSONResponse, StreamingResponse

from backend import config as svcconfig
from backend.core import duckdb as _ddb
from backend.core import metadata as metadata_db
from backend.core import storage as _ic
from backend.core.duckdb import log_usage_calls, purge_usage_log
from backend.core.metadata import usage_log_db
from backend.models import common as common_models
from backend.models.common import BaseResponse
from backend.utils import sqlite_profiler, telemetry_proxy
from backend.utils import telemetry as _t
from backend.utils import telemetry as tlm
from backend.utils.hll import DEFAULT_PRECISION, SERIALIZATION_VERSION, HyperLogLog, _alpha
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


# ── Merged from tests/utils/test_telemetry_proxy.py ──

MOCK_CDN_CFG = {
    "fos_access_key_id": "k",
    "fos_secret_access_key": "s",
    "fos_region": "us-east-1",
    "cdn_url": "https://cdn.example.net",
}
MOCK_FOS_CFG = {
    "fos_access_key_id": "testing",
    "fos_secret_access_key": "testing",
    "fos_region": "us-east-1",
}


@pytest.fixture
async def proxy_server():
    telemetry_proxy._reset_for_tests()
    telemetry_proxy.start_proxy_server()
    deadline = asyncio.get_event_loop().time() + 5.0
    while True:
        try:
            async with aiohttp.ClientSession() as s, s.get(f"{telemetry_proxy.proxy_endpoint()}/healthz") as r:
                if r.status == 200:
                    break
        except (aiohttp.ClientError, RuntimeError):
            pass
        if asyncio.get_event_loop().time() > deadline:
            telemetry_proxy.stop_proxy_server()
            raise RuntimeError("proxy did not become healthy in 5s")
        await asyncio.sleep(0.02)
    try:
        yield telemetry_proxy
    finally:
        telemetry_proxy.stop_proxy_server()
        telemetry_proxy._reset_for_tests()


@pytest.fixture
def moto_s3_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()
    endpoint_url = f"http://{host}:{port}"
    seed_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    seed_client.create_bucket(Bucket="test-bucket")
    try:
        yield endpoint_url, f"{host}:{port}"
    finally:
        server.stop()


def _mock_upstream(status=200, headers=None, chunks=(b"",)):
    async def _gen():
        for c in chunks:
            yield c

    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.headers = headers or {}
    mock_resp.content.iter_chunked = lambda _size: _gen()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_resp
    mock_ctx.__aexit__.return_value = None
    return mock_ctx, mock_resp


async def _proxy_call(
    proxy_server,
    path: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    status: int = 200,
    resp_headers: dict[str, str] | None = None,
    chunks: tuple[bytes, ...] = (b"",),
    cfg: dict | None = None,
):
    ctx, _ = _mock_upstream(status=status, headers=resp_headers, chunks=chunks)
    captured_req: dict = {}
    captured_rows: list[dict] = []
    captured_ctx: dict[str, str | None] = {"value": None}

    def _cap_req(*_a, **kw):
        captured_req.update(kw)
        return ctx

    def _cap_log(_sid, rows, process_context=None):
        captured_rows.extend(rows)
        captured_ctx["value"] = process_context

    if cfg is not None:
        telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=cfg) if cfg is not None else patch.dict(os.environ, {}),
        patch.object(telemetry_proxy._SESSION, "request", side_effect=_cap_req),
        patch("backend.core.metadata.log_usage_calls", side_effect=_cap_log),
    ):
        async with aiohttp.ClientSession() as s:
            url = f"{proxy_server.proxy_endpoint()}{path}"
            async with s.request(method, url, headers=headers or {}) as resp:
                body = await resp.read()
        telemetry_proxy._flush_log_writes_for_tests()
    return resp.status, body, captured_req, captured_rows, captured_ctx["value"]


async def test_proxy_healthz_returns_200_on_startup(proxy_server):
    async with aiohttp.ClientSession() as s, s.get(f"{proxy_server.proxy_endpoint()}/healthz") as resp:
        assert resp.status == 200 and (await resp.text()) == "OK"


async def test_proxy_refuses_request_without_x_fos_target_header(proxy_server):
    async with aiohttp.ClientSession() as s, s.get(f"{proxy_server.proxy_endpoint()}/some/path") as resp:
        assert resp.status == 400 and "Missing X-Fos-Target header" in (await resp.text())


async def test_proxy_streams_response_chunks_through_to_client(proxy_server):
    st, body, req, _, _ = await _proxy_call(
        proxy_server, "/test/stream?key=val", headers={"X-Fos-Target": "fake.cdn.net"}, chunks=(b"chunk1", b"chunk2")
    )
    assert st == 200 and body == b"chunk1chunk2"
    assert req["method"] == "GET" and str(req["url"]) == "https://fake.cdn.net/test/stream?key=val"


async def test_proxy_handles_upstream_5xx_returns_5xx_to_client(proxy_server):
    st, _, _, _, _ = await _proxy_call(proxy_server, "/error", headers={"X-Fos-Target": "fake.cdn.net"}, status=503)
    assert st == 503


async def test_proxy_strips_hop_by_hop_headers_before_forwarding(proxy_server):
    _, _, req, _, _ = await _proxy_call(
        proxy_server,
        "/x",
        headers={
            "X-Fos-Target": "fake.cdn.net",
            "X-Telemetry-Caller": "duckdb.httpfs",
            "Connection": "close",
            "TE": "trailers",
            "User-Agent": "myclient/1.0",
        },
        chunks=(b"ok",),
    )
    sent = req["headers"]
    for h in ("Host", "Connection", "TE", "X-Fos-Target", "X-Telemetry-Caller"):
        assert h not in sent
    assert sent.get("User-Agent") == "myclient/1.0"


async def test_proxy_does_not_sign_cdn_routed_requests(proxy_server):
    _, _, req, _, _ = await _proxy_call(
        proxy_server,
        "/file.log.gz?key=abc",
        headers={"X-Fos-Target": "cdn.example.net", "X-Telemetry-Service-Id": "test-svc"},
        cfg=MOCK_CDN_CFG,
    )
    assert "Authorization" not in req["headers"]


async def test_proxy_preserves_url_encoding_on_outbound_wire_so_sigv4_matches(proxy_server):
    received_on_upstream_wire: list[str] = []

    async def _echo(request):
        received_on_upstream_wire.append(request.path_qs)
        return web.Response(text="ok")

    upstream_app = web.Application()
    upstream_app.router.add_route("PUT", "/{tail:.*}", _echo)
    upstream_runner = web.AppRunner(upstream_app)
    await upstream_runner.setup()
    s_sock = socket.socket()
    s_sock.bind(("127.0.0.1", 0))
    upstream_port = s_sock.getsockname()[1]
    s_sock.close()
    await web.TCPSite(upstream_runner, "127.0.0.1", upstream_port).start()

    try:
        telemetry_proxy._bust_config_cache()
        encoded_path = "/buc/data/timestamp_hour%3D2026-05-19-23/x.parquet"
        client_url = yarl.URL(f"{proxy_server.proxy_endpoint()}{encoded_path}", encoded=True)
        with patch("backend.config.load_config", return_value=MOCK_FOS_CFG):
            async with (
                aiohttp.ClientSession() as s,
                s.put(
                    client_url,
                    data=b"payload",
                    headers={"X-Fos-Target": f"http://127.0.0.1:{upstream_port}", "X-Telemetry-Service-Id": "test-svc"},
                ) as resp,
            ):
                await resp.read()
    finally:
        await upstream_runner.cleanup()

    assert received_on_upstream_wire == [encoded_path]


async def test_boto3_unsigned_through_proxy_round_trips_against_moto(proxy_server, moto_s3_server):
    _, host_port = moto_s3_server
    bucket, key, body = "test-bucket", "roundtrip/file.txt", b"hello from boto3 through the proxy"
    proxied_s3 = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "test-boto3")
    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=MOCK_FOS_CFG):
        proxied_s3.put_object(Bucket=bucket, Key=key, Body=body)
        assert proxied_s3.get_object(Bucket=bucket, Key=key)["Body"].read() == body
        assert key in [o["Key"] for o in proxied_s3.list_objects_v2(Bucket=bucket).get("Contents", [])]


async def test_proxy_writes_one_usage_log_row_per_request(proxy_server):
    _, _, _, rows, pctx = await _proxy_call(
        proxy_server,
        "/key.parquet",
        headers={
            "X-Fos-Target": "bucket.s3.amazonaws.com",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "duckdb.httpfs",
            "X-Telemetry-Context": "api:GET /api/dashboard/aggregates",
        },
        resp_headers={"Content-Length": "11", "X-Cache": "HIT"},
        chunks=(b"hello world",),
    )
    assert len(rows) == 1
    row = rows[0]
    assert (row["method"], row["path"], row["bytes"], row["status"], row["caller"], row["service"]) == (
        "GET",
        "/key.parquet",
        11,
        "OK",
        "duckdb.httpfs",
        "FOS",
    )
    assert row["details"].startswith("HIT")
    assert pctx == "api:GET /api/dashboard/aggregates"


async def test_proxy_translates_fos_list_get_to_list_objects_v2(proxy_server):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/bucket?list-type=2&prefix=raw%2F&start-after=raw%2F2026-06-08%2F",
        headers={
            "X-Fos-Target": "bucket.s3.amazonaws.com",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "ingest_scan",
        },
        resp_headers={"Content-Length": "19"},
        chunks=(b"<ListBucketResult/>",),
    )
    assert len(rows) == 1 and rows[0]["service"] == "FOS" and rows[0]["method"] == "LIST_OBJECTS_V2"
    assert "list-type=2" in rows[0]["path"]


async def test_proxy_keeps_get_for_non_list_fos_reads(proxy_server):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/bucket/key.parquet?versionId=abc",
        headers={
            "X-Fos-Target": "bucket.s3.amazonaws.com",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "duckdb.httpfs",
        },
        resp_headers={"Content-Length": "32"},
        chunks=(b"x" * 32,),
    )
    assert len(rows) == 1 and rows[0]["method"] == "GET"


async def test_proxy_encodes_xcache_chain_in_details_for_shield_doubling(proxy_server):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/k.parquet",
        headers={
            "X-Fos-Target": "cdn.example.net",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "duckdb.httpfs",
        },
        resp_headers={"X-Cache": "MISS, MISS"},
        chunks=(b"x" * 1024,),
        cfg=MOCK_CDN_CFG,
    )
    cdn_rows = [r for r in rows if r["service"] == "CDN"]
    assert len(cdn_rows) == 1 and cdn_rows[0]["details"].split(" · ")[0] == "MISS, MISS"


async def test_proxy_handles_50_concurrent_requests_without_head_of_line_blocking(proxy_server):
    with patch.object(
        telemetry_proxy._SESSION, "request", side_effect=lambda *a, **kw: _mock_upstream(chunks=(b"x" * 1024,))[0]
    ):
        url = f"{proxy_server.proxy_endpoint()}/k"
        headers = {"X-Fos-Target": "fake.cdn.net"}
        async with aiohttp.ClientSession() as s:

            async def _one():
                async with s.get(url, headers=headers) as r:
                    await r.read()
                    return r.status

            t0 = asyncio.get_event_loop().time()
            results = await asyncio.gather(*[_one() for _ in range(50)])
            elapsed = asyncio.get_event_loop().time() - t0
    assert all(s == 200 for s in results) and elapsed < 5.0


async def test_proxy_logs_warning_when_dashboard_context_hits_fos(proxy_server, caplog):
    with caplog.at_level(logging.WARNING, logger="backend.utils.telemetry_proxy"):
        await _proxy_call(
            proxy_server,
            "/k.parquet",
            headers={
                "X-Fos-Target": "bucket.s3.amazonaws.com",
                "X-Telemetry-Service-Id": "test-svc",
                "X-Telemetry-Context": "api:POST /api/dashboard/aggregates",
            },
        )
    assert any("dashboard context hitting FOS" in r.message for r in caplog.records)


async def test_proxy_synthesizes_fos_get_object_for_cdn_full_miss(proxy_server):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/k.log.gz",
        method="HEAD",
        headers={
            "X-Fos-Target": "cdn.example.net",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "fastly.api",
        },
        resp_headers={"X-Cache": "MISS, MISS"},
        chunks=(b"x" * 512,),
        cfg=MOCK_CDN_CFG,
    )
    assert len(rows) == 2 and {r["service"] for r in rows} == {"CDN", "FOS"}
    synth = next(r for r in rows if r["service"] == "FOS")
    assert (synth["method"], synth["caller"], synth["path"], synth["bytes"]) == (
        "GET_OBJECT",
        "cdn.miss",
        "/k.log.gz",
        512,
    )


@pytest.mark.parametrize("x_cache", ["HIT", "HIT, HIT", "MISS, HIT"])
async def test_proxy_does_not_synthesize_when_cdn_chain_has_a_hit(proxy_server, x_cache):
    _, _, _, rows, _ = await _proxy_call(
        proxy_server,
        "/k.parquet",
        headers={
            "X-Fos-Target": "cdn.example.net",
            "X-Telemetry-Service-Id": "test-svc",
            "X-Telemetry-Caller": "duckdb.httpfs",
        },
        resp_headers={"X-Cache": x_cache},
        chunks=(b"x" * 100,),
        cfg=MOCK_CDN_CFG,
    )
    assert len(rows) == 1 and rows[0]["service"] == "CDN"


async def test_proxy_writes_actually_persist_to_metadata_db(proxy_server, tmp_path, monkeypatch):
    monkeypatch.setattr("backend.core.metadata._DATA_DIR", str(tmp_path))
    ctx, _ = _mock_upstream(chunks=(b"x" * 100,), headers={"X-Cache": "MISS, MISS"})
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=MOCK_CDN_CFG),
        patch("backend.config.is_usage_logging_enabled", return_value=True),
        patch.object(telemetry_proxy._SESSION, "request", return_value=ctx),
    ):
        async with (
            aiohttp.ClientSession() as s,
            s.get(
                f"{proxy_server.proxy_endpoint()}/k",
                headers={
                    "X-Fos-Target": "cdn.example.net",
                    "X-Telemetry-Service-Id": "real-svc-task6",
                    "X-Telemetry-Caller": "duckdb.httpfs",
                },
            ) as resp,
        ):
            await resp.read()
        telemetry_proxy._flush_log_writes_for_tests()

    from backend.core.metadata import usage_log_db

    con = usage_log_db.get_con("real-svc-task6")
    rows = con.execute(
        "SELECT operation_class, operation_type, url, bytes FROM usage_log WHERE operation_class = 'CDN' ORDER BY id DESC LIMIT 1"
    ).fetchall()
    assert [(r["operation_class"], r["operation_type"], r["url"], r["bytes"]) for r in rows] == [
        ("CDN", "GET", "/k", 200)
    ]


async def test_reset_for_tests_stops_log_flusher(proxy_server):
    with patch("backend.core.metadata.log_usage_calls"):
        proxy_server._submit_log_write("test-svc", {"method": "GET"}, None)
        proxy_server._flush_log_writes_for_tests()
    flusher = proxy_server._LOG_FLUSHER_THREAD
    assert flusher is not None and flusher.is_alive()
    proxy_server.stop_proxy_server()
    proxy_server._reset_for_tests()
    assert not flusher.is_alive()


def _seed_moto_object(endpoint_url: str, bucket: str, key: str, body: bytes) -> None:
    seed = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name="us-east-1",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    seed.put_object(Bucket=bucket, Key=key, Body=body)


def _build_proxied_s3(proxy_url: str, host_port: str, caller: str):
    def _inject(request, **_kwargs):
        request.headers["X-Fos-Target"] = f"http://{host_port}"
        request.headers["X-Telemetry-Service-Id"] = "test-svc"
        request.headers["X-Telemetry-Caller"] = caller

    client = boto3.client(
        "s3",
        endpoint_url=proxy_url,
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
    )
    client.meta.events.register("before-send.s3.*", _inject)
    return client


async def test_proxy_preserves_range_get_byte_ranges(proxy_server, moto_s3_server):
    endpoint_url, host_port = moto_s3_server
    body = bytes(range(256)) * 4096
    _seed_moto_object(endpoint_url, "test-bucket", "range/big.bin", body)
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "range-test")
    telemetry_proxy._bust_config_cache()
    with patch("backend.config.load_config", return_value=MOCK_FOS_CFG):
        resp = proxied.get_object(Bucket="test-bucket", Key="range/big.bin", Range="bytes=100-1099")
        chunk = resp["Body"].read()
    assert len(chunk) == 1000 and chunk == body[100:1100] and resp["ResponseMetadata"]["HTTPStatusCode"] == 206


async def test_proxy_records_each_range_get_as_separate_row(proxy_server, moto_s3_server):
    endpoint_url, host_port = moto_s3_server
    _seed_moto_object(endpoint_url, "test-bucket", "range/three.bin", bytes(range(256)) * 4096)
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "range-multi")
    captured_rows: list[dict] = []
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=MOCK_FOS_CFG),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, rows, process_context=None: captured_rows.extend(rows),
        ),
    ):
        for start, end in [(0, 99), (100, 199), (200, 299)]:
            proxied.get_object(Bucket="test-bucket", Key="range/three.bin", Range=f"bytes={start}-{end}")["Body"].read()
        telemetry_proxy._flush_log_writes_for_tests()
    get_rows = [r for r in captured_rows if r["method"] == "GET"]
    assert len(get_rows) == 3 and all(r["bytes"] == 100 for r in get_rows)


async def test_proxy_records_multipart_put_per_part(proxy_server, moto_s3_server):
    _, host_port = moto_s3_server
    proxied = _build_proxied_s3(proxy_server.proxy_endpoint(), host_port, "multipart-test")
    captured_rows: list[dict] = []
    part_bytes = 5 * 1024 * 1024
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=MOCK_FOS_CFG),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, rows, process_context=None: captured_rows.extend(rows),
        ),
    ):
        upload_id = proxied.create_multipart_upload(Bucket="test-bucket", Key="multipart/big.bin")["UploadId"]
        parts = []
        for i, ch in enumerate([b"a" * part_bytes, b"b" * part_bytes, b"c" * part_bytes], start=1):
            r = proxied.upload_part(
                Bucket="test-bucket", Key="multipart/big.bin", UploadId=upload_id, PartNumber=i, Body=ch
            )
            parts.append({"ETag": r["ETag"], "PartNumber": i})
        proxied.complete_multipart_upload(
            Bucket="test-bucket", Key="multipart/big.bin", UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
        telemetry_proxy._flush_log_writes_for_tests()
    put_rows = [r for r in captured_rows if r["method"] == "PUT"]
    assert len(put_rows) == 3
    for n, p in zip([1, 2, 3], sorted(r["path"] for r in put_rows)):
        assert f"partNumber={n}" in p


async def test_proxy_streams_large_response_without_buffering(proxy_server):
    body_size, chunk_size = 50 * 1024 * 1024, 64 * 1024

    async def _lazy_chunks():
        chunk = b"\x00" * chunk_size
        for _ in range(body_size // chunk_size):
            yield chunk

    mock_resp = AsyncMock(status=200, headers={"Content-Length": str(body_size)})
    mock_resp.content.iter_chunked = lambda _size: _lazy_chunks()
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__.return_value = mock_resp
    mock_ctx.__aexit__.return_value = None

    received_bytes = 0
    with patch.object(telemetry_proxy._SESSION, "request", return_value=mock_ctx):
        async with aiohttp.ClientSession() as session:
            tracemalloc.start()
            try:
                async with session.get(
                    f"{proxy_server.proxy_endpoint()}/big.bin", headers={"X-Fos-Target": "fake.upstream.net"}
                ) as resp:
                    assert resp.status == 200
                    async for chunk in resp.content.iter_chunked(chunk_size):
                        received_bytes += len(chunk)
                _, peak = tracemalloc.get_traced_memory()
            finally:
                tracemalloc.stop()
    assert received_bytes == body_size and (peak / (1024 * 1024)) < 10


async def test_proxy_retries_idempotent_get_on_connection_error_and_succeeds(proxy_server):
    call_count = {"n": 0}
    ok_ctx, _ = _mock_upstream(status=200, chunks=(b"hello",))

    def _flaky(*_a, **_kw):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise aiohttp.ServerDisconnectedError("simulated pool race")
        return ok_ctx

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_flaky):
        async with (
            aiohttp.ClientSession() as s,
            s.get(f"{proxy_server.proxy_endpoint()}/some/key.avro", headers={"X-Fos-Target": "fake.cdn.net"}) as resp,
        ):
            assert resp.status == 200 and (await resp.read()) == b"hello"
    assert call_count["n"] == 2


async def test_proxy_does_not_retry_non_idempotent_method_on_connection_error(proxy_server):
    call_count = {"n": 0}

    def _always_fails(*_a, **_kw):
        call_count["n"] += 1
        raise aiohttp.ServerDisconnectedError("simulated pool race")

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_always_fails):
        async with (
            aiohttp.ClientSession() as s,
            s.put(
                f"{proxy_server.proxy_endpoint()}/some/key.avro",
                data=b"payload",
                headers={"X-Fos-Target": "fake.cdn.net"},
            ) as resp,
        ):
            assert resp.status == 502
    assert call_count["n"] == 1


async def test_proxy_retries_at_most_once_on_repeated_connection_errors(proxy_server):
    call_count = {"n": 0}

    def _always_fails(*_a, **_kw):
        call_count["n"] += 1
        raise aiohttp.ServerDisconnectedError("upstream genuinely down")

    with patch.object(telemetry_proxy._SESSION, "request", side_effect=_always_fails):
        async with (
            aiohttp.ClientSession() as s,
            s.get(f"{proxy_server.proxy_endpoint()}/some/key.avro", headers={"X-Fos-Target": "fake.cdn.net"}) as resp,
        ):
            assert resp.status == 502
    assert call_count["n"] == 2


async def test_client_disconnect_mid_stream_is_not_logged_as_upstream_502(proxy_server):
    captured: list[dict] = []
    ok_ctx, _ = _mock_upstream(status=200, chunks=(b"chunk1", b"chunk2", b"chunk3"))
    write_count = {"n": 0}
    real_write = web.StreamResponse.write

    async def _flaky_write(self, data):
        write_count["n"] += 1
        if write_count["n"] >= 2:
            raise ConnectionResetError("Cannot write to closing transport")
        return await real_write(self, data)

    with (
        patch.object(telemetry_proxy._SESSION, "request", return_value=ok_ctx),
        patch.object(web.StreamResponse, "write", _flaky_write),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, rows, process_context=None: captured.extend(rows),
        ),
    ):
        async with aiohttp.ClientSession() as s:
            try:
                async with s.get(
                    f"{proxy_server.proxy_endpoint()}/some/key.avro",
                    headers={"X-Fos-Target": "fake.cdn.net", "X-Telemetry-Service-Id": "test-svc"},
                ) as resp:
                    try:
                        await resp.read()
                    except aiohttp.ClientError:
                        pass
            except aiohttp.ClientError:
                pass
        telemetry_proxy._flush_log_writes_for_tests()

    proxy_rows = [r for r in captured if r.get("service") in ("FOS", "CDN")]
    assert len(proxy_rows) >= 1 and proxy_rows[0]["status"] == "OK"


def test_proxy_connector_caps_keepalive_below_fastly_default():
    telemetry_proxy._reset_for_tests()
    telemetry_proxy.start_proxy_server()
    try:
        assert telemetry_proxy._SESSION is not None and telemetry_proxy._SESSION.connector is not None
        assert telemetry_proxy._SESSION.connector._keepalive_timeout < 5.0
    finally:
        telemetry_proxy.stop_proxy_server()
        telemetry_proxy._reset_for_tests()


@pytest.mark.parametrize(
    ("target", "cdn", "fos", "expected"),
    [
        ("127.0.0.1", None, None, True),
        ("127.0.0.1:9999", None, None, True),
        ("localhost", None, None, True),
        ("localhost:8080", None, None, True),
        ("::1", None, None, True),
        ("cdn.example.net", "cdn.example.net", "us-east-1.object.fastlystorage.app", True),
        ("cdn.example.org", "cdn.example.net", "us-east-1.object.fastlystorage.app", False),
        ("us-east-1.object.fastlystorage.app", None, "us-east-1.object.fastlystorage.app", True),
        ("eu-west-1.object.fastlystorage.app", None, "us-east-1.object.fastlystorage.app", False),
        ("evil.example.com", "cdn.example.net", "us-east-1.object.fastlystorage.app", False),
        ("169.254.169.254", "cdn.example.net", "us-east-1.object.fastlystorage.app", False),
        ("CDN.EXAMPLE.NET", "cdn.example.net", "us-east-1.object.fastlystorage.app", True),
        ("US-EAST-1.OBJECT.FASTLYSTORAGE.APP", None, "us-east-1.object.fastlystorage.app", True),
    ],
)
def test_is_target_host_allowed(target, cdn, fos, expected):
    assert telemetry_proxy._is_target_host_allowed(target, cdn, fos) is expected


async def test_proxy_rejects_disallowed_x_fos_target_with_400(proxy_server, caplog):
    mock_cfg = {**MOCK_CDN_CFG, "fos_native_endpoint": "us-east-1.object.fastlystorage.app"}
    with caplog.at_level("WARNING", logger=telemetry_proxy.logger.name):
        st, body, req, _, _ = await _proxy_call(
            proxy_server,
            "/some/path",
            headers={"X-Fos-Target": "evil.example.com", "X-Telemetry-Service-Id": "test-svc"},
            cfg=mock_cfg,
        )
    assert st == 400 and "not allowed" in body.decode().lower() and not req
    assert any(
        "rejected disallowed X-Fos-Target" in rec.message and "evil.example.com" in rec.message
        for rec in caplog.records
    )


async def test_proxy_allows_fos_target_matching_configured_fos_endpoint(proxy_server):
    mock_cfg = {**MOCK_FOS_CFG, "fos_endpoint": "us-east-1.object.fastlystorage.app"}
    st, _, req, _, _ = await _proxy_call(
        proxy_server,
        "/k",
        headers={"X-Fos-Target": "us-east-1.object.fastlystorage.app", "X-Telemetry-Service-Id": "test-svc"},
        cfg=mock_cfg,
    )
    assert st == 200 and "Authorization" in req["headers"]


async def test_proxy_skips_gate_when_no_service_id(proxy_server):
    st, _, req, _, _ = await _proxy_call(proxy_server, "/x", headers={"X-Fos-Target": "fake.cdn.net"}, chunks=(b"ok",))
    assert st == 200 and req


def _make_hooked_boto3_client(proxy_server, moto_s3_server, service_id: str):
    _, host_port = moto_s3_server
    source = {"name": service_id, "service_id": service_id, "fos_native_endpoint": f"http://{host_port}"}
    client = boto3.client(
        "s3",
        endpoint_url=proxy_server.proxy_endpoint(),
        region_name="us-east-1",
        config=Config(signature_version=UNSIGNED, s3={"addressing_style": "path"}),
    )
    telemetry_proxy.install_boto3_proxy_hook(client, source)
    return client, MOCK_FOS_CFG


async def test_install_boto3_proxy_hook_routes_request_through_proxy(proxy_server, moto_s3_server):
    client, mock_cfg = _make_hooked_boto3_client(proxy_server, moto_s3_server, "phase2-test")
    captured_rows: list[dict] = []
    captured_service: list[str] = []

    def _capture(service_id, rows, process_context=None):
        captured_service.append(service_id)
        captured_rows.extend(rows)

    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=mock_cfg),
        patch("backend.core.metadata.log_usage_calls", side_effect=_capture),
    ):
        resp = client.head_bucket(Bucket="test-bucket")
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200
        telemetry_proxy._flush_log_writes_for_tests()
    assert len(captured_rows) == 1 and captured_service == ["phase2-test"]
    assert captured_rows[0]["method"] == "HEAD" and captured_rows[0]["caller"] == "boto3.headbucket"


async def test_install_boto3_proxy_hook_passes_process_context_per_request(proxy_server, moto_s3_server):
    client, mock_cfg = _make_hooked_boto3_client(proxy_server, moto_s3_server, "phase2-ctx")
    contexts_seen: list[str | None] = []
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=mock_cfg),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, _r, process_context=None: contexts_seen.append(process_context),
        ),
    ):
        tlm._set_process_context_for_tests("cron:sync:phase2-ctx-A")
        client.head_bucket(Bucket="test-bucket")
        tlm._set_process_context_for_tests("cron:compact:phase2-ctx-B")
        client.head_bucket(Bucket="test-bucket")
        telemetry_proxy._flush_log_writes_for_tests()
        tlm._set_process_context_for_tests(None)
    assert contexts_seen == ["cron:sync:phase2-ctx-A", "cron:compact:phase2-ctx-B"]


async def test_install_boto3_proxy_hook_falls_back_to_thread_name_when_context_unset(proxy_server, moto_s3_server):
    client, mock_cfg = _make_hooked_boto3_client(proxy_server, moto_s3_server, "phase2-fallback")
    contexts_seen: list[str | None] = []
    tlm._PROCESS_CONTEXT.set(None)
    with tlm._LATEST_PROCESS_CONTEXT_LOCK:
        tlm._LATEST_PROCESS_CONTEXT = None
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=mock_cfg),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, _r, process_context=None: contexts_seen.append(process_context),
        ),
    ):
        client.head_bucket(Bucket="test-bucket")
        telemetry_proxy._flush_log_writes_for_tests()
    assert len(contexts_seen) == 1 and contexts_seen[0] is not None and contexts_seen[0].startswith("untagged:")


class _FakeBoto3Client:
    def __init__(self):
        self._handler = None
        self.meta = self
        self.events = self

    def register(self, _signal: str, handler):
        self._handler = handler

    def invoke(self, *, method: str, event_name: str, url: str = ""):
        req = SimpleNamespace(method=method, headers={}, url=url)
        assert self._handler is not None
        self._handler(request=req, event_name=event_name)
        return req.headers


def _cdn_source():
    return {
        "name": "streamj",
        "service_id": "streamj",
        "fos_native_endpoint": "us-east.object.fastlystorage.app",
        "cdn_url": "https://fos-streamj-logs.global.ssl.fastly.net",
        "cdn_secret": "cdn-secret-abc",
    }


@pytest.mark.parametrize(
    ("method", "event_name", "url", "expected_target", "expect_fastly_key"),
    [
        ("GET", "before-send.s3.GetObject", "", "fos-streamj-logs.global.ssl.fastly.net", True),
        (
            "GET",
            "before-send.s3.GetObject",
            "http://127.0.0.1:8000/bucket/raw/request/year=2026/file.gz",
            "fos-streamj-logs.global.ssl.fastly.net",
            True,
        ),
        ("HEAD", "before-send.s3.HeadObject", "", "fos-streamj-logs.global.ssl.fastly.net", True),
        ("GET", "before-send.s3.ListObjectsV2", "", "us-east.object.fastlystorage.app", False),
        ("PUT", "before-send.s3.PutObject", "", "us-east.object.fastlystorage.app", False),
        ("POST", "before-send.s3.DeleteObjects", "", "us-east.object.fastlystorage.app", False),
    ],
)
def test_install_boto3_proxy_hook_routing_by_operation(method, event_name, url, expected_target, expect_fastly_key):
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    headers = client.invoke(method=method, event_name=event_name, url=url)
    assert headers["X-Fos-Target"] == expected_target
    assert (headers.get("x-fastly-key") == "cdn-secret-abc") if expect_fastly_key else ("x-fastly-key" not in headers)


def test_install_boto3_proxy_hook_ingest_download_uses_cdn():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(client, _cdn_source())
    token = telemetry_proxy._BOTO3_CALLER_HINT.set("ingest_download")
    try:
        headers = client.invoke(method="GET", event_name="before-send.s3.GetObject")
        assert headers["X-Fos-Target"] == "fos-streamj-logs.global.ssl.fastly.net"
        assert headers["x-fastly-key"] == "cdn-secret-abc"
    finally:
        telemetry_proxy._BOTO3_CALLER_HINT.reset(token)


def test_install_boto3_proxy_hook_get_with_no_cdn_url_stays_native():
    client = _FakeBoto3Client()
    telemetry_proxy.install_boto3_proxy_hook(
        client,
        {
            "name": "streamj-nocdn",
            "service_id": "streamj-nocdn",
            "fos_native_endpoint": "us-east.object.fastlystorage.app",
        },
    )
    headers = client.invoke(method="GET", event_name="before-send.s3.GetObject")
    assert headers["X-Fos-Target"] == "us-east.object.fastlystorage.app" and "x-fastly-key" not in headers


def test_sign_request_adds_content_md5_for_bulk_delete(monkeypatch):
    monkeypatch.setattr(telemetry_proxy, "_load_config_cached", lambda sid: MOCK_FOS_CFG)
    signed = telemetry_proxy._sign_request(
        "POST",
        "https://us-east-1.object.fastlystorage.app/my-bucket?delete",
        {},
        b"<Delete><Object><Key>foo</Key></Object></Delete>",
        "svc-test",
    )
    assert ("Content-MD5" in signed or "content-md5" in signed) and "Authorization" in signed


def _get_fos_secret(source: dict, *, run_s3_cfg: bool = False):
    con = duckdb.connect(":memory:")
    try:
        _ddb._configure_fos(con, source)
        if run_s3_cfg:
            _ic.configure_duckdb_s3(con)
        rows = con.execute("SELECT name, type, secret_string FROM duckdb_secrets() WHERE name='fos_proxy'").fetchall()
        ep = con.execute("SELECT current_setting('s3_endpoint')").fetchone()[0] if run_s3_cfg else None
        return rows, ep
    finally:
        con.close()


def test_configure_fos_creates_proxy_secret_no_cdn(proxy_server):
    rows, _ = _get_fos_secret(
        {
            "name": "phase3a-no-cdn",
            "service_id": "phase3a-no-cdn",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "us-east-1.object.fastlystorage.app",
            "access_key_id": "AKIA-phase3a",
            "secret_access_key": "secret-phase3a",
            "region": "us-east-1",
        }
    )
    assert len(rows) == 1 and rows[0][1].upper() == "S3"
    sec = rows[0][2]
    assert proxy_server.proxy_endpoint().replace("http://", "") in sec
    for req in (
        "X-Fos-Target=us-east-1.object.fastlystorage.app",
        "X-Telemetry-Service-Id=phase3a-no-cdn",
        "X-Telemetry-Caller=duckdb.httpfs",
    ):
        assert req in sec
    assert "x-fastly-key" not in sec.lower()


def test_configure_fos_creates_proxy_secret_with_cdn(proxy_server):
    rows, _ = _get_fos_secret(
        {
            "name": "phase3a-cdn",
            "service_id": "phase3a-cdn",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "cdn.example.com",
            "cdn_url": "https://cdn.example.com",
            "cdn_secret": "fastly-secret-abc",
            "access_key_id": "AKIA-phase3a",
            "secret_access_key": "secret-phase3a",
            "region": "us-east-1",
        }
    )
    sec = rows[0][2]
    assert "X-Fos-Target=cdn.example.com" in sec
    assert "us-east-1.object.fastlystorage.app" not in sec
    assert "x-fastly-key=fastly-secret-abc" in sec


def test_configure_fos_uses_native_fos_when_edge_only_is_false(proxy_server):
    rows, _ = _get_fos_secret(
        {
            "name": "phase3a-native-reads",
            "service_id": "phase3a-native-reads",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "cdn.example.com",
            "cdn_url": "https://cdn.example.com",
            "cdn_secret": "fastly-secret-abc",
            "provisioning": {"edge_only": False},
            "access_key_id": "AKIA-phase3a",
            "secret_access_key": "secret-phase3a",
            "region": "us-east-1",
        }
    )
    sec = rows[0][2]
    assert "X-Fos-Target=us-east-1.object.fastlystorage.app" in sec
    assert "X-Fos-Target=cdn.example.com" not in sec
    assert "x-fastly-key" not in sec.lower()


def test_configure_duckdb_s3_does_not_clobber_proxy_secret(proxy_server):
    rows, ep = _get_fos_secret(
        {
            "name": "phase3a-icestack",
            "service_id": "phase3a-icestack",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "us-east-1.object.fastlystorage.app",
            "access_key_id": "AKIA-test",
            "secret_access_key": "secret-test",
            "region": "us-east-1",
        },
        run_s3_cfg=True,
    )
    assert len(rows) == 1 and proxy_server.proxy_endpoint().replace("http://", "") in rows[0][2]
    assert ep in ("", None)


async def test_duckdb_read_through_proxy_logs_telemetry(proxy_server, moto_s3_server, tmp_path):
    endpoint_url, host_port = moto_s3_server
    buf = io.BytesIO()
    pq.write_table(pa.table({"x": list(range(8))}), buf)
    _seed_moto_object(endpoint_url, "test-bucket", "hello.parquet", buf.getvalue())

    source = {
        "name": "phase3a-e2e",
        "service_id": "phase3a-e2e",
        "fos_native_endpoint": f"http://{host_port}",
        "endpoint": f"http://{host_port}",
        "access_key_id": "testing",
        "secret_access_key": "testing",
        "region": "us-east-1",
        "duckdb_path": str(tmp_path / "phase3a_e2e.duckdb"),
    }
    captured_rows: list[dict] = []
    telemetry_proxy._bust_config_cache()
    with (
        patch("backend.config.load_config", return_value=MOCK_FOS_CFG),
        patch(
            "backend.core.metadata.log_usage_calls",
            side_effect=lambda _s, rows, process_context=None: captured_rows.extend(rows),
        ),
    ):
        con = _ddb.get_connection(source, skip_view_update=True)
        try:
            (count,) = con.execute("SELECT count(*) FROM read_parquet('s3://test-bucket/hello.parquet')").fetchone()
            assert count == 8
        finally:
            con.close()
        telemetry_proxy._flush_log_writes_for_tests()

    duckdb_rows = [r for r in captured_rows if r.get("caller") == "duckdb.httpfs"]
    assert any("test-bucket" in (r.get("path") or "") and "hello.parquet" in (r.get("path") or "") for r in duckdb_rows)


def test_configure_fos_concurrent_no_write_write_conflict(proxy_server, tmp_path):
    db_path = str(tmp_path / "concurrent_fos.duckdb")
    source = {
        "name": "phase3a-concurrent",
        "service_id": "phase3a-concurrent",
        "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
        "endpoint": "us-east-1.object.fastlystorage.app",
        "access_key_id": "AKIA-concurrent",
        "secret_access_key": "secret-concurrent",
        "region": "us-east-1",
    }
    errors: list[Exception] = []

    def _worker(_i: int) -> None:
        try:
            con = duckdb.connect(db_path)
            try:
                _ddb._configure_fos(con, source)
            finally:
                con.close()
        except Exception as e:
            errors.append(e)

    with duckdb.connect(db_path) as _keep_attached, ThreadPoolExecutor(max_workers=16) as ex:
        list(ex.map(_worker, range(32)))
    assert not errors


def test_fastapi_lifespan_starts_proxy():
    from backend import main as _main

    called = {"n": 0}
    with patch(
        "backend.utils.telemetry_proxy.start_proxy_server", side_effect=lambda: called.__setitem__("n", called["n"] + 1)
    ):

        async def _drive():
            async with _main.lifespan(_main.app):
                pass

        asyncio.run(_drive())
    assert called["n"] >= 1


def test_get_fos_client_paginator_accepts_caller_hint():
    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()
    client = _ddb._get_fos_client(
        {
            "name": "phase4-caller-hint",
            "service_id": "phase4-caller-hint",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "https://us-east-1.object.fastlystorage.app",
            "access_key_id": "test",
            "secret_access_key": "test",
        }
    )
    pag = client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
    assert pag is not None and hasattr(pag, "paginate")


def test_get_fos_client_paginator_caller_hint_flows_to_proxy_header():
    with _ddb._fos_client_lock:
        _ddb._fos_client_cache.clear()
    client = _ddb._get_fos_client(
        {
            "name": "phase4-caller-hint-header",
            "service_id": "phase4-caller-hint-header",
            "fos_native_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "https://us-east-1.object.fastlystorage.app",
        }
    )
    pag = client.get_paginator("list_objects_v2", caller_hint="ingest_scan")
    captured = []
    req = SimpleNamespace(headers={})
    if hasattr(pag, "_set_caller_hint_context"):
        token = pag._set_caller_hint_context()
        try:
            client.meta.events.emit("before-send.s3.ListObjectsV2", request=req)
            captured.append(req.headers.get("X-Telemetry-Caller"))
        finally:
            pag._reset_caller_hint_context(token)
    else:
        token = telemetry_proxy._BOTO3_CALLER_HINT.set("ingest_scan")
        try:
            client.meta.events.emit("before-send.s3.ListObjectsV2", request=req)
            captured.append(req.headers.get("X-Telemetry-Caller"))
        finally:
            telemetry_proxy._BOTO3_CALLER_HINT.reset(token)
    assert captured == ["ingest_scan"]
    req2 = SimpleNamespace(headers={})
    client.meta.events.emit("before-send.s3.ListObjectsV2", request=req2)
    assert req2.headers.get("X-Telemetry-Caller") == "boto3.listobjectsv2"


def test_upstream_timeout_default_is_bounded():
    total = telemetry_proxy._UPSTREAM_TIMEOUT.total
    assert total is not None and 0 < total <= 120


def test_upstream_timeout_env_var_override():
    original = os.environ.get("FOS_PROXY_UPSTREAM_TIMEOUT_S")
    try:
        os.environ["FOS_PROXY_UPSTREAM_TIMEOUT_S"] = "37"
        importlib.reload(telemetry_proxy)
        assert telemetry_proxy._UPSTREAM_TIMEOUT.total == 37.0
    finally:
        if original is None:
            os.environ.pop("FOS_PROXY_UPSTREAM_TIMEOUT_S", None)
        else:
            os.environ["FOS_PROXY_UPSTREAM_TIMEOUT_S"] = original
        importlib.reload(telemetry_proxy)
