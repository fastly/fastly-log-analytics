"""Consolidated tests for bounded_cache, cache_registry, log_throttle, rate_limit, date_utils, active_requests, sqlite_profiler, structlog_config, system_jobs, and testcontainers_smoke."""

from __future__ import annotations

import ast
import json
import logging
import math
import os
import re
import shutil
import socket
import threading
import time
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import structlog
from backend.utils.active_requests import (
    _reset_for_tests,
    active_request_count,
    decrement_active_requests,
    increment_active_requests,
    should_defer_cron,
    yield_to_api,
)
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import (
    _parse_dt,
    ensure_utc,
    iso_z,
    iso_z_now,
    parse_date_window,
    parse_day_key_strict,
    parse_hour_key,
    parse_hour_key_strict,
    parse_iso_utc,
    parse_relative_time_window,
    parse_window,
    parse_window_str_to_dt,
    safe_iso,
    window_to_epoch,
)
from backend.utils.time_window import (
    _BANDS,
    ANCHOR_QUANTUM_SECONDS,
    STATIC_DEFAULT,
    VALID_RANGE_TOKENS,
    history_hours_from_earliest,
    invite_clamp_fingerprint,
    is_valid_range_token,
    pick_insights_default,
    quantize_anchor,
    resolve_window,
)
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from backend.core import query_registry
from backend.core.metadata import base, pg_connection
from backend.core.rollups._common import parse_hour_token
from backend.cron import decorators as system_jobs
from backend.utils import active_requests, remote_access, sqlite_profiler, structlog_config, tunnel
from backend.utils import telemetry as log_throttle
from backend.utils.telemetry import _SQLITE_QUERIES, log_once_per
from backend.utils.tunnel import manager as _session_mod

ra = remote_access

if hasattr(_session_mod, "MAX_TRACKED_IPS"):
    login_mod = _session_mod
else:
    from backend.utils.tunnel import rate_limiter as login_mod

ROOT = Path(__file__).resolve().parents[2]
_NOW = datetime(2026, 6, 29, 12, 30, 45, tzinfo=UTC)
_INSIGHTS_NOW = datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC)
_TS_SOURCE = ROOT / "frontend" / "lib" / "utils.ts"
_HOUR_KEYS = ["2026-10-19-00", "2026-10-19-23", "2026-02-28-23", "2024-02-29-12", "2026-1-5-3"]
_BAD_KEYS = ["", "2026-10-19", "2026-10-19-24", "2026-13-01-00", "garbage", "2026-10-19T05", "2026-10-19-05-00"]
_DROP_TABLE_FILES = sorted([*(ROOT / "backend" / "repositories").glob("*.py"), ROOT / "backend" / "core" / "duckdb.py"])


@pytest.fixture(autouse=True)
def _auto_isolate_all(monkeypatch):
    snapshot_names = CacheRegistry.names()
    monkeypatch.setattr(log_throttle, "_last", {})
    _reset_for_tests()
    sqlite_profiler.clear()
    structlog.reset_defaults()
    with system_jobs._status_lock:
        system_jobs._status.clear()
    yield
    for n in CacheRegistry.names():
        if n not in snapshot_names:
            with CacheRegistry._lock:
                CacheRegistry._entries.pop(n, None)
    _reset_for_tests()
    sqlite_profiler.clear()
    structlog.reset_defaults()


# ── BoundedTTLCache ──────────────────────────────────────────────────────────


def test_basic_set_get_and_miss():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    cache["k1"] = "v1"
    assert cache["k1"] == "v1" and cache.get("k1") == "v1" and "k1" in cache
    assert cache.get("missing") is None and cache.get("missing", "sentinel") == "sentinel"
    assert "missing" not in cache
    with pytest.raises(KeyError):
        _ = cache["missing"]


def test_ttl_expiry_treats_entry_as_absent():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=0.05)
    cache["k1"] = "v1"
    assert cache["k1"] == "v1"
    time.sleep(0.10)
    assert "k1" not in cache and cache.get("k1") is None
    with pytest.raises(KeyError):
        _ = cache["k1"]


def test_maxsize_evicts_lru_and_read_promotes():
    cache = BoundedTTLCache(maxsize=3, ttl_seconds=60)
    cache["a"], cache["b"], cache["c"] = 1, 2, 3
    assert cache["a"] == 1
    cache["d"] = 4
    assert "b" not in cache and (cache["a"], cache["c"], cache["d"], len(cache)) == (1, 3, 4, 3)

    c2 = BoundedTTLCache(maxsize=2, ttl_seconds=60)
    c2["warm"], c2["cold"] = "stays", "dies"
    for _ in range(5):
        _ = c2["warm"]
    c2["new"] = "added"
    assert "cold" not in c2 and c2["warm"] == "stays" and c2["new"] == "added"


def test_pop_and_clear():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    cache["k1"] = "v1"
    assert cache.pop("k1") == "v1" and "k1" not in cache
    assert cache.pop("missing", "default") == "default"
    with pytest.raises(KeyError):
        cache.pop("missing")

    for i in range(5):
        cache[f"k{i}"] = i
    assert len(cache) == 5
    cache.clear()
    assert len(cache) == 0 and all(f"k{i}" not in cache for i in range(5))


def test_lazy_reap_fires_after_n_writes():
    cache = BoundedTTLCache(maxsize=10_000, ttl_seconds=0.05)
    for i in range(50):
        cache[f"expired_{i}"] = i
    time.sleep(0.40)
    for i in range(100):
        cache[f"fresh_{i}"] = i
    assert all(f"expired_{i}" not in cache for i in range(50))
    assert all(cache[f"fresh_{i}"] == i for i in range(100))


def test_iteration_snapshot_invalid_args_and_tuple_values():
    cache = BoundedTTLCache(maxsize=100, ttl_seconds=60)
    for i in range(20):
        cache[f"k{i}"] = i
    keys = list(cache)
    cache["fresh"] = "added"
    assert len(keys) == 20 and "fresh" not in keys

    for maxsize, ttl in ((0, 10), (-1, 10), (10, 0), (10, -0.5)):
        with pytest.raises(ValueError):
            BoundedTTLCache(maxsize=maxsize, ttl_seconds=ttl)

    payload = (1.23, {"rows": [1, 2, 3], "_meta": "x"})
    cache["k"] = payload
    assert cache["k"] == payload and cache["k"] is payload


def test_thread_safety_under_concurrent_writes():
    cache = BoundedTTLCache(maxsize=200, ttl_seconds=60)
    errors: list[Exception] = []

    def writer(tid: int) -> None:
        try:
            for i in range(500):
                cache[f"t{tid}_k{i}"] = i
        except Exception as exc:
            errors.append(exc)

    def reader() -> None:
        try:
            for _ in range(500):
                for k in cache:
                    _ = cache.get(k)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)] + [
        threading.Thread(target=reader) for _ in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and len(cache) <= 200


# ── CacheRegistry ────────────────────────────────────────────────────────────


def test_cache_registry_register_and_clear():
    d: dict[str, int] = {"x": 1, "y": 2}
    s: set[str] = {"a", "b"}

    class Box:
        def __init__(self):
            self.items = [1, 2, 3]
            self.cleared = False

        def clear(self):
            self.items = []
            self.cleared = True

    b = Box()
    CacheRegistry.register("test.dict_a", d)
    CacheRegistry.register("test.set_a", s)
    CacheRegistry.register("test.box", b)
    CacheRegistry.register("test.bare_int", 42)
    CacheRegistry.clear_all()
    assert d == {} and s == set() and b.cleared is True and b.items == []


def test_iceberg_caches_are_registered_at_import():
    import backend.core.duckdb  # noqa: F401
    import backend.core.iceberg  # noqa: F401
    import backend.repositories.dashboard  # noqa: F401
    import backend.repositories.network  # noqa: F401

    names = set(CacheRegistry.names())
    for required in (
        "iceberg._view_cache",
        "dashboard._dashboard_cache",
        "network._response_cache",
        "duckdb._fos_client_cache",
        "duckdb._initialized_paths",
        "core._duckdb_status._schema_cache",
    ):
        assert required in names, f"missing registration: {required}"


# ── Log throttle ─────────────────────────────────────────────────────────────


def test_log_throttle_interval_independence_and_bounding(monkeypatch, caplog):
    t = [100.0]
    monkeypatch.setattr(log_throttle.time, "monotonic", lambda: t[0])
    assert log_once_per("k", 60) is True
    t[0] += 59
    assert log_once_per("k", 60) is False
    t[0] += 2
    assert log_once_per("k", 60) is True

    assert log_once_per("a", 60) is True and log_once_per("b", 60) is True and log_once_per("a", 60) is False

    monkeypatch.setattr(log_throttle, "_MAX_KEYS", 8)
    for i in range(100):
        log_once_per(f"k{i}", 3600)
    assert len(log_throttle._last) <= 8

    logger = logging.getLogger("t")
    with caplog.at_level(logging.DEBUG, logger="t"):
        for _ in range(500):
            try:
                raise ValueError("bad row")
            except ValueError:
                if log_once_per("row-parse", 60):
                    logger.debug("row parse failed", exc_info=True)
    assert len([r for r in caplog.records if r.exc_info]) == 1


def _unthrottled(path: str, only_funcs: set[str] | None, only_msg: str | None) -> list[str]:
    tree = ast.parse((ROOT / path).read_text())
    out: list[str] = []

    def walk(node: ast.AST, guarded: bool, func: str | None) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            func = node.name
        if (
            isinstance(node, ast.If)
            and isinstance(node.test, ast.Call)
            and getattr(node.test.func, "id", "") == "log_once_per"
        ):
            for child in node.body:
                walk(child, True, func)
            for child in node.orelse:
                walk(child, guarded, func)
            return
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr == "debug"
            and any(k.arg == "exc_info" for k in node.keywords)
            and not guarded
        ):
            msg = node.args[0].value if node.args and isinstance(node.args[0], ast.Constant) else ""
            if (only_funcs is None or func in only_funcs) and (only_msg is None or only_msg in str(msg)):
                out.append(f"{path}:{node.lineno}")
        for child in ast.iter_child_nodes(node):
            walk(child, guarded, func)

    walk(tree, False, None)
    return out


@pytest.mark.parametrize(
    ("path", "funcs", "msg"),
    [
        ("backend/core/duckdb_pool.py", None, None),
        ("backend/core/ingest.py", None, "_parse_rum_line"),
        ("backend/core/rum_ingest.py", {"extract_metrics_from_faro_payload"}, None),
        ("backend/repositories/dashboard.py", None, "analyst"),
        *[(p.relative_to(ROOT).as_posix(), None, "DROP TABLE") for p in _DROP_TABLE_FILES],
    ],
)
def test_hot_path_debug_tracebacks_are_throttled(path, funcs, msg):
    assert _unthrottled(path, funcs, msg) == []


# ── Rate limit characterization ──────────────────────────────────────────────


class Clock:
    def __init__(self, start: float = 1_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch):
    c = Clock()
    monkeypatch.setattr(time, "time", c)
    return c


@pytest.mark.security_regression
def test_quota_request_boundary_is_exact(clock):
    lim = ra._StaticAssetLimiter(req_limit=3)
    assert [lim.check("1.1.1.1", 0) for _ in range(3)] == [True, True, True]
    assert lim.check("1.1.1.1", 0) is False
    assert lim.check("1.1.1.1", 0) is False
    assert lim.check("2.2.2.2", 0) is True


@pytest.mark.security_regression
def test_quota_window_expiry_frees_budget(clock):
    lim = ra._StaticAssetLimiter(req_limit=2, window_s=60)
    assert lim.check("ip", 0) and lim.check("ip", 0) and not lim.check("ip", 0)
    clock.now += 59
    assert lim.check("ip", 0) is False
    clock.now += 1
    assert lim.check("ip", 0) is False
    clock.now += 1
    assert lim.check("ip", 0) is True


@pytest.mark.security_regression
def test_quota_byte_boundary_counts_the_request_that_crosses_it(clock):
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=1000)
    assert lim.check("ip", 1000) is True
    assert lim.check("ip", 1) is False and lim.check("ip", 0) is False
    clock.now += 61
    assert lim.check("ip", 1000) is True


@pytest.mark.security_regression
def test_quota_negative_content_length_counts_as_zero(clock):
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=10)
    assert all(lim.check("ip", -999) for _ in range(5))


@pytest.mark.security_regression
def test_quota_request_limit_hit_does_not_record_bytes(clock):
    lim = ra._StaticAssetLimiter(req_limit=1, byte_limit=100)
    assert lim.check("ip", 100) is True and lim.check("ip", 100_000) is False
    clock.now += 61
    assert lim.check("ip", 100) is True


def test_quota_defaults_and_stale_key_eviction(clock):
    lim = ra._StaticAssetLimiter()
    assert (lim.REQ_LIMIT, lim.BYTE_LIMIT, lim.window_s, lim.MAX_TRACKED_IPS) == (600, 50 * 1024 * 1024, 60, 10_000)
    assert ra._static_limiter is not ra._analyst_api_limiter

    lim2 = ra._StaticAssetLimiter(max_tracked_ips=5)
    for i in range(5):
        lim2.check(f"old-{i}", 1)
    clock.now += 120
    for i in range(3):
        lim2.check(f"new-{i}", 1)
    assert len(lim2._events) <= 5 and all(lim2.check(f"new-{i}", 1) for i in range(3))


@pytest.mark.security_regression
def test_quota_memory_is_bounded_under_key_flood(clock):
    lim = ra._StaticAssetLimiter(max_tracked_ips=50)
    for i in range(5000):
        lim.check(f"10.0.{i // 256}.{i % 256}", 10)
    assert len(lim._events) <= 50


def test_login_defaults_snapshot_and_lru(monkeypatch, clock):
    assert (login_mod.LOGIN_FAILURE_WINDOW_S, login_mod.LOGIN_FAILURE_THRESHOLD, login_mod.LOGIN_LOCKOUT_S) == (
        60,
        5,
        300,
    )
    assert login_mod.MAX_TRACKED_IPS == 10000

    rl = tunnel._LoginRateLimiter()
    rl.record_failure("lonely")
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("locked")
    snap = rl.snapshot()
    by_ip = {r["ip"]: r for r in snap["failures"]}
    assert by_ip["lonely"] == {"ip": "lonely", "count": 1, "window_s": tunnel.LOGIN_FAILURE_WINDOW_S}
    assert by_ip["locked"]["count"] == tunnel.LOGIN_FAILURE_THRESHOLD
    assert snap["lockouts"] == [{"ip": "locked", "remaining_s": tunnel.LOGIN_LOCKOUT_S}]
    clock.now += tunnel.LOGIN_LOCKOUT_S + 1
    assert rl.snapshot() == {"failures": [], "lockouts": []}

    monkeypatch.setattr(login_mod, "MAX_TRACKED_IPS", 3)
    rl2 = tunnel._LoginRateLimiter()
    for ip in ("a", "b", "c", "a", "d"):
        rl2.record_failure(ip)
    assert {r["ip"] for r in rl2.snapshot()["failures"]} == {"a", "c", "d"}


@pytest.mark.security_regression
def test_login_lockout_triggers_exactly_at_threshold(clock):
    rl = tunnel._LoginRateLimiter()
    results = [rl.record_failure("1.2.3.4") for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD)]
    assert results == [False] * (tunnel.LOGIN_FAILURE_THRESHOLD - 1) + [True]
    assert rl.record_failure("1.2.3.4") is False
    locked, remaining = rl.is_locked("1.2.3.4")
    assert locked and remaining == tunnel.LOGIN_LOCKOUT_S


@pytest.mark.security_regression
def test_login_failures_outside_window_do_not_accumulate(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD - 1):
        rl.record_failure("ip")
        clock.now += tunnel.LOGIN_FAILURE_WINDOW_S + 1
    assert rl.record_failure("ip") is False and rl.is_locked("ip") == (False, 0)


@pytest.mark.security_regression
def test_login_lockout_expiry_unlocks_and_remaining_counts_down(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("ip")
    clock.now += 100
    locked, remaining = rl.is_locked("ip")
    assert locked and remaining == tunnel.LOGIN_LOCKOUT_S - 100
    clock.now += tunnel.LOGIN_LOCKOUT_S
    assert rl.is_locked("ip") == (False, 0)


@pytest.mark.security_regression
def test_login_keys_are_isolated(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("a")
    assert rl.is_locked("a")[0] is True and rl.is_locked("b") == (False, 0)


@pytest.mark.security_regression
def test_login_memory_is_bounded_under_key_flood(monkeypatch, clock):
    monkeypatch.setattr(login_mod, "MAX_TRACKED_IPS", 20)
    rl = tunnel._LoginRateLimiter()
    for i in range(2000):
        rl.record_failure(f"ip-{i}")
    assert len(rl.snapshot()["failures"]) <= 20


@pytest.mark.security_regression
def test_quota_never_admits_more_than_limit_under_contention():
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=10**9, window_s=3600)
    admitted: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        n = sum(1 for _ in range(50) if lim.check("shared", 0))
        with lock:
            admitted.append(n)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(admitted) == 100


@pytest.mark.security_regression
def test_login_lockout_triggers_exactly_once_under_contention():
    rl = tunnel._LoginRateLimiter()
    triggers: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        n = sum(1 for _ in range(20) if rl.record_failure("shared"))
        with lock:
            triggers.append(n)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(triggers) == 1 and rl.is_locked("shared")[0] is True


def test_concurrent_distinct_keys_stay_within_cap():
    lim = ra._StaticAssetLimiter(max_tracked_ips=100)
    threads = [threading.Thread(target=lambda b=b: [lim.check(f"k-{b}-{i}", 1) for i in range(500)]) for b in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(lim._events) <= 100


# ── Date & time_window utils ─────────────────────────────────────────────────


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def test_date_utils_core_parsers():
    assert parse_iso_utc(None) is None and parse_iso_utc("") is None and parse_iso_utc("invalid-date-string") is None
    dt1 = parse_iso_utc("2026-08-19T12:00:00Z")
    dt2 = parse_iso_utc("2026-08-19")
    assert dt1 is not None and dt1.tzinfo == UTC and dt1.hour == 12
    assert dt2 is not None and dt2.tzinfo == UTC and dt2.hour == 0 and dt2.day == 19
    assert iso_z(datetime(2026, 8, 19, 12, 34, 56, tzinfo=UTC)) == "2026-08-19T12:34:56Z"
    now_str = iso_z_now()
    assert now_str.endswith("Z") and "T" in now_str

    default_val = datetime(2020, 1, 1, tzinfo=UTC)
    assert _parse_dt("2026-08-19T12:00:00Z", default_val).year == 2026
    assert _parse_dt("invalid", default_val) == default_val
    assert parse_date_window("2026-08-10", "2026-08-19") == ("2026-08-10T00:00:00Z", "2026-08-19T23:59:59Z")
    assert parse_date_window("2026-08-10T12:00:00Z", "2026-08-19T12:00:00Z") == (
        "2026-08-10T12:00:00Z",
        "2026-08-19T12:00:00Z",
    )

    assert safe_iso(None) is None and safe_iso("") is None
    assert safe_iso(datetime(2026, 8, 19, 12, 0, 0)) == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19") == "2026-08-19"
    assert safe_iso("2026-08-19 12:00:00") == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19 12:00:00.123") == "2026-08-19T12:00:00.123Z"
    assert safe_iso("2026-08-19T12:00:00Z") == "2026-08-19T12:00:00Z"
    assert safe_iso("2026-08-19T12:00:00+00:00") == "2026-08-19T12:00:00+00:00"

    dt = parse_window_str_to_dt("2026-08-19T12:00:00Z")
    assert (dt.year, dt.month) == (2026, 8)
    with pytest.raises(ValueError, match="Invalid date window string"):
        parse_window_str_to_dt("invalid")
    s, e, from_ts, to_ts = window_to_epoch("2026-08-10T00:00:00Z", "2026-08-19T12:00:00Z")
    assert (s, e) == ("2026-08-10T00:00:00Z", "2026-08-19T12:00:00Z") and 0 < from_ts < to_ts


@pytest.mark.parametrize(
    ("spec", "expected_sec"),
    [("", 3600), ("12h", 12 * 3600), ("5d", 5 * 24 * 3600), ("30m", 30 * 60), ("invalid", 3600)],
)
def test_parse_relative_time_window(spec, expected_sec):
    assert (datetime.now(UTC) - parse_relative_time_window(spec)).total_seconds() == pytest.approx(expected_sec, abs=10)


def test_parse_window_valid_and_invalid():
    assert parse_window("2026-08-05T10:00:00Z", "2026-08-05T12:30:00+00:00") == (
        datetime(2026, 8, 5, 10, tzinfo=UTC),
        datetime(2026, 8, 5, 12, 30, tzinfo=UTC),
    )
    assert parse_window("2026-08-05", "2026-08-06") == (
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    )
    for start, end in (
        (None, "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", None),
        ("", ""),
        ("garbage", "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", "garbage"),
        ("2026-08-05T12:00:00Z", "2026-08-05T12:00:00Z"),
        ("2026-08-05T12:00:00Z", "2026-08-05T10:00:00Z"),
    ):
        assert parse_window(start, end) is None


def test_hour_and_day_key_parsers_and_ensure_utc():
    for key in _HOUR_KEYS:
        raw = datetime.strptime(key, "%Y-%m-%d-%H").replace(tzinfo=UTC)
        assert parse_hour_key(key) == raw == parse_hour_key_strict(key) and parse_hour_key(key).tzinfo is UTC
    for key in _BAD_KEYS:
        assert parse_hour_key(key) is None and parse_hour_token(key) is None
        with pytest.raises(ValueError):
            parse_hour_key_strict(key)
    for key in ("2026-10-19", "2024-02-29", "2026-1-5"):
        assert parse_day_key_strict(key) == datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=UTC)
    for key in ("", "2026-02-30", "2026-10-19-05", "x"):
        with pytest.raises(ValueError):
            parse_day_key_strict(key)

    assert ensure_utc(datetime(2026, 10, 19, 5)) == datetime(2026, 10, 19, 5, tzinfo=UTC)
    src = datetime(2026, 10, 19, 5, tzinfo=timezone(timedelta(hours=5)))
    out = ensure_utc(src)
    assert out == src and out.utcoffset() == timedelta(0) and out.hour == 0


def test_range_tokens_filterbar_and_quantize_anchor():
    assert all(is_valid_range_token(t) for t in ("24h", "7d", "30d", "auto"))
    assert VALID_RANGE_TOKENS == {"1h", "3h", "6h", "12h", "24h", "3d", "7d", "30d", "auto"}
    assert all(not is_valid_range_token(b) for b in (None, "", "90d", "nonsense"))

    src = (ROOT / "frontend" / "components" / "FilterBar" / "FilterBar.tsx").read_text()
    presets = re.findall(r"pickRelative\(\s*'([^']+)'\s*,\s*([\d.]+)\s*\)", src)
    assert presets and len(presets) == src.count("pickRelative(")
    for label, hours in presets:
        assert is_valid_range_token(label)
        start, end = resolve_window(label, _NOW.isoformat(), now=_NOW)
        assert _dt(end) - _dt(start) == timedelta(hours=float(hours))

    assert ANCHOR_QUANTUM_SECONDS == 60
    assert quantize_anchor("2026-06-29T12:30:45Z") == "2026-06-29T12:30:00Z"
    assert quantize_anchor("2026-06-29T12:30:01Z") == quantize_anchor("2026-06-29T12:30:59Z")
    assert quantize_anchor("2026-06-29T12:30:01Z") != quantize_anchor("2026-06-29T12:31:00Z")
    assert quantize_anchor(None, now=_NOW) == "2026-06-29T12:30:00Z"
    assert quantize_anchor("not-a-date", now=_NOW) == "2026-06-29T12:30:00Z"


@pytest.mark.parametrize(
    ("token", "delta"),
    [
        ("1h", timedelta(hours=1)),
        ("3h", timedelta(hours=3)),
        ("6h", timedelta(hours=6)),
        ("12h", timedelta(hours=12)),
        ("24h", timedelta(hours=24)),
        ("3d", timedelta(days=3)),
        ("7d", timedelta(days=7)),
        ("30d", timedelta(days=30)),
    ],
)
def test_fixed_tokens_resolve_to_anchor_minus_delta(token, delta):
    start, end = resolve_window(token, _NOW.isoformat(), now=_NOW)
    assert end == "2026-06-29T12:30:00Z" and _dt(end) - _dt(start) == delta


def test_resolve_window_auto_and_invite_fingerprint():
    assert resolve_window("7d", "2026-06-29T12:30:05Z", now=_NOW) == resolve_window(
        "7d", "2026-06-29T12:30:55Z", now=_NOW
    )
    with pytest.raises(ValueError):
        resolve_window("90d", _NOW.isoformat(), now=_NOW)

    for history_hours, expected in ((1, "24h"), (6 * 24, "24h"), (7 * 24, "7d"), (29 * 24, "7d"), (45 * 24, "30d")):
        earliest = (_NOW - timedelta(hours=history_hours)).isoformat()
        assert resolve_window("auto", _NOW.isoformat(), earliest_log_at=earliest, now=_NOW) == resolve_window(
            expected, _NOW.isoformat(), now=_NOW
        )
    seven = resolve_window("7d", _NOW.isoformat(), now=_NOW)
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at=None, now=_NOW) == seven
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at="2026-06-01", now=_NOW) == seven

    class _Session:
        def __init__(self, qs=None, qe=None, qw=None):
            self.query_start_time, self.query_end_time, self.query_window_hours = qs, qe, qw

    assert invite_clamp_fingerprint(None) is None
    fps = {
        invite_clamp_fingerprint(_Session()),
        invite_clamp_fingerprint(_Session(qs="2026-01-01T00:00:00Z", qe="2026-02-01T00:00:00Z")),
        invite_clamp_fingerprint(_Session(qw=24)),
    }
    assert len(fps) == 3 and invite_clamp_fingerprint(_Session(qw=168)) == invite_clamp_fingerprint(_Session(qw=168))


@pytest.mark.parametrize(
    ("history_hours", "expected"),
    [
        (0.0, (0.25, 1.0)),
        (0.99, (0.25, 1.0)),
        (1.0, (1.0, 1.0)),
        (3.9, (1.0, 1.0)),
        (4.0, (4.0, 1.0)),
        (23.9, (4.0, 1.0)),
        (24.0, (4.0, 24.0)),
        (47.9, (4.0, 24.0)),
        (48.0, (24.0, 24.0)),
        (167.9, (24.0, 24.0)),
        (168.0, (24.0, 168.0)),
        (719.9, (24.0, 168.0)),
        (720.0, (24.0, 168.0)),
        (1270.0, (24.0, 168.0)),
    ],
)
def test_band_table(history_hours: float, expected: tuple[float, float]) -> None:
    assert pick_insights_default(history_hours) == expected


def test_insights_defaults_and_ts_band_mirror() -> None:
    for val in (None, math.nan, math.inf):
        assert pick_insights_default(val) == STATIC_DEFAULT == (24.0, 168.0)
    earliest = (_INSIGHTS_NOW - timedelta(hours=53 * 24)).isoformat()
    assert history_hours_from_earliest(earliest, now=_INSIGHTS_NOW) == pytest.approx(53 * 24)
    assert history_hours_from_earliest("2026-07-01", now=_INSIGHTS_NOW) == pytest.approx(6 * 24 + 12)
    assert history_hours_from_earliest((_INSIGHTS_NOW + timedelta(hours=5)).isoformat(), now=_INSIGHTS_NOW) == 0.0
    for bad in (None, "", "not-a-date"):
        assert history_hours_from_earliest(bad, now=_INSIGHTS_NOW) is None

    src = _TS_SOURCE.read_text()
    body = src[src.index("export function pickInsightsDefault") :]
    band_re = re.compile(
        r"if\s*\(\s*h\s*<\s*([\d.]+)\s*\)\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}"
    )
    ts_bands = [(float(u), float(w), float(b)) for u, w, b in band_re.findall(body)]
    tails = re.findall(r"^\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", body, re.MULTILINE)
    ts_bands.append((math.inf, float(tails[0][0]), float(tails[0][1])))
    assert tuple(ts_bands) == _BANDS
    m = re.search(r"STATIC_DEFAULT\s*=\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", src)
    assert m is not None and (float(m.group(1)), float(m.group(2))) == STATIC_DEFAULT


# ── Active requests & cron deferral ──────────────────────────────────────────


def test_active_request_counter_primitives_and_reset():
    assert active_request_count() == 0
    increment_active_requests()
    increment_active_requests()
    assert active_request_count() == 2
    decrement_active_requests()
    assert active_request_count() == 1
    decrement_active_requests()
    decrement_active_requests()
    assert active_request_count() == 0

    increment_active_requests()
    assert should_defer_cron("sync", "svc-A") is True and active_requests._defer_started_at
    _reset_for_tests()
    assert active_request_count() == 0 and active_requests._defer_started_at == {}


def test_concurrent_increments_and_decrements():
    b1 = threading.Barrier(50)
    t1 = [threading.Thread(target=lambda: (b1.wait(), increment_active_requests())) for _ in range(50)]
    for t in t1:
        t.start()
    for t in t1:
        t.join()
    assert active_request_count() == 50

    _reset_for_tests()
    for _ in range(10):
        increment_active_requests()
    b2 = threading.Barrier(30)
    t2 = [threading.Thread(target=lambda: (b2.wait(), decrement_active_requests())) for _ in range(30)]
    for t in t2:
        t.start()
    for t in t2:
        t.join()
    assert active_request_count() == 0


def test_should_defer_cron_lifecycle_and_starvation(monkeypatch, caplog):
    assert should_defer_cron("sync", "svc-A") is False
    increment_active_requests()
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    first_started = active_requests._defer_started_at[("sync", "svc-A")]
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    assert active_requests._defer_started_at[("sync", "svc-A")] == first_started

    assert should_defer_cron("sync", "svc-B") is True
    assert should_defer_cron("optimize", "svc-A") is True
    assert len(active_requests._defer_started_at) == 3

    decrement_active_requests()
    assert should_defer_cron("sync", "svc-A") is False
    assert ("sync", "svc-A") not in active_requests._defer_started_at

    increment_active_requests()
    assert should_defer_cron("sync", "svc-A", max_defer_secs=0.01) is True
    time.sleep(0.02)
    with caplog.at_level(logging.WARNING, logger="backend.utils.active_requests"):
        assert should_defer_cron("sync", "svc-A", max_defer_secs=0.01) is False
    assert ("sync", "svc-A") not in active_requests._defer_started_at
    assert any("active-request-gate" in r.getMessage() for r in caplog.records)

    fake_now = [1000.0]
    monkeypatch.setattr(active_requests.time, "monotonic", lambda: fake_now[0])
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True
    fake_now[0] = 1010.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True
    fake_now[0] = 1040.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is False


def test_yield_to_api_behavior():
    assert yield_to_api(max_wait_secs=1.0, poll_interval=0.05) == 0.0

    increment_active_requests()
    t = threading.Thread(target=lambda: (time.sleep(0.1), decrement_active_requests()))
    t.start()
    t0 = time.monotonic()
    slept = yield_to_api(max_wait_secs=2.0, poll_interval=0.05)
    elapsed = time.monotonic() - t0
    t.join()
    assert elapsed < 0.5 and 0.05 <= slept < 0.5

    increment_active_requests()
    sleep_calls: list[float] = []
    try:
        slept2 = yield_to_api(max_wait_secs=0.2, poll_interval=0.05)
        assert 0.15 <= slept2 <= 0.3 and active_request_count() == 1
        yield_to_api(max_wait_secs=0.2, poll_interval=0.05, sleep_fn=sleep_calls.append)
    finally:
        decrement_active_requests()
    assert len(sleep_calls) >= 2 and all(s == 0.05 for s in sleep_calls)


# ── SQLite / PG profiler ─────────────────────────────────────────────────────


@pytest.fixture()
def _request_collector():
    token = _SQLITE_QUERIES.set(None)
    yield
    _SQLITE_QUERIES.reset(token)


def test_sqlite_profiler_recording_and_ring_buffer(monkeypatch):
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.5, 0, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1 and snap["queries"][0]["sql"] == "CREATE TABLE t (x INTEGER)"

    sqlite_profiler.clear()
    sqlite_profiler._record("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)], 1.2, 3, "executemany")
    assert sqlite_profiler.get_recent()["queries"][0]["params_kind"] == "seq[3]"

    sqlite_profiler.clear()
    secret = "user-pii-do-not-capture@example.com"
    sqlite_profiler._record("INSERT INTO t VALUES (?)", (secret,), 0.4, 1, "execute")
    entry = sqlite_profiler.get_recent()["queries"][0]
    assert secret not in entry["sql"] and all(secret not in str(v) for v in entry.values())

    sqlite_profiler.clear()
    for i in range(1, 5):
        sqlite_profiler._record(f"SELECT {i}", None, 0.1, 1, "execute")
        if i == 2:
            midpoint = sqlite_profiler.get_recent()["last_seq"]
    sqls = [q["sql"] for q in sqlite_profiler.get_recent(since_seq=midpoint)["queries"]]
    assert sqls == ["SELECT 3", "SELECT 4"]

    sqlite_profiler.clear()
    for i in range(20):
        sqlite_profiler._record(f"SELECT {i}", None, 0.1, 1, "execute")
    assert [q["sql"] for q in sqlite_profiler.get_recent(limit=5)["queries"]] == [f"SELECT {i}" for i in range(15, 20)]

    last_seq_before = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler.clear()
    assert sqlite_profiler.get_recent()["buffer_size"] == 0
    sqlite_profiler._record("SELECT 2", None, 0.1, 1, "execute")
    assert sqlite_profiler.get_recent()["last_seq"] > last_seq_before

    monkeypatch.setattr(sqlite_profiler, "_buffer", type(sqlite_profiler._buffer)(maxlen=10))
    monkeypatch.setattr(sqlite_profiler, "_dropped", 0)
    for _ in range(25):
        sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    snap_cap = sqlite_profiler.get_recent()
    assert snap_cap["buffer_size"] == 10 and snap_cap["dropped"] >= 15


def test_sqlite_profiler_helpers_and_con_integration(monkeypatch, _request_collector):
    con = base.get_con("test-svc-traffic")
    assert isinstance(con, pg_connection.PgConnectionWrapper)
    con.execute("SELECT 1")
    assert any("SELECT 1" in q["sql"] for q in sqlite_profiler.get_recent()["queries"])

    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(1000))
    assert "chars]" in sqlite_profiler._summarize_sql(long_sql)
    assert "SELECT 1" in sqlite_profiler._summarize_sql(b"SELECT 1") and sqlite_profiler._summarize_sql(42) == "42"
    assert (
        sqlite_profiler._describe_params(None) == "none"
        and sqlite_profiler._describe_params([1, 2, 3]) == "seq[3]"
        and sqlite_profiler._describe_params((1,)) == "seq[1]"
        and sqlite_profiler._describe_params({"a": 1, "b": 2}) == "map[2]"
        and sqlite_profiler._describe_params(42) == "int"
    )

    sqlite_profiler.clear()
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    assert _SQLITE_QUERIES.get() is None

    log_throttle.start_call_tracking()
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.2, 0, "execute")
    assert len(log_throttle.get_sqlite_queries()) == 1

    monkeypatch.setattr(
        log_throttle, "record_sqlite_query", lambda e: (_ for _ in ()).throw(RuntimeError("collector broken"))
    )
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")

    bad = MagicMock()
    bad.append.side_effect = RuntimeError("buffer borked")
    monkeypatch.setattr(sqlite_profiler, "_buffer", bad)
    sqlite_profiler._record("SELECT 1", None, 0.5, 1, "execute")

    monkeypatch.setattr(
        query_registry.query_registry, "register", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("down"))
    )
    assert sqlite_profiler._live_register("SQLite", "SELECT 1", con=None) == -1
    sqlite_profiler._live_deregister(-1, error=None)
    monkeypatch.setattr(query_registry.query_registry, "deregister", MagicMock(side_effect=RuntimeError("dereg fail")))
    sqlite_profiler._live_deregister(42, error=None)


# ── Structlog config ─────────────────────────────────────────────────────────


def test_structlog_configuration_and_otel_context(monkeypatch):
    structlog_config.configure_structlog()
    structlog_config.configure_structlog()
    assert structlog.is_configured()

    out = structlog_config._add_otel_trace_context(None, "info", {"event": "test"})
    assert "trace_id" not in out and "span_id" not in out

    tracer = TracerProvider().get_tracer("test")
    with patch.object(trace, "get_current_span", wraps=trace.get_current_span):
        with tracer.start_as_current_span("test-span"):
            out2 = structlog_config._add_otel_trace_context(None, "info", {"event": "test"})
    assert len(out2["trace_id"]) == 32 and len(out2["span_id"]) == 16
    int(out2["trace_id"], 16)
    int(out2["span_id"], 16)

    structlog.reset_defaults()
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()
    structlog.get_logger("json-test").info("event", k=1)


def test_structlog_uvicorn_bridge_and_extra_fields(monkeypatch, capsys):
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()
    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        lg.handlers = [logging.NullHandler()]
        lg.propagate = False
    structlog_config.bridge_uvicorn_loggers()
    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        assert lg.handlers == [] and lg.propagate is True

    logging.getLogger("uvicorn.access").info('127.0.0.1 - "GET /x HTTP/1.1" 200')
    rec1 = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert rec1["event"] == '127.0.0.1 - "GET /x HTTP/1.1" 200'

    logging.getLogger("backend.web_vitals").info(
        "web_vitals", extra={"web_vitals_name": "LCP", "web_vitals_rating": "good"}
    )
    rec2 = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert (rec2["event"], rec2["web_vitals_name"], rec2["web_vitals_rating"]) == ("web_vitals", "LCP", "good")

    logging.getLogger("uvicorn.error").info("Uvicorn running", extra={"color_message": "Uvicorn \x1b[1mrunning\x1b[0m"})
    rec3 = json.loads(capsys.readouterr().err.strip().splitlines()[-1])
    assert rec3["event"] == "Uvicorn running" and "color_message" not in rec3


def test_console_exception_traceback_never_renders_local_secrets(monkeypatch, capsys):
    monkeypatch.delenv("STRUCTLOG_FORMAT", raising=False)
    structlog_config.configure_structlog()

    def _load_cfg():
        return {
            "fos_access_key_id": "AKIAFAKEEXAMPLE00000",
            "fos_secret_access_key": "fake-fos-secret-do-not-leak-00000000",
            "fastly_api_key": "fake-fastly-token-do-not-leak-11111111",
        }

    def _reconcile(cfg):
        if not cfg.get("nonexistent_marker"):
            raise RuntimeError("boom")

    try:
        _reconcile(_load_cfg())
    except RuntimeError:
        logging.getLogger("test.rum_sync").warning("Faro reconcile failed for svc-fake", exc_info=True)

    output = capsys.readouterr().err
    for secret in (
        "AKIAFAKEEXAMPLE00000",
        "fake-fos-secret-do-not-leak-00000000",
        "fake-fastly-token-do-not-leak-11111111",
    ):
        assert secret not in output
    assert "RuntimeError" in output and "Faro reconcile failed for svc-fake" in output


# ── System jobs & testcontainers smoke ───────────────────────────────────────


def test_system_jobs_status_recording():
    assert system_jobs.get_system_job_status() == {}
    system_jobs.record_job_run("rdns", status="success", duration_s=1.234, detail="processed 12")
    entry = system_jobs.get_system_job_status()["rdns"]
    assert (entry["status"], entry["duration_s"], entry["detail"]) == ("success", 1.23, "processed 12")
    assert entry["last_run_at"].endswith("Z")

    system_jobs.record_job_run("rdns", "error", 2.0, detail="boom")
    snap = system_jobs.get_system_job_status()
    assert (snap["rdns"]["status"], snap["rdns"]["duration_s"], snap["rdns"]["detail"]) == ("error", 2.0, "boom")
    snap.pop("rdns")
    assert "rdns" in system_jobs.get_system_job_status()

    with patch("backend.cron.decorators.iso_z_now", return_value="2026-05-15T12:30:45Z"):
        system_jobs.record_job_run("bots", "success", 0.1)
    assert system_jobs.get_system_job_status()["bots"]["last_run_at"] == "2026-05-15T12:30:45Z"


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    for path in ("/var/run/docker.sock", f"/Users/{os.environ.get('USER') or ''}/.docker/run/docker.sock"):
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
                s.settimeout(0.5)
                s.connect(path)
                return True
        except OSError:
            continue
    return False


@pytest.mark.skipif(not _docker_available(), reason="testcontainers requires a running Docker daemon")
def test_can_spawn_a_container_and_read_back_its_id():
    pytest.importorskip("testcontainers.core.container")
    from testcontainers.core.container import DockerContainer

    with DockerContainer("alpine:3.20").with_command("sleep 60") as c:
        assert c.get_wrapped_container().id
