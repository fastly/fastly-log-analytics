"""Consolidated tests for test_bounded_cache."""

from __future__ import annotations

import threading
import time

import pytest
from backend.utils.bounded_cache import BoundedTTLCache


def test_basic_set_get_roundtrip():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    cache["k1"] = "v1"
    assert cache["k1"] == "v1"
    assert cache.get("k1") == "v1"
    assert "k1" in cache


def test_get_returns_default_on_miss():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    assert cache.get("missing") is None
    assert cache.get("missing", "sentinel") == "sentinel"
    assert "missing" not in cache
    with pytest.raises(KeyError):
        _ = cache["missing"]


def test_ttl_expiry_treats_entry_as_absent():
    """After the TTL elapses, the entry is invisible to readers — even
    though the underlying storage hasn't been physically reaped yet."""
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=0.05)
    cache["k1"] = "v1"
    assert cache["k1"] == "v1"
    time.sleep(0.10)
    assert "k1" not in cache
    assert cache.get("k1") is None
    with pytest.raises(KeyError):
        _ = cache["k1"]


def test_maxsize_evicts_lru_entry_on_overflow():
    """The least-recently-used entry should be dropped when a write would
    push the cache past maxsize — newer writes win against older ones."""
    cache = BoundedTTLCache(maxsize=3, ttl_seconds=60)
    cache["a"] = 1
    cache["b"] = 2
    cache["c"] = 3
    # Touch "a" so "b" becomes LRU.
    assert cache["a"] == 1
    cache["d"] = 4
    # "b" should be the casualty (it's the oldest untouched key).
    assert "b" not in cache
    assert cache["a"] == 1
    assert cache["c"] == 3
    assert cache["d"] == 4
    assert len(cache) == 3


def test_read_promotes_to_most_recently_used():
    """Repeated reads should keep a key warm — the maxsize evictor must
    drop genuinely cold keys, not just FIFO."""
    cache = BoundedTTLCache(maxsize=2, ttl_seconds=60)
    cache["warm"] = "stays"
    cache["cold"] = "dies"
    # Hammer "warm" so it's the most-recently-used.
    for _ in range(5):
        _ = cache["warm"]
    cache["new"] = "added"
    # "cold" should be the eviction target, not "warm".
    assert "cold" not in cache
    assert cache["warm"] == "stays"
    assert cache["new"] == "added"


def test_pop_removes_entry():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    cache["k1"] = "v1"
    assert cache.pop("k1") == "v1"
    assert "k1" not in cache
    # Pop with default doesn't raise on miss.
    assert cache.pop("missing", "default") == "default"
    # Pop without default raises on miss.
    with pytest.raises(KeyError):
        cache.pop("missing")


def test_clear_drops_everything():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    for i in range(5):
        cache[f"k{i}"] = i
    assert len(cache) == 5
    cache.clear()
    assert len(cache) == 0
    for i in range(5):
        assert f"k{i}" not in cache


def test_lazy_reap_fires_after_n_writes():
    """The cache should automatically sweep expired entries every Nth
    write so cardinality stays bounded under heavy churn."""
    # Use a maxsize that DOESN'T force eviction — the only mechanism
    # cleaning up the expired entries should be the lazy reaper.
    cache = BoundedTTLCache(maxsize=10_000, ttl_seconds=0.05)
    # Seed expired entries.
    for i in range(50):
        cache[f"expired_{i}"] = i
    time.sleep(0.40)
    # Trigger the reaper by crossing the 100-writes threshold.
    for i in range(100):
        cache[f"fresh_{i}"] = i
    # The expired entries should be gone (lazy reaper fired at write 100).
    for i in range(50):
        assert f"expired_{i}" not in cache
    # Fresh entries should still be there.
    for i in range(100):
        assert cache[f"fresh_{i}"] == i


def test_iteration_returns_snapshot_safe_against_concurrent_writes():
    """Iterating shouldn't blow up if another thread writes mid-loop."""
    cache = BoundedTTLCache(maxsize=100, ttl_seconds=60)
    for i in range(20):
        cache[f"k{i}"] = i
    keys = list(cache)
    assert len(keys) == 20
    # Adding a new entry shouldn't affect the snapshot we already pulled.
    cache["fresh"] = "added"
    assert "fresh" not in keys


def test_invalid_construction_args():
    with pytest.raises(ValueError):
        BoundedTTLCache(maxsize=0, ttl_seconds=10)
    with pytest.raises(ValueError):
        BoundedTTLCache(maxsize=-1, ttl_seconds=10)
    with pytest.raises(ValueError):
        BoundedTTLCache(maxsize=10, ttl_seconds=0)
    with pytest.raises(ValueError):
        BoundedTTLCache(maxsize=10, ttl_seconds=-0.5)


def test_thread_safety_under_concurrent_writes():
    """No data corruption / KeyError / race when multiple threads write
    and read concurrently. This is a smoke test, not exhaustive — but a
    plain dict here would intermittently raise RuntimeError on iteration
    or lose writes."""
    cache = BoundedTTLCache(maxsize=200, ttl_seconds=60)
    errors: list[Exception] = []

    def writer(thread_id: int) -> None:
        try:
            for i in range(500):
                cache[f"t{thread_id}_k{i}"] = i
        except Exception as exc:
            errors.append(exc)

    def reader(thread_id: int) -> None:
        try:
            for _ in range(500):
                # Just iterate + read; should never raise even with
                # concurrent writes.
                for k in cache:
                    _ = cache.get(k)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)] + [
        threading.Thread(target=reader, args=(i,)) for i in range(4)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"unexpected exceptions: {errors}"
    # Cache must respect maxsize even after the write storm.
    assert len(cache) <= 200


def test_stores_tuple_values_for_legacy_callsites():
    """The existing call sites store ``(timestamp, payload)`` tuples; the
    cache should round-trip those without unwrapping or mutating them."""
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    payload = (1.23, {"rows": [1, 2, 3], "_meta": "x"})
    cache["k"] = payload
    out = cache["k"]
    assert out == payload
    assert out is payload  # not copied


# ── Merged from test_cache_registry.py ──────────────────────────────────────────────


from backend.utils.bounded_cache import CacheRegistry


@pytest.fixture(autouse=True)
def _isolate_registry():
    # Tests run after the autouse conftest fixture has already triggered
    # CacheRegistry.clear_all() once — registration state is stable, but
    # to avoid polluting other tests' assertions we snapshot the registry
    # before each test here and restore after.
    snapshot_names = CacheRegistry.names()
    yield
    # Drop any test-only registrations.
    for n in CacheRegistry.names():
        if n not in snapshot_names:
            with CacheRegistry._lock:
                CacheRegistry._entries.pop(n, None)


def test_register_and_clear_dict():
    d: dict[str, int] = {}
    CacheRegistry.register("test.dict_a", d)
    d["x"] = 1
    d["y"] = 2
    CacheRegistry.clear_all()
    assert d == {}


def test_register_and_clear_set():
    s: set[str] = set()
    CacheRegistry.register("test.set_a", s)
    s.update({"a", "b", "c"})
    CacheRegistry.clear_all()
    assert s == set()


def test_register_object_with_clear_method():
    class Box:
        def __init__(self):
            self.items = [1, 2, 3]
            self.cleared = False

        def clear(self):
            self.items = []
            self.cleared = True

    b = Box()
    CacheRegistry.register("test.box", b)
    CacheRegistry.clear_all()
    assert b.cleared is True
    assert b.items == []


def test_clear_all_skips_entries_without_clear_method():
    # An int doesn't have .clear(); the registry must not blow up.
    CacheRegistry.register("test.bare_int", 42)
    CacheRegistry.clear_all()  # no exception


def test_iceberg_caches_are_registered_at_import():
    # Pin that the production modules registered their caches —
    # otherwise the conftest fixture would be a no-op for those modules.
    # Importing each module triggers its register() calls; the test must
    # do that explicitly because the cache registry runs at module-load,
    # not at module-import-into-some-other-place.
    import backend.core.duckdb  # noqa: F401 — pulls in duckdb registrations
    import backend.core.iceberg  # noqa: F401 — pulls in iceberg registrations
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


# ── Merged from test_log_throttle.py ──────────────────────────────────────────────

import ast
import logging
from pathlib import Path

import pytest

from backend.utils import telemetry as log_throttle
from backend.utils.telemetry import log_once_per

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.setattr(log_throttle, "_last", {})


def test_first_call_passes_then_is_suppressed_within_the_interval(monkeypatch):
    t = [100.0]
    monkeypatch.setattr(log_throttle.time, "monotonic", lambda: t[0])
    assert log_once_per("k", 60) is True
    t[0] += 59
    assert log_once_per("k", 60) is False
    t[0] += 2
    assert log_once_per("k", 60) is True


def test_keys_are_independent():
    assert log_once_per("a", 60) is True
    assert log_once_per("b", 60) is True
    assert log_once_per("a", 60) is False


def test_key_table_is_bounded(monkeypatch):
    monkeypatch.setattr(log_throttle, "_MAX_KEYS", 8)
    for i in range(100):
        log_once_per(f"k{i}", 3600)
    assert len(log_throttle._last) <= 8


def test_a_persistent_failure_emits_one_traceback_not_one_per_row(caplog):
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


_DROP_TABLE_FILES = sorted([*(ROOT / "backend" / "repositories").glob("*.py"), ROOT / "backend" / "core" / "duckdb.py"])


@pytest.mark.parametrize(
    ("path", "funcs", "msg"),
    [
        ("backend/core/duckdb_pool.py", None, None),
        ("backend/core/ingest.py", None, "_parse_rum_line"),
        ("backend/core/rum_ingest.py", {"extract_metrics_from_faro_payload"}, None),
        ("backend/repositories/query.py", None, "analyst"),
        *[(p.relative_to(ROOT).as_posix(), None, "DROP TABLE") for p in _DROP_TABLE_FILES],
    ],
)
def test_hot_path_debug_tracebacks_are_throttled(path, funcs, msg):
    assert _unthrottled(path, funcs, msg) == []


# ── Merged from test_rate_limit_characterization.py ──────────────────────────────────────────────


import pytest

from backend.utils import remote_access as ra
from backend.utils import tunnel
from backend.utils.tunnel import manager as _session_mod

if hasattr(_session_mod, "MAX_TRACKED_IPS"):
    login_mod = _session_mod
else:
    from backend.utils.tunnel import rate_limiter as login_mod


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


# ── Per-IP request + byte quota (static assets, analyst API) ─────────────────


@pytest.mark.security_regression
def test_quota_request_boundary_is_exact(clock):
    lim = ra._StaticAssetLimiter(req_limit=3)
    assert [lim.check("1.1.1.1", 0) for _ in range(3)] == [True, True, True]
    assert lim.check("1.1.1.1", 0) is False
    assert lim.check("1.1.1.1", 0) is False  # blocked calls stay blocked
    assert lim.check("2.2.2.2", 0) is True  # other keys independent


@pytest.mark.security_regression
def test_quota_window_expiry_frees_budget(clock):
    lim = ra._StaticAssetLimiter(req_limit=2, window_s=60)
    assert lim.check("ip", 0) and lim.check("ip", 0)
    assert lim.check("ip", 0) is False
    clock.now += 59
    assert lim.check("ip", 0) is False
    clock.now += 1  # an event exactly window_s old is still counted (cutoff is inclusive)
    assert lim.check("ip", 0) is False
    clock.now += 1
    assert lim.check("ip", 0) is True


@pytest.mark.security_regression
def test_quota_byte_boundary_counts_the_request_that_crosses_it(clock):
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=1000)
    assert lim.check("ip", 1000) is True  # sum == limit is allowed
    assert lim.check("ip", 1) is False  # sum > limit is refused
    assert lim.check("ip", 0) is False  # the refused bytes were still recorded
    clock.now += 61
    assert lim.check("ip", 1000) is True


@pytest.mark.security_regression
def test_quota_negative_content_length_counts_as_zero(clock):
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=10)
    for _ in range(5):
        assert lim.check("ip", -999) is True


@pytest.mark.security_regression
def test_quota_request_limit_hit_does_not_record_bytes(clock):
    lim = ra._StaticAssetLimiter(req_limit=1, byte_limit=100)
    assert lim.check("ip", 100) is True
    assert lim.check("ip", 100_000) is False  # request cap: refused before bytes recorded
    clock.now += 61
    assert lim.check("ip", 100) is True


def test_quota_defaults_are_unchanged():
    lim = ra._StaticAssetLimiter()
    assert (lim.REQ_LIMIT, lim.BYTE_LIMIT, lim.window_s, lim.MAX_TRACKED_IPS) == (600, 50 * 1024 * 1024, 60, 10_000)
    assert ra._static_limiter is not ra._analyst_api_limiter


@pytest.mark.security_regression
def test_quota_memory_is_bounded_under_key_flood(clock):
    lim = ra._StaticAssetLimiter(max_tracked_ips=50)
    for i in range(5000):
        lim.check(f"10.0.{i // 256}.{i % 256}", 10)
    assert len(lim._events) <= 50


def test_quota_stale_keys_are_evicted_not_kept(clock):
    lim = ra._StaticAssetLimiter(max_tracked_ips=5)
    for i in range(5):
        lim.check(f"old-{i}", 1)
    clock.now += 120
    for i in range(3):
        lim.check(f"new-{i}", 1)
    assert len(lim._events) <= 5
    assert all(lim.check(f"new-{i}", 1) for i in range(3))


# ── Login failure lockout ───────────────────────────────────────────────────


def test_login_defaults_are_unchanged():
    assert (login_mod.LOGIN_FAILURE_WINDOW_S, login_mod.LOGIN_FAILURE_THRESHOLD, login_mod.LOGIN_LOCKOUT_S) == (
        60,
        5,
        300,
    )
    assert login_mod.MAX_TRACKED_IPS == 10000


@pytest.mark.security_regression
def test_login_lockout_triggers_exactly_at_threshold(clock):
    rl = tunnel._LoginRateLimiter()
    results = [rl.record_failure("1.2.3.4") for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD)]
    assert results == [False] * (tunnel.LOGIN_FAILURE_THRESHOLD - 1) + [True]
    assert rl.record_failure("1.2.3.4") is False  # already locked: not a NEW lockout
    locked, remaining = rl.is_locked("1.2.3.4")
    assert locked and remaining == tunnel.LOGIN_LOCKOUT_S


@pytest.mark.security_regression
def test_login_failures_outside_window_do_not_accumulate(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD - 1):
        rl.record_failure("ip")
        clock.now += tunnel.LOGIN_FAILURE_WINDOW_S + 1
    assert rl.record_failure("ip") is False
    assert rl.is_locked("ip") == (False, 0)


@pytest.mark.security_regression
def test_login_lockout_expiry_unlocks_and_remaining_counts_down(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("ip")
    clock.now += 100
    locked, remaining = rl.is_locked("ip")
    assert locked and remaining == tunnel.LOGIN_LOCKOUT_S - 100
    clock.now += tunnel.LOGIN_LOCKOUT_S  # past expiry
    assert rl.is_locked("ip") == (False, 0)


@pytest.mark.security_regression
def test_login_keys_are_isolated(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("a")
    assert rl.is_locked("a")[0] is True
    assert rl.is_locked("b") == (False, 0)


def test_login_snapshot_shape_and_pruning(clock):
    rl = tunnel._LoginRateLimiter()
    rl.record_failure("lonely")
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("locked")
    snap = rl.snapshot()
    assert set(snap) == {"failures", "lockouts"}
    by_ip = {r["ip"]: r for r in snap["failures"]}
    assert by_ip["lonely"] == {"ip": "lonely", "count": 1, "window_s": tunnel.LOGIN_FAILURE_WINDOW_S}
    assert by_ip["locked"]["count"] == tunnel.LOGIN_FAILURE_THRESHOLD
    assert snap["lockouts"] == [{"ip": "locked", "remaining_s": tunnel.LOGIN_LOCKOUT_S}]
    clock.now += tunnel.LOGIN_LOCKOUT_S + 1
    assert rl.snapshot() == {"failures": [], "lockouts": []}


@pytest.mark.security_regression
def test_login_memory_is_bounded_under_key_flood(monkeypatch, clock):
    monkeypatch.setattr(login_mod, "MAX_TRACKED_IPS", 20)
    rl = tunnel._LoginRateLimiter()
    for i in range(2000):
        rl.record_failure(f"ip-{i}")
    snap = rl.snapshot()
    assert len(snap["failures"]) <= 20


def test_login_lru_eviction_drops_oldest_key(monkeypatch, clock):
    monkeypatch.setattr(login_mod, "MAX_TRACKED_IPS", 3)
    rl = tunnel._LoginRateLimiter()
    for ip in ("a", "b", "c"):
        rl.record_failure(ip)
    rl.record_failure("a")  # touch a -> b is now LRU
    rl.record_failure("d")
    ips = {r["ip"] for r in rl.snapshot()["failures"]}
    assert ips == {"a", "c", "d"}


# ── Thread safety ───────────────────────────────────────────────────────────


@pytest.mark.security_regression
def test_quota_never_admits_more_than_limit_under_contention():
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=10**9, window_s=3600)
    admitted = []
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
    triggers = []
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
    assert sum(triggers) == 1
    assert rl.is_locked("shared")[0] is True


def test_concurrent_distinct_keys_stay_within_cap():
    lim = ra._StaticAssetLimiter(max_tracked_ips=100)

    def worker(base: int) -> None:
        for i in range(500):
            lim.check(f"k-{base}-{i}", 1)

    threads = [threading.Thread(target=worker, args=(b,)) for b in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(lim._events) <= 100


# ── Merged from tests/utils/test_date_utils.py ──────────────────────────────

import math
import re
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

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

from backend.core.rollups._common import parse_hour_token
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

_NOW = datetime(2026, 6, 29, 12, 30, 45, tzinfo=UTC)
_INSIGHTS_NOW = datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_TS_SOURCE = _REPO_ROOT / "frontend" / "lib" / "insights-defaults.ts"
_HOUR_KEYS = ["2026-10-19-00", "2026-10-19-23", "2026-02-28-23", "2024-02-29-12", "2026-1-5-3"]
_BAD_KEYS = ["", "2026-10-19", "2026-10-19-24", "2026-13-01-00", "garbage", "2026-10-19T05", "2026-10-19-05-00"]


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


def test_date_utils_core_parsers():
    assert parse_iso_utc(None) is None and parse_iso_utc("") is None and parse_iso_utc("invalid-date-string") is None
    dt1 = parse_iso_utc("2026-08-19T12:00:00Z")
    assert dt1 is not None and dt1.tzinfo == UTC and dt1.hour == 12
    dt2 = parse_iso_utc("2026-08-19")
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
    dt = parse_relative_time_window(spec)
    assert (datetime.now(UTC) - dt).total_seconds() == pytest.approx(expected_sec, abs=10)


def test_parse_window_returns_utc_pair_for_a_valid_window():
    assert parse_window("2026-08-05T10:00:00Z", "2026-08-05T12:30:00+00:00") == (
        datetime(2026, 8, 5, 10, tzinfo=UTC),
        datetime(2026, 8, 5, 12, 30, tzinfo=UTC),
    )
    assert parse_window("2026-08-05", "2026-08-06") == (
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    )


@pytest.mark.parametrize(
    ("start", "end"),
    [
        (None, "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", None),
        ("", ""),
        ("garbage", "2026-08-05T12:00:00Z"),
        ("2026-08-05T10:00:00Z", "garbage"),
        ("2026-08-05T12:00:00Z", "2026-08-05T12:00:00Z"),
        ("2026-08-05T12:00:00Z", "2026-08-05T10:00:00Z"),
    ],
)
def test_parse_window_is_none_for_missing_unparseable_or_non_positive_windows(start, end):
    assert parse_window(start, end) is None


@pytest.mark.parametrize("key", _HOUR_KEYS)
def test_parse_hour_key_matches_raw_strptime(key):
    raw = datetime.strptime(key, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    assert parse_hour_key(key) == raw == parse_hour_key_strict(key)
    assert parse_hour_key(key).tzinfo is UTC


@pytest.mark.parametrize("key", _BAD_KEYS)
def test_parse_hour_key_malformed(key):
    assert parse_hour_key(key) is None and parse_hour_token(key) is None
    with pytest.raises(ValueError):
        parse_hour_key_strict(key)


@pytest.mark.parametrize("key", ["2026-10-19", "2024-02-29", "2026-1-5"])
def test_parse_day_key_matches_raw_strptime(key):
    assert parse_day_key_strict(key) == datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=UTC)


@pytest.mark.parametrize("key", ["", "2026-02-30", "2026-10-19-05", "x"])
def test_parse_day_key_malformed(key):
    with pytest.raises(ValueError):
        parse_day_key_strict(key)


def test_ensure_utc_naive_and_aware():
    assert ensure_utc(datetime(2026, 10, 19, 5)) == datetime(2026, 10, 19, 5, tzinfo=UTC)
    src = datetime(2026, 10, 19, 5, tzinfo=timezone(timedelta(hours=5)))
    out = ensure_utc(src)
    assert out == src and out.utcoffset() == timedelta(0) and out.hour == 0


def test_valid_and_unknown_range_tokens():
    for tok in ("24h", "7d", "30d", "auto"):
        assert is_valid_range_token(tok)
    assert VALID_RANGE_TOKENS == {"1h", "3h", "6h", "12h", "24h", "3d", "7d", "30d", "auto"}
    for bad in (None, "", "90d", "nonsense"):
        assert not is_valid_range_token(bad)


def test_filterbar_presets_lockstep_with_token_vocabulary():
    filterbar = _REPO_ROOT / "frontend" / "components" / "FilterBar" / "FilterBar.tsx"
    src = filterbar.read_text()
    presets = re.findall(r"pickRelative\(\s*'([^']+)'\s*,\s*([\d.]+)\s*\)", src)
    assert presets and len(presets) == src.count("pickRelative(")
    for label, hours in presets:
        assert is_valid_range_token(label)
        start, end = resolve_window(label, _NOW.isoformat(), now=_NOW)
        assert _dt(end) - _dt(start) == timedelta(hours=float(hours))


def test_quantize_anchor_and_quantum():
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


def test_resolution_quantization_and_unknown_token():
    assert resolve_window("7d", "2026-06-29T12:30:05Z", now=_NOW) == resolve_window(
        "7d", "2026-06-29T12:30:55Z", now=_NOW
    )
    with pytest.raises(ValueError):
        resolve_window("90d", _NOW.isoformat(), now=_NOW)


@pytest.mark.parametrize(
    ("history_hours", "expected"),
    [(1, "24h"), (6 * 24, "24h"), (7 * 24, "7d"), (10 * 24, "7d"), (29 * 24, "7d"), (45 * 24, "30d")],
)
def test_auto_picks_band_from_history(history_hours, expected):
    earliest = (_NOW - timedelta(hours=history_hours)).isoformat()
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at=earliest, now=_NOW) == resolve_window(
        expected, _NOW.isoformat(), now=_NOW
    )


def test_auto_extents_fallbacks():
    seven = resolve_window("7d", _NOW.isoformat(), now=_NOW)
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at=None, now=_NOW) == seven
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at="2026-06-01", now=_NOW) == seven


class _Session:
    def __init__(self, qs=None, qe=None, qw=None):
        self.query_start_time = qs
        self.query_end_time = qe
        self.query_window_hours = qw


def test_invite_clamp_fingerprint():
    assert invite_clamp_fingerprint(None) is None
    open_fp = invite_clamp_fingerprint(_Session())
    restricted = invite_clamp_fingerprint(_Session(qs="2026-01-01T00:00:00Z", qe="2026-02-01T00:00:00Z"))
    windowed = invite_clamp_fingerprint(_Session(qw=24))
    assert len({open_fp, restricted, windowed}) == 3 and len(restricted) == 16
    assert invite_clamp_fingerprint(_Session(qw=168)) == invite_clamp_fingerprint(_Session(qw=168))


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


def test_insights_defaults_and_history_hours() -> None:
    for val in (None, math.nan, math.inf):
        assert pick_insights_default(val) == STATIC_DEFAULT == (24.0, 168.0)
    earliest = (_INSIGHTS_NOW - timedelta(hours=53 * 24)).isoformat()
    assert history_hours_from_earliest(earliest, now=_INSIGHTS_NOW) == pytest.approx(53 * 24)
    assert history_hours_from_earliest("2026-07-01", now=_INSIGHTS_NOW) == pytest.approx(6 * 24 + 12)
    assert history_hours_from_earliest((_INSIGHTS_NOW + timedelta(hours=5)).isoformat(), now=_INSIGHTS_NOW) == 0.0
    for bad in (None, "", "not-a-date"):
        assert history_hours_from_earliest(bad, now=_INSIGHTS_NOW) is None


def test_ts_band_table_and_static_default_match_python_mirror() -> None:
    src = _TS_SOURCE.read_text()
    body = src[src.index("export function pickInsightsDefault") :]
    band_re = re.compile(
        r"if\s*\(\s*h\s*<\s*([\d.]+)\s*\)\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}"
    )
    ts_bands = [(float(u), float(w), float(b)) for u, w, b in band_re.findall(body)]
    assert len(ts_bands) >= 5
    tails = re.findall(
        r"^\s*return\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}",
        body,
        re.MULTILINE,
    )
    assert len(tails) == 1
    ts_bands.append((math.inf, float(tails[0][0]), float(tails[0][1])))
    assert tuple(ts_bands) == _BANDS

    m = re.search(r"STATIC_DEFAULT\s*=\s*\{\s*window:\s*'([\d.]+)',\s*baseline:\s*'([\d.]+)'\s*\}", src)
    assert m is not None and (float(m.group(1)), float(m.group(2))) == STATIC_DEFAULT
