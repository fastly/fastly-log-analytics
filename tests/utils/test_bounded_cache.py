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


_DROP_TABLE_FILES = sorted(
    [*(ROOT / "backend" / "repositories").glob("*.py"), ROOT / "backend" / "core" / "_duckdb_status.py"]
)


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
