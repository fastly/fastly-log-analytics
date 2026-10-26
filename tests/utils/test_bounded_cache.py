"""Consolidated tests for test_bounded_cache."""

from __future__ import annotations

import threading
import time

import pytest
from backend.utils.bounded_cache import BoundedTTLCache


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_basic_set_get_roundtrip():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    cache["k1"] = "v1"
    assert cache["k1"] == "v1"
    assert cache.get("k1") == "v1"
    assert "k1" in cache


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_get_returns_default_on_miss():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    assert cache.get("missing") is None
    assert cache.get("missing", "sentinel") == "sentinel"
    assert "missing" not in cache
    with pytest.raises(KeyError):
        _ = cache["missing"]


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_clear_drops_everything():
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=60)
    for i in range(5):
        cache[f"k{i}"] = i
    assert len(cache) == 5
    cache.clear()
    assert len(cache) == 0
    for i in range(5):
        assert f"k{i}" not in cache


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_invalid_construction_args():
    with pytest.raises(ValueError):
        BoundedTTLCache(maxsize=0, ttl_seconds=10)
    with pytest.raises(ValueError):
        BoundedTTLCache(maxsize=-1, ttl_seconds=10)
    with pytest.raises(ValueError):
        BoundedTTLCache(maxsize=10, ttl_seconds=0)
    with pytest.raises(ValueError):
        BoundedTTLCache(maxsize=10, ttl_seconds=-0.5)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.fixture()
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_register_and_clear_dict():
    d: dict[str, int] = {}
    CacheRegistry.register("test.dict_a", d)
    d["x"] = 1
    d["y"] = 2
    CacheRegistry.clear_all()
    assert d == {}


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_register_and_clear_set():
    s: set[str] = set()
    CacheRegistry.register("test.set_a", s)
    s.update({"a", "b", "c"})
    CacheRegistry.clear_all()
    assert s == set()


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_clear_all_skips_entries_without_clear_method():
    # An int doesn't have .clear(); the registry must not blow up.
    CacheRegistry.register("test.bare_int", 42)
    CacheRegistry.clear_all()  # no exception


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.fixture()
def _clean(monkeypatch):
    monkeypatch.setattr(log_throttle, "_last", {})


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_first_call_passes_then_is_suppressed_within_the_interval(monkeypatch):
    t = [100.0]
    monkeypatch.setattr(log_throttle.time, "monotonic", lambda: t[0])
    assert log_once_per("k", 60) is True
    t[0] += 59
    assert log_once_per("k", 60) is False
    t[0] += 2
    assert log_once_per("k", 60) is True


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_keys_are_independent():
    assert log_once_per("a", 60) is True
    assert log_once_per("b", 60) is True
    assert log_once_per("a", 60) is False


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_key_table_is_bounded(monkeypatch):
    monkeypatch.setattr(log_throttle, "_MAX_KEYS", 8)
    for i in range(100):
        log_once_per(f"k{i}", 3600)
    assert len(log_throttle._last) <= 8


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_quota_request_boundary_is_exact(clock):
    lim = ra._StaticAssetLimiter(req_limit=3)
    assert [lim.check("1.1.1.1", 0) for _ in range(3)] == [True, True, True]
    assert lim.check("1.1.1.1", 0) is False
    assert lim.check("1.1.1.1", 0) is False  # blocked calls stay blocked
    assert lim.check("2.2.2.2", 0) is True  # other keys independent


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_quota_byte_boundary_counts_the_request_that_crosses_it(clock):
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=1000)
    assert lim.check("ip", 1000) is True  # sum == limit is allowed
    assert lim.check("ip", 1) is False  # sum > limit is refused
    assert lim.check("ip", 0) is False  # the refused bytes were still recorded
    clock.now += 61
    assert lim.check("ip", 1000) is True


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_quota_negative_content_length_counts_as_zero(clock):
    lim = ra._StaticAssetLimiter(req_limit=100, byte_limit=10)
    for _ in range(5):
        assert lim.check("ip", -999) is True


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_quota_request_limit_hit_does_not_record_bytes(clock):
    lim = ra._StaticAssetLimiter(req_limit=1, byte_limit=100)
    assert lim.check("ip", 100) is True
    assert lim.check("ip", 100_000) is False  # request cap: refused before bytes recorded
    clock.now += 61
    assert lim.check("ip", 100) is True


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_quota_defaults_are_unchanged():
    lim = ra._StaticAssetLimiter()
    assert (lim.REQ_LIMIT, lim.BYTE_LIMIT, lim.window_s, lim.MAX_TRACKED_IPS) == (600, 50 * 1024 * 1024, 60, 10_000)
    assert ra._static_limiter is not ra._analyst_api_limiter


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_quota_memory_is_bounded_under_key_flood(clock):
    lim = ra._StaticAssetLimiter(max_tracked_ips=50)
    for i in range(5000):
        lim.check(f"10.0.{i // 256}.{i % 256}", 10)
    assert len(lim._events) <= 50


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_login_defaults_are_unchanged():
    assert (login_mod.LOGIN_FAILURE_WINDOW_S, login_mod.LOGIN_FAILURE_THRESHOLD, login_mod.LOGIN_LOCKOUT_S) == (
        60,
        5,
        300,
    )
    assert login_mod.MAX_TRACKED_IPS == 10000


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_login_lockout_triggers_exactly_at_threshold(clock):
    rl = tunnel._LoginRateLimiter()
    results = [rl.record_failure("1.2.3.4") for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD)]
    assert results == [False] * (tunnel.LOGIN_FAILURE_THRESHOLD - 1) + [True]
    assert rl.record_failure("1.2.3.4") is False  # already locked: not a NEW lockout
    locked, remaining = rl.is_locked("1.2.3.4")
    assert locked and remaining == tunnel.LOGIN_LOCKOUT_S


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_login_failures_outside_window_do_not_accumulate(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD - 1):
        rl.record_failure("ip")
        clock.now += tunnel.LOGIN_FAILURE_WINDOW_S + 1
    assert rl.record_failure("ip") is False
    assert rl.is_locked("ip") == (False, 0)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_login_keys_are_isolated(clock):
    rl = tunnel._LoginRateLimiter()
    for _ in range(tunnel.LOGIN_FAILURE_THRESHOLD):
        rl.record_failure("a")
    assert rl.is_locked("a")[0] is True
    assert rl.is_locked("b") == (False, 0)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.security_regression
def test_login_memory_is_bounded_under_key_flood(monkeypatch, clock):
    monkeypatch.setattr(login_mod, "MAX_TRACKED_IPS", 20)
    rl = tunnel._LoginRateLimiter()
    for i in range(2000):
        rl.record_failure(f"ip-{i}")
    snap = rl.snapshot()
    assert len(snap["failures"]) <= 20


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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

from backend.core.rollups._common import parse_hour_token

_NOW = datetime(2026, 6, 29, 12, 30, 45, tzinfo=UTC)
_INSIGHTS_NOW = datetime(2026, 7, 7, 12, 0, 0, tzinfo=UTC)
_REPO_ROOT = Path(__file__).resolve().parents[2]
_TS_SOURCE = _REPO_ROOT / "frontend" / "lib" / "utils.ts"
_HOUR_KEYS = ["2026-10-19-00", "2026-10-19-23", "2026-02-28-23", "2024-02-29-12", "2026-1-5-3"]
_BAD_KEYS = ["", "2026-10-19", "2026-10-19-24", "2026-13-01-00", "garbage", "2026-10-19T05", "2026-10-19-05-00"]


def _dt(iso: str) -> datetime:
    return datetime.fromisoformat(iso.replace("Z", "+00:00"))


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.parametrize(
    ("spec", "expected_sec"),
    [("", 3600), ("12h", 12 * 3600), ("5d", 5 * 24 * 3600), ("30m", 30 * 60), ("invalid", 3600)],
)
def test_parse_relative_time_window(spec, expected_sec):
    dt = parse_relative_time_window(spec)
    assert (datetime.now(UTC) - dt).total_seconds() == pytest.approx(expected_sec, abs=10)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_parse_window_returns_utc_pair_for_a_valid_window():
    assert parse_window("2026-08-05T10:00:00Z", "2026-08-05T12:30:00+00:00") == (
        datetime(2026, 8, 5, 10, tzinfo=UTC),
        datetime(2026, 8, 5, 12, 30, tzinfo=UTC),
    )
    assert parse_window("2026-08-05", "2026-08-06") == (
        datetime(2026, 8, 5, tzinfo=UTC),
        datetime(2026, 8, 6, tzinfo=UTC),
    )


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.parametrize("key", _HOUR_KEYS)
def test_parse_hour_key_matches_raw_strptime(key):
    raw = datetime.strptime(key, "%Y-%m-%d-%H").replace(tzinfo=UTC)
    assert parse_hour_key(key) == raw == parse_hour_key_strict(key)
    assert parse_hour_key(key).tzinfo is UTC


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.parametrize("key", _BAD_KEYS)
def test_parse_hour_key_malformed(key):
    assert parse_hour_key(key) is None and parse_hour_token(key) is None
    with pytest.raises(ValueError):
        parse_hour_key_strict(key)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.parametrize("key", ["2026-10-19", "2024-02-29", "2026-1-5"])
def test_parse_day_key_matches_raw_strptime(key):
    assert parse_day_key_strict(key) == datetime.strptime(key, "%Y-%m-%d").replace(tzinfo=UTC)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.parametrize("key", ["", "2026-02-30", "2026-10-19-05", "x"])
def test_parse_day_key_malformed(key):
    with pytest.raises(ValueError):
        parse_day_key_strict(key)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_ensure_utc_naive_and_aware():
    assert ensure_utc(datetime(2026, 10, 19, 5)) == datetime(2026, 10, 19, 5, tzinfo=UTC)
    src = datetime(2026, 10, 19, 5, tzinfo=timezone(timedelta(hours=5)))
    out = ensure_utc(src)
    assert out == src and out.utcoffset() == timedelta(0) and out.hour == 0


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_valid_and_unknown_range_tokens():
    for tok in ("24h", "7d", "30d", "auto"):
        assert is_valid_range_token(tok)
    assert VALID_RANGE_TOKENS == {"1h", "3h", "6h", "12h", "24h", "3d", "7d", "30d", "auto"}
    for bad in (None, "", "90d", "nonsense"):
        assert not is_valid_range_token(bad)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_filterbar_presets_lockstep_with_token_vocabulary():
    filterbar = _REPO_ROOT / "frontend" / "components" / "FilterBar" / "FilterBar.tsx"
    src = filterbar.read_text()
    presets = re.findall(r"pickRelative\(\s*'([^']+)'\s*,\s*([\d.]+)\s*\)", src)
    assert presets and len(presets) == src.count("pickRelative(")
    for label, hours in presets:
        assert is_valid_range_token(label)
        start, end = resolve_window(label, _NOW.isoformat(), now=_NOW)
        assert _dt(end) - _dt(start) == timedelta(hours=float(hours))


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_quantize_anchor_and_quantum():
    assert ANCHOR_QUANTUM_SECONDS == 60
    assert quantize_anchor("2026-06-29T12:30:45Z") == "2026-06-29T12:30:00Z"
    assert quantize_anchor("2026-06-29T12:30:01Z") == quantize_anchor("2026-06-29T12:30:59Z")
    assert quantize_anchor("2026-06-29T12:30:01Z") != quantize_anchor("2026-06-29T12:31:00Z")
    assert quantize_anchor(None, now=_NOW) == "2026-06-29T12:30:00Z"
    assert quantize_anchor("not-a-date", now=_NOW) == "2026-06-29T12:30:00Z"


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_resolution_quantization_and_unknown_token():
    assert resolve_window("7d", "2026-06-29T12:30:05Z", now=_NOW) == resolve_window(
        "7d", "2026-06-29T12:30:55Z", now=_NOW
    )
    with pytest.raises(ValueError):
        resolve_window("90d", _NOW.isoformat(), now=_NOW)


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
@pytest.mark.parametrize(
    ("history_hours", "expected"),
    [(1, "24h"), (6 * 24, "24h"), (7 * 24, "7d"), (10 * 24, "7d"), (29 * 24, "7d"), (45 * 24, "30d")],
)
def test_auto_picks_band_from_history(history_hours, expected):
    earliest = (_NOW - timedelta(hours=history_hours)).isoformat()
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at=earliest, now=_NOW) == resolve_window(
        expected, _NOW.isoformat(), now=_NOW
    )


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_auto_extents_fallbacks():
    seven = resolve_window("7d", _NOW.isoformat(), now=_NOW)
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at=None, now=_NOW) == seven
    assert resolve_window("auto", _NOW.isoformat(), earliest_log_at="2026-06-01", now=_NOW) == seven


class _Session:
    def __init__(self, qs=None, qe=None, qw=None):
        self.query_start_time = qs
        self.query_end_time = qe
        self.query_window_hours = qw


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_invite_clamp_fingerprint():
    assert invite_clamp_fingerprint(None) is None
    open_fp = invite_clamp_fingerprint(_Session())
    restricted = invite_clamp_fingerprint(_Session(qs="2026-01-01T00:00:00Z", qe="2026-02-01T00:00:00Z"))
    windowed = invite_clamp_fingerprint(_Session(qw=24))
    assert len({open_fp, restricted, windowed}) == 3 and len(restricted) == 16
    assert invite_clamp_fingerprint(_Session(qw=168)) == invite_clamp_fingerprint(_Session(qw=168))


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
def test_insights_defaults_and_history_hours() -> None:
    for val in (None, math.nan, math.inf):
        assert pick_insights_default(val) == STATIC_DEFAULT == (24.0, 168.0)
    earliest = (_INSIGHTS_NOW - timedelta(hours=53 * 24)).isoformat()
    assert history_hours_from_earliest(earliest, now=_INSIGHTS_NOW) == pytest.approx(53 * 24)
    assert history_hours_from_earliest("2026-07-01", now=_INSIGHTS_NOW) == pytest.approx(6 * 24 + 12)
    assert history_hours_from_earliest((_INSIGHTS_NOW + timedelta(hours=5)).isoformat(), now=_INSIGHTS_NOW) == 0.0
    for bad in (None, "", "not-a-date"):
        assert history_hours_from_earliest(bad, now=_INSIGHTS_NOW) is None


@pytest.mark.usefixtures("_isolate_registry")
@pytest.mark.usefixtures("_clean")
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


# ── Merged from tests/utils/test_active_requests.py ──

"""Consolidated tests for test_active_requests."""


import pytest
from backend.utils.active_requests import (
    _reset_for_tests,
    active_request_count,
    decrement_active_requests,
    increment_active_requests,
    should_defer_cron,
    yield_to_api,
)

from backend.utils import active_requests


@pytest.fixture()
def _clean_state():
    """Reset module-level counter + defer window between every test."""
    _reset_for_tests()
    yield
    _reset_for_tests()


# ---------------------------------------------------------------------------
# Counter primitives
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_initial_count_is_zero_after_reset():
    assert active_request_count() == 0


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_increment_bumps_count():
    increment_active_requests()
    assert active_request_count() == 1
    increment_active_requests()
    assert active_request_count() == 2


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_decrement_reduces_count():
    increment_active_requests()
    increment_active_requests()
    decrement_active_requests()
    assert active_request_count() == 1


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_decrement_clamps_at_zero():
    """Belt-and-suspenders: a stray decrement must NOT take the counter
    negative, otherwise should_defer_cron's `_active_count == 0` branch
    would never fire again."""
    decrement_active_requests()
    decrement_active_requests()
    assert active_request_count() == 0


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_increment_then_decrement_back_to_zero():
    for _ in range(5):
        increment_active_requests()
    assert active_request_count() == 5
    for _ in range(5):
        decrement_active_requests()
    assert active_request_count() == 0


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_reset_for_tests_clears_counter_and_window():
    increment_active_requests()
    increment_active_requests()
    # Seed the defer-window dict via a deferral
    assert should_defer_cron("sync", "svc-A") is True
    assert active_requests._defer_started_at  # populated

    _reset_for_tests()

    assert active_request_count() == 0
    assert active_requests._defer_started_at == {}


# ---------------------------------------------------------------------------
# Thread safety (light coverage — `with _lock` is the contract here)
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_concurrent_increments_are_atomic():
    """Spawn 50 threads, each calls increment_active_requests once.
    Without the lock, the read-modify-write would race and lose updates."""
    workers = []
    barrier = threading.Barrier(50)

    def _worker():
        barrier.wait()  # maximize contention
        increment_active_requests()

    for _ in range(50):
        t = threading.Thread(target=_worker)
        workers.append(t)
        t.start()
    for t in workers:
        t.join()

    assert active_request_count() == 50


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_concurrent_decrements_clamp_at_zero():
    """Over-decrementing across threads must still bottom out at 0."""
    for _ in range(10):
        increment_active_requests()

    workers = []
    barrier = threading.Barrier(30)

    def _worker():
        barrier.wait()
        decrement_active_requests()

    for _ in range(30):
        t = threading.Thread(target=_worker)
        workers.append(t)
        t.start()
    for t in workers:
        t.join()

    assert active_request_count() == 0


# ---------------------------------------------------------------------------
# should_defer_cron — branch coverage
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_should_defer_returns_false_when_no_active_requests():
    assert active_request_count() == 0
    assert should_defer_cron("sync", "svc-A") is False


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_should_defer_clears_prior_window_when_quiet():
    """If a deferral window was set during a busy period but the system
    has since gone idle, the next call must clear that window so the
    NEXT contention round starts a fresh max_defer_secs clock."""
    increment_active_requests()
    assert should_defer_cron("sync", "svc-A") is True
    assert ("sync", "svc-A") in active_requests._defer_started_at

    # Drain in-flight requests.
    decrement_active_requests()
    assert active_request_count() == 0

    assert should_defer_cron("sync", "svc-A") is False
    assert ("sync", "svc-A") not in active_requests._defer_started_at


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_should_defer_returns_true_and_seeds_window_when_busy():
    increment_active_requests()
    assert ("sync", "svc-A") not in active_requests._defer_started_at

    assert should_defer_cron("sync", "svc-A") is True

    # Window must now exist with a monotonic timestamp.
    started = active_requests._defer_started_at[("sync", "svc-A")]
    assert isinstance(started, float)
    assert started > 0


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_should_defer_returns_true_while_window_within_budget():
    """Second call within max_defer_secs of the first must still defer,
    and the window must NOT be reset (otherwise starvation safeguard
    would never fire on a sustained-busy service)."""
    increment_active_requests()
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    first_started = active_requests._defer_started_at[("sync", "svc-A")]

    # A subsequent call still well within the 30 s budget should defer.
    assert should_defer_cron("sync", "svc-A", max_defer_secs=30.0) is True
    second_started = active_requests._defer_started_at[("sync", "svc-A")]
    assert second_started == first_started  # window preserved


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_should_defer_returns_false_after_starvation_window_expires(caplog):
    """Force the window to age past max_defer_secs by passing a tiny
    budget + sleeping. The third call must return False, clear the
    window, and emit a WARN."""
    increment_active_requests()

    # Seed window.
    assert should_defer_cron("sync", "svc-A", max_defer_secs=0.01) is True
    assert ("sync", "svc-A") in active_requests._defer_started_at

    time.sleep(0.02)

    with caplog.at_level(logging.WARNING, logger="backend.utils.active_requests"):
        result = should_defer_cron("sync", "svc-A", max_defer_secs=0.01)

    assert result is False
    # Starvation path must clear the window so the next round starts fresh.
    assert ("sync", "svc-A") not in active_requests._defer_started_at

    warn_records = [
        r for r in caplog.records if r.levelno == logging.WARNING and r.name == "backend.utils.active_requests"
    ]
    assert len(warn_records) == 1
    msg = warn_records[0].getMessage()
    assert "active-request-gate" in msg
    assert "sync" in msg
    assert "svc-A" in msg


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_should_defer_starvation_via_monkeypatched_monotonic(monkeypatch):
    """Belt-and-suspenders: drive the starvation branch deterministically
    by stubbing time.monotonic — avoids any flakiness from wall-clock
    jitter on a loaded CI box."""
    increment_active_requests()

    fake_now = [1000.0]

    def _fake_monotonic():
        return fake_now[0]

    monkeypatch.setattr(active_requests.time, "monotonic", _fake_monotonic)

    # t=1000.0 — seed window
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True
    assert active_requests._defer_started_at[("optimize", "svc-Z")] == 1000.0

    # t=1010.0 — within budget, still defers
    fake_now[0] = 1010.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is True

    # t=1040.0 — exceeds 30 s budget, runs anyway
    fake_now[0] = 1040.0
    assert should_defer_cron("optimize", "svc-Z", max_defer_secs=30.0) is False
    assert ("optimize", "svc-Z") not in active_requests._defer_started_at


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_should_defer_tracks_windows_per_key_independently():
    """Concurrent services must not share a starvation clock — deferring
    job=sync for svc-A must NOT mark job=sync for svc-B as deferred."""
    increment_active_requests()

    assert should_defer_cron("sync", "svc-A") is True
    assert ("sync", "svc-A") in active_requests._defer_started_at
    assert ("sync", "svc-B") not in active_requests._defer_started_at

    assert should_defer_cron("sync", "svc-B") is True
    assert ("sync", "svc-B") in active_requests._defer_started_at

    # Distinct jobs on the same service are also independent.
    assert should_defer_cron("optimize", "svc-A") is True
    assert ("optimize", "svc-A") in active_requests._defer_started_at

    assert len(active_requests._defer_started_at) == 3


# ---------------------------------------------------------------------------
# yield_to_api — cooperative mid-tick yield
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_yield_returns_immediately_when_no_active_requests():
    """Hot path: zero in-flight means zero sleep — must NOT call time.sleep."""
    assert active_request_count() == 0
    t0 = time.monotonic()
    slept = yield_to_api(max_wait_secs=1.0, poll_interval=0.05)
    elapsed = time.monotonic() - t0
    assert slept == 0.0
    assert elapsed < 0.02  # generous; the function should be essentially free


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_yield_returns_early_when_requests_drain_during_sleep():
    """Spawn a thread that decrements after a short delay. yield_to_api must
    notice the count dropped and return before max_wait_secs expires."""
    increment_active_requests()

    def _drain_soon():
        time.sleep(0.1)
        decrement_active_requests()

    t = threading.Thread(target=_drain_soon)
    t.start()

    t0 = time.monotonic()
    slept = yield_to_api(max_wait_secs=2.0, poll_interval=0.05)
    elapsed = time.monotonic() - t0
    t.join()

    # Drained after ~100ms; helper should have returned shortly after that
    # — well under the 2 s max.
    assert elapsed < 0.5, f"helper waited {elapsed:.3f}s after drain"
    assert 0.05 <= slept < 0.5


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_yield_caps_at_max_wait_secs_under_sustained_load():
    """Sustained in-flight requests must not stall sync forever. The helper
    returns after max_wait_secs even if count stays > 0."""
    increment_active_requests()
    try:
        t0 = time.monotonic()
        slept = yield_to_api(max_wait_secs=0.2, poll_interval=0.05)
        elapsed = time.monotonic() - t0
        # Bounded by max_wait_secs + one extra poll_interval slack.
        assert 0.15 <= elapsed < 0.4, f"helper waited {elapsed:.3f}s"
        # slept counter tracks the budget consumed; allow one extra tick.
        assert 0.15 <= slept <= 0.3
        # Counter was NOT touched — yield observes, doesn't mutate state.
        assert active_request_count() == 1
    finally:
        decrement_active_requests()


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_yield_polls_at_configured_interval():
    """Verify the helper actually polls — i.e. uses small sleeps in a loop
    so an early drain is observed promptly, rather than one big sleep that
    blocks past the drain moment."""
    increment_active_requests()
    sleep_calls: list[float] = []

    def _tracking_sleep(secs: float) -> None:
        sleep_calls.append(secs)

    try:
        yield_to_api(max_wait_secs=0.2, poll_interval=0.05, sleep_fn=_tracking_sleep)
    finally:
        decrement_active_requests()

    assert len(sleep_calls) >= 2, f"expected multiple polls, got {sleep_calls}"
    assert all(s == 0.05 for s in sleep_calls), f"unexpected sleep durations: {sleep_calls}"


# ── Merged from test_sqlite_profiler.py ──────────────────────────────────────────────

import pytest

from backend.utils import sqlite_profiler


@pytest.fixture()
def _clear_buffer():
    """Every test starts with an empty ring buffer."""
    sqlite_profiler.clear()
    yield
    sqlite_profiler.clear()


# ── Statement recording ──────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_execute_captures_a_statement():
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.5, 0, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1
    assert snap["queries"][0]["sql"] == "CREATE TABLE t (x INTEGER)"
    assert snap["queries"][0]["op"] == "execute"
    assert snap["queries"][0]["time_ms"] >= 0
    assert snap["queries"][0]["seq"] >= 1


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_executemany_captures_one_entry_with_seq_length():
    sqlite_profiler._record("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)], 1.2, 3, "executemany")
    snap = sqlite_profiler.get_recent()
    assert len(snap["queries"]) == 1
    assert snap["queries"][0]["op"] == "executemany"
    assert snap["queries"][0]["params_kind"] == "seq[3]"


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_params_are_not_captured_as_values():
    """PII safety: params must never appear in the captured SQL or anywhere
    in the entry body. Only shape metadata."""
    secret = "user-pii-do-not-capture@example.com"
    sqlite_profiler._record("INSERT INTO t VALUES (?)", (secret,), 0.4, 1, "execute")
    snap = sqlite_profiler.get_recent()
    entry = snap["queries"][0]
    assert secret not in entry["sql"]
    assert all(secret not in str(v) for v in entry.values())


# ── Ring buffer mechanics ────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_ring_buffer_caps_at_max(monkeypatch):
    """Push past cap, verify buffer_size never exceeds cap and dropped grows."""
    monkeypatch.setattr(sqlite_profiler, "_buffer", type(sqlite_profiler._buffer)(maxlen=10))
    monkeypatch.setattr(sqlite_profiler, "_dropped", 0)
    for _ in range(25):
        sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 10
    assert snap["dropped"] >= 15


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_since_seq_filters():
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    sqlite_profiler._record("SELECT 2", None, 0.1, 1, "execute")
    midpoint = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler._record("SELECT 3", None, 0.1, 1, "execute")
    sqlite_profiler._record("SELECT 4", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent(since_seq=midpoint)
    sqls = [q["sql"] for q in snap["queries"]]
    assert "SELECT 3" in sqls
    assert "SELECT 4" in sqls
    assert "SELECT 1" not in sqls
    assert "SELECT 2" not in sqls


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_limit_returns_most_recent():
    for i in range(20):
        sqlite_profiler._record(f"SELECT {i}", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent(limit=5)
    assert len(snap["queries"]) == 5
    sqls = [q["sql"] for q in snap["queries"]]
    assert sqls == [f"SELECT {i}" for i in range(15, 20)]


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_clear_resets_buffer_but_not_seq():
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")
    last_seq_before = sqlite_profiler.get_recent()["last_seq"]
    sqlite_profiler.clear()
    snap_after_clear = sqlite_profiler.get_recent()
    assert snap_after_clear["buffer_size"] == 0
    assert snap_after_clear["last_seq"] == 0
    sqlite_profiler._record("SELECT 2", None, 0.1, 1, "execute")
    snap = sqlite_profiler.get_recent()
    assert snap["last_seq"] > last_seq_before


# ── End-to-end via get_con() ─────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_get_con_returns_instrumented_connection():
    """metadata_db.get_con() must hand out PgConnectionWrapper under Postgres."""
    from backend.core.metadata import base, pg_connection

    con = base.get_con("test-svc")
    assert isinstance(con, pg_connection.PgConnectionWrapper)


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_metadata_db_traffic_appears_in_buffer():
    """Sanity end-to-end: a real metadata_db call populates the ring buffer."""
    from backend.core.metadata import base

    con = base.get_con("test-svc-traffic")
    con.execute("SELECT 1")
    snap = sqlite_profiler.get_recent()
    sqls = [q["sql"] for q in snap["queries"]]
    assert any("SELECT 1" in s for s in sqls)


# ── Helper-function direct tests (cover the small surface) ──────────────────


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_summarize_sql_truncates_long_strings():
    long_sql = "SELECT " + ", ".join(f"col_{i}" for i in range(1000))
    out = sqlite_profiler._summarize_sql(long_sql)
    assert "chars]" in out
    assert len(out) < len(long_sql)


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_summarize_sql_coerces_non_strings():
    """Some callers pass bytes / int; the profiler must not raise."""
    out = sqlite_profiler._summarize_sql(b"SELECT 1")
    assert isinstance(out, str)
    assert "SELECT 1" in out
    assert sqlite_profiler._summarize_sql(42) == "42"


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_describe_params_covers_each_shape():
    assert sqlite_profiler._describe_params(None) == "none"
    assert sqlite_profiler._describe_params([1, 2, 3]) == "seq[3]"
    assert sqlite_profiler._describe_params((1,)) == "seq[1]"
    assert sqlite_profiler._describe_params({"a": 1, "b": 2}) == "map[2]"
    assert sqlite_profiler._describe_params(42) == "int"


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_record_swallows_internal_exceptions(monkeypatch):
    """The profiler's hard contract: any internal failure must never
    reach the caller's SQL path."""
    from unittest.mock import MagicMock

    bad = MagicMock()
    bad.append.side_effect = RuntimeError("buffer borked")
    monkeypatch.setattr(sqlite_profiler, "_buffer", bad)
    sqlite_profiler._record("SELECT 1", None, 0.5, 1, "execute")


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_live_register_returns_sentinel_on_registry_failure(monkeypatch):
    """When the registry import / call raises, _live_register returns -1
    (a sentinel that _live_deregister treats as a no-op)."""
    from backend.core import query_registry

    monkeypatch.setattr(
        query_registry.query_registry,
        "register",
        lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("registry down")),
    )
    qid = sqlite_profiler._live_register("SQLite", "SELECT 1", con=None)
    assert qid == -1


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_live_deregister_is_noop_for_negative_qid():
    sqlite_profiler._live_deregister(-1, error=None)


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_live_deregister_swallows_registry_errors(monkeypatch):
    from unittest.mock import MagicMock

    from backend.core import query_registry

    monkeypatch.setattr(
        query_registry.query_registry,
        "deregister",
        MagicMock(side_effect=RuntimeError("dereg failed")),
    )
    sqlite_profiler._live_deregister(42, error=None)


# ── Per-request collector (Debug Panel "This page" scoping) ──────────────────


@pytest.fixture()
def _request_collector():
    """Isolate the telemetry request-scoped SQLite collector per test —
    start_call_tracking() sets a ContextVar that would otherwise leak
    into subsequent tests running in the same context."""
    from backend.utils.telemetry import _SQLITE_QUERIES

    token = _SQLITE_QUERIES.set(None)
    yield
    _SQLITE_QUERIES.reset(token)


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_statement_lands_in_request_collector_when_tracking_active(_request_collector):
    """The page-scoped Debug Panel view: a statement executed while a
    request is being tracked must land in BOTH the process-global ring
    buffer AND the request's contextvar collector."""
    from backend.utils import telemetry

    telemetry.start_call_tracking()
    sqlite_profiler._record("CREATE TABLE t (x INTEGER)", None, 0.2, 0, "execute")

    entries = telemetry.get_sqlite_queries()
    assert len(entries) == 1
    assert entries[0]["sql"] == "CREATE TABLE t (x INTEGER)"
    snap = sqlite_profiler.get_recent()
    assert snap["buffer_size"] == 1
    assert snap["queries"][0]["seq"] == entries[0]["seq"]


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_statement_skips_request_collector_outside_tracking(_request_collector):
    """Cron/startup statements (no start_call_tracking) must NOT create or
    populate a request collector — only the ring buffer sees them."""
    from backend.utils.telemetry import _SQLITE_QUERIES

    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")

    assert _SQLITE_QUERIES.get() is None
    assert sqlite_profiler.get_recent()["buffer_size"] == 1


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_request_collector_failure_does_not_break_sql_path(_request_collector, monkeypatch):
    """The profiler contract: observability failures never propagate into
    the calling SQL path. A broken record_sqlite_query must not raise."""
    from backend.utils import telemetry

    monkeypatch.setattr(
        telemetry,
        "record_sqlite_query",
        lambda entry: (_ for _ in ()).throw(RuntimeError("collector broken")),
    )
    sqlite_profiler._record("SELECT 1", None, 0.1, 1, "execute")


# ── Merged from test_structlog_config.py ──────────────────────────────────────────────

import json
from unittest.mock import patch

import pytest
import structlog
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

from backend.utils import structlog_config


@pytest.fixture()
def _reset_structlog():
    structlog.reset_defaults()
    yield
    structlog.reset_defaults()


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_configure_structlog_is_idempotent():
    structlog_config.configure_structlog()
    structlog_config.configure_structlog()
    # No exception. structlog.is_configured() should be True after.
    assert structlog.is_configured()


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_otel_trace_processor_skips_when_no_active_span():
    """When no recording span is active, the processor leaves the event
    dict unchanged."""
    event_dict: dict = {"event": "test", "key": "value"}
    out = structlog_config._add_otel_trace_context(None, "info", event_dict)
    assert "trace_id" not in out
    assert "span_id" not in out


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_otel_trace_processor_injects_trace_id_when_span_recording():
    """When a recording span is active, the processor adds 32-hex
    trace_id and 16-hex span_id keys."""
    provider = TracerProvider()
    tracer = provider.get_tracer("test")

    with patch.object(trace, "get_current_span", wraps=trace.get_current_span):
        with tracer.start_as_current_span("test-span"):
            event_dict: dict = {"event": "test"}
            out = structlog_config._add_otel_trace_context(None, "info", event_dict)

    assert "trace_id" in out
    assert "span_id" in out
    assert len(out["trace_id"]) == 32
    assert len(out["span_id"]) == 16
    # Both are hex.
    int(out["trace_id"], 16)
    int(out["span_id"], 16)


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_configure_structlog_with_json_format(monkeypatch):
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()
    log = structlog.get_logger("json-test")
    # Calling .info() on a json-configured logger does not raise.
    log.info("event", k=1)


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_bridge_uvicorn_loggers_repoints_them_at_root(monkeypatch, capsys):
    """uvicorn's loggers must propagate to the structlog root handler with no
    private handlers of their own, so their lines render as JSON (not uvicorn's
    plaintext ``INFO: …``) and carry the shared processor chain.

    Regression guard: without ``bridge_uvicorn_loggers`` the access log stays on
    uvicorn's own handler (propagate=False) and emits plaintext into an
    otherwise-JSON stream — see backend/utils/structlog_config.py.
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    # Simulate uvicorn's default LOGGING_CONFIG: a private handler +
    # propagate=False on each uvicorn logger.
    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        lg.handlers = [logging.NullHandler()]
        lg.propagate = False

    structlog_config.bridge_uvicorn_loggers()

    for name in structlog_config._UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        assert lg.handlers == [], f"{name} should have no private handlers"
        assert lg.propagate is True, f"{name} should propagate to root"

    # End-to-end: an access-style record now renders through the root structlog
    # handler as JSON, not uvicorn's plaintext formatter.
    logging.getLogger("uvicorn.access").info('127.0.0.1 - "GET /x HTTP/1.1" 200')
    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == '127.0.0.1 - "GET /x HTTP/1.1" 200'


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_stdlib_extra_fields_are_rendered(monkeypatch, capsys):
    """Fields passed via stdlib ``logging``'s ``extra={...}`` must survive
    the bridge into structlog and appear in the rendered output.

    Regression guard: without ``structlog.stdlib.ExtraAdder`` on the
    foreign pre-chain these keys are silently dropped, collapsing every
    web_vitals metric to a bare ``web_vitals`` line (see
    backend/routers/web_vitals.py).
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    logging.getLogger("backend.web_vitals").info(
        "web_vitals",
        extra={"web_vitals_name": "LCP", "web_vitals_rating": "good"},
    )

    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "web_vitals"
    assert record["web_vitals_name"] == "LCP"
    assert record["web_vitals_rating"] == "good"


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_console_exception_traceback_never_renders_local_secrets(monkeypatch, capsys):
    """A stdlib ``logger.warning(..., exc_info=True)`` call whose failing
    frame holds a secret-bearing local (e.g. a service ``cfg`` dict with FOS
    keys / a Fastly API token — see backend/cron/jobs/rum_sync.py's
    ``_reconcile_faro_bundle``) must never render those values into
    container logs.

    Regression guard: structlog's ``ConsoleRenderer`` defaults its
    exception formatter to ``rich.traceback`` with ``show_locals=True``
    (rich is a project dependency, so this fires in prod's default console
    format, not just when explicitly opted in). Every frame's local
    variables — including live secrets — used to get dumped verbatim into
    the traceback on any exception. ``configure_structlog`` must pin
    ``show_locals=False`` so this can't regress silently.
    """
    monkeypatch.delenv("STRUCTLOG_FORMAT", raising=False)  # console is the default
    structlog_config.configure_structlog()

    def _load_cfg():
        # Built separately from the raise site so the secret values are
        # only ever present as a LOCAL VARIABLE in the failing frame below —
        # never as a literal in the source line rich's code-context view
        # would echo back regardless of show_locals. That isolates this
        # test to the vulnerability class it's guarding: locals rendering.
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

    assert "AKIAFAKEEXAMPLE00000" not in output
    assert "fake-fos-secret-do-not-leak-00000000" not in output
    assert "fake-fastly-token-do-not-leak-11111111" not in output
    # The traceback itself, and the log message, must still render — this
    # isn't a test that exceptions stop being logged, only that locals don't.
    assert "RuntimeError" in output
    assert "Faro reconcile failed for svc-fake" in output


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_uvicorn_color_message_extra_is_dropped(monkeypatch, capsys):
    """uvicorn attaches ``extra={"color_message": ...}`` (ANSI + ``%``-template)
    to its boot/access records for its own colorizer. ExtraAdder would surface
    it as a noisy ``color_message='...'`` kv pair on every uvicorn line; the
    bridge must strip it.

    Regression guard for the ``Uvicorn running on ... color_message='...'``
    line that showed up in container logs once uvicorn was bridged through
    structlog (see ``_drop_uvicorn_color_message``).
    """
    monkeypatch.setenv("STRUCTLOG_FORMAT", "json")
    structlog_config.configure_structlog()

    logging.getLogger("uvicorn.error").info(
        "Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)",
        extra={"color_message": "Uvicorn running on \x1b[1m%s://%s:%d\x1b[0m (Press CTRL+C to quit)"},
    )

    line = capsys.readouterr().err.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "Uvicorn running on http://0.0.0.0:8000 (Press CTRL+C to quit)"
    assert "color_message" not in record


# ── Merged from test_system_jobs.py ──────────────────────────────────────────────


from backend.cron import decorators as system_jobs


def _reset() -> None:
    with system_jobs._status_lock:
        system_jobs._status.clear()


@pytest.fixture()
def _clear_system_jobs() -> None:
    _reset()


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_initial_status_is_empty():
    assert system_jobs.get_system_job_status() == {}


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_record_job_run_writes_snapshot():
    system_jobs.record_job_run("rdns", status="success", duration_s=1.234, detail="processed 12")
    snap = system_jobs.get_system_job_status()
    assert "rdns" in snap
    entry = snap["rdns"]
    assert entry["status"] == "success"
    assert entry["duration_s"] == 1.23  # rounded to 2dp
    assert entry["detail"] == "processed 12"
    assert entry["last_run_at"].endswith("Z")


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_record_job_run_overwrites_previous():
    system_jobs.record_job_run("rdns", "success", 1.0)
    system_jobs.record_job_run("rdns", "error", 2.0, detail="boom")
    snap = system_jobs.get_system_job_status()
    assert snap["rdns"]["status"] == "error"
    assert snap["rdns"]["duration_s"] == 2.0
    assert snap["rdns"]["detail"] == "boom"


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_get_returns_a_copy_not_live_state():
    """The returned dict must not let callers mutate the internal store."""
    system_jobs.record_job_run("rdns", "success", 0.5)
    snap = system_jobs.get_system_job_status()
    snap.pop("rdns")
    # Original store still has it
    assert "rdns" in system_jobs.get_system_job_status()


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
def test_last_run_at_format():
    with patch("backend.cron.decorators.iso_z_now", return_value="2026-05-15T12:30:45Z"):
        system_jobs.record_job_run("bots", "success", 0.1)
    assert system_jobs.get_system_job_status()["bots"]["last_run_at"] == "2026-05-15T12:30:45Z"


# ── Merged from test_testcontainers_smoke.py ──────────────────────────────────────────────

import shutil
import socket

import pytest


def _docker_available() -> bool:
    """Skip the test if Docker CLI isn't usable.

    Two failure modes: the binary isn't installed, or it's installed
    but the daemon isn't running (Docker Desktop closed on macOS).
    Both warrant a skip rather than a hard failure — testcontainers
    is opt-in and shouldn't gate the suite on local environment shape.
    """
    if shutil.which("docker") is None:
        return False
    # Attempt a connect to the daemon's default Unix socket on macOS /
    # Linux. We don't actually issue a Docker API call here — just
    # check the socket is reachable, since `testcontainers` itself will
    # do a richer probe on first use.
    for path in (
        "/var/run/docker.sock",
        "/Users/" + (__import__("os").environ.get("USER") or "") + "/.docker/run/docker.sock",
    ):
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            s.settimeout(0.5)
            s.connect(path)
            s.close()
            return True
        except OSError:
            continue
    return False


@pytest.mark.usefixtures("_clean_state")
@pytest.mark.usefixtures("_clear_buffer")
@pytest.mark.usefixtures("_reset_structlog")
@pytest.mark.usefixtures("_clear_system_jobs")
@pytest.mark.skipif(
    not _docker_available(),
    reason="testcontainers requires a running Docker daemon",
)
def test_can_spawn_a_container_and_read_back_its_id():
    """Pin: testcontainers' DockerContainer lifecycle works in this
    repo's venv. The next integration test that needs a real container
    dep can crib this pattern."""
    pytest.importorskip("testcontainers.core.container")
    from testcontainers.core.container import DockerContainer

    with DockerContainer("alpine:3.20").with_command("sleep 60") as c:
        # The wrapped container exposes its id once started.
        cid = c.get_wrapped_container().id
        assert cid, "expected a docker container id once the container started"
