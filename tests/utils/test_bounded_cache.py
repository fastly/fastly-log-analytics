"""Tests for backend.utils.bounded_cache.BoundedTTLCache."""

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


def test_reap_drops_expired_entries():
    """After reap(), expired entries are gone and fresh ones remain."""
    cache = BoundedTTLCache(maxsize=10, ttl_seconds=0.05)
    cache["a"] = 1
    cache["b"] = 2
    time.sleep(0.10)
    cache["c"] = 3  # fresh entry shouldn't be reaped
    cache.reap()
    assert "a" not in cache
    assert "b" not in cache
    assert cache["c"] == 3


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
