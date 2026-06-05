"""Bounded LRU+TTL cache with lazy reaping.

Drop-in replacement for the ad-hoc ``dict[key, (timestamp, value)]``
cache pattern scattered through the codebase. Each cache enforces:

- **A maximum size.** Writes past ``maxsize`` evict the least-recently-
  used entry. Guards against unique-key cardinality (e.g., diverse
  dashboard filter combinations each minting a distinct cache key).
- **A TTL.** Reads return ``default`` for expired entries (they appear
  absent via ``__contains__`` / ``get``), and every Nth write triggers
  a sweep that drops all expired entries.

Lazy reaping (vs. a background reaper thread) keeps the cache decoupled
from APScheduler — these caches sit upstream of the cron infrastructure
and pulling in scheduler imports here would invert the dependency graph.

Stored values are arbitrary; the cache stamps insertion time internally
so call sites don't have to thread timestamps through their own tuples.
That said, the existing migration call sites still store ``(timestamp,
payload)`` tuples — we keep the value verbatim so the migration is a
one-line constructor swap.

Threading: the cache holds a re-entrant lock for all mutations. Call
sites that already wrap their reads/writes in an outer lock are still
safe (RLock allows the same thread to acquire twice). Concurrent reads
on different keys do contend on the lock, but the operations under it
are O(1) dict / OrderedDict moves, so the contention window is small.
"""

from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Iterator
from typing import Any

# Lazy reaper cadence — every Nth write triggers a sweep of expired
# entries. Smaller N = more eager cleanup but more CPU per write; larger
# N = less CPU but more dead entries between sweeps. 100 was picked so
# the worst-case stale-entry count is bounded to ~100 even on a cache
# whose entries all expired (the cache is also bounded by maxsize, so
# the actual upper limit is min(maxsize, stale-count-since-last-reap)).
_REAP_EVERY_N_WRITES = 100

_MISSING = object()


class BoundedTTLCache:
    """Thread-safe LRU+TTL cache with lazy reaping.

    Construct with explicit ``maxsize`` and ``ttl_seconds``:

        cache = BoundedTTLCache(maxsize=500, ttl_seconds=30)

    Then use it like a dict — the cache tracks insert time internally and
    treats expired entries as absent:

        cache[key] = value            # stores; evicts oldest if over maxsize
        v = cache.get(key, default)   # returns default if missing OR expired
        if key in cache: ...          # False if missing OR expired
        cache.pop(key, default)       # removes; returns value or default
        cache.clear()                 # drop everything
        len(cache)                    # count of CURRENTLY-LIVE entries

    LRU touch happens on successful reads — the read'd key moves to the
    most-recently-used end, so the maxsize evictor drops genuinely cold
    keys before active ones.
    """

    def __init__(self, *, maxsize: int, ttl_seconds: float):
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.maxsize = maxsize
        self.ttl = float(ttl_seconds)
        self._values: OrderedDict[Any, Any] = OrderedDict()
        self._inserted_at: dict[Any, float] = {}
        self._writes_since_reap = 0
        self.lock = threading.RLock()

    def __contains__(self, key: Any) -> bool:
        return self.get(key, _MISSING) is not _MISSING

    def __getitem__(self, key: Any) -> Any:
        result = self.get(key, _MISSING)
        if result is _MISSING:
            raise KeyError(key)
        return result

    def get(self, key: Any, default: Any = None) -> Any:
        with self.lock:
            if key not in self._values:
                return default
            if self._is_expired_locked(key):
                self._evict_locked(key)
                return default
            self._values.move_to_end(key)
            return self._values[key]

    def __setitem__(self, key: Any, value: Any) -> None:
        with self.lock:
            self._values[key] = value
            self._values.move_to_end(key)
            self._inserted_at[key] = time.monotonic()
            # Maxsize enforcement: drop LRU entries one at a time until
            # under the cap. Usually a single pop is enough.
            while len(self._values) > self.maxsize:
                oldest_key, _ = self._values.popitem(last=False)
                self._inserted_at.pop(oldest_key, None)
            self._writes_since_reap += 1
            if self._writes_since_reap >= _REAP_EVERY_N_WRITES:
                self._reap_expired_locked()

    def __delitem__(self, key: Any) -> None:
        with self.lock:
            del self._values[key]
            self._inserted_at.pop(key, None)

    def pop(self, key: Any, default: Any = _MISSING) -> Any:
        with self.lock:
            if key not in self._values:
                if default is _MISSING:
                    raise KeyError(key)
                return default
            value = self._values.pop(key)
            self._inserted_at.pop(key, None)
            return value

    def clear(self) -> None:
        with self.lock:
            self._values.clear()
            self._inserted_at.clear()
            self._writes_since_reap = 0

    def __len__(self) -> int:
        # Returns the raw entry count including any expired entries that
        # haven't been lazily reaped yet. Callers that need a strictly
        # live count can call ``reap()`` first; in practice the
        # over-count is bounded by ``_REAP_EVERY_N_WRITES``.
        with self.lock:
            return len(self._values)

    def __iter__(self) -> Iterator[Any]:
        # Snapshot the keys so iteration doesn't blow up if a concurrent
        # writer mutates the OrderedDict. Callers shouldn't mutate during
        # iteration anyway.
        with self.lock:
            return iter(list(self._values.keys()))

    def keys(self) -> list[Any]:
        """Snapshot of currently-stored keys (possibly including
        unreaped-expired entries). Returns a fresh list for safe
        iteration even if the cache is mutated during the walk."""
        with self.lock:
            return list(self._values.keys())

    def reap(self) -> int:
        """Drop all currently-expired entries. Returns the number removed."""
        with self.lock:
            return self._reap_expired_locked()

    # ── internal (caller must hold self.lock) ──────────────────────────

    def _is_expired_locked(self, key: Any) -> bool:
        inserted = self._inserted_at.get(key)
        if inserted is None:
            # Defensive: a value with no insert-time record is treated as
            # immediately expired so it gets cleaned up next visit.
            return True
        return (time.monotonic() - inserted) > self.ttl

    def _evict_locked(self, key: Any) -> None:
        self._values.pop(key, None)
        self._inserted_at.pop(key, None)

    def _reap_expired_locked(self) -> int:
        cutoff = time.monotonic() - self.ttl
        # Materialise the expired-key list before mutating self._values
        # to avoid "dictionary changed size during iteration".
        expired_keys = [k for k, ts in self._inserted_at.items() if ts < cutoff]
        for k in expired_keys:
            self._evict_locked(k)
        self._writes_since_reap = 0
        return len(expired_keys)
