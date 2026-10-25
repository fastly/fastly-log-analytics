"""Thread-safe LRU+TTL cache.

Drop-in replacement for the ad-hoc ``dict[key, (timestamp, value)]``
cache pattern scattered through the codebase. Each cache enforces:

- **A maximum size.** Writes past ``maxsize`` evict the least-recently-
  used entry. Guards against unique-key cardinality (e.g., diverse
  dashboard filter combinations each minting a distinct cache key).
- **A TTL.** Reads return ``default`` for expired entries (they appear
  absent via ``__contains__`` / ``get``) and the underlying store
  evicts them on the next access.

Thin wrapper around ``cachetools.TTLCache`` (which is not thread-safe
on its own — every public method here holds an ``RLock``). The public
API is preserved so the 8 existing call sites consume the same shape.
"""

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Iterator
from typing import Any

import cachetools

_MISSING = object()


class BoundedTTLCache:
    """Thread-safe LRU+TTL cache backed by ``cachetools.TTLCache``.

    Construct with explicit ``maxsize`` and ``ttl_seconds``:

        cache = BoundedTTLCache(maxsize=500, ttl_seconds=30)

    Then use it like a dict — TTL handling is implicit:

        cache[key] = value            # stores; evicts LRU if over maxsize
        v = cache.get(key, default)   # returns default if missing OR expired
        if key in cache: ...          # False if missing OR expired
        cache.pop(key, default)       # removes; returns value or default
        cache.clear()                 # drop everything
        len(cache)                    # count of currently-live entries
    """

    def __init__(self, *, maxsize: int, ttl_seconds: float):
        if maxsize <= 0:
            raise ValueError("maxsize must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.maxsize = maxsize
        self.ttl = float(ttl_seconds)
        self._cache: cachetools.TTLCache = cachetools.TTLCache(maxsize=maxsize, ttl=ttl_seconds, timer=time.monotonic)
        self.lock = threading.RLock()

    def __contains__(self, key: Any) -> bool:
        with self.lock:
            return key in self._cache

    def __getitem__(self, key: Any) -> Any:
        with self.lock:
            return self._cache[key]

    def get(self, key: Any, default: Any = None) -> Any:
        with self.lock:
            return self._cache.get(key, default)

    def __setitem__(self, key: Any, value: Any) -> None:
        with self.lock:
            self._cache[key] = value

    def __delitem__(self, key: Any) -> None:
        with self.lock:
            del self._cache[key]

    def pop(self, key: Any, default: Any = _MISSING) -> Any:
        with self.lock:
            if default is _MISSING:
                return self._cache.pop(key)
            return self._cache.pop(key, default)

    def clear(self) -> None:
        with self.lock:
            self._cache.clear()

    def __len__(self) -> int:
        with self.lock:
            return len(self._cache)

    def __iter__(self) -> Iterator[Any]:
        with self.lock:
            return iter(list(self._cache.keys()))

    def keys(self) -> list[Any]:
        with self.lock:
            return list(self._cache.keys())


class CacheRegistry:
    """Singleton-style registry of module-level caches.

    Members must be one of:
      - dict / set / list (drained via .clear())
      - object with a callable .clear() method (e.g. BoundedTTLCache)
      - threading.Lock-protected dict — pass the underlying dict; the
        registry doesn't acquire the lock during clear because the
        test fixtures run between tests with no concurrent writers.
    """

    _entries: dict[str, Any] = {}
    _lock = threading.Lock()

    @classmethod
    def register(cls, name: str, cache: Any) -> None:
        """Register a cache under a unique ``name``. Re-registering the
        same name overwrites — useful when a module is reloaded under
        tests, harmless in production (modules import once)."""
        with cls._lock:
            cls._entries[name] = cache

    @classmethod
    def clear_all(cls) -> None:
        """Drain every registered cache. Best-effort: a cache that
        doesn't expose .clear() is skipped silently rather than
        breaking the test setup."""
        with cls._lock:
            entries = list(cls._entries.items())
        for _name, cache in entries:
            clear = getattr(cache, "clear", None)
            if callable(clear):
                try:
                    clear()
                except Exception:  # pragma: no cover — best-effort drain
                    continue

    @classmethod
    def clear(cls, name: str) -> bool:
        """Drain a single registered cache by ``name``. Returns True if a
        cache was found and cleared, False otherwise (best-effort, mirrors
        ``clear_all``). Lets a module invalidate another module's registered
        cache WITHOUT importing it — e.g. the provision router dropping the
        bootstrap router's short-TTL cache after a teardown/provision, which a
        direct import would forbid (import-linter "Routers are independent of
        each other")."""
        with cls._lock:
            cache = cls._entries.get(name)
        if cache is None:
            return False
        clear = getattr(cache, "clear", None)
        if not callable(clear):
            return False
        try:
            clear()
            return True
        except Exception:  # pragma: no cover — best-effort drain
            return False

    @classmethod
    def names(cls) -> list[str]:
        """Snapshot of registered cache names — useful for debugging."""
        with cls._lock:
            return list(cls._entries.keys())

    @classmethod
    def reset(cls) -> None:
        """Clear the registry itself. Only useful for tests that need
        to start from a clean slate."""
        with cls._lock:
            cls._entries.clear()


sys.modules["backend.utils.cache_registry"] = sys.modules[__name__]
