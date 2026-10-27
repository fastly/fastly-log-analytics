"""Thread-safe bounded TTL cache and process-wide cache registry."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from typing import Any

import cachetools

_MISSING = object()


class BoundedTTLCache:
    """Thread-safe TTL + LRU bounded cache backed by ``cachetools.TTLCache``."""

    def __init__(
        self,
        maxsize: int | None = None,
        ttl_seconds: float = 60.0,
        *,
        max_entries: int | None = None,
        name: str | None = None,
    ) -> None:
        if max_entries is not None:
            resolved_max = max_entries
        elif maxsize is None:
            resolved_max = 256
        else:
            resolved_max = maxsize
        if resolved_max <= 0:
            raise ValueError("maxsize must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.maxsize = resolved_max
        self.ttl = self.ttl_seconds = float(ttl_seconds)
        self.name = name
        self._cache: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=resolved_max,
            ttl=self.ttl,
            timer=lambda: time.monotonic(),
        )
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

    def set(self, key: Any, value: Any) -> None:
        self[key] = value

    def put(self, key: Any, value: Any) -> None:
        self[key] = value

    def __delitem__(self, key: Any) -> None:
        with self.lock:
            del self._cache[key]

    def delete(self, key: Any) -> bool:
        with self.lock:
            return self._cache.pop(key, _MISSING) is not _MISSING

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

    def values(self) -> list[Any]:
        with self.lock:
            return list(self._cache.values())

    def items(self) -> list[tuple[Any, Any]]:
        with self.lock:
            return list(self._cache.items())


BoundedCache = BoundedTTLCache


def _try_clear_cache(cache: Any) -> bool:
    clear_fn = getattr(cache, "clear", None)
    if not callable(clear_fn):
        return False
    try:
        clear_fn()
        return True
    except Exception:  # pragma: no cover — best-effort drain
        return False


_PREV_CACHE_ENTRIES: dict[str, Any] = getattr(globals().get("CacheRegistry"), "_entries", {})


class CacheRegistry:
    """Process-wide registry of clearable caches for test isolation and admin reset."""

    _entries: dict[str, Any] = _PREV_CACHE_ENTRIES
    _lock = threading.Lock()

    @classmethod
    def register(cls, name: str, cache: Any) -> None:
        with cls._lock:
            cls._entries[name] = cache

    @classmethod
    def clear_all(cls) -> None:
        with cls._lock:
            caches = list(cls._entries.values())
        for cache in caches:
            _try_clear_cache(cache)

    @classmethod
    def clear(cls, name: str) -> bool:
        with cls._lock:
            cache = cls._entries.get(name)
        return _try_clear_cache(cache) if cache is not None else False

    @classmethod
    def names(cls) -> list[str]:
        with cls._lock:
            return list(cls._entries.keys())

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            cls._entries.clear()


__all__ = [
    "BoundedCache",
    "BoundedTTLCache",
    "CacheRegistry",
    "_MISSING",
    "_PREV_CACHE_ENTRIES",
    "_try_clear_cache",
]
