"""Process-wide registry of module-level caches.

A-3 (testing_suite_audit_2026-06-14.md). The existing
``tests/conftest.py::_reset_module_caches`` fixture enumerates every
module-level cache by hand. Each new hot-path cache risks the same
order-dependent leak the R-1 work uncovered. CacheRegistry inverts
that: modules ``register()`` their cache mappings/sets on import,
and the conftest fixture calls ``CacheRegistry.clear_all()`` to drain
everything.

Adoption is incremental — modules can register at the bottom of the
file after the cache is constructed; the conftest still falls back to
the explicit-clear path for caches that haven't migrated yet. The
goal isn't a big-bang refactor; the goal is "the next hot-path cache
that lands ships its registration in the same PR".

Usage:

    # backend/core/iceberg/_core.py (or wherever the cache lives)
    from backend.utils.cache_registry import CacheRegistry

    _table_object_cache: dict[...] = {}
    CacheRegistry.register("iceberg._table_object_cache", _table_object_cache)

    # tests/conftest.py
    from backend.utils.cache_registry import CacheRegistry
    CacheRegistry.clear_all()
"""

from __future__ import annotations

import threading
from typing import Any


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
