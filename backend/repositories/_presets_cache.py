"""Cache for the /api/presets payload.

The presets handler returns a static list of canned queries that only
changes when an admin edits the service's log_fields (which mints a new
``format_hash``). Opening a read-only DuckDB connection on every read
contends with ingest under analyst-30d load and tails into multi-second
outliers; keying the cached payload on ``(service_id, format_hash)``
makes the cache survive across requests but auto-invalidates the instant
log_fields change. Misses fall back to the uncached path so legacy
configs without ``format_hash`` still serve correctly.

This module lives in ``backend.repositories`` (not ``backend.routers``)
so ``services/core.py`` can call :func:`invalidate_presets_cache` after
``svcconfig.save_config`` without router-to-router import coupling.
"""

from __future__ import annotations

from typing import Any

from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.cache_registry import CacheRegistry

_PRESETS_CACHE_TTL = 300.0  # 5 minutes
_presets_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_PRESETS_CACHE_TTL)
# Registry key kept stable for backward compatibility with any debug
# tooling that greps for the old "query._presets_cache" location.
CacheRegistry.register("query._presets_cache", _presets_cache)


def get_cached_presets(service_id: str, format_hash: str | None) -> Any | None:
    """Return the cached payload for ``(service_id, format_hash)`` or
    ``None`` on miss / when ``format_hash`` is unset.
    """
    if not format_hash:
        return None
    return _presets_cache.get((service_id, format_hash))


def set_cached_presets(service_id: str, format_hash: str | None, payload: Any) -> None:
    """Store ``payload`` under ``(service_id, format_hash)``. No-op when
    ``format_hash`` is unset — legacy configs without a hash can't
    invalidate safely, so they don't get cached.
    """
    if not format_hash:
        return
    _presets_cache[(service_id, format_hash)] = payload


def invalidate_presets_cache(service_id: str, format_hash: str | None) -> None:
    """Drop the presets cache entry for ``(service_id, format_hash)``.

    Called from ``api_service_log_fields_set`` after a successful save so
    the next /api/presets read for the OLD hash misses cleanly instead of
    waiting for TTL eviction. The new hash naturally misses on first
    request and re-populates.
    """
    if format_hash:
        _presets_cache.pop((service_id, format_hash), None)
