"""Query presets cache and query row limits."""

from __future__ import annotations

from typing import Any

from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry

MAX_QUERY_ROWS = 10_000

_PRESETS_CACHE_TTL = 300.0
_presets_cache: BoundedTTLCache = BoundedTTLCache(maxsize=256, ttl_seconds=_PRESETS_CACHE_TTL)
_CacheRegistry.register("query._presets_cache", _presets_cache)


def get_cached_presets(service_id: str, format_hash: str | None) -> Any | None:
    if not format_hash:
        return None
    return _presets_cache.get((service_id, format_hash))


def set_cached_presets(service_id: str, format_hash: str | None, payload: Any) -> None:
    if not format_hash:
        return
    _presets_cache[(service_id, format_hash)] = payload


def invalidate_presets_cache(service_id: str, format_hash: str | None) -> None:
    if format_hash:
        _presets_cache.pop((service_id, format_hash), None)


clear_presets_cache = invalidate_presets_cache

__all__ = [
    "MAX_QUERY_ROWS",
    "clear_presets_cache",
    "get_cached_presets",
    "invalidate_presets_cache",
    "set_cached_presets",
]
