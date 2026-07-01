"""Shared response-memo cache helpers for the repository layer.

Several repositories (origin, network) memoize each endpoint's full
response for a short TTL so nav-back / refetch ticks become dict lookups
instead of repeated parquet scans. The cache-KEY shape differs per
repository (different request params), but the get/put/marker discipline
is identical and easy to get subtly wrong — the ``is_cached``
serialization-alias trap and the "telemetry must not leak across
requests" rule both live here so callers can't skip them.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

from backend.repositories.utils.filters import filter_spec_attr
from backend.utils.bounded_cache import BoundedTTLCache

if TYPE_CHECKING:
    from backend.models.common import FiltersDict

# Keys never persisted in the memo cache: per-request telemetry would leak
# across requests if kept, and the cache-hit marker is stamped fresh on read.
_NEVER_CACHE = ("debug_queries", "debug_calls", "is_cached", "_is_cached")


def serialize_filters_for_key(filters: FiltersDict | None) -> dict[str, tuple[Any, list[str]]]:
    """Render a filter spec into the deterministic ``(mode, sorted-values)``
    shape used inside every response-cache key.

    Sorted by filter key, and each spec's values sorted as strings, so two
    semantically-equal filter sets hash identically regardless of insertion
    order. Extracted verbatim from origin/network ``_response_cache_key`` so
    the produced bytes stay identical (no cold-miss on deploy).
    """
    return {
        k: (filter_spec_attr(v, "mode"), sorted(str(x) for x in (filter_spec_attr(v, "values") or [])))
        for k, v in sorted((filters or {}).items())
    }


def digest_cache_key(payload: dict, src: dict) -> str:
    """Hash a per-endpoint ``payload`` dict plus the service identity into a
    response-cache key.

    The caller owns ``payload``'s key order (it is serialized as-is), so each
    repository keeps its existing field order and the emitted key bytes are
    unchanged. ``src`` resolves to ``name`` → ``service_id`` → ``""`` exactly
    as the inlined builders did.
    """
    body = json.dumps(payload, separators=(",", ":"), default=str)
    svc = src.get("name") or src.get("service_id") or ""
    return hashlib.sha256(f"{body}:{svc}".encode()).hexdigest()


def bucket_time_to_minute(ts: str | None) -> str | None:
    """Truncate an ISO timestamp to minute precision for cache-key bucketing.

    The frontend zustand store re-runs ``new Date()`` on a full page reload,
    so two reloads seconds apart would otherwise miss the response cache.
    Returns the input unchanged when it's too short to carry a minute field.
    """
    if not ts or len(ts) < 16:
        return ts
    return ts[:16]


def cache_get(cache: BoundedTTLCache, key: str) -> dict | None:
    """Return a copy of the cached payload with the cache-hit marker stamped,
    or ``None`` on a miss.

    The marker is stamped under the model FIELD name ``is_cached`` — not its
    ``_is_cached`` serialization alias, which Pydantic drops on validation
    (so a hit would otherwise serialize as ``"_is_cached": false``).
    """
    cached = cache.get(key)
    if cached is None:
        return None
    result = cached.copy()
    result["is_cached"] = True
    return result


def cache_put(cache: BoundedTTLCache, key: str, value: dict, *, strip: tuple[str, ...] = ()) -> None:
    """Persist ``value`` under ``key``, dropping per-request telemetry and the
    cache-hit marker (plus any extra ``strip`` keys, e.g. ``section_timings``)
    so the cached copy stays request-clean.
    """
    drop = set(_NEVER_CACHE) | set(strip)
    cache[key] = {k: v for k, v in value.items() if k not in drop}
