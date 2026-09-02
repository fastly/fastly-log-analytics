"""Cached async sampler for the admin celery-status SSE channel.

``get_celery_status`` does blocking broadcast waits (``inspect()``) plus
Redis scans, and the SSE feeder runs PER CONNECTION — without a shared
TTL cache, N open admin tabs would each hammer the broker every tick.
"""

import asyncio
import time

from backend.celery_status import get_celery_status

_CACHE_TTL_S = 4.0
_cache: dict = {"at": 0.0, "value": None}
_lock = asyncio.Lock()


async def sample_celery_status_cached():
    now = time.monotonic()
    if _cache["value"] is not None and now - _cache["at"] < _CACHE_TTL_S:
        return _cache["value"]
    async with _lock:
        # Re-check under the lock — a concurrent caller may have refreshed.
        now = time.monotonic()
        if _cache["value"] is not None and now - _cache["at"] < _CACHE_TTL_S:
            return _cache["value"]
        loop = asyncio.get_running_loop()
        value = await loop.run_in_executor(None, get_celery_status)
        _cache["value"] = value
        _cache["at"] = time.monotonic()
        return value
