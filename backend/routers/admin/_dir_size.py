"""Per-path directory-size cache shared by sync-status + health endpoints."""

from __future__ import annotations

import os
import threading
import time

_DIR_SIZE_CACHE: dict[str, tuple[float, int]] = {}
_DIR_SIZE_TTL_S = 30.0

# Coalescing primitives (mirrors service_manager._get_dir_stats). The walk is
# O(files-in-tree) — ~700ms over the ~19k-file parquet cache — so a burst of
# concurrent /api/bootstrap calls that all miss the TTL at once would each
# fire their own recursive scan, saturating CPU/disk and slowing the sync
# cron: a prod-outage amplifier (2026-06-23). Stale-while-revalidate + a
# per-path cold-lock collapse N concurrent walks into one.
_dir_size_lock = threading.Lock()
_dir_size_refresh_in_flight: set[str] = set()
_dir_size_cold_locks: dict[str, threading.Lock] = {}
_dir_size_cold_locks_meta_lock = threading.Lock()


def _get_dir_size(path: str) -> int:
    # Cache results per-path with a 30s TTL. The cache walk is O(files-in-tree)
    # and the per-service cache grew from ~300 files to ~19k after the rollups
    # backfill (one parquet per field × hour). Files only grow incrementally
    # (ingest + rollup-recompute) so a 30s staleness window means the
    # dashboard's reported disk usage can lag by at most that window — worth it
    # vs measuring exact-to-the-millisecond size on a poll endpoint.
    #
    # Coalescing semantics (mirror service_manager._get_dir_stats):
    #   - Fresh entry (age < TTL): return cached value, no work.
    #   - Stale entry (age >= TTL): return stale immediately AND kick off a
    #     single background refresh (coalesced via the in-flight set), so the
    #     walk never blocks a request after the first load.
    #   - No entry (first-ever): walk synchronously under a per-path cold lock
    #     so N concurrent first arrivals produce one walk, not N.
    now = time.monotonic()
    schedule_refresh = False
    with _dir_size_lock:
        cached = _DIR_SIZE_CACHE.get(path)
        if cached is not None and (now - cached[0]) < _DIR_SIZE_TTL_S:
            return cached[1]
        if cached is not None:
            if path not in _dir_size_refresh_in_flight:
                _dir_size_refresh_in_flight.add(path)
                schedule_refresh = True
            stale_value = cached[1]

    if cached is not None:
        if schedule_refresh:
            try:
                threading.Thread(
                    target=_refresh_dir_size_background,
                    args=(path,),
                    name=f"dir-size-refresh:{os.path.basename(path)}",
                    daemon=True,
                ).start()
            except Exception:
                # Resource exhaustion — release the marker so the next reader
                # retries; serve stale this round.
                with _dir_size_lock:
                    _dir_size_refresh_in_flight.discard(path)
        return stale_value

    # First-ever request for this path: coalesce concurrent cold arrivals via
    # a per-path lock. The first arrival walks + populates the cache;
    # subsequent arrivals wait, then see the populated entry and return.
    with _dir_size_cold_locks_meta_lock:
        cold_lock = _dir_size_cold_locks.setdefault(path, threading.Lock())
    with cold_lock:
        with _dir_size_lock:
            cached = _DIR_SIZE_CACHE.get(path)
            if cached is not None:
                return cached[1]
        total = _scan_dir_size(path)
        with _dir_size_lock:
            _DIR_SIZE_CACHE[path] = (time.monotonic(), total)
        return total


def _refresh_dir_size_background(path: str) -> None:
    """Run the walk off-thread and write the result back into the cache.
    Guarded by _dir_size_refresh_in_flight so concurrent expired reads on the
    same path coalesce to a single background walk."""
    try:
        total = _scan_dir_size(path)
        with _dir_size_lock:
            _DIR_SIZE_CACHE[path] = (time.monotonic(), total)
    finally:
        with _dir_size_lock:
            _dir_size_refresh_in_flight.discard(path)


def _scan_dir_size(path: str) -> int:
    total = 0
    if not os.path.exists(path):
        return 0
    try:
        with os.scandir(path) as it:
            for entry in it:
                if entry.is_file():
                    total += entry.stat().st_size
                elif entry.is_dir():
                    total += _scan_dir_size(entry.path)
    except Exception:
        pass
    return total


# R-1: drain the per-path size cache between tests so a previous test's
# sandbox directory size doesn't bleed into a later test that reuses the
# same path string.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("routers.admin._dir_size._DIR_SIZE_CACHE", _DIR_SIZE_CACHE)
