"""Per-path directory-size cache shared by sync-status + health endpoints.

Lives in utils (not routers) so cron jobs and the sync-status snapshot can
use it without creating a cron -> routers import edge (import-linter).
"""

from __future__ import annotations

import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

_DIR_SIZE_CACHE: dict[str, tuple[float, int]] = {}
_DIR_STATS_CACHE: dict[str, tuple[float, int, int]] = {}
_DIR_SIZE_TTL_S = 120.0

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


def _schedule_dir_walk(
    path: str,
    target,
    thread_name_prefix: str,
    in_flight: set[str],
    lock: threading.Lock,
    thread_cls=threading.Thread,
) -> None:
    try:
        thread_cls(
            target=target,
            args=(path,),
            name=f"{thread_name_prefix}:{os.path.basename(path)}",
            daemon=True,
        ).start()
    except Exception:
        with lock:
            in_flight.discard(path)


def get_dir_stats_swr(
    path: str,
    *,
    stats_cache: dict[str, tuple[float, int, int]],
    size_cache: dict[str, tuple[float, int]],
    shared_stats_cache: dict[str, tuple[float, int, int]],
    in_flight: set[str],
    lock: threading.Lock,
    ttl: float,
    walk_fn=None,
    thread_cls=threading.Thread,
) -> tuple[int, int]:
    """Return ``(total_size_bytes, file_count)`` with stale-while-revalidate semantics."""
    walker = walk_fn or walk_dir_stats
    now = time.monotonic()
    with lock:
        entry = stats_cache.get(path)
        if entry is not None and (now - entry[0]) < ttl:
            return (entry[1], entry[2])
        best_effort = (entry[1], entry[2]) if entry is not None else (0, 0)
        schedule_refresh = path not in in_flight
        if schedule_refresh:
            in_flight.add(path)

    if schedule_refresh:

        def _refresh(p: str) -> None:
            try:
                total_size, file_count = walker(p)
                ts = time.monotonic()
                with lock:
                    stats_cache[p] = (ts, total_size, file_count)
                    if stats_cache is shared_stats_cache:
                        size_cache[p] = (ts, total_size)
            finally:
                with lock:
                    in_flight.discard(p)

        _schedule_dir_walk(path, _refresh, "dir-stats-refresh", in_flight, lock, thread_cls)
    return best_effort


def _get_dir_size(path: str) -> int:
    # Coalescing semantics: fresh entry returns cached value; stale entry returns
    # stale immediately and schedules one background refresh; cold entry walks
    # synchronously under a per-path cold lock.
    now = time.monotonic()
    schedule_refresh = False
    with _dir_size_lock:
        cached = _DIR_SIZE_CACHE.get(path)
        if cached is not None and (now - cached[0]) < _DIR_SIZE_TTL_S:
            return cached[1]
        stats_cached = _DIR_STATS_CACHE.get(path)
        if stats_cached is not None and (now - stats_cached[0]) < _DIR_SIZE_TTL_S:
            _DIR_SIZE_CACHE[path] = (stats_cached[0], stats_cached[1])
            return stats_cached[1]
        if cached is not None:
            if path not in _dir_size_refresh_in_flight:
                _dir_size_refresh_in_flight.add(path)
                schedule_refresh = True
            stale_value = cached[1]

    if cached is not None:
        if schedule_refresh:
            _schedule_dir_walk(
                path,
                _refresh_dir_size_background,
                "dir-size-refresh",
                _dir_size_refresh_in_flight,
                _dir_size_lock,
            )
        return stale_value

    with _dir_size_cold_locks_meta_lock:
        cold_lock = _dir_size_cold_locks.setdefault(path, threading.Lock())
    try:
        with cold_lock:
            with _dir_size_lock:
                cached = _DIR_SIZE_CACHE.get(path)
                if cached is not None:
                    return cached[1]
            return _refresh_dir_size_background(path)
    finally:
        with _dir_size_cold_locks_meta_lock:
            _dir_size_cold_locks.pop(path, None)


def _refresh_dir_size_background(path: str) -> int:
    """Run the walk and write the result back into the shared size/stats caches."""
    try:
        total, file_count = walk_dir_stats(path)
        ts = time.monotonic()
        with _dir_size_lock:
            _DIR_SIZE_CACHE[path] = (ts, total)
            _DIR_STATS_CACHE[path] = (ts, total, file_count)
        return total
    finally:
        with _dir_size_lock:
            _dir_size_refresh_in_flight.discard(path)


def walk_dir_stats(path: str) -> tuple[int, int]:
    """Synchronous os.scandir walk. Returns (total_size_bytes, file_count).
    Symlinks are skipped (preserves the prior os.walk behavior)."""
    if not os.path.exists(path):
        return (0, 0)
    total_size = 0
    file_count = 0
    stack = [path]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry_de in it:
                    try:
                        if entry_de.is_symlink():
                            continue
                        if entry_de.is_dir(follow_symlinks=False):
                            stack.append(entry_de.path)
                        elif entry_de.is_file(follow_symlinks=False):
                            total_size += entry_de.stat(follow_symlinks=False).st_size
                            file_count += 1
                    except OSError:
                        continue
        except OSError:
            continue
    return (total_size, file_count)


def _scan_dir_size(path: str) -> int:
    return walk_dir_stats(path)[0]


# R-1: drain the per-path size cache between tests so a previous test's
# sandbox directory size doesn't bleed into a later test that reuses the
# same path string.
from backend.utils.cache_registry import CacheRegistry as _CacheRegistry  # noqa: E402

_CacheRegistry.register("utils.dir_size._DIR_SIZE_CACHE", _DIR_SIZE_CACHE)
_CacheRegistry.register("utils.dir_size._DIR_STATS_CACHE", _DIR_STATS_CACHE)
_CacheRegistry.register("utils.dir_size._dir_size_cold_locks", _dir_size_cold_locks)
