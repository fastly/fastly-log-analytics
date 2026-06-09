"""Service management layer for consistent configuration listing and enrichment."""

import os
import threading
import time
from typing import Any

from backend import config as svcconfig
from backend.core import duckdb as _db

# Cache dirs hold thousands of small parquet files; recursively stat'ing
# them on every /api/bootstrap, /api/services, and admin tile render was a
# big chunk of the page-navigation lag (200-1500ms per call). The dir
# contents change on cron tick (every 2 min for most services), so a
# 5-minute TTL is comfortably below the freshness floor users notice in
# the "Local Cache" column while eliminating the per-request walk.
#
# Cold-path mitigation uses stale-while-revalidate: when a cached entry
# is expired but present, _get_dir_stats returns the stale value
# immediately and kicks off a background refresh. Only the very first
# request after process startup pays the full walk cost; subsequent
# requests never wait on the syscall storm even after TTL expiry.
_DIR_STATS_TTL_SEC = 300.0
_dir_stats_cache: dict[str, tuple[float, int, int]] = {}
_dir_stats_lock = threading.Lock()
_dir_stats_refresh_in_flight: set[str] = set()
# Per-path lock used ONLY on the cold (no-cache-entry) path to coalesce
# concurrent first arrivals so we don't fire N parallel walks for the
# same path. Created lazily; never removed (set of unique paths is
# bounded by the configured-services count).
_dir_stats_cold_locks: dict[str, threading.Lock] = {}
_dir_stats_cold_locks_meta_lock = threading.Lock()


def _walk_dir_stats(path: str) -> tuple[int, int]:
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


def _refresh_dir_stats_background(path: str) -> None:
    """Run the walk off-thread and write the result back into the cache.
    Guarded by _dir_stats_refresh_in_flight so concurrent expired reads
    on the same path coalesce to a single background walk."""
    try:
        total_size, file_count = _walk_dir_stats(path)
        with _dir_stats_lock:
            _dir_stats_cache[path] = (time.monotonic(), total_size, file_count)
    finally:
        with _dir_stats_lock:
            _dir_stats_refresh_in_flight.discard(path)


def _get_dir_stats(path: str) -> tuple[int, int]:
    """Return ``(total_size_bytes, file_count)`` for ``path`` recursively.

    Uses os.scandir + DirEntry.stat so each file costs ~1 syscall instead
    of the os.walk+islink+getsize trio (3+ per file). Cache dirs with
    thousands of small parquet files were the main motivator.

    Stale-while-revalidate semantics:
      - Fresh entry (age < TTL): return cached value, no work.
      - Stale entry (age >= TTL): return cached value immediately AND
        kick off a background refresh (coalesced via in-flight set —
        at most one background refresh per path at a time).
      - No entry (first-ever request for this path): walk synchronously,
        coalesced via a per-path cold-lock so N concurrent first arrivals
        produce one walk, not N.

    The cache stores the result even when the path doesn't exist, so
    nonexistent paths only stat once per TTL window.
    """
    now = time.monotonic()
    schedule_refresh = False
    with _dir_stats_lock:
        entry = _dir_stats_cache.get(path)
        if entry is not None and (now - entry[0]) < _DIR_STATS_TTL_SEC:
            return (entry[1], entry[2])
        # Either expired or never cached.
        if entry is not None:
            # Stale-while-revalidate: serve stale, schedule background
            # refresh under the lock; start the thread AFTER releasing
            # so Thread().start()'s allocation cost doesn't block other
            # readers under load.
            if path not in _dir_stats_refresh_in_flight:
                _dir_stats_refresh_in_flight.add(path)
                schedule_refresh = True
            stale_value = (entry[1], entry[2])

    if entry is not None:
        if schedule_refresh:
            try:
                threading.Thread(
                    target=_refresh_dir_stats_background,
                    args=(path,),
                    name=f"dir-stats-refresh:{os.path.basename(path)}",
                    daemon=True,
                ).start()
            except Exception:
                # Resource exhaustion (RuntimeError 'can't start new thread',
                # MemoryError). The cache must NOT be permanently stuck —
                # release the in-flight marker so the next reader can try
                # again. Serve stale this round.
                with _dir_stats_lock:
                    _dir_stats_refresh_in_flight.discard(path)
        return stale_value

    # First-ever request for this path: coalesce concurrent cold arrivals
    # via a per-path lock. The first arrival walks and populates the cache;
    # subsequent arrivals wait on the lock, then see the populated entry
    # and return immediately.
    with _dir_stats_cold_locks_meta_lock:
        cold_lock = _dir_stats_cold_locks.setdefault(path, threading.Lock())
    with cold_lock:
        with _dir_stats_lock:
            entry = _dir_stats_cache.get(path)
            if entry is not None:
                return (entry[1], entry[2])
        total_size, file_count = _walk_dir_stats(path)
        with _dir_stats_lock:
            _dir_stats_cache[path] = (time.monotonic(), total_size, file_count)
        return (total_size, file_count)


def get_enriched_services(active_service_id: str | None = None) -> list[dict[str, Any]]:
    """Return all configured services enriched with status and database stats.

    Used by both /bootstrap and /services endpoints to ensure consistent representation.
    """
    configs = svcconfig.list_configs()
    name_map = svcconfig.refresh_all_service_names(configs)

    result = []
    for cfg in configs:
        sid = cfg.get("service_id", "")
        name = name_map.get(sid, sid)

        db_path = cfg.get("duckdb_path") or svcconfig.duckdb_path(sid)
        db_exists = os.path.exists(db_path)
        db_size = os.path.getsize(db_path) if db_exists else 0

        src_dict = _db._source_from_config(cfg)
        cache_size, cache_file_count = _get_dir_stats(_db._cache_dir(src_dict))
        total_size = db_size + cache_size

        prov = cfg.get("provisioning", {})
        cached_status = svcconfig.get_status(sid)
        cron_stats = cached_status.get("cron_stats", {})
        log_row_count = cached_status.get("local_rows", 0)

        result.append(
            {
                "service_id": sid,
                "name": name,
                "access_level": cfg.get("access_level", "read_write"),
                "storage_mode": "cloud",
                "log_period": cfg.get("log_period", 60),
                "fos_bucket": cfg.get("fos_bucket", ""),
                "fos_region": cfg.get("fos_region", ""),
                "cdn_url": src_dict.get("cdn_url", ""),
                "cdn_service_id": cfg.get("cdn_service_id", ""),
                "duckdb_exists": db_exists,
                "duckdb_size_bytes": total_size,
                "cache_file_count": cache_file_count,
                "log_row_count": log_row_count,
                "is_active": sid == active_service_id,
                "cron_stats": cron_stats,
                "status": cached_status,
                "cron_sync": prov.get(
                    "cron_sync",
                    {
                        "enabled": True,
                        "interval_mins": 2,
                        "log_enabled": True,
                        "log_retention_days": 7,
                        "delete_after": True,
                    },
                ),
                "cron_compact": prov.get(
                    "cron_compact", {"enabled": True, "interval_mins": 30, "log_enabled": True, "log_retention_days": 7}
                ),
                "cron_ngwaf": prov.get(
                    "cron_ngwaf", {"interval_mins": 15, "log_enabled": True, "log_retention_days": 7}
                ),
                "ngwaf_workspace_id": cfg.get("ngwaf_workspace_id"),
            }
        )

    # Sort: active first, then alphabetically
    result.sort(key=lambda x: (not x["is_active"], x["name"].lower()))
    return result
