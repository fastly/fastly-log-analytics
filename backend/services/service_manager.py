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
# contents change on cron tick (every 2 min for most services), so a 60s
# TTL is comfortably below the freshness floor users notice in the
# "Local Cache" column while eliminating the per-request walk.
_DIR_STATS_TTL_SEC = 60.0
_dir_stats_cache: dict[str, tuple[float, int, int]] = {}
_dir_stats_lock = threading.Lock()


def _get_dir_stats(path: str) -> tuple[int, int]:
    """Return ``(total_size_bytes, file_count)`` for ``path`` recursively.

    Uses os.scandir + DirEntry.stat so each file costs ~1 syscall instead
    of the os.walk+islink+getsize trio (3+ per file). Cache dirs with
    thousands of small parquet files were the main motivator.
    Symlinks are skipped (preserves the prior os.walk behavior).
    """
    now = time.monotonic()
    with _dir_stats_lock:
        entry = _dir_stats_cache.get(path)
        if entry and (now - entry[0]) < _DIR_STATS_TTL_SEC:
            return (entry[1], entry[2])
    if not os.path.exists(path):
        with _dir_stats_lock:
            _dir_stats_cache[path] = (now, 0, 0)
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
    with _dir_stats_lock:
        _dir_stats_cache[path] = (now, total_size, file_count)
    return (total_size, file_count)


def _bust_dir_stats_cache(path: str | None = None) -> None:
    """Invalidate a cached dir-stat entry. Called after operations that
    materially change the cache contents (rebuild, teardown, ingest)
    so the dashboard's Local Cache column updates immediately."""
    with _dir_stats_lock:
        if path is None:
            _dir_stats_cache.clear()
            return
        _dir_stats_cache.pop(path, None)


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
