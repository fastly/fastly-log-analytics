"""Service configuration and enriched status manager."""

from __future__ import annotations

import os
import threading
from typing import Any

import backend.utils.dir_size as _dir_size_mod
from backend import config as svcconfig
from backend.core import duckdb as _db
from backend.utils.bounded_cache import BoundedTTLCache
from backend.utils.bounded_cache import CacheRegistry as _CacheRegistry
from backend.utils.dir_size import (
    _DEFAULT_WALK_DIR_STATS,
    get_dir_stats_swr,
)
from backend.utils.dir_size import (
    _DIR_SIZE_CACHE as _shared_dir_size_cache,
)
from backend.utils.dir_size import (
    _DIR_STATS_CACHE as _shared_dir_stats_cache,
)
from backend.utils.dir_size import (
    _dir_size_lock as _shared_dir_size_lock,
)
from backend.utils.dir_size import (
    walk_dir_stats as _walk_dir_stats,
)

_DIR_STATS_TTL_SEC = 300.0
_dir_stats_cache: Any = BoundedTTLCache(maxsize=4096, ttl_seconds=_DIR_STATS_TTL_SEC)
_dir_stats_lock = _shared_dir_size_lock
_dir_stats_refresh_in_flight: set[str] = set()


def _get_dir_stats(path: str) -> tuple[int, int]:
    """Return ``(total_size_bytes, file_count)`` for ``path`` recursively via shared SWR cache."""
    fn = (
        _dir_size_mod.walk_dir_stats if _dir_size_mod.walk_dir_stats is not _DEFAULT_WALK_DIR_STATS else _walk_dir_stats
    )
    return get_dir_stats_swr(
        path,
        stats_cache=_dir_stats_cache,
        size_cache=_shared_dir_size_cache,
        shared_stats_cache=_shared_dir_stats_cache,
        in_flight=_dir_stats_refresh_in_flight,
        lock=_dir_stats_lock,
        ttl=_DIR_STATS_TTL_SEC,
        walk_fn=fn,
        thread_cls=threading.Thread,
    )


def get_enriched_services(active_service_id: str | None = None) -> list[dict[str, Any]]:
    """Return all configured services enriched with status and database stats.

    Used by both /bootstrap and /services endpoints to ensure consistent representation.
    """
    configs = svcconfig.list_configs()
    name_map = svcconfig.refresh_all_service_names(configs)

    hs_service_ids = {s.strip() for s in os.getenv("HIGH_SCALE_SERVICE_IDS", "").split(",") if s.strip()}

    result = []
    for cfg in configs:
        sid = cfg.get("service_id", "")
        if not sid:
            continue
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

        rum_val = cfg.get("rum")
        rum_dict: dict[str, Any] = rum_val if isinstance(rum_val, dict) else {}
        rum_lp = rum_dict.get("log_period")
        if rum_lp is None:
            rum_lp = cfg.get("rum_log_period")
        if rum_lp is not None:
            try:
                rum_lp = int(rum_lp)
            except (ValueError, TypeError):
                rum_lp = None

        is_hs = svcconfig.is_high_scale_mode(src_dict) and (not hs_service_ids or sid in hs_service_ids)
        result.append(
            {
                "service_id": sid,
                "name": name,
                "storage_mode": cfg.get("storage_mode", "cloud"),
                "is_high_scale": is_hs,
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
                "logging_enabled": cfg.get("logging_enabled", True),
                "rum_enabled": svcconfig.is_rum_enabled(cfg),
                "rum": cfg.get("rum"),
                "rum_log_period": rum_lp,
                "cmcd_enabled": bool((cfg.get("cmcd") or {}).get("enabled")),
            }
        )

    # Sort: active first, then alphabetically
    result.sort(key=lambda x: (not x["is_active"], x["name"].lower()))
    return result


_CacheRegistry.register("services.service_manager._dir_stats_cache", _dir_stats_cache)
_CacheRegistry.register("services.service_manager._dir_stats_refresh_in_flight", _dir_stats_refresh_in_flight)
