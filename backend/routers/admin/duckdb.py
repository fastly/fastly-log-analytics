"""DuckDB instance lifecycle and connection pool admin endpoints."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import Query

from backend.core import duckdb as _db
from backend.core import duckdb_pool as _pool
from backend.core import duckdb_recycle as _recycle

from ._router import router

logger = logging.getLogger(__name__)


@router.post("/admin/duckdb/recycle")
def trigger_duckdb_recycle_endpoint(
    force: bool = Query(False, description="Bypass RSS threshold check and force recycle"),
) -> dict[str, Any]:
    """Trigger on-demand recycle of DuckDB instances to free parquet metadata cache."""
    rss_before = _db.current_rss_bytes()
    threshold = _recycle._recycle_rss_threshold_bytes()

    if not force and threshold > 0 and rss_before is not None and rss_before < threshold:
        mb_before = rss_before / (1024 * 1024)
        mb_thresh = threshold / (1024 * 1024)
        return {
            "ok": True,
            "status": "skipped",
            "reason": f"RSS {mb_before:.0f}MB < threshold {mb_thresh:.0f}MB",
            "rss_mb": round(mb_before, 1),
            "threshold_mb": round(mb_thresh, 1),
        }

    detail = _recycle.recycle_once(reason="manual_admin")
    rss_after = _db.current_rss_bytes()
    freed = (rss_before - rss_after) if (rss_before is not None and rss_after is not None) else None

    return {
        "ok": True,
        "status": "recycled",
        "detail": detail,
        "rss_before_mb": round(rss_before / (1024 * 1024), 1) if rss_before is not None else None,
        "rss_after_mb": round(rss_after / (1024 * 1024), 1) if rss_after is not None else None,
        "freed_mb": round(freed / (1024 * 1024), 1) if freed is not None else None,
    }


@router.get("/admin/duckdb/status")
def get_duckdb_status_endpoint() -> dict[str, Any]:
    """Return memory metrics, recycle configuration, and active connection pool states."""
    rss = _db.current_rss_bytes()
    threshold = _recycle._recycle_rss_threshold_bytes()
    interval_min = _recycle.recycle_interval_min()
    barrier_active = _db.is_recycle_barrier_active()
    pool_stats = _pool.get_pool_status()

    return {
        "ok": True,
        "memory": {
            "current_rss_mb": round(rss / (1024 * 1024), 1) if rss is not None else None,
            "recycle_threshold_mb": round(threshold / (1024 * 1024), 1) if threshold > 0 else None,
            "recycle_interval_min": interval_min,
        },
        "barrier": {
            "is_active": bool(barrier_active),
            "active_paths": list(barrier_active) if isinstance(barrier_active, set) else [],
        },
        "pools": pool_stats["pools"],
        "retired_pools": pool_stats["retired_pools"],
    }
