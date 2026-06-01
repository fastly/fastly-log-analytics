"""Debug router — surfaces in-process profiling data to the Debug Panel.

Currently exposes the SQLite query ring buffer captured by
backend/utils/sqlite_profiler.py. No service-scoped auth: this is local-dev
observability that mirrors what the existing DuckDB Debug Panel already
surfaces from /api/query responses. Add auth here if this project ever
hardens for multi-tenant use.
"""

from __future__ import annotations

from fastapi import APIRouter, Query

from backend.models.debug import ClearSqliteResponse, RecentSqliteResponse
from backend.utils import sqlite_profiler

router = APIRouter(prefix="/api/debug", tags=["debug"])


@router.get("/recent-sqlite", response_model=RecentSqliteResponse)
def recent_sqlite(
    limit: int = Query(200, ge=1, le=1000),
    since_seq: int = Query(0, ge=0),
):
    """Return up to ``limit`` most-recent SQLite statements captured since
    ``since_seq``. The Debug Panel polls this every 2s when SQL debug is on.
    """
    return sqlite_profiler.get_recent(limit=limit, since_seq=since_seq)


@router.post("/clear-sqlite", response_model=ClearSqliteResponse)
def clear_sqlite():
    """Drain the SQLite ring buffer. Manual reset for the Debug Panel."""
    sqlite_profiler.clear()
    return {"ok": True, **sqlite_profiler.buffer_stats()}
