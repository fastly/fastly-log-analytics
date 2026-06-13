"""Sync-status surface: cached snapshot reader, full /sync-status,
analyst-safe /log-extents, and /admin/ingested-files."""

from __future__ import annotations

import logging
import os

from fastapi import Depends, HTTPException, Query

from backend.deps import get_service_id, get_source
from backend.models.admin import IngestedFilesResponse, LogExtentsResponse, SyncStatusResponse
from backend.utils.router_utils import query_errors

logger = logging.getLogger(__name__)

from ._dir_size import _get_dir_size
from ._router import router


# Moved out of /admin/ so analysts can also see sync status / time bounds
# for their scoped service. The endpoint returns per-service timestamps and
# row counts — no admin-specific info. Service-scope is still enforced by
# RemoteAccessMiddleware via the x-service-id check on the request.
def compute_sync_status_cached(service_id: str | None) -> dict | None:
    """Return the cached sync-status payload for ``service_id`` without
    grabbing a DuckDB connection.

    Mirrors the ``skip_fos=true`` fast path of /api/sync-status:
    same shape, no DB hop, returns ``None`` when no cached status has
    been persisted yet (caller falls back to the dedicated endpoint).
    Extracted so /api/bootstrap can fold the status into its response
    (perf audit Phase D-2) and the dedicated endpoint can stay
    authoritative for explicit / force / non-cached paths.

    Caller is responsible for analyst-scope enforcement — the dedicated
    endpoint is admin-only via RemoteAccessMiddleware; this helper
    trusts the caller.
    """
    from backend import config as svcconfig
    from backend.core import duckdb as _db

    if not service_id:
        return None
    src = _db.get_source_for_service(service_id)
    if not src:
        return None
    cached_status = svcconfig.get_status(src["name"])
    if not cached_status:
        return None  # fall through to dedicated endpoint
    cached_status["access_level"] = src.get("access_level", "read_write")
    cached_status["storage_mode"] = _db.STORAGE_MODE
    cached_status["configured"] = True

    db_path = src.get("duckdb_path") or svcconfig.duckdb_path(service_id)
    db_exists = os.path.exists(db_path)
    db_size = os.path.getsize(db_path) if db_exists else 0
    cache_size = _get_dir_size(_db._cache_dir(src))
    cached_status["duckdb_size_bytes"] = db_size + cache_size
    cached_status["duckdb_exists"] = db_exists

    from backend.cron_progress import get_latest_progress_for_service

    active_run = get_latest_progress_for_service(service_id)
    if active_run:
        cached_status["active_run"] = active_run
        cached_status["busy"] = True

    cfg = svcconfig.load_config(service_id) or {}
    cached_status["ngwaf_workspace_id"] = cfg.get("ngwaf_workspace_id")
    return cached_status


@router.get("/sync-status", response_model=SyncStatusResponse)
def sync_status(
    service_id: str | None = Depends(get_service_id),
    skip_fos: bool = Query(default=False),
    force: bool = Query(default=False),
) -> SyncStatusResponse:
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.core.duckdb import get_sync_status
    from backend.utils.telemetry import clear_queries

    clear_queries()

    src: dict | None = None
    if service_id:
        src = _db.get_source_for_service(service_id)
    if not src or not service_id:
        resp_empty: SyncStatusResponse = SyncStatusResponse.with_telemetry(configured=False)
        return resp_empty

    try:
        # Fast path: skip_fos=true callers (FilterBar polling, badge in
        # the page header, etc.) only need the cached snapshot that the
        # sync cron refreshes every minute. Return it without grabbing a
        # DuckDB connection, so that a busy dashboard load — agg/raw/
        # bots all racing for connections — doesn't starve sync-status
        # and trigger 503s when its max_wait expires.
        if skip_fos and not force:
            cached = compute_sync_status_cached(service_id)
            if cached is not None:
                resp_cached: SyncStatusResponse = SyncStatusResponse.with_telemetry(**cached)
                return resp_cached

        from backend.core.duckdb import get_connection

        _con = get_connection(source=src, max_wait=5, skip_view_update=True)
        try:
            status = get_sync_status(_con, src, skip_fos=skip_fos, force=force)
        finally:
            _con.close()

        db_path = src.get("duckdb_path") or svcconfig.duckdb_path(service_id)
        db_exists = os.path.exists(db_path)
        db_size = os.path.getsize(db_path) if db_exists else 0

        cache_size = _get_dir_size(_db._cache_dir(src))

        status["duckdb_size_bytes"] = db_size + cache_size
        status["duckdb_exists"] = db_exists

        from backend.cron_progress import get_latest_progress_for_service

        active_run = get_latest_progress_for_service(service_id)
        if active_run:
            status["active_run"] = active_run
            status["busy"] = True

        cfg = svcconfig.load_config(service_id) or {}
        status["ngwaf_workspace_id"] = cfg.get("ngwaf_workspace_id")

        resp: SyncStatusResponse = SyncStatusResponse.with_telemetry(**status)
        return resp
    except _db.DBBusyError as e:
        raise HTTPException(status_code=503, detail={"error": str(e), "busy": True})
    except Exception as e:
        from backend.utils.router_utils import raise_internal

        raise_internal(logger, e, code="sync_status_failed")


@router.get("/log-extents", response_model=LogExtentsResponse)
def log_extents(service_id: str | None = Depends(get_service_id)) -> LogExtentsResponse:
    """Return only the earliest/latest log timestamps for the FilterBar.

    Analyst-safe sibling of ``/api/sync-status``: same cached-status fast
    path but projected down to the two fields the FilterBar actually
    reads. ``/api/sync-status`` is blocked for analysts because it leaks
    ``ngwaf_workspace_id`` and active cron-task state; this endpoint
    drops both, so the middleware lets it through and the FilterBar's
    snap-to-extents UX works for analysts too.

    Reads only the persisted status snapshot — no DuckDB connection
    grabbed, no contention with cron, no 503 path. The snapshot is
    refreshed by the sync cron every minute so a freshly started
    service sees populated extents within ~60s.
    """
    from backend import config as svcconfig
    from backend.core import duckdb as _db

    if not service_id:
        empty1: LogExtentsResponse = LogExtentsResponse.with_telemetry(configured=False)
        return empty1
    src = _db.get_source_for_service(service_id)
    if not src:
        empty2: LogExtentsResponse = LogExtentsResponse.with_telemetry(configured=False)
        return empty2

    cached = svcconfig.get_status(src["name"]) or {}
    resp: LogExtentsResponse = LogExtentsResponse.with_telemetry(
        configured=True,
        earliest_log_at=cached.get("earliest_log_at"),
        latest_log_at=cached.get("latest_log_at"),
    )
    return resp


@router.get("/admin/ingested-files", response_model=IngestedFilesResponse)
@query_errors(status_code=500)
def ingested_files(source: dict = Depends(get_source)) -> IngestedFilesResponse:
    from backend.core.duckdb import get_ingested_files

    res = get_ingested_files(None, source)
    response: IngestedFilesResponse = IngestedFilesResponse.with_telemetry(files=res)
    return response
