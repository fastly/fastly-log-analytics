"""Sync-status, log-extents, and ingested-files admin/meta endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections.abc import AsyncIterator
from typing import Any, cast

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sse_starlette.sse import EventSourceResponse

from backend.core import duckdb as _db_mod
from backend.deps import get_service_id, get_source
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.admin import (
    IngestedFilesResponse,
    LogExtentsResponse,
    SyncStatusResponse,
)
from backend.sync_status_publisher import publisher as sync_status_publisher
from backend.sync_status_snapshot import compute_sync_status_cached
from backend.utils.dir_size import _get_dir_size
from backend.utils.router_utils import (
    make_error,
    query_errors,
    raise_internal,
    require_service_id,
)
from backend.utils.sse import iter_with_disconnect_ping, sse_response

from ._router import router

_db: Any = _db_mod
logger = logging.getLogger(__name__)

meta_router = APIRouter(prefix="/api", tags=["meta"], responses=DEFAULT_ERROR_RESPONSES)

_ORIG_COMPUTE_SYNC_STATUS_CACHED = compute_sync_status_cached


def _resolve_compute_sync_status_cached() -> Any:
    if compute_sync_status_cached is not _ORIG_COMPUTE_SYNC_STATUS_CACHED:
        return compute_sync_status_cached
    admin_pkg = sys.modules.get("backend.routers.admin")
    if admin_pkg is not None:
        pkg_fn = getattr(admin_pkg, "compute_sync_status_cached", None)
        if pkg_fn is not None and pkg_fn is not _ORIG_COMPUTE_SYNC_STATUS_CACHED:
            return pkg_fn
    return compute_sync_status_cached


@router.get("/admin/sync-status", response_model=SyncStatusResponse)
@router.get("/sync-status", response_model=SyncStatusResponse)
def sync_status(
    service_id: str | None = Depends(get_service_id),
    skip_fos: bool = Query(default=False),
    force: bool = Query(default=False),
) -> SyncStatusResponse:
    from backend import config as svcconfig
    from backend.utils.telemetry import clear_queries

    clear_queries()

    src: dict[str, Any] | None = None
    if service_id:
        src = _db.get_source_for_service(service_id)
    if not src or not service_id:
        resp_empty: SyncStatusResponse = SyncStatusResponse.with_telemetry(configured=False)
        return resp_empty

    try:
        from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

        compute_fn = _resolve_compute_sync_status_cached()
        high_scale_service = get_high_scale_service_registry().resolve(service_id)
        if high_scale_service is not None:
            cached = compute_fn(service_id)
            if cached is not None:
                high_scale_response: SyncStatusResponse = SyncStatusResponse.with_telemetry(**cached)
                return high_scale_response

        if skip_fos and not force:
            cached = compute_fn(service_id)
            if cached is not None:
                resp_cached: SyncStatusResponse = SyncStatusResponse.with_telemetry(**cached)
                return resp_cached

        is_durable = svcconfig.is_durable_serving_mode(src)
        _con = _db.get_connection(source=src, max_wait=5, skip_view_update=True, read_only=is_durable)
        try:
            status = _db.get_sync_status(_con, src, skip_fos=skip_fos, force=force)
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
        logger.warning("db_busy service_id=%s: %s", service_id, e)
        raise HTTPException(
            status_code=503,
            detail=make_error("db_busy", "Database is busy; retry shortly", busy=True),
        ) from e
    except Exception as e:
        raise_internal(logger, e, code="sync_status_failed")


@meta_router.get("/log-extents", response_model=LogExtentsResponse)
def log_extents(service_id: str | None = Depends(get_service_id)) -> LogExtentsResponse:
    """Return only the earliest/latest log timestamps for the FilterBar."""
    from backend import config as svcconfig

    if not service_id:
        empty1: LogExtentsResponse = LogExtentsResponse.with_telemetry(configured=False)
        return empty1
    src = _db.get_source_for_service(service_id)
    if not src:
        empty2: LogExtentsResponse = LogExtentsResponse.with_telemetry(configured=False)
        return empty2

    compute_fn = _resolve_compute_sync_status_cached()
    try:
        cached = compute_fn(service_id)
        if not cached:
            cached = svcconfig.get_status(src["name"]) or {}
    except Exception:
        cached = svcconfig.get_status(src["name"]) or {}
    request_status = cached.get("request")
    latest = request_status.get("latest_log_at") if isinstance(request_status, dict) else cached.get("latest_log_at")
    resp: LogExtentsResponse = LogExtentsResponse.with_telemetry(
        configured=True,
        earliest_log_at=cached.get("earliest_log_at"),
        latest_log_at=latest,
    )
    return resp


@meta_router.get("/log-extents/stream")
async def log_extents_stream(
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> EventSourceResponse:
    """Analyst-safe SSE projection of the cached sync-status snapshot."""
    service_id = require_service_id(service_id)

    def _project(snap: dict[str, Any] | None) -> dict[str, Any] | None:
        if snap is None:
            return None
        return {
            "latest_log_at": snap.get("latest_log_at"),
            "local_rows": snap.get("local_rows"),
            "rum": snap.get("rum"),
            "request": snap.get("request"),
        }

    async def stream() -> AsyncIterator[str]:
        compute_fn = _resolve_compute_sync_status_cached()
        initial = _project(await asyncio.to_thread(compute_fn, service_id))
        if initial is not None:
            yield json.dumps(initial)

        async for payload in iter_with_disconnect_ping(
            sync_status_publisher.subscribe(service_id), request, ping_seconds=15
        ):
            projected = _project(payload)
            if projected is not None:
                yield json.dumps(projected)

    return cast(EventSourceResponse, sse_response(stream()))


@router.get("/admin/ingested-files", response_model=IngestedFilesResponse)
@query_errors(status_code=500)
def ingested_files(source: dict[str, Any] = Depends(get_source)) -> IngestedFilesResponse:
    res = _db.get_ingested_files(None, source)
    response: IngestedFilesResponse = IngestedFilesResponse.with_telemetry(files=res)
    return response
