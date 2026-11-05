"""Sync-status, log-extents, and ingested-files admin/meta endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from collections.abc import AsyncIterator
from typing import Any, cast

from fastapi import APIRouter, Depends, Query, Request
from sse_starlette.sse import EventSourceResponse

from backend.celery_app import publisher as sync_status_publisher
from backend.core import fos_client as _db_mod
from backend.core.request_context import get_service_id, get_source, require_service_access
from backend.cron.scheduler import compute_sync_status_cached
from backend.models.admin import (
    IngestedFilesResponse,
    LogExtentsResponse,
    SyncStatusResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.router_utils import (
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
    _scope: str | None = Depends(require_service_access),
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
        compute_fn = _resolve_compute_sync_status_cached()
        cached = compute_fn(service_id)
        if cached is not None:
            resp_cached: SyncStatusResponse = SyncStatusResponse.with_telemetry(**cached)
            return resp_cached

        status = dict(svcconfig.get_status(service_id) or {"configured": True})
        status.setdefault("configured", True)
        from backend.cron.scheduler import get_latest_progress_for_service

        active_run = get_latest_progress_for_service(service_id)
        if active_run:
            status["active_run"] = active_run
            status["busy"] = True

        cfg = svcconfig.load_config(service_id) or {}
        status["ngwaf_workspace_id"] = cfg.get("ngwaf_workspace_id")

        resp: SyncStatusResponse = SyncStatusResponse.with_telemetry(**status)
        return resp
    except Exception as e:
        raise_internal(logger, e, code="sync_status_failed")


@meta_router.get("/log-extents", response_model=LogExtentsResponse)
def log_extents(
    service_id: str | None = Depends(get_service_id),
    _scope: str | None = Depends(require_service_access),
) -> LogExtentsResponse:
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


async def _safe_sample_sync_status(service_id: str) -> dict[str, Any] | None:
    try:
        res: dict[str, Any] | None = await asyncio.to_thread(_resolve_compute_sync_status_cached(), service_id)
        return res
    except Exception:
        return None


@meta_router.get("/log-extents/stream")
async def log_extents_stream(
    request: Request,
    service_id: str | None = Depends(get_service_id),
    _scope: str | None = Depends(require_service_access),
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
        last_projected = initial

        out_q: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=16)

        async def _sub_feeder() -> None:
            try:
                async for payload in sync_status_publisher.subscribe(service_id):
                    projected = _project(payload)
                    if projected is not None:
                        await out_q.put(projected)
            finally:
                await out_q.put(None)

        async def _poll_feeder() -> None:
            while True:
                await asyncio.sleep(5.0)
                snap = await _safe_sample_sync_status(service_id)
                projected = _project(snap)
                if projected is not None:
                    await out_q.put(projected)

        async def _q_iter() -> AsyncIterator[dict[str, Any]]:
            while True:
                item = await out_q.get()
                if item is None:
                    break
                yield item

        tasks = [asyncio.ensure_future(_sub_feeder()), asyncio.ensure_future(_poll_feeder())]
        try:
            async for projected in iter_with_disconnect_ping(_q_iter(), request, ping_seconds=15):
                if projected != last_projected:
                    last_projected = projected
                    yield json.dumps(projected)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    return cast(EventSourceResponse, sse_response(stream()))


_INGESTED_FILES_CACHE: BoundedTTLCache[str, list[Any]] = BoundedTTLCache(maxsize=64, ttl_seconds=30.0)
CacheRegistry.register("sync_status._INGESTED_FILES_CACHE", _INGESTED_FILES_CACHE)


@router.get("/admin/ingested-files", response_model=IngestedFilesResponse)
@query_errors(status_code=500)
def ingested_files(source: dict[str, Any] = Depends(get_source)) -> IngestedFilesResponse:
    service_id = str(source.get("name", ""))
    use_cache = "PYTEST_CURRENT_TEST" not in os.environ
    res = _INGESTED_FILES_CACHE.get(service_id) if use_cache else None
    if res is None:
        res = _db.get_ingested_files(None, source)
        if use_cache and isinstance(res, list):
            _INGESTED_FILES_CACHE.set(service_id, res)
    response: IngestedFilesResponse = IngestedFilesResponse.with_telemetry(files=res)
    return response
