"""Sync-status surface: cached snapshot reader, full /sync-status,
analyst-safe /log-extents, and /admin/ingested-files."""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from sse_starlette.sse import EventSourceResponse

from backend.deps import get_service_id, get_source
from backend.models.admin import IngestedFilesResponse, LogExtentsResponse, SyncStatusResponse
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.sync_status_publisher import publisher as sync_status_publisher
from backend.utils.router_utils import SSE_PASSTHROUGH_HEADERS, make_error, query_errors, raise_internal
from backend.utils.sse_subscription import iter_with_disconnect_ping

logger = logging.getLogger(__name__)

from ._dir_size import _get_dir_size
from ._router import router

# Dedicated router for the analyst-safe sync-status siblings (/api/log-extents
# + /stream). FastAPI MERGES router-level and route-level tags, so a bare
# ``tags=["meta"]`` on the decorators would yield ``["admin", "meta"]`` and
# leave the misleading "admin" audience signal the shared admin router stamps.
# A separate router carrying only the neutral "meta" tag is the clean way to
# drop "admin" while keeping the same /api paths and canonical error responses.
# Runtime audience is enforced by RemoteAccessMiddleware (path-based), not the
# OpenAPI tag, so this is a docs-only distinction. Included alongside
# ``admin.router`` in backend/main.py.
meta_router = APIRouter(prefix="/api", tags=["meta"], responses=DEFAULT_ERROR_RESPONSES)


# Implementation moved to backend/sync_status_snapshot.py so cron jobs can
# publish post-commit snapshots without a cron -> routers import edge
# (import-linter). Re-exported here for the historical import path; tests
# patch this module-level binding, so keep the name bound here.
from backend.sync_status_snapshot import compute_sync_status_cached  # noqa: E402, F401


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
        raise HTTPException(status_code=503, detail=make_error("db_busy", str(e), busy=True))
    except Exception as e:
        raise_internal(logger, e, code="sync_status_failed")


@meta_router.get("/log-extents", response_model=LogExtentsResponse)
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


@meta_router.get("/log-extents/stream")
async def log_extents_stream(
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> EventSourceResponse:
    """Analyst-safe SSE projection of the cached sync-status snapshot.

    Re-uses the existing ``sync_status_publisher`` (the admin
    ``/api/sync-status/stream`` endpoint subscribes to the same
    publisher) but projects every payload down to the two fields the
    header badge actually renders — ``latest_log_at`` + ``local_rows``
    — so analysts can receive real-time badge updates without leaking
    ``ngwaf_workspace_id``, ``active_run``, ``cdn_service_id``, etc.

    Path lives under ``/api/log-extents/*`` (the analyst-safe sibling
    of ``/api/sync-status``) so middleware does NOT auto-block it.
    Belt-and-suspenders: also listed in ``_ANALYST_SSE_ALLOWLIST`` in
    ``backend/utils/remote_access.py`` so the analyst-side firewall's
    SSE-defaults-off policy documents this opening explicitly.
    """
    if not service_id:
        raise HTTPException(status_code=422, detail=make_error("x_service_id_required"))

    def _project(snap: dict | None) -> dict | None:
        if snap is None:
            return None
        return {
            "latest_log_at": snap.get("latest_log_at"),
            "local_rows": snap.get("local_rows"),
            "rum": snap.get("rum"),
            "request": snap.get("request"),
        }

    async def stream() -> AsyncIterator[str]:
        # Initial snapshot so a freshly mounted analyst page doesn't
        # blank-flash waiting for the first cron tick.
        # compute_sync_status_cached touches disk (os.path.getsize +
        # directory traversal via _get_dir_size); calling it directly on
        # the SSE coroutine stalled the asyncio event loop for the
        # duration of the I/O, denying service to every other concurrent
        # request on the same worker. Off-load to a worker thread.
        initial = _project(await asyncio.to_thread(compute_sync_status_cached, service_id))
        if initial is not None:
            yield json.dumps(initial)

        # Race the publisher against disconnect detection — see
        # backend.utils.sse_subscription. Shields the long-lived
        # __anext__() task across timeouts so the publisher queue stays
        # parked between pings (a bare wait_for would cancel the
        # generator's q.get() and silently end the subscriber).
        async for payload in iter_with_disconnect_ping(
            sync_status_publisher.subscribe(service_id), request, ping_seconds=15
        ):
            projected = _project(payload)
            if projected is not None:
                yield json.dumps(projected)

    return EventSourceResponse(stream(), ping=5, headers=SSE_PASSTHROUGH_HEADERS)


@router.get("/admin/ingested-files", response_model=IngestedFilesResponse)
@query_errors(status_code=500)
def ingested_files(source: dict = Depends(get_source)) -> IngestedFilesResponse:
    from backend.core.duckdb import get_ingested_files

    res = get_ingested_files(None, source)
    response: IngestedFilesResponse = IngestedFilesResponse.with_telemetry(files=res)
    return response
