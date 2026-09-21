"""NGWAF sync and cache admin endpoints."""

from __future__ import annotations

import logging

from fastapi import HTTPException, Query

from backend.utils.router_utils import bad_request, not_found, raise_internal

from ._router import router

logger = logging.getLogger(__name__)


@router.post("/admin/ngwaf/sync/{service_id}")
def trigger_ngwaf_sync_endpoint(service_id: str):
    """Trigger manual NGWAF bot sync for a service."""
    from backend import config as svcconfig
    from backend.cron.jobs.metadata import _run_ngwaf_bot_sync
    from backend.utils.ngwaf_bot_cache import get_cache_stats

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail=not_found("service_not_found"))

    workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
    if not workspace_id:
        raise HTTPException(status_code=400, detail=bad_request("NGWAF workspace not configured for this service"))

    try:
        _run_ngwaf_bot_sync(service_id)
        return {"ok": True, "service_id": service_id, "workspace_id": workspace_id, "stats": get_cache_stats()}
    except Exception as e:
        raise_internal(logger, e, code="ngwaf_sync_failed", status=502)


@router.get("/admin/ngwaf/status")
def get_ngwaf_status_endpoint(service_id: str | None = Query(None, description="Optional service ID")):
    """Return NGWAF cache statistics and workspace sync watermarks."""
    from backend import config as svcconfig
    from backend.utils.ngwaf_bot_cache import get_cache_stats

    stats = get_cache_stats()
    if service_id:
        workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
        stats["service_workspace_id"] = workspace_id
        stats["service_configured"] = bool(workspace_id)
    return {"ok": True, **stats}
