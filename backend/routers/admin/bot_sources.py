"""Bot-sources admin endpoints."""

from __future__ import annotations

import logging

from fastapi import HTTPException, Query

from backend.models.admin import BotSourcesResponse, PopLocationsResponse, RefreshPopLocationsRequest
from backend.models.admin_actions import (
    BotSourceRefreshResponse,
    BotSourcesRefreshResponse,
    NgwafSyncResponse,
    RdnsEnrichResponse,
)
from backend.models.admin_status import NgwafStatusResponse, RdnsStatsResponse
from backend.utils.router_utils import (
    bad_request,
    load_service_config,
    not_found,
    query_errors,
    raise_internal,
    validation_failed,
)

from ._router import router

logger = logging.getLogger(__name__)


@router.get("/admin/pop-locations", response_model=PopLocationsResponse)
def get_pop_locations():
    """Return the cached POP locations (code, name, coordinates)."""
    from backend.utils.pop_utils import get_pop_locations

    return PopLocationsResponse.with_telemetry(pops=get_pop_locations())


@router.post("/admin/pop-locations/refresh", response_model=PopLocationsResponse)
def refresh_pop_locations(req: RefreshPopLocationsRequest | None = None, token: str | None = Query(default=None)):
    """Refresh the POP locations cache from the Fastly API."""
    api_key = ""
    if req is not None:
        api_key = req.token.strip()

    if not api_key:
        if token is None:
            raise HTTPException(status_code=422, detail=validation_failed("token_required", ["token is required"]))
        api_key = token.strip()
        if not api_key:
            raise HTTPException(status_code=400, detail=bad_request("api_key_required"))

    from backend.utils.pop_utils import fetch_pop_locations, get_pop_locations

    ok = fetch_pop_locations(api_key)
    if not ok:
        # Custom envelope: 502 still surfaces a user-friendly toast string
        # (frontend extractApiError reads detail.error verbatim) alongside
        # the machine-readable code.
        raise HTTPException(
            status_code=502,
            detail={
                "error": "Failed to fetch POP data from Fastly API. Check your API key.",
                "code": "fastly_pop_fetch_failed",
            },
        )
    return PopLocationsResponse.with_telemetry(pops=get_pop_locations())


@router.post("/admin/ngwaf/sync/{service_id}", response_model=NgwafSyncResponse, response_model_exclude_unset=True)
@query_errors(502, code="ngwaf_sync_failed", logger_obj=logger)
def trigger_ngwaf_sync_endpoint(service_id: str):
    """Trigger manual NGWAF bot sync for a service."""
    from backend import config as svcconfig
    from backend.cron.jobs.metadata import _run_ngwaf_bot_sync
    from backend.utils.ngwaf_bot_cache import get_cache_stats

    load_service_config(service_id)

    workspace_id = svcconfig.get_ngwaf_workspace_id(service_id)
    if not workspace_id:
        raise HTTPException(status_code=400, detail=bad_request("NGWAF workspace not configured for this service"))

    _run_ngwaf_bot_sync(service_id)
    return {"ok": True, "service_id": service_id, "workspace_id": workspace_id, "stats": get_cache_stats()}


@router.get("/admin/ngwaf/status", response_model=NgwafStatusResponse, response_model_exclude_unset=True)
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


@router.get("/admin/bot-sources", response_model=BotSourcesResponse)
def get_bot_sources_endpoint():
    """Return metadata for all bot sources plus rDNS cache stats."""
    from backend.utils.bot_sources import get_all_sources_meta
    from backend.utils.rdns_cache import get_stats as rdns_stats

    return BotSourcesResponse.with_telemetry(sources=get_all_sources_meta(), rdns=rdns_stats())


@router.post(
    "/admin/bot-sources/{source_id}/refresh", response_model=BotSourceRefreshResponse, response_model_exclude_unset=True
)
def refresh_bot_source_endpoint(source_id: str):
    """Fetch and re-cache a single bot source."""
    from backend.utils.bot_sources import fetch_and_cache_source

    try:
        meta = fetch_and_cache_source(source_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=not_found("bot_source_not_found"))
    except Exception as e:
        raise_internal(logger, e, code="bot_source_fetch_failed", status=502)
    return {"ok": True, "source": meta}


@router.post("/admin/bot-sources/refresh", response_model=BotSourcesRefreshResponse, response_model_exclude_unset=True)
@router.post("/admin/bots/refresh", response_model=BotSourcesRefreshResponse, response_model_exclude_unset=True)
def refresh_all_bot_sources_endpoint():
    """Fetch and re-cache all enabled bot sources."""
    from backend.utils.bot_sources import refresh_all_sources

    try:
        results = refresh_all_sources()
        failed = [r for r in results if r.get("failed")]
        succeeded = [r for r in results if not r.get("failed")]
        return {
            "ok": len(failed) == 0,
            "sources": results,
            "updated_count": len(succeeded),
            "failed_count": len(failed),
        }
    except Exception as e:
        raise_internal(logger, e, code="bot_sources_refresh_failed", status=502)


@router.post("/admin/rdns/enrich", response_model=RdnsEnrichResponse, response_model_exclude_unset=True)
def trigger_rdns_enrich_endpoint(
    limit: int | None = Query(None, description="Optional override for batch limit"),
):
    """Trigger manual rDNS enrichment batch."""
    from backend.utils.rdns_cache import enrich_batch

    summary = enrich_batch(limit=limit)
    return {"ok": True, "summary": summary}


@router.get("/admin/rdns/stats", response_model=RdnsStatsResponse, response_model_exclude_unset=True)
def get_rdns_stats_endpoint():
    """Return rDNS cache stats."""
    from backend.utils.rdns_cache import get_stats as rdns_stats

    return {"ok": True, "stats": rdns_stats()}
