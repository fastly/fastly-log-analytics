"""Bot-sources admin endpoints."""

from __future__ import annotations

import logging

from fastapi import HTTPException, Query

from backend.models.admin import BotSourcesResponse
from backend.models.admin_actions import (
    BotSourceRefreshResponse,
    BotSourcesRefreshResponse,
    RdnsEnrichResponse,
)
from backend.models.admin_status import RdnsStatsResponse
from backend.utils.router_utils import not_found, raise_internal

from ._router import router

logger = logging.getLogger(__name__)


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
