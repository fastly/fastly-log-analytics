"""Bot-sources admin endpoints."""

from __future__ import annotations

import logging

from fastapi import HTTPException

from backend.models.admin import BotSourcesResponse
from backend.utils.router_utils import not_found, raise_internal

from ._router import router

logger = logging.getLogger(__name__)


@router.get("/admin/bot-sources", response_model=BotSourcesResponse)
def get_bot_sources_endpoint():
    """Return metadata for all bot sources plus rDNS cache stats."""
    from backend.utils.bot_sources import get_all_sources_meta
    from backend.utils.rdns_cache import get_stats as rdns_stats

    return BotSourcesResponse.with_telemetry(sources=get_all_sources_meta(), rdns=rdns_stats())


@router.post("/admin/bot-sources/{source_id}/refresh")
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
