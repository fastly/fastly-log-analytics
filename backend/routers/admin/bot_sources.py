"""Bot-sources admin endpoints."""

from __future__ import annotations

from fastapi import HTTPException

from backend.models.admin import BotSourcesResponse

from ._router import router


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
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch bot source: {e}")
    return {"ok": True, "source": meta}
