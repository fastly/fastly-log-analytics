"""POP locations admin endpoints."""

from __future__ import annotations

from fastapi import HTTPException, Query
from pydantic import BaseModel, Field

from backend.models.admin import PopLocationsResponse

from ._router import router


@router.get("/admin/pop-locations", response_model=PopLocationsResponse)
def get_pop_locations():
    """Return the cached POP locations (code, name, coordinates)."""
    from backend.utils.pop_utils import get_pop_locations

    return PopLocationsResponse.with_telemetry(pops=get_pop_locations())


class RefreshPopLocationsRequest(BaseModel):
    token: str = Field(..., description="Fastly API key")


@router.post("/admin/pop-locations/refresh", response_model=PopLocationsResponse)
def refresh_pop_locations(req: RefreshPopLocationsRequest | None = None, token: str | None = Query(default=None)):
    """Refresh the POP locations cache from the Fastly API."""
    api_key = ""
    if req is not None:
        api_key = req.token.strip()

    if not api_key:
        if token is None:
            raise HTTPException(status_code=422, detail="token is required")
        api_key = token.strip()
        if not api_key:
            raise HTTPException(status_code=400, detail={"error": "api_key is required"})

    from backend.utils.pop_utils import fetch_pop_locations, get_pop_locations

    ok = fetch_pop_locations(api_key)
    if not ok:
        raise HTTPException(
            status_code=502, detail={"error": "Failed to fetch POP data from Fastly API. Check your API key."}
        )
    return PopLocationsResponse.with_telemetry(pops=get_pop_locations())
