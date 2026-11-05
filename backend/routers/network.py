"""Network router — network health, quality, POP health, and bundle endpoints."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import JSONResponse

from backend.core.request_context import RequestContext, build_request_context
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.network import (
    get_pop_health as hs_get_pop_health,
)
from backend.high_scale.network import (
    network_bundle as hs_network_bundle,
)
from backend.high_scale.network import (
    network_health as hs_network_health,
)
from backend.high_scale.network import (
    network_quality as hs_network_quality,
)
from backend.models.analytics import (
    NetworkBundleRequest,
    NetworkBundleResponse,
    NetworkHealthRequest,
    NetworkHealthResponse,
    NetworkQualityRequest,
    NetworkQualityResponse,
    NetworkSectionName,
    PopHealthListResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.utils.date_utils import parse_iso_utc
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.telemetry import process_context_scope
from backend.utils.time_window import (
    invite_clamp_fingerprint,
    is_valid_range_token,
    quantize_anchor,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["network"], responses=DEFAULT_ERROR_RESPONSES)
network_router = router

CORE_SECTIONS: frozenset[str] = frozenset(
    {"summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"}
)
SHIELDING_SECTIONS: frozenset[str] = frozenset({"shielding_analysis"})

_expand_network_sections = make_section_expander(NetworkSectionName)


def _require_high_scale(ctx: RequestContext) -> HighScaleService:
    if ctx.high_scale is None:
        raise HTTPException(
            status_code=503,
            detail={"error": "ClickHouse service is not available for this service"},
        )
    return ctx.high_scale


def _resolve_network_window(
    req: NetworkHealthRequest | NetworkQualityRequest | NetworkBundleRequest,
    ctx: RequestContext,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Resolve and clamp the time window + cache-key tokens for network endpoints."""
    start_time, end_time = ctx.resolve_window(req)
    if is_valid_range_token(req.range_token):
        return (
            start_time,
            end_time,
            req.range_token,
            quantize_anchor(req.anchor),
            invite_clamp_fingerprint(ctx.analyst_session),
        )
    return start_time, end_time, None, None, None


@router.post("/network-health", response_model=NetworkHealthResponse)
@query_errors()
def network_health(
    req: NetworkHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time, _, _, _ = _resolve_network_window(req, ctx)
    _expand_network_sections(req.sections)
    high_scale_service = _require_high_scale(ctx)
    resp = hs_network_health(high_scale_service, req, start_time, end_time)
    if (cached_json := getattr(resp, "_cached_json_dict", None)) is not None:
        return JSONResponse(content=cached_json)
    return resp


@router.post("/network-quality", response_model=NetworkQualityResponse)
@query_errors()
def network_quality(
    req: NetworkQualityRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)
    high_scale_service = _require_high_scale(ctx)
    return hs_network_quality(high_scale_service, req, start_time, end_time)


@router.get("/network/pop-health", response_model=PopHealthListResponse)
def get_pop_health(
    ctx: RequestContext = Depends(build_request_context),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
):
    """Aggregate edge-routing and cache metrics across Fastly POP locations."""
    clamped_start, clamped_end = ctx.clamp(
        start_time.isoformat() if start_time is not None else None,
        end_time.isoformat() if end_time is not None else None,
    )
    clamped_start_dt = parse_iso_utc(clamped_start) if clamped_start else start_time
    clamped_end_dt = parse_iso_utc(clamped_end) if clamped_end else end_time
    high_scale_service = _require_high_scale(ctx)
    return hs_get_pop_health(high_scale_service, clamped_start_dt, clamped_end_dt)


def _network_bundle_impl(
    req: NetworkBundleRequest,
    ctx: RequestContext,
) -> Any:
    with process_context_scope("network:bundle"):
        start_time, end_time, _, _, _ = _resolve_network_window(req, ctx)
        if req.sections:
            _expand_network_sections(req.sections)
        high_scale_service = _require_high_scale(ctx)
        bundle_resp = hs_network_bundle(high_scale_service, req, start_time, end_time)
        if (cached_json := getattr(bundle_resp, "_cached_json_dict", None)) is not None:
            return JSONResponse(content=cached_json)
        return bundle_resp


@router.post("/network/bundle", response_model=NetworkBundleResponse)
@query_errors()
def network_bundle(
    req: NetworkBundleRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    return _network_bundle_impl(req, ctx)


@router.get("/network/bundle", response_model=NetworkBundleResponse)
@query_errors()
def network_bundle_get(
    start_time: str | None = None,
    end_time: str | None = None,
    range_token: str | None = None,
    anchor: str | None = None,
    metric: str = "health_score",
    bucket_seconds: int = 300,
    top_n: int = 30,
    map_asn: str = "all",
    region_country: str = "US",
    sections: list[NetworkSectionName] | None = Query(default=None),
    ctx: RequestContext = Depends(build_request_context),
):
    req = NetworkBundleRequest(
        start_time=start_time,
        end_time=end_time,
        range_token=range_token,
        anchor=anchor,
        metric=metric,
        bucket_seconds=bucket_seconds,
        top_n=top_n,
        map_asn=map_asn,
        region_country=region_country,
        sections=sections,
    )
    return _network_bundle_impl(req, ctx)
