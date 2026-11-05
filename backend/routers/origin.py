"""Origin router — origin latency, status codes, slow URLs, IP health, and drilldowns."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends, HTTPException

from backend.core.request_context import RequestContext, build_request_context
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.network import origin_shielding_analysis as hs_shielding_analysis
from backend.high_scale.origin import (
    aggregates as high_scale_aggregates,
)
from backend.high_scale.origin import (
    origin_drilldown as hs_origin_drilldown,
)
from backend.high_scale.origin import (
    origin_section as hs_origin_section,
)
from backend.models.analytics import (
    OriginAggregatesRequest,
    OriginAggregatesResponse,
    OriginDrilldownRequest,
    OriginDrilldownResponse,
    OriginIpHealthRequest,
    OriginIpHealthResponse,
    OriginPathBreakdownResponse,
    OriginPopLatencyRequest,
    OriginPopLatencyResponse,
    OriginRequest,
    OriginShieldingAnalysisRequest,
    OriginShieldingAnalysisResponse,
    OriginSlowUrlsRequest,
    OriginSlowUrlsResponse,
    OriginStatusCodesResponse,
    OriginSummaryResponse,
    OriginTimeseriesRequest,
    OriginTimeseriesResponse,
)
from backend.models.analytics import (
    SectionName as OriginSectionName,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.repositories.utils.response_cache import cache_result
from backend.utils.router_utils import make_section_expander, query_errors

router = APIRouter(prefix="/api/origin", tags=["origin"], responses=DEFAULT_ERROR_RESPONSES)
origin_router = router

_TS_STATUS_PATH_TRIPLE: frozenset[str] = frozenset({"timeseries", "status_codes", "path_breakdown"})
_POP_IP_PAIR: frozenset[str] = frozenset({"pop_latency", "ip_health"})

_expand_origin_sections = make_section_expander(OriginSectionName, union_groups=(_TS_STATUS_PATH_TRIPLE, _POP_IP_PAIR))


def _require_high_scale(ctx: RequestContext) -> HighScaleService:
    if ctx.high_scale is None:
        raise HTTPException(
            status_code=503,
            detail={"error": "ClickHouse service is not available for this service"},
        )
    return ctx.high_scale


@router.post("/aggregates", response_model=OriginAggregatesResponse)
@query_errors()
async def origin_aggregates(
    req: OriginAggregatesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    """Composite of the seven origin cards."""
    start_time, end_time = ctx.resolve_window(req)
    sections = _expand_origin_sections(req.sections)
    high_scale_service = _require_high_scale(ctx)
    res = await asyncio.to_thread(
        high_scale_aggregates, high_scale_service, req, start_time, end_time, sections=sections
    )
    return OriginAggregatesResponse.with_telemetry(
        **(res if "_response_cache" in res else cache_result(res, state="bypass", prewarmer=False))
    )


@router.post("/summary", response_model=OriginSummaryResponse)
@query_errors()
def origin_summary(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return OriginSummaryResponse.with_telemetry(
        **hs_origin_section(high_scale_service, req, start_time, end_time, "summary")
    )


@router.post("/timeseries", response_model=OriginTimeseriesResponse)
@query_errors()
def origin_timeseries(
    req: OriginTimeseriesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return OriginTimeseriesResponse.with_telemetry(
        **hs_origin_section(high_scale_service, req, start_time, end_time, "timeseries")
    )


@router.post("/slow-urls", response_model=OriginSlowUrlsResponse)
@query_errors()
def origin_slow_urls(
    req: OriginSlowUrlsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return OriginSlowUrlsResponse.with_telemetry(
        **hs_origin_section(high_scale_service, req, start_time, end_time, "slow_urls")
    )


@router.post("/status-codes", response_model=OriginStatusCodesResponse)
@query_errors()
def origin_status_codes(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return OriginStatusCodesResponse.with_telemetry(
        **hs_origin_section(high_scale_service, req, start_time, end_time, "status_codes")
    )


@router.post("/path-breakdown", response_model=OriginPathBreakdownResponse)
@query_errors()
def origin_path_breakdown(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return OriginPathBreakdownResponse.with_telemetry(
        **hs_origin_section(high_scale_service, req, start_time, end_time, "path_breakdown")
    )


@router.post("/pop-latency", response_model=OriginPopLatencyResponse)
@query_errors()
def origin_pop_latency(
    req: OriginPopLatencyRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return OriginPopLatencyResponse.with_telemetry(
        **hs_origin_section(high_scale_service, req, start_time, end_time, "pop_latency")
    )


@router.post("/ip-health", response_model=OriginIpHealthResponse)
@query_errors()
def origin_ip_health(
    req: OriginIpHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return OriginIpHealthResponse.with_telemetry(
        **hs_origin_section(high_scale_service, req, start_time, end_time, "ip_health")
    )


@router.post("/shielding-analysis", response_model=OriginShieldingAnalysisResponse)
@query_errors()
def origin_shielding_analysis(
    req: OriginShieldingAnalysisRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return hs_shielding_analysis(high_scale_service, req, start_time, end_time)


@router.post("/drilldown", response_model=OriginDrilldownResponse)
@query_errors()
def origin_drilldown(
    req: OriginDrilldownRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return OriginDrilldownResponse.with_telemetry(**hs_origin_drilldown(high_scale_service, req, start_time, end_time))
