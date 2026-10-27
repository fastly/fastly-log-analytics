"""Origin router — origin latency, status codes, slow URLs, IP health, and drilldowns."""

from __future__ import annotations

import asyncio

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.models._base import DEFAULT_ERROR_RESPONSES
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
from backend.repositories import origin as origin_repo
from backend.repositories.utils.response_cache import cache_result
from backend.utils.router_utils import make_section_expander, query_errors

repo = origin_repo

router = APIRouter(prefix="/api/origin", tags=["origin"], responses=DEFAULT_ERROR_RESPONSES)
origin_router = router

_TS_STATUS_PATH_TRIPLE: frozenset[str] = frozenset({"timeseries", "status_codes", "path_breakdown"})
_POP_IP_PAIR: frozenset[str] = frozenset({"pop_latency", "ip_health"})

_expand_origin_sections = make_section_expander(OriginSectionName, union_groups=(_TS_STATUS_PATH_TRIPLE, _POP_IP_PAIR))


@router.post("/aggregates", response_model=OriginAggregatesResponse)
@query_errors()
async def origin_aggregates(
    req: OriginAggregatesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    """Composite of the seven origin cards."""
    start_time, end_time = ctx.resolve_window(req)
    sections = _expand_origin_sections(req.sections)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.origin import aggregates as high_scale_aggregates

        res = await asyncio.to_thread(
            high_scale_aggregates, high_scale_service, req, start_time, end_time, sections=sections
        )
        return OriginAggregatesResponse.with_telemetry(**cache_result(res, state="bypass", prewarmer=False))

    await ctx.prewarm_con()
    res = await origin_repo.get_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_minutes=req.bucket_minutes,
        split_by_leg=req.split_by_leg,
        timeseries_metric=req.timeseries_metric,
        timeseries_percentile=req.timeseries_percentile,
        slow_urls_limit=req.slow_urls_limit,
        slow_urls_min_requests=req.slow_urls_min_requests,
        ip_health_limit=req.ip_health_limit,
        pop_latency_limit=req.pop_latency_limit,
        sections=sections,
    )
    return OriginAggregatesResponse.with_telemetry(**cache_result(res, state="bypass", prewarmer=False))


@router.post("/summary", response_model=OriginSummaryResponse)
@query_errors()
def origin_summary(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_summary(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginSummaryResponse.with_telemetry(**res)


@router.post("/timeseries", response_model=OriginTimeseriesResponse)
@query_errors()
def origin_timeseries(
    req: OriginTimeseriesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_timeseries(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_minutes=req.bucket_minutes,
        split_by_leg=req.split_by_leg,
        metric=req.metric,
        percentile=req.percentile,
    )
    return OriginTimeseriesResponse.with_telemetry(**res)


@router.post("/slow-urls", response_model=OriginSlowUrlsResponse)
@query_errors()
def origin_slow_urls(
    req: OriginSlowUrlsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_slow_urls(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
        min_requests=req.min_requests,
    )
    return OriginSlowUrlsResponse.with_telemetry(**res)


@router.post("/status-codes", response_model=OriginStatusCodesResponse)
@query_errors()
def origin_status_codes(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_status_codes(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginStatusCodesResponse.with_telemetry(**res)


@router.post("/path-breakdown", response_model=OriginPathBreakdownResponse)
@query_errors()
def origin_path_breakdown(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_path_breakdown(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginPathBreakdownResponse.with_telemetry(**res)


@router.post("/pop-latency", response_model=OriginPopLatencyResponse)
@query_errors()
def origin_pop_latency(
    req: OriginPopLatencyRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_pop_latency(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginPopLatencyResponse.with_telemetry(**res)


@router.post("/ip-health", response_model=OriginIpHealthResponse)
@query_errors()
def origin_ip_health(
    req: OriginIpHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_ip_health(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginIpHealthResponse.with_telemetry(**res)


@router.post("/shielding-analysis", response_model=OriginShieldingAnalysisResponse)
@query_errors()
def origin_shielding_analysis(
    req: OriginShieldingAnalysisRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import origin_shielding_analysis as hs_shielding_analysis

        return hs_shielding_analysis(high_scale_service, req, start_time, end_time)
    res = origin_repo.get_shielding_analysis(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginShieldingAnalysisResponse.with_telemetry(**res)


@router.post("/drilldown", response_model=OriginDrilldownResponse)
@query_errors()
def origin_drilldown(
    req: OriginDrilldownRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_drilldown(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        dimension=req.dimension,
        limit=req.limit,
    )
    return OriginDrilldownResponse.with_telemetry(**res)
