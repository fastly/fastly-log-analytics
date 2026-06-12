"""Origin metrics router — fetch timing, error rates, IP health."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import FilteredRequest, Limit100, Limit200, Limit1440
from backend.models.origin import (
    OriginAggregatesResponse,
    OriginIpHealthResponse,
    OriginPathBreakdownResponse,
    OriginPopLatencyResponse,
    OriginShieldingAnalysisResponse,
    OriginSlowUrlsResponse,
    OriginStatusCodesResponse,
    OriginSummaryResponse,
    OriginTimeseriesResponse,
)
from backend.repositories import origin as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/origin", tags=["origin"])


class OriginRequest(FilteredRequest):
    pass


class OriginTimeseriesRequest(FilteredRequest):
    bucket_minutes: Limit1440 = 5
    split_by_leg: bool = False
    metric: Literal["ttfb", "ttlb"] = "ttfb"
    percentile: Literal["p50", "p95", "p99"] = "p95"


class OriginSlowUrlsRequest(FilteredRequest):
    limit: Limit100 = 20
    min_requests: int = 10


class OriginPopLatencyRequest(FilteredRequest):
    limit: Limit100 = 30


class OriginIpHealthRequest(FilteredRequest):
    limit: Limit100 = 30


class OriginShieldingAnalysisRequest(FilteredRequest):
    limit: Limit200 = 50


class OriginAggregatesRequest(FilteredRequest):
    bucket_minutes: Limit1440 = 5
    split_by_leg: bool = False
    timeseries_metric: Literal["ttfb", "ttlb"] = "ttfb"
    timeseries_percentile: Literal["p50", "p95", "p99"] = "p95"
    slow_urls_limit: Limit100 = 20
    slow_urls_min_requests: int = 10
    ip_health_limit: Limit100 = 30
    pop_latency_limit: Limit100 = 30


@router.post("/aggregates", response_model=OriginAggregatesResponse)
@query_errors()
def origin_aggregates(req: OriginAggregatesRequest, ctx: RequestContext = Depends(build_request_context)):
    """Composite of the six origin cards (summary, timeseries, slow-urls,
    status-codes, path-breakdown, pop-latency, ip-health) backed by ONE
    parquet scan. Shielding-analysis stays at /api/origin/shielding-analysis
    until item 13 folds it into /api/network-health.

    Granular endpoints below are unchanged so the frontend can roll back
    to the per-card pattern by flipping a feature flag without a backend
    redeploy.
    """
    res = repo.get_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        bucket_minutes=req.bucket_minutes,
        split_by_leg=req.split_by_leg,
        timeseries_metric=req.timeseries_metric,
        timeseries_percentile=req.timeseries_percentile,
        slow_urls_limit=req.slow_urls_limit,
        slow_urls_min_requests=req.slow_urls_min_requests,
        ip_health_limit=req.ip_health_limit,
        pop_latency_limit=req.pop_latency_limit,
    )
    return OriginAggregatesResponse.with_telemetry(**res)


@router.post("/summary", response_model=OriginSummaryResponse)
@query_errors()
def origin_summary(req: OriginRequest, ctx: RequestContext = Depends(build_request_context)):
    res = repo.get_summary(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    return OriginSummaryResponse.with_telemetry(**res)


@router.post("/timeseries", response_model=OriginTimeseriesResponse)
@query_errors()
def origin_timeseries(req: OriginTimeseriesRequest, ctx: RequestContext = Depends(build_request_context)):
    res = repo.get_timeseries(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        bucket_minutes=req.bucket_minutes,
        split_by_leg=req.split_by_leg,
        metric=req.metric,
        percentile=req.percentile,
    )
    return OriginTimeseriesResponse.with_telemetry(**res)


@router.post("/slow-urls", response_model=OriginSlowUrlsResponse)
@query_errors()
def origin_slow_urls(req: OriginSlowUrlsRequest, ctx: RequestContext = Depends(build_request_context)):
    res = repo.get_slow_urls(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=req.limit,
        min_requests=req.min_requests,
    )
    return OriginSlowUrlsResponse.with_telemetry(**res)


@router.post("/status-codes", response_model=OriginStatusCodesResponse)
@query_errors()
def origin_status_codes(req: OriginRequest, ctx: RequestContext = Depends(build_request_context)):
    res = repo.get_status_codes(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    return OriginStatusCodesResponse.with_telemetry(**res)


@router.post("/path-breakdown", response_model=OriginPathBreakdownResponse)
@query_errors()
def origin_path_breakdown(req: OriginRequest, ctx: RequestContext = Depends(build_request_context)):
    res = repo.get_path_breakdown(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    return OriginPathBreakdownResponse.with_telemetry(**res)


@router.post("/pop-latency", response_model=OriginPopLatencyResponse)
@query_errors()
def origin_pop_latency(req: OriginPopLatencyRequest, ctx: RequestContext = Depends(build_request_context)):
    res = repo.get_pop_latency(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginPopLatencyResponse.with_telemetry(**res)


@router.post("/ip-health", response_model=OriginIpHealthResponse)
@query_errors()
def origin_ip_health(req: OriginIpHealthRequest, ctx: RequestContext = Depends(build_request_context)):
    res = repo.get_ip_health(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginIpHealthResponse.with_telemetry(**res)


@router.post("/shielding-analysis", response_model=OriginShieldingAnalysisResponse)
@query_errors()
def origin_shielding_analysis(
    req: OriginShieldingAnalysisRequest, ctx: RequestContext = Depends(build_request_context)
):
    res = repo.get_shielding_analysis(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginShieldingAnalysisResponse.with_telemetry(**res)
