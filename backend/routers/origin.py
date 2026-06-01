"""Origin metrics router — fetch timing, error rates, IP health."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends

from backend.deps import AnalyticsDeps
from backend.models.common import FilteredRequest, Limit100, Limit200, Limit1440
from backend.models.origin import (
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


@router.post("/summary", response_model=OriginSummaryResponse)
@query_errors()
def origin_summary(req: OriginRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_summary(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    return OriginSummaryResponse.with_telemetry(**res)


@router.post("/timeseries", response_model=OriginTimeseriesResponse)
@query_errors()
def origin_timeseries(req: OriginTimeseriesRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_timeseries(
        con=deps.con,
        src=deps.source,
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
def origin_slow_urls(req: OriginSlowUrlsRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_slow_urls(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=req.limit,
        min_requests=req.min_requests,
    )
    return OriginSlowUrlsResponse.with_telemetry(**res)


@router.post("/status-codes", response_model=OriginStatusCodesResponse)
@query_errors()
def origin_status_codes(req: OriginRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_status_codes(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    return OriginStatusCodesResponse.with_telemetry(**res)


@router.post("/path-breakdown", response_model=OriginPathBreakdownResponse)
@query_errors()
def origin_path_breakdown(req: OriginRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_path_breakdown(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    return OriginPathBreakdownResponse.with_telemetry(**res)


@router.post("/pop-latency", response_model=OriginPopLatencyResponse)
@query_errors()
def origin_pop_latency(req: OriginPopLatencyRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_pop_latency(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginPopLatencyResponse.with_telemetry(**res)


@router.post("/ip-health", response_model=OriginIpHealthResponse)
@query_errors()
def origin_ip_health(req: OriginIpHealthRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_ip_health(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginIpHealthResponse.with_telemetry(**res)


@router.post("/shielding-analysis", response_model=OriginShieldingAnalysisResponse)
@query_errors()
def origin_shielding_analysis(req: OriginShieldingAnalysisRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_shielding_analysis(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginShieldingAnalysisResponse.with_telemetry(**res)
