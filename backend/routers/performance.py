"""Performance router — latency analysis, origin vs edge breakdown."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends

from backend.deps import AnalyticsDeps
from backend.models.common import FilteredRequest
from backend.models.performance import (
    PerformanceAggregatesResponse,
    PerformanceOriginTsResponse,
)
from backend.repositories import performance as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/performance", tags=["performance"])


class PerformanceRequest(FilteredRequest):
    sort_by: Literal["avg", "p50", "p95", "p99"] = "p99"


class OriginTsRequest(FilteredRequest):
    chart_interval: Literal["1 second", "1 minute", "1 hour", "1 day"] = "1 minute"
    origin_metric: Literal["ttfb", "ttlb"] = "ttfb"
    origin_percentile: Literal["p50", "p95", "p99"] = "p95"


@router.post("/aggregates", response_model=PerformanceAggregatesResponse)
@query_errors()
def performance_aggregates(req: PerformanceRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_performance_aggregates(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        sort_by=req.sort_by,
    )
    return PerformanceAggregatesResponse.with_telemetry(**res)


@router.post("/origin-ts", response_model=PerformanceOriginTsResponse)
@query_errors()
def performance_origin_ts(req: OriginTsRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_origin_ts(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        chart_interval=req.chart_interval,
        origin_metric=req.origin_metric,
        origin_percentile=req.origin_percentile,
    )
    return PerformanceOriginTsResponse.with_telemetry(**res)
