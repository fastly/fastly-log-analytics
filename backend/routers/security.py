"""Security router — TLS analysis, bot detection, and request anomalies."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from backend.deps import AnalyticsDeps
from backend.models.common import FilteredRequest
from backend.models.security import SecurityAggregatesResponse, SecurityTopBotsResponse
from backend.repositories import security as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/security", tags=["security"])


class SecurityAggregatesRequest(FilteredRequest):
    bucket_seconds: int = 300


@router.post("/aggregates", response_model=SecurityAggregatesResponse)
@query_errors()
def security_aggregates(req: SecurityAggregatesRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_security_aggregates(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        bucket_seconds=req.bucket_seconds,
    )
    return SecurityAggregatesResponse.with_telemetry(**res)


@router.post("/top-bots", response_model=SecurityTopBotsResponse)
@query_errors()
def top_bots(req: FilteredRequest, deps: AnalyticsDeps = Depends()):
    res = repo.get_top_bots(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    return SecurityTopBotsResponse.with_telemetry(**res)
