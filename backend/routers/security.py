"""Security router — TLS analysis, bot detection, and request anomalies."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Response

from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import FilteredRequest
from backend.models.security import SecurityAggregatesResponse, SecurityTopBotsResponse
from backend.repositories import security as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/security", tags=["security"])


class SecurityAggregatesRequest(FilteredRequest):
    bucket_seconds: int = 300


@router.post("/aggregates", response_model=SecurityAggregatesResponse)
@query_errors()
def security_aggregates(
    req: SecurityAggregatesRequest,
    response: Response,
    ctx: RequestContext = Depends(build_request_context),
):
    res = repo.get_security_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        bucket_seconds=req.bucket_seconds,
    )
    # 30-s edge cache + 120-s stale-while-revalidate. Aggregates are
    # hourly-bucketed at minimum, so 30 s staleness is well inside
    # what the UI already expects from the React Query layer. Range-
    # tweak round-trips collapse from 3-14 s to near-zero.
    response.headers["Cache-Control"] = "private, max-age=30, stale-while-revalidate=120"
    return SecurityAggregatesResponse.with_telemetry(**res)


@router.post("/top-bots", response_model=SecurityTopBotsResponse)
@query_errors()
def top_bots(req: FilteredRequest, ctx: RequestContext = Depends(build_request_context)):
    res = repo.get_top_bots(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    return SecurityTopBotsResponse.with_telemetry(**res)
