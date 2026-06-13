"""Insights router — anomaly detection."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.models.dashboard import InsightsRequest, InsightsResponse
from backend.repositories import insights as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["insights"])


@router.post("/insights", response_model=InsightsResponse)
@query_errors()
def insights_endpoint(req: InsightsRequest, ctx: RequestContext = Depends(build_request_context)):
    return repo.get_insights(
        con=ctx.con,
        src=ctx.source,
        window_hours=req.window_size_hrs,
        baseline_hours=req.baseline_hours,
    )
