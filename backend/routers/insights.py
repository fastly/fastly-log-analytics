"""Insights router — anomaly detection."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.models.dashboard import (
    CacheCollapseDetailRequest,
    CacheCollapseDetailResponse,
    InsightsRequest,
    InsightsResponse,
)
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import insights as repo
from backend.utils.remote_access import TimeBounds, clamp_or_400, get_analyst_time_bounds
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["insights"], responses=DEFAULT_ERROR_RESPONSES)


@router.post("/insights", response_model=InsightsResponse)
@query_errors()
def insights_endpoint(
    req: InsightsRequest,
    ctx: RequestContext = Depends(build_request_context),
    tb: TimeBounds = Depends(get_analyst_time_bounds),
):
    # M2: clamp the scanned range [now-(baseline+window), now] to the analyst's
    # allowed window. Admin (no analyst session) passes None/None → full range
    # and the shared prewarmer cache. The model already bounds the two windows
    # so the unclamped lookback is itself capped (≤ ~97d).
    clamp_start: str | None = None
    clamp_end: str | None = None
    mask_ips = False
    if ctx.analyst_session is not None:
        now = datetime.now(UTC)
        earliest = now - timedelta(hours=req.baseline_hours + req.window_size_hrs)
        clamp_start, clamp_end = clamp_or_400(
            tb, earliest.isoformat(), now.isoformat(), analyst_session=ctx.analyst_session
        )
        # M3: IP-keyed insights mask the client IP they surface in the label /
        # investigate_url when the invite carries mask_ips.
        policy = getattr(ctx.analyst_session, "pii_policy", None)
        mask_ips = bool(policy.get("mask_ips")) if isinstance(policy, dict) else False
    return repo.get_insights(
        con=ctx.con,
        src=ctx.source,
        window_hours=req.window_size_hrs,
        baseline_hours=req.baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
    )


@router.post("/insights/cache-collapse-detail", response_model=CacheCollapseDetailResponse)
@query_errors()
def cache_collapse_detail_endpoint(
    req: CacheCollapseDetailRequest,
    ctx: RequestContext = Depends(build_request_context),
    tb: TimeBounds = Depends(get_analyst_time_bounds),
):
    clamp_start: str | None = None
    clamp_end: str | None = None
    mask_ips = False
    if ctx.analyst_session is not None:
        now = datetime.now(UTC)
        earliest = now - timedelta(hours=req.baseline_hours + req.window_size_hrs)
        clamp_start, clamp_end = clamp_or_400(
            tb, earliest.isoformat(), now.isoformat(), analyst_session=ctx.analyst_session
        )
        policy = getattr(ctx.analyst_session, "pii_policy", None)
        mask_ips = bool(policy.get("mask_ips")) if isinstance(policy, dict) else False
    return repo.get_cache_collapse_detail(
        con=ctx.con,
        src=ctx.source,
        url=req.url,
        window_hours=req.window_size_hrs,
        baseline_hours=req.baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
    )
