"""Insights router — anomaly detection and cache-collapse detail endpoints."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException

from backend.core.request_context import RequestContext, build_request_context
from backend.cron.jobs.insights_prewarmer import record_insights_request
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import (
    cache_collapse_detail_endpoint as hs_detail,
)
from backend.high_scale.insights import (
    insights_endpoint as hs_insights,
)
from backend.models.analytics import (
    CacheCollapseDetailRequest,
    CacheCollapseDetailResponse,
    InsightsRequest,
    InsightsResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.utils.auth import mask_ips_for
from backend.utils.remote_access import analyst_clamp_cache_key
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api", tags=["insights"], responses=DEFAULT_ERROR_RESPONSES)
insights_router = router

# Per-service semaphore (capacity=1) serialises concurrent insights scans.
_insights_sems: dict[str, asyncio.Semaphore] = {}
_insights_sems_lock = threading.Lock()


def _require_high_scale(ctx: RequestContext) -> HighScaleService:
    if ctx.high_scale is None:
        raise HTTPException(
            status_code=503,
            detail={"error": "ClickHouse service is not available for this service"},
        )
    return ctx.high_scale


def _get_insights_sem(
    service_id: str,
    window_hours: float | None = None,
    baseline_hours: float | None = None,
) -> asyncio.Semaphore:
    sem_key = (
        service_id
        if window_hours is None and baseline_hours is None
        else f"{service_id}:{float(window_hours or 0):.2f}:{float(baseline_hours or 0):.2f}"
    )
    with _insights_sems_lock:
        if sem_key not in _insights_sems:
            _insights_sems[sem_key] = asyncio.Semaphore(1)
        return _insights_sems[sem_key]


def _analyst_lookback_clamp(
    ctx: RequestContext, baseline_hours: float, window_size_hrs: float
) -> tuple[str | None, str | None, bool, str | None]:
    """Resolve the analyst clamp window + mask_ips + stable cache key for the
    insights endpoints. Admin (no analyst session) → ``(None, None, False,
    None)`` so the scan runs the full range and hits the shared prewarmer cache.
    """
    s = ctx.analyst_session
    if s is None:
        return None, None, False, None
    now = datetime.now(UTC)
    earliest = now - timedelta(hours=baseline_hours + window_size_hrs)
    clamp_start, clamp_end = ctx.clamp(earliest.isoformat(), now.isoformat())
    cache_key = analyst_clamp_cache_key(
        getattr(s, "query_start_time", None),
        getattr(s, "query_end_time", None),
        getattr(s, "query_window_hours", None),
    )
    return clamp_start, clamp_end, mask_ips_for(s), cache_key


@router.post("/insights", response_model=InsightsResponse)
@query_errors()
async def insights_endpoint(
    req: InsightsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    record_insights_request(ctx.service_id)
    clamp_start, clamp_end, mask_ips, clamp_cache_key = _analyst_lookback_clamp(
        ctx, req.baseline_hours, req.window_size_hrs
    )
    high_scale_service = _require_high_scale(ctx)
    force_refresh = req.force_refresh if ctx.analyst_session is None else False
    sem = _get_insights_sem(ctx.service_id, req.window_size_hrs, req.baseline_hours)
    async with sem:
        return await asyncio.to_thread(
            hs_insights,
            high_scale_service,
            req,
            clamp_start,
            clamp_end,
            mask_ips,
            clamp_cache_key=clamp_cache_key,
            force_refresh=force_refresh,
        )


@router.post("/insights/cache-collapse-detail", response_model=CacheCollapseDetailResponse)
@query_errors()
def cache_collapse_detail_endpoint(
    req: CacheCollapseDetailRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    record_insights_request(ctx.service_id)
    clamp_start, clamp_end, mask_ips, _ = _analyst_lookback_clamp(ctx, req.baseline_hours, req.window_size_hrs)
    high_scale_service = _require_high_scale(ctx)
    return hs_detail(high_scale_service, req, clamp_start, clamp_end, mask_ips)
