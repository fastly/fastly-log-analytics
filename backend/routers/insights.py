"""Insights router — anomaly detection and cache-collapse detail endpoints."""

from __future__ import annotations

import asyncio
import threading
from collections import OrderedDict
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.cron.jobs.insights_prewarmer import record_insights_request
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
from backend.utils.date_utils import parse_iso_utc
from backend.utils.remote_access import analyst_clamp_cache_key
from backend.utils.router_utils import query_errors, require_high_scale

router = APIRouter(prefix="/api", tags=["insights"], responses=DEFAULT_ERROR_RESPONSES)
insights_router = router

# Per-service semaphore (capacity=1) serialises concurrent insights scans.
_INSIGHTS_SEMS_MAXSIZE = 256
_insights_sems: OrderedDict[str, asyncio.Semaphore] = OrderedDict()
_insights_sems_lock = threading.Lock()


def _get_insights_sem(
    service_id: str,
    window_hours: float | None = None,
    baseline_hours: float | None = None,
) -> asyncio.Semaphore:
    del window_hours, baseline_hours
    with _insights_sems_lock:
        existing = _insights_sems.get(service_id)
        if existing is not None:
            _insights_sems.move_to_end(service_id)
            return existing
        if len(_insights_sems) >= _INSIGHTS_SEMS_MAXSIZE:
            evict_key = next((k for k, sem in _insights_sems.items() if not sem.locked()), None)
            if evict_key is not None:
                del _insights_sems[evict_key]
            else:
                _insights_sems.popitem(last=False)
        sem = asyncio.Semaphore(1)
        _insights_sems[service_id] = sem
        return sem


def _analyst_lookback_clamp(
    ctx: RequestContext,
    baseline_hours: float,
    window_size_hrs: float,
    requested_end: str | None = None,
) -> tuple[str | None, str | None, bool, str | None]:
    """Resolve the analyst clamp window + mask_ips + stable cache key for the
    insights endpoints. Admin (no analyst session) → ``(None, requested_end, False,
    None)`` so default scans hit the shared prewarmer cache while explicit
    ``end_time``/``clamp_end`` requests still anchor at the requested cutoff.
    """
    s = ctx.analyst_session
    if s is None:
        return None, requested_end, False, None
    now = datetime.now(UTC)
    if requested_end:
        parsed_end = parse_iso_utc(requested_end)
        if parsed_end is not None:
            parsed_utc = parsed_end.astimezone(UTC) if parsed_end.tzinfo is not None else parsed_end.replace(tzinfo=UTC)
            if parsed_utc < now:
                now = parsed_utc
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
    requested_end = getattr(req, "clamp_end", None) or getattr(req, "end_time", None)
    clamp_start, clamp_end, mask_ips, clamp_cache_key = _analyst_lookback_clamp(
        ctx, req.baseline_hours, req.window_size_hrs, requested_end
    )
    high_scale_service = require_high_scale(ctx)
    force_refresh = req.force_refresh if ctx.analyst_session is None else False
    sem = _get_insights_sem(ctx.service_id)
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
    high_scale_service = require_high_scale(ctx)
    return hs_detail(high_scale_service, req, clamp_start, clamp_end, mask_ips)
