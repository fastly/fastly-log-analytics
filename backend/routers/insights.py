"""Insights router — anomaly detection and cache-collapse detail endpoints."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.core.storage import execute_with_stale_view_retry
from backend.cron.jobs.insights_prewarmer import record_insights_request
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.analytics import (
    CacheCollapseDetailRequest,
    CacheCollapseDetailResponse,
    InsightsRequest,
    InsightsResponse,
)
from backend.repositories import insights as insights_repo
from backend.utils.auth import mask_ips_for
from backend.utils.remote_access import analyst_clamp_cache_key
from backend.utils.router_utils import query_errors

repo = insights_repo

router = APIRouter(prefix="/api", tags=["insights"], responses=DEFAULT_ERROR_RESPONSES)
insights_router = router

# Per-service semaphore (capacity=1) serialises concurrent insights scans.
# A second request for the same service parks here; when the first finishes
# it writes the cache, and the second gets a cache hit → no duplicate scan.
# Dict access is guarded by a lock so concurrent service-registrations are safe.
_insights_sems: dict[str, asyncio.Semaphore] = {}
_insights_sems_lock = threading.Lock()


def _get_insights_sem(service_id: str) -> asyncio.Semaphore:
    with _insights_sems_lock:
        if service_id not in _insights_sems:
            _insights_sems[service_id] = asyncio.Semaphore(1)
        return _insights_sems[service_id]


def _analyst_lookback_clamp(
    ctx: RequestContext, baseline_hours: float, window_size_hrs: float
) -> tuple[str | None, str | None, bool, str | None]:
    """Resolve the analyst clamp window + mask_ips + stable cache key for the
    insights endpoints. Admin (no analyst session) → ``(None, None, False,
    None)`` so the scan runs the full range and hits the shared prewarmer cache.

    M2: clamp the scanned range [now-(baseline+window), now] to the analyst's
    allowed window via ``ctx.clamp``. The model already bounds the two windows
    so the unclamped lookback is itself capped (≤ ~97d).
    M3: IP-keyed insights mask the client IP they surface in the label /
    investigate_url when the invite carries mask_ips.

    The 4th value is the STABLE insights cache key fragment
    (``analyst_clamp_cache_key`` — keyed on the invite's window params, NOT the
    rolling resolved bounds) so repeated analyst requests reuse one cache entry
    and the prewarmer can warm the exact same key. ``get_cache_collapse_detail``
    is uncached and discards it.
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
    high_scale_service = ctx.high_scale
    force_refresh = req.force_refresh if ctx.analyst_session is None else False
    sem = _get_insights_sem(ctx.service_id)
    if high_scale_service is not None:
        from backend.high_scale.insights import insights_endpoint as hs_insights

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

    cached = insights_repo.check_insights_cache(
        src=ctx.source,
        window_hours=req.window_size_hrs,
        baseline_hours=req.baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        force_refresh=force_refresh,
        filters=req.filters,
    )
    if cached is not None:
        return cached

    await ctx.prewarm_con()
    async with sem:

        def _run_insights(con):
            return insights_repo.get_insights(
                con=con,
                src=ctx.source,
                window_hours=req.window_size_hrs,
                baseline_hours=req.baseline_hours,
                service_id=ctx.service_id,
                clamp_start=clamp_start,
                clamp_end=clamp_end,
                mask_ips=mask_ips,
                clamp_cache_key=clamp_cache_key,
                force_refresh=force_refresh,
                filters=req.filters,
            )

        return await asyncio.to_thread(execute_with_stale_view_retry, ctx.con, ctx.source, _run_insights)


@router.post("/insights/cache-collapse-detail", response_model=CacheCollapseDetailResponse)
@query_errors()
def cache_collapse_detail_endpoint(
    req: CacheCollapseDetailRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    record_insights_request(ctx.service_id)
    clamp_start, clamp_end, mask_ips, _ = _analyst_lookback_clamp(ctx, req.baseline_hours, req.window_size_hrs)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.insights import cache_collapse_detail_endpoint as hs_detail

        return hs_detail(high_scale_service, req, clamp_start, clamp_end, mask_ips)

    def _run_detail(con):
        return insights_repo.get_cache_collapse_detail(
            con=con,
            src=ctx.source,
            url=req.url,
            window_hours=req.window_size_hrs,
            baseline_hours=req.baseline_hours,
            clamp_start=clamp_start,
            clamp_end=clamp_end,
            mask_ips=mask_ips,
        )

    return execute_with_stale_view_retry(ctx.con, ctx.source, _run_detail)
