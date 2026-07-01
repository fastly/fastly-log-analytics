"""Origin metrics router — fetch timing, error rates, IP health."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import FilteredRequest, Limit100, Limit200, Limit1440
from backend.models.errors import DEFAULT_ERROR_RESPONSES
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
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.time_window import is_valid_range_token, resolve_window

router = APIRouter(prefix="/api/origin", tags=["origin"], responses=DEFAULT_ERROR_RESPONSES)
# Section selector — names mirror OriginAggregatesResponse fields one-to-one so
# the FE can request a subset. Coupling reflects the asyncio.gather partition
# inside get_aggregates so we never split a branch's reads across requests
# (each branch checks out one pool conn and runs its reads serially on it).
SectionName = Literal[
    "summary",
    "timeseries",
    "slow_urls",
    "status_codes",
    "path_breakdown",
    "pop_latency",
    "ip_health",
]

# Branch 3 of get_aggregates' gather — timeseries + status_codes + path_breakdown
# all run sequentially on extra_runners[1]. Splitting them across requests would
# either need an extra pool conn or serialize work that already shares one — so
# requesting any one auto-includes the other two.
_TS_STATUS_PATH_TRIPLE: frozenset[str] = frozenset({"timeseries", "status_codes", "path_breakdown"})

# Branch 4 of get_aggregates' gather — pop_latency + ip_health share
# extra_runners[2]. Same reasoning as the triple above.
_POP_IP_PAIR: frozenset[str] = frozenset({"pop_latency", "ip_health"})

_expand_sections = make_section_expander(SectionName, union_groups=(_TS_STATUS_PATH_TRIPLE, _POP_IP_PAIR))


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
    sections: list[SectionName] | None = None
    # Relative-range wire contract (additive, optional). When ``range_token`` is
    # a recognized token, the SERVER resolves the scan window itself from
    # (token, anchor) and ignores FE-supplied ``start_time``/``end_time`` on this
    # path — so a crafted body can't widen the scan. The resolved bounds are
    # still passed through ``ctx.clamp`` so the invite ceiling is enforced
    # regardless of token (an analyst can't widen past their invite by picking
    # "30d"). Unlike network-health this endpoint has no response memo to
    # stabilize — the token exists purely so the FE first-paint React Query key
    # is server-reproducible (it keys on the token + a quantized anchor instead
    # of a client-now()-anchored absolute window), which is what makes the
    # origin page SSR-seedable. Absent/unknown token → legacy absolute-bounds
    # path unchanged. See backend/utils/time_window.py + the spec.
    range_token: str | None = None
    anchor: str | None = None


@router.post("/aggregates", response_model=OriginAggregatesResponse)
@query_errors()
async def origin_aggregates(
    req: OriginAggregatesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    """Composite of the seven origin cards (summary, timeseries, slow-urls,
    status-codes, path-breakdown, pop-latency, ip-health) backed by ONE
    catalog-table materialization that four pool connections read in
    parallel via ``asyncio.gather``. Shielding-analysis stays at
    /api/origin/shielding-analysis until item 13 folds it into
    /api/network-health.

    Granular endpoints below are unchanged so the frontend can roll back
    to the per-card pattern by flipping a feature flag without a backend
    redeploy.
    """
    # ── Relative-range keyed path ───────────────────────────────────────────
    # When the caller sends a recognized ``range_token``, the SERVER resolves
    # the scan window from (token, anchor) — we do NOT trust the FE-supplied
    # absolute start/end here, so a crafted body can't widen the scan. The
    # resolved bounds are STILL passed through ctx.clamp so the invite ceiling
    # is enforced regardless of token (an analyst can't widen past their invite
    # by picking "30d"). resolve_window only sizes the window; ctx.clamp is the
    # enforcement point and runs AFTER it (mirrors routers/network.py). Unlike
    # network-health, get_aggregates has no response memo to stabilize, so the
    # token is NOT threaded into the repo — it exists purely to make the FE's
    # first-paint key server-reproducible (the origin SSR-seed contract).
    if is_valid_range_token(req.range_token):
        # earliest_log_at drives the "auto" adaptive default; sourced from the
        # persisted status snapshot (same field the bootstrap + /api/log-extents
        # read — no DuckDB connection, no cron contention).
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        start_time, end_time = ctx.clamp(resolved_start, resolved_end)
    else:
        start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    sections = _expand_sections(req.sections)
    res = await repo.get_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_minutes=req.bucket_minutes,
        split_by_leg=req.split_by_leg,
        timeseries_metric=req.timeseries_metric,
        timeseries_percentile=req.timeseries_percentile,
        slow_urls_limit=req.slow_urls_limit,
        slow_urls_min_requests=req.slow_urls_min_requests,
        ip_health_limit=req.ip_health_limit,
        pop_latency_limit=req.pop_latency_limit,
        sections=sections,
    )
    return OriginAggregatesResponse.with_telemetry(**res)


@router.post("/summary", response_model=OriginSummaryResponse)
@query_errors()
def origin_summary(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_summary(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginSummaryResponse.with_telemetry(**res)


@router.post("/timeseries", response_model=OriginTimeseriesResponse)
@query_errors()
def origin_timeseries(
    req: OriginTimeseriesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_timeseries(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_minutes=req.bucket_minutes,
        split_by_leg=req.split_by_leg,
        metric=req.metric,
        percentile=req.percentile,
    )
    return OriginTimeseriesResponse.with_telemetry(**res)


@router.post("/slow-urls", response_model=OriginSlowUrlsResponse)
@query_errors()
def origin_slow_urls(
    req: OriginSlowUrlsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_slow_urls(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
        min_requests=req.min_requests,
    )
    return OriginSlowUrlsResponse.with_telemetry(**res)


@router.post("/status-codes", response_model=OriginStatusCodesResponse)
@query_errors()
def origin_status_codes(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_status_codes(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginStatusCodesResponse.with_telemetry(**res)


@router.post("/path-breakdown", response_model=OriginPathBreakdownResponse)
@query_errors()
def origin_path_breakdown(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_path_breakdown(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginPathBreakdownResponse.with_telemetry(**res)


@router.post("/pop-latency", response_model=OriginPopLatencyResponse)
@query_errors()
def origin_pop_latency(
    req: OriginPopLatencyRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_pop_latency(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginPopLatencyResponse.with_telemetry(**res)


@router.post("/ip-health", response_model=OriginIpHealthResponse)
@query_errors()
def origin_ip_health(
    req: OriginIpHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_ip_health(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginIpHealthResponse.with_telemetry(**res)


@router.post("/shielding-analysis", response_model=OriginShieldingAnalysisResponse)
@query_errors()
def origin_shielding_analysis(
    req: OriginShieldingAnalysisRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_shielding_analysis(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginShieldingAnalysisResponse.with_telemetry(**res)
