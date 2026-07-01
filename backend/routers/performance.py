"""Performance router — latency analysis, origin vs edge breakdown."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import FilteredRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.performance import PerformanceAggregatesResponse
from backend.repositories import performance as repo
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.time_window import is_valid_range_token, resolve_window

router = APIRouter(prefix="/api/performance", tags=["performance"], responses=DEFAULT_ERROR_RESPONSES)
# Section selector — names mirror PerformanceAggregatesResponse fields one-to-one
# so the FE can request a subset and the per-card React Query hooks each pull
# only what they render.
SectionName = Literal[
    "waterfall",
    "top_urls",
    "top_asns",
    "ttl_dist",
    "scatter",
]

# top_urls + top_asns share a 2-pass CTE shape against the same per-request
# temp table (commit 8fc53e1). They run sequentially on the same scan and the
# FE always renders the two cards as a pair, so requesting one auto-includes
# the other — splitting them would force a second temp materialization for
# no FE benefit.
_TOP_N_PAIR: frozenset[str] = frozenset({"top_urls", "top_asns"})

# waterfall + scatter come from a single MATERIALIZED CTE (one spool, two
# consumers) — see scatter_waterfall_query. Requesting either auto-includes
# the other so the spool's cost is amortized across both rather than half-paid.
_WATERFALL_SCATTER_PAIR: frozenset[str] = frozenset({"waterfall", "scatter"})

_expand_sections = make_section_expander(SectionName, union_groups=(_TOP_N_PAIR, _WATERFALL_SCATTER_PAIR))


class PerformanceRequest(FilteredRequest):
    sort_by: Literal["avg", "p50", "p95", "p99"] = "p99"
    sections: list[SectionName] | None = None
    # Relative-range wire contract (additive, optional) — see origin.py /
    # backend/utils/time_window.py. When ``range_token`` is recognized the
    # SERVER resolves the scan window from (token, anchor) and ignores
    # FE-supplied start/end, then clamps to the invite ceiling. No response memo
    # here — the token exists purely so the FE first-paint key is
    # server-reproducible (origin SSR-seed contract). Absent/unknown token →
    # legacy absolute-bounds path unchanged.
    range_token: str | None = None
    anchor: str | None = None


@router.post("/aggregates", response_model=PerformanceAggregatesResponse)
@query_errors()
def performance_aggregates(
    req: PerformanceRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    # Keyed path: resolve window server-side from (range_token, anchor), ignore
    # FE-supplied bounds, clamp AFTER resolve. Mirrors routers/origin.py.
    if is_valid_range_token(req.range_token):
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        start_time, end_time = ctx.clamp(resolved_start, resolved_end)
    else:
        start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    sections = _expand_sections(req.sections)
    res = repo.get_performance_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        sort_by=req.sort_by,
        sections=sections,
    )
    return PerformanceAggregatesResponse.with_telemetry(**res)
