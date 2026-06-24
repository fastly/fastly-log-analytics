"""Performance router — latency analysis, origin vs edge breakdown."""

from __future__ import annotations

from typing import Literal, get_args

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import FilteredRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.performance import PerformanceAggregatesResponse
from backend.repositories import performance as repo
from backend.utils.router_utils import expand_sections, query_errors

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

_ALL_SECTIONS: frozenset[str] = frozenset(get_args(SectionName))

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


def _couple(expanded: set[str]) -> set[str]:
    if expanded & _TOP_N_PAIR:
        expanded |= _TOP_N_PAIR
    if expanded & _WATERFALL_SCATTER_PAIR:
        expanded |= _WATERFALL_SCATTER_PAIR
    return expanded


def _expand_sections(sections: list[SectionName] | None) -> set[str] | None:
    """Apply coupling rules + validate. None → no selector (full response)."""
    return expand_sections(sections, _ALL_SECTIONS, couple=_couple)


class PerformanceRequest(FilteredRequest):
    sort_by: Literal["avg", "p50", "p95", "p99"] = "p99"
    sections: list[SectionName] | None = None


@router.post("/aggregates", response_model=PerformanceAggregatesResponse)
@query_errors()
def performance_aggregates(
    req: PerformanceRequest,
    ctx: RequestContext = Depends(build_request_context),
):
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
