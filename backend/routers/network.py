"""Network router — health heatmap and quality metrics."""

from __future__ import annotations

from typing import Literal, get_args

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import FilteredRequest, Limit100, Seconds14400
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.network import NetworkHealthResponse, NetworkQualityResponse
from backend.repositories import network as repo
from backend.utils.router_utils import expand_sections, query_errors

router = APIRouter(prefix="/api", tags=["network"], responses=DEFAULT_ERROR_RESPONSES)
# Section selector — names mirror NetworkHealthResponse fields one-to-one so
# the FE can request a subset and the per-card React Query hooks each pull
# only what they render. Two coupling groups:
#   - CORE_SECTIONS share the per-request network temp table
#   - SHIELDING_SECTIONS hit the origin code path (different temp). Splitting
#     this out is the slice 2 audit win — shielding is the slowest/most-likely
#     -empty card and today it blocks heatmap render because everything ships
#     in one response.
SectionName = Literal[
    "summary",
    "heatmap",
    "buckets",
    "leaderboard",
    "metro_leaderboard",
    "cities",
    "map_buckets",
    "shielding_analysis",
]

_ALL_SECTIONS: frozenset[str] = frozenset(get_args(SectionName))

CORE_SECTIONS: frozenset[str] = frozenset(
    {"summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"}
)
SHIELDING_SECTIONS: frozenset[str] = frozenset({"shielding_analysis"})


def _expand_sections(sections: list[SectionName] | None) -> set[str] | None:
    """Validate selector. None → no selector (full response)."""
    return expand_sections(sections, _ALL_SECTIONS)


class NetworkHealthRequest(FilteredRequest):
    metric: str = "health_score"
    bucket_seconds: Seconds14400 = 300
    top_n: Limit100 = 30
    map_asn: str = "all"
    sections: list[SectionName] | None = None


class NetworkQualityRequest(FilteredRequest):
    region_country: str = "US"


@router.post("/network-health", response_model=NetworkHealthResponse)
@query_errors()
def network_health(
    req: NetworkHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    sections = _expand_sections(req.sections)

    # When the caller requests ONLY shielding, skip the network temp
    # materialization entirely — get_health's ~13s 30d temp create is wasted
    # work if no core section is wanted.
    want_core = sections is None or bool(sections & CORE_SECTIONS)
    want_shielding = sections is None or bool(sections & SHIELDING_SECTIONS)

    if want_core:
        res = repo.get_health(
            con=ctx.con,
            src=ctx.source,
            start_time=start_time,
            end_time=end_time,
            filters=req.filters,
            metric=req.metric,
            bucket_seconds=req.bucket_seconds,
            top_n=req.top_n,
            map_asn=req.map_asn,
            sections=sections,
        )
    else:
        res = {"available": True}

    if want_shielding:
        try:
            from backend.repositories import origin as _origin

            shielding = _origin.get_shielding_analysis(
                con=ctx.con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
            )
            shielding = {k: v for k, v in shielding.items() if not k.startswith("debug_")}
            res["shielding_analysis"] = shielding
        except Exception:
            res["shielding_analysis"] = None
    return NetworkHealthResponse.with_telemetry(**res)


@router.post("/network-quality", response_model=NetworkQualityResponse)
@query_errors()
def network_quality(
    req: NetworkQualityRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_quality(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        region_country=req.region_country,
    )
    return NetworkQualityResponse.with_telemetry(**res)
