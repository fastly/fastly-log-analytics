"""Security router — TLS analysis, bot detection, and request anomalies."""

from __future__ import annotations

from typing import Literal, get_args

from fastapi import APIRouter, Depends, Response

from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import FilteredRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.security import SecurityAggregatesResponse, SecurityTopBotsResponse
from backend.repositories import security as repo
from backend.utils.router_utils import expand_sections, query_errors

router = APIRouter(prefix="/api/security", tags=["security"], responses=DEFAULT_ERROR_RESPONSES)
# Section selector — names mirror SecurityAggregatesResponse fields one-to-one
# so the FE can request a subset and the per-card React Query hooks each pull
# only what they render. ngwaf_configured is a tiny bool derived from svc
# config (no SQL), bundled into the verified_bots_ts/ngwaf_* gate so any bot
# section request gets the badge.
SectionName = Literal[
    "verified_bots_ts",
    "ngwaf_verified_bots",
    "ngwaf_verified_bots_ts",
    "wellknown_bots",
    "tls_fingerprints",
    "fingerprint_coverage",
    "req_size_dist",
    "top_ips_header",
    "ipv6_adoption",
    "proxy_dist",
    "conn_reuse_dist",
]

_ALL_SECTIONS: frozenset[str] = frozenset(get_args(SectionName))

# Fingerprint cards share a single full-temp coverage scan
# (_build_security_response's FINGERPRINT_COVERAGE_BULK) — any one of them
# forces fingerprint_coverage to compute, otherwise the FE renders the card
# without its "low coverage" hint. Same idea inverted: coverage alone makes
# no sense without at least one card, but we auto-include the cards instead
# of erroring so the FE doesn't have to know the coupling.
_FINGERPRINT_CARDS: frozenset[str] = frozenset({"tls_fingerprints"})

# NGWAF cache ATTACH costs ~22 ms per connection. Both the table view and the
# time-series come from queries that share that ATTACH, so we run them as a
# pair — requesting just the table without the chart would still pay the
# ATTACH, and vice versa.
_NGWAF_BOT_PAIR: frozenset[str] = frozenset({"ngwaf_verified_bots", "ngwaf_verified_bots_ts"})


def _couple(expanded: set[str]) -> set[str]:
    # Asymmetric: any fingerprint card pulls in fingerprint_coverage (a
    # *different* member), it is NOT a symmetric union of the trigger set.
    if expanded & _FINGERPRINT_CARDS:
        expanded.add("fingerprint_coverage")
    if expanded & _NGWAF_BOT_PAIR:
        expanded |= _NGWAF_BOT_PAIR
    return expanded


def _expand_sections(sections: list[SectionName] | None) -> set[str] | None:
    """Apply coupling rules + validate. None → no selector (full response)."""
    return expand_sections(sections, _ALL_SECTIONS, couple=_couple)


class SecurityAggregatesRequest(FilteredRequest):
    bucket_seconds: int = 300
    sections: list[SectionName] | None = None


@router.post("/aggregates", response_model=SecurityAggregatesResponse)
@query_errors()
def security_aggregates(
    req: SecurityAggregatesRequest,
    response: Response,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    sections = _expand_sections(req.sections)
    res = repo.get_security_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_seconds=req.bucket_seconds,
        sections=sections,
    )
    # 30-s edge cache + 120-s stale-while-revalidate. Aggregates are
    # hourly-bucketed at minimum, so 30 s staleness is well inside
    # what the UI already expects from the React Query layer. Range-
    # tweak round-trips collapse from 3-14 s to near-zero.
    response.headers["Cache-Control"] = "private, max-age=30, stale-while-revalidate=120"
    return SecurityAggregatesResponse.with_telemetry(**res)


@router.post("/top-bots", response_model=SecurityTopBotsResponse)
@query_errors()
def top_bots(
    req: FilteredRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_top_bots(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return SecurityTopBotsResponse.with_telemetry(**res)
