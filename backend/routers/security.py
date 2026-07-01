"""Security router — TLS analysis, bot detection, and request anomalies."""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Depends, Response

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import FilteredRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.security import SecurityAggregatesResponse, SecurityTopBotsResponse
from backend.repositories import security as repo
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.time_window import is_valid_range_token, resolve_window

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

# Fingerprint cards share a single full-temp coverage scan
# (_build_security_response's FINGERPRINT_COVERAGE_BULK) — any one of them
# forces fingerprint_coverage to compute, otherwise the FE renders the card
# without its "low coverage" hint. Same idea inverted: coverage alone makes
# no sense without at least one card, but we auto-include the cards instead
# of erroring so the FE doesn't have to know the coupling. This is the
# ASYMMETRIC `implies` coupling: the trigger set adds a *different* member
# (fingerprint_coverage), not a symmetric union of itself.
_FINGERPRINT_CARDS: frozenset[str] = frozenset({"tls_fingerprints"})

# NGWAF cache ATTACH costs ~22 ms per connection. Both the table view and the
# time-series come from queries that share that ATTACH, so we run them as a
# pair — requesting just the table without the chart would still pay the
# ATTACH, and vice versa.
_NGWAF_BOT_PAIR: frozenset[str] = frozenset({"ngwaf_verified_bots", "ngwaf_verified_bots_ts"})

_expand_sections = make_section_expander(
    SectionName,
    union_groups=(_NGWAF_BOT_PAIR,),
    implies=((_FINGERPRINT_CARDS, "fingerprint_coverage"),),
)


class SecurityAggregatesRequest(FilteredRequest):
    bucket_seconds: int = 300
    sections: list[SectionName] | None = None
    # Relative-range wire contract (additive, optional) — see origin.py /
    # backend/utils/time_window.py. When ``range_token`` is recognized the
    # SERVER resolves the scan window from (token, anchor) and ignores
    # FE-supplied start/end, then clamps to the invite ceiling (an analyst can't
    # widen past their invite by picking "30d"). This endpoint has no response
    # memo to stabilize — the token exists purely so the FE first-paint key is
    # server-reproducible (origin SSR-seed contract). Absent/unknown token →
    # legacy absolute-bounds path unchanged.
    range_token: str | None = None
    anchor: str | None = None


@router.post("/aggregates", response_model=SecurityAggregatesResponse)
@query_errors()
def security_aggregates(
    req: SecurityAggregatesRequest,
    response: Response,
    ctx: RequestContext = Depends(build_request_context),
):
    # Keyed path: resolve the scan window server-side from (range_token, anchor),
    # ignoring FE-supplied absolute bounds; clamp AFTER resolve so the invite
    # ceiling is enforced regardless of token. Mirrors routers/origin.py.
    if is_valid_range_token(req.range_token):
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        start_time, end_time = ctx.clamp(resolved_start, resolved_end)
    else:
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
