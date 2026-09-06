from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import BaseResponse, FilteredRequest, Limit100, Seconds14400
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.network import NetworkHealthResponse, NetworkQualityResponse
from backend.repositories import network as repo
from backend.repositories._base import _safe_table
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.telemetry import track_query
from backend.utils.time_window import (
    invite_clamp_fingerprint,
    is_valid_range_token,
    quantize_anchor,
    resolve_window,
)

logger = logging.getLogger(__name__)

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

CORE_SECTIONS: frozenset[str] = frozenset(
    {"summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"}
)
SHIELDING_SECTIONS: frozenset[str] = frozenset({"shielding_analysis"})

# No coupling — every network section is independent.
_expand_sections = make_section_expander(SectionName)


class NetworkHealthRequest(FilteredRequest):
    metric: str = "health_score"
    bucket_seconds: Seconds14400 = 300
    top_n: Limit100 = 30
    map_asn: str = "all"
    sections: list[SectionName] | None = None
    # Relative-range wire contract (additive, optional). When ``range_token`` is
    # a recognized token, the server RESOLVES the scan window itself from
    # (token, anchor) — it does NOT trust FE-supplied ``start_time``/``end_time``
    # on this path — and the (token, quantized_anchor, invite-clamp fingerprint)
    # triple stabilizes the response-memo key so an analyst loading across
    # rolling minutes gets a cache HIT instead of recomputing the 30d pipeline.
    # Absent / unknown token → the legacy anchor-faithful absolute-bounds path.
    # See backend/utils/time_window.py + the spec for the security rationale.
    range_token: str | None = None
    anchor: str | None = None


class NetworkQualityRequest(FilteredRequest):
    region_country: str = "US"


@router.post("/network-health", response_model=NetworkHealthResponse)
@query_errors()
def network_health(
    req: NetworkHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    # ── Relative-range keyed path ───────────────────────────────────────────
    # When the caller sends a recognized ``range_token``, the SERVER resolves
    # the scan window from (token, quantized anchor) — we do NOT trust the
    # FE-supplied absolute start/end here, so a crafted body can't poison a
    # token+anchor cache entry with an arbitrary window. The resolved bounds are
    # still passed through ctx.clamp so the invite ceiling is enforced
    # regardless of token (an analyst can't widen past their invite by picking
    # "30d"). The (token, quantized_anchor, invite_clamp_fingerprint) triple is
    # then handed to get_health to STABILIZE the response-memo key across rolling
    # minutes — the network 30d analyst-cliff fix. An absent/unknown token falls
    # through to the legacy anchor-faithful absolute-bounds path unchanged.
    keyed_range_token: str | None = None
    keyed_anchor: str | None = None
    invite_fp: str | None = None
    if is_valid_range_token(req.range_token):
        # earliest_log_at drives the "auto" adaptive default; sourced from the
        # service's persisted status snapshot (same field /api/log-extents and
        # the bootstrap payload read — no DuckDB connection, no cron contention).
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        start_time, end_time = ctx.clamp(resolved_start, resolved_end)
        keyed_range_token = req.range_token
        keyed_anchor = quantize_anchor(req.anchor)
        invite_fp = invite_clamp_fingerprint(ctx.analyst_session)
    else:
        start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    sections = _expand_sections(req.sections)
    # mask_ips partitions the get_health response cache (masked vs unmasked).
    # network-health emits no IP today so this is belt-and-braces; it keeps the
    # cache key uniform with insights and prevents a future IP-bearing field
    # from leaking an unmasked entry to a masking analyst.
    mask_ips = mask_ips_for(ctx.analyst_session)

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
            mask_ips=mask_ips,
            range_token=keyed_range_token,
            quantized_anchor=keyed_anchor,
            invite_clamp_fingerprint=invite_fp,
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
            # Do NOT swallow silently: a real failure here (e.g. a SQL
            # regression in get_shielding_analysis) used to be
            # indistinguishable from "no shield data" — the card grid just
            # vanished and nothing surfaced in monitoring. Log it with a
            # stack trace and return an explicit error sentinel so the
            # frontend renders an "analysis unavailable" state instead of a
            # misleading empty one. (shielding audit 2026-06-30 M2)
            logger.exception(
                "shielding_analysis failed for service=%s",
                ctx.source.get("name"),
            )
            res["shielding_analysis"] = {"has_data": False, "error": True, "rows": []}
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


class PopHealthItem(BaseModel):
    pop: str
    requests: int
    errors: int
    error_rate: float
    p50_rtt_us: float | None
    p95_ttfb_ms: float | None
    cache_hit_rate: float
    bandwidth_bytes: int


class PopHealthListResponse(BaseResponse):
    data: list[PopHealthItem]


@router.get("/network/pop-health", response_model=PopHealthListResponse)
def get_pop_health(
    ctx: RequestContext = Depends(build_request_context),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
):
    """Aggregate edge-routing and cache metrics across Fastly POP locations."""
    table_name = _safe_table(ctx.source["name"])
    time_filter = "WHERE timestamp >= ? AND timestamp <= ?"
    params = [start_time or (datetime.now(UTC) - timedelta(hours=24)), end_time or datetime.now(UTC)]

    query = f"""
        SELECT
            pop,
            COUNT(*) AS requests,
            COUNT(*) FILTER (WHERE status >= 400 OR status = 0) AS errors,
            ROUND(COUNT(*) FILTER (WHERE status >= 400 OR status = 0) * 100.0 / COUNT(*), 2) AS error_rate,
            approx_quantile(tcp_rtt, 0.5) AS p50_rtt_us,
            approx_quantile(ttfb, 0.95) AS p95_ttfb_ms,
            ROUND(COUNT(*) FILTER (WHERE cache IN ('HIT', 'HIT-STALE')) * 100.0 / COUNT(*), 2) AS cache_hit_rate,
            SUM(resp_bytes) AS bandwidth_bytes
        FROM {table_name}
        {time_filter} AND pop IS NOT NULL AND pop != ''
        GROUP BY pop
        ORDER BY requests DESC
    """

    data = []
    with track_query(ctx.con, query, params, "pop_health") as cursor:
        for row in cursor.fetchall():
            data.append(
                PopHealthItem(
                    pop=row[0],
                    requests=row[1],
                    errors=row[2],
                    error_rate=row[3],
                    p50_rtt_us=row[4],
                    p95_ttfb_ms=row[5],
                    cache_hit_rate=row[6],
                    bandwidth_bytes=row[7] or 0,
                )
            )

    return PopHealthListResponse.with_telemetry(data=data)
