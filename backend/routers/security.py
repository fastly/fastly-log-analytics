from __future__ import annotations

import csv
import io
from datetime import UTC, datetime, timedelta
from typing import Literal

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.common import BaseResponse, FilteredRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.security import (
    SecurityAggregatesResponse,
    SecurityProxiesRequest,
    SecurityProxiesResponse,
    SecurityTopBotsResponse,
)
from backend.repositories import security as repo
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.telemetry import track_query
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


@router.post("/proxies", response_model=SecurityProxiesResponse)
@query_errors()
def get_proxies_data(
    req: SecurityProxiesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    if is_valid_range_token(req.range_token):
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        start_time, end_time = ctx.clamp(resolved_start, resolved_end)
    else:
        start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = repo.get_security_proxies(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return SecurityProxiesResponse.with_telemetry(**res)


@router.get("/proxies/export")
@query_errors()
def export_proxies_csv(
    start_time: str,
    end_time: str,
    threshold: str = "High",
    format: str = "fastly-acl",
    ctx: RequestContext = Depends(build_request_context),
):
    # Enforce correct date-bounds tenancy
    resolved_start, resolved_end = ctx.clamp(start_time, end_time)

    # Query repository
    res = repo.get_security_proxies(
        con=ctx.con,
        src=ctx.source,
        start_time=resolved_start,
        end_time=resolved_end,
        filters=None,
    )

    # Filter clients by risk threshold
    clients = res.get("active_clients", [])
    if threshold == "High":
        filtered_clients = [c for c in clients if c.get("risk_level") == "High"]
    else:
        filtered_clients = [c for c in clients if c.get("risk_level") in ("High", "Medium")]

    # De-duplicate clients by IP to ensure only unique IPs are exported
    seen_ips = set()
    unique_clients = []
    for c in filtered_clients:
        ip = c.get("ip")
        if ip and ip not in seen_ips:
            seen_ips.add(ip)
            unique_clients.append(c)
    filtered_clients = unique_clients

    # Create CSV memory buffer
    output = io.StringIO()
    writer = csv.writer(output)

    if format == "fastly-acl":
        writer.writerow(["ip", "comment"])
        for client in filtered_clients:
            reason = (
                "Impossible distance mismatch" if client.get("impossible_distance") else "Behavioral VPN/Proxy tunnel"
            )
            writer.writerow([client["ip"], f"Watchdog block: {reason}"])
    else:
        for client in filtered_clients:
            output.write(f"{client['ip']}\n")

    output.seek(0)
    filename = (
        f"watchdog_blocklist_{threshold.lower()}_{format}.csv"
        if format == "fastly-acl"
        else f"blocklist_{threshold.lower()}.txt"
    )
    media_type = "text/csv" if format == "fastly-acl" else "text/plain"

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


class ThreatIntelItem(BaseModel):
    tls_fingerprint: str  # ja3 or ja4
    requests: int
    top_country: str
    matched_waf_rules_count: int
    bot_percentage: float
    is_anonymized_proxy: bool


class ThreatIntelListResponse(BaseResponse):
    data: list[ThreatIntelItem]


@router.get("/threat-intel", response_model=ThreatIntelListResponse)
@query_errors()
def get_security_threat_intel(
    ctx: RequestContext = Depends(build_request_context),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
):
    """Correlate client TLS fingerprints with active WAF triggers and proxy flags."""
    resolved_start, resolved_end = ctx.clamp(start_time, end_time)

    if not resolved_start:
        resolved_start = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
    if not resolved_end:
        resolved_end = datetime.now(UTC).isoformat()

    from backend.repositories._base import _safe_table

    table_name = _safe_table(ctx.source["name"])
    time_filter = "WHERE timestamp >= CAST(? AS TIMESTAMPTZ) AND timestamp <= CAST(? AS TIMESTAMPTZ)"
    params = [resolved_start, resolved_end]

    probe = ctx.con.execute(f"SELECT * FROM {table_name} LIMIT 0").description or []
    cols = {d[0] for d in probe}

    if "ja3" in cols:
        fingerprint_col = "ja3"
    elif "ja4" in cols:
        fingerprint_col = "ja4"
    else:
        fingerprint_col = "CAST('Unknown' AS VARCHAR)"

    proxy_col = "p_type" if "p_type" in cols else "CAST(NULL AS VARCHAR)"
    waf_conditions = []
    if "waf_logged_rules" in cols:
        waf_conditions.append("waf_logged_rules IS NOT NULL")
    if "waf_blocked_rules" in cols:
        waf_conditions.append("waf_blocked_rules IS NOT NULL")
    waf_col = f"COUNT(*) FILTER (WHERE {' OR '.join(waf_conditions)})" if waf_conditions else "CAST(0 AS BIGINT)"
    bot_filter = "is_bot = true" if "is_bot" in cols else "FALSE"
    country_col = "country" if "country" in cols else "CAST('Unknown' AS VARCHAR)"

    query = f"""
        SELECT
            {fingerprint_col} AS tls_fingerprint,
            COUNT(*) AS requests,
            mode({country_col}) AS top_country,
            {waf_col} AS matched_waf_rules_count,
            ROUND(COUNT(*) FILTER (WHERE {bot_filter}) * 100.0 / COUNT(*), 2) AS bot_percentage,
            MAX(CASE WHEN {proxy_col} IS NOT NULL AND {proxy_col} != '' THEN 1 ELSE 0 END) AS is_anonymized_proxy
        FROM {table_name}
        {time_filter} AND {fingerprint_col} IS NOT NULL AND {fingerprint_col} != ''
        GROUP BY tls_fingerprint
        ORDER BY requests DESC
        LIMIT 50
    """

    data = []
    with track_query(ctx.con, query, params, "threat_intel") as cursor:
        for row in cursor.fetchall():
            data.append(
                ThreatIntelItem(
                    tls_fingerprint=row[0],
                    requests=row[1],
                    top_country=row[2] or "Unknown",
                    matched_waf_rules_count=row[3],
                    bot_percentage=row[4] or 0.0,
                    is_anonymized_proxy=bool(row[5]),
                )
            )

    return ThreatIntelListResponse.with_telemetry(data=data)
