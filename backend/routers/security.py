"""Security router — WAF, bots, proxy/VPN detection, and threat intelligence endpoints."""

from __future__ import annotations

import csv
import io

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import StreamingResponse

from backend.core.request_context import RequestContext, build_request_context
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.security import (
    get_proxies_data as hs_proxies,
)
from backend.high_scale.security import (
    get_security_threat_intel as hs_threat_intel,
)
from backend.high_scale.security import (
    security_aggregates as hs_aggregates,
)
from backend.models.analytics import (
    SecurityAggregatesRequest,
    SecurityAggregatesResponse,
    SecuritySectionName,
    ThreatIntelListResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import (
    make_section_expander,
    query_errors,
)

router = APIRouter(prefix="/api/security", tags=["network", "security"], responses=DEFAULT_ERROR_RESPONSES)
security_router = router

_FINGERPRINT_CARDS: frozenset[str] = frozenset({"tls_fingerprints"})
_NGWAF_BOT_PAIR: frozenset[str] = frozenset({"ngwaf_verified_bots", "ngwaf_verified_bots_ts"})

_expand_security_sections = make_section_expander(
    SecuritySectionName,
    union_groups=(_NGWAF_BOT_PAIR,),
    implies=((_FINGERPRINT_CARDS, "fingerprint_coverage"),),
)


def _require_high_scale(ctx: RequestContext) -> HighScaleService:
    if ctx.high_scale is None:
        raise HTTPException(
            status_code=503,
            detail={"error": "ClickHouse service is not available for this service"},
        )
    return ctx.high_scale


@router.post("/aggregates", response_model=SecurityAggregatesResponse)
@router.post("/bundle", response_model=SecurityAggregatesResponse)
@query_errors()
def security_aggregates(
    req: SecurityAggregatesRequest,
    response: Response,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)
    sections = _expand_security_sections(req.sections)
    mask_ips = mask_ips_for(ctx.analyst_session)
    high_scale_service = _require_high_scale(ctx)
    response.headers["Cache-Control"] = "private, max-age=30, stale-while-revalidate=120"
    return hs_aggregates(high_scale_service, req, start_time, end_time, sections=sections, mask_ips=mask_ips)


@router.get("/bundle", response_model=SecurityAggregatesResponse)
@query_errors()
def security_bundle_get(
    response: Response,
    start_time: str | None = None,
    end_time: str | None = None,
    range_token: str | None = None,
    anchor: str | None = None,
    bucket_seconds: int = 300,
    ctx: RequestContext = Depends(build_request_context),
):
    req = SecurityAggregatesRequest(
        start_time=start_time,
        end_time=end_time,
        range_token=range_token,
        anchor=anchor,
        bucket_seconds=bucket_seconds,
    )
    return security_aggregates(req, response, ctx)


@router.get("/proxies/export")
@query_errors()
def export_proxies_csv(
    start_time: str,
    end_time: str,
    threshold: str = "High",
    format: str = "fastly-acl",
    ctx: RequestContext = Depends(build_request_context),
):
    resolved_start, resolved_end = ctx.clamp(start_time, end_time)
    mask_ips = mask_ips_for(ctx.analyst_session)
    high_scale_service = _require_high_scale(ctx)
    res_raw = hs_proxies(high_scale_service, None, resolved_start, resolved_end, mask_ips=mask_ips)
    res = res_raw.model_dump() if hasattr(res_raw, "model_dump") else res_raw

    clients = res.get("active_clients", [])
    if threshold == "High":
        filtered_clients = [c for c in clients if c.get("risk_level") == "High"]
    else:
        filtered_clients = [c for c in clients if c.get("risk_level") in ("High", "Medium")]

    seen_ips = set()
    unique_clients = []
    for c in filtered_clients:
        raw_ip = c.get("ip")
        if not raw_ip:
            continue
        ip = str(raw_ip)
        if ip not in seen_ips:
            seen_ips.add(ip)
            item = dict(c)
            item["ip"] = ip
            unique_clients.append(item)
    filtered_clients = unique_clients

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


@router.get("/threat-intel", response_model=ThreatIntelListResponse)
@query_errors()
def get_security_threat_intel(
    ctx: RequestContext = Depends(build_request_context),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
):
    """Correlate client TLS fingerprints with active WAF triggers and proxy flags."""
    resolved_start, resolved_end = ctx.clamp(start_time, end_time)
    high_scale_service = _require_high_scale(ctx)
    return hs_threat_intel(high_scale_service, resolved_start, resolved_end)
