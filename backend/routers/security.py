"""Security router — WAF, bots, proxy/VPN detection, and threat intelligence endpoints."""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, Query, Response
from fastapi.responses import StreamingResponse

from backend.core.request_context import RequestContext, build_request_context
from backend.core.storage import execute_with_stale_view_retry
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.dashboard import (
    SecurityAggregatesRequest,
    SecurityAggregatesResponse,
    SecuritySectionName,
    ThreatIntelItem,
    ThreatIntelListResponse,
)
from backend.repositories import network as network_repo
from backend.repositories import security as security_repo
from backend.repositories._base import _safe_table
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import (
    make_section_expander,
    query_errors,
    run_cached_repo_endpoint,
)

repo = security_repo

router = APIRouter(prefix="/api/security", tags=["network", "security"], responses=DEFAULT_ERROR_RESPONSES)
security_router = router

_FINGERPRINT_CARDS: frozenset[str] = frozenset({"tls_fingerprints"})
_NGWAF_BOT_PAIR: frozenset[str] = frozenset({"ngwaf_verified_bots", "ngwaf_verified_bots_ts"})

_expand_security_sections = make_section_expander(
    SecuritySectionName,
    union_groups=(_NGWAF_BOT_PAIR,),
    implies=((_FINGERPRINT_CARDS, "fingerprint_coverage"),),
)


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
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.security import security_aggregates as hs_aggregates

        return hs_aggregates(high_scale_service, req, start_time, end_time, sections=sections, mask_ips=mask_ips)

    def _set_cache_header() -> None:
        response.headers["Cache-Control"] = "private, max-age=30, stale-while-revalidate=120"

    get_agg_fn = getattr(
        network_repo,
        "get_security_aggregates",
        getattr(repo, "get_security_aggregates", security_repo.get_security_aggregates),
    )
    peek_agg_fn = getattr(
        network_repo,
        "peek_security_aggregates",
        getattr(repo, "peek_security_aggregates", security_repo.peek_security_aggregates),
    )

    return run_cached_repo_endpoint(
        ctx,
        repo_module="backend.repositories.security",
        get_fn=get_agg_fn,
        peek_fn=peek_agg_fn,
        kwargs={
            "start_time": start_time,
            "end_time": end_time,
            "filters": req.filters,
            "bucket_seconds": req.bucket_seconds,
            "sections": sections,
            "mask_ips": mask_ips,
        },
        wrap=lambda r: SecurityAggregatesResponse.with_telemetry(**r),
        on_response=_set_cache_header,
    )


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

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.security import get_proxies_data as hs_proxies

        res_raw = hs_proxies(high_scale_service, None, resolved_start, resolved_end, mask_ips=mask_ips)
        res = res_raw.model_dump() if hasattr(res_raw, "model_dump") else res_raw
    else:
        get_proxies_fn = getattr(
            network_repo,
            "get_security_proxies",
            getattr(repo, "get_security_proxies", security_repo.get_security_proxies),
        )
        res = get_proxies_fn(
            con=ctx.con,
            src=ctx.source,
            start_time=resolved_start,
            end_time=resolved_end,
            filters=None,
            mask_ips=mask_ips,
        )

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

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.security import get_security_threat_intel as hs_threat_intel

        return hs_threat_intel(high_scale_service, resolved_start, resolved_end)

    if not resolved_start:
        resolved_start = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
    if not resolved_end:
        resolved_end = datetime.now(UTC).isoformat()

    table_name = _safe_table(ctx.source["name"])
    get_ti_rows = getattr(
        network_repo,
        "get_threat_intel_rows",
        getattr(repo, "get_threat_intel_rows", security_repo.get_threat_intel_rows),
    )

    def _run_threat_intel(con) -> list[ThreatIntelItem]:
        return [
            ThreatIntelItem(
                tls_fingerprint=row[0],
                requests=row[1],
                top_country=row[2] or "Unknown",
                matched_waf_rules_count=row[3],
                bot_percentage=row[4] or 0.0,
                is_anonymized_proxy=bool(row[5]),
            )
            for row in get_ti_rows(con, ctx.source, table_name, resolved_start, resolved_end)
        ]

    data = execute_with_stale_view_retry(ctx.con, ctx.source, _run_threat_intel)
    return ThreatIntelListResponse.with_telemetry(data=data)
