"""Network router — network health, quality, POP health, and bundle endpoints."""

from __future__ import annotations

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.core.iceberg import execute_with_stale_view_retry
from backend.core.request_context import RequestContext, build_request_context
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.network import (
    NetworkBundleRequest,
    NetworkBundleResponse,
    NetworkHealthRequest,
    NetworkHealthResponse,
    NetworkQualityRequest,
    NetworkQualityResponse,
    NetworkSectionName,
    PopHealthItem,
    PopHealthListResponse,
    TransportMetrics,
)
from backend.repositories import network as network_repo
from backend.repositories._base import SectionTimer, _safe_table
from backend.repositories.utils.response_cache import cache_result
from backend.utils.auth import mask_ips_for
from backend.utils.date_utils import parse_iso_utc
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.telemetry import process_context_scope
from backend.utils.time_window import (
    invite_clamp_fingerprint,
    is_valid_range_token,
    quantize_anchor,
)

logger = logging.getLogger(__name__)

repo = network_repo

router = APIRouter(prefix="/api", tags=["network"], responses=DEFAULT_ERROR_RESPONSES)
network_router = router

CORE_SECTIONS: frozenset[str] = frozenset(
    {"summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"}
)
SHIELDING_SECTIONS: frozenset[str] = frozenset({"shielding_analysis"})

_expand_network_sections = make_section_expander(NetworkSectionName)


def _resolve_network_window(
    req: NetworkHealthRequest | NetworkQualityRequest | NetworkBundleRequest,
    ctx: RequestContext,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Resolve and clamp the time window + cache-key tokens for network endpoints."""
    start_time, end_time = ctx.resolve_window(req)
    if is_valid_range_token(req.range_token):
        return (
            start_time,
            end_time,
            req.range_token,
            quantize_anchor(req.anchor),
            invite_clamp_fingerprint(ctx.analyst_session),
        )
    return start_time, end_time, None, None, None


def _execute_network_health_section(
    req: NetworkHealthRequest | NetworkBundleRequest,
    ctx: RequestContext,
    *,
    start_time: str | None,
    end_time: str | None,
    sections: set[str] | None,
    want_core: bool,
    want_shielding: bool,
    mask_ips: bool,
    keyed_range_token: str | None,
    keyed_anchor: str | None,
    invite_fp: str | None,
) -> dict[str, Any]:
    if want_core:
        res = network_repo.get_health(
            con_factory=ctx.con_factory(),
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
        res = cache_result({"available": True}, state="bypass", prewarmer=False)

    if "_response_cache" in res:
        res["_response_cache"] = {**res["_response_cache"], "scope": "self"}
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
        except HTTPException:
            raise
        except Exception:
            logger.exception(
                "shielding_analysis failed for service=%s",
                ctx.source.get("name"),
            )
            res["shielding_analysis"] = {"has_data": False, "error": True, "rows": []}
    return res


@router.post("/network-health", response_model=NetworkHealthResponse)
@query_errors()
def network_health(
    req: NetworkHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time, keyed_range_token, keyed_anchor, invite_fp = _resolve_network_window(req, ctx)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import network_health as hs_network_health

        return hs_network_health(high_scale_service, req, start_time, end_time)
    sections = _expand_network_sections(req.sections)
    mask_ips = mask_ips_for(ctx.analyst_session)

    want_core = sections is None or bool(sections & CORE_SECTIONS)
    want_shielding = sections is None or bool(sections & SHIELDING_SECTIONS)
    res = _execute_network_health_section(
        req,
        ctx,
        start_time=start_time,
        end_time=end_time,
        sections=sections,
        want_core=want_core,
        want_shielding=want_shielding,
        mask_ips=mask_ips,
        keyed_range_token=keyed_range_token,
        keyed_anchor=keyed_anchor,
        invite_fp=invite_fp,
    )
    return NetworkHealthResponse.with_telemetry(**res)


@router.post("/network-quality", response_model=NetworkQualityResponse)
@query_errors()
def network_quality(
    req: NetworkQualityRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import network_quality as hs_network_quality

        return hs_network_quality(high_scale_service, req, start_time, end_time)
    res = network_repo.get_quality(
        con_factory=ctx.con_factory(),
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        region_country=req.region_country,
    )
    return NetworkQualityResponse.with_telemetry(**cache_result(res, state="bypass", prewarmer=False))


def _fetch_pop_health_data(
    con: Any,
    source: dict[str, Any],
    start_time_dt: datetime,
    end_time_dt: datetime,
) -> tuple[list[PopHealthItem], bool, dict[str, Any]]:
    table_name = _safe_table(source["name"])
    from backend.repositories._base import QueryRunner

    runner = QueryRunner(con, source)
    timer = SectionTimer()
    started = time.perf_counter()
    rolled = runner.try_pop_health_from_rollup(
        start_time_dt.isoformat(),
        end_time_dt.isoformat(),
        has_filters=False,
    )
    timer.mark(
        "pop_health_rollup",
        started,
        source="rollup",
        activity="compute" if rolled is not None else "overhead",
        current_execution=True,
        outcome="served" if rolled is not None else "fallback",
    )
    observation = cache_result({"section_timings": timer.entries}, state="bypass", prewarmer=False)

    data = []
    if rolled is not None:
        for row in rolled:
            reqs = row[1]
            errs = row[2]
            hits = row[3]
            data.append(
                PopHealthItem(
                    pop=row[0],
                    requests=reqs,
                    errors=errs,
                    error_rate=round(errs * 100.0 / reqs, 2) if reqs else 0.0,
                    p50_rtt_us=row[5],
                    p95_ttfb_ms=row[6],
                    cache_hit_rate=round(hits * 100.0 / reqs, 2) if reqs else 0.0,
                    bandwidth_bytes=row[4] or 0,
                )
            )
        data.sort(key=lambda x: x.requests, reverse=True)
        return data, True, observation
    else:
        st_param = start_time_dt if start_time_dt.tzinfo is not None else start_time_dt.replace(tzinfo=UTC)
        et_param = end_time_dt if end_time_dt.tzinfo is not None else end_time_dt.replace(tzinfo=UTC)

        def _run_fallback(c):
            return [
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
                for row in network_repo.get_pop_health_rows(c, table_name, st_param, et_param)
            ]

        started = time.perf_counter()
        data = execute_with_stale_view_retry(con, source, _run_fallback)
        timer.mark("pop_health", started, source="live", activity="compute", current_execution=True, outcome="served")
        return data, False, observation


@router.get("/network/pop-health", response_model=PopHealthListResponse)
def get_pop_health(
    ctx: RequestContext = Depends(build_request_context),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
):
    """Aggregate edge-routing and cache metrics across Fastly POP locations."""
    clamped_start, clamped_end = ctx.clamp(
        start_time.isoformat() if start_time is not None else None,
        end_time.isoformat() if end_time is not None else None,
    )
    clamped_start_dt = parse_iso_utc(clamped_start) if clamped_start else start_time
    clamped_end_dt = parse_iso_utc(clamped_end) if clamped_end else end_time
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import get_pop_health as hs_get_pop_health

        return hs_get_pop_health(high_scale_service, clamped_start_dt, clamped_end_dt)

    start_time_dt = clamped_start_dt or (datetime.now(UTC) - timedelta(hours=24))
    end_time_dt = clamped_end_dt or datetime.now(UTC)
    data, is_approx, observation = _fetch_pop_health_data(ctx.con, ctx.source, start_time_dt, end_time_dt)
    return PopHealthListResponse.with_telemetry(data=data, approximate=is_approx, **observation)


def _network_bundle_impl(
    req: NetworkBundleRequest,
    ctx: RequestContext,
) -> NetworkBundleResponse:
    with process_context_scope("network:bundle"):
        start_time, end_time, keyed_range_token, keyed_anchor, invite_fp = _resolve_network_window(req, ctx)

        high_scale_service = ctx.high_scale
        if high_scale_service is not None:
            from backend.high_scale.network import network_bundle as hs_network_bundle

            return hs_network_bundle(high_scale_service, req, start_time, end_time)

        sections = _expand_network_sections(req.sections) if req.sections else None
        mask_ips = mask_ips_for(ctx.analyst_session)

        want_health = (
            sections is None
            or "health" in sections
            or any(s in CORE_SECTIONS or s in SHIELDING_SECTIONS for s in sections)
        )
        bundle_dq: list = []
        bundle_dc: list = []
        health_resp = None
        if want_health:
            want_core = sections is None or "health" in sections or bool(sections & CORE_SECTIONS)
            want_shielding = sections is None or "health" in sections or bool(sections & SHIELDING_SECTIONS)
            res = _execute_network_health_section(
                req,
                ctx,
                start_time=start_time,
                end_time=end_time,
                sections=sections,
                want_core=want_core,
                want_shielding=want_shielding,
                mask_ips=mask_ips,
                keyed_range_token=keyed_range_token,
                keyed_anchor=keyed_anchor,
                invite_fp=invite_fp,
            )
            bundle_dq.extend(res.get("debug_queries") or [])
            bundle_dc.extend(res.get("debug_calls") or [])
            health_resp = NetworkHealthResponse.with_telemetry(**res)

        want_quality = sections is None or "quality" in sections
        quality_resp = None
        if want_quality:
            q_res = network_repo.get_quality(
                con_factory=ctx.con_factory(),
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
                region_country=req.region_country,
            )
            bundle_dq.extend(q_res.get("debug_queries") or [])
            bundle_dc.extend(q_res.get("debug_calls") or [])
            quality_resp = NetworkQualityResponse.with_telemetry(**cache_result(q_res, state="bypass", prewarmer=False))

        want_pop_health = sections is None or "pop_health" in sections
        pop_health_items: list[PopHealthItem] | None = None
        pop_observation = None
        if want_pop_health:
            try:
                st_dt = (
                    datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    if start_time
                    else (datetime.now(UTC) - timedelta(hours=24))
                )
            except Exception:
                st_dt = datetime.now(UTC) - timedelta(hours=24)
            try:
                et_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")) if end_time else datetime.now(UTC)
            except Exception:
                et_dt = datetime.now(UTC)
            pop_health_items, _, pop_observation = _fetch_pop_health_data(ctx.con, ctx.source, st_dt, et_dt)

        want_transport = sections is None or "transport" in sections
        transport_resp = None
        if want_transport:
            t_res = network_repo.get_transport_metrics(
                con_factory=ctx.con_factory(),
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
            )
            bundle_dq.extend(t_res.get("debug_queries") or [])
            bundle_dc.extend(t_res.get("debug_calls") or [])
            transport_resp = TransportMetrics(**cache_result(t_res, state="bypass", prewarmer=False))

        return NetworkBundleResponse.with_telemetry(
            **cache_result({}, state="bypass", prewarmer=False),
            available=True,
            component_observations={"pop_health": pop_observation} if pop_observation is not None else None,
            health=health_resp,
            quality=quality_resp,
            pop_health=pop_health_items,
            transport=transport_resp,
            debug_queries=bundle_dq,
            debug_calls=bundle_dc,
        )


@router.post("/network/bundle", response_model=NetworkBundleResponse)
@query_errors()
def network_bundle(
    req: NetworkBundleRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    return _network_bundle_impl(req, ctx)


@router.get("/network/bundle", response_model=NetworkBundleResponse)
@query_errors()
def network_bundle_get(
    start_time: str | None = None,
    end_time: str | None = None,
    range_token: str | None = None,
    anchor: str | None = None,
    metric: str = "health_score",
    bucket_seconds: int = 300,
    top_n: int = 30,
    map_asn: str = "all",
    region_country: str = "US",
    sections: list[NetworkSectionName] | None = Query(default=None),
    ctx: RequestContext = Depends(build_request_context),
):
    req = NetworkBundleRequest(
        start_time=start_time,
        end_time=end_time,
        range_token=range_token,
        anchor=anchor,
        metric=metric,
        bucket_seconds=bucket_seconds,
        top_n=top_n,
        map_asn=map_asn,
        region_country=region_country,
        sections=sections,
    )
    return _network_bundle_impl(req, ctx)
