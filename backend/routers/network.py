from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.network import (
    NetworkBundleRequest,
    NetworkBundleResponse,
    NetworkHealthRequest,
    NetworkHealthResponse,
    NetworkQualityRequest,
    NetworkQualityResponse,
    PopHealthItem,
    PopHealthListResponse,
    SectionName,
    TransportMetrics,
)
from backend.repositories import network as repo
from backend.repositories._base import _safe_table
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.telemetry import process_context_scope
from backend.utils.time_window import (
    invite_clamp_fingerprint,
    is_valid_range_token,
    quantize_anchor,
    resolve_window,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["network"], responses=DEFAULT_ERROR_RESPONSES)

CORE_SECTIONS: frozenset[str] = frozenset(
    {"summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"}
)
SHIELDING_SECTIONS: frozenset[str] = frozenset({"shielding_analysis"})

# No coupling — every network section is independent.
_expand_sections = make_section_expander(SectionName)


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

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import network_health as hs_network_health

        return hs_network_health(high_scale_service, req, start_time, end_time)
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
            con_factory=lambda: ctx.con,
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
        except HTTPException:
            raise
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

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import network_quality as hs_network_quality

        return hs_network_quality(high_scale_service, req, start_time, end_time)
    res = repo.get_quality(
        con_factory=lambda: ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        region_country=req.region_country,
    )
    return NetworkQualityResponse.with_telemetry(**res)


def _fetch_pop_health_data(
    con: Any,
    source: dict[str, Any],
    start_time_dt: datetime,
    end_time_dt: datetime,
) -> tuple[list[PopHealthItem], bool]:
    table_name = _safe_table(source["name"])
    from backend.repositories._base import QueryRunner

    runner = QueryRunner(con, source)
    rolled = runner.try_pop_health_from_rollup(
        start_time_dt.isoformat(),
        end_time_dt.isoformat(),
        has_filters=False,
    )

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
        return data, True
    else:
        st_param = start_time_dt if start_time_dt.tzinfo is not None else start_time_dt.replace(tzinfo=UTC)
        et_param = end_time_dt if end_time_dt.tzinfo is not None else end_time_dt.replace(tzinfo=UTC)
        from backend.core.iceberg import execute_with_stale_view_retry

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
                for row in repo.get_pop_health_rows(c, table_name, st_param, et_param)
            ]

        data = execute_with_stale_view_retry(con, source, _run_fallback)
        return data, False


@router.get("/network/pop-health", response_model=PopHealthListResponse)
def get_pop_health(
    ctx: RequestContext = Depends(build_request_context),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
):
    """Aggregate edge-routing and cache metrics across Fastly POP locations."""
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import get_pop_health as hs_get_pop_health

        return hs_get_pop_health(high_scale_service, start_time, end_time)

    start_time_dt = start_time or (datetime.now(UTC) - timedelta(hours=24))
    end_time_dt = end_time or datetime.now(UTC)
    data, is_approx = _fetch_pop_health_data(ctx.con, ctx.source, start_time_dt, end_time_dt)
    return PopHealthListResponse.with_telemetry(data=data, approximate=is_approx)


def _network_bundle_impl(
    req: NetworkBundleRequest,
    ctx: RequestContext,
) -> NetworkBundleResponse:
    with process_context_scope("network:bundle"):
        if is_valid_range_token(req.range_token):
            earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
            resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
            start_time, end_time = ctx.clamp(resolved_start, resolved_end)
            keyed_range_token = req.range_token
            keyed_anchor = quantize_anchor(req.anchor)
            invite_fp = invite_clamp_fingerprint(ctx.analyst_session)
        else:
            start_time, end_time = ctx.clamp(req.start_time, req.end_time)
            keyed_range_token = None
            keyed_anchor = None
            invite_fp = None

        high_scale_service = ctx.high_scale
        if high_scale_service is not None:
            from backend.high_scale.network import network_bundle as hs_network_bundle

            return hs_network_bundle(high_scale_service, req, start_time, end_time)

        sections = _expand_sections(req.sections) if req.sections else None
        mask_ips = mask_ips_for(ctx.analyst_session)

        # 1. Health
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
            if want_core:
                res = repo.get_health(
                    con_factory=lambda: ctx.con,
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
                except HTTPException:
                    raise
                except Exception:
                    logger.exception(
                        "shielding_analysis failed for service=%s",
                        ctx.source.get("name"),
                    )
                    res["shielding_analysis"] = {"has_data": False, "error": True, "rows": []}
            bundle_dq.extend(res.get("debug_queries") or [])
            bundle_dc.extend(res.get("debug_calls") or [])
            health_resp = NetworkHealthResponse.with_telemetry(**res)

        # 2. Quality
        want_quality = sections is None or "quality" in sections
        quality_resp = None
        if want_quality:
            q_res = repo.get_quality(
                con_factory=lambda: ctx.con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
                region_country=req.region_country,
            )
            bundle_dq.extend(q_res.get("debug_queries") or [])
            bundle_dc.extend(q_res.get("debug_calls") or [])
            quality_resp = NetworkQualityResponse.with_telemetry(**q_res)

        # 3. Pop Health
        want_pop_health = sections is None or "pop_health" in sections
        pop_health_items: list[PopHealthItem] | None = None
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
            pop_health_items, _ = _fetch_pop_health_data(ctx.con, ctx.source, st_dt, et_dt)

        # 4. Transport Metrics
        want_transport = sections is None or "transport" in sections
        transport_resp = None
        if want_transport:
            t_res = repo.get_transport_metrics(
                con_factory=lambda: ctx.con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
            )
            bundle_dq.extend(t_res.get("debug_queries") or [])
            bundle_dc.extend(t_res.get("debug_calls") or [])
            transport_resp = TransportMetrics(**t_res)

        return NetworkBundleResponse.with_telemetry(
            available=True,
            health=health_resp,
            quality=quality_resp,
            pop_health=pop_health_items,
            transport=transport_resp,
            debug_queries=bundle_dq or None,
            debug_calls=bundle_dc or None,
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
    sections: list[SectionName] | None = Query(default=None),
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
