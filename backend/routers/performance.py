"""Performance, Assets & Shield, Fastly Value, and CMCD streaming QoE router."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Path

from backend.core.request_context import RequestContext, build_request_context
from backend.core.storage import execute_with_stale_view_retry
from backend.models.analytics import (
    AssetsAggregatesResponse,
    AssetsRequest,
    CmcdAggregatesResponse,
    CmcdRequest,
    CmcdSectionName,
    PerformanceAggregatesResponse,
    PerformanceRequest,
    PerformanceSectionName,
    ValueSectionName,
    ValueSummaryRequest,
    ValueSummaryResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES, CmcdDisableBody, CmcdEnableBody, CmcdStatusResponse
from backend.repositories import assets as assets_repo
from backend.repositories import cmcd as cmcd_repo
from backend.repositories import performance as perf_repo
from backend.repositories import value as value_repo
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import (
    make_section_expander,
    query_errors,
    run_cached_repo_endpoint,
)
from backend.utils.router_utils import (
    resolve_fastly_token as _resolve_token,
)
from backend.utils.sse import stream_iac_reconcile
from backend.utils.telemetry import process_context_scope

logger = logging.getLogger(__name__)

repo = perf_repo

router = APIRouter(prefix="/api", responses=DEFAULT_ERROR_RESPONSES)
performance_router = router

_TOP_N_PAIR: frozenset[str] = frozenset({"top_urls", "top_asns"})
_WATERFALL_SCATTER_PAIR: frozenset[str] = frozenset({"waterfall", "scatter"})

_expand_performance_sections = make_section_expander(
    PerformanceSectionName, union_groups=(_TOP_N_PAIR, _WATERFALL_SCATTER_PAIR)
)
_expand_cmcd_sections = make_section_expander(CmcdSectionName)
_expand_value_sections = make_section_expander(ValueSectionName)


@router.post("/performance/aggregates", tags=["performance"], response_model=PerformanceAggregatesResponse)
@query_errors()
def performance_aggregates(
    req: PerformanceRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)

    high_scale_service = ctx.high_scale
    sections = _expand_performance_sections(req.sections)
    if high_scale_service is not None:
        from backend.high_scale.performance import performance_aggregates as hs_performance

        return hs_performance(high_scale_service, req, start_time, end_time, sections=sections)
    return run_cached_repo_endpoint(
        ctx,
        repo_module="backend.repositories.performance",
        get_fn=perf_repo.get_performance_aggregates,
        peek_fn=perf_repo.peek_performance_aggregates,
        kwargs={
            "start_time": start_time,
            "end_time": end_time,
            "filters": req.filters,
            "sort_by": req.sort_by,
            "sections": sections,
        },
        wrap=lambda r: PerformanceAggregatesResponse.with_telemetry(**r),
    )


def _assets_aggregates_impl(
    req: AssetsRequest,
    ctx: RequestContext,
) -> AssetsAggregatesResponse:
    with process_context_scope("assets:aggregates"):
        start_time, end_time = ctx.resolve_window(req)
        high_scale_service = ctx.high_scale
        if high_scale_service is not None:
            from backend.high_scale.ingest_controller import assets_aggregates as hs_assets_aggregates

            return hs_assets_aggregates(high_scale_service, req, start_time, end_time)

        cached = assets_repo.check_assets_cache(ctx.source, start_time, end_time, req.filters)
        if cached is not None:
            return AssetsAggregatesResponse.with_telemetry(**cached)

        def _run(con):
            return assets_repo.get_assets_aggregates(
                con=con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
            )

        res = execute_with_stale_view_retry(ctx.con, ctx.source, _run)
        return AssetsAggregatesResponse.with_telemetry(**res)


@router.post("/assets/aggregates", tags=["assets"], response_model=AssetsAggregatesResponse)
@query_errors()
def assets_aggregates(
    req: AssetsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    return _assets_aggregates_impl(req, ctx)


@router.get("/assets/aggregates", tags=["assets"], response_model=AssetsAggregatesResponse)
@query_errors()
def assets_aggregates_get(
    start_time: str | None = None,
    end_time: str | None = None,
    range_token: str | None = None,
    anchor: str | None = None,
    ctx: RequestContext = Depends(build_request_context),
):
    req = AssetsRequest(
        start_time=start_time,
        end_time=end_time,
        range_token=range_token,
        anchor=anchor,
    )
    return _assets_aggregates_impl(req, ctx)


@router.post("/cmcd/aggregates", tags=["cmcd"], response_model=CmcdAggregatesResponse)
@query_errors()
def cmcd_aggregates(
    req: CmcdRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)

    sections = _expand_cmcd_sections(req.sections)
    mask_ips = mask_ips_for(ctx.analyst_session)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd

        res = hs_cmcd(high_scale_service, req, start_time, end_time, sections, mask_ips)
        return CmcdAggregatesResponse.with_telemetry(**res)

    t_entered = ctx.telemetry.elapsed_ms()
    acquire_ms = 0.0

    def _acquire_con():
        nonlocal acquire_ms
        t0 = ctx.telemetry.elapsed_ms()
        c = ctx.con
        acquire_ms = ctx.telemetry.elapsed_ms() - t0
        return c

    _is_real_repo = getattr(cmcd_repo.get_cmcd_aggregates, "__module__", "") == "backend.repositories.cmcd"
    con_arg = _acquire_con() if (ctx.has_eager_con or not _is_real_repo) else _acquire_con

    res = cmcd_repo.get_cmcd_aggregates(
        con=con_arg,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_seconds=req.bucket_seconds,
        top_n=req.top_n,
        sections=sections,
        mask_ips=mask_ips,
    )
    res["section_timings"] = [
        {"section": "request_setup", "time_ms": round(t_entered, 2)},
        {"section": "connection_acquire", "time_ms": round(acquire_ms, 2)},
        *res.get("section_timings", []),
    ]
    return CmcdAggregatesResponse.with_telemetry(**res)


@router.post("/value/summary", tags=["value"], response_model=ValueSummaryResponse)
@query_errors()
def value_summary(
    req: ValueSummaryRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time: str | None
    end_time: str | None
    start_time, end_time = ctx.resolve_window(req)

    sections = _expand_value_sections(req.sections)

    res = value_repo.get_summary(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        chart_interval=req.chart_interval,
        sections=sections,
        service_id=ctx.source.get("service_id"),
    )
    return ValueSummaryResponse.with_telemetry(**res)


@router.get(
    "/services/{service_id}/cmcd/status",
    tags=["cmcd-admin"],
    response_model=CmcdStatusResponse,
    response_model_exclude_unset=True,
)
def cmcd_status(
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    cmcd = cfg.get("cmcd")
    if not cmcd or not cmcd.get("enabled"):
        return {"enabled": False}
    return {
        "enabled": True,
        "mode": cmcd.get("mode", "query_string"),
        "version": cmcd.get("version", 1),
        "enabled_at": cmcd.get("enabled_at"),
    }


@router.post("/services/{service_id}/cmcd/enable", tags=["cmcd-admin"])
def cmcd_enable(
    service_id: str = Path(..., description="Logging service ID"),
    body: CmcdEnableBody | None = None,
):
    token = body.token if body else ""
    mode = body.mode if body else "query_string"
    version = body.version if body else 1
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required (pass in JSON body or set in service config)"},
        )

    import datetime as _dt

    from backend import config as svcconfig

    svcconfig.mutate_config(
        service_id,
        lambda c: c.__setitem__(
            "cmcd",
            {
                "enabled": True,
                "mode": mode,
                "version": version,
                "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
            },
        ),
    )

    def _done_event() -> dict:
        cfg = svcconfig.load_config(service_id) or {}
        return {
            "type": "done",
            "message": f"CMCD v{version} collection enabled.",
            "cmcd": cfg.get("cmcd", {}),
        }

    return stream_iac_reconcile(
        service_id,
        resolved_token,
        start_message=f"Enabling CMCD v{version} collection for {service_id}...",
        done_event=_done_event,
        log_label="cmcd_enable",
        logger_obj=logger,
    )


@router.post("/services/{service_id}/cmcd/disable", tags=["cmcd-admin"])
def cmcd_disable(
    service_id: str = Path(..., description="Logging service ID"),
    body: CmcdDisableBody | None = None,
):
    token = body.token if body else ""
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required"},
        )

    from backend import config as svcconfig

    def _disable(c: dict) -> None:
        if "cmcd" in c:
            c["cmcd"]["enabled"] = False

    svcconfig.mutate_config(service_id, _disable)

    return stream_iac_reconcile(
        service_id,
        resolved_token,
        start_message=f"Disabling CMCD collection for {service_id}...",
        done_event={"type": "done", "message": "CMCD collection disabled."},
        log_label="cmcd_disable",
        logger_obj=logger,
    )
