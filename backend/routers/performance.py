"""Performance, Assets & Shield, Fastly Value, and CMCD streaming QoE router."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Path, Request

from backend.core.request_context import RequestContext, build_request_context
from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.ingest_controller import assets_aggregates as hs_assets_aggregates
from backend.high_scale.performance import (
    performance_aggregates as hs_performance,
)
from backend.high_scale.performance import (
    value_summary as hs_value_summary,
)
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
from backend.utils.auth import analyst_allowed_services, mask_ips_for, require_service_in_scope
from backend.utils.router_utils import (
    make_section_expander,
    query_errors,
)
from backend.utils.router_utils import (
    resolve_fastly_token as _resolve_token,
)
from backend.utils.sse import stream_iac_reconcile
from backend.utils.telemetry import process_context_scope

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", responses=DEFAULT_ERROR_RESPONSES)
performance_router = router

_TOP_N_PAIR: frozenset[str] = frozenset({"top_urls", "top_asns"})
_WATERFALL_SCATTER_PAIR: frozenset[str] = frozenset({"waterfall", "scatter"})

_expand_performance_sections = make_section_expander(
    PerformanceSectionName, union_groups=(_TOP_N_PAIR, _WATERFALL_SCATTER_PAIR)
)
_expand_cmcd_sections = make_section_expander(CmcdSectionName)
_expand_value_sections = make_section_expander(ValueSectionName)


def _require_high_scale(ctx: RequestContext) -> HighScaleService:
    if ctx.high_scale is None:
        raise HTTPException(
            status_code=503,
            detail={"error": "ClickHouse service is not available for this service"},
        )
    return ctx.high_scale


@router.post("/performance/aggregates", tags=["performance"], response_model=PerformanceAggregatesResponse)
@query_errors()
def performance_aggregates(
    req: PerformanceRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)
    sections = _expand_performance_sections(req.sections)
    high_scale_service = _require_high_scale(ctx)
    return hs_performance(high_scale_service, req, start_time, end_time, sections=sections)


def _assets_aggregates_impl(
    req: AssetsRequest,
    ctx: RequestContext,
) -> AssetsAggregatesResponse:
    with process_context_scope("assets:aggregates"):
        start_time, end_time = ctx.resolve_window(req)
        high_scale_service = _require_high_scale(ctx)
        return hs_assets_aggregates(high_scale_service, req, start_time, end_time)


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
    high_scale_service = _require_high_scale(ctx)
    res = hs_cmcd(high_scale_service, req, start_time, end_time, sections, mask_ips)
    return CmcdAggregatesResponse.with_telemetry(**res)


@router.post("/value/summary", tags=["value"], response_model=ValueSummaryResponse)
@query_errors()
def value_summary(
    req: ValueSummaryRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)
    sections = _expand_value_sections(req.sections)
    high_scale_service = _require_high_scale(ctx)
    return hs_value_summary(high_scale_service, req, start_time, end_time, sections=sections)


@router.get(
    "/services/{service_id}/cmcd/status",
    tags=["cmcd-admin"],
    response_model=CmcdStatusResponse,
    response_model_exclude_unset=True,
)
def cmcd_status(
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    from backend import config as svcconfig

    require_service_in_scope(request, service_id)
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
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
    body: CmcdEnableBody | None = None,
):
    require_service_in_scope(request, service_id)
    if analyst_allowed_services(request) is not None:
        raise HTTPException(status_code=403, detail={"error": "admin_only"})
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
    request: Request,
    service_id: str = Path(..., description="Logging service ID"),
    body: CmcdDisableBody | None = None,
):
    require_service_in_scope(request, service_id)
    if analyst_allowed_services(request) is not None:
        raise HTTPException(status_code=403, detail={"error": "admin_only"})
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
