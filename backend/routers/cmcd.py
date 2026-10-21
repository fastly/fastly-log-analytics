"""CMCD (Common Media Client Data) router — streaming QoE analytics."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends

from backend.core.request_context import RequestContext, build_request_context
from backend.models.cmcd import CmcdAggregatesResponse, CmcdRequest, CmcdSectionName
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import cmcd as repo
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import make_section_expander, query_errors

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["cmcd"], responses=DEFAULT_ERROR_RESPONSES)

_expand_sections = make_section_expander(CmcdSectionName)


@router.post("/cmcd/aggregates", response_model=CmcdAggregatesResponse)
@query_errors()
def cmcd_aggregates(
    req: CmcdRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)

    sections = _expand_sections(req.sections)
    mask_ips = mask_ips_for(ctx.analyst_session)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd

        res = hs_cmcd(high_scale_service, req, start_time, end_time, sections, mask_ips)
        return CmcdAggregatesResponse.with_telemetry(**res)

    # Time spent before and around the repository call (dependency setup, pool
    # wait for the connection) is invisible to the repository's own section
    # timer, so record it as leading entries of `_section_timings`.
    t_entered = ctx.telemetry.elapsed_ms()
    con = ctx.con
    t_connected = ctx.telemetry.elapsed_ms()

    res = repo.get_cmcd_aggregates(
        con=con,
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
        {"section": "connection_acquire", "time_ms": round(t_connected - t_entered, 2)},
        *res.get("section_timings", []),
    ]
    return CmcdAggregatesResponse.with_telemetry(**res)
