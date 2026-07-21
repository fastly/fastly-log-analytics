"""CMCD (Common Media Client Data) router — streaming QoE analytics."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.cmcd import CmcdAggregatesResponse, CmcdRequest, CmcdSectionName
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import cmcd as repo
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.time_window import is_valid_range_token, resolve_window

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["cmcd"], responses=DEFAULT_ERROR_RESPONSES)

_expand_sections = make_section_expander(CmcdSectionName)


@router.post("/cmcd/aggregates", response_model=CmcdAggregatesResponse)
@query_errors()
def cmcd_aggregates(
    req: CmcdRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    if is_valid_range_token(req.range_token):
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        start_time, end_time = ctx.clamp(resolved_start, resolved_end)
    else:
        start_time, end_time = ctx.clamp(req.start_time, req.end_time)

    sections = _expand_sections(req.sections)
    mask_ips = mask_ips_for(ctx.analyst_session)

    res = repo.get_cmcd_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_seconds=req.bucket_seconds,
        top_n=req.top_n,
        sections=sections,
        mask_ips=mask_ips,
    )
    return CmcdAggregatesResponse.with_telemetry(**res)
