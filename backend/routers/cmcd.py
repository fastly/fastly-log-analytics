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

    from backend.high_scale.registry import get_high_scale_service_registry

    high_scale_service = get_high_scale_service_registry().resolve(ctx.service_id)
    if high_scale_service is not None:
        from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd

        return hs_cmcd(high_scale_service, req, start_time, end_time, sections, mask_ips)

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
