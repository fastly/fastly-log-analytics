"""Fastly Value router — executive summary across all Fastly products."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.value import ValueSectionName, ValueSummaryRequest, ValueSummaryResponse
from backend.repositories import value as repo
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.time_window import is_valid_range_token, resolve_window

router = APIRouter(prefix="/api/value", tags=["value"], responses=DEFAULT_ERROR_RESPONSES)

_expand_sections = make_section_expander(ValueSectionName)


@router.post("/summary", response_model=ValueSummaryResponse)
@query_errors()
def value_summary(
    req: ValueSummaryRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time: str | None
    end_time: str | None
    if is_valid_range_token(req.range_token):
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        start_time, end_time = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        start_time, end_time = ctx.clamp(start_time, end_time)
    else:
        start_time, end_time = ctx.clamp(req.start_time, req.end_time)

    sections = _expand_sections(req.sections)

    return repo.get_summary(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        chart_interval=req.chart_interval,
        sections=sections,
        service_id=ctx.source.get("service_id"),
    )
