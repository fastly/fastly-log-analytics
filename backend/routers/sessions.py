"""Sessions router — session list and detail."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from backend.core.request_context import RequestContext, build_request_context
from backend.models.dashboard import (
    SessionDetailRequest,
    SessionDetailResponse,
    SessionsRequest,
    SessionsResponse,
)
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import sessions as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/sessions", tags=["sessions"], responses=DEFAULT_ERROR_RESPONSES)


@router.post("", response_model=SessionsResponse)
@query_errors()
def sessions_endpoint(
    req: SessionsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    return repo.get_sessions(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        page=req.page,
        limit=req.limit,
        sort_by=req.sort_by,
        sort_dir=req.sort_dir,
        flagged_only=req.flagged_only,
        min_reqs_flag=req.min_reqs_flag,
        min_4xx_pct_flag=req.min_4xx_pct_flag,
    )


@router.post("/detail", response_model=SessionDetailResponse)
@query_errors()
def sessions_detail(
    req: SessionDetailRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    if not req.ip or not req.start_time or not req.end_time:
        raise HTTPException(status_code=400, detail={"error": "ip, session_start, and session_end are required"})
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    # clamp_or_400 returns (None, None) only when both inputs are None AND
    # there's no analyst session; we already required both inputs above,
    # so the clamp always returns concrete ISO strings here.
    assert start_time is not None and end_time is not None
    return repo.get_session_detail(
        con=ctx.con,
        src=ctx.source,
        ip=req.ip,
        ja4=req.ja4,
        session_start=start_time,
        session_end=end_time,
    )
