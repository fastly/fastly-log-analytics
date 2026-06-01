"""Sessions router — session list and detail."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from backend.deps import AnalyticsDeps
from backend.models.dashboard import (
    SessionDetailRequest,
    SessionDetailResponse,
    SessionsRequest,
    SessionsResponse,
)
from backend.repositories import sessions as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/sessions", tags=["sessions"])


@router.post("", response_model=SessionsResponse)
@query_errors()
def sessions_endpoint(req: SessionsRequest, deps: AnalyticsDeps = Depends()):
    return repo.get_sessions(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
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
def sessions_detail(req: SessionDetailRequest, deps: AnalyticsDeps = Depends()):
    if not req.ip or not req.start_time or not req.end_time:
        raise HTTPException(status_code=400, detail={"error": "ip, session_start, and session_end are required"})
    return repo.get_session_detail(
        con=deps.con,
        src=deps.source,
        ip=req.ip,
        ja4=req.ja4,
        session_start=req.start_time,
        session_end=req.end_time,
    )
