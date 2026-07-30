"""Sessions router — session list and detail."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException

from backend.core.request_context import RequestContext, build_request_context
from backend.core.session_token import (
    SessionTokenError,
    open_session_token,
    seal_session_token,
)
from backend.models.dashboard import (
    SessionDetailRequest,
    SessionDetailResponse,
    SessionsRequest,
    SessionsResponse,
)
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import sessions as repo
from backend.utils.auth import mask_ips_for
from backend.utils.date_utils import parse_iso_utc
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/sessions", tags=["sessions"], responses=DEFAULT_ERROR_RESPONSES)


@router.post("", response_model=SessionsResponse)
@query_errors()
def sessions_endpoint(
    req: SessionsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    # Guard against unbounded scans (14–20s observed) when the frontend hasn't
    # sent a time range yet. Default to the last 7 days — matches the max
    # window the repository enforces when a range IS provided.
    if not start_time or not end_time:
        _now = datetime.now(UTC)
        if not end_time:
            end_time = _now.isoformat()
        if not start_time:
            start_time = (_now - timedelta(days=7)).isoformat()
    result = repo.get_sessions(
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
        streaming_only=req.streaming_only,
    )
    # Mint an opaque token per row from the REAL ip/window — before the analyst
    # masking middleware rewrites ``ip`` on the way out. The detail endpoint
    # unseals it, so a masking analyst can drill in without the masked `ip`
    # (which can never match) being used as the lookup key. Uniform for admin +
    # analyst; the token is opaque ciphertext, so emitting it to everyone is safe.
    for row in result.get("sessions", []):
        row["session_token"] = seal_session_token(
            row.get("ip", ""),
            row.get("ja4"),
            row.get("session_start", ""),
            row.get("session_end", ""),
            service_id=ctx.service_id,
        )
    return result


@router.post("/detail", response_model=SessionDetailResponse)
@query_errors()
def sessions_detail(
    req: SessionDetailRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    ip, ja4 = req.ip, req.ja4
    start_raw, end_raw = req.start_time, req.end_time

    if req.session_token:
        # Token is the source of truth: derive the whole tuple from it and
        # ignore client-supplied ip/window. Prevents a token-pinned-IP +
        # widened-window mini-oracle, and is the only path open to a masking
        # analyst (who never holds the real ip).
        try:
            ip, ja4, start_raw, end_raw = open_session_token(req.session_token, service_id=ctx.service_id)
        except SessionTokenError as e:
            raise HTTPException(
                status_code=400,
                detail={
                    "error": "session_token_invalid",
                    "message": "Session reference expired — reload the page.",
                },
            ) from e
    elif mask_ips_for(ctx.analyst_session):
        # No token, but a masking analyst: a raw/masked top-level ``ip`` would
        # be a presence oracle (the masked value can never match a real IP, and
        # the PII filter-lock only inspects ``filters``, not this top-level
        # field). Masking analysts must drill in via the opaque token.
        raise HTTPException(status_code=403, detail={"error": "pii_policy_violation", "field": "ip"})

    if not ip or not start_raw or not end_raw:
        raise HTTPException(status_code=400, detail={"error": "ip, session_start, and session_end are required"})
    # A single-request session has session_start == session_end (a zero-width
    # window — ~28% of real sessions). ``ctx.clamp`` rejects an empty range, so
    # such a session would 400 ("time_range_empty") and the detail modal showed
    # "No results" for every role. Widen a zero/negative window by 1s on each
    # side: the repo's inclusive ``BETWEEN`` still captures the session's
    # request(s), and ctx.clamp re-caps the widened window to the analyst's
    # allowed bounds (so this can't be used to escape a query-window invite).
    s_dt, e_dt = parse_iso_utc(start_raw), parse_iso_utc(end_raw)
    if s_dt is not None and e_dt is not None and e_dt <= s_dt:
        start_raw = (s_dt - timedelta(seconds=1)).isoformat()
        end_raw = (e_dt + timedelta(seconds=1)).isoformat()
    start_time, end_time = ctx.clamp(start_raw, end_raw)
    # clamp_or_400 returns (None, None) only when both inputs are None AND
    # there's no analyst session; we already required both inputs above,
    # so the clamp always returns concrete ISO strings here.
    assert start_time is not None and end_time is not None

    from backend.core.iceberg import execute_with_stale_view_retry

    def _run_detail(con):
        return repo.get_session_detail(
            con=con,
            src=ctx.source,
            ip=ip,
            ja4=ja4,
            session_start=start_time,
            session_end=end_time,
        )

    return execute_with_stale_view_retry(ctx.con, ctx.source, _run_detail)
