"""Raw Logs Explorer router — high-throughput paginated access logs."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query

from backend.core.iceberg import execute_with_stale_view_retry
from backend.core.request_context import RequestContext, build_request_context
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.models.logs import LogsQueryRequest, LogsQueryResponse
from backend.repositories import logs as repo
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import query_errors
from backend.utils.telemetry import process_context_scope

router = APIRouter(prefix="/api/logs", tags=["logs"], responses=DEFAULT_ERROR_RESPONSES)


def _query_logs_impl(
    req: LogsQueryRequest,
    ctx: RequestContext,
) -> LogsQueryResponse:
    with process_context_scope("logs:query"):
        start_time, end_time = ctx.resolve_window(req)

        mask_ips = mask_ips_for(ctx.analyst_session)

        high_scale_service = ctx.high_scale
        if high_scale_service is not None:
            from backend.high_scale.logs import logs_query_endpoint

            res = logs_query_endpoint(
                service=high_scale_service,
                req=req,
                start_time=start_time,
                end_time=end_time,
                mask_ips=mask_ips,
            )
            return LogsQueryResponse.with_telemetry(**res)

        def _run(con):
            return repo.query_logs(
                con=con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
                limit=req.limit,
                offset=req.offset,
                sort_dir=req.sort_dir,
                search=req.search,
                columns=req.columns,
                mask_ips=mask_ips,
            )

        res = execute_with_stale_view_retry(ctx.con, ctx.source, _run)
        return LogsQueryResponse.with_telemetry(**res)


@router.post("", response_model=LogsQueryResponse)
@router.post("/query", response_model=LogsQueryResponse)
@query_errors()
def query_logs_post(
    req: LogsQueryRequest,
    ctx: RequestContext = Depends(build_request_context),
) -> LogsQueryResponse:
    """Execute a paginated query for raw log records."""
    return _query_logs_impl(req, ctx)


@router.get("", response_model=LogsQueryResponse)
@router.get("/{service_id}", response_model=LogsQueryResponse)
@query_errors()
def query_logs_get(
    service_id: str | None = None,
    service: str | None = Query(default=None),
    start_time: str | None = Query(default=None, alias="from"),
    end_time: str | None = Query(default=None, alias="to"),
    range_token: str | None = None,
    anchor: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    sort_dir: str = "desc",
    search: str | None = None,
    columns: str | None = None,
    status: str | None = None,
    pop: str | None = None,
    client_ip: str | None = None,
    url: str | None = None,
    origin: str | None = None,
    asn: str | None = None,
    ctx: RequestContext = Depends(build_request_context),
) -> LogsQueryResponse:
    """Execute a paginated query for raw log records via GET."""
    filters: dict[str, Any] = {}
    if status:
        filters["status"] = [int(s.strip()) if s.strip().isdigit() else s.strip() for s in status.split(",")]
    if pop:
        filters["pop"] = [p.strip() for p in pop.split(",")]
    if client_ip:
        filters["client_ip"] = [client_ip.strip()]
    if url:
        filters["url"] = [url.strip()]
    if origin:
        filters["origin"] = [origin.strip()]
    if asn:
        filters["asn"] = [asn.strip()]

    col_list = [c.strip() for c in columns.split(",") if c.strip()] if columns else None

    req = LogsQueryRequest(
        start_time=start_time,
        end_time=end_time,
        range_token=range_token,
        anchor=anchor,
        limit=limit,
        offset=offset,
        sort_dir=sort_dir,
        search=search,
        columns=col_list,
        filters=filters,
    )
    return _query_logs_impl(req, ctx)
