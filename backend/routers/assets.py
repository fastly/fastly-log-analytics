"""Assets & Shield Performance router — asset type breakdowns, cache/compression statistics, and hotspots."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from backend import config as svcconfig
from backend.core.iceberg import execute_with_stale_view_retry
from backend.core.request_context import RequestContext, build_request_context
from backend.models.assets import AssetsAggregatesResponse, AssetsRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import assets as repo
from backend.utils.router_utils import query_errors
from backend.utils.telemetry import process_context_scope
from backend.utils.time_window import is_valid_range_token, resolve_window

router = APIRouter(prefix="/api/assets", tags=["assets"], responses=DEFAULT_ERROR_RESPONSES)


def _assets_aggregates_impl(
    req: AssetsRequest,
    ctx: RequestContext,
) -> AssetsAggregatesResponse:
    with process_context_scope("assets:aggregates"):
        if is_valid_range_token(req.range_token):
            earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
            resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
            start_time, end_time = ctx.clamp(resolved_start, resolved_end)
        else:
            start_time, end_time = ctx.clamp(req.start_time, req.end_time)

        def _run(con):
            return repo.get_assets_aggregates(
                con=con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
            )

        res = execute_with_stale_view_retry(ctx.con, ctx.source, _run)
        return AssetsAggregatesResponse.with_telemetry(**res)


@router.post("/aggregates", response_model=AssetsAggregatesResponse)
@query_errors()
def assets_aggregates(
    req: AssetsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    return _assets_aggregates_impl(req, ctx)


@router.get("/aggregates", response_model=AssetsAggregatesResponse)
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
