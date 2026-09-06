"""Assets & Shield Performance router — asset type breakdowns, cache/compression statistics, and hotspots."""

from __future__ import annotations

from fastapi import APIRouter, Depends

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.assets import AssetsAggregatesResponse, AssetsRequest
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import assets as repo
from backend.utils.router_utils import query_errors
from backend.utils.time_window import is_valid_range_token, resolve_window

router = APIRouter(prefix="/api/assets", tags=["assets"], responses=DEFAULT_ERROR_RESPONSES)


@router.post("/aggregates", response_model=AssetsAggregatesResponse)
@query_errors()
def assets_aggregates(
    req: AssetsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    # Keyed path: resolve window server-side from (range_token, anchor), ignore
    # FE-supplied bounds, clamp AFTER resolve. Mirrors origin.py / performance.py.
    if is_valid_range_token(req.range_token):
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        start_time, end_time = ctx.clamp(resolved_start, resolved_end)
    else:
        start_time, end_time = ctx.clamp(req.start_time, req.end_time)

    res = repo.get_assets_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return AssetsAggregatesResponse.with_telemetry(**res)
