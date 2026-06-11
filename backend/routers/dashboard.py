"""Dashboard router — aggregates, raw logs, field value picker."""

from __future__ import annotations

import io
import time

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from backend.core.request_context import RequestContext, build_request_context
from backend.models.dashboard import (
    AggregatesRequest,
    AggregatesResponse,
    FieldValuesRequest,
    FieldValuesResponse,
    RawRequest,
    RawResponse,
)
from backend.repositories import dashboard as repo
from backend.utils.router_utils import query_errors

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.post("/aggregates", response_model=AggregatesResponse)
@query_errors()
def dashboard_aggregates(req: AggregatesRequest, ctx: RequestContext = Depends(build_request_context)):
    return repo.get_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        chart_interval=req.chart_interval,
        chart_metric=req.chart_metric,
    )


@router.post("/bundle")
@query_errors()
def dashboard_bundle(req: AggregatesRequest, ctx: RequestContext = Depends(build_request_context)):
    """Composite endpoint returning the two queries the dashboard page
    fires on every mount: /api/dashboard/aggregates + /api/security/top-bots.

    Saves one RTT per cold load — the frontend's useDashboardBundle
    hook fetches this once and seeds the existing
    ``['dashboard', 'aggregates', ...]`` and ``['dashboard',
    'top-bots', ...]`` React Query caches so the dedicated hooks
    return cached data without firing their own POSTs.

    Sequential execution (not parallel): the two queries share the
    same DuckDB connection from RequestContext, and DuckDB
    connections aren't thread-safe — running concurrently would
    require separate connections, which the connection-pool
    accounting on this endpoint isn't sized for. Sequential is
    correct + safe; the saving is the RTT, not backend wall-clock.

    Response shape is intentionally untyped (no response_model) so
    the existing dedicated endpoints stay the source of truth for
    AggregatesResponse / SecurityTopBotsResponse schemas — this
    composite passes through whatever those return.
    """
    from backend.repositories import security as security_repo

    section_timings: list[dict] = []
    t0 = time.perf_counter()
    aggregates = repo.get_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        chart_interval=req.chart_interval,
        chart_metric=req.chart_metric,
    )
    section_timings.append({"section": "bundle:aggregates", "time_ms": round((time.perf_counter() - t0) * 1000, 2)})
    t1 = time.perf_counter()
    top_bots = security_repo.get_top_bots(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
    section_timings.append({"section": "bundle:top_bots", "time_ms": round((time.perf_counter() - t1) * 1000, 2)})
    # Rename nested `section_timings` → `_section_timings` so the bundle
    # response mirrors what the dedicated /aggregates and /top-bots
    # endpoints emit (those go through Pydantic with
    # serialization_alias="_section_timings"). The composite has no
    # response_model so the rename has to happen here. Same for the
    # top-level bundle timings the perf harness reads from the root.
    for sub in (aggregates, top_bots):
        if isinstance(sub, dict) and "section_timings" in sub:
            sub["_section_timings"] = sub.pop("section_timings")
    return {
        "aggregates": aggregates,
        "top_bots": top_bots,
        "_section_timings": section_timings,
    }


@router.post("/raw", response_model=RawResponse)
@query_errors()
def dashboard_raw(req: RawRequest, ctx: RequestContext = Depends(build_request_context)):
    return repo.get_raw(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        page=req.page,
        limit=req.limit,
        sort_col=req.sort_col,
        sort_dir=req.sort_dir,
        columns=req.columns,
    )


@router.post("/raw/csv")
@query_errors()
def dashboard_raw_csv(req: RawRequest, ctx: RequestContext = Depends(build_request_context)):
    df = repo.get_raw_df(
        con=ctx.con,
        src=ctx.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        limit=50000,  # Cap at 50k rows for performance
        columns=req.columns,
    )

    if df.empty:
        return StreamingResponse(io.StringIO(""), media_type="text/csv")

    output = io.StringIO()
    df.to_csv(output, index=False)
    output.seek(0)

    filename = f"logs_{ctx.source['name']}_{int(time.time())}.csv"
    return StreamingResponse(
        output, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.post("/field-values", response_model=FieldValuesResponse)
@query_errors()
def dashboard_field_values(req: FieldValuesRequest, ctx: RequestContext = Depends(build_request_context)):
    return repo.get_field_values(
        con=ctx.con,
        src=ctx.source,
        field=req.field,
        search=req.search,
        limit=req.limit,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
