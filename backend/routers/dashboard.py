"""Dashboard router — aggregates, raw logs, field value picker."""

from __future__ import annotations

import io
import time

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from backend.deps import AnalyticsDeps
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
def dashboard_aggregates(req: AggregatesRequest, deps: AnalyticsDeps = Depends()):
    return repo.get_aggregates(
        con=deps.con,
        src=deps.source,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
        chart_interval=req.chart_interval,
        chart_metric=req.chart_metric,
    )


@router.post("/raw", response_model=RawResponse)
@query_errors()
def dashboard_raw(req: RawRequest, deps: AnalyticsDeps = Depends()):
    return repo.get_raw(
        con=deps.con,
        src=deps.source,
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
def dashboard_raw_csv(req: RawRequest, deps: AnalyticsDeps = Depends()):
    df = repo.get_raw_df(
        con=deps.con,
        src=deps.source,
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

    filename = f"logs_{deps.source['name']}_{int(time.time())}.csv"
    return StreamingResponse(
        output, media_type="text/csv", headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@router.post("/field-values", response_model=FieldValuesResponse)
@query_errors()
def dashboard_field_values(req: FieldValuesRequest, deps: AnalyticsDeps = Depends()):
    return repo.get_field_values(
        con=deps.con,
        src=deps.source,
        field=req.field,
        search=req.search,
        limit=req.limit,
        start_time=req.start_time,
        end_time=req.end_time,
        filters=req.filters,
    )
