"""Dashboard router — aggregates, raw logs, field value picker."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterator
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse

from backend.core.request_context import RequestContext, build_request_context
from backend.high_scale import dashboard as hs_dashboard
from backend.high_scale.ingest_controller import HighScaleService
from backend.models.analytics import (
    AggregatesRequest,
    AggregatesResponse,
    BundleResponse,
    DashboardSectionName,
    FieldValuesRequest,
    FieldValuesResponse,
    RawRequest,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.repositories.utils.response_cache import cache_result
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import (
    make_section_expander,
    query_errors,
)
from backend.utils.telemetry import get_sqlite_queries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"], responses=DEFAULT_ERROR_RESPONSES)
dashboard_router = router

# No coupling — every dashboard section is independent.
_expand_sections = make_section_expander(DashboardSectionName)


def _require_high_scale(ctx: RequestContext) -> HighScaleService:
    if ctx.high_scale is None:
        raise HTTPException(
            status_code=503,
            detail={"error": "ClickHouse service is not available for this service"},
        )
    return ctx.high_scale


def _resolve_aggregate_flags(
    req: AggregatesRequest,
    sections: set[str] | None,
) -> tuple[bool, bool, bool, bool]:
    """Translate the sections selector + existing include_* flags into the
    four gates: (time_series, conn_requests, map_data, top_n).
    """
    if sections is None:
        return (
            True if req.include_time_series is None else req.include_time_series,
            True if req.include_conn_requests is None else req.include_conn_requests,
            True if req.include_map_data is None else req.include_map_data,
            True,
        )
    want_core = "core" in sections
    want_topten = "topten" in sections
    return (want_core, want_core, want_core, want_topten)


@router.post("/aggregates", response_model=AggregatesResponse)
@query_errors()
def dashboard_aggregates(
    req: AggregatesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)
    _expand_sections(req.sections)
    high_scale_service = _require_high_scale(ctx)
    return hs_dashboard.aggregates(high_scale_service, req, start_time, end_time)


@router.post("/bundle", response_model=BundleResponse)
@query_errors()
async def dashboard_bundle(
    req: AggregatesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    """Composite endpoint returning /api/dashboard/aggregates + /api/security/top-bots."""
    start_time, end_time = ctx.resolve_window(req)
    _expand_sections(req.sections)
    high_scale_service = _require_high_scale(ctx)
    return await asyncio.to_thread(hs_dashboard.bundle, high_scale_service, req, start_time, end_time)


def _assemble_dashboard_bundle_response(
    aggregates: Any,
    top_bots: Any,
    section_timings: list[dict[str, Any]],
) -> dict[str, Any]:
    """Lift `debug_queries` / `debug_calls` from each sub-response into the top-level BundleResponse."""
    if isinstance(aggregates, dict):
        aggregates = dict(aggregates)
    if isinstance(top_bots, dict):
        top_bots = dict(top_bots)
    all_debug_queries: list = []
    all_debug_calls: list = []
    for sub in (aggregates, top_bots):
        if isinstance(sub, dict):
            all_debug_queries.extend(sub.pop("debug_queries", []) or [])
            all_debug_queries.extend(sub.pop("_debug_queries", []) or [])
            all_debug_calls.extend(sub.pop("debug_calls", []) or [])
            all_debug_calls.extend(sub.pop("_debug_calls", []) or [])
    return {
        **cache_result({}, state="bypass", prewarmer=False),
        "aggregates": aggregates,
        "top_bots": top_bots,
        "section_timings": section_timings,
        "debug_queries": all_debug_queries,
        "debug_calls": all_debug_calls,
        "debug_sqlite": list(get_sqlite_queries()),
    }


@router.post("/raw/csv")
@query_errors()
def dashboard_raw_csv(
    req: RawRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    csv_limit = min(max(1, int(req.limit)), 50000) if "limit" in req.model_fields_set and req.limit else 50000
    high_scale_service = _require_high_scale(ctx)
    df = hs_dashboard.raw_df(high_scale_service, req, start_time, end_time, limit=csv_limit)

    if df.empty:
        return StreamingResponse(iter([b""]), media_type="text/csv")

    if mask_ips_for(ctx.analyst_session):
        from backend.core.share_db import IP_FAMILY_KEYS, SESSION_ID_KEYS, mask_ip

        for col in IP_FAMILY_KEYS:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: mask_ip(v) if isinstance(v, str) else v)
        for col in SESSION_ID_KEYS:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: ("[redacted]" if v else v) if isinstance(v, str) else v)

    def csv_chunks() -> Iterator[bytes]:
        chunk_rows = 2000
        first = True
        for start in range(0, len(df), chunk_rows):
            slice_df = df.iloc[start : start + chunk_rows]
            yield slice_df.to_csv(header=first, index=False).encode("utf-8")
            first = False

    source_name = high_scale_service.service_id
    filename = f"logs_{source_name}_{int(time.time())}.csv"
    return StreamingResponse(
        csv_chunks(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.post("/field-values", response_model=FieldValuesResponse)
@query_errors()
def dashboard_field_values(
    req: FieldValuesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = _require_high_scale(ctx)
    return hs_dashboard.field_values(high_scale_service, req, start_time, end_time)
