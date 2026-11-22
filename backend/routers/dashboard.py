"""Dashboard router — aggregates, raw logs, field value picker."""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Iterator
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from backend.core.request_context import RequestContext, build_request_context
from backend.high_scale import dashboard as hs_dashboard
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
from backend.repositories.utils.response_shaping import build_telemetry_response_envelope
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import (
    make_section_expander,
    query_errors,
    require_high_scale,
)
from backend.utils.telemetry import get_sqlite_queries

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"], responses=DEFAULT_ERROR_RESPONSES)
dashboard_router = router

# No coupling — every dashboard section is independent.
_expand_sections = make_section_expander(DashboardSectionName)


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
    high_scale_service = require_high_scale(ctx)
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
    high_scale_service = require_high_scale(ctx)
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
        **build_telemetry_response_envelope(
            debug_queries=all_debug_queries,
            debug_calls=all_debug_calls,
            section_timings=section_timings,
            debug_sqlite=list(get_sqlite_queries()),
        ),
    }


_CLEAN_SIGNED_DECIMAL_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$", re.ASCII)
_CSV_DIRECT_DANGEROUS_LEAD = ("=", "+", "-", "@", "|", "%", "\t", "\r", "\n")
_CSV_FORMULA_TRIGGERS = ("=", "+", "-", "@", "|", "%")
_CSV_CONTROL_AND_ZERO_WIDTH = "".join(chr(c) for c in range(0x20)) + "\x7f\x85\ufeff\u200b\u200c\u200d\u2060"


def _sanitize_csv_cell(val: Any) -> Any:
    """Prefix spreadsheet formula triggers so Excel/Sheets treat them as literal text."""
    if not isinstance(val, str) or not val:
        return val
    if val[0] in _CSV_DIRECT_DANGEROUS_LEAD:
        if _CLEAN_SIGNED_DECIMAL_RE.fullmatch(val):
            return val
        return f"'{val}"
    stripped = val
    while stripped:
        nxt = stripped.lstrip().lstrip(_CSV_CONTROL_AND_ZERO_WIDTH)
        if nxt == stripped:
            break
        stripped = nxt
    if stripped and stripped[0] in _CSV_FORMULA_TRIGGERS:
        return f"'{val}"
    return val


@router.post("/raw/csv")
@query_errors()
def dashboard_raw_csv(
    req: RawRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    csv_limit = min(max(1, int(req.limit)), 50000) if "limit" in req.model_fields_set and req.limit else 50000
    high_scale_service = require_high_scale(ctx)
    df = hs_dashboard.raw_df(high_scale_service, req, start_time, end_time, limit=csv_limit)

    if df.empty:
        return StreamingResponse(iter([b""]), media_type="text/csv")

    df = df.copy()
    if mask_ips_for(ctx.analyst_session):
        from backend.core.share_db import (
            SESSION_ID_KEYS,
            _canon_pii_key,
            is_saved_view_pii_key,
            mask_ip_or_chain,
            scrub_ip_text,
        )

        for col in df.columns:
            col_str = str(col)
            if is_saved_view_pii_key(col_str):
                tail = col_str.strip().lower().rsplit(".", 1)[-1].strip("[]'\"`")
                is_sess = tail in SESSION_ID_KEYS or _canon_pii_key(tail) in SESSION_ID_KEYS
                if is_sess:
                    df[col] = df[col].map(lambda v: ("[redacted]" if v else v) if isinstance(v, str) else v)
                else:
                    df[col] = df[col].map(lambda v: mask_ip_or_chain(v) if isinstance(v, str) and v else v)
            elif df[col].dtype == object or str(df[col].dtype).startswith(("string", "str")):
                df[col] = df[col].map(lambda v: scrub_ip_text(v) if isinstance(v, str) and v else v)

    for col in df.columns:
        if df[col].dtype == object or str(df[col].dtype).startswith(("string", "str")):
            df[col] = df[col].map(_sanitize_csv_cell)
    df.columns = [_sanitize_csv_cell(str(c)) for c in df.columns]

    def csv_chunks() -> Iterator[bytes]:
        chunk_rows = 2000
        first = True
        for start in range(0, len(df), chunk_rows):
            slice_df = df.iloc[start : start + chunk_rows]
            yield slice_df.to_csv(header=first, index=False).encode("utf-8")
            first = False

    raw_source_name = str(high_scale_service.service_id or "service")
    safe_source = re.sub(r"[^A-Za-z0-9_.-]", "_", raw_source_name).strip("._")[:64] or "service"
    filename = f"logs_{safe_source}_{int(time.time())}.csv"
    return StreamingResponse(
        csv_chunks(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/field-values", response_model=FieldValuesResponse)
@query_errors()
def dashboard_field_values(
    req: FieldValuesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = require_high_scale(ctx)
    return hs_dashboard.field_values(high_scale_service, req, start_time, end_time)
