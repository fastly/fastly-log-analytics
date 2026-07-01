"""Dashboard router — aggregates, raw logs, field value picker."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator
from typing import Any

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from backend import config as svcconfig
from backend.core.request_context import RequestContext, build_request_context
from backend.models.dashboard import (
    AggregatesRequest,
    AggregatesResponse,
    BundleResponse,
    DashboardSectionName,
    FieldValuesRequest,
    FieldValuesResponse,
    RawRequest,
)
from backend.models.errors import DEFAULT_ERROR_RESPONSES
from backend.repositories import dashboard as repo
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import make_section_expander, query_errors
from backend.utils.time_window import is_valid_range_token, resolve_window


def _clamp_window(req: AggregatesRequest, ctx: RequestContext) -> tuple[str | None, str | None]:
    """Resolve the scan window once for the dashboard aggregates/bundle paths.

    Keyed path: when ``range_token`` is recognized the SERVER resolves the
    window from (token, anchor) — ignoring FE-supplied absolute start/end so a
    crafted body can't widen the scan — then clamps to the invite ceiling. The
    clamp runs AFTER resolve, so it is the single enforcement point (an analyst
    can't widen past their invite by picking "30d"). Legacy path clamps the
    FE-supplied bounds unchanged. Mirrors routers/origin.py.

    Returning a single pair is load-bearing for /dashboard/bundle: BOTH the
    aggregates branch and the top_bots branch close over the one clamped result,
    so neither sub-query can ever scan unclamped bounds.
    """
    if is_valid_range_token(req.range_token):
        earliest_log_at = svcconfig.get_status(ctx.source["name"]).get("earliest_log_at")
        resolved_start, resolved_end = resolve_window(req.range_token, req.anchor, earliest_log_at=earliest_log_at)
        return ctx.clamp(resolved_start, resolved_end)
    return ctx.clamp(req.start_time, req.end_time)


router = APIRouter(prefix="/api/dashboard", tags=["dashboard"], responses=DEFAULT_ERROR_RESPONSES)

# No coupling — every dashboard section is independent.
_expand_sections = make_section_expander(DashboardSectionName)


def _resolve_aggregate_flags(
    req: AggregatesRequest,
    sections: set[str] | None,
) -> tuple[bool, bool, bool, bool]:
    """Translate the sections selector + existing include_* flags into the
    four gates the repo consumes: (time_series, conn_requests, map_data,
    top_n). When ``sections`` is None the include_* flags pass through
    untouched (None → True downstream) and top_n is always on — preserves
    the pre-selector behavior. When ``sections`` is set it OVERRIDES the
    include_* flags so the selector is the unambiguous canonical surface.

    Coupling:
      core   → time_series=True, conn_requests=True, map_data=True
      topten → top_n=True (the per-field cards), fields filter passthrough
      bots   → no /aggregates effect (handled in /bundle's second branch)
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
    start_time, end_time = _clamp_window(req, ctx)
    sections = _expand_sections(req.sections)
    its, icr, imd, itn = _resolve_aggregate_flags(req, sections)
    return repo.get_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        chart_interval=req.chart_interval,
        chart_metric=req.chart_metric,
        fields_filter=req.fields,
        include_time_series=its,
        include_conn_requests=icr,
        include_map_data=imd,
        include_top_n=itn,
    )


@router.post("/bundle", response_model=BundleResponse)
@query_errors()
async def dashboard_bundle(
    req: AggregatesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    """Composite endpoint returning the two queries the dashboard page
    fires on every mount: /api/dashboard/aggregates + /api/security/top-bots.

    Saves one RTT per cold load — the frontend's useDashboardBundle
    hook fetches this once and seeds the existing
    ``['dashboard', 'aggregates', ...]`` and ``['dashboard',
    'top-bots', ...]`` React Query caches so the dedicated hooks
    return cached data without firing their own POSTs.

    Concurrent execution when a second pool connection is free:
    aggregates runs on ctx.con, top_bots runs on a separately-acquired
    pool connection, both wrapped in ``asyncio.to_thread`` and awaited
    via ``asyncio.gather``. The 2nd-connection acquire uses a short
    ``max_wait`` (200 ms) so that under pool saturation we bail with
    ``_PoolBusy`` and fall back to sequential execution on ctx.con —
    no latency regression on the saturated case. Pool size is 8 per
    service (DUCKDB_POOL_MAX_SIZE), so 4 concurrent bundle requests
    fit before fallback kicks in for the 5th.

    Response is BundleResponse (finding 013) — Pydantic re-engages
    the BaseResponse._strip_debug_when_disabled serializer so internal
    SQL queries and execution timings are redacted when DEBUG_RESPONSES
    is unset. The composite previously emitted an untyped dict that
    bypassed this filter, leaking sub-response debug telemetry into
    every prod response.
    """
    from backend.core.duckdb_pool import _PoolBusy, checkout_connection
    from backend.repositories import security as security_repo
    from backend.repositories._base import SectionTimer

    # Resolve+clamp ONCE here so BOTH sub-queries (_run_aggregates and
    # _run_top_bots) close over the same clamped window — neither can scan
    # unclamped bounds, and the keyed path's invite-ceiling clamp covers both.
    start_time, end_time = _clamp_window(req, ctx)

    sections = _expand_sections(req.sections)
    its, icr, imd, itn = _resolve_aggregate_flags(req, sections)
    # ``sections=None`` preserves the pre-selector contract (both branches
    # fire). Otherwise only run the aggregates branch when ``core`` OR
    # ``topten`` is requested, and only run top_bots when ``bots`` is.
    want_aggregates = sections is None or bool(sections & {"core", "topten"})
    want_bots = sections is None or "bots" in sections

    timer = SectionTimer()
    section_timings = timer.entries

    def _run_aggregates(con) -> Any:
        return repo.get_aggregates(
            con=con,
            src=ctx.source,
            start_time=start_time,
            end_time=end_time,
            filters=req.filters,
            chart_interval=req.chart_interval,
            chart_metric=req.chart_metric,
            fields_filter=req.fields,
            include_time_series=its,
            include_conn_requests=icr,
            include_map_data=imd,
            include_top_n=itn,
        )

    # The dashboard ALWAYS shows the two bot cards (Fastly Bots + NGWAF
    # Verified Bots), independent of which other top-N cards the lazy
    # fields list is hydrating. The prior gate (skip when fields is set
    # and doesn't include _bot_name/_ngwaf_bot_name) was checking the
    # wrong thing — the dashboard sends a lazy fields list that excludes
    # the bot virtual fields, so the gate fired in the common case and
    # seeded the React Query cache with empty bot arrays. The standalone
    # /api/security/top-bots refetch then read the seeded blank from the
    # cache instead of replacing it, leaving both cards visually empty
    # even though the backend had bot rows available.
    def _run_top_bots(con) -> dict[str, Any]:
        return security_repo.get_top_bots(
            con=con,
            src=ctx.source,
            start_time=start_time,
            end_time=end_time,
            filters=req.filters,
        )

    aggregates: Any = None
    top_bots: Any = None

    # Single-branch fast path — when the selector excluded the other
    # branch, skip the 2nd-conn checkout + asyncio.gather hardness
    # entirely. The remaining branch runs synchronously on ctx.con.
    if not (want_aggregates and want_bots):
        if want_aggregates:
            t0 = time.perf_counter()
            aggregates = await asyncio.to_thread(_run_aggregates, ctx.con)
            timer.mark("bundle:aggregates", t0)
        if want_bots:
            t1 = time.perf_counter()
            top_bots = await asyncio.to_thread(_run_top_bots, ctx.con)
            timer.mark("bundle:top_bots", t1)
        single_debug_queries: list = []
        single_debug_calls: list = []
        for sub in (aggregates, top_bots):
            if isinstance(sub, dict):
                single_debug_queries.extend(sub.pop("debug_queries", []) or [])
                single_debug_queries.extend(sub.pop("_debug_queries", []) or [])
                single_debug_calls.extend(sub.pop("debug_calls", []) or [])
                single_debug_calls.extend(sub.pop("_debug_calls", []) or [])
        return {
            "aggregates": aggregates,
            "top_bots": top_bots,
            "section_timings": section_timings,
            "debug_queries": single_debug_queries,
            "debug_calls": single_debug_calls,
        }

    # Two-branch path (preserves the F015-hardened gather + second_cm
    # guard untouched). ``sections=None`` and ``sections=['core','bots']``
    # both land here.
    # Try to acquire a 2nd pooled connection for parallel execution.
    # max_wait=0.2 lets us absorb very brief contention but bails before
    # the wait itself eats the parallel-execution savings.
    second_cm = None
    second_con = None
    parallel = False
    try:
        second_cm = checkout_connection(ctx.source, max_wait=0.2)
        second_con = second_cm.__enter__()
        parallel = True
    except _PoolBusy:
        second_cm = None
        second_con = None
    except Exception:
        if second_cm is not None:
            try:
                second_cm.__exit__(None, None, None)
            except Exception:
                pass
        second_cm = None
        second_con = None

    if parallel:
        # Two-connection concurrent path. Wrap each repo call in
        # ``asyncio.to_thread`` so the synchronous DuckDB execute()
        # calls run in worker threads, then await both via gather.
        # Per-branch timing is captured inside each thread so the
        # section_timings reflect the wall-time each query took (not
        # the gather wall-clock), preserving the prior shape.
        async def _aggregates_branch() -> Any:
            t = time.perf_counter()
            res = await asyncio.to_thread(_run_aggregates, ctx.con)
            timer.mark("bundle:aggregates", t)
            return res

        async def _top_bots_branch() -> dict[str, Any]:
            t = time.perf_counter()
            res = await asyncio.to_thread(_run_top_bots, second_con)
            timer.mark("bundle:top_bots", t)
            return res

        assert second_cm is not None  # narrowed by ``parallel`` flag
        try:
            # return_exceptions=True forces gather() to wait for BOTH
            # threads to finish before returning, even if one raises or
            # the client cancels mid-flight. The prior implementation
            # propagated the exception immediately, letting the finally
            # block return ``second_con`` to the pool while
            # _top_bots_branch's worker thread was still executing
            # against it — and DuckDB connections are not safe for
            # concurrent use. The next checkout would either deadlock on
            # the internal mutex (8 leaked-active connections exhaust
            # DUCKDB_POOL_MAX_SIZE → persistent DoS) or, if the mutex
            # were bypassed, corrupt the DuckDB process memory.
            # (F015, audit run 7ba15352)
            results = await asyncio.gather(
                _aggregates_branch(),
                _top_bots_branch(),
                return_exceptions=True,
            )
            aggregates, top_bots = results
            if isinstance(aggregates, BaseException):
                raise aggregates
            if isinstance(top_bots, BaseException):
                raise top_bots
        finally:
            try:
                second_cm.__exit__(None, None, None)
            except Exception:
                pass
    else:
        t0 = time.perf_counter()
        aggregates = await asyncio.to_thread(_run_aggregates, ctx.con)
        timer.mark("bundle:aggregates", t0)
        t1 = time.perf_counter()
        top_bots = await asyncio.to_thread(_run_top_bots, ctx.con)
        timer.mark("bundle:top_bots", t1)
    # Lift `debug_queries` / `debug_calls` from each sub-response into
    # the top-level BundleResponse so the frontend DebugPanel (which
    # reads response.data._debug_queries — the serialization_alias)
    # sees the queries from both endpoints aggregated. Without this,
    # the panel shows 0 queries / 0.00ms even with DEBUG_RESPONSES on,
    # because the telemetry sits one level deep under the sub-field.
    # We POP rather than copy so the sub-responses end up with empty
    # debug lists — Pydantic's BundleResponse will validate them and
    # then BaseResponse._strip_debug_when_disabled redacts any
    # remaining debug keys when DEBUG_RESPONSES is unset.
    all_debug_queries: list = []
    all_debug_calls: list = []
    for sub in (aggregates, top_bots):
        if isinstance(sub, dict):
            all_debug_queries.extend(sub.pop("debug_queries", []) or [])
            all_debug_queries.extend(sub.pop("_debug_queries", []) or [])
            all_debug_calls.extend(sub.pop("debug_calls", []) or [])
            all_debug_calls.extend(sub.pop("_debug_calls", []) or [])
    return {
        "aggregates": aggregates,
        "top_bots": top_bots,
        "section_timings": section_timings,
        "debug_queries": all_debug_queries,
        "debug_calls": all_debug_calls,
    }


@router.post("/raw/csv")
@query_errors()
def dashboard_raw_csv(
    req: RawRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    df = repo.get_raw_df(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=50000,  # Cap at 50k rows for performance
        columns=req.columns,
    )

    if df.empty:
        return StreamingResponse(iter([b""]), media_type="text/csv")

    # R-2: streaming CSV bypasses the middleware's _strip_analyst_envelope
    # PII pass (text/csv content-type). Apply mask_ips here so analyst
    # exports respect the per-invite policy. The same mask_ip helper the
    # JSON path uses is reused here for shape parity.
    if mask_ips_for(ctx.analyst_session):
        from backend.core.share_db.validation import IP_FAMILY_KEYS, mask_ip

        for col in IP_FAMILY_KEYS:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: mask_ip(v) if isinstance(v, str) else v)

    # Chunked serialization — avoids the StringIO double-buffer (the
    # whole CSV string in memory in addition to the DataFrame). For
    # 50k rows × ~75 cols this cuts peak resident-set ~50% by deleting
    # the per-export `output` allocation; the DataFrame itself still
    # lives across the response but it dies as soon as the generator
    # finishes. DuckDB COPY would be the next tier (zero DataFrame
    # materialization) but bot_metadata enrichment lives in Python so
    # the pushdown isn't free.
    def csv_chunks() -> Iterator[bytes]:
        chunk_rows = 2000
        first = True
        for start in range(0, len(df), chunk_rows):
            slice_df = df.iloc[start : start + chunk_rows]
            yield slice_df.to_csv(header=first, index=False).encode("utf-8")
            first = False

    filename = f"logs_{ctx.source['name']}_{int(time.time())}.csv"
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
    return repo.get_field_values(
        con=ctx.con,
        src=ctx.source,
        field=req.field,
        search=req.search,
        limit=req.limit,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
