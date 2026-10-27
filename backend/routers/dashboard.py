"""Dashboard router — aggregates, raw logs, field value picker."""

from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Path
from fastapi.responses import StreamingResponse

from backend.core.iceberg import execute_with_stale_view_retry
from backend.core.request_context import RequestContext, build_request_context
from backend.cron.jobs.insights_prewarmer import record_insights_request
from backend.deps import ServiceId, get_service_id
from backend.models._base import DEFAULT_ERROR_RESPONSES
from backend.models.analytics import (
    CacheCollapseDetailRequest,
    CacheCollapseDetailResponse,
    InsightsRequest,
    InsightsResponse,
)
from backend.models.common import CmcdDisableBody, CmcdEnableBody, CmcdStatusResponse
from backend.models.dashboard import (
    AggregatesRequest,
    AggregatesResponse,
    BundleResponse,
    DashboardSectionName,
    FieldValuesRequest,
    FieldValuesResponse,
    PresetQuery,
    QueryRequest,
    QueryResponse,
    QuerySchemaResponse,
    RawRequest,
)
from backend.models.performance import (
    AssetsAggregatesResponse,
    AssetsRequest,
    CmcdAggregatesResponse,
    CmcdRequest,
    CmcdSectionName,
    PerformanceAggregatesResponse,
    PerformanceRequest,
    PerformanceSectionName,
    ValueSectionName,
    ValueSummaryRequest,
    ValueSummaryResponse,
)
from backend.repositories import assets as assets_repo
from backend.repositories import cmcd as cmcd_repo
from backend.repositories import dashboard as repo
from backend.repositories import insights as insights_repo
from backend.repositories import performance as perf_repo
from backend.repositories import query as query_repo
from backend.repositories import value as value_repo
from backend.repositories.query import get_cached_presets, set_cached_presets
from backend.repositories.utils.response_cache import cache_result
from backend.utils.auth import mask_ips_for
from backend.utils.remote_access import analyst_clamp_cache_key
from backend.utils.router_utils import (
    make_error,
    make_section_expander,
    query_errors,
    redact_error_text,
    run_cached_repo_endpoint,
)
from backend.utils.router_utils import (
    resolve_fastly_token as _resolve_token,
)
from backend.utils.sse import stream_iac_reconcile
from backend.utils.telemetry import get_sqlite_queries, process_context_scope

logger = logging.getLogger(__name__)

router = APIRouter()
dashboard_router = APIRouter(prefix="/api/dashboard", tags=["dashboard"], responses=DEFAULT_ERROR_RESPONSES)

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


@dashboard_router.post("/aggregates", response_model=AggregatesResponse)
@query_errors()
def dashboard_aggregates(
    req: AggregatesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.dashboard import aggregates as high_scale_aggregates

        return high_scale_aggregates(high_scale_service, req, start_time, end_time)

    sections = _expand_sections(req.sections)
    its, icr, imd, itn = _resolve_aggregate_flags(req, sections)
    return run_cached_repo_endpoint(
        ctx,
        repo_module="backend.repositories.dashboard",
        get_fn=repo.get_aggregates,
        peek_fn=repo.peek_aggregates,
        kwargs={
            "start_time": start_time,
            "end_time": end_time,
            "filters": req.filters,
            "chart_interval": req.chart_interval,
            "chart_metric": req.chart_metric,
            "fields_filter": req.fields,
            "include_time_series": its,
            "include_conn_requests": icr,
            "include_map_data": imd,
            "include_top_n": itn,
        },
    )


@dashboard_router.post("/bundle", response_model=BundleResponse)
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

    timer = SectionTimer()
    section_timings = timer.entries

    # Resolve+clamp ONCE here so BOTH sub-queries (_run_aggregates and
    # _run_top_bots) close over the same clamped window — neither can scan
    # unclamped bounds, and the keyed path's invite-ceiling clamp covers both.
    start_time, end_time = ctx.resolve_window(req)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.dashboard import bundle as high_scale_bundle

        # Run the blocking ClickHouse aggregate queries OFF the asyncio event
        # loop. Called synchronously here (as it was), every per-field
        # ``_aggregate`` + ``_time_series`` query executes ON the loop, freezing
        # /api/health, SSE, and every concurrent request for the whole bundle —
        # the Elevation high-scale 24h "Crunching logs… → Failed to fetch"
        # wedge. Mirrors the non-high-scale branch's off-loop execution below.
        return await asyncio.to_thread(high_scale_bundle, high_scale_service, req, start_time, end_time)

    # Resolve the primary pooled connection OFF the asyncio event loop. The
    # first ``ctx.con`` access performs a BLOCKING pool checkout (DuckLake
    # attach + Iceberg view rebuild + up-to-max_wait lock retry); passed as an
    # argument to ``asyncio.to_thread(_run_aggregates, ctx.con)`` it is
    # evaluated ON the loop BEFORE the worker thread runs, so under
    # cron-vs-dashboard DuckLake contention it freezes /api/health and every
    # other request — cascading one slow dashboard query into a fully
    # "unhealthy" backend (GCE 2026-10-02 py-spy wedge). Pre-warming here in a
    # worker thread caches the connection on the holder so every downstream
    # ``ctx.con`` read is non-blocking and the loop stays responsive.
    #
    # The await MUST be cancellation-guarded. A bare un-shielded
    # ``to_thread`` prewarm cancelled mid-checkout
    # (client disconnect on a slow/503 response) raises CancelledError
    # immediately while the detached worker thread is still inside
    # ``_ConnectionHolder.__enter__``; the request-context dependency's
    # ``finally`` then races ``__exit__`` against the still-executing checkout
    # generator, so the connection is acquired (in_use++) but never released —
    # permanently stranding a pool slot (4 leaks → every request 503s "pool
    # saturated at 4" until restart; Local Std 2026-10-02). Mirror the guarded
    # sub-query pattern below: await the checkout task to completion on
    # cancellation so the holder records the connection for the finally to
    # release.
    t_prewarm = time.perf_counter()
    await ctx.prewarm_con()
    timer.mark("bundle:primary_checkout", t_prewarm)

    sections = _expand_sections(req.sections)
    its, icr, imd, itn = _resolve_aggregate_flags(req, sections)
    # ``sections=None`` preserves the pre-selector contract (both branches
    # fire). Otherwise only run the aggregates branch when ``core`` OR
    # ``topten`` is requested, and only run top_bots when ``bots`` is.
    want_aggregates = sections is None or bool(sections & {"core", "topten"})
    want_bots = sections is None or "bots" in sections

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

    async def _shielded_to_thread(fn: Any, *args: Any) -> Any:
        task = asyncio.create_task(asyncio.to_thread(fn, *args))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            await task
            raise

    # Single-branch fast path — when the selector excluded the other
    # branch, skip the 2nd-conn checkout + asyncio.gather hardness
    # entirely. The remaining branch runs synchronously on ctx.con.
    if not (want_aggregates and want_bots):
        if want_aggregates:
            t0 = time.perf_counter()
            aggregates = await _shielded_to_thread(_run_aggregates, ctx.con)
            timer.mark("bundle:aggregates", t0)
        if want_bots:
            t1 = time.perf_counter()
            top_bots = await _shielded_to_thread(_run_top_bots, ctx.con)
            timer.mark("bundle:top_bots", t1)
        return _assemble_dashboard_bundle_response(aggregates, top_bots, section_timings)

    # Two-branch path (preserves the F015-hardened gather + second_cm
    # guard untouched). ``sections=None`` and ``sections=['core','bots']``
    # both land here.
    # Try to acquire a 2nd pooled connection for parallel execution.
    # reuse_only: hand back an already-idle conn if one is instantly
    # available, else raise _PoolBusy at once and fall back to serial. It
    # must never build fresh — an admitted cold build holds a slot while it
    # stalls in the serialized DuckLake attach, and concurrent second-
    # checkouts then drain the pool and 503 every request (prod 2026-10-01,
    # Local Std; py-spy showed all 8 slots stuck in get_connection). The
    # best-effort parallel win is simply skipped under contention.
    second_cm = None
    second_con = None
    parallel = False

    def _checkout_second():
        cm = checkout_connection(ctx.source, max_wait=0.2, reuse_only=True)
        return cm, cm.__enter__()

    # Shield checkout to prevent connection leaks if client cancels mid-flight
    checkout_result = []

    async def _shielded_checkout():
        res = await asyncio.to_thread(_checkout_second)
        checkout_result.append(res)
        return res

    t_second_checkout = time.perf_counter()
    try:
        await asyncio.shield(_shielded_checkout())
        if checkout_result:
            second_cm, second_con = checkout_result[0]
            parallel = True
    except _PoolBusy:
        second_cm = None
        second_con = None
    except Exception:
        if checkout_result:
            second_cm, _ = checkout_result[0]
        if second_cm is not None:
            try:
                second_cm.__exit__(None, None, None)
            except Exception:
                logger.debug("dashboard_bundle: second_cm.__exit__ failed (ignored)", exc_info=True)
        second_cm = None
        second_con = None
    finally:
        timer.mark("bundle:second_checkout", t_second_checkout)

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

        if second_cm is None:  # ``parallel`` guarantees it
            raise RuntimeError("parallel bundle path is missing its second connection")
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
            gather_task = asyncio.gather(
                _aggregates_branch(),
                _top_bots_branch(),
                return_exceptions=True,
            )
            try:
                results = await asyncio.shield(gather_task)
            except asyncio.CancelledError:
                await gather_task
                raise
            aggregates, top_bots = results
            if isinstance(aggregates, BaseException):
                raise aggregates
            if isinstance(top_bots, BaseException):
                raise top_bots
        finally:
            cm_to_exit = second_cm
            if cm_to_exit is None and checkout_result:
                cm_to_exit, _ = checkout_result[0]
            if cm_to_exit is not None:
                try:
                    cm_to_exit.__exit__(None, None, None)
                except Exception:
                    logger.debug("dashboard_bundle: cm_to_exit.__exit__ failed (ignored)", exc_info=True)
    else:
        t0 = time.perf_counter()
        aggregates = await _shielded_to_thread(_run_aggregates, ctx.con)
        timer.mark("bundle:aggregates", t0)
        t1 = time.perf_counter()
        top_bots = await _shielded_to_thread(_run_top_bots, ctx.con)
        timer.mark("bundle:top_bots", t1)
    return _assemble_dashboard_bundle_response(aggregates, top_bots, section_timings)


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


@dashboard_router.post("/raw/csv")
@query_errors()
def dashboard_raw_csv(
    req: RawRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    csv_limit = min(max(1, int(req.limit)), 50000) if "limit" in req.model_fields_set and req.limit else 50000
    if ctx.high_scale is not None:
        from backend.high_scale import dashboard as hs_dashboard

        df = hs_dashboard.raw_df(ctx.high_scale, req, start_time, end_time, limit=csv_limit)
    else:
        df = repo.get_raw_df(
            con=ctx.con,
            src=ctx.source,
            start_time=start_time,
            end_time=end_time,
            filters=req.filters,
            limit=csv_limit,
            columns=req.columns,
        )

    if df.empty:
        return StreamingResponse(iter([b""]), media_type="text/csv")

    # R-2: streaming CSV bypasses the middleware's _strip_analyst_envelope
    # PII pass (text/csv content-type). Apply mask_ips here so analyst
    # exports respect the per-invite policy. The same mask_ip helper the
    # JSON path uses is reused here for shape parity. CSV projects the real
    # stored column names (raw/csv can't alias them the way /api/query can),
    # so masking by source-column name is complete here.
    if mask_ips_for(ctx.analyst_session):
        from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS, mask_ip

        for col in IP_FAMILY_KEYS:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: mask_ip(v) if isinstance(v, str) else v)
        # Phase-4 Track C: session identifiers (e.g. cookie_session) are
        # per-client PII with no reliable value shape — redact wholesale by
        # column (empty / non-string cells left untouched), mirroring
        # apply_pii_policy on the JSON surfaces.
        for col in SESSION_ID_KEYS:
            if col in df.columns:
                df[col] = df[col].apply(lambda v: ("[redacted]" if v else v) if isinstance(v, str) else v)

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

    source_name = ctx.high_scale.service_id if ctx.high_scale is not None else ctx.source["name"]
    filename = f"logs_{source_name}_{int(time.time())}.csv"
    return StreamingResponse(
        csv_chunks(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@dashboard_router.post("/field-values", response_model=FieldValuesResponse)
@query_errors()
def dashboard_field_values(
    req: FieldValuesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    if ctx.high_scale is not None:
        from backend.high_scale import dashboard as hs_dashboard

        return hs_dashboard.field_values(ctx.high_scale, req, start_time, end_time)
    _is_real_repo = getattr(repo.get_field_values, "__module__", "") == "backend.repositories.dashboard"
    return repo.get_field_values(
        con=ctx.con_or_factory(force_eager=not _is_real_repo),
        src=ctx.source,
        field=req.field,
        search=req.search,
        limit=req.limit,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )


# ── Insights endpoints (folded from backend.routers.insights) ───────────────

insights_router = APIRouter(prefix="/api", tags=["insights"], responses=DEFAULT_ERROR_RESPONSES)

# Per-service semaphore (capacity=1) serialises concurrent insights scans.
# A second request for the same service parks here; when the first finishes
# it writes the cache, and the second gets a cache hit → no duplicate scan.
# Dict access is guarded by a lock so concurrent service-registrations are safe.
_insights_sems: dict[str, asyncio.Semaphore] = {}
_insights_sems_lock = threading.Lock()


def _get_insights_sem(service_id: str) -> asyncio.Semaphore:
    with _insights_sems_lock:
        if service_id not in _insights_sems:
            _insights_sems[service_id] = asyncio.Semaphore(1)
        return _insights_sems[service_id]


def _analyst_lookback_clamp(
    ctx: RequestContext, baseline_hours: float, window_size_hrs: float
) -> tuple[str | None, str | None, bool, str | None]:
    """Resolve the analyst clamp window + mask_ips + stable cache key for the
    insights endpoints. Admin (no analyst session) → ``(None, None, False,
    None)`` so the scan runs the full range and hits the shared prewarmer cache.

    M2: clamp the scanned range [now-(baseline+window), now] to the analyst's
    allowed window via ``ctx.clamp``. The model already bounds the two windows
    so the unclamped lookback is itself capped (≤ ~97d).
    M3: IP-keyed insights mask the client IP they surface in the label /
    investigate_url when the invite carries mask_ips.

    The 4th value is the STABLE insights cache key fragment
    (``analyst_clamp_cache_key`` — keyed on the invite's window params, NOT the
    rolling resolved bounds) so repeated analyst requests reuse one cache entry
    and the prewarmer can warm the exact same key. ``get_cache_collapse_detail``
    is uncached and discards it.
    """
    s = ctx.analyst_session
    if s is None:
        return None, None, False, None
    now = datetime.now(UTC)
    earliest = now - timedelta(hours=baseline_hours + window_size_hrs)
    clamp_start, clamp_end = ctx.clamp(earliest.isoformat(), now.isoformat())
    cache_key = analyst_clamp_cache_key(
        getattr(s, "query_start_time", None),
        getattr(s, "query_end_time", None),
        getattr(s, "query_window_hours", None),
    )
    return clamp_start, clamp_end, mask_ips_for(s), cache_key


@insights_router.post("/insights", response_model=InsightsResponse)
@query_errors()
async def insights_endpoint(
    req: InsightsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    record_insights_request(ctx.service_id)
    clamp_start, clamp_end, mask_ips, clamp_cache_key = _analyst_lookback_clamp(
        ctx, req.baseline_hours, req.window_size_hrs
    )
    high_scale_service = ctx.high_scale
    force_refresh = req.force_refresh if ctx.analyst_session is None else False
    sem = _get_insights_sem(ctx.service_id)
    if high_scale_service is not None:
        from backend.high_scale.insights import insights_endpoint as hs_insights

        async with sem:
            return await asyncio.to_thread(
                hs_insights,
                high_scale_service,
                req,
                clamp_start,
                clamp_end,
                mask_ips,
                clamp_cache_key=clamp_cache_key,
                force_refresh=force_refresh,
            )

    cached = insights_repo.check_insights_cache(
        src=ctx.source,
        window_hours=req.window_size_hrs,
        baseline_hours=req.baseline_hours,
        clamp_start=clamp_start,
        clamp_end=clamp_end,
        mask_ips=mask_ips,
        clamp_cache_key=clamp_cache_key,
        force_refresh=force_refresh,
        filters=req.filters,
    )
    if cached is not None:
        return cached

    await ctx.prewarm_con()
    async with sem:

        def _run_insights(con):
            return insights_repo.get_insights(
                con=con,
                src=ctx.source,
                window_hours=req.window_size_hrs,
                baseline_hours=req.baseline_hours,
                service_id=ctx.service_id,
                clamp_start=clamp_start,
                clamp_end=clamp_end,
                mask_ips=mask_ips,
                clamp_cache_key=clamp_cache_key,
                force_refresh=force_refresh,
                filters=req.filters,
            )

        return await asyncio.to_thread(execute_with_stale_view_retry, ctx.con, ctx.source, _run_insights)


@insights_router.post("/insights/cache-collapse-detail", response_model=CacheCollapseDetailResponse)
@query_errors()
def cache_collapse_detail_endpoint(
    req: CacheCollapseDetailRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    record_insights_request(ctx.service_id)
    clamp_start, clamp_end, mask_ips, _ = _analyst_lookback_clamp(ctx, req.baseline_hours, req.window_size_hrs)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.insights import cache_collapse_detail_endpoint as hs_detail

        return hs_detail(high_scale_service, req, clamp_start, clamp_end, mask_ips)

    def _run_detail(con):
        return insights_repo.get_cache_collapse_detail(
            con=con,
            src=ctx.source,
            url=req.url,
            window_hours=req.window_size_hrs,
            baseline_hours=req.baseline_hours,
            clamp_start=clamp_start,
            clamp_end=clamp_end,
            mask_ips=mask_ips,
        )

    return execute_with_stale_view_retry(ctx.con, ctx.source, _run_detail)


# ── Query endpoints (folded from backend.routers.query) ─────────────────────

query_router = APIRouter(prefix="/api", tags=["query"], responses=DEFAULT_ERROR_RESPONSES)


def _redact_paths(msg: str) -> str:
    return redact_error_text(msg)


def _console_masked(analyst_session: object) -> bool:
    """Masked identity unless the invite's policy EXPLICITLY sets ``mask_ips`` to false."""
    policy = getattr(analyst_session, "pii_policy", None)
    return not (isinstance(policy, dict) and policy.get("mask_ips") is False)


@query_router.post("/query", response_model=QueryResponse, response_model_exclude_unset=True)
def query_endpoint(
    req: QueryRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    sql = req.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail=make_error("empty_sql", "No SQL provided"))

    analyst_session = ctx.analyst_session
    audit_session_id = getattr(analyst_session, "session_id", "admin") if analyst_session else "admin"

    start_time, end_time = ctx.clamp(None, None)
    time_filter = (start_time, end_time) if start_time and end_time else None
    mask_ips = mask_ips_for(analyst_session)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        query_client = None
        query_tables = None
        if analyst_session is not None:
            from backend.high_scale.console_access import get_console_clients

            try:
                console = get_console_clients().client_for(
                    high_scale_service.service_id, masked=_console_masked(analyst_session)
                )
            except Exception:
                logger.warning(
                    "analyst console identity unavailable for %s", high_scale_service.service_id, exc_info=True
                )
                console = None
            if console is None:
                raise HTTPException(
                    status_code=403,
                    detail=make_error(
                        "console_unavailable_for_analysts",
                        "The SQL console is not available to analysts on this high-scale deployment.",
                    ),
                )
            query_client, query_tables = console.client, console.tables
        from backend.high_scale.query import query_endpoint as hs_query

        try:
            return hs_query(
                high_scale_service,
                req,
                start_time,
                end_time,
                mask_ips,
                session_id=audit_session_id,
                client=query_client,
                tables=query_tables,
            )
        except PermissionError as e:
            raise HTTPException(status_code=403, detail=make_error("sql_not_permitted", _redact_paths(str(e))))
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=make_error("query_failed", _redact_paths(str(e))),
            )

    from backend.core.iceberg import execute_with_stale_view_retry as _exec_stale_retry

    def _run_with_compaction_retry(con, src: dict, fn, *, table_name: str):
        for attempt in (1, 2):
            try:
                return _exec_stale_retry(con, src, fn, table_name=table_name)
            except PermissionError as e:
                raise HTTPException(status_code=403, detail=make_error("sql_not_permitted", _redact_paths(str(e))))
            except Exception as e:
                msg = str(e)
                if attempt == 1 and "Cannot open file" in msg:
                    continue
                raise HTTPException(
                    status_code=400,
                    detail=make_error("query_failed", _redact_paths(msg)),
                )

    if req.dataset != "logs":
        from backend.core.duckdb import rum_source_for
        from backend.deps import _ConnectionHolder

        rum_source = rum_source_for(ctx.source)
        holder = _ConnectionHolder(rum_source, read_only=True)
        try:
            with holder as con:

                def _run_rum(c):
                    return query_repo.execute_query(
                        con=c,
                        src=rum_source,
                        sql=sql,
                        max_rows=req.max_rows,
                        want_explain=req.explain,
                        session_id=audit_session_id,
                        service_id=ctx.service_id,
                        time_filter=time_filter,
                        mask_ips=mask_ips,
                        dataset=req.dataset,
                    )

                return _run_with_compaction_retry(con, rum_source, _run_rum, table_name=req.dataset)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=make_error("query_failed", _redact_paths(str(e))),
            )
    else:

        def _run(con):
            return query_repo.execute_query(
                con=con,
                src=ctx.source,
                sql=sql,
                max_rows=req.max_rows,
                want_explain=req.explain,
                session_id=audit_session_id,
                service_id=ctx.service_id,
                time_filter=time_filter,
                mask_ips=mask_ips,
                dataset="logs",
            )

        return _run_with_compaction_retry(ctx.con, ctx.source, _run, table_name="logs")


_FALLBACK_SCHEMA_COLUMNS: list[dict[str, str]] = [
    {"column": "timestamp", "type": "TIMESTAMPTZ"},
    {"column": "ip", "type": "VARCHAR"},
    {"column": "url", "type": "VARCHAR"},
    {"column": "host", "type": "VARCHAR"},
    {"column": "method", "type": "VARCHAR"},
    {"column": "status", "type": "INTEGER"},
    {"column": "cache", "type": "VARCHAR"},
    {"column": "country", "type": "VARCHAR"},
    {"column": "pop", "type": "VARCHAR"},
    {"column": "asn", "type": "INTEGER"},
    {"column": "ua", "type": "VARCHAR"},
]


@query_router.get("/query/schema/{service_id}", response_model=QuerySchemaResponse)
def query_schema_endpoint(
    service_id: ServiceId,
    ctx: RequestContext = Depends(build_request_context),
):
    """Reflect schema columns and types for the active service."""
    del service_id
    from backend import config as svcconfig
    from backend.core import duckdb as _db
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    name = ctx.source["name"]
    status = svcconfig.get_status(name) or {}
    schema = status.get("schema")

    high_scale_service = get_high_scale_service_registry().resolve(ctx.service_id)
    if not schema and high_scale_service is None:
        try:
            schema = _db.get_schema(ctx.con, ctx.source)
        except Exception as exc:
            logger.debug("[query_schema] live schema fetch failed for %s: %s", name, exc)
            schema = []

    if not schema:
        schema = list(_FALLBACK_SCHEMA_COLUMNS)

    if mask_ips_for(ctx.analyst_session):
        schema = [col for col in schema if col.get("column") != "cookie_session"]

    return {
        "source": name,
        "schema": schema,
        "debug_queries": [],
        "debug_calls": [],
    }


@query_router.get("/presets", response_model=list[PresetQuery], response_model_exclude_unset=True)
def presets_endpoint(service_id: str | None = Depends(get_service_id)):
    from backend.core import duckdb as _db

    if not service_id:
        return []

    src = _db.get_source_for_service(service_id)
    if not src:
        return []

    format_hash = (src.get("log_fields") or {}).get("format_hash")
    cached = get_cached_presets(service_id, format_hash)
    if cached is not None:
        return cached

    result = query_repo.get_presets(src=src)
    set_cached_presets(service_id, format_hash, result)
    return result


# ---------------------------------------------------------------------------
# Performance, Assets & Shield, Fastly Value, and CMCD streaming QoE router
# (folded from backend.routers.performance)
# ---------------------------------------------------------------------------

performance_router = APIRouter(prefix="/api", responses=DEFAULT_ERROR_RESPONSES)

_TOP_N_PAIR: frozenset[str] = frozenset({"top_urls", "top_asns"})
_WATERFALL_SCATTER_PAIR: frozenset[str] = frozenset({"waterfall", "scatter"})

_expand_performance_sections = make_section_expander(
    PerformanceSectionName, union_groups=(_TOP_N_PAIR, _WATERFALL_SCATTER_PAIR)
)
_expand_cmcd_sections = make_section_expander(CmcdSectionName)
_expand_value_sections = make_section_expander(ValueSectionName)


@performance_router.post("/performance/aggregates", tags=["performance"], response_model=PerformanceAggregatesResponse)
@query_errors()
def performance_aggregates(
    req: PerformanceRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)

    high_scale_service = ctx.high_scale
    sections = _expand_performance_sections(req.sections)
    if high_scale_service is not None:
        from backend.high_scale.performance import performance_aggregates as hs_performance

        return hs_performance(high_scale_service, req, start_time, end_time, sections=sections)
    return run_cached_repo_endpoint(
        ctx,
        repo_module="backend.repositories.performance",
        get_fn=perf_repo.get_performance_aggregates,
        peek_fn=perf_repo.peek_performance_aggregates,
        kwargs={
            "start_time": start_time,
            "end_time": end_time,
            "filters": req.filters,
            "sort_by": req.sort_by,
            "sections": sections,
        },
        wrap=lambda r: PerformanceAggregatesResponse.with_telemetry(**r),
    )


def _assets_aggregates_impl(
    req: AssetsRequest,
    ctx: RequestContext,
) -> AssetsAggregatesResponse:
    with process_context_scope("assets:aggregates"):
        start_time, end_time = ctx.resolve_window(req)
        high_scale_service = ctx.high_scale
        if high_scale_service is not None:
            from backend.high_scale.assets import assets_aggregates as hs_assets_aggregates

            return hs_assets_aggregates(high_scale_service, req, start_time, end_time)

        cached = assets_repo.check_assets_cache(ctx.source, start_time, end_time, req.filters)
        if cached is not None:
            return AssetsAggregatesResponse.with_telemetry(**cached)

        def _run(con):
            return assets_repo.get_assets_aggregates(
                con=con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
            )

        res = execute_with_stale_view_retry(ctx.con, ctx.source, _run)
        return AssetsAggregatesResponse.with_telemetry(**res)


@performance_router.post("/assets/aggregates", tags=["assets"], response_model=AssetsAggregatesResponse)
@query_errors()
def assets_aggregates(
    req: AssetsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    return _assets_aggregates_impl(req, ctx)


@performance_router.get("/assets/aggregates", tags=["assets"], response_model=AssetsAggregatesResponse)
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


@performance_router.post("/cmcd/aggregates", tags=["cmcd"], response_model=CmcdAggregatesResponse)
@query_errors()
def cmcd_aggregates(
    req: CmcdRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)

    sections = _expand_cmcd_sections(req.sections)
    mask_ips = mask_ips_for(ctx.analyst_session)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd

        res = hs_cmcd(high_scale_service, req, start_time, end_time, sections, mask_ips)
        return CmcdAggregatesResponse.with_telemetry(**res)

    t_entered = ctx.telemetry.elapsed_ms()
    acquire_ms = 0.0

    def _acquire_con():
        nonlocal acquire_ms
        t0 = ctx.telemetry.elapsed_ms()
        c = ctx.con
        acquire_ms = ctx.telemetry.elapsed_ms() - t0
        return c

    _is_real_repo = getattr(cmcd_repo.get_cmcd_aggregates, "__module__", "") == "backend.repositories.cmcd"
    con_arg = _acquire_con() if (ctx.has_eager_con or not _is_real_repo) else _acquire_con

    res = cmcd_repo.get_cmcd_aggregates(
        con=con_arg,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_seconds=req.bucket_seconds,
        top_n=req.top_n,
        sections=sections,
        mask_ips=mask_ips,
    )
    res["section_timings"] = [
        {"section": "request_setup", "time_ms": round(t_entered, 2)},
        {"section": "connection_acquire", "time_ms": round(acquire_ms, 2)},
        *res.get("section_timings", []),
    ]
    return CmcdAggregatesResponse.with_telemetry(**res)


@performance_router.post("/value/summary", tags=["value"], response_model=ValueSummaryResponse)
@query_errors()
def value_summary(
    req: ValueSummaryRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time: str | None
    end_time: str | None
    start_time, end_time = ctx.resolve_window(req)

    sections = _expand_value_sections(req.sections)

    res = value_repo.get_summary(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        chart_interval=req.chart_interval,
        sections=sections,
        service_id=ctx.source.get("service_id"),
    )
    return ValueSummaryResponse.with_telemetry(**res)


@performance_router.get(
    "/services/{service_id}/cmcd/status",
    tags=["cmcd-admin"],
    response_model=CmcdStatusResponse,
    response_model_exclude_unset=True,
)
def cmcd_status(
    service_id: str = Path(..., description="Logging service ID"),
) -> dict:
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    cmcd = cfg.get("cmcd")
    if not cmcd or not cmcd.get("enabled"):
        return {"enabled": False}
    return {
        "enabled": True,
        "mode": cmcd.get("mode", "query_string"),
        "version": cmcd.get("version", 1),
        "enabled_at": cmcd.get("enabled_at"),
    }


@performance_router.post("/services/{service_id}/cmcd/enable", tags=["cmcd-admin"])
def cmcd_enable(
    service_id: str = Path(..., description="Logging service ID"),
    body: CmcdEnableBody | None = None,
):
    token = body.token if body else ""
    mode = body.mode if body else "query_string"
    version = body.version if body else 1
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required (pass in JSON body or set in service config)"},
        )

    import datetime as _dt

    from backend import config as svcconfig

    svcconfig.mutate_config(
        service_id,
        lambda c: c.__setitem__(
            "cmcd",
            {
                "enabled": True,
                "mode": mode,
                "version": version,
                "enabled_at": _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds"),
            },
        ),
    )

    def _done_event() -> dict:
        cfg = svcconfig.load_config(service_id) or {}
        return {
            "type": "done",
            "message": f"CMCD v{version} collection enabled.",
            "cmcd": cfg.get("cmcd", {}),
        }

    return stream_iac_reconcile(
        service_id,
        resolved_token,
        start_message=f"Enabling CMCD v{version} collection for {service_id}...",
        done_event=_done_event,
        log_label="cmcd_enable",
        logger_obj=logger,
    )


@performance_router.post("/services/{service_id}/cmcd/disable", tags=["cmcd-admin"])
def cmcd_disable(
    service_id: str = Path(..., description="Logging service ID"),
    body: CmcdDisableBody | None = None,
):
    token = body.token if body else ""
    resolved_token = _resolve_token(service_id, token)
    if not resolved_token:
        raise HTTPException(
            status_code=400,
            detail={"error": "Fastly API token required"},
        )

    from backend import config as svcconfig

    def _disable(c: dict) -> None:
        if "cmcd" in c:
            c["cmcd"]["enabled"] = False

    svcconfig.mutate_config(service_id, _disable)

    return stream_iac_reconcile(
        service_id,
        resolved_token,
        start_message=f"Disabling CMCD collection for {service_id}...",
        done_event={"type": "done", "message": "CMCD collection disabled."},
        log_label="cmcd_disable",
        logger_obj=logger,
    )


router.include_router(dashboard_router)
router.include_router(query_router)
router.include_router(performance_router)


# ── RUM endpoints (folded from backend.routers.rum) ─────────────────────────

import csv
import datetime as _dt_mod
import io
import json
import os
from types import SimpleNamespace
from typing import Annotated

from fastapi import Body, Query, Request, Response
from sse_starlette.sse import EventSourceResponse

from backend import config as svcconfig
from backend.core.duckdb import rum_source_for
from backend.core.faro_versions import DEFAULT_FARO_VERSION, fetch_available_faro_versions, get_faro_versions_cached
from backend.deps import _ConnectionHolder, require_admin, require_service_access
from backend.models.analytics import (
    OriginAggregatesRequest,
    OriginAggregatesResponse,
    OriginDrilldownRequest,
    OriginDrilldownResponse,
    OriginIpHealthRequest,
    OriginIpHealthResponse,
    OriginPathBreakdownResponse,
    OriginPopLatencyRequest,
    OriginPopLatencyResponse,
    OriginRequest,
    OriginShieldingAnalysisRequest,
    OriginShieldingAnalysisResponse,
    OriginSlowUrlsRequest,
    OriginSlowUrlsResponse,
    OriginStatusCodesResponse,
    OriginSummaryResponse,
    OriginTimeseriesRequest,
    OriginTimeseriesResponse,
)
from backend.models.analytics import (
    SectionName as OriginSectionName,
)
from backend.models.dashboard import (
    SecurityAggregatesRequest,
    SecurityAggregatesResponse,
    SecuritySectionName,
    ThreatIntelItem,
    ThreatIntelListResponse,
)
from backend.models.network import (
    NetworkBundleRequest,
    NetworkBundleResponse,
    NetworkHealthRequest,
    NetworkHealthResponse,
    NetworkQualityRequest,
    NetworkQualityResponse,
    NetworkSectionName,
    PopHealthItem,
    PopHealthListResponse,
    TransportMetrics,
)
from backend.models.provision import (
    RumDisableRequest,
    RumEnableRequest,
    RumSettingsUpdateRequest,
    RumUpgradeRequest,
    RumVersionsResponse,
)
from backend.models.rum import (
    RumAnalyticsResponse,
    RumBeaconHealthResponse,
    RumLiveEventItem,
    RumSettingsUpdateResponse,
    RumStatusResponse,
)
from backend.provision.orchestrator import run_with_events
from backend.provision.rum_orchestrator_v2 import (
    disable_rum,
    enable_rum,
    legacy_rum_vcl_fingerprint,
    rum_vcl_fingerprint,
    upgrade_faro_version,
)
from backend.repositories import network as network_repo
from backend.repositories import origin as origin_repo
from backend.repositories import origin as rum_repo
from backend.repositories import security as security_repo
from backend.repositories._base import SectionTimer, _safe_table
from backend.repositories.utils.response_cache import bucket_time_to_minute, digest_cache_key
from backend.utils.bounded_cache import BoundedTTLCache, CacheRegistry
from backend.utils.date_utils import iso_z, parse_iso_utc
from backend.utils.router_utils import (
    correlation_id,
    load_service_config,
    raise_internal,
    require_fastly_token,
    stored_fastly_token,
)
from backend.utils.sse import sse_response
from backend.utils.time_window import (
    invite_clamp_fingerprint,
    is_valid_range_token,
    quantize_anchor,
)

_RUM_SETTING_KEYS = ("capture_vitals", "capture_performance", "capture_errors", "capture_events", "custom_condition")

# Short/recent RUM windows (<= this span) are served directly from the
# already-attached read connection via a raw client_vitals/client_errors scan,
# never a synchronous in-request rollup recompute.
_RUM_RAW_WINDOW_MAX_S = int(os.getenv("RUM_RAW_WINDOW_MAX_S", str(6 * 60 * 60)))

_RUM_FILTERS_MESSAGE = (
    "filters must be a JSON object over browser, os, device, country, pathname, city, region, pop or tls"
)

rum_router = APIRouter(prefix="/api/services", tags=["rum"], responses=DEFAULT_ERROR_RESPONSES)

# APIRouter without a prefix for serving dynamic JS assets and ingesting beacons
asset_router = APIRouter(tags=["rum-assets"], responses=DEFAULT_ERROR_RESPONSES)

_FARO_BUNDLE_CACHE: dict[str, bytes] = {}
CacheRegistry.register("rum._FARO_BUNDLE_CACHE", _FARO_BUNDLE_CACHE)


@asset_router.api_route(
    "/rum-beacon",
    methods=["GET", "POST", "HEAD", "OPTIONS"],
    include_in_schema=False,
)
async def receive_rum_beacon_fallback() -> Response:
    """No-op 204 endpoint when `/rum-beacon` reaches the origin directly (local dev / direct tunnel)."""
    return Response(status_code=204)


def _resolve_rum_js_service_id(request: Request, service_id: str | None) -> str | None:
    """Resolve active service ID from query param, header, cookie, or default configured service."""
    sid = service_id
    if not sid:
        sid = request.headers.get("x-service-id")
    if not sid:
        sid = request.cookies.get("active_service_id")

    if sid and not svcconfig.load_config(sid):
        sid = None

    if not sid:
        sid = svcconfig.get_active_service_id(fallback_to_first=True)
    return sid


@asset_router.get("/js/rum.js")
async def get_rum_tracker(request: Request, service_id: str | None = None):
    def _build_tracker() -> tuple[str | None, str | None]:
        sid = _resolve_rum_js_service_id(request, service_id)
        if not sid:
            return None, None
        from backend.provision.rum_assets import generate_rum_tracker_js

        return sid, generate_rum_tracker_js(sid)

    try:
        sid, js_code = await asyncio.to_thread(_build_tracker)
    except Exception as e:
        logger.error("Failed to generate dynamic RUM tracker JS: %s", e, exc_info=True)
        return Response(
            content="/* Error generating tracker JS */",
            media_type="application/javascript",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )

    if not sid or js_code is None:
        return Response(
            content="/* RUM tracker disabled (no active services) */",
            media_type="application/javascript",
            headers={"Cache-Control": "private, no-cache, must-revalidate"},
        )

    return Response(
        content=js_code,
        media_type="application/javascript",
        headers={"Cache-Control": "private, no-cache, must-revalidate"},
    )


@asset_router.get("/js/faro-sdk.js")
async def get_faro_sdk(request: Request, service_id: str | None = None):
    sid = _resolve_rum_js_service_id(request, service_id)

    version = DEFAULT_FARO_VERSION
    if sid:
        cfg = svcconfig.load_config(sid)
        if cfg and "rum" in cfg:
            version = cfg["rum"].get("faro_version") or DEFAULT_FARO_VERSION

    if version in _FARO_BUNDLE_CACHE:
        bundle_bytes = _FARO_BUNDLE_CACHE[version]
    else:
        from backend.core.faro_versions import fetch_faro_bundle

        try:
            bundle_bytes = await fetch_faro_bundle(version)
            _FARO_BUNDLE_CACHE[version] = bundle_bytes
        except Exception as e:
            logger.error(f"Failed to fetch Faro bundle for version {version}: {e}")
            if _FARO_BUNDLE_CACHE:
                fallback_ver = list(_FARO_BUNDLE_CACHE.keys())[0]
                bundle_bytes = _FARO_BUNDLE_CACHE[fallback_ver]
            else:
                raise_internal(
                    logger,
                    e,
                    code="faro_sdk_fetch_failed",
                    status=502,
                    message="Failed to fetch Faro SDK",
                    request=request,
                )

    return Response(
        content=bundle_bytes,
        media_type="application/javascript",
        headers={"Cache-Control": "public, max-age=86400, stale-while-revalidate=3600"},
    )


def _reload_rum_scheduler_and_cfg(service_id: str, action: str) -> dict[str, Any]:
    """Reload the scheduler after RUM enable/disable and return the updated service config."""
    try:
        from backend.cron.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception as se:
        logger.error(f"[rum] Failed to reload scheduler after {action}: {se}")
    return svcconfig.load_config(service_id) or {}


def _normalize_sec(val: float | int | None) -> float | None:
    """Convert a metric value to seconds rounded to 2 decimal places when > 20ms."""
    if val is None:
        return None
    return round(val / 1000.0, 2) if val > 20 else round(val, 2)


@rum_router.post("/{service_id}/rum/enable", dependencies=[Depends(require_admin)])
async def enable_rum_handler(
    service_id: str = Path(...),
    body: RumEnableRequest | None = None,
) -> EventSourceResponse:
    """Enable RUM for a service (admin-only, SSE stream)."""
    body = body or RumEnableRequest()
    token = body.token

    cfg = load_service_config(service_id)

    stored_token = stored_fastly_token(cfg)
    if not token:
        token = stored_token

    if not token:
        raise HTTPException(status_code=400, detail={"error": "Fastly API token is required to enable RUM."})

    if token != stored_token:
        cfg["fastly_api_key"] = token
        svcconfig.mutate_config(service_id, lambda c: c.__setitem__("fastly_api_key", token))

    latest_faro_version = None
    try:
        available_versions = await fetch_available_faro_versions()
        if available_versions:
            latest_faro_version = available_versions[0]
    except Exception as e:
        logger.warning(f"Could not fetch latest Faro version dynamically, falling back to DEFAULT_FARO_VERSION: {e}")

    def stream():
        yield json.dumps({"type": "status", "message": f"Enabling Real User Monitoring (RUM) for {service_id}..."})
        try:
            for event in run_with_events(
                enable_rum,
                service_id,
                token,
                activate=body.activate,
                faro_version=latest_faro_version,
                raise_on_error=True,
            ):
                yield json.dumps(event)

            updated_cfg = _reload_rum_scheduler_and_cfg(service_id, "enable")
            yield json.dumps(
                {
                    "type": "done",
                    "message": "Real User Monitoring (RUM) enabled successfully!"
                    if body.activate
                    else "Real User Monitoring (RUM) draft configuration compiled and validated successfully!",
                    "rum": {
                        "enabled": svcconfig.is_rum_enabled(updated_cfg),
                        "enabled_at": updated_cfg.get("rum_enabled_at", ""),
                    },
                }
            )
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)})

    return sse_response(stream())


@rum_router.post("/{service_id}/rum/disable", dependencies=[Depends(require_admin)])
async def disable_rum_handler(
    service_id: str = Path(...),
    body: RumDisableRequest | None = None,
) -> EventSourceResponse:
    """Disable RUM for a service (admin-only, SSE stream)."""
    body = body or RumDisableRequest()
    token = require_fastly_token(service_id, body.token)

    def stream():
        yield json.dumps({"type": "status", "message": f"Disabling Real User Monitoring (RUM) for {service_id}..."})
        try:
            for event in run_with_events(
                disable_rum,
                service_id,
                token,
                remove_cloud_files=body.remove_cloud_files,
                remove_bucket=body.remove_bucket,
                activate=body.activate,
                raise_on_error=True,
            ):
                yield json.dumps(event)

            updated_cfg = _reload_rum_scheduler_and_cfg(service_id, "disable")
            yield json.dumps(
                {
                    "type": "done",
                    "message": "Real User Monitoring (RUM) disabled successfully!"
                    if body.activate
                    else "Real User Monitoring (RUM) draft configuration compiled and validated successfully!",
                    "rum": {
                        "enabled": svcconfig.is_rum_enabled(updated_cfg),
                    },
                }
            )
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)})

    return sse_response(stream())


def _require_rum_status_access(
    request: Request,
    service_id: str | None = Depends(get_service_id),
) -> str | None:
    analyst_session = getattr(getattr(request, "state", None), "analyst_session", None)
    if analyst_session is None:
        return service_id
    raw_service_ids = getattr(analyst_session, "service_ids", None)
    if raw_service_ids is None or isinstance(raw_service_ids, (list, tuple, set, frozenset)):
        if hasattr(request, "path_params"):
            return require_service_access(request, service_id)
        allowed = set(raw_service_ids or [])
        if service_id not in allowed:
            raise HTTPException(
                status_code=403,
                detail={"error": "service_not_authorized", "service": service_id},
            )
    return service_id


def _rum_status_sync(request: Request, service_id: str) -> dict[str, Any]:
    _require_rum_status_access(request, service_id)
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}

    enabled = svcconfig.is_rum_enabled(cfg)
    enabled_at = cfg.get("rum_enabled_at") or rum_cfg.get("enabled_at")

    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is not None:
        return {"enabled": enabled, "enabled_at": enabled_at}

    deployed_sha = cfg.get("rum_vcl_sha") or rum_cfg.get("deployed_vcl_sha")
    current_sha = rum_vcl_fingerprint(service_id)

    if enabled and not deployed_sha:
        deployed_sha = current_sha

    vcl_drift = deployed_sha != current_sha if enabled else False

    if enabled and vcl_drift and deployed_sha == legacy_rum_vcl_fingerprint(service_id):
        vcl_drift = False
        deployed_sha = current_sha
        cfg["rum_vcl_sha"] = current_sha
        svcconfig.mutate_config(service_id, lambda c: c.__setitem__("rum_vcl_sha", current_sha))

    return {
        "enabled": enabled,
        "enabled_at": enabled_at,
        "deployed_vcl_sha": deployed_sha,
        "current_vcl_sha": current_sha,
        "vcl_drift": vcl_drift,
        "capture_vitals": rum_cfg.get("capture_vitals", True),
        "capture_performance": rum_cfg.get("capture_performance", True),
        "capture_errors": rum_cfg.get("capture_errors", True),
        "capture_events": rum_cfg.get("capture_events", True),
        "custom_condition": rum_cfg.get("custom_condition", ""),
    }


@rum_router.get("/{service_id}/rum/status", response_model=RumStatusResponse, response_model_exclude_unset=True)
async def rum_status(
    request: Request,
    service_id: str = Path(...),
    _access: str | None = Depends(_require_rum_status_access),
) -> dict[str, Any]:
    """Get RUM enable/disable status and VCL drift detection."""
    return await asyncio.to_thread(_rum_status_sync, request, service_id)


@rum_router.get(
    "/{service_id}/rum/versions",
    response_model=RumVersionsResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
async def rum_versions(service_id: str = Path(...)) -> RumVersionsResponse:
    """List available Faro Web SDK versions + the pinned/latest state (admin-only)."""
    cfg = svcconfig.load_config(service_id) or {}
    rum_cfg = cfg.get("rum") or {}
    current = rum_cfg.get("faro_version")

    available = await get_faro_versions_cached()

    latest = available[0] if available else None
    update_available = bool(current and latest and current != latest)

    return RumVersionsResponse(
        available=available,
        current=current,
        latest=latest,
        update_available=update_available,
    )


@rum_router.post("/{service_id}/rum/upgrade", dependencies=[Depends(require_admin)])
async def upgrade_rum_handler(
    service_id: str = Path(...),
    body: RumUpgradeRequest = Body(...),
) -> EventSourceResponse:
    """Upgrade the pinned Faro Web SDK version for a service (admin-only, SSE stream)."""
    token = require_fastly_token(service_id, body.token)

    try:
        available = await fetch_available_faro_versions()
    except ValueError as e:
        raise_internal(
            logger,
            e,
            code="faro_registry_unavailable",
            status=503,
            message="Faro version registry unavailable",
        )

    if body.version not in available:
        raise HTTPException(
            status_code=400,
            detail=make_error("unknown_faro_version", f"Unknown Faro Web SDK version: {body.version}"),
        )

    def stream():
        yield json.dumps(
            {"type": "status", "message": f"Upgrading Faro Web SDK to v{body.version} for {service_id}..."}
        )
        try:
            for event in run_with_events(
                upgrade_faro_version,
                service_id,
                body.version,
                token,
                activate=body.activate,
                raise_on_error=True,
            ):
                yield json.dumps(event)

            updated_cfg = svcconfig.load_config(service_id) or {}
            updated_rum_cfg = updated_cfg.get("rum") or {}
            yield json.dumps(
                {
                    "type": "done",
                    "message": f"Faro Web SDK upgraded to v{body.version} successfully!"
                    if body.activate
                    else "Faro Web SDK upgrade draft compiled and validated successfully!",
                    "rum": {
                        "faro_version": updated_rum_cfg.get("faro_version"),
                    },
                }
            )
        except Exception as e:
            yield json.dumps({"type": "error", "message": str(e)})

    return sse_response(stream())


@rum_router.post(
    "/{service_id}/rum/settings",
    response_model=RumSettingsUpdateResponse,
    response_model_exclude_unset=True,
    dependencies=[Depends(require_admin)],
)
def update_rum_settings(
    service_id: str = Path(...),
    body: RumSettingsUpdateRequest = Body(...),
) -> dict[str, Any]:
    """Update RUM capture settings and re-deploy rum-tracker.js (admin-only)."""
    cfg = load_service_config(service_id)

    import copy

    original_cfg = copy.deepcopy(cfg)

    def _apply_settings(c: dict) -> None:
        rum_block = c.setdefault("rum", {})
        rum_block["capture_vitals"] = body.capture_vitals
        rum_block["capture_performance"] = body.capture_performance
        rum_block["capture_errors"] = body.capture_errors
        rum_block["capture_events"] = body.capture_events
        rum_block["custom_condition"] = body.custom_condition

    def _roll_back_settings(c: dict) -> None:
        original_rum = original_cfg.get("rum") or {}
        rum_block = c.setdefault("rum", {})
        for key in _RUM_SETTING_KEYS:
            if key in original_rum:
                rum_block[key] = original_rum[key]
            else:
                rum_block.pop(key, None)

    svcconfig.mutate_config(service_id, _apply_settings)

    token = require_fastly_token(service_id, body.token)

    from backend.provision.rum_assets import upload_rum_tracker_js

    try:
        res = upload_rum_tracker_js(service_id, token, overwrite=True)
    except Exception as e:
        try:
            svcconfig.mutate_config(service_id, _roll_back_settings)
        except Exception as rollback_err:
            logger.error(f"Failed to rollback service config for {service_id}: {rollback_err}")
        raise_internal(
            logger,
            e,
            code="rum_tracker_upload_failed",
            message="Failed to upload RUM tracker wrapper to FOS",
        )

    orig_rum = original_cfg.get("rum") or {}
    rum_was_enabled = svcconfig.is_rum_enabled(original_cfg)
    condition_changed = orig_rum.get("custom_condition") != body.custom_condition

    needs_reconciliation = not rum_was_enabled or condition_changed

    if needs_reconciliation:
        from backend.provision.declarative.reconciler import reconcile_vcl_state

        try:
            reconcile_vcl_state(service_id, token, dry_run=False, activate=True)
        except Exception as e:
            try:
                svcconfig.mutate_config(service_id, _roll_back_settings)
            except Exception as rollback_err:
                logger.error(f"Failed to rollback service config for {service_id}: {rollback_err}")
            raise_internal(
                logger,
                e,
                code="rum_reconcile_failed",
                message="Failed to reconcile and deploy updated RUM conditions to Fastly",
            )
    else:
        logger.info(
            f"[rum] Bypassing Fastly VCL reconciliation for service {service_id} "
            "because RUM is already enabled and custom_condition is unchanged."
        )

    return {
        "ok": True,
        "message": "RUM capture settings updated, wrapper re-deployed, and Fastly configuration reconciled!",
        "skipped": res.get("skipped", False),
    }


def _inject_telemetry(res: dict) -> dict:
    from backend.models.common import _debug_responses_enabled
    from backend.utils.telemetry import debug_envelope

    if not _debug_responses_enabled():
        return res
    return debug_envelope(res, gated=False)


def _parse_rum_filter_specs(filters: str | None) -> dict[str, Any]:
    """Parse and validate RUM ``filters`` query parameter into ``FilterSpec`` objects."""
    if not filters:
        return {}
    from backend.high_scale.rum import _FILTER_COLUMNS, parse_rum_filters
    from backend.models.common import FilterSpec
    from backend.repositories.utils.filters import filter_spec_attr, normalize_filter_key

    try:
        raw_dict = parse_rum_filters(filters)
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
        ) from exc

    parsed_filters: dict[str, Any] = {}
    unsupported: list[str] = []
    for raw_key, spec in raw_dict.items():
        values = [
            value
            for value in (filter_spec_attr(spec, "values") or [])
            if value is not None and not (isinstance(value, str) and value.strip() == "")
        ]
        column = normalize_filter_key(raw_key)
        if values and column not in _FILTER_COLUMNS:
            unsupported.append(raw_key)
            continue
        if isinstance(spec, dict):
            parsed_filters[raw_key] = FilterSpec(mode=spec.get("mode", "include"), values=spec.get("values", []))
        elif isinstance(spec, FilterSpec):
            parsed_filters[raw_key] = spec
    if unsupported:
        raise HTTPException(status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE))
    return parsed_filters


def _rum_beacon_health_sync(ctx: RequestContext) -> dict[str, Any]:
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import rum_beacon_health as hs_rum_health

        try:
            return hs_rum_health(high_scale_service, source=ctx.source)
        except TypeError:
            return hs_rum_health(high_scale_service)

    service_id = ctx.service_id
    cfg = svcconfig.load_config(service_id) or {}
    if not svcconfig.is_rum_enabled(cfg):
        return {
            "enabled": False,
            "beacon_fire_rate": 0,
            "recent_beacons": 0,
            "last_beacon_time": None,
            "setup_complete": False,
            "message": "RUM not enabled for this service",
        }

    try:
        rum_source = rum_source_for(ctx.source)

        with _ConnectionHolder(rum_source, read_only=True) as rum_con:
            recent_count, last_dt = execute_with_stale_view_retry(
                rum_con,
                rum_source,
                lambda con: rum_repo.get_beacon_health(con, source=rum_source),
            )

        last_beacon_time = None
        if last_dt:
            if isinstance(last_dt, _dt_mod.datetime):
                last_beacon_time = last_dt.isoformat()
            else:
                try:
                    last_beacon_dt = _dt_mod.datetime.fromisoformat(str(last_dt))
                    last_beacon_time = last_beacon_dt.isoformat()
                except (ValueError, TypeError):
                    pass

        has_data = recent_count > 0 or last_dt is not None
        return {
            "enabled": True,
            "beacon_fire_rate": recent_count,
            "recent_beacons": recent_count,
            "last_beacon_time": last_beacon_time,
            "setup_complete": has_data,
            "message": "Script installed and firing" if has_data else "Waiting for beacons...",
        }
    except Exception as e:
        logger.error(f"[rum] Setup check failed for {service_id}: {e}")
        return {
            "enabled": True,
            "beacon_fire_rate": 0,
            "recent_beacons": 0,
            "last_beacon_time": None,
            "setup_complete": False,
            "message": f"Setup check failed: {e}",
        }


@rum_router.get(
    "/{service_id}/rum/beacon-health", response_model=RumBeaconHealthResponse, response_model_exclude_unset=True
)
async def rum_beacon_health(
    request: Request,
    ctx: RequestContext = Depends(build_request_context),
) -> dict[str, Any]:
    """Check if RUM beacons are arriving (validation endpoint for setup)."""
    return _inject_telemetry(await asyncio.to_thread(_rum_beacon_health_sync, ctx))


@rum_router.get("/{service_id}/rum/analytics", response_model=RumAnalyticsResponse, response_model_exclude_unset=True)
async def rum_analytics(
    request: Request,
    start_time: str | None = Query(None),
    end_time: str | None = Query(None),
    filters: str | None = Query(None),
    ctx: RequestContext = Depends(build_request_context),
    range_token: Annotated[str | None, Query()] = None,
    anchor: Annotated[str | None, Query()] = None,
) -> dict[str, Any]:
    """Retrieve parsed RUM analytics from DuckDB views."""
    window = SimpleNamespace(range_token=range_token, anchor=anchor, start_time=start_time, end_time=end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import parse_rum_filters
        from backend.high_scale.rum import rum_analytics as hs_rum_analytics

        start_time, end_time = ctx.resolve_window(window)
        try:
            parsed_hs_filters = parse_rum_filters(filters)
            hs_result = await asyncio.to_thread(
                hs_rum_analytics, high_scale_service, start_time, end_time, parsed_hs_filters
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
            ) from exc
        return _inject_telemetry(hs_result)

    service_id = ctx.service_id

    start_time, end_time = ctx.resolve_window(window)

    if not start_time and not end_time:
        end_dt = _dt_mod.datetime.now(_dt_mod.UTC)
        start_dt = end_dt - _dt_mod.timedelta(hours=24)
        start_time = iso_z(start_dt)
        end_time = iso_z(end_dt)
    else:
        if start_time:
            parsed_start = parse_iso_utc(start_time)
            if parsed_start:
                start_time = iso_z(parsed_start)
        if end_time:
            parsed_end = parse_iso_utc(end_time)
            if parsed_end:
                end_time = iso_z(parsed_end)

    parsed_since = parse_iso_utc(start_time) if start_time else None
    since: _dt_mod.datetime = (
        parsed_since if parsed_since is not None else (_dt_mod.datetime.now(_dt_mod.UTC) - _dt_mod.timedelta(hours=24))
    )

    parsed_until = parse_iso_utc(end_time) if end_time else None
    until: _dt_mod.datetime = parsed_until if parsed_until is not None else _dt_mod.datetime.now(_dt_mod.UTC)

    parsed_filters = _parse_rum_filter_specs(filters)

    from backend.high_scale.rum import _FILTER_COLUMNS
    from backend.repositories.utils.filters import build_where_clause

    params, where_sql = build_where_clause(
        start_time,
        end_time,
        parsed_filters,
        actual_cols=list(_FILTER_COLUMNS),
        exclude_invalid_ips=False,
    )

    rum_source = rum_source_for(ctx.source)

    try:

        def _get_analytics(con):
            return rum_repo.fetch_rum_analytics(
                con,
                service_id=service_id,
                start_time=start_time,
                end_time=end_time,
                parsed_filters=parsed_filters,
                where_sql=where_sql,
                params=params,
                raw_window_max_s=_RUM_RAW_WINDOW_MAX_S,
                source=rum_source,
            )

        def _run_analytics_query():
            with _ConnectionHolder(rum_source, read_only=True) as rum_con:
                return execute_with_stale_view_retry(rum_con, rum_source, _get_analytics)

        db_res = await asyncio.to_thread(_run_analytics_query)

        if db_res.get("no_data", False):
            return _inject_telemetry(
                {
                    "is_mock": False,
                    "no_data": True,
                    "beacon_count": 0,
                    "pageview_count": 0,
                    "interaction_count": 0,
                    "error_count": 0,
                    "message": "Waiting for real-time RUM user events...",
                    "vitals": {
                        "lcp": {"p75": None, "distribution": None},
                        "cls": {"p75": None, "distribution": None},
                        "inp": {"p75": None, "distribution": None},
                        "fid": {"p75": None, "fcp": None, "ttfb": None},
                    },
                    "worst_pages": [],
                    "errors": [],
                    "trends": {"timestamps": [], "lcp": [], "cls": [], "error_rate": []},
                    "environments": {"browsers": {}, "os": {}, "devices": {}},
                }
            )

        vitals: dict[str, Any] = {
            "lcp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "cls": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "inp": {"p75": None, "distribution": {"good": 0, "needs_improvement": 0, "poor": 0}},
            "fid": {"p75": None, "fcp": None, "ttfb": None},
        }

        for metric, p75, total, good, ni, poor in db_res["vitals_rows"]:
            if not metric:
                continue
            key = metric.lower()
            if key in ("lcp", "cls", "inp"):
                if total > 0:
                    good_pct = int(good * 100 / total)
                    poor_pct = int(poor * 100 / total)
                    ni_pct = max(0, 100 - good_pct - poor_pct)
                else:
                    good_pct, ni_pct, poor_pct = 0, 0, 0

                if key == "cls":
                    p75_val = round(p75, 3) if p75 is not None else None
                elif key == "inp":
                    p75_val = int(p75) if p75 is not None else None
                else:
                    p75_val = _normalize_sec(p75)

                vitals[key] = {
                    "p75": p75_val,
                    "distribution": {"good": good_pct, "needs_improvement": ni_pct, "poor": poor_pct},
                }
            elif key == "fid":
                vitals["fid"]["p75"] = round(p75, 1) if p75 is not None else None
            elif key in ("fcp", "ttfb") and p75 is not None:
                vitals["fid"][key] = _normalize_sec(p75)

        worst_pages = [
            {
                "path": path,
                "views": views,
                "avg_load_time": _normalize_sec(avg_load),
                "lcp_p75": _normalize_sec(lcp),
                "cls_p75": round(cls, 3) if cls is not None else None,
                "error_rate": round(err_rate, 2) if err_rate is not None else 0.0,
            }
            for path, views, avg_load, lcp, cls, err_rate in db_res["worst_pages_rows"]
        ]

        errors = [
            {"message": msg, "file": file or "unknown.js", "line": line or 0, "col": col or 0, "count": count}
            for msg, file, line, col, count in db_res["errors_rows"]
        ]

        span_hours = (until - since).total_seconds() / 3600.0
        use_hourly = span_hours <= 48
        trend_buckets: dict[str, dict[str, float | int | None]] = {}

        curr = (
            since.replace(minute=0, second=0, microsecond=0)
            if use_hourly
            else since.replace(hour=0, minute=0, second=0, microsecond=0)
        )
        target = (
            until.replace(minute=0, second=0, microsecond=0)
            if use_hourly
            else until.replace(hour=0, minute=0, second=0, microsecond=0)
        )
        step = _dt_mod.timedelta(hours=1) if use_hourly else _dt_mod.timedelta(days=1)
        fmt = "%Y-%m-%d %H:00" if use_hourly else "%Y-%m-%d"

        while curr <= target:
            bucket_key = curr.strftime(fmt)
            trend_buckets[bucket_key] = {
                "lcp": None,
                "cls": None,
                "error_rate": None,
                "pageviews": 0,
                "interactions": 0,
                "errors": 0,
            }
            curr += step

        for hour_ts, lcp, cls, pvs, ints, errs, error_rate in db_res["trends_rows"]:
            if not hour_ts:
                continue
            if isinstance(hour_ts, str):
                try:
                    dt = _dt_mod.datetime.fromisoformat(hour_ts.replace("Z", "+00:00"))
                except Exception:
                    continue
            else:
                dt = hour_ts
            bucket_key = dt.strftime(fmt)
            if bucket_key in trend_buckets:
                trend_buckets[bucket_key] = {
                    "lcp": _normalize_sec(lcp),
                    "cls": round(cls, 3) if cls is not None else None,
                    "error_rate": round(error_rate, 2) if error_rate is not None else 0.0,
                    "pageviews": int(pvs or 0),
                    "interactions": int(ints or 0),
                    "errors": int(errs or 0),
                }

        sorted_keys = sorted(trend_buckets.keys())
        trend_timestamps: list[str] = []
        trend_lcps: list[float | None] = []
        trend_clss: list[float | None] = []
        trend_error_rates: list[float | None] = []
        trend_pageviews: list[int] = []
        trend_interactions: list[int] = []
        trend_errors: list[int] = []

        for k in sorted_keys:
            try:
                dt_parsed = _dt_mod.datetime.strptime(k, fmt)
                dt_parsed = dt_parsed.replace(tzinfo=_dt_mod.UTC)
                display_ts = dt_parsed.isoformat()
            except Exception:
                display_ts = k

            trend_timestamps.append(display_ts)
            stats = trend_buckets[k]
            trend_lcps.append(stats["lcp"])
            trend_clss.append(stats["cls"])
            trend_error_rates.append(stats["error_rate"])
            trend_pageviews.append(int(stats["pageviews"] or 0))
            trend_interactions.append(int(stats["interactions"] or 0))
            trend_errors.append(int(stats["errors"] or 0))

        return _inject_telemetry(
            {
                "is_mock": False,
                "no_data": False,
                "beacon_count": db_res["total_beacons"],
                "pageview_count": db_res.get("pageviews", 0),
                "interaction_count": db_res.get("interactions", 0),
                "error_count": db_res.get("errors_count", 0),
                "vitals": vitals,
                "worst_pages": worst_pages,
                "errors": errors,
                "trends": {
                    "timestamps": trend_timestamps,
                    "lcp": trend_lcps,
                    "cls": trend_clss,
                    "error_rate": trend_error_rates,
                    "pageviews": trend_pageviews,
                    "interactions": trend_interactions,
                    "errors": trend_errors,
                },
                "environments": {
                    "browsers": db_res["browsers"],
                    "os": db_res["os"],
                    "devices": db_res["devices"],
                },
            }
        )

    except Exception as e:
        error_id = correlation_id(request)
        logger.error(f"[rum] Analytics query failed for {service_id} error_id={error_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=make_error("rum_analytics_failed", "RUM analytics query failed", error_id=error_id),
        )


_live_events_cache: BoundedTTLCache = BoundedTTLCache(maxsize=128, ttl_seconds=15)
CacheRegistry.register("rum._live_events_cache", _live_events_cache)


@rum_router.get("/{service_id}/rum/live-events", response_model=list[RumLiveEventItem])
async def rum_live_events(
    start_time: str | None = None,
    end_time: str | None = None,
    filters: str | None = None,
    ctx: RequestContext = Depends(build_request_context),
    range_token: Annotated[str | None, Query()] = None,
    anchor: Annotated[str | None, Query()] = None,
) -> list[dict[str, Any]]:
    """Fetch recent live beacons stream to feed frontend ticker."""
    window = SimpleNamespace(range_token=range_token, anchor=anchor, start_time=start_time, end_time=end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.rum import parse_rum_filters
        from backend.high_scale.rum import rum_live_events as hs_rum_live

        start_time, end_time = ctx.resolve_window(window)
        try:
            parsed_hs_filters = parse_rum_filters(filters)
            return await asyncio.to_thread(hs_rum_live, high_scale_service, start_time, end_time, 50, parsed_hs_filters)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=make_error("rum_filters_unsupported", _RUM_FILTERS_MESSAGE)
            ) from exc

    service_id = ctx.service_id

    start_time, end_time = ctx.resolve_window(window)

    if not start_time and not end_time:
        end_dt = _dt_mod.datetime.now(_dt_mod.UTC)
        start_dt = end_dt - _dt_mod.timedelta(hours=24)
        start_time = iso_z(start_dt)
        end_time = iso_z(end_dt)

    parsed_filters = _parse_rum_filter_specs(filters)

    from backend.high_scale.rum import _FILTER_COLUMNS
    from backend.repositories.utils.filters import build_where_clause

    rum_source = rum_source_for(ctx.source)
    rum_db_path = rum_source.get("duckdb_path")
    db_mtime = 0
    if rum_db_path:
        try:
            db_mtime = os.stat(rum_db_path).st_mtime_ns
        except OSError:
            db_mtime = 0

    cache_key = digest_cache_key(
        {
            "svc": service_id,
            "s": bucket_time_to_minute(start_time),
            "e": bucket_time_to_minute(end_time),
            "f": filters or "",
            "m": db_mtime,
        },
        rum_source,
    )
    cached_events = _live_events_cache.get(cache_key)
    if cached_events:
        return list(cached_events)

    try:

        def _get_live_events(con):
            actual_cols = [c for c in rum_repo.get_client_vitals_columns(con) if c in _FILTER_COLUMNS] or list(
                _FILTER_COLUMNS
            )

            s_dt = parse_iso_utc(start_time) if start_time else None
            e_dt = parse_iso_utc(end_time) if end_time else None
            if s_dt and e_dt and (e_dt - s_dt) > _dt_mod.timedelta(hours=2):
                narrow_start = iso_z(e_dt - _dt_mod.timedelta(hours=2))
                narrow_params, narrow_where = build_where_clause(
                    narrow_start,
                    end_time,
                    parsed_filters,
                    actual_cols=actual_cols,
                    partition_pruning=True,
                    exclude_invalid_ips=False,
                )
                narrow_rows = rum_repo.fetch_rum_live_events(
                    con, where_sql=narrow_where, params=narrow_params, source=rum_source
                )
                if len(narrow_rows) >= 50:
                    return narrow_rows

            params, where_sql = build_where_clause(
                start_time,
                end_time,
                parsed_filters,
                actual_cols=actual_cols,
                partition_pruning=True,
                exclude_invalid_ips=False,
            )
            return rum_repo.fetch_rum_live_events(con, where_sql=where_sql, params=params, source=rum_source)

        def _run_live_events_query():
            with _ConnectionHolder(rum_source, read_only=True) as rum_con:
                return execute_with_stale_view_retry(rum_con, rum_source, _get_live_events)

        rows = await asyncio.to_thread(_run_live_events_query)

        events = []
        for (
            etype,
            ts,
            path,
            mname,
            mval,
            mrating,
            browser,
            os_name,
            device,
            cid,
            req_id,
            err_msg,
            city,
            region,
            country,
            pop,
            tls,
            ttfb,
        ) in rows:
            desc = "Page loaded successfully"
            if etype == "error":
                desc = err_msg or "JS Error"
            elif mname:
                metric_name_upper = mname.upper()
                val_str = f": {mval}" if mval is not None else ""
                desc = f"Metric {metric_name_upper}{val_str}"
                if mrating:
                    desc += f" ({mrating.upper()})"

            if ttfb is not None:
                if ttfb < 15:
                    ttfb = int(ttfb * 1000)
                else:
                    ttfb = int(ttfb)

            events.append(
                {
                    "time": ts.isoformat() if isinstance(ts, _dt_mod.datetime) else str(ts),
                    "type": etype,
                    "path": path or "/",
                    "desc": desc,
                    "browser": browser or "Unknown",
                    "os": os_name or "Unknown",
                    "raw_log": {
                        "meta": {
                            "browser": {"name": browser or "Unknown"},
                            "os": {"name": os_name or "Unknown"},
                            "device": {"type": device or "Unknown"},
                            "page": {"url": path or "/"},
                        },
                        "measurements": [
                            {
                                "type": "web-vitals",
                                "values": {mname: mval} if mname else {},
                                "context": {"rating": mrating or ""},
                            }
                        ]
                        if mname
                        else [],
                        "exceptions": [
                            {
                                "type": "Error",
                                "value": err_msg or "JS Error",
                            }
                        ]
                        if etype == "error"
                        else [],
                        "cid": cid,
                        "req_id": req_id,
                        "city": city or "Unknown",
                        "region": region or "Unknown",
                        "country": country or "Unknown",
                        "pop": pop or "Unknown",
                        "tls": tls or "Unknown",
                        "ttfb": ttfb,
                    },
                }
            )

        if events:
            _live_events_cache[cache_key] = events
        return events
    except Exception as e:
        logger.error(f"[rum] Failed to fetch live events for {service_id}: {e}")
        return []


# ── Network endpoints (folded from backend.routers.network) ─────────────────

network_router = APIRouter(prefix="/api", tags=["network"], responses=DEFAULT_ERROR_RESPONSES)

CORE_SECTIONS: frozenset[str] = frozenset(
    {"summary", "heatmap", "buckets", "leaderboard", "metro_leaderboard", "cities", "map_buckets"}
)
SHIELDING_SECTIONS: frozenset[str] = frozenset({"shielding_analysis"})

_expand_network_sections = make_section_expander(NetworkSectionName)


def _resolve_network_window(
    req: NetworkHealthRequest | NetworkQualityRequest | NetworkBundleRequest,
    ctx: RequestContext,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    """Resolve and clamp the time window + cache-key tokens for network endpoints."""
    start_time, end_time = ctx.resolve_window(req)
    if is_valid_range_token(req.range_token):
        return (
            start_time,
            end_time,
            req.range_token,
            quantize_anchor(req.anchor),
            invite_clamp_fingerprint(ctx.analyst_session),
        )
    return start_time, end_time, None, None, None


def _execute_network_health_section(
    req: NetworkHealthRequest | NetworkBundleRequest,
    ctx: RequestContext,
    *,
    start_time: str | None,
    end_time: str | None,
    sections: set[str] | None,
    want_core: bool,
    want_shielding: bool,
    mask_ips: bool,
    keyed_range_token: str | None,
    keyed_anchor: str | None,
    invite_fp: str | None,
) -> dict[str, Any]:
    if want_core:
        res = network_repo.get_health(
            con_factory=ctx.con_factory(),
            src=ctx.source,
            start_time=start_time,
            end_time=end_time,
            filters=req.filters,
            metric=req.metric,
            bucket_seconds=req.bucket_seconds,
            top_n=req.top_n,
            map_asn=req.map_asn,
            sections=sections,
            mask_ips=mask_ips,
            range_token=keyed_range_token,
            quantized_anchor=keyed_anchor,
            invite_clamp_fingerprint=invite_fp,
        )
    else:
        res = cache_result({"available": True}, state="bypass", prewarmer=False)

    if "_response_cache" in res:
        res["_response_cache"] = {**res["_response_cache"], "scope": "self"}
    if want_shielding:
        try:
            from backend.repositories import origin as _origin

            shielding = _origin.get_shielding_analysis(
                con=ctx.con,
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
            )
            shielding = {k: v for k, v in shielding.items() if not k.startswith("debug_")}
            res["shielding_analysis"] = shielding
        except HTTPException:
            raise
        except Exception:
            logger.exception(
                "shielding_analysis failed for service=%s",
                ctx.source.get("name"),
            )
            res["shielding_analysis"] = {"has_data": False, "error": True, "rows": []}
    return res


@network_router.post("/network-health", response_model=NetworkHealthResponse)
@query_errors()
def network_health(
    req: NetworkHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time, keyed_range_token, keyed_anchor, invite_fp = _resolve_network_window(req, ctx)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import network_health as hs_network_health

        return hs_network_health(high_scale_service, req, start_time, end_time)
    sections = _expand_network_sections(req.sections)
    mask_ips = mask_ips_for(ctx.analyst_session)

    want_core = sections is None or bool(sections & CORE_SECTIONS)
    want_shielding = sections is None or bool(sections & SHIELDING_SECTIONS)
    res = _execute_network_health_section(
        req,
        ctx,
        start_time=start_time,
        end_time=end_time,
        sections=sections,
        want_core=want_core,
        want_shielding=want_shielding,
        mask_ips=mask_ips,
        keyed_range_token=keyed_range_token,
        keyed_anchor=keyed_anchor,
        invite_fp=invite_fp,
    )
    return NetworkHealthResponse.with_telemetry(**res)


@network_router.post("/network-quality", response_model=NetworkQualityResponse)
@query_errors()
def network_quality(
    req: NetworkQualityRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import network_quality as hs_network_quality

        return hs_network_quality(high_scale_service, req, start_time, end_time)
    res = network_repo.get_quality(
        con_factory=ctx.con_factory(),
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        region_country=req.region_country,
    )
    return NetworkQualityResponse.with_telemetry(**cache_result(res, state="bypass", prewarmer=False))


def _fetch_pop_health_data(
    con: Any,
    source: dict[str, Any],
    start_time_dt: datetime,
    end_time_dt: datetime,
) -> tuple[list[PopHealthItem], bool, dict[str, Any]]:
    table_name = _safe_table(source["name"])
    from backend.repositories._base import QueryRunner

    runner = QueryRunner(con, source)
    timer = SectionTimer()
    started = time.perf_counter()
    rolled = runner.try_pop_health_from_rollup(
        start_time_dt.isoformat(),
        end_time_dt.isoformat(),
        has_filters=False,
    )
    timer.mark(
        "pop_health_rollup",
        started,
        source="rollup",
        activity="compute" if rolled is not None else "overhead",
        current_execution=True,
        outcome="served" if rolled is not None else "fallback",
    )
    observation = cache_result({"section_timings": timer.entries}, state="bypass", prewarmer=False)

    data = []
    if rolled is not None:
        for row in rolled:
            reqs = row[1]
            errs = row[2]
            hits = row[3]
            data.append(
                PopHealthItem(
                    pop=row[0],
                    requests=reqs,
                    errors=errs,
                    error_rate=round(errs * 100.0 / reqs, 2) if reqs else 0.0,
                    p50_rtt_us=row[5],
                    p95_ttfb_ms=row[6],
                    cache_hit_rate=round(hits * 100.0 / reqs, 2) if reqs else 0.0,
                    bandwidth_bytes=row[4] or 0,
                )
            )
        data.sort(key=lambda x: x.requests, reverse=True)
        return data, True, observation
    else:
        st_param = start_time_dt if start_time_dt.tzinfo is not None else start_time_dt.replace(tzinfo=UTC)
        et_param = end_time_dt if end_time_dt.tzinfo is not None else end_time_dt.replace(tzinfo=UTC)

        def _run_fallback(c):
            return [
                PopHealthItem(
                    pop=row[0],
                    requests=row[1],
                    errors=row[2],
                    error_rate=row[3],
                    p50_rtt_us=row[4],
                    p95_ttfb_ms=row[5],
                    cache_hit_rate=row[6],
                    bandwidth_bytes=row[7] or 0,
                )
                for row in network_repo.get_pop_health_rows(c, table_name, st_param, et_param)
            ]

        started = time.perf_counter()
        data = execute_with_stale_view_retry(con, source, _run_fallback)
        timer.mark("pop_health", started, source="live", activity="compute", current_execution=True, outcome="served")
        return data, False, observation


@network_router.get("/network/pop-health", response_model=PopHealthListResponse)
def get_pop_health(
    ctx: RequestContext = Depends(build_request_context),
    start_time: datetime | None = Query(default=None),
    end_time: datetime | None = Query(default=None),
):
    """Aggregate edge-routing and cache metrics across Fastly POP locations."""
    clamped_start, clamped_end = ctx.clamp(
        start_time.isoformat() if start_time is not None else None,
        end_time.isoformat() if end_time is not None else None,
    )
    clamped_start_dt = parse_iso_utc(clamped_start) if clamped_start else start_time
    clamped_end_dt = parse_iso_utc(clamped_end) if clamped_end else end_time
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import get_pop_health as hs_get_pop_health

        return hs_get_pop_health(high_scale_service, clamped_start_dt, clamped_end_dt)

    start_time_dt = clamped_start_dt or (datetime.now(UTC) - timedelta(hours=24))
    end_time_dt = clamped_end_dt or datetime.now(UTC)
    data, is_approx, observation = _fetch_pop_health_data(ctx.con, ctx.source, start_time_dt, end_time_dt)
    return PopHealthListResponse.with_telemetry(data=data, approximate=is_approx, **observation)


def _network_bundle_impl(
    req: NetworkBundleRequest,
    ctx: RequestContext,
) -> NetworkBundleResponse:
    with process_context_scope("network:bundle"):
        start_time, end_time, keyed_range_token, keyed_anchor, invite_fp = _resolve_network_window(req, ctx)

        high_scale_service = ctx.high_scale
        if high_scale_service is not None:
            from backend.high_scale.network import network_bundle as hs_network_bundle

            return hs_network_bundle(high_scale_service, req, start_time, end_time)

        sections = _expand_network_sections(req.sections) if req.sections else None
        mask_ips = mask_ips_for(ctx.analyst_session)

        want_health = (
            sections is None
            or "health" in sections
            or any(s in CORE_SECTIONS or s in SHIELDING_SECTIONS for s in sections)
        )
        bundle_dq: list = []
        bundle_dc: list = []
        health_resp = None
        if want_health:
            want_core = sections is None or "health" in sections or bool(sections & CORE_SECTIONS)
            want_shielding = sections is None or "health" in sections or bool(sections & SHIELDING_SECTIONS)
            res = _execute_network_health_section(
                req,
                ctx,
                start_time=start_time,
                end_time=end_time,
                sections=sections,
                want_core=want_core,
                want_shielding=want_shielding,
                mask_ips=mask_ips,
                keyed_range_token=keyed_range_token,
                keyed_anchor=keyed_anchor,
                invite_fp=invite_fp,
            )
            bundle_dq.extend(res.get("debug_queries") or [])
            bundle_dc.extend(res.get("debug_calls") or [])
            health_resp = NetworkHealthResponse.with_telemetry(**res)

        want_quality = sections is None or "quality" in sections
        quality_resp = None
        if want_quality:
            q_res = network_repo.get_quality(
                con_factory=ctx.con_factory(),
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
                region_country=req.region_country,
            )
            bundle_dq.extend(q_res.get("debug_queries") or [])
            bundle_dc.extend(q_res.get("debug_calls") or [])
            quality_resp = NetworkQualityResponse.with_telemetry(**cache_result(q_res, state="bypass", prewarmer=False))

        want_pop_health = sections is None or "pop_health" in sections
        pop_health_items: list[PopHealthItem] | None = None
        pop_observation = None
        if want_pop_health:
            try:
                st_dt = (
                    datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                    if start_time
                    else (datetime.now(UTC) - timedelta(hours=24))
                )
            except Exception:
                st_dt = datetime.now(UTC) - timedelta(hours=24)
            try:
                et_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00")) if end_time else datetime.now(UTC)
            except Exception:
                et_dt = datetime.now(UTC)
            pop_health_items, _, pop_observation = _fetch_pop_health_data(ctx.con, ctx.source, st_dt, et_dt)

        want_transport = sections is None or "transport" in sections
        transport_resp = None
        if want_transport:
            t_res = network_repo.get_transport_metrics(
                con_factory=ctx.con_factory(),
                src=ctx.source,
                start_time=start_time,
                end_time=end_time,
                filters=req.filters,
            )
            bundle_dq.extend(t_res.get("debug_queries") or [])
            bundle_dc.extend(t_res.get("debug_calls") or [])
            transport_resp = TransportMetrics(**cache_result(t_res, state="bypass", prewarmer=False))

        return NetworkBundleResponse.with_telemetry(
            **cache_result({}, state="bypass", prewarmer=False),
            available=True,
            component_observations={"pop_health": pop_observation} if pop_observation is not None else None,
            health=health_resp,
            quality=quality_resp,
            pop_health=pop_health_items,
            transport=transport_resp,
            debug_queries=bundle_dq,
            debug_calls=bundle_dc,
        )


@network_router.post("/network/bundle", response_model=NetworkBundleResponse)
@query_errors()
def network_bundle(
    req: NetworkBundleRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    return _network_bundle_impl(req, ctx)


@network_router.get("/network/bundle", response_model=NetworkBundleResponse)
@query_errors()
def network_bundle_get(
    start_time: str | None = None,
    end_time: str | None = None,
    range_token: str | None = None,
    anchor: str | None = None,
    metric: str = "health_score",
    bucket_seconds: int = 300,
    top_n: int = 30,
    map_asn: str = "all",
    region_country: str = "US",
    sections: list[NetworkSectionName] | None = Query(default=None),
    ctx: RequestContext = Depends(build_request_context),
):
    req = NetworkBundleRequest(
        start_time=start_time,
        end_time=end_time,
        range_token=range_token,
        anchor=anchor,
        metric=metric,
        bucket_seconds=bucket_seconds,
        top_n=top_n,
        map_asn=map_asn,
        region_country=region_country,
        sections=sections,
    )
    return _network_bundle_impl(req, ctx)


# ── Security endpoints (folded from backend.routers.security) ─────────────────

security_router = APIRouter(prefix="/security", tags=["security"], responses=DEFAULT_ERROR_RESPONSES)

_FINGERPRINT_CARDS: frozenset[str] = frozenset({"tls_fingerprints"})
_NGWAF_BOT_PAIR: frozenset[str] = frozenset({"ngwaf_verified_bots", "ngwaf_verified_bots_ts"})

_expand_security_sections = make_section_expander(
    SecuritySectionName,
    union_groups=(_NGWAF_BOT_PAIR,),
    implies=((_FINGERPRINT_CARDS, "fingerprint_coverage"),),
)


@security_router.post("/aggregates", response_model=SecurityAggregatesResponse)
@security_router.post("/bundle", response_model=SecurityAggregatesResponse)
@query_errors()
def security_aggregates(
    req: SecurityAggregatesRequest,
    response: Response,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.resolve_window(req)

    sections = _expand_security_sections(req.sections)
    mask_ips = mask_ips_for(ctx.analyst_session)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.security import security_aggregates as hs_aggregates

        return hs_aggregates(high_scale_service, req, start_time, end_time, sections=sections, mask_ips=mask_ips)

    def _set_cache_header() -> None:
        response.headers["Cache-Control"] = "private, max-age=30, stale-while-revalidate=120"

    get_agg_fn = getattr(
        network_repo,
        "get_security_aggregates",
        getattr(repo, "get_security_aggregates", security_repo.get_security_aggregates),
    )
    peek_agg_fn = getattr(
        network_repo,
        "peek_security_aggregates",
        getattr(repo, "peek_security_aggregates", security_repo.peek_security_aggregates),
    )

    return run_cached_repo_endpoint(
        ctx,
        repo_module="backend.repositories.security",
        get_fn=get_agg_fn,
        peek_fn=peek_agg_fn,
        kwargs={
            "start_time": start_time,
            "end_time": end_time,
            "filters": req.filters,
            "bucket_seconds": req.bucket_seconds,
            "sections": sections,
            "mask_ips": mask_ips,
        },
        wrap=lambda r: SecurityAggregatesResponse.with_telemetry(**r),
        on_response=_set_cache_header,
    )


@security_router.get("/bundle", response_model=SecurityAggregatesResponse)
@query_errors()
def security_bundle_get(
    response: Response,
    start_time: str | None = None,
    end_time: str | None = None,
    range_token: str | None = None,
    anchor: str | None = None,
    bucket_seconds: int = 300,
    ctx: RequestContext = Depends(build_request_context),
):
    req = SecurityAggregatesRequest(
        start_time=start_time,
        end_time=end_time,
        range_token=range_token,
        anchor=anchor,
        bucket_seconds=bucket_seconds,
    )
    return security_aggregates(req, response, ctx)


@security_router.get("/proxies/export")
@query_errors()
def export_proxies_csv(
    start_time: str,
    end_time: str,
    threshold: str = "High",
    format: str = "fastly-acl",
    ctx: RequestContext = Depends(build_request_context),
):
    resolved_start, resolved_end = ctx.clamp(start_time, end_time)

    mask_ips = mask_ips_for(ctx.analyst_session)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.security import get_proxies_data as hs_proxies

        res_raw = hs_proxies(high_scale_service, None, resolved_start, resolved_end, mask_ips=mask_ips)
        res = res_raw.model_dump() if hasattr(res_raw, "model_dump") else res_raw
    else:
        get_proxies_fn = getattr(
            network_repo,
            "get_security_proxies",
            getattr(repo, "get_security_proxies", security_repo.get_security_proxies),
        )
        res = get_proxies_fn(
            con=ctx.con,
            src=ctx.source,
            start_time=resolved_start,
            end_time=resolved_end,
            filters=None,
            mask_ips=mask_ips,
        )

    clients = res.get("active_clients", [])
    if threshold == "High":
        filtered_clients = [c for c in clients if c.get("risk_level") == "High"]
    else:
        filtered_clients = [c for c in clients if c.get("risk_level") in ("High", "Medium")]

    seen_ips = set()
    unique_clients = []
    for c in filtered_clients:
        raw_ip = c.get("ip")
        if not raw_ip:
            continue
        ip = str(raw_ip)
        if ip not in seen_ips:
            seen_ips.add(ip)
            item = dict(c)
            item["ip"] = ip
            unique_clients.append(item)
    filtered_clients = unique_clients

    output = io.StringIO()
    writer = csv.writer(output)

    if format == "fastly-acl":
        writer.writerow(["ip", "comment"])
        for client in filtered_clients:
            reason = (
                "Impossible distance mismatch" if client.get("impossible_distance") else "Behavioral VPN/Proxy tunnel"
            )
            writer.writerow([client["ip"], f"Watchdog block: {reason}"])
    else:
        for client in filtered_clients:
            output.write(f"{client['ip']}\n")

    output.seek(0)
    filename = (
        f"watchdog_blocklist_{threshold.lower()}_{format}.csv"
        if format == "fastly-acl"
        else f"blocklist_{threshold.lower()}.txt"
    )
    media_type = "text/csv" if format == "fastly-acl" else "text/plain"

    return StreamingResponse(
        io.BytesIO(output.getvalue().encode("utf-8")),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@security_router.get("/threat-intel", response_model=ThreatIntelListResponse)
@query_errors()
def get_security_threat_intel(
    ctx: RequestContext = Depends(build_request_context),
    start_time: str | None = Query(default=None),
    end_time: str | None = Query(default=None),
):
    """Correlate client TLS fingerprints with active WAF triggers and proxy flags."""
    resolved_start, resolved_end = ctx.clamp(start_time, end_time)

    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.security import get_security_threat_intel as hs_threat_intel

        return hs_threat_intel(high_scale_service, resolved_start, resolved_end)

    if not resolved_start:
        resolved_start = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
    if not resolved_end:
        resolved_end = datetime.now(UTC).isoformat()

    table_name = _safe_table(ctx.source["name"])
    get_ti_rows = getattr(
        network_repo,
        "get_threat_intel_rows",
        getattr(repo, "get_threat_intel_rows", security_repo.get_threat_intel_rows),
    )

    def _run_threat_intel(con) -> list[ThreatIntelItem]:
        return [
            ThreatIntelItem(
                tls_fingerprint=row[0],
                requests=row[1],
                top_country=row[2] or "Unknown",
                matched_waf_rules_count=row[3],
                bot_percentage=row[4] or 0.0,
                is_anonymized_proxy=bool(row[5]),
            )
            for row in get_ti_rows(con, ctx.source, table_name, resolved_start, resolved_end)
        ]

    data = execute_with_stale_view_retry(ctx.con, ctx.source, _run_threat_intel)
    return ThreatIntelListResponse.with_telemetry(data=data)


network_router.include_router(security_router)


# ── Origin endpoints (folded from backend.routers.origin) ───────────────────

origin_router = APIRouter(prefix="/api/origin", tags=["origin"], responses=DEFAULT_ERROR_RESPONSES)

_TS_STATUS_PATH_TRIPLE: frozenset[str] = frozenset({"timeseries", "status_codes", "path_breakdown"})
_POP_IP_PAIR: frozenset[str] = frozenset({"pop_latency", "ip_health"})

_expand_origin_sections = make_section_expander(OriginSectionName, union_groups=(_TS_STATUS_PATH_TRIPLE, _POP_IP_PAIR))


@origin_router.post("/aggregates", response_model=OriginAggregatesResponse)
@query_errors()
async def origin_aggregates(
    req: OriginAggregatesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    """Composite of the seven origin cards."""
    start_time, end_time = ctx.resolve_window(req)
    sections = _expand_origin_sections(req.sections)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.origin import aggregates as high_scale_aggregates

        res = await asyncio.to_thread(
            high_scale_aggregates, high_scale_service, req, start_time, end_time, sections=sections
        )
        return OriginAggregatesResponse.with_telemetry(**cache_result(res, state="bypass", prewarmer=False))

    await ctx.prewarm_con()
    res = await origin_repo.get_aggregates(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_minutes=req.bucket_minutes,
        split_by_leg=req.split_by_leg,
        timeseries_metric=req.timeseries_metric,
        timeseries_percentile=req.timeseries_percentile,
        slow_urls_limit=req.slow_urls_limit,
        slow_urls_min_requests=req.slow_urls_min_requests,
        ip_health_limit=req.ip_health_limit,
        pop_latency_limit=req.pop_latency_limit,
        sections=sections,
    )
    return OriginAggregatesResponse.with_telemetry(**cache_result(res, state="bypass", prewarmer=False))


@origin_router.post("/summary", response_model=OriginSummaryResponse)
@query_errors()
def origin_summary(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_summary(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginSummaryResponse.with_telemetry(**res)


@origin_router.post("/timeseries", response_model=OriginTimeseriesResponse)
@query_errors()
def origin_timeseries(
    req: OriginTimeseriesRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_timeseries(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        bucket_minutes=req.bucket_minutes,
        split_by_leg=req.split_by_leg,
        metric=req.metric,
        percentile=req.percentile,
    )
    return OriginTimeseriesResponse.with_telemetry(**res)


@origin_router.post("/slow-urls", response_model=OriginSlowUrlsResponse)
@query_errors()
def origin_slow_urls(
    req: OriginSlowUrlsRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_slow_urls(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
        min_requests=req.min_requests,
    )
    return OriginSlowUrlsResponse.with_telemetry(**res)


@origin_router.post("/status-codes", response_model=OriginStatusCodesResponse)
@query_errors()
def origin_status_codes(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_status_codes(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginStatusCodesResponse.with_telemetry(**res)


@origin_router.post("/path-breakdown", response_model=OriginPathBreakdownResponse)
@query_errors()
def origin_path_breakdown(
    req: OriginRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_path_breakdown(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
    )
    return OriginPathBreakdownResponse.with_telemetry(**res)


@origin_router.post("/pop-latency", response_model=OriginPopLatencyResponse)
@query_errors()
def origin_pop_latency(
    req: OriginPopLatencyRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_pop_latency(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginPopLatencyResponse.with_telemetry(**res)


@origin_router.post("/ip-health", response_model=OriginIpHealthResponse)
@query_errors()
def origin_ip_health(
    req: OriginIpHealthRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_ip_health(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginIpHealthResponse.with_telemetry(**res)


@origin_router.post("/shielding-analysis", response_model=OriginShieldingAnalysisResponse)
@query_errors()
def origin_shielding_analysis(
    req: OriginShieldingAnalysisRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    high_scale_service = ctx.high_scale
    if high_scale_service is not None:
        from backend.high_scale.network import origin_shielding_analysis as hs_shielding_analysis

        return hs_shielding_analysis(high_scale_service, req, start_time, end_time)
    res = origin_repo.get_shielding_analysis(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        limit=req.limit,
    )
    return OriginShieldingAnalysisResponse.with_telemetry(**res)


@origin_router.post("/drilldown", response_model=OriginDrilldownResponse)
@query_errors()
def origin_drilldown(
    req: OriginDrilldownRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    start_time, end_time = ctx.clamp(req.start_time, req.end_time)
    res = origin_repo.get_drilldown(
        con=ctx.con,
        src=ctx.source,
        start_time=start_time,
        end_time=end_time,
        filters=req.filters,
        dimension=req.dimension,
        limit=req.limit,
    )
    return OriginDrilldownResponse.with_telemetry(**res)


router.include_router(rum_router)
router.include_router(asset_router)
router.include_router(network_router)
router.include_router(origin_router)

_self_mod = sys.modules[__name__]
for _alias in ("insights", "query", "performance", "rum", "web_vitals", "network", "security", "origin"):
    sys.modules[f"backend.routers.{_alias}"] = _self_mod
    if "backend.routers" in sys.modules:
        setattr(sys.modules["backend.routers"], _alias, _self_mod)
