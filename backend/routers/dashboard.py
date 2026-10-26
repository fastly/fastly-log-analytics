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
from backend.models.insights import (
    CacheCollapseDetailRequest,
    CacheCollapseDetailResponse,
    InsightsRequest,
    InsightsResponse,
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
from backend.models.services import CmcdDisableBody, CmcdEnableBody, CmcdStatusResponse
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

_self_mod = sys.modules[__name__]
sys.modules["backend.routers.insights"] = _self_mod
sys.modules["backend.routers.query"] = _self_mod
sys.modules["backend.routers.performance"] = _self_mod
if "backend.routers" in sys.modules:
    setattr(sys.modules["backend.routers"], "insights", _self_mod)
    setattr(sys.modules["backend.routers"], "query", _self_mod)
    setattr(sys.modules["backend.routers"], "performance", _self_mod)
