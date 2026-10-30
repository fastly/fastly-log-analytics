"""Root-level system routes, middleware stack, and exception handlers for the FastAPI app."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from datetime import UTC, datetime

from fastapi import APIRouter, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from starlette_compress import CompressMiddleware, remove_compress_type

from backend.core import lifespan as _lifespan
from backend.core import request_telemetry
from backend.core.metadata.base import InvalidServiceIdError
from backend.models.analytics import HealthResponse
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.utils.remote_access import RemoteAccessMiddleware
from backend.utils.telemetry import TelemetryResponseBodyMiddleware

logger = logging.getLogger("backend.main")
router = APIRouter()


def _maybe_instrument_asgi(_app: FastAPI) -> None:
    """Wrap the app in FastAPIInstrumentor, unless OTel is disabled.

    Only the routers already migrated to build_request_context open an OTel
    span today (RequestTelemetry.start_request). FastAPIInstrumentor covers
    every route regardless of which dependency pattern it uses, so legacy
    get_source/get_con routes (most of admin/*, bootstrap.py) get RED
    metrics too instead of being invisible to any span-metrics processor.
    Gated the same way _setup_sdk is — skip the middleware-wrapping cost
    entirely when nothing will ever be exported (default: OTEL_EXPORTER=none,
    or pytest). request_telemetry.start_request() reuses this span instead
    of opening a second one for migrated routes, so a span-metrics
    processor doesn't double-count them — see its docstring.
    """
    if not request_telemetry.asgi_instrumentation_enabled():
        return
    request_telemetry.ensure_initialized()
    # exclude_spans: the ASGI receive/send child spans are pure transport
    # plumbing, not a route — left in, a span-metrics processor buckets
    # them as their own "{route} http send" series and pollutes every RED
    # dashboard built from span_name.
    FastAPIInstrumentor.instrument_app(_app, exclude_spans=["receive", "send"])


# ── Middleware stack ──────────────────────────────────────────────────────────
#
# Declared order (outermost → innermost). Asserted at boot below via
# assert_middleware_order(); a divergence crashes startup rather than
# shipping a silently-broken request pipeline. See ADR-04 for the rationale
# behind each layer's position.
MIDDLEWARE_ORDER = (
    "CompressMiddleware",  # outermost — sees final response body
    "TelemetryResponseBodyMiddleware",  # JSON-body backstop for debug panel
    "RemoteAccessMiddleware",  # analyst firewall — rejects before CORS, sets analyst_session
    "BaseHTTPMiddleware",  # @app.middleware('http') telemetry decorator — INSIDE RemoteAccess so analyst_session is populated
    "CORSMiddleware",  # innermost — closest to FastAPI routing
)


def assert_middleware_order(_app: FastAPI) -> None:
    """Boot-time assertion that middleware order matches ADR-04.

    Crashes on mismatch — a reorder that compiles is no longer enough to
    ship. ``user_middleware`` is in outermost-first order (Starlette
    reverses the add-order internally), so the comparison is direct.
    """
    # getattr fallback because starlette types `m.cls` as `_MiddlewareFactory[P]`
    # which has no static `__name__`; at runtime middleware classes always do.
    actual = tuple(getattr(m.cls, "__name__", repr(m.cls)) for m in _app.user_middleware)
    if actual != MIDDLEWARE_ORDER:
        raise RuntimeError(f"Middleware order violation (ADR-04). expected={MIDDLEWARE_ORDER} actual={actual}")


async def telemetry_middleware(request: Request, call_next):
    """Initialise call tracking, set process context, and flush FOS/CDN ops after the request.

    Uses process_context_scope (the context manager) so the global
    _LATEST_PROCESS_CONTEXT mirror reverts when the request exits. Otherwise
    out-of-thread readers keep reading
    the last-completed request's context and attribute cron-driven CDN
    reads to whichever API request happened most recently (observed in
    the 2026-05-24 audit: dashboard's cdn.miss rows landed tagged as
    `api:GET /api/debug/recent-sqlite` because the debug poller ran last).

    Also sets the Live Query Monitor's :data:`current_attribution`
    ContextVar here (NOT in :func:`build_request_context`) so the value
    propagates to sync dependencies and the route handler via FastAPI's
    ``run_in_threadpool`` — each threadpool call copies the parent
    context, and a ContextVar set INSIDE a dep doesn't flow forward to
    the route's separate threadpool call. Setting from the middleware
    avoids that gap.

    Registered INSIDE RemoteAccessMiddleware so ``request.state.analyst_session``
    is populated by the time we read it for attribution — see ADR-04 + audit
    finding 003. As a side benefit, blocked-by-RemoteAccess analyst requests
    no longer reach this layer, so they no longer pollute usage_log with
    admin-scoped rows.
    """
    import backend.scoring as _scoring_labels
    from backend import config as svcconfig
    from backend.core.request_telemetry import (
        Attribution as _Attribution,
    )
    from backend.core.request_telemetry import (
        current_attribution as _current_attribution,
    )
    from backend.utils.telemetry import (
        decrement_active_requests as _dec_active_requests,
    )
    from backend.utils.telemetry import (
        increment_active_requests as _inc_active_requests,
    )
    from backend.utils.telemetry import process_context_scope, start_call_tracking

    # Active-request gate (perf #84): cron ticks check this counter at
    # their entry point and defer up to 30 s if non-zero, so a cold-cache
    # API request doesn't lose its DuckDB pool slot + Fastly bandwidth to
    # a sync tick that fired the same second.
    _inc_active_requests()
    start_call_tracking()
    # Open a per-request memoization scope for scoring_labels. The
    # /admin/session-scoring composite fires list_labels / counts_by_label
    # against the same service_id from 10+ sub-handlers; without this each
    # one independently opens the per-service SQLite handle and runs the
    # same SELECT. Cleared in the finally below so cron / test paths fall
    # through to the live DB read.
    _scoring_labels.init_request_cache()
    page_load_id = request.headers.get("X-Page-Load-ID") or request.headers.get("x-page-load-id")
    from backend.utils.telemetry import get_page_load_id, set_page_load_id

    _prev_plid = get_page_load_id()
    set_page_load_id(page_load_id)

    # Modify process context to include the page_load_id suffix so proxy reads match
    if page_load_id:
        ctx_name = f"api:{request.method} {request.url.path}#{page_load_id}"
    else:
        ctx_name = f"api:{request.method} {request.url.path}"

    # Best-effort attribution: analyst_session is set by RemoteAccessMiddleware
    # (which now sits OUTSIDE this middleware). Falls back to admin when
    # the request is local-only / non-analyst.
    analyst_session = getattr(request.state, "analyst_session", None)
    request_path = request.url.path
    # SRE-01: the attribution correlation id MUST be populated in every
    # environment. The OTel trace_id is only valid when an exporter is wired
    # (OTEL_EXPORTER != none); in the prod default it is invalid → blank,
    # which left slow_queries.attr_request_id uniformly empty and broke the
    # slow-request → query → who-ran-it pivot. Prefer the trace_id when it is
    # real (keeps trace correlation working once a collector exists), else
    # fall back to the app-level id minted by RemoteAccessMiddleware. Stamp
    # the resolved value back onto request.state so the outer access-log
    # lines (SRE-02) join on exactly the same id.
    request_id = getattr(request.state, "request_id", None)
    try:
        from opentelemetry import trace as _otel_trace

        _span = _otel_trace.get_current_span()
        _sctx = _span.get_span_context() if _span is not None else None
        if _sctx and _sctx.is_valid:
            request_id = format(_sctx.trace_id, "032x")
    except Exception:
        logger.debug("otel trace_id lookup failed", exc_info=True)
    if not request_id:
        import secrets as _secrets

        request_id = _secrets.token_hex(8)
    try:
        request.state.request_id = request_id
    except Exception:
        logger.debug("request.state.request_id assignment failed", exc_info=True)
    import secrets as _cancel_secrets

    cancel_key = _cancel_secrets.token_hex(8)
    if analyst_session is not None:
        attr = _Attribution.analyst(
            analyst_id=getattr(analyst_session, "session_id", None) or "unknown",
            analyst_name=getattr(analyst_session, "name", None) or None,
            request_path=request_path,
            request_id=request_id,
            cancel_key=cancel_key,
        )
    else:
        from backend.utils.remote_access import client_ip as _client_ip

        attr = _Attribution.admin(
            admin_id=_client_ip(request, default="unknown") or "admin",
            request_path=request_path,
            request_id=request_id,
            cancel_key=cancel_key,
        )
    _prev_attr = _current_attribution.get()
    _current_attribution.set(attr)

    async def _watch_disconnect() -> None:
        try:
            await asyncio.sleep(0.2)
            while True:
                if request.method in ("GET", "HEAD", "DELETE", "OPTIONS") or getattr(
                    request, "_stream_consumed", False
                ):
                    if await request.is_disconnected():
                        from backend.core.request_telemetry import query_registry as _qr

                        _qr.cancel_for_key(cancel_key)
                        return
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            raise
        except Exception:
            return

    _disconnect_watcher = asyncio.create_task(_watch_disconnect())
    with process_context_scope(ctx_name):
        try:
            response = await call_next(request)
        except asyncio.CancelledError:
            # The client went away (a reload aborts its fetches) and Starlette cancelled the handler. The DuckDB
            # statements it already started keep running in worker threads, each holding a scarce pool slot; every
            # further reload would add another orphan until the pool saturates ("pool for <svc> saturated at 4").
            # Interrupt this request's queries (and only this request's) so the slots free immediately.
            from backend.core.request_telemetry import query_registry as _qr

            try:
                _qr.cancel_for_key(cancel_key)
            except Exception:
                logger.debug("cancel_for_key failed on request cancellation", exc_info=True)
            raise
        finally:
            _disconnect_watcher.cancel()
            # Active-request gate (perf #84): balance the increment above so the
            # cron-defer counter falls back to 0 when the request finishes.
            # Lives in the finally so an exception in call_next can't leak the
            # counter — a stuck +1 would cause should_defer_cron to return True
            # for up to max_defer_secs on every subsequent cron tick.
            try:
                _dec_active_requests()
            except Exception:
                logger.debug("_dec_active_requests failed", exc_info=True)
            try:
                sid = (
                    request.query_params.get("service")
                    or request.headers.get("x-fastly-service-id")
                    or request.headers.get("x-service-id")
                )
                if sid and not svcconfig.load_config(sid):
                    # Treat unknown IDs as cdn_service_id and resolve to the logging service.
                    sid = svcconfig.get_cdn_service_id_map().get(sid, sid)
                if not sid:
                    sid = svcconfig.get_active_service_id()
                if sid:
                    from backend.utils.telemetry import flush_usage_log

                    flush_usage_log(sid)
            except Exception:
                logger.debug("flush_usage_log failed", exc_info=True)
    # Restore the prior attribution value AFTER process_context_scope has
    # popped — so any final iothread drain still sees this request's
    # attribution, mirroring the rationale documented above for
    # _LATEST_PROCESS_CONTEXT.
    try:
        _current_attribution.set(_prev_attr)
    except Exception:
        logger.debug("restore _current_attribution failed", exc_info=True)
    # Close the scoring_labels per-request cache. Setting to None means
    # any post-response background work (or a subsequent request reusing
    # the same thread) sees a clean state and falls through to live reads
    # instead of stale cached rows.
    try:
        _scoring_labels.clear_request_cache()
    except Exception:
        logger.debug("clear_request_cache failed", exc_info=True)
    try:
        set_page_load_id(_prev_plid)
    except Exception:
        logger.debug("set_page_load_id restore failed", exc_info=True)
    return response


async def _invalid_service_id_handler(request: Request, exc: InvalidServiceIdError) -> JSONResponse:
    """Convert ``InvalidServiceIdError`` raised by ``metadata.pg_pool._validate_service_id_or_raise``
    into a 422 instead of letting it bubble as an unhandled 500 error. Triggered by
    routes whose ``service_id`` path parameter contains characters outside the
    allowed ``[A-Za-z0-9_-]{1,64}`` pattern.

    Body shape matches FastAPI's own ``HTTPValidationError`` schema so the
    response stays conformant to the OpenAPI spec.
    """
    return JSONResponse(
        status_code=422,
        content={
            "detail": [
                {
                    "loc": ["path", "service_id"],
                    "msg": str(exc),
                    "type": "value_error.invalid_service_id",
                }
            ],
        },
    )


def configure_root_system(app: FastAPI) -> None:
    """Attach ASGI instrumentation, the ADR-04 middleware stack, and exception handlers."""
    _maybe_instrument_asgi(app)

    # INVARIANT: CORSMiddleware is innermost (see ADR-04).
    app.add_middleware(
        CORSMiddleware,
        allow_origins=os.environ.get(
            "CORS_ORIGINS",
            "http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001,http://127.0.0.1:3001,http://localhost:13002,http://127.0.0.1:13002",
        ).split(","),
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # INVARIANT: BaseHTTPMiddleware (the telemetry decorator) is INNER to
    # RemoteAccessMiddleware (see ADR-04 + audit finding 003). Registered
    # first here so it ends up innermost relative to RemoteAccess — that
    # way ``request.state.analyst_session`` is already populated by the
    # time the dispatch reads it for attribution. Pre-fix, telemetry sat
    # OUTSIDE RemoteAccess and silently misattributed every analyst
    # request to a generic admin-by-client-host.
    app.middleware("http")(telemetry_middleware)

    # INVARIANT: RemoteAccessMiddleware sits OUTSIDE the telemetry decorator
    # (see ADR-04 + audit finding 003). Blocks analyst requests before they
    # reach the telemetry layer so blocked analyst hits don't pollute
    # usage_log with admin-scoped rows AND sets analyst_session early so
    # the inner telemetry middleware can attribute correctly.
    app.add_middleware(RemoteAccessMiddleware)

    # INVARIANT: TelemetryResponseBodyMiddleware inside Compress, outside
    # RemoteAccess (see ADR-04). Reads uncompressed JSON bodies to inject
    # debug fields; gated on DEBUG_RESPONSES.
    app.add_middleware(TelemetryResponseBodyMiddleware)

    app.add_exception_handler(InvalidServiceIdError, _invalid_service_id_handler)  # type: ignore[arg-type]

    # INVARIANT: CompressMiddleware is outermost (see ADR-04). Must wrap the
    # final response body so Content-Encoding survives all the way to the
    # client; an inner placement gets stripped by BaseHTTPMiddleware's
    # buffer-and-reemit (audited 2026-06-09: 11490 B raw uncompressed when
    # Compress sat inside the telemetry decorator).
    # Disable compression for Server-Sent Events (SSE) to prevent response buffering
    # and chunk boundaries corruption (which cause ERR_INVALID_CHUNKED_ENCODING on public endpoints).
    remove_compress_type("text/event-stream")
    app.add_middleware(CompressMiddleware, minimum_size=1024)

    # Boot-time middleware-order assertion. Crashes on violation rather than
    # shipping a silently-broken stack. See ADR-04 + assert_middleware_order().
    assert_middleware_order(app)


# ── Health check ──────────────────────────────────────────────────────────────


# Single source of truth for the version reported by /api/health. Read from the
# installed package metadata (pyproject ``version``) so it can't silently drift
# from the frontend's package.json the way the prior hardcoded "1.0.0" did.
# Falls back to a literal only if metadata lookup fails (e.g. running from a
# tree that was never installed).
try:
    from importlib.metadata import version as _pkg_version

    _APP_VERSION = _pkg_version("fastly-log-analytics")
except Exception:
    _APP_VERSION = "3.0.0-rc1"


# Documents the canonical error codes this probe can surface — notably the
# real 503 it returns when a service is degraded (previously undocumented).
# 422 is deliberately left as FastAPI's auto-generated HTTPValidationError:
# this is one of the few endpoints with typed query params (deep / stale_minutes)
# AND it's live-fuzzed by tests/test_openapi_and_contracts.py, so the documented
# 422 must match the real request-validation body. There is no app-wide
# RequestValidationError reshaper, so claiming ErrorEnvelope here would be a
# documentation lie the fuzzer catches (and reshaping the body would be an
# out-of-scope wire-format change). See tests/test_openapi_and_contracts.py.
_HEALTH_ERROR_RESPONSES = {code: spec for code, spec in DEFAULT_ERROR_RESPONSES.items() if code != 422}

# SRE-04: how long a sync row may sit in ``status='running'`` before deep
# /api/health treats it as a stall rather than a healthy in-progress sync. A
# normal sync caps at 240s (_run_service_cron max_seconds) and the orphan
# reaper reclaims leaked rows at 10 min; 15 min is comfortably past both, so a
# row still 'running' here means either a genuinely wedged ingest or a dead
# scheduler whose reaper never fired — both worth a 503.
_STUCK_SYNC_RUNNING_MINS = 15


@router.get("/metrics", include_in_schema=False)
def prometheus_metrics():
    try:
        from fastapi.responses import Response
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
    except ImportError:
        from fastapi import HTTPException

        raise HTTPException(status_code=501, detail="Prometheus client not installed")


@router.get(
    "/api/health",
    tags=["meta"],
    responses=_HEALTH_ERROR_RESPONSES,
    response_model=HealthResponse,
    response_model_exclude_unset=True,
)
def health_check(
    request: Request,
    deep: bool = False,
    stale_minutes: int = 30,
):
    """Liveness/readiness probe.

    Default (``?deep=0``) is a cheap liveness response — 200 as soon as the
    HTTP server is answering. Pass ``?deep=1`` to also verify ingest
    freshness per configured service. A service is reported ``degraded``
    when ANY of:
      - its newest ``ingested_files.ingested_at`` is older than
        ``stale_minutes`` (default 30) — SRE-22: before degrading on this
        check, the cutoff is widened to the service's own historical p95
        gap between non-empty ingests (never narrower than
        ``stale_minutes``), so a low-traffic service's organic quiet
        periods don't false-positive;
      - the last terminal ``sync`` cron run ended in ``error``;
      - a ``sync`` row is stuck in ``running`` past ``_STUCK_SYNC_RUNNING_MINS``
        (SRE-04 — the orphan-stall / OOM-leak the success-only filter hides);
      - the latest terminal run of a critical non-sync ingest task
        (``commit``) ended in ``error`` (SRE-05).
    The endpoint returns HTTP 503 when ANY service is degraded; otherwise 200.

    Cost: hits PostgreSQL metadata only — no FOS, no CDN, no Fastly API. Safe to wire
    into a load-balancer health check.

    Security (finding 003): the deep variant enumerates every configured
    service_id and reports per-service sync errors / staleness, which is
    operational metadata an analyst should not see. Force ``deep`` to
    False for remote (live-share) callers so analysts only ever get the
    shallow ok/degraded liveness signal.
    """
    from datetime import timedelta

    from fastapi.responses import JSONResponse

    from backend import config
    from backend.core import metadata as metadata_db
    from backend.utils.remote_access import is_request_remote

    if deep and is_request_remote(request):
        deep = False

    is_testing = "pytest" in sys.modules
    status = "ok" if (_lifespan._startup_complete or is_testing) else "initializing"
    payload: dict = {"status": status, "version": _APP_VERSION}
    if not (_lifespan._startup_complete or is_testing) or not deep:
        return payload

    # Clamp stale_minutes to a sane range — Schemathesis fuzzes this
    # with 2**63-1 which would OverflowError inside timedelta(...) and
    # 500 the liveness probe. 525 600 (1 year) is far past any realistic
    # operator-meaningful staleness window and well under the
    # timedelta(days) hard cap.
    from backend.utils.date_utils import iso_z

    stale_minutes = max(0, min(int(stale_minutes), 525_600))
    cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=stale_minutes))
    services_report: list[dict] = []
    overall_ok = True

    for sid in config.list_service_ids():
        svc_state: dict = {"service_id": sid, "status": "ok"}
        try:
            con = metadata_db.get_con(sid)
            row = con.execute(
                "SELECT max(ingested_at) AS last_ingest FROM ingested_files WHERE source_name = ?",
                (sid,),
            ).fetchone()
            last_ingest = row["last_ingest"] if row else None
            svc_state["last_ingest"] = last_ingest

            has_service_id = False
            try:
                con.execute("SELECT service_id FROM cron_runs LIMIT 0")
                has_service_id = True
            except Exception:
                has_service_id = False

            has_id = False
            try:
                con.execute("SELECT id FROM cron_runs LIMIT 0")
                has_id = True
            except Exception:
                has_id = False

            order_suffix = ", id DESC" if has_id else ""
            where_service = "service_id = ? AND " if has_service_id else ""
            service_args = (sid,) if has_service_id else ()

            cron_row = con.execute(
                f"SELECT status, started_at, error_message FROM cron_runs "
                f"WHERE {where_service}task = 'log_discovery' AND status != 'running' "
                f"ORDER BY started_at DESC{order_suffix} LIMIT 1",
                service_args,
            ).fetchone()
            if cron_row:
                svc_state["last_sync_status"] = cron_row["status"]
                svc_state["last_sync_at"] = cron_row["started_at"]
                if cron_row["status"] == "error":
                    svc_state["status"] = "degraded"
                    svc_state["reason"] = f"last sync errored: {cron_row['error_message'] or 'unknown'}"

            # SRE-04: a sync row stuck in 'running' (the documented orphan
            # stall / OOM-restart leak) is EXCLUDED by the filter above, so
            # without this the probe reports the older success as ok during
            # the stall window — and forever if the self-healing reaper never
            # runs (dead scheduler). Surface it as a stall once it outlives a
            # normal sync + the reaper window (_STUCK_SYNC_RUNNING_MINS). This
            # is an ADDITIVE degrade signal — wrapped so a query failure can
            # never flip a healthy service to 503 (worse than prior behavior);
            # the outer handler still degrades on genuine metadata-read loss.
            if svc_state["status"] == "ok":
                try:
                    stuck = con.execute(
                        f"SELECT started_at FROM cron_runs "
                        f"WHERE {where_service}task = 'log_discovery' AND status = 'running' "
                        f"ORDER BY started_at DESC{order_suffix} LIMIT 1",
                        service_args,
                    ).fetchone()
                    if stuck and stuck["started_at"]:
                        stuck_cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=_STUCK_SYNC_RUNNING_MINS))
                        norm_started = str(stuck["started_at"]).replace(" ", "T").rstrip("Z")
                        norm_stuck_cutoff = str(stuck_cutoff).replace(" ", "T").rstrip("Z")
                        if norm_started < norm_stuck_cutoff:
                            svc_state["status"] = "degraded"
                            svc_state["reason"] = (
                                f"sync stuck running since {stuck['started_at']} (>{_STUCK_SYNC_RUNNING_MINS}m)"
                            )
                except Exception:
                    logger.debug("deep health stuck-sync probe failed", exc_info=True)

            # SRE-05: the probe is otherwise task='sync'-blind. A commit cron
            # erroring every run (buffer growing, nothing reaching DuckLake)
            # stays ok. Degrade if the latest terminal run of any critical
            # ingest task errored. Maintenance tasks (optimize /
            # expire / *_cleanup / rollup_compact) are intentionally excluded
            # — their failure doesn't freeze ingestion. Additive + fail-safe
            # for the same reason as SRE-04 above.
            if svc_state["status"] == "ok":
                try:
                    # Recency window: only degrade on errors from the last 24h.
                    # A per-task-latest scan with no window pins 'degraded'
                    # FOREVER on a task that never runs again, so nothing ever
                    # supersedes its last error. A live failing cron keeps
                    # re-erroring inside the window, so real incidents still
                    # degrade.
                    crit_cutoff = iso_z(datetime.now(UTC) - timedelta(hours=24))
                    critical_tasks = ["commit"]
                    placeholders = ", ".join("?" for _ in critical_tasks)
                    crit_row = con.execute(
                        "SELECT task, error_message FROM ("
                        "  SELECT task, status, error_message, "
                        f"         ROW_NUMBER() OVER (PARTITION BY task ORDER BY started_at DESC{order_suffix}) AS rn "
                        f"  FROM cron_runs WHERE {where_service}task IN ({placeholders}) AND status != 'running'"
                        "  AND started_at >= ?"
                        # Boot-reap rows are lifecycle artifacts (the process
                        # stopped mid-run), not failing crons — a task that is
                        # never scheduled again would otherwise hold 'degraded'
                        # for the whole window on every restart.
                        "  AND COALESCE(error_message, '') != 'Process interrupted by server restart'"
                        ") WHERE rn = 1 AND status = 'error' LIMIT 1",
                        (*service_args, *critical_tasks, crit_cutoff),
                    ).fetchone()
                    if crit_row:
                        svc_state["status"] = "degraded"
                        svc_state["reason"] = (
                            f"{crit_row['task']} cron errored: {crit_row['error_message'] or 'unknown'}"
                        )
                except Exception:
                    logger.debug("deep health critical-cron probe failed", exc_info=True)

            # A brand-new service legitimately has no ingest yet; don't flag
            # it as degraded. Only flag services that have ingested at least
            # once AND fell behind the cutoff.
            if last_ingest and svc_state["status"] == "ok":
                norm_last = str(last_ingest).replace(" ", "T").rstrip("Z")
                norm_cutoff = str(cutoff).replace(" ", "T").rstrip("Z")
                if norm_last < norm_cutoff:
                    # SRE-22: a fixed stale_minutes cutoff false-positives on a
                    # low-traffic service's organic quiet periods. Before
                    # degrading, widen the cutoff to this service's own
                    # historical p95 inter-ingest gap (never narrower than
                    # stale_minutes) and re-check against that. Only reached
                    # once the naive check already looks stale, so the
                    # common healthy path pays zero extra query cost.
                    effective_minutes = metadata_db.adaptive_stale_minutes(
                        con, default_minutes=stale_minutes, service_id=sid if has_service_id else None
                    )
                    effective_cutoff = cutoff
                    if effective_minutes > stale_minutes:
                        effective_cutoff = iso_z(datetime.now(UTC) - timedelta(minutes=effective_minutes))
                        svc_state["stale_minutes_used"] = effective_minutes
                    norm_effective_cutoff = str(effective_cutoff).replace(" ", "T").rstrip("Z")
                    if norm_last < norm_effective_cutoff:
                        svc_state["status"] = "stale"
                        svc_state["reason"] = f"no ingest since {last_ingest} (cutoff {effective_cutoff})"
        except Exception as e:
            svc_state["status"] = "degraded"
            svc_state["reason"] = f"metadata_db query failed: {e}"

        if svc_state["status"] not in ("ok", "stale"):
            overall_ok = False
        services_report.append(svc_state)

    payload["services"] = services_report
    payload["status"] = "ok" if overall_ok else "degraded"

    if not overall_ok:
        return JSONResponse(status_code=503, content=payload)
    return payload
