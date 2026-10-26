from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar

logger = logging.getLogger(__name__)

# Global context for tracking API and FOS calls during a request
_CALLS: ContextVar[list[dict] | None] = ContextVar("_CALLS", default=None)
_QUERIES: ContextVar[list[dict] | None] = ContextVar("_QUERIES", default=None)
# SQLite statements executed while serving the current request. Fed by
# backend/utils/sqlite_profiler.py (which also keeps its process-global ring
# buffer — that one exists for cron visibility; THIS one is what makes the
# Debug Panel's SQLite section page-scoped). None outside a request, so cron
# statements never accumulate here.
_SQLITE_QUERIES: ContextVar[list[dict] | None] = ContextVar("_SQLITE_QUERIES", default=None)

# Per-request start time + dedupe flag for the iothread-call augmentation in
# get_tracked_calls(). ContextVars don't propagate into fsspec's iothread or
# pyiceberg's pool, so FOS/CDN calls made by those threads never reach
# `_CALLS` and the Debug Panel sees a hole. At end-of-request we query
# usage_log for rows tagged with this request's process_context since
# `_REQUEST_START_TS` and merge them in. `_IOTHREAD_QUERIED` prevents
# re-querying when get_tracked_calls is called multiple times per request.
_REQUEST_START_TS: ContextVar[float | None] = ContextVar("_REQUEST_START_TS", default=None)
_IOTHREAD_QUERIED: ContextVar[bool] = ContextVar("_IOTHREAD_QUERIED", default=False)

# Process context — identifies what triggered the current work (cron job name, API route, etc.)
_PROCESS_CONTEXT: ContextVar[str | None] = ContextVar("_PROCESS_CONTEXT", default=None)
_PAGE_LOAD_ID: ContextVar[str | None] = ContextVar("_PAGE_LOAD_ID", default=None)
_RECORDING_TELEMETRY: ContextVar[bool] = ContextVar("_RECORDING_TELEMETRY", default=False)

# Process-global stack of currently-active contexts. ContextVars don't
# propagate into raw worker threads or asyncio loop threads (fsspec's
# iothread, pyiceberg's ThreadPoolExecutor), so any code reading the
# context from such a thread sees None and the X-Telemetry-Context header
# is omitted — landing the row in usage_log with process_context=NULL.
# The stack below lets out-of-thread readers (see
# get_process_context_with_fallback) attribute the row to *some* currently
# running cron. The stack top is the most recently entered scope; popping
# on exit restores the prior cron's value rather than nulling it — so a
# long-running cron A overlapped by a quick cron B keeps its attribution
# after B exits. _LATEST_PROCESS_CONTEXT mirrors stack top for cheap reads.
_ACTIVE_CONTEXTS: list[str] = []
_LATEST_PROCESS_CONTEXT: str | None = None
_LATEST_PROCESS_CONTEXT_LOCK = threading.Lock()


def _set_process_context_for_tests(ctx: str | None) -> None:
    """Set the process-context ContextVar AND the active-stack mirror in
    one shot — WITHOUT the push/pop bookkeeping of ``process_context_scope``.

    **Test-fixture / introspection helper ONLY. NEVER call from production
    code.** The underscore prefix + name suffix are the contract: the
    public API is ``process_context_scope`` and only that.

    Why this exists at all: the test suite needs to assert what happens
    when the ContextVar is set by a code path that doesn't use the
    context manager (e.g. when validating the fallback's last-writer-wins
    semantics, or simulating a thread that inherits a partial context).
    Test code that emulates "a thread that set the context and exited"
    without doing the work of pushing/popping the active-context stack
    is exactly the failure mode we want to test, so this helper exists
    to enable that test, not to be used in real code.

    In production the plain setter loses the stack semantics that keep
    concurrent cron / request scopes from clobbering each other's mirror
    on exit: a setter call from one scope's body, followed by that
    scope's exit, would null the mirror while a concurrent scope's
    in-flight iothread I/O is still draining → ``untagged:fsspecIO``
    rows in usage_log.
    """
    global _LATEST_PROCESS_CONTEXT
    _PROCESS_CONTEXT.set(ctx)
    with _LATEST_PROCESS_CONTEXT_LOCK:
        _LATEST_PROCESS_CONTEXT = ctx


def get_process_context() -> str | None:
    return _PROCESS_CONTEXT.get()


def get_page_load_id() -> str | None:
    return _PAGE_LOAD_ID.get()


def set_page_load_id(page_load_id: str | None) -> None:
    _PAGE_LOAD_ID.set(page_load_id)


def get_process_context_with_fallback() -> str | None:
    """Return the ContextVar value if set, else the top of the active-context
    stack (process-global). Use this from threads that may not have
    inherited the original setter's ContextVar — fsspec's iothread,
    pyiceberg's ThreadPoolExecutor workers, etc. — where get_process_context()
    would otherwise return None and the telemetry row would land untagged.
    """
    ctx = _PROCESS_CONTEXT.get()
    if ctx is not None:
        return ctx
    with _LATEST_PROCESS_CONTEXT_LOCK:
        return _LATEST_PROCESS_CONTEXT


@contextmanager
def process_context_scope(name: str):
    """Set the process context for the lifetime of the block, then pop.

    Pushes *name* onto the active-context stack and sets the ContextVar.
    On exit: resets the ContextVar (via token) and removes *name* from the
    stack, restoring the previous stack top as the process-global mirror.

    Why a stack instead of a single mirror with CAS-clear: cron jobs run
    concurrently in APScheduler worker threads. A long-running cron A
    (e.g. cron_sync at 500s) overlapped by a short cron B (cron_compact
    at 15s) shares the fsspec iothread. Without the stack, B's exit would
    null out the mirror and A's subsequent I/O would land untagged.

    Pre-fix telemetry on 2026-05-20: 86% of pyiceberg.s3fs rows untagged
    (NULL). First-fix (CAS-clear) telemetry: 80% of pyiceberg.s3fs rows
    tagged "untagged:fsspecIO" — the iothread reading None between cron
    overlaps. The stack keeps the mirror non-None as long as ANY cron is
    active, bounding misattribution to the actual overlap window.

    Residual `untagged:fsspecIO` rows (~30-40 per cron tick, observed
    2026-05-21 09:20-10:56 MDT: 720 rows over 96 min = ~38/cron at 5-min
    cadence) come from a different failure mode the stack can't cover:
    a SOLO cron tick (no concurrent overlap) finishes its
    process_context_scope while fsspec's iothread still has in-flight
    GETs draining. The stack pops to empty AND the ContextVar resets, so
    the in-flight GETs see None on both sources and fall through to
    `untagged:fsspecIO`. Bounded behavior, not a bug — fixing it would
    require either (a) blocking scope exit on fsspec drain (deep
    coupling), or (b) a process-global "last-known-cron" mirror that
    never clears (loses cross-cron isolation). The current shape — at
    least labelled `untagged:fsspecIO` rather than NULL — preserves
    cost attribution to fsspec/manifest reads even when the originating
    cron is ambiguous.
    """
    global _LATEST_PROCESS_CONTEXT
    token = _PROCESS_CONTEXT.set(name)
    # Mirror into the Live Query Monitor attribution ContextVar so cron
    # queries get a kind="cron" row instead of "system". Use snapshot-and-
    # restore rather than token-reset because the cron scope can be entered
    # and exited across context boundaries (asyncio.to_thread, fsspec
    # iothread) which would make ContextVar.reset() raise.
    prev_attribution = None
    attribution_set = False
    try:
        from backend.core.query_attribution import (
            current_attribution,
            derive_from_process_context,
        )

        attribution = derive_from_process_context(name)
        if attribution is not None:
            prev_attribution = current_attribution.get()
            current_attribution.set(attribution)
            attribution_set = True
    except Exception:
        # Attribution wiring is observability, not control flow.
        attribution_set = False
    with _LATEST_PROCESS_CONTEXT_LOCK:
        _ACTIVE_CONTEXTS.append(name)
        _LATEST_PROCESS_CONTEXT = name
    try:
        yield
    finally:
        try:
            _PROCESS_CONTEXT.reset(token)
        except ValueError:
            _PROCESS_CONTEXT.set(None)
        if attribution_set:
            try:
                from backend.core.query_attribution import current_attribution

                current_attribution.set(prev_attribution)
            except Exception:
                logger.debug("process_context_scope: current_attribution.set failed (ignored)", exc_info=True)
        with _LATEST_PROCESS_CONTEXT_LOCK:
            try:
                # Remove the *last* occurrence so nested scopes with the
                # same name (rare but possible) pop in LIFO order.
                for i in range(len(_ACTIVE_CONTEXTS) - 1, -1, -1):
                    if _ACTIVE_CONTEXTS[i] == name:
                        del _ACTIVE_CONTEXTS[i]
                        break
            except ValueError:
                pass
            _LATEST_PROCESS_CONTEXT = _ACTIVE_CONTEXTS[-1] if _ACTIVE_CONTEXTS else None


def start_call_tracking():
    """Initialise call tracking for the current context."""
    _CALLS.set([])
    _QUERIES.set([])
    _SQLITE_QUERIES.set([])
    _REQUEST_START_TS.set(time.time())
    _IOTHREAD_QUERIED.set(False)


def get_tracked_calls() -> list[dict]:
    """Return the list of calls tracked in the current context, augmented
    with iothread/pool FOS/CDN calls captured via usage_log.

    In-thread calls (Fastly API client, in-handler boto3) hit `record_call`
    directly. Iothread calls (fsspec httpfs, pyiceberg pool, boto3
    connection pool) don't propagate the ContextVar so they bypass
    `record_call` — but they DO pass through telemetry_proxy and land in
    usage_log tagged with this request's process_context. We pick them up
    by querying usage_log at end-of-request, gated by usage logging being
    on (the Debug Panel's source of truth becomes usage_log when enabled).
    """
    res = _CALLS.get()
    if res is None:
        # Initialise on demand if start_call_tracking wasn't called (e.g. in some tests)
        res = []
        _CALLS.set(res)
    if not _IOTHREAD_QUERIED.get():
        _IOTHREAD_QUERIED.set(True)
        iothread = _query_iothread_calls_from_usage_log()
        if iothread:
            res = list(res) + iothread
            _CALLS.set(res)
    return res


def _query_iothread_calls_from_usage_log() -> list[dict]:
    """Pull rows from usage_log tagged with the current request's
    process_context since start_call_tracking() ran.

    No-op unless DEBUG_RESPONSES is on (the data is only surfaced via
    _debug_calls, which BaseResponse strips otherwise) AND usage logging
    is enabled AND the request was tagged with an "api:..." process_context.
    Bounded query: capped at 25 rows to keep the response body sub-2KB
    even under cron contention where /api/sync-status?skip_fos=true would
    otherwise see 122KB of iothread spam dragging admin nav from <500ms
    to 5+s (item 23 / commit 5e8b795).

    Visibility lag (item 24 / M5): we DO NOT block on the
    telemetry_proxy coalescer here. Previously this called
    `_flush_log_writes_for_tests(timeout=0.25)` to drain pending rows
    so iothread calls completed mid-request were guaranteed visible
    in the debug panel. Under cron contention that wait routinely
    hit the full 250 ms ceiling — the coalescer was busy serialising
    against cron's own usage_log writes — and a few of those per
    admin nav stacked to 500 ms - 5 s of extra wall time. Removing
    the wait trades up to one batch interval (~100 ms,
    `_LOG_BATCH_MAX_INTERVAL_S`) of visibility for iothread calls
    that completed in the very last slice of the request: those
    calls land in usage_log AFTER this SELECT, so they won't
    appear in this request's debug panel. They are still recorded
    correctly (tagged with this request's process_context) and
    surface in the Admin → Usage Log page for post-hoc inspection.
    """
    try:
        # Gate on DEBUG_RESPONSES — when off, BaseResponse strips
        # _debug_calls anyway, so the SQLite scan is pure overhead.
        from backend.models.common import _debug_responses_enabled

        if not _debug_responses_enabled():
            return []

        start_ts = _REQUEST_START_TS.get()
        if start_ts is None:
            return []
        ctx = get_process_context_with_fallback()
        if not ctx or not ctx.startswith("api:"):
            return []

        from backend import config as svcconfig

        if not svcconfig.is_usage_logging_enabled():
            return []
        sid = svcconfig.get_active_service_id()
        if not sid:
            return []

        from datetime import UTC, datetime

        from backend.core.metadata import usage_log_db as _usage_log_db
        from backend.utils.date_utils import iso_z

        start_iso = iso_z(datetime.fromtimestamp(start_ts, UTC))
        # Raw string compare on timestamp (no datetime() wrapping) so the
        # composite idx_usage_process_context_ts can be used end-to-end.
        # Safe because every row written since commit 08a485c uses
        # iso_z_now() ("YYYY-MM-DDTHH:MM:SSZ"); legacy-format rows would be
        # months old and can't have a timestamp >= a start_iso captured
        # seconds ago, so they're correctly excluded by string comparison.
        # LIMIT 25 caps the response body so an admin nav during a cron
        # tick doesn't drag in 500 rows of iothread spam (~120KB / 5s).
        #
        # Open the usage_log.db in URI mode=ro so this read path cannot
        # acquire the writer lock under any circumstances — a slow scan
        # here is guaranteed not to block the cron writer. The
        # connection is short-lived (closed in the finally below) so
        # we don't pool it.
        try:
            con = _usage_log_db.open_readonly(sid)
        except Exception:
            # File doesn't exist yet (writer hasn't run) — nothing to surface.
            return []
        try:
            cur = con.execute(
                "SELECT operation_type, url, status, duration_ms, function_name, bytes, operation_class "
                "FROM usage_log "
                "WHERE service_id = ? AND process_context = ? AND timestamp >= ? "
                "ORDER BY timestamp ASC LIMIT 25",
                (sid, ctx, start_iso),
            )
            rows = cur.fetchall()
        finally:
            try:
                con.close()
            except Exception:
                logger.debug("_query_iothread_calls_from_usage_log: con.close failed (ignored)", exc_info=True)
        return [
            {
                "service": "CDN" if r[6] == "CDN" else "FOS",
                "method": r[0],
                "path": r[1],
                "status": r[2],
                "time_ms": r[3],
                "caller": r[4],
                "bytes": r[5],
                "details": "iothread (via usage_log)",
            }
            for r in rows
        ]
    except Exception as e:
        logger.debug("[telemetry] iothread call query failed: %s", e)
        return []


def get_queries() -> list[dict]:
    """Return the list of queries tracked in the current context."""
    res = _QUERIES.get()
    if res is None:
        res = []
        _QUERIES.set(res)
    return res


def record_sqlite_query(entry: dict) -> None:
    """Append one profiled SQLite statement to the current request's
    collector. No-op when no request is being tracked (cron / startup /
    test paths that never called start_call_tracking) — those statements
    are still visible in the profiler's process-global ring buffer.

    Works across FastAPI's ``run_in_threadpool`` and ``asyncio.to_thread``
    because both copy the caller's context: the copied ContextVar still
    points at the SAME list object, so appends land in the middleware's
    view. Statements issued from raw worker threads (metadata writer
    thread, fsspec iothread) don't inherit the context and are skipped —
    same documented gap as ``_CALLS``.
    """
    if _RECORDING_TELEMETRY.get():
        return

    entries = _SQLITE_QUERIES.get()
    if entries is not None:
        entries.append(entry)

    record_telemetry_query(
        engine="sqlite",
        sql=entry.get("sql", ""),
        duration_ms=entry.get("time_ms", 0.0),
    )


def get_sqlite_queries() -> list[dict]:
    """Return the SQLite statements recorded for the current request."""
    res = _SQLITE_QUERIES.get()
    if res is None:
        res = []
        _SQLITE_QUERIES.set(res)
    return res


def record_telemetry_query(
    engine: str,
    sql: str,
    duration_ms: float,
    service_id: str | None = None,
    *,
    request_id: str | None = None,
    allow_active_service_fallback: bool = True,
) -> None:
    """Record a DuckDB or ClickHouse query into telemetry_queries when X-Page-Load-ID is active."""
    if _RECORDING_TELEMETRY.get():
        return
    plid = get_page_load_id()
    if not plid:
        return
    token = _RECORDING_TELEMETRY.set(True)
    try:
        sid = service_id
        if not sid and allow_active_service_fallback:
            from backend import config as svcconfig

            sid = svcconfig.get_active_service_id()
        if sid:
            from backend.core.metadata import usage_log

            usage_log.log_telemetry_query(
                service_id=sid,
                page_load_id=plid,
                request_id=request_id or "unknown",
                engine=engine,
                sql=sql,
                duration_ms=duration_ms,
            )
    except Exception as e:
        logger.debug("Failed to record %s query to telemetry: %s", engine, e)
    finally:
        _RECORDING_TELEMETRY.reset(token)


def debug_envelope(res: dict, *, gated: bool, is_cached: bool | None = False, inject_calls: bool = False) -> dict:
    """Attach the ``_debug_*`` / ``_is_cached`` envelope to a plain-dict response.

    Two gating semantics exist on purpose (each caller keeps its own):

    * ``gated=False`` (RUM): ALWAYS inject ``_debug_queries``, ``_debug_sqlite`` and
      ``_debug_calls`` from the request's collectors, mutating ``res`` in place.
    * ``gated=True``: returns a copy; ``_is_cached`` is set unless ``is_cached`` is ``None``
      and, unless ``DEBUG_RESPONSES`` is on, any ``_debug_queries`` / ``_debug_calls`` already
      present are dropped (mirrors ``BaseResponse._strip_debug_when_disabled``). Session scoring
      uses this alone: its stored value already carries the envelope. Admin routes that only
      trace Fastly calls pass ``inject_calls=True`` to also attach ``_debug_calls`` from the
      request's collector when ``DEBUG_RESPONSES`` is on, so the envelope never goes out
      with the flag off. The analyst-facing strip in ``RemoteAccessMiddleware`` is a separate,
      independent guard.
    """
    if not gated:
        res["_debug_queries"] = get_queries()
        res["_debug_sqlite"] = list(get_sqlite_queries())
        res["_debug_calls"] = get_tracked_calls()
        res["_is_cached"] = is_cached
        return res

    from backend.models.common import _debug_responses_enabled

    out = dict(res)
    if is_cached is not None:
        out["_is_cached"] = is_cached
    if not _debug_responses_enabled():
        for key in ("_debug_queries", "_debug_calls", "debug_queries", "debug_calls"):
            out.pop(key, None)
    elif inject_calls:
        out["_debug_calls"] = get_tracked_calls()
    return out


def clear_queries():
    _QUERIES.set([])


def record_call(
    method: str,
    path: str,
    time_ms: float,
    status: int | str | None = None,
    service: str = "Fastly API",
    details: str | None = None,
    caller: str | None = None,
    bytes_count: int | None = None,
):
    """Record a call in the current context."""
    if caller is None:
        try:
            import sys

            # Walk up past telemetry.py, the TrackedClient/Paginator wrappers in
            # duckdb.py, and contextlib so we surface the real application caller.
            # Using sys._getframe() is significantly faster than inspect.stack().
            # Declared as Optional because `frame.f_back` narrows to None at
            # the top of the stack and we reassign it back into the same name.
            from types import FrameType

            frame: FrameType | None = sys._getframe(1)
            while frame:
                code = getattr(frame, "f_code", None)
                if not code:
                    break
                fn = code.co_filename
                if "telemetry.py" in fn or "contextlib.py" in fn or "duckdb.py" in fn:
                    frame = frame.f_back
                    continue
                caller = code.co_name
                break
        except Exception:
            logger.debug("record_call: step failed (ignored)", exc_info=True)

    # Use the un-augmented view: this is the in-thread append path, and
    # triggering the iothread/usage_log query here would add a coalescer
    # flush on every single record_call.
    calls = _CALLS.get()
    if calls is None:
        calls = []
    calls.append(
        {
            "service": service,
            "method": method,
            "path": path,
            "time_ms": round(time_ms, 2),
            "status": status,
            "details": details,
            "caller": caller,
            "bytes": bytes_count,
        }
    )
    _CALLS.set(calls)

    # Phase 1 telemetry bridge — when we're inside a RequestTelemetry section
    # span (or any other start_as_current_span block), surface this call as
    # an OTel span event so the OTel pipeline sees the same external-call
    # attribution the debug panel renders. No-op when no span is current
    # (which is the common case for cron-driven boto3/fsspec hooks that run
    # off the request thread).
    try:
        from opentelemetry import trace as _otel_trace

        _span = _otel_trace.get_current_span()
        if _span is not None and _span.is_recording():
            attrs: dict = {
                "app.call.method": method,
                "app.call.path": path,
                "app.call.time_ms": float(time_ms),
                "app.call.service": service,
            }
            if status is not None:
                attrs["app.call.status"] = str(status)
            if details:
                attrs["app.call.details"] = details
            if caller:
                attrs["app.call.caller"] = caller
            if bytes_count is not None:
                attrs["app.call.bytes"] = int(bytes_count)
            _span.add_event(name="external_call", attributes=attrs)
    except Exception:
        # OTel SDK import or recording failure must never fail the caller —
        # this is best-effort telemetry, not load-bearing.
        logger.debug("record_call: step failed (ignored)", exc_info=True)


class track_query:
    """Context manager to execute and time a DuckDB query, yielding the cursor."""

    def __init__(self, con, query: str, params: list, label: str = "query"):
        self.con = con
        self.query = query
        self.params = params
        self.label = label
        self.t0 = 0

    def __enter__(self):
        self.t0 = time.time()
        return self.con.execute(self.query, self.params)

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = round((time.time() - self.t0) * 1000, 2)

        queries = get_queries()
        queries.append({"sql": self.query.strip(), "time_ms": elapsed, "engine": "DuckDB"})
        _QUERIES.set(queries)

        # New persistent SQLite recording
        record_telemetry_query(
            engine="duckdb",
            sql=self.query.strip(),
            duration_ms=elapsed,
        )

        # Phase 1 telemetry bridge — surface as a span event when inside
        # an active section span.
        try:
            from opentelemetry import trace as _otel_trace

            _span = _otel_trace.get_current_span()
            if _span is not None and _span.is_recording():
                _span.add_event(
                    name="db.query",
                    attributes={
                        "db.statement": self.query.strip()[:4000],
                        "db.elapsed_ms": float(elapsed),
                        "db.label": self.label,
                    },
                )
        except Exception:
            logger.debug("__exit__: step failed (ignored)", exc_info=True)


def is_full_miss(x_cache: str | None) -> bool:
    """Return True if every value in an X-Cache header chain is MISS or PASS.

    Fastly returns chains like "HIT, HIT" (edge HIT, shield HIT — no FOS read),
    "MISS, HIT" (edge MISS but shield served it — no FOS read), "MISS, MISS"
    (full miss — went to FOS), or "PASS" (uncacheable, also went to FOS).
    Only count an FOS op when *all* values in the chain are MISS/PASS.
    """
    if not x_cache:
        return False
    parts = [p.strip().upper() for p in x_cache.split(",") if p.strip()]
    if not parts:
        return False
    return all(p in ("MISS", "PASS") for p in parts)


def build_cdn_miss_synth_details(bytes_count: int | None) -> str:
    """Format the ``details`` string for the synthesised FOS GET_OBJECT row
    written when a CDN read was a full MISS.

    Both consumers (this module's :func:`record_cdn_call` and
    :mod:`backend.utils.telemetry_proxy`) used to compose this string by
    hand with drifted prefixes — one prepended bytes via f-string
    concatenation, the other inlined it into the format. Funnel here so
    both sides stay aligned.
    """
    if bytes_count is not None:
        return f"{bytes_count:,} bytes · Class B · synthesized from CDN MISS"
    return "Class B · synthesized from CDN MISS"


def record_cdn_call(
    method: str,
    key: str,
    elapsed_ms: float,
    headers: dict | None = None,
    bytes_count: int | None = None,
    caller: str | None = None,
    status: str | int | None = "OK",
):
    """Record a CDN GET/HEAD and, when X-Cache shows a full MISS, also record the
    underlying FOS Class B op (GET_OBJECT or HEAD_OBJECT) the CDN had to make.

    `headers` should be a urllib `resp.headers` (or any case-insensitive get())
    so we can read X-Cache. Pass None when headers aren't available — we'll skip
    the MISS-derived FOS op rather than guess.
    """
    x_cache = None
    age = None
    cache_control = None
    if headers is not None:
        try:
            x_cache = headers.get("X-Cache") or headers.get("x-cache")
            age = headers.get("Age") or headers.get("age")
            cache_control = headers.get("Cache-Control") or headers.get("cache-control")
        except Exception:
            logger.debug("record_cdn_call: headers.get failed (ignored)", exc_info=True)

    detail_parts: list[str] = []
    if x_cache:
        detail_parts.append(x_cache)
    if age:
        detail_parts.append(f"age={age}s")
    if cache_control:
        detail_parts.append(cache_control)
    cdn_details = " · ".join(detail_parts) if detail_parts else None

    record_call(
        method,
        key,
        elapsed_ms,
        status=status,
        service="CDN",
        details=cdn_details,
        caller=caller,
        bytes_count=bytes_count,
    )

    if is_full_miss(x_cache):
        # Fastly's typical behavior on a cache MISS against an object-storage
        # origin is to issue a GET to the origin (fetching the full body to
        # populate cache) regardless of whether the client sent HEAD or GET —
        # so subsequent reads of any method hit cache. The real FOS-side op
        # is therefore always GET_OBJECT, not HEAD_OBJECT. (Field-confirmed
        # by tracing single-file ingest paths: client HEAD MISS → CDN GETs
        # populated cache but never resulted in a paired FOS GET_OBJECT row
        # because every CDN GET after the HEAD was a HIT.)
        record_call(
            "GET_OBJECT",
            key,
            elapsed_ms,
            status=status,
            service="FOS",
            details=build_cdn_miss_synth_details(bytes_count),
            caller=caller or "cdn.miss",
            bytes_count=bytes_count,
        )


class tracked_call:
    """Context manager to time and record a call."""

    def __init__(self, method: str, path: str, service: str = "Fastly API", details: str | None = None):
        self.method = method
        self.path = path
        self.service = service
        self.details = details
        self.t0 = 0

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (time.time() - self.t0) * 1000
        status = "Error" if exc_type else "OK"
        record_call(self.method, self.path, elapsed, status=status, service=self.service, details=self.details)


_MAX_KEYS = 1024
_last: dict[str, float] = {}
_log_once_lock = threading.Lock()


def log_once_per(key: str, interval_s: float = 60.0) -> bool:
    """True when ``key`` has not been allowed in the last ``interval_s`` seconds (and records this call)."""
    now = time.monotonic()
    with _log_once_lock:
        prev = _last.get(key)
        if prev is not None and now - prev < interval_s:
            return False
        if len(_last) >= _MAX_KEYS:
            cutoff = now - interval_s
            for k in [k for k, t in _last.items() if t < cutoff]:
                del _last[k]
            if len(_last) >= _MAX_KEYS:
                _last.clear()
        _last[key] = now
        return True


def flush_usage_log(service_id: str) -> None:
    """Write the current context's tracked FOS/CDN calls to _usage_log.

    Safe to call from cron jobs and API middleware — exits silently on any error
    or when usage logging is disabled globally.
    """
    try:
        from backend import config as svcconfig

        if not svcconfig.is_usage_logging_enabled():
            return

        calls = _CALLS.get()
        if not calls:
            return

        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return

        source = svcconfig.config_to_source(cfg)
        process_context = get_process_context()

        from backend.core.duckdb import log_usage_calls

        log_usage_calls(source, calls, process_context=process_context)
    except Exception as e:
        logger.warning("[usage_logger] flush failed: %s", e)


def run_usage_log_cleanup(service_id: str) -> None:
    """Purge old rows from _usage_log per the configured retention period."""
    try:
        from backend import config as svcconfig

        cfg = svcconfig.load_config(service_id)
        if not cfg:
            return
        source = svcconfig.config_to_source(cfg)

        from backend.core.duckdb import purge_usage_log

        purge_usage_log(source)
    except Exception as e:
        logger.warning("[usage_logger] cleanup failed: %s", e)


# ---------------------------------------------------------------------------
# Always-on SQLite/Postgres query profiler ring buffer
# (folded from backend.utils.sqlite_profiler)
# ---------------------------------------------------------------------------

import itertools  # noqa: E402
import sys  # noqa: E402
from collections import deque  # noqa: E402
from datetime import UTC, datetime  # noqa: E402
from typing import Any  # noqa: E402

import structlog  # noqa: E402

_BUFFER_CAP = 1000
_SQL_TRUNCATE = 4096

_buffer: deque[dict[str, Any]] = deque(maxlen=_BUFFER_CAP)
_seq = itertools.count(1)
_dropped = 0


def _record(sql: str, params: Any, duration_ms: float, rowcount: int, op: str) -> None:
    """Append one profiled statement to the ring buffer."""
    global _dropped
    try:
        if _buffer.maxlen and len(_buffer) == _buffer.maxlen:
            _dropped += 1
        entry = {
            "seq": next(_seq),
            "ts": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "sql": _summarize_sql(sql),
            "params_kind": _describe_params(params),
            "time_ms": round(duration_ms, 3),
            "rows": rowcount,
            "op": op,
        }
        _buffer.append(entry)
        record_sqlite_query(entry)
    except Exception:
        logger.debug("sqlite_profiler record failed", exc_info=True)


def _summarize_sql(sql: str) -> str:
    if not isinstance(sql, str):
        sql = str(sql)
    if len(sql) > _SQL_TRUNCATE:
        return sql[:_SQL_TRUNCATE] + f"… [+{len(sql) - _SQL_TRUNCATE} chars]"
    return sql


def _describe_params(params: Any) -> str:
    if params is None:
        return "none"
    if isinstance(params, (list, tuple)):
        return f"seq[{len(params)}]"
    if isinstance(params, dict):
        return f"map[{len(params)}]"
    return type(params).__name__


def _live_register(db_type: str, sql: Any, con: Any) -> int:
    try:
        from backend.core.query_registry import query_registry

        service_id = getattr(con, "_service_id", None)
        qid = query_registry.register(db_type, str(sql), service_id=service_id, con=con)
        if qid >= 0:
            structlog.contextvars.bind_contextvars(query_id=qid)
        return qid
    except Exception:
        logger.debug("live-registry register failed", exc_info=True)
        return -1


def _live_deregister(qid: int, error: BaseException | None) -> None:
    if qid < 0:
        return
    try:
        from backend.core.query_registry import query_registry

        query_registry.deregister(qid, error=error)
    except Exception:
        logger.debug("live-registry deregister failed", exc_info=True)
    finally:
        try:
            structlog.contextvars.unbind_contextvars("query_id")
        except Exception:
            logger.debug("_live_deregister: structlog.contextvars.unbind_contextvars failed (ignored)", exc_info=True)


def get_recent(limit: int = 200, since_seq: int = 0) -> dict[str, Any]:
    """Return up to ``limit`` most-recent profiled statements."""
    snapshot = list(_buffer)
    if since_seq > 0:
        snapshot = [e for e in snapshot if e["seq"] > since_seq]
    if limit and len(snapshot) > limit:
        snapshot = snapshot[-limit:]
    return {
        "queries": snapshot,
        "buffer_size": len(_buffer),
        "buffer_cap": _BUFFER_CAP,
        "dropped": _dropped,
        "last_seq": _buffer[-1]["seq"] if _buffer else 0,
    }


def clear() -> None:
    """Drain the buffer."""
    global _dropped
    _buffer.clear()
    _dropped = 0


def buffer_stats() -> dict[str, int]:
    """Lightweight stats for telemetry / status pages."""
    return {
        "buffer_size": len(_buffer),
        "buffer_cap": _BUFFER_CAP,
        "dropped": _dropped,
    }


clear_recent_sqlite_queries = clear
get_recent_sqlite_queries = get_recent
get_sqlite_stats = buffer_stats


def get_page_telemetry(page_load_id: str | None = None) -> dict[str, Any]:
    from backend import config as svcconfig
    from backend.core.metadata import usage_log

    sid = svcconfig.get_active_service_id()
    if not sid or not page_load_id:
        return {"queries": [], "sections": [], "calls": []}
    return usage_log.get_page_telemetry(sid, page_load_id)


if "pytest" not in sys.modules:
    logger.info("sqlite_profiler active — capturing up to %d recent statements", _BUFFER_CAP)

sys.modules["backend.utils.sqlite_profiler"] = sys.modules[__name__]
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    _utils_pkg.sqlite_profiler = sys.modules[__name__]  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Valkey-backed pub/sub for SSE fan-out across pods
# (folded from backend.utils.valkey_publisher)
# ---------------------------------------------------------------------------

import asyncio  # noqa: E402
import json  # noqa: E402
from collections.abc import AsyncIterator  # noqa: E402

# Failure visibility: keyed counters, cheap to read from a health snapshot.
PUBLISH_STATS: dict[str, int] = {
    "published": 0,
    "publish_errors": 0,
    "decode_errors": 0,
}

_LOG_EVERY_S = 60.0


def _log_rate_limited(key: str, msg: str, *args: Any) -> None:
    if log_once_per(key, _LOG_EVERY_S):
        logger.warning(msg, *args)


_sync_redis_lock = threading.Lock()
_sync_redis: Any = None


def _get_sync_redis() -> Any:
    """One shared sync Redis client per process (publishers + seed reads)."""
    global _sync_redis
    with _sync_redis_lock:
        if _sync_redis is None:
            from redis import Redis as SyncRedis

            broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
            _sync_redis = SyncRedis.from_url(
                broker_url,
                socket_connect_timeout=1.0,
                socket_timeout=2.0,
                decode_responses=True,
            )
        return _sync_redis


class ValkeyPublisher:
    def __init__(self, channel_prefix: str = "sync-status", history_size: int = 0) -> None:
        self._channel_prefix = channel_prefix
        self._history_size = history_size
        self._loop: asyncio.AbstractEventLoop | None = None
        self._redis: Any = None  # async client, bound in bind_loop()

    def _channel(self, service_id: str) -> str:
        return f"sse:{self._channel_prefix}:{service_id}"

    def bind_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        """Bind to the FastAPI asyncio loop and create the async client.
        Optional — processes without a loop (Celery workers) publish through
        the shared sync client instead."""
        from redis.asyncio import Redis

        broker_url = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
        self._loop = loop
        self._redis = Redis.from_url(broker_url, decode_responses=True)

    # ── publish ───────────────────────────────────────────────────────────────

    def publish(self, service_id: str, payload: dict[str, Any]) -> None:
        payload_json = json.dumps(payload)
        channel = self._channel(service_id)

        if self._loop is not None and self._redis is not None:
            self._publish_async(channel, payload_json)
        else:
            self._publish_sync(channel, payload_json)

    def _apply_history(self, pipe: Any, channel: str, payload_json: str) -> None:
        if self._history_size > 0:
            history_key = f"{channel}:history"
            pipe.rpush(history_key, payload_json)
            pipe.ltrim(history_key, -self._history_size, -1)

    def _publish_sync(self, channel: str, payload_json: str) -> None:
        try:
            r = _get_sync_redis()
            pipe = r.pipeline()
            pipe.publish(channel, payload_json)
            self._apply_history(pipe, channel, payload_json)
            pipe.execute()
            PUBLISH_STATS["published"] += 1
        except Exception as e:
            PUBLISH_STATS["publish_errors"] += 1
            _log_rate_limited("publish_sync", "[valkey] publish to %s failed (sync): %s", channel, e)

    def _publish_async(self, channel: str, payload_json: str) -> None:
        async def _do() -> None:
            try:
                if self._redis is None:
                    raise RuntimeError("self._redis is unexpectedly None")
                pipe = self._redis.pipeline()
                pipe.publish(channel, payload_json)
                self._apply_history(pipe, channel, payload_json)
                await pipe.execute()
                PUBLISH_STATS["published"] += 1
            except Exception as e:
                PUBLISH_STATS["publish_errors"] += 1
                _log_rate_limited("publish_async", "[valkey] publish to %s failed: %s", channel, e)

        try:
            if self._loop is None:
                raise RuntimeError("self._loop is unexpectedly None")
            self._loop.call_soon_threadsafe(lambda: asyncio.ensure_future(_do()))
        except RuntimeError as e:
            # Loop shutting down — fall back to the sync client rather than drop.
            _log_rate_limited("loop_closed", "[valkey] loop unavailable (%s); publishing sync", e)
            self._publish_sync(channel, payload_json)

    # ── subscribe ─────────────────────────────────────────────────────────────

    async def subscribe(self, service_id: str) -> AsyncIterator[dict[str, Any]]:
        """Yield published payloads for ``service_id``; reconnects on broker
        loss. Requires ``bind_loop()`` — subscribing is only meaningful in the
        SSE-serving (FastAPI) process."""
        if self._redis is None:
            raise RuntimeError(
                "ValkeyPublisher.subscribe() before bind_loop() — the SSE process "
                "must bind the publisher during lifespan startup"
            )
        channel = self._channel(service_id)
        backoff = 1.0
        while True:
            pubsub = self._redis.pubsub()
            try:
                await pubsub.subscribe(channel)
                backoff = 1.0
                async for message in pubsub.listen():
                    if message["type"] != "message":
                        continue
                    try:
                        yield json.loads(message["data"])
                    except (TypeError, ValueError) as e:
                        PUBLISH_STATS["decode_errors"] += 1
                        _log_rate_limited("decode", "[valkey] undecodable payload on %s: %s", channel, e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                _log_rate_limited("subscribe", "[valkey] subscription to %s lost (%s); reconnecting", channel, e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 15.0)
            finally:
                try:
                    await pubsub.unsubscribe(channel)
                    await pubsub.close()
                except Exception:
                    logger.debug("subscribe: pubsub.close failed (ignored)", exc_info=True)

    # ── seed / introspection (sync callers) ───────────────────────────────────

    def get_recent_ticks(self, service_id: str, count: int = 60) -> list[dict[str, Any]]:
        """Recent history for initial SSE seed. Sync — call via to_thread from
        async routes."""
        if self._history_size == 0:
            return []
        try:
            items = _get_sync_redis().lrange(f"{self._channel(service_id)}:history", -count, -1)
            return [json.loads(item) for item in items]
        except Exception as e:
            _log_rate_limited("history", "[valkey] history read failed for %s: %s", service_id, e)
            return []

    def subscriber_count(self, service_id: str) -> int:
        """Subscribers on this channel across all pods. Sync — call via
        to_thread from async contexts."""
        try:
            result = _get_sync_redis().pubsub_numsub(self._channel(service_id))
            if result:
                return int(result[0][1])
        except Exception as e:
            _log_rate_limited("numsub", "[valkey] subscriber_count failed for %s: %s", service_id, e)
        return 0


sys.modules["backend.utils.valkey_publisher"] = sys.modules[__name__]
if _utils_pkg is not None:
    _utils_pkg.valkey_publisher = sys.modules[__name__]  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Minimal HyperLogLog for cross-hour distinct-IP counts in the rollup tree
# (folded from backend.utils.hll)
# ---------------------------------------------------------------------------

import hashlib  # noqa: E402
import math  # noqa: E402
import struct  # noqa: E402
from collections.abc import Iterable  # noqa: E402

DEFAULT_PRECISION = 8
SERIALIZATION_VERSION = 1
_HASH_BITS = 64


def _alpha(m: int) -> float:
    """Bias-correction constant α_m from the Flajolet paper, table 4."""
    if m == 16:
        return 0.673
    if m == 32:
        return 0.697
    if m == 64:
        return 0.709
    return 0.7213 / (1.0 + 1.079 / m)


def _hash64(item: str | bytes) -> int:
    """Stable 64-bit hash of an item."""
    if isinstance(item, str):
        item = item.encode("utf-8")
    digest = hashlib.md5(item, usedforsecurity=False).digest()  # noqa: S324
    return struct.unpack("<Q", digest[:8])[0]


def _leading_zero_count(value: int, width_bits: int) -> int:
    """Return ``min(width_bits, leading_zero_count(value) + 1)``."""
    if value == 0:
        return width_bits
    rank = 1
    msb = 1 << (width_bits - 1)
    while (value & msb) == 0 and rank < width_bits:
        rank += 1
        value <<= 1
    return rank


class HyperLogLog:
    """A small, mergeable HyperLogLog sketch."""

    __slots__ = ("_p", "_m", "_buckets")

    def __init__(self, precision: int = DEFAULT_PRECISION) -> None:
        if not 4 <= precision <= 16:
            raise ValueError(f"precision must be in [4, 16], got {precision}")
        self._p = precision
        self._m = 1 << precision
        self._buckets = bytearray(self._m)

    @property
    def precision(self) -> int:
        return self._p

    @property
    def m(self) -> int:
        return self._m

    def add(self, item: str | bytes) -> None:
        """Insert one item into the sketch."""
        h = _hash64(item)
        bucket = h >> (_HASH_BITS - self._p)
        observed = h & ((1 << (_HASH_BITS - self._p)) - 1)
        rank = _leading_zero_count(observed, _HASH_BITS - self._p)
        if rank > self._buckets[bucket]:
            self._buckets[bucket] = rank

    def update(self, items: Iterable[str | bytes]) -> None:
        """Bulk-insert from an iterable."""
        for item in items:
            self.add(item)

    def count(self) -> int:
        """Return the cardinality estimate."""
        m = self._m
        alpha = _alpha(m)
        raw_sum = sum(2.0**-b for b in self._buckets)
        if raw_sum == 0.0:
            return m
        estimate = alpha * m * m / raw_sum

        if estimate <= 2.5 * m:
            zeros = sum(1 for b in self._buckets if b == 0)
            if zeros > 0:
                estimate = m * math.log(m / zeros)

        if estimate > (1 << 64) / 30.0:
            estimate = -(1 << 64) * math.log(1 - estimate / (1 << 64))

        return int(round(estimate))

    def merge(self, other: HyperLogLog) -> None:
        """Union ``other`` into ``self`` in-place."""
        if other._p != self._p:
            raise ValueError(f"merge precision mismatch: self.p={self._p} vs other.p={other._p}")
        for i in range(self._m):
            if other._buckets[i] > self._buckets[i]:
                self._buckets[i] = other._buckets[i]

    def to_bytes(self) -> bytes:
        """Serialize to a compact ``bytes`` payload."""
        return bytes([SERIALIZATION_VERSION, self._p]) + bytes(self._buckets)

    @classmethod
    def from_bytes(cls, blob: bytes | bytearray) -> HyperLogLog:
        """Deserialize a sketch previously produced by :meth:`to_bytes`."""
        if len(blob) < 2:
            raise ValueError(f"sketch blob too short: {len(blob)} bytes")
        version = blob[0]
        if version != SERIALIZATION_VERSION:
            raise ValueError(f"unknown HLL serialization version {version}; expected {SERIALIZATION_VERSION}")
        precision = blob[1]
        if not 4 <= precision <= 16:
            raise ValueError(f"invalid precision in blob: {precision}")
        expected_len = 2 + (1 << precision)
        if len(blob) != expected_len:
            raise ValueError(f"sketch blob length mismatch: got {len(blob)}, expected {expected_len}")
        sketch = cls(precision=precision)
        sketch._buckets = bytearray(blob[2:])
        return sketch

    def __len__(self) -> int:
        return self.count()

    def __repr__(self) -> str:  # pragma: no cover - cosmetic only
        return f"HyperLogLog(p={self._p}, estimate≈{self.count()})"


HLL_SKETCH_NAMES: tuple[str, ...] = ("hll_sketch",)


def get_hll_sketch(precision: int = DEFAULT_PRECISION) -> HyperLogLog:
    return HyperLogLog(precision=precision)


def merge_hll_sketches(blobs: Iterable[bytes | bytearray], precision: int = DEFAULT_PRECISION) -> int:
    acc: HyperLogLog | None = None
    for blob in blobs:
        if not blob:
            continue
        sketch = HyperLogLog.from_bytes(blob)
        if acc is None:
            acc = sketch
        else:
            acc.merge(sketch)
    return acc.count() if acc is not None else 0


def query_hll_distinct(blobs: Iterable[bytes | bytearray]) -> int:
    return merge_hll_sketches(blobs)


def query_hll_distinct_by_bucket(rows: Iterable[tuple[Any, bytes | bytearray]]) -> dict[Any, int]:
    buckets: dict[Any, HyperLogLog] = {}
    for key, blob in rows:
        if not blob:
            continue
        sketch = HyperLogLog.from_bytes(blob)
        if key not in buckets:
            buckets[key] = sketch
        else:
            buckets[key].merge(sketch)
    return {k: v.count() for k, v in buckets.items()}


def query_hll_distinct_filtered(blobs: Iterable[bytes | bytearray]) -> int:
    return merge_hll_sketches(blobs)


sys.modules["backend.utils.hll"] = sys.modules[__name__]
if _utils_pkg is not None:
    _utils_pkg.hll = sys.modules[__name__]  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Relative-range window resolver (folded from backend.utils.time_window)
# ---------------------------------------------------------------------------

from datetime import timedelta  # noqa: E402

from backend.utils.date_utils import iso_z, parse_iso_utc  # noqa: E402

STATIC_DEFAULT: tuple[float, float] = (24.0, 168.0)

_BANDS: tuple[tuple[float, float, float], ...] = (
    (1.0, 0.25, 1.0),
    (4.0, 1.0, 1.0),
    (24.0, 4.0, 1.0),
    (48.0, 4.0, 24.0),
    (168.0, 24.0, 24.0),
    (math.inf, 24.0, 168.0),
)


def history_hours_from_earliest(earliest: str | None, *, now: datetime | None = None) -> float | None:
    """Hours of history available = ``now − earliest_log_at``, floored at 0."""
    dt = parse_iso_utc(earliest)
    if dt is None:
        return None
    anchor = now if now is not None else datetime.now(UTC)
    return max(0.0, (anchor - dt).total_seconds() / 3600.0)


def pick_insights_default(history_hours: float | None) -> tuple[float, float]:
    """Map available history to the default ``(window_hours, baseline_hours)``."""
    if history_hours is None or not math.isfinite(history_hours):
        return STATIC_DEFAULT
    for upper, window, baseline in _BANDS:
        if history_hours < upper:
            return (window, baseline)
    return STATIC_DEFAULT


ANCHOR_QUANTUM_SECONDS = 60

_FIXED_TOKEN_DELTAS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "3h": timedelta(hours=3),
    "6h": timedelta(hours=6),
    "12h": timedelta(hours=12),
    "24h": timedelta(hours=24),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}

VALID_RANGE_TOKENS: frozenset[str] = frozenset(_FIXED_TOKEN_DELTAS) | {"auto"}


def is_valid_range_token(token: str | None) -> bool:
    """True when ``token`` is a recognized relative-range token."""
    return token in VALID_RANGE_TOKENS


def quantize_anchor(anchor: str | None, *, now: datetime | None = None) -> str:
    """Floor an anchor instant to the ``ANCHOR_QUANTUM_SECONDS`` grid → ISO-Z."""
    dt = parse_iso_utc(anchor) if anchor else None
    if dt is None:
        dt = now if now is not None else datetime.now(UTC)
    epoch = int(dt.timestamp())
    floored = epoch - (epoch % ANCHOR_QUANTUM_SECONDS)
    return iso_z(datetime.fromtimestamp(floored, tz=UTC))


def _pick_auto_token(earliest_log_at: str | None, *, now: datetime) -> str:
    """Resolve ``"auto"`` to a concrete fixed token from the service's history."""
    history_hours = history_hours_from_earliest(earliest_log_at, now=now)
    if history_hours is None:
        return "7d"
    if history_hours < 7 * 24:
        return "24h"
    if history_hours < 30 * 24:
        return "7d"
    return "30d"


def resolve_window(
    range_token: str | None,
    anchor: str | None,
    *,
    earliest_log_at: str | None = None,
    now: datetime | None = None,
) -> tuple[str, str]:
    """Resolve ``(range_token, anchor) -> (start_iso, end_iso)`` deterministically."""
    if not is_valid_range_token(range_token):
        raise ValueError(f"unrecognized range_token: {range_token!r}")

    now_dt = now if now is not None else datetime.now(UTC)
    quantized = quantize_anchor(anchor, now=now_dt)
    anchor_dt = parse_iso_utc(quantized)
    if anchor_dt is None:
        raise RuntimeError("anchor_dt is unexpectedly None")

    if range_token == "auto":
        token = _pick_auto_token(earliest_log_at, now=now_dt)
    else:
        if range_token is None:
            raise RuntimeError("range_token is unexpectedly None")
        token = range_token

    delta = _FIXED_TOKEN_DELTAS[token]
    start_dt = anchor_dt - delta
    return iso_z(start_dt), iso_z(anchor_dt)


def invite_clamp_fingerprint(analyst_session: object | None) -> str | None:
    """Stable cache-key fragment identifying the analyst's invite-clamp shape."""
    if analyst_session is None:
        return None
    qs = getattr(analyst_session, "query_start_time", None)
    qe = getattr(analyst_session, "query_end_time", None)
    qw = getattr(analyst_session, "query_window_hours", None)
    raw = f"{qs or ''}|{qe or ''}|{qw or ''}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Thread-safe LRU+TTL cache & CacheRegistry (folded from backend.utils.bounded_cache)
# ---------------------------------------------------------------------------

from collections.abc import Iterator  # noqa: E402

import cachetools  # noqa: E402

_MISSING = object()


class BoundedTTLCache:
    """Thread-safe LRU+TTL cache backed by ``cachetools.TTLCache``."""

    def __init__(
        self,
        maxsize: int | None = None,
        ttl_seconds: float = 60.0,
        *,
        max_entries: int | None = None,
        name: str | None = None,
    ) -> None:
        resolved_max = max_entries if max_entries is not None else (256 if maxsize is None else maxsize)
        if resolved_max <= 0:
            raise ValueError("maxsize must be positive")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be positive")
        self.maxsize = resolved_max
        self.ttl = float(ttl_seconds)
        self.ttl_seconds = float(ttl_seconds)
        self.name = name
        self._cache: cachetools.TTLCache = cachetools.TTLCache(
            maxsize=resolved_max,
            ttl=float(ttl_seconds),
            timer=lambda: time.monotonic(),
        )
        self.lock = threading.RLock()

    def __contains__(self, key: Any) -> bool:
        with self.lock:
            return key in self._cache

    def __getitem__(self, key: Any) -> Any:
        with self.lock:
            return self._cache[key]

    def get(self, key: Any, default: Any = None) -> Any:
        with self.lock:
            return self._cache.get(key, default)

    def __setitem__(self, key: Any, value: Any) -> None:
        with self.lock:
            self._cache[key] = value

    def set(self, key: Any, value: Any) -> None:
        self[key] = value

    def put(self, key: Any, value: Any) -> None:
        self[key] = value

    def __delitem__(self, key: Any) -> None:
        with self.lock:
            del self._cache[key]

    def delete(self, key: Any) -> bool:
        with self.lock:
            return self._cache.pop(key, _MISSING) is not _MISSING

    def pop(self, key: Any, default: Any = _MISSING) -> Any:
        with self.lock:
            if default is _MISSING:
                return self._cache.pop(key)
            return self._cache.pop(key, default)

    def clear(self) -> None:
        with self.lock:
            self._cache.clear()

    def __len__(self) -> int:
        with self.lock:
            return len(self._cache)

    def __iter__(self) -> Iterator[Any]:
        with self.lock:
            return iter(list(self._cache.keys()))

    def keys(self) -> list[Any]:
        with self.lock:
            return list(self._cache.keys())

    def values(self) -> list[Any]:
        with self.lock:
            return list(self._cache.values())

    def items(self) -> list[tuple[Any, Any]]:
        with self.lock:
            return list(self._cache.items())


BoundedCache = BoundedTTLCache


class CacheRegistry:
    """Singleton-style registry of module-level caches."""

    _entries: dict[str, Any] = {}
    _lock = threading.Lock()

    @classmethod
    def register(cls, name: str, cache: Any) -> None:
        with cls._lock:
            cls._entries[name] = cache

    @classmethod
    def clear_all(cls) -> None:
        with cls._lock:
            entries = list(cls._entries.items())
        for _name, cache in entries:
            clear_fn = getattr(cache, "clear", None)
            if callable(clear_fn):
                try:
                    clear_fn()
                except Exception:  # pragma: no cover — best-effort drain
                    continue

    @classmethod
    def clear(cls, name: str) -> bool:
        with cls._lock:
            cache = cls._entries.get(name)
        if cache is None:
            return False
        clear_fn = getattr(cache, "clear", None)
        if not callable(clear_fn):
            return False
        try:
            clear_fn()
            return True
        except Exception:  # pragma: no cover — best-effort drain
            return False

    @classmethod
    def names(cls) -> list[str]:
        with cls._lock:
            return list(cls._entries.keys())

    @classmethod
    def reset(cls) -> None:
        with cls._lock:
            cls._entries.clear()


# ---------------------------------------------------------------------------
# Backstop telemetry response middleware & active request counter
# (folded from backend.utils.telemetry_response_middleware & active_requests)
# ---------------------------------------------------------------------------

import os as _os  # noqa: E402
from collections.abc import Callable  # noqa: E402

from starlette.middleware.base import BaseHTTPMiddleware  # noqa: E402
from starlette.requests import Request  # noqa: E402
from starlette.responses import Response  # noqa: E402

_JSON_CONTENT_TYPES: tuple[str, ...] = ("application/json",)
_STREAMING_CONTENT_TYPES: tuple[str, ...] = (
    "text/event-stream",
    "application/octet-stream",
    "application/x-ndjson",
    "application/jsonl",
)


def _content_type(response: Response) -> str:
    return (response.headers.get("content-type") or response.media_type or "").lower()


def _is_json_response(response: Response) -> bool:
    """True iff the response's Content-Type identifies it as JSON."""
    media = _content_type(response)
    return any(media.startswith(t) for t in _JSON_CONTENT_TYPES)


def _is_streaming_content_type(response: Response) -> bool:
    media = _content_type(response)
    return any(media.startswith(t) for t in _STREAMING_CONTENT_TYPES)


class TelemetryResponseBodyMiddleware(BaseHTTPMiddleware):
    """Inject telemetry into JSON dict responses that lack it."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        try:
            from backend.models.common import _DEBUG_ENVELOPE_KEYS, _debug_responses_enabled
        except Exception:
            return response

        master_enabled = _debug_responses_enabled()

        is_remote = getattr(request.state, "is_remote", False)
        analyst_query_enabled = analyst_api_enabled = False
        if is_remote and master_enabled:
            from backend.utils.remote_access import analyst_debug_visibility

            analyst_query_enabled, analyst_api_enabled = analyst_debug_visibility(request)
        if _is_streaming_content_type(response):
            return response
        if not _is_json_response(response):
            return response

        opt_in = master_enabled and (
            request.headers.get("x-debug-responses") == "1" or _os.getenv("DEBUG_RESPONSES_FORCE_INCLUDE") == "1"
        )
        if is_remote and opt_in and getattr(request.state, "_analyst_envelope_complete", False):
            return response

        try:
            body_chunks: list[bytes] = []
            async for chunk in response.body_iterator:
                body_chunks.append(chunk)
            body = b"".join(body_chunks)
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to read response body: %s", e)
            return response

        if not body:
            return _reconstruct(response, body)

        try:
            parsed: Any = json.loads(body)
        except (ValueError, json.JSONDecodeError) as e:
            logger.debug("[telemetry-middleware] body not JSON-parseable: %s", e)
            return _reconstruct(response, body)

        if not isinstance(parsed, dict):
            return _reconstruct(response, body)

        if not opt_in:
            stripped = False
            keys_to_strip = (*_DEBUG_ENVELOPE_KEYS, "_is_cached") if master_enabled else _DEBUG_ENVELOPE_KEYS
            for k in keys_to_strip:
                if k in parsed:
                    parsed.pop(k)
                    stripped = True
            if not stripped:
                return _reconstruct(response, body)
            try:
                new_body = json.dumps(parsed, default=str).encode("utf-8")
            except Exception as e:
                logger.warning("[telemetry-middleware] failed to strip telemetry: %s", e)
                return _reconstruct(response, body)
            return _reconstruct(response, new_body)

        if "_debug_queries" in parsed:
            return _reconstruct(response, body)

        try:
            inject_queries = analyst_query_enabled if is_remote else True
            inject_api_calls = analyst_api_enabled if is_remote else True

            if inject_queries:
                parsed["_debug_queries"] = get_queries()
                pg_queries = list(get_sqlite_queries())
                parsed["_debug_postgres"] = pg_queries
                parsed["_debug_sqlite"] = pg_queries
            if inject_api_calls:
                parsed["_debug_calls"] = get_tracked_calls()

            if not is_remote or inject_queries or inject_api_calls:
                parsed.setdefault("_is_cached", False)
            new_body = json.dumps(parsed, default=str).encode("utf-8")
        except Exception as e:
            logger.warning("[telemetry-middleware] failed to inject telemetry: %s", e)
            return _reconstruct(response, body)

        return _reconstruct(response, new_body)


def _reconstruct(original: Response, body: bytes) -> Response:
    """Build a new ``Response`` from ``body`` with the original's status code, media type, and headers."""
    drop = (b"content-length", b"content-encoding", b"content-type")
    media_type = original.media_type
    if media_type is None:
        for k, v in original.raw_headers:
            if k.lower() == b"content-type":
                try:
                    media_type = v.decode("ascii")
                except UnicodeDecodeError:
                    pass
                break
    new = Response(content=body, status_code=original.status_code, media_type=media_type)
    new.raw_headers.extend((k, v) for k, v in original.raw_headers if k.lower() not in drop)
    return new


_active_requests_logger = logging.getLogger("backend.utils.active_requests")

_lock = threading.Lock()
_active_count = 0
_defer_started_at: dict[tuple[str, str], float] = {}


def increment_active_requests() -> None:
    global _active_count
    with _lock:
        _active_count += 1


def decrement_active_requests() -> None:
    global _active_count
    with _lock:
        if _active_count > 0:
            _active_count -= 1


def active_request_count() -> int:
    """Snapshot read of the in-flight count. Mostly useful for tests."""
    with _lock:
        return _active_count


def should_defer_cron(job_name: str, service_id: str, *, max_defer_secs: float = 30.0) -> bool:
    """Returns True if the cron tick should be SKIPPED for now."""
    key = (job_name, service_id)
    with _lock:
        if _active_count == 0:
            _defer_started_at.pop(key, None)
            return False
        now = time.monotonic()
        started = _defer_started_at.get(key)
        if started is None:
            _defer_started_at[key] = now
            return True
        if (now - started) >= max_defer_secs:
            _defer_started_at.pop(key, None)
            _active_requests_logger.warning(
                "[active-request-gate] %s/%s ran after %.1fs of active-request deferrals — "
                "sustained traffic on this service; consider raising max_defer_secs.",
                job_name,
                service_id,
                now - started,
            )
            return False
        return True


def yield_to_api(
    *,
    max_wait_secs: float = 1.0,
    poll_interval: float = 0.1,
    sleep_fn: Callable[[float], None] | None = None,
) -> float:
    """Cooperative yield: if API requests are in flight, sleep briefly to let them make progress."""
    if active_request_count() == 0:
        return 0.0
    sleep = sleep_fn or time.sleep
    deadline = time.monotonic() + max_wait_secs
    slept = 0.0
    while time.monotonic() < deadline:
        sleep(poll_interval)
        slept += poll_interval
        if active_request_count() == 0:
            return slept
    return slept


def _reset_for_tests() -> None:
    """Tests: clear counter + defer windows between cases."""
    global _active_count
    with _lock:
        _active_count = 0
        _defer_started_at.clear()


class _ActiveRequestsResetAdapter:
    """Exposes ``.clear()`` → ``_reset_for_tests()`` for CacheRegistry."""

    @staticmethod
    def clear() -> None:
        _reset_for_tests()


CacheRegistry.register("utils.active_requests", _ActiveRequestsResetAdapter())


# ---------------------------------------------------------------------------
# structlog configuration, atomic file writes & error redaction
# (folded from backend.utils.structlog_config, atomic_write, error_redaction)
# ---------------------------------------------------------------------------

import re  # noqa: E402
import tempfile  # noqa: E402
from collections.abc import MutableMapping  # noqa: E402
from contextlib import suppress  # noqa: E402
from pathlib import Path  # noqa: E402

from opentelemetry import trace  # noqa: E402


def _add_otel_trace_context(_logger: Any, _method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """structlog processor: inject active OTel trace_id + span_id."""
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        ctx = span.get_span_context()
        if ctx.is_valid:
            event_dict["trace_id"] = format(ctx.trace_id, "032x")
            event_dict["span_id"] = format(ctx.span_id, "016x")
    return event_dict


def _drop_uvicorn_color_message(
    _logger: Any, _method_name: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """structlog processor: drop uvicorn's internal ``color_message`` extra."""
    event_dict.pop("color_message", None)
    return event_dict


def effective_format() -> str:
    """The renderer that's actually active: ``"json"`` or ``"console"``."""
    return "json" if os.environ.get("STRUCTLOG_FORMAT", "console").lower() == "json" else "console"


def configure_structlog() -> None:
    """Configure structlog + bridge stdlib through it. Idempotent."""
    use_json = os.environ.get("STRUCTLOG_FORMAT", "console").lower() == "json"

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_otel_trace_context,
        structlog.processors.StackInfoRenderer(),
    ]

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if use_json
        else structlog.dev.ConsoleRenderer(
            colors=sys.stderr.isatty(),
            sort_keys=True,
            exception_formatter=structlog.dev.RichTracebackFormatter(
                color_system="truecolor" if sys.stderr.isatty() else None,
                show_locals=False,
            ),
        )
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[*shared_processors, structlog.stdlib.ExtraAdder(), _drop_uvicorn_color_message],
        processor=renderer,
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)


_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def bridge_uvicorn_loggers() -> None:
    """Route uvicorn's loggers through the shared structlog root handler."""
    for name in _UVICORN_LOGGERS:
        uv_logger = logging.getLogger(name)
        uv_logger.handlers = []
        uv_logger.propagate = True


audit_log = structlog.get_logger("audit")


@contextmanager
def atomic_temp_path(
    target: str | Path,
    *,
    prefix: str | None = None,
    suffix: str = ".tmp",
    mode: int | None = None,
) -> Iterator[Path]:
    """Yield a sibling temporary file path that is atomically replaced onto ``target`` on exit."""
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=prefix if prefix is not None else f".{dst.name}.",
        suffix=suffix,
        dir=str(dst.parent),
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        if mode is not None:
            os.chmod(tmp_path, mode)
        yield tmp_path
        if tmp_path.exists():
            os.replace(tmp_path, dst)
    finally:
        with suppress(OSError):
            if tmp_path.exists():
                tmp_path.unlink()


def atomic_write_bytes(
    target: str | Path,
    data: bytes,
    *,
    fsync: bool = False,
    mode: int | None = None,
    prefix: str | None = None,
    suffix: str = ".tmp",
    fallback_in_place_errnos: set[int] | None = None,
) -> None:
    """Atomically write ``data`` to ``target`` via a sibling temporary file and ``os.replace``."""
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        prefix=prefix if prefix is not None else f".{dst.name}.",
        suffix=suffix,
        dir=str(dst.parent),
    )
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            if fsync:
                f.flush()
                os.fsync(f.fileno())
        if mode is not None:
            os.chmod(tmp_name, mode)
        os.replace(tmp_name, dst)
    except OSError as e:
        with suppress(OSError):
            os.unlink(tmp_name)
        err_msg = str(e).lower()
        if fallback_in_place_errnos and (
            e.errno in fallback_in_place_errnos or "busy" in err_msg or "permission" in err_msg
        ):
            with open(dst, "wb") as f:
                f.write(data)
                if fsync:
                    f.flush()
                    os.fsync(f.fileno())
            return
        raise
    except BaseException:
        with suppress(OSError):
            os.unlink(tmp_name)
        raise


def atomic_write_json(
    target: str | Path,
    data: Any,
    *,
    indent: int | None = 2,
    sort_keys: bool = False,
    separators: tuple[str, str] | None = None,
    default: Callable[[Any], Any] | None = None,
    fsync: bool = False,
    prefix: str | None = None,
    suffix: str = ".tmp",
    fallback_in_place_errnos: set[int] | None = None,
) -> None:
    """Serialize ``data`` as UTF-8 JSON and atomically write it to ``target``."""
    payload = json.dumps(
        data,
        indent=indent,
        sort_keys=sort_keys,
        separators=separators,
        default=default,
    ).encode("utf-8")
    atomic_write_bytes(
        target,
        payload,
        fsync=fsync,
        prefix=prefix,
        suffix=suffix,
        fallback_in_place_errnos=fallback_in_place_errnos,
    )


ABS_PATH_RE = re.compile(r"(?<![A-Za-z0-9])/(?:srv|var|tmp|data|home|opt|usr|mnt|app)\b(?:/[A-Za-z0-9_./-]*)?")
CID_SALT_LITERAL_RE = re.compile(r"(sha256\(\s*CONCAT\(\s*')[^']*(')", re.IGNORECASE)

REDACTED = "[redacted]"
_MIN_SALT_LEN = 8
_known_salts: set[str] = set()
_salts_lock = threading.Lock()


def register_salt(salt: str | None) -> None:
    """Remember cid salts so their literal values are scrubbed from later text."""
    if isinstance(salt, str) and len(salt) >= _MIN_SALT_LEN:
        with _salts_lock:
            _known_salts.add(salt)


def scrub_secrets(text: str) -> str:
    with _salts_lock:
        salts = sorted(_known_salts, key=len, reverse=True)
    for salt in salts:
        text = text.replace(salt, REDACTED)
    return CID_SALT_LITERAL_RE.sub(r"\1[redacted-salt]\2", text)


def redact_error_text(msg: object) -> str:
    """Strip absolute server paths and cid-salt material from analyst-visible text."""
    return ABS_PATH_RE.sub("<path>", scrub_secrets(str(msg)))


_FOLDED_TELEMETRY_SUBMODULES: tuple[str, ...] = (
    "sqlite_profiler",
    "valkey_publisher",
    "hll",
    "log_throttle",
    "usage_logger",
    "time_window",
    "bounded_cache",
    "cache_registry",
    "telemetry_response_middleware",
    "active_requests",
    "structlog_config",
    "atomic_write",
    "error_redaction",
)
_this_telemetry_mod = sys.modules[__name__]
for _submod_name in _FOLDED_TELEMETRY_SUBMODULES:
    sys.modules[f"backend.utils.{_submod_name}"] = _this_telemetry_mod
    if _utils_pkg is not None:
        setattr(_utils_pkg, _submod_name, _this_telemetry_mod)

_backend_pkg = sys.modules.get("backend")
if _backend_pkg is not None and hasattr(_backend_pkg, "_MODULE_ALIASES"):
    _backend_pkg._MODULE_ALIASES.update(
        {f"backend.utils.{_submod_name}": "backend.utils.telemetry" for _submod_name in _FOLDED_TELEMETRY_SUBMODULES}
    )
