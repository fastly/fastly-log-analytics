from __future__ import annotations

import logging
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar

logger = logging.getLogger(__name__)

# Global context for tracking API and FOS calls during a request
_CALLS: ContextVar[list[dict] | None] = ContextVar("_CALLS", default=None)
_QUERIES: ContextVar[list[dict] | None] = ContextVar("_QUERIES", default=None)

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


def set_process_context(ctx: str | None) -> None:
    global _LATEST_PROCESS_CONTEXT
    _PROCESS_CONTEXT.set(ctx)
    with _LATEST_PROCESS_CONTEXT_LOCK:
        _LATEST_PROCESS_CONTEXT = ctx


def get_process_context() -> str | None:
    return _PROCESS_CONTEXT.get()


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
    with _LATEST_PROCESS_CONTEXT_LOCK:
        _ACTIVE_CONTEXTS.append(name)
        _LATEST_PROCESS_CONTEXT = name
    try:
        yield
    finally:
        _PROCESS_CONTEXT.reset(token)
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

        from backend.core import metadata_db

        start_iso = datetime.fromtimestamp(start_ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        # Raw string compare on timestamp (no datetime() wrapping) so the
        # composite idx_usage_process_context_ts can be used end-to-end.
        # Safe because every row written since commit 08a485c uses
        # iso_z_now() ("YYYY-MM-DDTHH:MM:SSZ"); legacy-format rows would be
        # months old and can't have a timestamp >= a start_iso captured
        # seconds ago, so they're correctly excluded by string comparison.
        # LIMIT 25 caps the response body so an admin nav during a cron
        # tick doesn't drag in 500 rows of iothread spam (~120KB / 5s).
        con = metadata_db.get_con(sid)
        cur = con.execute(
            "SELECT operation_type, url, status, duration_ms, function_name, bytes, operation_class "
            "FROM usage_log "
            "WHERE process_context = ? AND timestamp >= ? "
            "ORDER BY timestamp ASC LIMIT 25",
            (ctx, start_iso),
        )
        rows = cur.fetchall()
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
            frame = sys._getframe(1)
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
            pass

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
        queries.append({"sql": self.query.strip(), "time_ms": elapsed})
        _QUERIES.set(queries)


def _is_full_miss(x_cache: str | None) -> bool:
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
            pass

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

    if _is_full_miss(x_cache):
        # Fastly's typical behavior on a cache MISS against an object-storage
        # origin is to issue a GET to the origin (fetching the full body to
        # populate cache) regardless of whether the client sent HEAD or GET —
        # so subsequent reads of any method hit cache. The real FOS-side op
        # is therefore always GET_OBJECT, not HEAD_OBJECT. (Field-confirmed
        # by tracing single-file ingest paths: client HEAD MISS → CDN GETs
        # populated cache but never resulted in a paired FOS GET_OBJECT row
        # because every CDN GET after the HEAD was a HIT.)
        synth_details = "Class B · synthesized from CDN MISS"
        if bytes_count is not None:
            synth_details = f"{bytes_count:,} bytes · Class B · synthesized from CDN MISS"
        record_call(
            "GET_OBJECT",
            key,
            elapsed_ms,
            status=status,
            service="FOS",
            details=synth_details,
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
