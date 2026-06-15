"""Shared utilities for routers.

- ``query_errors``: decorator that wraps a route handler in a standard
  try/except and raises HTTPException on failure.
- ``SSE_HEADERS``: standard Server-Sent Events response headers.
- ``sync_admin_state``: fire-and-forget state export after mutations.
- ``format_debug_request``: format outbound HTTP request metadata for debug output.
"""

from __future__ import annotations

import logging
import uuid
from functools import wraps
from logging import Logger
from typing import NoReturn

from fastapi import HTTPException

logger = logging.getLogger(__name__)


def start_or_resume_cron(
    source: dict,
    task: str,
    target,
    *,
    target_kwargs: dict | None = None,
    success_msg: str = "",
    in_progress_msg: str = "",
) -> dict:
    """Start a cron task in a daemon thread; resume the active run if already
    in progress; surface 503 with ``busy: True`` if neither path matches.

    Consolidates the 3 hand-rolled copies of this routine across
    ``backend.routers.admin.ingest`` (metadata_sync + sync) and
    ``backend.routers.admin.iceberg`` (commit). Each used to duplicate the
    same ``try: start_cron_run -> spawn thread -> return {ok}; except
    RuntimeError: scan list_active_runs -> return {ok, in_progress_msg};
    else: raise HTTPException(503)`` shape with only the task name +
    target kwargs varying.
    """
    import threading

    from backend.core.duckdb import start_cron_run
    from backend.cron_progress import list_active_runs, start_progress

    service_id = source["name"]
    try:
        run_id = start_cron_run(source, task)
        start_progress(run_id, service_id=service_id, task=task)
        threading.Thread(
            target=target,
            args=(service_id,),
            kwargs={"run_id": run_id, **(target_kwargs or {})},
            daemon=True,
        ).start()
        return {"ok": True, "message": success_msg, "run_id": run_id}
    except RuntimeError as e:
        for entry in list_active_runs():
            if entry.get("service_id") == service_id and entry.get("task") == task:
                return {"ok": True, "message": in_progress_msg, "run_id": entry["run_id"]}
        raise HTTPException(status_code=503, detail={"error": str(e), "busy": True}) from e


def load_service_config(service_id: str) -> dict:
    """Load a service's config or raise :class:`HTTPException` 404.

    The ``cfg = svcconfig.load_config(service_id); if not cfg: raise
    HTTPException(404, ...)`` preamble was written 16+ times across the
    router tree with two existing drift cases (a ``raise ValueError`` at
    services/core.py:87, a JSON-encoded SSE error yield at
    services/core.py:874). One funnel removes both drift surfaces and
    the per-call boilerplate.

    Callers that intentionally want the "empty-dict fallback on missing
    config" semantic (``load_config(service_id) or {}``) should keep
    calling ``load_config`` directly — this helper is for the strict
    "service must exist or 404" path that is the common case in
    request-time routes.
    """
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        # Keep the exact ``detail={"error": "Service not found"}`` shape
        # the migrated callers used — frontend code keys on this exact
        # message via ``error.detail.error === "Service not found"``.
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    return cfg


def raise_internal(
    log: Logger,
    exc: BaseException,
    *,
    code: str = "request_failed",
    status: int = 500,
) -> NoReturn:
    """Log the full exception server-side; raise a generic ``HTTPException``
    that does NOT echo the original exception message to the client.

    Use at except sites that previously did
    ``raise HTTPException(status_code=500, detail={"error": str(e)})`` —
    that pattern leaks upstream API response bodies (e.g. Fastly error
    text interpolated by ``backend.core.fastly.client.fastly()``) to the
    caller. ``error_id`` lets operators correlate a client report with
    the matching server-log line.
    """
    error_id = uuid.uuid4().hex[:8]
    log.exception("%s [error_id=%s]", code, error_id)
    raise HTTPException(
        status_code=status,
        detail={"error": code, "error_id": error_id},
    ) from exc


# ── Debug request formatting ──────────────────────────────────────────────────

_SENSITIVE_HEADERS = frozenset({"fastly-key", "authorization", "x-api-key", "x-api-token"})


def _obfuscate_header(name: str, value: str) -> str:
    if name.lower() in _SENSITIVE_HEADERS:
        return f"***{value[-4:]}" if len(value) >= 4 else "***"
    return value


def format_debug_request(
    method: str,
    url: str,
    headers: dict[str, str] | None = None,
    query: dict[str, str] | None = None,
) -> str:
    """Return a human-readable representation of an outbound request for debug output.

    Sensitive header values (API keys, auth tokens) are obfuscated.
    """
    lines = ["--- Request ---", f"{method} {url}"]
    if headers:
        for k, v in headers.items():
            lines.append(f"  {k}: {_obfuscate_header(k, v)}")
    if query:
        parts = [f"{k}={_obfuscate_header(k, v)}" for k, v in query.items()]
        lines.append(f"  QueryString: {'&'.join(parts)}")
    return "\n".join(lines)


# ── SSE ───────────────────────────────────────────────────────────────────────

SSE_HEADERS: dict[str, str] = {
    "Content-Type": "text/event-stream",
    # ``no-transform`` defends against intermediate proxies that recompress
    # or otherwise rewrite the body — Fastly's CDN respects it for the SSE
    # streams that pass through. Added when consolidating the inlined
    # variant from admin/compaction.py (audit r6); pure additive contract,
    # no behavior change for the other consumers.
    "Cache-Control": "no-cache, no-transform",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
}


def sse_flush_preamble(count: int = 8):
    """Yield empty SSE comment lines to flush nginx/proxy buffers before the first event."""
    for _ in range(count):
        yield f": {' ' * 1024}\n\n"


def sse_event(payload: dict, pad: int = 256):
    """Yield one SSE event followed by a padding comment that prevents
    proxy buffering of trailing events.

    Used by the SSE routers (provision, session_scoring) which all
    previously defined an identical ``def yj`` locally. ``pad=0`` disables
    the padding comment for callers that don't need it (e.g. the heartbeat
    sites in services/core.py)."""
    import json as _json

    yield f"data: {_json.dumps(payload)}\n\n"
    if pad:
        yield f": {' ' * pad}\n\n"


# ``sync_admin_state`` moved to ``backend.routers._state_sync`` — its two
# transitive imports (state_sync, scheduler) sit above ``utils`` in the
# layering, and the only callers are routers anyway.


def query_errors(status_code: int = 400):
    """Decorator that catches exceptions from a route handler and raises a
    standard ``HTTPException`` with ``{"error": str(e)}``.

    Security: the previous implementation embedded the full
    Python traceback under a ``trace`` key in the response body. Public
    callers could read internal file paths, module structure, and even
    secret values that leaked into exception messages. The fix is to log
    the traceback server-side (where operators can read it during triage)
    and return only the exception message to the client.

    Optionally catches ``ValueError`` / ``LookupError`` as 400/404 before
    the generic fallback.

    Usage::

        @router.post("/my-endpoint")
        @query_errors()
        def my_endpoint(req: MyRequest, con=Depends(get_con)):
            return repo.do_stuff(con, req)
    """

    def decorator(fn):
        import asyncio

        if asyncio.iscoroutinefunction(fn):
            # Async handler: await the coroutine and apply the same
            # exception-mapping. Necessary so an ``async def`` route can
            # still wear @query_errors and gather concurrent I/O (e.g.
            # M4 — Fastly call parallelisation in usage.py::prefill).
            @wraps(fn)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await fn(*args, **kwargs)
                except HTTPException:
                    raise
                except ValueError as e:
                    raise HTTPException(status_code=400, detail={"error": str(e)})
                except LookupError as e:
                    raise HTTPException(status_code=404, detail={"error": str(e)})
                except Exception as e:
                    logger.exception("[query_errors] unhandled exception in %s", fn.__qualname__)
                    raise HTTPException(
                        status_code=status_code,
                        detail={"error": str(e)},
                    )

            return async_wrapper

        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except HTTPException:
                raise
            except ValueError as e:
                raise HTTPException(status_code=400, detail={"error": str(e)})
            except LookupError as e:
                raise HTTPException(status_code=404, detail={"error": str(e)})
            except Exception as e:
                # logger.exception records the traceback to server logs
                # WITHOUT putting it on the wire. Triage requires opening
                # the backend log; that's an acceptable cost for the
                # security gain.
                logger.exception("[query_errors] unhandled exception in %s", fn.__qualname__)
                raise HTTPException(
                    status_code=status_code,
                    detail={"error": str(e)},
                )

        return wrapper

    return decorator
