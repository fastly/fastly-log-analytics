"""Shared utilities for routers.

- ``query_errors``: decorator that wraps a route handler in a standard
  try/except and raises HTTPException on failure.
- ``SSE_HEADERS``: standard Server-Sent Events response headers.
- ``sync_admin_state``: fire-and-forget state export after mutations.
- ``format_debug_request``: format outbound HTTP request metadata for debug output.
"""

from __future__ import annotations

import logging
from functools import wraps

from fastapi import HTTPException

logger = logging.getLogger(__name__)

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
    "Cache-Control": "no-cache",
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


# ── State sync ────────────────────────────────────────────────────────────────


def sync_admin_state(service_id: str | None) -> None:
    """Fire-and-forget admin state export after alert/view mutations.

    Also nudges the scheduler so that toggling alert count between 0 and >0
    immediately registers or removes the alerts evaluation cron — otherwise
    a user who just created their first alert would wait until the next
    process restart for evaluation to start.

    Swallows all exceptions so a sync failure never breaks the primary request.
    """
    if not service_id:
        return
    try:
        from backend.state_sync import export_admin_state

        export_admin_state(service_id)
    except Exception:
        pass
    try:
        from backend.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception:
        pass


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
