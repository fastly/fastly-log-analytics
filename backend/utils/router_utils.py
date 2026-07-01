"""Shared utilities for routers.

- ``query_errors``: decorator that wraps a route handler in a standard
  try/except and raises HTTPException on failure.
- ``sync_admin_state``: fire-and-forget state export after mutations.
- ``format_debug_request``: format outbound HTTP request metadata for debug output.
- ``SSE_PASSTHROUGH_HEADERS``: response headers that tell Fastly /
  Caddy / nginx not to buffer or transform streaming SSE responses.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Callable, Collection
from functools import wraps
from logging import Logger
from typing import Any, NoReturn, get_args

from fastapi import HTTPException

logger = logging.getLogger(__name__)


# ── SSE pass-through headers ────────────────────────────────────────────────
#
# Apply to every EventSourceResponse so intermediaries forward chunks as
# soon as the backend yields them, rather than buffering until the
# stream closes (which for SSE is never).
#
#  - ``Surrogate-Control: no-store`` is the Fastly-respected hint that
#    bypasses Varnish-shield caching. Without it, Fastly buffered our
#    analyst SSE responses indefinitely: client fetch never received
#    headers, the browser-side hook sat on "starting" forever, and the
#    "Latest Log" header badge never updated for analyst sessions
#    (observed 2026-06-15 via the Fastly URL — non-stream
#    /api/log-extents returned 200 instantly, /api/log-extents/stream
#    never delivered a single chunk).
#  - ``Cache-Control: private, no-store, no-transform`` — ``no-transform``
#    discourages proxies from re-compressing the body, which is a
#    different reason intermediaries hold onto chunks. ``no-store``
#    plus ``private`` keeps cookied SSE bodies out of any cache.
#  - ``X-Accel-Buffering: no`` — nginx/Caddy hint. Caddy already streams
#    SSE correctly via ``flush_interval -1`` in the Caddyfile, but we
#    set the header anyway in case the deployment topology changes.
#
# Pass via ``EventSourceResponse(stream(), ping=15, headers=SSE_PASSTHROUGH_HEADERS)``.
SSE_PASSTHROUGH_HEADERS: dict[str, str] = {
    "Surrogate-Control": "no-store",
    "Cache-Control": "private, no-store, no-transform",
    "X-Accel-Buffering": "no",
}


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
        raise HTTPException(status_code=503, detail=make_error("cron_busy", str(e), busy=True)) from e


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


# ── Standardised error-envelope helpers ───────────────────────────────────────
#
# Convention: every router error response carries ``detail = {"error":
# <machine-readable code>, ...}`` so the frontend's ``extractApiError`` can
# pattern-match on the ``error`` field rather than substring-matching on
# free-text. Use these helpers on every HTTPException site so the shape
# stays uniform across the 18-router surface.


def bad_request(code: str) -> dict[str, str]:
    """Envelope for a 400 ``{"error": code}``. Pair with
    ``raise HTTPException(status_code=400, detail=bad_request("..."))``."""
    return {"error": code}


def not_found(code: str = "not_found") -> dict[str, str]:
    """Envelope for a 404 ``{"error": code}``. ``code`` defaults to the
    generic ``"not_found"`` for resources that have no domain-specific code."""
    return {"error": code}


def validation_failed(code: str, messages: list[str]) -> dict:
    """Envelope for a 422 ``{"error": code, "messages": [...]}``. Use when a
    request fails domain validation that Pydantic can't express."""
    return {"error": code, "messages": messages}


def make_error(
    code: str,
    message: str | None = None,
    *,
    error_id: str | None = None,
    **extras: object,
) -> dict[str, object]:
    """Build the unified error-envelope detail dict.

    Shape: ``{"error": code[, "message": ..., "error_id": ..., **extras]}``.
    ``None`` fields are omitted so the wire payload doesn't carry empty
    keys. Pair with::

        raise HTTPException(status_code=400, detail=make_error("bad_input", str(exc)))

    ``code`` is the machine-readable identifier the frontend pattern-matches
    on; ``message`` carries the human-readable detail (safe-to-echo
    exception text from controlled call sites — e.g. ``parse_period``
    ValueError). For exceptions that may leak internal state (DuckDB file
    paths, upstream API bodies) use :func:`raise_internal` instead, which
    logs server-side and returns only an ``error_id``."""
    detail: dict[str, object] = {"error": code}
    if message is not None:
        detail["message"] = message
    if error_id is not None:
        detail["error_id"] = error_id
    detail.update(extras)
    return detail


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


# ── Section-selector validation ───────────────────────────────────────────────


def expand_sections(
    sections: Collection[str] | None,
    valid: frozenset[str],
    *,
    couple: Callable[[set[str]], set[str]] | None = None,
) -> set[str] | None:
    """Validate a section selector against ``valid``; apply optional coupling.

    ``None`` → no selector (full response). Unknown members raise the shared
    ``400 {"error": "unknown_section", "unknown": [...]}`` envelope. ``couple``
    lets a router fold in implied sections after validation (e.g. ts+status+path
    travel together; selecting any fingerprint card also computes coverage).
    """
    if sections is None:
        return None
    expanded = set(sections)
    unknown = expanded - valid
    if unknown:
        raise HTTPException(
            status_code=400,
            detail={"error": "unknown_section", "unknown": sorted(unknown)},
        )
    return couple(expanded) if couple else expanded


def make_section_expander(
    section_literal: Any,
    *,
    union_groups: tuple[frozenset[str], ...] = (),
    implies: tuple[tuple[frozenset[str], str], ...] = (),
) -> Callable[[Collection[str] | None], set[str] | None]:
    """Build a router's section-selector validator from its section ``Literal``.

    Removes the ``frozenset(get_args(...))`` + ``_expand_sections`` wrapper
    boilerplate each section-selector router repeated. Couplings are declared
    per router (keep each group's rationale comment on its constant at the call
    site):

    * ``union_groups`` — symmetric: selecting any member auto-includes the whole
      group (e.g. ``top_urls``/``top_asns`` share one temp materialization).
    * ``implies`` — asymmetric ``(trigger_set, member)``: selecting any member of
      ``trigger_set`` adds ``member`` (e.g. any fingerprint card pulls in
      ``fingerprint_coverage``).

    Each coupling targets a disjoint set, so the application order is immaterial.
    Returns the ``_expand_sections`` callable; ``None`` selector → full response.
    """
    valid = frozenset(get_args(section_literal))

    couple: Callable[[set[str]], set[str]] | None
    if not union_groups and not implies:
        couple = None
    else:

        def couple(expanded: set[str]) -> set[str]:
            for group in union_groups:
                if expanded & group:
                    expanded |= group
            for trigger, member in implies:
                if expanded & trigger:
                    expanded.add(member)
            return expanded

    def _expand(sections: Collection[str] | None) -> set[str] | None:
        return expand_sections(sections, valid, couple=couple)

    return _expand


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


# ``sync_admin_state`` moved to ``backend.routers._state_sync`` — its two
# transitive imports (state_sync, scheduler) sit above ``utils`` in the
# layering, and the only callers are routers anyway.


def query_errors(status_code: int = 400):
    """Decorator that maps unhandled exceptions to the unified error envelope.

    Three branches:

    * ``ValueError`` → 400 ``make_error("bad_request", str(e))``. The
      human-readable text rides on ``detail.message`` where
      ``extractApiError`` reads it as the user-facing string. Use
      ``ValueError`` from inside the handler whenever the message is
      safe to echo (validated input, period parsing, etc.).
    * ``LookupError`` → 404 ``make_error("not_found", str(e))`` — same
      shape, different code.
    * Any other ``Exception`` → ``raise_internal``: logs the full
      traceback server-side (operator triage) and returns ``{"error":
      "unhandled_error", "error_id": "..."}`` so upstream API bodies,
      DuckDB internals, and stack-trace text never reach the client.

    Wearers of the decorator no longer need to think about which exception
    text is safe to echo: the catch-all path collapses to a generic code +
    correlation id, and the typed branches put the message under
    ``detail.message`` instead of the machine-code slot.

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
                    raise HTTPException(status_code=400, detail=make_error("bad_request", str(e)))
                except LookupError as e:
                    raise HTTPException(status_code=404, detail=make_error("not_found", str(e)))
                except Exception as e:
                    raise_internal(logger, e, code="unhandled_error", status=status_code)

            return async_wrapper

        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except HTTPException:
                raise
            except ValueError as e:
                raise HTTPException(status_code=400, detail=make_error("bad_request", str(e)))
            except LookupError as e:
                raise HTTPException(status_code=404, detail=make_error("not_found", str(e)))
            except Exception as e:
                raise_internal(logger, e, code="unhandled_error", status=status_code)

        return wrapper

    return decorator
