"""Shared utilities for routers.

- ``query_errors``: decorator that wraps a route handler in a standard
  try/except and raises HTTPException on failure.
- ``sync_admin_state``: fire-and-forget state export after mutations.
- ``format_debug_request``: format outbound HTTP request metadata for debug output.
- ``SSE_PASSTHROUGH_HEADERS``: response headers that tell Fastly /
  Caddy / nginx not to buffer or transform streaming SSE responses.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import os
import queue
import secrets
import sys
import threading
import time
import types
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Callable, Collection, Generator, Iterator, Mapping
from functools import wraps
from logging import Logger
from typing import Any, NoReturn, get_args

from fastapi import HTTPException, Request
from sse_starlette.sse import EventSourceResponse

from backend import config as svcconfig
from backend.config import path_within_dir, service_id_from_source  # noqa: F401
from backend.utils.structlog_config import (  # noqa: F401
    ABS_PATH_RE,
    REDACTED,
    redact_error_text,
    register_salt,
    scrub_secrets,
)
from backend.utils.time_window import CacheRegistry as _CacheRegistry

logger = logging.getLogger(__name__)


def current_request_id(request: Request | None = None) -> str | None:
    """The access-log request id: from ``request.state`` when given, else the per-request attribution contextvar."""
    rid = getattr(getattr(request, "state", None), "request_id", None) if request is not None else None
    if not (isinstance(rid, str) and rid):
        from backend.core.query_attribution import current_attribution

        attr = current_attribution.get()
        rid = getattr(attr, "request_id", None)
    return rid if isinstance(rid, str) and rid else None


def correlation_id(request: Request | None = None) -> str:
    """The request id when one exists, else a fresh id (the caller must log it alongside the failure)."""
    return current_request_id(request) or secrets.token_hex(8)


def require_json_content_type(req: Request) -> None:
    """Reject any request whose Content-Type isn't application/json.

    CSRF defense for destructive POST routes: an HTML form with
    ``enctype=text/plain`` can POST a body that LOOKS like JSON without
    triggering a CORS preflight. Requiring ``Content-Type: application/json``
    forces the browser to preflight any cross-origin call (text/plain is
    "simple"; application/json is not), blocking the silent-invocation
    vector. Runs as a ``Depends()`` so it fires before FastAPI's body
    parser — otherwise a malformed text/plain body returns 422 from the
    parser and the explicit 415 never executes."""
    if not (req.headers.get("content-type") or "").startswith("application/json"):
        raise HTTPException(
            status_code=415,
            detail=make_error("unsupported_media_type", "Content-Type must be application/json"),
        )


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
# Pass via ``EventSourceResponse(stream(), ping=5, headers=SSE_PASSTHROUGH_HEADERS)``.
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
        logger.warning("cron_busy task=%s service_id=%s: %s", task, service_id, e)
        raise HTTPException(
            status_code=503,
            detail=make_error("cron_busy", "A run for this task is already in progress", busy=True),
        ) from e


def run_admin_cron_trigger(
    source: dict[str, Any],
    task_name: str,
    target_fn: Any,
    *,
    target_kwargs: dict[str, Any] | None = None,
    success_msg: str,
    in_progress_msg: str,
    readonly_detail: Any,
    invalidate_cache: bool = True,
    require_high_scale: bool = False,
    high_scale_detail: Any = None,
    precheck: Callable[[], None] | None = None,
) -> SyncStartResponse:
    """Shared execution wrapper for manual admin cron trigger routes."""
    if source.get("access_level") == "read_only":
        raise HTTPException(status_code=403, detail=readonly_detail)

    if require_high_scale:
        from backend import config as svcconfig

        if not svcconfig.is_high_scale_mode(source):
            raise HTTPException(
                status_code=400,
                detail=high_scale_detail or f"{task_name} is only supported under high-scale mode.",
            )

    if precheck is not None:
        precheck()

    if invalidate_cache:
        from backend.repositories.dashboard import invalidate_service

        invalidate_service(source["name"])

    res = start_or_resume_cron(
        source,
        task_name,
        target_fn,
        target_kwargs=target_kwargs,
        success_msg=success_msg,
        in_progress_msg=in_progress_msg,
    )
    return SyncStartResponse(**res)


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


def load_service_config_or_503(service_id: str, *, writable: bool = False) -> dict:
    """Load a service's config, raising 503 on read error, 404 if missing, or 403 if writable required."""
    from backend import config as svcconfig

    try:
        cfg = svcconfig.load_config(service_id)
    except Exception:
        raise HTTPException(
            503, detail=make_error("service_unavailable", "Service configuration unavailable")
        ) from None
    if not cfg:
        raise HTTPException(404, detail=make_error("service_not_found", "Service not found"))
    if writable and cfg.get("access_level", "read_write") != "read_write":
        raise HTTPException(403, detail=make_error("read_write_required", "Read-write service required"))
    return cfg


def mutate_service_config(service_id: str, mutate: Callable[[dict], None]) -> dict:
    """Locked read-modify-write of a service config, or :class:`HTTPException` 404.

    The write counterpart of :func:`load_service_config`: applies ``mutate`` to a fresh copy
    under :func:`backend.config.mutate_config` (so a concurrent writer's keys are not
    clobbered) and returns the saved config.
    """
    from backend import config as svcconfig

    saved = svcconfig.mutate_config(service_id, mutate)
    if saved is None:
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    return saved


def stored_fastly_token(cfg: Mapping[str, Any] | None) -> str:
    """The ``fastly_api_key`` stored in an already-loaded service config, stripped; ``""`` if absent.

    The single read of that field for routers that already hold ``cfg``. Never logs the value and
    never falls back to another service's key.
    """
    if not cfg:
        return ""
    return str(cfg.get("fastly_api_key") or "").strip()


def resolve_fastly_token(service_id: str, override: str | None = "") -> str:
    """Lenient token resolution: ``override`` if given, else the service config's stored
    ``fastly_api_key``, else ``""`` (callers raise their own error on empty).

    Reads the config for exactly ``service_id`` — it never falls back to the *active* service's key
    (unlike ``config.get_fastly_api_key``), so an empty or unknown id can't borrow another service's
    credential. The token value is never logged.
    """
    if override:
        return override
    from backend import config as svcconfig

    return stored_fastly_token(svcconfig.load_config(service_id))


def require_fastly_token(service_id: str, override: str | None = "") -> str:
    """Strict token resolution: ``override`` if given, else the stored key — 404 when the service has no
    config, 400 when it has no key. Error bodies match the RUM endpoints' historical ones."""
    if override:
        return override
    from backend import config as svcconfig

    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    token = stored_fastly_token(cfg)
    if not token:
        raise HTTPException(status_code=400, detail={"error": f"No Fastly API key for service {service_id}"})
    return token


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


def require_service_id(service_id: str | None) -> str:
    """Return ``service_id``, or raise the canonical 400 ``service_id_required``
    envelope when it is missing or empty. Every route that needs a service and
    did not get one answers with this, never FastAPI's 422 validation body."""
    if not service_id:
        raise HTTPException(status_code=400, detail=make_error("service_id_required"))
    return service_id


def raise_internal(
    log: Logger,
    exc: BaseException,
    *,
    code: str = "request_failed",
    status: int = 500,
    message: str | None = None,
    request: Request | None = None,
) -> NoReturn:
    """Log the full exception server-side; raise a generic ``HTTPException``
    that does NOT echo the original exception message to the client.

    Use at except sites that previously did
    ``raise HTTPException(status_code=500, detail={"error": str(e)})`` —
    that pattern leaks upstream API response bodies (e.g. Fastly error
    text interpolated by ``backend.core.fastly.client.fastly()``) to the
    caller. ``error_id`` lets operators correlate a client report with
    the matching server-log line; ``request_id`` (when the request is known)
    joins it to the access log. ``message`` must be a static string.
    """
    error_id = uuid.uuid4().hex[:8]
    request_id = current_request_id(request)
    log.exception("%s [error_id=%s request_id=%s]", code, error_id, request_id)
    detail: dict[str, object] = {"error": code}
    if message is not None:
        detail["message"] = message
    detail["error_id"] = error_id
    if request_id:
        detail["request_id"] = request_id
    raise HTTPException(status_code=status, detail=detail) from exc


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
        logger.warning("sync_admin_state: export_admin_state failed (ignored)", exc_info=True)
    try:
        from backend.cron.scheduler import get_scheduler

        get_scheduler().reload()
    except Exception:
        logger.warning("sync_admin_state: get_scheduler failed (ignored)", exc_info=True)


def run_cached_repo_endpoint(
    ctx,
    *,
    repo_module: str,
    get_fn,
    peek_fn,
    kwargs: dict,
    wrap=None,
    on_response=None,
):
    """Run a repository aggregate endpoint with peek-before-checkout caching.

    When ``get_fn`` has not been monkeypatched away from ``repo_module``, peeks
    the cache before checking out a pooled DuckDB connection.
    """
    is_real_repo = getattr(get_fn, "__module__", "") == repo_module
    if is_real_repo:
        cached = peek_fn(src=ctx.source, **kwargs)
        if cached is not None:
            if on_response is not None:
                on_response()
            return wrap(cached) if wrap is not None else cached
    res = get_fn(
        con=ctx.con_or_factory(force_eager=not is_real_repo),
        src=ctx.source,
        **kwargs,
    )
    if on_response is not None:
        on_response()
    return wrap(res) if wrap is not None else res


def _handle_query_error(
    e: Exception,
    *,
    logger_obj: Logger,
    code: str,
    status_code: int,
) -> NoReturn:
    if isinstance(e, HTTPException):
        raise e
    if isinstance(e, ValueError):
        raise HTTPException(status_code=400, detail=make_error("bad_request", redact_error_text(e))) from e
    if isinstance(e, LookupError):
        raise HTTPException(status_code=404, detail=make_error("not_found", redact_error_text(e))) from e
    raise_internal(logger_obj, e, code=code, status=status_code)


def query_errors(status_code: int = 400, *, code: str = "unhandled_error", logger_obj: Logger = logger):
    """Decorator that maps unhandled exceptions to the unified error envelope.

    Three branches:

    * ``ValueError`` → 400 ``make_error("bad_request", redact_error_text(e))``. The
      human-readable text rides on ``detail.message`` where
      ``extractApiError`` reads it as the user-facing string. Use
      ``ValueError`` from inside the handler whenever the message is
      safe to echo (validated input, period parsing, etc.).
    * ``LookupError`` → 404 ``make_error("not_found", redact_error_text(e))`` — same
      shape, different code.
    * Any other ``Exception`` → ``raise_internal``: logs the full
      traceback server-side (operator triage) and returns ``{"error":
      code, "error_id": "..."}`` so upstream API bodies,
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

            @wraps(fn)
            async def async_wrapper(*args, **kwargs):
                try:
                    return await fn(*args, **kwargs)
                except Exception as e:
                    _handle_query_error(e, logger_obj=logger_obj, code=code, status_code=status_code)

            return async_wrapper

        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                _handle_query_error(e, logger_obj=logger_obj, code=code, status_code=status_code)

        return wrapper

    return decorator


# ── Directory size & stats caching (folded from dir_size.py) ──────────────────

_DIR_SIZE_CACHE: dict[str, tuple[float, int]] = {}
_DIR_STATS_CACHE: dict[str, tuple[float, int, int]] = {}
_DIR_SIZE_TTL_S = 120.0

_dir_size_lock = threading.Lock()
_dir_size_refresh_in_flight: set[str] = set()
_dir_size_cold_locks: dict[str, threading.Lock] = {}
_dir_size_cold_locks_meta_lock = threading.Lock()


def _schedule_dir_walk(
    path: str,
    target,
    thread_name_prefix: str,
    in_flight: set[str],
    lock: threading.Lock,
    thread_cls=threading.Thread,
) -> None:
    try:
        thread_cls(
            target=target,
            args=(path,),
            name=f"{thread_name_prefix}:{os.path.basename(path)}",
            daemon=True,
        ).start()
    except Exception:
        with lock:
            in_flight.discard(path)


def get_dir_stats_swr(
    path: str,
    *,
    stats_cache: dict[str, tuple[float, int, int]],
    size_cache: dict[str, tuple[float, int]],
    shared_stats_cache: dict[str, tuple[float, int, int]],
    in_flight: set[str],
    lock: threading.Lock,
    ttl: float,
    walk_fn=None,
    thread_cls=threading.Thread,
) -> tuple[int, int]:
    """Return ``(total_size_bytes, file_count)`` with stale-while-revalidate semantics."""
    walker = walk_fn or walk_dir_stats
    now = time.monotonic()
    with lock:
        entry = stats_cache.get(path)
        if entry is not None and (now - entry[0]) < ttl:
            return (entry[1], entry[2])
        best_effort = (entry[1], entry[2]) if entry is not None else (0, 0)
        schedule_refresh = path not in in_flight
        if schedule_refresh:
            in_flight.add(path)

    if schedule_refresh:

        def _refresh(p: str) -> None:
            try:
                total_size, file_count = walker(p)
                ts = time.monotonic()
                with lock:
                    stats_cache[p] = (ts, total_size, file_count)
                    if stats_cache is shared_stats_cache:
                        size_cache[p] = (ts, total_size)
            finally:
                with lock:
                    in_flight.discard(p)

        _schedule_dir_walk(path, _refresh, "dir-stats-refresh", in_flight, lock, thread_cls)
    return best_effort


def _get_dir_size(path: str) -> int:
    now = time.monotonic()
    schedule_refresh = False
    with _dir_size_lock:
        cached = _DIR_SIZE_CACHE.get(path)
        if cached is not None and (now - cached[0]) < _DIR_SIZE_TTL_S:
            return cached[1]
        stats_cached = _DIR_STATS_CACHE.get(path)
        if stats_cached is not None and (now - stats_cached[0]) < _DIR_SIZE_TTL_S:
            _DIR_SIZE_CACHE[path] = (stats_cached[0], stats_cached[1])
            return stats_cached[1]
        if cached is not None:
            if path not in _dir_size_refresh_in_flight:
                _dir_size_refresh_in_flight.add(path)
                schedule_refresh = True
            stale_value = cached[1]

    if cached is not None:
        if schedule_refresh:
            _schedule_dir_walk(
                path,
                _refresh_dir_size_background,
                "dir-size-refresh",
                _dir_size_refresh_in_flight,
                _dir_size_lock,
            )
        return stale_value

    with _dir_size_cold_locks_meta_lock:
        cold_lock = _dir_size_cold_locks.setdefault(path, threading.Lock())
    try:
        with cold_lock:
            with _dir_size_lock:
                cached = _DIR_SIZE_CACHE.get(path)
                if cached is not None:
                    return cached[1]
            return _refresh_dir_size_background(path)
    finally:
        with _dir_size_cold_locks_meta_lock:
            _dir_size_cold_locks.pop(path, None)


def _refresh_dir_size_background(path: str) -> int:
    """Run the walk and write the result back into the shared size/stats caches."""
    try:
        total, file_count = walk_dir_stats(path)
        ts = time.monotonic()
        with _dir_size_lock:
            _DIR_SIZE_CACHE[path] = (ts, total)
            _DIR_STATS_CACHE[path] = (ts, total, file_count)
        return total
    finally:
        with _dir_size_lock:
            _dir_size_refresh_in_flight.discard(path)


def walk_dir_stats(path: str) -> tuple[int, int]:
    """Synchronous os.scandir walk. Returns (total_size_bytes, file_count).
    Symlinks are skipped (preserves the prior os.walk behavior)."""
    if not os.path.exists(path):
        return (0, 0)
    total_size = 0
    file_count = 0
    stack = [path]
    while stack:
        d = stack.pop()
        try:
            with os.scandir(d) as it:
                for entry_de in it:
                    try:
                        if entry_de.is_symlink():
                            continue
                        if entry_de.is_dir(follow_symlinks=False):
                            stack.append(entry_de.path)
                        elif entry_de.is_file(follow_symlinks=False):
                            total_size += entry_de.stat(follow_symlinks=False).st_size
                            file_count += 1
                    except OSError:
                        continue
        except OSError:
            continue
    return (total_size, file_count)


_DEFAULT_WALK_DIR_STATS = walk_dir_stats
_walk_dir_stats = walk_dir_stats

_CacheRegistry.register("utils.dir_size._DIR_SIZE_CACHE", _DIR_SIZE_CACHE)
_CacheRegistry.register("utils.dir_size._DIR_STATS_CACHE", _DIR_STATS_CACHE)
_CacheRegistry.register("utils.dir_size._dir_size_cold_locks", _dir_size_cold_locks)

sys.modules["backend.utils.dir_size"] = sys.modules[__name__]
if "backend.utils" in sys.modules:
    setattr(sys.modules["backend.utils"], "dir_size", sys.modules[__name__])


# ── Service Manager (folded from services/service_manager.py) ─────────────────

from backend.core import duckdb as _db  # noqa: E402
from backend.models.admin import SyncStartResponse  # noqa: E402, F401

_shared_dir_size_cache = _DIR_SIZE_CACHE
_shared_dir_stats_cache = _DIR_STATS_CACHE
_shared_dir_size_lock = _dir_size_lock
_DIR_STATS_TTL_SEC = 300.0
_dir_stats_cache: dict[str, tuple[float, int, int]] = _shared_dir_stats_cache
_dir_stats_lock = _shared_dir_size_lock
_dir_stats_refresh_in_flight: set[str] = set()


def _get_dir_stats(path: str) -> tuple[int, int]:
    """Return ``(total_size_bytes, file_count)`` for ``path`` recursively via shared SWR cache."""
    fn = walk_dir_stats if walk_dir_stats is not _DEFAULT_WALK_DIR_STATS else _walk_dir_stats
    return get_dir_stats_swr(
        path,
        stats_cache=_dir_stats_cache,
        size_cache=_shared_dir_size_cache,
        shared_stats_cache=_shared_dir_stats_cache,
        in_flight=_dir_stats_refresh_in_flight,
        lock=_dir_stats_lock,
        ttl=_DIR_STATS_TTL_SEC,
        walk_fn=fn,
        thread_cls=threading.Thread,
    )


def get_enriched_services(active_service_id: str | None = None) -> list[dict[str, Any]]:
    """Return all configured services enriched with status and database stats.

    Used by both /bootstrap and /services endpoints to ensure consistent representation.
    """
    configs = svcconfig.list_configs()
    name_map = svcconfig.refresh_all_service_names(configs)

    hs_service_ids = {s.strip() for s in os.getenv("HIGH_SCALE_SERVICE_IDS", "").split(",") if s.strip()}

    result = []
    for cfg in configs:
        sid = cfg.get("service_id", "")
        if not sid:
            continue
        name = name_map.get(sid, sid)

        db_path = cfg.get("duckdb_path") or svcconfig.duckdb_path(sid)
        db_exists = os.path.exists(db_path)
        db_size = os.path.getsize(db_path) if db_exists else 0

        src_dict = _db._source_from_config(cfg)
        cache_size, cache_file_count = _get_dir_stats(_db._cache_dir(src_dict))
        total_size = db_size + cache_size

        prov = cfg.get("provisioning", {})
        cached_status = svcconfig.get_status(sid)
        cron_stats = cached_status.get("cron_stats", {})
        log_row_count = cached_status.get("local_rows", 0)

        rum_val = cfg.get("rum")
        rum_dict: dict[str, Any] = rum_val if isinstance(rum_val, dict) else {}
        rum_lp = rum_dict.get("log_period")
        if rum_lp is None:
            rum_lp = cfg.get("rum_log_period")
        if rum_lp is not None:
            try:
                rum_lp = int(rum_lp)
            except (ValueError, TypeError):
                rum_lp = None

        is_hs = svcconfig.is_high_scale_mode(src_dict) and (not hs_service_ids or sid in hs_service_ids)
        result.append(
            {
                "service_id": sid,
                "name": name,
                "access_level": cfg.get("access_level", "read_write"),
                "storage_mode": cfg.get("storage_mode", "cloud"),
                "is_high_scale": is_hs,
                "log_period": cfg.get("log_period", 60),
                "fos_bucket": cfg.get("fos_bucket", ""),
                "fos_region": cfg.get("fos_region", ""),
                "cdn_url": src_dict.get("cdn_url", ""),
                "cdn_service_id": cfg.get("cdn_service_id", ""),
                "duckdb_exists": db_exists,
                "duckdb_size_bytes": total_size,
                "cache_file_count": cache_file_count,
                "log_row_count": log_row_count,
                "is_active": sid == active_service_id,
                "analyst_path_a_supported": not is_hs,
                "cron_stats": cron_stats,
                "status": cached_status,
                "cron_sync": prov.get(
                    "cron_sync",
                    {
                        "enabled": True,
                        "interval_mins": 2,
                        "log_enabled": True,
                        "log_retention_days": 7,
                        "delete_after": True,
                    },
                ),
                "cron_compact": prov.get(
                    "cron_compact", {"enabled": True, "interval_mins": 30, "log_enabled": True, "log_retention_days": 7}
                ),
                "cron_ngwaf": prov.get(
                    "cron_ngwaf", {"interval_mins": 15, "log_enabled": True, "log_retention_days": 7}
                ),
                "ngwaf_workspace_id": cfg.get("ngwaf_workspace_id"),
                "logging_enabled": cfg.get("logging_enabled", True),
                "rum_enabled": svcconfig.is_rum_enabled(cfg),
                "rum": cfg.get("rum"),
                "rum_log_period": rum_lp,
                "cmcd_enabled": bool((cfg.get("cmcd") or {}).get("enabled")),
            }
        )

    # Sort: active first, then alphabetically
    result.sort(key=lambda x: (not x["is_active"], x["name"].lower()))
    return result


_CacheRegistry.register("services.service_manager._dir_stats_cache", _dir_stats_cache)
_CacheRegistry.register("services.service_manager._dir_stats_refresh_in_flight", _dir_stats_refresh_in_flight)


# ── SSE helpers (folded from sse.py) ──────────────────────────────────────────


async def iter_with_disconnect_ping[T](
    source: AsyncIterable[T],
    request: Request,
    *,
    ping_seconds: float = 15,
) -> AsyncIterator[T]:
    """Yield each payload from ``source`` until the request disconnects."""
    aiter = source.__aiter__()
    pending: asyncio.Future[T] | None = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(aiter.__anext__())
            try:
                payload = await asyncio.wait_for(asyncio.shield(pending), timeout=ping_seconds)
            except TimeoutError:
                if await request.is_disconnected():
                    return
                continue
            except StopAsyncIteration:
                return
            pending = None
            if await request.is_disconnected():
                return
            yield payload
    finally:
        if pending is not None and not pending.done():
            pending.cancel()


class ClientDisconnected(Exception):
    """Raised when the client disconnects during a streaming response."""


class _AbortableQueue(queue.Queue):
    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self.aborted = False

    def put(self, item: Any, block: bool = True, timeout: float | None = None) -> None:
        if self.aborted:
            if item is None:
                return
            raise ClientDisconnected("Client disconnected during streaming")
        super().put(item, block, timeout)


def _stream_from_worker(worker: Callable[[_AbortableQueue], Any], *, maxsize: int = 10) -> Generator[Any]:
    """Run *worker(q)* in a daemon thread and yield items it puts into the queue until ``None``."""
    q: _AbortableQueue = _AbortableQueue(maxsize=maxsize)
    ctx = contextvars.copy_context()
    thread = threading.Thread(target=lambda: ctx.run(worker, q), daemon=True)
    thread.start()
    try:
        while True:
            chunk = q.get()
            if chunk is None:
                break
            yield chunk
    finally:
        q.aborted = True
        while True:
            try:
                q.get_nowait()
            except queue.Empty:
                break


def stream_sse_from_worker(
    worker: Callable[[_AbortableQueue], Any],
    *,
    ping: int = 5,
    maxsize: int = 0,
) -> EventSourceResponse:
    """Run *worker(q)* in a background daemon thread via ``_stream_from_worker`` and stream JSON SSE frames."""

    def _safe_worker(q: _AbortableQueue) -> None:
        try:
            worker(q)
        finally:
            q.put(None)

    async def _stream() -> AsyncIterator[str]:
        gen = _stream_from_worker(_safe_worker, maxsize=maxsize)
        sentinel = object()
        try:
            while True:
                event = await asyncio.to_thread(next, gen, sentinel)
                if event is sentinel:
                    break
                yield event if isinstance(event, str) else json.dumps(event)
        finally:
            gen.close()

    return sse_response(_stream(), ping=ping)


def sse_response(stream: AsyncIterable[Any] | Iterator[Any], *, ping: int = 5) -> EventSourceResponse:
    """Wrap an SSE ``stream`` in an ``EventSourceResponse``."""
    return EventSourceResponse(stream, ping=ping, headers=SSE_PASSTHROUGH_HEADERS)


def stream_iac_reconcile(
    service_id: str,
    resolved_token: str,
    *,
    start_message: str,
    done_event: dict[str, Any] | Callable[[], dict[str, Any]],
    after_done: Callable[[], None] | None = None,
    error_event: Callable[[Exception], dict[str, Any]] | None = None,
    log_label: str = "reconcile_infrastructure",
    logger_obj: Any = None,
) -> EventSourceResponse:
    """Stream ``reconcile_infrastructure`` events via ``run_with_events`` as SSE JSON frames."""
    from backend.provision.declarative.iac_reconciler import reconcile_infrastructure
    from backend.provision.orchestrator import run_with_events

    def _stream() -> Iterator[str]:
        yield json.dumps({"type": "status", "message": start_message})
        try:
            for event in run_with_events(reconcile_infrastructure, service_id, resolved_token):
                yield json.dumps(event)
            done_payload = done_event() if callable(done_event) else done_event
            yield json.dumps(done_payload)
            if after_done is not None:
                after_done()
        except Exception as e:
            if logger_obj is not None:
                logger_obj.exception("%s failed for %s", log_label, service_id)
            err_payload = error_event(e) if error_event is not None else {"type": "error", "message": str(e)}
            yield json.dumps(err_payload)

    return sse_response(_stream())


_self_mod = sys.modules[__name__]
sys.modules["backend.utils.dir_size"] = _self_mod
sys.modules["backend.utils.sse"] = _self_mod
sys.modules["backend.utils.error_utils"] = _self_mod
sys.modules["backend.utils.pii_masking"] = _self_mod
sys.modules["backend.services.service_manager"] = _self_mod
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    setattr(_utils_pkg, "dir_size", _self_mod)
    setattr(_utils_pkg, "sse", _self_mod)
_services_pkg = sys.modules.get("backend.services")
if _services_pkg is None:
    _services_pkg = types.ModuleType("backend.services")
    _services_pkg.__path__ = []
    sys.modules["backend.services"] = _services_pkg
    _backend_pkg = sys.modules.get("backend")
    if _backend_pkg is not None:
        setattr(_backend_pkg, "services", _services_pkg)
setattr(_services_pkg, "service_manager", _self_mod)


# ── Country & POP mappings (folded from backend.utils.countries) ─────────────

COUNTRY_MAP = {
    "AF": "Afghanistan",
    "AX": "Aland Islands",
    "AL": "Albania",
    "DZ": "Algeria",
    "AS": "American Samoa",
    "AD": "Andorra",
    "AO": "Angola",
    "AI": "Anguilla",
    "AQ": "Antarctica",
    "AG": "Antigua and Barbuda",
    "AR": "Argentina",
    "AM": "Armenia",
    "AW": "Aruba",
    "AU": "Australia",
    "AT": "Austria",
    "AZ": "Azerbaijan",
    "BS": "Bahamas",
    "BH": "Bahrain",
    "BD": "Bangladesh",
    "BB": "Barbados",
    "BY": "Belarus",
    "BE": "Belgium",
    "BZ": "Belize",
    "BJ": "Benin",
    "BM": "Bermuda",
    "BT": "Bhutan",
    "BO": "Bolivia",
    "BQ": "Bonaire, Sint Eustatius and Saba",
    "BA": "Bosnia and Herzegovina",
    "BW": "Botswana",
    "BV": "Bouvet Island",
    "BR": "Brazil",
    "IO": "British Indian Ocean Territory",
    "BN": "Brunei Darussalam",
    "BG": "Bulgaria",
    "BF": "Burkina Faso",
    "BI": "Burundi",
    "KH": "Cambodia",
    "CM": "Cameroon",
    "CA": "Canada",
    "CV": "Cape Verde",
    "KY": "Cayman Islands",
    "CF": "Central African Republic",
    "TD": "Chad",
    "CL": "Chile",
    "CN": "China",
    "CX": "Christmas Island",
    "CC": "Cocos (Keeling) Islands",
    "CO": "Colombia",
    "KM": "Comoros",
    "CG": "Congo",
    "CD": "Congo, Democratic Republic of the",
    "CK": "Cook Islands",
    "CR": "Costa Rica",
    "CI": "Cote d'Ivoire",
    "HR": "Croatia",
    "CU": "Cuba",
    "CW": "Curacao",
    "CY": "Cyprus",
    "CZ": "Czech Republic",
    "DK": "Denmark",
    "DJ": "Djibouti",
    "DM": "Dominica",
    "DO": "Dominican Republic",
    "EC": "Ecuador",
    "EG": "Egypt",
    "SV": "El Salvador",
    "GQ": "Equatorial Guinea",
    "ER": "Eritrea",
    "EE": "Estonia",
    "ET": "Ethiopia",
    "FK": "Falkland Islands (Malvinas)",
    "FO": "Faroe Islands",
    "FJ": "Fiji",
    "FI": "Finland",
    "FR": "France",
    "GF": "French Guiana",
    "PF": "French Polynesia",
    "TF": "French Southern Territories",
    "GA": "Gabon",
    "GM": "Gambia",
    "GE": "Georgia",
    "DE": "Germany",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GR": "Greece",
    "GL": "Greenland",
    "GD": "Grenada",
    "GP": "Guadeloupe",
    "GU": "Guam",
    "GT": "Guatemala",
    "GG": "Guernsey",
    "GN": "Guinea",
    "GW": "Guinea-Bissau",
    "GY": "Guyana",
    "HT": "Haiti",
    "HM": "Heard Island and McDonald Islands",
    "VA": "Holy See (Vatican City State)",
    "HN": "Honduras",
    "HK": "Hong Kong",
    "HU": "Hungary",
    "IS": "Iceland",
    "IN": "India",
    "ID": "Indonesia",
    "IR": "Iran, Islamic Republic of",
    "IQ": "Iraq",
    "IE": "Ireland",
    "IM": "Isle of Man",
    "IL": "Israel",
    "IT": "Italy",
    "JM": "Jamaica",
    "JP": "Japan",
    "JE": "Jersey",
    "JO": "Jordan",
    "KZ": "Kazakhstan",
    "KE": "Kenya",
    "KI": "Kiribati",
    "KP": "Korea, Democratic People's Republic of",
    "KR": "Korea, Republic of",
    "KW": "Kuwait",
    "KG": "Kyrgyzstan",
    "LA": "Lao People's Democratic Republic",
    "LV": "Latvia",
    "LB": "Lebanon",
    "LS": "Lesotho",
    "LR": "Liberia",
    "LY": "Libya",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MO": "Macao",
    "MK": "Macedonia, the Former Yugoslav Republic of",
    "MG": "Madagascar",
    "MW": "Malawi",
    "MY": "Malaysia",
    "MV": "Maldives",
    "ML": "Mali",
    "MT": "Malta",
    "MH": "Marshall Islands",
    "MQ": "Martinique",
    "MR": "Mauritania",
    "MU": "Mauritius",
    "YT": "Mayotte",
    "MX": "Mexico",
    "FM": "Micronesia, Federated States of",
    "MD": "Moldova, Republic of",
    "MC": "Monaco",
    "MN": "Mongolia",
    "ME": "Montenegro",
    "MS": "Montserrat",
    "MA": "Morocco",
    "MZ": "Mozambique",
    "MM": "Myanmar",
    "NA": "Namibia",
    "NR": "Nauru",
    "NP": "Nepal",
    "NL": "Netherlands",
    "NC": "New Caledonia",
    "NZ": "New Zealand",
    "NI": "Nicaragua",
    "NE": "Niger",
    "NG": "Nigeria",
    "NU": "Niue",
    "NF": "Norfolk Island",
    "MP": "Northern Mariana Islands",
    "NO": "Norway",
    "OM": "Oman",
    "PK": "Pakistan",
    "PW": "Palau",
    "PS": "Palestinian Territory, Occupied",
    "PA": "Panama",
    "PG": "Papua New Guinea",
    "PY": "Paraguay",
    "PE": "Peru",
    "PH": "Philippines",
    "PN": "Pitcairn",
    "PL": "Poland",
    "PT": "Portugal",
    "PR": "Puerto Rico",
    "QA": "Qatar",
    "RE": "Reunion",
    "RO": "Romania",
    "RU": "Russian Federation",
    "RW": "Rwanda",
    "BL": "Saint Barthelemy",
    "SH": "Saint Helena",
    "KN": "Saint Kitts and Nevis",
    "LC": "Saint Lucie",
    "MF": "Saint Martin",
    "PM": "Saint Pierre and Miquelon",
    "VC": "Saint Vincent and the Grenadines",
    "WS": "Samoa",
    "SM": "San Marino",
    "ST": "Sao Tome and Principe",
    "SA": "Saudi Arabia",
    "SN": "Senegal",
    "RS": "Serbia",
    "SC": "Seychelles",
    "SL": "Sierra Leone",
    "SG": "Singapore",
    "SX": "Sint Maarten",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "SB": "Solomon Islands",
    "SO": "Somalia",
    "ZA": "South Africa",
    "GS": "South Georgia and the South Sandwich Islands",
    "SS": "South Sudan",
    "ES": "Spain",
    "LK": "Sri Lanka",
    "SD": "Sudan",
    "SR": "Suriname",
    "SJ": "Svalbard and Jan Mayen",
    "SZ": "Swaziland",
    "SE": "Sweden",
    "CH": "Switzerland",
    "SY": "Syrian Arab Republic",
    "TW": "Taiwan",
    "TJ": "Tajikistan",
    "TZ": "Tanzania, United Republic of",
    "TH": "Thailand",
    "TL": "Timor-Leste",
    "TG": "Togo",
    "TK": "Tokelau",
    "TO": "Tonga",
    "TT": "Trinidad and Tobago",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TM": "Turkmenistan",
    "TC": "Turks and Caicos Islands",
    "TV": "Tuvalu",
    "UG": "Uganda",
    "UA": "Ukraine",
    "AE": "United Arab Emirates",
    "GB": "United Kingdom",
    "US": "United States",
    "UM": "United States Minor Outlying Islands",
    "UY": "Uruguay",
    "UZ": "Uzbekistan",
    "VU": "Vanuatu",
    "VE": "Venezuela",
    "VN": "Viet Nam",
    "VG": "Virgin Islands, British",
    "VI": "Virgin Islands, U.S.",
    "WF": "Wallis and Futuna",
    "EH": "Western Sahara",
    "YE": "Yemen",
    "ZM": "Zambia",
    "ZW": "Zimbabwe",
}


def format_city_label(city: str, country: str, region: str = "") -> str:
    city = city or ""
    country = country or ""
    region = region or ""
    c_name = COUNTRY_MAP.get(country.upper(), country.upper())

    parts = []
    if city:
        parts.append(city.title())
    if region:
        parts.append(region.upper())
    if c_name:
        parts.append(c_name)
    elif country:
        parts.append(country.upper())

    return ", ".join(parts) if parts else "Unknown"


# ---------------------------------------------------------------------------
# Fastly POP location cache & SQL helpers (folded from backend.utils.pop_utils)
# ---------------------------------------------------------------------------

import logging  # noqa: E402
import re  # noqa: E402
import sys  # noqa: E402
import threading  # noqa: E402

logger = logging.getLogger(__name__)

CACHE_FILE = "cache/pop_locations.json"

_DEFAULT_POP_COORDS: dict[str, tuple[float, float]] = {
    "SJC": (37.3382, -121.8863),
    "IAD": (38.9531, -77.4565),
    "NRT": (35.7720, 140.3929),
}
_SAFE_POP_CODE_RE = re.compile(r"^[A-Z0-9_]+$")

_lat_lon_cache: tuple[int, dict] | None = None
_lat_lon_cache_lock = threading.Lock()

_loc_cache: tuple[int, dict] | None = None
_loc_cache_lock = threading.Lock()


def fetch_pop_locations(api_key: str) -> bool:
    """Fetch Fastly POP locations using the provided token and cache them.
    Returns True if successful.
    """
    if not api_key:
        return False

    try:
        from backend.core.fastly.client import fastly

        pops = fastly("GET", "/datacenters", token=api_key)
        os.makedirs(os.path.dirname(CACHE_FILE), exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(pops, f)
        return True
    except Exception:
        logger.warning("POP fetch failed", exc_info=True)
        return False


def get_pop_locations():
    """Return cached POP locations if available, else empty list."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                return json.load(f)
        except Exception:
            logger.debug("get_pop_locations: step failed (ignored)", exc_info=True)
    return []


def _load_mtime_cached(slot: str, lock: threading.Lock, builder: Callable[[list[Any]], dict]) -> dict:
    try:
        mtime_ns = os.stat(CACHE_FILE).st_mtime_ns
    except FileNotFoundError:
        return {}

    cached = globals()[slot]
    if cached is not None and cached[0] == mtime_ns:
        return cached[1]

    resp_data = get_pop_locations()
    if not isinstance(resp_data, list):
        return {}

    result = builder(resp_data)
    with lock:
        globals()[slot] = (mtime_ns, result)
    return result


def get_pop_lat_lon_map():
    """Return a mapping of POP code to (lat, lon) using cached data."""
    return _load_mtime_cached(
        "_lat_lon_cache",
        _lat_lon_cache_lock,
        lambda resp_data: {
            str(p["code"]).upper(): (p["coordinates"]["latitude"], p["coordinates"]["longitude"])
            for p in resp_data
            if p.get("code")
            and "coordinates" in p
            and p["coordinates"].get("latitude") is not None
            and p["coordinates"].get("longitude") is not None
        },
    )


def get_pop_location_map() -> dict[str, dict]:
    """Return ``{CODE: {"city", "region", "country"}}`` parsed from cached Fastly ``/datacenters``."""

    def _build(resp_data: list[Any]) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for p in resp_data:
            if not isinstance(p, dict) or not p.get("code"):
                continue
            shield_parts = str(p.get("shield") or "").split("-")
            region = shield_parts[1] if len(shield_parts) == 3 else ""
            country = shield_parts[-1] if len(shield_parts) >= 2 else ""
            result[str(p["code"]).upper()] = {
                "city": str(p.get("name") or ""),
                "region": region,
                "country": country,
            }
        return result

    return _load_mtime_cached("_loc_cache", _loc_cache_lock, _build)


def _effective_pop_lat_lon_map() -> dict[str, tuple[float, float]]:
    merged = dict(_DEFAULT_POP_COORDS)
    for code, coords in get_pop_lat_lon_map().items():
        norm = str(code).upper()
        if _SAFE_POP_CODE_RE.fullmatch(norm) and isinstance(coords, (tuple, list)) and len(coords) == 2:
            try:
                merged[norm] = (float(coords[0]), float(coords[1]))
            except (TypeError, ValueError):
                continue
    return merged


def _haversine_sql_expr(lat_col: str, lon_col: str, pop_lat: float, pop_lon: float) -> str:
    return (
        f"6371 * 2 * ASIN(SQRT(POWER(SIN(RADIANS({lat_col} - {pop_lat}) / 2), 2) "
        f"+ COS(RADIANS({pop_lat})) * COS(RADIANS({lat_col})) "
        f"* POWER(SIN(RADIANS({lon_col} - {pop_lon}) / 2), 2)))"
    )


def _build_pop_case_sql(
    pop_col: str,
    value_fn: Callable[[float, float], object],
    else_expr: object,
) -> str:
    coords = _effective_pop_lat_lon_map()
    whens = [f"WHEN {pop_col} = '{code}' THEN {value_fn(plat, plon)}" for code, (plat, plon) in coords.items()]
    return "CASE " + " ".join(whens) + f" ELSE {else_expr} END"


def build_pop_haversine_case_sql(
    pop_col: str = "pop",
    lat_col: str = "lat",
    lon_col: str = "lon",
    default_lat: float = 38.0,
    default_lon: float = -97.0,
) -> str:
    """Build a SQL CASE expression computing Haversine distance (km) from POP to client (lat, lon)."""
    return _build_pop_case_sql(
        pop_col,
        lambda plat, plon: _haversine_sql_expr(lat_col, lon_col, plat, plon),
        _haversine_sql_expr(lat_col, lon_col, float(default_lat), float(default_lon)),
    )


def build_pop_lat_case_sql(pop_col: str = "pop", default_lat: float = 38.0) -> str:
    """Build a SQL CASE expression mapping POP code to latitude."""
    return _build_pop_case_sql(pop_col, lambda plat, _plon: plat, float(default_lat))


def build_pop_lon_case_sql(pop_col: str = "pop", default_lon: float = -97.0) -> str:
    """Build a SQL CASE expression mapping POP code to longitude."""
    return _build_pop_case_sql(pop_col, lambda _plat, plon: plon, float(default_lon))


sys.modules["backend.utils.pop_utils"] = sys.modules[__name__]
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    _utils_pkg.pop_utils = sys.modules[__name__]  # type: ignore[attr-defined]

sys.modules["backend.utils.countries"] = sys.modules[__name__]
if _utils_pkg is not None:
    _utils_pkg.countries = sys.modules[__name__]  # type: ignore[attr-defined]


# --- Merged from backend/utils/remote_access.py ---


import hmac
import logging
import re
import sys
import threading
from collections import deque
from typing import Literal
from urllib.parse import urlparse, urlsplit

import cachetools
from fastapi import Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

logger = logging.getLogger(__name__)


class SlidingWindowLimiter:
    def __init__(self, *, window_s: float, max_keys: int, clock: Callable[[], float] | None = None) -> None:
        self.window_s = window_s
        self.max_keys = max_keys
        self._clock = clock
        self._lock = threading.Lock()
        self._events: cachetools.LRUCache[str, deque[tuple[float, int]]] = cachetools.LRUCache(maxsize=max(1, max_keys))
        self._locks: cachetools.LRUCache[str, float] = cachetools.LRUCache(maxsize=max(1, max_keys))

    @property
    def MAX_TRACKED_IPS(self) -> int:
        return self.max_keys

    @MAX_TRACKED_IPS.setter
    def MAX_TRACKED_IPS(self, value: int) -> None:
        self.max_keys = value
        self._events._Cache__maxsize = max(1, value)
        self._locks._Cache__maxsize = max(1, value)

    def _now(self) -> float:
        return self._clock() if self._clock is not None else time.time()

    def _prune_key(self, key: str, cutoff: float) -> deque[tuple[float, int]]:
        events = self._events.get(key)
        if events is None:
            return deque()
        while events and events[0][0] < cutoff:
            events.popleft()
        return events

    def _prune_now(self, key: str) -> tuple[float, float, deque[tuple[float, int]]]:
        now = self._now()
        cutoff = now - self.window_s
        return now, cutoff, self._prune_key(key, cutoff)

    def _store(self, key: str, events: deque[tuple[float, int]], cutoff: float) -> None:
        self._events._Cache__maxsize = max(1, self.max_keys, len(self._events) + 1)
        self._events.pop(key, None)
        self._events[key] = events
        while self._events:
            oldest, history = next(iter(self._events._Cache__data.items()))
            if history and history[-1][0] >= cutoff:
                break
            del self._events[oldest]
        while len(self._events) > self.max_keys:
            self._events.popitem()
        self._events._Cache__maxsize = max(1, self.max_keys)

    def try_acquire(
        self, key: str, weight: int = 0, *, max_events: int | None = None, max_weight: int | None = None
    ) -> bool:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            if max_events is not None and len(events) >= max_events:
                self._store(key, events, cutoff)
                return False
            events.append((now, max(0, int(weight))))
            self._store(key, events, cutoff)
            return max_weight is None or sum(w for _, w in events) <= max_weight

    def strike(self, key: str, *, threshold: int, lockout_s: float, max_locks: int | None = None) -> bool:
        with self._lock:
            now, cutoff, events = self._prune_now(key)
            events.append((now, 1))
            self._store(key, events, cutoff)
            if len(events) >= threshold and key not in self._locks:
                cap = max_locks if max_locks is not None else self.max_keys
                self._locks._Cache__maxsize = max(1, cap, len(self._locks) + 1)
                self._locks[key] = now + lockout_s
                while len(self._locks) > cap:
                    self._locks.popitem()
                self._locks._Cache__maxsize = max(1, cap)
                return True
            return False

    def is_locked(self, key: str) -> tuple[bool, int]:
        with self._lock:
            until = self._locks.get(key)
            if until is None:
                return False, 0
            now = self._now()
            if now >= until:
                del self._locks[key]
                return False, 0
            return True, int(until - now)

    def snapshot(self) -> tuple[dict[str, int], dict[str, int]]:
        with self._lock:
            now = self._now()
            cutoff = now - self.window_s
            counts: dict[str, int] = {}
            for key in list(self._events):
                events = self._prune_key(key, cutoff)
                if events:
                    counts[key] = len(events)
                else:
                    del self._events[key]
            locks: dict[str, int] = {}
            for key, until in list(self._locks.items()):
                if until <= now:
                    del self._locks[key]
                else:
                    locks[key] = int(until - now)
            return counts, locks


ANALYST_SESSION_COOKIE = "analyst_session_id"
ANALYST_PENDING_SESSION_COOKIE = "analyst_pending_session_id"


def analyst_cookie_is_secure(request: Request, public_url: str | None) -> bool:
    if request.url.scheme.lower() == "https":
        return True
    if not public_url:
        return False
    try:
        parsed = urlsplit(public_url)
    except ValueError:
        return False
    return parsed.scheme.lower() == "https" and bool(parsed.netloc)


def set_analyst_session_cookies(
    response: Response,
    session_id: str,
    *,
    tos_pending: bool,
    secure: bool,
    samesite: Literal["strict", "lax"] = "strict",
) -> None:
    target_cookie = ANALYST_PENDING_SESSION_COOKIE if tos_pending else ANALYST_SESSION_COOKIE
    other_cookie = ANALYST_SESSION_COOKIE if tos_pending else ANALYST_PENDING_SESSION_COOKIE
    response.set_cookie(
        key=target_cookie,
        value=session_id,
        httponly=True,
        secure=secure,
        samesite=samesite,
        max_age=24 * 60 * 60,
        path="/",
    )
    response.delete_cookie(other_cookie, path="/")


def safe_audit_email(email: str | None) -> str:
    if not email:
        return "-"
    cleaned = "".join(c for c in email if c.isprintable())
    return cleaned[:254] or "-"


def analyst_allowed_services(request: Request) -> set[str] | None:
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        return None
    return set(analyst_session.service_ids or [])


def require_service_in_scope(request: Request, service_id: str | None) -> set[str] | None:
    allowed = analyst_allowed_services(request)
    if allowed is not None and service_id not in allowed:
        raise HTTPException(status_code=403, detail={"error": "service_not_authorized", "service": service_id})
    return allowed


def require_owned_row(
    request: Request, service_id: str, row_id: str, fetch: Callable[[str, str], dict | None]
) -> dict | None:
    allowed = require_service_in_scope(request, service_id)
    if allowed is None:
        return None
    existing = fetch(row_id, service_id)
    if existing and existing.get("service_id") not in allowed:
        raise HTTPException(
            status_code=403, detail={"error": "service_not_authorized", "service": existing.get("service_id")}
        )
    return existing


def mask_ips_for(analyst_session: object | None) -> bool:
    if analyst_session is None:
        return False
    policy = getattr(analyst_session, "pii_policy", None)
    return bool(policy.get("mask_ips")) if isinstance(policy, dict) else False


sys.modules["backend.utils.rate_limit"] = sys.modules[__name__]
sys.modules["backend.utils.auth"] = sys.modules[__name__]
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    _utils_pkg.rate_limit = sys.modules[__name__]  # type: ignore[attr-defined]
    _utils_pkg.auth = sys.modules[__name__]  # type: ignore[attr-defined]
from backend.core import share_db  # noqa: E402
from backend.core.share_db.validation import IP_FAMILY_KEYS, SESSION_ID_KEYS  # noqa: E402
from backend.utils.tunnel import compute_fingerprint, get_tunnel_manager  # noqa: E402

ADMIN_GATEWAY_HEADER = "X-Admin-Gateway-Token"


def admin_gateway_required() -> bool:
    return os.getenv("ADMIN_GATEWAY_REQUIRED", "") == "1"


def validate_admin_gateway_config() -> None:
    if os.getenv("ADMIN_GATEWAY_REQUIRED", "") not in ("", "0", "1"):
        raise ValueError("ADMIN_GATEWAY_REQUIRED must be 0 or 1")
    secret = os.getenv("ADMIN_GATEWAY_SECRET", "")
    if (admin_gateway_required() or secret) and len(secret) < 32:
        raise ValueError("ADMIN_GATEWAY_SECRET must contain at least 32 characters for admin gateway access")


def authenticated_admin_gateway(request: Request) -> bool:
    if getattr(request.state, "admin_gateway_authenticated", False):
        return True
    if request.headers.get("x-proxied-by-caddy"):
        return False
    secret = os.getenv("ADMIN_GATEWAY_SECRET", "")
    values = request.headers.getlist(ADMIN_GATEWAY_HEADER)
    if len(secret) < 32 or len(values) != 1:
        return False
    return hmac.compare_digest(values[0].encode("utf-8"), secret.encode("utf-8"))


BODY_INSPECT_MAX_BYTES = 4 * 1024 * 1024
_UNAUTH_ANALYST_PATHS = {
    "/api/share/login",
    "/api/share/logout",
    "/api/share/heartbeat",
    "/api/share/acknowledge",
    "/api/share/tos",
    "/api/share/auth-config",
    "/api/health",
    "/api/bootstrap",
    "/js/rum.js",
    "/js/faro-sdk.js",
    "/rum-beacon",
    "/api/web-vitals",
    "/api/ux-events",
}
_ANALYST_BLOCKED_PREFIXES = (
    "/api/admin/",
    "/api/provision/",
    "/api/debug/",
    "/api/usage/",
    "/api/cron-runs",
    "/api/audit-logs",
    "/api/alerts",
)
_ANALYST_BLOCKED_SUBPATHS = (
    "/api/download",
    "/api/download-all",
    "/api/download-folder",
    "/api/cron-schedule",
    "/api/sync-status",
    "/openapi.json",
    "/docs",
    "/docs/oauth2-redirect",
    "/redoc",
)
_ANALYST_BLOCKED_SUBPATH_REGEX: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/services/[^/]+/lake-info$"),
    re.compile(r"^/api/services/[^/]+/logging-settings(/.*)?$"),
    re.compile(r"^/api/services/[^/]+/log-fields$"),
    re.compile(r"^/api/services/[^/]+/custom-fields(/.*)?$"),
    re.compile(r"^/api/services/[^/]+/log-field-audit$"),
)
_ANALYST_BLOCKED_SCORING_SUFFIXES: tuple[str, ...] = (
    "/config",
    "/status",
    "/audit",
    "/threshold",
    "/exclude-regex",
    "/exclude-regex/validate",
    "/enforce-status-code",
    "/enforce-threshold",
    "/l2-enforce",
    "/matrix-versions",
    "/restore",
    "/retrain",
    "/rotate-key",
    "/enable",
    "/disable",
    "/dashboard",
    "/evaluation/per-reason",
)
_ANALYST_BLOCKED_RUM_SUFFIXES: tuple[str, ...] = ("/enable", "/disable", "/versions", "/upgrade", "/settings")
_ANALYST_ALLOWED_WRITE_PREFIXES = (
    "/api/share/",
    "/api/dashboard/",
    "/api/security/",
    "/api/origin/",
    "/api/performance/",
    "/api/insights",
    "/api/network-health",
    "/api/network-quality",
    "/api/network/",
    "/api/query",
    "/api/sessions",
    "/api/cmcd/",
    "/api/value/",
    "/api/web-vitals",
    "/api/charts/",
    "/api/ux-events",
    "/api/assets/",
    "/api/high-scale/",
)
_TELEMETRY_IDLE_EXEMPT_PREFIXES = ("/api/web-vitals", "/api/ux-events")
_ANALYST_SSE_ALLOWLIST: set[str] = {"/api/log-extents/stream", "/realtime-stream", "/control-room/error-stream"}
_LOCAL_HOST_ALLOWLIST = {
    "localhost",
    "127.0.0.1",
    "[::1]",
    "0.0.0.0",
    "testserver",
    "backend",
    "frontend",
    "caddy",
    "web",
}
_env_hosts = os.getenv("LOCAL_HOSTS") or os.getenv("LOCAL_HOST_ALLOWLIST") or os.getenv("ALLOWED_HOSTS")
if _env_hosts:
    for _h in _env_hosts.split(","):
        _clean_h = _h.strip().lower()
        if _clean_h:
            _LOCAL_HOST_ALLOWLIST.add(_clean_h)
import ipaddress

_LOCAL_ADMIN_CIDRS_ENV = "LOCAL_ADMIN_CIDRS"
_local_admin_cidrs_cache: tuple[str, tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]] = ("", ())


def _local_admin_networks() -> tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]:
    global _local_admin_cidrs_cache
    raw = os.environ.get(_LOCAL_ADMIN_CIDRS_ENV, "")
    if raw == _local_admin_cidrs_cache[0]:
        return _local_admin_cidrs_cache[1]
    nets: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            net = ipaddress.ip_network(part, strict=False)
        except ValueError:
            logger.error("[remote_access] ignoring invalid %s entry %r", _LOCAL_ADMIN_CIDRS_ENV, part)
            continue
        if not net.is_private or net.is_link_local or net.num_addresses > 2**16:
            logger.error(
                "[remote_access] REFUSING %s entry %r — admin trust segments must "
                "be private, non-link-local, and no wider than a /16. Scope it to "
                "the isolated infra network.",
                _LOCAL_ADMIN_CIDRS_ENV,
                part,
            )
            continue
        nets.append(net)
    _local_admin_cidrs_cache = (raw, tuple(nets))
    return _local_admin_cidrs_cache[1]


def _is_private_or_loopback(ip_str: str) -> bool:
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return ip_str in ("testclient", "localhost")
    if ip.is_loopback:
        return True
    return any(ip in net for net in _local_admin_networks())


def is_request_remote(request: Request) -> bool:
    if authenticated_admin_gateway(request):
        return False
    if admin_gateway_required() or ADMIN_GATEWAY_HEADER in request.headers:
        return True
    if request.client is None:
        return True
    host = client_ip(request, default="127.0.0.1")
    if not _is_private_or_loopback(host):
        return True
    if request.headers.get("x-remote-analyst") == "1":
        mgr = get_tunnel_manager()
        if mgr.is_sharing_active():
            return True
    return False


def client_ip(request: Request, *, default: str = "0.0.0.0") -> str:
    return request.client.host if request.client else default


def _host_base(host_header: str) -> str:
    return host_header.split(":")[0].lower() if host_header else ""


def _in_local_allowlist(base: str) -> bool:
    return "*" in _LOCAL_HOST_ALLOWLIST or base in _LOCAL_HOST_ALLOWLIST


def _local_host_allowed(host_header: str) -> bool:
    base = _host_base(host_header)
    return bool(base) and (_is_private_or_loopback(base) or _in_local_allowlist(base))


def _allowed_remote_hosts() -> list[str]:
    state = get_tunnel_manager().state
    candidates: list[str] = []
    if state.public_endpoint:
        hostname = urlparse(state.public_endpoint).hostname
        if hostname:
            candidates.append(hostname.lower())
    try:
        from backend import config

        for cfg in config.list_configs():
            rf = cfg.get("remote_frontend")
            if isinstance(rf, dict):
                candidates.extend(
                    val.strip().lower()
                    for k in ("domain_name", "origin_host", "override_host")
                    if isinstance(val := rf.get(k), str) and val
                )
    except Exception:
        logger.debug("_allowed_remote_hosts: step failed (ignored)", exc_info=True)
    return candidates


def _remote_host_allowed(host_header: str) -> bool:
    base = _host_base(host_header)
    return bool(base) and (_in_local_allowlist(base) or base in _allowed_remote_hosts())


def _origin_allowed(origin: str) -> bool:
    host = (urlparse(origin).hostname or "").lower() if origin else ""
    return bool(host) and host in _allowed_remote_hosts()


def _is_blocked_path(path: str) -> bool:
    normalized = path.rstrip("/\r\n") or "/"
    if any(normalized == p.rstrip("/") or normalized.startswith(p) for p in _ANALYST_BLOCKED_PREFIXES):
        return True
    for sp in _ANALYST_BLOCKED_SUBPATHS:
        if normalized == sp or normalized.startswith(sp + "/") or normalized.startswith(sp + "?"):
            return True
    if "/scoring/" in normalized and normalized.endswith(_ANALYST_BLOCKED_SCORING_SUFFIXES):
        return True
    if "/rum/" in normalized and normalized.endswith(_ANALYST_BLOCKED_RUM_SUFFIXES):
        return True
    if any(pat.fullmatch(normalized) for pat in _ANALYST_BLOCKED_SUBPATH_REGEX):
        return True
    return False


_PATH_SERVICE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/services/([^/]+)(?:/|$)"),
    re.compile(r"^/api/alerts/([^/]+)(?:/|$)"),
    re.compile(r"^/api/views/([^/]+)(?:/|$)"),
)


def _path_service_ids(request: Request) -> list[str]:
    routes = getattr(getattr(getattr(request, "app", None), "router", None), "routes", None)
    if routes:
        from starlette.routing import Match

        for route in routes:
            match, child_scope = route.matches(request.scope)
            if match == Match.FULL:
                path_params = child_scope.get("path_params", {})
                out = [path_params[k] for k in ("service_id", "service") if k in path_params]
                if out:
                    return out
                break
    path = request.url.path
    return [m.group(1) for pat in _PATH_SERVICE_PATTERNS if (m := pat.match(path))]


def _is_sse_route(path: str) -> bool:
    if "/sse" in path:
        return True
    last_segment = path.rsplit("/", 1)[-1]
    return last_segment == "stream" or last_segment.endswith("-stream")


def apply_response_hardening(response: Response) -> Response:
    response.headers.setdefault("Cache-Control", "private, no-store")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("Permissions-Policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()")
    response.headers.setdefault(
        "Content-Security-Policy",
        (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' blob:; "
            "worker-src 'self' blob:; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: blob:; "
            "font-src 'self' data:; "
            "connect-src 'self'; "
            "frame-ancestors 'none'; "
            "base-uri 'self'; "
            "form-action 'self'"
        ),
    )
    response.headers.setdefault("Cross-Origin-Opener-Policy", "same-origin")
    return response


_QUERY_DEBUG_KEYS: tuple[str, ...] = (
    "_debug_queries",
    "debug_queries",
    "_debug_sqlite",
    "debug_sqlite",
    "_debug_postgres",
    "debug_postgres",
)
_API_DEBUG_KEYS: tuple[str, ...] = ("_debug_calls", "debug_calls")


def analyst_debug_visibility(request: Request | None = None) -> tuple[bool, bool]:
    if request is not None:
        cached = getattr(request.state, "_analyst_debug_vis", None)
        if cached is not None:
            return cached
    try:
        from backend.core.share_db.settings import get_setting

        q_vis = get_setting("query_debug_visibility", "disabled")
        a_vis = get_setting("api_call_debug_visibility", "disabled")
        res = (q_vis in ("analysts", "both"), a_vis in ("analysts", "both"))
    except Exception:
        res = (False, False)
    if request is not None:
        request.state._analyst_debug_vis = res
    return res


def _rebuild_response(response: Response, content: bytes, media_type: str, *, update_length: bool = False) -> Response:
    headers = dict(response.headers)
    if update_length:
        headers["content-length"] = str(len(content))
    return Response(content=content, status_code=response.status_code, headers=headers, media_type=media_type)


async def _strip_analyst_envelope(
    response: Response, analyst_session: object | None = None, request: Request | None = None
) -> Response:
    ct = response.headers.get("content-type", "")
    if "application/json" not in ct:
        return response
    raw_policy = getattr(analyst_session, "pii_policy", None) if analyst_session is not None else None
    pii_policy = raw_policy if isinstance(raw_policy, dict) and raw_policy.get("mask_ips") else None
    body = b""
    async for chunk in response.body_iterator:  # type: ignore[attr-defined]
        body += chunk
    try:
        data = json.loads(body)
    except (json.JSONDecodeError, ValueError):
        return _rebuild_response(response, body, ct)
    changed = False
    if isinstance(data, dict):
        if request is not None and "_debug_queries" in data and "_debug_calls" in data:
            request.state._analyst_envelope_complete = True
        analyst_query_enabled, analyst_api_enabled = analyst_debug_visibility(request)
        from backend.models.common import _debug_responses_enabled

        if not _debug_responses_enabled():
            to_strip = (*_QUERY_DEBUG_KEYS, *_API_DEBUG_KEYS)
        else:
            to_strip = (
                *(_QUERY_DEBUG_KEYS if not analyst_query_enabled else ()),
                *(_API_DEBUG_KEYS if not analyst_api_enabled else ()),
            )
        for k in to_strip:
            if k in data:
                data.pop(k)
                changed = True
    if pii_policy is not None:
        from backend.core.share_db.validation import apply_pii_policy

        masked = apply_pii_policy(data, pii_policy)
        if masked is not data:
            data = masked
            changed = True
    if not changed:
        return _rebuild_response(response, body, ct)
    new_body = json.dumps(data, separators=(",", ":")).encode()
    return _rebuild_response(response, new_body, ct, update_length=True)


_BODY_INSPECT_UNSET = object()
_BODY_INSPECT_ATTR = "_analyst_body_json"


async def _inspect_request_body(request: Request) -> dict | None:
    cached = getattr(request.state, _BODY_INSPECT_ATTR, _BODY_INSPECT_UNSET)
    if cached is not _BODY_INSPECT_UNSET:
        return cached  # type: ignore[return-value]
    parsed = await _drain_request_body(request)
    setattr(request.state, _BODY_INSPECT_ATTR, parsed)
    return parsed


async def _drain_request_body(request: Request) -> dict | None:
    method = request.method.upper()
    if method != "POST":
        return None
    ct = request.headers.get("content-type", "")
    if "application/json" not in ct.lower():
        return None
    receive = request._receive  # type: ignore[attr-defined]
    chunks: list[bytes] = []
    bytes_read = 0
    try:
        more_body = True
        while more_body:
            msg = await receive()
            if msg.get("type") != "http.request":
                return None
            chunk = msg.get("body", b"")
            chunks.append(chunk)
            bytes_read += len(chunk)
            if bytes_read > BODY_INSPECT_MAX_BYTES:
                break
            more_body = bool(msg.get("more_body", False))
    except Exception:
        return None
    body_bytes = b"".join(chunks)
    sent = False

    async def _replay():
        nonlocal sent
        if sent:
            return {"type": "http.disconnect"}
        sent = True
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    request._receive = _replay  # type: ignore[attr-defined]
    if hasattr(request, "_body"):
        try:
            del request._body  # type: ignore[attr-defined]
        except AttributeError:
            pass
    if not body_bytes:
        return None
    try:
        body = json.loads(body_bytes)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(body, dict):
        return None
    return body


async def _body_service_ids(request: Request) -> list[str]:
    body = await _inspect_request_body(request) or {}
    return [str(v) for k in ("service_id", "service") if isinstance(v := body.get(k), (str, int)) and str(v)]


_PII_FORBIDDEN_FILTER_COLS = IP_FAMILY_KEYS | SESSION_ID_KEYS


async def _body_filter_keys(request: Request) -> list[str]:
    body = await _inspect_request_body(request) or {}
    filters = body.get("filters")
    return [str(k) for k in filters] if isinstance(filters, dict) else []


async def _body_field_param(request: Request) -> str | None:
    body = await _inspect_request_body(request) or {}
    field = body.get("field")
    return field if isinstance(field, str) else None


class _StaticAssetLimiter(SlidingWindowLimiter):
    def __init__(
        self,
        *,
        req_limit: int = 600,
        byte_limit: int = 50 * 1024 * 1024,
        window_s: int = 60,
        max_tracked_ips: int = 10_000,
    ) -> None:
        super().__init__(window_s=window_s, max_keys=max_tracked_ips)
        self.REQ_LIMIT = req_limit
        self.BYTE_LIMIT = byte_limit

    def check(self, ip: str, content_length: int) -> bool:
        return self.try_acquire(ip, content_length, max_events=self.REQ_LIMIT, max_weight=self.BYTE_LIMIT)

    def clear(self) -> None:
        with self._lock:
            self._events.clear()
            self._locks.clear()


_static_limiter = _StaticAssetLimiter()
_analyst_api_limiter = _StaticAssetLimiter()


def _new_request_id() -> str:
    return secrets.token_hex(8)


def _error_json(status_code: int, error: str, **extra: object) -> JSONResponse:
    return JSONResponse(status_code=status_code, content={"error": error, **extra})


def _log_access(logger_name: str, fmt: str, *args: object) -> None:
    try:
        logging.getLogger(logger_name).info(fmt, *args)
    except Exception:
        logger.debug("dispatch: logging.getLogger failed (ignored)", exc_info=True)


def _check_rate_limit(request: Request, path: str) -> JSONResponse | None:
    content_len = int(request.headers.get("content-length") or "0")
    if path.startswith(("/_next/", "/static/")):
        if not _static_limiter.check(client_ip(request), content_len):
            return _error_json(429, "rate_limited")
    elif path.startswith("/api/"):
        ip = client_ip(request)
        if not _is_private_or_loopback(ip) and not _analyst_api_limiter.check(ip, content_len):
            return _error_json(429, "rate_limited")
    return None


async def _enforce_service_and_pii_scope(request: Request, session) -> JSONResponse | None:
    from backend import config as svcconfig

    raw_candidates: list[str] = list(_path_service_ids(request))
    for src in (
        request.query_params.get("service"),
        request.query_params.get("service_id"),
        request.headers.get("x-fastly-service-id"),
        request.headers.get("x-service-id"),
    ):
        if src:
            raw_candidates.append(src)
    raw_candidates.extend(await _body_service_ids(request))
    cdn_map = svcconfig.get_cdn_service_id_map() if raw_candidates else {}
    resolved_candidates = [cand if svcconfig.load_config(cand) else cdn_map.get(cand, cand) for cand in raw_candidates]
    if not resolved_candidates:
        fallback = svcconfig.get_active_service_id()
        if fallback:
            resolved_candidates.append(fallback)
    allowed_services = set(session.service_ids or [])
    if not resolved_candidates:
        return _error_json(403, "service_not_authorized", service="")
    for eff in resolved_candidates:
        if not eff or eff not in allowed_services:
            return _error_json(403, "service_not_authorized", service=eff or "")
    if session.pii_policy.get("mask_ips"):
        from backend.repositories.utils.filters import normalize_filter_key

        dim = await _body_field_param(request)
        raw_keys = [*(await _body_filter_keys(request)), *((dim,) if dim is not None else ())]
        for raw_key in raw_keys:
            col = normalize_filter_key(raw_key)
            if col in _PII_FORBIDDEN_FILTER_COLS:
                return _error_json(403, "pii_policy_violation", field=col)
    return None


class RemoteAccessMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        method = request.method.upper()
        host_header = request.headers.get("host", "")
        gateway_authenticated = authenticated_admin_gateway(request)
        if ADMIN_GATEWAY_HEADER in request.headers and not gateway_authenticated:
            logger.warning("[remote_access] rejected invalid admin gateway credential")
            return _error_json(401, "admin_gateway_invalid")
        request.state.admin_gateway_authenticated = gateway_authenticated
        if gateway_authenticated:
            request.scope["headers"] = [
                (key, value)
                for key, value in request.scope["headers"]
                if key.lower() != ADMIN_GATEWAY_HEADER.lower().encode()
            ]
            if hasattr(request, "_headers"):
                del request._headers
        is_remote = is_request_remote(request)
        request.state.is_remote = is_remote
        request.state.analyst_session = None
        request.state.request_id = _new_request_id()
        mgr = get_tunnel_manager()
        if (is_remote and not _remote_host_allowed(host_header)) or (
            not is_remote and host_header and not _local_host_allowed(host_header)
        ):
            return _error_json(400, "host_not_allowed", host=host_header)
        if gateway_authenticated:
            origin = request.headers.get("origin")
            cross_site = request.headers.get("sec-fetch-site") == "cross-site"
            unsafe_origin = (
                method not in ("GET", "HEAD", "OPTIONS")
                and origin is not None
                and origin.lower() != f"https://{host_header.lower()}"
            )
            if cross_site or unsafe_origin:
                logger.warning("[remote_access] rejected cross-origin admin gateway request")
                return _error_json(403, "admin_origin_not_allowed")
        if not is_remote:
            _t0 = time.perf_counter()
            response = await call_next(request)
            _dur_ms = (time.perf_counter() - _t0) * 1000.0
            _log_access(
                "backend.access.admin",
                "[admin] [%s] rid=%s %s %s -> %d (%.1fms)",
                client_ip(request, default="127.0.0.1"),
                getattr(request.state, "request_id", "-"),
                method,
                path,
                response.status_code,
                _dur_ms,
            )
            return apply_response_hardening(response)
        if (rate_err := _check_rate_limit(request, path)) is not None:
            return rate_err
        if method not in ("GET", "HEAD") and not path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES):
            origin = request.headers.get("origin", "")
            if not _origin_allowed(origin):
                return _error_json(403, "origin_not_allowed", origin=origin)
        if path in _UNAUTH_ANALYST_PATHS or path.startswith(("/api/share/claim/", "/api/share/oauth/", "/js/")):
            response = await call_next(request)
            if path == "/api/bootstrap":
                sid_boot = request.cookies.get(ANALYST_SESSION_COOKIE)
                boot_session = mgr.validate_session(sid_boot) if sid_boot else None
                response = await _strip_analyst_envelope(response, analyst_session=boot_session, request=request)
            return apply_response_hardening(response)
        session = mgr.validate_session(request.cookies.get(ANALYST_SESSION_COOKIE))
        if session is None:
            return _error_json(401, "unauthenticated")
        if getattr(session, "tos_pending", False):
            return _error_json(403, "tos_pending")
        from backend.core.query_attribution import Attribution as _Attr
        from backend.core.query_attribution import current_attribution as _cur_attr

        _cur_attr.set(
            _Attr.analyst(
                analyst_id=getattr(session, "session_id", None) or "unknown",
                analyst_name=getattr(session, "name", None) or None,
                request_path=path,
                request_id=getattr(request.state, "request_id", None),
            )
        )
        headers_lc = {k.lower(): v for k, v in request.headers.items()}
        if compute_fingerprint(headers_lc) != session.fingerprint_signature:
            mgr.boot_session(session.session_id, reason="fingerprint mismatch")
            share_db.log_share_audit_event(
                event_type="FINGERPRINT_MISMATCH",
                email=session.email,
                ip_address=client_ip(request),
                details=f"path={path}",
            )
            return _error_json(401, "fingerprint_mismatch")
        if _is_blocked_path(path):
            return _error_json(403, "admin_only")
        if _is_sse_route(path) and not any(
            path == allowed
            or path.endswith(allowed)
            or (allowed.startswith("/api/") and path.startswith(f"{allowed}/"))
            for allowed in _ANALYST_SSE_ALLOWLIST
        ):
            return _error_json(403, "sse_blocked")
        if method in ("PUT", "PATCH", "DELETE") or (
            method == "POST" and not path.startswith(_ANALYST_ALLOWED_WRITE_PREFIXES)
        ):
            return _error_json(403, "read_only")
        if not (path.startswith("/api/share/") or path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES)):
            if (scope_err := await _enforce_service_and_pii_scope(request, session)) is not None:
                return scope_err
        current_ip = client_ip(request)
        if current_ip != session.ip_address:
            invite = share_db.get_remote_invite(session.invite_id)
            if invite and share_db.ip_in_whitelist(current_ip, invite.get("ip_whitelist")):
                mgr.touch_session(session.session_id, new_ip=current_ip, bump_active=False)
            else:
                return _error_json(403, "ip_not_whitelisted")
        request.state.analyst_session = session
        _idle_exempt = path.startswith(_TELEMETRY_IDLE_EXEMPT_PREFIXES) or _is_sse_route(path)
        _active_hdr = request.headers.get("x-user-active")
        _touched_idle = not _idle_exempt and _active_hdr != "0"
        if _touched_idle:
            mgr.touch_session(session.session_id, last_activity=f"{method} {path} act={_active_hdr or '-'}")
        _t0 = time.perf_counter()
        response = await call_next(request)
        _dur_ms = (time.perf_counter() - _t0) * 1000.0
        _log_access(
            "backend.access.analyst",
            "[analyst] %s (%s) [%s] rid=%s %s %s -> %d (%.1fms) act=%s idle_touch=%d",
            session.email,
            session.name or "no-name",
            client_ip(request),
            getattr(request.state, "request_id", "-"),
            method,
            path,
            response.status_code,
            _dur_ms,
            _active_hdr or "-",
            int(_touched_idle),
        )
        response = await _strip_analyst_envelope(response, analyst_session=session, request=request)
        apply_response_hardening(response)
        return response


from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from backend.utils.date_utils import parse_iso_utc

MAX_ANALYST_QUERY_SPAN = timedelta(days=366)


@dataclass
class TimeBounds:
    start: datetime | None = None
    end: datetime | None = None

    def clamp(
        self, req_start: datetime | None, req_end: datetime | None, *, max_span: timedelta | None = None
    ) -> tuple[datetime, datetime]:
        candidates_s = [c for c in (req_start, self.start) if c is not None]
        candidates_e = [c for c in (req_end, self.end) if c is not None]
        eff_start = max(candidates_s) if candidates_s else None
        eff_end = min(candidates_e) if candidates_e else None
        if eff_end is None:
            eff_end = datetime.now(UTC)
        if eff_start is None:
            eff_start = eff_end - timedelta(hours=1)
        if max_span is not None and (eff_end - eff_start) > max_span:
            eff_start = eff_end - max_span
        if eff_start >= eff_end:
            raise ValueError("clamped time range is empty")
        return eff_start, eff_end


def _time_bounds_from_params(
    query_start_time: str | None,
    query_end_time: str | None,
    query_window_hours: int | None,
    *,
    now: datetime | None = None,
) -> TimeBounds:
    end = parse_iso_utc(query_end_time) if query_end_time else None
    start = parse_iso_utc(query_start_time) if query_start_time else None
    if query_window_hours:
        anchor = now if now is not None else datetime.now(UTC)
        relative_start = anchor - timedelta(hours=int(query_window_hours))
        start = max(start, relative_start) if start else relative_start
        end = min(end, anchor) if end else anchor
    return TimeBounds(start=start, end=end)


def get_analyst_time_bounds(request: Request) -> TimeBounds:
    session = getattr(request.state, "analyst_session", None)
    if session is None:
        return TimeBounds()
    return _time_bounds_from_params(session.query_start_time, session.query_end_time, session.query_window_hours)


def analyst_clamp_cache_key(
    query_start_time: str | None, query_end_time: str | None, query_window_hours: int | None
) -> str:
    return f"{query_start_time or ''}|{query_end_time or ''}|{query_window_hours or ''}"


def resolve_analyst_insights_clamp(
    query_start_time: str | None,
    query_end_time: str | None,
    query_window_hours: int | None,
    *,
    baseline_hours: float,
    window_hours: float,
    now: datetime | None = None,
) -> tuple[str | None, str | None, str]:
    anchor = now if now is not None else datetime.now(UTC)
    tb = _time_bounds_from_params(query_start_time, query_end_time, query_window_hours, now=anchor)
    earliest = anchor - timedelta(hours=baseline_hours + window_hours)
    start, end = tb.clamp(earliest, anchor, max_span=MAX_ANALYST_QUERY_SPAN)
    cache_key = analyst_clamp_cache_key(query_start_time, query_end_time, query_window_hours)
    return start.isoformat(), end.isoformat(), cache_key


def clamp_or_400(
    tb: TimeBounds, req_start: str | None, req_end: str | None, *, analyst_session: object | None = None
) -> tuple[str | None, str | None]:
    if analyst_session is None and req_start is None and req_end is None:
        return None, None
    max_span = MAX_ANALYST_QUERY_SPAN if analyst_session is not None else None
    try:
        start, end = tb.clamp(
            parse_iso_utc(req_start) if req_start else None,
            parse_iso_utc(req_end) if req_end else None,
            max_span=max_span,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail={"error": redact_error_text(exc), "time_range_empty": True}
        ) from exc
    return start.isoformat(), end.isoformat()


_self_ru = sys.modules[__name__]
for _mod_name in (
    "backend.utils.pop_utils",
    "backend.utils.countries",
    "backend.utils.remote_access",
    "backend.utils.rate_limit",
    "backend.utils.auth",
):
    sys.modules[_mod_name] = _self_ru
_utils_pkg = sys.modules.get("backend.utils")
if _utils_pkg is not None:
    for _attr in ("pop_utils", "countries", "remote_access", "rate_limit", "auth"):
        setattr(_utils_pkg, _attr, _self_ru)
if "backend" in sys.modules and hasattr(sys.modules["backend"], "_MODULE_ALIASES"):
    for _alias in (
        "backend.utils.pop_utils",
        "backend.utils.countries",
        "backend.utils.remote_access",
        "backend.utils.rate_limit",
        "backend.utils.auth",
    ):
        sys.modules["backend"]._MODULE_ALIASES[_alias] = __name__
