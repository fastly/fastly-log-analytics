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
from backend.utils.error_redaction import (  # noqa: F401
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
