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
import re
import secrets
import threading
import uuid
from collections.abc import Callable, Collection, Mapping
from functools import wraps
from logging import Logger
from typing import Any, NoReturn, get_args

from fastapi import HTTPException, Request

from backend.config import path_within_dir, service_id_from_source  # noqa: F401
from backend.models.admin import SyncStartResponse

logger = logging.getLogger(__name__)

# NB: ``app`` is the prod container WORKDIR (backend/Dockerfile -> /app), so the real data root is
# /app/data/services/<sid>/... Only deploy-relevant Unix roots preceded by a non-alphanumeric char are
# stripped, so URL paths like https://example.com/var/foo are left alone.
ABS_PATH_RE = re.compile(r"(?<![A-Za-z0-9])/(?:srv|var|tmp|data|home|opt|usr|mnt|app)\b(?:/[A-Za-z0-9_./-]*)?")

# Fallback for a literal salt in SQL text when the value is not in the known-salt set.
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
