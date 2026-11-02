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
import threading
from collections.abc import Callable, Collection, Mapping
from typing import Any, get_args

from fastapi import HTTPException, Request

from backend import config as svcconfig
from backend.config import path_within_dir, service_id_from_source
from backend.models.admin import SyncStartResponse
from backend.services.service_manager import (
    _DIR_STATS_TTL_SEC,
    _dir_stats_cache,
    _dir_stats_lock,
    _dir_stats_refresh_in_flight,
    _get_dir_stats,
    _shared_dir_size_cache,
    _shared_dir_size_lock,
    _shared_dir_stats_cache,
    get_enriched_services,
)
from backend.utils.auth import (
    ANALYST_PENDING_SESSION_COOKIE,
    ANALYST_SESSION_COOKIE,
    analyst_allowed_services,
    analyst_cookie_is_secure,
    mask_ips_for,
    require_owned_row,
    require_service_in_scope,
    safe_audit_email,
    set_analyst_session_cookies,
)
from backend.utils.countries import (
    COUNTRY_MAP,
    format_city_label,
)
from backend.utils.dir_size import (
    _DEFAULT_WALK_DIR_STATS,
    _DIR_SIZE_CACHE,
    _DIR_SIZE_TTL_S,
    _DIR_STATS_CACHE,
    _dir_size_cold_locks,
    _dir_size_cold_locks_meta_lock,
    _dir_size_lock,
    _dir_size_refresh_in_flight,
    _get_dir_size,
    _refresh_dir_size_background,
    _schedule_dir_walk,
    _walk_dir_stats,
    get_dir_stats_swr,
    walk_dir_stats,
)
from backend.utils.error_utils import (
    _handle_query_error,
    bad_request,
    correlation_id,
    current_request_id,
    make_error,
    not_found,
    query_errors,
    raise_internal,
    validation_failed,
)
from backend.utils.pii_masking import (
    _MIN_SALT_LEN,
    _SENSITIVE_HEADERS,
    ABS_PATH_RE,
    CID_SALT_LITERAL_RE,
    REDACTED,
    _known_salts,
    _obfuscate_header,
    _salts_lock,
    format_debug_request,
    redact_error_text,
    register_salt,
    scrub_secrets,
)
from backend.utils.pop_utils import (
    _DEFAULT_POP_COORDS,
    _SAFE_POP_CODE_RE,
    CACHE_FILE,
    _build_pop_case_sql,
    _effective_pop_lat_lon_map,
    _haversine_sql_expr,
    _lat_lon_cache,
    _load_mtime_cached,
    _loc_cache,
    build_pop_haversine_case_sql,
    build_pop_lat_case_sql,
    build_pop_lon_case_sql,
    fetch_pop_locations,
    get_pop_lat_lon_map,
    get_pop_location_map,
    get_pop_locations,
)
from backend.utils.rate_limit import (
    SlidingWindowLimiter,
    SlidingWindowRateLimiter,
    _StaticAssetLimiter,
)
from backend.utils.remote_access import (
    _ANALYST_ALLOWED_WRITE_PREFIXES,
    _ANALYST_BLOCKED_PREFIXES,
    _ANALYST_BLOCKED_RUM_SUFFIXES,
    _ANALYST_BLOCKED_SCORING_SUFFIXES,
    _ANALYST_BLOCKED_SUBPATH_REGEX,
    _ANALYST_BLOCKED_SUBPATHS,
    _ANALYST_SSE_ALLOWLIST,
    _API_DEBUG_KEYS,
    _BODY_INSPECT_ATTR,
    _BODY_INSPECT_UNSET,
    _LOCAL_ADMIN_CIDRS_ENV,
    _LOCAL_HOST_ALLOWLIST,
    _PATH_SERVICE_PATTERNS,
    _PII_FORBIDDEN_FILTER_COLS,
    _QUERY_DEBUG_KEYS,
    _TELEMETRY_IDLE_EXEMPT_PREFIXES,
    _UNAUTH_ANALYST_PATHS,
    ADMIN_GATEWAY_HEADER,
    BODY_INSPECT_MAX_BYTES,
    MAX_ANALYST_QUERY_SPAN,
    RemoteAccessMiddleware,
    TimeBounds,
    _allowed_remote_hosts,
    _analyst_api_limiter,
    _body_field_param,
    _body_filter_keys,
    _body_service_ids,
    _check_rate_limit,
    _drain_request_body,
    _enforce_service_and_pii_scope,
    _error_json,
    _host_base,
    _in_local_allowlist,
    _inspect_request_body,
    _is_blocked_path,
    _is_private_or_loopback,
    _is_sse_route,
    _local_admin_cidrs_cache,
    _local_admin_networks,
    _local_host_allowed,
    _log_access,
    _new_request_id,
    _origin_allowed,
    _path_service_ids,
    _rebuild_response,
    _remote_host_allowed,
    _static_limiter,
    _strip_analyst_envelope,
    _time_bounds_from_params,
    admin_gateway_required,
    analyst_clamp_cache_key,
    analyst_debug_visibility,
    apply_response_hardening,
    authenticated_admin_gateway,
    clamp_or_400,
    client_ip,
    get_analyst_time_bounds,
    is_request_remote,
    resolve_analyst_insights_clamp,
    validate_admin_gateway_config,
)
from backend.utils.sse import (
    SSE_PASSTHROUGH_HEADERS,
    ClientDisconnected,
    _AbortableQueue,
    _stream_from_worker,
    iter_with_disconnect_ping,
    sse_response,
    stream_iac_reconcile,
    stream_sse_from_worker,
)

logger = logging.getLogger(__name__)


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
    ``backend.routers.admin.ingest`` (sync) and
    ``backend.routers.admin.compaction`` (commit). Each used to duplicate the
    same ``try: start_cron_run -> spawn thread -> return {ok}; except
    RuntimeError: scan list_active_runs -> return {ok, in_progress_msg};
    else: raise HTTPException(503)`` shape with only the task name +
    target kwargs varying.
    """
    from backend.core.duckdb import start_cron_run
    from backend.cron.scheduler import list_active_runs, start_progress

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
    invalidate_cache: bool = True,
    require_high_scale: bool = False,
    high_scale_detail: Any = None,
    precheck: Callable[[], None] | None = None,
) -> SyncStartResponse:
    """Shared execution wrapper for manual admin cron trigger routes."""
    if require_high_scale and not svcconfig.is_high_scale_mode(source):
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
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        # Keep the exact ``detail={"error": "Service not found"}`` shape
        # the migrated callers used — frontend code keys on this exact
        # message via ``error.detail.error === "Service not found"``.
        raise HTTPException(status_code=404, detail={"error": "Service not found"})
    return cfg


def load_service_config_or_503(service_id: str) -> dict:
    """Load a service's config, raising 503 on read error or 404 if missing."""
    try:
        cfg = svcconfig.load_config(service_id)
    except Exception:
        raise HTTPException(
            503, detail=make_error("service_unavailable", "Service configuration unavailable")
        ) from None
    if not cfg:
        raise HTTPException(404, detail=make_error("service_not_found", "Service not found"))
    return cfg


def mutate_service_config(service_id: str, mutate: Callable[[dict], None]) -> dict:
    """Locked read-modify-write of a service config, or :class:`HTTPException` 404.

    The write counterpart of :func:`load_service_config`: applies ``mutate`` to a fresh copy
    under :func:`backend.config.mutate_config` (so a concurrent writer's keys are not
    clobbered) and returns the saved config.
    """
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
    return stored_fastly_token(svcconfig.load_config(service_id))


def require_fastly_token(service_id: str, override: str | None = "") -> str:
    """Strict token resolution: ``override`` if given, else the stored key — 404 when the service has no
    config, 400 when it has no key. Error bodies match the RUM endpoints' historical ones."""
    if override:
        return override
    cfg = svcconfig.load_config(service_id)
    if not cfg:
        raise HTTPException(status_code=404, detail={"error": f"No config for service {service_id}"})
    token = stored_fastly_token(cfg)
    if not token:
        raise HTTPException(status_code=400, detail={"error": f"No Fastly API key for service {service_id}"})
    return token


def require_service_id(service_id: str | None) -> str:
    """Return ``service_id``, or raise the canonical 400 ``service_id_required``
    envelope when it is missing or empty. Every route that needs a service and
    did not get one answers with this, never FastAPI's 422 validation body."""
    if not service_id:
        raise HTTPException(status_code=400, detail=make_error("service_id_required"))
    return service_id


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
        from backend.core.metadata.base import export_admin_state

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


__all__ = [
    "ABS_PATH_RE",
    "ADMIN_GATEWAY_HEADER",
    "ANALYST_PENDING_SESSION_COOKIE",
    "ANALYST_SESSION_COOKIE",
    "BODY_INSPECT_MAX_BYTES",
    "CACHE_FILE",
    "CID_SALT_LITERAL_RE",
    "COUNTRY_MAP",
    "ClientDisconnected",
    "MAX_ANALYST_QUERY_SPAN",
    "REDACTED",
    "RemoteAccessMiddleware",
    "SSE_PASSTHROUGH_HEADERS",
    "SlidingWindowLimiter",
    "SlidingWindowRateLimiter",
    "SyncStartResponse",
    "TimeBounds",
    "_ANALYST_ALLOWED_WRITE_PREFIXES",
    "_ANALYST_BLOCKED_PREFIXES",
    "_ANALYST_BLOCKED_RUM_SUFFIXES",
    "_ANALYST_BLOCKED_SCORING_SUFFIXES",
    "_ANALYST_BLOCKED_SUBPATHS",
    "_ANALYST_BLOCKED_SUBPATH_REGEX",
    "_ANALYST_SSE_ALLOWLIST",
    "_API_DEBUG_KEYS",
    "_AbortableQueue",
    "_BODY_INSPECT_ATTR",
    "_BODY_INSPECT_UNSET",
    "_DEFAULT_POP_COORDS",
    "_DEFAULT_WALK_DIR_STATS",
    "_DIR_SIZE_CACHE",
    "_DIR_SIZE_TTL_S",
    "_DIR_STATS_CACHE",
    "_DIR_STATS_TTL_SEC",
    "_LOCAL_ADMIN_CIDRS_ENV",
    "_LOCAL_HOST_ALLOWLIST",
    "_MIN_SALT_LEN",
    "_PATH_SERVICE_PATTERNS",
    "_PII_FORBIDDEN_FILTER_COLS",
    "_QUERY_DEBUG_KEYS",
    "_SAFE_POP_CODE_RE",
    "_SENSITIVE_HEADERS",
    "_StaticAssetLimiter",
    "_TELEMETRY_IDLE_EXEMPT_PREFIXES",
    "_UNAUTH_ANALYST_PATHS",
    "_allowed_remote_hosts",
    "_analyst_api_limiter",
    "_body_field_param",
    "_body_filter_keys",
    "_body_service_ids",
    "_build_pop_case_sql",
    "_check_rate_limit",
    "_dir_size_cold_locks",
    "_dir_size_cold_locks_meta_lock",
    "_dir_size_lock",
    "_dir_size_refresh_in_flight",
    "_dir_stats_cache",
    "_dir_stats_lock",
    "_dir_stats_refresh_in_flight",
    "_drain_request_body",
    "_effective_pop_lat_lon_map",
    "_enforce_service_and_pii_scope",
    "_error_json",
    "_get_dir_size",
    "_get_dir_stats",
    "_handle_query_error",
    "_haversine_sql_expr",
    "_host_base",
    "_in_local_allowlist",
    "_inspect_request_body",
    "_is_blocked_path",
    "_is_private_or_loopback",
    "_is_sse_route",
    "_known_salts",
    "_lat_lon_cache",
    "_load_mtime_cached",
    "_loc_cache",
    "_local_admin_cidrs_cache",
    "_local_admin_networks",
    "_local_host_allowed",
    "_log_access",
    "_new_request_id",
    "_obfuscate_header",
    "_origin_allowed",
    "_path_service_ids",
    "_rebuild_response",
    "_refresh_dir_size_background",
    "_remote_host_allowed",
    "_salts_lock",
    "_schedule_dir_walk",
    "_shared_dir_size_cache",
    "_shared_dir_size_lock",
    "_shared_dir_stats_cache",
    "_static_limiter",
    "_stream_from_worker",
    "_strip_analyst_envelope",
    "_time_bounds_from_params",
    "_walk_dir_stats",
    "admin_gateway_required",
    "analyst_allowed_services",
    "analyst_clamp_cache_key",
    "analyst_cookie_is_secure",
    "analyst_debug_visibility",
    "apply_response_hardening",
    "authenticated_admin_gateway",
    "bad_request",
    "build_pop_haversine_case_sql",
    "build_pop_lat_case_sql",
    "build_pop_lon_case_sql",
    "clamp_or_400",
    "client_ip",
    "correlation_id",
    "current_request_id",
    "expand_sections",
    "fetch_pop_locations",
    "format_city_label",
    "format_debug_request",
    "get_analyst_time_bounds",
    "get_dir_stats_swr",
    "get_enriched_services",
    "get_pop_lat_lon_map",
    "get_pop_location_map",
    "get_pop_locations",
    "is_request_remote",
    "iter_with_disconnect_ping",
    "load_service_config",
    "load_service_config_or_503",
    "make_error",
    "make_section_expander",
    "mask_ips_for",
    "mutate_service_config",
    "not_found",
    "path_within_dir",
    "query_errors",
    "raise_internal",
    "redact_error_text",
    "register_salt",
    "require_fastly_token",
    "require_json_content_type",
    "require_owned_row",
    "require_service_id",
    "require_service_in_scope",
    "resolve_analyst_insights_clamp",
    "resolve_fastly_token",
    "run_admin_cron_trigger",
    "run_cached_repo_endpoint",
    "safe_audit_email",
    "scrub_secrets",
    "service_id_from_source",
    "set_analyst_session_cookies",
    "sse_response",
    "start_or_resume_cron",
    "stored_fastly_token",
    "stream_iac_reconcile",
    "stream_sse_from_worker",
    "sync_admin_state",
    "validate_admin_gateway_config",
    "validation_failed",
    "walk_dir_stats",
]
