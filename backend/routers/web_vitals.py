"""Web Vitals and UX event telemetry endpoints."""

from __future__ import annotations

import logging
from typing import Literal
from urllib.parse import urlsplit

from fastapi import APIRouter, HTTPException, Request

from backend.core import request_telemetry as web_vitals_store
from backend.models.common import DEFAULT_ERROR_RESPONSES, OkResponse, UxEventPayload, WebVitalsPayload
from backend.utils.date_utils import iso_z_now
from backend.utils.rate_limit import SlidingWindowRateLimiter, normalize_rate_limit_ip
from backend.utils.remote_access import (
    ANALYST_SESSION_COOKIE,
    _origin_allowed,
    client_ip,
    get_tunnel_manager,
    is_request_remote,
    validate_analyst_request_binding,
)
from backend.utils.router_utils import make_error

web_vitals_logger = logging.getLogger("backend.web_vitals")
ux_logger = logging.getLogger("backend.ux_events")

WEB_VITALS_RATE_LIMIT = 120
WEB_VITALS_RATE_WINDOW_S = 60.0
_WEB_VITALS_LIMITER = SlidingWindowRateLimiter(window_s=WEB_VITALS_RATE_WINDOW_S, max_keys=4096)


def _reset_web_vitals_limiter_for_tests() -> None:
    _WEB_VITALS_LIMITER._reset_for_tests()


def _strict_origin_parts(origin: str) -> tuple[str, str, str] | None:
    """Return ``(scheme, hostname, netloc)`` for a clean ``scheme://host[:port]`` Origin, else None."""
    try:
        parts = urlsplit(origin.strip())
        _ = parts.port
    except ValueError:
        return None
    scheme = parts.scheme.lower()
    hostname = (parts.hostname or "").lower()
    if scheme not in {"http", "https"} or not hostname:
        return None
    if parts.username is not None or parts.password is not None:
        return None
    if parts.path not in ("", "/") or parts.query or parts.fragment:
        return None
    return scheme, hostname, parts.netloc.lower()


def _is_loopback_origin(origin: str) -> bool:
    parsed = _strict_origin_parts(origin)
    return parsed is not None and parsed[1] in {"localhost", "127.0.0.1", "::1"}


def _origin_allowed_strict(origin: str) -> bool:
    return _strict_origin_parts(origin) is not None and _origin_allowed(origin)


def _origin_from_referer(referer: str) -> str:
    try:
        parts = urlsplit(referer.strip())
        _ = parts.port
    except ValueError:
        return ""
    if (
        parts.scheme.lower() not in {"http", "https"}
        or not parts.hostname
        or parts.username is not None
        or parts.password is not None
    ):
        return ""
    return f"{parts.scheme}://{parts.netloc}"


def _resolve_caller_identity(request: Request) -> tuple[object | None, bool, Literal["admin", "analyst"]]:
    """Return ``(analyst_session, is_remote, cohort)`` without bumping session idle timers."""
    is_remote = bool(getattr(request.state, "is_remote", False) or is_request_remote(request))
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        sid = request.cookies.get(ANALYST_SESSION_COOKIE)
        if sid:
            mgr = get_tunnel_manager()
            if mgr.is_sharing_active():
                cand = mgr.validate_session(sid)
                if cand is not None and validate_analyst_request_binding(request, cand, mgr=mgr) is None:
                    analyst_session = cand
                    request.state.analyst_session = cand
    cohort: Literal["admin", "analyst"] = "analyst" if (analyst_session is not None or is_remote) else "admin"
    return analyst_session, is_remote, cohort


def _verify_web_vitals_caller(request: Request) -> Literal["admin", "analyst"]:
    sfs = (request.headers.get("sec-fetch-site") or "").strip().lower()
    if sfs == "cross-site":
        raise HTTPException(
            status_code=403,
            detail=make_error("origin_not_allowed", "Cross-site telemetry submissions are not allowed"),
        )
    origin = (request.headers.get("origin") or "").strip()
    referer = (request.headers.get("referer") or "").strip()
    ref_origin = _origin_from_referer(referer) if referer else ""
    analyst_session, is_remote, cohort = _resolve_caller_identity(request)

    if is_remote or analyst_session is not None:
        if analyst_session is None:
            raise HTTPException(
                status_code=401,
                detail=make_error("unauthenticated", "Authentication required for remote telemetry submissions"),
            )
        if origin and not _origin_allowed_strict(origin):
            raise HTTPException(
                status_code=403,
                detail=make_error("origin_not_allowed", "Telemetry origin is not allowed"),
            )
        if not origin and ref_origin and not _origin_allowed_strict(ref_origin):
            raise HTTPException(
                status_code=403,
                detail=make_error("origin_not_allowed", "Telemetry referer is not allowed"),
            )
    elif origin:
        host = (request.headers.get("host") or "").strip().lower()
        parsed_origin = _strict_origin_parts(origin)
        same_host = parsed_origin is not None and bool(host) and parsed_origin[2] == host
        if not (same_host or _is_loopback_origin(origin) or _origin_allowed_strict(origin)):
            raise HTTPException(
                status_code=403,
                detail=make_error("origin_not_allowed", "Telemetry origin is not allowed"),
            )

    ip_key = f"wv:{normalize_rate_limit_ip(client_ip(request, default='unknown'))}"
    allowed, retry_after = _WEB_VITALS_LIMITER.try_acquire_with_retry(ip_key, max_events=WEB_VITALS_RATE_LIMIT)
    if not allowed:
        raise HTTPException(
            status_code=429,
            detail=make_error("rate_limited", "Too many telemetry submissions"),
            headers={"Retry-After": str(retry_after)},
        )
    return cohort


router = APIRouter(prefix="/api", responses=DEFAULT_ERROR_RESPONSES)


@router.post("/web-vitals", tags=["web-vitals"], response_model=OkResponse, response_model_exclude_unset=True)
def report_web_vitals(payload: WebVitalsPayload, request: Request) -> dict:
    """Record one Web Vitals metric event. Returns ``{"ok": true}``.

    When collection is disabled the sample is dropped quietly (still a
    200 so the fire-and-forget client never sees an error). When enabled
    it's both logged (structlog key/value emission for log-analysis) and
    appended to the JSONL sink for ``scripts/analyze_web_vitals.py``.
    """
    cohort = _verify_web_vitals_caller(request)
    if not web_vitals_store.collection_enabled():
        return {"ok": True}

    web_vitals_logger.info(
        "web_vitals",
        extra={
            "web_vitals_name": payload.name,
            "web_vitals_value": payload.value,
            "web_vitals_rating": payload.rating,
            "web_vitals_id": payload.id,
            "web_vitals_pathname": payload.pathname,
            "web_vitals_navigation_type": payload.navigation_type,
            "web_vitals_delta": payload.delta,
            "web_vitals_cohort": cohort,
        },
    )

    web_vitals_store.append_sample(
        {
            "ts": iso_z_now(),
            "name": payload.name,
            "value": payload.value,
            "rating": payload.rating,
            "id": payload.id,
            "pathname": payload.pathname,
            "navigation_type": payload.navigation_type,
            "delta": payload.delta,
            "cohort": cohort,
        }
    )
    return {"ok": True}


@router.post("/ux-events", tags=["ux-events"], response_model=OkResponse, response_model_exclude_unset=True)
def report_ux_event(payload: UxEventPayload, request: Request) -> dict:
    """Log one UX event. Returns ``{"ok": true}``.

    Same structlog-extra shape as web_vitals so log slicing patterns
    transfer (filter by cohort / pathname / event)."""
    cohort = _verify_web_vitals_caller(request)

    ux_logger.info(
        "ux_event",
        extra={
            "ux_event": payload.event,
            "ux_pathname": payload.pathname,
            "ux_component_id": payload.component_id,
            "ux_details": payload.details,
            "ux_cohort": cohort,
        },
    )
    return {"ok": True}
