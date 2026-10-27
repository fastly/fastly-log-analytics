"""Analyst session cookie, authorization scope, and PII policy helpers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal
from urllib.parse import urlsplit

from fastapi import HTTPException, Request, Response

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
