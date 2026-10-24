"""Shared analyst-scope helpers.

Routers that mix admin + analyst access need a fast read of "which service
IDs is the caller scoped to?" so cross-tenant queries can filter accordingly.
Pre-extract, alerts.py and views.py each defined byte-identical copies of
``_analyst_allowed_services``; bootstrap.py and deps.py inlined the same
``set(analyst_session.service_ids or [])`` pattern at 5+ sites. One bug
discovered in one copy meant a security-relevant fix had to be re-applied
across 7 places.

Lives in ``backend/utils/auth.py`` (not ``backend/deps.py``) because it's a
plain-function read of ``request.state.analyst_session`` populated by
``RemoteAccessMiddleware``, not a FastAPI Depends-style dependency.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Literal
from urllib.parse import urlsplit

from fastapi import HTTPException, Request, Response

# The analyst bearer-token cookie names. While TOS is pending the session id
# lives in the PENDING cookie; `/api/share/acknowledge` upgrades it to the full
# cookie (and rotates the id). Both share routers follow this two-cookie protocol.
ANALYST_SESSION_COOKIE = "analyst_session_id"
ANALYST_PENDING_SESSION_COOKIE = "analyst_pending_session_id"


def analyst_cookie_is_secure(request: Request, public_url: str | None) -> bool:
    """Keep analyst bearer cookies secure when TLS terminates at a proxy."""
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
    """Issue the active or TOS-pending analyst session cookie and clear the counterpart."""
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
    """Bound + strip an unauth-supplied email before it reaches the audit log."""
    if not email:
        return "-"
    cleaned = "".join(c for c in email if c.isprintable())
    return cleaned[:254] or "-"


def analyst_allowed_services(request: Request) -> set[str] | None:
    """Return the set of service IDs the caller (analyst session) can see,
    or ``None`` for admin requests (no scope restriction).

    Security: every read / mutation on a multi-service collection must
    filter by this set so an analyst scoped to ``svc-A`` cannot enumerate
    or modify ``svc-B``'s data via the cross-tenant pattern (e.g.
    ``GET /api/alerts/``, ``GET /api/views/{other_id}``).

    Returns ``None`` for admin sessions so caller code can treat ``None``
    as "no filter" with a single ``if allowed is not None`` branch.
    """
    analyst_session = getattr(request.state, "analyst_session", None)
    if analyst_session is None:
        return None  # admin — unrestricted
    return set(analyst_session.service_ids or [])


def require_service_in_scope(request: Request, service_id: str | None) -> set[str] | None:
    """Raise 403 ``service_not_authorized`` when the caller's analyst scope
    excludes ``service_id``; return the allowed-service set (``None`` for
    admins) so callers can reuse it for follow-up record-ownership checks
    without re-reading ``request.state``.

    ``service_id`` is ``str | None`` because some callers pass
    ``source.get("name")``; a ``None`` (unknown service) is never in an
    analyst's allowed set, so it 403s for analysts and is a no-op for admins
    — matching the inlined checks this replaces.

    Centralizes the membership half of the cross-tenant gate that was
    hand-written across views / alerts / bootstrap / services / session
    scoring. Record-existence / ownership checks that compare a stored row's
    ``service_id`` against the same set stay inline at the call site (a
    membership pass doesn't prove the addressed row belongs to the caller).
    """
    allowed = analyst_allowed_services(request)
    if allowed is not None and service_id not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": service_id},
        )
    return allowed


def require_owned_row(
    request: Request,
    service_id: str,
    row_id: str,
    fetch: Callable[[str, str], dict | None],
) -> dict | None:
    """Pre-mutation cross-tenant gate for a row addressed by ``(service_id, row_id)`` (alerts, saved views).

    Admins (``allowed is None``) skip the lookup. For a scoped analyst the addressed ``service_id`` must be
    in scope (403 ``service_not_authorized``), and when ``fetch(row_id, service_id)`` finds the row its own
    ``service_id`` must be in scope too — both checks run BEFORE the caller mutates, so a cross-tenant write
    never lands and the analyst only ever sees the 403. A row that doesn't exist passes (the mutation reports
    its own not-found). Returns the fetched row (``None`` for admins or a missing row).

    ``fetch`` is the repository's ``get_*_by_id(row_id, service_id)``; the caller still owns the
    service_id-required 400 and the admin-only mutation check (audit finding 018).
    """
    allowed = require_service_in_scope(request, service_id)
    if allowed is None:
        return None
    existing = fetch(row_id, service_id)
    if existing and existing.get("service_id") not in allowed:
        raise HTTPException(
            status_code=403,
            detail={"error": "service_not_authorized", "service": existing.get("service_id")},
        )
    return existing


def mask_ips_for(analyst_session: object | None) -> bool:
    """Return ``True`` iff the analyst's invite carries ``mask_ips``.

    ``None`` / missing / non-dict ``pii_policy`` → ``False``. Centralizes the
    ``getattr(session, "pii_policy", None)`` + ``isinstance(policy, dict)``
    guard that was hand-written across insights / query / dashboard /
    bootstrap, so the security-relevant predicate — and its safety against a
    ``pii_policy`` revoked to ``None`` mid-session — lives in one place.
    """
    if analyst_session is None:
        return False
    policy = getattr(analyst_session, "pii_policy", None)
    return bool(policy.get("mask_ips")) if isinstance(policy, dict) else False
