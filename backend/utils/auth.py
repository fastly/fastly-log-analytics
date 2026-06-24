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

from fastapi import HTTPException, Request


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
