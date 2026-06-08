"""Caller-supplied Fastly token validation for destructive operations.

Security finding: destructive endpoints (teardown, NGWAF workspace
modification) must NEVER fall back to server-stored credentials, and the
caller-supplied token must be validated as having the ``global`` scope (the
only Fastly scope that grants destructive service operations). If the token
binds to a specific service list, the target ``service_id`` must appear in it.

The validation goes through Fastly's authoritative ``GET /tokens/self``
endpoint — we don't try to introspect the token locally.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import HTTPException

from backend.core.fastly.client import fastly

logger = logging.getLogger(__name__)

# Fastly's token-scope vocabulary, per
# https://www.fastly.com/documentation/reference/api/auth-tokens/user/
# ``global`` is the ONLY scope that grants destructive service operations.
# ``global:read``, ``purge_select``, ``purge_all`` must all be rejected for
# destructive use.
_DESTRUCTIVE_SCOPE = "global"


def _parse_scopes(raw: Any) -> list[str]:
    """Normalize the ``scope`` field from /tokens/self into a list of scopes.

    Fastly returns scope as a space-separated string for some token shapes
    and as a list of strings for others. Both forms get normalized to a list
    so the membership check below is unambiguous.
    """
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(s).strip() for s in raw if s]
    if isinstance(raw, str):
        return [s for s in raw.split() if s]
    return []


def validate_destructive_token(token: str, *, service_id: str) -> dict[str, Any]:
    """Validate that ``token`` is allowed to perform destructive ops on ``service_id``.

    Raises ``HTTPException(401)`` on any failure:
      * empty/missing token
      * non-200 response from /tokens/self (invalid token, network error, etc.)
      * ``scope`` missing, not parseable, or doesn't include ``global``
      * ``services`` is a non-empty list and ``service_id`` is not a member

    Returns the validated token response dict on success so the caller can log
    ``token_data["id"]`` / ``user_id`` for audit.
    """
    token = (token or "").strip()
    if not token:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "token_required",
                "message": (
                    "A Fastly API token with the 'global' scope is required "
                    "for destructive operations. Server-stored credentials are "
                    "not accepted here."
                ),
            },
        )

    try:
        token_data = fastly("GET", "/tokens/self", token=token)
    except Exception as e:
        # Don't leak the raw error to the caller — could include the token
        # value or a useful error message for the attacker.
        logger.warning("[fastly-auth] /tokens/self call failed: %s", e)
        raise HTTPException(
            status_code=401,
            detail={"error": "token_validation_failed", "message": "Could not validate token with Fastly."},
        )

    if not isinstance(token_data, dict):
        logger.warning("[fastly-auth] /tokens/self returned non-dict: %r", type(token_data))
        raise HTTPException(
            status_code=401,
            detail={"error": "token_validation_failed", "message": "Unexpected token response shape."},
        )

    scopes = _parse_scopes(token_data.get("scope"))
    if _DESTRUCTIVE_SCOPE not in scopes:
        logger.warning(
            "[fastly-auth] token (id=%s, user=%s) missing 'global' scope; got=%r",
            token_data.get("id"),
            token_data.get("user_id") or "(automation)",
            scopes,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "insufficient_scope",
                "message": (
                    "Token does not have the 'global' scope required for destructive operations. "
                    "Use a Fastly token with full 'global' permissions, not 'global:read', "
                    "'purge_select', or 'purge_all'."
                ),
            },
        )

    # Service binding check: empty/missing services list means "unrestricted",
    # which is acceptable. Non-empty list must include the target service.
    bound_services = token_data.get("services")
    if isinstance(bound_services, list) and bound_services:
        if service_id not in bound_services:
            logger.warning(
                "[fastly-auth] token (id=%s) bound to %d services but not target service_id=%s",
                token_data.get("id"),
                len(bound_services),
                service_id,
            )
            raise HTTPException(
                status_code=401,
                detail={
                    "error": "service_not_authorized",
                    "message": (
                        "Token is bound to a service list that does not include the target service. "
                        "Use a token with global access or with this service in its allow-list."
                    ),
                },
            )

    # Tenant binding check: the scope+services checks above don't prevent
    # "use a global token from MY own Fastly account against someone
    # else's service ID". Cross-reference the service's owning
    # ``customer_id`` (fetched with the same token, so any access denial
    # there fails closed too) against the token holder's
    # ``customer_id``. Mismatch → reject.
    token_customer = (token_data.get("customer_id") or "").strip()
    try:
        service_data = fastly("GET", f"/service/{service_id}", token=token)
    except Exception as e:
        logger.warning(
            "[fastly-auth] /service/%s call failed during tenant verification: %s",
            service_id,
            e,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "tenant_verification_failed",
                "message": "Could not verify token tenant against target service.",
            },
        )

    service_customer = ""
    if isinstance(service_data, dict):
        service_customer = (service_data.get("customer_id") or "").strip()

    if not token_customer or not service_customer or token_customer != service_customer:
        logger.warning(
            "[fastly-auth] tenant mismatch: token customer=%r vs service customer=%r (token id=%s, service=%s)",
            token_customer or "(missing)",
            service_customer or "(missing)",
            token_data.get("id"),
            service_id,
        )
        raise HTTPException(
            status_code=401,
            detail={
                "error": "tenant_mismatch",
                "message": (
                    "The supplied token is not authorized for the target service's tenant. "
                    "Use a token issued under the same Fastly account that owns this service."
                ),
            },
        )

    logger.info(
        "[fastly-auth] destructive op authorized: token id=%s user=%s service=%s",
        token_data.get("id"),
        token_data.get("user_id") or "(automation)",
        service_id,
    )
    return token_data
