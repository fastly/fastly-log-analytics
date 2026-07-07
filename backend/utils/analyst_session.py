"""Shared analyst-session primitives used by more than one share router.

The passcode router (`share_auth`) and the OAuth router (`share_oauth`) both set
the analyst session cookie and sanitize attacker-supplied emails before audit.
Those shared bits live here — in the utils layer — so the routers stay
independent of each other (the import-linter "Routers are independent" contract).
"""

from __future__ import annotations

# The analyst bearer-token cookie names. While TOS is pending the session id
# lives in the PENDING cookie; `/api/share/acknowledge` upgrades it to the full
# cookie (and rotates the id). Both share routers follow this two-cookie protocol.
ANALYST_SESSION_COOKIE = "analyst_session_id"
ANALYST_PENDING_SESSION_COOKIE = "analyst_pending_session_id"


def safe_audit_email(email: str | None) -> str:
    """Bound + strip an unauth-supplied email before it reaches the audit log.

    The login-failure / OAuth-callback paths are reachable pre-auth with a fully
    attacker-controlled email. Logging it raw would let an attacker inject
    newlines/control chars into the audit stream (log forging) or bloat it. Strip
    non-printable characters and cap the length; the value is for operator
    forensics only, never re-parsed.
    """
    if not email:
        return "-"
    cleaned = "".join(c for c in email if c.isprintable())
    return cleaned[:254] or "-"
