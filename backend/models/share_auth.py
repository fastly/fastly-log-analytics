"""Response models for the share-auth router.

Centralized here so OpenAPI codegen surfaces concrete schemas for every
analyst-facing endpoint — the frontend imports the resulting TS types from
`@/types/api.generated` instead of hand-rolling its own response shapes.
"""

from __future__ import annotations

from pydantic import BaseModel

from backend.models.common import OkResponse


class TosDocument(BaseModel):
    version: str
    text: str


class ShareLoginResponse(OkResponse):
    # session_id is intentionally NOT returned: the session id is the bearer
    # token and lives only in the httponly/secure/samesite cookie. Mirroring
    # it into the JSON body would defeat httponly if any XSS landed. The
    # frontend never reads it — auth rides entirely on the cookie.
    name: str
    email: str
    tos_pending: bool
    tos: TosDocument | None = None
    service_ids: list[str] = []
    redirect: str


class ShareLogoutResponse(OkResponse):
    pass


class ShareAcknowledgeResponse(OkResponse):
    pass


class ShareHeartbeatResponse(OkResponse):
    # session_id omitted for the same reason as ShareLoginResponse — the
    # bearer token stays in the httponly cookie, never the JSON body.
    last_active: str | float


class ShareClaimResponse(OkResponse):
    name: str | None = None
    email: str | None = None
    expires_at: str | None = None
    service_ids: list[str] = []


# ── Request payloads ──────────────────────────────────────────────────────────


class ShareLoginPayload(BaseModel):
    """Body for ``POST /api/share/login``."""

    email: str
    passcode: str


class TosAckPayload(BaseModel):
    """Body for ``POST /api/share/acknowledge`` — the TOS version the analyst
    is acknowledging. Server compares against the latest stored version and
    rejects on mismatch (audit finding 021)."""

    version: str
