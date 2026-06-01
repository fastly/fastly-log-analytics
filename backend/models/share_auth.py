"""Response models for the share-auth router.

Centralized here so OpenAPI codegen surfaces concrete schemas for every
analyst-facing endpoint — the frontend imports the resulting TS types from
`@/types/api.generated` instead of hand-rolling its own response shapes.
"""

from __future__ import annotations

from pydantic import BaseModel


class TosDocument(BaseModel):
    version: str
    text: str


class ShareLoginResponse(BaseModel):
    ok: bool = True
    session_id: str
    name: str
    email: str
    tos_pending: bool
    tos: TosDocument | None = None
    service_ids: list[str] = []
    redirect: str


class ShareLogoutResponse(BaseModel):
    ok: bool = True


class ShareAcknowledgeResponse(BaseModel):
    ok: bool = True


class ShareHeartbeatResponse(BaseModel):
    ok: bool = True
    session_id: str
    last_active: str | float


class ShareClaimResponse(BaseModel):
    ok: bool = True
    name: str | None = None
    email: str | None = None
    expires_at: str | None = None
    service_ids: list[str] = []
