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
    session_id: str
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
    session_id: str
    last_active: str | float


class ShareClaimResponse(OkResponse):
    name: str | None = None
    email: str | None = None
    expires_at: str | None = None
    service_ids: list[str] = []
