"""Pydantic request models for the share-admin router.

Carved out of ``backend.routers.share_admin`` so the request schemas live
alongside the rest of ``backend.models.*`` and OpenAPI codegen has a
single canonical home for them.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class ShareStartPayload(BaseModel):
    """Body for ``POST /api/share-admin/start`` — start the analyst-facing
    tunnel on an optional public endpoint, forwarding to ``forward_port``."""

    public_endpoint: str | None = None
    forward_port: int = 3000


class InvitePayload(BaseModel):
    """Body for ``POST /api/share-admin/invites`` — create a new
    analyst-facing share invite."""

    name: str
    email: str
    passcode: str
    duration_hours: int | None = Field(default=24)
    ip_whitelist: str | None = None
    service_ids: list[str] = Field(default_factory=list)
    pii_policy: dict | None = None
    query_window_hours: int | None = None
    query_start_time: str | None = None
    query_end_time: str | None = None


class ServiceScopePayload(BaseModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/services`` — replace
    the invite's service-id allowlist."""

    service_ids: list[str]


class PasscodePayload(BaseModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/passcode`` — rotate
    the invite's passcode in-place (keeps audit history + TOS acceptance)."""

    passcode: str


class PiiPolicyPayload(BaseModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/pii`` — toggle the
    invite's PII policy (currently just ``mask_ips``) after creation."""

    mask_ips: bool


class BackupExportPayload(BaseModel):
    """Body for ``POST /api/share-admin/backup/export`` — passphrase used
    to encrypt the share-db backup blob (minimum 12 characters)."""

    passphrase: str


class GdprErasePayload(BaseModel):
    """Body for ``POST /api/share-admin/gdpr/erase`` — GDPR right-to-be-
    forgotten request for an analyst email."""

    email: str
    reason: str


class SettingsPayload(BaseModel):
    """Body for ``PATCH /api/share-admin/settings`` — partial-update knobs
    for the share-admin global settings (currently just the session cap)."""

    max_concurrent_analyst_sessions: int | None = None
