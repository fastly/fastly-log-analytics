"""Pydantic request models for the share-admin router.

Carved out of ``backend.routers.share_admin`` so the request schemas live
alongside the rest of ``backend.models.*`` and OpenAPI codegen has a
single canonical home for them.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class ShareStartPayload(BaseModel):
    """Body for ``POST /api/share-admin/start`` — start the analyst-facing
    tunnel on an optional public endpoint, forwarding to ``forward_port``."""

    public_endpoint: str | None = None
    forward_port: int = 3000


class InvitePayload(BaseModel):
    """Body for ``POST /api/share-admin/invites`` — create a new
    analyst-facing share invite.

    ``auth_method`` selects how the analyst redeems the invite:

    * ``'passcode'`` (default) — a ``passcode`` is required and hashed.
    * ``'oauth'`` — ``oauth_provider`` (a configured registry key) is required;
      no ``passcode`` is sent (the backend synthesizes an unguessable argon2id
      placeholder for the NOT NULL column). Passcode flow is unchanged.
    """

    name: str
    email: str
    passcode: str | None = None
    duration_hours: int | None = Field(default=24)
    ip_whitelist: str | None = None
    service_ids: list[str] = Field(default_factory=list)
    pii_policy: dict | None = None
    query_window_hours: int | None = None
    query_start_time: str | None = None
    query_end_time: str | None = None
    allow_concurrent_sessions: bool = False
    auth_method: Literal["passcode", "oauth"] = "passcode"
    oauth_provider: str | None = None


class OAuthProviderInfo(BaseModel):
    """One configured OIDC provider as seen by the admin invite form."""

    id: str
    display_name: str
    enabled: bool


class OAuthProvidersResponse(BaseModel):
    """Body for ``GET /api/admin/share/oauth-providers`` — the configured
    providers an admin can attach to an OAuth invite (may include disabled)."""

    providers: list[OAuthProviderInfo] = Field(default_factory=list)


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


class SharingPolicyPayload(BaseModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/sharing`` — toggle
    whether the invite allows shared (concurrent) analyst logins."""

    allow_concurrent_sessions: bool


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


# ── Wire-safe response models for the share-admin reads/mutations ───────────
#
# Same contract as backend/models/session_scoring.py: ``extra="allow"`` so
# undeclared/future producer keys pass through verbatim, all-Optional fields
# so validation can never 500 the admin surface, and
# ``response_model_exclude_unset=True`` at the decorator for branch-dependent
# key sets. Field lists derive from the producers (TunnelManager, share_db).


class _ShareAdminRead(BaseModel):
    """Base for share-admin responses — passes undeclared keys through."""

    model_config = ConfigDict(extra="allow")


class ShareBannerResponse(_ShareAdminRead):
    sharing_active: bool | None = None
    public_url: str | None = None


class InviteRecord(_ShareAdminRead):
    """One remote-invite row (``SELECT *`` + derived fields). The stored
    passcode hash rides through ``extra`` untouched — intentionally NOT
    declared here so the OpenAPI surface doesn't advertise it; the schema
    itself may grow columns via share_db migrations, which ``extra`` also
    absorbs. ``last_login_at`` is attached by ``build_share_status`` only."""

    id: str | None = None
    name: str | None = None
    email: str | None = None
    expires_at: str | None = None
    ip_whitelist: str | None = None
    pii_policy: dict[str, Any] | None = None
    query_window_hours: int | None = None
    query_start_time: str | None = None
    query_end_time: str | None = None
    created_at: str | None = None
    revoked: int | None = None
    tos_accepted_at: str | None = None
    tos_version: str | None = None
    allow_concurrent_sessions: bool | None = None
    auth_method: str | None = None
    oauth_provider: str | None = None
    oauth_subject: str | None = None
    last_login_at: str | None = None
    service_ids: list[str] | None = None


class ShareServiceEntry(_ShareAdminRead):
    service_id: str | None = None
    name: str | None = None
    remote_frontend_deployed: bool | None = None
    sharing_domain: str | None = None
    remote_service_id: str | None = None


class ShareStatusResponse(_ShareAdminRead):
    """``build_share_status()``. The high-churn nested collections
    (sessions / audit_logs / rate_limits / telemetry) stay free dicts:
    their shapes are owned by TunnelManager internals and consumed
    generically."""

    sharing_active: bool | None = None
    public_endpoint: str | None = None
    public_url: str | None = None
    forward_port: int | None = None
    started_at: str | None = None
    max_concurrent_sessions: int | None = None
    active_session_count: int | None = None
    services: list[ShareServiceEntry] | None = None
    invites: list[InviteRecord] | None = None
    sessions: list[dict] | None = None
    audit_logs: list[dict] | None = None
    rate_limits: dict | None = None
    telemetry: dict | None = None


class ShareLiveResponse(_ShareAdminRead):
    """``build_share_live_payload()`` — the lean 10-s poll subset."""

    sharing_active: bool | None = None
    public_url: str | None = None
    active_session_count: int | None = None
    rate_limits: dict | None = None
    telemetry: dict | None = None


class ShareAuditLogsResponse(_ShareAdminRead):
    audit_logs: list[dict] | None = None


class ShareStartResponse(_ShareAdminRead):
    public_url: str | None = None


class SharePanicResponse(_ShareAdminRead):
    sessions_booted: int | None = None


class InviteMutationAck(_ShareAdminRead):
    """``{ok: true}`` acks that also report booted session count."""

    ok: bool | None = None
    booted_sessions: int | None = None


class ClaimTokenResponse(_ShareAdminRead):
    token: str | None = None


class BackupImportResponse(_ShareAdminRead):
    inserted: int | None = None
    skipped: int | None = None
    merged: int | None = None


class GdprEraseResponse(_ShareAdminRead):
    deleted_invites: int | None = None
    redacted_log_rows: int | None = None
    retained_recent_rows: int | None = None


class ShareSettingsResponse(_ShareAdminRead):
    max_concurrent_analyst_sessions: int | None = None


class WordphraseResponse(_ShareAdminRead):
    passcode: str | None = None
