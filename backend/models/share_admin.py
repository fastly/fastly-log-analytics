"""Pydantic request models for the share-admin router.

Carved out of ``backend.routers.share_admin`` so the request schemas live
alongside the rest of ``backend.models.*`` and OpenAPI codegen has a
single canonical home for them.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

from backend.models._base import PassthroughModel, WireSafeModel


class PiiPolicyPayload(WireSafeModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/pii`` — toggle the
    invite's PII policy (currently just ``mask_ips``) after creation."""

    mask_ips: bool = False


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
    pii_policy: PiiPolicyPayload | None = None
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


class ShareBannerResponse(WireSafeModel):
    sharing_active: bool | None = None
    public_url: str | None = None


class InviteRecord(PassthroughModel):
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
    pii_policy: PiiPolicyPayload | None = None
    query_window_hours: int | None = None
    query_start_time: str | None = None
    query_end_time: str | None = None
    created_at: str | None = None
    # Wire value is the SQLite INTEGER flag, 0 (active) or 1 (revoked), never a JSON
    # boolean; the literal keeps the bytes unchanged while staying truthiness-compatible.
    revoked: Literal[0, 1] | None = None
    tos_accepted_at: str | None = None
    tos_version: str | None = None
    allow_concurrent_sessions: bool | None = None
    auth_method: Literal["passcode", "oauth"] | None = None
    oauth_provider: str | None = None
    oauth_subject: str | None = None
    last_login_at: str | None = None
    service_ids: list[str] | None = None


class ShareServiceEntry(PassthroughModel):
    service_id: str | None = None
    name: str | None = None
    remote_frontend_deployed: bool | None = None
    sharing_domain: str | None = None
    remote_service_id: str | None = None


class ShareSessionRecord(PassthroughModel):
    """One live analyst session: ``AnalystSession.to_dict()`` (a dataclass
    ``asdict``, so every key is always present)."""

    session_id: str
    invite_id: str
    name: str
    email: str
    ip_address: str
    user_agent: str
    fingerprint_signature: str
    pii_policy: PiiPolicyPayload
    query_window_hours: int | None
    query_start_time: str | None
    query_end_time: str | None
    login_time: str
    last_active_time: str
    last_activity: str | None
    tos_pending: bool
    service_ids: list[str]
    auth_method: Literal["passcode", "oauth"]
    oauth_provider: str | None


class ShareAuditLogRow(PassthroughModel):
    """One ``remote_share_audit_logs`` row (``SELECT *``)."""

    id: int
    timestamp: str
    event_type: str
    email: str | None
    ip_address: str
    details: str


class ShareRateLimitFailure(PassthroughModel):
    ip: str
    count: int
    window_s: int


class ShareRateLimitLockout(PassthroughModel):
    ip: str
    remaining_s: int


class ShareRateLimits(PassthroughModel):
    """``LoginRateLimiter.snapshot()``."""

    failures: list[ShareRateLimitFailure]
    lockouts: list[ShareRateLimitLockout]


class ShareTunnelUptimeEntry(PassthroughModel):
    started_at: str
    ended_at: str
    duration_s: int
    reason: str


class ShareTelemetry(PassthroughModel):
    """``TunnelManager.get_telemetry()``."""

    heartbeat_unauth_count: int
    current_uptime_s: int | None
    tunnel_uptime_history: list[ShareTunnelUptimeEntry]


class ShareStatusResponse(PassthroughModel):
    """``build_share_status()``."""

    sharing_active: bool | None = None
    public_endpoint: str | None = None
    public_url: str | None = None
    forward_port: int | None = None
    started_at: str | None = None
    max_concurrent_sessions: int | None = None
    active_session_count: int | None = None
    services: list[ShareServiceEntry] | None = None
    invites: list[InviteRecord] | None = None
    sessions: list[ShareSessionRecord] | None = None
    audit_logs: list[ShareAuditLogRow] | None = None
    rate_limits: ShareRateLimits | None = None
    telemetry: ShareTelemetry | None = None


class ShareLiveResponse(PassthroughModel):
    """``build_share_live_payload()`` — the lean 10-s poll subset."""

    sharing_active: bool | None = None
    public_url: str | None = None
    active_session_count: int | None = None
    rate_limits: ShareRateLimits | None = None
    telemetry: ShareTelemetry | None = None


class ShareAuditLogsResponse(PassthroughModel):
    audit_logs: list[ShareAuditLogRow] | None = None


class ShareStartResponse(PassthroughModel):
    public_url: str | None = None


class SharePanicResponse(PassthroughModel):
    sessions_booted: int | None = None


class InviteMutationAck(PassthroughModel):
    """``{ok: true}`` acks that also report booted session count."""

    ok: bool | None = None
    booted_sessions: int | None = None


class ClaimTokenResponse(PassthroughModel):
    token: str | None = None


class BackupImportResponse(PassthroughModel):
    inserted: int | None = None
    skipped: int | None = None
    merged: int | None = None


class GdprEraseResponse(PassthroughModel):
    deleted_invites: int | None = None
    redacted_log_rows: int | None = None
    retained_recent_rows: int | None = None


class ShareSettingsResponse(PassthroughModel):
    max_concurrent_analyst_sessions: int | None = None


class WordphraseResponse(PassthroughModel):
    passcode: str | None = None


class ShareInvitesResponse(PassthroughModel):
    invites: list[InviteRecord] | None = None


class PasscodeRegenerateResponse(PassthroughModel):
    ok: bool | None = None
    invite_id: str | None = None
    passcode: str | None = None


class ShareTosPayload(BaseModel):
    """Body for ``POST /api/admin/share/tos`` — publish a new Terms of Service version."""

    version: str
    text: str


class ShareTosResponse(PassthroughModel):
    ok: bool | None = None
    version: str | None = None
    text: str | None = None
    published_at: str | None = None


class SharingDeployRequest(BaseModel):
    service_name: str
    domain_name: str
    origin_host: str
    origin_port: int = 80
    use_ssl: bool = False
    token_override: str | None = None
    override_host: str | None = None
    service_id: str | None = None


class SharingDeployResponse(BaseModel):
    service_id: str
    version: int
    domain_name: str
    origin_host: str


class SharingTeardownRequest(BaseModel):
    service_id: str
    token_override: str | None = None
