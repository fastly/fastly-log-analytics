"""Provisioning, RUM setup, NGWAF workspace, Terraform preview, and control-room models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, RootModel

from backend.models.common.base import PassthroughModel, WireSafeModel

# ── Control Room Models (folded from control_room.py) ─────────────────────────


class MutationStub(BaseModel):
    """Generic request body for Phase 0 mutation stubs."""

    action: str = ""
    payload: dict[str, Any] = {}


class ControlRoomActionResponse(WireSafeModel):
    status: str | None = None
    message: str | None = None


class CorrelateRequest(BaseModel):
    dimension: str
    window_minutes: int = Field(default=5, ge=1, le=60)
    limit: int = Field(default=10, ge=1, le=100)


class WizardStepRequest(BaseModel):
    """Request body for recording a wizard step completion."""

    step: str
    details: dict[str, Any] = {}


class ControlRoomTabResponse(WireSafeModel):
    """Canned per-tab payload (``TAB_STUBS``); the metric keys differ per tab."""

    tab: str
    status: str | None = None


class RealtimeSeedResponse(WireSafeModel):
    ticks: list[dict[str, Any]]


class LogFieldAuditFeature(WireSafeModel):
    name: str
    required_fields: list[str]
    status: str
    missing_fields: list[str]


class LogFieldAuditResponse(WireSafeModel):
    service_id: str
    enabled_fields: list[str]
    features: list[LogFieldAuditFeature]


class CorrelatorTopRow(WireSafeModel):
    value: str | int | float | None
    count: int


class CorrelatorFreshness(WireSafeModel):
    latest_log_at: str | None = None
    lag_seconds: int


class CorrelateResponse(WireSafeModel):
    dimension: str
    top: list[CorrelatorTopRow]
    freshness: CorrelatorFreshness


class WizardStateResponse(WireSafeModel):
    service_id: str
    completed_steps: list[str]
    is_complete: bool


class WizardStepResponse(WireSafeModel):
    recorded: bool
    step: str


# ── Provisioning Models (folded from provision.py) ────────────────────────────


class CheckFosRequest(BaseModel):
    """Body for ``POST /api/provision/check-fos``."""

    bucket: str
    region: str
    access_key: str
    secret_key: str


class ProvisionExecuteRequest(BaseModel):
    """Body for ``POST /api/provision/execute``."""

    token: str
    service_id: str
    service_name: str | None = None
    endpoint_name: str = "Fastly Object Storage Logs"
    fos_region: str = "us-east-1"
    fos_bucket_name: str
    fos_prefix: str = ""
    sample_rate: str = "100"
    edge_only: bool = True
    custom_condition: str | None = None
    log_period: str = "1 minute"
    cdn_service_name: str | None = None
    cdn_url: str | None = None
    cdn_shield: str = "none"
    enable_cron_sync: bool = True
    delete_after: bool = True
    commit_interval_mins: int = 5
    enable_cron_compact: bool = True
    log_retention_days: int = 30
    rum_retention_days: int = 30
    log_fields: str | None = None
    cmcd_enabled: bool = False
    cmcd_mode: str | None = None
    cmcd_version: int | None = None
    logging_enabled: bool = True
    rum_enabled: bool = False
    rum_log_period: str | None = None
    faro_version: str | None = None
    rum_custom_condition: str | None = None


class ProvisionValidateRequest(BaseModel):
    """Body for ``POST /api/provision/validate``."""

    token: str = ""
    service_id: str = ""


class ProvisionReconcileRequest(BaseModel):
    """Body for ``POST /api/provision/reconcile``."""

    service_id: str = ""
    token: str = ""


class ProvisionTeardownRequest(BaseModel):
    """Body for ``POST /api/provision/teardown``."""

    token: str = ""
    service_id: str | None = None
    remove_logging: bool = True
    remove_rum: bool = True
    remove_cdn: bool = True
    remove_bucket: bool = True
    remove_cloud_files: bool = True
    remove_scoring: bool = True
    remove_cache: bool = True
    remove_cron: bool = False
    remove_fos_tokens: bool = True


class RumDisableRequest(BaseModel):
    """Body for ``POST /api/services/{service_id}/rum/disable``."""

    token: str = ""
    remove_cloud_files: bool = True
    remove_bucket: bool = False
    activate: bool = True


class RumEnableRequest(BaseModel):
    """Body for ``POST /api/services/{service_id}/rum/enable``."""

    token: str = ""
    activate: bool = True


class RumUpgradeRequest(BaseModel):
    """Body for ``POST /api/services/{service_id}/rum/upgrade``."""

    version: str
    token: str = ""
    activate: bool = True


class RumSettingsUpdateRequest(BaseModel):
    """Body for ``POST /api/services/{service_id}/rum/settings``."""

    capture_vitals: bool = True
    capture_performance: bool = True
    capture_errors: bool = True
    capture_events: bool = True
    custom_condition: str = ""
    token: str = ""


class ProvisionConfigRequest(PassthroughModel):
    """Body shape shared by ``/provision/terraform/preview``,
    ``/provision/terraform/export``, and ``/provision/ingest``."""

    token: str | None = None
    service_id: str | None = None
    logging_service_id: str | None = None
    service_name: str | None = None
    endpoint_name: str | None = None
    fos_region: str | None = None
    fos_bucket_name: str | None = None
    fos_prefix: str | None = None
    fos_access_key: str | None = None
    fos_secret_key: str | None = None
    sample_rate: str | int | None = None
    edge_only: bool | None = None
    custom_condition: str | None = None
    log_period: str | int | None = None
    cdn_service_name: str | None = None
    cdn_prefix: str | None = None
    cdn_url: str | None = None
    cdn_shield: str | None = None
    cdn_secret: str | None = None
    enable_cron_sync: bool | None = None
    delete_after: bool | None = None
    commit_interval_mins: int | None = None
    enable_cron_compact: bool | None = None
    log_retention_days: int | None = None
    log_fields: str | dict[str, Any] | None = None
    logging_enabled: bool | None = None
    rum_enabled: bool | None = None


class ProvisionTokenInfo(PassthroughModel):
    id: str | None = None
    name: str | None = None
    user_id: str | None = None
    type: Literal["user", "automation"] | None = None


class ProvisionDefaults(PassthroughModel):
    endpoint_name: str | None = None
    fos_region: str | None = None
    fos_bucket_name: str | None = None
    fos_prefix: str | None = None
    sample_rate: int | None = None
    edge_only: bool | None = None
    log_period: str | None = None
    cdn_service_name: str | None = None
    cdn_prefix: str | None = None


class ProvisionValidateResponse(PassthroughModel):
    service_name: str | None = None
    token_info: ProvisionTokenInfo | None = None
    defaults: ProvisionDefaults | None = None


class ProvisionCheckDomainResponse(PassthroughModel):
    available: bool | None = None
    reason: str | None = None
    note: str | None = None


class ProvisionCheckFosResponse(PassthroughModel):
    ok: bool | None = None
    error: str | None = None


class ProvisionIngestResponse(PassthroughModel):
    ok: bool | None = None
    service_id: str | None = None


class ProvisionCheckConfigItem(PassthroughModel):
    ok: bool
    details: str


class ProvisionCheckConfigResponse(PassthroughModel):
    logging_service: ProvisionCheckConfigItem
    cdn_service: ProvisionCheckConfigItem


class NgwafWorkspace(PassthroughModel):
    id: str | None = None
    name: str | None = None


class NgwafWorkspacesResponse(PassthroughModel):
    workspaces: list[NgwafWorkspace] | None = None
    note: str | None = None
    error: str | None = None
    message: str | None = None
    error_hint: str | None = None
    debug_raw: str | None = Field(default=None, alias="_debug_raw")


class NgwafWorkspaceCreateRequest(PassthroughModel):
    ngwaf_workspace_id: str | None = None


class NgwafWorkspaceSetResponse(PassthroughModel):
    ok: bool | None = None
    ngwaf_workspace_id: str | None = None


class TerraformPreviewResponse(RootModel[dict[str, str]]):
    pass


class RumVersionsResponse(PassthroughModel):
    available: list[str] = []
    current: str | None = None
    latest: str | None = None
    update_available: bool = False
