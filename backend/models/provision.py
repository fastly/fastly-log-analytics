"""Pydantic models for the provisioning router.

Carved out of ``backend.routers.provision`` so the request schemas live
beside the rest of ``backend.models.*`` and OpenAPI codegen has one
canonical home for them.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class CheckFosRequest(BaseModel):
    """Body for ``POST /api/provision/check-fos`` — FOS credentials to
    validate by attempting a single list-objects call."""

    bucket: str
    region: str
    access_key: str
    secret_key: str


class LakeInfoRequest(BaseModel):
    """Body for ``POST /api/provision/lake-info`` — credentials + optional
    Iceberg-metadata pointer used to read an Iceberg table's range without
    registering it as a service."""

    bucket: str
    region: str
    access_key: str
    secret_key: str
    prefix: str = ""
    endpoint: str | None = None
    iceberg_metadata_location: str | None = None


class ProvisionExecuteRequest(BaseModel):
    """Body for ``POST /api/provision/execute`` — full Fastly-side
    provisioning payload (service, FOS sink, optional CDN, cron schedule,
    log fields)."""

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
    rum_retention_days: int = 90
    log_fields: str | None = None
    cmcd_enabled: bool = False
    cmcd_mode: str | None = None
    cmcd_version: int | None = None
    logging_enabled: bool = True
    rum_enabled: bool = False


class ProvisionValidateRequest(BaseModel):
    """Body for ``POST /api/provision/validate`` — Fastly token + service id
    the wizard uses to look up the service name and seed the bucket-name
    defaults before any side effects."""

    token: str = ""
    service_id: str = ""


class ProvisionTeardownRequest(BaseModel):
    """Body for ``POST /api/provision/teardown`` — destructive removal of a
    provisioned service. ``remove_*`` flags toggle each component.
    ``service_id`` and ``token`` are required at the handler level so the
    existing 400/404 envelopes are preserved."""

    token: str = ""
    service_id: str | None = None
    remove_logging: bool = True
    remove_cdn: bool = True
    remove_bucket: bool = True
    remove_cloud_files: bool = True
    remove_scoring: bool = True
    remove_cache: bool = True
    remove_cron: bool = False
    remove_fos_tokens: bool = True


class RumDisableRequest(BaseModel):
    """Body for ``POST /api/services/{service_id}/rum/disable`` — selective
    destructive removal of Real User Monitoring (RUM)."""

    token: str = ""
    remove_cloud_files: bool = True
    remove_bucket: bool = False
    activate: bool = True


class RumEnableRequest(BaseModel):
    """Body for ``POST /api/services/{service_id}/rum/enable`` — selective
    onboarding/enabling of Real User Monitoring (RUM)."""

    token: str = ""
    activate: bool = True


class RumUpgradeRequest(BaseModel):
    """Body for ``POST /api/services/{service_id}/rum/upgrade`` — pin a new
    Faro Web SDK version and reconcile the deployed bundle to match."""

    version: str
    token: str = ""
    activate: bool = True


class ProvisionConfigRequest(BaseModel):
    """Body shape shared by ``/provision/terraform/preview``,
    ``/provision/terraform/export``, and ``/provision/ingest``.

    All three endpoints accept a wizard-shaped service config that flows
    through to ``backend.utils.terraform_gen.generate_terraform`` or to
    the on-disk config writer. The wizard sometimes posts partial
    configs (terraform preview during mid-edit) so every field is
    optional; ``extra="allow"`` lets future / experimental wizard fields
    flow through unchanged without forcing the model in lockstep with
    every UI tweak. Required-field checks (token / fos_bucket_name on
    the ingest path) stay in the handlers so the existing 400 envelopes
    are preserved."""

    model_config = ConfigDict(extra="allow")

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


class CustomFieldsImportBody(BaseModel):
    """Body for ``POST /api/services/{id}/custom-fields/import``.

    The handler validates each entry through
    ``backend.core.field_registry.validate_custom_field`` — too dynamic
    to typify in Pydantic — so the list elements stay loosely typed
    here. The wrapper exists so the OpenAPI surface shows
    ``{custom_fields: list[object]}`` instead of an opaque dict."""

    custom_fields: list[dict[str, Any]] = []


# ── Wire-safe response models for the provision wizard endpoints ────────────
#
# Same contract as backend/models/session_scoring.py: ``extra="allow"`` +
# all-Optional fields + ``response_model_exclude_unset=True`` at the
# decorator, so branch-dependent key sets (e.g. check-domain's
# reason-vs-note, lake-info's table-exists variants) and the ``_debug_calls``
# telemetry envelope pass through byte-identically.


class _ProvisionRead(BaseModel):
    """Base for provision responses — passes undeclared keys through."""

    model_config = ConfigDict(extra="allow")


class ProvisionTokenInfo(_ProvisionRead):
    id: str | None = None
    name: str | None = None
    user_id: str | None = None
    type: str | None = None


class ProvisionDefaults(_ProvisionRead):
    endpoint_name: str | None = None
    fos_region: str | None = None
    fos_bucket_name: str | None = None
    fos_prefix: str | None = None
    sample_rate: int | None = None
    edge_only: bool | None = None
    log_period: str | None = None
    cdn_service_name: str | None = None
    cdn_prefix: str | None = None


class ProvisionValidateResponse(_ProvisionRead):
    service_name: str | None = None
    token_info: ProvisionTokenInfo | None = None
    defaults: ProvisionDefaults | None = None


class ProvisionCheckDomainResponse(_ProvisionRead):
    available: bool | None = None
    # Mutually exclusive by branch: ``reason`` when unavailable, ``note``
    # when available-with-caveat. exclude_unset keeps whichever was set.
    reason: str | None = None
    note: str | None = None


class ProvisionCheckFosResponse(_ProvisionRead):
    ok: bool | None = None
    error: str | None = None


class ProvisionLakeInfoResponse(_ProvisionRead):
    """``fetch_lake_info`` result. Only the branch-stable keys are declared;
    the table-details payload (row counts / calendar / extents) varies by
    Iceberg layout and rides through ``extra``."""

    ok: bool | None = None
    table_exists: bool | None = None
    message: str | None = None
    error: str | None = None


class ProvisionIngestResponse(_ProvisionRead):
    ok: bool | None = None
    service_id: str | None = None


class ProvisionCheckConfigItem(_ProvisionRead):
    ok: bool | None = None
    details: str | None = None


class ProvisionCheckConfigResponse(_ProvisionRead):
    logging_service: ProvisionCheckConfigItem | None = None
    cdn_service: ProvisionCheckConfigItem | None = None


class NgwafWorkspace(_ProvisionRead):
    id: str | None = None
    name: str | None = None


class NgwafWorkspacesResponse(_ProvisionRead):
    workspaces: list[NgwafWorkspace] | None = None
    note: str | None = None
    error: str | None = None
    message: str | None = None


class NgwafWorkspaceSetResponse(_ProvisionRead):
    ok: bool | None = None
    ngwaf_workspace_id: str | None = None


class RumVersionsResponse(_ProvisionRead):
    """Response for ``GET /api/services/{service_id}/rum/versions`` —
    available Faro Web SDK releases plus the operator's pinned/latest
    state. Only returned on a successful registry lookup; a registry
    failure surfaces as 503 instead of a degraded body (see the handler)."""

    available: list[str] = []
    current: str | None = None
    latest: str | None = None
    update_available: bool = False
