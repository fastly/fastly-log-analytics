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
    log_fields: str | None = None


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
    remove_cache: bool = True
    remove_cron: bool = False


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


class CustomFieldsImportBody(BaseModel):
    """Body for ``POST /api/services/{id}/custom-fields/import``.

    The handler validates each entry through
    ``backend.core.field_registry.validate_custom_field`` — too dynamic
    to typify in Pydantic — so the list elements stay loosely typed
    here. The wrapper exists so the OpenAPI surface shows
    ``{custom_fields: list[object]}`` instead of an opaque dict."""

    custom_fields: list[dict[str, Any]] = []
