from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from backend.models.common import BaseResponse


class ServiceCronSync(BaseModel):
    enabled: bool
    interval_mins: int | None = None
    commit_interval_mins: int | None = None
    delete_after: bool | None = None
    log_enabled: bool | None = None
    log_retention_days: int | None = None
    data_retention_days: int | None = None
    cache_retention_days: int | None = None


class ServiceCronCompact(BaseModel):
    enabled: bool
    interval_mins: int | None = None
    log_enabled: bool | None = None
    log_retention_days: int | None = None


class ServiceCronNgwaf(BaseModel):
    interval_mins: int | None = None
    log_enabled: bool | None = None
    log_retention_days: int | None = None


class ServiceConfig(BaseModel):
    service_id: str
    name: str
    # ``fos_bucket`` is an operator-internal infra string — the analyst-
    # trimmed view in api_services_list strips it out, and the serializer
    # must not reject the slim payload. Admin responses still carry the
    # populated value; the optional shape only changes the contract for
    # analyst-scoped reads.
    fos_bucket: str | None = None
    log_period: int | None = None
    access_level: str | None = None
    storage_mode: str | None = None
    duckdb_size_bytes: int | None = None
    cache_file_count: int | None = None
    log_row_count: int | None = None
    is_active: bool | None = None
    cron_sync: ServiceCronSync | None = None
    cron_compact: ServiceCronCompact | None = None
    cron_ngwaf: ServiceCronNgwaf | None = None
    ngwaf_workspace_id: str | None = None


class ServicesListResponse(BaseResponse):
    services: list[ServiceConfig]


class LogFieldsConfig(BaseModel):
    groups: list[str]
    field_overrides: dict[str, bool]
    field_limits: dict[str, int] | None = None


class LogFieldsUpdateRequest(BaseModel):
    log_fields: dict[str, Any]


class LogFieldsResponse(BaseResponse):
    log_fields: LogFieldsConfig
    waf_warning: bool
    history: list[dict[str, Any]]
    estimate: int
    line_budget_warning: dict[str, Any] | None = None


class LoggingSettingsResponse(BaseResponse):
    ok: bool
    prefix: str
    period: int
    sample_rate: float
    edge_only: bool
    custom_condition: str | None = None
    format_match: bool | None = None
    version: int | str | None = None


class AnalystInvite(BaseResponse):
    name: str
    service_id: str
    fos_bucket: str
    fos_region: str
    fos_endpoint: str
    fos_prefix: str
    access_key_id: str
    secret_key: str
    iceberg_metadata_location: str | None = None
    cdn_url: str | None = None
    cdn_service_id: str | None = None
    cdn_secret: str | None = None


class CronSettingsPartial(BaseModel):
    """Partial-update slice of a single cron block (sync / compact / ngwaf).

    Distinct from ``ServiceCronSync`` etc. above which model the full
    persisted config (``enabled`` required). Here every field is
    optional — the handler only writes the keys the caller actually sent
    so the matching cron block stays unchanged for fields the operator
    didn't touch."""

    enabled: bool | None = None
    interval_mins: int | None = None
    commit_interval_mins: int | None = None
    log_enabled: bool | None = None
    log_retention_days: int | None = None
    data_retention_days: int | None = None
    cache_retention_days: int | None = None
    delete_after: bool | None = None


class ServiceCronSettingsBody(BaseModel):
    """Body for ``POST /api/services/{service_id}/cron-settings``.

    Every cron block is optional — operators can update one or all
    three without re-sending the others. Fields inside each block are
    also optional (see :class:`CronSettingsPartial`)."""

    cron_sync: CronSettingsPartial | None = None
    cron_compact: CronSettingsPartial | None = None
    cron_ngwaf: CronSettingsPartial | None = None


class ServiceCredentialsBody(BaseModel):
    """Body for ``PATCH /api/services/{service_id}/credentials``.

    Two modes the handler picks between:
      - ``api_token`` set → admin-only path that mints a fresh FOS key
        via the Fastly API and replaces the old one.
      - ``access_key`` + ``secret_key`` set → validate-and-save mode for
        operator-provided credentials.

    Cross-field validation (one mode or the other, never both empty)
    stays in the handler so the existing 400 envelopes are preserved."""

    api_token: str = ""
    access_key: str = ""
    secret_key: str = ""
