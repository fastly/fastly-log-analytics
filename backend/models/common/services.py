"""Service configuration, log fields catalog, custom fields, VCL linting, and saved views models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

from backend.core.catalog import VALID_NAME_RE
from backend.models.common.base import BaseResponse, PassthroughModel, WireSafeModel


class LogFieldCatalogEntry(WireSafeModel):
    id: str
    # ``None`` for the base-group built-ins; ``"custom"`` for custom fields.
    group: str | None
    label: str
    description: str
    duckdb_type: str
    # Built-in entries only.
    typical_bytes: int | None = None
    required_by: list[str] | None = None
    formatter: str | None = None
    unit: str | None = None
    precision: int | None = None
    individually_toggleable: bool | None = None
    default_off: bool | None = None
    note: str | None = None
    limit: int | None = None
    has_limit: bool | None = None
    # Custom-field entries only.
    show_in_dashboard: bool | None = None
    show_in_logs: bool | None = None
    filterable: bool | None = None
    value_type: str | None = None
    is_custom: bool | None = None


class LogFieldCatalogGroup(WireSafeModel):
    # ``get_groups_for_api`` emits every key for every group; the base group's id is ``None``.
    id: str | None
    label: str
    description: str
    locked: bool
    requires: str | None
    note: str | None
    recommended_with: list[str] | None
    total_bytes: int
    fields: list[str]


class LogFieldInsightDefinition(WireSafeModel):
    id: str
    category: str
    title: str
    description: str
    required_fields: list[str]
    required_groups: list[str]


class LogFieldPreset(WireSafeModel):
    label: str
    description: str
    groups: list[str]


class LogFieldsCatalogResponse(WireSafeModel):
    groups: list[LogFieldCatalogGroup] | None = None
    fields: list[LogFieldCatalogEntry] | None = None
    insights: list[LogFieldInsightDefinition] | None = None
    presets: dict[str, LogFieldPreset] | None = None


class CronSettingsPartial(BaseModel):
    """Partial-update slice of a single cron block (sync / compact / ngwaf).

    Distinct from ``ServiceCronSync`` etc. below which model the full
    persisted config (``enabled`` required). Here every field is
    optional — the handler only writes the keys the caller actually sent
    so the matching cron block stays unchanged for fields the operator
    didn't touch."""

    enabled: bool | None = None
    polling_mode: Literal["regular", "adaptive"] | None = None
    interval_mins: int | None = None
    # The persisted sync config uses interval_seconds, and the scheduler reads
    # it (interval_mins takes priority when both are set). Omitting it here made
    # the settings endpoint silently DROP a caller-supplied interval_seconds and
    # still answer "Successfully applied changes" — the value never reached the
    # config (observed 2026-08-13). Pydantic ignores unknown fields, so an
    # absent field on this partial is a silent no-op, not an error.
    interval_seconds: int | None = None
    commit_interval_mins: int | None = None
    log_enabled: bool | None = None
    log_retention_days: int | None = None
    data_retention_days: int | None = None
    rum_retention_days: int | None = None
    cache_retention_days: int | None = None
    rollup_retention_months: int | None = None
    # Snapshot-history window + expiry cadence (see run_cloud_maintenance and
    # the scheduler's expire job). keep_snapshot_days drives metadata.json size
    # and therefore per-commit cost; expire_interval_mins is how often it's
    # enforced.
    keep_snapshot_days: int | None = None
    expire_interval_mins: int | None = None
    delete_after: bool | None = None
    high_scale_shared_source: bool | None = None


class ServiceCronSync(CronSettingsPartial):
    enabled: bool
    polling_mode: Literal["regular", "adaptive"] = "adaptive"


class ServiceCronCompact(BaseModel):
    enabled: bool
    interval_mins: int | None = None
    log_enabled: bool | None = None
    log_retention_days: int | None = None


class ServiceCronNgwaf(BaseModel):
    interval_mins: int | None = None
    log_enabled: bool | None = None
    log_retention_days: int | None = None


class RumConfigSection(WireSafeModel):
    """Typed RUM configuration block on a persisted service config."""

    enabled: bool | None = None
    log_period: int | None = None
    sync_interval_seconds: int | None = None
    commit_interval_mins: int | None = None
    delete_after: bool | None = None


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
    storage_mode: str | None = None
    duckdb_size_bytes: int | None = None
    cache_file_count: int | None = None
    log_row_count: int | None = None
    is_active: bool | None = None
    cron_sync: ServiceCronSync | None = None
    cron_compact: ServiceCronCompact | None = None
    cron_ngwaf: ServiceCronNgwaf | None = None
    ngwaf_workspace_id: str | None = None
    logging_enabled: bool | None = None
    rum_enabled: bool | None = None
    rum: RumConfigSection | None = None
    rum_log_period: int | None = None
    is_high_scale: bool | None = None


class ServicesListResponse(BaseResponse):
    services: list[ServiceConfig]


class LogFieldsConfig(BaseModel):
    model_config = ConfigDict(extra="ignore")
    groups: list[str] = Field(default_factory=list)
    field_overrides: dict[str, bool] = Field(default_factory=dict)
    field_limits: dict[str, int] | None = None
    custom_fields: list[dict[str, JsonValue]] = Field(default_factory=list)


class LogFieldsUpdateRequest(BaseModel):
    log_fields: LogFieldsConfig


class LogFormatHistoryEntry(WireSafeModel):
    """One entry in ``log_format_history``."""

    timestamp: str | None = None
    changed_at: str | None = None
    actor: str | None = None
    changed_by: str | None = None
    summary: str | None = None
    preset: str | None = None
    groups: list[str] | None = None
    groups_before: list[str] | None = None
    groups_after: list[str] | None = None
    fields_added: list[str] | None = None
    fields_removed: list[str] | None = None
    field_overrides: dict[str, bool] | None = None
    field_limits: dict[str, int] | None = None


class LineBudgetWarning(WireSafeModel):
    """Line-budget / WAF warning payload returned with log-fields responses."""

    exceeded: bool | None = None
    estimate: int | None = None
    limit: int | None = None
    message: str | None = None


class LogFieldsResponse(BaseResponse):
    log_fields: LogFieldsConfig
    waf_warning: bool
    history: list[LogFormatHistoryEntry]
    estimate: int
    line_budget_warning: LineBudgetWarning | None = None


class CmcdSettingsResponse(BaseModel):
    """CMCD feature settings."""

    enabled: bool = False
    mode: str | None = None
    version: int | None = None


class LoggingSettingsResponse(BaseResponse):
    ok: bool
    prefix: str
    period: int
    sample_rate: float
    edge_only: bool
    custom_condition: str | None = None
    format_match: bool | None = None
    version: int | str | None = None
    cmcd: CmcdSettingsResponse | None = None


class RumSettingsPartial(BaseModel):
    """Partial-update for RUM-specific config (sync interval + retention)."""

    enabled: bool | None = None
    log_period: int | None = None
    sync_interval_seconds: int | None = None
    commit_interval_mins: int | None = None
    delete_after: bool | None = None


class ServiceCronSettingsBody(BaseModel):
    """Body for ``POST /api/services/{service_id}/cron-settings``.

    Every cron block is optional — operators can update one or all
    three without re-sending the others. Fields inside each block are
    also optional (see :class:`CronSettingsPartial`)."""

    cron_sync: CronSettingsPartial | None = None
    cron_compact: CronSettingsPartial | None = None
    cron_ngwaf: CronSettingsPartial | None = None
    rum: RumSettingsPartial | None = None


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


class OkMessageResponse(PassthroughModel):
    """``{ok, message}`` ack used by time-range clear and similar routes."""

    ok: bool | None = None
    message: str | None = None


class CredentialsUpdateResponse(PassthroughModel):
    """PATCH /services/{id}/credentials — ``access_key_id`` present only on
    the Fastly-API rotation branch."""

    ok: bool | None = None
    message: str | None = None
    access_key_id: str | None = None


class LogFieldsUpdateResponse(PassthroughModel):
    """POST /services/{id}/log-fields — no-change branch carries
    ``message``; the applied branch carries ``estimate`` +
    ``line_budget_warning`` (the ``waf_warning``-shaped dict, or null)."""

    ok: bool | None = None
    message: str | None = None
    estimate: int | None = None
    line_budget_warning: LineBudgetWarning | None = None


class CustomFieldsImportResponse(PassthroughModel):
    ok: bool | None = None
    imported_count: int | None = None


class CmcdEnableBody(BaseModel):
    token: str = ""
    mode: Literal["query_string", "headers"] = "query_string"
    version: int = 1


class CmcdDisableBody(BaseModel):
    token: str = ""


class CmcdStatusResponse(WireSafeModel):
    """``GET /api/services/{id}/cmcd/status``; only ``enabled`` is set when CMCD is off."""

    enabled: bool
    mode: str | None = None
    version: int | None = None
    enabled_at: str | None = None


# ── Custom Log Fields Models (folded from custom_fields.py) ───────────────────


class CustomField(BaseModel):
    name: str
    label: str
    description: str = ""
    vcl_log_expression: str
    collection_stage: Literal["edge", "origin", "deliver"] = "edge"
    origin_log_frequency: Literal["all", "miss_pass"] = "all"
    duckdb_type: Literal["VARCHAR", "INTEGER", "BIGINT", "DOUBLE", "BOOLEAN"] = "VARCHAR"
    value_type: Literal["string", "numeric", "boolean", "ip", "url"] = "string"
    bytes_estimate: int = 20
    nullable: bool = True
    enabled: bool = True
    show_in_dashboard: bool = False
    show_in_logs: bool = True
    filterable: bool = True

    @field_validator("name")
    @classmethod
    def _name_valid(cls, v: str) -> str:
        if not VALID_NAME_RE.fullmatch(v):
            raise ValueError("name must be lowercase alphanumeric + underscore, start with a letter, 1–48 chars")
        return v

    @field_validator("bytes_estimate")
    @classmethod
    def _bytes_in_range(cls, v: int) -> int:
        if not (1 <= v <= 1024):
            raise ValueError("bytes_estimate must be 1–1024")
        return v


class CustomFieldCreate(CustomField):
    pass


class CustomFieldUpdate(BaseModel):
    """All fields optional for PATCH semantics."""

    label: str | None = None
    description: str | None = None
    vcl_log_expression: str | None = None
    collection_stage: Literal["edge", "origin", "deliver"] | None = None
    origin_log_frequency: Literal["all", "miss_pass"] | None = None
    duckdb_type: Literal["VARCHAR", "INTEGER", "BIGINT", "DOUBLE", "BOOLEAN"] | None = None
    value_type: Literal["string", "numeric", "boolean", "ip", "url"] | None = None
    bytes_estimate: int | None = None
    nullable: bool | None = None
    enabled: bool | None = None
    show_in_dashboard: bool | None = None
    show_in_logs: bool | None = None
    filterable: bool | None = None

    @field_validator("bytes_estimate")
    @classmethod
    def _bytes_in_range(cls, v: int | None) -> int | None:
        if v is not None and not (1 <= v <= 1024):
            raise ValueError("bytes_estimate must be 1–1024")
        return v


class CustomFieldResponse(BaseResponse):
    field: CustomField
    warnings: list[str] = []


class CustomFieldsListResponse(BaseResponse):
    fields: list[CustomField]


class VclLintRequest(BaseModel):
    vcl_log_expression: str
    collection_stage: Literal["edge", "origin", "deliver"] = "edge"
    log_fields_config: dict | None = None


class VclLintResponse(BaseModel):
    valid: bool
    errors: list[str]
    warnings: list[str]
    format_length: int | None = None
    format_length_limit: int = 12000


class CustomFieldsImportBody(BaseModel):
    """Body for ``POST /api/services/{id}/custom-fields/import``."""

    custom_fields: list[dict[str, Any]] = []


class CustomFieldsExportFile(CustomFieldsImportBody):
    """Body of the ``GET /api/services/{id}/custom-fields/export`` file download."""


class SavedView(WireSafeModel):
    id: str | None = None
    service_id: str
    name: str
    filters_json: str
    time_range_type: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    page: str | None = None
    created_at: str | None = None


class SavedViewRecord(SavedView):
    """Wire-safe read shape for ``GET /api/views/{service_id}`` rows.

    Distinct from the strict ``SavedView`` request body: ``id`` is guaranteed
    non-null on persisted rows; ``extra="allow"`` + ``response_model_exclude_unset``
    is inherited from ``SavedView`` (``WireSafeModel``)."""

    id: str


class ViewSaveResponse(BaseModel):
    """Ack from ``POST /api/views/`` — id of the created row."""

    id: str
    status: str
