from __future__ import annotations

import sys
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, RootModel, field_validator

from backend.core.field_registry import VALID_NAME_RE
from backend.models._base import PassthroughModel, SchemaColumn, WireSafeModel
from backend.models.common import BaseResponse


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
    logging_enabled: bool | None = None
    rum_enabled: bool | None = None
    rum: RumConfigSection | None = None
    rum_log_period: int | None = None
    analyst_path_a_supported: bool | None = None
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


class CronRunEntry(WireSafeModel):
    """One row of GET /api/cron-runs.

    ``extra="allow"`` passes any future cron_runs column through verbatim, and
    the field order mirrors ``metadata.cron_log.get_cron_runs`` so the
    serialized key order is byte-identical. Types match the producer exactly to
    avoid any coercion of the wire bytes: ``duration_s`` is a float (writers
    store ``0.0``), the count columns are ints, ``parquet_keys`` is the
    json.loads'd list."""

    # ``id`` / ``task`` / ``started_at`` are NOT NULL columns; ``status`` is nullable text.
    id: int
    task: str
    started_at: str
    duration_s: float | None = None
    status: str | None
    error_message: str | None = None
    files_downloaded: int | None = None
    files_deleted_fos: int | None = None
    rows_ingested: int | None = None
    corrupt_rows: int | None = None
    parquet_files_created: int | None = None
    parquet_files_optimized: int | None = None
    parquet_keys: list[str] = []
    summary: str | None = None


class CronRunsResponse(WireSafeModel):
    total: int | None = None
    page: int | None = None
    per_page: int | None = None
    entries: list[CronRunEntry] = []


# ── Wire-safe responses for the remaining services/core routes ─────────────
#
# Same contract as CronRunEntry above: ``extra="allow"`` + all-Optional +
# ``response_model_exclude_unset=True`` at the decorator so branch-dependent
# key sets stay byte-identical on the wire.


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


class CronScheduleEntry(PassthroughModel):
    """One row of GET /api/cron-schedule (``build_cron_schedule_payload``).
    ``disabled_reason`` appears only on synthesized disabled rows; the
    last_run_* trio only when a run has been recorded."""

    task: str
    next_run_time: str | None
    last_run_time: str | None = None
    last_run_status: str | None = None
    last_run_duration_s: float | None = None
    last_run_summary: str | None = None
    disabled_reason: str | None = None


class CronScheduleResponse(PassthroughModel):
    schedules: list[CronScheduleEntry] = []


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


# ── Status & Read Models (folded from status_reads.py) ────────────────────────


class CmcdEnableBody(BaseModel):
    token: str = ""
    mode: Literal["query_string", "headers"] = "query_string"
    version: int = 1


class CmcdDisableBody(BaseModel):
    token: str = ""


class DebugStateResponse(WireSafeModel):
    """``GET /api/debug/state``."""

    debug_responses_enabled: bool


class SchemaResponse(WireSafeModel):
    """``GET /api/schema``: cached or live column list plus the safe table name."""

    schema_: list[SchemaColumn] = Field(alias="schema")
    table_name: str


class AuditLogEntry(WireSafeModel):
    """One ``metadata.get_audit_logs`` row (``details`` is the json-decoded payload)."""

    id: int
    timestamp: str
    event_type: str
    details: dict[str, Any]
    actor: str | None
    source: str


class AuditLogsResponse(WireSafeModel):
    """``GET /api/audit-logs``. ``_debug_calls`` rides along as an extra."""

    total: int
    page: int
    per_page: int
    entries: list[AuditLogEntry]


class CmcdStatusResponse(WireSafeModel):
    """``GET /api/services/{id}/cmcd/status``; only ``enabled`` is set when CMCD is off."""

    enabled: bool
    mode: str | None = None
    version: int | None = None
    enabled_at: str | None = None


class SqliteProfilerEntry(BaseModel):
    seq: int
    ts: str
    sql: str
    params_kind: str
    time_ms: float
    rows: int
    op: Literal["execute", "executemany", "executescript"]


class RecentSqliteResponse(BaseModel):
    queries: list[SqliteProfilerEntry]
    buffer_size: int
    buffer_cap: int
    dropped: int
    last_seq: int


class ClearSqliteResponse(BaseModel):
    ok: bool = True
    buffer_size: int
    buffer_cap: int
    dropped: int


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


# ── Provisioning Models (folded from provision.py) ────────────────────────────


class CheckFosRequest(BaseModel):
    """Body for ``POST /api/provision/check-fos``."""

    bucket: str
    region: str
    access_key: str
    secret_key: str


class LakeInfoRequest(BaseModel):
    """Body for ``POST /api/provision/lake-info``."""

    bucket: str
    region: str
    access_key: str
    secret_key: str
    prefix: str = ""
    endpoint: str | None = None
    iceberg_metadata_location: str | None = None


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


class CustomFieldsImportBody(BaseModel):
    """Body for ``POST /api/services/{id}/custom-fields/import``."""

    custom_fields: list[dict[str, Any]] = []


class CustomFieldsExportFile(CustomFieldsImportBody):
    """Body of the ``GET /api/services/{id}/custom-fields/export`` file download."""


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


class ProvisionLakeInfoRange(PassthroughModel):
    start: str | None = None
    end: str | None = None


class ProvisionLakeInfoCalendarDay(PassthroughModel):
    data_files: int
    size_bytes: int


class ProvisionLakeInfoTable(PassthroughModel):
    table_name: str | None = None
    snapshots: int | None = None
    data_files: int | None = None
    size_bytes: int | None = None
    latest_snapshot_at: str | None = None
    buffer_files: int | None = None
    buffer_size_bytes: int | None = None
    table_location: str | None = None
    region: str | None = None
    min_timestamp: str | None = None
    max_timestamp: str | None = None


class ProvisionLakeInfoResponse(PassthroughModel):
    ok: bool | None = None
    table_exists: bool | None = None
    message: str | None = None
    error: str | None = None
    info: ProvisionLakeInfoTable | None = None
    calendar: dict[str, ProvisionLakeInfoCalendarDay] | None = None
    range: ProvisionLakeInfoRange | None = None


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


_self_mod = sys.modules[__name__]
sys.modules["backend.models.provision"] = _self_mod
sys.modules["backend.models.service"] = _self_mod
_models_pkg = sys.modules.get("backend.models")
if _models_pkg is not None:
    setattr(_models_pkg, "provision", _self_mod)
    setattr(_models_pkg, "service", _self_mod)
