from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_validator

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
