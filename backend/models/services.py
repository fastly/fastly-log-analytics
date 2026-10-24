from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from backend.models._base import PassthroughModel, WireSafeModel
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
