"""Admin maintenance, metadata retention, debug settings, and usage/billing models."""

from __future__ import annotations

from pydantic import BaseModel, Field

from backend.models.common.base import BaseResponse, PassthroughModel

# ── POP locations admin ────────────────────────────────────────────────────


class RefreshPopLocationsRequest(BaseModel):
    """Body for ``POST /api/admin/pop-locations/refresh`` — the Fastly API
    key used to pull the live POP catalog."""

    token: str = Field(..., description="Fastly API key")


class ResetLogsRequest(BaseModel):
    """Body for ``POST /api/admin/reset-logs`` — a destructive wipe of one
    service's log data. ``confirm`` must equal the resolved service id
    (belt-and-suspenders alongside the ``x-service-id``/``?service_id``
    resolution) so a stale/mistargeted request fails loud instead of
    silently wiping the wrong service."""

    confirm: str = Field(..., description="Must equal the target service_id.")
    delete_raw_logs: bool = Field(
        False,
        description="Also delete not-yet-ingested raw .gz logs in cloud storage. "
        "Default off — see the re-ingestion-storm warning in the design doc.",
    )
    preserve_usage_history: bool = Field(True, description="Keep Class A/B usage-log (billing) history.")


# ── /api/admin/usage-logging POST/PATCH body ───────────────────────────────


class _BillingRatesMixin(BaseModel):
    class_a_rate_per_1k: float | None = None
    class_b_rate_per_10k: float | None = None
    cdn_egress_rate_per_gb: float | None = None
    storage_rate_per_gb_month: float | None = None
    min_billed_days: int | float | None = None
    billing_zone: str | None = None


class UsageLoggingUpdateBody(_BillingRatesMixin):
    """Body for ``PATCH /api/admin/usage-logging``.

    All fields are optional — only keys explicitly present in the request
    are applied (preserves the existing partial-update semantics). Numeric
    fields are validated for positivity at the handler so the existing
    error envelope stays intact; the model just pins types for OpenAPI."""

    enabled: bool | None = None
    retention_days: float | None = None


# ── Wire-safe admin maintenance/config responses ───────────────────────────
#
# Same contract as the scoring read models (backend/models/analytics/sessions.py):
# ``extra="allow"`` so undeclared/future producer keys pass through verbatim,
# all-Optional fields so validation can never 500, and
# ``response_model_exclude_unset=True`` at the decorator so branch-dependent
# key sets stay byte-identical on the wire.
# Field lists derive from the producers, not from any frontend consumer.


class UsageLoggingConfigResponse(_BillingRatesMixin, PassthroughModel):
    """Merged global usage-logging config (``_USAGE_LOGGING_DEFAULTS`` +
    stored overrides). GET and PATCH both return this shape. The two
    day-count fields are ``int | float`` because the defaults are ints but
    a PATCH body stores floats — smart-union keeps whichever arrives."""

    enabled: bool | None = None
    retention_days: int | float | None = None
    class_a_rate_per_1k: int | float | None = None
    class_b_rate_per_10k: int | float | None = None
    cdn_egress_rate_per_gb: int | float | None = None
    storage_rate_per_gb_month: int | float | None = None


class SyncStartResponse(PassthroughModel):
    """Outcome and run ID from ``POST /admin/sync/{service_id}``."""

    ok: bool = True
    message: str | None = None
    run_id: int | str | None = None


class MetadataRetentionValues(PassthroughModel):
    """Resolved retention days (defaults merged with per-service cfg)."""

    usage_log_days: int | None = None
    ingested_files_days: int | None = None
    cron_runs_days: int | None = None


class MetadataRetentionResponse(PassthroughModel):
    retention: MetadataRetentionValues | None = None


class MetadataTableStat(PassthroughModel):
    """Per-table row count + estimated bytes."""

    rows: int | None = None
    bytes: int | None = None


class MetadataStorageResponse(PassthroughModel):
    """``GET /admin/metadata-storage`` — feeds the Metadata Storage card."""

    tables: dict[str, MetadataTableStat] | None = None
    db_bytes: int | None = None
    db_path: str | None = None
    retention: MetadataRetentionValues | None = None
    ingested_files_locked: bool | None = None


class MetadataRetentionUpdate(PassthroughModel):
    usage_log_days: int | str | None = None
    ingested_files_days: int | str | None = None
    cron_runs_days: int | str | None = None


class QuarantineDeleteResponse(PassthroughModel):
    purged: int


class DebugSettingsResponse(PassthroughModel):
    query_debug_visibility: str
    api_call_debug_visibility: str


class DebugSettingsUpdateBody(BaseModel):
    query_debug_visibility: str | None = None
    api_call_debug_visibility: str | None = None


# ── Usage & Billing Models (folded from usage.py) ─────────────────────────────


class _PrefillRatesBase(_BillingRatesMixin, BaseResponse):
    """Shared base for /api/usage/prefill and /api/usage/prefill/rates.

    Holds the fields the cost-page stat cards need on first paint
    (rates, local config, byte estimates). The full PrefillResponse
    adds the Fastly-stats + ClickHouse-derived fields on top.
    """

    avg_log_file_size_kb: float | None = None
    estimated_bytes_per_line: int | None = None
    data_days: int = 0
    log_period_seconds: int | None = None
    commit_interval_mins: int = 5
    sample_rate: int = 100
    edge_only: bool = False
    compaction_enabled: bool = True
    delete_after: bool = True
    log_retention_days: int = 30
    rum_retention_days: int = 30
    avg_nodes_per_flush: int | None = None
    min_billed_days: int | None = None
    billing_zone: str | None = "MBZ200"


class PrefillResponse(_PrefillRatesBase):
    requests_per_day: int | None = None
    edge_requests_per_day: int | None = None
    edge_ratio: float | None = None
    rum_beacons_per_day: int | None = None


class PrefillRatesResponse(_PrefillRatesBase):
    """Fast-path subset of PrefillResponse for /api/usage/prefill/rates.
    Omits ``requests_per_day`` / ``edge_requests_per_day`` (Fastly stats)
    and ``edge_ratio`` (ClickHouse) since those gate on the lazy /prefill call.
    """


class CurrentStorageResponse(BaseResponse):
    live_bytes: int
    live_files: int
    deleted_bytes: int
    quarantine_bytes: int = 0
    total_billed_bytes: int
    total_billed_gb_hours: float
    archive_bytes: int = 0
    archive_objects: int = 0
    archive_expired_bytes: int = 0
    cloud_copy_deleted_bytes: int = 0
    total_files: int
    total_bytes: int
    start: str
    end: str
    rum_bytes: int = 0
    regular_log_bytes: int = 0


class UsageOperationsPoint(BaseModel):
    date: str
    class_a: int
    class_b: int


class UsageOperationsResponse(BaseResponse):
    data: list[UsageOperationsPoint]
    total_class_a: int
    total_class_b: int
    granularity: str
    note: str
    fos_fields_found: list[str]


class UsageBandwidthPoint(BaseModel):
    time: str
    bandwidth_bytes: int
    requests: int


class UsageBandwidthResponse(BaseResponse):
    data: list[UsageBandwidthPoint]
    total_bytes: int
    total_log_bytes: int
    granularity: str


class UsageLogActivityPoint(BaseModel):
    time: str
    row_count: int
    bytes: int
    api_requests: int | None = None
    # Fastly's authoritative count of log records emitted to FOS for this
    # bucket — same field as /api/admin/log-accounting. Overlaying this on
    # the Processed chart lets the user directly compare emissions vs ingest.
    fastly_log_records: int | None = None


class UsageLogActivityResponse(BaseResponse):
    data: list[UsageLogActivityPoint]
    total_rows: int
    total_bytes: int
    total_api_requests: int | None = None
    total_fastly_log_records: int | None = None
    granularity: str


class RumBreakdownPoint(BaseModel):
    date: str
    beacon_count: int
    estimated_class_a_ops: int
    estimated_cost_usd: float


class UsageRumBreakdownResponse(BaseResponse):
    data: list[RumBreakdownPoint]
    total_beacons: int
    total_estimated_class_a: int
    total_estimated_cost_usd: float
    class_a_rate_per_1k: float | None = None
    average_beacons_per_day: float
    note: str
