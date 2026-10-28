"""Admin maintenance, compaction/rollup operations, metadata retention, debug settings, and usage/billing models."""

from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from backend.models.common.base import BaseResponse, PassthroughModel

# ── POP locations admin ────────────────────────────────────────────────────


class RefreshPopLocationsRequest(BaseModel):
    """Body for ``POST /api/admin/pop-locations/refresh`` — the Fastly API
    key used to pull the live POP catalog. Token can also arrive as a
    ``?token=`` query param for backward compat with older clients."""

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
# Same contract as the scoring read models (backend/models/session_scoring.py):
# ``extra="allow"`` so undeclared/future producer keys pass through verbatim,
# all-Optional fields so validation can never 500, and
# ``response_model_exclude_unset=True`` at the decorator so branch-dependent
# key sets (e.g. optimize-now's error branch) stay byte-identical on the wire.
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
    track_duckdb_httpfs: bool | None = None


class OptimizeNowResponse(PassthroughModel):
    """``optimize_table`` result — success carries the rewrite counters,
    the error branches carry ``error`` (+ ``files_rewritten: 0``)."""

    files_rewritten: int | None = None
    files_added: int | None = None
    eligible_partitions: int | None = None
    partition_errors: list[str] | None = None
    error: str | None = None


class ExpireSnapshotsResponse(PassthroughModel):
    """Outcome and counters from ``POST /admin/expire-snapshots/{service_id}``."""

    status: str | None = None
    service_id: str | None = None
    summary: str | None = None
    retention_deleted_rows: int | None = None
    snapshots_expired: int | None = None
    files_unlinked: int | None = None
    error: str | None = None


class SyncStartResponse(PassthroughModel):
    """Outcome and run ID from ``POST /admin/sync/{service_id}``."""

    ok: bool = True
    message: str | None = None
    run_id: int | str | None = None


class BackfillBundleRollupsResponse(PassthroughModel):
    """Per-kind bundle counts from ``POST /admin/backfill-bundle-rollups``."""

    time_series: int | None = None
    sessions: int | None = None
    slow_urls: int | None = None
    origin_summary: int | None = None
    origin_summary_days: int | None = None
    origin_dims: int | None = None
    origin_dims_days: int | None = None
    origin_latency_ts: int | None = None
    origin_latency_ts_days: int | None = None
    network_rtt: int | None = None
    network_rtt_days: int | None = None
    network_speed: int | None = None
    network_speed_days: int | None = None
    verified_bots_ts: int | None = None
    verified_bots_ts_days: int | None = None
    perf_latency: int | None = None
    perf_latency_days: int | None = None
    security_dims: int | None = None
    security_dims_days: int | None = None
    security_proxies: int | None = None
    security_proxies_days: int | None = None
    perf_ttl_dist: int | None = None
    perf_ttl_dist_days: int | None = None
    ngwaf_bots: int | None = None
    ngwaf_bots_days: int | None = None
    ngwaf_bots_ts: int | None = None
    ngwaf_bots_ts_days: int | None = None
    wellknown_bots: int | None = None
    overview: int | None = None
    overview_days: int | None = None
    network_heatmap: int | None = None
    network_geo: int | None = None
    network_quality: int | None = None
    pop_health: int | None = None
    rum: int | None = None


class RebuildTimeSeriesRollupsRequest(BaseModel):
    """Explicit closed UTC hours/days whose time-series bundles need repair."""

    hours: list[str] = Field(default_factory=list, max_length=24)
    days: list[str] = Field(default_factory=list, max_length=7)

    @model_validator(mode="after")
    def require_scope(self) -> RebuildTimeSeriesRollupsRequest:
        if not self.hours and not self.days:
            raise ValueError("at least one rollup hour or day is required")
        return self


class RebuildTimeSeriesRollupsResponse(PassthroughModel):
    """Result from ``POST /admin/rebuild-time-series-rollups``."""

    service_id: str
    requested_hours: list[str]
    rebuilt: int
    requested_days: list[str] = Field(default_factory=list)
    rebuilt_days: int | None = None


class ConsolidateRollupsResponse(PassthroughModel):
    """Result from POST /admin/consolidate-rollups."""

    service_id: str | None = None
    files_before: int | None = None
    files_after: int | None = None
    files_deleted: int | None = None
    hours_ip_bundled: int | None = None
    days_compacted: int | None = None
    days_bundled: int | None = None
    cleaned_entries: int | None = None
    duration_s: float | None = None


class RollupCompactResponse(PassthroughModel):
    """Result from POST /admin/rollups/compact/{service_id}."""

    status: str | None = None
    service_id: str | None = None
    rebuilt: int | None = None
    bundled: int | None = None
    retired_files: int | None = None
    retired_bytes: int | None = None
    duration_s: float | None = None
    summary: str | None = None
    errors: list[str] | None = None


class RollupHealResponse(PassthroughModel):
    """Result from POST /admin/rollup-heal/{service_id}."""

    status: str | None = None
    service_id: str | None = None
    missing: int | None = None
    rebuilt_fields: int | None = None
    bundled: int | None = None
    stamped_empty: int | None = None
    coverage_verified: bool | None = None
    duration_s: float | None = None
    summary: str | None = None
    run_id: int | None = None
    error_message: str | None = None


class RollupStatusResponse(PassthroughModel):
    """Rollup status from GET /admin/rollups/status."""

    service_id: str | None = None
    days_bundled: int | None = None
    hour_bundles: int | None = None
    latest_day_bundle: str | None = None
    earliest_day_bundle: str | None = None


class LocalCompactNowResponse(PassthroughModel):
    """``compact_local_partitions`` result dict."""

    partitions_scanned: int | None = None
    partitions_compacted: int | None = None
    files_merged: int | None = None
    files_removed: int | None = None
    bytes_before: int | None = None
    bytes_after: int | None = None
    daily_rollups: int | None = None
    weekly_rollups: int | None = None
    active_hour_skipped: bool | None = None
    stale_tmp_removed: int | None = None
    errors: list[str] | None = None
    duration_ms: int | None = None
    dry_run: bool | None = None


class PartialHourMergeResponse(PassthroughModel):
    """``merge_partial_hour`` result dict from ``POST /admin/partial-hour-merge``."""

    service_id: str | None = None
    hour: str | None = None
    new_files: int | None = None
    duration_ms: float | None = None
    gc_hours_removed: int | None = None
    status: str | None = None
    error: str | None = None


class PartialHourStatusResponse(PassthroughModel):
    """Partial-hour rollup status from ``GET /admin/partial-hour-status``."""

    service_id: str | None = None
    hour: str | None = None
    watermark: float | None = None
    watermark_iso: str | None = None
    file_exists: bool | None = None
    total_rows: int | None = None


class MetadataRetentionValues(PassthroughModel):
    """Resolved retention days (defaults merged with per-service cfg)."""

    usage_log_days: int | None = None
    ingested_files_days: int | None = None
    cron_runs_days: int | None = None


class MetadataRetentionResponse(PassthroughModel):
    retention: MetadataRetentionValues | None = None


class MetadataTableStat(PassthroughModel):
    """Per-table row count + estimated bytes (``None`` when SQLite lacks
    the ``dbstat`` vtable)."""

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
    adds the Fastly-stats + DuckDB-derived fields on top.
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
    deployment_mode: str = "standard"
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
    and ``edge_ratio`` (DuckDB) since those gate on the lazy /prefill call.
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
