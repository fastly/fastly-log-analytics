from __future__ import annotations

import sys
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_serializer, model_validator

from backend.models.common import (
    BaseResponse,
    ErrorDetail,
    JsonAny,
    JsonDict,
    LogExtentsMixin,
    OkResponse,
    PassthroughModel,
    WireSafeModel,
)

MAX_PAGE_SIZE = 500

# ── Metric snapshots history ───────────────────────────────────────────────


class MetricHistoryPoint(BaseModel):
    """One sample from the metric_snapshots time-series."""

    ts: str
    value: float


class MetricHistoryBatchResponse(BaseModel):
    """Every series newer than ``since``, returned by ``GET /api/admin/metric-history/batch``.

    Key shape: ``"{metric}"`` for global metrics, ``"{metric}|{service_id}"``
    for per-service, ``"{metric}|{service_id}|{task}"`` for per-task.
    """

    series: dict[str, list[MetricHistoryPoint]] = Field(default_factory=dict)


# ── Compaction stats ───────────────────────────────────────────────────────


class CompactionStatsResponse(BaseModel):
    """File-count distribution across local cache partitions.

    Returned by ``GET /api/admin/compaction-stats`` and consumed by the
    admin System Health card.
    """

    total_files: int = 0
    partitions: int = 0
    partitions_above_3: int = 0
    partitions_above_10: int = 0
    daily_files: int = 0
    weekly_files: int = 0
    avg_files_per_partition: float = 0.0


# ── Health snapshot ──────────────────────────────────────────────────────


class HealthLoadAverages(BaseModel):
    avg_1m: float
    avg_5m: float
    avg_15m: float


class HealthMemoryStats(BaseModel):
    total_mb: int
    available_mb: int
    used_pct: float | None = None


class HealthDiskStats(BaseModel):
    total_gb: float
    used_gb: float
    free_gb: float
    used_pct: float | None = None


class HealthInFlightRun(BaseModel):
    run_id: int
    service_id: str | None = None
    task: str | None = None
    started_at: str | None = None


class HealthCronFailure(BaseModel):
    """One service+task whose most recent terminal cron run ended in error.

    Powers the SRE-03 ``recent_cron_failures`` glance surface: a non-``sync``
    per-service cron (commit / optimize / rollup_compact / …)
    erroring every tick is otherwise visible only by opening that service's
    Cron History. The aggregate keeps the standing System Health card honest.
    """

    service_id: str
    task: str
    status: str
    started_at: str | None = None
    error_message: str | None = None


class HealthObservability(BaseModel):
    """Effective logging/telemetry mode (SRE-20).

    Tells an incident responder what their ``jq``/``grep`` over ``docker logs``
    will actually match: prod leaves ``STRUCTLOG_FORMAT`` unset → ConsoleRenderer
    (bracketed text, not JSON), and ``OTEL_EXPORTER=none`` means no span records
    → empty trace fields. The ADR documents the wiring; this is the runtime truth.
    """

    log_format: str  # "console" | "json"
    otel_exporter: str  # "none" | "console" | "otlp"


class HealthConfigBackup(BaseModel):
    """Freshness of the off-VM service-config backup (SRE-11 / ADR-13 §2.1).

    Read from a VM-local marker the backup writes on success. ``None`` fields
    mean no backup has ever been recorded — the honest answer when the only
    unrecoverable VM state has never been captured.
    """

    last_backup_at: str | None = None
    age_s: float | None = None
    source: str | None = None


class HealthFosProbe(BaseModel):
    """Per-service FOS reachability result (SRE-13), populated only when the
    snapshot is requested with ``?probe_fos=1``."""

    reachable: bool
    error: str | None = None


class HealthCeleryStats(BaseModel):
    queue_depth: int
    active_workers: int
    broker_reachable: bool


class HealthHighScaleDisk(BaseModel):
    """Latest serving-volume disk-guard evaluation (high-scale topology only)."""

    mode: Literal["normal", "warning", "protective", "emergency"]
    free_fraction: float
    reserve_bytes: int
    total_bytes: int
    free_bytes: int
    reasoning: str
    sample_age_s: float


class HealthHighScaleTopology(BaseModel):
    """Configured portable topology profile (what production HA claims rest on)."""

    name: str
    clickhouse_replicas_per_shard: int
    keeper_members: int
    persistent_serving_storage: bool
    production_ha: bool
    shard_count: int


class HealthHighScale(BaseModel):
    """High-scale-only health block; absent from the payload in standard mode."""

    disk: HealthHighScaleDisk | None = None
    topology: HealthHighScaleTopology | None = None


class HealthSnapshotResponse(BaseModel):
    """One-shot health snapshot served by ``GET /api/admin/health-snapshot``.

    The endpoint synthesises independent sub-system probes (load / memory /
    per-mount disk / in-flight runs / per-service compaction / DuckDB pool
    waits / scheduler-liveness / recent cron failures). Any individual probe
    that fails returns the null sentinel of its slot rather than failing the
    whole response, so the UI can render partial data when one subsystem is
    sick.
    """

    load: HealthLoadAverages | None = None
    vcpus: int | None = None
    memory: HealthMemoryStats | None = None
    data_mount: HealthDiskStats | None = None
    root_disk: HealthDiskStats | None = None
    in_flight_runs: list[HealthInFlightRun] = Field(default_factory=list)
    compaction: dict[str, CompactionStatsResponse | None] = Field(default_factory=dict)
    pool_wait: list[dict[str, Any]] = Field(default_factory=list)
    # SRE-06: age of the newest metric_snapshots row. The 60s sampler is a
    # global scheduler job, so a large/None value witnesses a dead scheduler
    # (vs. a healthy-idle one). None = no samples yet (fresh boot).
    scheduler_last_tick_age_s: float | None = None
    # SRE-03: services+tasks whose latest terminal cron run errored — the
    # cross-service glance the ADR-09 §2.3 runbook assumes exists.
    recent_cron_failures: list[HealthCronFailure] = Field(default_factory=list)
    # True once this process ran a config write without the cross-process flock (read-only FS / no fcntl).
    config_lock_degraded: bool = False
    # SRE-20: effective log/exporter mode so an incident grep is informed.
    observability: HealthObservability | None = None
    # SRE-11: freshness of the off-VM service-config backup (None = no marker).
    config_backup: HealthConfigBackup | None = None
    # SRE-13: per-service FOS reachability — only populated when probe_fos=1.
    fos: dict[str, HealthFosProbe] | None = None
    # Scaling Phase 10: Celery metrics (queue depth and worker count)
    celery: HealthCeleryStats | None = None
    # High-scale topology only (serving-volume disk guard). Omitted entirely in
    # standard mode so that mode's payload is unchanged.
    high_scale: HealthHighScale | None = None

    @model_serializer(mode="wrap")
    def _omit_inactive_high_scale(self, handler):  # type: ignore[no-untyped-def]
        data = handler(self)
        if isinstance(data, dict) and data.get("high_scale") is None:
            data.pop("high_scale", None)
        return data


class TreeNode(BaseModel):
    name: str
    type: Literal["file", "directory"]
    size: int | None = None
    mtime: str | None = None
    key: str | None = None
    prefix: str | None = None
    sync_status: Literal["synced", "local", "cloud"] | None = None
    is_cloud: bool | None = None


class TreeResponse(BaseResponse):
    nodes: list[TreeNode]


class PopLocation(BaseModel):
    code: str
    name: str
    latitude: float | None = None
    longitude: float | None = None
    attributes: dict[str, Any] | None = None
    coordinates: dict[str, Any] | None = None


class PopLocationsResponse(BaseResponse):
    pops: list[PopLocation]


class IngestedFile(BaseModel):
    file_name: str
    ingested_at: str
    row_count: int | None = None
    file_size_bytes: int | None = None


class IngestedFilesResponse(BaseResponse):
    files: list[IngestedFile]


class QuarantineEvidenceItem(BaseModel):
    id: int
    source_type: Literal["request", "rum"]
    original_key: str
    line_ordinal: int | None = None
    byte_offset: int | None = None
    byte_length: int
    error_category: str
    error_text: str
    sha256: str
    quarantined_at: str


class QuarantineSummary(BaseModel):
    total_items: int = 0
    total_bytes: int = 0
    request_items: int = 0
    rum_items: int = 0
    oldest_at: str | None = None
    newest_at: str | None = None
    category_counts: dict[str, int] = Field(default_factory=dict)


class QuarantineListResponse(BaseResponse):
    items: list[QuarantineEvidenceItem] = Field(default_factory=list)
    total: int = 0


class StreamMetrics(WireSafeModel):
    latest_log_at: str | None = None
    total_rows: int | None = None
    last_sync_at: str | None = None


class ShareHeaderBadgeResponse(WireSafeModel):
    latest_log_at: str | None = None
    local_rows: int | None = None
    rum: StreamMetrics | None = None
    request: StreamMetrics | None = None


class SyncStatus(ShareHeaderBadgeResponse, LogExtentsMixin):
    configured: bool = True
    busy: bool = False
    storage_mode: str | None = None
    latest_ingested_file_at: str | None = None
    latest_available_file_at: str | None = None
    duckdb_size_bytes: int | None = None
    duckdb_exists: bool | None = None
    active_run: dict[str, Any] | None = None
    ngwaf_workspace_id: str | None = None


class SyncStatusResponse(BaseResponse, SyncStatus):
    pass


class LogExtentsResponse(BaseResponse, LogExtentsMixin):
    """Minimal extents projection for the FilterBar's time-range snap.

    Sibling of ``SyncStatusResponse`` but strips every field that the
    middleware blocks ``/api/sync-status`` for an analyst over: no
    ``ngwaf_workspace_id``, no ``active_run``, no cron task state, no
    DuckDB size, no storage mode. Just the two timestamps the
    FilterBar needs to snap its range, plus a ``configured`` flag so
    the frontend can short-circuit when a service has no source.
    """

    configured: bool = True


class BotSourceMeta(BaseModel):
    id: str
    name: str
    url: str | None = None
    last_updated: str | None = None
    entry_count: int | None = None


class RdnsStats(BaseModel):
    total: int
    pending: int
    last_enrichment_at: str | None = None


class BotSourcesResponse(BaseResponse):
    sources: list[BotSourceMeta]
    rdns: RdnsStats


class SystemJobStatus(BaseModel):
    id: str
    name: str
    last_run_at: str | None = None
    status: str | None = None
    duration_s: float | None = None
    detail: str | None = None
    next_run_at: str | None = None


class SystemJobsResponse(BaseResponse):
    jobs: list[SystemJobStatus]


class StorageTableInfo(BaseModel):
    table_name: str
    table_location: str | None = None
    snapshots: int
    data_files: int
    size_bytes: int
    latest_snapshot_at: str | None = None
    buffer_files: int = 0
    buffer_size_bytes: int = 0
    region: str | None = None


class StorageTableInfoResponse(BaseResponse, StorageTableInfo):
    pass


class UsageLogEntry(BaseModel):
    # service_id is hoisted to UsageLogResponse — every row in the
    # response is scoped to a single service anyway, so repeating it
    # per row was wire-byte overhead. The frontend page mapper
    # re-injects it into each row for the table renderer.
    id: int
    timestamp: str
    operation_class: str | None = None
    operation_type: str | None = None
    url: str | None = None
    bytes: int | None = None
    duration_ms: float | None = None
    function_name: str | None = None
    process_context: str | None = None
    status: str | None = None
    estimated_cost: float | None = None
    count: int = 1


class UsageLogAggregate(BaseModel):
    total_class_a: int = 0
    total_class_b: int = 0
    total_cdn_downloads: int = 0
    total_cdn_bytes: int = 0
    total_fos_bytes: int = 0
    estimated_cost_class_a: float = 0.0
    estimated_cost_class_b: float = 0.0
    estimated_cost_cdn: float = 0.0
    estimated_cost_total: float = 0.0
    class_a_breakdown: dict[str, int] = {}
    class_b_breakdown: dict[str, int] = {}


class UsageLogResponse(BaseResponse):
    service_id: str | None = None
    entries: list[UsageLogEntry]
    total: int
    aggregate: UsageLogAggregate


class ProvisionService(BaseModel):
    id: str
    name: str
    provisioned: bool


class LogAccountingBucket(BaseModel):
    ts: str
    fastly_requests: int
    our_rows: int
    file_count: int
    gap: int
    gap_pct: float


class LogAccountingTotals(BaseModel):
    fastly_requests: int
    our_rows: int
    gap: int
    gap_pct: float
    worst_bucket_ts: str | None = None
    worst_bucket_gap_pct: float | None = None


class SustainedLossAlert(BaseModel):
    started_at: str
    n_buckets: int
    max_gap_pct: float
    total_lost_lines: int


class IngestCatchupStatus(BaseModel):
    """How far behind ingest is from the wallclock — derived from the
    max(ingested_at) of any successful ingest. ``lag_seconds`` is the gap
    between now and that timestamp; the ``status`` field collapses the
    raw number into something a human can act on at a glance.
    """

    latest_ingest_ts: str | None
    lag_seconds: int | None
    status: Literal["caught_up", "backfilling", "stalled", "no_data"]


class LogAccountingResponse(BaseResponse):
    by: Literal["hour", "day"]
    from_ts: str
    to_ts: str
    buckets: list[LogAccountingBucket]
    totals: LogAccountingTotals
    sustained_loss: SustainedLossAlert | None = None
    catchup: IngestCatchupStatus | None = None


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


# ── High-Scale Models (folded from high_scale.py) ─────────────────────────────


class HighScaleRequestFactRequest(BaseModel):
    start_time: str | None = None
    end_time: str | None = None
    limit: int = Field(default=MAX_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    cursor: str | None = None
    status: str | int | None = None
    domain: str | None = None
    pop: str | None = None
    country: str | None = None
    batch_id: str | None = None
    emulate_standard: bool = False


class ServingWatermarkResponse(BaseModel):
    service_id: str
    domain: str
    owner_epoch: int
    coverage_start: datetime | None
    coverage_end: datetime | None
    last_accepted_cursor: str | None
    last_archived_event_id: str | None
    last_visible_event_id: str | None
    exact: bool


class QueryResponseMetadataResponse(BaseModel):
    status: Literal["complete", "partial", "queued", "running", "failed"]
    exact: bool
    coverage: float
    freshness_lag_seconds: float
    watermark: ServingWatermarkResponse
    approximation_error: float | None
    error: str | None
    mode: Literal["clickhouse", "duckdb_emulation"] = "clickhouse"


class HighScaleRequestFactResponse(BaseResponse):
    rows: list[dict[str, Any]]
    metadata: QueryResponseMetadataResponse
    next_cursor: str | None


class HighScaleRumFactResponse(BaseResponse):
    rows: list[dict[str, Any]]
    metadata: QueryResponseMetadataResponse
    next_cursor: str | None


ExportDomain = Literal["request", "rum_vitals", "rum_errors", "cmcd"]


class HighScaleExportRequest(HighScaleRequestFactRequest):
    domain: ExportDomain = "request"


class HighScaleExportResponse(BaseResponse):
    export_id: str
    service_id: str
    state: Literal["queued", "running", "completed", "cancelled", "failed", "expired"]
    rows_written: int
    bytes_written: int
    error: str | None


class HighScaleExportCancelResponse(HighScaleExportResponse):
    cancelled: bool


class HighScaleAggregateRequest(BaseModel):
    domain: ExportDomain = "request"
    dimension: str | None = None
    start_time: str | None = None
    end_time: str | None = None


class HighScaleAggregateValue(BaseModel):
    value: str
    count: int


class HighScaleAggregateResponse(BaseResponse):
    service_id: str
    domain: str
    dimension: str
    request_count: int
    top_values: list[HighScaleAggregateValue]
    coverage: float
    freshness_lag_seconds: float
    exact: bool
    approximation_error: float | None


class HighScaleQueryRequest(BaseModel):
    domain: ExportDomain = "request"
    start_time: str | None = None
    end_time: str | None = None
    limit: int = Field(default=MAX_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)
    cursor: str | None = None


class HighScaleQueryPlanResponse(BaseModel):
    tier: str
    sources: list[str]
    queued: bool
    bounded: bool
    max_rows: int
    reason: str


class HighScaleQueryResponse(BaseResponse):
    service_id: str
    domain: str
    plan: HighScaleQueryPlanResponse
    rows: list[dict[str, Any]] | None
    next_cursor: str | None
    metadata: QueryResponseMetadataResponse | None
    job_id: str | None


class HighScaleHistoricalJobResponse(BaseResponse):
    job_id: str
    state: Literal["queued", "running", "completed", "cancelled", "failed", "expired"]
    rows: list[dict[str, Any]]
    row_count: int
    output_bytes: int
    truncated: bool
    error: str | None
    created_at: datetime
    expires_at: datetime
    terminal_at: datetime | None


class HighScaleHistoricalJobCancelResponse(BaseResponse):
    job_id: str
    cancelled: bool


class HighScaleRetentionRecommendationResponse(BaseResponse):
    """Advisory hot/warm retention for one service (never applied automatically).

    ``effective_retention_days`` is the explicit per-service
    ``provisioning.cron_sync.data_retention_days`` whenever one is set
    (``source="override"``; ``null`` when it is ``0``, meaning keep forever),
    otherwise the recommended warm window (``source="recommendation"``).
    """

    service_id: str
    recommended_hot_days: float
    recommended_warm_days: float
    reasoning: str
    volume_total_bytes: int
    volume_total_source: Literal["request", "sampled"]
    explicit_retention_days: int | None = None
    effective_retention_days: float | None = None
    source: Literal["override", "recommendation"]


# ── Admin Status Models (folded from admin_status.py) ─────────────────────────

Reference = Annotated[str, Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")]


class ClickHouseActiveGeneration(BaseModel):
    generation: str
    dataset_id: str
    coverage_start: str
    coverage_end: str
    expires_at: str
    expired: bool
    target_matches: bool
    coverage_age_seconds: float


class ClickHouseStatusResponse(BaseResponse):
    service_id: str
    enabled: bool
    health: Literal["ok", "disabled", "unavailable"]
    schema_version: int | None = None
    publication_counts: dict[str, int] | None = None
    oldest_pending_age_seconds: float | None = None
    last_published_at: str | None = None
    active_generation: ClickHouseActiveGeneration | None = None
    expired_dataset_count: int | None = None


class ClickHouseReplayRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_id: str = Field(min_length=1, max_length=128)
    dataset_id: Reference | None = None
    generation: Reference | None = None
    dry_run: bool = True
    limit: int = Field(default=100, ge=1, le=1000, strict=True)

    @model_validator(mode="after")
    def exactly_one_reference(self) -> Self:
        if (self.dataset_id is None) == (self.generation is None):
            raise ValueError("provide exactly one of dataset_id or generation")
        return self


class ClickHouseReplayResponse(BaseResponse):
    service_id: str
    dry_run: bool
    operation: Literal["rebuild", "resume"]
    dataset_id: str
    generation: str | None
    limit: int
    artifact_count: int
    planned_artifacts: int
    attempted: int = 0
    published: int = 0
    activated: bool = False


class ClickHouseReplayErrorResponse(BaseResponse):
    detail: ErrorDetail


class CeleryActiveTask(WireSafeModel):
    """One entry of Celery ``inspect().active()``; other inspect keys ride as extras."""

    id: str
    name: str
    args: list[Any]
    time_start: int | float


class CeleryWorkers(WireSafeModel):
    """``workers`` block of ``get_celery_status``. ``count`` is ``len(stats)``;
    ``error`` is present only when the inspect call failed."""

    active_tasks: dict[str, list[CeleryActiveTask]]
    stats: JsonDict
    registered: dict[str, list[str]]
    scheduled: JsonDict
    count: int
    error: str | None = None


class InProcessScheduleEntry(WireSafeModel):
    name: str
    trigger: str
    next_run_at: float | None = None


class InProcessActiveRun(WireSafeModel):
    run_id: int
    service_id: str | None = None
    task: str | None = None
    started_at: float | None = None


class StandardIngestSummary(WireSafeModel):
    ingested_files: int = 0
    quarantined_items: int = 0


class CeleryStatusResponse(WireSafeModel):
    """``queues`` is ``{queue: depth}``; ``ledger`` is ``{ingest_ledger.status: count}``
    summed across services, so both keep open string keys."""

    deployment_mode: Literal["standard", "high_scale"] = "standard"
    broker_reachable: bool | None = None
    workers: CeleryWorkers | None = None
    queues: dict[str, int] | None = None
    ledger: dict[str, int] | None = None
    inprocess_schedules: list[InProcessScheduleEntry] | None = None
    inprocess_active_runs: list[InProcessActiveRun] | None = None
    standard_ingest: StandardIngestSummary | None = None


class IngestLedgerEntry(WireSafeModel):
    """Single row from ``ingest_ledger`` exposed on ``/api/admin/celery/ledger``."""

    service_id: str
    object_key: str
    source_type: Literal["request", "rum"]
    status: str
    attempts: int = 0
    error_kind: str | None = None
    last_error: str | None = None
    claimed_by: str | None = None
    claimed_at: float | None = None
    discovered_at: float | None = None
    committed_at: float | None = None
    next_attempt_at: float | None = None
    originating_task: str | None = None
    originating_run_id: int | None = None
    size_bytes: int | None = None


class IngestLedgerListResponse(WireSafeModel):
    items: list[IngestLedgerEntry]
    total: int


class IngestLedgerItemTarget(WireSafeModel):
    service_id: str
    object_key: str


class IngestLedgerActionRequest(WireSafeModel):
    """Target either specific ``items`` or all rows matching ``status`` (optionally
    scoped to ``service_id``)."""

    items: list[IngestLedgerItemTarget] | None = None
    status: Literal["dead_letter", "quarantined", "problem"] | None = None
    service_id: str | None = None


class IngestLedgerActionResponse(WireSafeModel):
    ok: bool = True
    affected: int = 0
    dispatched: int = 0


class DuckdbStatusResponse(WireSafeModel):
    ok: bool
    memory: JsonDict | None = None
    barrier: JsonDict | None = None
    pools: JsonAny = None
    retired_pools: JsonAny = None


class NgwafStatusResponse(WireSafeModel):
    """``total_cached_bots`` / ``workspaces`` / ``top_bots`` come from the cache stats;
    ``service_workspace_id`` / ``service_configured`` only when ``service_id`` was passed."""

    ok: bool
    total_cached_bots: int | None = None
    workspaces: dict[str, str | None] | None = None
    top_bots: dict[str, int] | None = None
    service_workspace_id: str | None = None
    service_configured: bool | None = None


class RdnsStatsResponse(WireSafeModel):
    ok: bool
    stats: JsonDict | None = None


# ── Share-Admin Models (folded from share_admin.py) ───────────────────────────


class TosDocument(BaseModel):
    version: str
    text: str


class AuthConfigProvider(BaseModel):
    """One enabled OIDC provider as seen by the unauthenticated login page.

    Deliberately id + display_name ONLY — no client_id / discovery_url / secret
    is ever exposed to an unauthenticated caller.
    """

    id: str
    display_name: str


class AuthConfigResponse(BaseModel):
    """Body for the unauth ``GET /api/share/auth-config`` — drives graceful
    degradation on ``/share-login`` (which auth modes to render)."""

    passcode_enabled: bool
    providers: list[AuthConfigProvider] = []


class ShareLoginResponse(OkResponse):
    # session_id is intentionally NOT returned: the session id is the bearer
    # token and lives only in the httponly/secure/samesite cookie. Mirroring
    # it into the JSON body would defeat httponly if any XSS landed. The
    # frontend never reads it — auth rides entirely on the cookie.
    name: str
    email: str
    tos_pending: bool
    tos: TosDocument | None = None
    service_ids: list[str] = []
    redirect: str


class ShareLogoutResponse(OkResponse):
    pass


class ShareAcknowledgeResponse(OkResponse):
    pass


class ShareHeartbeatResponse(OkResponse):
    # session_id omitted for the same reason as ShareLoginResponse — the
    # bearer token stays in the httponly cookie, never the JSON body.
    last_active: str | float


class ShareClaimResponse(OkResponse):
    name: str | None = None
    email: str | None = None
    expires_at: str | None = None
    service_ids: list[str] = []


class ShareLoginPayload(BaseModel):
    """Body for ``POST /api/share/login``."""

    email: str
    passcode: str


class TosAckPayload(BaseModel):
    """Body for ``POST /api/share/acknowledge`` — the TOS version the analyst
    is acknowledging. Server compares against the latest stored version and
    rejects on mismatch (audit finding 021)."""

    version: str


class PiiPolicyPayload(WireSafeModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/pii`` — toggle the
    invite's PII policy (currently just ``mask_ips``) after creation."""

    mask_ips: bool = False


class ShareStartPayload(BaseModel):
    """Body for ``POST /api/share-admin/start`` — start the analyst-facing
    tunnel on an optional public endpoint, forwarding to ``forward_port``."""

    public_endpoint: str | None = None
    forward_port: int = 3000


class InvitePayload(BaseModel):
    """Body for ``POST /api/share-admin/invites`` — create a new
    analyst-facing share invite.

    ``auth_method`` selects how the analyst redeems the invite:

    * ``'passcode'`` (default) — a ``passcode`` is required and hashed.
    * ``'oauth'`` — ``oauth_provider`` (a configured registry key) is required;
      no ``passcode`` is sent (the backend synthesizes an unguessable argon2id
      placeholder for the NOT NULL column). Passcode flow is unchanged.
    """

    name: str
    email: str
    passcode: str | None = None
    duration_hours: int | None = Field(default=24)
    ip_whitelist: str | None = None
    service_ids: list[str] = Field(default_factory=list)
    pii_policy: PiiPolicyPayload | None = None
    query_window_hours: int | None = None
    query_start_time: str | None = None
    query_end_time: str | None = None
    allow_concurrent_sessions: bool = False
    auth_method: Literal["passcode", "oauth"] = "passcode"
    oauth_provider: str | None = None


class OAuthProviderInfo(BaseModel):
    """One configured OIDC provider as seen by the admin invite form."""

    id: str
    display_name: str
    enabled: bool


class OAuthProvidersResponse(BaseModel):
    """Body for ``GET /api/admin/share/oauth-providers`` — the configured
    providers an admin can attach to an OAuth invite (may include disabled)."""

    providers: list[OAuthProviderInfo] = Field(default_factory=list)


class ServiceScopePayload(BaseModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/services`` — replace
    the invite's service-id allowlist."""

    service_ids: list[str]


class PasscodePayload(BaseModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/passcode`` — rotate
    the invite's passcode in-place (keeps audit history + TOS acceptance)."""

    passcode: str


class SharingPolicyPayload(BaseModel):
    """Body for ``PATCH /api/share-admin/invites/{id}/sharing`` — toggle
    whether the invite allows shared (concurrent) analyst logins."""

    allow_concurrent_sessions: bool


class BackupExportPayload(BaseModel):
    """Body for ``POST /api/share-admin/backup/export`` — passphrase used
    to encrypt the share-db backup blob (minimum 12 characters)."""

    passphrase: str


class GdprErasePayload(BaseModel):
    """Body for ``POST /api/share-admin/gdpr/erase`` — GDPR right-to-be-
    forgotten request for an analyst email."""

    email: str
    reason: str


class SettingsPayload(BaseModel):
    """Body for ``PATCH /api/share-admin/settings`` — partial-update knobs
    for the share-admin global settings (currently just the session cap)."""

    max_concurrent_analyst_sessions: int | None = None


class ShareBannerResponse(WireSafeModel):
    sharing_active: bool | None = None
    public_url: str | None = None


class InviteRecord(PassthroughModel):
    """One remote-invite row (``SELECT *`` + derived fields). The stored
    passcode hash rides through ``extra`` untouched — intentionally NOT
    declared here so the OpenAPI surface doesn't advertise it; the schema
    itself may grow columns via share_db migrations, which ``extra`` also
    absorbs. ``last_login_at`` is attached by ``build_share_status`` only."""

    id: str | None = None
    name: str | None = None
    email: str | None = None
    expires_at: str | None = None
    ip_whitelist: str | None = None
    pii_policy: PiiPolicyPayload | None = None
    query_window_hours: int | None = None
    query_start_time: str | None = None
    query_end_time: str | None = None
    created_at: str | None = None
    # Wire value is the SQLite INTEGER flag, 0 (active) or 1 (revoked), never a JSON
    # boolean; the literal keeps the bytes unchanged while staying truthiness-compatible.
    revoked: Literal[0, 1] | None = None
    tos_accepted_at: str | None = None
    tos_version: str | None = None
    allow_concurrent_sessions: bool | None = None
    auth_method: Literal["passcode", "oauth"] | None = None
    oauth_provider: str | None = None
    oauth_subject: str | None = None
    last_login_at: str | None = None
    service_ids: list[str] | None = None


class ShareServiceEntry(PassthroughModel):
    service_id: str | None = None
    name: str | None = None
    remote_frontend_deployed: bool | None = None
    sharing_domain: str | None = None
    remote_service_id: str | None = None


class ShareSessionRecord(PassthroughModel):
    """One live analyst session: ``AnalystSession.to_dict()`` (a dataclass
    ``asdict``, so every key is always present)."""

    session_id: str
    invite_id: str
    name: str
    email: str
    ip_address: str
    user_agent: str
    fingerprint_signature: str
    pii_policy: PiiPolicyPayload
    query_window_hours: int | None
    query_start_time: str | None
    query_end_time: str | None
    login_time: str
    last_active_time: str
    last_activity: str | None
    tos_pending: bool
    service_ids: list[str]
    auth_method: Literal["passcode", "oauth"]
    oauth_provider: str | None


class ShareAuditLogRow(PassthroughModel):
    """One ``remote_share_audit_logs`` row (``SELECT *``)."""

    id: int
    timestamp: str
    event_type: str
    email: str | None
    ip_address: str
    details: str


class ShareRateLimitFailure(PassthroughModel):
    ip: str
    count: int
    window_s: int


class ShareRateLimitLockout(PassthroughModel):
    ip: str
    remaining_s: int


class ShareRateLimits(PassthroughModel):
    """``LoginRateLimiter.snapshot()``."""

    failures: list[ShareRateLimitFailure]
    lockouts: list[ShareRateLimitLockout]


class ShareTunnelUptimeEntry(PassthroughModel):
    started_at: str
    ended_at: str
    duration_s: int
    reason: str


class ShareTelemetry(PassthroughModel):
    """``TunnelManager.get_telemetry()``."""

    heartbeat_unauth_count: int
    current_uptime_s: int | None
    tunnel_uptime_history: list[ShareTunnelUptimeEntry]


class ShareStatusResponse(PassthroughModel):
    """``build_share_status()``."""

    sharing_active: bool | None = None
    public_endpoint: str | None = None
    public_url: str | None = None
    forward_port: int | None = None
    started_at: str | None = None
    max_concurrent_sessions: int | None = None
    active_session_count: int | None = None
    services: list[ShareServiceEntry] | None = None
    invites: list[InviteRecord] | None = None
    sessions: list[ShareSessionRecord] | None = None
    audit_logs: list[ShareAuditLogRow] | None = None
    rate_limits: ShareRateLimits | None = None
    telemetry: ShareTelemetry | None = None


class ShareLiveResponse(PassthroughModel):
    """``build_share_live_payload()`` — the lean 10-s poll subset."""

    sharing_active: bool | None = None
    public_url: str | None = None
    active_session_count: int | None = None
    rate_limits: ShareRateLimits | None = None
    telemetry: ShareTelemetry | None = None


class ShareAuditLogsResponse(PassthroughModel):
    audit_logs: list[ShareAuditLogRow] | None = None


class ShareStartResponse(PassthroughModel):
    public_url: str | None = None


class SharePanicResponse(PassthroughModel):
    sessions_booted: int | None = None


class InviteMutationAck(PassthroughModel):
    """``{ok: true}`` acks that also report booted session count."""

    ok: bool | None = None
    booted_sessions: int | None = None


class ClaimTokenResponse(PassthroughModel):
    token: str | None = None


class BackupImportResponse(PassthroughModel):
    inserted: int | None = None
    skipped: int | None = None
    merged: int | None = None


class GdprEraseResponse(PassthroughModel):
    deleted_invites: int | None = None
    redacted_log_rows: int | None = None
    retained_recent_rows: int | None = None


class ShareSettingsResponse(PassthroughModel):
    max_concurrent_analyst_sessions: int | None = None


class WordphraseResponse(PassthroughModel):
    passcode: str | None = None


class ShareInvitesResponse(PassthroughModel):
    invites: list[InviteRecord] | None = None


class PasscodeRegenerateResponse(PassthroughModel):
    ok: bool | None = None
    invite_id: str | None = None
    passcode: str | None = None


class ShareTosPayload(BaseModel):
    """Body for ``POST /api/admin/share/tos`` — publish a new Terms of Service version."""

    version: str
    text: str


class ShareTosResponse(PassthroughModel):
    ok: bool | None = None
    version: str | None = None
    text: str | None = None
    published_at: str | None = None


class SharingDeployRequest(BaseModel):
    service_name: str
    domain_name: str
    origin_host: str
    origin_port: int = 80
    use_ssl: bool = False
    token_override: str | None = None
    override_host: str | None = None
    service_id: str | None = None


class SharingDeployResponse(BaseModel):
    service_id: str
    version: int
    domain_name: str
    origin_host: str


class SharingTeardownRequest(BaseModel):
    service_id: str
    token_override: str | None = None


_self_mod = sys.modules[__name__]
sys.modules["backend.models.high_scale"] = _self_mod
sys.modules["backend.models.admin_status"] = _self_mod
sys.modules["backend.models.share_admin"] = _self_mod
_models_pkg = sys.modules.get("backend.models")
if _models_pkg is not None:
    setattr(_models_pkg, "high_scale", _self_mod)
    setattr(_models_pkg, "admin_status", _self_mod)
    setattr(_models_pkg, "share_admin", _self_mod)
