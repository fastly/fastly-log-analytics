from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel

from backend.models.common import BaseResponse, LogExtentsMixin

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


class HealthPoolWaitStat(BaseModel):
    """One per-pool snapshot from the DuckDB-pool wait sampler.

    Shape is open — different pool types report slightly different
    fields (the Phase 6 sampler returns a dict-of-dicts keyed by pool
    name). The Any escape hatch matches what callers read.
    """

    name: str
    samples: int = 0
    wait_p95_ms: float | None = None
    wait_p99_ms: float | None = None
    extras: dict[str, Any] = {}


class HealthCronFailure(BaseModel):
    """One service+task whose most recent terminal cron run ended in error.

    Powers the SRE-03 ``recent_cron_failures`` glance surface: a non-``sync``
    per-service cron (commit / optimize / rollup_compact / metadata_sync / …)
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
    # SRE-20: effective log/exporter mode so an incident grep is informed.
    observability: HealthObservability | None = None
    # SRE-11: freshness of the off-VM service-config backup (None = no marker).
    config_backup: HealthConfigBackup | None = None
    # SRE-13: per-service FOS reachability — only populated when probe_fos=1.
    fos: dict[str, HealthFosProbe] | None = None


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


class QuarantinedFile(BaseModel):
    id: int
    file_name: str
    error_key: str
    valid_rows: int = 0
    corrupt_rows: int = 0
    file_size_bytes: int | None = None
    corrupt_samples: list[str] = Field(default_factory=list)
    reason_counts: dict[str, int] = Field(default_factory=dict)
    quarantined_at: str


class QuarantineSummary(BaseModel):
    total_files: int = 0
    total_corrupt_rows: int = 0
    oldest_at: str | None = None
    newest_at: str | None = None


class QuarantineListResponse(BaseResponse):
    files: list[QuarantinedFile] = Field(default_factory=list)
    total: int = 0
    summary: QuarantineSummary = Field(default_factory=QuarantineSummary)


class SyncStatus(LogExtentsMixin):
    configured: bool = True
    busy: bool = False
    storage_mode: str | None = None
    access_level: str | None = None
    local_rows: int | None = None
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


class IcebergTableInfo(BaseModel):
    table_name: str
    table_location: str | None = None
    snapshots: int
    data_files: int
    size_bytes: int
    latest_snapshot_at: str | None = None
    buffer_files: int = 0
    buffer_size_bytes: int = 0
    region: str | None = None


class IcebergTableInfoResponse(BaseResponse, IcebergTableInfo):
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


class ProvisionServicesResponse(RootModel[list[ProvisionService]]):
    pass


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


class UsageLoggingUpdateBody(BaseModel):
    """Body for ``PATCH /api/admin/usage-logging``.

    All fields are optional — only keys explicitly present in the request
    are applied (preserves the existing partial-update semantics). Numeric
    fields are validated for positivity at the handler so the existing
    error envelope stays intact; the model just pins types for OpenAPI."""

    enabled: bool | None = None
    retention_days: float | None = None
    class_a_rate_per_1k: float | None = None
    class_b_rate_per_10k: float | None = None
    cdn_egress_rate_per_gb: float | None = None
    storage_rate_per_gb_month: float | None = None
    min_billed_days: float | None = None


# ── Wire-safe admin maintenance/config responses ───────────────────────────
#
# Same contract as the scoring read models (backend/models/session_scoring.py):
# ``extra="allow"`` so undeclared/future producer keys pass through verbatim,
# all-Optional fields so validation can never 500, and
# ``response_model_exclude_unset=True`` at the decorator so branch-dependent
# key sets (e.g. optimize-now's error branch) stay byte-identical on the wire.
# Field lists derive from the producers, not from any frontend consumer.


class _AdminMaintRead(BaseModel):
    """Base for the maintenance read responses — passes undeclared keys
    through instead of stripping them."""

    model_config = ConfigDict(extra="allow")


class UsageLoggingConfigResponse(_AdminMaintRead):
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
    min_billed_days: int | float | None = None
    track_duckdb_httpfs: bool | None = None


class OptimizeNowResponse(_AdminMaintRead):
    """``optimize_table`` result — success carries the rewrite counters,
    the error branches carry ``error`` (+ ``files_rewritten: 0``)."""

    files_rewritten: int | None = None
    files_added: int | None = None
    eligible_partitions: int | None = None
    partition_errors: list[str] | None = None
    error: str | None = None


class BackfillBundleRollupsResponse(_AdminMaintRead):
    """Per-kind bundle counts from ``POST /admin/backfill-bundle-rollups``."""

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
    perf_ttl_dist: int | None = None
    perf_ttl_dist_days: int | None = None
    ngwaf_bots: int | None = None
    ngwaf_bots_days: int | None = None
    wellknown_bots: int | None = None
    overview: int | None = None
    overview_days: int | None = None


class LocalCompactNowResponse(_AdminMaintRead):
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


class MetadataRetentionValues(_AdminMaintRead):
    """Resolved retention days (defaults merged with per-service cfg)."""

    usage_log_days: int | None = None
    ingested_files_days: int | None = None
    cron_runs_days: int | None = None


class MetadataRetentionResponse(_AdminMaintRead):
    retention: MetadataRetentionValues | None = None


class MetadataTableStat(_AdminMaintRead):
    """Per-table row count + estimated bytes (``None`` when SQLite lacks
    the ``dbstat`` vtable)."""

    rows: int | None = None
    bytes: int | None = None


class MetadataStorageResponse(_AdminMaintRead):
    """``GET /admin/metadata-storage`` — feeds the Metadata Storage card."""

    tables: dict[str, MetadataTableStat] | None = None
    db_bytes: int | None = None
    db_path: str | None = None
    retention: MetadataRetentionValues | None = None
    ingested_files_locked: bool | None = None


class DebugSettingsResponse(_AdminMaintRead):
    query_debug_visibility: str
    api_call_debug_visibility: str


class DebugSettingsUpdateBody(BaseModel):
    query_debug_visibility: str | None = None
    api_call_debug_visibility: str | None = None
