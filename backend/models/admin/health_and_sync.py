"""Admin health snapshot, metric history, compaction stats, tree/POP/quarantine, sync status, and log accounting models."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_serializer

from backend.models.common.base import BaseResponse, LogExtentsMixin, WireSafeModel

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
