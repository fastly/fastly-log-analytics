"""High-Scale topology, ClickHouse status/replay, Celery/ingest ledger, and DuckDB/NGWAF/rDNS status models."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.models.admin.health_and_sync import MAX_PAGE_SIZE
from backend.models.common.base import BaseResponse, ErrorDetail, JsonAny, JsonDict, WireSafeModel

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
