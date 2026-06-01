from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, RootModel

from backend.models.common import BaseResponse


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


class SyncStatus(BaseModel):
    configured: bool = True
    busy: bool = False
    storage_mode: str | None = None
    access_level: str | None = None
    local_rows: int | None = None
    earliest_log_at: str | None = None
    latest_log_at: str | None = None
    latest_ingested_file_at: str | None = None
    latest_available_file_at: str | None = None
    duckdb_size_bytes: int | None = None
    duckdb_exists: bool | None = None
    active_run: dict[str, Any] | None = None
    ngwaf_workspace_id: str | None = None


class SyncStatusResponse(BaseResponse, SyncStatus):
    pass


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
    id: int
    timestamp: str
    service_id: str | None = None
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
    fastly_logs: int
    our_rows: int
    file_count: int
    gap: int
    gap_pct: float


class LogAccountingTotals(BaseModel):
    fastly_logs: int
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
    fastly_field_used: str | None = None
    buckets: list[LogAccountingBucket]
    totals: LogAccountingTotals
    sustained_loss: SustainedLossAlert | None = None
    catchup: IngestCatchupStatus | None = None
