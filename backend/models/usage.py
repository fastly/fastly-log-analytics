from __future__ import annotations

from pydantic import BaseModel

from backend.models.common import BaseResponse


class PrefillResponse(BaseResponse):
    requests_per_day: int | None = None
    edge_requests_per_day: int | None = None
    avg_log_file_size_kb: float | None = None
    estimated_bytes_per_line: int | None = None
    data_days: int = 0
    log_period_seconds: int | None = None
    commit_interval_mins: int = 5
    sample_rate: int = 100
    edge_only: bool = False
    edge_ratio: float | None = None
    compaction_enabled: bool = True
    delete_after: bool = True
    log_retention_days: int = 90
    avg_nodes_per_flush: int | None = None
    class_a_rate_per_1k: float | None = None
    class_b_rate_per_10k: float | None = None
    cdn_egress_rate_per_gb: float | None = None
    storage_rate_per_gb_month: float | None = None
    min_billed_days: int | None = None


class CurrentStorageResponse(BaseResponse):
    live_bytes: int
    live_files: int
    deleted_bytes: int
    total_billed_bytes: int
    total_billed_gb_hours: float
    total_files: int
    total_bytes: int
    start: str
    end: str


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
