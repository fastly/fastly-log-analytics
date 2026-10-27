from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.models.analytics._common import (
    HistoryObservation,
    ReaderCoverage,
    TimeSeriesPoint,
    ValueTimeSeriesPoint,
    _aliased_optional_field,
    _OkWireSafeBase,
    _TotalRequestsMetricsBase,
)
from backend.models.analytics.security import SecurityTopBotsResponse
from backend.models.common import (
    BaseResponse,
    FilteredRequest,
    JsonAny,
    JsonDict,
    JsonRows,
    Limit500,
    LogExtentsMixin,
    PaginationMixin,
    PassthroughModel,
    RangeKeyedRequest,
    SchemaColumn,
    WireSafeModel,
)


class QueryAttribution(PassthroughModel):
    kind: str | None = None
    label: str | None = None
    principal_id: str | None = None
    caller_qualname: str | None = None
    caller_file: str | None = None
    request_path: str | None = None
    request_id: str | None = None
    cron_job: str | None = None
    cron_run_id: str | None = None
    pool_slot: str | None = None


class _QueryRowBase(PassthroughModel):
    query_id: int | None = None
    db_type: str | None = None
    sql_preview: str | None = None
    sql: str | None = None
    sql_len: int | None = None
    attribution: QueryAttribution | None = None
    service_id: str | None = None
    started_at_utc: float | None = None


class ActiveQueryRow(_QueryRowBase):
    duration_ms: float | None = None
    cancellable: bool | None = None
    cancelled_at: float | None = None


class CompletedQueryRow(_QueryRowBase):
    ended_at_utc: float | None = None
    duration_ms: float | None = None
    outcome: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    peak_memory_mb: float | None = None


class SnapshotResponse(BaseModel):
    last_seq: int
    active: list[ActiveQueryRow]
    completed: list[CompletedQueryRow]


class SummaryResponse(BaseModel):
    active_total: int
    by_db_type: dict[str, int]
    longest_ms: float


class CancelResponse(BaseModel):
    state: Literal["cancelled", "not_found", "already_finished", "connection_gone"]
    query_id: int


class SlowQueriesCountResponse(BaseModel):
    count: int
    since_hours: int
    threshold_ms: float


class SlowQueriesResponse(BaseModel):
    rows: list[CompletedQueryRow]
    since_hours: int
    threshold_ms: float


class SlowQueriesClearResponse(BaseModel):
    deleted: int
    service_id: str


class QueryMonitorConfigResponse(BaseModel):
    enabled: bool


class BotSourceRefreshResponse(_OkWireSafeBase):
    source: JsonDict


class BotSourcesRefreshResponse(_OkWireSafeBase):
    sources: JsonRows
    updated_count: int
    failed_count: int


class RdnsEnrichSummary(WireSafeModel):
    resolved: int
    errors: int
    discovered: int
    batch_limit: int


class RdnsEnrichResponse(_OkWireSafeBase):
    summary: RdnsEnrichSummary


class DuckdbRecycleResponse(_OkWireSafeBase):
    status: str
    reason: str | None = None
    rss_mb: float | None = None
    threshold_mb: float | None = None
    detail: str | None = None
    rss_before_mb: float | None = None
    rss_after_mb: float | None = None
    freed_mb: float | None = None


class NgwafSyncResponse(_OkWireSafeBase):
    service_id: str
    workspace_id: str
    stats: JsonDict


class SharePurgeResponse(_OkWireSafeBase):
    deleted_audit_logs: int | None = None
    deleted_expired_invites: int | None = None
    deleted_stale_sessions: int | None = None
    deleted_claim_tokens: int | None = None
    audit_retention_days: int | None = None
    max_idle_session_days: int | None = None


class StorageCalendarDay(WireSafeModel):
    data_files: int
    size_bytes: int


class StorageCalendarResponse(WireSafeModel):
    __pydantic_extra__: dict[str, StorageCalendarDay | list]  # type: ignore[assignment]


class VclHealthResponse(WireSafeModel):
    service_id: str
    is_clean: bool
    error: str | None = None
    active_version: int | str | None = None
    legacy_snippets_found: int | None = None
    legacy_names: list[str] | None = None
    consolidated_snippets_found: int | None = None
    consolidated_names: list[str] | None = None
    recommendation: str | None = None


class PageTelemetryQuery(WireSafeModel):
    engine: str | None = None
    sql_query: str | None = None
    duration_ms: float | None = None


class PageTelemetrySection(WireSafeModel):
    section_name: str | None = None
    duration_ms: float | None = None


class PageTelemetryCall(WireSafeModel):
    service: str
    method: str | None = None
    path: str | None = None
    status: str | int | None = None
    time_ms: float | None = None
    caller: str | None = None
    bytes: int | None = None


class PageTelemetryResponse(WireSafeModel):
    queries: list[PageTelemetryQuery]
    sections: list[PageTelemetrySection]
    calls: list[PageTelemetryCall]


class HealthServiceState(WireSafeModel):
    service_id: str
    status: str
    last_ingest: JsonAny = None
    last_sync_status: str | None = None
    last_sync_at: JsonAny = None
    reason: str | None = None
    stale_minutes_used: int | None = None


class HealthResponse(WireSafeModel):
    status: str
    version: str
    services: list[HealthServiceState] | None = None


ValueSectionName = Literal["overview", "caching", "security", "bots", "performance", "network", "io"]


class ValueSummaryRequest(RangeKeyedRequest):
    sections: list[ValueSectionName] | None = None
    chart_interval: str = "1 day"


class OverviewMetrics(_TotalRequestsMetricsBase):
    origin_offload_pct: float | None = None
    threats_blocked: int | None = None
    cache_acceleration_factor: float | None = None
    total_bandwidth_bytes: int | None = None
    edge_hit_requests: int | None = None
    origin_requests: int | None = None


class CachingMetrics(_TotalRequestsMetricsBase):
    origin_offload_pct: float | None = None
    bandwidth_saved_bytes: int | None = None
    shield_effectiveness_pct: float | None = None
    hit_requests: int | None = None
    miss_requests: int | None = None
    pass_requests: int | None = None
    synth_requests: int | None = None
    offload_time_series: list[ValueTimeSeriesPoint] = []
    cache_state_time_series: list[ValueTimeSeriesPoint] = []


class WafSignalRow(WireSafeModel):
    signal: str | None = None
    count: int


class TopBotRow(WireSafeModel):
    name: str | None = None
    count: int


class IOOpportunityRow(WireSafeModel):
    url: str | None = None
    request_count: int
    total_bytes: int
    avg_kb: float | None = None


class IOFormatConversionPair(WireSafeModel):
    input_format: str | None = None
    output_format: str | None = None
    count: int
    avg_ratio: float | None = None


class SecurityMetrics(_TotalRequestsMetricsBase):
    waf_blocked: int | None = None
    waf_logged: int | None = None
    waf_passed: int | None = None
    threat_time_series: list[ValueTimeSeriesPoint] = []
    top_waf_signals: list[WafSignalRow] = []


class BotMetrics(_TotalRequestsMetricsBase):
    bot_requests: int | None = None
    verified_bots: int | None = None
    top_bots: list[TopBotRow] = []


class PerformanceMetrics(_TotalRequestsMetricsBase):
    cache_accel_factor: float | None = None
    avg_hit_latency_ms: float | None = None
    avg_miss_latency_ms: float | None = None
    p99_latency_ms: float | None = None
    latency_time_series: list[ValueTimeSeriesPoint] = []


class NetworkMetrics(_TotalRequestsMetricsBase):
    http3_pct: float | None = None
    tls_pct: float | None = None
    ipv6_pct: float | None = None
    h2_pct: float | None = None
    protocol_time_series: list[ValueTimeSeriesPoint] = []


class IOFormatBreakdown(BaseModel):
    format: str
    count: int
    pct: float


class IOMetrics(_TotalRequestsMetricsBase):
    io_transforms: int | None = None
    io_bandwidth_bytes: int | None = None
    io_shield_bandwidth_bytes: int | None = None
    io_time_series: list[ValueTimeSeriesPoint] = []
    io_pct_of_traffic: float | None = None
    io_estimated_cost_usd: float | None = None
    format_distribution: list[IOFormatBreakdown] = []
    modern_format_pct: float | None = None
    io_bandwidth_time_series: list[ValueTimeSeriesPoint] = []
    optimization_opportunities: list[IOOpportunityRow] = []
    io_actual_bandwidth_saved_bytes: int | None = None
    io_actual_compression_ratio: float | None = None
    io_format_conversion_pairs: list[IOFormatConversionPair] = []
    io_compression_time_series: list[ValueTimeSeriesPoint] = []
    image_request_count: int | None = None
    image_bandwidth_bytes: int | None = None
    estimated_savings_bytes: int | None = None


class ValueSummaryResponse(BaseResponse):
    overview: OverviewMetrics | None = None
    caching: CachingMetrics | None = None
    security: SecurityMetrics | None = None
    bots: BotMetrics | None = None
    performance: PerformanceMetrics | None = None
    network: NetworkMetrics | None = None
    io: IOMetrics | None = None


ChartMetric = Literal[
    "requests", "5xx", "4xx", "hit_rate", "p50_latency", "p95_latency", "p99_latency", "throughput", "req_size", "ttfb"
]
DashboardSectionName = Literal["core", "topten", "bots"]


class AggregatesRequest(RangeKeyedRequest):
    chart_interval: str = "1 minute"
    chart_metric: ChartMetric = "requests"
    fields: list[str] | None = None
    include_time_series: bool | None = None
    include_conn_requests: bool | None = None
    include_map_data: bool | None = None
    sections: list[DashboardSectionName] | None = None


class FieldTopEntry(BaseModel):
    value: Any
    count: int
    label: str | None = None


class FieldAggregate(BaseModel):
    top: list[FieldTopEntry]
    total: int


class MapPoint(BaseModel):
    country: str
    count: int


class AggregatesResponse(BaseResponse, LogExtentsMixin):
    history: HistoryObservation | None = _aliased_optional_field("history")
    reader_coverage: list[ReaderCoverage] | None = _aliased_optional_field("reader_coverage")
    data: dict[str, FieldAggregate]
    time_series: list[TimeSeriesPoint]
    map_data: list[MapPoint]
    where_clause: str
    interval: str
    metric: str
    total_rows: int
    total_rows_total: int


class BundleResponse(BaseResponse):
    aggregates: AggregatesResponse | None = None
    top_bots: SecurityTopBotsResponse | None = None


class RawRequest(FilteredRequest, PaginationMixin):
    limit: Limit500 = 50
    sort_col: str | None = None
    columns: list[str] = []


class FieldValuesRequest(FilteredRequest):
    field: str
    search: str = ""
    limit: Limit500 = 100


class FieldValuesResponse(BaseResponse):
    values: list[FieldTopEntry]
    field: str


QueryDataset = Literal["logs", "client_vitals", "client_errors"]


class QueryRequest(BaseModel):
    sql: str
    dataset: QueryDataset = "logs"
    max_rows: int = Field(default=500, ge=1, le=10_000)
    explain: bool = False


class QueryResponse(WireSafeModel):
    columns: list[str]
    data: JsonRows
    row_count: int
    total_rows: int
    truncated: bool
    elapsed_ms: int
    explain_plan: str | None = None


class PresetQuery(WireSafeModel):
    name: str
    description: str
    sql: str


class QuerySchemaResponse(BaseResponse):
    model_config = ConfigDict(populate_by_name=True)

    source: str | None = None
    table_name: str = "logs"
    service_id: str | None = None
    schema_: list[SchemaColumn] = Field(default_factory=list, alias="schema", serialization_alias="schema")
    columns: list[SchemaColumn] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _sync_schema_and_columns(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        raw_cols = data.get("schema") if "schema" in data else data.get("columns", [])
        cols = [
            {
                **c,
                "column": c.get("column") or c.get("name") or "",
                "name": c.get("name") or c.get("column") or "",
                "type": c.get("type", ""),
                **{k: c.get(k) for k in ("min", "max", "approx_unique", "null_percentage", "count")},
            }
            if isinstance(c, dict)
            else c
            for c in (raw_cols or [])
        ]
        return {**data, "schema": cols, "columns": data.get("columns") if "columns" in data else cols}
