from __future__ import annotations

import sys

sys.modules["backend.models.insights"] = sys.modules[__name__]
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_serializer, model_validator

from backend.models._base import JsonAny, JsonDict, JsonRows, PassthroughModel, ScalarCell, SchemaColumn, WireSafeModel


def _aliased_optional_field(name: str) -> Any:
    return Field(
        default=None,
        validation_alias=AliasChoices(name, f"_{name}"),
        serialization_alias=f"_{name}",
        exclude_if=lambda value: value is None,
    )


def _move_fields(cls: type[BaseModel], *names: str, after: str | None = None) -> None:
    keys = list(cls.model_fields)
    tail = [k for k in keys[keys.index(after) + 1 :] if k not in names] if after in keys else []
    for k in (*names, *tail):
        if k in cls.model_fields:
            cls.model_fields[k] = cls.model_fields.pop(k)
    cls.model_rebuild(force=True)


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


class _OkWireSafeBase(WireSafeModel):
    ok: bool


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


class IcebergCalendarDay(WireSafeModel):
    data_files: int
    size_bytes: int


class IcebergCalendarResponse(WireSafeModel):
    __pydantic_extra__: dict[str, IcebergCalendarDay | list]  # type: ignore[assignment]


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


_self_mod = sys.modules[__name__]
_models_pkg = sys.modules.get("backend.models")
for _submod in (
    "admin_actions",
    "origin",
    "network",
    "performance",
    "rum",
    "session_scoring",
    "dashboard",
    "security",
    "sessions",
):
    sys.modules[f"backend.models.{_submod}"] = _self_mod
    if _models_pkg is not None:
        setattr(_models_pkg, _submod, _self_mod)
del _submod
_backend_pkg = sys.modules.get("backend")
_aliases = getattr(_backend_pkg, "_MODULE_ALIASES", None)
if isinstance(_aliases, dict):
    _aliases.update(
        {
            "backend.models.session_scoring": "backend.models.analytics",
            "backend.models.dashboard": "backend.models.analytics",
            "backend.models.security": "backend.models.analytics",
            "backend.models.sessions": "backend.models.analytics",
            "backend.repositories._sql.rum": "backend.repositories.rum",
            "backend.repositories._sql.security": "backend.repositories.security",
            "backend.repositories._sql.alerts": "backend.repositories.assets",
            "backend.repositories._sql.query": "backend.repositories.dashboard",
            "backend.repositories.utils.filters": "backend.repositories._base",
            "backend.repositories.query": "backend.repositories.dashboard",
            "backend.repositories.alerts": "backend.repositories.assets",
            "backend.repositories.audit": "backend.repositories.assets",
            "backend.routers.alerts": "backend.routers.control_room",
            "backend.routers.audit": "backend.routers.control_room",
        }
    )

from backend.models.common import (  # noqa: E402
    BaseResponse,
    FilteredRequest,
    HasDataMixin,
    Limit100,
    Limit200,
    Limit500,
    Limit1440,
    LimitedRequest,
    LogExtentsMixin,
    PaginationMixin,
    RangeKeyedRequest,
    ResponseCacheObservation,
    Seconds14400,
    SectionTimingEntry,
)


class NetworkWorstEntry(BaseModel):
    label: str
    score: float | None = None


class ShieldingRow(BaseModel):
    edge_pop: str | None = None
    shield_pop: str | None = None
    requests: int | None = None
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    distance_km: float | None = None
    light_speed_rtt_ms: float | None = None
    efficiency_ratio: float | None = None
    low_sample: bool = False
    anomaly_eligible: bool = False
    anomaly_static: bool = False
    edge_lat: float | None = None
    edge_lon: float | None = None
    shield_lat: float | None = None
    shield_lon: float | None = None


class ShieldingAnalysis(PassthroughModel):
    response_cache: ResponseCacheObservation | None = _aliased_optional_field("response_cache")
    has_data: bool = False
    rows: list[ShieldingRow] = []
    edge_only: bool | None = None
    requires_fields: list[str] | None = None
    total_routes: int | None = None
    truncated: bool | None = None
    error: bool | None = None

    @model_serializer(mode="wrap")
    def _drop_none_discriminators(self, handler):
        data = handler(self)
        for key in ("edge_only", "requires_fields", "total_routes", "truncated", "error"):
            if data.get(key) is None:
                data.pop(key, None)
        return data


class _NetworkBucketBase(WireSafeModel):
    bucket_idx: int
    bucket: str


class NetworkHeatmapCell(_NetworkBucketBase):
    throughput_bps: float | None = None
    rtt_med_us: float | None = None
    rtt_baseline_us: float | None = None
    rtt_congestion_us: float | None = None
    avg_ploss: float | None = None
    rtt_jitter_us: float | None = None
    error_pct: float | None = None
    health_score: float
    reqs: int


class _NetworkAsnLabelBase(WireSafeModel):
    asn: int
    label: str


class NetworkHeatmapRow(_NetworkAsnLabelBase):
    total_reqs: int
    buckets: list[NetworkHeatmapCell] = []


class NetworkMapCity(WireSafeModel):
    country: str | None = None
    city_idx: int
    metro_code: int | None = None
    rtt_med_us: float | None = None
    avg_ploss: float | None = None
    error_pct: float | None = None
    health_score: float
    reqs: int


class NetworkMapBucket(_NetworkBucketBase):
    cities: list[NetworkMapCity] = []


class NetworkLeaderboardRow(_NetworkAsnLabelBase):
    health_score_now: float | None = None
    health_score_1h_ago: float | None = None
    health_score_1w_ago: float | None = None
    trend: Literal["stable", "degrading", "improving"]
    total_reqs: int
    c_speed_mix: dict[str, float] = {}
    p95_rtt_us: float | None = None
    p99_rtt_us: float | None = None


class NetworkMetroRow(WireSafeModel):
    country: str | None = None
    region: str
    raw_city: str
    city: str
    total_reqs: int
    health_score: float | None = None


class NetworkQualityBarRow(WireSafeModel):
    value: str
    label: str
    rtt_ms: float
    reqs: int


class NetworkQualityScatterPoint(WireSafeModel):
    rtt_ms: float
    ttfb_ms: float
    cache: str


class NetworkCityEntry(BaseModel):
    name: str
    lat: float | None = None
    lon: float | None = None


class NetworkHealthSummary(BaseModel):
    global_health_score: float | int
    avg_rtt_ms: float | int
    total_reqs: int
    worst_asn: NetworkWorstEntry | None = None
    worst_country: NetworkWorstEntry | None = None


class NetworkHealthResponse(BaseResponse):
    has_data: bool = True
    reason: str | None = None
    available: bool
    metric: str | None = None
    bucket_seconds: int | None = None
    buckets: list[str] = []
    heatmap: list[NetworkHeatmapRow] = []
    map_buckets: list[NetworkMapBucket] = []
    cities: list[NetworkCityEntry] = []
    leaderboard: list[NetworkLeaderboardRow] = []
    metro_leaderboard: list[NetworkMetroRow] = []
    summary: NetworkHealthSummary | None = None
    countries: list[str] = []
    _approx: bool = False
    has_metro: bool = False
    shielding_analysis: ShieldingAnalysis | None = None


class NetworkQualityResponse(BaseResponse):
    available: bool
    by_country: list[NetworkQualityBarRow] = []
    by_asn: list[NetworkQualityBarRow] = []
    by_region: list[NetworkQualityBarRow] = []
    region_country: str | None = None
    by_pop: list[NetworkQualityBarRow] = []
    scatter: list[NetworkQualityScatterPoint] = []
    countries: list[str] = []
    _approx: bool = False


class PopHealthItem(BaseModel):
    pop: str
    requests: int
    errors: int
    error_rate: float
    p50_rtt_us: float | None = None
    p95_ttfb_ms: float | None = None
    cache_hit_rate: float
    bandwidth_bytes: int


class PopHealthListResponse(BaseResponse):
    data: list[PopHealthItem]


class TransportProtocolShare(BaseModel):
    protocol: str
    requests: int = 0
    share_pct: float = 0.0


class TransportMetrics(BaseModel):
    section_timings: list[SectionTimingEntry] | None = Field(
        default=None, serialization_alias="_section_timings", exclude_if=lambda value: value is None
    )
    response_cache: ResponseCacheObservation | None = _aliased_optional_field("response_cache")
    has_transport_data: bool = True
    tcp_rtt_p50_ms: float | None = None
    tcp_rtt_p90_ms: float | None = None
    tcp_rtt_p95_ms: float | None = None
    tcp_rtt_p99_ms: float | None = None
    retrans_rate_pct: float | None = None
    packet_loss_pct: float | None = None
    delivery_rate_mbps: float | None = None
    bandwidth_bytes: int = 0
    total_requests: int = 0
    protocol_distribution: list[TransportProtocolShare] = []
    missing_fields: list[str] = []


NetworkSectionName = Literal[
    "health",
    "summary",
    "heatmap",
    "buckets",
    "leaderboard",
    "metro_leaderboard",
    "cities",
    "map_buckets",
    "shielding_analysis",
    "quality",
    "pop_health",
    "transport",
]


class NetworkHealthRequest(RangeKeyedRequest):
    metric: str = "health_score"
    bucket_seconds: Seconds14400 = 300
    top_n: Limit100 = 30
    map_asn: str = "all"
    sections: list[NetworkSectionName] | None = None


class NetworkQualityRequest(RangeKeyedRequest):
    region_country: str = "US"


class NetworkBundleRequest(NetworkHealthRequest):
    region_country: str = "US"


class NetworkBundleResponse(BaseResponse):
    component_observations: dict[str, BaseResponse] | None = Field(
        default=None, serialization_alias="_component_observations"
    )
    available: bool = True
    reason: str | None = None
    health: NetworkHealthResponse | None = None
    quality: NetworkQualityResponse | None = None
    pop_health: list[PopHealthItem] | None = None
    transport: TransportMetrics | None = None


class _OriginLatencyTailMixin(WireSafeModel):
    p50_ms: float | None = None
    p95_ms: float | None = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        _move_fields(cls, "p50_ms", "p95_ms", "p99_ms", after="requests")


class _OriginLatencyP99TailMixin(_OriginLatencyTailMixin):
    p99_ms: float | None = None


class _UrlRequestsWireBase(WireSafeModel):
    url: str
    requests: int


class OriginSlowUrlRow(_UrlRequestsWireBase, _OriginLatencyP99TailMixin):
    pass


class OriginStatusCodeRow(WireSafeModel):
    status: int
    count: int
    pct: float | None = None


class OriginPathRow(_OriginLatencyTailMixin):
    edge: bool | None = None
    requests: int


class OriginPopLatencyRow(_OriginLatencyTailMixin):
    pop: str
    requests: int
    elevated: bool


class OriginIpHealthRow(_OriginLatencyTailMixin):
    oip: str
    requests: int
    error_pct: float | None = None


class OriginDrilldownRow(_OriginLatencyP99TailMixin):
    key: str
    requests: int
    error_rate: float
    bytes: int | None = None


class OriginTimeseriesPoint(WireSafeModel):
    time: str
    miss_count: int
    value: float | None = None
    edge: bool | None = None


class OriginDrilldownResponse(HasDataMixin, BaseResponse):
    dimension: str = "host"
    rows: list[OriginDrilldownRow] = []


class _HasDataOnlyMixin(BaseModel):
    has_data: bool = False


class _OriginSummaryMetricsMixin(BaseModel):
    total_misses: int | None = None
    total_passes: int | None = None
    ottfb_p50_ms: float | None = None
    ottfb_p75_ms: float | None = None
    ottfb_p95_ms: float | None = None
    ottfb_p99_ms: float | None = None
    ottlb_p50_ms: float | None = None
    ottlb_p95_ms: float | None = None
    cdn_overhead_p50_ms: float | None = None
    origin_error_rate: float | None = None
    obytes_p50: float | None = None


class OriginSummaryResponse(_OriginSummaryMetricsMixin, HasDataMixin, BaseResponse):
    pass


class _OriginTimeseriesMixin(BaseModel):
    series: list[OriginTimeseriesPoint] = []


class _OriginSlowUrlsMixin(BaseModel):
    rows: list[OriginSlowUrlRow] = []


class _OriginStatusCodesMixin(BaseModel):
    rows: list[OriginStatusCodeRow] = []


class _OriginPathBreakdownMixin(BaseModel):
    shielding_detected: bool = False
    rows: list[OriginPathRow] = []


class _OriginPopLatencyMixin(BaseModel):
    requires_group_c: bool = False
    median_p95_ms: float | int | None = None
    rows: list[OriginPopLatencyRow] = []


class _OriginIpHealthMixin(BaseModel):
    rows: list[OriginIpHealthRow] = []


class OriginTimeseriesResponse(_OriginTimeseriesMixin, HasDataMixin, BaseResponse):
    pass


class OriginSlowUrlsResponse(_OriginSlowUrlsMixin, HasDataMixin, BaseResponse):
    pass


class OriginStatusCodesResponse(_OriginStatusCodesMixin, HasDataMixin, BaseResponse):
    pass


class OriginPathBreakdownResponse(_OriginPathBreakdownMixin, HasDataMixin, BaseResponse):
    pass


class OriginPopLatencyResponse(_OriginPopLatencyMixin, HasDataMixin, BaseResponse):
    pass


class OriginIpHealthResponse(_OriginIpHealthMixin, HasDataMixin, BaseResponse):
    pass


class OriginShieldingAnalysisResponse(HasDataMixin, BaseResponse):
    requires_fields: list[str] = []
    edge_only: bool = False
    rows: list[ShieldingRow] = []
    total_routes: int | None = None
    truncated: bool | None = None
    error: bool = False


class _OriginSectionApproxMixin(BaseModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginSummarySection(_OriginSectionApproxMixin, _OriginSummaryMetricsMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginTimeseriesSection(_OriginSectionApproxMixin, _OriginTimeseriesMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginSlowUrlsSection(_OriginSectionApproxMixin, _OriginSlowUrlsMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginStatusCodesSection(_OriginSectionApproxMixin, _OriginStatusCodesMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginPathBreakdownSection(
    _OriginSectionApproxMixin, _OriginPathBreakdownMixin, _HasDataOnlyMixin, WireSafeModel
):
    pass


class OriginPopLatencySection(_OriginSectionApproxMixin, _OriginPopLatencyMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginIpHealthSection(_OriginSectionApproxMixin, _OriginIpHealthMixin, _HasDataOnlyMixin, WireSafeModel):
    pass


class OriginAggregatesResponse(HasDataMixin, BaseResponse):
    summary: OriginSummarySection = Field(default_factory=OriginSummarySection)
    timeseries: OriginTimeseriesSection = Field(default_factory=OriginTimeseriesSection)
    slow_urls: OriginSlowUrlsSection = Field(default_factory=OriginSlowUrlsSection)
    status_codes: OriginStatusCodesSection = Field(default_factory=OriginStatusCodesSection)
    path_breakdown: OriginPathBreakdownSection = Field(default_factory=OriginPathBreakdownSection)
    pop_latency: OriginPopLatencySection = Field(default_factory=OriginPopLatencySection)
    ip_health: OriginIpHealthSection = Field(default_factory=OriginIpHealthSection)


SectionName = Literal[
    "summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"
]


class OriginRequest(FilteredRequest):
    pass


class _OriginBucketLegMixin(BaseModel):
    bucket_minutes: Limit1440 = 5
    split_by_leg: bool = False


class OriginTimeseriesRequest(_OriginBucketLegMixin, FilteredRequest):
    metric: Literal["ttfb", "ttlb"] = "ttfb"
    percentile: Literal["p50", "p95", "p99"] = "p95"


class OriginSlowUrlsRequest(FilteredRequest):
    limit: Limit100 = 20
    min_requests: int = 10


class OriginPopLatencyRequest(LimitedRequest):
    pass


class OriginIpHealthRequest(LimitedRequest):
    pass


class OriginShieldingAnalysisRequest(FilteredRequest):
    limit: Limit200 = 50


class OriginDrilldownRequest(FilteredRequest):
    dimension: Literal["host", "shield_pop", "datacenter", "protocol", "pop", "oip"] = "host"
    limit: Limit100 = 30


class OriginAggregatesRequest(_OriginBucketLegMixin, RangeKeyedRequest):
    timeseries_metric: Literal["ttfb", "ttlb"] = "ttfb"
    timeseries_percentile: Literal["p50", "p95", "p99"] = "p95"
    slow_urls_limit: Limit100 = 20
    slow_urls_min_requests: int = 10
    ip_health_limit: Limit100 = 30
    pop_latency_limit: Limit100 = 30
    sections: list[SectionName] | None = None


PerformanceSectionName = Literal["waterfall", "top_urls", "top_asns", "ttl_dist", "scatter"]


class PerformanceRequest(RangeKeyedRequest):
    sort_by: Literal["avg", "p50", "p95", "p99"] = "p99"
    sections: list[PerformanceSectionName] | None = None


class _PerformanceLatencyTailMixin(WireSafeModel):
    avg: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        _move_fields(cls, "avg", "p50", "p95", "p99")


class PerformanceUrlRow(_UrlRequestsWireBase, _PerformanceLatencyTailMixin):
    pass


class PerformanceAsnRow(_PerformanceLatencyTailMixin):
    asn: int | str
    label: str | None = None
    requests: int


class PerformanceTtlBucket(WireSafeModel):
    bucket: str
    count: int


class PerformanceScatterPoint(WireSafeModel):
    origin: float | None = None
    edge: float | None = None
    cache: str | None = None


class PerformanceWaterfallAvg(WireSafeModel):
    edge_processing: float
    origin_wait: float
    origin_download: float
    client_download: float


class PerformanceWaterfall(WireSafeModel):
    avg: PerformanceWaterfallAvg | None = None


class PerformanceAggregatesResponse(BaseResponse):
    model_config = ConfigDict(populate_by_name=True)

    top_urls: list[PerformanceUrlRow] = []
    top_asns: list[PerformanceAsnRow] = []
    ttl_dist: list[PerformanceTtlBucket] = []
    scatter: list[PerformanceScatterPoint] = []
    waterfall: PerformanceWaterfall = Field(default_factory=PerformanceWaterfall)
    approx: bool = Field(default=False, alias="_approx")


class AssetTypeBreakdownRow(BaseModel):
    asset_type: str
    requests: int
    egress_bytes: int
    cache_hit_ratio: float
    compression_rate: float


class CachePerformanceRow(BaseModel):
    asset_type: str
    cache_status: str
    requests: int
    bytes: int


class CompressionPerformanceRow(BaseModel):
    asset_type: str
    content_encoding: str
    requests: int
    bytes: int


class _UrlRequestsBase(BaseModel):
    url: str
    requests: int


class LargeUncompressedAssetRow(_UrlRequestsBase):
    avg_bytes: float
    total_bytes: int
    status: int


class LowTtlAssetRow(_UrlRequestsBase):
    avg_ttl: float
    asset_type: str


class AssetsAggregatesResponse(BaseResponse):
    asset_type_breakdown: list[AssetTypeBreakdownRow] = []
    cache_performance: list[CachePerformanceRow] = []
    compression_performance: list[CompressionPerformanceRow] = []
    large_uncompressed_assets: list[LargeUncompressedAssetRow] = []
    low_ttl_assets: list[LowTtlAssetRow] = []
    missing_fields: list[str] = Field(default_factory=list)


class AssetsRequest(RangeKeyedRequest):
    pass


CmcdSectionName = Literal[
    "overview",
    "sessions_ts",
    "buffer_health_ts",
    "bitrate_ts",
    "throughput_ts",
    "top_content",
    "rebuffer_by_country",
    "rebuffer_by_asn",
    "object_type_dist",
    "streaming_format_dist",
    "startup_ts",
    "session_duration_dist",
]


class CmcdRequest(RangeKeyedRequest):
    bucket_seconds: Seconds14400 = 300
    top_n: Limit100 = 30
    sections: list[CmcdSectionName] | None = None


class CmcdOverview(WireSafeModel):
    active_sessions: int
    rebuffer_rate: float | None = None
    avg_bitrate: float | None = None
    avg_buffer_length: float | None = None
    median_throughput: float | None = None
    peak_viewers: int
    avg_session_duration: int | None = None


class _CmcdBucketPointBase(WireSafeModel):
    bucket: str


class CmcdSessionsPoint(_CmcdBucketPointBase):
    concurrent_sessions: int
    rebuffer_session_pct: float | None = None
    new_sessions: int


class CmcdBufferHealthPoint(_CmcdBucketPointBase):
    p50_buffer: float | None = None
    p95_buffer: float | None = None
    starvation_rate: float | None = None


class CmcdBitratePoint(_CmcdBucketPointBase):
    avg_bitrate: float | None = None
    utilization_ratio: float | None = None


class CmcdThroughputPoint(_CmcdBucketPointBase):
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


class CmcdStartupPoint(_CmcdBucketPointBase):
    startup_ratio: float | None = None


class _CmcdBitrateBufferMixin(WireSafeModel):
    rebuffer_rate: float | None = None
    avg_bitrate: float | None = None
    avg_buffer_length: float | None = None

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        extra = ("rebuffer_rate", "session_count") if "label" in cls.model_fields else ("rebuffer_rate",)
        _move_fields(cls, *extra, "avg_bitrate", "avg_buffer_length")


class CmcdTopContentRow(_CmcdBitrateBufferMixin):
    content_id: str | None = None
    session_count: int


class CmcdRebufferByCountryRow(WireSafeModel):
    country: str | None = None
    rebuffer_rate: float | None = None
    session_count: int
    median_throughput: float | None = None


class CmcdRebufferByAsnRow(_CmcdBitrateBufferMixin):
    asn: int | str
    label: str
    session_count: int


class CmcdObjectTypeRow(WireSafeModel):
    object_type: str | None = None
    request_count: int


class CmcdStreamingFormatRow(WireSafeModel):
    streaming_format: str | None = None
    session_count: int


class CmcdSessionDurationRow(WireSafeModel):
    duration_bucket: str
    session_count: int


class CmcdAggregatesResponse(BaseResponse):
    available: bool
    has_data: bool = True
    overview: CmcdOverview | None = None
    buffer_health_ts: list[CmcdBufferHealthPoint] = []
    bitrate_ts: list[CmcdBitratePoint] = []
    throughput_ts: list[CmcdThroughputPoint] = []
    top_content: list[CmcdTopContentRow] = []
    rebuffer_by_country: list[CmcdRebufferByCountryRow] = []
    rebuffer_by_asn: list[CmcdRebufferByAsnRow] = []
    object_type_dist: list[CmcdObjectTypeRow] = []
    streaming_format_dist: list[CmcdStreamingFormatRow] = []
    sessions_ts: list[CmcdSessionsPoint] = []
    startup_ts: list[CmcdStartupPoint] = []
    session_duration_dist: list[CmcdSessionDurationRow] = []


ValueSectionName = Literal["overview", "caching", "security", "bots", "performance", "network", "io"]


class ValueSummaryRequest(RangeKeyedRequest):
    sections: list[ValueSectionName] | None = None
    chart_interval: str = "1 day"


class ValueTimeSeriesPoint(BaseModel):
    time: str
    value: float
    category: str | None = None


class _TotalRequestsMetricsBase(BaseModel):
    total_requests: int | None = None


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


class RumStatusResponse(WireSafeModel):
    enabled: bool | None = None
    enabled_at: str | None = None
    deployed_vcl_sha: str | None = None
    current_vcl_sha: str | None = None
    vcl_drift: bool | None = None
    capture_vitals: bool | None = None
    capture_performance: bool | None = None
    capture_errors: bool | None = None
    capture_events: bool | None = None
    custom_condition: str | None = None


class RumSettingsUpdateResponse(_OkWireSafeBase):
    message: str | None = None
    skipped: bool | None = None


class RumBeaconHealthResponse(WireSafeModel):
    enabled: bool | None = None
    beacon_fire_rate: int | None = None
    recent_beacons: int | None = None
    last_beacon_time: str | None = None
    setup_complete: bool | None = None
    message: str | None = None


RumNumber = int | float


class RumCwvDistribution(WireSafeModel):
    good: int
    needs_improvement: int
    poor: int


class RumCwvVital(WireSafeModel):
    p75: RumNumber | None
    distribution: RumCwvDistribution | None


class RumFidVital(WireSafeModel):
    p75: RumNumber | None
    fcp: RumNumber | None
    ttfb: RumNumber | None


class RumVitals(WireSafeModel):
    lcp: RumCwvVital
    cls: RumCwvVital
    inp: RumCwvVital
    fid: RumFidVital


class RumWorstPage(WireSafeModel):
    path: str
    views: int
    avg_load_time: RumNumber
    lcp_p75: RumNumber | None
    cls_p75: RumNumber | None
    error_rate: RumNumber


class RumJsError(WireSafeModel):
    message: str | None
    file: str
    line: int
    col: int
    count: int


class RumTrends(WireSafeModel):
    timestamps: list[str]
    lcp: list[RumNumber | None]
    cls: list[RumNumber | None]
    error_rate: list[RumNumber | None]
    pageviews: list[int] | None = None
    interactions: list[int] | None = None
    errors: list[int] | None = None


class RumEnvironments(WireSafeModel):
    browsers: dict[str, int]
    os: dict[str, int]
    devices: dict[str, int]


class RumAnalyticsResponse(WireSafeModel):
    is_mock: bool | None = None
    no_data: bool | None = None
    beacon_count: int | None = None
    pageview_count: int | None = None
    interaction_count: int | None = None
    error_count: int | None = None
    message: str | None = None
    error: str | None = None
    vitals: RumVitals | None = None
    worst_pages: list[RumWorstPage] | None = None
    errors: list[RumJsError] | None = None
    trends: RumTrends | None = None
    environments: RumEnvironments | None = None


class RumLiveEventItem(WireSafeModel):
    time: str
    type: str
    path: str
    desc: str
    browser: str
    os: str
    raw_log: dict[str, Any] = {}


class ScoringThresholdBody(BaseModel):
    threshold: int | None = None


class ScoringL2EnforceBody(BaseModel):
    enabled: bool = False


class ScoringExcludeRegexBody(BaseModel):
    regex: str = ""


class ScoringEnforceStatusCodeBody(BaseModel):
    status_code: int | None = None


class ScoringTokenBody(BaseModel):
    token: str = ""


class ScoringLabelCreate(BaseModel):
    sid: str = ""
    label: str = ""
    notes: str = ""
    flagged_by: str = "admin"
    sample_ip: str = ""
    sample_ua: str = ""
    sample_url: str = ""


class ScoringLabelUpdate(BaseModel):
    label: str | None = None
    notes: str | None = None


class _ScoringEventFieldsBase(PassthroughModel):
    edge_cookie_compliance: str | None = None
    edge_score_reason: str | None = None
    ip: str | None = None
    ua: str | None = None
    url: str | None = None
    status: int | None = None


class ScoringTopFlaggedRow(PassthroughModel):
    timestamp: str
    edge_sid: str | None
    edge_score: int
    edge_score_l1: int | None = None
    edge_score_l2: int | None = None
    edge_score_reason: str | None = None
    edge_cookie_compliance: str | None = None
    ip: str | None = None
    country: str | None = None
    ua: str | None = None
    url: str | None = None
    status: int | None = None


class _ScoringSinceHoursBase(PassthroughModel):
    since_hours: int | None = None


class ScoringTopFlaggedResponse(_ScoringSinceHoursBase):
    rows: list[ScoringTopFlaggedRow] = []


class ScoringDistributionRow(PassthroughModel):
    hour: str
    bucket: str
    count: int


class ScoringScoreDistributionResponse(_ScoringSinceHoursBase):
    rows: list[ScoringDistributionRow] = []


class ScoringComplianceRow(PassthroughModel):
    hour: str
    compliance: str
    count: int


class ScoringComplianceBreakdownResponse(_ScoringSinceHoursBase):
    rows: list[ScoringComplianceRow] = []


class _ScoringLatencyMetricsBase(PassthroughModel):
    rtt_p50_us: int | None = None
    rtt_p95_us: int | None = None
    rtt_p99_us: int | None = None
    exec_p50_us: int | None = None
    exec_p95_us: int | None = None


class ScoringLatencyRow(_ScoringLatencyMetricsBase):
    hour: str | None = None
    scored_count: int | None = None
    fail_open_count: int | None = None
    total_count: int | None = None


class ScoringLatencyTimeseriesResponse(_ScoringSinceHoursBase):
    rows: list[ScoringLatencyRow] = []
    has_latency: bool = False
    granularity: str | None = None


class ScoringReasonCount(PassthroughModel):
    reason: str | None = None
    count: int | None = None


class ScoringLatencySnapshot(_ScoringLatencyMetricsBase):
    available: bool = False
    rtt_max_us: int | None = None


class ScoringMatrixStaleness(PassthroughModel):
    l2_evaluated: int | None = None
    l2_high_count: int | None = None
    l2_high_pct: float | None = None
    is_stale: bool | None = None
    threshold_pct: float | None = None


class ScoringL2EnforceState(PassthroughModel):
    available: bool
    enabled: bool
    l2_enabled_at: int | None
    days_since_optin: float | None
    ramp_progress: float
    fully_ramped: bool
    warmup_days_remaining: float | None
    scoring_enabled_at: int | None
    deployment_age_days: float | None
    ready: bool
    ramp_days: float
    readiness_days: float


class _ScoringGoodBadBase(PassthroughModel):
    n_good: int | None = None
    n_bad: int | None = None


class _ScoringClassSamplesBase(_ScoringGoodBadBase):
    min_per_class: int | None = None
    has_min_samples: bool | None = None


class _ScoringAucBase(_ScoringGoodBadBase):
    auc: float | None = None
    passed: bool | None = None
    threshold: float | None = None


class _ScoringEvalMetricsBase(_ScoringClassSamplesBase, _ScoringAucBase):
    pass


class ScoringPerReasonBucket(_ScoringEvalMetricsBase):
    reason: str | None = None


class ScoringPerReasonResponse(_ScoringGoodBadBase):
    buckets: list[ScoringPerReasonBucket] = []
    min_per_class: int | None = None
    known_reasons: list[str] | None = None
    has_min_samples_overall: bool | None = None


class ScoringHealthResponse(_ScoringSinceHoursBase):
    total_edge_rows: int | None = None
    scored_rows: int | None = None
    fire_rate_pct: float | None = None
    distinct_sids: int | None = None
    avg_score: float | None = None
    p50_score: float | None = None
    p95_score: float | None = None
    max_score: int | None = None
    scorer_errors: int | None = None
    fail_open_rate_pct: float | None = None
    top_reasons: list[ScoringReasonCount] = []
    fail_open_breakdown: list[ScoringReasonCount] = []
    latency: ScoringLatencySnapshot | None = None
    matrix_staleness: ScoringMatrixStaleness | None = None
    l2_enforce: ScoringL2EnforceState | None = None


class ScoringEvaluationResponse(_ScoringEvalMetricsBase):
    n_neutral: int | None = None
    matrix_version: str | None = None
    error: str | None = None
    n_reconstructed: int | None = None
    n_labels_total: int | None = None
    default_min_auc: float | None = None


class ScoringCurvePoint(PassthroughModel):
    threshold: int | None = None
    fpr: float | None = None
    tpr: float | None = None
    precision: float | None = None
    recall: float | None = None


class ScoringCurvesResponse(_ScoringClassSamplesBase):
    note: str | None = None
    n_labels_total: int | None = None
    auc: float | None = None
    average_precision: float | None = None
    roc: list[ScoringCurvePoint] | None = None
    pr: list[ScoringCurvePoint] | None = None


class ScoringThresholdBucket(PassthroughModel):
    total: int | None = None
    good: int | None = None
    bad: int | None = None
    unlabeled: int | None = None


class ScoringThresholdPreviewResponse(_ScoringSinceHoursBase):
    threshold: int | None = None
    total_scored_sessions: int | None = None
    flagged: ScoringThresholdBucket | None = None
    passed: ScoringThresholdBucket | None = None
    precision: float | None = None
    recall: float | None = None


class _ScoringCorePanelsBase(PassthroughModel):
    top_flagged: ScoringTopFlaggedResponse | None = None
    score_distribution: ScoringScoreDistributionResponse | None = None
    compliance_breakdown: ScoringComplianceBreakdownResponse | None = None
    health: ScoringHealthResponse | None = None
    evaluation: ScoringEvaluationResponse | None = None
    evaluation_per_reason: ScoringPerReasonResponse | None = None


class ScoringAnalyticsResponse(_ScoringCorePanelsBase):
    latency_timeseries: ScoringLatencyTimeseriesResponse | None = None


class ScoringLabelRow(WireSafeModel):
    id: str
    service_id: str | None = None
    sid: str
    label: Literal["good", "bad", "neutral"]
    notes: str | None = None
    flagged_by: str | None = None
    sample_ip: str | None = None
    sample_ua: str | None = None
    sample_url: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class ScoringLabelCounts(WireSafeModel):
    good: int
    bad: int
    neutral: int


class ScoringLabelsListResponse(WireSafeModel):
    labels: list[ScoringLabelRow]
    counts: ScoringLabelCounts


class ScoringRetrainRejected(PassthroughModel):
    too_few_events: int | None = None
    too_fast: int | None = None
    kept: int | None = None
    routes_seen: int | None = None


class ScoringAucAgainstLabels(_ScoringAucBase):
    pass


class ScoringRetrainResponse(PassthroughModel):
    ok: bool | None = None
    matrix_version: str | None = None
    since_days: int | None = None
    sessions_trained_on: int | None = None
    transitions: int | None = None
    vocab_size: int | None = None
    rejected: ScoringRetrainRejected | None = None
    auc_against_labels: ScoringAucAgainstLabels | None = None
    default_min_auc: float | None = None
    local_matrix_saved: bool | None = None
    fos_matrix_published: bool | None = None
    matrix_kv_written: bool | None = None
    deploy_hint: str | None = None


class ScoringSessionEvent(_ScoringEventFieldsBase):
    ts: str | None = None
    edge_score: int | None = None


class ScoringSessionEventsResponse(PassthroughModel):
    sid: str | None = None
    since_days: int | None = None
    event_count: int | None = None
    events: list[ScoringSessionEvent] = []


class ScoringEnforceThresholdResponse(PassthroughModel):
    threshold: int | None
    enforced: bool
    key: str


class _ScoringOkMessageBase(PassthroughModel):
    ok: bool | None = None
    message: str | None = None


class ScoringEnforceThresholdPutResponse(_ScoringOkMessageBase):
    threshold: int | None = None
    enforced: bool | None = None


class ScoringL2EnforcePutResponse(_ScoringOkMessageBase):
    enabled: bool | None = None
    l2_enabled_at: int | None = None


class ScoringExcludeRegexState(PassthroughModel):
    current: str
    is_default: bool
    default: str
    effective: str


class _ScoringVclPutResponseBase(_ScoringOkMessageBase):
    is_default: bool | None = None
    logging_service_active_version: int | None = None


class ScoringExcludeRegexPutResponse(_ScoringVclPutResponseBase):
    effective_regex: str | None = None
    lint_warnings: list[str] | None = None


class ScoringExcludeRegexValidateResponse(PassthroughModel):
    ok: bool | None = None
    lint_warnings: list[str] | None = None
    error: str | None = None
    reason: str | None = None


class ScoringEnforceStatusCodeState(PassthroughModel):
    current: int | None
    default: int
    effective: int
    min: int
    max: int
    is_default: bool


class ScoringEnforceStatusCodePutResponse(_ScoringVclPutResponseBase):
    effective_status_code: int | None = None


class ScoringMatrixVersion(PassthroughModel):
    version: str | None = None
    key: str | None = None
    size_bytes: int | None = None
    last_modified: str | None = None


class ScoringMatrixVersionsResponse(PassthroughModel):
    versions: list[ScoringMatrixVersion] = []
    current_version: str | None = None


class ScoringMatrixRestoreResponse(PassthroughModel):
    ok: bool | None = None
    restored_version: str | None = None
    restored_at: str | None = None
    matrix_kv_written: bool | None = None
    deploy_hint: str | None = None


class ScoringRotateKeyResponse(_ScoringOkMessageBase):
    rotated_at: str | None = None
    previous_key_grace: bool | None = None


class ScoringAuditRow(PassthroughModel):
    id: int | None = None
    timestamp: str | None = None
    action: str | None = None
    actor: str | None = None
    details: Any = None


class ScoringAuditListResponse(PassthroughModel):
    audit: list[ScoringAuditRow] = []
    limit: int | None = None


class ScoringThresholdState(PassthroughModel):
    threshold: int | None
    set_at: str | None
    enforced: bool


class ScoringStatusResponse(PassthroughModel):
    enabled: bool | None = None
    scoring_service_id: str | None = None
    scoring_service_name: str | None = None
    scoring_domain: str | None = None
    matrix_version: str | None = None
    enabled_at: str | None = None
    scoring_active_version: int | None = None
    scoring_activated_at: str | None = None
    scorer_drift: bool | None = None
    drift_detail: str | None = None
    drift_known: bool | None = None
    preflight_pragmas_ready: bool | None = None


class _ScoringConfigBase(PassthroughModel):
    status: ScoringStatusResponse | None = None
    exclude_regex: ScoringExcludeRegexState | None = None
    enforce_status_code: ScoringEnforceStatusCodeState | None = None


class ScoringDashboardResponse(_ScoringSinceHoursBase, _ScoringCorePanelsBase, _ScoringConfigBase):
    threshold: int | None = None
    curves: ScoringCurvesResponse | None = None
    threshold_preview: ScoringThresholdPreviewResponse | None = None
    config_threshold: ScoringThresholdState | None = None


class ScoringConfigCompositeResponse(_ScoringConfigBase):
    threshold: ScoringThresholdState | None = None


class ScoringLabelDeleteResponse(PassthroughModel):
    status: str | None = None
    id: str | None = None


class _RollupActiveClosedBase(BaseModel):
    active_hour_start: str | None = None
    closed_hours: int | None = Field(default=None, ge=0)


class _RollupCoverageFractionBase(_RollupActiveClosedBase):
    covered_hours: int | None = Field(default=None, ge=0)
    coverage_fraction: float | None = Field(default=None, ge=0, le=1)


class _RollupWindowCoverageBase(_RollupCoverageFractionBase):
    window_start: str | None = None
    window_end: str | None = None


class RollupPathCoverage(_RollupCoverageFractionBase):
    bundle: Literal["security_conn_reuse", "all_fields", "ngwaf_bots"]
    reason: Literal["unknown", "durable_unready", "no_bundled_root", "no_paths", "below_coverage_floor", "planned"] = (
        "unknown"
    )
    minimum_coverage_fraction: float = Field(default=0.5, ge=0, le=1)
    partial_start_skipped: bool | None = None
    day_files: int | None = Field(default=None, ge=0)
    hour_files: int | None = Field(default=None, ge=0)
    done_markers: int | None = Field(default=None, ge=0)


class HourlyRollupCoverage(_RollupWindowCoverageBase):
    reason: Literal[
        "unknown", "no_bundled_root", "durable_unready", "writer_behind", "no_paths", "old_gaps", "planned"
    ] = "unknown"  # noqa: E501
    bundle_hours: int | None = Field(default=None, ge=0)
    verified_empty_hours: int | None = Field(default=None, ge=0)
    missing_hours: int | None = Field(default=None, ge=0)
    heal_horizon_hours: int | None = Field(default=None, ge=0)


class ScalarRollupCoverage(_RollupWindowCoverageBase):
    reason: Literal["unknown", "trailing_unavailable", "boundary_changed", "planned"] = "unknown"
    prefix_hours: int = Field(default=0, ge=0)
    day_time_series_files: int = Field(default=0, ge=0)
    day_overview_files: int = Field(default=0, ge=0)
    trailing_start: str | None = None
    trailing: HourlyRollupCoverage | None = None


class TopNRollupCoverage(_RollupActiveClosedBase):
    coverage_scope: Literal["any_field_file_presence"] = "any_field_file_presence"
    any_field_covered_hours: int | None = Field(default=None, ge=0)
    field_complete_hours: int | None = Field(default=None, ge=0)
    bundled_day_files: int = Field(default=0, ge=0)
    bundled_hour_files: int = Field(default=0, ge=0)
    per_field_day_files: int = Field(default=0, ge=0)
    per_field_hour_files: int = Field(default=0, ge=0)
    heal_horizon_hours: int = Field(default=48, ge=0)
    heal_candidates: int | None = Field(default=None, ge=0)
    heal_missing_hours: int | None = Field(default=None, ge=0)
    compacted_hours_skipped: int | None = Field(default=None, ge=0)
    outside_heal_horizon_hours: int | None = Field(default=None, ge=0)
    rolled_read: Literal["not_attempted", "used", "failed"] = "not_attempted"


class HistoryObservation(BaseModel):
    extent_source: Literal["unknown", "status_cache", "live_query", "count_only_query", "unavailable"] = "unknown"
    extent_rows: int | None = Field(default=None, ge=0)
    earliest_event_at: str | None = None
    latest_event_at: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    raw_extent_reaches_window_start: bool | None = None
    filters_requested: bool | None = None
    window_rows: int | None = Field(default=None, ge=0)
    window_count_source: Literal["unknown", "rollup", "raw", "filtered_temp"] = "unknown"


class ReaderCoverage(BaseModel):
    reader: Literal[
        "dashboard.conn_requests_histogram",
        "dashboard.ngwaf_bots",
        "dashboard.count",
        "dashboard.time_series",
        "dashboard.top_n",
        "dashboard.ua_bots",
    ]
    minimum_window_hours: int | None = Field(default=None, ge=0)
    maximum_window_days: int | None = Field(default=366, ge=0)
    scalar_plan: ScalarRollupCoverage | None = None
    top_n_plan: TopNRollupCoverage | None = None
    closed_hour_heal: Literal["not_attempted", "planned", "used", "unavailable", "failed"] = "not_attempted"
    eligible: bool | None = None
    eligibility_reason: Literal[
        "unknown",
        "filtered",
        "bounds_missing",
        "top_asns_empty",
        "invalid_window",
        "window_too_short",
        "window_too_long",
        "eligible",
        "unsupported_metric",
        "unsupported_interval",
        "unsafe_fields",
        "unsafe_service",
        "durable_unready",
        "rollups_missing",
    ] = "unknown"
    window_start: str | None = None
    window_end: str | None = None
    attempts: list[RollupPathCoverage] = Field(default_factory=list, max_length=2)
    outcome: Literal["unknown", "ineligible", "no_coverage", "read_failed", "empty_result", "served"] = "unknown"
    live_top_up: Literal[
        "not_attempted",
        "outside_window",
        "missing_column",
        "unavailable",
        "empty",
        "used",
        "failed",
        "view_fallback",
        "unverified",
    ] = "not_attempted"
    live_start: str | None = None
    live_end: str | None = None


class ActiveClientItem(WireSafeModel):
    ip: str
    rtt_min_ms: float | None = None
    tcp_rtt_ms: float | None = None
    distance_km: float | None = None
    pop: str | None = None
    client_lat: float | None = None
    client_lon: float | None = None
    pop_lat: float | None = None
    pop_lon: float | None = None
    impossible_distance: bool = False
    risk_level: str
    country: str | None = None
    city: str | None = None
    asn_name: str | None = None


class SecurityBotRow(WireSafeModel):
    id: str | None = None
    name: str
    category: str | None = None
    request_count: int
    verified_count: int | None = None
    impersonator_count: int | None = None
    unverified_count: int | None = None
    verification_coverage: float | None = None
    pending_count: int | None = None


class WellknownBotRow(WireSafeModel):
    id: str
    name: str
    category: str
    request_count: int
    verified_count: int
    impersonator_count: int
    unverified_count: int
    pending_count: int
    verification_coverage: float


class BotEfficiencyRow(WireSafeModel):
    bot_name: str
    category: str
    request_count: int
    resp_bytes: int = 0
    bandwidth_pct: float = 0.0
    referrals: int = 0
    requests_per_referral: float = 0.0
    bytes_per_referral: float = 0.0


class TlsFingerprintRow(WireSafeModel):
    fingerprint: str | None = None
    ip_count: int | None = None
    request_count: int | None = None
    count: int | None = None
    ips: int | None = None


class BucketCountRow(WireSafeModel):
    bucket: str | None = None
    label: str | None = None
    count: int


class TopIpHeaderRow(WireSafeModel):
    ip: str | None = None
    max_header: int | None = None


class Ipv6AdoptionRow(WireSafeModel):
    time: str | None = None
    pct: float | None = None
    version: str | None = None
    count: int | None = None


class ProxyDistRow(WireSafeModel):
    type: str | None = None
    count: int


class VerifiedBotsTsRow(WireSafeModel):
    time: str
    bot_type: str | None = None
    count: int


class NgwafVerifiedBotRow(WireSafeModel):
    bot_name: str | None = None
    wellknown_bot_name: str | None = None
    category: str | None = None
    request_count: int


class NgwafVerifiedBotsTsRow(WireSafeModel):
    time: str
    bot_name: str | None = None
    count: int


class TrafficQualityRow(WireSafeModel):
    label: str
    value: float


class SuspiciousIspRow(WireSafeModel):
    isp: str
    asn: int | None = None
    count: int


class _SecurityBotsPairMixin(BaseResponse):
    bots: list[SecurityBotRow] = []
    ngwaf_bots: list[SecurityBotRow] = []


class SecurityAggregatesResponse(_SecurityBotsPairMixin):
    active_proxies_count: int = 0
    tunnel_requests_count: int = 0
    distance_mismatches_count: int = 0
    traffic_quality: list[TrafficQualityRow] = []
    suspicious_isps: list[SuspiciousIspRow] = []
    active_clients: list[ActiveClientItem] = []
    tls_fingerprints: list[TlsFingerprintRow] = []
    fingerprint_coverage: dict[str, float] = {}
    req_size_dist: list[BucketCountRow] = []
    top_ips_header: list[TopIpHeaderRow] = []
    ipv6_adoption: list[Ipv6AdoptionRow] = []
    proxy_dist: list[ProxyDistRow] = []
    conn_reuse_dist: list[BucketCountRow] = []
    verified_bots_ts: list[VerifiedBotsTsRow] = []
    ngwaf_configured: bool = False
    ngwaf_verified_bots: list[NgwafVerifiedBotRow] = []
    ngwaf_verified_bots_ts: list[NgwafVerifiedBotsTsRow] = []
    wellknown_bots: list[WellknownBotRow] = []
    bot_efficiency: list[BotEfficiencyRow] = []


class SecurityTopBotsResponse(_SecurityBotsPairMixin):
    reader_coverage: list[ReaderCoverage] | None = _aliased_optional_field("reader_coverage")


SecuritySectionName = Literal[
    "verified_bots_ts",
    "ngwaf_verified_bots",
    "ngwaf_verified_bots_ts",
    "wellknown_bots",
    "bot_efficiency",
    "tls_fingerprints",
    "fingerprint_coverage",
    "req_size_dist",
    "top_ips_header",
    "ipv6_adoption",
    "proxy_dist",
    "conn_reuse_dist",
    "proxies",
    "top_bots",
]


class SecurityAggregatesRequest(RangeKeyedRequest):
    bucket_seconds: int = 300
    sections: list[SecuritySectionName] | None = None


class ThreatIntelItem(WireSafeModel):
    tls_fingerprint: str
    requests: int
    top_country: str
    matched_waf_rules_count: int
    bot_percentage: float
    is_anonymized_proxy: bool


class ThreatIntelListResponse(BaseResponse):
    data: list[ThreatIntelItem]


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


class TimeSeriesPoint(ValueTimeSeriesPoint):
    baseline: float | None = None


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


Num = int | float
InsightFilters = dict[str, str | int | float | None]


class _FiltersOnlyBaseMeta(WireSafeModel):
    filters: InsightFilters

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        _move_fields(cls, "filters", "ottfb_p95_ms")


class _RequestsFiltersBaseMeta(_FiltersOnlyBaseMeta):
    requests: Num


class _IntRequestsFiltersBaseMeta(_FiltersOnlyBaseMeta):
    requests: int


class _ErrorSpikeBaseMeta(_RequestsFiltersBaseMeta):
    errors: Num


class _SpikeRatioBaseMeta(_FiltersOnlyBaseMeta):
    spike_ratio: float


class _RegressionRequestsBaseMeta(_FiltersOnlyBaseMeta):
    regression_ratio: Num
    requests: Num


class _WindowBaselineRequestsBaseMeta(_FiltersOnlyBaseMeta):
    window_requests: Num
    baseline_requests: Num


class _RegressionWindowRequestsBaseMeta(_FiltersOnlyBaseMeta):
    regression_ratio: Num
    window_requests: Num


class _RepeatedPatternBaseMeta(WireSafeModel):
    score: int
    cv: float
    modal_frac: float
    mean_interval_s: float
    stddev_s: float
    mode_gap_s: int | None
    n_gaps: int
    n_events: int
    span_s: int
    rps: float


class ErrorSpikesMeta(_ErrorSpikeBaseMeta):
    pass


class BotnetGroupingMeta(_RequestsFiltersBaseMeta):
    ip_ratio: float


class NewCountryTrafficMeta(_FiltersOnlyBaseMeta):
    pass


class CitySurgesMeta(_SpikeRatioBaseMeta):
    pass


class CityErrorSpikesMeta(_ErrorSpikeBaseMeta):
    pass


class CityLatencyRegressionsMeta(_RegressionRequestsBaseMeta):
    pass


class NewCityTrafficMeta(_FiltersOnlyBaseMeta):
    pass


class UaMonocultureMeta(_RequestsFiltersBaseMeta):
    pass


class NewProbeUrlsMeta(_FiltersOnlyBaseMeta):
    error_pct: Num


class WafSignalSpikesMeta(_SpikeRatioBaseMeta):
    pass


class ProxySurgeMeta(_RequestsFiltersBaseMeta):
    pass


class AsnConcentrationMeta(_RequestsFiltersBaseMeta):
    asn: int


class AsnMetroPerformanceMeta(_RegressionRequestsBaseMeta):
    pass


class CacheCollapseMeta(_WindowBaselineRequestsBaseMeta):
    pass


class CacheabilityRegressionMeta(_WindowBaselineRequestsBaseMeta):
    pass


class LatencyRegressionMeta(_RegressionWindowRequestsBaseMeta):
    pass


class ImpossibleDistanceMeta(_FiltersOnlyBaseMeta):
    hits: Num
    excess_km: float
    pop: str | None
    sample_ip: str | None
    client_lat: Num | None
    client_lon: Num | None
    pop_lat: Num
    pop_lon: Num
    tcp_rtt: Num | None
    country: str | None
    city: str | None


class TailLatencyMeta(_FiltersOnlyBaseMeta):
    p99_ms: float
    p50_ms: float
    ratio: float
    requests: Num


class CipherSpreadMeta(_RequestsFiltersBaseMeta):
    baseline_total_ips: Num
    sha: str


class RequestSizeAnomalyMeta(_FiltersOnlyBaseMeta):
    max_bytes: int
    avg_bytes: int
    requests: Num
    p95_baseline: int


class ConnectionAbuseMeta(_FiltersOnlyBaseMeta):
    max_conn_reqs: int
    avg_conn_reqs: int
    requests: Num
    p95_baseline: int


class RegionLatencyMeta(_RegressionWindowRequestsBaseMeta):
    ottfb_p95_ms: float | None = None


class CacheTtlMismatchMeta(_FiltersOnlyBaseMeta):
    avg_ttl_s: float
    avg_hits: float
    avg_age_s: float
    samples: Num


class ImageOptimizationOpportunitiesMeta(_RequestsFiltersBaseMeta):
    total_mb: float
    mobile_ratio: Num


class OriginLatencySpikeMeta(_RequestsFiltersBaseMeta):
    pass


class OriginErrorRateMeta(_FiltersOnlyBaseMeta):
    pass


class OriginRetriesMeta(_FiltersOnlyBaseMeta):
    max_retries: int


class OriginIpFailureMeta(_RequestsFiltersBaseMeta):
    pass


class ShieldPathDegradationMeta(WireSafeModel):
    ratio: float
    requests: Num


class RepeatedPatternsMeta(_RepeatedPatternBaseMeta, _FiltersOnlyBaseMeta):
    distinct_ua: int


class RepeatedPatternsFpMeta(_RepeatedPatternBaseMeta, _FiltersOnlyBaseMeta):
    distinct_ip: int


class LowAndSlowMeta(_FiltersOnlyBaseMeta):
    hits: int
    distinct_paths: int
    span_s: int
    rps: float


class CredentialEnumerationMeta(_FiltersOnlyBaseMeta):
    attempts: int
    denied: int
    fail_rate_pct: float
    distinct_paths: int


class NetworkAsnHealthMeta(_FiltersOnlyBaseMeta):
    window_packet_loss_pct: float
    baseline_packet_loss_pct: float
    window_jitter_ms: float
    baseline_jitter_ms: float
    window_retrans_avg: float
    baseline_retrans_avg: float
    requests: int
    asn: int


class ContentDiscoveryMeta(_IntRequestsFiltersBaseMeta):
    not_found: int
    not_found_rate_pct: float
    distinct_404_urls: int


class RefererMonocultureMeta(_RequestsFiltersBaseMeta):
    pass


class MethodDriftMeta(_RequestsFiltersBaseMeta):
    method: str | None


class NewAsnTrafficMeta(AsnConcentrationMeta):
    pass


class AsnHostingShiftMeta(_FiltersOnlyBaseMeta):
    window_hosting: Num
    window_total: Num
    asn: int


class MetroDeliveryDegradationMeta(_FiltersOnlyBaseMeta):
    window_requests: Num
    window_mbps: float
    baseline_mbps: float


class ConnectionTypeMixMeta(_RequestsFiltersBaseMeta):
    c_type: str | None
    c_speed: str | None


class PopLatencyRegressionMeta(_RegressionWindowRequestsBaseMeta):
    pass


class Http3FallbackMeta(_FiltersOnlyBaseMeta):
    window_quic: int
    window_total: int
    share_drop_pts: float


class CacheHitCliffMeta(WireSafeModel):
    window_cacheable: int
    baseline_cacheable: int
    drop_pts: float


class PayloadCompressionRegressionMeta(_WindowBaselineRequestsBaseMeta):
    pass


class SessionHarvestingMeta(_IntRequestsFiltersBaseMeta):
    distinct_sessions: int
    baseline_distinct_sessions: int


class TimeoutSplitMeta(WireSafeModel):
    phase: Literal["connect", "read"]
    connect_p95_ms: float
    baseline_connect_p95_ms: float
    read_p95_ms: float
    baseline_read_p95_ms: float
    window_requests: int


class CrawlerDisparityMeta(WireSafeModel):
    crawls: int
    referrals: int
    w_bytes: int = 0
    bandwidth_share_pct: float = 0.0
    crawls_per_referral: float
    baseline_crawls_per_referral: float
    bytes_per_referral: float = 0.0
    baseline_bytes_per_referral: float = 0.0


class StaleBrowserVersionMeta(WireSafeModel):
    requests: int
    ips: int
    surge_ratio: float


class OrphanedDeepCrawlMeta(_IntRequestsFiltersBaseMeta):
    distinct_urls: int
    requests_per_url: float
    surge_ratio: float


class ResidentialFingerprintDispersionMeta(WireSafeModel):
    ja4: str
    ips: int
    asns: int
    requests: int
    asn_expansion_ratio: float


class HighScale5xxMeta(WireSafeModel):
    pass


class _WindowBaselineHoursMixin(BaseModel):
    window_size_hrs: float = Field(default=24.0, gt=0, le=168)
    baseline_hours: float = Field(default=168.0, gt=0, le=2160)


class InsightsRequest(_WindowBaselineHoursMixin, FilteredRequest):
    force_refresh: bool = False


class InsightItem[MetaT: WireSafeModel](BaseModel):
    label: str
    current_val: float | None = None
    baseline_val: float | None = None
    baseline_label: str = "baseline"
    unit: str | None = None
    severity: str = "clean"
    meta: MetaT
    investigate_url: str | None = None
    tooltip: str | None = None


class InsightCardBase[MetaT: WireSafeModel](BaseModel):
    id: str
    title: str
    description: str
    severity: str
    summary: str
    items: list[InsightItem[MetaT]]
    category: str | None = None


class ErrorSpikesInsightCard(InsightCardBase[ErrorSpikesMeta]):
    id: Literal["error_spikes"]


class BotnetGroupingInsightCard(InsightCardBase[BotnetGroupingMeta]):
    id: Literal["botnet_grouping"]


class NewCountryTrafficInsightCard(InsightCardBase[NewCountryTrafficMeta]):
    id: Literal["new_country_traffic"]


class CitySurgesInsightCard(InsightCardBase[CitySurgesMeta]):
    id: Literal["city_surges"]


class CityErrorSpikesInsightCard(InsightCardBase[CityErrorSpikesMeta]):
    id: Literal["city_error_spikes"]


class CityLatencyRegressionsInsightCard(InsightCardBase[CityLatencyRegressionsMeta]):
    id: Literal["city_latency_regressions"]


class NewCityTrafficInsightCard(InsightCardBase[NewCityTrafficMeta]):
    id: Literal["new_city_traffic"]


class UaMonocultureInsightCard(InsightCardBase[UaMonocultureMeta]):
    id: Literal["ua_monoculture"]


class NewProbeUrlsInsightCard(InsightCardBase[NewProbeUrlsMeta]):
    id: Literal["new_probe_urls"]


class WafSignalSpikesInsightCard(InsightCardBase[WafSignalSpikesMeta]):
    id: Literal["waf_signal_spikes"]


class ProxySurgeInsightCard(InsightCardBase[ProxySurgeMeta]):
    id: Literal["proxy_surge"]


class AsnConcentrationInsightCard(InsightCardBase[AsnConcentrationMeta]):
    id: Literal["asn_concentration"]


class AsnMetroPerformanceInsightCard(InsightCardBase[AsnMetroPerformanceMeta]):
    id: Literal["asn_metro_performance"]


class CacheCollapseInsightCard(InsightCardBase[CacheCollapseMeta]):
    id: Literal["cache_collapse"]


class CacheabilityRegressionInsightCard(InsightCardBase[CacheabilityRegressionMeta]):
    id: Literal["cacheability_regression"]


class LatencyRegressionInsightCard(InsightCardBase[LatencyRegressionMeta]):
    id: Literal["latency_regression"]


class ImpossibleDistanceInsightCard(InsightCardBase[ImpossibleDistanceMeta]):
    id: Literal["impossible_distance"]


class TailLatencyInsightCard(InsightCardBase[TailLatencyMeta]):
    id: Literal["tail_latency"]


class CipherSpreadInsightCard(InsightCardBase[CipherSpreadMeta]):
    id: Literal["cipher_spread"]


class RequestSizeAnomalyInsightCard(InsightCardBase[RequestSizeAnomalyMeta]):
    id: Literal["request_size_anomaly"]


class ConnectionAbuseInsightCard(InsightCardBase[ConnectionAbuseMeta]):
    id: Literal["connection_abuse"]


class RegionLatencyInsightCard(InsightCardBase[RegionLatencyMeta]):
    id: Literal["region_latency"]


class CacheTtlMismatchInsightCard(InsightCardBase[CacheTtlMismatchMeta]):
    id: Literal["cache_ttl_mismatch"]


class ImageOptimizationOpportunitiesInsightCard(InsightCardBase[ImageOptimizationOpportunitiesMeta]):
    id: Literal["image_optimization_opportunities"]


class OriginLatencySpikeInsightCard(InsightCardBase[OriginLatencySpikeMeta]):
    id: Literal["origin_latency_spike"]


class OriginErrorRateInsightCard(InsightCardBase[OriginErrorRateMeta]):
    id: Literal["origin_error_rate"]


class OriginRetriesInsightCard(InsightCardBase[OriginRetriesMeta]):
    id: Literal["origin_retries"]


class OriginIpFailureInsightCard(InsightCardBase[OriginIpFailureMeta]):
    id: Literal["origin_ip_failure"]


class ShieldPathDegradationInsightCard(InsightCardBase[ShieldPathDegradationMeta]):
    id: Literal["shield_path_degradation"]


class RepeatedPatternsInsightCard(InsightCardBase[RepeatedPatternsMeta]):
    id: Literal["repeated_patterns"]


class RepeatedPatternsFpInsightCard(InsightCardBase[RepeatedPatternsFpMeta]):
    id: Literal["repeated_patterns_fp"]


class LowAndSlowInsightCard(InsightCardBase[LowAndSlowMeta]):
    id: Literal["low_and_slow"]


class CredentialEnumerationInsightCard(InsightCardBase[CredentialEnumerationMeta]):
    id: Literal["credential_enumeration"]


class NetworkAsnHealthInsightCard(InsightCardBase[NetworkAsnHealthMeta]):
    id: Literal["network_asn_health"]


class ContentDiscoveryInsightCard(InsightCardBase[ContentDiscoveryMeta]):
    id: Literal["content_discovery"]


class RefererMonocultureInsightCard(InsightCardBase[RefererMonocultureMeta]):
    id: Literal["referer_monoculture"]


class MethodDriftInsightCard(InsightCardBase[MethodDriftMeta]):
    id: Literal["method_drift"]


class NewAsnTrafficInsightCard(InsightCardBase[NewAsnTrafficMeta]):
    id: Literal["new_asn_traffic"]


class AsnHostingShiftInsightCard(InsightCardBase[AsnHostingShiftMeta]):
    id: Literal["asn_hosting_shift"]


class MetroDeliveryDegradationInsightCard(InsightCardBase[MetroDeliveryDegradationMeta]):
    id: Literal["metro_delivery_degradation"]


class ConnectionTypeMixInsightCard(InsightCardBase[ConnectionTypeMixMeta]):
    id: Literal["connection_type_mix"]


class PopLatencyRegressionInsightCard(InsightCardBase[PopLatencyRegressionMeta]):
    id: Literal["pop_latency_regression"]


class Http3FallbackInsightCard(InsightCardBase[Http3FallbackMeta]):
    id: Literal["http3_fallback"]


class CacheHitCliffInsightCard(InsightCardBase[CacheHitCliffMeta]):
    id: Literal["cache_hit_cliff"]


class PayloadCompressionRegressionInsightCard(InsightCardBase[PayloadCompressionRegressionMeta]):
    id: Literal["payload_compression_regression"]


class SessionHarvestingInsightCard(InsightCardBase[SessionHarvestingMeta]):
    id: Literal["session_harvesting"]


class TimeoutSplitInsightCard(InsightCardBase[TimeoutSplitMeta]):
    id: Literal["timeout_split"]


class CrawlerDisparityInsightCard(InsightCardBase[CrawlerDisparityMeta]):
    id: Literal["crawler_disparity"]


class StaleBrowserVersionInsightCard(InsightCardBase[StaleBrowserVersionMeta]):
    id: Literal["stale_browser_version"]


class OrphanedDeepCrawlInsightCard(InsightCardBase[OrphanedDeepCrawlMeta]):
    id: Literal["orphaned_deep_crawl"]


class ResidentialFingerprintDispersionInsightCard(InsightCardBase[ResidentialFingerprintDispersionMeta]):
    id: Literal["residential_fingerprint_dispersion"]


class HighScale5xxInsightCard(InsightCardBase[HighScale5xxMeta]):
    id: Literal["high_scale_5xx"]


InsightCard = Annotated[
    ErrorSpikesInsightCard
    | BotnetGroupingInsightCard
    | NewCountryTrafficInsightCard
    | CitySurgesInsightCard
    | CityErrorSpikesInsightCard
    | CityLatencyRegressionsInsightCard
    | NewCityTrafficInsightCard
    | UaMonocultureInsightCard
    | NewProbeUrlsInsightCard
    | WafSignalSpikesInsightCard
    | ProxySurgeInsightCard
    | AsnConcentrationInsightCard
    | AsnMetroPerformanceInsightCard
    | CacheCollapseInsightCard
    | CacheabilityRegressionInsightCard
    | LatencyRegressionInsightCard
    | ImpossibleDistanceInsightCard
    | TailLatencyInsightCard
    | CipherSpreadInsightCard
    | RequestSizeAnomalyInsightCard
    | ConnectionAbuseInsightCard
    | RegionLatencyInsightCard
    | CacheTtlMismatchInsightCard
    | ImageOptimizationOpportunitiesInsightCard
    | OriginLatencySpikeInsightCard
    | OriginErrorRateInsightCard
    | OriginRetriesInsightCard
    | OriginIpFailureInsightCard
    | ShieldPathDegradationInsightCard
    | RepeatedPatternsInsightCard
    | RepeatedPatternsFpInsightCard
    | LowAndSlowInsightCard
    | CredentialEnumerationInsightCard
    | NetworkAsnHealthInsightCard
    | ContentDiscoveryInsightCard
    | RefererMonocultureInsightCard
    | MethodDriftInsightCard
    | NewAsnTrafficInsightCard
    | AsnHostingShiftInsightCard
    | MetroDeliveryDegradationInsightCard
    | ConnectionTypeMixInsightCard
    | PopLatencyRegressionInsightCard
    | Http3FallbackInsightCard
    | CacheHitCliffInsightCard
    | PayloadCompressionRegressionInsightCard
    | SessionHarvestingInsightCard
    | TimeoutSplitInsightCard
    | CrawlerDisparityInsightCard
    | StaleBrowserVersionInsightCard
    | OrphanedDeepCrawlInsightCard
    | ResidentialFingerprintDispersionInsightCard
    | HighScale5xxInsightCard,
    Field(discriminator="id"),
]


class InsightsResponse(BaseResponse):
    insights: list[InsightCard]
    window_start: str
    window_end: str
    baseline_start: str
    baseline_end: str
    computed_at: str
    window_hours: float
    baseline_hours: float


class InsightAvailability(BaseModel):
    id: str
    title: str
    description: str
    category: str | None = None
    missing_fields: list[str] | None = None
    required_fields: list[str] | None = None
    required_groups: list[str] | None = None
    available: bool


class InsightsAvailabilityResponse(BaseModel):
    available: bool | list[str]
    insights: list[InsightAvailability] | None = None
    unavailable: list[InsightAvailability] | None = None


class CacheCollapseDetailRequest(_WindowBaselineHoursMixin):
    url: str


class CacheCollapseTimelinePoint(BaseModel):
    bucket: str
    expected_hits: float
    real_hits: int
    misses: int
    total_requests: int
    hit_rate: float


class CacheMissPoint(BaseModel):
    timestamp: str
    cache: str
    pop: str | None = None
    ip: str | None = None
    status: int | None = None


class CacheBreakdown(BaseModel):
    hits: int = 0
    misses: int = 0
    passes: int = 0
    other: int = 0


class CacheCollapseDetailResponse(BaseResponse):
    url: str = ""
    timeline: list[CacheCollapseTimelinePoint] = Field(default_factory=list)
    recent_misses: list[CacheMissPoint] = Field(default_factory=list)
    breakdown: CacheBreakdown = Field(default_factory=CacheBreakdown)
    baseline_hit_rate: float = 0.0
    window_hit_rate: float = 0.0
    baseline_pass_rate: float = 0.0
    window_pass_rate: float = 0.0
    has_data: bool = True
    clusters: list[Any] = Field(default_factory=list)


class SessionsRequest(RangeKeyedRequest, PaginationMixin):
    limit: Limit500 = 50
    sort_by: str = "session_start"
    flagged_only: bool = False
    streaming_only: bool = False
    min_reqs_flag: int | None = None
    min_4xx_pct_flag: float | None = None


class Session(BaseModel):
    ip: str
    ua: str | None = None
    ja4: str | None = None
    country: str | None = None
    asn: int | None = None
    session_start: str
    session_end: str
    req_count: int
    edge_count: int | None = None
    shield_count: int | None = None
    unique_urls: int | None = None
    reqs_4xx: int | None = None
    reqs_5xx: int | None = None
    total_bytes: int | None = None
    median_rtt_ms: float | None = None
    edge_sid: str | None = None
    is_streaming: bool | None = None
    flagged: bool
    session_token: str | None = None


class SessionsResponse(BaseResponse):
    sessions: list[Session]
    asn_names: dict[int, str] = {}
    total: int
    page: int
    limit: int
    has_rtt: bool
    has_ja4: bool
    has_edge: bool
    has_edge_sid: bool = False
    has_cmcd: bool = False
    min_reqs_flag: int | None = None
    min_4xx_pct_flag: float | None = None


class SessionDetailRequest(BaseModel):
    session_token: str | None = None
    ip: str | None = None
    ja4: str | None = None
    start_time: str | None = None
    end_time: str | None = None


class SessionDetailResponse(BaseResponse):
    columns: list[str]
    data: list[dict[str, ScalarCell]]


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
