"""Consolidated analytics (Origin) and admin action/status response models."""

from __future__ import annotations

import sys
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, model_serializer

from backend.models._base import JsonAny, JsonDict, JsonRows, PassthroughModel, WireSafeModel

# ── Admin Action & Status Models (folded from admin_actions.py) ───────────────


class QueryAttribution(PassthroughModel):
    """``attribution`` block from ``_attribution_payload``."""

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


class ActiveQueryRow(PassthroughModel):
    """One in-flight query (``_row_for_active``). ``sql`` is only populated
    when the caller asked for ``full_sql``; ``started_at_utc`` is epoch
    seconds (``time.time()``)."""

    query_id: int | None = None
    db_type: str | None = None
    sql_preview: str | None = None
    sql: str | None = None
    sql_len: int | None = None
    attribution: QueryAttribution | None = None
    service_id: str | None = None
    started_at_utc: float | None = None
    duration_ms: float | None = None
    cancellable: bool | None = None
    cancelled_at: float | None = None


class CompletedQueryRow(PassthroughModel):
    """One finished query (``_row_for_completed``); also the exact layout
    the persisted ``/slow-queries`` rows are re-shaped into."""

    query_id: int | None = None
    db_type: str | None = None
    sql_preview: str | None = None
    sql: str | None = None
    sql_len: int | None = None
    attribution: QueryAttribution | None = None
    service_id: str | None = None
    started_at_utc: float | None = None
    ended_at_utc: float | None = None
    duration_ms: float | None = None
    outcome: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    peak_memory_mb: float | None = None


class SnapshotResponse(BaseModel):
    """Incremental snapshot of the in-process query registry.

    ``last_seq`` is the high-water mark the client should send back as
    ``since_seq`` on its next poll so the registry can stream only new rows.
    """

    last_seq: int
    active: list[ActiveQueryRow]
    completed: list[CompletedQueryRow]


class SummaryResponse(BaseModel):
    """Cheap counts that power the live-monitor tab badge."""

    active_total: int
    by_db_type: dict[str, int]
    longest_ms: float


class CancelResponse(BaseModel):
    """Outcome of ``POST /api/admin/queries/{qid}/cancel``."""

    state: Literal["cancelled", "not_found", "already_finished", "connection_gone"]
    query_id: int


class SlowQueriesCountResponse(BaseModel):
    """Aggregate row-count for the operations-overview card."""

    count: int
    since_hours: int
    threshold_ms: float


class SlowQueriesResponse(BaseModel):
    """Persisted slow-SQL history (``GET /api/admin/slow-queries``)."""

    rows: list[CompletedQueryRow]
    since_hours: int
    threshold_ms: float


class SlowQueriesClearResponse(BaseModel):
    """Outcome of ``DELETE /api/admin/slow-queries``."""

    deleted: int
    service_id: str


class QueryMonitorConfigResponse(BaseModel):
    """Nav-gating flag for the Live Query Monitor tab."""

    enabled: bool


class BotSourceRefreshResponse(WireSafeModel):
    """``POST /admin/bot-sources/{source_id}/refresh``."""

    ok: bool
    source: JsonDict


class BotSourcesRefreshResponse(WireSafeModel):
    """``POST /admin/bot-sources/refresh`` and ``/admin/bots/refresh``."""

    ok: bool
    sources: JsonRows
    updated_count: int
    failed_count: int


class RdnsEnrichSummary(WireSafeModel):
    resolved: int
    errors: int
    discovered: int
    batch_limit: int


class RdnsEnrichResponse(WireSafeModel):
    """``POST /admin/rdns/enrich``."""

    ok: bool
    summary: RdnsEnrichSummary


class DuckdbRecycleResponse(WireSafeModel):
    """``POST /admin/duckdb/recycle``: ``skipped`` carries reason/rss_mb/threshold_mb, ``recycled`` the rest."""

    ok: bool
    status: str
    reason: str | None = None
    rss_mb: float | None = None
    threshold_mb: float | None = None
    detail: str | None = None
    rss_before_mb: float | None = None
    rss_after_mb: float | None = None
    freed_mb: float | None = None


class NgwafSyncResponse(WireSafeModel):
    """``POST /admin/ngwaf/sync/{service_id}``."""

    ok: bool
    service_id: str
    workspace_id: str
    stats: JsonDict


class SharePurgeResponse(WireSafeModel):
    """``POST /admin/share/purge``: counts from ``share_db.purge_all_share_records``."""

    ok: bool
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
    """``GET /admin/iceberg-calendar``: ISO date -> per-day entry (open-keyed), plus debug envelope keys."""

    __pydantic_extra__: dict[str, IcebergCalendarDay | list]  # type: ignore[assignment]


class VclHealthResponse(WireSafeModel):
    """``GET /admin/vcl-health``: ``error`` replaces the snippet fields when the check cannot run."""

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
    """``GET /debug/page-telemetry``."""

    queries: list[PageTelemetryQuery]
    sections: list[PageTelemetrySection]
    calls: list[PageTelemetryCall]


class BackfillWindowResponse(WireSafeModel):
    """``POST /admin/backfill-window``: ``iceberg.sync_data`` counters, or ``error`` + ``files_downloaded=0``."""

    error: str | None = None
    files_downloaded: int | None = None
    rows_downloaded: int | None = None
    files_removed: int | None = None


class HealthServiceState(WireSafeModel):
    service_id: str
    status: str
    last_ingest: JsonAny = None
    last_sync_status: str | None = None
    last_sync_at: JsonAny = None
    reason: str | None = None
    stale_minutes_used: int | None = None


class HealthResponse(WireSafeModel):
    """``GET /api/health``: liveness payload; ``services`` only for ``?deep=1`` local callers."""

    status: str
    version: str
    services: list[HealthServiceState] | None = None


_self_mod = sys.modules[__name__]
sys.modules["backend.models.admin_actions"] = _self_mod
sys.modules["backend.models.origin"] = _self_mod
sys.modules["backend.models.network"] = _self_mod
sys.modules["backend.models.performance"] = _self_mod
sys.modules["backend.models.rum"] = _self_mod
_models_pkg = sys.modules.get("backend.models")
if _models_pkg is not None:
    setattr(_models_pkg, "admin_actions", _self_mod)
    setattr(_models_pkg, "origin", _self_mod)
    setattr(_models_pkg, "network", _self_mod)
    setattr(_models_pkg, "performance", _self_mod)
    setattr(_models_pkg, "rum", _self_mod)


# ── Network & Origin Models (folded from network.py and origin.py) ────────────

from backend.models.common import (  # noqa: E402
    BaseResponse,
    FilteredRequest,
    HasDataMixin,
    Limit100,
    Limit200,
    Limit1440,
    LimitedRequest,
    RangeKeyedRequest,
    ResponseCacheObservation,
    Seconds14400,
    SectionTimingEntry,
)


class NetworkWorstEntry(BaseModel):
    label: str
    score: float | None = None


class ShieldingRow(BaseModel):
    """One edge→shield POP pair in the transit map / analysis table."""

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
    """Typed sub-payload for ``NetworkHealthResponse.shielding_analysis``."""

    response_cache: ResponseCacheObservation | None = Field(
        default=None,
        validation_alias=AliasChoices("response_cache", "_response_cache"),
        serialization_alias="_response_cache",
        exclude_if=lambda value: value is None,
    )
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


class NetworkHeatmapCell(WireSafeModel):
    """One (ASN, time bucket) health cell."""

    bucket_idx: int
    bucket: str
    throughput_bps: float | None = None
    rtt_med_us: float | None = None
    rtt_baseline_us: float | None = None
    rtt_congestion_us: float | None = None
    avg_ploss: float | None = None
    rtt_jitter_us: float | None = None
    error_pct: float | None = None
    health_score: float
    reqs: int


class NetworkHeatmapRow(WireSafeModel):
    asn: int
    label: str
    total_reqs: int
    buckets: list[NetworkHeatmapCell] = []


class NetworkMapCity(WireSafeModel):
    """One city cell inside a map bucket; ``city_idx`` indexes ``NetworkHealthResponse.cities``."""

    country: str | None = None
    city_idx: int
    metro_code: int | None = None
    rtt_med_us: float | None = None
    avg_ploss: float | None = None
    error_pct: float | None = None
    health_score: float
    reqs: int


class NetworkMapBucket(WireSafeModel):
    bucket_idx: int
    bucket: str
    cities: list[NetworkMapCity] = []


class NetworkLeaderboardRow(WireSafeModel):
    asn: int
    label: str
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
        default=None,
        serialization_alias="_section_timings",
        exclude_if=lambda value: value is None,
    )
    response_cache: ResponseCacheObservation | None = Field(
        default=None,
        validation_alias=AliasChoices("response_cache", "_response_cache"),
        serialization_alias="_response_cache",
        exclude_if=lambda value: value is None,
    )
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
        default=None,
        serialization_alias="_component_observations",
    )
    available: bool = True
    reason: str | None = None
    health: NetworkHealthResponse | None = None
    quality: NetworkQualityResponse | None = None
    pop_health: list[PopHealthItem] | None = None
    transport: TransportMetrics | None = None


class OriginSlowUrlRow(WireSafeModel):
    url: str
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None


class OriginStatusCodeRow(WireSafeModel):
    """``status == -1`` buckets any origin status outside 100-599."""

    status: int
    count: int
    pct: float | None = None


class OriginPathRow(WireSafeModel):
    edge: bool | None = None
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None


class OriginPopLatencyRow(WireSafeModel):
    pop: str
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None
    elevated: bool


class OriginIpHealthRow(WireSafeModel):
    oip: str
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None
    error_pct: float | None = None


class OriginDrilldownRow(WireSafeModel):
    key: str
    requests: int
    p50_ms: float | None = None
    p95_ms: float | None = None
    p99_ms: float | None = None
    error_rate: float
    bytes: int | None = None


class OriginTimeseriesPoint(WireSafeModel):
    """``edge`` is only present when the series is split by leg."""

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


class OriginSummarySection(_OriginSummaryMetricsMixin, _HasDataOnlyMixin, WireSafeModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginTimeseriesSection(_OriginTimeseriesMixin, _HasDataOnlyMixin, WireSafeModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginSlowUrlsSection(_OriginSlowUrlsMixin, _HasDataOnlyMixin, WireSafeModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginStatusCodesSection(_OriginStatusCodesMixin, _HasDataOnlyMixin, WireSafeModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginPathBreakdownSection(_OriginPathBreakdownMixin, _HasDataOnlyMixin, WireSafeModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginPopLatencySection(_OriginPopLatencyMixin, _HasDataOnlyMixin, WireSafeModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginIpHealthSection(_OriginIpHealthMixin, _HasDataOnlyMixin, WireSafeModel):
    approx: bool | None = Field(default=None, alias="_approx")
    approx_note: str | None = Field(default=None, alias="_approx_note")


class OriginAggregatesResponse(HasDataMixin, BaseResponse):
    """Composite of every origin card on the /origin page."""

    summary: OriginSummarySection = Field(default_factory=OriginSummarySection)
    timeseries: OriginTimeseriesSection = Field(default_factory=OriginTimeseriesSection)
    slow_urls: OriginSlowUrlsSection = Field(default_factory=OriginSlowUrlsSection)
    status_codes: OriginStatusCodesSection = Field(default_factory=OriginStatusCodesSection)
    path_breakdown: OriginPathBreakdownSection = Field(default_factory=OriginPathBreakdownSection)
    pop_latency: OriginPopLatencySection = Field(default_factory=OriginPopLatencySection)
    ip_health: OriginIpHealthSection = Field(default_factory=OriginIpHealthSection)


SectionName = Literal[
    "summary",
    "timeseries",
    "slow_urls",
    "status_codes",
    "path_breakdown",
    "pop_latency",
    "ip_health",
]


class OriginRequest(FilteredRequest):
    pass


class OriginTimeseriesRequest(FilteredRequest):
    bucket_minutes: Limit1440 = 5
    split_by_leg: bool = False
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


class OriginAggregatesRequest(RangeKeyedRequest):
    bucket_minutes: Limit1440 = 5
    split_by_leg: bool = False
    timeseries_metric: Literal["ttfb", "ttlb"] = "ttfb"
    timeseries_percentile: Literal["p50", "p95", "p99"] = "p95"
    slow_urls_limit: Limit100 = 20
    slow_urls_min_requests: int = 10
    ip_health_limit: Limit100 = 30
    pop_latency_limit: Limit100 = 30
    sections: list[SectionName] | None = None


# ── Performance, Assets, CMCD, Value & RUM Models (folded from performance.py) ──

PerformanceSectionName = Literal[
    "waterfall",
    "top_urls",
    "top_asns",
    "ttl_dist",
    "scatter",
]


class PerformanceRequest(RangeKeyedRequest):
    sort_by: Literal["avg", "p50", "p95", "p99"] = "p99"
    sections: list[PerformanceSectionName] | None = None


class PerformanceUrlRow(WireSafeModel):
    url: str
    requests: int
    avg: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


class PerformanceAsnRow(WireSafeModel):
    """``asn`` is an int, or the raw dimension string when the value is not numeric; ``label``
    is the resolved ASN name on every path."""

    asn: int | str
    label: str | None = None
    requests: int
    avg: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


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
    """Empty (``{}``) when scatter/waterfall were not requested or could not be computed."""

    avg: PerformanceWaterfallAvg | None = None


class PerformanceAggregatesResponse(BaseResponse):
    model_config = ConfigDict(populate_by_name=True)

    top_urls: list[PerformanceUrlRow] = []
    top_asns: list[PerformanceAsnRow] = []
    ttl_dist: list[PerformanceTtlBucket] = []
    scatter: list[PerformanceScatterPoint] = []
    waterfall: PerformanceWaterfall = Field(default_factory=PerformanceWaterfall)
    # True when top_urls / top_asns were served from the perf_latency rollup
    # (>= 48 h unfiltered windows): the cross-hour percentile combine is a
    # request-weighted average, so the values are approximate. Emitted as
    # ``_approx`` (same key the origin Aggregates badge reads).
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


class LargeUncompressedAssetRow(BaseModel):
    url: str
    requests: int
    avg_bytes: float
    total_bytes: int
    status: int


class LowTtlAssetRow(BaseModel):
    url: str
    requests: int
    avg_ttl: float
    asset_type: str


class AssetsAggregatesResponse(BaseResponse):
    """Composite of assets & shield performance cards."""

    asset_type_breakdown: list[AssetTypeBreakdownRow] = []
    cache_performance: list[CachePerformanceRow] = []
    compression_performance: list[CompressionPerformanceRow] = []
    large_uncompressed_assets: list[LargeUncompressedAssetRow] = []
    low_ttl_assets: list[LowTtlAssetRow] = []
    missing_fields: list[str] = Field(default_factory=list)


class AssetsRequest(RangeKeyedRequest):
    """Assets analytics request schema supporting relative-range resolving."""


# ── CMCD Streaming QoE Models (folded from cmcd.py) ───────────────────────────

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


class CmcdSessionsPoint(WireSafeModel):
    bucket: str
    concurrent_sessions: int
    rebuffer_session_pct: float | None = None
    new_sessions: int


class CmcdBufferHealthPoint(WireSafeModel):
    bucket: str
    p50_buffer: float | None = None
    p95_buffer: float | None = None
    starvation_rate: float | None = None


class CmcdBitratePoint(WireSafeModel):
    bucket: str
    avg_bitrate: float | None = None
    utilization_ratio: float | None = None


class CmcdThroughputPoint(WireSafeModel):
    bucket: str
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


class CmcdStartupPoint(WireSafeModel):
    bucket: str
    startup_ratio: float | None = None


class CmcdTopContentRow(WireSafeModel):
    content_id: str | None = None
    session_count: int
    rebuffer_rate: float | None = None
    avg_bitrate: float | None = None
    avg_buffer_length: float | None = None


class CmcdRebufferByCountryRow(WireSafeModel):
    country: str | None = None
    rebuffer_rate: float | None = None
    session_count: int
    median_throughput: float | None = None


class CmcdRebufferByAsnRow(WireSafeModel):
    asn: int | str
    label: str
    rebuffer_rate: float | None = None
    session_count: int
    avg_bitrate: float | None = None
    avg_buffer_length: float | None = None


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


# ── Fastly Value Executive Summary Models (folded from value.py) ──────────────

ValueSectionName = Literal[
    "overview",
    "caching",
    "security",
    "bots",
    "performance",
    "network",
    "io",
]


class ValueSummaryRequest(RangeKeyedRequest):
    sections: list[ValueSectionName] | None = None
    chart_interval: str = "1 day"


class ValueTimeSeriesPoint(BaseModel):
    time: str
    value: float
    category: str | None = None


class OverviewMetrics(BaseModel):
    origin_offload_pct: float | None = None
    threats_blocked: int | None = None
    cache_acceleration_factor: float | None = None
    total_requests: int | None = None
    total_bandwidth_bytes: int | None = None
    edge_hit_requests: int | None = None
    origin_requests: int | None = None


class CachingMetrics(BaseModel):
    origin_offload_pct: float | None = None
    bandwidth_saved_bytes: int | None = None
    shield_effectiveness_pct: float | None = None
    total_requests: int | None = None
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


class SecurityMetrics(BaseModel):
    waf_blocked: int | None = None
    waf_logged: int | None = None
    waf_passed: int | None = None
    total_requests: int | None = None
    threat_time_series: list[ValueTimeSeriesPoint] = []
    top_waf_signals: list[WafSignalRow] = []


class BotMetrics(BaseModel):
    bot_requests: int | None = None
    verified_bots: int | None = None
    total_requests: int | None = None
    top_bots: list[TopBotRow] = []


class PerformanceMetrics(BaseModel):
    cache_accel_factor: float | None = None
    avg_hit_latency_ms: float | None = None
    avg_miss_latency_ms: float | None = None
    p99_latency_ms: float | None = None
    total_requests: int | None = None
    latency_time_series: list[ValueTimeSeriesPoint] = []


class NetworkMetrics(BaseModel):
    http3_pct: float | None = None
    tls_pct: float | None = None
    ipv6_pct: float | None = None
    h2_pct: float | None = None
    total_requests: int | None = None
    protocol_time_series: list[ValueTimeSeriesPoint] = []


class IOFormatBreakdown(BaseModel):
    format: str
    count: int
    pct: float


class IOMetrics(BaseModel):
    io_transforms: int | None = None
    io_bandwidth_bytes: int | None = None
    io_shield_bandwidth_bytes: int | None = None
    total_requests: int | None = None
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


# ── RUM Models (folded from rum.py) ───────────────────────────────────────────


class RumStatusResponse(WireSafeModel):
    """``GET /{service_id}/rum/status``. Analyst sessions only get the first two keys."""

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


class RumSettingsUpdateResponse(WireSafeModel):
    ok: bool
    message: str | None = None
    skipped: bool | None = None


class RumBeaconHealthResponse(WireSafeModel):
    enabled: bool | None = None
    beacon_fire_rate: int | None = None
    recent_beacons: int | None = None
    last_beacon_time: str | None = None
    setup_complete: bool | None = None
    message: str | None = None


# Numeric fields below are ``int | float`` (not ``float``) so an integer the
# producer emitted (an INP p75, a whole-number average) stays ``5`` on the wire
# instead of being coerced to ``5.0``.
RumNumber = int | float


class RumCwvDistribution(WireSafeModel):
    good: int
    needs_improvement: int
    poor: int


class RumCwvVital(WireSafeModel):
    """One Core Web Vital. ``distribution`` is ``None`` only on the no-data branch."""

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
    """Parallel per-bucket series. ``pageviews`` / ``interactions`` / ``errors``
    are absent on the no-data branch."""

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
    """``GET /{service_id}/rum/analytics`` (data, no-data and failure branches)."""

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
