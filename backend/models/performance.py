from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from backend.models._base import WireSafeModel
from backend.models.common import BaseResponse, Limit100, RangeKeyedRequest, Seconds14400

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
