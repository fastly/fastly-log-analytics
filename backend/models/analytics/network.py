from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, model_serializer

from backend.models.analytics._common import (
    ShieldingRow,
    _aliased_optional_field,
)
from backend.models.common import (
    BaseResponse,
    Limit100,
    PassthroughModel,
    RangeKeyedRequest,
    ResponseCacheObservation,
    Seconds14400,
    SectionTimingEntry,
    WireSafeModel,
)


class NetworkWorstEntry(BaseModel):
    label: str
    score: float | None = None


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
