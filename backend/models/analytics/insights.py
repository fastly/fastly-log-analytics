from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

from backend.models.analytics._common import _move_fields
from backend.models.common import (
    BaseResponse,
    FilteredRequest,
    WireSafeModel,
)

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
