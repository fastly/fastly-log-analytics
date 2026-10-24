"""Pydantic models for automated insights and anomaly detection endpoints."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

from backend.models._base import WireSafeModel
from backend.models.common import BaseResponse, FilteredRequest

Num = int | float
# Column -> value payload that seeds ``investigate_url``. Keys vary per insight (and per
# fingerprint column); values are the raw group-by value.
InsightFilters = dict[str, str | int | float | None]


class _FiltersOnlyBaseMeta(WireSafeModel):
    filters: InsightFilters


class _RequestsFiltersBaseMeta(WireSafeModel):
    requests: Num
    filters: InsightFilters


class _ErrorSpikeBaseMeta(WireSafeModel):
    requests: Num
    errors: Num
    filters: InsightFilters


class _SpikeRatioBaseMeta(WireSafeModel):
    spike_ratio: float
    filters: InsightFilters


class _RegressionRequestsBaseMeta(WireSafeModel):
    regression_ratio: Num
    requests: Num
    filters: InsightFilters


class _WindowBaselineRequestsBaseMeta(WireSafeModel):
    window_requests: Num
    baseline_requests: Num
    filters: InsightFilters


class _RegressionWindowRequestsBaseMeta(WireSafeModel):
    regression_ratio: Num
    window_requests: Num
    filters: InsightFilters


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


class BotnetGroupingMeta(WireSafeModel):
    requests: Num
    ip_ratio: float
    filters: InsightFilters


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


class NewProbeUrlsMeta(WireSafeModel):
    error_pct: Num
    filters: InsightFilters


class WafSignalSpikesMeta(_SpikeRatioBaseMeta):
    pass


class ProxySurgeMeta(_RequestsFiltersBaseMeta):
    pass


class AsnConcentrationMeta(WireSafeModel):
    requests: Num
    asn: int
    filters: InsightFilters


class AsnMetroPerformanceMeta(_RegressionRequestsBaseMeta):
    pass


class CacheCollapseMeta(_WindowBaselineRequestsBaseMeta):
    pass


class CacheabilityRegressionMeta(_WindowBaselineRequestsBaseMeta):
    pass


class LatencyRegressionMeta(_RegressionWindowRequestsBaseMeta):
    pass


class ImpossibleDistanceMeta(WireSafeModel):
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
    filters: InsightFilters


class TailLatencyMeta(WireSafeModel):
    p99_ms: float
    p50_ms: float
    ratio: float
    requests: Num
    filters: InsightFilters


class CipherSpreadMeta(WireSafeModel):
    requests: Num
    baseline_total_ips: Num
    sha: str
    filters: InsightFilters


class RequestSizeAnomalyMeta(WireSafeModel):
    max_bytes: int
    avg_bytes: int
    requests: Num
    p95_baseline: int
    filters: InsightFilters


class ConnectionAbuseMeta(WireSafeModel):
    max_conn_reqs: int
    avg_conn_reqs: int
    requests: Num
    p95_baseline: int
    filters: InsightFilters


class RegionLatencyMeta(WireSafeModel):
    regression_ratio: Num
    window_requests: Num
    filters: InsightFilters
    # Present only when the service logs origin TTFB; appended after ``filters`` by the producer.
    ottfb_p95_ms: float | None = None


class CacheTtlMismatchMeta(WireSafeModel):
    avg_ttl_s: float
    avg_hits: float
    avg_age_s: float
    samples: Num
    filters: InsightFilters


class ImageOptimizationOpportunitiesMeta(WireSafeModel):
    requests: Num
    total_mb: float
    mobile_ratio: Num
    filters: InsightFilters


class OriginLatencySpikeMeta(_RequestsFiltersBaseMeta):
    pass


class OriginErrorRateMeta(_FiltersOnlyBaseMeta):
    pass


class OriginRetriesMeta(WireSafeModel):
    max_retries: int
    filters: InsightFilters


class OriginIpFailureMeta(_RequestsFiltersBaseMeta):
    pass


class ShieldPathDegradationMeta(WireSafeModel):
    ratio: float
    requests: Num


class RepeatedPatternsMeta(_RepeatedPatternBaseMeta):
    distinct_ua: int
    filters: InsightFilters


class RepeatedPatternsFpMeta(_RepeatedPatternBaseMeta):
    distinct_ip: int
    filters: InsightFilters


class LowAndSlowMeta(WireSafeModel):
    hits: int
    distinct_paths: int
    span_s: int
    rps: float
    filters: InsightFilters


class CredentialEnumerationMeta(WireSafeModel):
    attempts: int
    denied: int
    fail_rate_pct: float
    distinct_paths: int
    filters: InsightFilters


class NetworkAsnHealthMeta(WireSafeModel):
    window_packet_loss_pct: float
    baseline_packet_loss_pct: float
    window_jitter_ms: float
    baseline_jitter_ms: float
    window_retrans_avg: float
    baseline_retrans_avg: float
    requests: int
    asn: int
    filters: InsightFilters


class ContentDiscoveryMeta(WireSafeModel):
    requests: int
    not_found: int
    not_found_rate_pct: float
    distinct_404_urls: int
    filters: InsightFilters


class RefererMonocultureMeta(_RequestsFiltersBaseMeta):
    pass


class MethodDriftMeta(WireSafeModel):
    requests: Num
    method: str | None
    filters: InsightFilters


class NewAsnTrafficMeta(AsnConcentrationMeta):
    pass


class AsnHostingShiftMeta(WireSafeModel):
    window_hosting: Num
    window_total: Num
    asn: int
    filters: InsightFilters


class MetroDeliveryDegradationMeta(WireSafeModel):
    window_requests: Num
    window_mbps: float
    baseline_mbps: float
    filters: InsightFilters


class ConnectionTypeMixMeta(WireSafeModel):
    requests: Num
    c_type: str | None
    c_speed: str | None
    filters: InsightFilters


class PopLatencyRegressionMeta(_RegressionWindowRequestsBaseMeta):
    pass


class Http3FallbackMeta(WireSafeModel):
    window_quic: int
    window_total: int
    share_drop_pts: float
    filters: InsightFilters


class CacheHitCliffMeta(WireSafeModel):
    window_cacheable: int
    baseline_cacheable: int
    drop_pts: float


class PayloadCompressionRegressionMeta(_WindowBaselineRequestsBaseMeta):
    pass


class SessionHarvestingMeta(WireSafeModel):
    requests: int
    distinct_sessions: int
    baseline_distinct_sessions: int
    filters: InsightFilters


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


class OrphanedDeepCrawlMeta(WireSafeModel):
    requests: int
    distinct_urls: int
    requests_per_url: float
    surge_ratio: float
    filters: InsightFilters


class ResidentialFingerprintDispersionMeta(WireSafeModel):
    ja4: str
    ips: int
    asns: int
    requests: int
    asn_expansion_ratio: float


class HighScale5xxMeta(WireSafeModel):
    pass


class InsightsRequest(FilteredRequest):
    """Request payload for /api/insights."""

    # M2: bound both windows. Unbounded, these fed
    # ``baseline_start = now - (baseline_hours + window_size_hrs)`` into a
    # temp-table scan — a value like 8_760_000 reached ~1000 years back and
    # scanned every retained row (DoS + time-scope bypass). 168h / 2160h
    # (7d / 90d) cover every legitimate insight selection.
    window_size_hrs: float = Field(default=24.0, gt=0, le=168)
    baseline_hours: float = Field(default=168.0, gt=0, le=2160)
    force_refresh: bool = False


class InsightItem[MetaT: WireSafeModel](BaseModel):
    label: str
    current_val: float | None = None
    baseline_val: float | None = None
    baseline_label: str = "baseline"
    unit: str | None = None
    severity: str = "clean"
    # Detection-specific evidence; the concrete model is fixed by the owning card's ``id``.
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
    # Thematic section key ({security, origin, edge, traffic, network}); drives the
    # sectioned Anomaly Detection page (frontend/lib/insight-sections.ts).
    # Optional-with-default, NOT required: the /api/insights cache stores the
    # raw pre-serialisation dict, so a stale pre-deploy entry that predates
    # this field must serialise as ``null`` instead of raising a
    # ValidationError for the whole TTL window. The field is load-bearing —
    # a ``category`` key on the repo dict alone is silently dropped on the
    # wire (Pydantic ``extra='ignore'``), the same way window_total_requests is.
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


# Discriminated on the insight id, so ``items[].meta`` is typed per detection. A registered
# insight without a variant here fails ``tests/models/test_wire_insights.py``.
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
    # Same thematic section key as InsightCard.category, mirrored here so the
    # availability-derived skeleton cards group into the identical sections
    # (CLS: the loading layout must match the loaded layout). Sourced from the
    # INSIGHT_DEFINITIONS dicts in backend/core/_log_fields_data.py — a
    # SEPARATE subsystem from the runtime registry, so category is set in both.
    category: str | None = None
    missing_fields: list[str] | None = None
    required_fields: list[str] | None = None
    required_groups: list[str] | None = None
    available: bool


class InsightsAvailabilityResponse(BaseModel):
    available: bool | list[str]
    insights: list[InsightAvailability] | None = None
    unavailable: list[InsightAvailability] | None = None


# ── Cache Collapse Detail ─────────────────────────────────────────────────────


class CacheCollapseDetailRequest(BaseModel):
    url: str
    window_size_hrs: float = Field(default=24.0, gt=0, le=168)
    baseline_hours: float = Field(default=168.0, gt=0, le=2160)


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
    # Window-scoped request counts by cache disposition.
    hits: int = 0
    misses: int = 0
    passes: int = 0
    other: int = 0


class CacheCollapseDetailResponse(BaseResponse):
    url: str = ""
    timeline: list[CacheCollapseTimelinePoint] = Field(default_factory=list)
    recent_misses: list[CacheMissPoint] = Field(default_factory=list)
    breakdown: CacheBreakdown = Field(default_factory=CacheBreakdown)
    # Cacheable hit ratio HIT/(HIT+MISS); PASS excluded.
    baseline_hit_rate: float = 0.0
    window_hit_rate: float = 0.0
    # Uncacheable share PASS/total.
    baseline_pass_rate: float = 0.0
    window_pass_rate: float = 0.0
    has_data: bool = True
    clusters: list[Any] = Field(default_factory=list)
