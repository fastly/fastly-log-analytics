"""Pydantic models for automated insights and anomaly detection endpoints."""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field

from backend.models._base import WireSafeModel
from backend.models.common import BaseResponse, FilteredRequest
from backend.models.insight_meta import (
    AsnConcentrationMeta,
    AsnHostingShiftMeta,
    AsnMetroPerformanceMeta,
    BotnetGroupingMeta,
    CacheabilityRegressionMeta,
    CacheCollapseMeta,
    CacheHitCliffMeta,
    CacheTtlMismatchMeta,
    CipherSpreadMeta,
    CityErrorSpikesMeta,
    CityLatencyRegressionsMeta,
    CitySurgesMeta,
    ConnectionAbuseMeta,
    ConnectionTypeMixMeta,
    ContentDiscoveryMeta,
    CrawlerDisparityMeta,
    CredentialEnumerationMeta,
    ErrorSpikesMeta,
    HighScale5xxMeta,
    Http3FallbackMeta,
    ImageOptimizationOpportunitiesMeta,
    ImpossibleDistanceMeta,
    LatencyRegressionMeta,
    LowAndSlowMeta,
    MethodDriftMeta,
    MetroDeliveryDegradationMeta,
    NetworkAsnHealthMeta,
    NewAsnTrafficMeta,
    NewCityTrafficMeta,
    NewCountryTrafficMeta,
    NewProbeUrlsMeta,
    OriginErrorRateMeta,
    OriginIpFailureMeta,
    OriginLatencySpikeMeta,
    OriginRetriesMeta,
    OrphanedDeepCrawlMeta,
    PayloadCompressionRegressionMeta,
    PopLatencyRegressionMeta,
    ProxySurgeMeta,
    RefererMonocultureMeta,
    RegionLatencyMeta,
    RepeatedPatternsFpMeta,
    RepeatedPatternsMeta,
    RequestSizeAnomalyMeta,
    ResidentialFingerprintDispersionMeta,
    SessionHarvestingMeta,
    ShieldPathDegradationMeta,
    StaleBrowserVersionMeta,
    TailLatencyMeta,
    TimeoutSplitMeta,
    UaMonocultureMeta,
    WafSignalSpikesMeta,
)


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
