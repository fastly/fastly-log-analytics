"""Per-detection ``InsightItem.meta`` models, one per insight id (the real producers are the
``row_processor`` callbacks in ``backend/repositories/insights/definitions.py``).

Every model is a ``WireSafeModel`` (``extra="allow"``): a producer that grows a key stays on the wire
untouched. Fields typed ``Num`` are passed through from SQL rows as-is and may be ``int`` or ``float``;
fields the processor wraps in ``int()`` / ``float()`` are typed exactly.
"""

from __future__ import annotations

from typing import Literal

from backend.models._base import WireSafeModel

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
