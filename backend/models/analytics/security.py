from __future__ import annotations

from typing import Literal

from backend.models.analytics._common import (
    ReaderCoverage,
    _aliased_optional_field,
)
from backend.models.common import (
    BaseResponse,
    RangeKeyedRequest,
    WireSafeModel,
)


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
