from __future__ import annotations

from backend.models._base import WireSafeModel
from backend.models.common import BaseResponse, RangeKeyedRequest


class ActiveClientItem(WireSafeModel):
    """Field order mirrors ``GET_ACTIVE_PROXY_CLIENTS`` (``asn`` is swapped for a
    trailing ``asn_name`` by the repository)."""

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
    """One top-bot leaderboard row. UA-matched rows carry ``id``; NGWAF rows
    don't (the rollup/direct-join variants only emit name/category/request_count,
    the high-scale one adds the verification counters)."""

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
    """UA-matched bot with FCrDNS verification counters (``verification_coverage`` is
    annotated last, after the counters)."""

    id: str
    name: str
    category: str
    request_count: int
    verified_count: int
    impersonator_count: int
    unverified_count: int
    pending_count: int
    verification_coverage: float


class TlsFingerprintRow(WireSafeModel):
    """Live and rollup paths emit ``fingerprint``/``ip_count``/``request_count``;
    the high-scale ClickHouse path emits ``fingerprint``/``count``/``ips``."""

    fingerprint: str | None = None
    ip_count: int | None = None
    request_count: int | None = None
    count: int | None = None
    ips: int | None = None


class BucketCountRow(WireSafeModel):
    """Histogram bucket. Repository paths key the bucket as ``bucket``; the
    high-scale path keys it as ``label``."""

    bucket: str | None = None
    label: str | None = None
    count: int


class TopIpHeaderRow(WireSafeModel):
    ip: str | None = None
    max_header: int | None = None


class Ipv6AdoptionRow(WireSafeModel):
    """Repository paths emit ``time``/``pct``; the high-scale path emits ``version``/``count``."""

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


class SecurityAggregatesResponse(BaseResponse):
    tls_fingerprints: list[TlsFingerprintRow] = []
    # Per-fingerprint-card coverage: {"tls_ciphers_sha": 0.99}. Drives the FE
    # "low coverage" hint so an analyst seeing a bare or trivial leaderboard
    # understands whether it's a field-not-enabled problem vs the field being
    # legitimately sparse for the current traffic mix (e.g. TLS fingerprints on
    # a service whose traffic is mostly shielded — the code is fine, the data
    # just isn't there).
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

    # ── Merged /top-bots and /proxies endpoints ──
    bots: list[SecurityBotRow] = []
    ngwaf_bots: list[SecurityBotRow] = []
    active_proxies_count: int = 0
    tunnel_requests_count: int = 0
    distance_mismatches_count: int = 0
    traffic_quality: list[TrafficQualityRow] = []
    suspicious_isps: list[SuspiciousIspRow] = []
    active_clients: list[ActiveClientItem] = []


class SecurityTopBotsResponse(BaseResponse):
    bots: list[SecurityBotRow] = []
    ngwaf_bots: list[SecurityBotRow] = []


class SecurityProxiesRequest(RangeKeyedRequest):
    pass


class SecurityProxiesResponse(BaseResponse):
    active_proxies_count: int = 0
    tunnel_requests_count: int = 0
    distance_mismatches_count: int = 0
    traffic_quality: list[TrafficQualityRow] = []
    suspicious_isps: list[SuspiciousIspRow] = []
    active_clients: list[ActiveClientItem] = []


# Alias for bundle endpoint contract
SecurityBundleResponse = SecurityAggregatesResponse
