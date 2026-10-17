from __future__ import annotations

from typing import Any

from backend.models._base import WireSafeModel
from backend.models.common import BaseResponse, RangeKeyedRequest


class ActiveClientItem(BaseResponse):
    ip: str
    risk_level: str
    asn_name: str | None = None
    impossible_distance: bool = False
    rtt_min_ms: float | None = None
    tcp_rtt_ms: float | None = None
    distance_km: float | None = None
    pop: str | None = None
    client_lat: float | None = None
    client_lon: float | None = None
    pop_lat: float | None = None
    pop_lon: float | None = None
    country: str | None = None
    city: str | None = None


class SecurityAggregatesResponse(BaseResponse):
    tls_fingerprints: list[dict[str, Any]] = []
    # Per-fingerprint-card coverage: {"tls_ciphers_sha": 0.99}. Drives the FE
    # "low coverage" hint so an analyst seeing a bare or trivial leaderboard
    # understands whether it's a field-not-enabled problem vs the field being
    # legitimately sparse for the current traffic mix (e.g. TLS fingerprints on
    # a service whose traffic is mostly shielded — the code is fine, the data
    # just isn't there).
    fingerprint_coverage: dict[str, float] = {}
    req_size_dist: list[dict[str, Any]] = []
    top_ips_header: list[dict[str, Any]] = []
    ipv6_adoption: list[dict[str, Any]] = []
    proxy_dist: list[dict[str, Any]] = []
    conn_reuse_dist: list[dict[str, Any]] = []
    verified_bots_ts: list[dict[str, Any]] = []
    ngwaf_configured: bool = False
    ngwaf_verified_bots: list[dict[str, Any]] = []
    ngwaf_verified_bots_ts: list[dict[str, Any]] = []
    wellknown_bots: list[dict[str, Any]] = []

    # ── Merged /top-bots and /proxies endpoints ──
    bots: list[dict[str, Any]] = []
    ngwaf_bots: list[dict[str, Any]] = []
    active_proxies_count: int = 0
    tunnel_requests_count: int = 0
    distance_mismatches_count: int = 0
    traffic_quality: list[dict[str, Any]] = []
    suspicious_isps: list[dict[str, Any]] = []
    active_clients: list[ActiveClientItem] = []


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


class SecurityTopBotsResponse(BaseResponse):
    bots: list[SecurityBotRow] = []
    ngwaf_bots: list[SecurityBotRow] = []


class SecurityProxiesRequest(RangeKeyedRequest):
    pass


class SecurityProxiesResponse(BaseResponse):
    active_proxies_count: int = 0
    tunnel_requests_count: int = 0
    distance_mismatches_count: int = 0
    traffic_quality: list[dict[str, Any]] = []
    suspicious_isps: list[dict[str, Any]] = []
    active_clients: list[ActiveClientItem] = []


# Alias for bundle endpoint contract
SecurityBundleResponse = SecurityAggregatesResponse
