from __future__ import annotations

from typing import Any

from backend.models.common import BaseResponse


class SecurityAggregatesResponse(BaseResponse):
    tls_fingerprints: list[dict[str, Any]] = []
    h2_fingerprints: list[dict[str, Any]] = []
    oh_fingerprints: list[dict[str, Any]] = []
    # Per-fingerprint-card coverage: {"tls_ciphers_sha": 0.99, "h2_fingerprint":
    # 0.0002, "oh_fingerprint": 0.54}. Drives the FE "low coverage" hint so an
    # analyst seeing a bare or trivial leaderboard understands whether it's a
    # field-not-enabled problem vs the field being legitimately sparse for the
    # current traffic mix (e.g. h2 fingerprints on a service that's ~99.99%
    # HTTP/1.1 — the code is fine, the data just isn't there).
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


class SecurityTopBotsResponse(BaseResponse):
    bots: list[dict[str, Any]] = []
    ngwaf_bots: list[dict[str, Any]] = []
