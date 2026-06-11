from __future__ import annotations

from typing import Any

from backend.models.common import BaseResponse


class SecurityAggregatesResponse(BaseResponse):
    tls_fingerprints: list[dict[str, Any]] = []
    h2_fingerprints: list[dict[str, Any]] = []
    oh_fingerprints: list[dict[str, Any]] = []
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
