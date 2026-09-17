from __future__ import annotations

from datetime import datetime
from typing import Any

from backend.high_scale.aggregate_query import query_clickhouse_aggregate
from backend.high_scale.aggregates import AggregateRequest
from backend.high_scale.registry import HighScaleService
from backend.models.security import (
    SecurityAggregatesResponse,
    SecurityProxiesResponse,
    SecurityTopBotsResponse,
)


def _range_value(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _aggregate(
    service: HighScaleService,
    *,
    start_time: str | None,
    end_time: str | None,
    dimension: str,
):
    start = _range_value(start_time)
    end = _range_value(end_time)
    return query_clickhouse_aggregate(
        service.client,
        AggregateRequest(
            service_id=service.service_id,
            domain="request",
            start=start,
            end=end,
            dimension=dimension,
        ),
        watermark=service.watermark_for("request"),
    )


def security_aggregates(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityAggregatesResponse:
    # Resolve requested proxy distribution
    proxy_response = _aggregate(service, start_time=start_time, end_time=end_time, dimension="p_desc")
    proxy_dist = [{"type": value, "count": count} for value, count in proxy_response.top_values]

    # Resolve TLS Protocol breakdown
    tls_response = _aggregate(service, start_time=start_time, end_time=end_time, dimension="tls")
    tls_protocol = [{"protocol": value, "count": count} for value, count in tls_response.top_values]

    # Resolve WAF Status breakdown
    waf_response = _aggregate(service, start_time=start_time, end_time=end_time, dimension="waf_resp")
    waf_status = [{"status": value, "count": count} for value, count in waf_response.top_values]

    return SecurityAggregatesResponse.with_telemetry(
        tls_fingerprints=[],
        req_size_dist=[],
        ipv6_adoption=[],
        proxy_dist=proxy_dist,
        conn_reuse_dist=[],
        verified_bots_ts=[],
        ngwaf_verified_bots=[],
        ngwaf_verified_bots_ts=[],
        wellknown_bots=[],
        fingerprint_coverage={},
        tls_config=[],
        tls_protocol=tls_protocol,
        ciphers_pfs=[],
        ciphers_aead=[],
        ciphers_algo=[],
        waf_ts=[],
        waf_status=waf_status,
        ngwaf_configured=False,
    )


def top_bots(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityTopBotsResponse:
    return SecurityTopBotsResponse.with_telemetry(bots=[], ngwaf_bots=[])


def get_proxies_data(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityProxiesResponse:
    return SecurityProxiesResponse.with_telemetry(proxies=[])


def get_security_threat_intel(
    service: HighScaleService, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    return {"has_data": False, "fingerprints": []}
