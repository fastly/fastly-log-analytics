from __future__ import annotations

from datetime import datetime
from typing import Any

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


def security_aggregates(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityAggregatesResponse:
    start = _range_value(start_time)
    end = _range_value(end_time)

    def _query(query: str):
        try:
            return service.client.execute(query, {"service_id": service.service_id, "start": start, "end": end})
        except Exception:
            return []

    # 1. proxy_dist
    proxy_rows = _query("""
        SELECT custom_fields['p_desc'] as type, count() as count
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String} AND publication_state='visible'
          AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}
          AND custom_fields['p_desc'] != ''
        GROUP BY type ORDER BY count DESC LIMIT 10
    """)

    # 2. tls_fingerprints
    tls_fp_rows = _query("""
        SELECT custom_fields['tls_ciphers_sha'] as fingerprint, count() as count, count(distinct client_ip) as ips
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String} AND publication_state='visible'
          AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}
          AND custom_fields['tls_ciphers_sha'] != ''
        GROUP BY fingerprint ORDER BY count DESC LIMIT 20
    """)

    # 3. req_size_dist
    # DuckDB bucket logic: case when req_bytes <= 1024 then '<1KB' etc.
    # We will just map it simply.
    size_rows = _query("""
        SELECT
            CASE
                WHEN toInt64OrZero(custom_fields['req_bytes']) <= 1024 THEN '<1KB'
                WHEN toInt64OrZero(custom_fields['req_bytes']) <= 10240 THEN '1-10KB'
                WHEN toInt64OrZero(custom_fields['req_bytes']) <= 102400 THEN '10-100KB'
                WHEN toInt64OrZero(custom_fields['req_bytes']) <= 1048576 THEN '100KB-1MB'
                ELSE '>1MB'
            END as label,
            count() as count
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String} AND publication_state='visible'
          AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}
        GROUP BY label ORDER BY count DESC
    """)

    # 4. ipv6_adoption
    ipv6_rows = _query("""
        SELECT
            if(custom_fields['is_ipv6'] = '1', 'IPv6', 'IPv4') as version,
            count() as count
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String} AND publication_state='visible'
          AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}
        GROUP BY version ORDER BY count DESC
    """)

    # 5. conn_reuse_dist
    conn_rows = _query("""
        SELECT
            CASE
                WHEN toInt64OrZero(custom_fields['conn_requests']) = 1 THEN '1'
                WHEN toInt64OrZero(custom_fields['conn_requests']) <= 5 THEN '2-5'
                WHEN toInt64OrZero(custom_fields['conn_requests']) <= 10 THEN '6-10'
                WHEN toInt64OrZero(custom_fields['conn_requests']) <= 50 THEN '11-50'
                ELSE '51+'
            END as label,
            count() as count
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String} AND publication_state='visible'
          AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}
        GROUP BY label ORDER BY count DESC
    """)

    # 6. ngwaf_verified_bots
    bot_rows = _query("""
        SELECT
            custom_fields['ngwaf_bot'] as bot_name,
            count() as request_count
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String} AND publication_state='visible'
          AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}
          AND custom_fields['ngwaf_bot'] != ''
        GROUP BY bot_name ORDER BY request_count DESC LIMIT 50
    """)
    ngwaf_verified_bots = [
        {
            "bot_name": r["bot_name"],
            "wellknown_bot_name": None,
            "category": "unknown",
            "request_count": int(r["request_count"]),
        }
        for r in bot_rows
    ]

    return SecurityAggregatesResponse.with_telemetry(
        tls_fingerprints=tls_fp_rows,
        req_size_dist=size_rows,
        ipv6_adoption=ipv6_rows,
        proxy_dist=proxy_rows,
        conn_reuse_dist=conn_rows,
        verified_bots_ts=[],
        ngwaf_verified_bots=ngwaf_verified_bots,
        ngwaf_verified_bots_ts=[],
        wellknown_bots=[],
        fingerprint_coverage={"tls_ciphers_sha": 1.0},
        ngwaf_configured=True if ngwaf_verified_bots else False,
    )


def top_bots(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityTopBotsResponse:
    start = _range_value(start_time)
    end = _range_value(end_time)

    query = """
        SELECT
            custom_fields['ngwaf_bot'] as bot_name,
            count() as request_count
        FROM fastly_log_analytics.request_facts
        WHERE service_id={service_id:String} AND publication_state='visible'
          AND event_timestamp >= {start:DateTime64(3)} AND event_timestamp < {end:DateTime64(3)}
          AND custom_fields['ngwaf_bot'] != ''
        GROUP BY bot_name ORDER BY request_count DESC LIMIT 10
    """
    try:
        rows = service.client.execute(query, {"service_id": service.service_id, "start": start, "end": end})
    except Exception:
        rows = []

    ngwaf_bots = [
        {
            "id": r["bot_name"],
            "name": r["bot_name"],
            "category": "unknown",
            "request_count": int(r["request_count"]),
            "verified_count": int(r["request_count"]),
            "impersonator_count": 0,
            "unverified_count": 0,
            "verification_coverage": 1.0,
            "pending_count": 0,
        }
        for r in rows
    ]
    return SecurityTopBotsResponse.with_telemetry(bots=[], ngwaf_bots=ngwaf_bots)


def get_proxies_data(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityProxiesResponse:
    return SecurityProxiesResponse.with_telemetry(
        active_proxies_count=0,
        tunnel_requests_count=0,
        distance_mismatches_count=0,
        traffic_quality=[],
        suspicious_isps=[],
        active_clients=[],
    )


def get_security_threat_intel(
    service: HighScaleService, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    return {"has_data": False, "fingerprints": []}
