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


def _where(
    service_id: str,
    domain: str,
    start_time: str | None,
    end_time: str | None,
    *,
    dimension: str | None = None,
) -> tuple[str, dict[str, Any]]:
    clauses = [
        "service_id={service_id:String}",
        "publication_state='visible'",
        "batch_id IN ("
        "SELECT batch_id FROM high_scale_batch_publications FINAL "
        "WHERE service_id={service_id:String} AND domain={publication_domain:String} "
        "AND publication_state='visible')",
    ]
    params: dict[str, Any] = {
        "service_id": service_id,
        "publication_domain": domain,
    }
    start = _range_value(start_time)
    end = _range_value(end_time)
    if start is not None:
        clauses.append("bucket_start >= {start:DateTime64(3)}")
        params["start"] = start
    if end is not None:
        clauses.append("bucket_start < {end:DateTime64(3)}")
        params["end"] = end
    if dimension is not None:
        clauses.append("dimension={dimension:String}")
        params["dimension"] = dimension
    return " AND ".join(clauses), params


def _dimension_rows(
    service: HighScaleService,
    *,
    marker: str,
    dimension: str,
    start_time: str | None,
    end_time: str | None,
    select: str,
    group_by: str = "value",
    order_by: str,
    limit: int | None = None,
    having: str | None = None,
    extra_params: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    where, params = _where(
        service.service_id,
        "security_dimensions",
        start_time,
        end_time,
        dimension=dimension,
    )
    suffix = f" HAVING {having}" if having else ""
    params.update(extra_params or {})
    if limit is not None:
        params["limit"] = int(limit)
        suffix += " ORDER BY " + order_by + " LIMIT {limit:UInt32}"
    else:
        suffix += " ORDER BY " + order_by
    return service.client.execute(
        f"/* sec:{marker} */ SELECT {select} FROM security_minute_dimensions WHERE {where} GROUP BY {group_by}{suffix}",
        params,
    )


def _aggregate(
    service: HighScaleService,
    *,
    start_time: str | None,
    end_time: str | None,
    dimension: str,
):
    from backend.high_scale.aggregate_query import query_clickhouse_aggregate
    from backend.high_scale.aggregates import AggregateRequest

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

    # NGWAF Bots Aggregation
    ngwaf_bots_rows = _dimension_rows(
        service,
        marker="ngwaf_bots",
        dimension="ngwaf_bot",
        start_time=start_time,
        end_time=end_time,
        select="value AS bot_name, any(wellknown_bot_name) AS wellknown_bot_name, any(bot_category) AS category, sum(requests) AS request_count",
        group_by="value",
        order_by="request_count DESC",
        limit=50,
    )
    ngwaf_verified_bots = [
        {
            "bot_name": row["bot_name"],
            "wellknown_bot_name": row["wellknown_bot_name"] if row["wellknown_bot_name"] else None,
            "category": row["category"] if row["category"] else "unknown",
            "request_count": int(row["request_count"]),
        }
        for row in ngwaf_bots_rows
    ]

    # NGWAF Bots Time Series
    ngwaf_bots_ts_rows = _dimension_rows(
        service,
        marker="ngwaf_bots_ts",
        dimension="ngwaf_bot",
        start_time=start_time,
        end_time=end_time,
        select="bucket_start AS time, value AS bot_name, sum(requests) AS count",
        group_by="bucket_start, value",
        order_by="time ASC, count DESC",
    )
    ngwaf_verified_bots_ts = [
        {
            "time": row["time"].isoformat() if isinstance(row["time"], datetime) else row["time"],
            "bot_name": row["bot_name"],
            "count": int(row["count"]),
        }
        for row in ngwaf_bots_ts_rows
    ]

    return SecurityAggregatesResponse.with_telemetry(
        tls_fingerprints=[],
        req_size_dist=[],
        ipv6_adoption=[],
        proxy_dist=proxy_dist,
        conn_reuse_dist=[],
        verified_bots_ts=[],
        ngwaf_verified_bots=ngwaf_verified_bots,
        ngwaf_verified_bots_ts=ngwaf_verified_bots_ts,
        wellknown_bots=[],
        fingerprint_coverage={},
        tls_config=[],
        tls_protocol=tls_protocol,
        ciphers_pfs=[],
        ciphers_aead=[],
        ciphers_algo=[],
        waf_ts=[],
        waf_status=waf_status,
        ngwaf_configured=True if ngwaf_verified_bots else False,
    )


def top_bots(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityTopBotsResponse:
    # NGWAF Bots for Top Bots panel
    ngwaf_bots_rows = _dimension_rows(
        service,
        marker="top_bots",
        dimension="ngwaf_bot",
        start_time=start_time,
        end_time=end_time,
        select="value AS bot_name, any(wellknown_bot_name) AS wellknown_bot_name, any(bot_category) AS category, sum(requests) AS request_count, sum(verified_count) AS verified, sum(impersonator_count) AS impersonator",
        group_by="value",
        order_by="request_count DESC",
        limit=10,
    )
    ngwaf_bots = [
        {
            "id": row["bot_name"],
            "name": row["wellknown_bot_name"] or row["bot_name"],
            "category": row["category"] or "unknown",
            "request_count": int(row["request_count"]),
            "verified_count": int(row["verified"]),
            "impersonator_count": int(row["impersonator"]),
            "unverified_count": int(row["request_count"]) - int(row["verified"]) - int(row["impersonator"]),
            "verification_coverage": round(
                (int(row["verified"]) + int(row["impersonator"])) / int(row["request_count"]), 3
            )
            if int(row["request_count"]) > 0
            else 0.0,
            "pending_count": 0,
        }
        for row in ngwaf_bots_rows
    ]
    return SecurityTopBotsResponse.with_telemetry(bots=[], ngwaf_bots=ngwaf_bots)


def get_proxies_data(
    service: HighScaleService, req: Any, start_time: str | None, end_time: str | None
) -> SecurityProxiesResponse:
    return SecurityProxiesResponse.with_telemetry(proxies=[])


def get_security_threat_intel(
    service: HighScaleService, start_time: str | None, end_time: str | None
) -> dict[str, Any]:
    return {"has_data": False, "fingerprints": []}
