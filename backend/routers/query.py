"""SQL console router — ad-hoc query execution, schema reflection, and preset queries."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException

from backend.core.fos_client import _safe_table_name, get_source_for_service
from backend.core.request_context import (
    RequestContext,
    ServiceId,
    build_request_context,
    get_service_id,
    require_service_access,
)
from backend.models.analytics import (
    PresetQuery,
    QueryRequest,
    QueryResponse,
    QuerySchemaResponse,
)
from backend.models.common import DEFAULT_ERROR_RESPONSES
from backend.repositories.query import get_cached_presets, set_cached_presets
from backend.utils.auth import mask_ips_for
from backend.utils.router_utils import make_error, redact_error_text

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["query"], responses=DEFAULT_ERROR_RESPONSES)
query_router = router


def _get_presets(src: dict | None) -> list[dict]:
    if not src:
        return []
    table_name = _safe_table_name(src["name"])
    return [
        # Traffic (5)
        {
            "name": "Sample rows",
            "description": "Preview 100 raw log rows",
            "sql": f"SELECT * FROM {table_name} LIMIT 100",
            "category": "Traffic",
        },
        {
            "name": "Row count",
            "description": "Total number of rows",
            "sql": f"SELECT count(*) AS total_rows FROM {table_name}",
            "category": "Traffic",
        },
        {
            "name": "Column stats",
            "description": "Summary row, IP, URL, and timestamp bounds",
            "sql": f"SELECT count(*) AS total_rows, count(DISTINCT ip) AS unique_ips, count(DISTINCT url) AS unique_urls, min(timestamp) AS earliest_ts, max(timestamp) AS latest_ts FROM {table_name}",
            "category": "Traffic",
        },
        {
            "name": "Status breakdown",
            "description": "Request count by HTTP status code",
            "sql": f"SELECT status, count(*) AS requests FROM {table_name} GROUP BY status ORDER BY requests DESC",
            "category": "Traffic",
        },
        {
            "name": "Top requested URLs",
            "description": "Most frequently requested URL paths and total egress bytes",
            "sql": f"SELECT url, count(*) AS requests, sum(resp_bytes) AS total_bytes FROM {table_name} GROUP BY url ORDER BY requests DESC LIMIT 50",
            "category": "Traffic",
        },
        # Performance (4)
        {
            "name": "Slowest endpoints by latency",
            "description": "URLs with highest average and max edge response time in milliseconds",
            "sql": f"SELECT url, count(*) AS requests, round(avg(elapsed) / 1000.0, 2) AS avg_ms, round(max(elapsed) / 1000.0, 2) AS max_ms FROM {table_name} GROUP BY url HAVING count(*) >= 10 ORDER BY avg_ms DESC LIMIT 25",
            "category": "Performance",
        },
        {
            "name": "Latency stats by PoP",
            "description": "Min, average, and max response times across Fastly edge PoPs",
            "sql": f"SELECT pop, count(*) AS requests, round(min(elapsed) / 1000.0, 2) AS min_ms, round(avg(elapsed) / 1000.0, 2) AS avg_ms, round(max(elapsed) / 1000.0, 2) AS max_ms FROM {table_name} GROUP BY pop ORDER BY requests DESC LIMIT 50",
            "category": "Performance",
        },
        {
            "name": "High-latency outliers (max vs avg)",
            "description": "Endpoints with the highest maximum response times",
            "sql": f"SELECT url, count(*) AS requests, round(avg(elapsed) / 1000.0, 2) AS avg_ms, round(max(elapsed) / 1000.0, 2) AS max_ms FROM {table_name} GROUP BY url HAVING count(*) >= 20 ORDER BY max_ms DESC LIMIT 25",
            "category": "Performance",
        },
        {
            "name": "Large uncompressed payloads",
            "description": "Responses over 100 KB served with high latency",
            "sql": f"SELECT url, status, resp_bytes, round(elapsed / 1000.0, 2) AS elapsed_ms FROM {table_name} WHERE resp_bytes > 102400 ORDER BY resp_bytes DESC LIMIT 50",
            "category": "Performance",
        },
        # Cache (4)
        {
            "name": "Cache status distribution",
            "description": "Request volume and bandwidth share by Fastly cache state",
            "sql": f"SELECT cache, count(*) AS requests, sum(resp_bytes) AS total_bytes FROM {table_name} GROUP BY cache ORDER BY requests DESC",
            "category": "Cache",
        },
        {
            "name": "Top cache misses by URL",
            "description": "URLs generating the most cache MISS responses",
            "sql": f"SELECT url, count(*) AS misses, sum(resp_bytes) AS miss_bytes FROM {table_name} WHERE cache LIKE 'MISS%' GROUP BY url ORDER BY misses DESC LIMIT 25",
            "category": "Cache",
        },
        {
            "name": "Uncacheable PASS endpoints",
            "description": "Endpoints bypassing cache via PASS",
            "sql": f"SELECT url, count(*) AS pass_requests FROM {table_name} WHERE cache LIKE 'PASS%' GROUP BY url ORDER BY pass_requests DESC LIMIT 25",
            "category": "Cache",
        },
        {
            "name": "Low-TTL cached objects",
            "description": "Cached HIT responses with short TTLs under 60 seconds",
            "sql": f"SELECT url, count(*) AS hits, round(avg(ttl), 1) AS avg_ttl_s, round(avg(age), 1) AS avg_age_s FROM {table_name} WHERE cache LIKE 'HIT%' AND ttl < 60 GROUP BY url ORDER BY hits DESC LIMIT 25",
            "category": "Cache",
        },
        # Origin (4)
        {
            "name": "Origin 5xx errors by URL",
            "description": "URLs returning server errors (HTTP 500-599)",
            "sql": f"SELECT url, status, count(*) AS error_count FROM {table_name} WHERE status >= 500 GROUP BY url, status ORDER BY error_count DESC LIMIT 50",
            "category": "Origin",
        },
        {
            "name": "Backend traffic & error breakdown",
            "description": "Requests and 5xx error counts per origin backend",
            "sql": f"SELECT backend, count(*) AS requests, sum(CASE WHEN status >= 500 THEN 1 ELSE 0 END) AS errors_5xx FROM {table_name} WHERE backend IS NOT NULL GROUP BY backend ORDER BY requests DESC",
            "category": "Origin",
        },
        {
            "name": "Slowest origin backends",
            "description": "Average and max latency grouped by backend",
            "sql": f"SELECT backend, count(*) AS requests, round(avg(elapsed) / 1000.0, 2) AS avg_ms, round(max(elapsed) / 1000.0, 2) AS max_ms FROM {table_name} WHERE backend IS NOT NULL GROUP BY backend ORDER BY avg_ms DESC",
            "category": "Origin",
        },
        {
            "name": "Edge vs shield request split",
            "description": "Comparison of edge requests versus origin-shield requests",
            "sql": f"SELECT edge, cache, count(*) AS requests FROM {table_name} GROUP BY edge, cache ORDER BY requests DESC",
            "category": "Origin",
        },
        # Security (5)
        {
            "name": "Top client IPs by request volume",
            "description": "Highest-volume client IP addresses and 4xx/5xx error counts",
            "sql": f"SELECT ip, country, asn, count(*) AS requests, sum(CASE WHEN status >= 400 THEN 1 ELSE 0 END) AS errors FROM {table_name} GROUP BY ip, country, asn ORDER BY requests DESC LIMIT 50",
            "category": "Security",
        },
        {
            "name": "JA4 TLS fingerprint distribution",
            "description": "Top JA4 client TLS fingerprints and distinct IP counts",
            "sql": f"SELECT ja4, count(*) AS requests, count(DISTINCT ip) AS distinct_ips FROM {table_name} WHERE ja4 IS NOT NULL AND ja4 != '' GROUP BY ja4 ORDER BY requests DESC LIMIT 25",
            "category": "Security",
        },
        {
            "name": "Authentication 401/403 spikes",
            "description": "Client IPs triggering HTTP 401 or 403 responses",
            "sql": f"SELECT ip, url, status, count(*) AS denied_count FROM {table_name} WHERE status IN (401, 403) GROUP BY ip, url, status ORDER BY denied_count DESC LIMIT 50",
            "category": "Security",
        },
        {
            "name": "404 content discovery scanners",
            "description": "Client IPs hitting the highest number of distinct 404 URLs",
            "sql": f"SELECT ip, count(*) AS total_404s, count(DISTINCT url) AS distinct_404_urls FROM {table_name} WHERE status = 404 GROUP BY ip HAVING count(DISTINCT url) >= 5 ORDER BY distinct_404_urls DESC LIMIT 25",
            "category": "Security",
        },
        {
            "name": "WAF blocked & flagged requests",
            "description": "Requests where WAF inspection triggered or blocked traffic",
            "sql": f"SELECT ip, url, waf_resp, status, count(*) AS hits FROM {table_name} WHERE waf = true OR waf_resp > 0 GROUP BY ip, url, waf_resp, status ORDER BY hits DESC LIMIT 50",
            "category": "Security",
        },
        # Network (4)
        {
            "name": "Traffic by country",
            "description": "Request volume and bandwidth grouped by client country",
            "sql": f"SELECT country, count(*) AS requests, sum(resp_bytes) AS total_bytes FROM {table_name} GROUP BY country ORDER BY requests DESC LIMIT 50",
            "category": "Network",
        },
        {
            "name": "Top autonomous systems (ASNs)",
            "description": "Request count and egress bytes by client ASN",
            "sql": f"SELECT asn, count(*) AS requests, sum(resp_bytes) AS total_bytes FROM {table_name} WHERE asn IS NOT NULL GROUP BY asn ORDER BY requests DESC LIMIT 50",
            "category": "Network",
        },
        {
            "name": "Transport protocol mix (HTTP/3 vs HTTP/2)",
            "description": "Breakdown of HTTP protocol versions and transport layer",
            "sql": f"SELECT proto, transport, count(*) AS requests FROM {table_name} GROUP BY proto, transport ORDER BY requests DESC",
            "category": "Network",
        },
        {
            "name": "Proxy & hosting network traffic",
            "description": "Requests classified by proxy type and connection speed",
            "sql": f"SELECT p_type, c_type, count(*) AS requests FROM {table_name} WHERE p_type IS NOT NULL AND p_type != '' GROUP BY p_type, c_type ORDER BY requests DESC LIMIT 25",
            "category": "Network",
        },
    ]


def _redact_paths(msg: str) -> str:
    return redact_error_text(msg)


def _console_masked(analyst_session: object) -> bool:
    """Masked identity unless the invite's policy EXPLICITLY sets ``mask_ips`` to false."""
    policy = getattr(analyst_session, "pii_policy", None)
    return not (isinstance(policy, dict) and policy.get("mask_ips") is False)


@router.post("/query", response_model=QueryResponse, response_model_exclude_unset=True)
def query_endpoint(
    req: QueryRequest,
    ctx: RequestContext = Depends(build_request_context),
):
    sql = req.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail=make_error("empty_sql", "No SQL provided"))

    analyst_session = ctx.analyst_session
    audit_session_id = getattr(analyst_session, "session_id", "admin") if analyst_session else "admin"

    start_time, end_time = ctx.clamp(None, None)
    mask_ips = mask_ips_for(analyst_session)

    high_scale_service = ctx.high_scale
    if high_scale_service is None:
        raise HTTPException(
            status_code=503,
            detail=make_error("high_scale_unavailable", "ClickHouse query service is not available for this service."),
        )

    query_client = None
    query_tables = None
    if analyst_session is not None:
        from backend.high_scale.postgres_control import get_console_clients

        try:
            console = get_console_clients().client_for(
                high_scale_service.service_id, masked=_console_masked(analyst_session)
            )
        except Exception:
            logger.warning("analyst console identity unavailable for %s", high_scale_service.service_id, exc_info=True)
            console = None
        if console is None:
            raise HTTPException(
                status_code=403,
                detail=make_error(
                    "console_unavailable_for_analysts",
                    "The SQL console is not available to analysts on this high-scale deployment.",
                ),
            )
        query_client, query_tables = console.client, console.tables
    from backend.high_scale.query import query_endpoint as hs_query

    try:
        return hs_query(
            high_scale_service,
            req,
            start_time,
            end_time,
            mask_ips,
            session_id=audit_session_id,
            client=query_client,
            tables=query_tables,
        )
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=make_error("sql_not_permitted", _redact_paths(str(e))))
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=make_error("query_failed", _redact_paths(str(e))),
        )


_FALLBACK_SCHEMA_COLUMNS: list[dict[str, str]] = [
    {"column": "timestamp", "type": "TIMESTAMPTZ"},
    {"column": "ip", "type": "VARCHAR"},
    {"column": "url", "type": "VARCHAR"},
    {"column": "host", "type": "VARCHAR"},
    {"column": "method", "type": "VARCHAR"},
    {"column": "status", "type": "INTEGER"},
    {"column": "cache", "type": "VARCHAR"},
    {"column": "country", "type": "VARCHAR"},
    {"column": "pop", "type": "VARCHAR"},
    {"column": "asn", "type": "INTEGER"},
    {"column": "ua", "type": "VARCHAR"},
]


@router.get("/query/schema/{service_id}", response_model=QuerySchemaResponse)
def query_schema_endpoint(
    service_id: ServiceId,
    ctx: RequestContext = Depends(build_request_context),
):
    """Reflect schema columns and types for the active service."""
    del service_id
    from backend import config as svcconfig

    name = ctx.source["name"]
    status = svcconfig.get_status(name) or {}
    schema = status.get("schema")

    if not schema:
        schema = list(_FALLBACK_SCHEMA_COLUMNS)

    if mask_ips_for(ctx.analyst_session):
        from backend.core.share_db import is_saved_view_pii_key

        schema = [
            col
            for col in schema
            if not is_saved_view_pii_key(str(col.get("name") or col.get("column") or ""))
            and str(col.get("name") or col.get("column") or "").strip().lower() != "origin_ip"
        ]

    return {
        "source": name,
        "schema": schema,
        "debug_queries": [],
        "debug_calls": [],
    }


@router.get("/presets", response_model=list[PresetQuery], response_model_exclude_unset=True)
def presets_endpoint(
    service_id: str | None = Depends(get_service_id),
    _scope: str | None = Depends(require_service_access),
):
    if not service_id:
        return []

    src = get_source_for_service(service_id)
    if not src:
        return []

    format_hash = (src.get("log_fields") or {}).get("format_hash")
    cached = get_cached_presets(service_id, format_hash)
    if cached is not None:
        return cached

    result = _get_presets(src=src)
    set_cached_presets(service_id, format_hash, result)
    return result
