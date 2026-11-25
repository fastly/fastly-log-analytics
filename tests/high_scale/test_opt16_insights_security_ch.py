"""Regression tests for Round 16 Elevation ClickHouse slow-query optimizations."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from backend.core.clickhouse.client import ClickHouseClient
from backend.high_scale import security
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator
from backend.high_scale.insights.batches import (
    _build_fingerprint_spread,
    _build_ip_abuse,
    _build_ip_outlier,
    _build_latency_dim,
    _build_network_asn,
    _build_url_perf,
    _build_window_url,
)
from backend.high_scale.insights.sql_queries import DETECTIONS, _base_sql
from backend.models.analytics import InsightsRequest


class _FakeRealClickHouseClient(ClickHouseClient):
    """Fake ClickHouseClient that triggers real-CH SQL paths."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._http = SimpleNamespace(_transport=object())

    def execute(self, query: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        self.calls.append((query, dict(params or {})))
        if "/* insight:history */" in query:
            now = datetime.now(UTC)
            return [
                {
                    "earliest": now - timedelta(days=8),
                    "window_total": 5000,
                    "hours": [],
                    "hour_rows": [],
                    "hour_checksums": [],
                }
            ]
        if "/* insight:fields */" in query:
            return [
                {
                    "fields": [
                        "ua",
                        "referer",
                        "resp_bytes",
                        "elapsed",
                        "ottfb",
                        "pop",
                        "city",
                        "region",
                        "asn",
                        "metro",
                        "ja4",
                        "tls_ciphers_sha",
                    ]
                }
            ]
        if "/* hs_security:overview_5in1 */" in query:
            return [
                {
                    "bucket_ts": 1700000000,
                    "total_rows": 100,
                    "tls_populated": 80,
                    "ipv6_rows": 25,
                    "h_map": (["<1KB", "1-2KB"], [70, 30]),
                    "c_map": (["1", "2-5"], [60, 40]),
                    "p_map": (["hosting"], [10]),
                }
            ]
        if "/* hs_security:top_ips_header */" in query:
            return [{"ip": "203.0.113.10", "max_header": 8192}]
        if "/* hs_security:proxies_combined */" in query:
            return [
                {
                    "active_proxies_count": 2,
                    "total_requests_count": 50,
                    "distance_mismatches_count": 1,
                    "q_tunnel": 10,
                    "q_wifi": 5,
                    "q_total": 100,
                    "isp_map": ([15169], [10]),
                    "top_clients": [],
                }
            ]
        return []


def _service(client: _FakeRealClickHouseClient) -> HighScaleService:
    now = datetime.now(UTC)
    return HighScaleService(
        service_id="svc-opt16",
        client=client,
        cursor_secret=b"opt16-test-secret-32-bytes-long!",
        request_watermark=lambda: now,
    )


def _sec_req() -> tuple[Any, str, str]:
    now = datetime.now(UTC)
    req = SimpleNamespace(
        filters={},
        filter_mode="include",
        filters_exclude={},
        status_codes=None,
        search_query=None,
        cache_status=None,
        countries=None,
        pops=None,
        methods=None,
        content_types=None,
        client_ips=None,
        user_agents=None,
        hosts=None,
        urls=None,
        referers=None,
        asns=None,
        ja3s=None,
        ja4s=None,
        tls_versions=None,
        tls_ciphers=None,
        http_protocols=None,
        waf_actions=None,
        waf_signals=None,
        is_bot=None,
        bot_categories=None,
        proxy_types=None,
        min_latency_ms=None,
        max_latency_ms=None,
        min_bytes=None,
        max_bytes=None,
        min_header_bytes=None,
        max_header_bytes=None,
    )
    return req, (now - timedelta(hours=24)).isoformat(), now.isoformat()


def test_security_overview_groups_by_bucket_only_and_splits_top_ips_header() -> None:
    client = _FakeRealClickHouseClient()
    req, start_s, end_s = _sec_req()
    result = security.security_aggregates(
        _service(client),
        req,
        start_s,
        end_s,
        sections={
            "fingerprint_coverage",
            "req_size_dist",
            "top_ips_header",
            "ipv6_adoption",
            "proxy_dist",
            "conn_reuse_dist",
        },
    )

    overview_calls = [sql for sql, _ in client.calls if "/* hs_security:overview_5in1 */" in sql]
    top_ip_calls = [sql for sql, _ in client.calls if "/* hs_security:top_ips_header */" in sql]
    assert len(overview_calls) == 1
    assert len(top_ip_calls) == 1

    overview_sql = overview_calls[0]
    assert "GROUP BY bucket_ts ORDER BY bucket_ts" in overview_sql
    assert "GROUP BY bucket_ts, client_ip" not in overview_sql
    assert "groupArraySortedIf" not in overview_sql

    top_ip_sql = top_ip_calls[0]
    assert "PREWHERE " in top_ip_sql
    assert "custom_fields['req_header_bytes'] != ''" in top_ip_sql

    dumped = result.model_dump()
    assert dumped["fingerprint_coverage"]["tls_ciphers_sha"] == 0.8
    assert dumped["top_ips_header"] == [{"ip": "203.0.113.10", "max_header": 8192}]


def test_security_bot_efficiency_and_proxies_optimize_prewhere_and_string_filters() -> None:
    client = _FakeRealClickHouseClient()
    req, start_s, end_s = _sec_req()
    result = security.security_aggregates(_service(client), req, start_s, end_s, sections={"bot_efficiency", "proxies"})

    bot_sql = next(sql for sql, _ in client.calls if "/* hs_security:bot_efficiency */" in sql)
    assert "multiSearchAnyCaseInsensitive(custom_fields['ua']" in bot_sql
    assert "NOT match(lower(custom_fields['ua'])" not in bot_sql
    assert "lower(extract(custom_fields['referer']" in bot_sql
    assert "FROM request_facts PREWHERE " in bot_sql
    assert "PREWHERE service_id={service_id:String}" in bot_sql

    proxies_sql = next(sql for sql, _ in client.calls if "/* hs_security:proxies_combined */" in sql)
    assert "AND custom_fields['rtt_min'] != ''" in proxies_sql
    assert "(SELECT count() FROM request_facts PREWHERE " in proxies_sql
    quality_by_label = {row["label"]: row["value"] for row in result.model_dump()["traffic_quality"]}
    assert quality_by_label == {"Direct Connection": 85.0, "Active Tunnel / Proxy": 10.0, "WiFi / Mobile": 5.0}


def test_security_aggregates_bounds_parallel_clickhouse_workers() -> None:
    src = inspect.getsource(security.security_aggregates)
    assert "max_workers=4" in src


def test_insights_standalone_queries_use_prewhere_and_avoid_expensive_scans() -> None:
    crawler_sql = _base_sql(DETECTIONS["crawler_disparity"], "crawler_disparity")
    assert "FROM request_facts PREWHERE " in crawler_sql
    assert "GROUP BY ua)" in crawler_sql
    assert "IN (SELECT lower(ua) FROM base" not in crawler_sql
    assert "uniqCombinedState(ip) AS ip_state" in crawler_sql
    assert "uniqCombinedMergeIf(h.ip_state," in crawler_sql
    assert "replaceRegexpAll(c.bot_stem" not in crawler_sql.split("combined AS")[1]
    assert "multiSearchAnyCaseInsensitive(ua," in crawler_sql

    stale_sql = _base_sql(DETECTIONS["stale_browser_version"], "stale_browser_version")
    assert "FROM request_facts PREWHERE " in stale_sql
    assert "IN (SELECT chrome_version FROM base" not in stale_sql
    assert "uniqCombinedIf(ip, is_w)" in stale_sql
    assert "multiSearchAnyCaseInsensitive(ua, ['bot', 'crawler', 'spider'])" in stale_sql

    shield_sql = _base_sql(DETECTIONS["shield_path_degradation"], "shield_path_degradation")
    assert "FROM request_facts PREWHERE " in shield_sql
    assert "(rid != '' OR prid != '')" in shield_sql
    assert "lower(edge)" not in shield_sql


def test_insights_batches_prune_window_partitions_and_precompute_image_predicate() -> None:
    present = {
        "url",
        "elapsed",
        "ttl",
        "hits",
        "age",
        "status",
        "resp_bytes",
        "ua",
        "oretries",
        "pop",
        "server_region",
        "ottfb",
        "city",
        "region",
        "country",
        "resp_header_content_encoding",
        "ip",
        "req_header_bytes",
        "conn_requests",
        "cookie_session",
        "referer",
        "asn",
        "metro",
        "ploss",
        "rtt_var",
        "retrans",
        "tcp_rtt",
        "delivery_rate",
        "ja4",
        "p_type",
        "tls_ciphers_sha",
    }

    win_url_sql, _ = _build_window_url(
        {"tail_latency", "image_optimization_opportunities"}, present=present, rollup_span=None, filter_clause=""
    )
    assert "AS _is_img" in win_url_sql
    assert "countIf(_is_img)" in win_url_sql
    assert "multiSearchAnyCaseInsensitive(url," in win_url_sql
    assert "ILIKE" not in win_url_sql

    lat_sql, _ = _build_latency_dim(
        {"pop_latency_regression", "region_latency", "city_latency_regressions"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert lat_sql.count("GROUP BY custom_fields['city'] HAVING count() >= 10") == 1
    assert "event_timestamp >= {window_start:DateTime64(3)}" in lat_sql

    url_sql, _ = _build_url_perf(
        {"latency_regression", "payload_compression_regression"}, present=present, rollup_span=None, filter_clause=""
    )
    assert "FROM base WHERE is_w" not in url_sql
    assert "multiSearchAnyCaseInsensitive(resp_header_content_encoding," in url_sql

    ip_out_sql, _ = _build_ip_outlier(
        {"request_size_anomaly", "connection_abuse"}, present=present, rollup_span=None, filter_clause=""
    )
    assert "FROM base WHERE is_w" not in ip_out_sql
    assert "IN (SELECT client_ip FROM request_facts PREWHERE " in ip_out_sql

    ip_abuse_sql, _ = _build_ip_abuse(
        {"content_discovery", "credential_enumeration", "session_harvesting", "orphaned_deep_crawl"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "FROM base WHERE is_w" not in ip_abuse_sql
    assert "IN (SELECT client_ip FROM request_facts PREWHERE " in ip_abuse_sql

    asn_sql, _ = _build_network_asn(
        {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "GROUP BY custom_fields['asn'] HAVING count() >= 20" in asn_sql
    assert "GROUP BY custom_fields['metro'] HAVING count() >= 20" in asn_sql

    fp_sql, _ = _build_fingerprint_spread(
        {"botnet_grouping", "cipher_spread", "residential_fingerprint_dispersion"},
        present=present,
        rollup_span=None,
        filter_clause="",
    )
    assert "HAVING uniqCombined(client_ip) >= 4" in fp_sql


def test_insights_metadata_queries_use_prewhere_and_ch248_safe_sql() -> None:
    client = _FakeRealClickHouseClient()
    evaluator.insights_endpoint(
        _service(client),
        InsightsRequest(window_size_hrs=24, baseline_hours=168),
        None,
        None,
        force_refresh=True,
    )
    history_sql = next(sql for sql, _ in client.calls if "/* insight:history */" in sql)
    fields_sql = next(sql for sql, _ in client.calls if "/* insight:fields */" in sql)
    assert "FROM request_facts PREWHERE " in history_sql
    assert "FROM request_facts PREWHERE " in fields_sql
    for sql, _ in client.calls:
        assert ":Array(String)}" not in sql
        assert "} BY " not in sql
