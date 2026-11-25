"""Regression tests for Opt 17 ClickHouse network, insights, prewarmer, and security optimizations."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest

from backend.core.clickhouse.constants import _QUERY_SETTINGS
from backend.high_scale import security
from backend.high_scale.ingest_controller import HighScaleService
from backend.high_scale.insights import evaluator as hs_evaluator
from backend.high_scale.insights.batches import (
    _baseline_sample_stride,
    _build_fingerprint_spread,
    _build_ip_abuse,
    _build_ip_outlier,
    _build_latency_dim,
    _build_network_asn,
    _build_origin_perf,
    _build_url_perf,
)
from backend.high_scale.insights.sql_queries import DETECTIONS, _base_sql
from backend.high_scale.network import (
    _network_json_cache,
    _network_memo,
    _network_model_cache,
    _network_time_sample_clause,
    _shielding_analysis,
    get_pop_health,
    get_transport_metrics,
    network_health,
    network_quality,
)
from backend.models.analytics import NetworkHealthRequest, NetworkQualityRequest


class _RecordingCHClient:
    def __init__(self, handler: Any = None) -> None:
        self.calls: list[tuple[str, dict[str, Any], dict[str, str] | None]] = []
        self._handler = handler
        self._http = SimpleNamespace(_transport=SimpleNamespace())

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        settings: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(params or {}), dict(settings) if settings else None))
        if callable(self._handler):
            return self._handler(sql, params or {})
        return list(self._handler or [])


def _clear_network_caches() -> None:
    _network_memo.cache.clear()
    _network_model_cache.clear()
    _network_json_cache.clear()


def test_network_time_sample_clause_uses_granule_skipping_event_timestamp() -> None:
    assert _network_time_sample_clause(1) == ""
    assert _network_time_sample_clause(20) == " AND intDiv(toUnixTimestamp(event_timestamp), 30) % 20 = 0"
    assert (
        _network_time_sample_clause(20, bucket_seconds=300)
        == " AND intDiv(toUnixTimestamp(event_timestamp), 15) % 20 = 0"
    )
    assert (
        _network_time_sample_clause(100, bucket_seconds=3600)
        == " AND intDiv(toUnixTimestamp(event_timestamp), 30) % 100 = 0"
    )
    assert _network_time_sample_clause(100, bucket_seconds=60) == " AND line_ordinal % 100 = 0"


def test_network_health_uses_time_block_sampling_and_empty_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 25, 13, 0, tzinfo=UTC)
    start = end - timedelta(hours=24)

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "FROM request_aggregates" in sql:
            return [{"asn": 13335, "cnt": 1000}]
        if "intDiv(toUnixTimestamp(event_timestamp), 15) % 20 = 0" in sql:
            return []
        if "AS rtt_quants" in sql and "GROUP BY asn, bucket_ts" in sql:
            return [
                {
                    "asn": 13335,
                    "bucket_ts": int(start.timestamp()),
                    "reqs": 10,
                    "throughput_bps": 1000.0,
                    "rtt_avg_us": 20000.0,
                    "rtt_quants": [10000.0, 20000.0, 40000.0, 60000.0],
                    "rtt_baseline_us": 15000.0,
                    "rtt_congestion_us": 5000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1000.0,
                    "error_pct": 0.0,
                    "speed_map": {},
                }
            ]
        return []

    client = _RecordingCHClient(_handler)
    service = HighScaleService(service_id="svc-nh", client=client, cursor_secret=b"s", request_watermark=None)
    req = NetworkHealthRequest(sections=["heatmap", "map_buckets"], bucket_seconds=300)
    resp = network_health(service, req, start.isoformat(), end.isoformat())

    fact_sqls = [sql for sql, _, _ in client.calls if "FROM request_facts" in sql]
    assert any("intDiv(toUnixTimestamp(event_timestamp), 15) % 20 = 0" in s for s in fact_sqls)
    assert not any("line_ordinal % 20" in s for s in fact_sqls)
    assert any("intDiv(toUnixTimestamp(event_timestamp), 15) % 20 = 0" not in s for s in fact_sqls)
    assert resp.available is True


def test_pop_health_quality_transport_and_shielding_use_granule_skipping_and_fast_predicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_network_caches()
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    end = datetime(2026, 11, 25, 13, 0, tzinfo=UTC)
    start_24h = end - timedelta(hours=24)
    start_7d = end - timedelta(days=7)

    pop_client = _RecordingCHClient(
        [
            {
                "kind": "rollup",
                "pop": "SJC",
                "req_count": 2000,
                "errors": 10,
                "p50_rtt_us": 12000.0,
                "p95_ttfb_ms": 40.0,
                "cache_hits": 0,
                "bandwidth_bytes": 0,
            },
            {
                "kind": "fact",
                "pop": "SJC",
                "req_count": 100,
                "errors": None,
                "p50_rtt_us": None,
                "p95_ttfb_ms": None,
                "cache_hits": 80,
                "bandwidth_bytes": 50000,
            },
        ]
    )
    pop_svc = HighScaleService(service_id="svc-pop", client=pop_client, cursor_secret=b"s", request_watermark=None)
    pop_res = get_pop_health(pop_svc, start_24h, end)
    pop_sql = pop_client.calls[0][0]
    assert "intDiv(toUnixTimestamp(event_timestamp), 30) % 20 = 0" in pop_sql
    assert "line_ordinal % 20" not in pop_sql
    assert "upper(custom_fields['cache'])" not in pop_sql
    assert "startsWith(custom_fields['cache'], 'HIT')" in pop_sql
    assert pop_res.data[0].requests == 2000

    q_client = _RecordingCHClient(
        lambda sql, _p: (
            [
                {"dimension": "country", "label": "US", "rtt_ms": 20.0, "reqs": 100},
                {"dimension": "asn", "label": "13335", "rtt_ms": 20.0, "reqs": 100},
            ]
            if "FROM network_minute_dimensions" in sql
            else [{"rtt_ms": 20.0, "ttfb_ms": 50.0, "cache_state": "HIT"}] * 200
        )
    )
    q_svc = HighScaleService(service_id="svc-q", client=q_client, cursor_secret=b"s", request_watermark=None)
    network_quality(q_svc, NetworkQualityRequest(), start_24h.isoformat(), end.isoformat())
    q_sqls = [sql for sql, _, _ in q_client.calls]
    rollup_union = next(s for s in q_sqls if "FROM network_minute_dimensions" in s)
    assert "intDiv(toUnixTimestamp(event_timestamp), 30) % 20 = 0" in rollup_union
    assert "line_ordinal % 20" not in rollup_union
    scatter_q = next(s for s in q_sqls if "AS ttfb_ms" in s)
    assert scatter_q.index("custom_fields['tcp_rtt'] != ''") < scatter_q.index("line_ordinal % 64 = 0")

    tr_client = _RecordingCHClient(
        [
            {
                "total_reqs": 50,
                "total_bytes": 50000,
                "rtt_quants": [10000.0, 20000.0, 30000.0, 40000.0],
                "avg_retrans": 0.1,
                "avg_ploss_pct": 0.05,
                "avg_del_rate_mbps": 12.5,
                "proto_map": {"HTTP/2": 50},
            }
        ]
    )
    tr_svc = HighScaleService(service_id="svc-tr", client=tr_client, cursor_secret=b"s", request_watermark=None)
    get_transport_metrics(tr_svc, start_24h.isoformat(), end.isoformat(), {})
    assert "intDiv(toUnixTimestamp(event_timestamp), 30) % 20 = 0" in tr_client.calls[0][0]
    assert "line_ordinal % 20" not in tr_client.calls[0][0]

    sh_client = _RecordingCHClient(
        lambda sql, _p: (
            [{"n": 1}]
            if "SELECT count() AS n" in sql
            else [
                {
                    "edge_pop": "SJC",
                    "shield_pop": "IAD",
                    "requests": 100,
                    "p50_ms": 10.0,
                    "p95_ms": 20.0,
                    "p99_ms": 30.0,
                }
            ]
        )
    )
    sh_svc = HighScaleService(service_id="svc-sh", client=sh_client, cursor_secret=b"s", request_watermark=None)
    _shielding_analysis(sh_svc, start_7d, end, None)
    sh_join_sql = sh_client.calls[1][0]
    assert "lower(custom_fields['edge'])" not in sh_join_sql
    assert "custom_fields['edge'] IN ('true', '1', 'True', 'TRUE')" in sh_join_sql
    assert sh_join_sql.index("custom_fields['rid'] != ''") < sh_join_sql.index("cityHash64(custom_fields['rid']) % 100")
    assert "ifNotFinite(" in sh_join_sql


def test_insights_baseline_sampling_pushed_pre_filters_and_standalone_fixes() -> None:
    assert _baseline_sample_stride(24.0) == 1
    assert _baseline_sample_stride(48.0) == 5
    assert _baseline_sample_stride(168.0) == 10

    present = {
        "url",
        "elapsed",
        "status",
        "resp_bytes",
        "resp_header_content_encoding",
        "pop",
        "server_region",
        "ottfb",
        "oconnect_ms",
        "city",
        "region",
        "country",
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

    sample_clause = (
        "event_timestamp >= {window_start:DateTime64(3)} OR intDiv(toUnixTimestamp(event_timestamp), 60) % 10 = 0"
    )
    lat_sql, _ = _build_latency_dim(
        {"pop_latency_regression"}, present=present, rollup_span=None, filter_clause="", baseline_hours=168.0
    )
    assert sample_clause in lat_sql
    assert "countIf(is_b) * 10 AS b_total" in lat_sql

    url_sql, _ = _build_url_perf(
        {"latency_regression", "payload_compression_regression"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
    )
    assert sample_clause in url_sql
    assert "countIf(is_b AND elapsed IS NOT NULL) * 10 AS l_b_total" in url_sql

    origin_sql, _ = _build_origin_perf(
        {"origin_latency_spike", "timeout_split"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
    )
    assert sample_clause in origin_sql
    assert "custom_fields['ottfb'] != ''" in origin_sql
    assert "countIf(is_b AND oconnect_ms IS NOT NULL) * 10 AS ts_b_total" in origin_sql

    asn_sql, _ = _build_network_asn(
        {"network_asn_health", "asn_metro_performance", "metro_delivery_degradation"},
        present=present,
        rollup_span=None,
        filter_clause="",
        baseline_hours=168.0,
    )
    assert sample_clause in asn_sql
    assert "countIf(is_b) * 10 AS nah_b_total" in asn_sql

    ip_out_sql, _ = _build_ip_outlier(
        {"request_size_anomaly", "connection_abuse"}, present=present, rollup_span=None, filter_clause=""
    )
    assert ip_out_sql.index("client_ip IN (SELECT client_ip") < ip_out_sql.index("FROM base")

    ip_abuse_sql, _ = _build_ip_abuse(
        {"content_discovery", "session_harvesting"}, present=present, rollup_span=None, filter_clause=""
    )
    assert ip_abuse_sql.index("client_ip IN (SELECT client_ip") < ip_abuse_sql.index("FROM base")

    fp_sql, _ = _build_fingerprint_spread(
        {"botnet_grouping", "cipher_spread"}, present=present, rollup_span=None, filter_clause=""
    )
    assert fp_sql.index("IN (SELECT custom_fields['ja4']") < fp_sql.index("FROM base")

    crawler_sql = _base_sql(DETECTIONS["crawler_disparity"], "crawler_disparity")
    assert "FROM base WHERE is_w" not in crawler_sql
    assert "event_timestamp >= {window_start:DateTime64(3)}" in crawler_sql

    low_slow_sql = _base_sql(DETECTIONS["low_and_slow"], "low_and_slow")
    assert "uniqCombined(url)" in low_slow_sql
    assert "uniqExact(url)" not in low_slow_sql

    shield_sql = _base_sql(DETECTIONS["shield_path_degradation"], "shield_path_degradation")
    assert "custom_fields['ottfb'] != ''" in shield_sql


def test_background_insights_throttles_clickhouse_max_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    assert "max_threads" in _QUERY_SETTINGS
    hs_evaluator._INSIGHTS_CACHE.clear()
    hs_evaluator._INSIGHTS_TELEMETRY_CACHE.clear()
    hs_evaluator._INSIGHTS_PARTIAL_CACHE.clear()
    hs_evaluator._INSIGHTS_LAST_RESULT.clear()
    hs_evaluator._INSIGHTS_REFRESH_FAILURES.clear()

    def _handler(sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        if "/* insight:history */" in sql:
            return [{"earliest": datetime(2026, 11, 1, 0, 0, tzinfo=UTC), "window_total": 1000, "hours": []}]
        if "/* insight:fields */" in sql:
            return [{"fields": ["status", "elapsed", "pop"]}]
        return []

    client = _RecordingCHClient(_handler)
    service = HighScaleService(service_id="svc-bg-threads", client=client, cursor_secret=b"s", request_watermark=None)
    req = SimpleNamespace(window_size_hrs=24, baseline_hours=168, filters=None)

    hs_evaluator.insights_endpoint(service, req, None, None, force_refresh=True)
    assert client.calls
    for _, _, call_settings in client.calls:
        assert call_settings is not None
        assert call_settings.get("max_threads") == hs_evaluator._BACKGROUND_QUERY_MAX_THREADS

    assert "max_workers=4" in inspect.getsource(security.security_aggregates)
