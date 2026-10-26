"""CMCD streaming row models keep every producer's wire bytes (repository + high-scale)."""

from __future__ import annotations

import json
import time
import typing
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd_aggregates
from backend.high_scale.insights import insights_endpoint as hs_insights_endpoint
from backend.high_scale.origin import aggregates as hs_aggregates
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.security import security_aggregates as hs_security_aggregates
from backend.high_scale.sessions import sessions_detail as hs_sessions_detail
from backend.models.dashboard import AggregatesResponse, SessionDetailRequest, SessionDetailResponse
from backend.models.insights import InsightCard, InsightsResponse
from backend.models.network import NetworkHealthResponse, NetworkQualityResponse
from backend.models.origin import (
    OriginAggregatesResponse,
    OriginDrilldownResponse,
    OriginIpHealthResponse,
    OriginPathBreakdownResponse,
    OriginPopLatencyResponse,
    OriginShieldingAnalysisResponse,
    OriginSlowUrlsResponse,
    OriginStatusCodesResponse,
    OriginSummaryResponse,
    OriginTimeseriesResponse,
)
from backend.models.performance import (
    CmcdAggregatesResponse,
    CmcdRequest,
    IOFormatConversionPair,
    IOOpportunityRow,
    PerformanceAggregatesResponse,
    TopBotRow,
    WafSignalRow,
)
from backend.models.security import SecurityAggregatesRequest, SecurityAggregatesResponse, SecurityTopBotsResponse
from backend.provision.cmcd_fields import _CMCD_CUSTOM_FIELDS
from backend.repositories.cmcd import _response_cache, get_cmcd_aggregates
from backend.repositories.performance import get_performance_aggregates
from pydantic import AliasChoices
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests._helpers.wire import assert_rows_wire_identical, assert_sections_wire_identical, wire

import backend  # noqa: F401 - register module aliases
from backend import config as svcconfig
from backend.core.duckdb import _clear_schema_cache
from backend.core.fastly.client import NgwafWorkspacesResponse, StatsRecord, StatsResponse
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.high_scale import network as hs_network
from backend.high_scale import performance as hs_performance
from backend.models.common import BaseResponse, DebugCall, DebugQuery
from backend.repositories import origin as origin_mod
from backend.repositories._base import _safe_table
from backend.repositories.dashboard import get_aggregates as dash_aggs
from backend.repositories.insights import _insights_cache, get_insights, registry
from backend.repositories.network import get_health, get_quality
from backend.repositories.origin import (
    get_ip_health,
    get_path_breakdown,
    get_pop_latency,
    get_shielding_analysis,
    get_slow_urls,
    get_status_codes,
    get_summary,
    get_timeseries,
)
from backend.repositories.security import get_security_aggregates, get_security_proxies, get_top_bots
from backend.repositories.sessions import get_session_detail
from backend.repositories.value import get_summary as get_value_summary
from tests.repositories.test_insights_golden import (
    _MOCK_POP_MAP,
    _create_impossible_distance_table,
    _seed_network_edge_data,
    _seed_track_c_data,
)


def _capture_with_telemetry(monkeypatch, model_cls) -> dict:
    captured: dict = {}

    def _capture(cls, **kwargs):
        captured.update(kwargs)
        return cls(**kwargs)

    monkeypatch.setattr(model_cls, "with_telemetry", classmethod(_capture))
    return captured


# ── Merged from tests/models/test_common.py ──
@pytest.mark.parametrize(
    "debug_env, by_alias, present_keys, absent_keys, cached_key",
    [
        ("", True, (), ("_debug_queries", "_debug_calls", "debug_queries", "debug_calls"), "_is_cached"),
        ("", False, (), ("debug_queries", "debug_calls", "_debug_queries", "_debug_calls"), "is_cached"),
        ("1", True, ("_debug_queries", "_debug_calls"), (), "_is_cached"),
        ("1", False, ("debug_queries", "debug_calls"), (), "is_cached"),
    ],
)
def test_base_response_telemetry_redaction_and_preservation(
    monkeypatch, debug_env, by_alias, present_keys, absent_keys, cached_key
):
    monkeypatch.setenv("DEBUG_RESPONSES", debug_env)
    resp = BaseResponse(
        debug_queries=[DebugQuery(sql="SELECT 1", time_ms=1.5)],
        debug_calls=[DebugCall(service="s3", method="GET", path="foo", time_ms=2.0)],
        is_cached=True,
    )
    data = resp.model_dump(by_alias=by_alias)
    assert all(k in data for k in present_keys) and all(k not in data for k in absent_keys) and data[cached_key] is True


@pytest.mark.parametrize("enabled", [False, True])
def test_base_response_debug_sqlite_redaction_and_preservation(monkeypatch, enabled):
    monkeypatch.setenv("DEBUG_RESPONSES", "1" if enabled else "")
    row = {"seq": 1, "sql": "SELECT 1", "time_ms": 0.4}
    resp = BaseResponse(debug_sqlite=[row])
    data_alias, data_no_alias = resp.model_dump(by_alias=True), resp.model_dump(by_alias=False)
    if enabled:
        assert data_alias["_debug_sqlite"] == [row] and "debug_sqlite" in data_no_alias
    else:
        assert not ({"_debug_sqlite", "debug_sqlite"} & (set(data_alias) | set(data_no_alias)))


def test_with_telemetry_pulls_sqlite_collector(monkeypatch):
    """with_telemetry snapshots the request-scoped SQLite collector and copies it."""
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    from backend.utils import telemetry

    telemetry.start_call_tracking()
    telemetry.record_sqlite_query({"seq": 9, "sql": "SELECT x FROM t", "time_ms": 1.2})
    try:
        resp = BaseResponse.with_telemetry()
        assert resp.debug_sqlite == [{"seq": 9, "sql": "SELECT x FROM t", "time_ms": 1.2}]
        telemetry.record_sqlite_query({"seq": 10, "sql": "SELECT y", "time_ms": 0.1})
        assert len(resp.debug_sqlite) == 1
    finally:
        telemetry._SQLITE_QUERIES.set(None)


def test_with_telemetry_skips_collectors_when_debug_responses_are_disabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    from backend.utils import telemetry

    def unexpected_collector():
        pytest.fail("Disabled diagnostics must not read telemetry collectors")

    for name in ("get_queries", "get_sqlite_queries", "get_tracked_calls"):
        monkeypatch.setattr(telemetry, name, unexpected_collector)

    dumped = BaseResponse.with_telemetry().model_dump(by_alias=True)
    debug_keys = (
        "_debug_queries",
        "_debug_calls",
        "_debug_sqlite",
        "_debug_postgres",
        "debug_queries",
        "debug_calls",
        "debug_sqlite",
        "debug_postgres",
    )
    assert not any(key in dumped for key in debug_keys) and dumped["_is_cached"] is False


def test_debug_query_params_absent_from_wire_unless_set(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    producer_row = {"sql": "SELECT 1", "time_ms": 1.5, "engine": "DuckDB"}
    plain = BaseResponse(debug_queries=[producer_row]).model_dump(by_alias=True, mode="json")
    assert plain["_debug_queries"] == [{**producer_row, "is_cached": False}]
    bound = BaseResponse(debug_queries=[{**producer_row, "params": ["a", 1]}]).model_dump(by_alias=True, mode="json")
    assert bound["_debug_queries"][0]["params"] == ["a", 1]


def test_security_proxies_and_request_validation():
    data = {
        "active_proxies_count": 42,
        "tunnel_requests_count": 9800,
        "distance_mismatches_count": 5,
        "traffic_quality": [
            {"label": "Direct Connection", "value": 81.6},
            {"label": "WiFi / Mobile", "value": 15.3},
            {"label": "Active Tunnel / Proxy", "value": 3.1},
        ],
        "suspicious_isps": [
            {"isp": "DigitalOcean, LLC", "asn": 14061, "count": 120},
            {"isp": "Linode, LLC", "asn": 63949, "count": 80},
        ],
        "active_clients": [
            {
                "ip": "192.0.2.1",
                "risk_level": "High",
                "asn_name": "DigitalOcean, LLC",
                "impossible_distance": True,
                "rtt_min_ms": 1.2,
                "tcp_rtt_ms": 120.5,
                "distance_km": 8400.2,
            }
        ],
    }
    resp = SecurityAggregatesResponse(**data)
    assert resp.active_proxies_count == 42 and len(resp.active_clients) == 1
    assert resp.active_clients[0].impossible_distance is True
    req = SecurityAggregatesRequest(start_time="2026-08-17T00:00:00Z", end_time="2026-08-17T23:59:59Z")
    assert req.start_time == "2026-08-17T00:00:00Z" and req.end_time == "2026-08-17T23:59:59Z"


def _src_empty():
    return {"name": "test_empty_src", "service_id": "test_empty_src"}


@pytest.mark.parametrize(
    "repo_fn, pydantic_model, extra_args",
    [
        (dash_aggs, AggregatesResponse, {"chart_interval": "1 hour", "chart_metric": "requests"}),
        (get_security_aggregates, SecurityAggregatesResponse, {"bucket_seconds": 3600}),
        (get_health, NetworkHealthResponse, {"bucket_seconds": 3600}),
        (get_quality, NetworkQualityResponse, {}),
        (get_summary, OriginSummaryResponse, {}),
        (get_performance_aggregates, PerformanceAggregatesResponse, {}),
        (get_timeseries, OriginTimeseriesResponse, {}),
        (get_slow_urls, OriginSlowUrlsResponse, {}),
        (get_status_codes, OriginStatusCodesResponse, {}),
        (get_path_breakdown, OriginPathBreakdownResponse, {}),
        (get_pop_latency, OriginPopLatencyResponse, {}),
        (get_ip_health, OriginIpHealthResponse, {}),
        (get_shielding_analysis, OriginShieldingAnalysisResponse, {}),
    ],
)
def test_repository_keys_match_pydantic_model(in_memory_duckdb, repo_fn, pydantic_model, extra_args):
    """Ensure repository return dictionaries match Pydantic response model keys."""
    src = _src_empty()
    if repo_fn.__name__ in ("get_health", "get_quality"):
        repo_result = repo_fn(
            con_factory=lambda: in_memory_duckdb, src=src, start_time=None, end_time=None, filters={}, **extra_args
        )
    else:
        repo_result = repo_fn(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={}, **extra_args)

    repo_keys = set(repo_result.keys()) - {"_debug_queries", "_debug_calls", "_is_cached"}
    parsed = pydantic_model.model_validate(repo_result)
    model_keys = set(pydantic_model.model_fields.keys())
    for name in parsed.model_fields_set:
        alias = pydantic_model.model_fields[name].validation_alias
        if isinstance(alias, str):
            model_keys.add(alias)
        elif isinstance(alias, AliasChoices):
            model_keys.update(choice for choice in alias.choices if isinstance(choice, str))

    missing_in_model = repo_keys - model_keys
    assert not missing_in_model, f"{pydantic_model.__name__} missing fields: {missing_in_model}"


def test_stats_record_and_response_validation():
    rec = StatsRecord.model_validate(
        {
            "start_time": 1760000000,
            "object_storage_class_a_operations_count": 10,
            "object_storage_class_b_operations_count": "20",
            "some_future_field": {"x": 1},
        }
    )
    assert (
        rec.fos_ops() == (10, 20) and rec.start_time == 1760000000 and rec.model_extra["some_future_field"] == {"x": 1}
    )
    nested = StatsRecord.model_validate(
        {"object_storage": {"class_a_operations_count": 3, "class_b_operations_count": 4}}
    )
    assert nested.fos_ops() == (3, 4)
    coerced = StatsRecord.model_validate({"requests": None, "edge_requests": "n/a", "start_time": None})
    assert coerced.requests == 0 and coerced.edge_requests == 0 and coerced.start_time is None
    assert StatsResponse.model_validate({}).data == [] and StatsResponse.model_validate({"data": None}).data == []
    assert StatsResponse.model_validate({"data": [{"requests": 5}], "meta": {"to": 1}}).data[0].requests == 5


@pytest.mark.parametrize("payload", [{}, {"object_storage": None}, {"object_storage": "bad"}])
def test_stats_record_no_fos_data_is_zero(payload):
    assert StatsRecord.model_validate(payload).fos_ops() == (0, 0)


def test_ngwaf_workspaces_data_shape_and_name_fallbacks():
    resp = NgwafWorkspacesResponse.model_validate(
        {
            "data": [{"id": "w1", "name": "Prod"}, {"id": "w2", "attributes": {"name": "Attr Name"}}, {"id": "w3"}],
            "meta": {"limit": 100},
        }
    )
    assert resp.workspaces_list() == [
        {"id": "w1", "name": "Prod"},
        {"id": "w2", "name": "Attr Name"},
        {"id": "w3", "name": "w3"},
    ]
    assert NgwafWorkspacesResponse.model_validate({"workspaces": [{"id": "a", "name": "A"}]}).workspaces_list() == [
        {"id": "a", "name": "A"}
    ]
    assert NgwafWorkspacesResponse.model_validate({"data": [], "workspaces": [{"id": "a"}]}).workspaces_list() == []


# ── Merged from tests/models/test_wire_cmcd.py ──
SECTIONS = (
    "overview",
    "sessions_ts",
    "buffer_health_ts",
    "bitrate_ts",
    "throughput_ts",
    "startup_ts",
    "top_content",
    "rebuffer_by_country",
    "rebuffer_by_asn",
    "object_type_dist",
    "streaming_format_dist",
    "session_duration_dist",
)


def _seed(con, src, n=60):
    table = _safe_table(src["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    parts = [f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields] + [
        f'"{cf["name"]}" {cf["duckdb_type"]}' for cf in _CMCD_CUSTOM_FIELDS
    ]
    con.execute(f"CREATE TABLE {table} ({', '.join(parts)})")
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log.update(
            cmcd_sid=f"session_{i % 5}",
            cmcd_bl=15 + i,
            cmcd_br=4500 + i * 100,
            cmcd_bs=(i % 4 == 0),
            cmcd_mtp=6000 + i * 200,
            cmcd_ot=["v", "a"][i % 2],
            cmcd_sf="h",
            cmcd_su=(i % 10 == 0),
            cmcd_tb=5000 + i * 100,
            cmcd_rtp=4800 + i * 100,
            cmcd_cid=f"content_{i % 3}",
            cmcd_dl=50 + i,
            country="US" if i % 2 == 0 else "GB",
            asn=7922 if i % 2 == 0 else 3320,
        )
    insert_mock_logs(con, table, logs)


class TestWireCmcd:
    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        _response_cache.clear()
        yield
        _response_cache.clear()

    @pytest.mark.parametrize("windowed", [False, True])
    def test_repository_wire_identical(self, in_memory_duckdb, test_service_source, monkeypatch, windowed):
        _seed(in_memory_duckdb, test_service_source)
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
        now = datetime.now(UTC)
        start, end = ((now - timedelta(hours=3)).isoformat(), now.isoformat()) if windowed else (None, None)
        res = get_cmcd_aggregates(
            con=in_memory_duckdb,
            src=test_service_source,
            start_time=start,
            end_time=end,
            filters={},
            bucket_seconds=600,
        )
        assert_sections_wire_identical(CmcdAggregatesResponse, res, non_empty=SECTIONS)

    def test_repository_unavailable_wire_identical(self, in_memory_duckdb):
        src = {"name": "cmcd_wire_missing", "service_id": "cmcd_wire_missing"}
        res = get_cmcd_aggregates(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={})
        assert_sections_wire_identical(CmcdAggregatesResponse, res)

    def test_high_scale_wire_identical(self, monkeypatch):
        monkeypatch.setattr("backend.high_scale.cmcd._asn_names", lambda sid, asns: {7922: "Comcast"})
        present = {
            "events": 9,
            "sessions": 3,
            **{f"has_{k}": 1 for k in ("bl", "br", "bs", "mtp", "ot", "sf", "su", "tb", "cid", "country", "asn")},
        }
        bucket = "2026-09-14 10:05:00"
        canned = {
            "cmcd:probe": [present],
            "cmcd:overview_peak": [{"peak_viewers": 3}],
            "cmcd:overview_duration": [{"avg_duration": 90.0}],
            "cmcd:overview": [
                {
                    "active_sessions": 3,
                    "rebuffer_rate": 33.3,
                    "avg_bitrate": 4000.0,
                    "avg_buffer_length": 20.0,
                    "median_throughput": 7000.0,
                }
            ],
            "cmcd:timeseries": [
                {
                    "bucket": bucket,
                    "concurrent_sessions": 3,
                    "rebuffer_session_pct": 33.3,
                    "p50_buffer": 10.0,
                    "p95_buffer": 20.0,
                    "starvation_rate": 5.0,
                    "avg_bitrate": 4000.0,
                    "utilization_ratio": 0.5,
                    "p50_throughput": 7000.0,
                    "p95_throughput": 9000.0,
                    "p99_throughput": 9500.0,
                    "startup_ratio": 2.0,
                }
            ],
            "cmcd:new_sessions": [{"bucket": bucket, "new_sessions": 3}],
            "cmcd:top_content": [
                {"content_id": "c1", "session_count": 3, "rebuffer_rate": 10.0, "avg_br": 4000.0, "avg_bl": 9.0}
            ],
            "cmcd:rebuffer_by_country": [
                {"country": "US", "rebuffer_rate": 10.0, "session_count": 3, "median_mtp": 7000.0}
            ],
            "cmcd:rebuffer_by_asn": [
                {"asn": 7922, "rebuffer_rate": 10.0, "session_count": 3, "avg_br": 4000.0, "avg_bl": 9.0}
            ],
            "cmcd:object_type_dist": [{"object_type": "v", "request_count": 5}],
            "cmcd:streaming_format_dist": [{"streaming_format": "h", "session_count": 3}],
            "cmcd:session_duration_dist": [{"sort_order": 3, "session_count": 3}],
        }

        class _Client:
            def execute(self, sql, params=None):
                for key, rows in canned.items():
                    if key in sql:
                        return rows
                raise AssertionError(sql)

        service = SimpleNamespace(service_id="svc", client=_Client())
        res = hs_cmcd_aggregates(
            service, CmcdRequest(bucket_seconds=300), "2026-09-14T10:00:00Z", "2026-09-14T10:10:00Z", None, False
        )
        assert res["has_data"] is True
        assert_sections_wire_identical(
            CmcdAggregatesResponse, res, non_empty=("overview",) + tuple(s for s in SECTIONS if s != "overview")
        )


# ── Merged from tests/models/test_wire_dashboard.py ──
def _seed_wire_dashboard(con, src, n=120):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log.update(
            elapsed=100_000 + (i * 7919) % 900_000,
            ttfb=f"{(50_000 + (i * 1013) % 400_000) / 1_000_000:.6f}",
            ottfb=20_000 + (i * 331) % 30_000,
            ottlb=60_000 + (i * 331) % 30_000,
            url=f"/path-{i % 4}",
            asn=[7922, 15169][i % 2],
            ttl=[0, 5, 45, 3600, 90000][i % 5],
            cache="HIT" if i % 3 else "MISS",
        )
    insert_mock_logs(con, _safe_table(src["name"]), logs)


class _HsClient:
    def execute(self, sql, params=None):
        row = {"value": "/slow", "req_count": 9, "avg_ms": 12.5, "p50_ms": 10.0, "p95_ms": 20.0, "p99_ms": 30.0}
        if "perf:top_urls" in sql:
            return [row]
        if "perf:top_asns" in sql:
            return [{**row, "value": "7922"}]
        if "perf:ttl_dist" in sql:
            return [{"bucket": "0s", "count": 3, "min_ttl": 0.0}, {"bucket": "<1h", "count": 4, "min_ttl": 45.0}]
        if "perf:waterfall" in sql:
            return [
                {
                    "component_count": 2,
                    "edge_processing": 1.0,
                    "origin_wait": 2.0,
                    "origin_download": 3.0,
                    "client_download": 4.0,
                }
            ]
        if "perf:scatter" in sql:
            return [{"origin_ms": 10.0, "edge_ms": 5.0, "cache": "HIT"}]
        return []


_START = "2020-01-01T00:00:00+00:00"
_END = "2100-01-01T00:00:00+00:00"


def _summary(con, src):
    logs = generate_mock_logs(src, num_logs=120, hours_ago=1)
    for i, log in enumerate(logs):
        log.update(
            waf_sig=["SQLI,XSS", "XSS", "VERIFIED-BOT.search-engine"][i % 3],
            _bot_name=["googlebot", "bingbot", ""][i % 3],
            url=f"/img/{i % 4}.jpg",
            io_input_bytes=100_000 + i,
            io_output_bytes=40_000 + i,
            io_input_format=["jpeg", "png"][i % 2],
            io_output_format=["webp", "avif"][i % 2],
            resp_bytes=50_000 + i,
        )
    insert_mock_logs(con, _safe_table(src["name"]), logs)
    return get_value_summary(con, src, None, None, {}, sections={"security", "bots", "io"}, service_id=None)


class TestWireDashboard:
    def test_repository_live_wire_identical(self, in_memory_duckdb, test_service_source, monkeypatch):
        _seed_wire_dashboard(in_memory_duckdb, test_service_source)
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
        res = get_performance_aggregates(
            con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
        )
        assert_sections_wire_identical(
            PerformanceAggregatesResponse, res, non_empty=("top_urls", "top_asns", "ttl_dist", "scatter", "waterfall")
        )

    def test_repository_rollup_hit_wire_identical(self, in_memory_duckdb, test_service_source, monkeypatch):
        _seed_wire_dashboard(in_memory_duckdb, test_service_source)
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})

        def _hit(self, *_a, **kw):
            value = "/rolled" if kw.get("dimension") == "url" else 7922
            return {
                "rows": [{"value": value, "requests": 100, "avg_ms": 1.0, "p50_ms": 1.0, "p95_ms": 2.0, "p99_ms": 3.0}]
            }

        with patch("backend.repositories._base.QueryRunner.try_perf_latency_from_rollup", _hit):
            res = get_performance_aggregates(
                con=in_memory_duckdb, src=test_service_source, start_time=None, end_time=None, filters={}
            )
        assert res["approx"] is True
        assert_sections_wire_identical(PerformanceAggregatesResponse, res, non_empty=("top_urls", "top_asns"))

    def test_repository_empty_schema_wire_identical(self, in_memory_duckdb):
        src = {"name": "perf_wire_missing", "service_id": "perf_wire_missing"}
        res = get_performance_aggregates(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={})
        assert_sections_wire_identical(PerformanceAggregatesResponse, res)

    def test_high_scale_wire_identical(self, monkeypatch):
        captured = _capture_with_telemetry(monkeypatch, PerformanceAggregatesResponse)
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
        hs_performance.performance_aggregates(
            SimpleNamespace(service_id="svc", client=_HsClient()), SimpleNamespace(), None, None
        )
        assert captured["top_asns"][0]["asn"] == 7922 and captured["top_asns"][0]["label"] == "Comcast (7922)"
        assert_sections_wire_identical(
            PerformanceAggregatesResponse,
            captured,
            non_empty=("top_urls", "top_asns", "ttl_dist", "scatter", "waterfall"),
        )

    def test_repository_detail_wire_identical_across_every_log_column(self, in_memory_duckdb, test_service_source):
        table = _safe_table(test_service_source["name"])
        logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
        for log in logs:
            log["ip"] = "203.0.113.7"
        insert_mock_logs(in_memory_duckdb, table, logs)

        res = get_session_detail(in_memory_duckdb, test_service_source, "203.0.113.7", _START, _END)
        raw_ids = {f["id"] for f in LOG_FIELD_CATALOG if f.get("vcl") is not None}
        assert raw_ids <= set(res["columns"]), "fixture must exercise every raw log column"
        assert {str, int, float} <= {type(v) for row in res["data"] for v in row.values()}
        assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data", "columns"))

    def test_repository_detail_keeps_booleans_and_nulls(self, in_memory_duckdb):
        src = {"name": "sess_wire_bool", "service_id": "sess_wire_bool"}
        table = _safe_table(src["name"])
        now = datetime.now(UTC)
        in_memory_duckdb.execute(
            f'CREATE TABLE {table} ("timestamp" TIMESTAMPTZ, "ip" VARCHAR, "flag" BOOLEAN, "n" BIGINT, "x" DOUBLE, "s" VARCHAR)'
        )
        in_memory_duckdb.execute(
            f"INSERT INTO {table} VALUES (?, '203.0.113.7', TRUE, 5, 2.0, NULL), (?, '203.0.113.7', FALSE, NULL, 0.5, 'a')",
            [now.isoformat(), (now + timedelta(seconds=1)).isoformat()],
        )
        res = get_session_detail(in_memory_duckdb, src, "203.0.113.7", _START, _END)
        assert [r["flag"] for r in res["data"]] == [True, False]
        assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data",))

    def test_high_scale_detail_wire_identical(self):
        ts = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)

        def _row(offset: int, url: str, status: int, asn: int | None) -> dict:
            return {
                "ip": "203.0.113.7",
                "ja4": "t13d1516h2_x",
                "timestamp": ts + timedelta(seconds=offset),
                "country": "US",
                "url": url,
                "status": status,
                "method": "GET",
                "protocol": "h2",
                "resp_bytes": 512,
                "tcp_rtt": 12.5,
                "edge": "1",
                "edge_sid": "sid-1",
                "ua": "curl/8",
                "asn": asn,
            }

        client = SimpleNamespace(execute=lambda q, p: [_row(0, "/a", 200, 64500), _row(2, "/b", 404, None)])
        service = type("Svc", (), {"client": client, "service_id": "svc"})()
        res = hs_sessions_detail(service, SessionDetailRequest(ip="203.0.113.7"), _START, _END, "203.0.113.7", None)  # type: ignore[arg-type]
        assert res["data"][0]["timestamp"].endswith("Z")
        assert_sections_wire_identical(SessionDetailResponse, res, non_empty=("data", "columns"))

    def test_non_scalar_cells_are_rejected(self):
        with pytest.raises(ValueError, match="data"):
            SessionDetailResponse.model_validate({"columns": ["a"], "data": [{"a": [1, 2]}]})

    def test_value_list_rows_wire_identical(self, in_memory_duckdb, test_service_source):
        res = _summary(in_memory_duckdb, test_service_source)
        assert_rows_wire_identical(WafSignalRow, res["security"]["top_waf_signals"])
        assert_rows_wire_identical(TopBotRow, res["bots"]["top_bots"])
        assert_rows_wire_identical(IOOpportunityRow, res["io"]["optimization_opportunities"])
        assert_rows_wire_identical(IOFormatConversionPair, res["io"]["io_format_conversion_pairs"])


# ── Merged from tests/models/test_wire_insights.py ──
_TAGS = {t.model_fields["id"].annotation.__args__[0] for t in typing.get_args(typing.get_args(InsightCard)[0])}  # type: ignore[union-attr]

_CONTEXT: dict[str, Any] = {
    "window_hours": 24.0,
    "baseline_hours": 168.0,
    "fp_col": "ja4",
    "actual_cols": {"ja4", "ip"},
    "mask_ips": False,
    "asn_names": {64500: "Example ISP"},
    "dma_map": {"501": "New York"},
}

_GEO = ("Berlin, DE", "Berlin", "BE", "DE")

_ROWS: dict[str, list[tuple]] = {
    "error_spikes": [("/api/x", 0.6, 0.01, 30, 50, 500)],
    "botnet_grouping": [("abc123", 80, 900, 2, 12.34)],
    "new_country_traffic": [("DE", 25, 0)],
    "city_surges": [(*_GEO, 500, 100, 5.55)],
    "city_error_spikes": [(*_GEO, 0.7, 0.01, 70, 100, 1000)],
    "city_latency_regressions": [(*_GEO, 6000.0, 1200.0, 300, 3000), (*_GEO, 6000.0, 0.0, 300, 3000)],
    "new_city_traffic": [(*_GEO, 60, 0), ("Tokyo, JP", "Tokyo", "", "JP", 60, 0)],
    "ua_monoculture": [("curl/8.0", 600, 10, 1000, 1000)],
    "new_probe_urls": [("/.env", 12, 0, 83.3), ("/.git", 12, 0, None)],
    "waf_signal_spikes": [("SQLI", 100, 20, 12.5)],
    "proxy_surge": [("vpn", 300, 10, 1000, 5000)],
    "asn_concentration": [(64500, 800, 20, 1000, 1000)],
    "asn_metro_performance": [(64500, 501, 150.0, 30.0, 400, 4000), (64500, 501, 150.0, 0.0, 400, 4000)],
    "cache_collapse": [("/a", 0.05, 0.9, 120, 1500)],
    "cacheability_regression": [("/a", 0.9, 0.05, 130, 1600)],
    "latency_regression": [("/a", 6000.0, 1000.0, 120, 1500), ("/a", 6000.0, 0.0, 120, 1500)],
    "impossible_distance": [
        ("fp1", 4, 7000.0, 8880.0, 200.0, "JFK", "1.2.3.4", 0.0, 0.0, 40.641, -73.778, 1000, "US", "Nowhere"),
        ("fp2", 2, 7000.0, 8880.0, 200.0, "JFK", None, 1.5, 2.5, 40.641, -73.778, 1000, None, None),
    ],
    "tail_latency": [("/a", 900.0, 50.0, 18.0, 40)],
    "cipher_spread": [("a" * 40, 60, 900, 12)],
    "request_size_anomaly": [("1.2.3.4", 70000, 5000.5, 30, 4000.0)],
    "connection_abuse": [("1.2.3.4", 700, 20.4, 30, 15.0)],
    "region_latency": [("us-east", 6000.0, 1000.0, 100, 1000, 200.5), ("us-west", 6000.0, 0.0, 100, 1000, None)],
    "cache_ttl_mismatch": [("/a", 86400.0, 1.2, 300.0, 40)],
    "image_optimization_opportunities": [
        ("/i.png", 500, 40_000_000, 80.5, 0.62),
        ("/j.png", 5, 3_000_000, 600.0, None),
    ],
    "origin_latency_spike": [("/a", 900.0, 900.0, 100.0, 80)],
    "origin_error_rate": [(503, 40, 400, 4000, 40, 5)],
    "origin_retries": [("/a", 40, 1.8, 5)],
    "origin_ip_failure": [("203.0.113.1", 300, 35.2, 1.04)],
    "shield_path_degradation": [("SJC", "IAD", 90.0, 20.0, 70), ("SJC", "IAD", 90.0, 0.0, 70)],
    "repeated_patterns": [
        ("1.2.3.4", 40, 41, 30.0, 0.5, 0.02, 0.95, 1, 1230, 30),
        ("1.2.3.5", 4, 5, 30.5, 9.5, 0.4, 0.2, 2, 0, None),
    ],
    "repeated_patterns_fp": [
        ("t13d1516h2_8daaf6152771", 40, 41, 30.0, 0.5, 0.02, 0.95, 7, 1230, 30),
        ("t13d1516h2_other", 4, 5, 30.5, 9.5, 0.4, 0.2, 2, 0, None),
    ],
    "low_and_slow": [("1.2.3.4", 12, 6, 3600, 0.0033)],
    "credential_enumeration": [("1.2.3.4", 150, 400, 5, 3), ("1.2.3.5", 5, 0, 1, 0)],
    "network_asn_health": [(64500, 0.08, 0.001, 5000.0, 1000.0, 2.5, 0.1, 60)],
    "content_discovery": [("1.2.3.4", 120, 300, 80, 2), ("1.2.3.5", 5, 0, 1, 0)],
    "referer_monoculture": [("https://example.com/", 600, 5, 1000, 1000)],
    "method_drift": [("POST", 300, 5, 1000, 1000), (None, 300, 5, 1000, 1000)],
    "new_asn_traffic": [(64501, 150, 0)],
    "asn_hosting_shift": [(64502, 80, 100, 10, 100)],
    "metro_delivery_degradation": [(501, 1_000_000.0, 10_000_000.0, 150)],
    "connection_type_mix": [("cellular", "mobile", 120, 5, 150, 300), (None, None, 120, 5, 150, 300)],
    "pop_latency_regression": [("JFK", 6000.0, 1000.0, 150, 900), ("JFK", 6000.0, 0.0, 150, 900)],
    "http3_fallback": [(0, 150, 240, 300)],
    "cache_hit_cliff": [(0, 150, 210, 300)],
    "payload_compression_regression": [("/app.js", 0.95, 0.02, 120, 300)],
    "session_harvesting": [("1.2.3.4", 120, 600, 3)],
    "timeout_split": [(500.0, 5.0, 60.0, 50.0, 60), (5.0, 5.0, 560.0, 60.0, 60)],
    "crawler_disparity": [
        ("GPTBot", 5000, 2, 10_240_000, 2500.0, 100.0, 5_120_000.0, 200_000.0, 42.5),
        (None, 50, 0, 0, 50.0, 1.0, 0.0, 0.0, 0.0),
    ],
    "stale_browser_version": [("Chrome/90", 300, 12, 6.5)],
    "orphaned_deep_crawl": [("1.2.3.4", 600, 20, 10, 30.0, 12.5)],
    "residential_fingerprint_dispersion": [("t13d1516h2_8daaf6152771", 40, 12, 900, 3, 4.5)],
}

_ENVELOPE: dict[str, Any] = {
    "window_start": "2026-09-14T00:00:00Z",
    "window_end": "2026-09-15T00:00:00Z",
    "baseline_start": "2026-09-07T00:00:00Z",
    "baseline_end": "2026-09-14T00:00:00Z",
    "computed_at": "2026-09-15T00:00:00Z",
    "window_hours": 24.0,
    "baseline_hours": 168.0,
}


def _dump_cards(cards: list[dict]) -> list[dict]:
    model = InsightsResponse.model_validate({**_ENVELOPE, "insights": cards})
    return json.loads(model.model_dump_json(by_alias=True))["insights"]


def _assert_cards_wire_identical(cards: list[dict]) -> int:
    """Every item's ``meta`` serializes to the producer's exact bytes; the other item keys match by value."""
    dumped_cards = _dump_cards(cards)
    checked = 0
    for card, dumped in zip(cards, dumped_cards, strict=True):
        assert dumped["id"] == card["id"] and len(dumped["items"]) == len(card["items"])
        for item, out in zip(card["items"], dumped["items"], strict=True):
            assert wire(out["meta"]) == wire(item["meta"]), f"{card['id']} meta wire bytes differ"
            for key, value in item.items():
                if key != "meta":
                    assert out[key] == pytest.approx(value) if isinstance(value, float) else out[key] == value
            checked += 1
    return checked


def _card_for(insight_id: str, items: list[dict]) -> dict:
    definition = registry.get(insight_id)
    assert definition is not None
    return {
        "id": insight_id,
        "title": definition.title,
        "description": definition.description,
        "category": str(definition.category),
        "severity": "warning",
        "summary": f"{len(items)} anomalies detected",
        "items": items,
    }


class TestWireInsights:
    def test_every_registered_insight_has_a_card_variant_and_a_row_case(self):
        registered = {d.id for d in registry.get_all()}
        assert registered == set(_ROWS), "add a synthetic producer row for the new insight"
        assert registered | {"high_scale_5xx"} == _TAGS, "add an InsightCard variant for the new insight"

    @pytest.mark.parametrize("insight_id", sorted(_ROWS))
    def test_processor_meta_wire_identical(self, insight_id):
        definition = registry.get(insight_id)
        assert definition is not None and definition.row_processor is not None
        items = [
            {
                **definition.row_processor(row, definition, dict(_CONTEXT)),
                "investigate_url": "/dashboard?service=svc&start=a&end=b",
            }
            for row in _ROWS[insight_id]
        ]
        assert _assert_cards_wire_identical([_card_for(insight_id, items)]) == len(items)

    def test_masked_ip_processors_keep_wire_bytes(self):
        for insight_id in ("request_size_anomaly", "repeated_patterns", "session_harvesting", "impossible_distance"):
            definition = registry.get(insight_id)
            assert definition is not None and definition.row_processor is not None
            item = definition.row_processor(_ROWS[insight_id][0], definition, {**_CONTEXT, "mask_ips": True})
            assert _assert_cards_wire_identical([_card_for(insight_id, [item])]) == 1

    def test_unknown_insight_id_is_rejected(self):
        with pytest.raises(ValueError, match="insights"):
            _dump_cards([{**_card_for("error_spikes", []), "id": "not_an_insight"}])

    def test_extra_meta_keys_survive_for_forward_compat(self):
        card = _card_for(
            "error_spikes", [{"label": "/a", "meta": {"requests": 1, "errors": 1, "filters": {}, "future": [1]}}]
        )
        assert _dump_cards([card])[0]["items"][0]["meta"]["future"] == [1]

    def test_repository_impossible_distance_wire_identical(self, in_memory_duckdb, test_service_source):
        _create_impossible_distance_table(in_memory_duckdb, _safe_table(test_service_source["name"]))
        _insights_cache.clear()
        with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value=_MOCK_POP_MAP):
            res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
        cards = [c for c in res["insights"] if c["id"] == "impossible_distance"]
        assert _assert_cards_wire_identical(cards) > 0

    def test_repository_network_edge_and_track_c_wire_identical(self, in_memory_duckdb, test_service_source):
        table = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            '"timestamp" TIMESTAMPTZ, "metro" USMALLINT, "delivery_rate" UBIGINT, "c_type" VARCHAR, "c_speed" VARCHAR, '
            '"pop" VARCHAR, "elapsed" UBIGINT, "transport" VARCHAR, "cache" VARCHAR, "url" VARCHAR, "status" USMALLINT, '
            '"resp_bytes" UBIGINT, "resp_header_content_encoding" VARCHAR, "ip" VARCHAR, "cookie_session" VARCHAR, '
            '"oconnect_ms" UINTEGER, "ottfb" UBIGINT)'
        )
        _seed_network_edge_data(in_memory_duckdb, table)
        _seed_track_c_data(in_memory_duckdb, table)
        _insights_cache.clear()
        res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
        cards = [c for c in res["insights"] if c["items"]]
        assert {
            "metro_delivery_degradation",
            "connection_type_mix",
            "pop_latency_regression",
            "http3_fallback",
            "cache_hit_cliff",
            "payload_compression_regression",
            "session_harvesting",
            "timeout_split",
        } <= {c["id"] for c in cards}
        assert _assert_cards_wire_identical(cards) >= 8

    def test_repository_network_asn_health_and_low_and_slow_wire_identical(self, in_memory_duckdb, test_service_source):
        table = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(
            f'CREATE TABLE IF NOT EXISTS {table} ("timestamp" TIMESTAMPTZ, "asn" INTEGER, "ploss" DOUBLE, '
            '"rtt_var" INTEGER, "retrans" DOUBLE, "ip" VARCHAR, "url" VARCHAR, "ja3" VARCHAR)'
        )
        now = datetime.now(UTC)
        for count, mins, ploss in ((60, 30, 0.08), (120, 90, 0.001)):
            ts = (now - timedelta(minutes=mins)).isoformat()
            in_memory_duckdb.executemany(
                f"INSERT INTO {table} VALUES (?, 64500, ?, 1000, 0.0, NULL, NULL, NULL)", [(ts, ploss)] * count
            )
        probes = ["/admin", "/.env", "/.git", "/wp-login.php", "/phpmyadmin/index.php", "/config.json"]
        for path, mins in zip(probes, [110, 88, 66, 44, 22, 6], strict=True):
            in_memory_duckdb.execute(
                f"INSERT INTO {table} VALUES (?, NULL, NULL, NULL, NULL, '192.0.2.55', ?, NULL)",
                [(now - timedelta(minutes=mins)).isoformat(), path],
            )
        _insights_cache.clear()
        with patch("backend.core.duckdb.get_asn_names", return_value={}):
            res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=1)
        cards = [c for c in res["insights"] if c["id"] in {"network_asn_health", "low_and_slow"}]
        assert {c["id"] for c in cards if c["items"]} == {"network_asn_health", "low_and_slow"}
        assert _assert_cards_wire_identical(cards) == 2

    def test_high_scale_cards_are_the_standard_cards_and_wire_identical(self):
        class _Client:
            def execute(self, query, params):
                if "insight:history" in query:
                    return [{"earliest": params["window_end"].replace(year=2020), "window_total": 9}]
                if "insight:fields" in query:
                    return [{"fields": ["status", "elapsed", "cache"]}]
                if "insight:error_spikes" in query:
                    return [{"url": "/a", "w_rate": 0.7, "b_rate": 0.0, "w_errors": 7, "w_total": 10, "b_total": 99}]
                return []

        service = type("Svc", (), {"client": _Client(), "service_id": "svc"})()
        req = type("Req", (), {"window_size_hrs": 24.0, "baseline_hours": 168.0})()
        res = hs_insights_endpoint(service, req, None, None)  # type: ignore[arg-type]
        cards = {card["id"]: card for card in json.loads(res.model_dump_json(by_alias=True))["insights"]}
        assert "high_scale_5xx" not in cards
        card = cards["error_spikes"]
        assert card["items"][0]["label"] == "/a" and card["items"][0]["meta"] == {
            "requests": 10,
            "errors": 7,
            "filters": {"url": "/a"},
        }
        assert card["severity"] == "critical" and card["category"] == "origin"


# ── Merged from tests/models/test_wire_network.py ──
def _seed_wire_network(con, src, n=120):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=2)
    for i, log in enumerate(logs):
        log.update(
            tcp_rtt=20_000 + (i * 997) % 90_000,
            rtt_min=10_000 + (i * 131) % 8_000,
            rtt_var=500 + (i * 53) % 5_000,
            ploss=0.0 if i % 7 else 0.02,
            asn=[7922, 15169, 701][i % 3],
            status=200 if i % 9 else 503,
            pop=["SJC", "IAD", "LAX"][i % 3],
            region=["CA", "NY"][i % 2],
        )
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def _health(con, src, **kw):
    return get_health(lambda: con, src, None, None, {}, **kw)


_START_wire_network = datetime(2026, 9, 15, 0, 0, tzinfo=UTC)
_END_wire_network = datetime(2026, 9, 15, 1, 0, tzinfo=UTC)
_B0 = int(_START_wire_network.timestamp())
_B1 = _B0 + 300


class _Req:
    start_time = _START_wire_network.isoformat().replace("+00:00", "Z")
    end_time = _END_wire_network.isoformat().replace("+00:00", "Z")
    bucket_seconds = 300
    top_n = 30
    region_country = "US"


class _HsClient_wire_network:
    def execute(self, sql, params=None):
        if "INNER JOIN" in sql:
            return []
        if "rtt_jitter_us" in sql and "GROUP BY asn" in sql:
            return [
                {
                    "asn": 7922,
                    "bucket_ts": _B0,
                    "reqs": 100,
                    "rtt_med_us": 20000.0,
                    "rtt_baseline_us": 18000.0,
                    "rtt_congestion_us": 2000.0,
                    "avg_ploss": 0.0,
                    "rtt_jitter_us": 1000.0,
                    "error_pct": 0.0,
                },
                {
                    "asn": 15169,
                    "bucket_ts": _B1,
                    "reqs": 60,
                    "rtt_med_us": 90000.0,
                    "rtt_baseline_us": 30000.0,
                    "rtt_congestion_us": 150000.0,
                    "avg_ploss": 0.03,
                    "rtt_jitter_us": 80000.0,
                    "error_pct": 8.0,
                },
            ]
        if "AS lat" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "lat": 37.33,
                    "lon": -121.89,
                    "metro": "807",
                    "bucket_ts": _B0,
                    "rtt_med_us": 20000.0,
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "reqs": 100,
                }
            ]
        if "AS region" in sql:
            return [
                {
                    "country": "US",
                    "city": "san jose",
                    "region": "California",
                    "metro": "807",
                    "avg_ploss": 0.0,
                    "error_pct": 0.0,
                    "total_reqs": 100,
                }
            ]
        if "GROUP BY asn, c_speed" in sql:
            return [{"asn": 7922, "c_speed": "fast", "cnt": 150}, {"asn": 15169, "c_speed": "slow", "cnt": 60}]
        if "quantile(0.95)" in sql:
            return [{"asn": 7922, "p95": 40000.0, "p99": 60000.0}, {"asn": 15169, "p95": 180000.0, "p99": 260000.0}]
        if "AS label" in sql:
            return [{"value": "US", "label": "US", "rtt_ms": 21.0, "reqs": 160}]
        return []


def _service() -> HighScaleService:
    return HighScaleService(
        service_id="svc", client=_HsClient_wire_network(), cursor_secret=b"secret", request_watermark=None
    )


class TestWireNetwork:
    def test_repository_health_wire_identical(self, in_memory_duckdb, test_service_source, monkeypatch):
        _seed_wire_network(in_memory_duckdb, test_service_source)
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast", 15169: "Google"})
        res = _health(in_memory_duckdb, test_service_source)
        assert_sections_wire_identical(
            NetworkHealthResponse,
            res,
            non_empty=("heatmap", "leaderboard", "map_buckets", "cities", "metro_leaderboard", "summary", "buckets"),
        )

    def test_repository_health_dense_window_wire_identical(self, in_memory_duckdb, test_service_source):
        _seed_wire_network(in_memory_duckdb, test_service_source)
        now = datetime.now(UTC)
        res = get_health(
            lambda: in_memory_duckdb,
            test_service_source,
            (now - timedelta(hours=3)).isoformat(),
            now.isoformat(),
            {},
            bucket_seconds=600,
            top_n=2,
        )
        assert_sections_wire_identical(NetworkHealthResponse, res, non_empty=("heatmap", "leaderboard"))

    def test_repository_quality_wire_identical(self, in_memory_duckdb, test_service_source, monkeypatch):
        _seed_wire_network(in_memory_duckdb, test_service_source)
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
        res = get_quality(lambda: in_memory_duckdb, test_service_source, None, None, {}, region_country="US")
        assert_sections_wire_identical(
            NetworkQualityResponse, res, non_empty=("by_country", "by_asn", "by_pop", "scatter", "countries")
        )

    def test_repository_unavailable_shapes_wire_identical(self, in_memory_duckdb):
        src = {"name": "net_wire_missing", "service_id": "net_wire_missing"}
        assert_sections_wire_identical(
            NetworkQualityResponse, get_quality(lambda: in_memory_duckdb, src, None, None, {}, region_country="US")
        )
        assert_sections_wire_identical(NetworkHealthResponse, get_health(lambda: in_memory_duckdb, src, None, None, {}))

    @pytest.mark.parametrize(
        "fn, model", [("network_health", NetworkHealthResponse), ("network_quality", NetworkQualityResponse)]
    )
    def test_high_scale_wire_identical(self, monkeypatch, fn, model):
        captured = _capture_with_telemetry(monkeypatch, model)
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda _sid, asns: {a: f"AS{a}" for a in asns})
        getattr(hs_network, fn)(_service(), _Req(), _Req.start_time, _Req.end_time)
        non_empty = (
            ("heatmap", "leaderboard", "map_buckets", "cities", "metro_leaderboard", "summary")
            if fn == "network_health"
            else ("by_country", "by_region")
        )
        assert_sections_wire_identical(model, captured, non_empty=non_empty)


# ── Merged from tests/models/test_wire_origin.py ──
_ORIGIN_SECTIONS = ("summary", "timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health")


def _seed_wire_origin(con, src, n=60):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        ottlb = 90000 + i * 1000
        log.update(
            ottfb=50000 + i * 1000,
            ottlb=ottlb,
            elapsed=ottlb + 5000,
            ost=829 if i == 3 else (200 if i % 5 else 500),
            oip="203.0.113.1" if i < 25 else "203.0.113.2",
            url="/api/data" if i % 2 == 0 else "/images/logo.png",
            pop=["SJC", "IAD", "LAX"][i % 3],
            host="example.com" if i % 2 else "www.example.com",
            edge=(i % 4 != 0),
            cache="MISS" if i % 3 == 0 else ("PASS" if i % 3 == 1 else "HIT"),
            rid=f"rid-{i}",
            prid=f"rid-{i - 1}" if i % 4 == 0 and i else None,
        )
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def _call(fn, con, src, **kw):
    return fn(con=con, src=src, start_time=None, end_time=None, filters={}, **kw)


def _origin_hs_req():
    return SimpleNamespace(
        filters={},
        split_by_leg=False,
        bucket_minutes=5,
        timeseries_metric="ttfb",
        timeseries_percentile="p95",
        slow_urls_limit=20,
        slow_urls_min_requests=1,
        pop_latency_limit=30,
        ip_health_limit=30,
    )


class _HsClient_wire_origin:
    _CANNED = {
        "origin:summary": [
            {
                "requests": 10,
                "misses": 6,
                "passes": 2,
                "origin_5xx": 1,
                "status_count": 10,
                "latency_q": [1000.0, 1500.0, 2500.0, 3000.0],
                "latency_n": 10,
                "ttlb_q": [2000.0, 4000.0],
                "ttlb_n": 10,
                "overhead_q": [500.0],
                "overhead_n": 10,
                "bytes_q": [128.0],
                "bytes_n": 10,
            }
        ],
        "origin:timeseries": [{"time": "2026-09-14 10:00:00", "miss_count": 6, "value_q": [2500.0], "value_n": 6}],
        "origin:slow_urls": [{"value": "/slow", "requests": 5, "latency_q": [1000.0, 2500.0, 3000.0], "latency_n": 5}],
        "origin:status_codes": [{"value": "503", "requests": 2, "total_requests": 10}],
        "origin:path_breakdown": [
            {"value": "false", "requests": 4, "latency_q": [1200.0, 2400.0], "latency_n": 4},
            {"value": "true", "requests": 6, "latency_q": [1000.0, 2000.0], "latency_n": 6},
        ],
        "origin:pop_latency": [
            {"value": "SJC", "requests": 4, "latency_q": [1000.0, 1000.0], "latency_n": 4},
            {"value": "LAX", "requests": 3, "latency_q": [1500.0, 5000.0], "latency_n": 3},
        ],
        "origin:ip_health": [
            {"value": "203.0.113.1", "requests": 7, "origin_5xx": 1, "latency_q": [1000.0, 2000.0], "latency_n": 7}
        ],
    }

    def execute(self, sql, params=None):
        for key, rows in self._CANNED.items():
            if key in sql:
                return rows
        return []


class TestWireOrigin:
    @pytest.fixture(autouse=True)
    def _clear_origin_caches(self):
        _clear_schema_cache()
        origin_mod._response_cache.clear()
        yield
        _clear_schema_cache()
        origin_mod._response_cache.clear()

    def test_granular_repositories_wire_identical(self, in_memory_duckdb, test_service_source):
        _seed_wire_origin(in_memory_duckdb, test_service_source)
        con, src = in_memory_duckdb, test_service_source
        cases = [
            (origin_mod.get_summary, OriginSummaryResponse, {}, ("has_data",)),
            (origin_mod.get_timeseries, OriginTimeseriesResponse, {}, ("series",)),
            (origin_mod.get_slow_urls, OriginSlowUrlsResponse, {"min_requests": 1}, ("rows",)),
            (origin_mod.get_status_codes, OriginStatusCodesResponse, {}, ("rows",)),
            (origin_mod.get_path_breakdown, OriginPathBreakdownResponse, {}, ("rows",)),
            (origin_mod.get_pop_latency, OriginPopLatencyResponse, {}, ("rows",)),
            (origin_mod.get_ip_health, OriginIpHealthResponse, {}, ("rows",)),
            (origin_mod.get_shielding_analysis, OriginShieldingAnalysisResponse, {}, ("rows",)),
            *[
                (origin_mod.get_drilldown, OriginDrilldownResponse, {"dimension": d}, ("rows",))
                for d in ("host", "pop", "oip")
            ],
        ]
        for fn, model, kw, non_empty in cases:
            assert_sections_wire_identical(model, _call(fn, con, src, **kw), non_empty=non_empty)

        split = _call(origin_mod.get_timeseries, con, src, split_by_leg=True)
        assert any("edge" in point for point in split["series"])
        assert_sections_wire_identical(OriginTimeseriesResponse, split, non_empty=("series",))

    @pytest.mark.asyncio
    async def test_composite_aggregates_wire_identical(self, in_memory_duckdb, test_service_source):
        _seed_wire_origin(in_memory_duckdb, test_service_source)
        res = await origin_mod.get_aggregates(
            in_memory_duckdb, test_service_source, None, None, {}, slow_urls_min_requests=1
        )
        assert_sections_wire_identical(OriginAggregatesResponse, res, non_empty=_ORIGIN_SECTIONS)
        for section in _ORIGIN_SECTIONS[1:]:
            assert res[section]["rows" if section != "timeseries" else "series"], section

    @pytest.mark.asyncio
    async def test_composite_aggregates_empty_schema_wire_identical(self, in_memory_duckdb):
        src = {"name": "origin_wire_empty", "service_id": "origin_wire_empty"}
        res = await origin_mod.get_aggregates(in_memory_duckdb, src, None, None, {})
        assert_sections_wire_identical(OriginAggregatesResponse, res)

    def test_high_scale_aggregates_wire_identical(self):
        service = SimpleNamespace(service_id="svc", client=_HsClient_wire_origin())
        res = hs_aggregates(service, _origin_hs_req(), None, None, sections=None)
        assert_sections_wire_identical(OriginAggregatesResponse, res, non_empty=_ORIGIN_SECTIONS)
        dumped = OriginAggregatesResponse.model_validate(res).model_dump(by_alias=True)
        for section in ("summary", "timeseries", "slow_urls", "path_breakdown", "pop_latency", "ip_health"):
            assert dumped[section]["_approx"] is True and "histogram sketch" in dumped[section]["_approx_note"], section

    def test_high_scale_empty_sections_wire_identical(self):
        service = SimpleNamespace(service_id="svc", client=SimpleNamespace(execute=lambda sql, params=None: []))
        res = hs_aggregates(service, _origin_hs_req(), None, None, sections=None)
        assert_sections_wire_identical(OriginAggregatesResponse, res)


# ── Merged from tests/models/test_wire_security.py ──
START = "2026-08-01T00:00:00Z"
END = "2026-08-02T00:00:00Z"


def _seed_wire_security(con, src, n=200):
    table = _safe_table(src["name"])
    con.execute(
        f"""CREATE TABLE {table} (
            timestamp TIMESTAMPTZ, ip VARCHAR, ua VARCHAR, pop VARCHAR, rtt_min INTEGER, tcp_rtt INTEGER,
            lat DOUBLE, lon DOUBLE, asn INTEGER, country VARCHAR, city VARCHAR, tls_ciphers_sha VARCHAR,
            req_header_bytes INTEGER, conn_requests INTEGER, is_ipv6 BOOLEAN, p_type VARCHAR,
            waf_sig VARCHAR, waf_req_id VARCHAR)"""
    )
    rows = [
        (
            f"2026-08-01 {12 + i % 3}:{i % 60:02d}:00Z",
            f"10.0.{i % 7}.{i % 11}",
            "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)" if i % 2 else "curl/8",
            "SJC",
            [5, 10, 500, 1000][i % 4],
            1000,
            37.0,
            -121.0,
            [1111, 2222, 3333][i % 3],
            "US",
            "San Jose",
            f"fp{i % 5}",
            100 + (i * 97) % 40000,
            1 + i % 120,
            i % 3 == 0,
            ["vpn", "tor", ""][i % 3],
            "VERIFIED-BOT.search-engine,OTHER.tag" if i % 4 == 0 else None,
            None,
        )
        for i in range(n)
    ]
    con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * 18)})", rows)


class _FakeClickHouse:
    def execute(self, sql, params=None):
        tag = sql.split("hs_security:")[1].split(" ")[0]
        return {
            "proxy_dist": [{"type": "vpn", "count": 7}],
            "tls_fingerprints": [{"fingerprint": "abc", "ip_count": 2, "request_count": 5}],
            "req_size_dist": [{"bucket": "0-256B", "count": 9, "min_val": 0}],
            "ipv6_adoption": [{"bucket_ts": 1_790_000_000, "pct": 33.3}],
            "conn_reuse_dist": [{"bucket": "1 (None)", "count": 4, "min_val": 1}],
            "ngwaf_bots": [{"bot_name": "bing", "wk_name": "", "bot_cat": "search", "request_count": 11}],
            "ngwaf_verified_bots_ts": [{"bucket_ts": 1_790_000_000, "bot_name": "bing", "count": 11}],
            "verified_bots_ts": [{"bucket_ts": 1_790_000_000, "bot_type": "search-engine", "count": 3}],
            "top_ips_header": [{"ip": "10.0.0.1", "max_header": 9000}],
            "proxy_stats": [{"active_proxies_count": 1, "total_requests_count": 2, "distance_mismatches_count": 1}],
            "traffic_quality": [{"type": "Direct Connection", "count": 3}],
            "suspicious_isps": [{"asn": 15169, "count": 2}],
            "active_clients": [
                {
                    "ip": "10.0.0.1",
                    "asn": 15169,
                    "rtt_min_ms": 5.0,
                    "tcp_rtt_ms": 100.0,
                    "distance_km": 10.0,
                    "pop": "SJC",
                    "client_lat": 1.0,
                    "client_lon": 2.0,
                    "pop_lat": 37.0,
                    "pop_lon": -121.0,
                    "impossible_distance": 1,
                    "risk_level": "High",
                    "country": "US",
                    "city": "x",
                }
            ],
            "fingerprint_coverage": [{"total_rows": 10, "populated": 5}],
        }.get(tag, [])


class TestWireSecurity:
    def test_repository_aggregates_wire_identical(self, in_memory_duckdb, test_service_source):
        _seed_wire_security(in_memory_duckdb, test_service_source)
        with patch("backend.core.duckdb.get_asn_names", return_value={}):
            res = get_security_aggregates(
                con=in_memory_duckdb,
                src=test_service_source,
                start_time=START,
                end_time=END,
                filters={},
                bucket_seconds=3600,
            )
        assert_sections_wire_identical(
            SecurityAggregatesResponse,
            res,
            non_empty=("tls_fingerprints", "req_size_dist", "conn_reuse_dist", "ipv6_adoption", "proxy_dist"),
        )

    def test_repository_masked_top_ips_wire_identical(self, in_memory_duckdb, test_service_source):
        _seed_wire_security(in_memory_duckdb, test_service_source)
        res = get_security_aggregates(
            con=in_memory_duckdb,
            src=test_service_source,
            start_time=START,
            end_time=END,
            filters={},
            sections={"top_ips_header"},
            mask_ips=True,
        )
        assert_sections_wire_identical(SecurityAggregatesResponse, res, non_empty=("top_ips_header",))

    def test_repository_proxies_and_top_bots_wire_identical(self, in_memory_duckdb, test_service_source):
        _seed_wire_security(in_memory_duckdb, test_service_source, n=300)
        with patch("backend.core.duckdb.get_asn_names", return_value={}):
            proxies = get_security_proxies(
                con=in_memory_duckdb, src=test_service_source, start_time=START, end_time=END, filters={}
            )
        assert_sections_wire_identical(
            SecurityAggregatesResponse, proxies, non_empty=("traffic_quality", "suspicious_isps", "active_clients")
        )
        bots = get_top_bots(con=in_memory_duckdb, src=test_service_source, start_time=START, end_time=END, filters={})
        assert_sections_wire_identical(SecurityTopBotsResponse, bots)

    def test_repository_wellknown_and_verified_bots_wire_identical(self, in_memory_duckdb, test_service_source):
        _seed_wire_security(in_memory_duckdb, test_service_source, n=60)
        entry = {"id": "googlebot", "categories": ["search-engine"], "verification": {"domains": ["googlebot.com"]}}
        with (
            patch(
                "backend.utils.bot_sources.build_matcher",
                return_value=lambda ua: (entry,) if "Googlebot" in (ua or "") else (),
            ),
            patch("backend.utils.bot_sources.get_bot_regex_pattern", return_value=None),
            patch("backend.utils.rdns_cache.get_hostnames", return_value={}),
            patch("backend.utils.rdns_cache.enqueue"),
        ):
            res = get_security_aggregates(
                con=in_memory_duckdb,
                src=test_service_source,
                start_time=START,
                end_time=END,
                filters={},
                sections={"wellknown_bots", "verified_bots_ts"},
                bucket_seconds=3600,
            )
        assert_sections_wire_identical(
            SecurityAggregatesResponse, res, non_empty=("wellknown_bots", "verified_bots_ts")
        )

    def test_high_scale_aggregates_wire_identical(self, monkeypatch):
        from backend.high_scale import security as hs_security

        svc = HighScaleService(
            service_id="svc",
            client=_FakeClickHouse(),
            cursor_secret=b"secret",
            request_watermark=ServingWatermark(
                service_id="svc",
                domain="request",
                owner_epoch=1,
                coverage_start=None,
                coverage_end=None,
                last_accepted_cursor=None,
                last_archived_event_id=None,
                last_visible_event_id=None,
                exact=True,
            ),
        )
        captured = _capture_with_telemetry(monkeypatch, hs_security.SecurityAggregatesResponse)
        with patch("backend.core.duckdb.get_asn_names", return_value={}):
            hs_security_aggregates(svc, None, START, END)
        assert captured["tls_fingerprints"][0]["ip_count"] == 2 and captured["req_size_dist"][0]["bucket"] == "0-256B"
        assert captured["ipv6_adoption"][0]["pct"] == 33.3
        assert_sections_wire_identical(
            SecurityAggregatesResponse,
            captured,
            non_empty=(
                "tls_fingerprints",
                "req_size_dist",
                "conn_reuse_dist",
                "ipv6_adoption",
                "proxy_dist",
                "verified_bots_ts",
                "ngwaf_verified_bots",
                "ngwaf_verified_bots_ts",
                "ngwaf_bots",
                "top_ips_header",
                "traffic_quality",
                "suspicious_isps",
                "active_clients",
            ),
        )


# ── Merged from tests/test_config.py ──


@pytest.fixture(autouse=True)
def _isolate_config_dirs(tmp_path, monkeypatch):
    """Point CONFIGS_DIR + DATA_DIR at tmp_path for each test.

    All of these are module-level Path constants that production code
    reads on every call, so monkeypatching them per-test gives total
    isolation without touching the real configs/ tree.
    """
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path / "configs")
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", tmp_path / "data" / "services")
    monkeypatch.setattr(svcconfig, "NGWAF_DATA_DIR", tmp_path / "data" / "ngwaf")
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", tmp_path / "data" / "cache")
    monkeypatch.setattr(svcconfig, "SYSTEM_DATA_DIR", tmp_path / "data" / "system")
    monkeypatch.setattr(svcconfig, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "data" / "system" / "usage_logging.json")
    # Clear the in-memory caches between tests
    svcconfig._name_cache.clear()
    svcconfig._config_cache.clear()
    svcconfig._invalidate_service_ids_cache()
    yield
    svcconfig._name_cache.clear()
    svcconfig._config_cache.clear()
    svcconfig._invalidate_service_ids_cache()


def _cfg(**overrides) -> dict:
    base = {
        "service_id": "svc-1",
        "name": "My Service",
        "access_level": "read_write",
        "fos_endpoint": "us-east-1.object.fastlystorage.app",
        "fos_access_key_id": "key",
        "fos_secret_access_key": "secret",
        "fos_bucket": "bkt",
        "fos_prefix": "",
        "fos_region": "us-east-1",
        "fastly_api_key": "api-key",
        "cdn_service_id": "cdn-svc-id",
    }
    base.update(overrides)
    return base


# ── Path helpers ─────────────────────────────────────────────────────────────


def test_config_path_uses_service_id_as_filename(tmp_path):
    p = svcconfig.config_path("svc-abc")
    assert p.name == "svc-abc.json"
    assert p.parent.name == "configs"


def test_duckdb_path_lives_in_services_data_dir():
    p = svcconfig.duckdb_path("svc-abc")
    assert p.endswith("services/svc-abc.duckdb")


def test_ngwaf_db_path_is_shared_across_services():
    """One ngwaf cache file for the whole install — pinned because a
    refactor that per-svc'd this would multiply the cron's API hits."""
    assert svcconfig.ngwaf_db_path().endswith("data/ngwaf_bot_cache.db")


def test_ngwaf_sqlite_cache_override_ignores_the_default_path(monkeypatch, tmp_path):
    """The default ``ngwaf_bot_cache.db`` is a stale pre-Postgres leftover, never a data source;
    only an explicitly different path (test fixtures) selects SQLite."""
    assert svcconfig.ngwaf_sqlite_cache_override() is None
    monkeypatch.setattr(svcconfig, "ngwaf_db_path", lambda: str(tmp_path / "fixture.db"))
    assert svcconfig.ngwaf_sqlite_cache_override() == str(tmp_path / "fixture.db")


# ── load_config / save_config / delete ───────────────────────────────────────


def test_load_config_returns_none_for_missing_service():
    assert svcconfig.load_config("nonexistent") is None


def test_save_then_load_roundtrip():
    cfg = _cfg(service_id="svc-rt")
    svcconfig.save_config("svc-rt", cfg)
    loaded = svcconfig.load_config("svc-rt")
    assert loaded == cfg


def test_save_config_writes_atomically(tmp_path):
    """The implementation writes to ``.tmp`` then ``rename`` — pinned so
    a refactor that skips the tmp file doesn't reintroduce torn-write
    risk on power loss."""
    svcconfig.save_config("svc-x", _cfg(service_id="svc-x"))
    # After save, the tmp file must not exist (it was renamed)
    assert not (svcconfig.CONFIGS_DIR / "svc-x.tmp").exists()
    assert (svcconfig.CONFIGS_DIR / "svc-x.json").exists()


def test_save_config_creates_configs_dir_if_missing(tmp_path):
    """Bootstrap install: configs/ doesn't exist yet. ``save_config``
    must create it (via ``_ensure_dirs``)."""
    # conftest's ``isolate_metadata_db`` pre-creates the sandbox tree to
    # work around _ensure_dirs not using parents=True. This test pins
    # the bootstrap contract — wipe the dir so we're back to the
    # first-run shape.
    import shutil

    shutil.rmtree(svcconfig.CONFIGS_DIR, ignore_errors=True)
    assert not svcconfig.CONFIGS_DIR.exists()
    svcconfig.save_config("svc-1", _cfg())
    assert svcconfig.CONFIGS_DIR.exists()


def test_load_config_returns_fresh_dict_each_call():
    """Cache returns reparsed dict on every call so callers that mutate
    the result (e.g. update_status appending to status) don't poison
    subsequent loads. Pinned because returning the cached instance
    directly would silently break update_status under concurrent calls."""
    svcconfig.save_config("svc-mut", _cfg(service_id="svc-mut"))
    a = svcconfig.load_config("svc-mut")
    b = svcconfig.load_config("svc-mut")
    assert a is not b
    a["mutated"] = True  # mutate the first dict
    c = svcconfig.load_config("svc-mut")
    assert "mutated" not in c


def test_load_config_picks_up_save_via_mtime():
    """save_config writes via atomic os.replace which bumps st_mtime_ns.
    The cache uses mtime as its revalidation key, so the next load_config
    sees the new content without an explicit cache bust. Pinned because
    a downgrade to st_mtime (float) could lose precision on rapid writes
    and serve stale configs."""
    svcconfig.save_config("svc-rev", _cfg(service_id="svc-rev", name="before"))
    first = svcconfig.load_config("svc-rev")
    assert first is not None and first["name"] == "before"

    # Ensure clock advances; mtime_ns gives nanosecond granularity so a
    # short sleep is usually overkill, but a few ms is bulletproof on any
    # filesystem.
    time.sleep(0.005)
    svcconfig.save_config("svc-rev", _cfg(service_id="svc-rev", name="after"))
    second = svcconfig.load_config("svc-rev")
    assert second is not None and second["name"] == "after"


def test_save_config_invalidates_load_config_cache_for_mtime_collision():
    """Two rapid save_config calls can produce identical st_mtime_ns on
    Linux ext4/tmpfs (same-microsecond writes). Without an explicit cache
    invalidation in save_config, the second load_config returns the
    pre-write cached bytes — which silently clobbered update_status's
    merge in CI (KeyError: 'last_sync_at'). Simulate the collision by
    pinning stat().st_mtime_ns to a constant across writes."""
    import os as _os
    from unittest.mock import patch

    svcconfig.save_config("svc-coll", _cfg(service_id="svc-coll", name="first"))
    svcconfig.load_config("svc-coll")  # prime the cache

    # Freeze mtime_ns: every stat() returns the same value, so the cache's
    # revalidation key cannot distinguish between writes.
    real_stat = _os.stat
    frozen_ns = svcconfig.config_path("svc-coll").stat().st_mtime_ns

    class _FrozenStat:
        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            return getattr(self._real, name)

        @property
        def st_mtime_ns(self):
            return frozen_ns

    def _patched_stat(path, *args, **kwargs):
        return _FrozenStat(real_stat(path, *args, **kwargs))

    with patch("os.stat", side_effect=_patched_stat):
        svcconfig.save_config("svc-coll", _cfg(service_id="svc-coll", name="second"))
        loaded = svcconfig.load_config("svc-coll")

    assert loaded is not None and loaded["name"] == "second", (
        "save_config must invalidate the load_config cache; otherwise a "
        "same-nanosecond write collision serves stale bytes"
    )


def test_load_config_avoids_reread_when_mtime_unchanged(monkeypatch):
    """The cache hit path must NOT re-read the file. Pinned by counting
    open() calls — if a refactor drops the mtime check, every hot caller
    (sync-status, cron tick) pays a syscall per load."""
    svcconfig.save_config("svc-hit", _cfg(service_id="svc-hit"))
    # Prime the cache
    svcconfig.load_config("svc-hit")

    open_calls = {"n": 0}
    real_open = svcconfig.open if hasattr(svcconfig, "open") else open

    def counting_open(*args, **kwargs):
        open_calls["n"] += 1
        return real_open(*args, **kwargs)

    monkeypatch.setattr("builtins.open", counting_open)
    for _ in range(5):
        svcconfig.load_config("svc-hit")
    assert open_calls["n"] == 0, "cache hit must skip open()"


# ── update_status / get_status ───────────────────────────────────────────────


def test_update_status_merges_into_existing_status():
    """Subsequent calls update fields without clobbering unrelated ones —
    pinned because the cron writes ``last_sync_at`` and the UI writes
    ``manual_pause`` independently."""
    svcconfig.save_config("svc-st", _cfg(service_id="svc-st"))
    svcconfig.update_status("svc-st", {"last_sync_at": "2026-01-01"})
    svcconfig.update_status("svc-st", {"manual_pause": True})

    out = svcconfig.get_status("svc-st")
    assert out["last_sync_at"] == "2026-01-01"
    assert out["manual_pause"] is True
    assert "updated_at" in out  # auto-stamped


def test_update_status_silently_drops_unknown_service():
    """No config → no-op (consistent with ``delete_config``)."""
    svcconfig.update_status("ghost", {"x": 1})  # must not raise
    assert svcconfig.get_status("ghost") == {}


def test_get_status_returns_empty_dict_for_unknown_service():
    """Pinned: callers iterate over the dict; returning None here
    would crash every status-render code path."""
    assert svcconfig.get_status("ghost") == {}


def test_update_status_stamps_updated_at_timestamp():
    svcconfig.save_config("svc-ts", _cfg(service_id="svc-ts"))
    before = time.time()
    svcconfig.update_status("svc-ts", {"foo": "bar"})
    after = time.time()

    status = svcconfig.get_status("svc-ts")
    assert before <= status["updated_at"] <= after


# ── list_service_ids / list_configs ──────────────────────────────────────────


def test_list_service_ids_returns_sorted_ids():
    """The frontend's service switcher renders in this order — sorted
    is stable across reloads, picking it up from the filename pins the
    sort key explicitly (no surprises from dict insertion order)."""
    svcconfig.save_config("zeta", _cfg(service_id="zeta"))
    svcconfig.save_config("alpha", _cfg(service_id="alpha"))
    svcconfig.save_config("mu", _cfg(service_id="mu"))

    assert svcconfig.list_service_ids() == ["alpha", "mu", "zeta"]


def test_list_service_ids_returns_empty_on_missing_dir():
    """Fresh install: no configs dir yet → ``[]``."""
    assert svcconfig.list_service_ids() == []


def test_list_service_ids_caches_glob_until_dir_changes(monkeypatch):
    """The hot per-request path (telemetry_middleware →
    get_active_service_id → list_service_ids) must not re-glob the configs
    dir on every call — a synchronous glob on the event loop stalls under
    shared-host disk contention. Revalidation is by directory mtime, so a
    save/delete is still reflected immediately."""
    from pathlib import Path

    svcconfig.save_config("alpha", _cfg(service_id="alpha"))

    real_glob = Path.glob
    calls = {"n": 0}

    def counting_glob(self, pattern, *a, **k):
        if pattern == "*.json" and self == svcconfig.CONFIGS_DIR:
            calls["n"] += 1
        return real_glob(self, pattern, *a, **k)

    monkeypatch.setattr(Path, "glob", counting_glob)

    assert svcconfig.list_service_ids() == ["alpha"]
    first = calls["n"]
    assert first >= 1
    # Repeated calls with an unchanged dir must NOT re-glob.
    for _ in range(5):
        svcconfig.list_service_ids()
    assert calls["n"] == first

    # A newly saved config appears immediately (dir mtime bump + invalidation).
    svcconfig.save_config("beta", _cfg(service_id="beta"))
    assert svcconfig.list_service_ids() == ["alpha", "beta"]

    # A deleted config disappears immediately.
    svcconfig.config_path("alpha").unlink()
    svcconfig._invalidate_service_ids_cache()
    assert svcconfig.list_service_ids() == ["beta"]


def test_list_configs_skips_unparseable_files():
    """If one config file is corrupt JSON the others must still load.
    Pinned because a single bad write should not brick the bootstrap
    response for every other service."""
    svcconfig.save_config("good", _cfg(service_id="good"))
    # Drop a malformed file
    svcconfig._ensure_dirs()
    (svcconfig.CONFIGS_DIR / "bad.json").write_text("{not json")

    # Loading raises; ``list_configs`` should NOT include None for the bad one.
    # Note: current implementation actually raises on bad JSON. Pinned here
    # as documenting the behaviour — if we ever wrap load_config in try/except,
    # this test catches the change and the caller can update strategy.
    with pytest.raises(json.JSONDecodeError):
        svcconfig.list_configs()


def test_list_configs_returns_full_dicts():
    svcconfig.save_config("a", _cfg(service_id="a", name="A"))
    svcconfig.save_config("b", _cfg(service_id="b", name="B"))

    out = svcconfig.list_configs()
    assert [c["service_id"] for c in out] == ["a", "b"]
    assert {c["name"] for c in out} == {"A", "B"}


# ── get_active_service_id ────────────────────────────────────────────────────


def test_active_service_id_returns_first_configured():
    svcconfig.save_config("zeta", _cfg(service_id="zeta"))
    svcconfig.save_config("alpha", _cfg(service_id="alpha"))

    assert svcconfig.get_active_service_id() == "alpha"  # sorted, first


def test_active_service_id_returns_none_when_no_configs():
    assert svcconfig.get_active_service_id() is None


def test_active_service_id_respects_fallback_flag():
    """``fallback_to_first=False`` → return None even when configs
    exist. Pinned because the provision wizard explicitly disables the
    fallback so it can detect the "fresh install" state."""
    svcconfig.save_config("a", _cfg(service_id="a"))
    assert svcconfig.get_active_service_id(fallback_to_first=False) is None


# ── config_to_source: CDN-vs-native endpoint selection ──────────────────────


def test_config_to_source_uses_cdn_endpoint_when_cdn_url_set():
    """``cdn_url`` present → DuckDB httpfs reads go through the CDN
    (cheap egress). Pinned because losing this would invisibly 10x
    the bandwidth bill."""
    src = svcconfig.config_to_source(_cfg(cdn_url="https://cdn.example.com/path/extra"))
    # The CDN URL's host wins, stripping scheme + path
    assert src["endpoint"] == "cdn.example.com"
    # ...but the native endpoint is preserved for boto3 writes
    assert src["fos_native_endpoint"] == "us-east-1.object.fastlystorage.app"


def test_config_to_source_falls_back_to_native_endpoint_without_cdn():
    src = svcconfig.config_to_source(_cfg(cdn_url=None))
    assert src["endpoint"] == "us-east-1.object.fastlystorage.app"


def test_config_to_source_uses_fixed_v3_raw_root():
    src = svcconfig.config_to_source(_cfg(fos_prefix="customer-prefix"))
    assert src["prefix"] == ""


def test_config_to_source_native_endpoint_derives_from_region_when_missing():
    """If ``fos_endpoint`` is absent the helper synthesises it from
    region. Pinned because a typo here would silently point boto3 at
    the wrong region's FOS shard."""
    cfg = _cfg(fos_region="eu-west-1")
    del cfg["fos_endpoint"]
    src = svcconfig.config_to_source(cfg)
    assert src["fos_native_endpoint"] == "eu-west-1.object.fastlystorage.app"


def test_config_to_source_includes_log_period_as_int():
    """``log_period`` is sometimes stored as a string (from form input).
    Pinned to int because the scheduler does arithmetic on it."""
    src = svcconfig.config_to_source(_cfg(log_period="120"))
    assert src["log_period"] == 120
    assert isinstance(src["log_period"], int)


def test_config_to_source_defaults_log_period_to_60():
    cfg = _cfg()
    # log_period not in cfg
    src = svcconfig.config_to_source(cfg)
    assert src["log_period"] == 60


def test_config_to_source_passes_through_provisioning_dict():
    """Teardown reads ``provisioning.fos_key_id`` to know which FOS
    access key to revoke. Lossy refactor would orphan it in FOS."""
    src = svcconfig.config_to_source(_cfg(provisioning={"fos_key_id": "key-123", "endpoint_name": "FOS Logs"}))
    assert src["provisioning"]["fos_key_id"] == "key-123"


def test_config_to_source_defaults_access_level_to_read_write():
    cfg = _cfg()
    del cfg["access_level"]
    src = svcconfig.config_to_source(cfg)
    assert src["access_level"] == "read_write"


def test_config_to_source_includes_rum_fields():
    """Verify config_to_source maps rum_enabled and rum config blocks."""
    # rum.enabled wins over the legacy root flag (the scheduler's precedence; see is_rum_enabled)
    cfg = _cfg(rum_enabled=True, rum={"enabled": False})
    src = svcconfig.config_to_source(cfg)
    assert src["rum_enabled"] is False
    assert src["rum"] == {"enabled": False}

    # Root flag applies when the sub-dictionary has no opinion
    cfg_root = _cfg(rum_enabled=True)
    assert svcconfig.config_to_source(cfg_root)["rum_enabled"] is True

    # Test with rum enabled on sub-dictionary
    cfg2 = _cfg(rum_enabled=False, rum={"enabled": True})
    src2 = svcconfig.config_to_source(cfg2)
    assert src2["rum_enabled"] is True
    assert src2["rum"] == {"enabled": True}

    # Test with both disabled / missing
    cfg3 = _cfg(rum_enabled=False)
    src3 = svcconfig.config_to_source(cfg3)
    assert src3["rum_enabled"] is False
    assert src3["rum"] == {}


# ── fetch_service_name ───────────────────────────────────────────────────────


def test_fetch_service_name_returns_name_on_success():
    fake_resp = MagicMock()
    fake_resp.read.return_value = b'{"name": "Production CDN"}'
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=fake_resp):
        assert svcconfig.fetch_service_name("svc-1", "key") == "Production CDN"


def test_fetch_service_name_returns_none_on_network_error():
    """Any exception from the API call → return None (caller falls
    back to the cached or config name)."""
    with patch("urllib.request.urlopen", side_effect=OSError("DNS")):
        assert svcconfig.fetch_service_name("svc-1", "key") is None


def test_fetch_service_name_returns_none_when_name_missing_from_payload():
    fake_resp = MagicMock()
    fake_resp.read.return_value = b'{"other_field": "value"}'
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=fake_resp):
        assert svcconfig.fetch_service_name("svc-1", "key") is None


# ── refresh_service_name: cache + fallback chain ────────────────────────────


def test_refresh_service_name_returns_cached_within_ttl():
    """Cache hit within TTL must NOT trigger an API call — saves
    Fastly rate-limit and latency on every page load."""
    svcconfig._name_cache["svc-1"] = {"name": "Cached Name", "fetched_at": time.time()}

    with patch("backend.config.fetch_service_name") as mock_fetch:
        out = svcconfig.refresh_service_name("svc-1", "key")

    assert out == "Cached Name"
    mock_fetch.assert_not_called()


def test_refresh_service_name_fetches_when_cache_expired():
    """Cache older than TTL → re-fetch from API."""
    svcconfig._name_cache["svc-1"] = {
        "name": "Stale Name",
        "fetched_at": time.time() - 1000,  # >> TTL (300)
    }
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="From Config"))

    with patch("backend.config.fetch_service_name", return_value="Fresh Name") as mock_fetch:
        out = svcconfig.refresh_service_name("svc-1", "key")

    assert out == "Fresh Name"
    mock_fetch.assert_called_once_with("svc-1", "key")


def test_refresh_service_name_persists_to_config_when_name_changed():
    """When the API returns a name that differs from what's in the
    config file, the config gets re-saved with the new name. Pinned
    because skipping this lets the UI show "service-abc-id" forever
    even after the Fastly admin renames the service."""
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Old Name"))

    with patch("backend.config.fetch_service_name", return_value="New Name"):
        svcconfig.refresh_service_name("svc-1", "key")

    assert svcconfig.load_config("svc-1")["name"] == "New Name"


def test_refresh_service_name_falls_back_to_config_when_api_fails():
    """API returns None (network down, 403) → use the cached config name."""
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Config Name"))

    with patch("backend.config.fetch_service_name", return_value=None):
        out = svcconfig.refresh_service_name("svc-1", "key")

    assert out == "Config Name"


def test_refresh_service_name_falls_back_to_id_when_no_config():
    """No API, no config → return the service_id itself so the UI
    always has SOMETHING to display."""
    with patch("backend.config.fetch_service_name", return_value=None):
        out = svcconfig.refresh_service_name("unknown-svc", "key")

    assert out == "unknown-svc"


def test_refresh_service_name_skips_api_when_no_api_key():
    """No api_key → don't even try the API. Pinned because attempting
    with an empty key hits Fastly's 401 path repeatedly, which counts
    against rate limits."""
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Local Name"))

    with patch("backend.config.fetch_service_name") as mock_fetch:
        out = svcconfig.refresh_service_name("svc-1", api_key=None)

    assert out == "Local Name"
    mock_fetch.assert_not_called()


def test_refresh_service_name_skips_api_when_api_key_whitespace_only():
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Local"))

    with patch("backend.config.fetch_service_name") as mock_fetch:
        svcconfig.refresh_service_name("svc-1", api_key="   ")

    mock_fetch.assert_not_called()


def test_refresh_service_name_failure_caches_short_retry():
    """When the API fails, the cached entry gets stamped with
    ``fetched_at = now - TTL + 120`` — i.e. the next retry will fire
    in 120 seconds, not 300. Pinned because a longer back-off would
    leave a stale name visible for the full TTL after a transient
    failure."""
    svcconfig.save_config("svc-1", _cfg(service_id="svc-1", name="Local"))

    before = time.time()
    with patch("backend.config.fetch_service_name", return_value=None):
        svcconfig.refresh_service_name("svc-1", "key")

    entry = svcconfig._name_cache["svc-1"]
    # fetched_at is now - TTL + 120 → next refresh in ~120s
    age = before - entry["fetched_at"]
    assert 175 <= age <= 185  # 300 - 120 with small tolerance


# ── refresh_all_service_names: parallel ─────────────────────────────────────


def test_refresh_all_service_names_returns_map_of_id_to_name():
    configs = [
        {"service_id": "a", "fastly_api_key": "k", "name": "A"},
        {"service_id": "b", "fastly_api_key": "k", "name": "B"},
    ]

    def _fake_refresh(sid, _key):
        return f"Name-{sid}"

    with patch("backend.config.refresh_service_name", side_effect=_fake_refresh):
        out = svcconfig.refresh_all_service_names(configs, _sync=True)

    assert out == {"a": "Name-a", "b": "Name-b"}


def test_refresh_all_service_names_falls_back_to_config_name_on_per_service_exception():
    """If one service's refresh raises, the others still return.
    Pinned because losing all names because one service has bad creds
    would break the sidebar entirely."""
    configs = [
        {"service_id": "ok", "fastly_api_key": "k", "name": "OK-svc"},
        {"service_id": "bad", "fastly_api_key": "k", "name": "Bad-svc"},
    ]

    def _fake_refresh(sid, _key):
        if sid == "bad":
            raise RuntimeError("boom")
        return f"Fresh-{sid}"

    with patch("backend.config.refresh_service_name", side_effect=_fake_refresh):
        out = svcconfig.refresh_all_service_names(configs, _sync=True)

    assert out["ok"] == "Fresh-ok"
    assert out["bad"] == "Bad-svc"  # fell back to cfg["name"]


# ── Fastly credential helpers ───────────────────────────────────────────────


def test_get_fastly_api_key_returns_key_for_given_service():
    svcconfig.save_config("svc-a", _cfg(service_id="svc-a", fastly_api_key="key-a"))
    assert svcconfig.get_fastly_api_key("svc-a") == "key-a"


def test_get_fastly_api_key_falls_back_to_active_service():
    svcconfig.save_config("alpha", _cfg(service_id="alpha", fastly_api_key="key-active"))
    assert svcconfig.get_fastly_api_key(None) == "key-active"  # alpha is first/active


def test_get_fastly_api_key_returns_empty_string_when_unconfigured():
    assert svcconfig.get_fastly_api_key("nonexistent") == ""


def test_save_config_handles_read_only_filesystem():
    """When /app/configs is on a read-only filesystem (e.g. mounted Secret),
    save_config logs a warning instead of raising an unhandled OSError."""
    with patch("backend.config._atomic_write_json", side_effect=OSError(30, "Read-only file system")):
        # Should not raise
        svcconfig.save_config("svc-ro", _cfg(service_id="svc-ro"))

    assert svcconfig.get_fastly_api_key(None) == ""  # no configs at all


def test_get_fastly_service_id_returns_cdn_service_id():
    """``cdn_service_id`` is the Fastly service that fronts FOS — used
    by the VCL routes. Distinct from the logging service id."""
    svcconfig.save_config("svc", _cfg(service_id="svc", cdn_service_id="cdn-123"))
    assert svcconfig.get_fastly_service_id("svc") == "cdn-123"


def test_get_fastly_logging_service_id_returns_service_id_itself():
    """For the logging service the ``service_id`` IS the logging
    service id (they're the same Fastly service, just used differently
    in different contexts)."""
    svcconfig.save_config("log-svc", _cfg(service_id="log-svc"))
    assert svcconfig.get_fastly_logging_service_id("log-svc") == "log-svc"


# ── get_ngwaf_workspace_id ──────────────────────────────────────────────────


def test_get_ngwaf_workspace_id_returns_value_when_set():
    svcconfig.save_config("svc", _cfg(service_id="svc", ngwaf_workspace_id="ws-1"))
    assert svcconfig.get_ngwaf_workspace_id("svc") == "ws-1"


def test_get_ngwaf_workspace_id_returns_none_when_unset_or_empty():
    """Both missing key and empty string → None. Pinned because the
    sync cron uses ``if ws_id:`` to skip services that aren't
    NGWAF-enabled."""
    svcconfig.save_config("svc1", _cfg(service_id="svc1"))
    svcconfig.save_config("svc2", _cfg(service_id="svc2", ngwaf_workspace_id=""))

    assert svcconfig.get_ngwaf_workspace_id("svc1") is None
    assert svcconfig.get_ngwaf_workspace_id("svc2") is None


def test_get_ngwaf_workspace_id_returns_none_for_unknown_service():
    assert svcconfig.get_ngwaf_workspace_id("ghost") is None


# ── Usage logging config ────────────────────────────────────────────────────


def test_load_usage_logging_config_returns_defaults_when_file_missing():
    out = svcconfig.load_usage_logging_config()
    assert out == svcconfig._USAGE_LOGGING_DEFAULTS


def test_load_usage_logging_config_merges_stored_over_defaults():
    """A partially-saved config (only ``enabled``) must still surface
    all the other defaults — otherwise a stale config could leave
    ``retention_days`` missing and crash the prune job."""
    svcconfig._ensure_dirs()
    svcconfig._USAGE_LOGGING_CONFIG_PATH.write_text(json.dumps({"enabled": True}))

    out = svcconfig.load_usage_logging_config()
    assert out["enabled"] is True
    assert out["retention_days"] == svcconfig._USAGE_LOGGING_DEFAULTS["retention_days"]
    assert out["track_duckdb_httpfs"] is True  # default preserved


def test_load_usage_logging_config_falls_back_to_defaults_on_corrupt_json():
    """Corrupt JSON → return defaults. Pinned because raising here
    would break every analytical request (the connection cache calls
    this on every open)."""
    svcconfig._ensure_dirs()
    svcconfig._USAGE_LOGGING_CONFIG_PATH.write_text("{not json")

    out = svcconfig.load_usage_logging_config()
    assert out == svcconfig._USAGE_LOGGING_DEFAULTS


def test_save_usage_logging_config_writes_atomically():
    svcconfig.save_usage_logging_config({"enabled": True, "retention_days": 60})
    out = svcconfig.load_usage_logging_config()
    assert out["enabled"] is True
    assert out["retention_days"] == 60
    # Tmp file must be gone
    assert not svcconfig._USAGE_LOGGING_CONFIG_PATH.with_suffix(".tmp").exists()


def test_is_usage_logging_enabled_always_false_under_pytest():
    """Pinned as the explicit test-only short-circuit — even with the
    config explicitly enabled, ``pytest`` in ``sys.modules`` returns
    False. Without this every test would log usage rows and pollute
    the dev environment."""
    svcconfig.save_usage_logging_config({"enabled": True, "retention_days": 30})
    # ``pytest`` is in sys.modules during test runs → always False
    assert svcconfig.is_usage_logging_enabled() is False


def test_is_usage_logging_enabled_returns_config_value_when_not_under_pytest():
    """Simulate non-test mode by removing pytest from sys.modules
    temporarily — verifies the production code path returns the
    config value rather than always False."""
    import sys

    pytest_module = sys.modules.pop("pytest", None)
    try:
        svcconfig.save_usage_logging_config({"enabled": True, "retention_days": 30})
        assert svcconfig.is_usage_logging_enabled() is True

        svcconfig.save_usage_logging_config({"enabled": False, "retention_days": 30})
        assert svcconfig.is_usage_logging_enabled() is False
    finally:
        if pytest_module is not None:
            sys.modules["pytest"] = pytest_module


# ── _ensure_dirs ─────────────────────────────────────────────────────────────


def test_ensure_dirs_creates_full_directory_tree():
    """First-run bootstrap: nothing exists. ``_ensure_dirs`` must
    create configs/ AND every data/* sub-dir without raising."""
    # See sibling test ``test_save_config_creates_configs_dir_if_missing``
    # — wipe the conftest-pre-created sandbox to test the bootstrap path.
    import shutil

    shutil.rmtree(svcconfig.CONFIGS_DIR, ignore_errors=True)
    shutil.rmtree(svcconfig.DATA_DIR, ignore_errors=True)
    svcconfig._ensured_dirs.clear()
    assert not svcconfig.CONFIGS_DIR.exists()
    assert not svcconfig.DATA_DIR.exists()

    svcconfig._ensure_dirs()

    assert svcconfig.CONFIGS_DIR.exists()
    assert svcconfig.DATA_DIR.exists()
    assert svcconfig.SERVICES_DATA_DIR.exists()
    assert svcconfig.NGWAF_DATA_DIR.exists()
    assert svcconfig.CACHE_DATA_DIR.exists()
    assert svcconfig.SYSTEM_DATA_DIR.exists()


def test_ensure_dirs_is_idempotent():
    """Repeated calls (every save_config does one) must not raise."""
    svcconfig._ensure_dirs()
    svcconfig._ensure_dirs()  # again — must not raise


@pytest.fixture
def clickhouse_env(monkeypatch):
    import os

    for name in os.environ:
        if name.startswith("CLICKHOUSE_"):
            monkeypatch.delenv(name)
    for key, value in {
        "ENABLED": "true",
        "HOST": "localhost",
        "DATABASE": "analytics",
        "USER": "default",
        "SECURE": "false",
    }.items():
        monkeypatch.setenv(f"CLICKHOUSE_{key}", value)


def test_clickhouse_disabled_by_default(clickhouse_env, monkeypatch):
    monkeypatch.delenv("CLICKHOUSE_ENABLED")
    monkeypatch.setenv("CLICKHOUSE_QUERY_TIMEOUT_S", "not-a-timeout")
    assert svcconfig.load_clickhouse_config() is None


@pytest.mark.parametrize("value", ["false", "0", "no", "off", " FALSE "])
def test_clickhouse_disabled_values(clickhouse_env, monkeypatch, value):
    monkeypatch.setenv("CLICKHOUSE_ENABLED", value)
    assert svcconfig.load_clickhouse_config() is None


@pytest.mark.parametrize("value", ["true", "1", "yes", "on", " TRUE "])
def test_clickhouse_enabled_values(clickhouse_env, monkeypatch, value):
    monkeypatch.setenv("CLICKHOUSE_ENABLED", value)
    settings = svcconfig.load_clickhouse_config()
    assert settings.host == "localhost"
    assert settings.port == 8123
    assert settings.password == ""
    assert settings.connect_timeout_s > 0
    assert settings.query_timeout_s > 0


@pytest.mark.parametrize("key", ["ENABLED", "SECURE"])
@pytest.mark.parametrize("value", ["", "maybe", "2"])
def test_clickhouse_invalid_boolean(clickhouse_env, monkeypatch, key, value):
    monkeypatch.setenv(f"CLICKHOUSE_{key}", value)
    with pytest.raises(ValueError, match=f"CLICKHOUSE_{key}"):
        svcconfig.load_clickhouse_config()


@pytest.mark.parametrize("key", ["HOST", "DATABASE", "USER"])
@pytest.mark.parametrize("value", [None, "", "  "])
def test_clickhouse_incomplete_config(clickhouse_env, monkeypatch, key, value):
    if value is None:
        monkeypatch.delenv(f"CLICKHOUSE_{key}")
    else:
        monkeypatch.setenv(f"CLICKHOUSE_{key}", value)
    with pytest.raises(ValueError, match=f"CLICKHOUSE_{key}"):
        svcconfig.load_clickhouse_config()


@pytest.mark.parametrize("key", ["CONNECT_TIMEOUT_S", "QUERY_TIMEOUT_S", "INSERT_TIMEOUT_S", "POOL_TIMEOUT_S"])
@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "0", "-1", "999999", "bad", ""])
def test_clickhouse_bounded_timeouts(clickhouse_env, monkeypatch, key, value):
    monkeypatch.setenv(f"CLICKHOUSE_{key}", value)
    with pytest.raises(ValueError, match=f"CLICKHOUSE_{key}"):
        svcconfig.load_clickhouse_config()


@pytest.mark.parametrize(
    "key,value",
    [
        ("PORT", "0"),
        ("PORT", "65536"),
        ("PORT", "1.5"),
        ("PORT", "bad"),
        ("POOL_MAX_SIZE", "0"),
        ("POOL_MAX_SIZE", "65"),
    ],
)
def test_clickhouse_bounded_integers(clickhouse_env, monkeypatch, key, value):
    monkeypatch.setenv(f"CLICKHOUSE_{key}", value)
    with pytest.raises(ValueError, match=f"CLICKHOUSE_{key}"):
        svcconfig.load_clickhouse_config()


@pytest.mark.parametrize(
    "key,value",
    [
        ("HOST", "http://user:secret@localhost"),
        ("HOST", "localhost/path"),
        ("HOST", "localhost?password=secret"),
        ("DATABASE", "logs; DROP DATABASE logs"),
        ("DATABASE", "db.table"),
        ("USER", "user\nX-Header: secret"),
    ],
)
def test_clickhouse_rejects_unsafe_settings(clickhouse_env, monkeypatch, key, value):
    monkeypatch.setenv(f"CLICKHOUSE_{key}", value)
    with pytest.raises(ValueError, match=f"CLICKHOUSE_{key}") as error:
        svcconfig.load_clickhouse_config()
    assert value not in str(error.value)


def test_clickhouse_remote_requires_password_and_defaults_to_tls(clickhouse_env, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "clickhouse.example.com")
    monkeypatch.delenv("CLICKHOUSE_SECURE")
    with pytest.raises(ValueError, match="CLICKHOUSE_PASSWORD"):
        svcconfig.load_clickhouse_config()
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "FAKE_PASSWORD")
    settings = svcconfig.load_clickhouse_config()
    assert settings.secure is True
    assert settings.port == 8443
    assert "FAKE_PASSWORD" not in repr(settings)


# ── high_scale_shared_source_enabled / resolve_raw_delete_after ─────────────
#
# The contract: a service's raw request/RUM stream may be shared with an
# out-of-band high-scale consumer (provisioning.cron_sync.high_scale_shared_
# source). While that's set, every legacy raw-deletion path — standard
# sync's delete_after, celery-mode finalize_committed_raw, RUM cleanup, and
# the ingested_files dedup-gate safety check — must stand down regardless of
# its own delete_after setting, because raw-deletion authority for that
# stream belongs to the high-scale archive deletion controller. This is the
# fix for legacy cleanup racing a concurrent high-scale worker and deleting
# raw objects it hasn't archived/published yet.


def test_high_scale_shared_source_enabled_defaults_false():
    assert svcconfig.high_scale_shared_source_enabled(None) is False
    assert svcconfig.high_scale_shared_source_enabled({}) is False
    assert svcconfig.high_scale_shared_source_enabled({"provisioning": {"cron_sync": {}}}) is False


def test_high_scale_shared_source_enabled_reads_the_configured_flag():
    cfg = {"provisioning": {"cron_sync": {"high_scale_shared_source": True}}}
    assert svcconfig.high_scale_shared_source_enabled(cfg) is True


def test_resolve_raw_delete_after_defaults_true_when_unconfigured():
    assert svcconfig.resolve_raw_delete_after(None) is True
    assert svcconfig.resolve_raw_delete_after({}) is True


def test_resolve_raw_delete_after_honors_configured_delete_after():
    cfg = {"provisioning": {"cron_sync": {"delete_after": False}}}
    assert svcconfig.resolve_raw_delete_after(cfg) is False


def test_resolve_raw_delete_after_honors_explicit_requested_override():
    cfg = {"provisioning": {"cron_sync": {"delete_after": True}}}
    assert svcconfig.resolve_raw_delete_after(cfg, requested=False) is False
    assert svcconfig.resolve_raw_delete_after({}, requested=True) is True


def test_resolve_raw_delete_after_forces_false_for_a_high_scale_shared_source():
    """The core regression case: delete_after=True and an explicit True
    override must both lose to a high-scale shared source."""
    cfg = {
        "provisioning": {
            "cron_sync": {"delete_after": True, "high_scale_shared_source": True},
        }
    }
    assert svcconfig.resolve_raw_delete_after(cfg) is False
    assert svcconfig.resolve_raw_delete_after(cfg, requested=True) is False


# ── snapshot + concurrent read-modify-write ──────────────────────────────────


def test_snapshot_config_reads_persisted_content_or_returns_none():
    assert svcconfig.snapshot_config("svc-1") is None
    svcconfig.save_config("svc-1", _cfg())
    snap = svcconfig.snapshot_config("svc-1")
    assert snap is not None
    assert json.loads(snap)["name"] == "My Service"


def test_concurrent_mutate_config_writers_do_not_lose_each_others_keys():
    import threading

    svcconfig.save_config("svc-1", _cfg())
    rounds = 25

    def writer(prefix: str) -> None:
        for i in range(rounds):
            svcconfig.mutate_config("svc-1", lambda c, i=i: c.__setitem__(f"{prefix}{i}", i))

    threads = [threading.Thread(target=writer, args=(p,)) for p in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    final = svcconfig.load_config("svc-1")
    for prefix in ("a", "b"):
        assert all(final[f"{prefix}{i}"] == i for i in range(rounds))


def test_clickhouse_provisioner_is_absent_unless_a_user_is_configured(clickhouse_env):
    assert svcconfig.load_clickhouse_provisioner_config() is None


def test_clickhouse_provisioner_reuses_the_connection_settings_with_its_own_credentials(clickhouse_env, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_PROVISION_USER", "fla_provisioner")
    monkeypatch.setenv("CLICKHOUSE_PROVISION_PASSWORD", "provisioner-pw")
    app = svcconfig.load_clickhouse_config()

    provisioner = svcconfig.load_clickhouse_provisioner_config()

    assert (provisioner.user, provisioner.password) == ("fla_provisioner", "provisioner-pw")
    assert (provisioner.host, provisioner.port, provisioner.database, provisioner.secure) == (
        app.host,
        app.port,
        app.database,
        app.secure,
    )


def test_clickhouse_provisioner_needs_a_password_for_a_secure_or_remote_endpoint(clickhouse_env, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_PROVISION_USER", "fla_provisioner")
    monkeypatch.setenv("CLICKHOUSE_SECURE", "true")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "app-pw")

    with pytest.raises(ValueError, match="CLICKHOUSE_PROVISION_PASSWORD"):
        svcconfig.load_clickhouse_provisioner_config()


def test_clickhouse_provisioner_user_must_look_like_a_user_name(clickhouse_env, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_PROVISION_USER", "bad user; DROP")
    monkeypatch.setenv("CLICKHOUSE_PROVISION_PASSWORD", "pw")

    with pytest.raises(ValueError, match="CLICKHOUSE_PROVISION_USER"):
        svcconfig.load_clickhouse_provisioner_config()


def test_clickhouse_provisioner_is_none_when_clickhouse_is_disabled(clickhouse_env, monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_ENABLED", "false")
    monkeypatch.setenv("CLICKHOUSE_PROVISION_USER", "fla_provisioner")

    assert svcconfig.load_clickhouse_provisioner_config() is None


# ── Merged from test_endpoint_routing.py ──


def _routing_cfg(with_cdn: bool) -> dict:
    base = {
        "service_id": "svc1",
        "name": "Test Service",
        "fos_region": "us-east-1",
        "fos_endpoint": "us-east-1.object.fastlystorage.app",
        "fos_access_key_id": "k",
        "fos_secret_access_key": "s",
        "fos_bucket": "test-bucket",
        "fos_prefix": "",
    }
    if with_cdn:
        base["cdn_url"] = "https://cdn.example.com"
        base["cdn_secret"] = "abc123"
        base["cdn_service_id"] = "cdn-svc"
    return base


class TestConfigToSource:
    def test_native_endpoint_always_present(self):
        src = svcconfig.config_to_source(_routing_cfg(with_cdn=False))
        assert src["fos_native_endpoint"] == "us-east-1.object.fastlystorage.app"
        assert src["endpoint"] == "us-east-1.object.fastlystorage.app"

    def test_cdn_routed_endpoint_keeps_native_separate(self):
        """When a CDN is configured, source['endpoint'] points at the CDN
        (used by DuckDB httpfs for cached parquet GETs) but
        source['fos_native_endpoint'] remains the native FOS host so boto3
        and PyIceberg can do lists/writes/deletes."""
        src = svcconfig.config_to_source(_routing_cfg(with_cdn=True))
        assert src["endpoint"] == "cdn.example.com"
        assert src["fos_native_endpoint"] == "us-east-1.object.fastlystorage.app"
        assert src["fos_native_endpoint"] != src["endpoint"]


# ── Merged from test_config_rum_enabled.py ──

import pytest

from backend.config import is_rum_enabled


@pytest.mark.parametrize(
    "cfg,expected",
    [
        (None, False),
        ({}, False),
        ({"rum_enabled": True}, True),
        ({"rum_enabled": False}, False),
        ({"rum": {"enabled": True}}, True),
        ({"rum": {"enabled": False}}, False),
        ({"rum": {"enabled": False}, "rum_enabled": True}, False),
        ({"rum": {"enabled": True}, "rum_enabled": False}, True),
        ({"rum": {"enabled": None}, "rum_enabled": True}, True),
        ({"rum": {}, "rum_enabled": True}, True),
        ({"rum": "garbage", "rum_enabled": True}, True),
        ({"rum": None, "rum_enabled": True}, True),
        ({"rum": {"enabled": 1}}, True),
        ({"rum": {"enabled": 0}, "rum_enabled": True}, False),
    ],
)
def test_is_rum_enabled_semantics(cfg, expected):
    assert is_rum_enabled(cfg) is expected


def test_rum_status_route_agrees_with_the_scheduler_when_fields_disagree(client, monkeypatch):
    from backend import config as svcconfig
    from tests.conftest import MOCK_SERVICE_ID

    disagreeing = {"service_id": MOCK_SERVICE_ID, "rum": {"enabled": False}, "rum_enabled": True}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: dict(disagreeing))
    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/rum/status")
    assert resp.status_code == 200
    assert resp.json()["enabled"] is is_rum_enabled(disagreeing) is False


_CASES = [
    ({"rum": {"enabled": False}, "rum_enabled": True}, False),
    ({"rum": {"enabled": True}, "rum_enabled": False}, True),
    ({"rum_enabled": True}, True),
    ({"rum": {"enabled": True}}, True),
    ({"rum": {"enabled": None}, "rum_enabled": True}, True),
    ({}, False),
]


@pytest.mark.parametrize("extra,expected", _CASES)
def test_feature_state_from_config_uses_the_shared_resolution(extra, expected):
    from backend.provision.declarative.state import FeatureState

    state = FeatureState.from_config({"service_id": "svc", "log_period": 60, **extra})
    assert state.rum_enabled is expected is is_rum_enabled(extra)


@pytest.mark.parametrize("extra,expected", _CASES)
def test_terraform_generation_uses_the_shared_resolution(extra, expected, monkeypatch):
    from backend.provision.declarative.state import FeatureState

    from backend.utils import terraform_gen

    seen: list[dict] = []
    real = FeatureState.from_config

    def spy(cfg):
        seen.append(cfg)
        return real(cfg)

    monkeypatch.setattr(terraform_gen.FeatureState, "from_config", staticmethod(spy))
    terraform_gen.generate_terraform({"logging_service_id": "svc", "log_period": 60, **extra}, "AK", "SK")
    assert seen[0]["rum_enabled"] is expected


@pytest.mark.parametrize(
    ("final_cfg", "expected"),
    [
        ({"rum": {"enabled": False}, "rum_enabled": True}, False),
        ({"rum": {"enabled": True}, "rum_enabled": False}, True),
        ({"rum_enabled": True}, True),
    ],
)
def test_rum_enable_done_event_reports_the_shared_resolution(final_cfg, expected, client, monkeypatch):
    import json

    from backend import config as svcconfig
    from tests.conftest import MOCK_SERVICE_ID

    state = {"cfg": {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "TOKEN"}}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: dict(state["cfg"]))

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "enabling RUM"}
        state["cfg"] = {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "TOKEN", **final_cfg}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/rum/enable", json={"activate": True})
    assert resp.status_code == 200
    events = [json.loads(line[len("data: ") :]) for line in resp.text.splitlines() if line.startswith("data: ")]
    done = next(e for e in events if e["type"] == "done")
    assert done["rum"]["enabled"] is expected


# ── Merged from test_validate_ingest_mode.py ──

import pytest

_PG = "postgresql://fla:pw@pg:5432/ducklake"


@pytest.fixture
def high_scale_env(monkeypatch):
    """A fully coherent high_scale configuration."""
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_scale")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", _PG)
    monkeypatch.setenv("METADATA_DSN", _PG)
    return monkeypatch


def test_sync_mode_requires_postgres_dsns_but_no_broker(monkeypatch):
    """Standard mode requires Postgres DSNs, but does not require CELERY_BROKER_URL."""
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "standard")
    monkeypatch.setattr(svcconfig, "CELERY_BROKER_URL", "")
    monkeypatch.setattr(svcconfig, "DUCKLAKE_CATALOG", _PG)
    monkeypatch.setenv("METADATA_DSN", _PG)

    assert svcconfig.validate_deployment_mode() is None


def test_coherent_high_scale_config_passes(high_scale_env):
    assert svcconfig.validate_deployment_mode() is None


def test_high_throughput_is_rejected(monkeypatch):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "high_throughput")
    with pytest.raises(
        RuntimeError, match="DEPLOYMENT_MODE=high_throughput has been removed; use 'standard' or 'high_scale'"
    ):
        svcconfig.validate_deployment_mode()


@pytest.mark.parametrize(
    "catalog",
    ["", "/app/data/services/svc.ducklake", "svc.ducklake"],
    ids=["unset", "absolute-file", "relative-file"],
)
def test_high_scale_rejects_non_postgres_ducklake_catalog(high_scale_env, catalog):
    high_scale_env.setattr(svcconfig, "DUCKLAKE_CATALOG", catalog)

    with pytest.raises(RuntimeError, match="requires DUCKLAKE_CATALOG to be a Postgres DSN"):
        svcconfig.validate_deployment_mode()


@pytest.mark.parametrize(
    "dsn",
    [None, "", "/app/data/services/svc.metadata.db", "sqlite:///app/data/svc.metadata.db"],
    ids=["unset", "empty", "sqlite-path", "sqlite-url"],
)
def test_high_scale_rejects_non_postgres_metadata_dsn(high_scale_env, dsn):
    if dsn is None:
        high_scale_env.delenv("METADATA_DSN", raising=False)
    else:
        high_scale_env.setenv("METADATA_DSN", dsn)

    with pytest.raises(RuntimeError, match="requires METADATA_DSN to be a Postgres DSN"):
        svcconfig.validate_deployment_mode()


@pytest.mark.parametrize("scheme", ["postgres", "postgresql"])
def test_both_postgres_url_schemes_accepted(high_scale_env, scheme):
    """libpq accepts both spellings; the gate must not reject the short one
    and send an operator hunting a phantom misconfiguration."""
    dsn = f"{scheme}://fla:pw@pg:5432/db"
    high_scale_env.setattr(svcconfig, "DUCKLAKE_CATALOG", dsn)
    high_scale_env.setenv("METADATA_DSN", dsn)

    assert svcconfig.validate_deployment_mode() is None


def test_metadata_dsn_error_names_the_shared_state_at_risk(high_scale_env):
    """The message must explain WHY, matching the DuckLake error's style —
    an operator seeing it should not need to read the source to know what
    breaks."""
    high_scale_env.delenv("METADATA_DSN", raising=False)

    with pytest.raises(RuntimeError) as exc:
        svcconfig.validate_deployment_mode()

    msg = str(exc.value)
    assert "job_runs" in msg
    assert "ingest ledger" in msg
    assert "pod-local" in msg


def test_unknown_deployment_mode_is_rejected(monkeypatch):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", "unknown")

    with pytest.raises(RuntimeError, match="DEPLOYMENT_MODE"):
        svcconfig.validate_deployment_mode()


@pytest.mark.parametrize("mode", ["standard", "high_scale"])
def test_config_to_source_emits_deployment_mode(monkeypatch, tmp_path, mode):
    monkeypatch.setattr(svcconfig, "DEPLOYMENT_MODE", mode)
    monkeypatch.setattr(svcconfig, "duckdb_path", lambda _service_id: str(tmp_path / "service.duckdb"))

    source = svcconfig.config_to_source({"service_id": "svc", "raw_layout_version": 3})

    assert source["deployment_mode"] == mode
    assert "ingest_mode" not in source
    assert "serving_mode" not in source
