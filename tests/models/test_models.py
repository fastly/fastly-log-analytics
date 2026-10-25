"""CMCD streaming row models keep every producer's wire bytes (repository + high-scale)."""

from __future__ import annotations

import json
import typing
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import AliasChoices

from backend.core.duckdb import _clear_schema_cache
from backend.core.fastly.client import NgwafWorkspacesResponse, StatsRecord, StatsResponse
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.high_scale import network as hs_network
from backend.high_scale import performance as hs_performance
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.cmcd import cmcd_aggregates as hs_cmcd_aggregates
from backend.high_scale.insights import insights_endpoint as hs_insights_endpoint
from backend.high_scale.origin import aggregates as hs_aggregates
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.high_scale.security import security_aggregates as hs_security_aggregates
from backend.high_scale.sessions import sessions_detail as hs_sessions_detail
from backend.models.cmcd import CmcdAggregatesResponse, CmcdRequest
from backend.models.common import BaseResponse, DebugCall, DebugQuery
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
from backend.models.performance import PerformanceAggregatesResponse
from backend.models.security import (
    SecurityAggregatesRequest,
    SecurityAggregatesResponse,
    SecurityTopBotsResponse,
)
from backend.models.value import IOFormatConversionPair, IOOpportunityRow, TopBotRow, WafSignalRow
from backend.provision.cmcd_fields import _CMCD_CUSTOM_FIELDS
from backend.repositories import origin as origin_mod
from backend.repositories._base import _safe_table
from backend.repositories.cmcd import _response_cache, get_cmcd_aggregates
from backend.repositories.dashboard import get_aggregates as dash_aggs
from backend.repositories.insights import _insights_cache, get_insights, registry
from backend.repositories.network import get_health, get_quality
from backend.repositories.network import get_health as net_health_aggs
from backend.repositories.network import get_quality as net_qual_aggs
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
from backend.repositories.performance import get_performance_aggregates
from backend.repositories.performance import get_performance_aggregates as perf_aggs
from backend.repositories.security import get_security_aggregates, get_security_proxies, get_top_bots
from backend.repositories.security import get_security_aggregates as sec_aggs
from backend.repositories.sessions import get_session_detail
from backend.repositories.value import get_summary as get_value_summary
from tests._helpers.wire import assert_rows_wire_identical, assert_sections_wire_identical, wire
from tests.repositories.test_insights import (
    _MOCK_POP_MAP,
    _create_impossible_distance_table,
    _seed_network_edge_data,
    _seed_track_c_data,
)
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


# ── Merged from tests/models/test_common.py ──
def test_base_response_telemetry_redaction_by_alias_true(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "")  # Disabled

    resp = BaseResponse(
        debug_queries=[DebugQuery(sql="SELECT 1", time_ms=1.5)],
        debug_calls=[DebugCall(service="s3", method="GET", path="foo", time_ms=2.0)],
        is_cached=True,
    )

    # Serialize with by_alias=True
    data = resp.model_dump(by_alias=True)
    assert "_debug_queries" not in data
    assert "_debug_calls" not in data
    assert "debug_queries" not in data
    assert "debug_calls" not in data
    assert data["_is_cached"] is True


def test_base_response_telemetry_redaction_by_alias_false(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "")  # Disabled

    resp = BaseResponse(
        debug_queries=[DebugQuery(sql="SELECT 1", time_ms=1.5)],
        debug_calls=[DebugCall(service="s3", method="GET", path="foo", time_ms=2.0)],
        is_cached=True,
    )

    # Serialize with by_alias=False
    data = resp.model_dump(by_alias=False)
    assert "debug_queries" not in data
    assert "debug_calls" not in data
    assert "_debug_queries" not in data
    assert "_debug_calls" not in data
    assert data["is_cached"] is True


def test_base_response_telemetry_preserved_when_enabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")  # Enabled

    resp = BaseResponse(
        debug_queries=[DebugQuery(sql="SELECT 1", time_ms=1.5)],
        debug_calls=[DebugCall(service="s3", method="GET", path="foo", time_ms=2.0)],
        is_cached=True,
    )

    # Check by_alias=True
    data_alias = resp.model_dump(by_alias=True)
    assert "_debug_queries" in data_alias
    assert "_debug_calls" in data_alias

    # Check by_alias=False
    data_no_alias = resp.model_dump(by_alias=False)
    assert "debug_queries" in data_no_alias
    assert "debug_calls" in data_no_alias


def test_base_response_debug_sqlite_redacted_when_disabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "")  # Disabled

    resp = BaseResponse(debug_sqlite=[{"seq": 1, "sql": "SELECT 1", "time_ms": 0.4}])

    data_alias = resp.model_dump(by_alias=True)
    assert "_debug_sqlite" not in data_alias
    assert "debug_sqlite" not in data_alias
    data_no_alias = resp.model_dump(by_alias=False)
    assert "debug_sqlite" not in data_no_alias
    assert "_debug_sqlite" not in data_no_alias


def test_base_response_debug_sqlite_preserved_when_enabled(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")  # Enabled

    resp = BaseResponse(debug_sqlite=[{"seq": 1, "sql": "SELECT 1", "time_ms": 0.4}])

    assert resp.model_dump(by_alias=True)["_debug_sqlite"] == [{"seq": 1, "sql": "SELECT 1", "time_ms": 0.4}]
    assert "debug_sqlite" in resp.model_dump(by_alias=False)


def test_with_telemetry_pulls_sqlite_collector(monkeypatch):
    """with_telemetry must snapshot the request-scoped SQLite collector —
    and copy it (get_tracked_calls can append a usage_log SELECT to the
    live list after the snapshot)."""
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    from backend.utils import telemetry

    telemetry.start_call_tracking()
    telemetry.record_sqlite_query({"seq": 9, "sql": "SELECT x FROM t", "time_ms": 1.2})
    try:
        resp = BaseResponse.with_telemetry()
        assert resp.debug_sqlite == [{"seq": 9, "sql": "SELECT x FROM t", "time_ms": 1.2}]
        # Snapshot is a copy — later collector appends don't mutate it.
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

    wire = BaseResponse.with_telemetry().model_dump(by_alias=True)

    assert not any(
        key in wire
        for key in (
            "_debug_queries",
            "_debug_calls",
            "_debug_sqlite",
            "_debug_postgres",
            "debug_queries",
            "debug_calls",
            "debug_sqlite",
            "debug_postgres",
        )
    )
    assert wire["_is_cached"] is False


def test_debug_query_params_absent_from_wire_unless_set(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    producer_row = {"sql": "SELECT 1", "time_ms": 1.5, "engine": "DuckDB"}

    plain = BaseResponse(debug_queries=[producer_row]).model_dump(by_alias=True, mode="json")
    assert plain["_debug_queries"] == [{**producer_row, "is_cached": False}]

    bound = BaseResponse(debug_queries=[{**producer_row, "params": ["a", 1]}]).model_dump(by_alias=True, mode="json")
    assert bound["_debug_queries"][0]["params"] == ["a", 1]


def test_security_proxies_response_validation():
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
    assert resp.active_proxies_count == 42
    assert len(resp.active_clients) == 1
    assert resp.active_clients[0].impossible_distance is True


def test_security_aggregates_request_validation():
    req = SecurityAggregatesRequest(start_time="2026-08-17T00:00:00Z", end_time="2026-08-17T23:59:59Z")
    assert req.start_time == "2026-08-17T00:00:00Z"
    assert req.end_time == "2026-08-17T23:59:59Z"


def _src_empty():
    return {"name": "test_empty_src", "service_id": "test_empty_src"}


@pytest.mark.parametrize(
    "repo_fn, pydantic_model, extra_args",
    [
        # Existing coverage
        (dash_aggs, AggregatesResponse, {"chart_interval": "1 hour", "chart_metric": "requests"}),
        (sec_aggs, SecurityAggregatesResponse, {"bucket_seconds": 3600}),
        (net_health_aggs, NetworkHealthResponse, {"bucket_seconds": 3600}),
        (net_qual_aggs, NetworkQualityResponse, {}),
        (get_summary, OriginSummaryResponse, {}),
        (perf_aggs, PerformanceAggregatesResponse, {}),
        # Origin family — Milestone C 2.4 expansion. Each repo function pairs
        # with its dedicated Pydantic response model. If a repo grows a new
        # key without the model declaring it, FastAPI silently strips it on
        # serialization and the dashboard breaks in a way that's hard to
        # debug. This catches the drift at test time.
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
    """
    Ensure that the repository returns dictionaries whose keys are exactly matched by
    the Pydantic response models. This prevents data from being silently stripped by FastAPI.
    """
    src = _src_empty()

    # Call the repository with an empty source/table to get the default empty structure
    if repo_fn.__name__ in ("get_health", "get_quality"):
        repo_result = repo_fn(
            con_factory=lambda: in_memory_duckdb, src=src, start_time=None, end_time=None, filters={}, **extra_args
        )
    else:
        repo_result = repo_fn(con=in_memory_duckdb, src=src, start_time=None, end_time=None, filters={}, **extra_args)

    # Pydantic models automatically include _debug_queries and _debug_calls when initialized normally
    # We remove these telemetry keys from the check because Pydantic serialization aliases handle them.
    repo_keys = set(repo_result.keys())
    repo_keys.discard("_debug_queries")
    repo_keys.discard("_debug_calls")
    repo_keys.discard("_is_cached")

    parsed = pydantic_model.model_validate(repo_result)
    model_keys = set(pydantic_model.model_fields.keys())
    for name in parsed.model_fields_set:
        alias = pydantic_model.model_fields[name].validation_alias
        if isinstance(alias, str):
            model_keys.add(alias)
        elif isinstance(alias, AliasChoices):
            model_keys.update(choice for choice in alias.choices if isinstance(choice, str))

    # Check if the repository is returning keys that the model doesn't know about
    missing_in_model = repo_keys - model_keys

    assert not missing_in_model, (
        f"{pydantic_model.__name__} is missing fields returned by the repository: {missing_in_model}"
    )


def test_stats_record_flattened_fos_ops():
    rec = StatsRecord.model_validate(
        {
            "start_time": 1760000000,
            "object_storage_class_a_operations_count": 10,
            "object_storage_class_b_operations_count": "20",
            "some_future_field": {"x": 1},
        }
    )
    assert rec.fos_ops() == (10, 20)
    assert rec.start_time == 1760000000
    assert rec.model_extra["some_future_field"] == {"x": 1}


def test_stats_record_nested_fos_ops_fallback():
    rec = StatsRecord.model_validate({"object_storage": {"class_a_operations_count": 3, "class_b_operations_count": 4}})
    assert rec.fos_ops() == (3, 4)


@pytest.mark.parametrize("payload", [{}, {"object_storage": None}, {"object_storage": "bad"}])
def test_stats_record_no_fos_data_is_zero(payload):
    assert StatsRecord.model_validate(payload).fos_ops() == (0, 0)


def test_stats_record_null_and_garbage_counts_coerce_to_zero():
    rec = StatsRecord.model_validate({"requests": None, "edge_requests": "n/a", "start_time": None})
    assert rec.requests == 0
    assert rec.edge_requests == 0
    assert rec.start_time is None


def test_stats_response_tolerates_missing_or_null_data():
    assert StatsResponse.model_validate({}).data == []
    assert StatsResponse.model_validate({"data": None}).data == []
    resp = StatsResponse.model_validate({"data": [{"requests": 5}], "meta": {"to": 1}})
    assert resp.data[0].requests == 5


def test_ngwaf_workspaces_data_shape_and_name_fallbacks():
    resp = NgwafWorkspacesResponse.model_validate(
        {
            "data": [
                {"id": "w1", "name": "Prod"},
                {"id": "w2", "attributes": {"name": "Attr Name"}},
                {"id": "w3"},
            ],
            "meta": {"limit": 100},
        }
    )
    assert resp.workspaces_list() == [
        {"id": "w1", "name": "Prod"},
        {"id": "w2", "name": "Attr Name"},
        {"id": "w3", "name": "w3"},
    ]


def test_ngwaf_workspaces_legacy_workspaces_key_and_empty_list():
    assert NgwafWorkspacesResponse.model_validate({"workspaces": [{"id": "a", "name": "A"}]}).workspaces_list() == [
        {"id": "a", "name": "A"}
    ]
    # present-but-empty "data" must not fall through to "workspaces"
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
    parts = [f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields]
    parts += [f'"{cf["name"]}" {cf["duckdb_type"]}' for cf in _CMCD_CUSTOM_FIELDS]
    con.execute(f"CREATE TABLE {table} ({', '.join(parts)})")
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log["cmcd_sid"] = f"session_{i % 5}"
        log["cmcd_bl"] = 15 + i
        log["cmcd_br"] = 4500 + i * 100
        log["cmcd_bs"] = i % 4 == 0
        log["cmcd_mtp"] = 6000 + i * 200
        log["cmcd_ot"] = ["v", "a"][i % 2]
        log["cmcd_sf"] = "h"
        log["cmcd_su"] = i % 10 == 0
        log["cmcd_tb"] = 5000 + i * 100
        log["cmcd_rtp"] = 4800 + i * 100
        log["cmcd_cid"] = f"content_{i % 3}"
        log["cmcd_dl"] = 50 + i
        log["country"] = "US" if i % 2 == 0 else "GB"
        log["asn"] = 7922 if i % 2 == 0 else 3320
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
        start = (now - timedelta(hours=3)).isoformat() if windowed else None
        end = now.isoformat() if windowed else None
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
        present = {"events": 9, "sessions": 3}
        present.update(
            {f"has_{key}": 1 for key in ("bl", "br", "bs", "mtp", "ot", "sf", "su", "tb", "cid", "country", "asn")}
        )
        bucket = "2026-09-14 10:05:00"

        class _Client:
            def execute(self, sql, params=None):
                if "cmcd:probe" in sql:
                    return [present]
                if "cmcd:overview_peak" in sql:
                    return [{"peak_viewers": 3}]
                if "cmcd:overview_duration" in sql:
                    return [{"avg_duration": 90.0}]
                if "cmcd:overview" in sql:
                    return [
                        {
                            "active_sessions": 3,
                            "rebuffer_rate": 33.3,
                            "avg_bitrate": 4000.0,
                            "avg_buffer_length": 20.0,
                            "median_throughput": 7000.0,
                        }
                    ]
                if "cmcd:timeseries" in sql:
                    return [
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
                    ]
                if "cmcd:new_sessions" in sql:
                    return [{"bucket": bucket, "new_sessions": 3}]
                if "cmcd:top_content" in sql:
                    return [
                        {"content_id": "c1", "session_count": 3, "rebuffer_rate": 10.0, "avg_br": 4000.0, "avg_bl": 9.0}
                    ]
                if "cmcd:rebuffer_by_country" in sql:
                    return [{"country": "US", "rebuffer_rate": 10.0, "session_count": 3, "median_mtp": 7000.0}]
                if "cmcd:rebuffer_by_asn" in sql:
                    return [{"asn": 7922, "rebuffer_rate": 10.0, "session_count": 3, "avg_br": 4000.0, "avg_bl": 9.0}]
                if "cmcd:object_type_dist" in sql:
                    return [{"object_type": "v", "request_count": 5}]
                if "cmcd:streaming_format_dist" in sql:
                    return [{"streaming_format": "h", "session_count": 3}]
                if "cmcd:session_duration_dist" in sql:
                    return [{"sort_order": 3, "session_count": 3}]
                raise AssertionError(sql)

        service = SimpleNamespace(service_id="svc", client=_Client())
        res = hs_cmcd_aggregates(
            service, CmcdRequest(bucket_seconds=300), "2026-09-14T10:00:00Z", "2026-09-14T10:10:00Z", None, False
        )
        assert res["has_data"] is True
        assert_sections_wire_identical(
            CmcdAggregatesResponse,
            res,
            non_empty=("overview",) + tuple(section for section in SECTIONS if section != "overview"),
        )


# ── Merged from tests/models/test_wire_dashboard.py ──
def _seed_wire_dashboard(con, src, n=120):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log["elapsed"] = 100_000 + (i * 7919) % 900_000
        log["ttfb"] = f"{(50_000 + (i * 1013) % 400_000) / 1_000_000:.6f}"
        log["ottfb"] = 20_000 + (i * 331) % 30_000
        log["ottlb"] = 60_000 + (i * 331) % 30_000
        log["url"] = f"/path-{i % 4}"
        log["asn"] = [7922, 15169][i % 2]
        log["ttl"] = [0, 5, 45, 3600, 90000][i % 5]
        log["cache"] = "HIT" if i % 3 else "MISS"
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
        log["waf_sig"] = ["SQLI,XSS", "XSS", "VERIFIED-BOT.search-engine"][i % 3]
        log["_bot_name"] = ["googlebot", "bingbot", ""][i % 3]
        log["url"] = f"/img/{i % 4}.jpg"
        log["io_input_bytes"] = 100_000 + i
        log["io_output_bytes"] = 40_000 + i
        log["io_input_format"] = ["jpeg", "png"][i % 2]
        log["io_output_format"] = ["webp", "avif"][i % 2]
        log["resp_bytes"] = 50_000 + i
    insert_mock_logs(con, _safe_table(src["name"]), logs)
    return get_value_summary(
        con,
        src,
        None,
        None,
        {},
        sections={"security", "bots", "io"},
        service_id=None,
    )


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
        captured: dict = {}

        def _capture(cls, **kwargs):
            captured.update(kwargs)
            return cls(**kwargs)

        monkeypatch.setattr(PerformanceAggregatesResponse, "with_telemetry", classmethod(_capture))
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda sid, asns: {7922: "Comcast"})
        service = SimpleNamespace(service_id="svc", client=_HsClient())
        hs_performance.performance_aggregates(service, SimpleNamespace(), None, None)
        assert captured["top_asns"][0]["asn"] == 7922
        assert captured["top_asns"][0]["label"] == "Comcast (7922)"
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
        kinds = {type(v) for row in res["data"] for v in row.values()}
        assert {str, int, float} <= kinds, "fixture must exercise string, integer and float cells"
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

        class _Client:
            def execute(self, query, params):
                return [_row(0, "/a", 200, 64500), _row(2, "/b", 404, None)]

        service = type("Svc", (), {"client": _Client(), "service_id": "svc"})()
        req = SessionDetailRequest(ip="203.0.113.7")
        res = hs_sessions_detail(service, req, _START, _END, "203.0.113.7", None)  # type: ignore[arg-type]
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
_TAGS = {
    t.model_fields["id"].annotation.__args__[0]  # type: ignore[union-attr]
    for t in typing.get_args(typing.get_args(InsightCard)[0])
}

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
        assert dumped["id"] == card["id"]
        assert len(dumped["items"]) == len(card["items"])
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
        items = []
        for row in _ROWS[insight_id]:
            item = definition.row_processor(row, definition, dict(_CONTEXT))
            item["investigate_url"] = "/dashboard?service=svc&start=a&end=b"
            items.append(item)
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
            '"timestamp" TIMESTAMPTZ, "metro" USMALLINT, "delivery_rate" UBIGINT, "c_type" VARCHAR, '
            '"c_speed" VARCHAR, "pop" VARCHAR, "elapsed" UBIGINT, "transport" VARCHAR, "cache" VARCHAR, '
            '"url" VARCHAR, "status" USMALLINT, "resp_bytes" UBIGINT, "resp_header_content_encoding" VARCHAR, '
            '"ip" VARCHAR, "cookie_session" VARCHAR, "oconnect_ms" UINTEGER, "ottfb" UBIGINT)'
        )
        _seed_network_edge_data(in_memory_duckdb, table)
        _seed_track_c_data(in_memory_duckdb, table)
        _insights_cache.clear()
        res = get_insights(in_memory_duckdb, test_service_source, window_hours=1, baseline_hours=24)
        cards = [c for c in res["insights"] if c["items"]]
        fired = {c["id"] for c in cards}
        assert {
            "metro_delivery_degradation",
            "connection_type_mix",
            "pop_latency_regression",
            "http3_fallback",
            "cache_hit_cliff",
            "payload_compression_regression",
            "session_harvesting",
            "timeout_split",
        } <= fired
        assert _assert_cards_wire_identical(cards) >= 8

    def test_repository_network_asn_health_and_low_and_slow_wire_identical(self, in_memory_duckdb, test_service_source):
        from datetime import timedelta

        table = _safe_table(test_service_source["name"])
        in_memory_duckdb.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            '("timestamp" TIMESTAMPTZ, "asn" INTEGER, "ploss" DOUBLE, "rtt_var" INTEGER, "retrans" DOUBLE, '
            '"ip" VARCHAR, "url" VARCHAR, "ja3" VARCHAR)'
        )
        now = datetime.now(UTC)
        for _ in range(60):
            in_memory_duckdb.execute(
                f"INSERT INTO {table} VALUES (?, 64500, 0.08, 1000, 0.0, NULL, NULL, NULL)",
                [(now - timedelta(minutes=30)).isoformat()],
            )
        for _ in range(120):
            in_memory_duckdb.execute(
                f"INSERT INTO {table} VALUES (?, 64500, 0.001, 1000, 0.0, NULL, NULL, NULL)",
                [(now - timedelta(minutes=90)).isoformat()],
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
        assert card["items"][0]["label"] == "/a"
        assert card["items"][0]["meta"] == {"requests": 10, "errors": 7, "filters": {"url": "/a"}}
        assert card["severity"] == "critical" and card["category"] == "origin"


# ── Merged from tests/models/test_wire_network.py ──
def _seed_wire_network(con, src, n=120):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=2)
    for i, log in enumerate(logs):
        log["tcp_rtt"] = 20_000 + (i * 997) % 90_000
        log["rtt_min"] = 10_000 + (i * 131) % 8_000
        log["rtt_var"] = 500 + (i * 53) % 5_000
        log["ploss"] = 0.0 if i % 7 else 0.02
        log["asn"] = [7922, 15169, 701][i % 3]
        log["status"] = 200 if i % 9 else 503
        log["pop"] = ["SJC", "IAD", "LAX"][i % 3]
        log["region"] = ["CA", "NY"][i % 2]
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
        if "INNER JOIN" in sql:  # shielding self-join: covered in tests/high_scale/test_network_parity.py
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
        captured: dict = {}

        def _capture(cls, **kwargs):
            captured.update(kwargs)
            return cls(**kwargs)

        monkeypatch.setattr(model, "with_telemetry", classmethod(_capture))
        monkeypatch.setattr("backend.core.duckdb.get_asn_names", lambda _sid, asns: {a: f"AS{a}" for a in asns})
        getattr(hs_network, fn)(_service(), _Req(), _Req.start_time, _Req.end_time)
        non_empty = (
            ("heatmap", "leaderboard", "map_buckets", "cities", "metro_leaderboard", "summary")
            if fn == "network_health"
            else ("by_country", "by_region")
        )
        assert_sections_wire_identical(model, captured, non_empty=non_empty)


# ── Merged from tests/models/test_wire_origin.py ──
def _seed_wire_origin(con, src, n=60):
    logs = generate_mock_logs(src, num_logs=n, hours_ago=1)
    for i, log in enumerate(logs):
        log["ottfb"] = 50000 + i * 1000
        log["ottlb"] = 90000 + i * 1000
        log["elapsed"] = log["ottlb"] + 5000
        log["ost"] = 200 if i % 5 else 500
        if i == 3:
            log["ost"] = 829
        log["oip"] = "203.0.113.1" if i < 25 else "203.0.113.2"
        log["url"] = "/api/data" if i % 2 == 0 else "/images/logo.png"
        log["pop"] = ["SJC", "IAD", "LAX"][i % 3]
        log["host"] = "example.com" if i % 2 else "www.example.com"
        log["edge"] = i % 4 != 0
        log["cache"] = "MISS" if i % 3 == 0 else ("PASS" if i % 3 == 1 else "HIT")
        log["rid"] = f"rid-{i}"
        log["prid"] = f"rid-{i - 1}" if i % 4 == 0 and i else None
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def _call(fn, con, src, **kw):
    return fn(con=con, src=src, start_time=None, end_time=None, filters={}, **kw)


class _HsClient_wire_origin:
    def execute(self, sql, params=None):
        if "origin:summary" in sql:
            return [
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
            ]
        if "origin:timeseries" in sql:
            return [{"time": "2026-09-14 10:00:00", "miss_count": 6, "value_q": [2500.0], "value_n": 6}]
        if "origin:slow_urls" in sql:
            return [{"value": "/slow", "requests": 5, "latency_q": [1000.0, 2500.0, 3000.0], "latency_n": 5}]
        if "origin:status_codes" in sql:
            return [{"value": "503", "requests": 2, "total_requests": 10}]
        if "origin:path_breakdown" in sql:
            return [
                {"value": "false", "requests": 4, "latency_q": [1200.0, 2400.0], "latency_n": 4},
                {"value": "true", "requests": 6, "latency_q": [1000.0, 2000.0], "latency_n": 6},
            ]
        if "origin:pop_latency" in sql:
            return [
                {"value": "SJC", "requests": 4, "latency_q": [1000.0, 1000.0], "latency_n": 4},
                {"value": "LAX", "requests": 3, "latency_q": [1500.0, 5000.0], "latency_n": 3},
            ]
        if "origin:ip_health" in sql:
            return [
                {"value": "203.0.113.1", "requests": 7, "origin_5xx": 1, "latency_q": [1000.0, 2000.0], "latency_n": 7}
            ]
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
        assert_sections_wire_identical(
            OriginSummaryResponse, _call(origin_mod.get_summary, con, src), non_empty=("has_data",)
        )
        assert_sections_wire_identical(
            OriginTimeseriesResponse, _call(origin_mod.get_timeseries, con, src), non_empty=("series",)
        )
        split = _call(origin_mod.get_timeseries, con, src, split_by_leg=True)
        assert any("edge" in point for point in split["series"])
        assert_sections_wire_identical(OriginTimeseriesResponse, split, non_empty=("series",))
        assert_sections_wire_identical(
            OriginSlowUrlsResponse, _call(origin_mod.get_slow_urls, con, src, min_requests=1), non_empty=("rows",)
        )
        assert_sections_wire_identical(
            OriginStatusCodesResponse, _call(origin_mod.get_status_codes, con, src), non_empty=("rows",)
        )
        assert_sections_wire_identical(
            OriginPathBreakdownResponse, _call(origin_mod.get_path_breakdown, con, src), non_empty=("rows",)
        )
        assert_sections_wire_identical(
            OriginPopLatencyResponse, _call(origin_mod.get_pop_latency, con, src), non_empty=("rows",)
        )
        assert_sections_wire_identical(
            OriginIpHealthResponse, _call(origin_mod.get_ip_health, con, src), non_empty=("rows",)
        )
        for dim in ("host", "pop", "oip"):
            assert_sections_wire_identical(
                OriginDrilldownResponse, _call(origin_mod.get_drilldown, con, src, dimension=dim), non_empty=("rows",)
            )
        assert_sections_wire_identical(
            OriginShieldingAnalysisResponse, _call(origin_mod.get_shielding_analysis, con, src), non_empty=("rows",)
        )

    @pytest.mark.asyncio
    async def test_composite_aggregates_wire_identical(self, in_memory_duckdb, test_service_source):
        _seed_wire_origin(in_memory_duckdb, test_service_source)
        res = await origin_mod.get_aggregates(
            in_memory_duckdb, test_service_source, None, None, {}, slow_urls_min_requests=1
        )
        assert_sections_wire_identical(
            OriginAggregatesResponse,
            res,
            non_empty=(
                "summary",
                "timeseries",
                "slow_urls",
                "status_codes",
                "path_breakdown",
                "pop_latency",
                "ip_health",
            ),
        )
        for section in ("timeseries", "slow_urls", "status_codes", "path_breakdown", "pop_latency", "ip_health"):
            assert res[section]["rows" if section != "timeseries" else "series"], section

    @pytest.mark.asyncio
    async def test_composite_aggregates_empty_schema_wire_identical(self, in_memory_duckdb):
        src = {"name": "origin_wire_empty", "service_id": "origin_wire_empty"}
        res = await origin_mod.get_aggregates(in_memory_duckdb, src, None, None, {})
        assert_sections_wire_identical(OriginAggregatesResponse, res)

    def test_high_scale_aggregates_wire_identical(self):
        service = SimpleNamespace(service_id="svc", client=_HsClient_wire_origin())
        req = SimpleNamespace(
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
        res = hs_aggregates(service, req, None, None, sections=None)
        assert_sections_wire_identical(
            OriginAggregatesResponse,
            res,
            non_empty=(
                "summary",
                "timeseries",
                "slow_urls",
                "status_codes",
                "path_breakdown",
                "pop_latency",
                "ip_health",
            ),
        )
        dumped = OriginAggregatesResponse.model_validate(res).model_dump(by_alias=True)
        for section in ("summary", "timeseries", "slow_urls", "path_breakdown", "pop_latency", "ip_health"):
            assert dumped[section]["_approx"] is True
            assert "histogram sketch" in dumped[section]["_approx_note"], section

    def test_high_scale_empty_sections_wire_identical(self):
        class _Empty:
            def execute(self, sql, params=None):
                return []

        service = SimpleNamespace(service_id="svc", client=_Empty())
        req = SimpleNamespace(
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
        res = hs_aggregates(service, req, None, None, sections=None)
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
    rows = []
    for i in range(n):
        rows.append(
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
        )
    con.executemany(f"INSERT INTO {table} VALUES ({', '.join('?' * 18)})", rows)


def _wellknown_and_verified(con, src):
    res = get_security_aggregates(
        con=con,
        src=src,
        start_time=START,
        end_time=END,
        filters={},
        sections={"wellknown_bots", "verified_bots_ts"},
        bucket_seconds=3600,
    )
    return res


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
            res = _wellknown_and_verified(in_memory_duckdb, test_service_source)
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
        captured: dict = {}

        def _capture(cls, **kwargs):
            captured.update(kwargs)
            return cls(**kwargs)

        monkeypatch.setattr(hs_security.SecurityAggregatesResponse, "with_telemetry", classmethod(_capture))
        with patch("backend.core.duckdb.get_asn_names", return_value={}):
            hs_security_aggregates(svc, None, START, END)
        assert captured["tls_fingerprints"][0]["ip_count"] == 2
        assert captured["req_size_dist"][0]["bucket"] == "0-256B"
        assert captured["ipv6_adoption"][0]["pct"] == 33.3
        assert_sections_wire_identical(
            SecurityAggregatesResponse,
            captured,
            non_empty=(
                "tls_fingerprints",
                "req_size_dist",
                "ipv6_adoption",
                "conn_reuse_dist",
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
