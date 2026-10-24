from __future__ import annotations

from backend.models.common import BaseResponse, DebugCall, DebugQuery


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


def test_debug_query_params_absent_from_wire_unless_set(monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    producer_row = {"sql": "SELECT 1", "time_ms": 1.5, "engine": "DuckDB"}

    plain = BaseResponse(debug_queries=[producer_row]).model_dump(by_alias=True, mode="json")
    assert plain["_debug_queries"] == [{**producer_row, "is_cached": False}]

    bound = BaseResponse(debug_queries=[{**producer_row, "params": ["a", 1]}]).model_dump(by_alias=True, mode="json")
    assert bound["_debug_queries"][0]["params"] == ["a", 1]


# ── Merged from test_security_models.py ──

from backend.models.security import SecurityAggregatesRequest, SecurityAggregatesResponse


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


# ── Merged from test_schema_sync.py ──

import pytest
from pydantic import AliasChoices

from backend.models.dashboard import AggregatesResponse
from backend.models.network import NetworkHealthResponse, NetworkQualityResponse
from backend.models.origin import (
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
from backend.repositories.dashboard import get_aggregates as dash_aggs
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
from backend.repositories.performance import get_performance_aggregates as perf_aggs
from backend.repositories.security import get_security_aggregates as sec_aggs


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


# ── Merged from test_fastly_models.py ──

import pytest

from backend.core.fastly.models import NgwafWorkspacesResponse, StatsRecord, StatsResponse


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
