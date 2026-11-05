"""Consolidated unit and integration tests for analytics routers."""

from __future__ import annotations

import json
import re
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from starlette.routing import Mount

import backend.core.metadata as metadata_db
import backend.provision.rum_assets as _rum_assets  # noqa: F401
from backend import config as svcconfig
from backend.core.request_context import build_request_context
from backend.main import app  # noqa: E402
from backend.models.analytics import AssetsRequest, InsightsRequest, SecurityAggregatesResponse
from backend.utils import telemetry as telemetry_mod
from backend.utils.bounded_cache import CacheRegistry
from backend.utils.date_utils import iso_z
from backend.utils.remote_access import _ANALYST_ALLOWED_WRITE_PREFIXES, TimeBounds
from tests.conftest import MOCK_SERVICE_ID, override_request_context

_SVC_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}


@pytest.fixture(autouse=True)
def _default_high_scale_service(monkeypatch):
    def _execute(sql: str, params: dict | None = None, settings: dict | None = None) -> list[dict[str, Any]]:
        queries = telemetry_mod.get_queries()
        queries.append({"sql": str(sql), "time_ms": 0.1, "engine": "ClickHouse", "is_cached": False})
        telemetry_mod._QUERIES.set(queries)
        s = str(sql)
        if "system.one" in s or "SELECT count() AS n" in s:
            return [{"n": 0}]
        if "SELECT count(*)" in s:
            return [{"count()": 20}]
        if "insight:cache_collapse_rates" in s:
            return [
                {
                    "w_hits": 0,
                    "w_miss": 5,
                    "w_pass": 0,
                    "w_total": 5,
                    "b_hits": 0,
                    "b_miss": 0,
                    "b_pass": 0,
                    "b_total": 0,
                }
            ]
        return []

    services: dict[str, Any] = {}

    def _wm(sid: str, domain: str = "request"):
        return SimpleNamespace(
            service_id=sid,
            domain=domain,
            coverage_start=None,
            coverage_end=None,
            last_visible_event_id=None,
            validate=lambda: None,
        )

    def _resolve(sid: str):
        if sid not in services:
            client = SimpleNamespace(execute=_execute)
            services[sid] = SimpleNamespace(
                service_id=sid,
                client=client,
                watermark=lambda s=sid: _wm(s, "request"),
                watermark_for=lambda domain="request", s=sid: _wm(s, domain),
            )
        return services[sid]

    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=_resolve),
    )


def _naive_iso_window(hours: int = 3) -> tuple[str, str]:
    now = datetime.now(UTC)
    return (
        (now - timedelta(hours=hours)).replace(tzinfo=None).isoformat(),
        (now + timedelta(hours=hours)).replace(tzinfo=None).isoformat(),
    )


@contextmanager
def _with_analyst_ctx(
    src,
    con=None,
    *,
    mask_ips: bool = False,
    time_bounds=None,
    path: str | None = None,
    session_id: str = "sess-analyst-1",
):
    s_kw = {"session_id": session_id, "pii_policy": {"mask_ips": mask_ips}}
    session = SimpleNamespace(**s_kw, service_ids=[src["service_id"]], time_bounds=time_bounds)
    kw = {"source": src, "con": con, "_con_override": con, "session": session, "time_bounds": time_bounds}
    if path is not None:
        kw["path"] = path
    app.dependency_overrides[build_request_context] = override_request_context(**kw)
    try:
        yield
    finally:
        app.dependency_overrides.pop(build_request_context, None)


@contextmanager
def _patch_range_token(
    earliest: str = "2026-08-24T00:00:00Z",
    window: tuple[str, str] = ("2026-08-24T12:00:00Z", "2026-08-24T13:00:00Z"),
):
    with (
        patch("backend.config.get_status", return_value={"earliest_log_at": earliest}),
        patch("backend.utils.time_window.is_valid_range_token", return_value=True),
        patch("backend.utils.time_window.resolve_window", return_value=window),
    ):
        yield


def _register_high_scale(monkeypatch, test_service_source):
    def _exec(sql: str, params: dict | None = None, settings: dict | None = None) -> list[dict[str, Any]]:
        queries = telemetry_mod.get_queries()
        queries.append({"sql": str(sql), "time_ms": 0.1, "engine": "ClickHouse", "is_cached": False})
        telemetry_mod._QUERIES.set(queries)
        return []

    sid = test_service_source["service_id"]
    service = SimpleNamespace(
        service_id=sid,
        client=SimpleNamespace(execute=_exec),
        watermark=lambda: SimpleNamespace(
            service_id=sid,
            domain="request",
            coverage_start=None,
            coverage_end=None,
            last_visible_event_id=None,
            validate=lambda: None,
        ),
        watermark_for=lambda domain="request": SimpleNamespace(
            service_id=sid,
            domain=domain,
            coverage_start=None,
            coverage_end=None,
            last_visible_event_id=None,
            validate=lambda: None,
        ),
    )
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=MagicMock(return_value=service)),
    )
    return service


# ── Insights Router Tests ──
def _stub_insights() -> dict:
    now = "2026-10-12T00:00:00+00:00"
    item = {"label": "San Jose, CA, US", "current_val": 120.0, "baseline_val": 20.0, "baseline_label": "baseline"}
    item |= {"unit": "req", "severity": "medium", "meta": {"spike_ratio": 6.0, "filters": {}}}
    ins = {"id": "city_surges", "title": "Geographic Surges", "description": "Cities with sudden traffic surges"}
    ins |= {"severity": "medium", "summary": "1 city surging", "items": [item], "category": "traffic"}
    res = {"insights": [ins], "window_start": now, "window_end": now, "baseline_start": now, "baseline_end": now}
    return {**res, "computed_at": now, "window_hours": 24.0, "baseline_hours": 168.0}


def _stub_cache_collapse_detail() -> dict:
    tl = {"bucket": "2026-10-12T00:00:00+00:00", "expected_hits": 90.0, "real_hits": 10, "misses": 90}
    tl |= {"total_requests": 100, "hit_rate": 0.1}
    miss = {"timestamp": "2026-10-12T00:01:00+00:00", "cache": "MISS", "pop": "SJC", "ip": "1.2.3.4", "status": 200}
    res = {"url": "/api/v1/heavy", "timeline": [tl], "recent_misses": [miss]}
    res |= {"breakdown": {"hits": 10, "misses": 90, "passes": 0, "other": 0}, "baseline_hit_rate": 0.95}
    return {
        **res,
        "window_hit_rate": 0.1,
        "baseline_pass_rate": 0.0,
        "window_pass_rate": 0.0,
        "has_data": True,
        "clusters": [],
    }


def test_insights_request_validation():
    valid = InsightsRequest(window_size_hrs=24.0, baseline_hours=168.0, force_refresh=True)
    assert (valid.window_size_hrs, valid.baseline_hours, valid.force_refresh) == (24.0, 168.0, True)
    for bad_kw in ({"window_size_hrs": 0}, {"window_size_hrs": 169}, {"baseline_hours": 0}, {"baseline_hours": 2161}):
        with pytest.raises(Exception):
            InsightsRequest(**bad_kw)


@pytest.mark.parametrize("is_analyst", [False, True])
def test_insights_role_clamping_and_force_refresh(client, test_service_source, is_analyst):
    now = datetime.now(UTC)
    start, end = now - timedelta(hours=3), now - timedelta(hours=2)
    captured: dict = {}

    def _fake(service, req, clamp_start, clamp_end, mask_ips, *, clamp_cache_key=None, force_refresh=False):
        captured.update(
            force_refresh=force_refresh,
            mask_ips=mask_ips,
            clamp_start=clamp_start,
            clamp_end=clamp_end,
        )
        return _stub_insights()

    tb = TimeBounds(start=start, end=end)
    ctx_mgr = (
        _with_analyst_ctx(test_service_source, mask_ips=True, time_bounds=tb, path="/api/insights")
        if is_analyst
        else contextmanager(lambda: (yield))()
    )
    req_json = {"window_size_hrs": 1.0, "baseline_hours": 168.0, "force_refresh": True}
    with ctx_mgr, patch("backend.routers.insights.hs_insights", side_effect=_fake):
        resp = client.post("/api/insights", headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200, resp.text
    if is_analyst:
        assert (captured["force_refresh"], captured["mask_ips"]) == (False, True)
        assert (captured["clamp_start"], captured["clamp_end"]) == (start.isoformat(), end.isoformat())
    else:
        assert (captured["force_refresh"], captured["clamp_start"], captured["clamp_end"]) == (True, None, None)


def test_insights_endpoint_stale_view_retry_and_real_execution(client):
    with patch("backend.routers.insights.hs_insights", return_value=_stub_insights()):
        resp = client.post(
            "/api/insights", headers=_SVC_HEADERS, json={"window_size_hrs": 24.0, "baseline_hours": 168.0}
        )
    assert resp.status_code == 200 and resp.json()["insights"][0]["id"] == "city_surges"

    resp_real = client.post("/api/insights", headers=_SVC_HEADERS, json={"window_hours": 1.0, "baseline_hours": 24.0})
    assert resp_real.status_code == 200 and isinstance(resp_real.json()["insights"], list)


def test_cache_collapse_detail_endpoint_stub_and_real(client):
    req_stub = {"url": "/api/v1/heavy", "window_size_hrs": 24.0, "baseline_hours": 168.0}
    with patch("backend.routers.insights.hs_detail", return_value=_stub_cache_collapse_detail()):
        resp = client.post("/api/insights/cache-collapse-detail", headers=_SVC_HEADERS, json=req_stub)
    assert resp.status_code == 200
    data = resp.json()
    assert (data["url"], data["baseline_hit_rate"], data["breakdown"]["hits"]) == ("/api/v1/heavy", 0.95, 10)

    req_real = {"url": "/test-url-collapse", "window_size_hrs": 1.0, "baseline_hours": 24.0}
    resp_real = client.post("/api/insights/cache-collapse-detail", headers=_SVC_HEADERS, json=req_real)
    assert resp_real.status_code == 200
    real_data = resp_real.json()
    assert real_data["url"] == "/test-url-collapse"
    assert (real_data["baseline_hit_rate"], real_data["window_hit_rate"]) == (0.0, 0.0)
    assert (real_data["baseline_pass_rate"], real_data["window_pass_rate"]) == (0.0, 0.0)
    assert set(real_data["breakdown"].keys()) == {"hits", "misses", "passes", "other"} and "evictions" not in real_data


# ── Shared Cross-Router Contract Tests ──
@pytest.mark.parametrize(
    "endpoint",
    ["/api/network-health", "/api/origin/aggregates", "/api/security/aggregates", "/api/performance/aggregates"],
)
def test_aggregates_reject_unknown_section(client, endpoint):
    resp = client.post(endpoint, headers=_SVC_HEADERS, json={"filters": {}, "sections": ["not_a_section"]})
    assert resp.status_code in (400, 422)


_RANGE_TOKEN_ENDPOINTS = [
    ("/api/origin/aggregates", "summary"),
    ("/api/performance/aggregates", "top_urls"),
    ("/api/security/aggregates", "tls_fingerprints"),
    ("/api/value/summary", "overview"),
    ("/api/assets/aggregates", "asset_type_breakdown"),
]


@pytest.mark.parametrize(("endpoint", "expected_key"), _RANGE_TOKEN_ENDPOINTS)
def test_aggregates_range_token_resolution(client, endpoint, expected_key):
    with _patch_range_token():
        resp = client.post(
            endpoint, headers=_SVC_HEADERS, json={"filters": {}, "range_token": "24h", "anchor": "2026-08-24T13:00:00Z"}
        )
    assert resp.status_code == 200 and expected_key in resp.json()


_CLAMP_CASES = [
    (
        "/api/performance/aggregates",
        "backend.routers.performance.hs_performance",
        {"top_urls": [], "top_asns": [], "ttl_dist": [], "scatter": [], "waterfall": {}},
    ),
    (
        "/api/value/summary",
        "backend.routers.performance.hs_value_summary",
        {"overview": {"total_requests": 100, "origin_offload_pct": 95.0}},
    ),
]


@pytest.mark.parametrize(("endpoint", "repo_target", "stub_return"), _CLAMP_CASES)
def test_analyst_session_clamps_requested_window(client, test_service_source, endpoint, repo_target, stub_return):
    start, end = datetime(2026, 6, 17, 9, 0, tzinfo=UTC), datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    captured: dict = {}

    def _fake(service, req, start_time, end_time, **kw):
        captured.update(start_time=start_time, end_time=end_time, **kw)
        return stub_return

    tb = TimeBounds(start=start, end=end)
    req_json = {"filters": {}, "start_time": "2026-06-17T00:00:00Z", "end_time": "2026-06-17T23:59:59Z"}
    with (
        _with_analyst_ctx(test_service_source, mask_ips=True, time_bounds=tb, path=endpoint),
        patch(repo_target, side_effect=_fake),
    ):
        resp = client.post(endpoint, headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200, resp.text
    assert (captured["start_time"], captured["end_time"]) == (start.isoformat(), end.isoformat())


@pytest.mark.parametrize("endpoint", ["/api/performance/aggregates", "/api/security/aggregates"])
def test_aggregates_telemetry_attribution(client, monkeypatch, endpoint):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    resp = client.post(endpoint, headers={**_SVC_HEADERS, "X-Page-Load-ID": "test-page-load-42"}, json={"filters": {}})
    assert resp.status_code == 200 and len(resp.json().get("_debug_queries", [])) > 0


# ── Network Router Tests ──
def test_network_health_selectors_and_error_sentinel(client):
    full = client.post("/api/network-health", headers=_SVC_HEADERS, json={"filters": {}}).json()
    for k in ("shielding_analysis", "summary", "heatmap", "available"):
        assert k in full

    sh_only = client.post(
        "/api/network-health", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["shielding_analysis"]}
    ).json()
    assert "shielding_analysis" in sh_only
    timings = {t["section"] for t in sh_only.get("_section_timings", [])}
    for blocked in {"heatmap", "map", "metro", "speed", "percentiles"}:
        assert blocked not in timings

    combo = client.post(
        "/api/network-health",
        headers=_SVC_HEADERS,
        json={"filters": {}, "sections": ["leaderboard", "shielding_analysis"]},
    ).json()
    assert "leaderboard" in combo and "shielding_analysis" in combo


def test_get_pop_health_returns_data(client, monkeypatch):
    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        if "custom_fields['pop'] AS pop" in str(sql):
            return [
                {
                    "pop": "IAD",
                    "requests": 5,
                    "errors": 0,
                    "p50_rtt_us": 12000.0,
                    "p95_ttfb_ms": 50.0,
                    "cache_hits": 5,
                    "bandwidth_bytes": 1000,
                }
            ]
        return []

    svc = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=SimpleNamespace(execute=_exec), watermark=lambda: None)
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda _sid: svc),
    )
    st, et = _naive_iso_window(3)
    response = client.get(f"/api/network/pop-health?start_time={st}&end_time={et}", headers=_SVC_HEADERS)
    assert response.status_code == 200
    row = response.json()["data"][0]
    assert (row["pop"], row["requests"], row["errors"], row["cache_hit_rate"]) == ("IAD", 5, 0, 100.0)


def test_network_bundle_post_get_selective_and_debug(client, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES_FORCE_INCLUDE", "0")

    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        queries = telemetry_mod.get_queries()
        queries.append({"sql": str(sql), "time_ms": 0.1, "engine": "ClickHouse", "is_cached": False})
        telemetry_mod._QUERIES.set(queries)
        s = str(sql)
        if "custom_fields['pop'] AS pop, count() AS requests" in s:
            return [
                {
                    "pop": "IAD",
                    "requests": 5,
                    "errors": 0,
                    "p50_rtt_us": 20000.0,
                    "p95_ttfb_ms": 45.0,
                    "cache_hits": 5,
                    "bandwidth_bytes": 10000,
                },
                {
                    "pop": "SJC",
                    "requests": 5,
                    "errors": 0,
                    "p50_rtt_us": 20000.0,
                    "p95_ttfb_ms": 45.0,
                    "cache_hits": 5,
                    "bandwidth_bytes": 10000,
                },
            ]
        if "sum(toInt64OrZero(custom_fields['resp_bytes'])) AS total_bytes" in s:
            return [
                {
                    "total_reqs": 10,
                    "total_bytes": 20000,
                    "rtt_p50": 20.0,
                    "rtt_p90": 20.0,
                    "rtt_p95": 20.0,
                    "rtt_p99": 20.0,
                    "avg_retrans": 1.0,
                    "avg_ploss_pct": 0.5,
                    "avg_del_rate_mbps": 80.0,
                    "seen_tcp_rtt": 10,
                    "seen_retrans": 10,
                    "seen_ploss": 10,
                    "seen_delivery_rate": 10,
                }
            ]
        if "AS proto_clean, count() AS reqs" in s:
            return [{"proto_clean": "HTTP/2", "reqs": 10}]
        if "SELECT count() AS n" in s:
            return [{"n": 0}]
        return []

    svc = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=SimpleNamespace(execute=_exec), watermark=lambda: None)
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda _sid: svc),
    )
    st, et = _naive_iso_window(3)

    all_secs = ["health", "quality", "pop_health", "transport"]
    resp = client.post(
        "/api/network/bundle",
        headers=_SVC_HEADERS,
        json={"start_time": st, "end_time": et, "sections": all_secs, "filters": {}},
    )
    assert resp.status_code == 200
    data = resp.json()
    for sec in all_secs:
        assert data.get(sec) is not None
    assert len(data["pop_health"]) == 2 and "_component_observations" not in data
    t = data["transport"]
    t_vals = (
        t["total_requests"],
        t["tcp_rtt_p50_ms"],
        t["retrans_rate_pct"],
        t["packet_loss_pct"],
        t["delivery_rate_mbps"],
    )
    assert t_vals == (10, 20.0, 1.0, 0.5, 80.0) and len(t["protocol_distribution"]) > 0

    get_resp = client.get(f"/api/network/bundle?start_time={st}&end_time={et}", headers=_SVC_HEADERS)
    assert get_resp.status_code == 200 and "health" in get_resp.json() and "transport" in get_resp.json()

    debug_headers = {**_SVC_HEADERS, "x-debug-responses": "1"}
    sel_resp = client.post(
        "/api/network/bundle",
        headers=debug_headers,
        json={"start_time": st, "end_time": et, "sections": ["transport"], "filters": {}},
    )
    assert sel_resp.status_code == 200
    sel_data = sel_resp.json()
    assert sel_data["transport"] is not None
    assert (sel_data["health"], sel_data["quality"], sel_data["pop_health"]) == (None, None, None)
    assert len(sel_data["_debug_queries"]) > 0 and sel_data.get("_component_observations") is None

    payload = {"start_time": st, "end_time": et, "sections": ["pop_health"], "filters": {}}
    pop_data = client.post("/api/network/bundle", headers=debug_headers, json=payload).json()
    assert len(pop_data["pop_health"]) == 2
    owner = pop_data["_component_observations"]["pop_health"]
    assert "data" not in owner and owner["_response_cache"]["state"] == "bypass"
    assert [row["section"].removeprefix("network.") for row in owner["_section_timings"]] == ["pop_health"]

    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    no_debug = client.post("/api/network/bundle", headers=debug_headers, json=payload)
    assert no_debug.status_code == 200 and "_component_observations" not in no_debug.json()
    assert any("/api/network/bundle".startswith(prefix) for prefix in _ANALYST_ALLOWED_WRITE_PREFIXES)


def test_network_bundle_missing_optional_fields_fallback(client, monkeypatch):
    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        if "count() AS total_reqs" in str(sql):
            return [
                {
                    "total_reqs": 5,
                    "total_bytes": 1000,
                    "seen_tcp_rtt": 0,
                    "seen_retrans": 0,
                    "seen_ploss": 0,
                    "seen_delivery_rate": 0,
                }
            ]
        return []

    svc = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=SimpleNamespace(execute=_exec), watermark=lambda: None)
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda _sid: svc),
    )
    st, et = _naive_iso_window(1)
    resp = client.post(
        "/api/network/bundle", headers=_SVC_HEADERS, json={"start_time": st, "end_time": et, "sections": ["transport"]}
    )
    assert resp.status_code == 200
    transport = resp.json()["transport"]
    assert (transport["total_requests"], transport["has_transport_data"]) == (5, False)
    assert len(transport["missing_fields"]) == 4 and "tcp_rtt" in transport["missing_fields"]


# ── Origin Router Tests ──
@pytest.mark.parametrize(
    ("endpoint", "payload"),
    [
        ("/api/origin/timeseries", {"filters": {}, "metric": "totally_made_up"}),
        ("/api/origin/timeseries", {"filters": {}, "percentile": "p42"}),
        ("/api/origin/drilldown", {"filters": {}, "dimension": "invalid_dimension"}),
    ],
)
def test_origin_enum_validation_returns_422(client, endpoint, payload):
    assert client.post(endpoint, headers=_SVC_HEADERS, json=payload).status_code == 422


_TS_KW = {"bucket_minutes": 15, "split_by_leg": True, "metric": "ttlb", "percentile": "p99"}
_ORIGIN_FORWARD_CASES = [
    ("timeseries", "hs_origin_section", {"has_data": False, "series": []}, _TS_KW, _TS_KW),
    (
        "slow-urls",
        "hs_origin_section",
        {"has_data": False, "rows": []},
        {"limit": 5, "min_requests": 25},
        {"limit": 5, "min_requests": 25},
    ),
    ("slow-urls", "hs_origin_section", {"has_data": False, "rows": []}, {"limit": 100_000}, {"limit": 100}),
    (
        "shielding-analysis",
        "hs_shielding_analysis",
        {"has_data": False, "rows": [], "edge_only": False, "error": False, "requires_fields": []},
        {"limit": 500},
        {"limit": 200},
    ),
    (
        "drilldown",
        "hs_origin_drilldown",
        {"has_data": True, "dimension": "pop", "rows": []},
        {"dimension": "pop", "limit": 15},
        {"dimension": "pop", "limit": 15},
    ),
]


@pytest.mark.parametrize(("subpath", "fn_name", "stub_return", "extra", "expected_kwargs"), _ORIGIN_FORWARD_CASES)
def test_origin_kwargs_forwarding_and_limit_clamping(client, subpath, fn_name, stub_return, extra, expected_kwargs):
    with patch(f"backend.routers.origin.{fn_name}", return_value=stub_return) as mock:
        resp = client.post(f"/api/origin/{subpath}", headers=_SVC_HEADERS, json={"filters": {}, **extra})
    assert resp.status_code == 200
    req_arg = mock.call_args.args[1]
    for k, v in expected_kwargs.items():
        assert getattr(req_arg, k) == v


@pytest.mark.parametrize(
    ("exc", "expected_status"), [(ValueError("bad filter"), 400), (KeyError("unknown source"), 404)]
)
def test_origin_summary_error_mapping(client, exc, expected_status):
    with patch("backend.routers.origin.hs_origin_section", side_effect=exc):
        resp = client.post("/api/origin/summary", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp.status_code == expected_status


@pytest.mark.parametrize(
    ("sections", "required_timings", "blocked_timings"),
    [
        (
            ["summary"],
            ("summary",),
            ("slow_urls", "timeseries", "status_codes", "path_breakdown", "pop_latency", "ip_health"),
        ),
        (
            ["timeseries"],
            ("timeseries", "status_codes", "path_breakdown"),
            ("summary", "slow_urls", "pop_latency", "ip_health"),
        ),
    ],
)
def test_origin_aggregates_section_coupling_and_pruning(client, sections, required_timings, blocked_timings):
    resp = client.post("/api/origin/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": sections})
    assert resp.status_code == 200
    timings = {t["section"].removeprefix("origin.") for t in resp.json().get("_section_timings", [])}
    for need in required_timings:
        assert need in timings
    for blocked in blocked_timings:
        assert blocked not in timings


def test_origin_aggregates_dispatches_registered_high_scale_service(client, monkeypatch, test_service_source):
    rows_stub = {"has_data": True, "rows": []}
    expected = {"has_data": True, "total": 12, "summary": {"total_misses": 7, "total_passes": 5}}
    expected |= {"timeseries": {"has_data": True, "series": []}, "slow_urls": rows_stub, "status_codes": rows_stub}
    expected |= {"path_breakdown": {**rows_stub, "shielding_detected": False}, "ip_health": rows_stub}
    expected["pop_latency"] = {**rows_stub, "requires_group_c": False}
    high_scale_aggregates = MagicMock(return_value=expected)
    monkeypatch.setattr("backend.routers.origin.high_scale_aggregates", high_scale_aggregates)
    _register_high_scale(monkeypatch, test_service_source)

    response = client.post(
        "/api/origin/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["summary"]}
    )
    assert response.status_code == 200 and response.json()["total"] == 12
    assert high_scale_aggregates.call_args.kwargs["sections"] == {"summary"}


def test_origin_drilldown_default_and_empty_table(client):
    empty_resp = client.post("/api/origin/drilldown", headers=_SVC_HEADERS, json={"filters": {}, "dimension": "oip"})
    assert empty_resp.status_code == 200
    ej = empty_resp.json()
    assert (ej["dimension"], ej["has_data"], ej["rows"]) == ("oip", False, [])

    seeded_resp = client.post("/api/origin/drilldown", headers=_SVC_HEADERS, json={"filters": {}})
    assert seeded_resp.status_code == 200 and seeded_resp.json()["dimension"] == "host"


# ── Performance Router Tests ──
def _stub_perf_service(monkeypatch):
    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        queries = telemetry_mod.get_queries()
        queries.append({"sql": str(sql), "time_ms": 0.1, "engine": "ClickHouse", "is_cached": False})
        telemetry_mod._QUERIES.set(queries)
        s = str(sql)
        if "perf:top_urls" in s:
            return [
                {
                    "value": "/test-perf-url",
                    "req_count": 20,
                    "avg_ms": 120.0,
                    "p50_ms": 100.0,
                    "p95_ms": 140.0,
                    "p99_ms": 150.0,
                }
            ]
        if "perf:top_asns" in s:
            return [
                {
                    "value": "12345",
                    "req_count": 20,
                    "avg_ms": 120.0,
                    "p50_ms": 100.0,
                    "p95_ms": 140.0,
                    "p99_ms": 150.0,
                }
            ]
        if "perf:waterfall" in s:
            return [
                {
                    "n_components": 20,
                    "edge_processing": 10.0,
                    "origin_wait": 30.0,
                    "origin_download": 20.0,
                    "client_download": 60.0,
                }
            ]
        if "perf:scatter" in s:
            return [{"origin_ms": 40.0, "edge_ms": 80.0, "cache": "HIT"}]
        if "perf:ttl_dist" in s:
            return [{"bucket": "1h - 6h", "count": 20, "min_ttl": 3600}]
        return []

    svc = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=SimpleNamespace(execute=_exec), watermark=lambda: None)
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda _sid: svc),
    )


def test_performance_aggregates_section_pruning_and_auto_expand(client, monkeypatch):
    _stub_perf_service(monkeypatch)

    resp_top = client.post(
        "/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["top_urls"]}
    )
    assert resp_top.status_code == 200
    data_top = resp_top.json()
    assert len(data_top["top_urls"]) > 0 and len(data_top["top_asns"]) > 0
    assert (data_top["ttl_dist"], data_top["scatter"], data_top["waterfall"]) == ([], [], {})
    top_timings = {t["section"].removeprefix("performance.") for t in data_top.get("_section_timings", [])}
    assert "top_urls" in top_timings and "top_asns" in top_timings
    assert "ttl_dist" not in top_timings and "waterfall" not in top_timings

    resp_wf = client.post(
        "/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["waterfall"]}
    )
    assert resp_wf.status_code == 200
    data_wf = resp_wf.json()
    assert (data_wf["top_urls"], data_wf["top_asns"], data_wf["ttl_dist"]) == ([], [], [])
    assert "avg" in data_wf["waterfall"] and len(data_wf["scatter"]) > 0

    resp_ttl = client.post(
        "/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["ttl_dist"]}
    )
    assert resp_ttl.status_code == 200
    data_ttl = resp_ttl.json()
    assert (data_ttl["top_urls"], data_ttl["top_asns"], data_ttl["scatter"], data_ttl["waterfall"]) == ([], [], [], {})
    ttl_timings = {t["section"].removeprefix("performance.") for t in data_ttl.get("_section_timings", [])}
    assert "ttl_dist" in ttl_timings
    for blocked in ("top_urls", "top_asns", "waterfall"):
        assert blocked not in ttl_timings


@pytest.mark.parametrize("sort_by", ["avg", "p50", "p95", "p99"])
def test_performance_aggregates_sort_by_variants(client, monkeypatch, sort_by):
    _stub_perf_service(monkeypatch)
    resp = client.post("/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sort_by": sort_by})
    assert resp.status_code == 200 and len(resp.json()["top_urls"]) > 0


def test_performance_aggregates_high_scale_dispatch_and_expanded_sections(client, monkeypatch, test_service_source):
    _register_high_scale(monkeypatch, test_service_source)
    row = {"url": "/hs", "requests": 50, "avg": 10.0, "p50": 8.0, "p95": 15.0, "p99": 20.0}
    fake_response = {"top_urls": [row], "top_asns": [], "ttl_dist": [], "scatter": [], "waterfall": {}, "_approx": True}
    req_json = {"filters": {}, "sort_by": "avg", "sections": ["scatter", "top_urls"]}
    with patch("backend.routers.performance.hs_performance", return_value=fake_response) as mock_hs_perf:
        resp = client.post("/api/performance/aggregates", headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200 and resp.json()["_approx"] is True
    assert mock_hs_perf.call_args.kwargs["sections"] == {"scatter", "waterfall", "top_urls", "top_asns"}
    assert mock_hs_perf.call_args.args[1].sort_by == "avg"


# ── Security Router Tests ──
def test_security_aggregates_and_bundle_contracts(client):
    resp = client.post("/api/security/aggregates", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp.status_code == 200
    data = resp.json()
    for k in ("tls_fingerprints", "req_size_dist", "ipv6_adoption", "proxy_dist", "conn_reuse_dist", "_debug_queries"):
        assert k in data

    b_post = client.post("/api/security/bundle", headers=_SVC_HEADERS, json={"filters": {}, "bucket_seconds": 300})
    assert b_post.status_code == 200 and "tls_fingerprints" in b_post.json()
    with _patch_range_token():
        b_get = client.get(
            "/api/security/bundle", headers=_SVC_HEADERS, params={"range_token": "24h", "bucket_seconds": 300}
        )
    assert b_get.status_code == 200 and "tls_fingerprints" in b_get.json()

    sel_resp = client.post(
        "/api/security/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["conn_reuse_dist"]}
    )
    assert sel_resp.status_code == 200
    timings_names = {t["section"].removeprefix("security.") for t in sel_resp.json().get("_section_timings", [])}
    assert "conn_reuse_dist" in timings_names
    for blocked in {"ipv6_adoption", "proxy_dist", "verified_bots_ts", "wellknown_bots_query"}:
        assert blocked not in timings_names

    with patch("backend.routers.security.hs_aggregates", return_value=SecurityAggregatesResponse()) as mock_repo:
        client.post(
            "/api/security/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["tls_fingerprints"]}
        )
    sections_called = mock_repo.call_args.kwargs.get("sections")
    assert "tls_fingerprints" in sections_called and "fingerprint_coverage" in sections_called


def test_security_analyst_masking_and_window_clamping(client, monkeypatch, test_service_source):
    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        if "hs_security:top_ips_header" in str(sql):
            return [{"ip": "198.51.100.42", "max_header": 2048}]
        return []

    svc = SimpleNamespace(service_id=MOCK_SERVICE_ID, client=SimpleNamespace(execute=_exec), watermark=lambda: None)
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda _sid: svc),
    )
    earlier, later = datetime.now(UTC) - timedelta(hours=2), datetime.now(UTC)
    req_json = {
        "filters": {},
        "sections": ["top_ips_header"],
        "start_time": (datetime.now(UTC) - timedelta(days=7)).isoformat(),
        "end_time": later.isoformat(),
    }
    with _with_analyst_ctx(test_service_source, mask_ips=True, time_bounds=TimeBounds(start=earlier, end=later)):
        resp = client.post("/api/security/aggregates", headers=_SVC_HEADERS, json=req_json)
        assert resp.status_code == 200
        entries = resp.json().get("top_ips_header", [])
        assert len(entries) == 1
        for entry in entries:
            ip = entry.get("ip") or entry.get("client_ip")
            if ip:
                assert "xxx" in ip or "*" in ip or ip == "0.0.0.0" or "[redacted]" in ip


@pytest.mark.parametrize(
    ("fmt", "mask_ips", "expected_ct"),
    [("fastly-acl", False, "text/csv"), ("plain", False, "text/plain"), ("plain", True, "text/plain")],
)
def test_export_proxies_formats_and_masking(client, test_service_source, fmt, mask_ips, expected_ct):
    now = datetime.now(UTC)
    ctx_mgr = _with_analyst_ctx(test_service_source, mask_ips=True) if mask_ips else contextmanager(lambda: (yield))()
    params = {
        "start_time": (now - timedelta(hours=2)).isoformat(),
        "end_time": now.isoformat(),
        "threshold": "High",
        "format": fmt,
    }
    fake_clients = [
        {
            "ip": "198.51.100.xxx" if mask_ips else "198.51.100.1",
            "risk_level": "High",
            "impossible_distance": True,
        }
    ]
    with ctx_mgr, patch("backend.routers.security.hs_proxies", return_value={"active_clients": fake_clients}):
        resp = client.get("/api/security/proxies/export", headers=_SVC_HEADERS, params=params)
    assert resp.status_code == 200
    assert expected_ct in resp.headers.get("content-type", "")
    assert "attachment; filename=" in resp.headers.get("content-disposition", "")
    if fmt == "fastly-acl":
        assert "ip,comment" in resp.text
    if mask_ips:
        for line in (ln.strip() for ln in resp.text.splitlines() if ln.strip()):
            assert "xxx" in line or "*" in line or line == "0.0.0.0" or "[redacted]" in line


def test_security_threat_intel_default(client):
    resp = client.get("/api/security/threat-intel", headers=_SVC_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data


def test_security_high_scale_aggregates_threat_intel_and_proxies_export(client, monkeypatch, test_service_source):
    service = _register_high_scale(monkeypatch, test_service_source)
    hs_aggregates = MagicMock(return_value=SecurityAggregatesResponse())
    hs_threat_intel = MagicMock(return_value={"data": []})
    c1 = {"ip": "198.51.100.xxx", "risk_level": "High", "impossible_distance": True}
    c2 = {"ip": "198.51.101.xxx", "risk_level": "Medium"}
    hs_proxies = MagicMock(return_value={"active_clients": [c1, c2]})
    monkeypatch.setattr("backend.routers.security.hs_aggregates", hs_aggregates)
    monkeypatch.setattr("backend.routers.security.hs_threat_intel", hs_threat_intel)
    monkeypatch.setattr("backend.routers.security.hs_proxies", hs_proxies)

    ceiling_start, ceiling_end = datetime.now(UTC) - timedelta(hours=2), datetime.now(UTC)
    tb = TimeBounds(start=ceiling_start, end=ceiling_end)
    with _with_analyst_ctx(test_service_source, mask_ips=True, time_bounds=tb):
        resp_agg = client.post(
            "/api/security/aggregates",
            headers=_SVC_HEADERS,
            json={"filters": {}, "sections": ["tls_fingerprints", "ngwaf_verified_bots"]},
        )
        assert resp_agg.status_code == 200, resp_agg.text
        assert hs_aggregates.call_args.args[0] is service
        exp_secs = {"tls_fingerprints", "fingerprint_coverage", "ngwaf_verified_bots", "ngwaf_verified_bots_ts"}
        assert hs_aggregates.call_args.kwargs["sections"] == exp_secs
        assert hs_aggregates.call_args.kwargs["mask_ips"] is True

        ti_params = {
            "start_time": (ceiling_start - timedelta(days=30)).isoformat(),
            "end_time": ceiling_end.isoformat(),
        }
        resp_ti = client.get("/api/security/threat-intel", headers=_SVC_HEADERS, params=ti_params)
        assert resp_ti.status_code == 200, resp_ti.text
        queried_start = datetime.fromisoformat(hs_threat_intel.call_args.args[1].replace("Z", "+00:00"))
        assert queried_start >= ceiling_start - timedelta(seconds=1)

        exp_params = {
            "start_time": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
            "end_time": datetime.now(UTC).isoformat(),
            "threshold": "High",
            "format": "plain",
        }
        resp_exp = client.get("/api/security/proxies/export", headers=_SVC_HEADERS, params=exp_params)
        assert resp_exp.status_code == 200 and resp_exp.text.split() == ["198.51.100.xxx"]
        assert hs_proxies.call_args.kwargs["mask_ips"] is True


# ── Value Router Tests ──
@pytest.mark.parametrize(
    "sections",
    [
        None,
        ["overview", "caching", "network"],
        *([s] for s in ("overview", "caching", "security", "bots", "performance", "network", "io")),
    ],
)
def test_value_summary_accepts_section_selectors(client, test_service_source, sections):
    resp = client.post("/api/value/summary", headers=_SVC_HEADERS, json={"filters": {}, "sections": sections})
    assert resp.status_code == 200, resp.text


def test_value_summary_empty_and_missing_network_columns(client, test_service_source):
    resp_empty = client.post("/api/value/summary", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp_empty.status_code == 200, resp_empty.text

    now = datetime.now(UTC)
    req_json = {
        "filters": {},
        "sections": ["network"],
        "start_time": iso_z(now - timedelta(hours=1)),
        "end_time": iso_z(now + timedelta(hours=1)),
    }
    resp = client.post("/api/value/summary", headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200, resp.text


def test_value_summary_tls_and_verified_bots_fallbacks(client, test_service_source, monkeypatch):
    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        s = str(sql)
        if "value:combined_buckets" in s:
            return [
                {
                    "bucket": "2026-07-23T12:00:00Z",
                    "total": 2,
                    "hit_cnt": 0,
                    "miss_cnt": 2,
                    "pass_cnt": 0,
                    "synth_cnt": 0,
                    "bandwidth_saved": 0,
                    "total_bytes": 300,
                    "origin_cnt": 2,
                    "shield_hit": 0,
                    "shield_total": 0,
                    "threats_cnt": 0,
                    "sum_hit_elapsed": 0.0,
                    "cnt_hit_elapsed": 0,
                    "sum_miss_elapsed": 0.0,
                    "cnt_miss_elapsed": 0,
                    "waf_inspected": 0,
                    "waf_blocked": 0,
                    "waf_logged": 0,
                    "waf_passed": 0,
                    "waf_sig_cnt": 1,
                    "http3_cnt": 0,
                    "h2_cnt": 2,
                    "h11_cnt": 0,
                    "other_proto_cnt": 0,
                    "tls_cnt": 1,
                    "ipv6_cnt": 0,
                }
            ]
        if "value:bots" in s:
            return [{"bot_requests": 1, "verified_bots": 1}]
        if "value:top_bots" in s:
            return [{"name": "Googlebot", "cnt": 1}]
        return []

    empty_wm = SimpleNamespace(coverage_start=None, coverage_end=None, last_visible_event_id=None)
    svc = SimpleNamespace(
        service_id=test_service_source["service_id"],
        client=SimpleNamespace(execute=_exec),
        watermark=lambda: empty_wm,
        watermark_for=lambda domain="request": empty_wm,
    )
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda sid: svc),
    )
    req_json = {
        "filters": {},
        "sections": ["network", "bots"],
        "start_time": "2026-07-23T11:00:00Z",
        "end_time": "2026-07-23T13:00:00Z",
    }
    resp = client.post("/api/value/summary", headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert (data["network"]["tls_pct"], data["bots"]["verified_bots"]) == (50.0, 1)


def test_value_summary_populated_buckets(client, test_service_source, monkeypatch):
    now = datetime.now(UTC).replace(minute=10, second=0, microsecond=0)
    prev_hour = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        if "value:combined_buckets" in str(sql):
            return [
                {
                    "bucket": iso_z(prev_hour),
                    "total": 12,
                    "hit_cnt": 9,
                    "miss_cnt": 3,
                    "pass_cnt": 0,
                    "synth_cnt": 0,
                    "bandwidth_saved": 9000,
                    "total_bytes": 13000,
                    "origin_cnt": 3,
                    "shield_hit": 5,
                    "shield_total": 6,
                    "threats_cnt": 2,
                    "sum_hit_elapsed": 9200.0,
                    "cnt_hit_elapsed": 9,
                    "sum_miss_elapsed": 12800.0,
                    "cnt_miss_elapsed": 3,
                    "waf_inspected": 2,
                    "waf_blocked": 2,
                    "waf_logged": 0,
                    "waf_passed": 0,
                    "waf_sig_cnt": 0,
                    "http3_cnt": 7,
                    "h2_cnt": 4,
                    "h11_cnt": 1,
                    "other_proto_cnt": 0,
                    "tls_cnt": 12,
                    "ipv6_cnt": 6,
                }
            ]
        return []

    empty_wm = SimpleNamespace(coverage_start=None, coverage_end=None, last_visible_event_id=None)
    svc = SimpleNamespace(
        service_id=test_service_source["service_id"],
        client=SimpleNamespace(execute=_exec),
        watermark=lambda: empty_wm,
        watermark_for=lambda domain="request": empty_wm,
    )
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda sid: svc),
    )
    req_json = {
        "filters": {},
        "sections": ["overview", "caching", "network"],
        "start_time": iso_z(prev_hour),
        "end_time": iso_z(now + timedelta(minutes=5)),
        "chart_interval": "1 hour",
    }
    resp = client.post("/api/value/summary", headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    vals = (body["overview"]["total_requests"], body["caching"]["hit_requests"], body["network"]["total_requests"])
    assert vals == (12, 9, 12) and len(body["network"]["protocol_time_series"]) > 0


# ── Page & Endpoint Smoke Tests ──
_SMOKE_ENDPOINTS = [
    ("POST", "/api/origin/status-codes", {"filters": {}}, ("has_data",)),
    ("POST", "/api/origin/path-breakdown", {"filters": {}}, ("has_data",)),
    ("POST", "/api/origin/pop-latency", {"filters": {}, "limit": 10}, ("has_data",)),
    ("POST", "/api/origin/ip-health", {"filters": {}, "limit": 10}, ("has_data",)),
    ("POST", "/api/origin/shielding-analysis", {"filters": {}, "limit": 150}, ("has_data",)),
    ("POST", "/api/origin/timeseries", {"filters": {}}, ("has_data",)),
    ("POST", "/api/origin/summary", {"filters": {}}, ("_debug_queries",)),
    ("POST", "/api/network-quality", {"filters": {}}, ("available",)),
    ("POST", "/api/dashboard/aggregates", {"filters": {}}, ("data",)),
    ("POST", "/api/performance/aggregates", {"filters": {}, "sort_by": "p99"}, ("top_urls", "top_asns")),
    ("POST", "/api/sessions", {"filters": {}}, ("sessions", "total")),
    ("GET", f"/api/alerts/{MOCK_SERVICE_ID}", None, ("data",)),
]


@pytest.mark.parametrize(("method", "path", "payload", "expected_keys"), _SMOKE_ENDPOINTS)
def test_analytics_page_smoke_endpoints(client, test_service_source, method, path, payload, expected_keys):
    resp = (
        client.post(path, headers=_SVC_HEADERS, json=payload)
        if method == "POST"
        else client.get(path, headers=_SVC_HEADERS)
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    for key in expected_keys:
        assert key in body


def test_query_and_views_endpoints(client, test_service_source):
    q_resp = client.post("/api/query", headers=_SVC_HEADERS, json={"sql": "SELECT count(*) FROM logs"})
    assert q_resp.status_code == 200, q_resp.text
    assert "columns" in q_resp.json() and len(q_resp.json()["data"]) == 1

    v_resp = client.get(f"/api/views/{MOCK_SERVICE_ID}")
    assert v_resp.status_code == 200 and isinstance(v_resp.json(), list)


def test_log_activity_endpoint_empty_and_seeded(client, test_service_source):
    empty_resp = client.get(
        "/api/usage/log-activity", headers=_SVC_HEADERS, params={"start": "2026-01-01", "end": "2026-01-02"}
    )
    assert empty_resp.status_code == 200 and {"data", "total_rows", "total_bytes"} <= set(empty_resp.json().keys())

    src_name = test_service_source["name"]
    con = metadata_db.get_con(src_name)
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at) VALUES (?, ?, ?, ?, ?)",
        [
            ("log1.gz", src_name, 100, 5000, "2026-01-01T10:00:00Z"),
            ("log2.gz", src_name, 200, 8000, "2026-01-01T11:00:00Z"),
        ],
    )
    con.commit()

    resp = client.get(
        "/api/usage/log-activity",
        headers=_SVC_HEADERS,
        params={"start": "2026-01-01", "end": "2026-01-02", "by": "hour"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert (data["granularity"], data["total_rows"], data["total_bytes"]) == ("hour", 300, 13000)
    assert {"row_count", "bytes", "time"} <= set(data["data"][0].keys())
    assert "generated" not in data and "processed" not in data


# ── Service-ID Guard & Wrong HTTP Method Contract Tests ──
EXPLICIT_SERVICE_ID_ROUTES = [
    ("GET", "/api/admin/slow-queries", None),
    ("GET", "/api/admin/slow-queries/count", None),
    ("DELETE", "/api/admin/slow-queries", None),
    ("GET", "/api/admin/clickhouse/status", None),
    ("GET", "/api/admin/ledger/quarantine", None),
    ("GET", "/api/provision/check-config", {"token": "t", "cdn_service_id": "c", "bucket": "b"}),
    ("GET", "/api/provision/ngwaf-workspaces", {"token": "t"}),
    ("GET", "/api/log-extents/stream", None),
    ("DELETE", "/api/alerts/alert-1", None),
    ("DELETE", "/api/views/view-1", None),
]

ANALYTICS_ROUTES = [
    ("POST", "/api/dashboard/aggregates"),
    ("POST", "/api/origin/aggregates"),
    ("POST", "/api/security/aggregates"),
    ("POST", "/api/performance/aggregates"),
    ("POST", "/api/network-health"),
]


@pytest.fixture
def bare_client(monkeypatch):
    if "backend.core.fastly.ngwaf" not in sys.modules:
        from backend.models.common import NgwafWorkspacesResponse

        monkeypatch.setitem(
            sys.modules, "backend.core.fastly.ngwaf", SimpleNamespace(NgwafWorkspacesResponse=NgwafWorkspacesResponse)
        )
    saved = dict(app.dependency_overrides)
    app.dependency_overrides.clear()
    try:
        with TestClient(app) as c:
            yield c
    finally:
        app.dependency_overrides.clear()
        app.dependency_overrides.update(saved)


@pytest.mark.parametrize(("method", "path", "params"), EXPLICIT_SERVICE_ID_ROUTES)
def test_missing_service_id_is_400_service_id_required(bare_client, method, path, params) -> None:
    resp = bare_client.request(method, path, params=params)
    assert resp.status_code == 400 and resp.json()["detail"] == {"error": "service_id_required"}


@pytest.mark.parametrize(("method", "path"), ANALYTICS_ROUTES)
def test_analytics_route_without_service_is_400_envelope(bare_client, method, path) -> None:
    resp = bare_client.request(method, path, json={})
    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert detail["error"] == "no_service" and detail["no_service"] is True


_PATH_SUBS = {"{service_id}": "test-service-id", "{run_id}": "1", "{log_id}": "1", "{source_id}": "wellknown"}
_PATH_SUBS |= {"{alert_id}": "test-alert-id", "{view_id}": "test-view-id"}
_METHOD_PRIORITY = ["GET", "POST", "PUT", "PATCH", "DELETE"]


def _sub_path(path: str) -> str:
    for placeholder, value in _PATH_SUBS.items():
        path = path.replace(placeholder, value)
    return path


def _pick_wrong_method(registered: set[str]) -> str | None:
    return next((m for m in _METHOD_PRIORITY if m not in registered), None)


def _normalize_template(path: str) -> str:
    return re.sub(r"\{[^}]+\}", "{p}", path)


def _norm_to_regex(norm: str) -> re.Pattern[str]:
    parts = norm.split("{p}")
    return re.compile(
        "".join(re.escape(p) + "([^/]+)" if i < len(parts) - 1 else re.escape(p) for i, p in enumerate(parts))
    )


def _shadowed_by_another_route(path: str, wrong_method: str, own_norm: str) -> bool:
    for norm, methods in _methods_by_norm.items():
        if norm != own_norm and wrong_method in methods and _norm_to_regex(norm).fullmatch(path):
            return True
    return False


_test_app = FastAPI()
for _r in app.routes:
    if not isinstance(_r, Mount):
        _test_app.routes.append(_r)

_methods_by_norm: dict[str, set[str]] = {}
_path_by_norm: dict[str, str] = {}
for _route in app.routes:
    if isinstance(_route, APIRoute):
        _norm = _normalize_template(_route.path)
        _methods_by_norm.setdefault(_norm, set()).update(_route.methods or set())
        _path_by_norm.setdefault(_norm, _sub_path(_route.path))

_cases: list[tuple[str, str, str]] = [
    (_wrong, _path_by_norm[_norm], str(sorted(_all_methods)))
    for _norm, _all_methods in sorted(_methods_by_norm.items())
    if (_wrong := _pick_wrong_method(_all_methods))
    and not _shadowed_by_another_route(_path_by_norm[_norm], _wrong, _norm)
]


@pytest.mark.parametrize("wrong_method,path,registered", _cases, ids=[f"{wrong} {path}" for wrong, path, _ in _cases])
def test_wrong_http_method_returns_405_not_404(wrong_method: str, path: str, registered: str) -> None:
    client = TestClient(_test_app, raise_server_exceptions=False)
    kwargs = {"json": {}} if wrong_method in ("POST", "PUT", "PATCH") else {}
    resp = getattr(client, wrong_method.lower())(path, **kwargs)
    assert resp.status_code == 405, f"{wrong_method} {path} returned {resp.status_code} (registered: {registered})"


# ── Response Cache Observation & Dashboard Custom Field Tests ──
@pytest.fixture
def observation_request(test_service_source, tmp_path, monkeypatch) -> dict:
    from backend.utils import bot_sources

    source_dir = tmp_path / "bot-sources"
    source_dir.mkdir()
    bot_entry = {"id": "googlebot", "name": "Googlebot", "pattern": {"accepted": ["Googlebot"]}}
    bot_entry["verification"] = {"domains": [], "cidrs": []}
    (source_dir / "well-known-bots.json").write_text(json.dumps({"entries": [bot_entry]}))
    monkeypatch.setattr(bot_sources, "_CACHE_DIR", source_dir)

    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        queries = telemetry_mod.get_queries()
        queries.append({"sql": str(sql), "time_ms": 0.1, "engine": "ClickHouse", "is_cached": False})
        telemetry_mod._QUERIES.set(queries)
        s = str(sql)
        if "dimension='ua'" in s or "custom_fields['ua'] AS ua" in s:
            return [{"ua": "Googlebot/2.1", "cnt": 40}]
        return []

    sid = test_service_source["service_id"]
    svc = SimpleNamespace(
        service_id=sid,
        client=SimpleNamespace(execute=_exec),
        watermark=lambda: SimpleNamespace(
            service_id=sid,
            domain="request",
            coverage_start=None,
            coverage_end=None,
            last_visible_event_id=None,
            validate=lambda: None,
        ),
        watermark_for=lambda domain="request": SimpleNamespace(
            service_id=sid,
            domain=domain,
            coverage_start=None,
            coverage_end=None,
            last_visible_event_id=None,
            validate=lambda: None,
        ),
    )
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda sid: svc),
    )
    now = datetime.now(UTC)
    return {"start_time": (now - timedelta(hours=2)).isoformat(), "end_time": now.isoformat(), "filters": {}}


@pytest.mark.parametrize(
    "path,sections",
    [
        ("/api/security/aggregates", ["ipv6_adoption"]),
        ("/api/performance/aggregates", ["ttl_dist"]),
        ("/api/value/summary", ["overview"]),
        ("/api/dashboard/aggregates", ["core"]),
    ],
)
def test_standard_owner_miss_and_router_peek_hit_survive_wire_serialization(
    client, observation_request: dict, monkeypatch: pytest.MonkeyPatch, path: str, sections: list[str]
) -> None:
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    body = {**observation_request, "sections": sections}
    first = client.post(path, json=body, headers=_SVC_HEADERS)
    assert first.status_code == 200 and first.json()["_response_cache"]["state"] == "miss"
    second = client.post(path, json=body, headers=_SVC_HEADERS)
    assert second.status_code == 200
    s_json = second.json()
    rc = s_json["_response_cache"]
    assert (rc["state"], rc["prewarmer"], s_json["_is_cached"]) == ("hit", False, True)
    assert "_debug_queries" not in s_json


def test_bundle_and_composite_response_cache_observations(client, observation_request: dict) -> None:
    first = client.post("/api/dashboard/bundle", json=observation_request, headers=_SVC_HEADERS).json()
    assert first["_response_cache"]["state"] == "bypass"
    assert first["aggregates"]["_response_cache"]["state"] == "miss"
    assert first["top_bots"]["_response_cache"]["state"] == "miss"
    assert (first["top_bots"]["bots"][0]["name"], first["top_bots"]["bots"][0]["request_count"]) == ("Googlebot", 40)
    second = client.post("/api/dashboard/bundle", json=observation_request, headers=_SVC_HEADERS).json()
    assert (second["aggregates"]["_response_cache"]["state"], second["top_bots"]["_response_cache"]["state"]) == (
        "hit",
        "hit",
    )

    for expected in ("miss", "hit"):
        body = client.post("/api/network/bundle", json=observation_request, headers=_SVC_HEADERS).json()
        assert body["_response_cache"]["state"] == "bypass"
        assert body["health"]["_response_cache"]["state"] == expected
        assert body["transport"]["_response_cache"]["state"] == "bypass"


def test_dashboard_custom_fields_appear_in_top10(client, test_service_source, monkeypatch):
    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        s = str(sql)
        if "AS v, count() AS c" in s:
            return [{"v": "42", "c": 5}]
        return []

    sid = test_service_source["service_id"]
    svc = SimpleNamespace(
        service_id=sid,
        client=SimpleNamespace(execute=_exec),
        watermark=lambda: SimpleNamespace(
            service_id=sid,
            domain="request",
            coverage_start=None,
            coverage_end=None,
            last_visible_event_id=None,
            validate=lambda: None,
        ),
        watermark_for=lambda domain="request": SimpleNamespace(
            service_id=sid,
            domain=domain,
            coverage_start=None,
            coverage_end=None,
            last_visible_event_id=None,
            validate=lambda: None,
        ),
    )
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda sid: svc),
    )
    response = client.post(
        "/api/dashboard/aggregates",
        headers=_SVC_HEADERS,
        json={"filters": {}, "fields": ["url", "cache"]},
    )
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert "url" in data and "cache" in data and "host" not in data

    fv_resp = client.post(
        "/api/dashboard/field-values",
        headers=_SVC_HEADERS,
        json={"field": "my_edge_field"},
    )
    assert fv_resp.status_code == 200, fv_resp.text
    assert fv_resp.json()["values"] == [{"value": "42", "count": 5, "label": None}]


# ── Assets Router Tests ──
def test_assets_aggregates_post_get_and_populated_logs(client, test_service_source, monkeypatch):
    monkeypatch.setattr(svcconfig, "get_status", lambda sid: {"earliest_log_at": "2026-08-19T00:00:00Z"})
    w_json = {"filters": {}, "start_time": "2026-08-19T12:00:00Z", "end_time": "2026-08-19T13:00:00Z"}
    w_qs = "start_time=2026-08-19T12:00:00Z&end_time=2026-08-19T13:00:00Z"
    exp_keys = {"asset_type_breakdown", "cache_performance", "compression_performance", "missing_fields"}
    for mode_resp in (
        client.post("/api/assets/aggregates", headers=_SVC_HEADERS, json=w_json),
        client.get("/api/assets/aggregates?range_token=24h&anchor=2026-08-19T12:00:00Z", headers=_SVC_HEADERS),
        client.get(f"/api/assets/aggregates?{w_qs}", headers=_SVC_HEADERS),
    ):
        assert mode_resp.status_code == 200 and exp_keys <= set(mode_resp.json().keys())

    d1 = {
        "asset_type": "Images",
        "cache_status": "HIT",
        "content_encoding_norm": "gzip",
        "is_compressed": 1,
        "is_compressible": 0,
        "requests": 10,
        "bytes": 450000,
    }
    d2 = {
        "asset_type": "JavaScript/CSS",
        "cache_status": "MISS",
        "content_encoding_norm": "br",
        "is_compressed": 1,
        "is_compressible": 1,
        "requests": 10,
        "bytes": 1200000,
    }

    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        queries = telemetry_mod.get_queries()
        queries.append({"sql": str(sql), "time_ms": 0.1, "engine": "ClickHouse", "is_cached": False})
        telemetry_mod._QUERIES.set(queries)
        if "assets:summary_dims" in str(sql):
            return [d1, d2]
        return []

    empty_wm = SimpleNamespace(coverage_start=None, coverage_end=None, last_visible_event_id=None)
    svc = SimpleNamespace(
        service_id=test_service_source["service_id"],
        client=SimpleNamespace(execute=_exec),
        watermark=lambda: empty_wm,
        watermark_for=lambda domain="request": empty_wm,
    )
    monkeypatch.setattr(
        "backend.high_scale.ingest_controller.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda sid: svc),
    )
    CacheRegistry.clear_all()

    pop_resp = client.post("/api/assets/aggregates", headers=_SVC_HEADERS, json=w_json)
    assert pop_resp.status_code == 200
    assert len(pop_resp.json()["asset_type_breakdown"]) > 0 and pop_resp.json()["missing_fields"] == []
    hit_resp = client.post("/api/assets/aggregates", headers=_SVC_HEADERS, json=w_json)
    assert hit_resp.status_code == 200 and hit_resp.json()["_is_cached"] is True


def test_assets_aggregates_high_scale():
    from backend.high_scale.ingest_controller import assets_aggregates

    calls: list[tuple[str, dict]] = []
    d1 = {"asset_type": "JavaScript/CSS", "cache_status": "HIT", "content_encoding_norm": "br"}
    d1 |= {"is_compressed": 1, "is_compressible": 1, "requests": 80, "bytes": 80000}
    d2 = {"asset_type": "JavaScript/CSS", "cache_status": "MISS", "content_encoding_norm": "uncompressed"}
    d2 |= {"is_compressed": 0, "is_compressible": 1, "requests": 20, "bytes": 2000000}
    h1 = {"kind": "large", "url": "/static/app.js", "requests": 20, "metric_float": 100000.0}
    h1 |= {"total_bytes": 2000000, "status": 200, "asset_type": ""}
    h2 = {"kind": "low_ttl", "url": "/static/app.js", "requests": 20, "metric_float": 0.0}
    h2 |= {"total_bytes": 0, "status": 0, "asset_type": "JavaScript/CSS"}

    class _FakeCHClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            calls.append((sql, params or {}))
            if "assets:summary_dims" in sql:
                return [d1, d2]
            return [h1, h2] if "assets:hotspots" in sql else []

    svc = SimpleNamespace(service_id="hs-svc-1", client=_FakeCHClient())
    req = AssetsRequest(filters={}, start_time="2026-08-19T12:00:00Z", end_time="2026-08-19T13:00:00Z")
    res1 = assets_aggregates(svc, req, "2026-08-19T12:00:00Z", "2026-08-19T13:00:00Z")  # type: ignore[arg-type]
    assert len(res1.asset_type_breakdown) == 1 and res1.asset_type_breakdown[0].asset_type == "JavaScript/CSS"
    assert (res1.asset_type_breakdown[0].cache_hit_ratio, res1.asset_type_breakdown[0].compression_rate) == (0.8, 0.8)
    assert len(res1.large_uncompressed_assets) == 1 and len(res1.low_ttl_assets) == 1 and len(calls) == 2

    res2 = assets_aggregates(svc, req, "2026-08-19T12:00:00+00:00", "2026-08-19T13:00:00.000000Z")  # type: ignore[arg-type]
    assert res2.is_cached is True and len(calls) == 2

    res3 = assets_aggregates(svc, req, "2026-08-19T12:00:30Z", "2026-08-19T13:00:45Z")  # type: ignore[arg-type]
    assert res3.is_cached is False and len(calls) == 4


# ── Merged from tests/routers/test_alerts_and_views.py ──
_SERVICE_ID = "test-service-id"
_ALERT_HDR = {"x-fastly-service-id": _SERVICE_ID}
_ALERT_BODY = {
    "service_id": _SERVICE_ID,
    "name": "High 5xx Rate",
    "category": "reliability",
    "metric": "5xx_rate",
    "evaluation_type": "absolute",
    "evaluation_scope": "all",
    "operator": ">",
    "threshold": 5.0,
    "window_min": 5.0,
}
_VIEW_BODY = {"service_id": _SERVICE_ID, "name": "My View", "filters_json": '{"status": [500]}', "page": "dashboard"}
_PREVIEW_DDL = (
    "CREATE TABLE {name} (timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
    "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
)


def _alert_row(alert_id: str) -> dict | None:
    con = metadata_db.get_con(_SERVICE_ID)
    row = con.execute("SELECT name, metric, threshold, enabled FROM alerts WHERE id = ?", (alert_id,)).fetchone()
    return dict(row) if row else None


def _view_row(view_id: str) -> dict | None:
    con = metadata_db.get_con(_SERVICE_ID)
    row = con.execute("SELECT name, filters_json, page FROM views WHERE id = ?", (view_id,)).fetchone()
    return dict(row) if row else None


def _seed_alert(client, **overrides) -> str:
    resp = client.post("/api/alerts/", json={**_ALERT_BODY, **overrides}, headers=_ALERT_HDR)
    assert resp.status_code == 201
    return resp.json()["data"]["id"]


def _seed_view(client, **overrides) -> str:
    resp = client.post("/api/views/", json={**_VIEW_BODY, **overrides}, headers=_ALERT_HDR)
    assert resp.status_code == 201
    return resp.json()["id"]


@contextmanager
def _patch_preview_source(src: dict | None = {"name": "test_service", "service_id": _SERVICE_ID}):
    with patch("backend.routers.alerts.get_source_for_service", return_value=src):
        yield


def _make_alert_ch_client(
    *,
    val: float = 5.0,
    cnt: int = 10,
    queries_out: list[str] | None = None,
):
    now = datetime.now(UTC)

    def _exec(sql: str, params: dict | None = None, settings: dict | None = None):
        s = str(sql)
        if queries_out is not None:
            queries_out.append(s)
        if "SELECT max(event_timestamp) AS max_ts" in s:
            return [{"max_ts": now, "cnt": cnt}] if cnt > 0 else [{"max_ts": None, "cnt": 0}]
        if "AS mean_val, stddevSamp(val) AS stddev_val" in s:
            return [{"hr": now.hour, "mean_val": 1.0, "stddev_val": 0.5}]
        if "AS val, count() AS req_count" in s:
            return [{"val": val, "req_count": cnt}]
        if "AS ts," in s:
            return [{"ts": now, "val": val}] if cnt > 0 else []
        return []

    return SimpleNamespace(execute=_exec)


def _preview_body(**overrides) -> dict:
    return {**_ALERT_BODY, "name": "Preview Alert", "metric": "5xx", "threshold": 1.0, **overrides}


def test_create_alert_persists_and_returns_fields(client):
    # 201 Created — resource POST convention (body shape unchanged).
    resp = client.post("/api/alerts/", json=_ALERT_BODY, headers=_ALERT_HDR)
    assert resp.status_code == 201
    data = resp.json()["data"]
    assert data["status"] == "success" and data["id"]
    row = _alert_row(data["id"])
    assert row is not None and (row["name"], row["metric"]) == ("High 5xx Rate", "5xx_rate")
    assert row["threshold"] == pytest.approx(5.0)

    resp2 = client.post(
        "/api/alerts/", json={**_ALERT_BODY, "name": "Latency Alert", "metric": "p95_latency"}, headers=_ALERT_HDR
    )
    assert resp2.status_code == 201 and resp2.json()["data"]["status"] == "success" and resp2.json()["data"]["id"]


@pytest.mark.parametrize(
    ("method", "path", "payload"),
    [
        ("POST", "/api/alerts/", {**_ALERT_BODY, "metric": "nonexistent_metric"}),
        ("POST", "/api/alerts/", {**_ALERT_BODY, "operator": "!="}),
        ("POST", "/api/views/", {"service_id": "svc1"}),
        ("GET", f"/api/alerts/{_SERVICE_ID}?limit=99999", None),
        ("GET", f"/api/alerts/{_SERVICE_ID}?limit=0", None),
    ],
)
def test_alerts_and_views_validation_422(client, method, path, payload):
    resp = client.post(path, json=payload, headers=_ALERT_HDR) if method == "POST" else client.get(path)
    assert resp.status_code == 422


def test_toggle_and_delete_alert_lifecycle(client):
    alert_id = _seed_alert(client)

    r_dis = client.patch(f"/api/alerts/{alert_id}/enabled", json={"enabled": False}, headers=_ALERT_HDR)
    assert r_dis.status_code == 200 and (_alert_row(alert_id) or {})["enabled"] == 0

    r_en = client.patch(f"/api/alerts/{alert_id}/enabled", json={"enabled": True}, headers=_ALERT_HDR)
    assert r_en.status_code == 200 and (_alert_row(alert_id) or {})["enabled"] == 1

    assert client.patch(f"/api/alerts/{alert_id}/enabled", json={}, headers=_ALERT_HDR).status_code == 422

    r_del = client.delete(f"/api/alerts/{alert_id}", headers=_ALERT_HDR)
    assert r_del.status_code == 204 and r_del.content == b"" and _alert_row(alert_id) is None

    # Deleting a non-existent alert should still succeed (no-op).
    assert client.delete("/api/alerts/non-existent-id", headers=_ALERT_HDR).status_code == 204


def test_create_and_delete_view_lifecycle(client):
    resp = client.post("/api/views/", json=_VIEW_BODY, headers=_ALERT_HDR)
    assert resp.status_code == 201 and resp.json()["status"] == "success"
    view_id = resp.json()["id"]
    assert view_id
    row = _view_row(view_id)
    assert row is not None and (row["name"], row["page"]) == ("My View", "dashboard")

    r_del = client.delete(f"/api/views/{view_id}", headers=_ALERT_HDR)
    assert r_del.status_code == 204 and r_del.content == b"" and _view_row(view_id) is None

    # Deleting a non-existent view should still succeed (no-op).
    assert client.delete("/api/views/nonexistent-view-id", headers=_ALERT_HDR).status_code == 204


def test_preview_alert_evaluation_types_and_status_exception(client):
    """Happy path across absolute, relative_increase, anomaly_zscore, and status-cache error fallback."""
    ch_client = _make_alert_ch_client(val=5.0, cnt=10)
    with (
        _patch_preview_source(),
        patch("backend.routers.alerts._resolve_ch_client", return_value=ch_client),
    ):
        resp_abs = client.post("/api/alerts/preview", json=_preview_body())
        assert resp_abs.status_code == 200
        d_abs = resp_abs.json()["data"]
        assert d_abs["type"] == "absolute" and len(d_abs["times"]) == len(d_abs["values"]) and sum(d_abs["values"]) > 0

        rel_body = _preview_body(evaluation_type="relative_increase", comparison_period_min=60, threshold=1.5)
        d_rel = client.post("/api/alerts/preview?lookback_hours=2", json=rel_body).json()["data"]
        assert d_rel["type"] == "relative" and len(d_rel["hist_values"]) == len(d_rel["values"])

        zs_body = _preview_body(evaluation_type="anomaly_zscore", baseline_period_days=7, zscore_threshold=3.0)
        d_zs = client.post("/api/alerts/preview", json=zs_body).json()["data"]
        assert d_zs["type"] == "anomaly_zscore"
        assert len(d_zs["hist_values"]) == len(d_zs["values"]) == len(d_zs["threshold_values"])

        with patch("backend.config.get_status", side_effect=Exception("mocked status exception")):
            assert client.post("/api/alerts/preview", json=_preview_body(evaluation_type="absolute")).status_code == 200


def test_preview_alert_unknown_service_and_query_failure(client):
    """No source → 404; ClickHouse error → 200 with inline error field."""
    with _patch_preview_source(None):
        resp_404 = client.post("/api/alerts/preview", json=_preview_body())
    assert resp_404.status_code == 404 and resp_404.json()["detail"] == {"error": "Service not found"}

    failing_ch = SimpleNamespace(execute=MagicMock(side_effect=RuntimeError("ClickHouse query failed")))
    with (
        _patch_preview_source(),
        patch("backend.routers.alerts._resolve_ch_client", return_value=failing_ch),
    ):
        resp_err = client.post("/api/alerts/preview", json=_preview_body())
    assert resp_err.status_code == 200 and resp_err.json().get("data") is None and resp_err.json().get("error")


@pytest.mark.parametrize(
    ("status_payload", "cnt", "expect_nonzero"),
    [
        ({"local_rows": 10, "latest_log_at": "2026-06-26T12:00:00Z"}, 10, True),
        ({}, 0, False),
    ],
)
def test_preview_alert_stale_view_self_heal_guard(client, status_payload, cnt, expect_nonzero):
    """Preview resolves anchor from status cache when populated and returns empty series when ClickHouse has 0 rows."""
    ch_client = _make_alert_ch_client(val=5.0 if cnt else 0.0, cnt=cnt)
    with (
        _patch_preview_source(),
        patch("backend.config.get_status", return_value=status_payload),
        patch("backend.routers.alerts._resolve_ch_client", return_value=ch_client),
    ):
        resp = client.post("/api/alerts/preview", json=_preview_body())

    assert resp.status_code == 200
    assert (sum(resp.json()["data"]["values"]) > 0) is expect_nonzero


@pytest.mark.parametrize(
    ("scope", "expected_clause"),
    [
        ("edge", "lower(custom_fields['edge']) IN ('true', '1')"),
        ("origin", "lower(custom_fields['edge']) IN ('false', '0')"),
    ],
)
def test_preview_alert_scope_filtering(client, scope, expected_clause):
    """``evaluation_scope`` ('edge' vs 'origin') filters ClickHouse ``custom_fields['edge']`` rows."""
    queries: list[str] = []
    ch_client = _make_alert_ch_client(val=0.0, cnt=4, queries_out=queries)
    with (
        _patch_preview_source(),
        patch("backend.routers.alerts._resolve_ch_client", return_value=ch_client),
    ):
        resp = client.post("/api/alerts/preview", json=_preview_body(evaluation_scope=scope))
    assert resp.status_code == 200 and sum(resp.json()["data"]["values"]) == 0
    assert any(expected_clause in q for q in queries)


@pytest.mark.parametrize(
    ("lookback_hours", "expected_interval"),
    [
        (1, "toStartOfMinute(event_timestamp)"),
        (12, "toStartOfFifteenMinutes(event_timestamp)"),
        (48, "toStartOfHour(event_timestamp)"),
    ],
)
def test_preview_alert_bucket_granularity_scales_with_lookback(client, lookback_hours, expected_interval):
    """The bucket size cascade (1m / 15m / 1h) is purely lookback-driven."""
    queries: list[str] = []
    ch_client = _make_alert_ch_client(val=5.0, cnt=5, queries_out=queries)
    with (
        _patch_preview_source(),
        patch("backend.routers.alerts._resolve_ch_client", return_value=ch_client),
    ):
        resp = client.post(f"/api/alerts/preview?lookback_hours={lookback_hours}", json=_preview_body())
    assert resp.status_code == 200 and any(expected_interval in q for q in queries)


def test_list_alerts_and_views_roundtrip_and_limits(client):
    """Round-trip create+list and d180c4c bounded list payload limit parameter."""
    for name in ("High 5xx Rate", "Second Alert", "Third Alert"):
        _seed_alert(client, name=name)
    all_alerts = client.get(f"/api/alerts/{_SERVICE_ID}").json()["data"]
    assert {"High 5xx Rate", "Second Alert"} <= {a["name"] for a in all_alerts}
    assert len(client.get(f"/api/alerts/{_SERVICE_ID}?limit=2").json()["data"]) == 2

    for name in ("My View", "Second View", "Third View"):
        _seed_view(client, name=name)
    all_views = client.get(f"/api/views/{_SERVICE_ID}").json()
    assert {"My View", "Second View"} <= {v["name"] for v in all_views}
    assert len(client.get(f"/api/views/{_SERVICE_ID}?limit=2").json()) == 2


@pytest.mark.parametrize(("allowed", "expect_other"), [(None, True), ({_SERVICE_ID}, False)])
def test_list_all_alerts_role_scoping(client, allowed, expect_other):
    _seed_alert(client)
    other_id = "other-service-id"
    client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "service_id": other_id, "name": "Other Service Alert"},
        headers={"x-fastly-service-id": other_id},
    )
    with (
        patch("backend.routers.alerts._analyst_allowed_services", return_value=allowed),
        patch(
            "backend.repositories.assets.svcconfig.list_configs",
            return_value=[{"service_id": _SERVICE_ID}, {"service_id": other_id}],
        ),
    ):
        resp = client.get("/api/alerts/")
    assert resp.status_code == 200
    names = {a["name"] for a in resp.json()["data"]}
    assert "High 5xx Rate" in names and ("Other Service Alert" in names) is expect_other


@pytest.mark.parametrize(
    ("method", "path", "json_body"),
    [
        ("PATCH", "/api/alerts/alert-id/enabled", {"enabled": False}),
        ("DELETE", "/api/alerts/alert-id", None),
        ("DELETE", "/api/views/view-id", None),
    ],
)
def test_alert_and_view_missing_service_id_returns_400(client, method, path, json_body):
    from backend.core.request_context import get_service_id

    app.dependency_overrides[get_service_id] = lambda: None
    try:
        kw = {"json": json_body} if json_body is not None else {}
        resp = client.request(method, path, headers={"x-fastly-service-id": ""}, **kw)
        assert resp.status_code == 400 and resp.json()["detail"]["error"] == "service_id_required"
    finally:
        app.dependency_overrides.pop(get_service_id, None)


@pytest.mark.parametrize(
    ("kind", "method", "suffix", "json_body"),
    [
        ("alert", "PATCH", "/enabled", {"enabled": False}),
        ("alert", "DELETE", "", None),
        ("view", "DELETE", "", None),
    ],
)
def test_alert_and_view_analyst_not_authorized_returns_403(client, kind, method, suffix, json_body):
    from backend.core.request_context import get_service_id

    other_id = "other-service-id"
    other_hdr = {"x-fastly-service-id": other_id}
    if kind == "alert":
        r = client.post("/api/alerts/", json={**_ALERT_BODY, "service_id": other_id}, headers=other_hdr)
        target_path = f"/api/alerts/{r.json()['data']['id']}{suffix}"
    else:
        r = client.post("/api/views/", json={**_VIEW_BODY, "service_id": other_id}, headers=other_hdr)
        target_path = f"/api/views/{r.json()['id']}{suffix}"

    app.dependency_overrides[get_service_id] = lambda: other_id
    try:
        with (
            patch("backend.routers.alerts._analyst_allowed_services", return_value={_SERVICE_ID}),
            patch("backend.utils.auth.analyst_allowed_services", return_value={_SERVICE_ID}),
            patch(
                "backend.repositories.assets.svcconfig.list_configs",
                return_value=[{"service_id": _SERVICE_ID}, {"service_id": other_id}],
            ),
        ):
            kw = {"json": json_body} if json_body is not None else {}
            resp = client.request(method, target_path, **kw)
        assert resp.status_code == 403 and resp.json()["detail"]["error"] == "service_not_authorized"
    finally:
        app.dependency_overrides.pop(get_service_id, None)


def test_update_alert_persists_changes(client):
    alert_id = _seed_alert(client)
    updated_body = {
        **_ALERT_BODY,
        "name": "Updated 5xx Rate Rule",
        "threshold": 12.5,
        "window_min": 15.0,
        "channels": [{"type": "slack", "url": "https://hooks.slack.com/services/T00/B00/X00"}],
    }
    resp = client.put(f"/api/alerts/{alert_id}", json=updated_body, headers=_ALERT_HDR)
    assert resp.status_code == 200
    assert (resp.json()["data"]["status"], resp.json()["data"]["id"]) == ("success", alert_id)
    row = _alert_row(alert_id)
    assert row is not None and row["name"] == "Updated 5xx Rate Rule" and row["threshold"] == pytest.approx(12.5)


def test_list_alerts_status_filtering(client):
    _seed_alert(client, name="Healthy Rule", enabled=True)
    _seed_alert(client, name="Muted Rule", enabled=False)
    firing_id = _seed_alert(client, name="Firing Rule", enabled=True, window_min=5.0)
    metadata_db.update_alert_last_triggered(_SERVICE_ID, firing_id, datetime.now(UTC).isoformat())

    for status, expected in (
        ("all", {"Healthy Rule", "Muted Rule", "Firing Rule"}),
        ("alerting", {"Firing Rule"}),
        ("ok", {"Healthy Rule"}),
        ("muted", {"Muted Rule"}),
    ):
        resp = client.get(f"/api/alerts/{_SERVICE_ID}?status={status}")
        assert resp.status_code == 200 and {a["name"] for a in resp.json()["data"]} == expected


def test_evaluate_alerts_endpoint(client):
    ch_client = _make_alert_ch_client(val=5.0, cnt=10)
    _seed_alert(client, name="Immediate 5xx Check", metric="5xx", threshold=1.0, enabled=True)
    with (
        _patch_preview_source(),
        patch("backend.routers.alerts._resolve_ch_client", return_value=ch_client),
    ):
        eval_resp = client.post("/api/alerts/evaluate", json={"service_id": _SERVICE_ID}, headers=_ALERT_HDR)
    assert eval_resp.status_code == 200
    payload = eval_resp.json()["data"]
    assert payload["service_id"] == _SERVICE_ID and payload["evaluated"] >= 1 and payload["triggered"] >= 1
    assert any(r["name"] == "Immediate 5xx Check" and r["triggered"] is True for r in payload["results"])


@pytest.mark.parametrize(
    "unsafe_url",
    [
        "http://127.0.0.1:8080/hook",
        "http://localhost/admin",
        "http://169.254.169.254/latest/meta-data/",
        "http://10.0.0.1/internal",
        "http://192.168.1.1/hook",
        "ftp://hooks.slack.com/services/T00/B00/X00",
    ],
)
def test_create_alert_rejects_ssrf_webhook_urls(client, unsafe_url):
    resp = client.post("/api/alerts/", json={**_ALERT_BODY, "webhook_url": unsafe_url}, headers=_ALERT_HDR)
    assert resp.status_code == 400 and resp.json()["detail"]["error"] == "invalid_webhook_url"


def test_test_alert_webhook_dry_run_and_dispatch(client):
    alert_id = _seed_alert(
        client,
        name="Webhook Rule",
        channels=[{"type": "slack", "url": "https://hooks.slack.com/services/T00/B00/X00"}],
    )
    dry_resp = client.post(
        f"/api/alerts/{alert_id}/test", json={"service_id": _SERVICE_ID, "dry_run": True}, headers=_ALERT_HDR
    )
    assert dry_resp.status_code == 200
    dry_data = dry_resp.json()["data"]
    assert (dry_data["alert_id"], dry_data["status"], dry_data["targets_tested"], dry_data["channel_type"]) == (
        alert_id,
        "validated",
        1,
        "slack",
    )

    with patch("backend.cron.jobs.metadata._post_slack_notification") as mock_post:
        live_resp = client.post(
            f"/api/alerts/{_SERVICE_ID}/{alert_id}/test", json={"dry_run": False}, headers=_ALERT_HDR
        )
    assert live_resp.status_code == 200
    assert (live_resp.json()["data"]["status"], live_resp.json()["data"]["targets_tested"]) == ("sent", 1)
    assert mock_post.call_count == 1


def test_analyst_in_scope_mutations_return_403_admin_only(client):
    alert_id = _seed_alert(client)
    mutations = [
        ("POST", "/api/alerts/", _ALERT_BODY),
        ("PUT", f"/api/alerts/{alert_id}", _ALERT_BODY),
        ("PATCH", f"/api/alerts/{alert_id}/enabled", {"enabled": False}),
        ("DELETE", f"/api/alerts/{alert_id}", None),
        ("POST", "/api/alerts/evaluate", {"service_id": _SERVICE_ID}),
        ("POST", f"/api/alerts/{alert_id}/test", {"service_id": _SERVICE_ID, "dry_run": True}),
    ]
    with patch("backend.routers.alerts._analyst_allowed_services", return_value={_SERVICE_ID}):
        for method, path, body in mutations:
            kw = {"json": body} if body is not None else {}
            resp = client.request(method, path, headers=_ALERT_HDR, **kw)
            assert resp.status_code == 403 and resp.json()["detail"]["error"] == "admin_only"


def test_evaluate_alerts_zero_enabled_skips_con_and_preview_cache(client, monkeypatch):
    with patch("backend.routers.alerts._resolve_ch_client") as mock_ch:
        eval_resp = client.post("/api/alerts/evaluate", json={"service_id": _SERVICE_ID}, headers=_ALERT_HDR)
    assert eval_resp.status_code == 200
    assert eval_resp.json()["data"]["evaluated"] == 0
    mock_ch.assert_not_called()

    ch_client = _make_alert_ch_client(val=5.0, cnt=6)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    from backend.routers import alerts as alerts_router_mod

    alerts_router_mod._ALERT_PREVIEW_CACHE.clear()
    with (
        _patch_preview_source(),
        patch("backend.routers.alerts._resolve_ch_client", return_value=ch_client),
    ):
        r1 = client.post("/api/alerts/preview", json=_preview_body())
        assert r1.status_code == 200
    with (
        _patch_preview_source(),
        patch("backend.routers.alerts._resolve_ch_client") as mock_preview_ch,
    ):
        r2 = client.post("/api/alerts/preview", json=_preview_body())
        assert r2.status_code == 200 and r2.json()["data"] == r1.json()["data"]
        mock_preview_ch.assert_not_called()
    alerts_router_mod._ALERT_PREVIEW_CACHE.clear()
