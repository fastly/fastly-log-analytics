"""Consolidated unit and integration tests for analytics routers."""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import duckdb
import pytest
from backend.deps import get_con, get_source
from backend.high_scale.assets import assets_aggregates
from backend.models.insights import InsightsRequest
from backend.models.performance import AssetsRequest
from backend.models.security import SecurityAggregatesResponse
from backend.utils.bounded_cache import CacheRegistry
from backend.utils.date_utils import iso_z
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from starlette.routing import Mount

from backend import config as svcconfig
from backend.core import metadata as metadata_db
from backend.core.duckdb import _clear_schema_cache
from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.request_context import build_request_context
from backend.core.rollups import NETWORK_SUMMARY_BUNDLE_FILENAME, OVERVIEW_BUNDLE_FILENAME, _hour_bundled_root
from backend.main import app
from backend.repositories import dashboard as dashboard_repo
from backend.repositories._base import QueryRunner, _safe_table, _schema_cols_cache
from backend.utils import bot_sources
from backend.utils import telemetry as telemetry_mod
from backend.utils.remote_access import _ANALYST_ALLOWED_WRITE_PREFIXES, TimeBounds
from tests._helpers.duckdb_fixtures import generate_mock_logs, insert_mock_logs
from tests.conftest import MOCK_SERVICE_ID, override_request_context

_SVC_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}


def _seed_logs(con, src, num_logs: int = 10, hours_ago: int | None = None, **overrides) -> list[dict]:
    kwargs = {"hours_ago": hours_ago} if hours_ago is not None else {}
    logs = generate_mock_logs(src, num_logs=num_logs, **kwargs)
    if overrides:
        for log in logs:
            log.update(overrides)
    insert_mock_logs(con, _safe_table(src["name"]), logs)
    return logs


def _naive_iso_window(hours: int = 3) -> tuple[str, str]:
    now = datetime.now(UTC)
    return (
        (now - timedelta(hours=hours)).replace(tzinfo=None).isoformat(),
        (now + timedelta(hours=hours)).replace(tzinfo=None).isoformat(),
    )


@contextmanager
def _with_analyst_ctx(
    src, con, *, mask_ips: bool = False, time_bounds=None, path: str | None = None, session_id: str = "sess-analyst-1"
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


def _seed_origin_table(con, src) -> None:
    con.execute(f"DROP VIEW IF EXISTS {_safe_table(src['name'])}")
    CacheRegistry.clear_all()
    logs = generate_mock_logs(src, num_logs=40, hours_ago=1)
    for i, log in enumerate(logs):
        oip = "203.0.113.1" if i < 25 else "203.0.113.2"
        log.update(ottfb=50_000 + i * 1000, ost=200 if i % 5 != 0 else 500, oip=oip)
        log.update(url="/api/foo" if i % 2 else "/api/bar", edge=True, pop="SJC")
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def _seed_performance_table(con, src) -> None:
    kw = {"elapsed": 120000, "ttfb": 0.040, "url": "/test-perf-url", "asn": 12345}
    kw |= {"cache": "HIT", "ttl": 3600, "ottfb": 30000.0, "ottlb": 50000.0}
    _seed_logs(con, src, 20, hours_ago=1, **kw)


def _seed_security_table(con, src) -> None:
    _schema_cols_cache.clear()
    _clear_schema_cache()
    logs = generate_mock_logs(src, num_logs=40, hours_ago=1)
    j3_a, j3_b = "771,4865-4866-4867,0-23-65281-10-11,29-23-24,0", "772,4865-4866,0-23-10,29-23,0"
    j4_a, j4_b = "t13d1516h2_8daaf6152771_0271d18f93db", "t13d1516h2_aabbccddeeff_112233445566"
    ua_bot = "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ua_mac = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
    for i, log in enumerate(logs):
        even = i % 2 == 0
        log.update(ja3=j3_a if even else j3_b, ja4=j4_a if even else j4_b, tls="1.3" if even else "1.2")
        log.update(tls_ciphers_sha="aabbcc11" if i < 20 else "ddeeff22", p_type="VPN" if i % 3 == 0 else None)
        log.update(ua=ua_bot if i % 4 == 0 else ua_mac, host="example.com", waf=i % 7 == 0)
        log.update(
            waf_sig="SQLI,XSS" if i < 10 else "", waf_resp=403 if i < 10 else 200, req_header_bytes=1024 + i * 50
        )
        log.update(rtt_min=5 if i % 3 == 0 else 1000, tcp_rtt=1000, lat=37.7749, lon=-122.4194)
        log.update(country="US" if even else "DE", asn=15169 if even else 3320)
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def _register_high_scale(monkeypatch, test_service_source):
    service = SimpleNamespace(service_id=test_service_source["service_id"])
    monkeypatch.setattr(
        "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
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
def test_insights_role_clamping_and_force_refresh(client, in_memory_duckdb, test_service_source, is_analyst):
    now = datetime.now(UTC)
    start, end = now - timedelta(hours=3), now - timedelta(hours=2)
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_insights()

    tb = TimeBounds(start=start, end=end)
    ctx_mgr = (
        _with_analyst_ctx(test_service_source, in_memory_duckdb, mask_ips=True, time_bounds=tb, path="/api/insights")
        if is_analyst
        else contextmanager(lambda: (yield))()
    )
    req_json = {"window_size_hrs": 1.0, "baseline_hours": 168.0, "force_refresh": True}
    with ctx_mgr, patch("backend.repositories.insights.get_insights", side_effect=_fake):
        resp = client.post("/api/insights", headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200, resp.text
    if is_analyst:
        assert (captured["force_refresh"], captured["mask_ips"]) == (False, True)
        assert (captured["clamp_start"], captured["clamp_end"]) == (start.isoformat(), end.isoformat())
    else:
        assert (captured["force_refresh"], captured["clamp_start"], captured["clamp_end"]) == (True, None, None)


def test_insights_endpoint_stale_view_retry_and_real_execution(client, in_memory_duckdb, test_service_source):
    call_count = 0

    def _side_effect(**kw):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception('IO Error: No files found that match the pattern "buffer/batch_1.parquet"')
        return _stub_insights()

    with (
        patch("backend.repositories.insights.get_insights", side_effect=_side_effect),
        patch("backend.core.iceberg._core.clear_source_caches") as mock_clear,
        patch("backend.core.iceberg._core.update_iceberg_view") as mock_update,
    ):
        resp = client.post(
            "/api/insights", headers=_SVC_HEADERS, json={"window_size_hrs": 24.0, "baseline_hours": 168.0}
        )
    assert resp.status_code == 200 and resp.json()["insights"][0]["id"] == "city_surges"
    assert call_count == 2 and mock_clear.called and mock_update.called

    _seed_logs(in_memory_duckdb, test_service_source, 50)
    resp_real = client.post("/api/insights", headers=_SVC_HEADERS, json={"window_hours": 1.0, "baseline_hours": 24.0})
    assert resp_real.status_code == 200 and isinstance(resp_real.json()["insights"], list)


def test_cache_collapse_detail_endpoint_stub_and_real(client, in_memory_duckdb, test_service_source):
    req_stub = {"url": "/api/v1/heavy", "window_size_hrs": 24.0, "baseline_hours": 168.0}
    with patch("backend.repositories.insights.get_cache_collapse_detail", return_value=_stub_cache_collapse_detail()):
        resp = client.post("/api/insights/cache-collapse-detail", headers=_SVC_HEADERS, json=req_stub)
    assert resp.status_code == 200
    data = resp.json()
    assert (data["url"], data["baseline_hit_rate"], data["breakdown"]["hits"]) == ("/api/v1/heavy", 0.95, 10)

    _seed_logs(in_memory_duckdb, test_service_source, 5, url="/test-url-collapse", cache="MISS", ip="1.2.3.4")
    req_real = {"url": "/test-url-collapse", "window_size_hrs": 1.0, "baseline_hours": 24.0}
    resp_real = client.post("/api/insights/cache-collapse-detail", headers=_SVC_HEADERS, json=req_real)
    assert resp_real.status_code == 200
    real_data = resp_real.json()
    assert real_data["url"] == "/test-url-collapse"
    assert (real_data["baseline_hit_rate"], real_data["window_hit_rate"]) == (0.0, 0.0)
    assert (real_data["baseline_pass_rate"], real_data["window_pass_rate"]) == (0.0, 0.0)
    assert set(real_data["breakdown"].keys()) == {"hits", "misses", "passes", "other"} and "evictions" not in real_data
    assert all(row["cache"].upper().startswith("MISS") for row in real_data["recent_misses"])


# ── Shared Cross-Router Contract Tests ──
@pytest.mark.parametrize(
    "endpoint",
    ["/api/network-health", "/api/origin/aggregates", "/api/security/aggregates", "/api/performance/aggregates"],
)
def test_aggregates_reject_unknown_section(client, in_memory_duckdb, test_service_source, endpoint):
    _seed_logs(in_memory_duckdb, test_service_source, 5)
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
def test_aggregates_range_token_resolution(client, in_memory_duckdb, test_service_source, endpoint, expected_key):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with _patch_range_token():
        resp = client.post(
            endpoint, headers=_SVC_HEADERS, json={"filters": {}, "range_token": "24h", "anchor": "2026-08-24T13:00:00Z"}
        )
    assert resp.status_code == 200 and expected_key in resp.json()


_CLAMP_CASES = [
    (
        "/api/performance/aggregates",
        "backend.repositories.performance.get_performance_aggregates",
        {"top_urls": [], "top_asns": [], "ttl_dist": [], "scatter": [], "waterfall": {}},
    ),
    (
        "/api/value/summary",
        "backend.repositories.value.get_summary",
        {"overview": {"total_requests": 100, "origin_offload_pct": 95.0}},
    ),
]


@pytest.mark.parametrize(("endpoint", "repo_target", "stub_return"), _CLAMP_CASES)
def test_analyst_session_clamps_requested_window(
    client, in_memory_duckdb, test_service_source, endpoint, repo_target, stub_return
):
    start, end = datetime(2026, 6, 17, 9, 0, tzinfo=UTC), datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return stub_return

    tb = TimeBounds(start=start, end=end)
    req_json = {"filters": {}, "start_time": "2026-06-17T00:00:00Z", "end_time": "2026-06-17T23:59:59Z"}
    with (
        _with_analyst_ctx(test_service_source, in_memory_duckdb, mask_ips=True, time_bounds=tb, path=endpoint),
        patch(repo_target, side_effect=_fake),
    ):
        resp = client.post(endpoint, headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200, resp.text
    assert (captured["start_time"], captured["end_time"]) == (start.isoformat(), end.isoformat())


@pytest.mark.parametrize(
    ("endpoint", "seeder"),
    [("/api/performance/aggregates", _seed_performance_table), ("/api/security/aggregates", _seed_security_table)],
)
def test_aggregates_telemetry_attribution(client, in_memory_duckdb, test_service_source, monkeypatch, endpoint, seeder):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    seeder(in_memory_duckdb, test_service_source)
    resp = client.post(endpoint, headers={**_SVC_HEADERS, "X-Page-Load-ID": "test-page-load-42"}, json={"filters": {}})
    assert resp.status_code == 200 and len(resp.json().get("_debug_queries", [])) > 0


# ── Network Router Tests ──
def test_network_health_selectors_and_error_sentinel(client, in_memory_duckdb, test_service_source):
    _seed_logs(in_memory_duckdb, test_service_source, 10, tcp_rtt=25_000, asn=7922, country="US")

    full = client.post("/api/network-health", headers=_SVC_HEADERS, json={"filters": {}}).json()
    for k in ("shielding_analysis", "summary", "heatmap", "available"):
        assert k in full

    sh_only = client.post(
        "/api/network-health", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["shielding_analysis"]}
    ).json()
    assert "shielding_analysis" in sh_only
    timings = {t["section"] for t in sh_only.get("_section_timings", [])}
    for blocked in {"temp_table_create", "heatmap_query", "map_query", "metro_query"}:
        assert blocked not in timings

    combo = client.post(
        "/api/network-health",
        headers=_SVC_HEADERS,
        json={"filters": {}, "sections": ["leaderboard", "shielding_analysis"]},
    ).json()
    assert "leaderboard" in combo and "shielding_analysis" in combo

    with patch("backend.repositories.origin.get_shielding_analysis", side_effect=RuntimeError("boom")):
        err_resp = client.post(
            "/api/network-health", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["shielding_analysis"]}
        )
    assert err_resp.status_code == 200
    sa = err_resp.json()["shielding_analysis"]
    assert sa["error"] is True and sa["has_data"] is False


def test_get_pop_health_returns_data_and_self_heals_on_lake_detach_race(client, in_memory_duckdb, test_service_source):
    kw = {"pop": "IAD", "status": 200, "tcp_rtt": 12000, "ttfb": 50.0, "cache": "HIT", "resp_bytes": 1000}
    _seed_logs(in_memory_duckdb, test_service_source, 5, **kw)
    st, et = _naive_iso_window(3)
    real_track_query = telemetry_mod.track_query
    calls = {"n": 0}

    class _RaiseOnEnter:
        def __enter__(self):
            raise duckdb.CatalogException(
                'Catalog Error: Table with name "lake.logs_x" does not exist because schema "lake" does not exist'
            )

        def __exit__(self, *_a):
            return False

    def flaky_track_query(con, query, params, label):
        if label == "pop_health":
            calls["n"] += 1
            if calls["n"] == 1:
                return _RaiseOnEnter()
        return real_track_query(con, query, params, label)

    with (
        patch.object(telemetry_mod, "track_query", flaky_track_query),
        patch("backend.core.iceberg._core.update_iceberg_view", lambda *a, **k: None),
        patch("backend.core.iceberg._core.clear_source_caches", lambda *a, **k: None),
    ):
        response = client.get(f"/api/network/pop-health?start_time={st}&end_time={et}", headers=_SVC_HEADERS)

    assert response.status_code == 200
    row = response.json()["data"][0]
    assert (row["pop"], row["requests"], row["errors"], row["cache_hit_rate"]) == ("IAD", 5, 0, 100.0)
    assert calls["n"] == 2


def test_network_bundle_post_get_selective_and_debug(client, in_memory_duckdb, test_service_source, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES_FORCE_INCLUDE", "0")
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for i, log in enumerate(logs):
        log.update(pop="IAD" if i < 5 else "SJC", status=200, tcp_rtt=20_000, ttfb=45.0, cache="HIT", resp_bytes=2000)
        log.update(retrans=1, ploss=0.005, delivery_rate=10_000_000, proto="HTTP/2", asn=7922, country="US")
    insert_mock_logs(in_memory_duckdb, table, logs)
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
    assert [(row["source"], row.get("outcome")) for row in owner["_section_timings"]] == [
        ("rollup", "fallback"),
        ("live", "served"),
    ]

    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    no_debug = client.post("/api/network/bundle", headers=debug_headers, json=payload)
    assert no_debug.status_code == 200 and "_component_observations" not in no_debug.json()
    assert any("/api/network/bundle".startswith(prefix) for prefix in _ANALYST_ALLOWED_WRITE_PREFIXES)


def test_network_bundle_missing_optional_fields_fallback(client, in_memory_duckdb, test_service_source):
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table}")
    excluded = {"tcp_rtt", "retrans", "ploss", "delivery_rate"}
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["status"] = 200
        for k in excluded:
            log.pop(k, None)

    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None and f["id"] not in excluded]
    columns_map = {f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"]) for f in raw_fields}
    for log in logs:
        for k, v in log.items():
            if k not in columns_map and k not in excluded:
                t_map = {bool: "BOOLEAN", int: "BIGINT", float: "DOUBLE"}
                columns_map[k] = t_map.get(type(v), "VARCHAR")
    schema_def = ", ".join([f'"{col}" {dtype}' for col, dtype in columns_map.items()])
    in_memory_duckdb.execute(f"CREATE TABLE {table} ({schema_def})")

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as tf:
        tf_path = tf.name
        for log in logs:
            tf.write(json.dumps(log, default=str) + "\n")
    try:
        cols_sql = "{" + ", ".join(f"'{k}': '{v}'" for k, v in columns_map.items()) + "}"
        in_memory_duckdb.execute(
            f"INSERT INTO {table} BY NAME SELECT * FROM read_json_auto('{tf_path}', columns={cols_sql})"
        )
    finally:
        os.unlink(tf_path)

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
    ("timeseries", "get_timeseries", {"has_data": False, "series": []}, _TS_KW, _TS_KW),
    (
        "slow-urls",
        "get_slow_urls",
        {"has_data": False, "rows": []},
        {"limit": 5, "min_requests": 25},
        {"limit": 5, "min_requests": 25},
    ),
    ("slow-urls", "get_slow_urls", {"has_data": False, "rows": []}, {"limit": 100_000}, {"limit": 100}),
    ("shielding-analysis", "get_shielding_analysis", {"has_data": False, "rows": []}, {"limit": 500}, {"limit": 200}),
    (
        "drilldown",
        "get_drilldown",
        {"has_data": True, "dimension": "pop", "rows": []},
        {"dimension": "pop", "limit": 15},
        {"dimension": "pop", "limit": 15},
    ),
]


@pytest.mark.parametrize(("subpath", "fn_name", "stub_return", "extra", "expected_kwargs"), _ORIGIN_FORWARD_CASES)
def test_origin_kwargs_forwarding_and_limit_clamping(
    client, in_memory_duckdb, test_service_source, subpath, fn_name, stub_return, extra, expected_kwargs
):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch(f"backend.repositories.origin.{fn_name}", return_value=stub_return) as mock:
        resp = client.post(f"/api/origin/{subpath}", headers=_SVC_HEADERS, json={"filters": {}, **extra})
    assert resp.status_code == 200
    for k, v in expected_kwargs.items():
        assert mock.call_args.kwargs[k] == v


@pytest.mark.parametrize(
    ("exc", "expected_status"), [(ValueError("bad filter"), 400), (KeyError("unknown source"), 404)]
)
def test_origin_summary_error_mapping(client, exc, expected_status):
    with patch("backend.repositories.origin.get_summary", side_effect=exc):
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
def test_origin_aggregates_section_coupling_and_pruning(
    client, in_memory_duckdb, test_service_source, sections, required_timings, blocked_timings
):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/origin/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": sections})
    assert resp.status_code == 200
    timings = {t["section"] for t in resp.json().get("_section_timings", [])}
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
    monkeypatch.setitem(sys.modules, "backend.high_scale.origin", SimpleNamespace(aggregates=high_scale_aggregates))
    _register_high_scale(monkeypatch, test_service_source)
    standard = AsyncMock(side_effect=AssertionError("standard Origin repository called"))
    monkeypatch.setattr("backend.repositories.origin.get_aggregates", standard)

    response = client.post(
        "/api/origin/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["summary"]}
    )
    assert response.status_code == 200 and response.json()["total"] == 12
    assert high_scale_aggregates.call_args.kwargs["sections"] == {"summary"}
    standard.assert_not_awaited()


def test_origin_drilldown_default_and_empty_table(client, in_memory_duckdb, test_service_source):
    empty_resp = client.post("/api/origin/drilldown", headers=_SVC_HEADERS, json={"filters": {}, "dimension": "oip"})
    assert empty_resp.status_code == 200
    ej = empty_resp.json()
    assert (ej["dimension"], ej["has_data"], ej["rows"]) == ("oip", False, [])

    _seed_origin_table(in_memory_duckdb, test_service_source)
    seeded_resp = client.post("/api/origin/drilldown", headers=_SVC_HEADERS, json={"filters": {}})
    assert seeded_resp.status_code == 200 and seeded_resp.json()["dimension"] == "host"


# ── Performance Router Tests ──
def test_performance_aggregates_section_pruning_and_auto_expand(client, in_memory_duckdb, test_service_source):
    _seed_performance_table(in_memory_duckdb, test_service_source)

    resp_top = client.post(
        "/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["top_urls"]}
    )
    assert resp_top.status_code == 200
    data_top = resp_top.json()
    assert len(data_top["top_urls"]) > 0 and len(data_top["top_asns"]) > 0
    assert (data_top["ttl_dist"], data_top["scatter"], data_top["waterfall"]) == ([], [], {})
    top_timings = {t["section"] for t in data_top.get("_section_timings", [])}
    assert "top_urls_query" in top_timings and "top_asns_query" in top_timings
    assert "ttl_dist_query" not in top_timings and "scatter_waterfall_query" not in top_timings

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
    ttl_timings = {t["section"] for t in data_ttl.get("_section_timings", [])}
    assert "ttl_dist_query" in ttl_timings
    for blocked in ("top_urls_query", "top_asns_query", "scatter_waterfall_query"):
        assert blocked not in ttl_timings


@pytest.mark.parametrize("sort_by", ["avg", "p50", "p95", "p99"])
def test_performance_aggregates_sort_by_variants(client, in_memory_duckdb, test_service_source, sort_by):
    _seed_performance_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sort_by": sort_by})
    assert resp.status_code == 200 and len(resp.json()["top_urls"]) > 0


def test_performance_aggregates_high_scale_dispatch_and_expanded_sections(client, monkeypatch, test_service_source):
    _register_high_scale(monkeypatch, test_service_source)
    row = {"url": "/hs", "requests": 50, "avg": 10.0, "p50": 8.0, "p95": 15.0, "p99": 20.0}
    fake_response = {"top_urls": [row], "top_asns": [], "ttl_dist": [], "scatter": [], "waterfall": {}, "_approx": True}
    req_json = {"filters": {}, "sort_by": "avg", "sections": ["scatter", "top_urls"]}
    with patch("backend.high_scale.performance.performance_aggregates", return_value=fake_response) as mock_hs_perf:
        resp = client.post("/api/performance/aggregates", headers=_SVC_HEADERS, json=req_json)
    assert resp.status_code == 200 and resp.json()["_approx"] is True
    assert mock_hs_perf.call_args.kwargs["sections"] == {"scatter", "waterfall", "top_urls", "top_asns"}
    assert mock_hs_perf.call_args.args[1].sort_by == "avg"


# ── Security Router Tests ──
def test_security_aggregates_and_bundle_contracts(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)

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
    timings_names = {t["section"] for t in sel_resp.json().get("_section_timings", [])}
    assert "conn_reuse_dist" in timings_names
    for blocked in {"ipv6_adoption", "proxy_dist", "verified_bots_ts", "wellknown_bots_query"}:
        assert blocked not in timings_names

    with patch("backend.repositories.security.get_security_aggregates", return_value={}) as mock_repo:
        client.post(
            "/api/security/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["tls_fingerprints"]}
        )
    sections_called = mock_repo.call_args.kwargs.get("sections")
    assert "tls_fingerprints" in sections_called and "fingerprint_coverage" in sections_called


def test_security_analyst_masking_and_window_clamping(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    earlier, later = datetime.now(UTC) - timedelta(hours=2), datetime.now(UTC)
    req_json = {
        "filters": {},
        "sections": ["top_ips_header"],
        "start_time": (datetime.now(UTC) - timedelta(days=7)).isoformat(),
        "end_time": later.isoformat(),
    }
    with _with_analyst_ctx(
        test_service_source, in_memory_duckdb, mask_ips=True, time_bounds=TimeBounds(start=earlier, end=later)
    ):
        resp = client.post("/api/security/aggregates", headers=_SVC_HEADERS, json=req_json)
        assert resp.status_code == 200
        for entry in resp.json().get("top_ips_header", []):
            ip = entry.get("ip") or entry.get("client_ip")
            if ip:
                assert "xxx" in ip or "*" in ip or ip == "0.0.0.0" or "[redacted]" in ip


@pytest.mark.parametrize(
    ("fmt", "mask_ips", "expected_ct"),
    [("fastly-acl", False, "text/csv"), ("plain", False, "text/plain"), ("plain", True, "text/plain")],
)
def test_export_proxies_formats_and_masking(client, in_memory_duckdb, test_service_source, fmt, mask_ips, expected_ct):
    _seed_security_table(in_memory_duckdb, test_service_source)
    now = datetime.now(UTC)
    ctx_mgr = (
        _with_analyst_ctx(test_service_source, in_memory_duckdb, mask_ips=True)
        if mask_ips
        else contextmanager(lambda: (yield))()
    )
    params = {
        "start_time": (now - timedelta(hours=2)).isoformat(),
        "end_time": now.isoformat(),
        "threshold": "High",
        "format": fmt,
    }
    with ctx_mgr:
        resp = client.get("/api/security/proxies/export", headers=_SVC_HEADERS, params=params)
    assert resp.status_code == 200
    assert expected_ct in resp.headers.get("content-type", "")
    assert "attachment; filename=" in resp.headers.get("content-disposition", "")
    if fmt == "fastly-acl":
        assert "ip,comment" in resp.text
    if mask_ips:
        for line in (ln.strip() for ln in resp.text.splitlines() if ln.strip()):
            assert "xxx" in line or "*" in line or line == "0.0.0.0" or "[redacted]" in line


def test_security_threat_intel_default(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.get("/api/security/threat-intel", headers=_SVC_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    if data["data"]:
        keys = "tls_fingerprint requests top_country matched_waf_rules_count bot_percentage is_anonymized_proxy"
        for k in keys.split():
            assert k in data["data"][0]


def test_security_high_scale_aggregates_threat_intel_and_proxies_export(
    client, monkeypatch, in_memory_duckdb, test_service_source
):
    service = _register_high_scale(monkeypatch, test_service_source)
    hs_aggregates = MagicMock(return_value=SecurityAggregatesResponse())
    hs_threat_intel = MagicMock(return_value={"data": []})
    c1 = {"ip": "198.51.100.xxx", "risk_level": "High", "impossible_distance": True}
    c2 = {"ip": "198.51.101.xxx", "risk_level": "Medium"}
    hs_proxies = MagicMock(return_value={"active_clients": [c1, c2]})
    monkeypatch.setattr("backend.high_scale.security.security_aggregates", hs_aggregates)
    monkeypatch.setattr("backend.high_scale.security.get_security_threat_intel", hs_threat_intel)
    monkeypatch.setattr("backend.high_scale.security.get_proxies_data", hs_proxies)

    ceiling_start, ceiling_end = datetime.now(UTC) - timedelta(hours=2), datetime.now(UTC)
    tb = TimeBounds(start=ceiling_start, end=ceiling_end)
    with _with_analyst_ctx(test_service_source, in_memory_duckdb, mask_ips=True, time_bounds=tb):
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
def test_value_summary_accepts_section_selectors(client, in_memory_duckdb, test_service_source, sections):
    _seed_logs(in_memory_duckdb, test_service_source, 20, hours_ago=1)
    resp = client.post("/api/value/summary", headers=_SVC_HEADERS, json={"filters": {}, "sections": sections})
    assert resp.status_code == 200, resp.text


def test_value_summary_empty_and_missing_network_columns(client, in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

    resp_empty = client.post("/api/value/summary", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp_empty.status_code == 200, resp_empty.text

    in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table_name}")
    in_memory_duckdb.execute(f"CREATE TABLE {table_name} (timestamp TIMESTAMPTZ, req_bytes INT)")
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (TIMESTAMPTZ '2026-07-23 12:00:00Z', 100)")
    CacheRegistry.clear_all()

    bundled_root = _hour_bundled_root(test_service_source)
    os.makedirs(bundled_root, exist_ok=True)
    now = datetime.now(UTC)
    req_json = {
        "filters": {},
        "sections": ["network"],
        "start_time": iso_z(now - timedelta(hours=1)),
        "end_time": iso_z(now + timedelta(hours=1)),
    }
    try:
        resp = client.post("/api/value/summary", headers=_SVC_HEADERS, json=req_json)
        assert resp.status_code == 200, resp.text
    finally:
        shutil.rmtree(os.path.dirname(bundled_root), ignore_errors=True)


def test_value_summary_tls_and_verified_bots_fallbacks(client, in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (timestamp TIMESTAMPTZ, tls VARCHAR, waf_sig VARCHAR, _bot_name VARCHAR, resp_bytes INT)"
    )
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} VALUES "
        "(TIMESTAMPTZ '2026-07-23 12:00:00Z', '1.3', 'SHIELD,VERIFIED-BOT.googlebot', 'Googlebot', 100), "
        "(TIMESTAMPTZ '2026-07-23 12:05:00Z', '', NULL, '', 200)"
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


def test_value_summary_rollup_crosses_active_hour(client, in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    now = datetime.now(UTC).replace(minute=10, second=0, microsecond=0)
    prev_hour = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    in_memory_duckdb.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} ("
        "timestamp TIMESTAMPTZ, cache VARCHAR, resp_bytes BIGINT, is_shield BOOLEAN, "
        "waf INT, waf_resp INT, elapsed DOUBLE, protocol VARCHAR, is_ssl BOOLEAN, ipv6 BOOLEAN)"
    )
    in_memory_duckdb.execute(
        f"INSERT INTO {table_name} VALUES "
        f"(TIMESTAMPTZ '{iso_z(now)}', 'HIT', 1000, true, 0, 200, 1200.0, 'HTTP/3', true, true), "
        f"(TIMESTAMPTZ '{iso_z(now)}', 'MISS', 2000, false, 1, 406, 4800.0, 'HTTP/2', true, false)"
    )

    bundled_root = _hour_bundled_root(test_service_source)
    hour_dir = os.path.join(bundled_root, f"hour={prev_hour.strftime('%Y-%m-%d-%H')}")
    os.makedirs(hour_dir, exist_ok=True)
    ov_path = os.path.join(hour_dir, OVERVIEW_BUNDLE_FILENAME)
    net_path = os.path.join(hour_dir, NETWORK_SUMMARY_BUNDLE_FILENAME)

    try:
        in_memory_duckdb.execute(
            f"COPY (SELECT TIMESTAMPTZ '{iso_z(prev_hour)}' AS hour_start, "
            "CAST(10 AS BIGINT) AS requests, CAST(8 AS BIGINT) AS hit_requests, "
            "CAST(2 AS BIGINT) AS miss_requests, CAST(0 AS BIGINT) AS pass_requests, "
            "CAST(0 AS BIGINT) AS synth_requests, CAST(2 AS BIGINT) AS origin_requests, "
            "CAST(8000 AS BIGINT) AS bandwidth_saved_bytes, CAST(10000 AS BIGINT) AS total_bandwidth_bytes, "
            "CAST(4 AS BIGINT) AS shield_hit_requests, CAST(5 AS BIGINT) AS shield_total_requests, "
            "CAST(1 AS BIGINT) AS threats_blocked, CAST(8000.0 AS DOUBLE) AS hit_elapsed_sum, "
            "CAST(8 AS BIGINT) AS hit_elapsed_count, CAST(8000.0 AS DOUBLE) AS miss_elapsed_sum, "
            f"CAST(2 AS BIGINT) AS miss_elapsed_count) TO '{ov_path}' (FORMAT PARQUET)"
        )
        in_memory_duckdb.execute(
            f"COPY (SELECT TIMESTAMPTZ '{iso_z(prev_hour)}' AS hour_start, "
            "CAST(10 AS BIGINT) AS requests, CAST(6 AS BIGINT) AS http3_requests, "
            "CAST(3 AS BIGINT) AS h2_requests, CAST(1 AS BIGINT) AS h11_requests, "
            "CAST(0 AS BIGINT) AS other_proto_requests, CAST(10 AS BIGINT) AS tls_requests, "
            f"CAST(5 AS BIGINT) AS ipv6_requests) TO '{net_path}' (FORMAT PARQUET)"
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
    finally:
        shutil.rmtree(os.path.dirname(bundled_root), ignore_errors=True)


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
def test_analytics_page_smoke_endpoints(
    client, in_memory_duckdb, test_service_source, method, path, payload, expected_keys
):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = (
        client.post(path, headers=_SVC_HEADERS, json=payload)
        if method == "POST"
        else client.get(path, headers=_SVC_HEADERS)
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    for key in expected_keys:
        assert key in body


def test_query_and_views_endpoints(client, in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])
    _seed_logs(in_memory_duckdb, test_service_source, 20)

    q_resp = client.post("/api/query", headers=_SVC_HEADERS, json={"sql": f"SELECT count(*) FROM {table_name}"})
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
    ("GET", "/api/admin/vcl-health", None),
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
        from backend.models.provision import NgwafWorkspacesResponse

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
def observation_request(in_memory_duckdb, test_service_source, tmp_path, monkeypatch) -> dict:
    in_memory_duckdb.execute("SET TimeZone = 'UTC'")
    test_service_source["_cache_dir_override"] = str(tmp_path / "request-cache")
    monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
    source_dir = tmp_path / "bot-sources"
    source_dir.mkdir()
    bot_entry = {"id": "googlebot", "name": "Googlebot", "pattern": {"accepted": ["Googlebot"]}}
    bot_entry["verification"] = {"domains": [], "cidrs": []}
    (source_dir / "well-known-bots.json").write_text(json.dumps({"entries": [bot_entry]}))
    monkeypatch.setattr(bot_sources, "_CACHE_DIR", source_dir)
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    seed_time = datetime.now(UTC)
    closed_time = seed_time.replace(minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
    cmcd_kw = {"cmcd_sid": "test-session", "cmcd_bl": 1000, "cmcd_bs": False, "cmcd_br": 1000, "cmcd_mtp": 2000}
    cmcd_kw |= {"cmcd_ot": "v", "cmcd_sf": "h", "cmcd_cid": "test-content"}
    for index, log in enumerate(logs):
        ts = (seed_time - timedelta(microseconds=1) if index % 2 else closed_time).isoformat()
        log.update(timestamp=ts, ua="Googlebot/2.1", tcp_rtt=1000, asn=15169, country="US", **cmcd_kw)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)
    now = datetime.now(UTC)
    return {"start_time": (now - timedelta(hours=2)).isoformat(), "end_time": now.isoformat(), "filters": {}}


@pytest.mark.parametrize(
    "path,sections",
    [
        ("/api/security/aggregates", ["ipv6_adoption"]),
        ("/api/performance/aggregates", ["ttl_dist"]),
        ("/api/cmcd/aggregates", ["overview"]),
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

    for _ in range(2):
        response = client.post(
            "/api/origin/aggregates", json={**observation_request, "sections": ["summary"]}, headers=_SVC_HEADERS
        )
        assert response.status_code == 200
        assert response.json()["_response_cache"] == {"state": "bypass", "prewarmer": False}

    for expected in ("miss", "hit"):
        body = client.post("/api/network/bundle", json=observation_request, headers=_SVC_HEADERS).json()
        assert body["_response_cache"]["state"] == "bypass"
        assert (body["health"]["_response_cache"]["state"], body["health"]["_response_cache"]["scope"]) == (
            expected,
            "self",
        )
        assert (body["quality"]["_response_cache"]["state"], body["transport"]["_response_cache"]["state"]) == (
            "bypass",
            "bypass",
        )
        assert body["health"]["shielding_analysis"]["_response_cache"]["state"] in {"miss", "hit"}


def test_dashboard_custom_fields_appear_in_top10(in_memory_duckdb, test_service_source):
    dashboard_repo._dashboard_cache.clear()
    f1 = {"name": "my_edge_field", "label": "My Edge Field", "vcl_log_expression": "randomint(1, 100)"}
    f1 |= {"collection_stage": "edge", "duckdb_type": "INTEGER", "value_type": "numeric"}
    f1 |= {"enabled": True, "show_in_dashboard": True}
    f2 = {"name": "hidden_field", "label": "Hidden", "vcl_log_expression": "req.http.X-Test"}
    f2 |= {"collection_stage": "edge", "duckdb_type": "VARCHAR", "value_type": "string"}
    f2 |= {"enabled": True, "show_in_dashboard": False}
    custom_source = {**test_service_source, "log_fields": {"schema_version": 2, "custom_fields": [f1, f2]}}

    table_name = _safe_table(test_service_source["name"])
    _seed_logs(in_memory_duckdb, test_service_source, 20)
    in_memory_duckdb.execute(f"ALTER TABLE {table_name} ADD COLUMN my_edge_field INTEGER")
    in_memory_duckdb.execute(f"ALTER TABLE {table_name} ADD COLUMN hidden_field VARCHAR")
    in_memory_duckdb.execute(f"UPDATE {table_name} SET my_edge_field = (random() * 100)::INTEGER + 1")

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: custom_source
    app.dependency_overrides[build_request_context] = override_request_context(
        source=custom_source, _con_override=in_memory_duckdb, path="/api/dashboard/aggregates"
    )
    try:
        with TestClient(app) as c:
            response = c.post("/api/dashboard/aggregates", headers=_SVC_HEADERS, json={"filters": {}})
    finally:
        app.dependency_overrides.pop(get_con, None)
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(build_request_context, None)

    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert "my_edge_field" in data and "hidden_field" not in data
    assert "top" in data["my_edge_field"] and "total" in data["my_edge_field"]


# ── Assets Router Tests ──
def test_assets_aggregates_post_get_and_populated_logs(client, in_memory_duckdb, test_service_source, monkeypatch):
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

    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    presets = (
        ("/images/banner.png", "HIT", "gzip", 3600, 45000),
        ("/bundle.js", "MISS", "br", 0, 120000),
        ("/api/v1/user", "PASS", "uncompressed", 0, 1000),
    )
    for i, log in enumerate(logs):
        url, cs, enc, ttl, rbb = presets[i % 3]
        log.update(
            timestamp="2026-08-19T12:30:00Z", url=url, cache_status=cs, resp_header_content_encoding=enc, ttl=ttl
        )
        log["resp_body_bytes"] = rbb

    in_memory_duckdb.execute(f"DROP VIEW IF EXISTS {table}")
    insert_mock_logs(in_memory_duckdb, table, logs)
    CacheRegistry.clear_all()

    pop_resp = client.post("/api/assets/aggregates", headers=_SVC_HEADERS, json=w_json)
    assert pop_resp.status_code == 200
    assert len(pop_resp.json()["asset_type_breakdown"]) > 0 and pop_resp.json()["missing_fields"] == []

    in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table}")
    in_memory_duckdb.execute(
        f"CREATE TABLE {table} (url VARCHAR, cache VARCHAR, resp_bytes BIGINT, status INTEGER, timestamp TIMESTAMPTZ)"
    )
    CacheRegistry.clear_all()
    miss_resp = client.post("/api/assets/aggregates", headers=_SVC_HEADERS, json=w_json)
    assert miss_resp.status_code == 200
    assert {"resp_header_content_encoding", "ttl"} <= set(miss_resp.json()["missing_fields"])


def test_assets_aggregates_high_scale():
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

    res2 = assets_aggregates(svc, req, "2026-08-19T12:00:30Z", "2026-08-19T13:00:45Z")  # type: ignore[arg-type]
    assert res2.is_cached is True and len(calls) == 2


# ── Merged from tests/routers/test_alerts_and_views.py ──

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SERVICE_ID = "test-service-id"

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

_VIEW_BODY = {
    "service_id": _SERVICE_ID,
    "name": "My View",
    "filters_json": '{"status": [500]}',
    "page": "dashboard",
}


def _alert_row(alert_id: str) -> dict | None:
    con = metadata_db.get_con(_SERVICE_ID)
    row = con.execute(
        "SELECT name, metric, threshold, enabled FROM alerts WHERE id = ?",
        (alert_id,),
    ).fetchone()
    return dict(row) if row else None


def _view_row(view_id: str) -> dict | None:
    con = metadata_db.get_con(_SERVICE_ID)
    row = con.execute(
        "SELECT name, filters_json, page FROM views WHERE id = ?",
        (view_id,),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Alerts: POST
# ---------------------------------------------------------------------------


def test_create_alert_persists_to_db(client):
    response = client.post("/api/alerts/", json=_ALERT_BODY, headers={"x-fastly-service-id": _SERVICE_ID})

    # 201 Created — resource POST convention (was 200 before the
    # 28/audit reclass; body shape unchanged).
    assert response.status_code == 201
    data = response.json()["data"]
    assert data["status"] == "success"
    alert_id = data["id"]
    assert alert_id

    row = _alert_row(alert_id)
    assert row is not None
    assert row["name"] == "High 5xx Rate"
    assert row["metric"] == "5xx_rate"
    assert row["threshold"] == pytest.approx(5.0)


def test_create_alert_returns_correct_fields(client):
    response = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "name": "Latency Alert", "metric": "p95_latency"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 201
    data = response.json()["data"]
    assert data["status"] == "success"
    assert data["id"]


def test_create_alert_invalid_metric_returns_422(client):
    response = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "metric": "nonexistent_metric"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 422


def test_create_alert_invalid_operator_returns_422(client):
    response = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "operator": "!="},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Alerts: PATCH enabled
# ---------------------------------------------------------------------------


def _seed_alert(client) -> str:
    resp = client.post("/api/alerts/", json=_ALERT_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    assert resp.status_code == 201
    return resp.json()["data"]["id"]


def test_toggle_alert_disabled(client):
    alert_id = _seed_alert(client)

    response = client.patch(
        f"/api/alerts/{alert_id}/enabled",
        json={"enabled": False},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )

    assert response.status_code == 200
    row = _alert_row(alert_id)
    assert row is not None
    assert row["enabled"] == 0


def test_toggle_alert_re_enabled(client):
    alert_id = _seed_alert(client)
    client.patch(
        f"/api/alerts/{alert_id}/enabled",
        json={"enabled": False},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    response = client.patch(
        f"/api/alerts/{alert_id}/enabled",
        json={"enabled": True},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )

    assert response.status_code == 200
    row = _alert_row(alert_id)
    assert row is not None
    assert row["enabled"] == 1


def test_toggle_alert_enabled_field_required(client):
    alert_id = _seed_alert(client)
    response = client.patch(
        f"/api/alerts/{alert_id}/enabled",
        json={},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Alerts: DELETE
# ---------------------------------------------------------------------------


def test_delete_alert_removes_from_db(client):
    alert_id = _seed_alert(client)

    response = client.delete(f"/api/alerts/{alert_id}", headers={"x-fastly-service-id": _SERVICE_ID})

    assert response.status_code == 204
    assert response.content == b""

    assert _alert_row(alert_id) is None


def test_delete_alert_idempotent(client):
    """Deleting a non-existent alert should still succeed (no-op)."""
    response = client.delete("/api/alerts/non-existent-id", headers={"x-fastly-service-id": _SERVICE_ID})
    assert response.status_code == 204


# ---------------------------------------------------------------------------
# Alerts: POST /preview
# ---------------------------------------------------------------------------
#
# ``preview_alert`` builds a time-bucketed query against the service's
# DuckDB-backed logs table and returns ``{times, values, type}``. The bucket
# granularity is picked from ``lookback_hours``; ``relative_*`` evaluation
# types also pull a historical series shifted by ``comparison_period_min``.


def _seed_preview_table(con, *, name: str = "logs_test_service", row_count: int = 10) -> None:
    """Create the table ``preview_alert`` will query, seeded with rows in
    the last 5 minutes so the default lookback covers them."""
    con.execute(
        f"CREATE TABLE {name} ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    rows = [
        (now - timedelta(seconds=i * 10), 200 if i % 2 == 0 else 500, i % 2 == 0, 50.0, 100, "HIT", 1024, 200, 100)
        for i in range(row_count)
    ]
    con.executemany(
        f"INSERT INTO {name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _preview_body(**overrides) -> dict:
    body = {
        "service_id": _SERVICE_ID,
        "name": "Preview Alert",
        "category": "reliability",
        "metric": "5xx",
        "evaluation_type": "absolute",
        "evaluation_scope": "all",
        "operator": ">",
        "threshold": 1.0,
        "window_min": 5.0,
    }
    body.update(overrides)
    return body


def test_preview_alert_returns_series_for_absolute_metric(client, in_memory_duckdb):
    """Happy path: a seeded logs table → preview returns aligned times/values."""
    _seed_preview_table(in_memory_duckdb)

    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=_preview_body())

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["type"] == "absolute"
    assert "times" in data and "values" in data
    assert len(data["times"]) == len(data["values"])
    # Some 5xx in the seeded data → at least one non-zero value
    assert sum(data["values"]) > 0


def test_preview_alert_unknown_service_returns_404(client):
    """No source → 404 (don't silently build a query against a missing table)."""
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        resp = client.post("/api/alerts/preview", json=_preview_body())
    assert resp.status_code == 404
    assert resp.json()["detail"] == {"error": "Service not found"}


def test_preview_alert_self_heals_stale_view(client, in_memory_duckdb):
    """View-lag self-heal: the preview window is anchored to ``(SELECT
    max(timestamp) FROM table)``, which is NULL when the cached Iceberg view
    SQL lags a fresh ingest → 0 preview rows even though data exists. When the
    first query returns nothing BUT the status cache reports data, force ONE
    view rebuild and re-run.

    Simulated by starting with an empty table and having the patched
    ``force_rebuild_view`` seed the rows (the rebuild is what makes the
    previously-stale view return data)."""
    table = "logs_test_service"
    # Empty table (schema only) → first query returns no rows.
    in_memory_duckdb.execute(
        f"CREATE TABLE {table} ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )

    rebuild_calls = {"n": 0}

    def fake_rebuild(con, src):
        # The rebuild makes the previously-empty view non-empty: insert rows
        # into the EXISTING table (don't re-CREATE it).
        rebuild_calls["n"] += 1
        now = datetime.now(UTC)
        rows = [
            (now - timedelta(seconds=i * 10), 200 if i % 2 == 0 else 500, i % 2 == 0, 50.0, 100, "HIT", 1024, 200, 100)
            for i in range(10)
        ]
        con.executemany(f"INSERT INTO {table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)

    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": _SERVICE_ID},
        ),
        # Status cache reports data exists (the fresh-ingest snapshot).
        patch("backend.config.get_status", return_value={"local_rows": 10, "latest_log_at": "2026-06-26T12:00:00Z"}),
        patch("backend.repositories._base.force_rebuild_view", side_effect=fake_rebuild),
    ):
        resp = client.post("/api/alerts/preview", json=_preview_body())

    assert resp.status_code == 200
    assert rebuild_calls["n"] == 1, f"preview self-heal must rebuild EXACTLY once; got {rebuild_calls['n']}"
    data = resp.json()["data"]
    # After the rebuild + re-run, the seeded rows are visible.
    assert sum(data["values"]) > 0


def test_preview_alert_no_self_heal_when_status_cache_empty(client, in_memory_duckdb):
    """A genuinely empty service (no status-cache row count) must NOT trigger
    a view rebuild on an empty preview — the false-positive guard."""
    table = "logs_test_service"
    in_memory_duckdb.execute(
        f"CREATE TABLE {table} ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )

    rebuild_calls = {"n": 0}

    def fake_rebuild(con, src):
        rebuild_calls["n"] += 1

    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": _SERVICE_ID},
        ),
        # Status cache reports NO data → not a stale-view symptom.
        patch("backend.config.get_status", return_value={}),
        patch("backend.repositories._base.force_rebuild_view", side_effect=fake_rebuild),
    ):
        resp = client.post("/api/alerts/preview", json=_preview_body())

    assert resp.status_code == 200
    assert rebuild_calls["n"] == 0, "preview self-heal must NOT fire when the status cache reports no data"
    assert sum(resp.json()["data"]["values"]) == 0


def test_preview_alert_relative_evaluation_returns_hist_values(client, in_memory_duckdb):
    """``relative_increase`` returns a parallel ``hist_values`` series shifted
    by ``comparison_period_min`` — needed to render the comparison overlay."""
    _seed_preview_table(in_memory_duckdb)

    body = _preview_body(
        evaluation_type="relative_increase",
        comparison_period_min=60,
        threshold=1.5,
    )
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview?lookback_hours=2", json=body)

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["type"] == "relative"
    assert "hist_values" in data
    assert len(data["hist_values"]) == len(data["values"])


def test_preview_alert_edge_scope_filters_to_edge_rows(client, in_memory_duckdb):
    """``evaluation_scope='edge'`` must only count rows where ``edge=true``."""
    in_memory_duckdb.execute(
        "CREATE TABLE logs_test_service ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    # 4 edge=false 500s, 0 edge=true 500s — scope=edge must report 0
    in_memory_duckdb.executemany(
        "INSERT INTO logs_test_service VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(now - timedelta(seconds=i), 500, False, 50.0, 100, "MISS", 1024, 200, 100) for i in range(4)],
    )

    body = _preview_body(evaluation_scope="edge")
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=body)

    assert resp.status_code == 200
    assert sum(resp.json()["data"]["values"]) == 0


def test_preview_alert_origin_scope_filters_to_origin_rows(client, in_memory_duckdb):
    """``evaluation_scope='origin'`` mirrors edge — must only count
    ``edge=false`` rows. Pinned because the WHERE-clause assembly is hand-
    rolled per scope; a typo would flip the meaning silently."""
    in_memory_duckdb.execute(
        "CREATE TABLE logs_test_service ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    in_memory_duckdb.executemany(
        "INSERT INTO logs_test_service VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        # All 500s on edge=true → origin-scoped count is 0
        [(now - timedelta(seconds=i), 500, True, 50.0, 100, "MISS", 1024, 200, 100) for i in range(3)],
    )

    body = _preview_body(evaluation_scope="origin")
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=body)

    assert resp.status_code == 200
    assert sum(resp.json()["data"]["values"]) == 0


def test_preview_alert_query_failure_returns_error_field(client, in_memory_duckdb):
    """If the generated SQL fails (e.g. table doesn't exist) the endpoint
    must return a 200 with ``error`` set — the UI displays the error
    inline rather than treating it as a transport failure."""
    # No table created — query will raise; preview should return error
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=_preview_body())

    assert resp.status_code == 200
    body = resp.json()
    assert body.get("data") is None
    assert body.get("error")  # populated with the exception message


@pytest.mark.parametrize(
    "lookback_hours,expected_interval",
    [
        (1, "1 minute"),  # <= 180 min → 1m buckets
        (12, "15 minutes"),  # <= 1440 min → 15m buckets
        (48, "1 hour"),  # > 1440 min → 1h buckets
    ],
)
def test_preview_alert_bucket_granularity_scales_with_lookback(
    client, in_memory_duckdb, lookback_hours, expected_interval
):
    """The bucket size cascade (1m / 15m / 1h) is purely lookback-driven;
    each branch is exercised so a refactor of the thresholds doesn't
    silently widen buckets on the short-window dashboards."""
    _seed_preview_table(in_memory_duckdb, row_count=5)
    captured: dict = {}

    from backend.utils import telemetry as _telemetry

    real_track = _telemetry.track_query

    def _spy(con, q, params, label):
        captured.setdefault("q", q)
        return real_track(con, q, params, label)

    # ``preview_alert`` imports ``track_query`` inside the function body,
    # so patching ``backend.utils.telemetry.track_query`` (the source
    # module) is what reaches the call site.
    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": _SERVICE_ID},
        ),
        patch("backend.utils.telemetry.track_query", _spy),
        patch("backend.repositories.assets.track_query", _spy),
    ):
        resp = client.post(f"/api/alerts/preview?lookback_hours={lookback_hours}", json=_preview_body())

    assert resp.status_code == 200
    assert expected_interval in captured["q"]


# ---------------------------------------------------------------------------
# Views: POST
# ---------------------------------------------------------------------------


def test_create_view_persists_to_db(client):
    response = client.post("/api/views/", json=_VIEW_BODY, headers={"x-fastly-service-id": _SERVICE_ID})

    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "success"
    view_id = data["id"]
    assert view_id

    row = _view_row(view_id)
    assert row is not None
    assert row["name"] == "My View"
    assert row["page"] == "dashboard"


def test_create_view_returns_id(client):
    response = client.post("/api/views/", json=_VIEW_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    assert response.status_code == 201
    assert response.json()["id"]


def test_create_view_missing_required_field_returns_422(client):
    response = client.post(
        "/api/views/",
        json={"service_id": "svc1"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Views: DELETE
# ---------------------------------------------------------------------------


def _seed_view(client) -> str:
    resp = client.post("/api/views/", json=_VIEW_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    assert resp.status_code == 201
    return resp.json()["id"]


def test_delete_view_removes_from_db(client):
    view_id = _seed_view(client)

    response = client.delete(f"/api/views/{view_id}", headers={"x-fastly-service-id": _SERVICE_ID})

    assert response.status_code == 204
    assert response.content == b""

    assert _view_row(view_id) is None


def test_delete_view_idempotent(client):
    """Deleting a non-existent view should still succeed (no-op)."""
    response = client.delete("/api/views/nonexistent-view-id", headers={"x-fastly-service-id": _SERVICE_ID})
    assert response.status_code == 204


# ---------------------------------------------------------------------------
# Round-trip: create then list
# ---------------------------------------------------------------------------


def test_list_alerts_after_create(client):
    client.post("/api/alerts/", json=_ALERT_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "name": "Second Alert"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )

    response = client.get(f"/api/alerts/{_SERVICE_ID}")

    assert response.status_code == 200
    data = response.json()["data"]
    names = [a["name"] for a in data]
    assert "High 5xx Rate" in names
    assert "Second Alert" in names


def test_list_views_after_create(client):
    client.post("/api/views/", json=_VIEW_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    client.post(
        "/api/views/",
        json={**_VIEW_BODY, "name": "Second View"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )

    response = client.get(f"/api/views/{_SERVICE_ID}")

    assert response.status_code == 200
    names = [v["name"] for v in response.json()]
    assert "My View" in names
    assert "Second View" in names


# ── limit query param (d180c4c bounded list payloads) ─────────────────────


def test_list_service_alerts_respects_limit_param(client):
    """d180c4c added limit: int = Query(500, ge=1, le=2000) to /api/alerts/
    to cap the unbounded JSON payload. Pinned because losing the slice
    (alerts = alerts[:limit]) would silently regress to the unbounded
    response — slow-growth resource exhaustion on the page-load path."""
    # Seed 3 alerts under the same service.
    for i in range(3):
        client.post(
            "/api/alerts/",
            json={**_ALERT_BODY, "name": f"Alert {i}"},
            headers={"x-fastly-service-id": _SERVICE_ID},
        )

    response = client.get(f"/api/alerts/{_SERVICE_ID}?limit=2")

    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) == 2


def test_list_service_alerts_rejects_limit_above_ceiling(client):
    """The le=2000 upper bound prevents callers from un-doing the cap by
    asking for limit=10_000. FastAPI's Query validator returns 422."""
    response = client.get(f"/api/alerts/{_SERVICE_ID}?limit=99999")
    assert response.status_code == 422


def test_list_service_alerts_rejects_limit_below_floor(client):
    """ge=1 floor blocks limit=0 (would return an empty payload but still
    do the full unbounded read + filter pass on the server)."""
    response = client.get(f"/api/alerts/{_SERVICE_ID}?limit=0")
    assert response.status_code == 422


def test_list_views_respects_limit_param(client):
    """Same bound applied to /api/views/{service_id}."""
    for i in range(3):
        client.post(
            "/api/views/",
            json={**_VIEW_BODY, "name": f"View {i}"},
            headers={"x-fastly-service-id": _SERVICE_ID},
        )

    response = client.get(f"/api/views/{_SERVICE_ID}?limit=2")

    assert response.status_code == 200
    assert len(response.json()) == 2


def test_list_all_alerts_admin(client):
    # Seed alerts under two different services
    client.post("/api/alerts/", json=_ALERT_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    other_service_id = "other-service-id"
    client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "service_id": other_service_id, "name": "Other Service Alert"},
        headers={"x-fastly-service-id": other_service_id},
    )

    with (
        patch("backend.routers.alerts._analyst_allowed_services", return_value=None),
        patch(
            "backend.repositories.alerts.svcconfig.list_configs",
            return_value=[{"service_id": _SERVICE_ID}, {"service_id": other_service_id}],
        ),
    ):
        response = client.get("/api/alerts/")

    assert response.status_code == 200
    data = response.json()["data"]
    names = [a["name"] for a in data]
    assert "High 5xx Rate" in names
    assert "Other Service Alert" in names


def test_list_all_alerts_analyst_filters_scope(client):
    # Seed alerts under two different services
    client.post("/api/alerts/", json=_ALERT_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    other_service_id = "other-service-id"
    client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "service_id": other_service_id, "name": "Other Service Alert"},
        headers={"x-fastly-service-id": other_service_id},
    )

    # Scoped only to _SERVICE_ID (not other_service_id)
    with (
        patch("backend.routers.alerts._analyst_allowed_services", return_value={_SERVICE_ID}),
        patch(
            "backend.repositories.alerts.svcconfig.list_configs",
            return_value=[{"service_id": _SERVICE_ID}, {"service_id": other_service_id}],
        ),
    ):
        response = client.get("/api/alerts/")

    assert response.status_code == 200
    data = response.json()["data"]
    names = [a["name"] for a in data]
    assert "High 5xx Rate" in names
    assert "Other Service Alert" not in names


def test_preview_alert_anomaly_zscore(client, in_memory_duckdb):
    _seed_preview_table(in_memory_duckdb)
    body = _preview_body(
        evaluation_type="anomaly_zscore",
        baseline_period_days=7,
        zscore_threshold=3.0,
    )
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=body)

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["type"] == "anomaly_zscore"
    assert "hist_values" in data
    assert "threshold_values" in data
    assert len(data["hist_values"]) == len(data["values"])
    assert len(data["threshold_values"]) == len(data["values"])


def test_toggle_alert_missing_service_id_returns_400(client):
    from backend.deps import get_service_id

    from backend.main import app

    app.dependency_overrides[get_service_id] = lambda: None
    try:
        response = client.patch(
            "/api/alerts/alert-id/enabled",
            json={"enabled": False},
            headers={"x-fastly-service-id": ""},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "service_id_required"
    finally:
        del app.dependency_overrides[get_service_id]


def test_toggle_alert_analyst_not_authorized_returns_403(client):
    # Seed alert under other_service_id
    other_service_id = "other-service-id"
    alert_resp = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "service_id": other_service_id, "name": "Other Service Alert"},
        headers={"x-fastly-service-id": other_service_id},
    )
    alert_id = alert_resp.json()["data"]["id"]

    from backend.deps import get_service_id

    from backend.main import app

    app.dependency_overrides[get_service_id] = lambda: other_service_id

    try:
        # Analyst scoped to _SERVICE_ID trying to toggle other_service_id's alert
        with (
            patch("backend.routers.alerts._analyst_allowed_services", return_value={_SERVICE_ID}),
            patch("backend.utils.auth.analyst_allowed_services", return_value={_SERVICE_ID}),
            patch(
                "backend.repositories.alerts.svcconfig.list_configs",
                return_value=[{"service_id": _SERVICE_ID}, {"service_id": other_service_id}],
            ),
        ):
            response = client.patch(
                f"/api/alerts/{alert_id}/enabled",
                json={"enabled": False},
            )
        assert response.status_code == 403
        assert response.json()["detail"]["error"] == "service_not_authorized"
    finally:
        del app.dependency_overrides[get_service_id]


def test_delete_alert_missing_service_id_returns_400(client):
    from backend.deps import get_service_id

    from backend.main import app

    app.dependency_overrides[get_service_id] = lambda: None
    try:
        response = client.delete(
            "/api/alerts/alert-id",
            headers={"x-fastly-service-id": ""},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "service_id_required"
    finally:
        del app.dependency_overrides[get_service_id]


def test_delete_alert_analyst_not_authorized_returns_403(client):
    # Seed alert under other_service_id
    other_service_id = "other-service-id"
    alert_resp = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "service_id": other_service_id, "name": "Other Service Alert"},
        headers={"x-fastly-service-id": other_service_id},
    )
    alert_id = alert_resp.json()["data"]["id"]

    from backend.deps import get_service_id

    from backend.main import app

    app.dependency_overrides[get_service_id] = lambda: other_service_id

    try:
        # Analyst scoped to _SERVICE_ID trying to delete other_service_id's alert
        with (
            patch("backend.routers.alerts._analyst_allowed_services", return_value={_SERVICE_ID}),
            patch("backend.utils.auth.analyst_allowed_services", return_value={_SERVICE_ID}),
            patch(
                "backend.repositories.alerts.svcconfig.list_configs",
                return_value=[{"service_id": _SERVICE_ID}, {"service_id": other_service_id}],
            ),
        ):
            response = client.delete(f"/api/alerts/{alert_id}")
        assert response.status_code == 403
        assert response.json()["detail"]["error"] == "service_not_authorized"
    finally:
        del app.dependency_overrides[get_service_id]


def test_status_cache_has_data_handles_exception(client, in_memory_duckdb):
    _seed_preview_table(in_memory_duckdb)
    body = _preview_body(evaluation_type="absolute")
    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": _SERVICE_ID},
        ),
        patch("backend.config.get_status", side_effect=Exception("mocked status exception")),
    ):
        resp = client.post("/api/alerts/preview", json=body)
    assert resp.status_code == 200


def test_delete_view_missing_service_id_returns_400(client):
    from backend.deps import get_service_id

    from backend.main import app

    app.dependency_overrides[get_service_id] = lambda: None
    try:
        response = client.delete(
            "/api/views/view-id",
            headers={"x-fastly-service-id": ""},
        )
        assert response.status_code == 400
        assert response.json()["detail"]["error"] == "service_id_required"
    finally:
        del app.dependency_overrides[get_service_id]


def test_delete_view_analyst_not_authorized_returns_403(client):
    # Seed a view under other_service_id
    other_service_id = "other-service-id"
    view_body = {
        **_VIEW_BODY,
        "service_id": other_service_id,
        "name": "Other Service View",
    }
    view_resp = client.post(
        "/api/views/",
        json=view_body,
        headers={"x-fastly-service-id": other_service_id},
    )
    assert view_resp.status_code == 201
    view_id = view_resp.json()["id"]

    from backend.deps import get_service_id

    from backend.main import app

    app.dependency_overrides[get_service_id] = lambda: other_service_id

    try:
        # Analyst scoped only to _SERVICE_ID trying to delete other_service_id's view
        with (
            patch("backend.utils.auth.analyst_allowed_services", return_value={_SERVICE_ID}),
        ):
            response = client.delete(f"/api/views/{view_id}")
        assert response.status_code == 403
        assert response.json()["detail"]["error"] == "service_not_authorized"
    finally:
        del app.dependency_overrides[get_service_id]


# ---------------------------------------------------------------------------
# Alerts: PUT /api/alerts/{alert_id}
# ---------------------------------------------------------------------------


def test_update_alert_persists_changes(client):
    alert_id = _seed_alert(client)

    updated_body = {
        **_ALERT_BODY,
        "name": "Updated 5xx Rate Rule",
        "threshold": 12.5,
        "window_min": 15.0,
        "channels": [{"type": "slack", "url": "https://hooks.slack.com/services/T00/B00/X00"}],
    }
    response = client.put(
        f"/api/alerts/{alert_id}",
        json=updated_body,
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["status"] == "success"
    assert data["id"] == alert_id

    row = _alert_row(alert_id)
    assert row is not None
    assert row["name"] == "Updated 5xx Rate Rule"
    assert row["threshold"] == pytest.approx(12.5)


# ---------------------------------------------------------------------------
# Alerts: Status Filtering (?status=all|alerting|ok|muted)
# ---------------------------------------------------------------------------


def test_list_alerts_status_filtering(client):
    # 1. Healthy enabled alert (never triggered -> 'ok')
    r1 = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "name": "Healthy Rule", "enabled": True},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert r1.status_code == 201

    # 2. Muted alert (enabled=False -> 'muted')
    r2 = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "name": "Muted Rule", "enabled": False},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert r2.status_code == 201

    # 3. Alerting rule (recently triggered within 2x window -> 'alerting')
    now_iso = datetime.now(UTC).isoformat()
    r3 = client.post(
        "/api/alerts/",
        json={
            **_ALERT_BODY,
            "name": "Firing Rule",
            "enabled": True,
            "window_min": 5.0,
        },
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert r3.status_code == 201
    metadata_db.update_alert_last_triggered(_SERVICE_ID, r3.json()["data"]["id"], now_iso)

    resp_all = client.get(f"/api/alerts/{_SERVICE_ID}?status=all")
    assert resp_all.status_code == 200
    assert {a["name"] for a in resp_all.json()["data"]} == {"Healthy Rule", "Muted Rule", "Firing Rule"}

    resp_alerting = client.get(f"/api/alerts/{_SERVICE_ID}?status=alerting")
    assert resp_alerting.status_code == 200
    assert [a["name"] for a in resp_alerting.json()["data"]] == ["Firing Rule"]

    resp_ok = client.get(f"/api/alerts/{_SERVICE_ID}?status=ok")
    assert resp_ok.status_code == 200
    assert [a["name"] for a in resp_ok.json()["data"]] == ["Healthy Rule"]

    resp_muted = client.get(f"/api/alerts/{_SERVICE_ID}?status=muted")
    assert resp_muted.status_code == 200
    assert [a["name"] for a in resp_muted.json()["data"]] == ["Muted Rule"]


# ---------------------------------------------------------------------------
# Alerts: POST /api/alerts/evaluate
# ---------------------------------------------------------------------------


def test_evaluate_alerts_endpoint(client, in_memory_duckdb):
    _seed_preview_table(in_memory_duckdb, row_count=10)
    # Create an enabled rule on 5xx > 1.0 (seeded table has 5 rows with status=500)
    create_resp = client.post(
        "/api/alerts/",
        json={
            **_ALERT_BODY,
            "name": "Immediate 5xx Check",
            "metric": "5xx",
            "operator": ">",
            "threshold": 1.0,
            "window_min": 5.0,
            "enabled": True,
        },
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert create_resp.status_code == 201

    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        eval_resp = client.post(
            "/api/alerts/evaluate",
            json={"service_id": _SERVICE_ID},
            headers={"x-fastly-service-id": _SERVICE_ID},
        )

    assert eval_resp.status_code == 200
    payload = eval_resp.json()["data"]
    assert payload["service_id"] == _SERVICE_ID
    assert payload["evaluated"] >= 1
    assert payload["triggered"] >= 1
    assert any(r["name"] == "Immediate 5xx Check" and r["triggered"] is True for r in payload["results"])


# ---------------------------------------------------------------------------
# Alerts: Webhook SSRF Validation & POST /api/alerts/{alert_id}/test
# ---------------------------------------------------------------------------


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
    resp = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "webhook_url": unsafe_url},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "invalid_webhook_url"


def test_test_alert_webhook_dry_run_and_dispatch(client):
    resp = client.post(
        "/api/alerts/",
        json={
            **_ALERT_BODY,
            "name": "Webhook Rule",
            "channels": [{"type": "slack", "url": "https://hooks.slack.com/services/T00/B00/X00"}],
        },
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert resp.status_code == 201
    alert_id = resp.json()["data"]["id"]

    # Dry run validates URLs without external network call
    dry_resp = client.post(
        f"/api/alerts/{alert_id}/test",
        json={"service_id": _SERVICE_ID, "dry_run": True},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert dry_resp.status_code == 200
    dry_data = dry_resp.json()["data"]
    assert dry_data["alert_id"] == alert_id
    assert dry_data["status"] == "validated"
    assert dry_data["targets_tested"] == 1
    assert dry_data["channel_type"] == "slack"

    # Service-scoped alias with mocked _post_slack_notification
    with patch("backend.cron.jobs.metadata._post_slack_notification") as mock_post:
        live_resp = client.post(
            f"/api/alerts/{_SERVICE_ID}/{alert_id}/test",
            json={"dry_run": False},
            headers={"x-fastly-service-id": _SERVICE_ID},
        )
    assert live_resp.status_code == 200
    assert live_resp.json()["data"]["status"] == "sent"
    assert live_resp.json()["data"]["targets_tested"] == 1
    assert mock_post.call_count == 1


# ---------------------------------------------------------------------------
# Alerts: Analyst Path B 403 admin_only Enforcement on In-Scope Mutations
# ---------------------------------------------------------------------------


def test_analyst_in_scope_mutations_return_403_admin_only(client):
    alert_id = _seed_alert(client)

    with patch("backend.routers.alerts._analyst_allowed_services", return_value={_SERVICE_ID}):
        # POST /api/alerts/
        r_create = client.post(
            "/api/alerts/",
            json=_ALERT_BODY,
            headers={"x-fastly-service-id": _SERVICE_ID},
        )
        assert r_create.status_code == 403
        assert r_create.json()["detail"]["error"] == "admin_only"

        # PUT /api/alerts/{alert_id}
        r_update = client.put(
            f"/api/alerts/{alert_id}",
            json=_ALERT_BODY,
            headers={"x-fastly-service-id": _SERVICE_ID},
        )
        assert r_update.status_code == 403
        assert r_update.json()["detail"]["error"] == "admin_only"

        # PATCH /api/alerts/{alert_id}/enabled
        r_toggle = client.patch(
            f"/api/alerts/{alert_id}/enabled",
            json={"enabled": False},
            headers={"x-fastly-service-id": _SERVICE_ID},
        )
        assert r_toggle.status_code == 403
        assert r_toggle.json()["detail"]["error"] == "admin_only"

        # DELETE /api/alerts/{alert_id}
        r_delete = client.delete(
            f"/api/alerts/{alert_id}",
            headers={"x-fastly-service-id": _SERVICE_ID},
        )
        assert r_delete.status_code == 403
        assert r_delete.json()["detail"]["error"] == "admin_only"

        # POST /api/alerts/evaluate
        r_eval = client.post(
            "/api/alerts/evaluate",
            json={"service_id": _SERVICE_ID},
            headers={"x-fastly-service-id": _SERVICE_ID},
        )
        assert r_eval.status_code == 403
        assert r_eval.json()["detail"]["error"] == "admin_only"

        # POST /api/alerts/{alert_id}/test
        r_test = client.post(
            f"/api/alerts/{alert_id}/test",
            json={"service_id": _SERVICE_ID, "dry_run": True},
            headers={"x-fastly-service-id": _SERVICE_ID},
        )
        assert r_test.status_code == 403
        assert r_test.json()["detail"]["error"] == "admin_only"
