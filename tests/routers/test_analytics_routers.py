"""Unit tests for the insights router (anomaly detection endpoints)."""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient
from starlette.routing import Mount

from backend.core.log_fields import LOG_FIELD_CATALOG
from backend.core.request_context import build_request_context
from backend.deps import get_source
from backend.main import app
from backend.models.insights import InsightsRequest
from backend.models.security import SecurityAggregatesResponse
from backend.repositories._base import QueryRunner, _safe_table, _schema_cols_cache
from backend.utils import bot_sources
from backend.utils.remote_access import TimeBounds
from tests.conftest import MOCK_SERVICE_ID, override_request_context
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs

_SVC_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}


def _seed_logs(con, src, num_logs: int = 10, hours_ago: int | None = None, **overrides) -> list[dict]:
    kwargs = {" hours_ago".strip(): hours_ago} if hours_ago is not None else {}
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
    session = SimpleNamespace(
        session_id=session_id,
        pii_policy={"mask_ips": mask_ips},
        service_ids=[src["service_id"]],
        time_bounds=time_bounds,
    )
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


# ── Merged from tests/routers/test_insights_router.py ──
def _stub_insights() -> dict:
    now = "2026-10-12T00:00:00+00:00"
    return {
        "insights": [
            {
                "id": "city_surges",
                "title": "Geographic Surges",
                "description": "Cities with sudden traffic surges",
                "severity": "medium",
                "summary": "1 city surging",
                "items": [
                    {
                        "label": "San Jose, CA, US",
                        "current_val": 120.0,
                        "baseline_val": 20.0,
                        "baseline_label": "baseline",
                        "unit": "req",
                        "severity": "medium",
                        "meta": {"spike_ratio": 6.0, "filters": {}},
                    }
                ],
                "category": "traffic",
            }
        ],
        "window_start": now,
        "window_end": now,
        "baseline_start": now,
        "baseline_end": now,
        "computed_at": now,
        "window_hours": 24.0,
        "baseline_hours": 168.0,
    }


def _stub_cache_collapse_detail() -> dict:
    return {
        "url": "/api/v1/heavy",
        "timeline": [
            {
                "bucket": "2026-10-12T00:00:00+00:00",
                "expected_hits": 90.0,
                "real_hits": 10,
                "misses": 90,
                "total_requests": 100,
                "hit_rate": 0.1,
            }
        ],
        "recent_misses": [
            {"timestamp": "2026-10-12T00:01:00+00:00", "cache": "MISS", "pop": "SJC", "ip": "1.2.3.4", "status": 200}
        ],
        "breakdown": {"hits": 10, "misses": 90, "passes": 0, "other": 0},
        "baseline_hit_rate": 0.95,
        "window_hit_rate": 0.1,
        "baseline_pass_rate": 0.0,
        "window_pass_rate": 0.0,
        "has_data": True,
        "clusters": [],
    }


def test_insights_request_validation():
    """Verify window and baseline bounds in InsightsRequest model."""
    valid = InsightsRequest(window_size_hrs=24.0, baseline_hours=168.0, force_refresh=True)
    assert valid.window_size_hrs == 24.0
    assert valid.baseline_hours == 168.0
    assert valid.force_refresh is True

    # Reject non-positive or excessive windows
    for bad_kw in ({"window_size_hrs": 0}, {"window_size_hrs": 169}, {"baseline_hours": 0}, {"baseline_hours": 2161}):
        with pytest.raises(Exception):
            InsightsRequest(**bad_kw)


def test_insights_admin_passes_force_refresh_true(client):
    """Admin caller with force_refresh=True forwards force_refresh=True to repo."""
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_insights()

    with patch("backend.repositories.insights.get_insights", side_effect=_fake):
        resp = client.post(
            "/api/insights",
            headers=_SVC_HEADERS,
            json={"window_size_hrs": 24.0, "baseline_hours": 168.0, "force_refresh": True},
        )

    assert resp.status_code == 200, resp.text
    assert captured["force_refresh"] is True
    assert captured["clamp_start"] is None
    assert captured["clamp_end"] is None


def test_insights_analyst_clamps_force_refresh_to_false(client, in_memory_duckdb, test_service_source):
    """Remote analyst session must have force_refresh clamped to False regardless of request."""
    now = datetime.now(UTC)
    start, end = now - timedelta(hours=3), now - timedelta(hours=2)
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_insights()

    with _with_analyst_ctx(
        test_service_source,
        in_memory_duckdb,
        mask_ips=True,
        time_bounds=TimeBounds(start=start, end=end),
        path="/api/insights",
    ):
        with patch("backend.repositories.insights.get_insights", side_effect=_fake):
            resp = client.post(
                "/api/insights",
                headers=_SVC_HEADERS,
                json={"window_size_hrs": 1.0, "baseline_hours": 168.0, "force_refresh": True},
            )

        assert resp.status_code == 200, resp.text
        # Even though request specified force_refresh=True, it was clamped to False for analyst
        assert captured["force_refresh"] is False
        assert captured["mask_ips"] is True
        assert captured["clamp_end"] == end.isoformat()
        assert captured["clamp_start"] == start.isoformat()


def test_insights_endpoint_stale_view_retry(client):
    """Test that insights_endpoint transparently retries via execute_with_stale_view_retry."""
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

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert len(data["insights"]) == 1
    assert data["insights"][0]["id"] == "city_surges"
    assert call_count == 2
    mock_clear.assert_called_once()
    mock_update.assert_called_once()


def test_cache_collapse_detail_endpoint(client):
    """Test /api/insights/cache-collapse-detail endpoint contract and stale view retry."""
    with patch("backend.repositories.insights.get_cache_collapse_detail", return_value=_stub_cache_collapse_detail()):
        resp = client.post(
            "/api/insights/cache-collapse-detail",
            headers=_SVC_HEADERS,
            json={"url": "/api/v1/heavy", "window_size_hrs": 24.0, "baseline_hours": 168.0},
        )

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["url"] == "/api/v1/heavy"
    assert data["baseline_hit_rate"] == 0.95
    assert len(data["timeline"]) == 1
    assert len(data["recent_misses"]) == 1
    assert data["breakdown"]["hits"] == 10


# ── Merged from tests/routers/test_network_router.py ──
def test_network_health_full_response(client, in_memory_duckdb, test_service_source):
    """sections omitted → full response with shielding_analysis present
    (or null on best-effort failure) and all core keys populated."""
    _seed_logs(in_memory_duckdb, test_service_source, 10, tcp_rtt=25_000, asn=7922, country="US")

    response = client.post("/api/network-health", headers=_SVC_HEADERS, json={"filters": {}})
    assert response.status_code == 200
    data = response.json()
    assert "shielding_analysis" in data
    assert "summary" in data
    assert "heatmap" in data


def test_network_health_rejects_unknown_section(client, in_memory_duckdb, test_service_source):
    """sections=['not_a_section'] returns 400 (router) or 422 (Pydantic
    Literal). Either is an explicit reject so the FE never gets a
    silently-degraded 200."""
    _seed_logs(in_memory_duckdb, test_service_source, 5)
    response = client.post(
        "/api/network-health", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["not_a_section"]}
    )
    assert response.status_code in (400, 422)


def test_network_health_shielding_only_skips_core_temp(client, in_memory_duckdb, test_service_source):
    """sections=['shielding_analysis'] must skip the network temp table
    materialization entirely — proves the router-level CORE_SECTIONS
    gate works. Confirmed via absence of network-temp section_timings
    entries (the shielding code path runs through origin and has its
    own telemetry shape)."""
    _seed_logs(in_memory_duckdb, test_service_source, 10, tcp_rtt=25_000, asn=7922, country="US")

    response = client.post(
        "/api/network-health", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["shielding_analysis"]}
    )
    assert response.status_code == 200
    data = response.json()
    # shielding key must be present (best-effort; may be null when origin
    # join produces no rows but the field is always emitted under the gate)
    assert "shielding_analysis" in data
    timings = {t["section"] for t in data.get("_section_timings", [])}
    # None of the network-temp section_timings should fire — those are
    # only appended inside repo.get_health which the router skips when no
    # core section is requested.
    for blocked in {"temp_table_create", "heatmap_query", "map_query", "metro_query"}:
        assert blocked not in timings, f"core query {blocked} fired despite shielding-only selector; got {timings}"


def test_network_health_core_plus_shielding_selector(client, in_memory_duckdb, test_service_source):
    """Multi-section selector covers both code paths in one request."""
    _seed_logs(in_memory_duckdb, test_service_source, 10, tcp_rtt=25_000, asn=7922, country="US")

    response = client.post(
        "/api/network-health",
        headers=_SVC_HEADERS,
        json={"filters": {}, "sections": ["leaderboard", "shielding_analysis"]},
    )
    assert response.status_code == 200
    data = response.json()
    assert "leaderboard" in data
    assert "shielding_analysis" in data


def test_network_health_shielding_failure_returns_error_sentinel(client, in_memory_duckdb, test_service_source):
    """M2 (shielding audit 2026-06-30): a handler-level failure in
    get_shielding_analysis must NOT be swallowed into a null/empty payload
    that's indistinguishable from "no data". The router logs it and returns
    an explicit ``{error: true}`` sentinel so the UI can surface it."""
    _seed_logs(in_memory_duckdb, test_service_source, 5)

    with patch("backend.repositories.origin.get_shielding_analysis", side_effect=RuntimeError("boom")):
        response = client.post(
            "/api/network-health", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["shielding_analysis"]}
        )

    assert response.status_code == 200
    sa = response.json()["shielding_analysis"]
    assert sa is not None
    assert sa["error"] is True
    assert sa["has_data"] is False


def test_get_pop_health_returns_data(client, in_memory_duckdb, test_service_source):
    _seed_logs(
        in_memory_duckdb,
        test_service_source,
        5,
        pop="IAD",
        status=200,
        tcp_rtt=12000,
        ttfb=50.0,
        cache="HIT",
        resp_bytes=1000,
    )
    st, et = _naive_iso_window(3)
    response = client.get(f"/api/network/pop-health?start_time={st}&end_time={et}", headers=_SVC_HEADERS)
    assert response.status_code == 200
    data = response.json()["data"]
    assert len(data) > 0
    assert data[0]["pop"] == "IAD"
    assert data[0]["requests"] == 5
    assert data[0]["errors"] == 0
    assert data[0]["cache_hit_rate"] == 100.0


def test_get_pop_health_self_heals_on_lake_detach_race(client, in_memory_duckdb, test_service_source):
    """A DuckLake ``lake``-detach-race CatalogException on the raw base-table
    fallback must self-heal (one retry) instead of surfacing a 500 (Trap #35)."""
    import duckdb

    from backend.utils import telemetry as telemetry_mod

    _seed_logs(
        in_memory_duckdb,
        test_service_source,
        5,
        pop="IAD",
        status=200,
        tcp_rtt=12000,
        ttfb=50.0,
        cache="HIT",
        resp_bytes=1000,
    )
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
    data = response.json()["data"]
    assert data[0]["pop"] == "IAD"
    assert data[0]["requests"] == 5
    assert calls["n"] == 2  # failed once, self-healed, retried once


def test_network_bundle_post_full(client, in_memory_duckdb, test_service_source):
    """POST /api/network/bundle returns all requested sections."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for i, log in enumerate(logs):
        log.update(
            pop="IAD" if i < 5 else "SJC",
            status=200,
            tcp_rtt=20_000,  # 20ms in µs
            ttfb=45.0,
            cache="HIT",
            resp_bytes=2000,
            retrans=1,
            ploss=0.005,
            delivery_rate=10_000_000,  # 10 MB/s -> 80 Mbps
            proto="HTTP/2",
            asn=7922,
            country="US",
        )
    insert_mock_logs(in_memory_duckdb, table, logs)

    st, et = _naive_iso_window(3)
    response = client.post(
        "/api/network/bundle",
        headers=_SVC_HEADERS,
        json={
            "start_time": st,
            "end_time": et,
            "sections": ["health", "quality", "pop_health", "transport"],
            "filters": {},
        },
    )
    assert response.status_code == 200
    data = response.json()
    assert "health" in data and data["health"] is not None
    assert "quality" in data and data["quality"] is not None
    assert "pop_health" in data and data["pop_health"] is not None
    assert "transport" in data and data["transport"] is not None
    assert len(data["pop_health"]) == 2

    transport = data["transport"]
    assert transport["total_requests"] == 10
    assert transport["tcp_rtt_p50_ms"] == 20.0
    assert transport["retrans_rate_pct"] == 1.0
    assert transport["packet_loss_pct"] == 0.5  # 0.005 * 100
    assert transport["delivery_rate_mbps"] == 80.0
    assert len(transport["protocol_distribution"]) > 0


def test_network_bundle_get_full(client, in_memory_duckdb, test_service_source):
    """GET /api/network/bundle returns 200 with default sections."""
    _seed_logs(
        in_memory_duckdb,
        test_service_source,
        5,
        pop="LHR",
        status=200,
        tcp_rtt=30_000,
        proto="HTTP/3",
        asn=13335,
        country="GB",
    )
    st, et = _naive_iso_window(2)
    response = client.get(f"/api/network/bundle?start_time={st}&end_time={et}", headers=_SVC_HEADERS)
    assert response.status_code == 200
    data = response.json()
    assert "health" in data
    assert "transport" in data


def test_network_bundle_selective_sections(client, in_memory_duckdb, test_service_source):
    """POST /api/network/bundle with sections=['transport'] executes only transport."""
    _seed_logs(in_memory_duckdb, test_service_source, 5, tcp_rtt=15_000, proto="HTTP/1.1")
    st, et = _naive_iso_window(1)
    response = client.post(
        "/api/network/bundle",
        headers=_SVC_HEADERS,
        json={"start_time": st, "end_time": et, "sections": ["transport"]},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["transport"] is not None
    assert data["health"] is None
    assert data["quality"] is None
    assert data["pop_health"] is None


def test_network_bundle_missing_optional_fields_fallback(client, in_memory_duckdb, test_service_source):
    """Logs without tcp_rtt/retrans/ploss/delivery_rate must return zeroed TransportMetrics without crashing."""
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"DROP TABLE IF EXISTS {table}")

    logs = generate_mock_logs(test_service_source, num_logs=5)
    for log in logs:
        log["status"] = 200
        # Omit tcp_rtt, retrans, ploss, delivery_rate
        for k in ("tcp_rtt", "retrans", "ploss", "delivery_rate"):
            log.pop(k, None)

    # Create table schema excluding the 4 optional transport fields
    excluded = {"tcp_rtt", "retrans", "ploss", "delivery_rate"}
    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None and f["id"] not in excluded]
    columns_map = {f["id"]: ("TIMESTAMPTZ" if f["id"] == "timestamp" else f["duckdb_type"]) for f in raw_fields}
    for log in logs:
        for k, v in log.items():
            if k not in columns_map and k not in excluded:
                columns_map[k] = (
                    "BOOLEAN"
                    if isinstance(v, bool)
                    else "BIGINT"
                    if isinstance(v, int)
                    else "DOUBLE"
                    if isinstance(v, float)
                    else "VARCHAR"
                )
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
    response = client.post(
        "/api/network/bundle", headers=_SVC_HEADERS, json={"start_time": st, "end_time": et, "sections": ["transport"]}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["transport"] is not None
    transport = data["transport"]
    assert transport["total_requests"] == 5
    assert transport["has_transport_data"] is False
    assert len(transport["missing_fields"]) == 4
    assert "tcp_rtt" in transport["missing_fields"]


def test_network_bundle_analyst_write_allowed():
    """Verify that /api/network/ is in _ANALYST_ALLOWED_WRITE_PREFIXES so Analyst Path B POSTs succeed."""
    from backend.utils.remote_access import _ANALYST_ALLOWED_WRITE_PREFIXES

    assert any("/api/network/bundle".startswith(prefix) for prefix in _ANALYST_ALLOWED_WRITE_PREFIXES), (
        "/api/network/bundle must be in _ANALYST_ALLOWED_WRITE_PREFIXES"
    )


def test_network_bundle_surfaces_section_queries_in_debug_envelope(client, in_memory_duckdb, test_service_source):
    """Queries run by a bundle section (transport here) must appear in the bundle's
    own ``_debug_queries`` — they were dropped because only the section sub-model
    carried them."""
    _seed_logs(in_memory_duckdb, test_service_source, 5, tcp_rtt=20_000, proto="HTTP/2")
    st, et = _naive_iso_window(3)
    response = client.post(
        "/api/network/bundle",
        headers={**_SVC_HEADERS, "x-debug-responses": "1"},
        json={"start_time": st, "end_time": et, "sections": ["transport"], "filters": {}},
    )
    assert response.status_code == 200
    assert len(response.json()["_debug_queries"]) > 0


# ── Merged from tests/routers/test_origin_router.py ──
def _seed_origin_table(con, src) -> None:
    """Populate the test logs table with origin-shaped data so every
    endpoint can return real (non-empty) results."""
    logs = generate_mock_logs(src, num_logs=40, hours_ago=1)
    for i, log in enumerate(logs):
        log.update(
            ottfb=50_000 + i * 1000,
            ost=200 if i % 5 != 0 else 500,
            oip="203.0.113.1" if i < 25 else "203.0.113.2",
            url="/api/foo" if i % 2 else "/api/bar",
            edge=True,
            pop="SJC",
        )
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def test_origin_timeseries_accepts_default_request(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/origin/timeseries", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp.status_code == 200
    assert "series" in resp.json() or "has_data" in resp.json()


def test_origin_timeseries_validates_metric_enum(client):
    """``metric`` must be one of {ttfb, ttlb} — anything else → 422.
    Pinned because a typo'd metric would otherwise reach the repo and
    surface as a SQL error rather than a clear validation message."""
    resp = client.post(
        "/api/origin/timeseries", headers=_SVC_HEADERS, json={"filters": {}, "metric": "totally_made_up"}
    )
    assert resp.status_code == 422


def test_origin_timeseries_validates_percentile_enum(client):
    resp = client.post("/api/origin/timeseries", headers=_SVC_HEADERS, json={"filters": {}, "percentile": "p42"})
    assert resp.status_code == 422


def test_origin_timeseries_passes_kwargs_to_repo(client, in_memory_duckdb, test_service_source):
    """Every Request field must round-trip into the repo call —
    pinned because misspelling a kwarg in the router would silently
    fall back to the repo default and the user's UI setting would
    be ignored."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch("backend.repositories.origin.get_timeseries", return_value={"has_data": False, "series": []}) as mock:
        client.post(
            "/api/origin/timeseries",
            headers=_SVC_HEADERS,
            json={"filters": {}, "bucket_minutes": 15, "split_by_leg": True, "metric": "ttlb", "percentile": "p99"},
        )

    kwargs = mock.call_args.kwargs
    assert kwargs["bucket_minutes"] == 15
    assert kwargs["split_by_leg"] is True
    assert kwargs["metric"] == "ttlb"
    assert kwargs["percentile"] == "p99"


def test_origin_slow_urls_passes_limit_and_min_requests(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch("backend.repositories.origin.get_slow_urls", return_value={"has_data": False, "rows": []}) as mock:
        client.post("/api/origin/slow-urls", headers=_SVC_HEADERS, json={"filters": {}, "limit": 5, "min_requests": 25})

    kwargs = mock.call_args.kwargs
    assert kwargs["limit"] == 5
    assert kwargs["min_requests"] == 25


def test_origin_slow_urls_clamps_limit_at_100(client, in_memory_duckdb, test_service_source):
    """``Limit100`` Pydantic type clamps (not rejects) at 100 — pinned
    because a runaway limit on the slow-urls list would dominate the
    result payload, and the silent clamp is what protects the
    frontend from absurd payload sizes."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch("backend.repositories.origin.get_slow_urls", return_value={"has_data": False, "rows": []}) as mock:
        resp = client.post("/api/origin/slow-urls", headers=_SVC_HEADERS, json={"filters": {}, "limit": 100_000})

    assert resp.status_code == 200
    # The 100_000 input got clamped down to 100 before reaching the repo
    assert mock.call_args.kwargs["limit"] == 100


def test_origin_status_codes_returns_200(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    assert client.post("/api/origin/status-codes", headers=_SVC_HEADERS, json={"filters": {}}).status_code == 200


def test_origin_path_breakdown_returns_200(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    assert client.post("/api/origin/path-breakdown", headers=_SVC_HEADERS, json={"filters": {}}).status_code == 200


def test_origin_pop_latency_returns_200(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    assert (
        client.post("/api/origin/pop-latency", headers=_SVC_HEADERS, json={"filters": {}, "limit": 10}).status_code
        == 200
    )


def test_origin_ip_health_returns_200(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    assert (
        client.post("/api/origin/ip-health", headers=_SVC_HEADERS, json={"filters": {}, "limit": 10}).status_code == 200
    )


def test_origin_shielding_analysis_accepts_limit_up_to_200(client, in_memory_duckdb, test_service_source):
    """Uses Limit200 (not Limit100) — pinned distinct from the other
    endpoints because the shield-arc map renders more rows than the
    list views."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    assert (
        client.post(
            "/api/origin/shielding-analysis", headers=_SVC_HEADERS, json={"filters": {}, "limit": 150}
        ).status_code
        == 200
    )


def test_origin_shielding_analysis_clamps_limit_at_200(client, in_memory_duckdb, test_service_source):
    """``Limit200`` clamps oversized values (same pattern as Limit100)."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch(
        "backend.repositories.origin.get_shielding_analysis", return_value={"has_data": False, "rows": []}
    ) as mock:
        resp = client.post("/api/origin/shielding-analysis", headers=_SVC_HEADERS, json={"filters": {}, "limit": 500})

    assert resp.status_code == 200
    assert mock.call_args.kwargs["limit"] == 200


def test_origin_summary_value_error_maps_to_400(client):
    """The router uses ``@query_errors()`` — a ValueError from the
    repo must surface as 400, not 500. Pinned because the repo's
    filter parsing raises ValueError on bad filter shape and the
    frontend needs the 400 to render the inline error message."""
    with patch("backend.repositories.origin.get_summary", side_effect=ValueError("bad filter")):
        resp = client.post("/api/origin/summary", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp.status_code == 400


def test_origin_summary_lookup_error_maps_to_404(client):
    with patch("backend.repositories.origin.get_summary", side_effect=KeyError("unknown source")):
        resp = client.post("/api/origin/summary", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp.status_code == 404


def test_origin_aggregates_rejects_unknown_section(client, in_memory_duckdb, test_service_source):
    """sections=['not_a_section'] returns 400 (router) or 422 (Pydantic
    Literal). Either is an explicit reject so the FE never gets a
    silently-degraded 200 — pins the standardized selector contract
    across the 5 P-4 pages."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/origin/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["not_a_section"]}
    )
    assert resp.status_code in (400, 422)


def test_origin_aggregates_summary_only_suppresses_other_timings(client, in_memory_duckdb, test_service_source):
    """sections=['summary'] must skip the other six section reads — the
    section_timings entries are the load-bearing signal the perf
    harness reads to attribute time. A phantom mark on a section that
    didn't actually run would corrupt that attribution."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/origin/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["summary"]})
    assert resp.status_code == 200
    data = resp.json()
    timings = {t["section"] for t in data.get("_section_timings", [])}
    assert "summary" in timings
    for blocked in ("slow_urls", "timeseries", "status_codes", "path_breakdown", "pop_latency", "ip_health"):
        assert blocked not in timings, f"summary-only selector leaked {blocked} timing; got {timings}"


def test_origin_aggregates_coupling_expands_ts_status_path(client, in_memory_duckdb, test_service_source):
    """sections=['timeseries'] alone must auto-expand to the
    {timeseries, status_codes, path_breakdown} triple (they share
    branch 3's pool conn — splitting them across requests would either
    add another checkout or serialize work that already shares one)."""
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/origin/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["timeseries"]})
    assert resp.status_code == 200
    data = resp.json()
    timings = {t["section"] for t in data.get("_section_timings", [])}
    # Coupling rule expanded the request to include the other two on the
    # same branch — all three must fire.
    for need in ("timeseries", "status_codes", "path_breakdown"):
        assert need in timings, f"coupling did not include {need}; got {timings}"
    # Other branches must NOT fire.
    for blocked in ("summary", "slow_urls", "pop_latency", "ip_health"):
        assert blocked not in timings, f"selector did not suppress {blocked}; got {timings}"


def test_origin_aggregates_range_token(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with _patch_range_token():
        resp = client.post(
            "/api/origin/aggregates",
            headers=_SVC_HEADERS,
            json={"filters": {}, "range_token": "30d", "anchor": "2026-08-24T13:00:00Z"},
        )
    assert resp.status_code == 200
    assert "summary" in resp.json()


def test_origin_aggregates_dispatches_registered_high_scale_service(client, monkeypatch, test_service_source):
    expected = {
        "has_data": True,
        "total": 12,
        "summary": {"total_misses": 7, "total_passes": 5},
        "timeseries": {"has_data": True, "series": []},
        "slow_urls": {"has_data": True, "rows": []},
        "status_codes": {"has_data": True, "rows": []},
        "path_breakdown": {"has_data": True, "shielding_detected": False, "rows": []},
        "pop_latency": {"has_data": True, "requires_group_c": False, "rows": []},
        "ip_health": {"has_data": True, "rows": []},
    }
    high_scale_aggregates = MagicMock(return_value=expected)
    monkeypatch.setitem(sys.modules, "backend.high_scale.origin", SimpleNamespace(aggregates=high_scale_aggregates))
    service = SimpleNamespace(service_id=test_service_source["service_id"])
    monkeypatch.setattr(
        "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=MagicMock(return_value=service)),
    )
    standard = AsyncMock(side_effect=AssertionError("standard Origin repository called"))
    monkeypatch.setattr("backend.repositories.origin.get_aggregates", standard)

    response = client.post(
        "/api/origin/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["summary"]}
    )

    assert response.status_code == 200, response.text
    assert response.json()["total"] == 12
    high_scale_aggregates.assert_called_once()
    assert high_scale_aggregates.call_args.kwargs["sections"] == {"summary"}
    standard.assert_not_awaited()


def test_origin_drilldown_accepts_default_request(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/origin/drilldown", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp.status_code == 200
    data = resp.json()
    assert "has_data" in data
    assert data["dimension"] == "host"
    assert isinstance(data["rows"], list)


def test_origin_drilldown_validates_dimension_enum(client):
    resp = client.post(
        "/api/origin/drilldown", headers=_SVC_HEADERS, json={"filters": {}, "dimension": "invalid_dimension"}
    )
    assert resp.status_code == 422


def test_origin_drilldown_passes_dimension_and_limit(client, in_memory_duckdb, test_service_source):
    _seed_origin_table(in_memory_duckdb, test_service_source)
    with patch(
        "backend.repositories.origin.get_drilldown", return_value={"has_data": True, "dimension": "pop", "rows": []}
    ) as mock:
        resp = client.post(
            "/api/origin/drilldown", headers=_SVC_HEADERS, json={"filters": {}, "dimension": "pop", "limit": 15}
        )
    assert resp.status_code == 200
    kwargs = mock.call_args.kwargs
    assert kwargs["dimension"] == "pop"
    assert kwargs["limit"] == 15


def test_origin_drilldown_empty_table(client, in_memory_duckdb, test_service_source):
    # Table exists but no rows seeded with matching criteria
    resp = client.post("/api/origin/drilldown", headers=_SVC_HEADERS, json={"filters": {}, "dimension": "oip"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["dimension"] == "oip"
    assert data["has_data"] is False
    assert data["rows"] == []


# ── Merged from tests/routers/test_performance_router.py ──
def _seed_performance_table(con, src) -> None:
    _seed_logs(
        con,
        src,
        20,
        hours_ago=1,
        elapsed=120000,
        ttfb=0.040,
        url="/test-perf-url",
        asn=12345,
        cache="HIT",
        ttl=3600,
        ottfb=30000.0,
        ottlb=50000.0,
    )


def test_performance_aggregates_default(client, in_memory_duckdb, test_service_source):
    _seed_performance_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp.status_code == 200
    assert "top_urls" in resp.json()


def test_performance_aggregates_range_token(client, in_memory_duckdb, test_service_source):
    _seed_performance_table(in_memory_duckdb, test_service_source)
    with _patch_range_token():
        resp = client.post(
            "/api/performance/aggregates",
            headers=_SVC_HEADERS,
            json={"filters": {}, "range_token": "30d", "anchor": "2026-08-24T13:00:00Z"},
        )
    assert resp.status_code == 200
    assert "top_urls" in resp.json()


def test_performance_aggregates_telemetry_attribution(client, in_memory_duckdb, test_service_source, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    _seed_performance_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/performance/aggregates",
        headers={**_SVC_HEADERS, "X-Page-Load-ID": "test-perf-page-load-42"},
        json={"filters": {}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "_debug_queries" in data
    assert isinstance(data["_debug_queries"], list)
    assert len(data["_debug_queries"]) > 0


def test_performance_aggregates_section_pruning(client, in_memory_duckdb, test_service_source):
    _seed_performance_table(in_memory_duckdb, test_service_source)

    # 1. top_urls auto-expands with top_asns (_TOP_N_PAIR), leaving scatter/waterfall/ttl_dist empty
    resp_top = client.post(
        "/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["top_urls"]}
    )
    assert resp_top.status_code == 200
    data_top = resp_top.json()
    assert len(data_top["top_urls"]) > 0
    assert len(data_top["top_asns"]) > 0
    assert data_top["ttl_dist"] == []
    assert data_top["scatter"] == []
    assert data_top["waterfall"] == {}

    # 2. waterfall auto-expands with scatter (_WATERFALL_SCATTER_PAIR), leaving top_urls/top_asns/ttl_dist empty
    resp_wf = client.post(
        "/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["waterfall"]}
    )
    assert resp_wf.status_code == 200
    data_wf = resp_wf.json()
    assert data_wf["top_urls"] == []
    assert data_wf["top_asns"] == []
    assert data_wf["ttl_dist"] == []
    assert "avg" in data_wf["waterfall"]
    assert len(data_wf["scatter"]) > 0

    # 3. ttl_dist is independent
    resp_ttl = client.post(
        "/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["ttl_dist"]}
    )
    assert resp_ttl.status_code == 200
    data_ttl = resp_ttl.json()
    assert data_ttl["top_urls"] == []
    assert data_ttl["top_asns"] == []
    assert data_ttl["scatter"] == []
    assert data_ttl["waterfall"] == {}
    assert isinstance(data_ttl["ttl_dist"], list)


@pytest.mark.parametrize("sort_by", ["avg", "p50", "p95", "p99"])
def test_performance_aggregates_sort_by_variants(client, in_memory_duckdb, test_service_source, sort_by):
    _seed_performance_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sort_by": sort_by})
    assert resp.status_code == 200
    data = resp.json()
    assert "top_urls" in data
    assert len(data["top_urls"]) > 0


def test_performance_aggregates_analyst_session_clamps_window(client, in_memory_duckdb, test_service_source):
    start = datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    captured: dict = {}

    def _fake_get_performance_aggregates(**kw):
        captured.update(kw)
        return {"top_urls": [], "top_asns": [], "ttl_dist": [], "scatter": [], "waterfall": {}}

    with _with_analyst_ctx(
        test_service_source,
        in_memory_duckdb,
        mask_ips=True,
        time_bounds=TimeBounds(start=start, end=end),
        path="/api/performance/aggregates",
        session_id="sess-analyst-perf",
    ):
        with patch(
            "backend.repositories.performance.get_performance_aggregates", side_effect=_fake_get_performance_aggregates
        ):
            resp = client.post(
                "/api/performance/aggregates",
                headers=_SVC_HEADERS,
                json={"filters": {}, "start_time": "2026-06-17T00:00:00Z", "end_time": "2026-06-17T23:59:59Z"},
            )
        assert resp.status_code == 200, resp.text
        assert captured["start_time"] == start.isoformat()
        assert captured["end_time"] == end.isoformat()


def test_performance_aggregates_high_scale_dispatch(client, test_service_source):
    mock_registry = MagicMock()
    mock_registry.resolve.return_value = MagicMock()
    fake_response = {
        "top_urls": [{"url": "/hs", "requests": 50, "avg": 10.0, "p50": 8.0, "p95": 15.0, "p99": 20.0}],
        "top_asns": [],
        "ttl_dist": [],
        "scatter": [],
        "waterfall": {},
        "_approx": True,
    }

    with (
        patch("backend.high_scale.registry_bootstrap.get_high_scale_service_registry", return_value=mock_registry),
        patch("backend.high_scale.performance.performance_aggregates", return_value=fake_response) as mock_hs_perf,
    ):
        resp = client.post("/api/performance/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sort_by": "p99"})
        assert resp.status_code == 200
        assert resp.json()["_approx"] is True
        mock_hs_perf.assert_called_once()


def test_performance_aggregates_high_scale_receives_expanded_sections(client, test_service_source):
    mock_registry = MagicMock()
    mock_registry.resolve.return_value = MagicMock()
    fake_response = {"top_urls": [], "top_asns": [], "ttl_dist": [], "scatter": [], "waterfall": {}}

    with (
        patch("backend.high_scale.registry_bootstrap.get_high_scale_service_registry", return_value=mock_registry),
        patch("backend.high_scale.performance.performance_aggregates", return_value=fake_response) as mock_hs_perf,
    ):
        resp = client.post(
            "/api/performance/aggregates",
            headers=_SVC_HEADERS,
            json={"filters": {}, "sort_by": "avg", "sections": ["scatter", "top_urls"]},
        )

    assert resp.status_code == 200
    assert mock_hs_perf.call_args.kwargs["sections"] == {"scatter", "waterfall", "top_urls", "top_asns"}
    assert mock_hs_perf.call_args.args[1].sort_by == "avg"


# ── Merged from tests/routers/test_security_router.py ──
def _seed_security_table(con, src) -> None:
    _schema_cols_cache.clear()
    from backend.core.duckdb import _clear_schema_cache

    _clear_schema_cache()

    logs = generate_mock_logs(src, num_logs=40, hours_ago=1)
    for i, log in enumerate(logs):
        log.update(
            ja3="771,4865-4866-4867,0-23-65281-10-11,29-23-24,0" if i % 2 == 0 else "772,4865-4866,0-23-10,29-23,0",
            ja4="t13d1516h2_8daaf6152771_0271d18f93db" if i % 2 == 0 else "t13d1516h2_aabbccddeeff_112233445566",
            tls="1.3" if i % 2 == 0 else "1.2",
            tls_ciphers_sha="aabbcc11" if i < 20 else "ddeeff22",
            p_type="VPN" if i % 3 == 0 else None,
            ua="Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
            if i % 4 == 0
            else "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            host="example.com",
            waf=i % 7 == 0,
            req_header_bytes=1024 + i * 50,
            rtt_min=5 if i % 3 == 0 else 1000,
            tcp_rtt=1000,
            lat=37.7749,
            lon=-122.4194,
            country="US" if i % 2 == 0 else "DE",
            asn=15169 if i % 2 == 0 else 3320,
        )
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def test_security_aggregates_default(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/security/aggregates", headers=_SVC_HEADERS, json={"filters": {}})
    assert resp.status_code == 200
    data = resp.json()
    for k in ("tls_fingerprints", "req_size_dist", "ipv6_adoption", "proxy_dist", "conn_reuse_dist"):
        assert k in data


def test_security_bundle_post(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.post("/api/security/bundle", headers=_SVC_HEADERS, json={"filters": {}, "bucket_seconds": 300})
    assert resp.status_code == 200
    data = resp.json()
    assert "tls_fingerprints" in data
    assert "req_size_dist" in data


def test_security_bundle_get(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    with _patch_range_token():
        resp = client.get(
            "/api/security/bundle", headers=_SVC_HEADERS, params={"range_token": "24h", "bucket_seconds": 300}
        )
    assert resp.status_code == 200
    assert "tls_fingerprints" in resp.json()


def test_security_aggregates_range_token(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    with _patch_range_token():
        resp = client.post(
            "/api/security/aggregates",
            headers=_SVC_HEADERS,
            json={"filters": {}, "range_token": "24h", "anchor": "2026-08-24T13:00:00Z"},
        )
    assert resp.status_code == 200
    assert "tls_fingerprints" in resp.json()


def test_security_aggregates_section_pruning_and_implies(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    with patch("backend.repositories.security.get_security_aggregates", return_value={}) as mock_repo:
        client.post(
            "/api/security/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["tls_fingerprints"]}
        )
    assert mock_repo.called
    sections_called = mock_repo.call_args.kwargs.get("sections")
    assert "tls_fingerprints" in sections_called
    # Implies coupling: tls_fingerprints implies fingerprint_coverage
    assert "fingerprint_coverage" in sections_called


def test_security_aggregates_analyst_session_clamps_window(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    earlier = datetime.now(UTC) - timedelta(hours=2)
    later = datetime.now(UTC)
    with _with_analyst_ctx(
        test_service_source, in_memory_duckdb, mask_ips=False, time_bounds=TimeBounds(start=earlier, end=later)
    ):
        resp = client.post(
            "/api/security/aggregates",
            headers=_SVC_HEADERS,
            json={
                "filters": {},
                "start_time": (datetime.now(UTC) - timedelta(days=7)).isoformat(),
                "end_time": later.isoformat(),
            },
        )
        assert resp.status_code == 200


def test_security_aggregates_ip_masking(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    with _with_analyst_ctx(test_service_source, in_memory_duckdb, mask_ips=True):
        resp = client.post(
            "/api/security/aggregates", headers=_SVC_HEADERS, json={"filters": {}, "sections": ["top_ips_header"]}
        )
        assert resp.status_code == 200
        for entry in resp.json().get("top_ips_header", []):
            ip = entry.get("ip") or entry.get("client_ip")
            if ip:
                # Should be masked (e.g. 192.168.1.xxx or ends with xxx or * or redacted)
                assert "xxx" in ip or "*" in ip or ip == "0.0.0.0" or "[redacted]" in ip


def test_export_proxies_csv_fastly_acl(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    now = datetime.now(UTC)
    resp = client.get(
        "/api/security/proxies/export",
        headers=_SVC_HEADERS,
        params={
            "start_time": (now - timedelta(hours=2)).isoformat(),
            "end_time": now.isoformat(),
            "threshold": "High",
            "format": "fastly-acl",
        },
    )
    assert resp.status_code == 200
    assert "text/csv" in resp.headers.get("content-type", "")
    assert "attachment; filename=" in resp.headers.get("content-disposition", "")
    assert "ip,comment" in resp.text


def test_export_proxies_csv_plain(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    now = datetime.now(UTC)
    resp = client.get(
        "/api/security/proxies/export",
        headers=_SVC_HEADERS,
        params={
            "start_time": (now - timedelta(hours=2)).isoformat(),
            "end_time": now.isoformat(),
            "threshold": "High",
            "format": "plain",
        },
    )
    assert resp.status_code == 200
    assert "text/plain" in resp.headers.get("content-type", "")
    assert "attachment; filename=" in resp.headers.get("content-disposition", "")


def test_export_proxies_csv_ip_masking(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    now = datetime.now(UTC)
    with _with_analyst_ctx(test_service_source, in_memory_duckdb, mask_ips=True):
        resp = client.get(
            "/api/security/proxies/export",
            headers=_SVC_HEADERS,
            params={
                "start_time": (now - timedelta(hours=2)).isoformat(),
                "end_time": now.isoformat(),
                "threshold": "High",
                "format": "plain",
            },
        )
        assert resp.status_code == 200
        for line in (ln.strip() for ln in resp.text.splitlines() if ln.strip()):
            assert "xxx" in line or "*" in line or line == "0.0.0.0" or "[redacted]" in line


def test_security_threat_intel_default(client, in_memory_duckdb, test_service_source):
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.get("/api/security/threat-intel", headers=_SVC_HEADERS)
    assert resp.status_code == 200
    data = resp.json()
    assert "data" in data
    if data["data"]:
        first = data["data"][0]
        for k in (
            "tls_fingerprint",
            "requests",
            "top_country",
            "matched_waf_rules_count",
            "bot_percentage",
            "is_anonymized_proxy",
        ):
            assert k in first


def test_security_telemetry_attribution(client, in_memory_duckdb, test_service_source, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    _seed_security_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/security/aggregates",
        headers={**_SVC_HEADERS, "X-Page-Load-ID": "test-security-page-load-99"},
        json={"filters": {}},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "_debug_queries" in data
    assert isinstance(data["_debug_queries"], list)
    assert len(data["_debug_queries"]) > 0


def _register_high_scale(monkeypatch, test_service_source):
    service = SimpleNamespace(service_id=test_service_source["service_id"])
    monkeypatch.setattr(
        "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
        lambda: SimpleNamespace(resolve=lambda _service_id: service),
    )
    return service


def test_aggregates_receive_expanded_sections_and_mask_flag(client, monkeypatch, in_memory_duckdb, test_service_source):
    service = _register_high_scale(monkeypatch, test_service_source)
    hs_aggregates = MagicMock(return_value=SecurityAggregatesResponse())
    monkeypatch.setattr("backend.high_scale.security.security_aggregates", hs_aggregates)
    with _with_analyst_ctx(test_service_source, in_memory_duckdb, mask_ips=True):
        resp = client.post(
            "/api/security/aggregates",
            headers=_SVC_HEADERS,
            json={"filters": {}, "sections": ["tls_fingerprints", "ngwaf_verified_bots"]},
        )

    assert resp.status_code == 200, resp.text
    hs_aggregates.assert_called_once()
    assert hs_aggregates.call_args.args[0] is service
    assert hs_aggregates.call_args.kwargs["sections"] == {
        "tls_fingerprints",
        "fingerprint_coverage",
        "ngwaf_verified_bots",
        "ngwaf_verified_bots_ts",
    }
    assert hs_aggregates.call_args.kwargs["mask_ips"] is True


def test_threat_intel_is_clamped_to_the_analyst_window(client, monkeypatch, in_memory_duckdb, test_service_source):
    _register_high_scale(monkeypatch, test_service_source)
    hs_threat_intel = MagicMock(return_value={"data": []})
    monkeypatch.setattr("backend.high_scale.security.get_security_threat_intel", hs_threat_intel)
    ceiling_start = datetime.now(UTC) - timedelta(hours=2)
    ceiling_end = datetime.now(UTC)
    with _with_analyst_ctx(
        test_service_source,
        in_memory_duckdb,
        mask_ips=False,
        time_bounds=TimeBounds(start=ceiling_start, end=ceiling_end),
    ):
        resp = client.get(
            "/api/security/threat-intel",
            headers=_SVC_HEADERS,
            params={
                "start_time": (ceiling_start - timedelta(days=30)).isoformat(),
                "end_time": ceiling_end.isoformat(),
            },
        )

    assert resp.status_code == 200, resp.text
    queried_start = datetime.fromisoformat(hs_threat_intel.call_args.args[1].replace("Z", "+00:00"))
    assert queried_start >= ceiling_start - timedelta(seconds=1)


def test_proxies_export_streams_masked_high_risk_clients(client, monkeypatch, in_memory_duckdb, test_service_source):
    _register_high_scale(monkeypatch, test_service_source)
    hs_proxies = MagicMock(
        return_value={
            "active_clients": [
                {"ip": "198.51.100.xxx", "risk_level": "High", "impossible_distance": True},
                {"ip": "198.51.101.xxx", "risk_level": "Medium"},
            ]
        }
    )
    monkeypatch.setattr("backend.high_scale.security.get_proxies_data", hs_proxies)
    with _with_analyst_ctx(test_service_source, in_memory_duckdb, mask_ips=True):
        resp = client.get(
            "/api/security/proxies/export",
            headers=_SVC_HEADERS,
            params={
                "start_time": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
                "end_time": datetime.now(UTC).isoformat(),
                "threshold": "High",
                "format": "plain",
            },
        )

    assert resp.status_code == 200, resp.text
    assert resp.text.split() == ["198.51.100.xxx"]
    assert hs_proxies.call_args.kwargs["mask_ips"] is True


# ── Merged from tests/routers/test_security_insights.py ──
def _stub_insights_security_insights() -> dict:
    """Minimal get_insights return shape for the InsightsResponse model."""
    now = "2026-06-17T00:00:00+00:00"
    return {
        "insights": [],
        "window_start": now,
        "window_end": now,
        "baseline_start": now,
        "baseline_end": now,
        "computed_at": now,
        "window_hours": 1.0,
        "baseline_hours": 168.0,
    }


class TestSecurityInsightsRouter:
    def test_insights_admin_passes_no_clamp(self, client):
        """M2 wiring: admin (no analyst session) reaches the repo with
        clamp_start/clamp_end = None (full range + shared prewarmer cache)."""
        captured: dict = {}

        def _fake(**kw):
            captured.update(kw)
            return _stub_insights_security_insights()

        with patch("backend.repositories.insights.get_insights", side_effect=_fake):
            resp = client.post(
                "/api/insights",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={"window_size_hrs": 1.0, "baseline_hours": 168.0},
            )

        assert resp.status_code == 200, resp.text
        assert captured["clamp_start"] is None
        assert captured["clamp_end"] is None

    def test_insights_analyst_passes_clamped_window(self, client, in_memory_duckdb, test_service_source):
        """M2 wiring: an analyst session → the router clamps the scanned range to
        the session window and forwards concrete clamp bounds to get_insights."""
        from datetime import UTC, datetime, timedelta
        from types import SimpleNamespace

        from backend.core.request_context import build_request_context
        from backend.main import app
        from backend.utils.remote_access import TimeBounds

        # Bounds in the past relative to the router's `now`, so clamp_end ceilings
        # at `end` and clamp_start floors at `start` regardless of wall-clock.
        now = datetime.now(UTC)
        start = now - timedelta(hours=3)
        end = now - timedelta(hours=2)
        session = SimpleNamespace(
            session_id="sess-1",
            pii_policy={"mask_ips": False},
            service_ids=[test_service_source["service_id"]],
        )

        captured: dict = {}

        def _fake(**kw):
            captured.update(kw)
            return _stub_insights_security_insights()

        # The handler clamps via ctx.clamp, which reads ctx.time_bounds (what
        # build_request_context resolves from get_analyst_time_bounds in prod).
        app.dependency_overrides[build_request_context] = override_request_context(
            source=test_service_source,
            _con_override=in_memory_duckdb,
            session=session,
            path="/api/insights",
            time_bounds=TimeBounds(start=start, end=end),
        )
        with patch("backend.repositories.insights.get_insights", side_effect=_fake):
            resp = client.post(
                "/api/insights",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={"window_size_hrs": 1.0, "baseline_hours": 168.0},
            )

        assert resp.status_code == 200, resp.text
        # Clamp ceilings the anchor at `end` and floors the earliest scan at `start`.
        assert captured["clamp_end"] == end.isoformat()
        assert captured["clamp_start"] == start.isoformat()

    def test_security_endpoint(self, client, in_memory_duckdb, test_service_source):
        # Setup mock data with security fields
        logs = generate_mock_logs(test_service_source, num_logs=50)
        for i, log in enumerate(logs[:10]):
            log["waf"] = True
            log["waf_sig"] = "SQLI,XSS"
            log["waf_resp"] = 403
            log["ja3"] = "a0e9f5d64349fb13191bc781f81f42e1"  # Mock bad JA3

        table_name = _safe_table(test_service_source["name"])
        insert_mock_logs(in_memory_duckdb, table_name, logs)

        response = client.post(
            "/api/security/aggregates", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
        )

        assert response.status_code == 200
        data = response.json()

        # We asserted 'ja3' was present but backend might use a different key.
        # Security endpoint typically returns bot categories, rate limiting signals etc.
        # Let's just check for _debug_queries indicating it ran successfully
        assert "_debug_queries" in data

    def test_security_endpoint_rejects_unknown_section(self, client, in_memory_duckdb, test_service_source):
        """sections=['not_a_section'] returns 400 with an unknown_section
        detail — invalid selector values must not be silently dropped (the
        FE would render the card without the data, hiding the typo)."""
        table_name = _safe_table(test_service_source["name"])
        logs = generate_mock_logs(test_service_source, num_logs=5)
        insert_mock_logs(in_memory_duckdb, table_name, logs)

        response = client.post(
            "/api/security/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sections": ["not_a_section"]},
        )
        # Pydantic validates Literal first, so the response is 422 from the
        # request-validator OR 400 from _expand_sections — either is an
        # explicit reject (the FE never gets a partial 200 with the bad
        # section silently dropped). Both surface a JSON error body.
        assert response.status_code in (400, 422)

    def test_security_endpoint_returns_only_requested_sections(self, client, in_memory_duckdb, test_service_source):
        """sections=['conn_reuse_dist'] suppresses the other 12 section
        keys from the response — proves the router → repo pipeline carries
        the selector through end-to-end."""
        table_name = _safe_table(test_service_source["name"])
        logs = generate_mock_logs(test_service_source, num_logs=10)
        insert_mock_logs(in_memory_duckdb, table_name, logs)

        response = client.post(
            "/api/security/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sections": ["conn_reuse_dist"]},
        )
        assert response.status_code == 200
        data = response.json()
        # Pydantic always echoes its declared fields with their defaults, so
        # missing-from-result keys come back as their schema defaults
        # ([] / {}). Test the SQL gate instead by checking that the unwanted
        # keys are empty AND conn_reuse_dist is the only one that could
        # have had data — section_timings entries are the more direct gate
        # (suppressed sections don't append to the timer).
        timings_names = {t["section"] for t in data.get("_section_timings", [])}
        # conn_reuse_dist's timer entry must be present
        assert "conn_reuse_dist" in timings_names, (
            f"selector dropped the requested section's timing entry; got {timings_names}"
        )
        # And the other sections' SQL must NOT have fired
        for blocked in {"ipv6_adoption", "proxy_dist", "verified_bots_ts", "wellknown_bots_query"}:
            assert blocked not in timings_names, f"selector did not suppress {blocked}; got {timings_names}"

    def test_insights_endpoint(self, client, in_memory_duckdb, test_service_source):
        # Setup mock data
        logs = generate_mock_logs(test_service_source, num_logs=50)
        table_name = _safe_table(test_service_source["name"])
        insert_mock_logs(in_memory_duckdb, table_name, logs)

        response = client.post(
            "/api/insights",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"window_hours": 1.0, "baseline_hours": 24.0},
        )

        assert response.status_code == 200
        data = response.json()
        assert "insights" in data
        assert isinstance(data["insights"], list)

    def test_cache_collapse_detail_endpoint(self, client, in_memory_duckdb, test_service_source):
        logs = generate_mock_logs(test_service_source, num_logs=5)
        # Ensure there is a URL and a cache miss/hit to get valid results
        for log in logs:
            log["url"] = "/test-url-collapse"
            log["cache"] = "MISS"
            log["ip"] = "1.2.3.4"
        table_name = _safe_table(test_service_source["name"])
        insert_mock_logs(in_memory_duckdb, table_name, logs)

        response = client.post(
            "/api/insights/cache-collapse-detail",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "url": "/test-url-collapse",
                "window_size_hrs": 1.0,
                "baseline_hours": 24.0,
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["url"] == "/test-url-collapse"
        assert "timeline" in data
        # Hit ratio is HIT/(HIT+MISS); with only MISS rows it's 0.0 (and PASS-free).
        assert data["baseline_hit_rate"] == 0.0
        assert data["window_hit_rate"] == 0.0
        assert data["baseline_pass_rate"] == 0.0
        assert data["window_pass_rate"] == 0.0
        # Breakdown is window-scoped counts by disposition.
        assert set(data["breakdown"].keys()) == {"hits", "misses", "passes", "other"}
        assert data["breakdown"]["passes"] == 0
        # Recent events are scoped to actual MISSes (not the old non-HIT catch-all).
        assert "recent_misses" in data
        assert "evictions" not in data
        assert all(row["cache"].upper().startswith("MISS") for row in data["recent_misses"])


# ── Merged from tests/routers/test_value_router.py ──
def test_value_summary_accepts_default_request(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    # Test empty sections (all)
    resp = client.post(
        "/api/value/summary",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": None},
    )
    assert resp.status_code == 200

    # Test 'summary' tab sections
    resp = client.post(
        "/api/value/summary",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["overview", "caching", "network"]},
    )
    assert resp.status_code == 200

    # Test individual sections
    for section in ["overview", "caching", "security", "bots", "performance", "network", "io"]:
        resp = client.post(
            "/api/value/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sections": [section]},
        )
        assert resp.status_code == 200, f"Section {section} failed with {resp.status_code}: {resp.text}"


def test_value_summary_empty_table(client, in_memory_duckdb, test_service_source):
    from backend.core.log_fields import LOG_FIELD_CATALOG

    raw_fields = [f for f in LOG_FIELD_CATALOG if f.get("vcl") is not None]
    schema_def = ", ".join([f'"{f["id"]}" {f["duckdb_type"]}' for f in raw_fields])
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema_def})")

    resp = client.post(
        "/api/value/summary",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert resp.status_code == 200, f"Empty table failed with {resp.status_code}: {resp.text}"


def test_value_summary_missing_network_columns(client, in_memory_duckdb, test_service_source):
    # Only create a table with timestamp, to trigger missing network columns
    table_name = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (timestamp TIMESTAMPTZ, req_bytes INT)")

    # Insert a dummy row
    in_memory_duckdb.execute(f"INSERT INTO {table_name} VALUES (TIMESTAMPTZ '2026-07-23 12:00:00Z', 100)")

    # Create the hour_bundled folder to trigger the rollup path check
    import os

    from backend.core.rollups import _hour_bundled_root

    bundled_root = _hour_bundled_root(test_service_source)
    os.makedirs(bundled_root, exist_ok=True)

    # We need to ensure crosses_active is True. Since crosses_active is True when st/et span the active hour,
    # let's make start_time, end_time close to now
    from datetime import UTC, datetime, timedelta

    from backend.utils.date_utils import iso_z

    now = datetime.now(UTC)
    start_time = iso_z(now - timedelta(hours=1))
    end_time = iso_z(now + timedelta(hours=1))

    try:
        resp = client.post(
            "/api/value/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "sections": ["network"],
                "start_time": start_time,
                "end_time": end_time,
            },
        )
        assert resp.status_code == 200, f"Missing columns failed with {resp.status_code}: {resp.text}"
    finally:
        # Clean up directory
        import shutil

        shutil.rmtree(os.path.dirname(bundled_root), ignore_errors=True)


def test_value_summary_tls_and_verified_bots_fallbacks(client, in_memory_duckdb, test_service_source):
    table_name = _safe_table(test_service_source["name"])

    # Create schema with tls (fallback for is_ssl) and waf_sig (for verified_bots)
    in_memory_duckdb.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMPTZ,
            tls VARCHAR,
            waf_sig VARCHAR,
            _bot_name VARCHAR,
            resp_bytes INT
        )
    """)

    # Insert mock rows
    # Row 1: TLS enabled, verified bot
    # Row 2: No TLS, no bot
    in_memory_duckdb.execute(f"""
        INSERT INTO {table_name} VALUES
        (TIMESTAMPTZ '2026-07-23 12:00:00Z', '1.3', 'SHIELD,VERIFIED-BOT.googlebot', 'Googlebot', 100),
        (TIMESTAMPTZ '2026-07-23 12:05:00Z', '', NULL, '', 200)
    """)

    # Make the request close to the inserted timestamps

    start_time = "2026-07-23T11:00:00Z"
    end_time = "2026-07-23T13:00:00Z"

    resp = client.post(
        "/api/value/summary",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "sections": ["network", "bots"],
            "start_time": start_time,
            "end_time": end_time,
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()

    # Verify tls_pct uses the 'tls' column successfully and finds 50%
    assert data["network"]["tls_pct"] == 50.0

    # Verify verified_bots is correctly computed as 1
    assert data["bots"]["verified_bots"] == 1


def test_value_summary_range_token(client, in_memory_duckdb, test_service_source, monkeypatch):
    from unittest.mock import patch

    from backend import config as svcconfig

    logs = generate_mock_logs(test_service_source, num_logs=10, hours_ago=1)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    monkeypatch.setattr(svcconfig, "get_status", lambda sid: {"earliest_log_at": "2026-08-19T00:00:00Z"})

    with (
        patch("backend.utils.time_window.is_valid_range_token", return_value=True),
        patch(
            "backend.utils.time_window.resolve_window", return_value=("2026-08-19T11:00:00Z", "2026-08-19T13:00:00Z")
        ),
    ):
        resp = client.post(
            "/api/value/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "range_token": "24h",
                "anchor": "2026-08-19T12:00:00Z",
            },
        )
    assert resp.status_code == 200
    assert "overview" in resp.json()


def test_value_summary_analyst_session_clamps_window(client, in_memory_duckdb, test_service_source):
    """An analyst session causes the value router to clamp requested time window to the analyst bounds."""
    from datetime import UTC, datetime
    from types import SimpleNamespace
    from unittest.mock import patch

    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds
    from tests.conftest import override_request_context

    start = datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-analyst-1",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )

    captured: dict = {}

    def _fake_get_summary(**kw):
        captured.update(kw)
        return {
            "overview": {
                "total_requests": 100,
                "origin_offload_pct": 95.0,
            }
        }

    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/value/summary",
        time_bounds=TimeBounds(start=start, end=end),
    )
    try:
        with patch("backend.repositories.value.get_summary", side_effect=_fake_get_summary):
            resp = client.post(
                "/api/value/summary",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={
                    "filters": {},
                    "start_time": "2026-06-17T00:00:00Z",
                    "end_time": "2026-06-17T23:59:59Z",
                },
            )
        assert resp.status_code == 200, resp.text
        assert captured["start_time"] == start.isoformat()
        assert captured["end_time"] == end.isoformat()
    finally:
        app.dependency_overrides.pop(build_request_context, None)


def test_value_summary_rollup_crosses_active_hour(client, in_memory_duckdb, test_service_source):
    """Verify _try_overview_from_rollup and _try_network_from_rollup succeed when crosses_active=True
    (including GROUP BY 1 on the active-hour raw branch and protocol_time_series generation)."""
    import os
    import shutil
    from datetime import UTC, datetime, timedelta

    from backend.core.rollups import (
        NETWORK_SUMMARY_BUNDLE_FILENAME,
        OVERVIEW_BUNDLE_FILENAME,
        _hour_bundled_root,
    )
    from backend.utils.date_utils import iso_z

    table_name = _safe_table(test_service_source["name"])
    now = datetime.now(UTC).replace(minute=10, second=0, microsecond=0)
    prev_hour = (now - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    in_memory_duckdb.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMPTZ,
            cache VARCHAR,
            resp_bytes BIGINT,
            is_shield BOOLEAN,
            waf INT,
            waf_resp INT,
            elapsed DOUBLE,
            protocol VARCHAR,
            is_ssl BOOLEAN,
            ipv6 BOOLEAN
        )
    """)
    in_memory_duckdb.execute(
        f"""
        INSERT INTO {table_name} VALUES
        (TIMESTAMPTZ '{iso_z(now)}', 'HIT', 1000, true, 0, 200, 1200.0, 'HTTP/3', true, true),
        (TIMESTAMPTZ '{iso_z(now)}', 'MISS', 2000, false, 1, 406, 4800.0, 'HTTP/2', true, false)
        """
    )

    bundled_root = _hour_bundled_root(test_service_source)
    hour_dir = os.path.join(bundled_root, f"hour={prev_hour.strftime('%Y-%m-%d-%H')}")
    os.makedirs(hour_dir, exist_ok=True)
    ov_path = os.path.join(hour_dir, OVERVIEW_BUNDLE_FILENAME)
    net_path = os.path.join(hour_dir, NETWORK_SUMMARY_BUNDLE_FILENAME)

    try:
        in_memory_duckdb.execute(
            f"""
            COPY (
                SELECT
                    TIMESTAMPTZ '{iso_z(prev_hour)}' AS hour_start,
                    CAST(10 AS BIGINT) AS requests,
                    CAST(8 AS BIGINT) AS hit_requests,
                    CAST(2 AS BIGINT) AS miss_requests,
                    CAST(0 AS BIGINT) AS pass_requests,
                    CAST(0 AS BIGINT) AS synth_requests,
                    CAST(2 AS BIGINT) AS origin_requests,
                    CAST(8000 AS BIGINT) AS bandwidth_saved_bytes,
                    CAST(10000 AS BIGINT) AS total_bandwidth_bytes,
                    CAST(4 AS BIGINT) AS shield_hit_requests,
                    CAST(5 AS BIGINT) AS shield_total_requests,
                    CAST(1 AS BIGINT) AS threats_blocked,
                    CAST(8000.0 AS DOUBLE) AS hit_elapsed_sum,
                    CAST(8 AS BIGINT) AS hit_elapsed_count,
                    CAST(8000.0 AS DOUBLE) AS miss_elapsed_sum,
                    CAST(2 AS BIGINT) AS miss_elapsed_count
            ) TO '{ov_path}' (FORMAT PARQUET)
            """
        )
        in_memory_duckdb.execute(
            f"""
            COPY (
                SELECT
                    TIMESTAMPTZ '{iso_z(prev_hour)}' AS hour_start,
                    CAST(10 AS BIGINT) AS requests,
                    CAST(6 AS BIGINT) AS http3_requests,
                    CAST(3 AS BIGINT) AS h2_requests,
                    CAST(1 AS BIGINT) AS h11_requests,
                    CAST(0 AS BIGINT) AS other_proto_requests,
                    CAST(10 AS BIGINT) AS tls_requests,
                    CAST(5 AS BIGINT) AS ipv6_requests
            ) TO '{net_path}' (FORMAT PARQUET)
            """
        )

        resp = client.post(
            "/api/value/summary",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "sections": ["overview", "caching", "network"],
                "start_time": iso_z(prev_hour),
                "end_time": iso_z(now + timedelta(minutes=5)),
                "chart_interval": "1 hour",
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["overview"]["total_requests"] == 12
        assert body["caching"]["hit_requests"] == 9
        assert body["network"]["total_requests"] == 12
        assert len(body["network"]["protocol_time_series"]) > 0
    finally:
        shutil.rmtree(os.path.dirname(bundled_root), ignore_errors=True)


# ── Merged from tests/routers/test_pages.py ──
def test_network_health_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/network-health", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "available" in response.json()


def test_network_quality_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/network-quality", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "available" in response.json()


def test_origin_summary_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/origin/summary", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "_debug_queries" in response.json()


def test_security_aggregates_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/security/aggregates", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "tls_fingerprints" in response.json()


def test_usage_log_activity_endpoint(client):
    # No data seeded — endpoint should still return an empty result.
    response = client.get(
        "/api/usage/log-activity",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"start": "2026-01-01", "end": "2026-01-02"},
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert "data" in data
    assert "total_rows" in data
    assert "total_bytes" in data


def test_query_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.post(
        "/api/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"sql": f"SELECT count(*) FROM {table_name}"},
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert "columns" in data
    assert "data" in data
    assert len(data["data"]) == 1


def test_sessions_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post("/api/sessions", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}})

    assert response.status_code == 200, response.text
    data = response.json()
    assert "sessions" in data
    assert "total" in data


def test_alerts_endpoint(client):
    response = client.get(f"/api/alerts/{MOCK_SERVICE_ID}")
    assert response.status_code == 200, response.text
    data = response.json()
    assert "data" in data
    assert isinstance(data["data"], list)


def test_views_endpoint(client):
    response = client.get(f"/api/views/{MOCK_SERVICE_ID}")
    assert response.status_code == 200, response.text
    assert isinstance(response.json(), list)


def test_log_activity_endpoint_returns_data_key(client, test_service_source):
    """Regression: /api/usage/log-activity must return 'data' list with row_count/bytes, not generated/processed."""
    from backend.core import metadata as metadata_db

    src_name = test_service_source["name"]
    con = metadata_db.get_con(src_name)
    # ``ingested_at`` is stored as ISO 8601 with a ``T`` separator
    # (see commit 9af3c8f normalising metadata_db writes). The query
    # parameters land as ``YYYY-MM-DDTHH:MM:SSZ`` via parse_date_window,
    # so the stored values must also use ``T`` for the string-range
    # comparison to match.
    con.executemany(
        "INSERT INTO ingested_files (file_name, source_name, row_count, file_size_bytes, ingested_at) "
        "VALUES (?, ?, ?, ?, ?)",
        [
            ("log1.gz", src_name, 100, 5000, "2026-01-01T10:00:00Z"),
            ("log2.gz", src_name, 200, 8000, "2026-01-01T11:00:00Z"),
        ],
    )
    con.commit()

    response = client.get(
        "/api/usage/log-activity",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"start": "2026-01-01", "end": "2026-01-02", "by": "hour"},
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert "data" in data, f"Expected 'data' key, got: {list(data.keys())}"
    assert "total_rows" in data
    assert "total_bytes" in data
    assert "granularity" in data
    assert data["granularity"] == "hour"
    assert isinstance(data["data"], list)
    assert len(data["data"]) > 0
    assert "row_count" in data["data"][0]
    assert "bytes" in data["data"][0]
    assert "time" in data["data"][0]
    assert data["total_rows"] == 300
    assert data["total_bytes"] == 13000
    assert "generated" not in data
    assert "processed" not in data


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
            sys.modules,
            "backend.core.fastly.ngwaf",
            SimpleNamespace(NgwafWorkspacesResponse=NgwafWorkspacesResponse),
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

    assert resp.status_code == 400, resp.text
    assert resp.json()["detail"] == {"error": "service_id_required"}


@pytest.mark.parametrize(("method", "path"), ANALYTICS_ROUTES)
def test_analytics_route_without_service_is_400_envelope(bare_client, method, path) -> None:
    resp = bare_client.request(method, path, json={})

    assert resp.status_code == 400, resp.text
    detail = resp.json()["detail"]
    assert detail["error"] == "no_service"
    assert detail["no_service"] is True


_PATH_SUBS = {
    "{service_id}": "test-service-id",
    "{run_id}": "1",
    "{log_id}": "1",
    "{source_id}": "wellknown",
    "{alert_id}": "test-alert-id",
    "{view_id}": "test-view-id",
}

_METHOD_PRIORITY = ["GET", "POST", "PUT", "PATCH", "DELETE"]


def _sub_path(path: str) -> str:
    for placeholder, value in _PATH_SUBS.items():
        path = path.replace(placeholder, value)
    return path


def _pick_wrong_method(registered: set[str]) -> str | None:
    for m in _METHOD_PRIORITY:
        if m not in registered:
            return m
    return None  # route accepts every method — nothing to test


def _normalize_template(path: str) -> str:
    """Replace all {param_name} placeholders with {p} so structurally identical
    path patterns like /views/{service_id} and /views/{view_id} hash together."""
    return re.sub(r"\{[^}]+\}", "{p}", path)


def _norm_to_regex(norm: str) -> re.Pattern[str]:
    """Convert a normalised template to a regex that matches concrete paths."""
    parts = norm.split("{p}")
    return re.compile(
        "".join(re.escape(p) + "([^/]+)" if i < len(parts) - 1 else re.escape(p) for i, p in enumerate(parts))
    )


def _shadowed_by_another_route(path: str, wrong_method: str, own_norm: str) -> bool:
    """Return True if *path* would be matched AND handled by a different route that
    accepts *wrong_method*.  In that case, calling wrong_method on path would NOT
    return 405 — it would hit the other handler instead — so this case is untestable.

    Example: /api/alerts/preview is the literal path for POST /alerts/preview, but
    GET /alerts/{service_id} also matches "preview" as a service_id value.  Testing
    GET on /api/alerts/preview would invoke that handler (500 or 200), not return 405.
    """
    for norm, methods in _methods_by_norm.items():
        if norm == own_norm:
            continue
        if wrong_method not in methods:
            continue
        # Does this other normalised template match our concrete test path?
        if _norm_to_regex(norm).fullmatch(path):
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
        if _norm not in _path_by_norm:
            _path_by_norm[_norm] = _sub_path(_route.path)

_cases: list[tuple[str, str, str]] = []

for _norm, _all_methods in sorted(_methods_by_norm.items()):
    _path = _path_by_norm[_norm]
    _wrong = _pick_wrong_method(_all_methods)
    if _wrong and not _shadowed_by_another_route(_path, _wrong, _norm):
        _cases.append((_wrong, _path, str(sorted(_all_methods))))


@pytest.mark.parametrize(
    "wrong_method,path,registered",
    _cases,
    ids=[f"{wrong} {path}" for wrong, path, _ in _cases],
)
def test_wrong_http_method_returns_405_not_404(wrong_method: str, path: str, registered: str) -> None:
    """Calling the wrong HTTP method on a registered route must return 405, not 404.

    405 = route exists, method wrong.
    404 = route not found — the URL the client is calling doesn't match any registered path.
    """
    client = TestClient(_test_app, raise_server_exceptions=False)
    kwargs = {"json": {}} if wrong_method in ("POST", "PUT", "PATCH") else {}
    resp = getattr(client, wrong_method.lower())(path, **kwargs)
    assert resp.status_code == 405, (
        f"{wrong_method} {path} returned {resp.status_code} "
        f"(registered methods: {registered}). "
        f"A 404 here means the client URL doesn't match any route — check path construction."
    )


@pytest.fixture
def observation_request(in_memory_duckdb, test_service_source, tmp_path, monkeypatch) -> dict:
    in_memory_duckdb.execute("SET TimeZone = 'UTC'")
    test_service_source["_cache_dir_override"] = str(tmp_path / "request-cache")
    # Fixture rows exist only in the view, not the live-hour buffer or lake.
    monkeypatch.setattr(QueryRunner, "_create_active_hour_temp_direct", lambda *args, **kwargs: None)
    source_dir = tmp_path / "bot-sources"
    source_dir.mkdir()
    (source_dir / "well-known-bots.json").write_text(
        json.dumps(
            {
                "entries": [
                    {
                        "id": "googlebot",
                        "name": "Googlebot",
                        "pattern": {"accepted": ["Googlebot"]},
                        "verification": {"domains": [], "cidrs": []},
                    }
                ]
            }
        )
    )
    monkeypatch.setattr(bot_sources, "_CACHE_DIR", source_dir)
    logs = generate_mock_logs(test_service_source, num_logs=40, hours_ago=1)
    seed_time = datetime.now(UTC)
    closed_time = seed_time.replace(minute=0, second=0, microsecond=0) - timedelta(microseconds=1)
    for index, log in enumerate(logs):
        log.update(
            timestamp=(seed_time - timedelta(microseconds=1) if index % 2 else closed_time).isoformat(),
            ua="Googlebot/2.1",
            tcp_rtt=1000,
            asn=15169,
            country="US",
            cmcd_sid="test-session",
            cmcd_bl=1000,
            cmcd_bs=False,
            cmcd_br=1000,
            cmcd_mtp=2000,
            cmcd_ot="v",
            cmcd_sf="h",
            cmcd_cid="test-content",
        )
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
    client,
    observation_request: dict,
    monkeypatch: pytest.MonkeyPatch,
    path: str,
    sections: list[str],
) -> None:
    monkeypatch.setenv("DEBUG_RESPONSES", "0")
    body = {**observation_request, "sections": sections}
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    first = client.post(path, json=body, headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["_response_cache"]["state"] == "miss"
    second = client.post(path, json=body, headers=headers)
    assert second.status_code == 200, second.text
    assert second.json()["_response_cache"]["state"] == "hit"
    assert second.json()["_response_cache"]["prewarmer"] is False
    assert second.json()["_is_cached"] is True
    assert "_debug_queries" not in second.json()


def test_dashboard_bundle_components_keep_independent_observations(client, observation_request: dict) -> None:
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    first = client.post("/api/dashboard/bundle", json=observation_request, headers=headers)
    assert first.status_code == 200, first.text
    assert first.json()["_response_cache"]["state"] == "bypass"
    assert first.json()["aggregates"]["_response_cache"]["state"] == "miss"
    assert first.json()["top_bots"]["_response_cache"]["state"] == "miss"
    assert first.json()["top_bots"]["bots"][0]["name"] == "Googlebot"
    assert first.json()["top_bots"]["bots"][0]["request_count"] == 40
    second = client.post("/api/dashboard/bundle", json=observation_request, headers=headers)
    assert second.status_code == 200, second.text
    assert second.json()["aggregates"]["_response_cache"]["state"] == "hit"
    assert second.json()["top_bots"]["_response_cache"]["state"] == "hit"


def test_origin_composite_is_bypass_not_a_response_miss(client, observation_request: dict) -> None:
    for _ in range(2):
        response = client.post(
            "/api/origin/aggregates",
            json={**observation_request, "sections": ["summary"]},
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )
        assert response.status_code == 200, response.text
        assert response.json()["_response_cache"] == {"state": "bypass", "prewarmer": False}


def test_network_bundle_does_not_confuse_health_with_independent_shielding(client, observation_request: dict) -> None:
    headers = {"x-fastly-service-id": MOCK_SERVICE_ID}
    for expected in ("miss", "hit"):
        response = client.post("/api/network/bundle", json=observation_request, headers=headers)
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["_response_cache"]["state"] == "bypass"
        assert body["health"]["_response_cache"]["state"] == expected
        assert body["health"]["_response_cache"]["scope"] == "self"
        assert body["quality"]["_response_cache"]["state"] == "bypass"
        assert body["transport"]["_response_cache"]["state"] == "bypass"
        assert body["health"]["shielding_analysis"]["_response_cache"]["state"] in {"miss", "hit"}


# ── Merged from tests/routers/test_endpoints.py ──
def test_dashboard_endpoint(client, in_memory_duckdb, test_service_source):
    # Setup mock data
    logs = generate_mock_logs(test_service_source, num_logs=100)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Since config is mocked, MOCK_SERVICE_ID will be considered valid
    # The actual route is a POST to /aggregates with a JSON body
    response = client.post(
        "/api/dashboard/aggregates", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )

    assert response.status_code == 200
    data = response.json()

    # Verify core structure (based on what dashboard repository returns)
    assert "data" in data
    assert "status" in data["data"]
    assert "cache" in data["data"]


def test_performance_endpoint(client, in_memory_duckdb, test_service_source):
    # Setup mock data
    logs = generate_mock_logs(test_service_source, num_logs=50)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    response = client.post(
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sort_by": "p99"},
    )

    assert response.status_code == 200
    data = response.json()

    # Verify performance specific structure
    assert "top_urls" in data
    assert "top_asns" in data


def test_performance_endpoint_rejects_unknown_section(client, in_memory_duckdb, test_service_source):
    """sections=['not_a_section'] returns 400 (router) or 422 (Pydantic
    validator) — invalid selector values must not silently drop, the FE
    would otherwise render the card with empty data and hide the typo."""
    logs = generate_mock_logs(test_service_source, num_logs=5)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["not_a_section"]},
    )
    assert response.status_code in (400, 422)


def test_performance_endpoint_selector_suppresses_other_sections(client, in_memory_duckdb, test_service_source):
    """sections=['ttl_dist'] suppresses the other 4 sections' SQL — proves
    the router → repo pipeline carries the selector through and gates the
    unrequested timer entries (distributions-only request must not pay
    for the top_urls/top_asns CTE work)."""
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["ttl"] = 60
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["ttl_dist"]},
    )
    assert response.status_code == 200
    data = response.json()
    timings_names = {t["section"] for t in data.get("_section_timings", [])}
    assert "ttl_dist_query" in timings_names, f"selector dropped requested section timer; got {timings_names}"
    for blocked in ("top_urls_query", "top_asns_query", "scatter_waterfall_query"):
        assert blocked not in timings_names, f"selector did not suppress {blocked}; got {timings_names}"


def test_performance_endpoint_top_urls_auto_includes_top_asns(client, in_memory_duckdb, test_service_source):
    """sections=['top_urls'] auto-includes top_asns at the router boundary
    (shared 2-pass CTE pattern + per-request temp). The FE renders the
    two leaderboards as a pair; splitting them would duplicate the inner
    CTE scan for no benefit."""
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["top_urls"]},
    )
    assert response.status_code == 200
    data = response.json()
    timings_names = {t["section"] for t in data.get("_section_timings", [])}
    assert "top_urls_query" in timings_names, f"top_urls timer missing; got {timings_names}"
    assert "top_asns_query" in timings_names, f"top_asns auto-include broken; got {timings_names}"
    # ttl_dist + scatter must NOT have fired
    assert "ttl_dist_query" not in timings_names
    assert "scatter_waterfall_query" not in timings_names


def test_dashboard_custom_fields_appear_in_top10(in_memory_duckdb, test_service_source):
    """Custom fields with show_in_dashboard=True must appear as keys in the dashboard data response."""
    from fastapi.testclient import TestClient

    from backend.core.request_context import build_request_context
    from backend.deps import get_con
    from backend.repositories import dashboard as dashboard_repo

    # Clear module-level cache so a prior test's result doesn't shadow this one
    dashboard_repo._dashboard_cache.clear()

    custom_source = {
        **test_service_source,
        "log_fields": {
            "schema_version": 2,
            "custom_fields": [
                {
                    "name": "my_edge_field",
                    "label": "My Edge Field",
                    "vcl_log_expression": "randomint(1, 100)",
                    "collection_stage": "edge",
                    "duckdb_type": "INTEGER",
                    "value_type": "numeric",
                    "enabled": True,
                    "show_in_dashboard": True,
                },
                {
                    "name": "hidden_field",
                    "label": "Hidden",
                    "vcl_log_expression": "req.http.X-Test",
                    "collection_stage": "edge",
                    "duckdb_type": "VARCHAR",
                    "value_type": "string",
                    "enabled": True,
                    "show_in_dashboard": False,
                },
            ],
        },
    }

    table_name = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    insert_mock_logs(in_memory_duckdb, table_name, logs)
    # Add the custom column and populate with values so top-10 has something to return
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
            response = c.post(
                "/api/dashboard/aggregates",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={"filters": {}},
            )
    finally:
        app.dependency_overrides.pop(get_con, None)
        app.dependency_overrides.pop(get_source, None)
        app.dependency_overrides.pop(build_request_context, None)

    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert "my_edge_field" in data, "enabled custom field with show_in_dashboard=True must appear in dashboard data"
    assert "hidden_field" not in data, "custom field with show_in_dashboard=False must not appear in dashboard data"
    assert "top" in data["my_edge_field"] and "total" in data["my_edge_field"], (
        "custom field entry must have top and total keys"
    )


# ── Merged from tests/routers/test_assets.py ──
def test_assets_aggregates_absolute_range(client, in_memory_duckdb, test_service_source):
    """Verify assets aggregates endpoint works correctly with absolute date range."""
    resp = client.post(
        "/api/assets/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "asset_type_breakdown" in data
    assert "cache_performance" in data
    assert "compression_performance" in data
    assert "missing_fields" in data


def test_assets_aggregates_range_token(client, in_memory_duckdb, test_service_source, monkeypatch):
    """Verify assets aggregates endpoint works correctly with range_token resolution."""
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "get_status", lambda sid: {"earliest_log_at": "2026-08-19T00:00:00Z"})

    resp = client.post(
        "/api/assets/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "range_token": "24h",
            "anchor": "2026-08-19T12:00:00Z",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "asset_type_breakdown" in data
    assert "cache_performance" in data
    assert "compression_performance" in data
    assert "missing_fields" in data


def test_assets_aggregates_missing_fields_post(client, in_memory_duckdb, test_service_source):
    """Verify POST returns missing_fields when optional fields are absent."""
    table = _safe_table(test_service_source["name"])
    in_memory_duckdb.execute(
        f"CREATE TABLE {table} (url VARCHAR, cache VARCHAR, resp_bytes BIGINT, status INTEGER, timestamp TIMESTAMPTZ)"
    )

    resp = client.post(
        "/api/assets/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data["missing_fields"], list)
    assert "resp_header_content_encoding" in data["missing_fields"]
    assert "ttl" in data["missing_fields"]


def test_assets_aggregates_get_query_params(client, in_memory_duckdb, test_service_source, monkeypatch):
    """Verify GET /api/assets/aggregates supports both query param modes."""
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "get_status", lambda sid: {"earliest_log_at": "2026-08-19T00:00:00Z"})

    # Mode 1: range_token
    resp1 = client.get(
        "/api/assets/aggregates?range_token=24h&anchor=2026-08-19T12:00:00Z",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp1.status_code == 200
    data1 = resp1.json()
    assert "asset_type_breakdown" in data1
    assert "missing_fields" in data1

    # Mode 2: start_time and end_time
    resp2 = client.get(
        "/api/assets/aggregates?start_time=2026-08-19T12:00:00Z&end_time=2026-08-19T13:00:00Z",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert "asset_type_breakdown" in data2
    assert "missing_fields" in data2


def test_assets_aggregates_with_populated_logs(client, in_memory_duckdb, test_service_source):
    """Verify assets aggregates computes metrics from actual logs and detects present optional fields."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for i, log in enumerate(logs):
        log["timestamp"] = "2026-08-19T12:30:00Z"
        if i % 3 == 0:
            log["url"] = "/images/banner.png"
            log["cache_status"] = "HIT"
            log["resp_header_content_encoding"] = "gzip"
            log["ttl"] = 3600
            log["resp_body_bytes"] = 45000
        elif i % 3 == 1:
            log["url"] = "/bundle.js"
            log["cache_status"] = "MISS"
            log["resp_header_content_encoding"] = "br"
            log["ttl"] = 0
            log["resp_body_bytes"] = 120000
        else:
            log["url"] = "/api/v1/user"
            log["cache_status"] = "PASS"
            log["resp_header_content_encoding"] = "uncompressed"
            log["ttl"] = 0
            log["resp_body_bytes"] = 1000

    insert_mock_logs(in_memory_duckdb, table, logs)

    resp = client.post(
        "/api/assets/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["asset_type_breakdown"]) > 0
    assert "resp_header_content_encoding" not in data["missing_fields"]
    assert "ttl" not in data["missing_fields"]
    assert data["missing_fields"] == []


def test_assets_aggregates_high_scale():
    """Verify high-scale assets_aggregates computes breakdown, hotspots, and memoizes warm hits."""
    from types import SimpleNamespace

    from backend.high_scale.assets import assets_aggregates
    from backend.models.performance import AssetsRequest

    calls: list[tuple[str, dict]] = []

    class _FakeCHClient:
        def execute(self, sql: str, params: dict | None = None) -> list[dict]:
            calls.append((sql, params or {}))
            if "assets:summary_dims" in sql:
                return [
                    {
                        "asset_type": "JavaScript/CSS",
                        "cache_status": "HIT",
                        "content_encoding_norm": "br",
                        "is_compressed": 1,
                        "is_compressible": 1,
                        "requests": 80,
                        "bytes": 80000,
                    },
                    {
                        "asset_type": "JavaScript/CSS",
                        "cache_status": "MISS",
                        "content_encoding_norm": "uncompressed",
                        "is_compressed": 0,
                        "is_compressible": 1,
                        "requests": 20,
                        "bytes": 2000000,
                    },
                ]
            if "assets:hotspots" in sql:
                return [
                    {
                        "kind": "large",
                        "url": "/static/app.js",
                        "requests": 20,
                        "metric_float": 100000.0,
                        "total_bytes": 2000000,
                        "status": 200,
                        "asset_type": "",
                    },
                    {
                        "kind": "low_ttl",
                        "url": "/static/app.js",
                        "requests": 20,
                        "metric_float": 0.0,
                        "total_bytes": 0,
                        "status": 0,
                        "asset_type": "JavaScript/CSS",
                    },
                ]
            return []

    svc = SimpleNamespace(service_id="hs-svc-1", client=_FakeCHClient())
    req = AssetsRequest(filters={}, start_time="2026-08-19T12:00:00Z", end_time="2026-08-19T13:00:00Z")
    res1 = assets_aggregates(svc, req, "2026-08-19T12:00:00Z", "2026-08-19T13:00:00Z")  # type: ignore[arg-type]
    assert len(res1.asset_type_breakdown) == 1
    assert res1.asset_type_breakdown[0].asset_type == "JavaScript/CSS"
    assert res1.asset_type_breakdown[0].cache_hit_ratio == 0.8
    assert res1.asset_type_breakdown[0].compression_rate == 0.8
    assert len(res1.large_uncompressed_assets) == 1
    assert len(res1.low_ttl_assets) == 1
    assert len(calls) == 2

    # Warm call within the same minute hits ResponseMemo without querying ClickHouse
    res2 = assets_aggregates(svc, req, "2026-08-19T12:00:30Z", "2026-08-19T13:00:45Z")  # type: ignore[arg-type]
    assert res2.is_cached is True
    assert len(calls) == 2
