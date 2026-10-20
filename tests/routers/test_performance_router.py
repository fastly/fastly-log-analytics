from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from backend.core.request_context import build_request_context
from backend.main import app
from backend.repositories._base import _safe_table
from backend.utils.remote_access import TimeBounds
from tests.conftest import MOCK_SERVICE_ID, override_request_context
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def _seed_performance_table(con, src) -> None:
    logs = generate_mock_logs(src, num_logs=20, hours_ago=1)
    for log in logs:
        log["elapsed"] = 120000
        log["ttfb"] = 0.040
        log["url"] = "/test-perf-url"
        log["asn"] = 12345
        log["cache"] = "HIT"
        log["ttl"] = 3600
        log["ottfb"] = 30000.0
        log["ottlb"] = 50000.0
    insert_mock_logs(con, _safe_table(src["name"]), logs)


def test_performance_aggregates_default(client, in_memory_duckdb, test_service_source):
    _seed_performance_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}},
    )
    assert resp.status_code == 200
    assert "top_urls" in resp.json()


def test_performance_aggregates_range_token(client, in_memory_duckdb, test_service_source):
    _seed_performance_table(in_memory_duckdb, test_service_source)
    with (
        patch("backend.config.get_status", return_value={"earliest_log_at": "2026-08-24T00:00:00Z"}),
        patch("backend.utils.time_window.is_valid_range_token", return_value=True),
        patch(
            "backend.utils.time_window.resolve_window", return_value=("2026-08-24T12:00:00Z", "2026-08-24T13:00:00Z")
        ),
    ):
        resp = client.post(
            "/api/performance/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "range_token": "30d", "anchor": "2026-08-24T13:00:00Z"},
        )
    assert resp.status_code == 200
    assert "top_urls" in resp.json()


def test_performance_aggregates_telemetry_attribution(client, in_memory_duckdb, test_service_source, monkeypatch):
    monkeypatch.setenv("DEBUG_RESPONSES", "1")
    _seed_performance_table(in_memory_duckdb, test_service_source)
    resp = client.post(
        "/api/performance/aggregates",
        headers={
            "x-fastly-service-id": MOCK_SERVICE_ID,
            "X-Page-Load-ID": "test-perf-page-load-42",
        },
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
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["top_urls"]},
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
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["waterfall"]},
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
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sections": ["ttl_dist"]},
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
    resp = client.post(
        "/api/performance/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "sort_by": sort_by},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "top_urls" in data
    assert len(data["top_urls"]) > 0


def test_performance_aggregates_analyst_session_clamps_window(client, in_memory_duckdb, test_service_source):
    start = datetime(2026, 6, 17, 9, 0, tzinfo=UTC)
    end = datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    session = SimpleNamespace(
        session_id="sess-analyst-perf",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )

    captured: dict = {}

    def _fake_get_performance_aggregates(**kw):
        captured.update(kw)
        return {
            "top_urls": [],
            "top_asns": [],
            "ttl_dist": [],
            "scatter": [],
            "waterfall": {},
        }

    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/performance/aggregates",
        time_bounds=TimeBounds(start=start, end=end),
    )
    try:
        with patch(
            "backend.repositories.performance.get_performance_aggregates",
            side_effect=_fake_get_performance_aggregates,
        ):
            resp = client.post(
                "/api/performance/aggregates",
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


def test_performance_aggregates_high_scale_dispatch(client, test_service_source):
    mock_service = MagicMock()
    mock_registry = MagicMock()
    mock_registry.resolve.return_value = mock_service

    fake_response = {
        "top_urls": [{"url": "/hs", "requests": 50, "avg": 10.0, "p50": 8.0, "p95": 15.0, "p99": 20.0}],
        "top_asns": [],
        "ttl_dist": [],
        "scatter": [],
        "waterfall": {},
        "_approx": True,
    }

    with (
        patch("backend.high_scale.registry.get_high_scale_service_registry", return_value=mock_registry),
        patch("backend.high_scale.performance.performance_aggregates", return_value=fake_response) as mock_hs_perf,
    ):
        resp = client.post(
            "/api/performance/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sort_by": "p99"},
        )
        assert resp.status_code == 200
        assert resp.json()["_approx"] is True
        mock_hs_perf.assert_called_once()


def test_performance_aggregates_high_scale_receives_expanded_sections(client, test_service_source):
    mock_registry = MagicMock()
    mock_registry.resolve.return_value = MagicMock()
    fake_response = {"top_urls": [], "top_asns": [], "ttl_dist": [], "scatter": [], "waterfall": {}}

    with (
        patch("backend.high_scale.registry.get_high_scale_service_registry", return_value=mock_registry),
        patch("backend.high_scale.performance.performance_aggregates", return_value=fake_response) as mock_hs_perf,
    ):
        resp = client.post(
            "/api/performance/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"filters": {}, "sort_by": "avg", "sections": ["scatter", "top_urls"]},
        )

    assert resp.status_code == 200
    assert mock_hs_perf.call_args.kwargs["sections"] == {"scatter", "waterfall", "top_urls", "top_asns"}
    assert mock_hs_perf.call_args.args[1].sort_by == "avg"
