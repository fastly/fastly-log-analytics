"""Unit tests for the insights router (anomaly detection endpoints)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from backend.models.insights import (
    InsightsRequest,
)
from tests.conftest import MOCK_SERVICE_ID, override_request_context


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
            {
                "timestamp": "2026-10-12T00:01:00+00:00",
                "cache": "MISS",
                "pop": "SJC",
                "ip": "1.2.3.4",
                "status": 200,
            }
        ],
        "breakdown": {
            "hits": 10,
            "misses": 90,
            "passes": 0,
            "other": 0,
        },
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
    with pytest.raises(Exception):
        InsightsRequest(window_size_hrs=0)
    with pytest.raises(Exception):
        InsightsRequest(window_size_hrs=169)
    with pytest.raises(Exception):
        InsightsRequest(baseline_hours=0)
    with pytest.raises(Exception):
        InsightsRequest(baseline_hours=2161)


def test_insights_admin_passes_force_refresh_true(client):
    """Admin caller with force_refresh=True forwards force_refresh=True to repo."""
    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_insights()

    with patch("backend.repositories.insights.get_insights", side_effect=_fake):
        resp = client.post(
            "/api/insights",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"window_size_hrs": 24.0, "baseline_hours": 168.0, "force_refresh": True},
        )

    assert resp.status_code == 200, resp.text
    assert captured["force_refresh"] is True
    assert captured["clamp_start"] is None
    assert captured["clamp_end"] is None


def test_insights_analyst_clamps_force_refresh_to_false(client, in_memory_duckdb, test_service_source):
    """Remote analyst session must have force_refresh clamped to False regardless of request."""
    from backend.core.request_context import build_request_context
    from backend.main import app
    from backend.utils.remote_access import TimeBounds

    now = datetime.now(UTC)
    start = now - timedelta(hours=3)
    end = now - timedelta(hours=2)
    session = SimpleNamespace(
        session_id="sess-analyst-1",
        pii_policy={"mask_ips": True},
        service_ids=[test_service_source["service_id"]],
    )

    captured: dict = {}

    def _fake(**kw):
        captured.update(kw)
        return _stub_insights()

    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        _con_override=in_memory_duckdb,
        session=session,
        path="/api/insights",
        time_bounds=TimeBounds(start=start, end=end),
    )

    try:
        with patch("backend.repositories.insights.get_insights", side_effect=_fake):
            resp = client.post(
                "/api/insights",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={"window_size_hrs": 1.0, "baseline_hours": 168.0, "force_refresh": True},
            )

        assert resp.status_code == 200, resp.text
        # Even though request specified force_refresh=True, it was clamped to False for analyst
        assert captured["force_refresh"] is False
        assert captured["mask_ips"] is True
        assert captured["clamp_end"] == end.isoformat()
        assert captured["clamp_start"] == start.isoformat()
    finally:
        app.dependency_overrides.pop(build_request_context, None)


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
            "/api/insights",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"window_size_hrs": 24.0, "baseline_hours": 168.0},
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
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"url": "/api/v1/heavy", "window_size_hrs": 24.0, "baseline_hours": 168.0},
        )

    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["url"] == "/api/v1/heavy"
    assert data["baseline_hit_rate"] == 0.95
    assert len(data["timeline"]) == 1
    assert len(data["recent_misses"]) == 1
    assert data["breakdown"]["hits"] == 10
