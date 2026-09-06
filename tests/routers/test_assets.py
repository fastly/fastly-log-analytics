"""HTTP-level smoke tests for the assets router.

Each ``/api/assets/*`` endpoint is a thin wrapper around the
corresponding repo function.
"""

from __future__ import annotations

from tests.conftest import MOCK_SERVICE_ID


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
