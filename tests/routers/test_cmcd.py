"""HTTP-level smoke tests for the CMCD router.

Each ``/api/cmcd/*`` endpoint is a thin wrapper around the corresponding repo function.
"""

from __future__ import annotations

from unittest.mock import patch

from tests.conftest import MOCK_SERVICE_ID


def test_cmcd_aggregates_absolute_range(client, in_memory_duckdb, test_service_source):
    """Verify CMCD aggregates endpoint works correctly with absolute date range."""
    # We patch the repo call because it requires custom tables and complex schema,
    # and we just want to verify the HTTP layer and router logic.
    mock_ret = {
        "available": True,
        "has_data": True,
        "overview": {"sessions": 10},
    }
    with patch("backend.repositories.cmcd.get_cmcd_aggregates", return_value=mock_ret) as mock_get:
        resp = client.post(
            "/api/cmcd/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "start_time": "2026-08-19T12:00:00Z",
                "end_time": "2026-08-19T13:00:00Z",
                "sections": ["overview"],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is True
        assert data["overview"] == {"sessions": 10}
        mock_get.assert_called_once()


def test_cmcd_aggregates_range_token(client, in_memory_duckdb, test_service_source, monkeypatch):
    """Verify CMCD aggregates endpoint works correctly with range_token resolution."""
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "get_status", lambda sid: {"earliest_log_at": "2026-08-19T00:00:00Z"})

    mock_ret = {
        "available": True,
        "has_data": False,
    }
    with patch("backend.repositories.cmcd.get_cmcd_aggregates", return_value=mock_ret) as mock_get:
        resp = client.post(
            "/api/cmcd/aggregates",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "range_token": "last_24h",
                "anchor": "2026-08-19T12:00:00Z",
                "sections": ["bitrate_ts"],
            },
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["available"] is True
        assert data["has_data"] is False
        mock_get.assert_called_once()
