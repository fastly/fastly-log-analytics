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


def test_content_security_route_registered():
    from backend.main import app

    assert "post" in app.openapi()["paths"]["/api/cmcd/content-security"]


def test_content_security_passes_content_id(client, in_memory_duckdb, test_service_source):
    mock_ret = {
        "available": True,
        "fields": {"host": True},
        "content_id": "movie-1",
        "top_hosts": [{"value": "pirate.example", "requests": 3, "bytes": 3000}],
        "bandwidth_ts": [{"bucket": "2026-08-19 12:00:00", "edge_bytes": 10, "shield_bytes": 2}],
    }
    with patch("backend.repositories.content_security.get_content_security", return_value=mock_ret) as mock_get:
        resp = client.post(
            "/api/cmcd/content-security",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={
                "filters": {},
                "start_time": "2026-08-19T12:00:00Z",
                "end_time": "2026-08-19T13:00:00Z",
                "content_id": "movie-1",
            },
        )
    assert resp.status_code == 200
    data = resp.json()
    assert data["top_hosts"][0]["value"] == "pirate.example"
    assert data["bandwidth_ts"][0]["shield_bytes"] == 2
    assert mock_get.call_args.kwargs["content_id"] == "movie-1"
    assert mock_get.call_args.kwargs["top_n"] == 10


def test_content_security_rejects_oversized_content_id(client):
    resp = client.post(
        "/api/cmcd/content-security",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "content_id": "x" * 513},
    )
    assert resp.status_code == 422


_SUBSCRIBERS_RET = {
    "available": True,
    "fields": {"token_subscriber_id": True},
    "top_subscribers": [
        {"subscriber_id": "acct-123", "distinct_ips": 9, "requests": 40},
        {"subscriber_id": "acct-456", "distinct_ips": 2, "requests": 80},
    ],
}


def _post_content_security(c):
    return c.post(
        "/api/cmcd/content-security",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "start_time": "2026-08-19T12:00:00Z", "end_time": "2026-08-19T13:00:00Z"},
    )


def test_content_security_admin_sees_raw_subscriber_ids(client):
    with patch("backend.repositories.content_security.get_content_security", return_value=_SUBSCRIBERS_RET):
        data = _post_content_security(client).json()

    assert [r["subscriber_id"] for r in data["top_subscribers"]] == ["acct-123", "acct-456"]
    assert data["subscribers_masked"] is False


def test_content_security_analyst_gets_rank_labels(client, in_memory_duckdb, test_service_source):
    """Subscriber ids are per-subscriber PII: analysts see ranks, counts intact,
    and the repo's (role-shared) cached rows are not mutated."""
    from types import SimpleNamespace

    from backend.core.request_context import build_request_context
    from backend.main import app
    from tests.conftest import override_request_context

    session = SimpleNamespace(pii_policy={}, service_ids=[test_service_source["service_id"]])
    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source, _con_override=in_memory_duckdb, session=session
    )
    try:
        with patch("backend.repositories.content_security.get_content_security", return_value=_SUBSCRIBERS_RET):
            data = _post_content_security(client).json()
    finally:
        app.dependency_overrides.pop(build_request_context, None)

    assert data["top_subscribers"] == [
        {"subscriber_id": "Subscriber #1", "distinct_ips": 9, "requests": 40},
        {"subscriber_id": "Subscriber #2", "distinct_ips": 2, "requests": 80},
    ]
    assert data["subscribers_masked"] is True
    assert _SUBSCRIBERS_RET["top_subscribers"][0]["subscriber_id"] == "acct-123"
