"""HTTP-level smoke and regression tests for the assets router.

Each ``/api/assets/*`` endpoint wraps ``backend.repositories.assets``
with tenancy, stale view retry, and RequestContext scoping.
"""

from __future__ import annotations

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


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
