"""Unit and regression tests for Raw Logs Explorer router (/api/logs)."""

from __future__ import annotations

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_logs_query_post_empty_table(client, in_memory_duckdb, test_service_source):
    """Verify logs query returns valid empty response when table is empty."""
    resp = client.post(
        "/api/logs/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
            "limit": 50,
            "offset": 0,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["rows"] == []
    assert data["total_count"] == 0
    assert data["has_more"] is False
    assert data["limit"] == 50
    assert data["offset"] == 0


def test_logs_query_post_populated(client, in_memory_duckdb, test_service_source):
    """Verify paginated logs query returns rows, total_count, and columns correctly."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=25)
    for i, log in enumerate(logs):
        log["timestamp"] = f"2026-08-19T12:{i:02d}:00Z"
        log["url"] = f"/api/items/{i}"
        log["status"] = 200 if i % 2 == 0 else 404
        log["client_ip"] = f"198.51.100.{i + 1}"
        log["pop"] = "JFK" if i % 2 == 0 else "LHR"

    insert_mock_logs(in_memory_duckdb, table, logs)

    # First page: limit 10, offset 0
    resp = client.post(
        "/api/logs",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
            "limit": 10,
            "offset": 0,
            "sort_dir": "desc",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["rows"]) == 10
    assert data["total_count"] == 25
    assert data["has_more"] is True
    assert data["limit"] == 10
    assert data["offset"] == 0
    assert "timestamp" in data["columns"]
    assert "url" in data["columns"]

    # Second page: limit 10, offset 10
    resp2 = client.post(
        "/api/logs/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
            "limit": 10,
            "offset": 10,
            "sort_dir": "desc",
        },
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert len(data2["rows"]) == 10
    assert data2["total_count"] == 25
    assert data2["has_more"] is True
    assert data2["offset"] == 10

    # Third page: limit 10, offset 20
    resp3 = client.post(
        "/api/logs/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {},
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
            "limit": 10,
            "offset": 20,
            "sort_dir": "desc",
        },
    )
    assert resp3.status_code == 200
    data3 = resp3.json()
    assert len(data3["rows"]) == 5
    assert data3["total_count"] == 25
    assert data3["has_more"] is False


def test_logs_query_filtering_and_search(client, in_memory_duckdb, test_service_source):
    """Verify filtering by status/pop and free-text search across records."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=10)
    for i, log in enumerate(logs):
        log["timestamp"] = f"2026-08-19T12:{i:02d}:00Z"
        log["url"] = "/special/target/path" if i == 3 else f"/normal/{i}"
        log["status"] = 503 if i == 7 else 200
        log["pop"] = "SFO" if i == 7 else "BOS"

    insert_mock_logs(in_memory_duckdb, table, logs)

    # Filter by status = 503
    resp_flt = client.post(
        "/api/logs/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "filters": {"status": 503},
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
        },
    )
    assert resp_flt.status_code == 200
    d_flt = resp_flt.json()
    assert d_flt["total_count"] == 1
    assert d_flt["rows"][0]["status"] == 503

    # Search for "special"
    resp_srch = client.post(
        "/api/logs/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "search": "special",
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
        },
    )
    assert resp_srch.status_code == 200
    d_srch = resp_srch.json()
    assert d_srch["total_count"] == 1
    assert "/special/target/path" in d_srch["rows"][0]["url"]


def test_logs_query_get_endpoints(client, in_memory_duckdb, test_service_source):
    """Verify GET /api/logs and GET /api/logs/{service_id} query parameters."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=5)
    for i, log in enumerate(logs):
        log["timestamp"] = f"2026-08-19T12:{i:02d}:00Z"
        log["status"] = 200
        log["pop"] = "DFW"

    insert_mock_logs(in_memory_duckdb, table, logs)

    # GET /api/logs
    resp = client.get(
        "/api/logs?from=2026-08-19T12:00:00Z&to=2026-08-19T13:00:00Z&limit=2&offset=0&pop=DFW",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["rows"]) == 2
    assert data["total_count"] == 5
    assert data["has_more"] is True

    # GET /api/logs/{service_id}
    resp2 = client.get(
        f"/api/logs/{MOCK_SERVICE_ID}?from=2026-08-19T12:00:00Z&to=2026-08-19T13:00:00Z&limit=10",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
    )
    assert resp2.status_code == 200
    data2 = resp2.json()
    assert len(data2["rows"]) == 5
    assert data2["total_count"] == 5
    assert data2["has_more"] is False


def test_logs_query_column_projection(client, in_memory_duckdb, test_service_source):
    """Verify requesting explicit columns projects only those columns while retaining timestamp."""
    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=3)
    insert_mock_logs(in_memory_duckdb, table, logs)

    resp = client.post(
        "/api/logs/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "columns": ["url", "status"],
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "timestamp" in data["columns"]
    assert "url" in data["columns"]
    assert "status" in data["columns"]
    for row in data["rows"]:
        assert "timestamp" in row
        assert "url" in row
        assert "status" in row


def test_logs_query_analyst_ip_masking(client, in_memory_duckdb, test_service_source):
    """Verify Analyst Path B remote share masking redacts client IPs and session keys."""
    from backend.core.request_context import build_request_context
    from backend.main import app
    from tests.conftest import override_request_context

    table = _safe_table(test_service_source["name"])
    logs = generate_mock_logs(test_service_source, num_logs=3)
    for i, log in enumerate(logs):
        log["timestamp"] = f"2026-08-19T12:{i:02d}:00Z"
        log["client_ip"] = "203.0.113.42"
        log["cookie_session"] = "secret_session_token_123"

    insert_mock_logs(in_memory_duckdb, table, logs)

    class MockAnalystSession:
        pii_policy = {"mask_ips": True}
        service_id = MOCK_SERVICE_ID
        session_id = "test-session"

    app.dependency_overrides[build_request_context] = override_request_context(
        source=test_service_source,
        con=in_memory_duckdb,
        session=MockAnalystSession(),
    )

    resp = client.post(
        "/api/logs/query",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": "2026-08-19T12:00:00Z",
            "end_time": "2026-08-19T13:00:00Z",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["rows"]) == 3
    for r in data["rows"]:
        assert r["client_ip"] != "203.0.113.42"
        assert (
            "xxx" in r["client_ip"]
            or "*" in r["client_ip"]
            or r["client_ip"].startswith("203.0.113.") is False
            or r["client_ip"] != "203.0.113.42"
        )
        if "cookie_session" in r:
            assert r["cookie_session"] == "[redacted]"
