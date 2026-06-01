from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


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


def test_origin_ts_endpoint_returns_timeseries(client, in_memory_duckdb, test_service_source):
    """Regression: /api/performance/origin-ts response must contain 'timeseries', not 'origin_ts'."""
    logs = generate_mock_logs(test_service_source, num_logs=20)
    for log in logs:
        log["ottfb"] = 50000  # 50ms in microseconds
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/performance/origin-ts",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"filters": {}, "origin_metric": "ttfb", "origin_percentile": "p95"},
    )

    assert response.status_code == 200, response.text
    data = response.json()
    assert "timeseries" in data, f"Expected 'timeseries' key in response, got: {list(data.keys())}"
    assert isinstance(data["timeseries"], list)
    assert len(data["timeseries"]) > 0


def test_security_aggregates_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/security/aggregates", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    assert "tls_fingerprints" in response.json()


def test_security_top_bots_endpoint(client, in_memory_duckdb, test_service_source):
    logs = generate_mock_logs(test_service_source, num_logs=30)
    insert_mock_logs(in_memory_duckdb, _safe_table(test_service_source["name"]), logs)

    response = client.post(
        "/api/security/top-bots", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, json={"filters": {}}
    )
    assert response.status_code == 200, response.text
    data = response.json()
    assert "bots" in data
    assert "ngwaf_bots" in data


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
    from backend.core import metadata_db

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
