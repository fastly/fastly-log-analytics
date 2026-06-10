from backend.deps import get_source
from backend.main import app
from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


def test_dashboard_endpoint(client, in_memory_duckdb, test_service_source):
    # Setup mock data
    logs = generate_mock_logs(test_service_source, num_logs=100)
    table_name = _safe_table(test_service_source["name"])
    insert_mock_logs(in_memory_duckdb, table_name, logs)

    # Call the API endpoint
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
    assert "latency_ts" in data


def test_dashboard_custom_fields_appear_in_top10(in_memory_duckdb, test_service_source):
    """Custom fields with show_in_dashboard=True must appear as keys in the dashboard data response."""
    from fastapi.testclient import TestClient

    from backend.deps import get_con, get_con
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
    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: custom_source
    try:
        with TestClient(app) as c:
            response = c.post(
                "/api/dashboard/aggregates",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
                json={"filters": {}},
            )
    finally:
        app.dependency_overrides.pop(get_con, None)
        app.dependency_overrides.pop(get_con, None)
        app.dependency_overrides.pop(get_source, None)

    assert response.status_code == 200, response.text
    data = response.json()["data"]
    assert "my_edge_field" in data, "enabled custom field with show_in_dashboard=True must appear in dashboard data"
    assert "hidden_field" not in data, "custom field with show_in_dashboard=False must not appear in dashboard data"
    assert "top" in data["my_edge_field"] and "total" in data["my_edge_field"], (
        "custom field entry must have top and total keys"
    )
