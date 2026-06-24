from backend.deps import get_source
from backend.main import app
from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID, override_request_context
from tests.utils.mock_data import generate_mock_logs, insert_mock_logs


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
        source=custom_source, con=in_memory_duckdb, path="/api/dashboard/aggregates"
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
