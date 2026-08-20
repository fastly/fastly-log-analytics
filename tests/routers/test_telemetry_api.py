from fastapi.testclient import TestClient

from backend.main import app


def test_page_telemetry_endpoint(monkeypatch, tmp_path):
    from backend.core.metadata import usage_log_db

    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "demo_service")

    # Pre-populate some dummy entries
    from backend.core.metadata import usage_log

    usage_log.log_telemetry_query("demo_service", "page-456", "req-789", "duckdb", "SELECT 42", 12.3)

    client = TestClient(app)
    response = client.get("/api/debug/page-telemetry?page_load_id=page-456")
    assert response.status_code == 200
    data = response.json()
    assert "queries" in data
    assert data["queries"][0]["sql_query"] == "SELECT 42"
