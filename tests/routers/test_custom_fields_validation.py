from fastapi.testclient import TestClient

from backend import config
from backend.main import app

client = TestClient(app)


def test_api_create_custom_field_validation_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)

    svc_id = "test_svc_validation"
    cfg = {"service_id": svc_id, "log_fields": {"schema_version": 2, "groups": ["A"], "custom_fields": []}}
    config.save_config(svc_id, cfg)

    # 1. Invalid name
    payload = {
        "name": "INVALID NAME!",
        "label": "Invalid",
        "description": "",
        "vcl_log_expression": "req.http.Host",
        "collection_stage": "edge",
        "duckdb_type": "VARCHAR",
        "value_type": "string",
        "bytes_estimate": 20,
        "nullable": True,
        "enabled": True,
        "show_in_dashboard": False,
        "show_in_logs": True,
        "filterable": True,
    }

    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422
    assert "detail" in resp.json()

    # 2. Invalid VCL expression (semicolon)
    payload["name"] = "valid_name"
    payload["vcl_log_expression"] = "req.http.Host;"
    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422
    err_json = resp.json()
    assert "errors" in err_json["detail"]
    assert any("semicolon" in e for e in err_json["detail"]["errors"])

    # 3. Create a valid one
    payload["vcl_log_expression"] = "req.http.Host"
    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 200

    # 4. Try to create duplicate
    resp = client.post(f"/api/services/{svc_id}/custom-fields", json=payload)
    assert resp.status_code == 422
    err_json = resp.json()
    assert "errors" in err_json["detail"]
    assert any("already exists" in e for e in err_json["detail"]["errors"])
