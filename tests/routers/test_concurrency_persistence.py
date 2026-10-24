import json
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend import config
from backend.main import app
from tests._helpers.config_fakes import mutate_via_save

client = TestClient(app)


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.provision.update_logging_endpoint")
@patch("backend.core.duckdb.get_source_for_service")
def test_long_running_endpoint_preserves_background_changes(
    mock_get_source, mock_update, mock_save_config, mock_load_config
):
    """
    Regression test: Ensure that long-running streaming endpoints
    reload the config before saving, so they don't overwrite changes
    made by other requests during the stream.
    """
    mock_get_source.return_value = None

    # This dictionary simulates our database/disk state
    fake_disk_state = {
        "service_id": "test-service",
        "name": "Initial Name",
        "log_period": 60,
        "provisioning": {"sample_rate": 100, "edge_only": False},
    }

    # load_config will always return the CURRENT state of fake_disk_state
    def _dynamic_load(service_id):
        return json.loads(json.dumps(fake_disk_state))

    mock_load_config.side_effect = _dynamic_load

    def _dynamic_save(service_id, new_cfg):
        nonlocal fake_disk_state
        fake_disk_state = json.loads(json.dumps(new_cfg))

    mock_save_config.side_effect = _dynamic_save

    # We mock the long-running generator
    def fake_generator(cfg, token):
        yield {"type": "status", "message": "starting"}

        # SIMULATE BACKGROUND CHANGE HAPPENING MID-STREAM
        # We write directly to our "disk state" to simulate another worker saving
        fake_disk_state["name"] = "Updated Background Name"

        yield {"type": "done", "changed": True}

    mock_update.side_effect = fake_generator

    # Call the endpoint (security: GET → POST)
    with patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save_config)):
        response = client.post(
            "/api/services/test-service/logging-settings/update",
            params={"period": 3600, "sample_rate": 50, "edge_only": True},
        )

    assert response.status_code == 200

    # Let's read what the final "disk" state looks like
    assert fake_disk_state["log_period"] == 3600
    assert fake_disk_state["provisioning"]["sample_rate"] == 50
    assert fake_disk_state["provisioning"]["edge_only"] is True

    # Crucially, the background change must NOT have been overwritten
    assert fake_disk_state["name"] == "Updated Background Name", (
        "The background change was overwritten by a stale config!"
    )


def test_get_catalog():
    response = client.get("/api/log-fields/catalog")
    assert response.status_code == 200
    data = response.json()

    groups = data["groups"]
    group_ids = [g["id"] for g in groups]
    assert "METRICS" in group_ids

    fields = data["fields"]
    field_ids = [f["id"] for f in fields]

    required_metrics = [
        "requests",
        "hit_rate",
        "5xx",
        "4xx",
        "p50_latency",
        "p95_latency",
        "p99_latency",
        "throughput",
        "req_size",
        "ttfb_ms",
    ]
    for rm in required_metrics:
        assert rm in field_ids, f"Metric {rm} missing from catalog"
        field = next(f for f in fields if f["id"] == rm)
        assert field["group"] == "METRICS"

    hit_rate = next(f for f in fields if f["id"] == "hit_rate")
    assert hit_rate["formatter"] == "percent"
    assert hit_rate["unit"] == "%"


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


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.core.duckdb.get_source_for_service")
def test_custom_fields_persist_on_log_fields_update(mock_get_source, mock_save_config, mock_load_config):
    """
    Regression test: Ensure that updating standard log fields (groups)
    does not overwrite/delete existing custom fields in the shared config state.
    """
    mock_get_source.return_value = None  # Skip DB queries

    mock_load_config.return_value = {
        "log_fields": {
            "groups": ["A", "B"],
            "custom_fields": [
                {"name": "my_custom_field", "label": "My Custom Field", "collection_stage": "edge", "enabled": True}
            ],
        }
    }

    with patch("backend.config.mutate_config", side_effect=mutate_via_save(mock_save_config)):
        response = client.post(
            "/api/services/test-service/log-fields",
            json={"log_fields": {"groups": ["A", "B", "C"], "field_overrides": {}}},
        )

    assert response.status_code == 200

    mock_save_config.assert_called_once()
    saved_cfg = mock_save_config.call_args[0][1]

    assert "my_custom_field" in [cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]], (
        "Custom fields were overwritten during save!"
    )
    assert saved_cfg["log_fields"].get("schema_version") == 2, "schema_version was lost during save!"

    read_lf = saved_cfg["log_fields"]
    assert len(read_lf.get("custom_fields", [])) == 1, "Custom fields were wiped during save!"
