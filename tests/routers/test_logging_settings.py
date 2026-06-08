import json
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app

client = TestClient(app)


@patch("backend.config.load_config")
@patch("backend.core.fastly.service.get_active_version")
@patch("backend.core.fastly.client.fastly")
@patch("backend.core.fastly.service.find_condition")
def test_logging_settings_extracts_custom_condition(
    mock_find_condition, mock_fastly, mock_get_active_version, mock_load_config
):
    """Verify that the GET endpoint extracts the custom_condition from the VCL statement."""
    mock_load_config.return_value = {
        "fastly_api_key": "test_token",
        "provisioning": {"endpoint_name": "Test Endpoint"},
    }
    mock_get_active_version.return_value = 1

    # Mock the Fastly S3 endpoint response
    mock_fastly.return_value = {
        "name": "Test Endpoint",
        "period": 120,
        "path": "/logs/raw/%Y-%m-%d/",
        "response_condition": "Log Sampling",
    }

    # Mock the condition statement containing a custom condition
    mock_find_condition.return_value = {
        "statement": '!segmented_caching.is_inner_req && (req.restarts == 0 && fastly.ff.visits_this_service == 0) && randombool(50, 100) && (std.tolower(req.url) !~ "\\.(jpg|png)$")'
    }

    response = client.get("/api/services/test-service/logging-settings")

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["period"] == 120
    assert data["sample_rate"] == 50
    assert data["edge_only"] is True
    assert data["custom_condition"] == 'std.tolower(req.url) !~ "\\.(jpg|png)$"'


@patch("backend.config.load_config")
@patch("backend.core.fastly.service.get_active_version")
@patch("backend.core.fastly.client.fastly")
@patch("backend.core.fastly.service.find_condition")
def test_logging_settings_custom_condition_fallback(
    mock_find_condition, mock_fastly, mock_get_active_version, mock_load_config
):
    """Verify that the GET endpoint uses the config fallback if the VCL condition doesn't have it."""
    mock_load_config.return_value = {
        "fastly_api_key": "test_token",
        "provisioning": {
            "endpoint_name": "Test Endpoint",
            "custom_condition": 'req.http.host == "example.com"',
        },
    }
    mock_get_active_version.return_value = 1

    # Mock FOS endpoint
    mock_fastly.return_value = {
        "name": "Test Endpoint",
        "period": 60,
        "path": "/logs/raw/",
        "response_condition": "Log Sampling",
    }

    # Mock the condition statement WITHOUT a custom condition
    mock_find_condition.return_value = {"statement": "!segmented_caching.is_inner_req && randombool(100, 100)"}

    response = client.get("/api/services/test-service/logging-settings")

    assert response.status_code == 200
    data = response.json()
    assert data["ok"] is True
    assert data["custom_condition"] == 'req.http.host == "example.com"'


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.provision.update_logging_endpoint")
@patch("backend.core.duckdb.get_source_for_service")
def test_update_logging_settings_passes_custom_condition(
    mock_get_source, mock_update, mock_save_config, mock_load_config
):
    """Verify that the UPDATE endpoint passes the custom_condition to the provisioning engine."""
    mock_get_source.return_value = None

    fake_disk_state = {
        "service_id": "test-service",
        "log_period": 60,
        "provisioning": {"sample_rate": 100, "edge_only": False},
    }

    def _dynamic_load(service_id):
        return json.loads(json.dumps(fake_disk_state))

    mock_load_config.side_effect = _dynamic_load

    def _dynamic_save(service_id, new_cfg):
        nonlocal fake_disk_state
        fake_disk_state = json.loads(json.dumps(new_cfg))

    mock_save_config.side_effect = _dynamic_save

    # Capture the update_cfg passed to update_logging_endpoint
    captured_cfg = {}

    def fake_generator(cfg, token):
        nonlocal captured_cfg
        captured_cfg = cfg
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "changed": True}

    mock_update.side_effect = fake_generator

    # Security: route moved from GET → POST/PATCH. Tests use POST.
    response = client.post(
        "/api/services/test-service/logging-settings/update",
        params={
            "period": 120,
            "sample_rate": 25,
            "edge_only": True,
            "custom_condition": 'req.http.x-test == "1"',
        },
    )

    assert response.status_code == 200

    # Verify the captured config
    assert captured_cfg["log_period"] == 120
    assert captured_cfg["sample_rate"] == 25
    assert captured_cfg["edge_only"] is True
    assert captured_cfg["custom_condition"] == 'req.http.x-test == "1"'

    # Verify it was saved to disk
    assert fake_disk_state["provisioning"]["custom_condition"] == 'req.http.x-test == "1"'
