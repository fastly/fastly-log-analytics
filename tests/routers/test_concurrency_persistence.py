import json
from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app

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
