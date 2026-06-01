from unittest.mock import patch

from fastapi.testclient import TestClient

from backend.main import app

client = TestClient(app)


@patch("backend.config.load_config")
@patch("backend.config.save_config")
@patch("backend.core.duckdb.get_source_for_service")
def test_custom_fields_persist_on_log_fields_update(mock_get_source, mock_save_config, mock_load_config):
    """
    Regression test: Ensure that updating standard log fields (groups)
    does not overwrite/delete existing custom fields in the shared config state.
    """
    mock_get_source.return_value = None  # Skip DB queries

    # Initial state: has some groups and a custom field
    mock_load_config.return_value = {
        "log_fields": {
            "groups": ["A", "B"],
            "custom_fields": [
                {"name": "my_custom_field", "label": "My Custom Field", "collection_stage": "edge", "enabled": True}
            ],
        }
    }

    # Simulate UI sending updated groups (but NO custom_fields)
    response = client.post(
        "/api/services/test-service/log-fields", json={"log_fields": {"groups": ["A", "B", "C"], "field_overrides": {}}}
    )

    assert response.status_code == 200

    # Verify that save_config was called with the custom_fields preserved
    mock_save_config.assert_called_once()
    saved_cfg = mock_save_config.call_args[0][1]

    assert "my_custom_field" in [cf["name"] for cf in saved_cfg["log_fields"]["custom_fields"]], (
        "Custom fields were overwritten during save!"
    )
    assert saved_cfg["log_fields"].get("schema_version") == 2, "schema_version was lost during save!"

    # Verify the "read path": custom_fields must survive a direct read (no migration needed).
    read_lf = saved_cfg["log_fields"]
    assert len(read_lf.get("custom_fields", [])) == 1, "Custom fields were wiped during save!"
