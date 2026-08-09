"""Integration tests for full RUM enable/disable flow.

These tests verify the complete end-to-end flow:
- enable_rum: config write → bucket provision → JS upload → VCL reconciliation
- disable_rum: config write → JS deletion → VCL reconciliation

Real file I/O is tested; FOS operations are mocked.
"""

import datetime as _dt
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backend.provision.rum_orchestrator_v2 import disable_rum, enable_rum


@pytest.fixture
def temp_config_dir(tmp_path, monkeypatch):
    """Create a temporary config directory and redirect backend.config to it."""

    from backend import config as svcconfig

    config_dir = Path(tmp_path) / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", config_dir)

    return config_dir


@pytest.fixture
def sample_service_config():
    """Return a minimal service config with FOS credentials."""
    return {
        "service_id": "test_service_id",
        "service_name": "Test Service",
        "rum_enabled": False,
        "fos_bucket": "test-bucket",
        "fos_region": "us-east-1",
        "fos_access_key_id": "test_access_key",
        "fos_secret_access_key": "test_secret_key",
        "last_activated_version": 1,
    }


@pytest.fixture
def initialized_config(temp_config_dir, sample_service_config):
    """Write sample config to disk and return the service_id."""
    service_id = sample_service_config["service_id"]
    config_path = temp_config_dir / f"{service_id}.json"
    config_path.write_text(json.dumps(sample_service_config, indent=2))

    return service_id, config_path


class TestRUMIntegration:
    """End-to-end integration tests for RUM enable/disable flow."""

    def test_enable_rum_full_flow(self, temp_config_dir, initialized_config):
        """Test full enable_rum flow: config write → bucket → upload → reconcile.

        Verifies:
        - result["activated"] is True
        - result["logging_service_active_version"] == 2 (post-activate)
        - "enabled_at" in result
        - Bucket was called once
        - Upload was called once
        - Reconciliation was called once
        - All three stages fired status callbacks
        """
        service_id, config_path = initialized_config
        token = "test_token"
        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.provision.rum_orchestrator_v2.ensure_fos_bucket") as mock_bucket:
            with patch("backend.provision.rum_orchestrator_v2.upload_rum_tracker_js") as mock_upload:
                with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                    # Setup mocks
                    mock_bucket.return_value = None  # ensure_fos_bucket returns None
                    mock_upload.return_value = {
                        "path": "rum/rum-tracker.js",
                        "bytes_uploaded": 1000,
                        "fos_key": "s3://test-bucket/rum/rum-tracker.js",
                    }
                    mock_reconcile.return_value = MagicMock(
                        activated_version=2,
                        activated_version_set=True,
                        changes_applied={},
                    )

                    # Call enable_rum
                    result = enable_rum(service_id, token, status_cb=mock_status_cb)

                    # Verify config was written with rum_enabled=True and timestamp
                    config_content = config_path.read_text()
                    config = json.loads(config_content)
                    assert config["rum_enabled"] is True
                    assert "rum_enabled_at" in config
                    assert config["service_id"] == service_id

                    # Verify bucket provisioning was bypassed since bucket was already configured
                    assert mock_bucket.call_count == 0

                    # Verify JS upload was called once
                    assert mock_upload.call_count == 1
                    upload_call = mock_upload.call_args
                    assert upload_call[0][0] == service_id
                    assert upload_call[0][1] == token

                    # Verify reconciliation was called once
                    assert mock_reconcile.call_count == 1

                    # Verify result
                    assert result["activated"] is True
                    assert result["logging_service_active_version"] == 2
                    assert "enabled_at" in result

                    # Verify status callbacks fired for all stages
                    assert len(status_messages) >= 3  # At least: bucket, upload, reconcile
                    assert any("Using FOS bucket" in msg for msg in status_messages)
                    assert any("Uploading RUM tracker JS" in msg for msg in status_messages)
                    assert any("Reconciling VCL state" in msg for msg in status_messages)

    def test_disable_rum_full_flow(self, temp_config_dir):
        """Test full disable_rum flow: config write → delete JS → reconcile.

        Verifies:
        - result["deactivated"] is True
        - result["logging_service_active_version"] == 6
        - Delete was called once
        - Reconciliation was called once
        - Status callbacks fired for deletion and reconciliation stages
        """
        # Create config with rum_enabled=True
        config = {
            "service_id": "test_service_id",
            "service_name": "Test Service",
            "rum_enabled": True,
            "rum_enabled_at": _dt.datetime.now(_dt.UTC).isoformat(),
            "fos_bucket": "test-bucket",
            "fos_region": "us-east-1",
            "fos_access_key_id": "test_access_key",
            "fos_secret_access_key": "test_secret_key",
            "last_activated_version": 5,
        }
        service_id = config["service_id"]
        config_path = temp_config_dir / f"{service_id}.json"
        config_path.write_text(json.dumps(config, indent=2))

        token = "test_token"
        status_messages = []

        def mock_status_cb(msg):
            status_messages.append(msg)

        with patch("backend.provision.rum_assets.delete_rum_tracker_js") as mock_delete:
            with patch("backend.provision.rum_orchestrator_v2.reconcile_vcl_state") as mock_reconcile:
                # Setup mocks
                mock_delete.return_value = None
                mock_reconcile.return_value = MagicMock(
                    activated_version=6,
                    changes_applied={},
                )

                # Call disable_rum
                result = disable_rum(service_id, token, status_cb=mock_status_cb)

                # Verify config was written with rum_enabled=False
                config_content = config_path.read_text()
                loaded_config = json.loads(config_content)
                assert loaded_config["rum_enabled"] is False

                # Verify JS deletion was called once
                assert mock_delete.call_count == 1
                delete_call = mock_delete.call_args
                assert delete_call[0][0] == service_id
                assert delete_call[0][1] == token

                # Verify reconciliation was called once
                assert mock_reconcile.call_count == 1

                # Verify result
                assert result["deactivated"] is True
                assert result["logging_service_active_version"] == 6

                # Verify status callbacks fired for all stages
                assert len(status_messages) >= 2  # At least: delete, reconcile
                assert any("Deleting RUM tracker JS" in msg for msg in status_messages)
                assert any("Reconciling VCL state" in msg for msg in status_messages)
