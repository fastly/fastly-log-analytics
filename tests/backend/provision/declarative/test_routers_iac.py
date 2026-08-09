import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.main import app


@pytest.fixture
def mock_config_dir(tmp_path, monkeypatch):
    """Isolate configurations in a temporary directory."""
    config_dir = tmp_path / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr("backend.config.CONFIGS_DIR", config_dir)

    from pathlib import Path as RealPath

    def mock_path(path_str):
        if "configs/" in path_str:
            parts = path_str.split("/")
            return config_dir / parts[-1]
        return RealPath(path_str)

    monkeypatch.setattr("backend.provision.declarative.iac_reconciler.Path", mock_path)
    return config_dir


class TestRoutersIaC:
    @patch("backend.routers.session_scoring._resolve_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_scoring_enable_route(self, mock_reconcile, mock_resolve, mock_config_dir):
        """Verify POST /api/services/{service_id}/scoring/enable works with the declarative loop."""
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {"enabled": False, "scoring_domain": "scoring.example.com"},
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(cfg))

        client = TestClient(app)
        response = client.post(
            "/api/services/srv_123/scoring/enable",
            json={"token": "test_token"},
        )

        assert response.status_code == 200
        # Wait, since it returns an EventSourceResponse, let's parse the SSE stream lines
        lines = [line for line in response.iter_lines() if line]
        assert any("Enabling session" in line for line in lines)

        # Check config mutated on disk
        updated_cfg = svcconfig.load_config("srv_123")
        assert updated_cfg["scoring"]["enabled"] is True

        # Verify reconcile_infrastructure called
        mock_reconcile.assert_called_once()
        args, kwargs = mock_reconcile.call_args
        assert args[0] == "srv_123"
        assert args[1] == "fake_token"

    @patch("backend.routers.session_scoring._resolve_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_scoring_disable_route(self, mock_reconcile, mock_resolve, mock_config_dir):
        """Verify POST /api/services/{service_id}/scoring/disable works with the declarative loop."""
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {"enabled": True, "scoring_domain": "scoring.example.com"},
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(cfg))

        client = TestClient(app)
        response = client.post(
            "/api/services/srv_123/scoring/disable",
            json={"token": "test_token"},
        )

        assert response.status_code == 200
        lines = [line for line in response.iter_lines() if line]
        assert any("Disabling session" in line for line in lines)

        # Check config mutated on disk
        updated_cfg = svcconfig.load_config("srv_123")
        assert updated_cfg["scoring"]["enabled"] is False

        # Verify reconcile_infrastructure called
        mock_reconcile.assert_called_once()
        args, kwargs = mock_reconcile.call_args
        assert args[0] == "srv_123"
        assert args[1] == "fake_token"

    @patch("backend.routers.cmcd_admin._resolve_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_cmcd_enable_route(self, mock_reconcile, mock_resolve, mock_config_dir):
        """Verify POST /api/services/{service_id}/cmcd/enable works with the declarative loop."""
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(cfg))

        client = TestClient(app)
        response = client.post(
            "/api/services/srv_123/cmcd/enable",
            json={"token": "test_token", "mode": "headers", "version": 2},
        )

        assert response.status_code == 200
        lines = [line for line in response.iter_lines() if line]
        assert any("Enabling CMCD" in line for line in lines)

        # Check config mutated on disk
        updated_cfg = svcconfig.load_config("srv_123")
        assert updated_cfg["cmcd"]["enabled"] is True
        assert updated_cfg["cmcd"]["mode"] == "headers"
        assert updated_cfg["cmcd"]["version"] == 2

        # Verify reconcile_infrastructure called
        mock_reconcile.assert_called_once()
        args, kwargs = mock_reconcile.call_args
        assert args[0] == "srv_123"
        assert args[1] == "fake_token"

    @patch("backend.routers.cmcd_admin._resolve_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_cmcd_disable_route(self, mock_reconcile, mock_resolve, mock_config_dir):
        """Verify POST /api/services/{service_id}/cmcd/disable works with the declarative loop."""
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(cfg))

        client = TestClient(app)
        response = client.post(
            "/api/services/srv_123/cmcd/disable",
            json={"token": "test_token"},
        )

        assert response.status_code == 200
        lines = [line for line in response.iter_lines() if line]
        assert any("Disabling CMCD" in line for line in lines)

        # Check config mutated on disk
        updated_cfg = svcconfig.load_config("srv_123")
        assert updated_cfg["cmcd"]["enabled"] is False

        # Verify reconcile_infrastructure called
        mock_reconcile.assert_called_once()
        args, kwargs = mock_reconcile.call_args
        assert args[0] == "srv_123"
        assert args[1] == "fake_token"
