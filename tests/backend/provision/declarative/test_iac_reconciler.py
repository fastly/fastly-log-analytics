"""Unit and integration tests for the Declarative Multi-Service IaC Engine."""

import json
from unittest.mock import MagicMock, patch

import pytest

from backend.provision.declarative.iac_reconciler import (
    DeclarativeComputeService,
    build_desired_state,
    discover_current_state,
    reconcile_infrastructure,
    verify_compute_scorer_readiness,
)


@pytest.fixture
def mock_config_dir(tmp_path, monkeypatch):
    """Create a temporary configs/ directory and mock config loading."""
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(exist_ok=True)

    monkeypatch.setattr("backend.config.CONFIGS_DIR", configs_dir)
    return configs_dir


class TestIaCReconciler:
    """Test suite for unified multi-service Declarative IaC controller."""

    @patch("backend.provision.declarative.iac_reconciler._SCORER_PACKAGE")
    @patch("backend.provision.session_scoring_orchestrator._resolve_tenant_matrix_for_deploy")
    def test_build_desired_state_scoring_disabled(self, mock_resolve_matrix, mock_scorer_pkg, mock_config_dir):
        """Verify desired state construction when session scoring is disabled."""
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {"enabled": False},
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(cfg))

        desired = build_desired_state("srv_123", "token")

        assert desired.service_id == "srv_123"
        assert desired.compute_scorer is None
        assert len(desired.kv_stores) == 0
        assert desired.vcl_service.name == "Fastly Log Analytics"

    @patch("backend.scoring.matrix.serialize_kv")
    @patch("backend.provision.session_scoring_orchestrator._resolve_tenant_matrix_for_deploy")
    def test_build_desired_state_scoring_enabled(
        self, mock_resolve_matrix, mock_serialize_kv, mock_config_dir, tmp_path
    ):
        """Verify desired state construction when session scoring is enabled."""
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {
                "enabled": True,
                "scoring_domain": "fos-srv123-session-scorer.edgecompute.app",
            },
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(cfg))

        # Create mock matrix file
        matrix_file = tmp_path / "matrix_srv_123.json"
        matrix_file.write_text(json.dumps({"weights": [1, 2, 3]}))
        mock_resolve_matrix.return_value = matrix_file
        mock_serialize_kv.return_value = b"serialized-matrix"

        desired = build_desired_state("srv_123", "token")

        assert desired.service_id == "srv_123"
        assert desired.compute_scorer is not None
        assert desired.compute_scorer.name == "Session Scoring Service for srv_123"
        assert desired.compute_scorer.domains == ["fos-srv123-session-scorer.edgecompute.app"]
        assert "scoring_matrix_srv_123" in desired.kv_stores
        assert desired.kv_stores["scoring_matrix_srv_123"].keys["matrix"] == b"serialized-matrix".hex()

    @patch("backend.provision.declarative.iac_reconciler._find_kv_store")
    @patch("backend.provision.declarative.iac_reconciler.find_service_by_name")
    def test_discover_current_state(self, mock_find_service, mock_find_kv):
        """Verify discovery of active live edge resources on Fastly account."""
        mock_find_kv.return_value = {"id": "kv_abc", "name": "scoring_matrix_srv_123"}
        mock_find_service.return_value = {"id": "compute_123", "name": "Session Scoring Service for srv_123"}

        current = discover_current_state("srv_123", "token")

        assert "scoring_matrix_srv_123" in current["kv_stores"]
        assert current["kv_stores"]["scoring_matrix_srv_123"]["id"] == "kv_abc"
        assert current["compute_scorer"]["id"] == "compute_123"

    @patch("httpx.get")
    @patch("time.sleep")
    def test_verify_readiness_probe_retries_and_succeeds(self, mock_sleep, mock_get):
        """Verify readiness probe retries on failure and succeeds on success code."""
        compute_def = DeclarativeComputeService(
            name="test-scorer",
            package_path="path/to/pkg",
            domains=["test-scorer.edgecompute.app"],
        )

        # Mock failure then success responses
        mock_response_fail = MagicMock()
        mock_response_fail.status_code = 502
        mock_response_ok = MagicMock()
        mock_response_ok.status_code = 401  # auth check returned 401, which signifies route is hot

        mock_get.side_effect = [RuntimeError("connection error"), mock_response_fail, mock_response_ok]
        status_cb = MagicMock()

        verify_compute_scorer_readiness(compute_def, "token", status_cb=status_cb)
        assert mock_get.call_count == 3
        status_cb.assert_any_call("✅ Compute Scorer is hot (status 401).")

    @patch("backend.provision.declarative.iac_reconciler.reconcile_vcl_state")
    @patch("backend.provision.declarative.iac_reconciler.verify_compute_scorer_readiness")
    @patch("backend.provision.declarative.iac_reconciler._deploy_wasm_package")
    @patch("backend.provision.declarative.iac_reconciler._write_matrix_to_kv")
    @patch("backend.provision.declarative.iac_reconciler.ensure_scoring_service")
    @patch("backend.provision.declarative.iac_reconciler.discover_current_state")
    def test_reconcile_infrastructure_enables_scoring_transactional_success(
        self,
        mock_discover,
        mock_ensure_scoring_svc,
        mock_write_matrix,
        mock_deploy_wasm,
        mock_verify_readiness,
        mock_reconcile_vcl,
        mock_config_dir,
    ):
        """Verify successful scoring enablement pipeline with transactional update."""
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {
                "enabled": True,
                "scoring_domain": "fos-srv123-session-scorer.edgecompute.app",
            },
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(cfg))

        mock_discover.return_value = {"kv_stores": {}, "compute_scorer": None}
        mock_ensure_scoring_svc.return_value = {
            "scoring_service_id": "compute_srv_id",
            "scoring_matrix_store_id": "matrix_kv_id",
            "scoring_domain": "fos-srv123-session-scorer.edgecompute.app",
        }
        mock_deploy_wasm.return_value = {
            "sha": "wasm_sha_abc",
            "files_hash": "wasm_files_hash_xyz",
        }

        status_cb = MagicMock()
        reconcile_infrastructure("srv_123", "token", dry_run=False, status_cb=status_cb)

        # Config should be updated with all meta from Fastly API
        updated_cfg = json.loads(config_file.read_text())
        assert updated_cfg["scoring"]["enabled"] is True
        assert updated_cfg["scoring"]["scoring_service_id"] == "compute_srv_id"
        assert updated_cfg["scoring"]["deployed_package_sha"] == "wasm_sha_abc"

        mock_verify_readiness.assert_called_once()
        mock_reconcile_vcl.assert_called_once_with("srv_123", "token", dry_run=False, status_cb=status_cb)

    @patch("backend.provision.declarative.iac_reconciler.reconcile_vcl_state")
    @patch("backend.provision.declarative.iac_reconciler.ensure_scoring_service")
    @patch("backend.provision.declarative.iac_reconciler.discover_current_state")
    def test_reconcile_infrastructure_transactional_rollback_on_failure(
        self,
        mock_discover,
        mock_ensure_scoring_svc,
        mock_reconcile_vcl,
        mock_config_dir,
    ):
        """Verify transaction rollback: restores original config file if downstream steps fail."""
        original_cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {
                "enabled": True,
                "scoring_domain": "fos-srv123-session-scorer.edgecompute.app",
            },
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(original_cfg))

        mock_discover.return_value = {"kv_stores": {}, "compute_scorer": None}
        # Simulate Fastly API failure during service provisioning
        mock_ensure_scoring_svc.side_effect = RuntimeError("Fastly API rate limit 429")

        status_cb = MagicMock()
        with pytest.raises(RuntimeError, match="Fastly API rate limit 429"):
            reconcile_infrastructure("srv_123", "token", dry_run=False, status_cb=status_cb)

        # File should be rolled back to its exact pre-deployment content
        assert json.loads(config_file.read_text()) == original_cfg

    @patch("backend.provision.declarative.iac_reconciler.delete_scoring_service")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_vcl_state")
    @patch("backend.provision.declarative.iac_reconciler.discover_current_state")
    def test_reconcile_infrastructure_disables_scoring_reverse_topological_teardown(
        self,
        mock_discover,
        mock_reconcile_vcl,
        mock_delete_scoring_svc,
        mock_config_dir,
    ):
        """Verify reverse-topological teardown: strips VCL first, sleep/drains 5s, then deletes Compute & KV."""
        original_cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {
                "enabled": False,
                "scoring_service_id": "compute_srv_id",
                "scoring_keys_store_id": "keys_store_id",
                "scoring_config_store_id": "cfg_store_id",
            },
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(original_cfg))

        mock_discover.return_value = {
            "kv_stores": {"scoring_matrix_srv_123": {"id": "matrix_store_id"}},
            "compute_scorer": {"id": "compute_srv_id", "name": "Session Scoring Service for srv_123"},
        }

        status_cb = MagicMock()

        # Capture sleep duration to verify drain
        with patch("time.sleep") as mock_sleep:
            reconcile_infrastructure("srv_123", "token", dry_run=False, status_cb=status_cb)
            mock_sleep.assert_called_once_with(5.0)

        # VCL reconciliation should be triggered (this deactivates the restart hooks and scoring backend first)
        mock_reconcile_vcl.assert_called_once_with("srv_123", "token", dry_run=False, status_cb=status_cb)

        # Compute service and its associated Config/KV stores should be deleted last
        mock_delete_scoring_svc.assert_called_once_with(
            scoring_service_id="compute_srv_id",
            scoring_keys_store_id="keys_store_id",
            scoring_config_store_id="cfg_store_id",
            scoring_matrix_store_id="matrix_store_id",
            token="token",
            status_cb=status_cb,
        )

        # Config should be updated with scoring block cleaned up
        updated_cfg = json.loads(config_file.read_text())
        assert "scoring" not in updated_cfg
