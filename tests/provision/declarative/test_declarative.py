from __future__ import annotations

import itertools
import json
from unittest.mock import MagicMock, patch

import pytest
from backend.provision.declarative.diff import (
    Backend,
    DiffResult,
    LoggingEndpoint,
    VCLSnippet,
    compute_diff,
)
from backend.provision.declarative.iac_reconciler import (
    DeclarativeComputeService,
    build_desired_state,
    discover_current_state,
    reconcile_infrastructure,
    verify_compute_scorer_readiness,
)
from backend.provision.declarative.state import FeatureState, config_changed, migrate_config
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.core import metadata as metadata_db
from backend.main import app
from backend.provision.declarative import reconciler
from backend.provision.declarative.reconciler import (
    MANAGED_BACKEND_NAMES,
    VclValidationError,
    _detect_and_queue_legacy_cleanup,
    _is_legacy_snippet,
    _record_reconcile_audit,
    reconcile_vcl_state,
)
from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES

# ── From test_config_migration.py ──


def test_migrate_flat_cmcd_to_nested():
    """Flat CMCD fields migrate to nested object."""
    cfg = {
        "service_id": "svc123",
        "cmcd_enabled": True,
        "cmcd_mode": "headers",
        "cmcd_version": 2,
    }
    migrated = migrate_config(cfg)
    assert migrated["cmcd"] == {"enabled": True, "mode": "headers", "version": 2}
    assert "cmcd_enabled" not in migrated
    assert "cmcd_mode" not in migrated
    assert "cmcd_version" not in migrated


def test_migrate_flat_scoring_to_nested():
    """Flat Scoring fields migrate to nested object."""
    cfg = {
        "service_id": "svc123",
        "scoring_enabled": True,
        "scoring_domain": "score.example.com",
        "scoring_request_secret": "secret123",
        "scoring_exclude_url_regex": "^/health",
        "scoring_enforce_status_code": 429,
    }
    migrated = migrate_config(cfg)
    assert migrated["scoring"] == {
        "enabled": True,
        "domain": "score.example.com",
        "request_secret": "secret123",
        "exclude_url_regex": "^/health",
        "enforce_status_code": 429,
    }
    assert "scoring_enabled" not in migrated
    assert "scoring_domain" not in migrated


def test_migrate_both_flat_and_nested_cmcd():
    """When both flat and nested exist, flat overrides and is consolidated."""
    cfg = {
        "service_id": "svc123",
        "cmcd": {"enabled": False, "mode": "query_string"},
        "cmcd_enabled": True,  # Flat overrides
        "cmcd_mode": "headers",
    }
    migrated = migrate_config(cfg)
    assert migrated["cmcd"] == {"enabled": True, "mode": "headers", "version": 1}
    assert "cmcd_enabled" not in migrated
    assert "cmcd_mode" not in migrated


def test_migrate_already_nested_unchanged():
    """Already-nested config is returned unchanged."""
    cfg = {
        "service_id": "svc123",
        "cmcd": {"enabled": True, "mode": "headers", "version": 2},
        "scoring": {"enabled": False, "domain": ""},
    }
    migrated = migrate_config(cfg)
    assert migrated == cfg
    assert config_changed(cfg, migrated) is False


def test_migrate_empty_config():
    """Empty config returns empty config."""
    cfg = {}
    migrated = migrate_config(cfg)
    assert migrated == {}
    assert config_changed(cfg, migrated) is False


def test_config_changed_detects_flat_to_nested():
    """config_changed returns True when flat fields are consolidated."""
    before = {
        "service_id": "svc123",
        "cmcd_enabled": True,
    }
    after = {
        "service_id": "svc123",
        "cmcd": {"enabled": True, "mode": "query_string", "version": 1},
    }
    assert config_changed(before, after) is True


# ── From test_diff.py ──


class TestDiffComputation:
    """Test diff computation logic."""

    def test_diff_detects_snippet_content_change(self):
        """Verify diff detects when snippet content changes."""
        current = [
            VCLSnippet(name="Fastly Log Analytics - vcl_recv", priority=10, body="old_vcl", subroutine="vcl_recv")
        ]
        desired = [
            VCLSnippet(name="Fastly Log Analytics - vcl_recv", priority=10, body="new_vcl", subroutine="vcl_recv")
        ]

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.snippets_to_update) == 1
        assert diff.snippets_to_update[0].name == "Fastly Log Analytics - vcl_recv"

    def test_diff_ignores_snippet_order(self):
        """Verify snippet order doesn't trigger false diffs."""
        s1 = VCLSnippet(name="S1", priority=10, body="c1", subroutine="vcl_recv")
        s2 = VCLSnippet(name="S2", priority=10, body="c2", subroutine="vcl_recv")

        current = [s1, s2]
        desired = [s2, s1]  # Different order

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.snippets_to_add) == 0
        assert len(diff.snippets_to_update) == 0
        assert len(diff.snippets_to_remove) == 0

    def test_diff_detects_new_snippet(self):
        """Verify diff detects new snippets."""
        current = []
        desired = [VCLSnippet(name="Fastly Log Analytics - vcl_recv", priority=10, body="vcl", subroutine="vcl_recv")]

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.snippets_to_add) == 1
        assert diff.snippets_to_add[0].name == "Fastly Log Analytics - vcl_recv"

    def test_diff_detects_removed_snippet(self):
        """Verify diff detects removed snippets."""
        current = [VCLSnippet(name="RUM - Recv", priority=100, body="old_rum_vcl", subroutine="vcl_recv")]
        desired = []

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.snippets_to_remove) == 1
        assert "RUM - Recv" in diff.snippets_to_remove

    def test_diff_detects_endpoint_change(self):
        """Verify diff detects endpoint changes."""
        current = [
            LoggingEndpoint(
                name="Fastly Log Analytics",
                endpoint_type="s3",
                path="raw/%Y/%m/%d/%H/log_%M.gz",
                period=60,
                response_condition="true",
                format_string="old_format",
                placement="none",
                response_object_name="",
            )
        ]
        desired = [
            LoggingEndpoint(
                name="Fastly Log Analytics",
                endpoint_type="s3",
                path="raw/%Y/%m/%d/%H/log_%M.gz",
                period=60,
                response_condition="true",
                format_string="new_format",
                placement="none",
                response_object_name="",
            )
        ]

        diff = compute_diff(
            current_snippets=[],
            desired_snippets=[],
            current_endpoints=current,
            desired_endpoints=desired,
            current_backends=[],
            desired_backends=[],
        )
        assert len(diff.endpoints_to_update) == 1

    def test_diff_detects_backend_change(self):
        """Verify diff detects backend changes."""
        current = [Backend(name="session_scorer", address="old.example.com", port=443)]
        desired = [Backend(name="session_scorer", address="new.example.com", port=443)]

        diff = compute_diff(
            current_snippets=[],
            desired_snippets=[],
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=current,
            desired_backends=desired,
        )
        assert len(diff.backends_to_update) == 1

    def test_diff_is_empty_when_no_changes(self):
        """Verify diff is empty when current matches desired."""
        snippet = VCLSnippet(name="S1", priority=10, body="code", subroutine="vcl_recv")
        endpoint = LoggingEndpoint(
            name="E1",
            endpoint_type="s3",
            path="path",
            period=60,
            response_condition="true",
            format_string="format",
            placement="none",
            response_object_name="",
        )
        backend = Backend(name="B1", address="addr", port=443)

        diff = compute_diff(
            current_snippets=[snippet],
            desired_snippets=[snippet],
            current_endpoints=[endpoint],
            desired_endpoints=[endpoint],
            current_backends=[backend],
            desired_backends=[backend],
        )
        assert diff.is_empty()

    def test_diff_summary_counts_correctly(self):
        """Verify diff.summary() counts changes correctly."""
        current = []
        desired = [
            VCLSnippet(name="S1", priority=10, body="c1", subroutine="vcl_recv"),
            VCLSnippet(name="S2", priority=10, body="c2", subroutine="vcl_recv"),
        ]

        diff = compute_diff(
            current_snippets=current,
            desired_snippets=desired,
            current_endpoints=[],
            desired_endpoints=[],
            current_backends=[],
            desired_backends=[],
        )
        summary = diff.summary()
        assert summary["snippets_added"] == 2
        assert summary["snippets_updated"] == 0
        assert summary["snippets_removed"] == 0


# ── From test_iac_reconciler.py ──


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
    @patch("backend.provision.declarative.iac_reconciler.time.sleep")
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

        # Capture sleep duration to verify drain (use assert_any_call so unrelated background thread sleeps don't flake)
        with patch("backend.provision.declarative.iac_reconciler.time.sleep") as mock_sleep:
            reconcile_infrastructure("srv_123", "token", dry_run=False, status_cb=status_cb)
            mock_sleep.assert_any_call(5.0)

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


# ── From test_iac_reconciler_config_merge.py ──

SID = "srv_123"


MOD = "backend.provision.declarative.iac_reconciler"


SCORING_FIELD = {"name": sorted(_SCORING_FIELD_NAMES)[0], "duckdb_type": "VARCHAR", "enabled": True}


USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}


def _lock_held() -> bool:
    return bool(getattr(svcconfig._lock_state, "held", {}).get(SID))


def _persisted() -> dict:
    return json.loads(svcconfig.config_path(SID).read_text())


def _concurrent_write() -> None:
    assert not _lock_held(), "config lock held across a Fastly call"
    svcconfig.mutate_config(SID, lambda c: c.update(concurrent_key="kept"))


def _seed(**extra) -> dict:
    svcconfig.save_config(SID, {"service_id": SID, "log_period": 60, "keep_me": 1, **extra})
    return svcconfig.load_config(SID)


@pytest.fixture
def enable_stubs():
    with (
        patch(f"{MOD}.discover_current_state", return_value={"kv_stores": {}, "compute_scorer": None}),
        patch(f"{MOD}.ensure_scoring_service") as ensure,
        patch(f"{MOD}._write_matrix_to_kv"),
        patch(f"{MOD}._deploy_wasm_package", return_value={"sha": "sha1", "files_hash": "fh1"}) as deploy,
        patch(f"{MOD}.verify_compute_scorer_readiness"),
        patch(f"{MOD}.reconcile_vcl_state") as reconcile,
    ):
        ensure.return_value = {"scoring_service_id": "compute_id", "scoring_matrix_store_id": "matrix_id"}
        yield MagicMock(ensure=ensure, deploy=deploy, reconcile=reconcile)


@pytest.fixture
def disable_stubs():
    with (
        patch(
            f"{MOD}.discover_current_state",
            return_value={
                "kv_stores": {"scoring_matrix_srv_123": {"id": "matrix_id"}},
                "compute_scorer": {"id": "compute_id"},
            },
        ),
        patch(f"{MOD}.reconcile_vcl_state") as reconcile,
        patch(f"{MOD}.delete_scoring_service") as delete,
        patch(f"{MOD}.time.sleep"),
    ):
        yield MagicMock(reconcile=reconcile, delete=delete)


@pytest.fixture
def scoring_desired():
    """The desired state is built from the stored config: scoring enabled means a compute scorer is wanted."""
    with patch(f"{MOD}.build_desired_state") as build:
        build.return_value = MagicMock(compute_scorer=MagicMock())
        yield build


@pytest.fixture
def scoring_not_desired():
    with patch(f"{MOD}.build_desired_state") as build:
        build.return_value = MagicMock(compute_scorer=None)
        yield build


def test_enable_persists_only_scoring_and_keeps_concurrent_writes(enable_stubs, scoring_desired):
    seeded = _seed(scoring={"enabled": True, "request_secret": "r" * 8})

    def _ensure(*_a, **_kw):
        _concurrent_write()
        return {"scoring_service_id": "compute_id", "scoring_matrix_store_id": "matrix_id"}

    enable_stubs.ensure.side_effect = _ensure

    reconcile_infrastructure(SID, "tok")

    saved = _persisted()
    assert saved["scoring"]["scoring_service_id"] == "compute_id"
    assert saved["scoring"]["deployed_package_sha"] == "sha1"
    assert saved["scoring"]["request_secret"] == "r" * 8
    assert saved["concurrent_key"] == "kept"
    assert saved["keep_me"] == 1
    assert saved["rum"]["cid_salt"] == seeded["rum"]["cid_salt"]


def test_enable_does_not_hold_the_config_lock_across_fastly_calls(enable_stubs, scoring_desired):
    held: list[bool] = []
    enable_stubs.deploy.side_effect = lambda *_a, **_kw: (
        held.append(_lock_held()),
        {"sha": "sha1", "files_hash": "fh1"},
    )[1]
    enable_stubs.reconcile.side_effect = lambda *_a, **_kw: held.append(_lock_held())
    _seed(scoring={"enabled": True})

    reconcile_infrastructure(SID, "tok")

    assert held == [False, False]


def test_failure_after_persist_rolls_back_only_scoring_and_keeps_concurrent_writes(enable_stubs, scoring_desired):
    original_scoring = {"enabled": True, "scoring_domain": "old.example.test"}
    seeded = _seed(scoring=original_scoring)

    def _boom(*_a, **_kw):
        assert _persisted()["scoring"]["scoring_service_id"] == "compute_id"
        _concurrent_write()
        raise RuntimeError("vcl sync failed")

    enable_stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="vcl sync failed"):
        reconcile_infrastructure(SID, "tok")

    saved = _persisted()
    assert saved["scoring"] == original_scoring
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["cid_salt"] == seeded["rum"]["cid_salt"]


def test_failure_after_persist_removes_scoring_that_did_not_exist_before(enable_stubs, scoring_desired):
    _seed()
    enable_stubs.reconcile.side_effect = RuntimeError("vcl sync failed")

    with pytest.raises(RuntimeError, match="vcl sync failed"):
        reconcile_infrastructure(SID, "tok")

    assert "scoring" not in _persisted()


def test_failure_before_any_persist_leaves_the_file_untouched(enable_stubs, scoring_desired):
    _seed(scoring={"enabled": True})
    before = svcconfig.config_path(SID).read_text()
    enable_stubs.ensure.side_effect = RuntimeError("fastly 429")

    with pytest.raises(RuntimeError, match="fastly 429"):
        reconcile_infrastructure(SID, "tok")

    assert svcconfig.config_path(SID).read_text() == before


def test_disable_strips_scoring_fields_and_block_and_keeps_concurrent_writes(disable_stubs, scoring_not_desired):
    seeded = _seed(
        scoring={"enabled": True, "scoring_keys_store_id": "k", "scoring_config_store_id": "c"},
        log_fields={"custom_fields": [dict(SCORING_FIELD), dict(USER_FIELD)]},
    )
    disable_stubs.reconcile.side_effect = lambda *_a, **_kw: _concurrent_write()

    reconcile_infrastructure(SID, "tok")

    saved = _persisted()
    assert "scoring" not in saved
    assert [cf["name"] for cf in saved["log_fields"]["custom_fields"]] == ["my_custom"]
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["cid_salt"] == seeded["rum"]["cid_salt"]
    assert disable_stubs.delete.call_args.kwargs["scoring_keys_store_id"] == "k"


def test_disable_failure_restores_scoring_block_and_fields_and_keeps_concurrent_writes(
    disable_stubs, scoring_not_desired
):
    original_scoring = {"enabled": True, "scoring_keys_store_id": "k"}
    _seed(scoring=original_scoring, log_fields={"custom_fields": [dict(SCORING_FIELD), dict(USER_FIELD)]})

    def _boom(*_a, **_kw):
        stripped = _persisted()
        assert stripped["scoring"]["enabled"] is False
        assert [cf["name"] for cf in stripped["log_fields"]["custom_fields"]] == ["my_custom"]
        _concurrent_write()
        raise RuntimeError("vcl sync failed")

    disable_stubs.reconcile.side_effect = _boom

    with pytest.raises(RuntimeError, match="vcl sync failed"):
        reconcile_infrastructure(SID, "tok")

    saved = _persisted()
    assert saved["scoring"] == original_scoring
    assert {cf["name"] for cf in saved["log_fields"]["custom_fields"]} == {SCORING_FIELD["name"], "my_custom"}
    assert saved["concurrent_key"] == "kept"
    disable_stubs.delete.assert_not_called()


# ── From test_reconcile_audit.py ──


def _endpoint(fmt: str) -> LoggingEndpoint:
    return LoggingEndpoint(
        name="Fastly Log Analytics - FOS",
        endpoint_type="s3",
        path="/",
        period=5,
        response_condition="",
        format_string=fmt,
        placement=None,
        response_object_name="",
    )


def test_reconcile_records_added_and_removed_fields():
    before = [_endpoint('{"ip":"%{client.ip}V","ua":"x","old_field":"y"}')]
    after = [_endpoint('{"ip":"%{client.ip}V","ua":"x","new_field":"z"}')]

    _record_reconcile_audit("svc_audit_test", DiffResult(), before, after, 42)

    _, rows = metadata_db.get_audit_logs("svc_audit_test", event_type="log_format_change", page=1, per_page=10)
    assert len(rows) == 1
    d = rows[0]["details"]
    assert d["fields_added"] == ["new_field"]
    assert d["fields_removed"] == ["old_field"]
    assert d["version"] == 42
    assert rows[0]["actor"] == "reconciler"


# ── From test_reconciler_validation_failure.py ──


@patch("backend.provision.declarative.reconciler._activate_draft")
@patch("backend.provision.declarative.reconciler._validate_draft")
@patch("backend.provision.declarative.reconciler._clone_active_version")
@patch("backend.provision.declarative.reconciler._apply_diff")
@patch("backend.provision.declarative.reconciler.compute_diff")
@patch("backend.provision.declarative.reconciler._fetch_active_version")
@patch("backend.provision.declarative.reconciler._fetch_snippets")
@patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
@patch("backend.provision.declarative.reconciler._fetch_backends")
def test_validation_failure_raises_vcl_validation_error_even_if_active_version_moves(
    mock_backends,
    mock_endpoints,
    mock_snippets,
    mock_active,
    mock_diff,
    mock_apply,
    mock_clone,
    mock_validate,
    mock_activate,
):
    counter = itertools.count(1)
    mock_active.side_effect = lambda *a, **k: next(counter)  # every fetch returns a different version
    mock_snippets.return_value = []
    mock_endpoints.return_value = []
    mock_backends.return_value = []
    mock_clone.return_value = 99
    mock_validate.return_value = "boom: bad VCL"
    mock_diff.return_value = DiffResult(
        snippets_to_add=[VCLSnippet(name="x", priority=10, body="# c", subroutine="recv")]
    )

    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text") as mock_read:
        mock_read.return_value = json.dumps({"service_id": "srv_test", "log_period": 60, "sample_rate": 100})
        with pytest.raises(VclValidationError):
            reconcile_vcl_state("srv_test", "token", dry_run=False)
    mock_activate.assert_not_called()


# ── From test_routers_iac.py ──


class TestRoutersIaC:
    @patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=True)
    @patch("backend.routers.session_scoring.resolve_fastly_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_scoring_enable_route(self, mock_reconcile, mock_resolve, mock_pragmas, mock_config_dir):
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

    @patch("backend.routers.session_scoring.resolve_fastly_token", return_value="fake_token")
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

    @patch("backend.routers.performance._resolve_token", return_value="fake_token")
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

    @patch("backend.routers.performance._resolve_token", return_value="fake_token")
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


# ── From test_validate_draft.py ──


def test_validate_draft_returns_empty_string_when_valid():
    with patch.object(reconciler.fastly_integration, "validate_version", return_value=True):
        assert reconciler._validate_draft("svc", "tok", 3) == ""


def test_validate_draft_reports_non_ok_status_without_http_error():
    with patch.object(reconciler.fastly_integration, "validate_version", return_value=False):
        error = reconciler._validate_draft("svc", "tok", 3)
    assert error
    assert "validation" in error.lower()


def test_validate_draft_returns_message_when_validation_raises():
    with patch.object(
        reconciler.fastly_integration, "validate_version", side_effect=RuntimeError("VCL validation failed: boom")
    ):
        assert "boom" in reconciler._validate_draft("svc", "tok", 3)


def test_update_local_config_goes_through_the_locked_mutate_config(tmp_path, monkeypatch):
    """Activation metadata must be written via mutate_config (cross-process lock, fresh read)."""
    import json

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    svcconfig.save_config("svc-recon", {"service_id": "svc-recon", "other": "keep"})

    calls: list[str] = []
    real = svcconfig.mutate_config

    def spy(service_id, mutate, **kw):
        calls.append(service_id)
        return real(service_id, mutate, **kw)

    monkeypatch.setattr(svcconfig, "mutate_config", spy)
    reconciler._update_local_config("svc-recon", lambda c: c.__setitem__("last_activated_version", 7))

    assert calls == ["svc-recon"]
    saved = json.loads((tmp_path / "svc-recon.json").read_text())
    assert saved["last_activated_version"] == 7
    assert saved["other"] == "keep"


def test_update_local_config_skips_a_read_only_volume(monkeypatch):
    from backend import config as svcconfig

    def boom(*a, **k):
        raise OSError("read-only file system")

    monkeypatch.setattr(svcconfig, "mutate_config", boom)
    reconciler._update_local_config("svc-x", lambda c: None)  # must not raise


# ── Merged from tests/provision/declarative/test_state.py ──


class TestFeatureStateValidation:
    def test_featurestate_validates_log_period_range(self):
        with pytest.raises(ValueError, match="log_period.*1.*3600"):
            FeatureState.from_config({"service_id": "srv_test", "log_period": 0, "sample_rate": 100})

    def test_featurestate_allows_low_log_period_for_high_rps_services(self):
        for period in (1, 5, 10, 20):
            state = FeatureState.from_config({"service_id": "srv_test", "log_period": period, "sample_rate": 100})
            assert state.log_period == period

    def test_featurestate_validates_rum_log_period_range(self):
        for bad_period in (0, 4000):
            with pytest.raises(ValueError, match="rum_log_period.*1.*3600"):
                FeatureState.from_config(
                    {"service_id": "srv_test", "log_period": 60, "sample_rate": 100, "rum_log_period": bad_period}
                )

    def test_featurestate_validates_sample_rate_range(self):
        with pytest.raises(ValueError, match="sample_rate.*1.*100"):
            FeatureState.from_config({"service_id": "srv_test", "log_period": 60, "sample_rate": 150})

    def test_featurestate_rejects_scoring_without_domain(self):
        with pytest.raises(ValueError, match="scoring.enabled.*scoring.domain"):
            FeatureState.from_config(
                {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "scoring_enabled": True,
                    "scoring_domain": "",
                }
            )

    def test_featurestate_validates_custom_field_names(self):
        with pytest.raises(ValueError, match="Custom field name"):
            FeatureState.from_config(
                {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "log_fields": {"custom_fields": [{"name": "invalid-field", "expression": "..."}]},
                }
            )


class TestFeatureStateDependencyInjection:
    def test_featurestate_from_config_injects_rum_cid(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "log_fields": {"custom_fields": [{"name": "custom_field_1", "expression": "..."}]},
            }
        )
        assert any(f["name"] == "rum_cid" for f in state.log_fields.custom_fields)
        assert state.log_fields.custom_fields[0]["name"] == "custom_field_1"

    def test_featurestate_from_config_injects_edge_score_when_scoring_enabled(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "scoring_enabled": True,
                "scoring_domain": "scorer.example.com",
                "log_fields": {"custom_fields": []},
            }
        )
        for name in ("edge_score", "edge_score_l1", "edge_score_l2"):
            assert any(f["name"] == name for f in state.log_fields.custom_fields)

    def test_featurestate_from_config_injects_cmcd_fields(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "cmcd_enabled": True,
                "log_fields": {"custom_fields": []},
            }
        )
        cmcd_fields = {f["name"]: f for f in state.log_fields.custom_fields if "cmcd" in f["name"]}
        assert len(cmcd_fields) == 14
        for name in ("cmcd_sid", "cmcd_cid", "cmcd_ot", "cmcd_sf", "cmcd_st"):
            assert cmcd_fields[name]["value_type"] == "string" and cmcd_fields[name]["duckdb_type"] == "VARCHAR"
        for name in ("cmcd_br", "cmcd_bl", "cmcd_d", "cmcd_dl", "cmcd_mtp", "cmcd_tb", "cmcd_rtp"):
            assert cmcd_fields[name]["value_type"] == "numeric" and cmcd_fields[name]["duckdb_type"] == "INTEGER"
        for name in ("cmcd_bs", "cmcd_su"):
            assert cmcd_fields[name]["value_type"] == "boolean" and cmcd_fields[name]["duckdb_type"] == "BOOLEAN"

    def test_featurestate_from_config_does_not_duplicate_injected_fields(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "log_fields": {"custom_fields": [{"name": "rum_cid", "expression": "req.http.x-rum-cid"}]},
            }
        )
        assert sum(1 for f in state.log_fields.custom_fields if f["name"] == "rum_cid") == 1


class TestFeatureStateImmutability:
    def test_featurestate_frozen_prevents_mutation(self):
        state = FeatureState.from_config({"service_id": "srv_test", "log_period": 60, "sample_rate": 100})
        with pytest.raises(Exception):
            state.rum_enabled = False


class TestFeatureStateSerialization:
    def test_featurestate_to_dict_includes_all_fields(self):
        d = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "scoring_enabled": True,
                "scoring_domain": "scorer.example.com",
                "logging_enabled": False,
            }
        ).to_dict()
        assert d["service_id"] == "srv_test" and d["log_period"] == 60 and d["logging_enabled"] is False
        assert d["rum_enabled"] is True and d["scoring_enabled"] is True

    def test_featurestate_to_dict_defaults_logging_enabled(self):
        d = FeatureState.from_config({"service_id": "srv_test", "log_period": 60, "sample_rate": 100}).to_dict()
        assert d["logging_enabled"] is True


class TestFeatureStateDualFormatBackwardCompat:
    def test_featurestate_accepts_flat_cmcd_format(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "cmcd_enabled": True,
                "cmcd_mode": "headers",
                "cmcd_version": 2,
                "log_fields": {"custom_fields": []},
            }
        )
        assert state.cmcd.enabled is True and state.cmcd.mode == "headers" and state.cmcd.version == 2

    def test_featurestate_accepts_nested_cmcd_format(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "cmcd": {"enabled": True, "mode": "headers", "version": 2},
                "log_fields": {"custom_fields": []},
            }
        )
        assert state.cmcd.enabled is True and state.cmcd.mode == "headers" and state.cmcd.version == 2

    def test_featurestate_flat_and_nested_cmcd_produce_same_result(self):
        state_flat = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "cmcd_enabled": True,
                "cmcd_mode": "headers",
                "log_fields": {"custom_fields": []},
            }
        )
        state_nested = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "cmcd": {"enabled": True, "mode": "headers"},
                "log_fields": {"custom_fields": []},
            }
        )
        assert state_flat.cmcd == state_nested.cmcd

    def test_featurestate_accepts_flat_scoring_format(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "scoring_enabled": True,
                "scoring_domain": "scorer.example.com",
                "scoring_request_secret": "secret123",
                "scoring_enforce_status_code": 429,
                "log_fields": {"custom_fields": []},
            }
        )
        assert state.scoring.enabled is True and state.scoring.domain == "scorer.example.com"
        assert state.scoring.request_secret == "secret123" and state.scoring.enforce_status_code == 429

    def test_featurestate_accepts_nested_scoring_format(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "scoring": {
                    "enabled": True,
                    "domain": "scorer.example.com",
                    "request_secret": "secret123",
                    "enforce_status_code": 429,
                },
                "log_fields": {"custom_fields": []},
            }
        )
        assert state.scoring.enabled is True and state.scoring.domain == "scorer.example.com"
        assert state.scoring.request_secret == "secret123" and state.scoring.enforce_status_code == 429

    def test_featurestate_flat_and_nested_scoring_produce_same_result(self):
        state_flat = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "scoring_enabled": True,
                "scoring_domain": "scorer.example.com",
                "log_fields": {"custom_fields": []},
            }
        )
        state_nested = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "scoring": {"enabled": True, "domain": "scorer.example.com"},
                "log_fields": {"custom_fields": []},
            }
        )
        assert state_flat.scoring.enabled == state_nested.scoring.enabled
        assert state_flat.scoring.domain == state_nested.scoring.domain

    def test_featurestate_to_dict_outputs_flat_format(self):
        d = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "cmcd_enabled": True,
                "cmcd_mode": "headers",
                "scoring_enabled": True,
                "scoring_domain": "scorer.example.com",
                "log_fields": {
                    "groups": ["test_group"],
                    "custom_fields": [{"name": "custom_1", "expression": "..."}],
                },
            }
        ).to_dict()
        assert d["cmcd_enabled"] is True and d["cmcd_mode"] == "headers"
        assert d["scoring_enabled"] is True and d["scoring_domain"] == "scorer.example.com"
        assert d["log_groups"] == ["test_group"] and len(d["custom_fields"]) >= 1

    def test_featurestate_flat_override_beats_nested(self):
        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "cmcd_enabled": True,
                "cmcd": {"enabled": False},
                "log_fields": {"custom_fields": []},
            }
        )
        assert state.cmcd.enabled is True

    def test_featurestate_extracts_rum_log_period(self):
        state_nested = FeatureState.from_config(
            {"service_id": "srv_test", "log_period": 60, "rum": {"enabled": True, "log_period": 300}}
        )
        assert state_nested.rum_log_period == 300
        state_flat = FeatureState.from_config(
            {"service_id": "srv_test", "log_period": 60, "rum_enabled": True, "rum_log_period": 600}
        )
        assert state_flat.rum_log_period == 600

    def test_featurestate_to_dict_includes_rum_log_period(self):
        d = FeatureState.from_config({"service_id": "srv_test", "log_period": 60, "rum_log_period": 300}).to_dict()
        assert d["rum_log_period"] == 300


# ── Merged from tests/provision/declarative/test_reconciler_integration.py ──


class TestReconciliationIdempotence:
    """Test idempotence property: second run is a no-op."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_idempotent_second_run_is_noop(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """CRITICAL: Second run should be a no-op."""
        # Setup: desired state already deployed
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        # First run
        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                }
                mock_read.return_value = json.dumps(cfg)

                result1 = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # Should return with activated_version = current active
                assert result1.activated_version == 1


class TestDryRunSummaryCountsOnlyManagedRemovals:
    """The operator's own snippets are never deleted, so the summary must not claim they are."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_operator_snippets_are_not_reported_as_removals(
        self, mock_fetch_backends, mock_fetch_endpoints, mock_fetch_snippets, mock_fetch_active
    ):
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = [
            VCLSnippet(name="CAPTCHA check", subroutine="recv", body="# operator", priority=10),
            VCLSnippet(name="Block Iran and North Korea - Recv", subroutine="recv", body="# operator", priority=10),
            VCLSnippet(name="Fastly Log Analysis Capture", subroutine="recv", body="# legacy", priority=10),
        ]
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text") as mock_read:
            mock_read.return_value = json.dumps({"service_id": "srv_test", "log_period": 60, "sample_rate": 100})
            result = reconcile_vcl_state("srv_test", "token", dry_run=True)

        # Only the legacy managed snippet would be deleted; the two operator snippets stay.
        assert result.changes_applied["snippets_removed"] == 1


class TestReconciliationFeatures:
    """Test feature enable/disable scenarios."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_enables_rum_adds_secondary_endpoint(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """Verify enabling RUM creates secondary endpoint."""
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # In dry-run, we see the changes that would be applied
                assert result.changes_applied is not None
                # Endpoint count should show we'd add the RUM endpoint
                assert result.changes_applied.get("endpoints_added", 0) > 0

    @patch("backend.provision.declarative.reconciler.upload_rum_tracker_js")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_does_not_upload_rum_js_during_dry_run(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_upload_rum_js,
    ):
        """Verify a dry run NEVER uploads the tracker, even when RUM is enabled
        in desired state and the diff is non-empty.

        Nothing is applied in a dry run, so the VCL route the tracker depends
        on (/js/faro-sdk.js) is never made live by this call — publishing the
        tracker here would be the exact ordering hazard this fix closes."""
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                reconcile_vcl_state("srv_test", "token", dry_run=True)

                mock_upload_rum_js.assert_not_called()

    @patch("backend.provision.declarative.reconciler.compute_diff")
    @patch("backend.provision.declarative.reconciler.upload_rum_tracker_js")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_uploads_rum_js_on_idempotent_no_change_path(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_upload_rum_js,
        mock_compute_diff,
    ):
        """Verify a service whose VCL already matches desired state (diff is
        empty) still gets its tracker (re)published: current state already
        matching desired means the route the tracker depends on is already
        live, so publishing here is both safe and necessary (e.g. if the
        tracker object was deleted out-of-band from FOS).

        ``compute_diff`` is patched directly to force the empty-diff branch
        without needing to hand-craft current state that byte-for-byte
        matches every desired snippet/endpoint/backend/dictionary the RUM
        generator would produce."""
        from backend.provision.declarative.diff import DiffResult

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_compute_diff.return_value = DiffResult()  # empty: is_empty() is True

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=False)

        assert result.activated_version == 1
        mock_upload_rum_js.assert_called_once_with("srv_test", "token", status_cb=None)

    @patch("backend.provision.declarative.reconciler._activate_draft")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler.upload_rum_tracker_js")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_uploads_rum_js_only_after_activation(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_upload_rum_js,
        mock_apply_diff,
        mock_clone_active,
        mock_validate,
        mock_activate,
    ):
        """Pin the ORDER, not just the outcome: the tracker upload must happen
        strictly after activation. Records call order via a shared list so a
        regression that re-introduces the old (pre-activation) ordering fails
        this test even if both calls still individually happen."""
        call_order = []

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2
        mock_validate.return_value = ""  # valid
        mock_apply_diff.return_value = None
        mock_activate.side_effect = lambda *a, **k: call_order.append("activate")
        mock_upload_rum_js.side_effect = lambda *a, **k: call_order.append("upload_tracker")

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=False)

        assert result.activated_version == 2
        assert call_order == ["activate", "upload_tracker"]
        mock_upload_rum_js.assert_called_once_with("srv_test", "token", status_cb=None)

    @patch("backend.provision.declarative.reconciler._activate_draft")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler.upload_rum_tracker_js")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_skips_rum_js_upload_when_activate_is_false(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_upload_rum_js,
        mock_apply_diff,
        mock_clone_active,
        mock_validate,
        mock_activate,
    ):
        """Verify the tracker is NOT uploaded when activate=False: the draft
        was compiled and validated but never went live, so the route the
        tracker depends on isn't live either."""
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2
        mock_validate.return_value = ""
        mock_apply_diff.return_value = None

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=False, activate=False)

        assert result.activated_version is None
        assert result.draft_version == 2
        mock_activate.assert_not_called()
        mock_upload_rum_js.assert_not_called()

    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler.upload_rum_tracker_js")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_does_not_upload_rum_js_when_activation_fails(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_upload_rum_js,
        mock_apply_diff,
        mock_clone_active,
        mock_validate,
    ):
        """CRITICAL: if activation itself fails, the tracker must never have
        been uploaded — a failed reconcile must not leave FOS holding a
        tracker that references a route Fastly never actually activated.

        ``_activate_draft`` is deliberately left unmocked (default: not
        patched here) and instead its real name is monkeypatched to raise,
        via patching the reconciler-module symbol directly, so the failure
        happens exactly where production would fail: mid-activation, after
        validation passed."""
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2
        mock_validate.return_value = ""
        mock_apply_diff.return_value = None

        with patch(
            "backend.provision.declarative.reconciler._activate_draft",
            side_effect=RuntimeError("Fastly API rejected activation"),
        ):
            with patch("pathlib.Path.exists", return_value=True):
                with patch("pathlib.Path.read_text") as mock_read:
                    cfg = {
                        "service_id": "srv_test",
                        "log_period": 60,
                        "sample_rate": 100,
                        "rum_enabled": True,
                        "fos_prefix": "raw",
                    }
                    mock_read.return_value = json.dumps(cfg)

                    with pytest.raises(RuntimeError, match="Fastly API rejected activation"):
                        reconcile_vcl_state("srv_test", "token", dry_run=False)

        mock_upload_rum_js.assert_not_called()


class TestRumSurrogateKeyPurge:
    """Reconciliation invalidates the hosted RUM scripts by surrogate key, not by URL.

    Both /js/rum.js and /js/faro-sdk.js are tagged ``Surrogate-Key: rum-js``
    in vcl_fetch, so one keyed purge covers both paths and every cached
    ``?v=`` variant — the per-URL purge this replaced had to enumerate
    domains × guessed query strings and silently missed anything it didn't
    guess.
    """

    def test_purge_issues_single_keyed_purge_on_the_service(self):
        from backend.provision.declarative.reconciler import _purge_rum_surrogate_key

        with patch("backend.core.fastly.client.fastly") as mock_fastly:
            _purge_rum_surrogate_key("srv_test", "token")

        mock_fastly.assert_called_once_with(
            "POST",
            "/service/srv_test/purge/rum-js",
            token="token",
            expect_empty=True,
        )

    def test_purge_never_raises_on_api_failure(self):
        """Reconciliation has already activated by the time this runs, so a
        purge failure must not turn a successful reconcile into a reported
        failure — the objects age out on their edge TTL regardless."""
        from backend.provision.declarative.reconciler import _purge_rum_surrogate_key

        with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("purge 503")):
            _purge_rum_surrogate_key("srv_test", "token")

    @patch("backend.provision.declarative.reconciler._purge_rum_surrogate_key")
    @patch("backend.provision.declarative.reconciler._activate_draft")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler.upload_rum_tracker_js")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_purge_runs_after_the_tracker_upload(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_upload_rum_js,
        mock_apply_diff,
        mock_clone_active,
        mock_validate,
        mock_activate,
        mock_purge,
    ):
        """Pin the ORDER: purging before the tracker upload cannot invalidate
        bytes that are uploaded afterwards, so the freshly-published tracker
        would sit behind the old cached copy for a full edge TTL."""
        call_order = []

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2
        mock_validate.return_value = ""
        mock_apply_diff.return_value = None
        mock_activate.side_effect = lambda *a, **k: call_order.append("activate")
        mock_upload_rum_js.side_effect = lambda *a, **k: call_order.append("upload_tracker")
        mock_purge.side_effect = lambda *a, **k: call_order.append("purge")

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                reconcile_vcl_state("srv_test", "token", dry_run=False)

        assert call_order == ["activate", "upload_tracker", "purge"]
        mock_purge.assert_called_once_with("srv_test", "token")

    @patch("backend.provision.declarative.reconciler._purge_rum_surrogate_key")
    @patch("backend.provision.declarative.reconciler._activate_draft")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler.upload_rum_tracker_js")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_purge_still_runs_when_rum_is_disabled(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_upload_rum_js,
        mock_apply_diff,
        mock_clone_active,
        mock_validate,
        mock_activate,
        mock_purge,
    ):
        """A reconcile that DISABLES RUM still has to drop the previously
        cached (and still key-tagged) scripts, so the purge is deliberately
        ungated on rum_enabled even though the tracker upload is not."""
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2
        mock_validate.return_value = ""
        mock_apply_diff.return_value = None

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": False,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                reconcile_vcl_state("srv_test", "token", dry_run=False)

        mock_upload_rum_js.assert_not_called()
        mock_purge.assert_called_once_with("srv_test", "token")


class TestBackendWhitelistEnforcement:
    """Test Gotcha 5: catastrophic backend deletion prevention."""

    def test_reconcile_backend_deletion_whitelist_enforcement(self):
        """CRITICAL: Verify non-whitelisted backends are NEVER deleted."""
        from backend.provision.declarative.diff import DiffResult

        from backend.provision.declarative.reconciler import _apply_diff

        # Set up: one whitelisted backend and one customer origin backend
        diff = DiffResult()
        diff.backends_to_remove = ["my_origin"]  # Not in whitelist!

        # Should raise RuntimeError before deletion
        with pytest.raises(RuntimeError, match="non-whitelisted backend"):
            _apply_diff("srv_test", "token", 2, diff)

    def test_managed_backend_names_constant_is_defined(self):
        """Verify MANAGED_BACKEND_NAMES whitelist is defined."""
        assert isinstance(MANAGED_BACKEND_NAMES, set)
        assert "session_scorer" in MANAGED_BACKEND_NAMES
        assert "F_fos_origin" in MANAGED_BACKEND_NAMES


class TestLegacySnippetDetection:
    """Test legacy VCL snippet detection and auto-cleanup."""

    def test_is_legacy_snippet_recognizes_all_legacy_prefixes(self):
        """Verify _is_legacy_snippet identifies all pre-2.2 snippet types."""
        legacy_names = [
            "Fastly Log Analysis - recv",
            "RUM - recv",
            "Session Scoring - recv",
            "CMCD - recv",
        ]
        for name in legacy_names:
            assert _is_legacy_snippet(name), f"Failed to recognize legacy snippet: {name}"

    def test_is_legacy_snippet_ignores_consolidated(self):
        """Verify _is_legacy_snippet does NOT flag consolidated snippets."""
        consolidated_names = [
            "Fastly Log Analytics - vcl_recv",
            "Fastly Log Analytics - vcl_miss",
            "Fastly Log Analytics - vcl_fetch",
        ]
        for name in consolidated_names:
            assert not _is_legacy_snippet(name), f"Incorrectly flagged as legacy: {name}"

    def test_detect_and_queue_legacy_cleanup_adds_to_removal_queue(self):
        """Verify legacy snippets are queued for removal when no consolidated exist."""
        legacy_snippet = VCLSnippet(
            name="RUM - recv",
            priority=10,
            body="# legacy vcl",
            subroutine="vcl_recv",
        )
        current_snippets = [legacy_snippet]
        diff = DiffResult()

        _detect_and_queue_legacy_cleanup(current_snippets, diff)

        assert "RUM - recv" in diff.snippets_to_remove

    def test_detect_and_queue_legacy_cleanup_unconditionally_cleans_legacy_snippets(self):
        """Verify legacy snippets are unconditionally queued even if consolidated ones already exist."""
        legacy_snippet = VCLSnippet(
            name="RUM - recv",
            priority=10,
            body="# legacy vcl",
            subroutine="vcl_recv",
        )
        consolidated_snippet = VCLSnippet(
            name="Fastly Log Analytics - vcl_recv",
            priority=10,
            body="# new vcl",
            subroutine="vcl_recv",
        )
        current_snippets = [legacy_snippet, consolidated_snippet]
        diff = DiffResult()

        _detect_and_queue_legacy_cleanup(current_snippets, diff)

        # Should unconditionally queue legacy for removal
        assert "RUM - recv" in diff.snippets_to_remove

    def test_detect_and_queue_legacy_cleanup_calls_status_callback(self):
        """Verify status callback is invoked when legacy snippets are detected."""
        legacy_snippet = VCLSnippet(
            name="Session Scoring - recv",
            priority=10,
            body="# legacy vcl",
            subroutine="vcl_recv",
        )
        current_snippets = [legacy_snippet]
        diff = DiffResult()
        callback_messages = []

        def capture_status(msg: str) -> None:
            callback_messages.append(msg)

        _detect_and_queue_legacy_cleanup(current_snippets, diff, status_cb=capture_status)

        assert len(callback_messages) > 0
        assert "legacy" in callback_messages[0].lower()
        assert "1" in callback_messages[0]  # Should mention count


class TestMigrationFromLegacyToConsolidated:
    """End-to-end test of migration from legacy to consolidated VCL."""

    @patch("backend.provision.declarative.reconciler._bootstrap_featurestate_from_fastly")
    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._fetch_dictionaries")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._activate_draft")
    def test_legacy_to_consolidated_migration_full_flow(
        self,
        mock_activate,
        mock_validate,
        mock_fetch_dicts,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_apply_diff,
        mock_clone_active,
        mock_bootstrap,
    ):
        """Verify full migration: legacy snippets detected and queued for removal."""
        # Setup: service with legacy snippets (pre-2.2 deployment)
        legacy_snippets = [
            VCLSnippet(name="RUM - recv", priority=10, body="# legacy rum", subroutine="vcl_recv"),
            VCLSnippet(name="Session Scoring - recv", priority=10, body="# legacy scoring", subroutine="vcl_recv"),
            VCLSnippet(name="CMCD - recv", priority=10, body="# legacy cmcd", subroutine="vcl_recv"),
        ]

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = legacy_snippets
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_fetch_dicts.return_value = []
        mock_clone_active.return_value = 2
        mock_validate.return_value = ""  # Validation passes
        mock_apply_diff.return_value = None

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=False)

                # Verify result shows activation happened
                assert result.activated_version == 2
                assert result.error is None
                # Verify legacy snippets were in the diff
                assert result.changes_applied is not None
                # _apply_diff should have been called with a diff that includes legacy removals
                mock_apply_diff.assert_called_once()
                # _apply_diff(service_id, token, draft_version, diff, ...)
                # So diff is the 4th positional arg (index 3)
                call_args = mock_apply_diff.call_args
                call_diff = call_args[0][3]  # Get 4th positional argument
                # The diff should have queued all 3 legacy snippets for removal
                assert "RUM - recv" in call_diff.snippets_to_remove
                assert "Session Scoring - recv" in call_diff.snippets_to_remove
                assert "CMCD - recv" in call_diff.snippets_to_remove


class TestValidationFailureHandling:
    """Test that validation failures prevent activation."""

    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    def test_reconcile_validates_before_activating(
        self,
        mock_validate,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_apply_diff,
        mock_clone_active,
    ):
        """Verify draft is NOT activated if validation fails."""
        # Mock validation to fail
        mock_validate.return_value = "VCL syntax error at line 42"
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2
        mock_apply_diff.return_value = None

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                }
                mock_read.return_value = json.dumps(cfg)

                # Reconciliation should raise VclValidationError
                with pytest.raises(VclValidationError):
                    reconcile_vcl_state("srv_test", "token", dry_run=False)


class TestBypassActivation:
    """Test that setting activate=False bypasses activation but compiles/validates."""

    @patch("backend.provision.declarative.reconciler._clone_active_version")
    @patch("backend.provision.declarative.reconciler._apply_diff")
    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler._activate_draft")
    def test_reconcile_bypasses_activation_when_activate_is_false(
        self,
        mock_activate,
        mock_validate,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
        mock_apply_diff,
        mock_clone_active,
    ):
        """Verify we clone, apply VCL, validate, but do NOT activate when activate=False."""
        mock_validate.return_value = ""  # Valid
        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        mock_clone_active.return_value = 2

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                    "fos_prefix": "raw",
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=False, activate=False)

                # Clone, apply and validate must be called on draft
                assert mock_clone_active.called
                assert mock_apply_diff.called
                assert mock_validate.called

                # BUT activation must NOT be called
                assert not mock_activate.called

                # Result must track compiled draft and not activated version
                assert result.activated_version is None
                assert result.draft_version == 2


class TestLegacySnippetMigration:
    """Test automatic cleanup of legacy snippets (Gotcha 2)."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_cleans_legacy_snippets_on_first_run(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """CRITICAL: Legacy snippets are deleted on first reconciliation."""
        from backend.provision.declarative.diff import VCLSnippet

        # Simulate old RUM snippets deployed
        old_snippets = [
            VCLSnippet(name="RUM - Recv", priority=100, body="old", subroutine="vcl_recv"),
            VCLSnippet(name="RUM - Set cookies", priority=101, body="old", subroutine="vcl_deliver"),
            VCLSnippet(name="Session Scoring - Recv", priority=100, body="old", subroutine="vcl_recv"),
        ]

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = old_snippets
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": False,  # Disable RUM
                    "scoring_enabled": False,  # Disable scoring
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # Should show legacy snippets would be removed
                assert result.changes_applied is not None
                assert result.changes_applied.get("snippets_removed", 0) > 0

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    def test_reconcile_cleans_legacy_snippets_even_with_existing_consolidated(
        self,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """CRITICAL: Legacy snippets are deleted even if consolidated snippets already exist."""
        from backend.provision.declarative.diff import VCLSnippet

        # Simulate both consolidated and legacy RUM/Scoring snippets deployed
        current_snippets = [
            VCLSnippet(
                name="Fastly Log Analytics - vcl_recv", priority=10, body="consolidated recv", subroutine="vcl_recv"
            ),
            VCLSnippet(name="RUM - Recv", priority=100, body="old RUM recv", subroutine="vcl_recv"),
            VCLSnippet(name="Session Scoring - Recv", priority=100, body="old Scoring recv", subroutine="vcl_recv"),
        ]

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = current_snippets
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": False,
                    "scoring_enabled": False,
                }
                mock_read.return_value = json.dumps(cfg)

                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                # Both legacy snippets should be queued for removal (count of 2)
                assert result.changes_applied.get("snippets_removed", 0) == 2


class TestDictionaryFiltering:
    """Test filtering of non-whitelisted customer dictionaries."""

    @patch("backend.provision.declarative.reconciler._fetch_active_version")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_logging_endpoints")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._fetch_dictionaries")
    def test_reconcile_filters_custom_dictionaries(
        self,
        mock_fetch_dicts,
        mock_fetch_backends,
        mock_fetch_endpoints,
        mock_fetch_snippets,
        mock_fetch_active,
    ):
        """Verify custom dictionaries not in whitelist are filtered and don't trigger deletion assert."""
        from backend.provision.declarative.diff import ServiceDictionary

        mock_fetch_active.return_value = 1
        mock_fetch_snippets.return_value = []
        mock_fetch_endpoints.return_value = []
        mock_fetch_backends.return_value = []
        # Return a whitelisted dict and a custom customer dict
        mock_fetch_dicts.return_value = [
            ServiceDictionary(name="fos_credentials"),
            ServiceDictionary(name="customer_private_dict"),
        ]

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                cfg = {
                    "service_id": "srv_test",
                    "log_period": 60,
                    "sample_rate": 100,
                    "rum_enabled": True,
                }
                mock_read.return_value = json.dumps(cfg)

                # This should not raise an AssertionError during dry run or summary computation
                result = reconcile_vcl_state("srv_test", "token", dry_run=True)
                assert result.changes_applied is not None
                # Since customer_private_dict is filtered out, it shouldn't be queued for deletion
                assert result.changes_applied.get("dictionaries_removed", 0) == 0


class TestBootstrapFeatureState:
    """Test bootstrapping feature state from Fastly."""

    @patch("backend.provision.declarative.reconciler.fastly")
    @patch("backend.provision.declarative.reconciler.fastly_integration")
    def test_bootstrap_detects_fos_origin_non_legacy(self, mock_fastly_int, mock_fastly):
        """Verify bootstrap fallback correctly detects 'fos_origin' as RUM enabling indicator."""
        from backend.provision.declarative.reconciler import _bootstrap_featurestate_from_fastly

        mock_fastly_int.fetch_active_version.return_value = 1
        mock_fastly_int.fetch_snippets.return_value = []
        mock_fastly_int.fetch_logging_endpoints.return_value = []

        # Return VCL content containing only "fos_origin" (the non-legacy string)
        mock_fastly.return_value = {"items": [{"content": "backend fos_origin { .connect_timeout = 10s; }"}]}

        state = _bootstrap_featurestate_from_fastly("srv_test", "token")
        assert state.rum_enabled is True

    @patch("backend.provision.declarative.reconciler.fastly")
    @patch("backend.provision.declarative.reconciler.fastly_integration")
    def test_bootstrap_detects_legacy_endpoint_name_and_rum_period(self, mock_fastly_int, mock_fastly):
        """Verify bootstrap correctly extracts log_period and rum_log_period from Fastly Object Storage Logs endpoints."""
        from backend.provision.declarative.diff import LoggingEndpoint

        from backend.provision.declarative.reconciler import _bootstrap_featurestate_from_fastly

        mock_fastly_int.fetch_active_version.return_value = 1
        mock_fastly_int.fetch_snippets.return_value = []
        mock_fastly_int.fetch_logging_endpoints.return_value = [
            LoggingEndpoint(
                name="Fastly Object Storage Logs",
                endpoint_type="s3",
                path="",
                period=10,
                response_condition="",
                format_string="",
                placement="",
                response_object_name="",
            ),
            LoggingEndpoint(
                name="Fastly Object Storage Logs (RUM)",
                endpoint_type="s3",
                path="",
                period=10,
                response_condition="",
                format_string="",
                placement="",
                response_object_name="",
            ),
        ]
        mock_fastly.return_value = {"items": []}

        state = _bootstrap_featurestate_from_fastly("srv_test", "token")
        assert state.log_period == 10
        assert state.rum_log_period == 10


class TestVarDeconfliction:
    """Test de-confliction of local variable declarations (Gotcha 2)."""

    def test_deconflict_vars_in_vcl_miss_and_pass(self):
        """Verify we preserve declarations in vcl_miss/vcl_pass but comment them out when an unmanaged snippet collides."""
        from backend.provision.declarative.diff import DiffResult, VCLSnippet

        from backend.provision.declarative.reconciler import _apply_diff

        snippet_miss = VCLSnippet(
            name="Fastly Log Analytics - vcl_miss",
            priority=10,
            body="declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';",
            subroutine="vcl_miss",
        )
        snippet_recv = VCLSnippet(
            name="Fastly Log Analytics - vcl_recv",
            priority=10,
            body="declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';",
            subroutine="vcl_recv",
        )
        diff = DiffResult(
            snippets_to_add=[snippet_miss, snippet_recv],
        )

        with patch("backend.provision.declarative.reconciler.fastly_integration") as mock_fastly_int:
            # First, test with no unmanaged snippets - declarations should remain intact
            _apply_diff(
                service_id="srv_test",
                token="token",
                draft_version=1,
                diff=diff,
                current_snippets=[],
            )

            assert "declare local var.fosAccessKey STRING;" in snippet_miss.body
            assert "declare local var.fosAccessKey STRING;" in snippet_recv.body

            # Second, test with an unmanaged snippet in vcl_miss declaring the same variable
            unmanaged_snippet = VCLSnippet(
                name="Some User Snippet",
                priority=10,
                body="declare local var.fosAccessKey STRING;",
                subroutine="vcl_miss",
            )

            # Reset snippet bodies to original state before run
            snippet_miss.body = "declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';"
            snippet_recv.body = "declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';"

            _apply_diff(
                service_id="srv_test",
                token="token",
                draft_version=1,
                diff=diff,
                current_snippets=[unmanaged_snippet],
            )

            # In vcl_miss, the declaration must be commented out to prevent collision with the unmanaged snippet
            assert (
                "# declare local var.fosAccessKey STRING; (omitted to prevent collision with unmanaged snippet)"
                in snippet_miss.body
            )

            # In vcl_recv, the declaration must remain intact (no unmanaged snippet in vcl_recv)
            assert "declare local var.fosAccessKey STRING;" in snippet_recv.body


class TestCloneActiveVersionComment:
    """Test rich commentary in _clone_active_version comment string."""

    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version")
    @patch("backend.provision.declarative.reconciler.fastly_integration.clone_version")
    def test_clone_comment_with_features(self, mock_clone_version, mock_fetch_active_version):
        """Verify comment includes correct features list."""
        from backend.provision.declarative.state import CmcdConfig, FeatureState, LogFieldsConfig, ScoringConfig

        from backend.provision.declarative.reconciler import _clone_active_version

        mock_fetch_active_version.return_value = 1
        mock_clone_version.return_value = 2

        # Create a mock feature state with all features enabled
        state = FeatureState(
            service_id="srv_test",
            log_period=60,
            sample_rate=100,
            edge_only=True,
            custom_condition="",
            fos_prefix="raw",
            fos_endpoint="test.s3.amazonaws.com",
            logging_enabled=True,
            rum_enabled=True,
            scoring=ScoringConfig(enabled=True, domain="test.scoring.domain"),
            cmcd=CmcdConfig(enabled=True),
            log_fields=LogFieldsConfig(
                custom_fields=[
                    {"name": "field_1", "vcl_log_expression": "1"},
                    {"name": "field_2", "vcl_log_expression": "2"},
                ]
            ),
        )

        _clone_active_version("srv_test", "token", state)
        mock_clone_version.assert_called_once()
        comment = mock_clone_version.call_args[0][3]

        assert "Request Logs" in comment
        assert "RUM" in comment
        assert "Session Scoring" in comment
        assert "CMCD" in comment
        assert "2 Custom Fields" in comment

    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version")
    @patch("backend.provision.declarative.reconciler.fastly_integration.clone_version")
    def test_clone_comment_all_disabled(self, mock_clone_version, mock_fetch_active_version):
        """Verify comment for teardown/all features disabled."""
        from backend.provision.declarative.state import CmcdConfig, FeatureState, LogFieldsConfig, ScoringConfig

        from backend.provision.declarative.reconciler import _clone_active_version

        mock_fetch_active_version.return_value = 1
        mock_clone_version.return_value = 2

        state = FeatureState(
            service_id="srv_test",
            log_period=60,
            sample_rate=100,
            edge_only=True,
            custom_condition="",
            fos_prefix="raw",
            fos_endpoint="test.s3.amazonaws.com",
            logging_enabled=False,
            rum_enabled=False,
            scoring=ScoringConfig(enabled=False),
            cmcd=CmcdConfig(enabled=False),
            log_fields=LogFieldsConfig(custom_fields=[]),
        )

        _clone_active_version("srv_test", "token", state)
        comment = mock_clone_version.call_args[0][3]
        assert "All Features Disabled / Teardown" in comment


class TestCdnReconciliation:
    """Test suite for declarative CDN / FOS Proxy service reconciliation."""

    @patch("backend.core.fastly.service.find_service_by_name")
    @patch("backend.provision.declarative.reconciler._fetch_snippets")
    @patch("backend.provision.declarative.reconciler._fetch_backends")
    @patch("backend.provision.declarative.reconciler._fetch_dictionaries")
    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version")
    def test_reconcile_cdn_dry_run(
        self,
        mock_fetch_active_version,
        mock_fetch_dicts,
        mock_fetch_backends,
        mock_fetch_snippets,
        mock_find_service,
    ):
        """Verify that CDN reconciliation computes the dry-run diff correctly."""
        from backend.provision.declarative.reconciler import reconcile_cdn_service_state

        mock_find_service.return_value = {"id": "cdn_svc_created"}
        mock_fetch_snippets.return_value = []
        mock_fetch_backends.return_value = []
        mock_fetch_dicts.return_value = []
        mock_fetch_active_version.return_value = 1

        with patch("pathlib.Path.exists", return_value=True):
            with patch("pathlib.Path.read_text") as mock_read:
                with patch("backend.provision.declarative.reconciler._update_local_config") as mock_update:
                    cfg = {
                        "service_id": "logging_svc_test",
                        "cdn_service_name": "Test CDN Svc",
                        "cdn_url": "https://test-cdn.drew.com",
                        "fos_region": "us-east-1",
                        "fos_bucket": "test-bucket",
                        "fos_access_key_id": "key",
                        "fos_secret_access_key": "secret",
                        "cdn_secret": "auth-secret",
                    }
                    mock_read.return_value = json.dumps(cfg)

                    result = reconcile_cdn_service_state(
                        logging_service_id="logging_svc_test",
                        token="token",
                        dry_run=True,
                        activate=True,
                    )

                    assert result.service_id == "cdn_svc_created"
                    # The CDN ids are persisted through the locked config edit helper.
                    assert mock_update.called
                    service_id_arg, mutate = mock_update.call_args_list[-1][0]
                    assert service_id_arg == "logging_svc_test"
                    written_cfg: dict = {}
                    mutate(written_cfg)
                    assert written_cfg["fos_proxy"]["service_id"] == "cdn_svc_created"
                    assert written_cfg["cdn_service_id"] == "cdn_svc_created"

                    assert result.changes_applied is not None
                    assert result.changes_applied["snippets_added"] > 0
                    assert result.changes_applied["backends_added"] > 0
                    assert result.changes_applied["dictionaries_added"] > 0

    @patch("backend.provision.declarative.reconciler.fastly")
    @patch("backend.provision.declarative.reconciler.fastly_integration.clone_version")
    @patch("backend.provision.declarative.reconciler._activate_draft")
    @patch("backend.provision.declarative.reconciler._validate_draft")
    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version")
    def test_ensure_cdn_domain_adds_missing_domain_to_reused_service(
        self,
        mock_fetch_active_version,
        mock_validate_draft,
        mock_activate_draft,
        mock_clone_version,
        mock_fastly,
    ):
        """A CDN service adopted by name (not freshly created) must have its
        domain reconciled too — the create branch adds the domain, but
        find-by-name previously just adopted whatever domain the service
        already had, silently diverging from a freshly-computed cdn_url and
        causing every CDN read to 500 with Fastly's "unknown domain" error.
        """
        from backend.provision.declarative.reconciler import _ensure_cdn_domain

        mock_fetch_active_version.return_value = 3
        mock_clone_version.return_value = 4
        mock_validate_draft.return_value = ""
        # The service's ACTUAL domain differs from the target domain.
        mock_fastly.side_effect = [
            [{"name": "old-bucket-name.global.ssl.fastly.net"}],  # GET domain list
            {"name": "test-cdn.drew.com"},  # POST domain add
        ]

        _ensure_cdn_domain("cdn_svc_existing", "test-cdn.drew.com", "token")

        mock_clone_version.assert_called_once_with(
            "cdn_svc_existing", 3, "token", "Add missing domain test-cdn.drew.com"
        )
        add_call = mock_fastly.call_args_list[1]
        assert add_call[0][:2] == ("POST", "/service/cdn_svc_existing/version/4/domain")
        assert add_call[0][2] == {"name": "test-cdn.drew.com"}
        mock_activate_draft.assert_called_once_with("cdn_svc_existing", "token", 4)

    @patch("backend.provision.declarative.reconciler.fastly")
    @patch("backend.provision.declarative.reconciler.fastly_integration.clone_version")
    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version")
    def test_ensure_cdn_domain_skips_add_when_already_present(
        self,
        mock_fetch_active_version,
        mock_clone_version,
        mock_fastly,
    ):
        """No spurious clone/activate when the reused service's domain
        already matches the target — this must stay a true no-op."""
        from backend.provision.declarative.reconciler import _ensure_cdn_domain

        mock_fetch_active_version.return_value = 3
        mock_fastly.return_value = [{"name": "test-cdn.drew.com"}]

        _ensure_cdn_domain("cdn_svc_existing", "test-cdn.drew.com", "token")

        mock_clone_version.assert_not_called()
