from __future__ import annotations

import itertools
import json
import re
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from backend.provision.declarative.diff import (
    Backend,
    DiffResult,
    LoggingEndpoint,
    ServiceDictionary,
    VCLSnippet,
    compute_diff,
)
from backend.provision.declarative.generators import (
    desired_backends,
    desired_logging_endpoints,
    desired_snippets,
    generate_consolidated_snippet,
    generate_log_format,
    logging_service_snippets,
)
from backend.provision.declarative.iac_reconciler import (
    DeclarativeComputeService,
    build_desired_state,
    discover_current_state,
    reconcile_infrastructure,
    verify_compute_scorer_readiness,
)
from backend.provision.declarative.state import (
    CmcdConfig,
    FeatureState,
    LogFieldsConfig,
    ScoringConfig,
    config_changed,
    migrate_config,
)
from fastapi.testclient import TestClient

from backend import config as svcconfig
from backend.core import metadata as metadata_db
from backend.main import app
from backend.provision.declarative import fastly_integration as fi
from backend.provision.declarative import reconciler
from backend.provision.declarative.reconciler import (
    MANAGED_BACKEND_NAMES,
    VclValidationError,
    _apply_diff,
    _bootstrap_featurestate_from_fastly,
    _clone_active_version,
    _detect_and_queue_legacy_cleanup,
    _ensure_cdn_domain,
    _is_legacy_snippet,
    _purge_rum_surrogate_key,
    _record_reconcile_audit,
    reconcile_cdn_service_state,
    reconcile_vcl_state,
)
from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES


def _state(**kw: Any) -> FeatureState:
    return FeatureState.from_config({"service_id": "srv_test", "log_period": 60, "sample_rate": 100, **kw})


def _diff(
    current_snippets=None,
    desired_snippets=None,
    current_endpoints=None,
    desired_endpoints=None,
    current_backends=None,
    desired_backends=None,
) -> DiffResult:
    return compute_diff(
        current_snippets=current_snippets or [],
        desired_snippets=desired_snippets or [],
        current_endpoints=current_endpoints or [],
        desired_endpoints=desired_endpoints or [],
        current_backends=current_backends or [],
        desired_backends=desired_backends or [],
    )


@contextmanager
def _mock_cfg_file(**extra: Any):
    cfg = {"service_id": "srv_test", "log_period": 60, "sample_rate": 100, **extra}
    with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.read_text") as mock_read:
        mock_read.return_value = json.dumps(cfg)
        yield mock_read


# ── From test_config_migration.py ──


def test_migrate_flat_cmcd_to_nested():
    cfg = {"service_id": "svc123", "cmcd_enabled": True, "cmcd_mode": "headers", "cmcd_version": 2}
    migrated = migrate_config(cfg)
    assert migrated["cmcd"] == {"enabled": True, "mode": "headers", "version": 2}
    assert all(k not in migrated for k in ("cmcd_enabled", "cmcd_mode", "cmcd_version"))


def test_migrate_flat_scoring_to_nested():
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
    assert "scoring_enabled" not in migrated and "scoring_domain" not in migrated


def test_migrate_both_flat_and_nested_cmcd():
    cfg = {
        "service_id": "svc123",
        "cmcd": {"enabled": False, "mode": "query_string"},
        "cmcd_enabled": True,
        "cmcd_mode": "headers",
    }
    migrated = migrate_config(cfg)
    assert migrated["cmcd"] == {"enabled": True, "mode": "headers", "version": 1}
    assert "cmcd_enabled" not in migrated and "cmcd_mode" not in migrated


def test_migrate_already_nested_unchanged():
    cfg = {
        "service_id": "svc123",
        "cmcd": {"enabled": True, "mode": "headers", "version": 2},
        "scoring": {"enabled": False, "domain": ""},
    }
    migrated = migrate_config(cfg)
    assert migrated == cfg and config_changed(cfg, migrated) is False


def test_migrate_empty_config():
    cfg: dict[str, Any] = {}
    migrated = migrate_config(cfg)
    assert migrated == {} and config_changed(cfg, migrated) is False


def test_config_changed_detects_flat_to_nested():
    before = {"service_id": "svc123", "cmcd_enabled": True}
    after = {"service_id": "svc123", "cmcd": {"enabled": True, "mode": "query_string", "version": 1}}
    assert config_changed(before, after) is True


# ── From test_diff.py ──


class TestDiffComputation:
    def test_diff_detects_snippet_content_change(self):
        current = [VCLSnippet("Fastly Log Analytics - vcl_recv", 10, "old_vcl", "vcl_recv")]
        desired = [VCLSnippet("Fastly Log Analytics - vcl_recv", 10, "new_vcl", "vcl_recv")]
        diff = _diff(current_snippets=current, desired_snippets=desired)
        assert [s.name for s in diff.snippets_to_update] == ["Fastly Log Analytics - vcl_recv"]

    def test_diff_ignores_snippet_order(self):
        s1 = VCLSnippet("S1", 10, "c1", "vcl_recv")
        s2 = VCLSnippet("S2", 10, "c2", "vcl_recv")
        diff = _diff(current_snippets=[s1, s2], desired_snippets=[s2, s1])
        assert not (diff.snippets_to_add or diff.snippets_to_update or diff.snippets_to_remove)

    def test_diff_detects_new_snippet(self):
        desired = [VCLSnippet("Fastly Log Analytics - vcl_recv", 10, "vcl", "vcl_recv")]
        diff = _diff(desired_snippets=desired)
        assert [s.name for s in diff.snippets_to_add] == ["Fastly Log Analytics - vcl_recv"]

    def test_diff_detects_removed_snippet(self):
        current = [VCLSnippet("RUM - Recv", 100, "old_rum_vcl", "vcl_recv")]
        diff = _diff(current_snippets=current)
        assert diff.snippets_to_remove == ["RUM - Recv"]

    def test_diff_detects_endpoint_change(self):
        current = [
            LoggingEndpoint(
                "Fastly Log Analytics", "s3", "raw/%Y/%m/%d/%H/log_%M.gz", 60, "true", "old_format", "none", ""
            )
        ]
        desired = [
            LoggingEndpoint(
                "Fastly Log Analytics", "s3", "raw/%Y/%m/%d/%H/log_%M.gz", 60, "true", "new_format", "none", ""
            )
        ]
        diff = _diff(current_endpoints=current, desired_endpoints=desired)
        assert len(diff.endpoints_to_update) == 1

    def test_diff_detects_backend_change(self):
        current = [Backend("session_scorer", "old.example.com", 443)]
        desired = [Backend("session_scorer", "new.example.com", 443)]
        diff = _diff(current_backends=current, desired_backends=desired)
        assert len(diff.backends_to_update) == 1

    def test_diff_is_empty_when_no_changes(self):
        snippet = VCLSnippet("S1", 10, "code", "vcl_recv")
        endpoint = LoggingEndpoint("E1", "s3", "path", 60, "true", "format", "none", "")
        backend = Backend("B1", "addr", 443)
        diff = _diff([snippet], [snippet], [endpoint], [endpoint], [backend], [backend])
        assert diff.is_empty()

    def test_diff_summary_counts_correctly(self):
        desired = [VCLSnippet("S1", 10, "c1", "vcl_recv"), VCLSnippet("S2", 10, "c2", "vcl_recv")]
        summary = _diff(desired_snippets=desired).summary()
        assert (summary["snippets_added"], summary["snippets_updated"], summary["snippets_removed"]) == (2, 0, 0)


# ── From test_iac_reconciler.py ──


@pytest.fixture
def mock_config_dir(tmp_path, monkeypatch):
    configs_dir = tmp_path / "configs"
    configs_dir.mkdir(exist_ok=True)
    monkeypatch.setattr("backend.config.CONFIGS_DIR", configs_dir)
    return configs_dir


class TestIaCReconciler:
    @patch("backend.provision.declarative.iac_reconciler._SCORER_PACKAGE")
    @patch("backend.provision.session_scoring_orchestrator._resolve_tenant_matrix_for_deploy")
    def test_build_desired_state_scoring_disabled(self, mock_resolve_matrix, mock_scorer_pkg, mock_config_dir):
        cfg = {"service_id": "srv_123", "log_period": 60, "sample_rate": 100, "scoring": {"enabled": False}}
        (mock_config_dir / "srv_123.json").write_text(json.dumps(cfg))
        desired = build_desired_state("srv_123", "token")
        assert desired.service_id == "srv_123" and desired.compute_scorer is None and len(desired.kv_stores) == 0
        assert desired.vcl_service.name == "Fastly Log Analytics"

    @patch("backend.scoring.matrix.serialize_kv")
    @patch("backend.provision.session_scoring_orchestrator._resolve_tenant_matrix_for_deploy")
    def test_build_desired_state_scoring_enabled(
        self, mock_resolve_matrix, mock_serialize_kv, mock_config_dir, tmp_path
    ):
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {"enabled": True, "scoring_domain": "fos-srv123-session-scorer.edgecompute.app"},
        }
        (mock_config_dir / "srv_123.json").write_text(json.dumps(cfg))
        matrix_file = tmp_path / "matrix_srv_123.json"
        matrix_file.write_text(json.dumps({"weights": [1, 2, 3]}))
        mock_resolve_matrix.return_value = matrix_file
        mock_serialize_kv.return_value = b"serialized-matrix"

        desired = build_desired_state("srv_123", "token")
        assert desired.service_id == "srv_123" and desired.compute_scorer is not None
        assert desired.compute_scorer.name == "Session Scoring Service for srv_123"
        assert desired.compute_scorer.domains == ["fos-srv123-session-scorer.edgecompute.app"]
        assert desired.kv_stores["scoring_matrix_srv_123"].keys["matrix"] == b"serialized-matrix".hex()

    @patch("backend.provision.declarative.iac_reconciler._find_kv_store")
    @patch("backend.provision.declarative.iac_reconciler.find_service_by_name")
    def test_discover_current_state(self, mock_find_service, mock_find_kv):
        mock_find_kv.return_value = {"id": "kv_abc", "name": "scoring_matrix_srv_123"}
        mock_find_service.return_value = {"id": "compute_123", "name": "Session Scoring Service for srv_123"}
        current = discover_current_state("srv_123", "token")
        assert current["kv_stores"]["scoring_matrix_srv_123"]["id"] == "kv_abc"
        assert current["compute_scorer"]["id"] == "compute_123"

    @patch("httpx.get")
    @patch("backend.provision.declarative.iac_reconciler.time.sleep")
    def test_verify_readiness_probe_retries_and_succeeds(self, mock_sleep, mock_get):
        compute_def = DeclarativeComputeService(
            name="test-scorer", package_path="path/to/pkg", domains=["test-scorer.edgecompute.app"]
        )
        mock_get.side_effect = [
            RuntimeError("connection error"),
            MagicMock(status_code=502),
            MagicMock(status_code=401),
        ]
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
        cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {"enabled": True, "scoring_domain": "fos-srv123-session-scorer.edgecompute.app"},
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(cfg))
        mock_discover.return_value = {"kv_stores": {}, "compute_scorer": None}
        mock_ensure_scoring_svc.return_value = {
            "scoring_service_id": "compute_srv_id",
            "scoring_matrix_store_id": "matrix_kv_id",
            "scoring_domain": "fos-srv123-session-scorer.edgecompute.app",
        }
        mock_deploy_wasm.return_value = {"sha": "wasm_sha_abc", "files_hash": "wasm_files_hash_xyz"}

        status_cb = MagicMock()
        reconcile_infrastructure("srv_123", "token", dry_run=False, status_cb=status_cb)
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
        self, mock_discover, mock_ensure_scoring_svc, mock_reconcile_vcl, mock_config_dir
    ):
        original_cfg = {
            "service_id": "srv_123",
            "log_period": 60,
            "sample_rate": 100,
            "scoring": {"enabled": True, "scoring_domain": "fos-srv123-session-scorer.edgecompute.app"},
        }
        config_file = mock_config_dir / "srv_123.json"
        config_file.write_text(json.dumps(original_cfg))
        mock_discover.return_value = {"kv_stores": {}, "compute_scorer": None}
        mock_ensure_scoring_svc.side_effect = RuntimeError("Fastly API rate limit 429")

        with pytest.raises(RuntimeError, match="Fastly API rate limit 429"):
            reconcile_infrastructure("srv_123", "token", dry_run=False, status_cb=MagicMock())
        assert json.loads(config_file.read_text()) == original_cfg

    @patch("backend.provision.declarative.iac_reconciler.delete_scoring_service")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_vcl_state")
    @patch("backend.provision.declarative.iac_reconciler.discover_current_state")
    def test_reconcile_infrastructure_disables_scoring_reverse_topological_teardown(
        self, mock_discover, mock_reconcile_vcl, mock_delete_scoring_svc, mock_config_dir
    ):
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
        with patch("backend.provision.declarative.iac_reconciler.time.sleep") as mock_sleep:
            reconcile_infrastructure("srv_123", "token", dry_run=False, status_cb=status_cb)
            mock_sleep.assert_any_call(5.0)

        mock_reconcile_vcl.assert_called_once_with("srv_123", "token", dry_run=False, status_cb=status_cb)
        mock_delete_scoring_svc.assert_called_once_with(
            scoring_service_id="compute_srv_id",
            scoring_keys_store_id="keys_store_id",
            scoring_config_store_id="cfg_store_id",
            scoring_matrix_store_id="matrix_store_id",
            token="token",
            status_cb=status_cb,
        )
        assert "scoring" not in json.loads(config_file.read_text())


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
    def _post_route(self, mock_config_dir, extra_cfg, path, payload, expected_msg):
        cfg = {"service_id": "srv_123", "log_period": 60, "sample_rate": 100, **extra_cfg}
        (mock_config_dir / "srv_123.json").write_text(json.dumps(cfg))
        response = TestClient(app).post(path, json=payload)
        assert response.status_code == 200
        assert any(expected_msg in line for line in response.iter_lines() if line)
        return svcconfig.load_config("srv_123")

    @patch("backend.provision.fastly_api.service_has_scoring_pragmas", return_value=True)
    @patch("backend.routers.session_scoring.resolve_fastly_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_scoring_enable_route(self, mock_reconcile, mock_resolve, mock_pragmas, mock_config_dir):
        updated_cfg = self._post_route(
            mock_config_dir,
            {"scoring": {"enabled": False, "scoring_domain": "scoring.example.com"}},
            "/api/services/srv_123/scoring/enable",
            {"token": "test_token"},
            "Enabling session",
        )
        assert updated_cfg["scoring"]["enabled"] is True
        mock_reconcile.assert_called_once()
        assert mock_reconcile.call_args[0][:2] == ("srv_123", "fake_token")

    @patch("backend.routers.session_scoring.resolve_fastly_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_scoring_disable_route(self, mock_reconcile, mock_resolve, mock_config_dir):
        updated_cfg = self._post_route(
            mock_config_dir,
            {"scoring": {"enabled": True, "scoring_domain": "scoring.example.com"}},
            "/api/services/srv_123/scoring/disable",
            {"token": "test_token"},
            "Disabling session",
        )
        assert updated_cfg["scoring"]["enabled"] is False
        mock_reconcile.assert_called_once()
        assert mock_reconcile.call_args[0][:2] == ("srv_123", "fake_token")

    @patch("backend.routers.performance._resolve_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_cmcd_enable_route(self, mock_reconcile, mock_resolve, mock_config_dir):
        updated_cfg = self._post_route(
            mock_config_dir,
            {},
            "/api/services/srv_123/cmcd/enable",
            {"token": "test_token", "mode": "headers", "version": 2},
            "Enabling CMCD",
        )
        assert {k: updated_cfg["cmcd"][k] for k in ("enabled", "mode", "version")} == {
            "enabled": True,
            "mode": "headers",
            "version": 2,
        }
        assert "enabled_at" in updated_cfg["cmcd"]
        mock_reconcile.assert_called_once()
        assert mock_reconcile.call_args[0][:2] == ("srv_123", "fake_token")

    @patch("backend.routers.performance._resolve_token", return_value="fake_token")
    @patch("backend.provision.declarative.iac_reconciler.reconcile_infrastructure")
    def test_cmcd_disable_route(self, mock_reconcile, mock_resolve, mock_config_dir):
        updated_cfg = self._post_route(
            mock_config_dir,
            {"cmcd": {"enabled": True, "mode": "query_string", "version": 1}},
            "/api/services/srv_123/cmcd/disable",
            {"token": "test_token"},
            "Disabling CMCD",
        )
        assert updated_cfg["cmcd"]["enabled"] is False
        mock_reconcile.assert_called_once()
        assert mock_reconcile.call_args[0][:2] == ("srv_123", "fake_token")


# ── From test_validate_draft.py ──


def test_validate_draft_returns_empty_string_when_valid():
    with patch.object(reconciler.fastly_integration, "validate_version", return_value=True):
        assert reconciler._validate_draft("svc", "tok", 3) == ""


def test_validate_draft_reports_non_ok_status_without_http_error():
    with patch.object(reconciler.fastly_integration, "validate_version", return_value=False):
        error = reconciler._validate_draft("svc", "tok", 3)
    assert error and "validation" in error.lower()


def test_validate_draft_returns_message_when_validation_raises():
    with patch.object(
        reconciler.fastly_integration, "validate_version", side_effect=RuntimeError("VCL validation failed: boom")
    ):
        assert "boom" in reconciler._validate_draft("svc", "tok", 3)


def test_update_local_config_goes_through_the_locked_mutate_config(tmp_path, monkeypatch):
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    svcconfig.save_config("svc-recon", {"service_id": "svc-recon", "other": "keep"})
    calls: list[str] = []
    real = svcconfig.mutate_config
    monkeypatch.setattr(
        svcconfig,
        "mutate_config",
        lambda service_id, mutate, **kw: (calls.append(service_id), real(service_id, mutate, **kw))[1],
    )
    reconciler._update_local_config("svc-recon", lambda c: c.__setitem__("last_activated_version", 7))
    assert calls == ["svc-recon"]
    saved = json.loads((tmp_path / "svc-recon.json").read_text())
    assert saved["last_activated_version"] == 7 and saved["other"] == "keep"


def test_update_local_config_skips_a_read_only_volume(monkeypatch):
    def boom(*a, **k):
        raise OSError("read-only file system")

    monkeypatch.setattr(svcconfig, "mutate_config", boom)
    reconciler._update_local_config("svc-x", lambda c: None)


# ── Merged from tests/provision/declarative/test_state.py ──


class TestFeatureStateValidation:
    def test_featurestate_validates_log_period_range(self):
        with pytest.raises(ValueError, match="log_period.*1.*3600"):
            _state(log_period=0)

    def test_featurestate_allows_low_log_period_for_high_rps_services(self):
        for period in (1, 5, 10, 20):
            assert _state(log_period=period).log_period == period

    def test_featurestate_validates_rum_log_period_range(self):
        for bad_period in (0, 4000):
            with pytest.raises(ValueError, match="rum_log_period.*1.*3600"):
                _state(rum_log_period=bad_period)

    def test_featurestate_validates_sample_rate_range(self):
        with pytest.raises(ValueError, match="sample_rate.*1.*100"):
            _state(sample_rate=150)

    def test_featurestate_rejects_scoring_without_domain(self):
        with pytest.raises(ValueError, match="scoring.enabled.*scoring.domain"):
            _state(scoring_enabled=True, scoring_domain="")

    def test_featurestate_validates_custom_field_names(self):
        with pytest.raises(ValueError, match="Custom field name"):
            _state(log_fields={"custom_fields": [{"name": "invalid-field", "expression": "..."}]})


class TestFeatureStateDependencyInjection:
    def test_featurestate_from_config_injects_rum_cid(self):
        state = _state(
            rum_enabled=True, log_fields={"custom_fields": [{"name": "custom_field_1", "expression": "..."}]}
        )
        assert any(f["name"] == "rum_cid" for f in state.log_fields.custom_fields)
        assert state.log_fields.custom_fields[0]["name"] == "custom_field_1"

    def test_featurestate_from_config_injects_edge_score_when_scoring_enabled(self):
        state = _state(scoring_enabled=True, scoring_domain="scorer.example.com", log_fields={"custom_fields": []})
        for name in ("edge_score", "edge_score_l1", "edge_score_l2"):
            assert any(f["name"] == name for f in state.log_fields.custom_fields)

    def test_featurestate_from_config_injects_cmcd_fields(self):
        state = _state(cmcd_enabled=True, log_fields={"custom_fields": []})
        cmcd_fields = {f["name"]: f for f in state.log_fields.custom_fields if "cmcd" in f["name"]}
        assert len(cmcd_fields) == 14
        for name in ("cmcd_sid", "cmcd_cid", "cmcd_ot", "cmcd_sf", "cmcd_st"):
            assert cmcd_fields[name]["value_type"] == "string" and cmcd_fields[name]["duckdb_type"] == "VARCHAR"
        for name in ("cmcd_br", "cmcd_bl", "cmcd_d", "cmcd_dl", "cmcd_mtp", "cmcd_tb", "cmcd_rtp"):
            assert cmcd_fields[name]["value_type"] == "numeric" and cmcd_fields[name]["duckdb_type"] == "INTEGER"
        for name in ("cmcd_bs", "cmcd_su"):
            assert cmcd_fields[name]["value_type"] == "boolean" and cmcd_fields[name]["duckdb_type"] == "BOOLEAN"

    def test_featurestate_from_config_does_not_duplicate_injected_fields(self):
        state = _state(
            rum_enabled=True, log_fields={"custom_fields": [{"name": "rum_cid", "expression": "req.http.x-rum-cid"}]}
        )
        assert sum(1 for f in state.log_fields.custom_fields if f["name"] == "rum_cid") == 1


class TestFeatureStateImmutability:
    def test_featurestate_frozen_prevents_mutation(self):
        state = _state()
        with pytest.raises(Exception):
            state.rum_enabled = False


class TestFeatureStateSerialization:
    def test_featurestate_to_dict_includes_all_fields(self):
        d = _state(
            rum_enabled=True, scoring_enabled=True, scoring_domain="scorer.example.com", logging_enabled=False
        ).to_dict()
        assert d["service_id"] == "srv_test" and d["log_period"] == 60 and d["logging_enabled"] is False
        assert d["rum_enabled"] is True and d["scoring_enabled"] is True

    def test_featurestate_to_dict_defaults_logging_enabled(self):
        assert _state().to_dict()["logging_enabled"] is True


class TestFeatureStateDualFormatBackwardCompat:
    def test_featurestate_accepts_flat_cmcd_format(self):
        state = _state(cmcd_enabled=True, cmcd_mode="headers", cmcd_version=2, log_fields={"custom_fields": []})
        assert state.cmcd.enabled is True and state.cmcd.mode == "headers" and state.cmcd.version == 2

    def test_featurestate_accepts_nested_cmcd_format(self):
        state = _state(cmcd={"enabled": True, "mode": "headers", "version": 2}, log_fields={"custom_fields": []})
        assert state.cmcd.enabled is True and state.cmcd.mode == "headers" and state.cmcd.version == 2

    def test_featurestate_flat_and_nested_cmcd_produce_same_result(self):
        state_flat = _state(cmcd_enabled=True, cmcd_mode="headers", log_fields={"custom_fields": []})
        state_nested = _state(cmcd={"enabled": True, "mode": "headers"}, log_fields={"custom_fields": []})
        assert state_flat.cmcd == state_nested.cmcd

    def test_featurestate_accepts_flat_scoring_format(self):
        state = _state(
            scoring_enabled=True,
            scoring_domain="scorer.example.com",
            scoring_request_secret="secret123",
            scoring_enforce_status_code=429,
            log_fields={"custom_fields": []},
        )
        assert state.scoring.enabled is True and state.scoring.domain == "scorer.example.com"
        assert state.scoring.request_secret == "secret123" and state.scoring.enforce_status_code == 429

    def test_featurestate_accepts_nested_scoring_format(self):
        state = _state(
            scoring={
                "enabled": True,
                "domain": "scorer.example.com",
                "request_secret": "secret123",
                "enforce_status_code": 429,
            },
            log_fields={"custom_fields": []},
        )
        assert state.scoring.enabled is True and state.scoring.domain == "scorer.example.com"
        assert state.scoring.request_secret == "secret123" and state.scoring.enforce_status_code == 429

    def test_featurestate_flat_and_nested_scoring_produce_same_result(self):
        state_flat = _state(scoring_enabled=True, scoring_domain="scorer.example.com", log_fields={"custom_fields": []})
        state_nested = _state(
            scoring={"enabled": True, "domain": "scorer.example.com"}, log_fields={"custom_fields": []}
        )
        assert state_flat.scoring.enabled == state_nested.scoring.enabled
        assert state_flat.scoring.domain == state_nested.scoring.domain

    def test_featurestate_to_dict_outputs_flat_format(self):
        d = _state(
            cmcd_enabled=True,
            cmcd_mode="headers",
            scoring_enabled=True,
            scoring_domain="scorer.example.com",
            log_fields={"groups": ["test_group"], "custom_fields": [{"name": "custom_1", "expression": "..."}]},
        ).to_dict()
        assert d["cmcd_enabled"] is True and d["cmcd_mode"] == "headers"
        assert d["scoring_enabled"] is True and d["scoring_domain"] == "scorer.example.com"
        assert d["log_groups"] == ["test_group"] and len(d["custom_fields"]) >= 1

    def test_featurestate_flat_override_beats_nested(self):
        state = _state(cmcd_enabled=True, cmcd={"enabled": False}, log_fields={"custom_fields": []})
        assert state.cmcd.enabled is True

    def test_featurestate_extracts_rum_log_period(self):
        assert _state(rum={"enabled": True, "log_period": 300}).rum_log_period == 300
        assert _state(rum_enabled=True, rum_log_period=600).rum_log_period == 600

    def test_featurestate_to_dict_includes_rum_log_period(self):
        assert _state(rum_log_period=300).to_dict()["rum_log_period"] == 300


# ── Merged from tests/provision/declarative/test_reconciler_integration.py ──


@contextmanager
def _reconcile_env(*, snippets=None, endpoints=None, backends=None, dicts=None, validate="", clone=2):
    with (
        patch("backend.provision.declarative.reconciler._fetch_active_version", return_value=1),
        patch("backend.provision.declarative.reconciler._fetch_snippets", return_value=snippets or []),
        patch("backend.provision.declarative.reconciler._fetch_logging_endpoints", return_value=endpoints or []),
        patch("backend.provision.declarative.reconciler._fetch_backends", return_value=backends or []),
        patch("backend.provision.declarative.reconciler._fetch_dictionaries", return_value=dicts or []),
        patch("backend.provision.declarative.reconciler._validate_draft", return_value=validate) as m_val,
        patch("backend.provision.declarative.reconciler._clone_active_version", return_value=clone) as m_clone,
        patch("backend.provision.declarative.reconciler._apply_diff", return_value=None) as m_apply,
        patch("backend.provision.declarative.reconciler._activate_draft") as m_act,
        patch("backend.provision.declarative.reconciler.upload_rum_tracker_js") as m_upload,
        patch("backend.provision.declarative.reconciler._purge_rum_surrogate_key") as m_purge,
    ):
        yield {
            "validate": m_val,
            "clone": m_clone,
            "apply": m_apply,
            "activate": m_act,
            "upload": m_upload,
            "purge": m_purge,
        }


class TestReconciliationIdempotence:
    def test_reconcile_idempotent_second_run_is_noop(self):
        with _reconcile_env(), _mock_cfg_file():
            assert reconcile_vcl_state("srv_test", "token", dry_run=True).activated_version == 1


class TestDryRunSummaryCountsOnlyManagedRemovals:
    def test_operator_snippets_are_not_reported_as_removals(self):
        snips = [
            VCLSnippet(name="CAPTCHA check", subroutine="recv", body="# operator", priority=10),
            VCLSnippet(name="Block Iran and North Korea - Recv", subroutine="recv", body="# operator", priority=10),
            VCLSnippet(name="Fastly Log Analysis Capture", subroutine="recv", body="# legacy", priority=10),
        ]
        with _reconcile_env(snippets=snips), _mock_cfg_file():
            result = reconcile_vcl_state("srv_test", "token", dry_run=True)
        assert result.changes_applied["snippets_removed"] == 1


class TestReconciliationFeatures:
    def test_reconcile_enables_rum_adds_secondary_endpoint(self):
        with _reconcile_env(), _mock_cfg_file(rum_enabled=True, fos_prefix="raw"):
            result = reconcile_vcl_state("srv_test", "token", dry_run=True)
            assert result.changes_applied is not None and result.changes_applied.get("endpoints_added", 0) > 0

    def test_reconcile_does_not_upload_rum_js_during_dry_run(self):
        with _reconcile_env() as m, _mock_cfg_file(rum_enabled=True, fos_prefix="raw"):
            reconcile_vcl_state("srv_test", "token", dry_run=True)
            m["upload"].assert_not_called()

    @patch("backend.provision.declarative.reconciler.compute_diff", return_value=DiffResult())
    def test_reconcile_uploads_rum_js_on_idempotent_no_change_path(self, mock_compute_diff):
        with _reconcile_env() as m, _mock_cfg_file(rum_enabled=True, fos_prefix="raw"):
            result = reconcile_vcl_state("srv_test", "token", dry_run=False)
        assert result.activated_version == 1
        m["upload"].assert_called_once_with("srv_test", "token", status_cb=None)

    def test_reconcile_uploads_rum_js_only_after_activation(self):
        call_order = []
        with _reconcile_env() as m, _mock_cfg_file(rum_enabled=True, fos_prefix="raw"):
            m["activate"].side_effect = lambda *a, **k: call_order.append("activate")
            m["upload"].side_effect = lambda *a, **k: call_order.append("upload_tracker")
            result = reconcile_vcl_state("srv_test", "token", dry_run=False)
        assert result.activated_version == 2 and call_order == ["activate", "upload_tracker"]
        m["upload"].assert_called_once_with("srv_test", "token", status_cb=None)

    def test_reconcile_skips_rum_js_upload_when_activate_is_false(self):
        with _reconcile_env() as m, _mock_cfg_file(rum_enabled=True, fos_prefix="raw"):
            result = reconcile_vcl_state("srv_test", "token", dry_run=False, activate=False)
        assert result.activated_version is None and result.draft_version == 2
        m["activate"].assert_not_called()
        m["upload"].assert_not_called()

    def test_reconcile_does_not_upload_rum_js_when_activation_fails(self):
        with _reconcile_env() as m, _mock_cfg_file(rum_enabled=True, fos_prefix="raw"):
            m["activate"].side_effect = RuntimeError("Fastly API rejected activation")
            with pytest.raises(RuntimeError, match="Fastly API rejected activation"):
                reconcile_vcl_state("srv_test", "token", dry_run=False)
        m["upload"].assert_not_called()


class TestRumSurrogateKeyPurge:
    def test_purge_issues_single_keyed_purge_on_the_service(self):
        with patch("backend.core.fastly.client.fastly") as mock_fastly:
            _purge_rum_surrogate_key("srv_test", "token")
        mock_fastly.assert_called_once_with("POST", "/service/srv_test/purge/rum-js", token="token", expect_empty=True)

    def test_purge_never_raises_on_api_failure(self):
        with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("purge 503")):
            _purge_rum_surrogate_key("srv_test", "token")

    def test_purge_runs_after_the_tracker_upload(self):
        call_order = []
        with _reconcile_env() as m, _mock_cfg_file(rum_enabled=True, fos_prefix="raw"):
            m["activate"].side_effect = lambda *a, **k: call_order.append("activate")
            m["upload"].side_effect = lambda *a, **k: call_order.append("upload_tracker")
            m["purge"].side_effect = lambda *a, **k: call_order.append("purge")
            reconcile_vcl_state("srv_test", "token", dry_run=False)
        assert call_order == ["activate", "upload_tracker", "purge"]
        m["purge"].assert_called_once_with("srv_test", "token")

    def test_purge_still_runs_when_rum_is_disabled(self):
        with _reconcile_env() as m, _mock_cfg_file(rum_enabled=False, fos_prefix="raw"):
            reconcile_vcl_state("srv_test", "token", dry_run=False)
        m["upload"].assert_not_called()
        m["purge"].assert_called_once_with("srv_test", "token")


class TestBackendWhitelistEnforcement:
    def test_reconcile_backend_deletion_whitelist_enforcement(self):
        diff = DiffResult(backends_to_remove=["my_origin"])
        with pytest.raises(RuntimeError, match="non-whitelisted backend"):
            _apply_diff("srv_test", "token", 2, diff)

    def test_managed_backend_names_constant_is_defined(self):
        assert isinstance(MANAGED_BACKEND_NAMES, set)
        assert "session_scorer" in MANAGED_BACKEND_NAMES and "F_fos_origin" in MANAGED_BACKEND_NAMES


class TestLegacySnippetDetection:
    def test_is_legacy_snippet_recognizes_all_legacy_prefixes(self):
        for name in ("Fastly Log Analysis - recv", "RUM - recv", "Session Scoring - recv", "CMCD - recv"):
            assert _is_legacy_snippet(name), f"Failed to recognize legacy snippet: {name}"

    def test_is_legacy_snippet_ignores_consolidated(self):
        for name in (
            "Fastly Log Analytics - vcl_recv",
            "Fastly Log Analytics - vcl_miss",
            "Fastly Log Analytics - vcl_fetch",
        ):
            assert not _is_legacy_snippet(name), f"Incorrectly flagged as legacy: {name}"

    def test_detect_and_queue_legacy_cleanup_adds_to_removal_queue(self):
        diff = DiffResult()
        _detect_and_queue_legacy_cleanup(
            [VCLSnippet(name="RUM - recv", priority=10, body="# legacy vcl", subroutine="vcl_recv")], diff
        )
        assert "RUM - recv" in diff.snippets_to_remove

    def test_detect_and_queue_legacy_cleanup_unconditionally_cleans_legacy_snippets(self):
        diff = DiffResult()
        _detect_and_queue_legacy_cleanup(
            [
                VCLSnippet(name="RUM - recv", priority=10, body="# legacy vcl", subroutine="vcl_recv"),
                VCLSnippet(
                    name="Fastly Log Analytics - vcl_recv", priority=10, body="# new vcl", subroutine="vcl_recv"
                ),
            ],
            diff,
        )
        assert "RUM - recv" in diff.snippets_to_remove

    def test_detect_and_queue_legacy_cleanup_calls_status_callback(self):
        diff = DiffResult()
        callback_messages: list[str] = []
        _detect_and_queue_legacy_cleanup(
            [VCLSnippet(name="Session Scoring - recv", priority=10, body="# legacy vcl", subroutine="vcl_recv")],
            diff,
            status_cb=callback_messages.append,
        )
        assert callback_messages and "legacy" in callback_messages[0].lower() and "1" in callback_messages[0]


class TestMigrationFromLegacyToConsolidated:
    @patch("backend.provision.declarative.reconciler._bootstrap_featurestate_from_fastly")
    def test_legacy_to_consolidated_migration_full_flow(self, mock_bootstrap):
        snips = [
            VCLSnippet(name="RUM - recv", priority=10, body="# legacy rum", subroutine="vcl_recv"),
            VCLSnippet(name="Session Scoring - recv", priority=10, body="# legacy scoring", subroutine="vcl_recv"),
            VCLSnippet(name="CMCD - recv", priority=10, body="# legacy cmcd", subroutine="vcl_recv"),
        ]
        with _reconcile_env(snippets=snips) as m, _mock_cfg_file():
            result = reconcile_vcl_state("srv_test", "token", dry_run=False)
            assert result.activated_version == 2 and result.error is None and result.changes_applied is not None
            m["apply"].assert_called_once()
            call_diff = m["apply"].call_args[0][3]
            for name in ("RUM - recv", "Session Scoring - recv", "CMCD - recv"):
                assert name in call_diff.snippets_to_remove


class TestValidationFailureHandling:
    def test_reconcile_validates_before_activating(self):
        with _reconcile_env(validate="VCL syntax error at line 42"), _mock_cfg_file():
            with pytest.raises(VclValidationError):
                reconcile_vcl_state("srv_test", "token", dry_run=False)


class TestBypassActivation:
    def test_reconcile_bypasses_activation_when_activate_is_false(self):
        with _reconcile_env() as m, _mock_cfg_file(rum_enabled=True, fos_prefix="raw"):
            result = reconcile_vcl_state("srv_test", "token", dry_run=False, activate=False)
            assert m["clone"].called and m["apply"].called and m["validate"].called and not m["activate"].called
            assert result.activated_version is None and result.draft_version == 2


class TestLegacySnippetMigration:
    def test_reconcile_cleans_legacy_snippets_on_first_run(self):
        snips = [
            VCLSnippet(name="RUM - Recv", priority=100, body="old", subroutine="vcl_recv"),
            VCLSnippet(name="RUM - Set cookies", priority=101, body="old", subroutine="vcl_deliver"),
            VCLSnippet(name="Session Scoring - Recv", priority=100, body="old", subroutine="vcl_recv"),
        ]
        with _reconcile_env(snippets=snips), _mock_cfg_file(rum_enabled=False, scoring_enabled=False):
            result = reconcile_vcl_state("srv_test", "token", dry_run=True)
            assert result.changes_applied is not None and result.changes_applied.get("snippets_removed", 0) > 0

    def test_reconcile_cleans_legacy_snippets_even_with_existing_consolidated(self):
        snips = [
            VCLSnippet(
                name="Fastly Log Analytics - vcl_recv", priority=10, body="consolidated recv", subroutine="vcl_recv"
            ),
            VCLSnippet(name="RUM - Recv", priority=100, body="old RUM recv", subroutine="vcl_recv"),
            VCLSnippet(name="Session Scoring - Recv", priority=100, body="old Scoring recv", subroutine="vcl_recv"),
        ]
        with _reconcile_env(snippets=snips), _mock_cfg_file(rum_enabled=False, scoring_enabled=False):
            result = reconcile_vcl_state("srv_test", "token", dry_run=True)
            assert result.changes_applied.get("snippets_removed", 0) == 2


class TestDictionaryFiltering:
    def test_reconcile_filters_custom_dictionaries(self):
        dicts = [ServiceDictionary(name="fos_credentials"), ServiceDictionary(name="customer_private_dict")]
        with _reconcile_env(dicts=dicts), _mock_cfg_file(rum_enabled=True):
            result = reconcile_vcl_state("srv_test", "token", dry_run=True)
            assert result.changes_applied is not None and result.changes_applied.get("dictionaries_removed", 0) == 0


class TestBootstrapFeatureState:
    @patch("backend.provision.declarative.reconciler.fastly")
    @patch("backend.provision.declarative.reconciler.fastly_integration")
    def test_bootstrap_detects_fos_origin_non_legacy(self, mock_fastly_int, mock_fastly):
        mock_fastly_int.fetch_active_version.return_value = 1
        mock_fastly_int.fetch_snippets.return_value = []
        mock_fastly_int.fetch_logging_endpoints.return_value = []
        mock_fastly.return_value = {"items": [{"content": "backend fos_origin { .connect_timeout = 10s; }"}]}
        assert _bootstrap_featurestate_from_fastly("srv_test", "token").rum_enabled is True

    @patch("backend.provision.declarative.reconciler.fastly")
    @patch("backend.provision.declarative.reconciler.fastly_integration")
    def test_bootstrap_detects_legacy_endpoint_name_and_rum_period(self, mock_fastly_int, mock_fastly):
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
        assert state.log_period == 10 and state.rum_log_period == 10


class TestVarDeconfliction:
    def test_deconflict_vars_in_vcl_miss_and_pass(self):
        body = "declare local var.fosAccessKey STRING;\nset var.fosAccessKey = 'test';"
        snippet_miss = VCLSnippet(name="Fastly Log Analytics - vcl_miss", priority=10, body=body, subroutine="vcl_miss")
        snippet_recv = VCLSnippet(name="Fastly Log Analytics - vcl_recv", priority=10, body=body, subroutine="vcl_recv")
        diff = DiffResult(snippets_to_add=[snippet_miss, snippet_recv])

        with patch("backend.provision.declarative.reconciler.fastly_integration"):
            _apply_diff(service_id="srv_test", token="token", draft_version=1, diff=diff, current_snippets=[])
            assert "declare local var.fosAccessKey STRING;" in snippet_miss.body
            assert "declare local var.fosAccessKey STRING;" in snippet_recv.body

            unmanaged_snippet = VCLSnippet(
                name="Some User Snippet",
                priority=10,
                body="declare local var.fosAccessKey STRING;",
                subroutine="vcl_miss",
            )
            snippet_miss.body = body
            snippet_recv.body = body
            _apply_diff(
                service_id="srv_test", token="token", draft_version=1, diff=diff, current_snippets=[unmanaged_snippet]
            )
            assert (
                "# declare local var.fosAccessKey STRING; (omitted to prevent collision with unmanaged snippet)"
                in snippet_miss.body
            )
            assert "declare local var.fosAccessKey STRING;" in snippet_recv.body


class TestCloneActiveVersionComment:
    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version", return_value=1)
    @patch("backend.provision.declarative.reconciler.fastly_integration.clone_version", return_value=2)
    def test_clone_comment_with_features(self, mock_clone_version, mock_fetch_active_version):
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
        comment = mock_clone_version.call_args[0][3]
        for expected in ("Request Logs", "RUM", "Session Scoring", "CMCD", "2 Custom Fields"):
            assert expected in comment

    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version", return_value=1)
    @patch("backend.provision.declarative.reconciler.fastly_integration.clone_version", return_value=2)
    def test_clone_comment_all_disabled(self, mock_clone_version, mock_fetch_active_version):
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
        assert "All Features Disabled / Teardown" in mock_clone_version.call_args[0][3]


class TestCdnReconciliation:
    @patch("backend.core.fastly.service.find_service_by_name", return_value={"id": "cdn_svc_created"})
    @patch("backend.provision.declarative.reconciler._fetch_snippets", return_value=[])
    @patch("backend.provision.declarative.reconciler._fetch_backends", return_value=[])
    @patch("backend.provision.declarative.reconciler._fetch_dictionaries", return_value=[])
    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version", return_value=1)
    def test_reconcile_cdn_dry_run(
        self, mock_fetch_active_version, mock_fetch_dicts, mock_fetch_backends, mock_fetch_snippets, mock_find_service
    ):
        with (
            patch("pathlib.Path.exists", return_value=True),
            patch("pathlib.Path.read_text") as mock_read,
            patch("backend.provision.declarative.reconciler._update_local_config") as mock_update,
        ):
            mock_read.return_value = json.dumps(
                {
                    "service_id": "logging_svc_test",
                    "cdn_service_name": "Test CDN Svc",
                    "cdn_url": "https://test-cdn.drew.com",
                    "fos_region": "us-east-1",
                    "fos_bucket": "test-bucket",
                    "fos_access_key_id": "key",
                    "fos_secret_access_key": "secret",
                    "cdn_secret": "auth-secret",
                }
            )
            result = reconcile_cdn_service_state(
                logging_service_id="logging_svc_test", token="token", dry_run=True, activate=True
            )
            assert result.service_id == "cdn_svc_created"
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
    @patch("backend.provision.declarative.reconciler.fastly_integration.clone_version", return_value=4)
    @patch("backend.provision.declarative.reconciler._activate_draft")
    @patch("backend.provision.declarative.reconciler._validate_draft", return_value="")
    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version", return_value=3)
    def test_ensure_cdn_domain_adds_missing_domain_to_reused_service(
        self, mock_fetch_active_version, mock_validate_draft, mock_activate_draft, mock_clone_version, mock_fastly
    ):
        mock_fastly.side_effect = [
            [{"name": "old-bucket-name.global.ssl.fastly.net"}],
            {"name": "test-cdn.drew.com"},
        ]
        _ensure_cdn_domain("cdn_svc_existing", "test-cdn.drew.com", "token")
        mock_clone_version.assert_called_once_with(
            "cdn_svc_existing", 3, "token", "Add missing domain test-cdn.drew.com"
        )
        add_call = mock_fastly.call_args_list[1]
        assert add_call[0][:3] == ("POST", "/service/cdn_svc_existing/version/4/domain", {"name": "test-cdn.drew.com"})
        mock_activate_draft.assert_called_once_with("cdn_svc_existing", "token", 4)

    @patch("backend.provision.declarative.reconciler.fastly", return_value=[{"name": "test-cdn.drew.com"}])
    @patch("backend.provision.declarative.reconciler.fastly_integration.clone_version")
    @patch("backend.provision.declarative.reconciler.fastly_integration.fetch_active_version", return_value=3)
    def test_ensure_cdn_domain_skips_add_when_already_present(
        self, mock_fetch_active_version, mock_clone_version, mock_fastly
    ):
        _ensure_cdn_domain("cdn_svc_existing", "test-cdn.drew.com", "token")
        mock_clone_version.assert_not_called()


# ── Merged from tests/provision/declarative/test_generators.py ──


class TestGeneratorVCLStructure:
    def test_generator_no_duplicate_var_declarations(self):
        state = _state(
            rum_enabled=True,
            scoring={"enabled": True, "domain": "scorer.example.com"},
            cmcd={"enabled": True},
        )
        for subroutine in ["vcl_recv", "vcl_fetch", "vcl_deliver", "vcl_error"]:
            vcl = generate_consolidated_snippet(state, subroutine)
            var_decls = re.findall(r"declare\s+local\s+var\.(\w+)", vcl)
            duplicates = [v for v in set(var_decls) if var_decls.count(v) > 1]
            assert not duplicates, f"{subroutine}: duplicate var declarations {duplicates}"

    def test_rum_beacon_fields_are_extracted_before_capture(self):
        vcl = generate_consolidated_snippet(_state(rum_enabled=True), "vcl_recv")
        extraction = vcl.index(
            'set req.http.x-fos-edge-data:rum_metric_name = querystring.get(req.url, "rum_metric_name");'
        )
        capture = vcl.index("# Capture edge data for logging")
        assert extraction < capture
        assert 'querystring.get(req.url, "rum_metric_value")' in vcl
        assert 'querystring.get(req.url, "rum_error_message")' in vcl

    def test_generator_builds_5_consolidated_snippets(self):
        snippets = desired_snippets(_state())
        assert len(snippets) == 6
        assert {s.name for s in snippets} == {
            "Fastly Log Analytics - vcl_recv",
            "Fastly Log Analytics - vcl_miss",
            "Fastly Log Analytics - vcl_pass",
            "Fastly Log Analytics - vcl_fetch",
            "Fastly Log Analytics - vcl_deliver",
            "Fastly Log Analytics - vcl_error",
        }

    def test_generator_snippet_priority_is_10(self):
        for snippet in desired_snippets(_state()):
            assert snippet.priority == 10

    def test_generator_includes_all_custom_fields_in_format(self):
        state = _state(
            log_fields={
                "custom_fields": [
                    {"name": "custom_field_1", "expression": "req.http.Custom-1", "enabled": True},
                    {"name": "custom_field_2", "expression": "req.http.Custom-2", "enabled": True},
                ]
            }
        )
        log_format = generate_log_format(state)
        for field in state.log_fields.custom_fields:
            if field.get("enabled", True):
                assert field["name"] in log_format

    def test_generator_rum_fetch_ttl(self):
        vcl_enabled = generate_consolidated_snippet(_state(rum_enabled=True), "vcl_fetch")
        for expected in (
            "set beresp.ttl = 86400s;",
            "set beresp.cacheable = true;",
            'set beresp.http.Surrogate-Key = "rum-js";',
            'set beresp.http.Surrogate-Control = "max-age=86400";',
            'set beresp.http.Cache-Control = "public, max-age=300";',
            'req.url.path == "/js/rum.js"',
        ):
            assert expected in vcl_enabled
        assert "immutable" not in vcl_enabled
        assert "set beresp.ttl = 86400s;" not in generate_consolidated_snippet(_state(rum_enabled=False), "vcl_fetch")

    def test_rum_js_caching_gates_on_200_status(self):
        vcl = generate_consolidated_snippet(_state(rum_enabled=True), "vcl_fetch")
        rum_block = vcl.split('if (req.url.path == "/js/rum.js") {', 1)[1]
        status_idx = rum_block.index("if (beresp.status == 200)")
        for directive in (
            "set beresp.ttl = 86400s;",
            "set beresp.cacheable = true;",
            'set beresp.http.Surrogate-Key = "rum-js";',
        ):
            assert status_idx < rum_block.index(directive)
        assert "set beresp.cacheable = false;" in rum_block


class TestScoringExcludesRumAssets:
    def _state(self, rum_enabled, exclude_url_regex=None):
        scoring = {"enabled": True, "request_secret": "s3cret", "domain": "scorer.example.com"}
        if exclude_url_regex is not None:
            scoring["exclude_url_regex"] = exclude_url_regex
        return _state(rum_enabled=rum_enabled, scoring=scoring)

    def test_rum_assets_excluded_from_scoring_when_rum_enabled(self):
        vcl = generate_consolidated_snippet(self._state(rum_enabled=True), "vcl_recv")
        assert 'req.url.path != "/js/rum.js"' in vcl and 'req.url.path != "/js/faro-sdk.js"' in vcl

    def test_no_rum_guard_when_rum_disabled(self):
        vcl = generate_consolidated_snippet(self._state(rum_enabled=False), "vcl_recv")
        assert 'req.url.path != "/js/rum.js"' not in vcl

    def test_guard_survives_a_narrow_operator_exclude_regex(self):
        vcl = generate_consolidated_snippet(self._state(rum_enabled=True, exclude_url_regex=r"^/static/"), "vcl_recv")
        assert "^/static/" in vcl
        assert 'req.url.path != "/js/rum.js"' in vcl and 'req.url.path != "/js/faro-sdk.js"' in vcl

    def test_guard_precedes_the_scoring_return_pass(self):
        vcl = generate_consolidated_snippet(self._state(rum_enabled=True), "vcl_recv")
        guard_idx = vcl.index('req.url.path != "/js/rum.js"')
        pass_idx = vcl.index("return(pass);")
        assert guard_idx < pass_idx and "{" in vcl[guard_idx:pass_idx]


class TestLoggingEndpointGeneration:
    def test_generator_main_endpoint_created(self):
        main = [e for e in desired_logging_endpoints(_state(fos_prefix="raw")) if e.name == "Fastly Log Analytics"]
        assert len(main) == 1 and "year=%Y" in main[0].path

    def test_generator_endpoints_use_null_placement(self):
        endpoints = desired_logging_endpoints(_state(rum_enabled=True, fos_prefix="raw"))
        main = [e for e in endpoints if e.name == "Fastly Log Analytics"]
        rum = [e for e in endpoints if e.name == "Fastly RUM Logs"]
        assert len(main) == 1 and len(rum) == 1
        assert main[0].placement is None and rum[0].placement is None

    def test_generator_main_endpoint_not_created_when_logging_disabled(self):
        endpoints = desired_logging_endpoints(_state(fos_prefix="raw", logging_enabled=False))
        assert not [e for e in endpoints if e.name == "Fastly Log Analytics"]

    def test_generator_rum_endpoint_created_when_enabled(self):
        endpoints = desired_logging_endpoints(_state(rum_enabled=True, fos_prefix="raw"))
        rum = [e for e in endpoints if e.name == "Fastly RUM Logs"]
        assert len(rum) == 1 and "raw/rum/" in rum[0].path
        assert rum[0].response_condition == "rum_log_condition" and rum[0].period == 60

    def test_generator_rum_endpoint_respects_rum_log_period(self):
        endpoints = desired_logging_endpoints(_state(rum_enabled=True, rum_log_period=300, fos_prefix="raw"))
        rum = [e for e in endpoints if e.name == "Fastly RUM Logs"]
        assert len(rum) == 1 and rum[0].period == 300

    def test_generator_rum_endpoint_not_created_when_disabled(self):
        assert not [e for e in desired_logging_endpoints(_state(rum_enabled=False)) if "RUM" in e.name]


class TestBackendGeneration:
    def test_generator_scoring_backend_created_when_enabled(self):
        backends = desired_backends(_state(scoring={"enabled": True, "domain": "scorer.example.com"}))
        scorer = [b for b in backends if b.name == "session_scorer"]
        assert len(scorer) == 1
        assert scorer[0].address == "scorer.example.com" and scorer[0].port == 443
        assert scorer[0].ssl_hostname == "scorer.example.com"

    def test_generator_rum_backend_created_when_enabled(self):
        fos = [b for b in desired_backends(_state(rum_enabled=True)) if b.name == "fos_origin"]
        assert len(fos) == 1 and fos[0].port == 443

    def test_generator_no_backends_when_features_disabled(self):
        assert len(desired_backends(_state(rum_enabled=False, scoring={"enabled": False}))) == 0


class TestRUMAssetFetchGeneration:
    def test_rum_asset_fetch_included_in_vcl_recv_when_enabled(self):
        recv_snippet = next(s for s in desired_snippets(_state(rum_enabled=True)) if s.subroutine == "vcl_recv")
        for expected in ("/js/rum.js", "F_fos_origin", "X-FOS-Request"):
            assert expected in recv_snippet.body

    def test_rum_sigv4_signing_included_in_vcl_miss_when_enabled(self):
        miss_snippet = next(s for s in desired_snippets(_state(rum_enabled=True)) if s.subroutine == "vcl_miss")
        for expected in (
            "X-FOS-Request",
            'bereq.url = "/rum/rum-tracker.js";',
            "fosAccessKey",
            "AWS4-HMAC-SHA256",
            "x-amz-date",
        ):
            assert expected in miss_snippet.body

    def test_rum_error_handler_included_in_vcl_error_when_enabled(self):
        error_snippet = next(s for s in desired_snippets(_state(rum_enabled=True)) if s.subroutine == "vcl_error")
        for expected in ("obj.status == 611", "set obj.status = 204", "synthetic"):
            assert expected in error_snippet.body

    def test_rum_asset_fetch_not_included_when_disabled(self):
        snippets = desired_snippets(_state(rum_enabled=False))
        recv_snippet = next(s for s in snippets if s.subroutine == "vcl_recv")
        miss_snippet = next(s for s in snippets if s.subroutine == "vcl_miss")
        assert "/js/rum.js" not in recv_snippet.body and "fosAccessKey" not in miss_snippet.body

    def test_rum_recv_and_asset_fetch_both_in_vcl_recv(self):
        recv_snippet = next(s for s in desired_snippets(_state(rum_enabled=True)) if s.subroutine == "vcl_recv")
        for expected in ("x-rum-req-id", "/rum-beacon", "/js/rum.js"):
            assert expected in recv_snippet.body

    def test_rum_request_id_minting_moved_to_section_6(self):
        body = next(s for s in desired_snippets(_state(rum_enabled=True)) if s.subroutine == "vcl_recv").body
        idx_6 = body.index("# Section 6: Log Field Capture (vcl_recv)")
        idx_5 = body.index("# Section 5: RUM (vcl_recv)")
        assert idx_6 < idx_5
        assert "set req.http.x-rum-req-id = randomstr(12);" in body[idx_6:idx_5]
        assert "set req.http.x-rum-req-id = randomstr(12);" not in body[idx_5:]

    def test_backend_shield_mapping(self):
        for region, cdn_shield, expected in (
            ("us-east-1", "", "iad-va-us"),
            ("us-central-1", "", "chi-il-us"),
            ("us-east-1", "frankfurt-de", "frankfurt-de"),
            ("us-east-1", "none", ""),
        ):
            fos = next(
                b
                for b in desired_backends(_state(rum_enabled=True, fos_region=region, cdn_shield=cdn_shield))
                if b.name == "fos_origin"
            )
            assert fos.shield == expected


class TestLoggingServiceSnippets:
    def test_logging_service_snippets_creates_capture_snippet(self):
        snippets = logging_service_snippets(_state(logging_enabled=True))
        capture = [s for s in snippets if s.name == "Fastly Log Analysis Capture"]
        assert len(capture) == 1 and capture[0].subroutine == "vcl_recv" and capture[0].priority == 1
        assert "fastly.ff.visits_this_service" in capture[0].body

    def test_logging_service_snippets_empty_when_disabled(self):
        assert len(logging_service_snippets(_state(logging_enabled=False))) == 0

    def test_logging_service_snippets_includes_miss_pass_when_group_l(self):
        snippets = logging_service_snippets(
            _state(logging_enabled=True, log_fields={"groups": ["L"], "custom_fields": []})
        )
        names = {s.name for s in snippets}
        assert "Fastly Log Analysis Capture" in names
        assert "Fastly Log Analysis Miss" in names or len(snippets) >= 1


class TestFaroVersionThreading:
    def _rum_state(self, faro_version=None):
        extra = {"rum": {"faro_version": faro_version}} if faro_version is not None else {}
        return _state(rum_enabled=True, **extra)

    def test_from_config_defaults_faro_version_to_none(self):
        assert _state(rum_enabled=True).faro_version is None

    def test_from_config_reads_pinned_faro_version(self):
        assert self._rum_state(faro_version="2.9.0").faro_version == "2.9.0"

    def test_faro_version_none_output_is_byte_identical_to_baseline(self):
        state = self._rum_state(faro_version=None)
        vcl_recv = generate_consolidated_snippet(state, "vcl_recv")
        assert "/js/faro-sdk.js" not in vcl_recv
        assert (
            """# Fetch RUM tracker JS from FOS with SigV4 signing
if (req.url.path == "/js/rum.js" && req.method == "GET") {
    # Backend points to FOS endpoint (shared with logging)
    set req.backend = fastly.try_select_shield(ssl_shield_iad_va_us, F_fos_origin);
    # Flag for SigV4 signing in miss/pass (req.backend.name not available there)
    set req.http.X-FOS-Request = "1";
    return(lookup);
}"""
            in vcl_recv
        )
        vcl_fetch = generate_consolidated_snippet(state, "vcl_fetch")
        assert "faro-sdk" not in vcl_fetch and "rum-faro-sdk" not in vcl_fetch
        assert (
            vcl_fetch
            == """# Section 5: RUM (vcl_fetch)
# Cache RUM tracker JS at the edge; browsers get a short TTL so a
# Surrogate-Key purge of "rum-js" becomes visible to clients quickly.
if (req.url.path == "/js/rum.js") {
    if (beresp.status == 200) {
        set beresp.ttl = 86400s;
        set beresp.cacheable = true;
        set beresp.http.Surrogate-Key = "rum-js";
        set beresp.http.Surrogate-Control = "max-age=86400";
        set beresp.http.Cache-Control = "public, max-age=300";
    } else {
        set beresp.ttl = 0s;
        set beresp.cacheable = false;
    }
}"""
        )
        vcl_miss = generate_consolidated_snippet(state, "vcl_miss")
        assert "faro-sdk" not in vcl_miss and "/js/rum.js" in vcl_miss

    def test_faro_version_pinned_adds_recv_routing(self):
        vcl_recv = generate_consolidated_snippet(self._rum_state(faro_version="2.9.0"), "vcl_recv")
        assert (
            'if ((req.url.path == "/js/rum.js" || req.url.path == "/js/faro-sdk.js") && req.method == "GET")'
            in vcl_recv
        )

    def test_faro_version_pinned_rewrites_object_path_in_miss(self):
        vcl_miss = generate_consolidated_snippet(self._rum_state(faro_version="2.9.0"), "vcl_miss")
        assert '"/rum/faro-web-sdk-v2.9.0.iife.js"' in vcl_miss and '"/rum/rum-tracker.js"' in vcl_miss

    def test_faro_version_pinned_adds_purgeable_fetch_caching(self):
        vcl_fetch = generate_consolidated_snippet(self._rum_state(faro_version="2.9.0"), "vcl_fetch")
        assert '"/js/faro-sdk.js" && req.http.X-FOS-Request == "1"' in vcl_fetch
        assert 'set beresp.http.Surrogate-Key = "rum-faro-sdk rum-js";' in vcl_fetch
        assert "set beresp.ttl = 604800s;" in vcl_fetch and "set beresp.ttl = 86400s;" in vcl_fetch
        keys = re.findall(r'Surrogate-Key = "([^"]+)"', vcl_fetch)
        assert [set(k.split()) for k in keys] == [{"rum-js"}, {"rum-faro-sdk", "rum-js"}]

    def test_faro_version_invalid_rejected_at_generation(self):
        state = self._rum_state(faro_version="not-a-version")
        for sub in ("vcl_recv", "vcl_miss"):
            with pytest.raises(ValueError, match="faro_version must be a plain"):
                generate_consolidated_snippet(state, sub)


# ── Merged from tests/provision/declarative/test_fastly_integration.py ──────


def _fastly_mock(responses: dict[tuple[str, str], Any]) -> Any:
    def _fake(method, path, body=None, *, token, **kwargs):
        key = (method, path)
        if key not in responses:
            raise AssertionError(f"unexpected fastly() call: {key}")
        value = responses[key]
        if isinstance(value, Exception):
            raise value
        return value

    return _fake


@pytest.mark.parametrize(
    ("responses", "expected"),
    [
        ({("GET", "/service/svc1/version/active"): {"active": True, "number": 7}}, 7),
        (
            {
                ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
                ("GET", "/service/svc1/version"): [{"number": 1, "active": False}, {"number": 2, "active": True}],
            },
            2,
        ),
        (
            {
                ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
                ("GET", "/service/svc1/version"): {"items": [{"number": 3, "active": True}]},
            },
            3,
        ),
        (
            {
                ("GET", "/service/svc1/version/active"): {"active": False},
                ("GET", "/service/svc1/version"): [{"number": 1, "active": False}],
            },
            None,
        ),
        (
            {
                ("GET", "/service/svc1/version/active"): RuntimeError("HTTP 404"),
                ("GET", "/service/svc1/version"): RuntimeError("HTTP 500"),
            },
            None,
        ),
    ],
)
def test_fetch_active_version_variants(responses, expected):
    with patch.object(fi, "fastly", _fastly_mock(responses)):
        assert fi.fetch_active_version("svc1", "tok") == expected


def test_fetch_snippets_logging_backends_and_dictionaries():
    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {
                ("GET", "/service/svc1/version/3/snippet"): [
                    {"name": "recv1", "priority": 50, "snippet": "# vcl", "type": "vcl_recv"}
                ]
            }
        ),
    ):
        assert fi.fetch_snippets("svc1", 3, "tok") == [
            VCLSnippet(name="recv1", priority=50, body="# vcl", subroutine="vcl_recv")
        ]

    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/snippet"): {"items": [{}]}})):
        assert fi.fetch_snippets("svc1", 3, "tok") == [
            VCLSnippet(name="", priority=100, body="", subroutine="vcl_recv")
        ]

    for bad_snip in (RuntimeError("HTTP 500"), {"not_items": []}):
        with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/snippet"): bad_snip})):
            assert fi.fetch_snippets("svc1", 3, "tok") == []

    ep_raw = {
        "name": "ep1",
        "path": "/raw/",
        "period": 60,
        "response_condition": "cond1",
        "format": "%h",
        "placement": "none",
        "response_object_name": "",
    }
    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/logging/s3"): [ep_raw]})):
        assert fi.fetch_logging_endpoints("svc1", 3, "tok") == [
            LoggingEndpoint("ep1", "s3", "/raw/", 60, "cond1", "%h", "none", "")
        ]
    with patch.object(
        fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/logging/s3"): RuntimeError("boom")})
    ):
        assert fi.fetch_logging_endpoints("svc1", 3, "tok") == []

    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {("GET", "/service/svc1/version/3/backend"): [{"name": "origin1", "address": "origin.example.test"}]}
        ),
    ):
        assert fi.fetch_backends("svc1", 3, "tok") == [Backend(name="origin1", address="origin.example.test", port=443)]
    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/backend"): RuntimeError("boom")})):
        assert fi.fetch_backends("svc1", 3, "tok") == []

    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {
                ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1", "write_only": False}],
                ("GET", "/service/svc1/dictionary/did1/items"): [
                    {"item_key": "k1", "item_value": "v1"},
                    {"key": "k2", "value": "v2"},
                ],
            }
        ),
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == [
            ServiceDictionary(name="d1", write_only=False, items={"k1": "v1", "k2": "v2"})
        ]

    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {
                ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1"}],
                ("GET", "/service/svc1/dictionary/did1/items"): {"item_key": "solo", "item_value": "only"},
            }
        ),
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok")[0].items == {"solo": "only"}

    with patch.object(
        fi,
        "fastly",
        _fastly_mock(
            {
                ("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": "did1"}],
                ("GET", "/service/svc1/dictionary/did1/items"): RuntimeError("boom"),
            }
        ),
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == [ServiceDictionary(name="d1", write_only=False, items={})]

    with patch.object(
        fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/dictionary"): [{"name": "d1", "id": ""}]})
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == [ServiceDictionary(name="d1", items={})]

    with patch.object(
        fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/dictionary"): RuntimeError("boom")})
    ):
        assert fi.fetch_dictionaries("svc1", 3, "tok") == []


def test_clone_delete_validate_and_activate_mutations():
    with patch.object(fi, "fastly", _fastly_mock({("PUT", "/service/svc1/version/3/clone"): {"number": 4}})):
        assert fi.clone_version("svc1", 3, "tok") == 4

    calls: list[tuple[str, str, Any]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, **kw: (calls.append((m, p, b)), {"number": 9} if p.endswith("/clone") else {})[1],
    ):
        assert fi.clone_version("svc1", 3, "tok", comment="reconciler pass") == 9
    assert ("PUT", "/service/svc1/version/9", {"comment": "reconciler pass"}) in calls

    del_calls: list[tuple[str, str, bool]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, *, expect_empty=False, **kw: (del_calls.append((m, p, expect_empty)), {})[1],
    ):
        fi.delete_snippet("svc1", 3, "weird name/slash", "tok")
        fi.delete_logging_endpoint("svc1", 3, "My Endpoint", "tok")
        fi.delete_backend("svc1", 3, "origin/1", "tok")
        fi.delete_dictionary("svc1", 3, "my dict", "tok")
    assert del_calls == [
        ("DELETE", "/service/svc1/version/3/snippet/weird%20name%2Fslash", True),
        ("DELETE", "/service/svc1/version/3/logging/s3/My%20Endpoint", True),
        ("DELETE", "/service/svc1/version/3/backend/origin%2F1", True),
        ("DELETE", "/service/svc1/version/3/dictionary/my%20dict", True),
    ]

    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/validate"): {"status": "ok"}})):
        assert fi.validate_version("svc1", 3, "tok") is True
    with patch.object(fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/validate"): {"status": "error"}})):
        assert fi.validate_version("svc1", 3, "tok") is False
    with patch.object(
        fi, "fastly", _fastly_mock({("GET", "/service/svc1/version/3/validate"): RuntimeError("bad VCL")})
    ):
        with pytest.raises(RuntimeError, match="VCL validation failed"):
            fi.validate_version("svc1", 3, "tok")

    act_calls: list[tuple[str, str]] = []
    with patch.object(fi, "fastly", lambda m, p, **kw: (act_calls.append((m, p)), {})[1]):
        fi.activate_version("svc1", 3, "tok")
    assert act_calls == [("PUT", "/service/svc1/version/3/activate")]


def test_create_or_update_snippet_logging_backend_and_dictionary():
    for exists, subr, exp_method, exp_type in (
        (True, "vcl_recv", "PUT", "recv"),
        (False, "deliver", "POST", "deliver"),
    ):
        calls: list[tuple[str, str, Any]] = []

        def _snip_fake(m, p, b=None, _exists=exists, _calls=calls, **kw):
            _calls.append((m, p, b))
            if m == "GET":
                if not _exists:
                    raise RuntimeError("HTTP 404")
                return {"name": "s"}
            return {}

        with patch.object(fi, "fastly", _snip_fake):
            fi.create_or_update_snippet(
                "svc1", 3, VCLSnippet(name="s", priority=50, body="# vcl", subroutine=subr), "tok"
            )
        assert [c[0] for c in calls] == ["GET", exp_method]
        assert calls[1][2]["type"] == exp_type and calls[1][2]["content"] == "# vcl"

    ep_calls: list[tuple[str, str, Any]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, **kw: (
            ep_calls.append((m, p, b)),
            (_ for _ in ()).throw(RuntimeError("404")) if m == "GET" else {},
        )[1],
    ):
        fi.create_or_update_logging_endpoint(
            "svc1",
            3,
            LoggingEndpoint("ep1", "s3", "/raw/", 60, "", "%h", "none", ""),
            "tok",
            bucket_name="my-bucket",
            domain="s3.example.test",
            access_key="AKIA",
            secret_key="shh",
        )
    assert [c[0] for c in ep_calls] == ["GET", "POST"]
    post_ep = ep_calls[1][2]
    assert (post_ep["bucket_name"], post_ep["domain"], post_ep["access_key"], post_ep["secret_key"]) == (
        "my-bucket",
        "s3.example.test",
        "AKIA",
        "shh",
    )
    assert "response_condition" not in post_ep

    ep_upd: list[tuple[str, str, Any]] = []
    with patch.object(fi, "fastly", lambda m, p, b=None, **kw: (ep_upd.append((m, p, b)), {"name": "ep1"})[1]):
        fi.create_or_update_logging_endpoint(
            "svc1", 3, LoggingEndpoint("ep1", "s3", "/raw/", 60, "always_true", "%h", "none", ""), "tok"
        )
    assert [c[0] for c in ep_upd] == ["GET", "PUT"]
    assert ep_upd[1][2]["response_condition"] == "always_true" and "bucket_name" not in ep_upd[1][2]

    be_calls: list[tuple[str, str, Any]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, **kw: (
            be_calls.append((m, p, b)),
            (_ for _ in ()).throw(RuntimeError("404")) if m == "GET" else {},
        )[1],
    ):
        fi.create_or_update_backend(
            "svc1",
            3,
            Backend("origin1", "origin.example.test", 443, override_host="custom.example.test", shield="dca-dc-us"),
            "tok",
        )
    assert [c[0] for c in be_calls] == ["GET", "POST"]
    assert be_calls[1][2]["shield"] == "dca-dc-us" and be_calls[1][2]["override_host"] == "custom.example.test"

    be_upd: list[tuple[str, str, Any]] = []
    with patch.object(fi, "fastly", lambda m, p, b=None, **kw: (be_upd.append((m, p, b)), {"name": "origin1"})[1]):
        fi.create_or_update_backend("svc1", 3, Backend("origin1", "origin.example.test", 443), "tok")
    assert [c[0] for c in be_upd] == ["GET", "PUT"]
    assert be_upd[1][2]["shield"] is None and "override_host" not in be_upd[1][2]

    d_calls: list[tuple[str, str, Any]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, b=None, **kw: (
            d_calls.append((m, p, b)),
            (_ for _ in ()).throw(RuntimeError("404"))
            if m == "GET"
            else ({"id": "dict-id-1"} if p.endswith("/dictionary") else {}),
        )[1],
    ):
        fi.create_or_update_dictionary(
            "svc1", 3, ServiceDictionary(name="my_dict", write_only=True, items={"key1": "val1"}), "tok"
        )
    assert [(c[0], c[1].split("/")[-1]) for c in d_calls] == [
        ("GET", "my_dict"),
        ("POST", "dictionary"),
        ("PUT", "key1"),
    ]

    d_upd: list[str] = []

    def _dict_fallback(m, p, b=None, **kw):
        d_upd.append(m)
        if m == "GET":
            return {"name": "my_dict"}
        if m == "PUT" and "/dictionary/my_dict" in p:
            return {"id": "dict-id-1"}
        if m == "PUT" and "/item/new_key" in p:
            raise RuntimeError("HTTP 500")
        return {}

    with patch.object(fi, "fastly", _dict_fallback):
        fi.create_or_update_dictionary(
            "svc1", 3, ServiceDictionary(name="my_dict", items={"new_key": "new_val"}), "tok"
        )
    assert d_upd == ["GET", "PUT", "PUT", "POST"]

    with patch.object(
        fi,
        "fastly",
        lambda m, p, *a, **kw: (_ for _ in ()).throw(RuntimeError("404")) if m == "GET" else {},
    ):
        with pytest.raises(RuntimeError, match="Failed to get dictionary ID"):
            fi.create_or_update_dictionary("svc1", 3, ServiceDictionary(name="my_dict", items={"k": "v"}), "tok")

    d_empty: list[tuple[str, str]] = []
    with patch.object(
        fi,
        "fastly",
        lambda m, p, *a, **kw: (
            d_empty.append((m, p)),
            (_ for _ in ()).throw(RuntimeError("404")) if m == "GET" else {"id": "dict-id-1"},
        )[1],
    ):
        fi.create_or_update_dictionary("svc1", 3, ServiceDictionary(name="empty_dict", items={}), "tok")
    assert [c[0] for c in d_empty] == ["GET", "POST"] and all(c[0] != "PATCH" for c in d_empty)


class TestBuildRumLogCondition:
    """RUM beacon log-emission condition must fire only at the true edge."""

    def test_edge_guards_present_without_custom(self):
        from backend.provision.declarative.reconciler import build_rum_log_condition

        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_prefix": "raw",
                "rum": {},
            }
        )
        stmt = build_rum_log_condition(state)
        for token in (
            'req.url.path == "/rum-beacon"',
            'req.method == "POST"',
            "fastly.ff.visits_this_service == 0",
            "req.restarts == 0",
        ):
            assert token in stmt
        assert 'req.url.path !~ "^/api/"' not in stmt
        assert not re.search(r" && \((?!req\.restarts == 0)(.+)\)$", stmt)

    def test_custom_condition_appended_and_extractable(self):
        from backend.provision.declarative.reconciler import build_rum_log_condition

        state = FeatureState.from_config(
            {
                "service_id": "srv_test",
                "log_period": 60,
                "sample_rate": 100,
                "rum_enabled": True,
                "fos_prefix": "raw",
                "rum": {"custom_condition": 'req.http.host == "example.com"'},
            }
        )
        stmt = build_rum_log_condition(state)
        assert "fastly.ff.visits_this_service == 0" in stmt and "req.restarts == 0" in stmt
        assert stmt.endswith('&& (req.http.host == "example.com")')
        m = re.search(r" && \((?!req\.restarts == 0)(.+)\)$", stmt)
        assert m is not None and m.group(1) == 'req.http.host == "example.com"'
