"""Tests for backend.provision.session_scoring_orchestrator.

Covers the happy-path Fastly call order, rollback shape, per-tenant matrix
resolution, Wasm package deployment, smart-redeploy skipping, full teardown,
re-publish helpers (``update_recv_exclusion_regex``, ``update_enforce_status_code``),
request-secret self-heal on re-enable, backend/snippet 404-tolerance helpers,
and status_cb fan-out.
"""

from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from backend import config as svcconfig
from backend.provision import fastly_api, orchestrator
from backend.provision import session_scoring_orchestrator as sso
from backend.provision.cmcd_fields import (
    _CMCD_FIELD_NAMES,
    reconcile_cfg_system_custom_fields,
    reconcile_system_custom_fields,
    system_feature_flags,
)
from backend.provision.declarative.reconciler import ReconciliationResult
from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES
from backend.provision.session_scoring_vcl import (
    SCORING_DELIVER_NAME,
    SCORING_FETCH_NAME,
    SCORING_RECV_NAME,
    scoring_snippet_names,
)
from tests._helpers.config_fakes import (
    config_lock_held as _lock_held,
)
from tests._helpers.config_fakes import (
    make_happy_path_fastly_mock,
    make_happy_path_fastly_mock_with_post_secret,
    make_scoring_meta,
)
from tests._helpers.config_fakes import (
    read_persisted_config as _persisted,
)

# ── Merged from tests/provision/test_session_scoring_orchestrator.py ──
LOG_SVC = "TestLogSvc456"

SCORE_SVC = "ScoringSvcZ"

TOKEN = "FAKE_TOKEN"

KEYS_STORE = "KEYS_STORE_ID"

CFG_STORE = "CFG_STORE_ID"


@pytest.fixture
def _no_local_config_writes():
    """Activation metadata is persisted through the locked ``mutate_config`` helper; these tests
    drive the orchestrator with mocked Fastly responses (non-JSON draft versions) and have no
    service config on disk, so isolate that persistence step."""
    with patch("backend.provision.declarative.reconciler._update_local_config"):
        yield


def _scoring_meta(*, with_secret: bool = True) -> dict:
    return make_scoring_meta(
        log_svc=LOG_SVC,
        score_svc=SCORE_SVC,
        keys_store=KEYS_STORE,
        cfg_store=CFG_STORE,
        with_secret=with_secret,
    )


def _happy_path_fastly_mock():
    return make_happy_path_fastly_mock(log_svc=LOG_SVC)


def _happy_path_fastly_mock_with_post_secret(*, post_secret_409: bool = False):
    return make_happy_path_fastly_mock_with_post_secret(
        log_svc=LOG_SVC, keys_store=KEYS_STORE, post_secret_409=post_secret_409
    )


def _make_clone_snippet_fastly_mock(
    *,
    active_version: int = 300,
    validate_ok: bool = True,
    validate_error: str = "bad VCL",
) -> MagicMock:
    """Build a Fastly mock that clones `active_version` -> `active_version + 1`,
    returns an empty snippet list on the draft, and returns either ok or error
    on `/validate`."""
    draft_version = active_version + 1

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": active_version, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/{active_version}/clone"):
            return {"number": draft_version}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/{draft_version}/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/{draft_version}/validate"):
            if validate_ok:
                return {"status": "ok"}
            return {"status": "error", "errors": [validate_error]}
        return {}

    return MagicMock(side_effect=side_effect)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_add_custom_fields_preserves_unrelated_fields():
    cfg = {"log_fields": {"custom_fields": [{"name": "user_id", "collection_stage": "edge", "enabled": True}]}}
    sso._add_scoring_custom_fields(cfg)
    names = [cf["name"] for cf in cfg["log_fields"]["custom_fields"]]
    assert "user_id" in names
    for required in (
        "edge_score",
        "edge_score_l1",
        "edge_score_l2",
        "edge_cookie_compliance",
        "edge_score_reason",
        "edge_sid",
        "edge_score_rtt_us",
        "edge_score_exec_us",
    ):
        assert required in names


@pytest.mark.usefixtures("_no_local_config_writes")
def test_add_custom_fields_is_idempotent():
    """Re-running enable_scoring shouldn't duplicate the scoring fields."""
    cfg = {"log_fields": {"custom_fields": []}}
    sso._add_scoring_custom_fields(cfg)
    sso._add_scoring_custom_fields(cfg)
    names = [cf["name"] for cf in cfg["log_fields"]["custom_fields"]]
    assert len(names) == len(sso._SCORING_FIELD_NAMES)
    assert sorted(names) == sorted(sso._SCORING_FIELD_NAMES)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_remove_custom_fields_strips_only_scoring_fields():
    cfg = {
        "log_fields": {
            "custom_fields": [
                {"name": "user_id", "enabled": True},
                {"name": "edge_score", "enabled": True},
                {"name": "edge_score_l1", "enabled": True},
                {"name": "another_one", "enabled": True},
            ]
        }
    }
    sso._remove_scoring_custom_fields(cfg)
    names = [cf["name"] for cf in cfg["log_fields"]["custom_fields"]]
    assert names == ["user_id", "another_one"]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_remove_custom_fields_handles_missing_log_fields():
    """A config without a log_fields block should not crash on remove."""
    cfg = {}
    sso._remove_scoring_custom_fields(cfg)
    assert cfg == {}


@pytest.mark.usefixtures("_no_local_config_writes")
def test_remove_scoring_backend_treats_404_as_already_gone():
    """The disable path is idempotent — a missing backend is a no-op, not
    a re-raised RuntimeError that would abort the teardown."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "DELETE":
            raise RuntimeError("404 Not Found")
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch.object(sso, "fastly", fastly_mock):
        sso._remove_scoring_backend(LOG_SVC, 200, TOKEN)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_remove_scoring_backend_propagates_non_404():
    """Non-404 failures must surface."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "DELETE":
            raise RuntimeError("500 internal")
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch.object(sso, "fastly", fastly_mock):
        with pytest.raises(RuntimeError, match="500 internal"):
            sso._remove_scoring_backend(LOG_SVC, 200, TOKEN)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_remove_scoring_snippets_skips_absent_names():
    """When only some scoring snippets are present, DELETE only fires for those."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET" and path.endswith("/snippet"):
            return [{"name": SCORING_RECV_NAME}]
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        sso._remove_scoring_snippets(LOG_SVC, 200, TOKEN)

    deletes = [c for c in fastly_mock.call_args_list if c.args[0] == "DELETE"]
    assert len(deletes) == 1
    assert SCORING_RECV_NAME.replace(" ", "%20") in deletes[0].args[1]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_remove_scoring_snippets_tolerates_race_404():
    """Race: list says snippet X is there, but another writer deletes it
    before our DELETE lands. 404 should be silently absorbed."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET" and path.endswith("/snippet"):
            return [{"name": SCORING_RECV_NAME}]
        if method == "DELETE":
            raise RuntimeError("404 Not Found")
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        sso._remove_scoring_snippets(LOG_SVC, 200, TOKEN)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_happy_path_runs_all_stages(monkeypatch, tmp_path):
    """End-to-end enable on a clean service: ensure_scoring → wasm deploy
    → declarative reconciler."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package") as wasm_mock,
        patch.object(sso, "_write_matrix_to_kv") as kv_mock,
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        result = sso.enable_scoring(LOG_SVC, TOKEN)

    assert result["scoring_service_id"] == SCORE_SVC
    assert result["logging_service_active_version"] == 101
    wasm_mock.assert_called_once_with(
        SCORE_SVC, "MATRIX_STORE", TOKEN, status_cb=None, prior_package_sha="", prior_files_hash=""
    )
    kv_mock.assert_called_once_with("MATRIX_STORE", LOG_SVC, TOKEN, status_cb=None)

    saved_calls = save_mock.call_args_list
    assert len(saved_calls) >= 1
    final_cfg = saved_calls[-1].args[1]
    assert final_cfg["scoring"]["enabled"] is True
    assert final_cfg["scoring"]["scoring_service_id"] == SCORE_SVC
    field_names = [cf["name"] for cf in final_cfg["log_fields"]["custom_fields"]]
    assert "edge_score" in field_names
    reconcile_mock.assert_called_once_with(LOG_SVC, TOKEN, status_cb=None)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_stamps_deploy_identity_for_drift_detection(monkeypatch):
    """enable_scoring stamps the deployed Wasm sha + VCL fingerprint into
    cfg.scoring so /scoring/status can flag when the edge falls behind a newer
    shipped build."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package", return_value={"sha": "deadbeef_pkg_sha", "version": 7}),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    saved_cfg = save_mock.call_args_list[-1].args[1]
    assert saved_cfg["scoring"]["deployed_package_sha"] == "deadbeef_pkg_sha"
    assert saved_cfg["scoring"]["deployed_vcl_sha"] == sso.scorer_vcl_fingerprint(LOG_SVC)
    assert saved_cfg["scoring"]["deployed_vcl_sha"] == sso.shipped_scorer_identity(LOG_SVC)["vcl_sha"]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_scorer_vcl_fingerprint_deterministic_and_service_scoped():
    """The VCL drift fingerprint depends only on the generator code + service id."""
    a = sso.scorer_vcl_fingerprint(LOG_SVC)
    assert a == sso.scorer_vcl_fingerprint(LOG_SVC)
    assert sso.scorer_vcl_fingerprint("SomeOtherSvc") != a


@pytest.mark.usefixtures("_no_local_config_writes")
def test_deploy_wasm_package_returns_uploaded_bytes_sha(monkeypatch, tmp_path):
    """_deploy_wasm_package returns the sha256 of the exact package bytes it
    uploaded plus the new Compute version it activated."""
    import hashlib

    pkg = tmp_path / "session-scorer.tar.gz"
    pkg.write_bytes(b"fake-wasm-package-bytes")
    expected = hashlib.sha256(b"fake-wasm-package-bytes").hexdigest()

    versions = [{"number": 5, "active": True}]

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/SCORESVC/version"):
            return versions
        if (method, path) == ("PUT", "/service/SCORESVC/version/5/clone"):
            return {"number": 6}
        return {}

    shared = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso, "_SCORER_PACKAGE", pkg),
        patch.object(sso, "fastly", shared),
        patch("backend.core.fastly.service.fastly", shared),
        patch.object(sso, "fastly_raw", MagicMock(return_value={})),
    ):
        result = sso._deploy_wasm_package("SCORESVC", "MATRIX_STORE", TOKEN)
    assert result["sha"] == expected
    assert result["version"] == 6
    assert result["skipped"] is False


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_populates_default_exclude_url_regex_on_first_enable(monkeypatch):
    """On first turn-on, enable_scoring must persist the bundled
    DEFAULT_ASSET_EXT_REGEX literal into cfg.scoring.exclude_url_regex."""
    from backend.provision.declarative.reconciler import ReconciliationResult
    from backend.provision.session_scoring_vcl import DEFAULT_ASSET_EXT_REGEX

    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    saved_cfg = save_mock.call_args_list[-1].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] == DEFAULT_ASSET_EXT_REGEX


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_preserves_operator_overrides_on_reenable(monkeypatch):
    """Re-enabling scoring must NOT wipe operator-tunable overrides."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        "scoring": {
            "enabled": True,
            "exclude_url_regex": r"^/(healthz|metrics)$",
            "enforce_status_code": 403,
        },
    }
    fastly_mock = _happy_path_fastly_mock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    saved_cfg = save_mock.call_args_list[-1].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] == r"^/(healthz|metrics)$"
    assert saved_cfg["scoring"]["enforce_status_code"] == 403


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_adds_backend_with_correct_shape(monkeypatch):
    """The scoring backend and VCL snippets are managed and deployed via the Declarative Reconciler."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    reconcile_mock.assert_called_once_with(LOG_SVC, TOKEN, status_cb=None)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_raises_when_no_config():
    """A logging service ID we don't recognise should fail fast."""
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.enable_scoring("UnknownSvc", TOKEN)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_heals_missing_request_secret_via_post(monkeypatch):
    """When ensure_scoring_service comes back without a request_secret AND
    the prior config block has none either, enable_scoring must mint a
    fresh one and POST it into the keys ConfigStore."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    meta = _scoring_meta(with_secret=False)
    fastly_mock = _happy_path_fastly_mock_with_post_secret()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "ensure_scoring_service", return_value=meta),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.fastly_integration.fastly", fastly_mock),
        patch("backend.core.fastly.client.fastly", fastly_mock),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN)

    secret_posts = [
        c
        for c in fastly_mock.call_args_list
        if c.args[0] == "POST" and c.args[1] == f"/resources/stores/config/{KEYS_STORE}/item"
    ]
    assert len(secret_posts) == 1
    assert secret_posts[0].args[2]["item_key"] == "request_secret"
    assert len(secret_posts[0].args[2]["item_value"]) == 64


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_heals_request_secret_via_patch_when_post_409s(monkeypatch):
    """If the keys store already has a stale request_secret entry, POST
    409s and we must fall through to PATCH."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    meta = _scoring_meta(with_secret=False)
    happy = _happy_path_fastly_mock_with_post_secret(post_secret_409=True)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "ensure_scoring_service", return_value=meta),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", happy),
        patch("backend.core.fastly.service.fastly", happy),
        patch("backend.provision.declarative.fastly_integration.fastly", happy),
        patch("backend.core.fastly.client.fastly", happy),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN)

    patch_calls = [
        c
        for c in happy.call_args_list
        if c.args[0] == "PATCH" and c.args[1] == f"/resources/stores/config/{KEYS_STORE}/item/request_secret"
    ]
    assert len(patch_calls) == 1


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_heal_aborts_when_no_keys_store_id():
    """If both the prior cfg AND the fresh ensure_scoring_service result
    are missing scoring_keys_store_id, raise rather than silently lose it."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    meta = _scoring_meta(with_secret=False)
    meta.pop("scoring_keys_store_id")

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "ensure_scoring_service", return_value=meta),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
    ):
        with pytest.raises(RuntimeError, match="scoring_keys_store_id"):
            sso.enable_scoring(LOG_SVC, TOKEN)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_status_cb_fires_on_each_stage():
    """status_cb must fire at each major stage boundary."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    cb = MagicMock()

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.fastly_integration.fastly", fastly_mock),
        patch("backend.core.fastly.client.fastly", fastly_mock),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN, status_cb=cb)

    msgs = [c.args[0] for c in cb.call_args_list]
    joined = " ".join(msgs).lower()
    for needle in ("enabling", "cloning", "snippets", "log format", "validating", "activating", "done"):
        assert needle in joined, f"status_cb missed beat: {needle!r} (got {msgs!r})"


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_rolls_back_on_validation_failure(monkeypatch):
    """When reconcile_vcl_state raises an exception, we MUST revert the on-disk config changes."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }

    fastly_mock = _happy_path_fastly_mock_with_post_secret()
    save_calls = []

    def fake_save(svc_id, c, **_kw):
        save_calls.append(dict(c))

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=fake_save),
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.side_effect = RuntimeError("Validation failed")
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.enable_scoring(LOG_SVC, TOKEN)

    assert save_calls, "expected at least one save"
    final = save_calls[-1]
    assert "scoring" not in final
    field_names = [cf["name"] for cf in final.get("log_fields", {}).get("custom_fields", [])]
    assert "edge_score" not in field_names


@pytest.mark.usefixtures("_no_local_config_writes")
def test_disable_scoring_no_op_when_not_enabled(monkeypatch):
    cfg = {"service_id": LOG_SVC, "log_fields": {"custom_fields": []}}
    fastly_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fastly_mock),
        patch.object(sso, "delete_scoring_service") as delete_mock,
    ):
        sso.disable_scoring(LOG_SVC, TOKEN)
    fastly_mock.assert_not_called()
    delete_mock.assert_not_called()


@pytest.mark.usefixtures("_no_local_config_writes")
def test_disable_scoring_full_teardown_when_enabled(monkeypatch):
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {
            "custom_fields": [
                {
                    "name": "user_id",
                    "vcl_log_expression": "req.http.X-User-Id",
                    "collection_stage": "edge",
                    "enabled": True,
                },
                {
                    "name": "edge_score",
                    "vcl_log_expression": "req.http.x-edge-score",
                    "collection_stage": "deliver",
                    "enabled": True,
                },
                {
                    "name": "edge_score_l1",
                    "vcl_log_expression": "req.http.x-edge-score-l1",
                    "collection_stage": "deliver",
                    "enabled": True,
                },
            ]
        },
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        "scoring": {
            "enabled": True,
            "scoring_service_id": SCORE_SVC,
            "scoring_keys_store_id": "KEYS",
            "scoring_config_store_id": "CFG",
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 200, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/200/clone"):
            return {"number": 201}
        if path == f"/service/{LOG_SVC}/version/201" and method == "PUT":
            return {}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/snippet"):
            return [
                {"name": SCORING_RECV_NAME},
                {"name": SCORING_FETCH_NAME},
                {"name": SCORING_DELIVER_NAME},
            ]
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/logging/s3"):
            return [{"name": "Fastly Object Storage Logs"}]
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/validate"):
            return {"status": "ok"}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    saved = []

    def fake_save(svc_id, c, **_kw):
        saved.append(dict(c))

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=fake_save),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch.object(sso, "delete_scoring_service", return_value=[]) as delete_mock,
        patch.object(sso, "_delete_scoring_matrix_from_fos") as fos_cleanup_mock,
    ):
        sso.disable_scoring(LOG_SVC, TOKEN)

    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    for name in (SCORING_RECV_NAME, SCORING_FETCH_NAME, SCORING_DELIVER_NAME):
        assert any(
            method == "DELETE"
            and path.startswith(f"/service/{LOG_SVC}/version/201/snippet/")
            and (name.replace(" ", "%20").replace(":", "%3A") in path)
            for method, path in calls
        ), f"DELETE for {name} not found"
    assert any(
        method == "DELETE" and path.startswith(f"/service/{LOG_SVC}/version/201/backend/") for method, path in calls
    )
    assert ("PUT", f"/service/{LOG_SVC}/version/201/activate") in calls
    delete_mock.assert_called_once()
    fos_cleanup_mock.assert_called_once_with(LOG_SVC)
    assert "scoring" not in saved[-1]
    final_field_names = [cf["name"] for cf in saved[-1].get("log_fields", {}).get("custom_fields", [])]
    assert final_field_names == ["user_id"]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_disable_scoring_rolls_back_when_validate_fails():
    """If validate fails during disable, the prior active version must
    be re-activated and delete_scoring_service MUST NOT run."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        "scoring": {
            "enabled": True,
            "scoring_service_id": SCORE_SVC,
            "scoring_keys_store_id": KEYS_STORE,
            "scoring_config_store_id": CFG_STORE,
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 200, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/200/clone"):
            return {"number": 201}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/logging/s3"):
            return [{"name": "Fastly Object Storage Logs"}]
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/validate"):
            return {"status": "error", "errors": ["bad VCL"]}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch.object(sso, "delete_scoring_service") as delete_mock,
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.disable_scoring(LOG_SVC, TOKEN)

    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/200/activate") in calls
    assert ("PUT", f"/service/{LOG_SVC}/version/201/activate") not in calls
    delete_mock.assert_not_called()


@pytest.mark.usefixtures("_no_local_config_writes")
def test_resolve_tenant_matrix_prefers_local_tenant_path(tmp_path, monkeypatch):
    """Local ``matrix_{sid}.json`` wins without an FOS round-trip."""
    import json as _json

    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)

    tenant_path = sso._tenant_matrix_path(LOG_SVC)
    tenant_path.parent.mkdir(parents=True, exist_ok=True)
    tenant_path.write_text(_json.dumps({"vocab_size": 7, "version": "local-1"}))

    with patch("backend.state_sync.fetch_matrix_from_fos") as mock_fetch:
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)

    assert resolved == tenant_path
    mock_fetch.assert_not_called()


@pytest.mark.usefixtures("_no_local_config_writes")
def test_resolve_tenant_matrix_fetches_from_fos_when_local_missing(tmp_path, monkeypatch):
    """No local file → fall back to FOS for this tenant and materialise it locally."""
    import json as _json

    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)

    fos_matrix = {"vocab_size": 11, "version": "fos-1"}
    with patch("backend.state_sync.fetch_matrix_from_fos", return_value=fos_matrix) as mock_fetch:
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)

    expected = sso._tenant_matrix_path(LOG_SVC)
    assert resolved == expected
    mock_fetch.assert_called_once_with(LOG_SVC)
    assert _json.loads(expected.read_text())["version"] == "fos-1"


@pytest.mark.usefixtures("_no_local_config_writes")
def test_resolve_tenant_matrix_returns_none_when_nothing_trained(tmp_path, monkeypatch):
    """No local file AND FOS returns nothing → None, deploy proceeds with empty default."""
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)
    fake_root.write_text('{"vocab_size": 99, "version": "leaked-from-another-tenant"}')

    with patch("backend.state_sync.fetch_matrix_from_fos", return_value=None):
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)

    assert resolved is None


@pytest.mark.usefixtures("_no_local_config_writes")
def test_resolve_tenant_matrix_returns_none_when_fos_returns_empty_vocab(tmp_path, monkeypatch):
    """vocab_size==0 means an untrained default; treat it as no matrix."""
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)

    with patch("backend.state_sync.fetch_matrix_from_fos", return_value={"vocab_size": 0}):
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)

    assert resolved is None
    assert not sso._tenant_matrix_path(LOG_SVC).exists()


@pytest.mark.usefixtures("_no_local_config_writes")
def test_deploy_wasm_package_clones_links_uploads_activates(tmp_path, monkeypatch):
    """Toolchain-free deploy: clone the latest version → ensure the matrix KV
    resource link → upload the prebuilt package (raw multipart) → activate."""
    pkg = tmp_path / "session-scorer.tar.gz"
    pkg.write_bytes(b"FAKEWASMPKG")
    monkeypatch.setattr(sso, "_SCORER_PACKAGE", pkg)

    calls = []

    def fake_fastly(method, path, body=None, **kwargs):
        calls.append((method, path, body))
        if method == "GET" and path.endswith("/version"):
            return [{"number": 1}, {"number": 3}]
        if method == "PUT" and path.endswith("/version/3/clone"):
            return {"number": 4}
        return {}

    raw_calls = []

    def fake_fastly_raw(method, path, data, *, content_type, token, **kwargs):
        raw_calls.append((method, path, content_type, data))
        return {}

    with (
        patch.object(sso, "fastly", side_effect=fake_fastly),
        patch.object(sso, "fastly_raw", side_effect=fake_fastly_raw),
    ):
        sso._deploy_wasm_package(SCORE_SVC, "MATRIX_STORE", TOKEN)

    methods = [(m, p) for m, p, _ in calls]
    assert ("PUT", f"/service/{SCORE_SVC}/version/3/clone") in methods
    assert any(p.endswith("/version/4/resource") and (b or {}).get("name") == "scoring_matrix" for m, p, b in calls)
    assert ("PUT", f"/service/{SCORE_SVC}/version/4/activate") in methods
    assert raw_calls and raw_calls[0][0] == "PUT"
    assert raw_calls[0][1].endswith("/version/4/package")
    assert raw_calls[0][2].startswith("multipart/form-data; boundary=")
    assert b"FAKEWASMPKG" in raw_calls[0][3]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_deploy_wasm_package_missing_artifact_raises(tmp_path, monkeypatch):
    """A missing committed package is an operator error, not a silent no-op."""
    monkeypatch.setattr(sso, "_SCORER_PACKAGE", tmp_path / "does-not-exist.tar.gz")
    with patch.object(sso, "fastly"), patch.object(sso, "fastly_raw"):
        with pytest.raises(RuntimeError, match="prebuilt scorer package not found"):
            sso._deploy_wasm_package(SCORE_SVC, "MATRIX_STORE", TOKEN)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_write_matrix_to_kv_writes_tenant_matrix(tmp_path, monkeypatch):
    """The tenant's local matrix is PUT to the scoring_matrix KV store under
    the agreed key — never the legacy shared matrix.json."""
    import json as _json

    monkeypatch.setattr(sso, "_MATRIX_PATH", tmp_path / "matrix.json")
    tenant_path = sso._tenant_matrix_path(LOG_SVC)
    tenant_path.parent.mkdir(parents=True, exist_ok=True)
    tenant_path.write_text(_json.dumps({"vocab_size": 5, "version": "tenant-only"}))

    raw_calls = []

    def fake_fastly_raw(method, path, data, *, content_type, token, **kwargs):
        raw_calls.append((method, path, data))
        return {}

    with patch.object(sso, "fastly_raw", side_effect=fake_fastly_raw):
        sso._write_matrix_to_kv("MATRIX_STORE", LOG_SVC, TOKEN)

    assert len(raw_calls) == 1
    method, path, data = raw_calls[0]
    assert method == "PUT"
    assert path == "/resources/stores/kv/MATRIX_STORE/keys/matrix"
    from backend.scoring.matrix import serialize_kv

    assert isinstance(data, (bytes, bytearray))
    assert data[:4] == b"FSM1"
    assert b"tenant-only" in data
    assert data == serialize_kv({"vocab_size": 5, "version": "tenant-only"})


@pytest.mark.usefixtures("_no_local_config_writes")
def test_write_matrix_to_kv_skips_when_no_tenant_matrix(tmp_path, monkeypatch):
    """No tenant matrix anywhere → no KV write."""
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)
    fake_root.write_text('{"vocab_size": 99, "version": "leaked"}')

    raw_calls = []
    with (
        patch.object(sso, "fastly_raw", side_effect=lambda *a, **k: raw_calls.append(a)),
        patch("backend.state_sync.fetch_matrix_from_fos", return_value=None),
    ):
        sso._write_matrix_to_kv("MATRIX_STORE", LOG_SVC, TOKEN)

    assert raw_calls == []


def _scoring_meta_full() -> dict:
    return {
        "enabled": True,
        "scoring_service_id": SCORE_SVC,
        "scoring_keys_store_id": "KEYS",
        "scoring_config_store_id": "CFG",
        "scoring_matrix_store_id": "MTX",
        "scoring_domain": "scorer.edgecompute.app",
    }


@pytest.mark.usefixtures("_no_local_config_writes")
def test_deploy_wasm_package_skips_when_package_and_edge_unchanged(tmp_path):
    """_deploy_wasm_package skips the upload when the committed sha matches the
    prior stamp AND the live edge files_hash matches."""
    import hashlib

    pkg = tmp_path / "session-scorer.tar.gz"
    pkg.write_bytes(b"unchanged-bytes")
    sha = hashlib.sha256(b"unchanged-bytes").hexdigest()

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", "/service/SCORESVC/version"):
            return [{"number": 9, "active": True}]
        if (method, path) == ("GET", "/service/SCORESVC/version/9/package"):
            return {"metadata": {"files_hash": "EDGEHASH"}}
        return {}

    shared = MagicMock(side_effect=side_effect)
    raw = MagicMock()
    with (
        patch.object(sso, "_SCORER_PACKAGE", pkg),
        patch.object(sso, "fastly", shared),
        patch("backend.core.fastly.service.fastly", shared),
        patch.object(sso, "fastly_raw", raw),
    ):
        result = sso._deploy_wasm_package("SCORESVC", "MTX", TOKEN, prior_package_sha=sha, prior_files_hash="EDGEHASH")

    assert result["skipped"] is True
    assert result["version"] == 9
    assert result["sha"] == sha
    raw.assert_not_called()
    methods_paths = [(c.args[0], c.args[1]) for c in shared.call_args_list]
    assert not any(m == "PUT" and "/clone" in p for m, p in methods_paths)
    assert not any(m == "PUT" and p.endswith("/activate") for m, p in methods_paths)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_skips_logging_redeploy_when_reconciler_reports_no_change():
    """On a redeploy where the declarative reconciler finds the live VCL already
    current, enable_scoring reuses the active logging version."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        "scoring": {**_scoring_meta_full(), "request_secret": "sek", "deployed_package_sha": "PKG"},
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 8, "active": True}]
        return {}

    shared = MagicMock(side_effect=side_effect)
    meta = _scoring_meta()
    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "ensure_scoring_service", return_value=meta),
        patch.object(
            sso, "_deploy_wasm_package", return_value={"version": 2, "sha": "PKG", "files_hash": "F", "skipped": True}
        ),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch.object(sso, "fastly", shared),
        patch("backend.core.fastly.service.fastly", shared),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=None)
        result = sso.enable_scoring(LOG_SVC, TOKEN)

    assert result["logging_service_active_version"] == 8
    reconcile_mock.assert_called_once_with(LOG_SVC, TOKEN, status_cb=None)
    methods_paths = [(c.args[0], c.args[1]) for c in shared.call_args_list]
    assert not any(
        m == "PUT" and p.startswith(f"/service/{LOG_SVC}/") and p.endswith("/clone") for m, p in methods_paths
    )
    assert not any(
        m == "PUT" and p.startswith(f"/service/{LOG_SVC}/") and p.endswith("/activate") for m, p in methods_paths
    )


@pytest.mark.usefixtures("_no_local_config_writes")
def test_teardown_scoring_resources_strips_vcl_then_deletes():
    """teardown_scoring_resources strips the scoring VCL on a fresh clone
    (validate→activate) and THEN deletes the Compute service + stores."""
    order = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 50, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/50/clone"):
            return {"number": 51}
        if method == "GET" and path.endswith("/version/51/snippet"):
            return [{"name": n} for n in scoring_snippet_names()]
        if method == "GET" and path.endswith("/version/51/validate"):
            return {"status": "ok"}
        if method == "PUT" and path.endswith("/version/51/activate"):
            order.append("activate")
        return {}

    shared = MagicMock(side_effect=side_effect)

    def fake_delete(*args, **kwargs):
        order.append("delete_scoring_service")
        return []

    with (
        patch.object(sso, "fastly", shared),
        patch("backend.core.fastly.service.fastly", shared),
        patch.object(sso, "delete_scoring_service", side_effect=fake_delete) as delete_mock,
    ):
        failed = sso.teardown_scoring_resources(LOG_SVC, _scoring_meta_full(), TOKEN)

    assert failed == []
    methods_paths = [(c.args[0], c.args[1]) for c in shared.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/50/clone") in methods_paths
    assert any(m == "DELETE" and "/version/51/snippet/" in p for m, p in methods_paths)
    assert any(m == "DELETE" and "/version/51/backend/" in p for m, p in methods_paths)
    assert ("PUT", f"/service/{LOG_SVC}/version/51/activate") in methods_paths
    delete_mock.assert_called_once()
    assert delete_mock.call_args.kwargs["scoring_keys_store_id"] == "KEYS"
    assert delete_mock.call_args.kwargs["scoring_config_store_id"] == "CFG"
    assert delete_mock.call_args.kwargs["scoring_matrix_store_id"] == "MTX"
    assert order == ["activate", "delete_scoring_service"]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_teardown_scoring_resources_deletes_even_if_vcl_strip_fails():
    """A VCL-strip failure must not block the owned-resource deletion."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            raise RuntimeError("Fastly 503 on version probe")
        return {}

    shared = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso, "fastly", shared),
        patch("backend.core.fastly.service.fastly", shared),
        patch.object(sso, "delete_scoring_service", return_value=[]) as delete_mock,
    ):
        failed = sso.teardown_scoring_resources(LOG_SVC, _scoring_meta_full(), TOKEN)

    assert failed == []
    delete_mock.assert_called_once()


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_recv_exclusion_regex_requires_scoring_enabled():
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": False}}
    with patch.object(sso.svcconfig, "load_config", return_value=cfg):
        with pytest.raises(RuntimeError, match="not enabled"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/healthz$")


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_recv_exclusion_regex_works_without_request_secret():
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    fastly_mock = _make_clone_snippet_fastly_mock(active_version=300)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/healthz$")

    assert result["logging_service_active_version"] == 301
    assert ("PUT", f"/service/{LOG_SVC}/version/301/activate") in [
        (c.args[0], c.args[1]) for c in fastly_mock.call_args_list
    ]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_recv_exclusion_regex_happy_path_persists_and_activates():
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "request_secret": "s" * 64},
    }
    fastly_mock = _make_clone_snippet_fastly_mock(active_version=300)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=save_mock),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex=r"^/(healthz|metrics)$")

    assert result["logging_service_active_version"] == 301
    assert result["is_default"] is False
    assert result["effective_regex"] == r"^/(healthz|metrics)$"
    saved_cfg = save_mock.call_args_list[0].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] == r"^/(healthz|metrics)$"
    snippet_writes = [
        c
        for c in fastly_mock.call_args_list
        if c.args[0] in ("POST", "PUT") and "/snippet" in c.args[1] and "/version/301" in c.args[1]
    ]
    assert snippet_writes, "expected at least one snippet write on the draft"
    assert ("PUT", f"/service/{LOG_SVC}/version/301/activate") in [
        (c.args[0], c.args[1]) for c in fastly_mock.call_args_list
    ]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_recv_exclusion_regex_empty_string_persists_as_none():
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
            "exclude_url_regex": r"^/old$",
        },
    }
    fastly_mock = _make_clone_snippet_fastly_mock(active_version=300)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=save_mock),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="   ")

    saved_cfg = save_mock.call_args_list[0].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] is None
    assert result["is_default"] is True


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_recv_exclusion_regex_rolls_back_on_validate_failure():
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "request_secret": "s" * 64},
    }
    fastly_mock = _make_clone_snippet_fastly_mock(active_version=300, validate_ok=False, validate_error="bad regex")
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="bad")

    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/300/activate") in calls
    assert ("PUT", f"/service/{LOG_SVC}/version/301/activate") not in calls


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_recv_exclusion_regex_raises_when_no_config():
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/$")


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_recv_exclusion_regex_raises_when_no_active_version():
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "request_secret": "s" * 64},
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return []
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        with pytest.raises(RuntimeError, match="no active version"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/$")


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_enforce_status_code_requires_scoring_enabled():
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": False}}
    with patch.object(sso.svcconfig, "load_config", return_value=cfg):
        with pytest.raises(RuntimeError, match="not enabled"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=418)


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_enforce_status_code_works_without_request_secret():
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": True}}
    fastly_mock = _make_clone_snippet_fastly_mock(active_version=400)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=418)

    assert result["effective_status_code"] == 418
    assert result["logging_service_active_version"] == 401
    assert ("PUT", f"/service/{LOG_SVC}/version/401/activate") in [
        (c.args[0], c.args[1]) for c in fastly_mock.call_args_list
    ]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_enforce_status_code_happy_path():
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "request_secret": "s" * 64},
    }
    fastly_mock = _make_clone_snippet_fastly_mock(active_version=400)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=save_mock),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=418)

    assert result["effective_status_code"] == 418
    assert result["is_default"] is False
    assert result["logging_service_active_version"] == 401
    saved_cfg = save_mock.call_args_list[0].args[1]
    assert saved_cfg["scoring"]["enforce_status_code"] == 418
    snippet_writes = [
        c
        for c in fastly_mock.call_args_list
        if c.args[0] in ("POST", "PUT") and "/snippet" in c.args[1] and "/version/401" in c.args[1]
    ]
    assert snippet_writes


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_enforce_status_code_none_persists_as_none_and_uses_default():
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
            "enforce_status_code": 418,
        },
    }
    fastly_mock = _make_clone_snippet_fastly_mock(active_version=400)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=save_mock),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=None)

    assert result["is_default"] is True
    assert result["effective_status_code"] == 429
    saved_cfg = save_mock.call_args_list[0].args[1]
    assert saved_cfg["scoring"]["enforce_status_code"] is None


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_enforce_status_code_rolls_back_on_validate_failure():
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True, "request_secret": "s" * 64},
    }
    fastly_mock = _make_clone_snippet_fastly_mock(active_version=400, validate_ok=False, validate_error="bad enforce")
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=403)

    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/400/activate") in calls
    assert ("PUT", f"/service/{LOG_SVC}/version/401/activate") not in calls


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_enforce_status_code_raises_when_no_config():
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=403)


# ── Merged from tests/provision/test_session_scoring_call_order.py ──
"""Fastly call order for the session-scoring clone-validate-activate sites, pinned across the draft_version migration."""


def _order(calls: list[tuple[str, str]]) -> list[str]:
    out: list[str] = []
    for method, path in calls:
        if path.endswith("/clone"):
            out.append("clone")
        elif path.endswith("/validate"):
            out.append("validate")
        elif path.endswith("/activate"):
            out.append("activate:" + path.split("/version/")[1].split("/")[0])
        elif method == "PUT" and path.rstrip("/").rsplit("/", 1)[-1].isdigit() and "/version/" in path:
            out.append("comment")
        elif method == "PUT" and path.endswith("/package"):
            out.append("package")
        elif method == "DELETE":
            out.append("delete")
    return out


class _Fastly:
    """Records (method, path, kwargs) and answers the shapes the orchestrator depends on."""

    def __init__(self, *, active=200, clone_number=201, validate_ok=True, fail_on=None, rollback_raises=False):
        self.calls: list[tuple[str, str]] = []
        self.kwargs: list[dict] = []
        self.active = active
        self.clone_number = clone_number
        self.validate_ok = validate_ok
        self.fail_on = fail_on
        self.rollback_raises = rollback_raises

    def __call__(self, method, path, body=None, **kwargs):
        self.calls.append((method, path))
        self.kwargs.append(kwargs)
        if self.fail_on and self.fail_on(method, path):
            raise RuntimeError("edit rejected")
        if self.rollback_raises and path.endswith(f"/version/{self.active}/activate"):
            raise RuntimeError("rollback api down")
        if path.endswith("/clone"):
            return {"number": self.clone_number}
        if path.endswith("/validate"):
            return {"status": "ok"} if self.validate_ok else {"status": "error", "errors": ["bad VCL"]}
        if method == "GET" and path.endswith("/version"):
            return [{"number": self.active, "active": True}]
        if method == "GET" and path.endswith("/snippet"):
            return []
        if method == "GET" and path.endswith("/logging/s3"):
            return [{"name": "Fastly Object Storage Logs"}]
        return {}

    def kwargs_for(self, predicate) -> list[dict]:
        return [kw for (m, p), kw in zip(self.calls, self.kwargs) if predicate(m, p)]


def _cfg() -> dict:
    return {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        "scoring": {
            "enabled": True,
            "scoring_service_id": SCORE_SVC,
            "scoring_keys_store_id": "K",
            "scoring_config_store_id": "C",
        },
    }


def _patched(fake: _Fastly):
    return (
        patch.object(sso.svcconfig, "load_config", return_value=_cfg()),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fake),
        patch("backend.core.fastly.service.fastly", fake),
        patch.object(sso, "delete_scoring_service", return_value=[]),
        patch.object(sso, "_delete_scoring_matrix_from_fos"),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch("backend.state_sync.export_admin_state"),
    )


def _run_disable(fake: _Fastly):
    from contextlib import ExitStack

    with ExitStack() as stack:
        for p in _patched(fake):
            stack.enter_context(p)
        return sso.disable_scoring(LOG_SVC, TOKEN)


def test_disable_success_order_is_clone_comment_validate_activate():
    fake = _Fastly()
    _run_disable(fake)
    order = _order(fake.calls)
    assert order[:2] == ["clone", "comment"]
    assert order[-2:] == ["validate", "activate:201"]
    assert fake.kwargs_for(lambda m, p: p.endswith("/version/201/activate")) == [{"token": TOKEN}]


def test_disable_validation_failure_reactivates_previous_version_and_skips_draft_activation():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_disable(fake)
    order = _order(fake.calls)
    assert order[-2:] == ["validate", "activate:200"]
    assert "activate:201" not in order


def test_disable_edit_failure_skips_validate_and_rolls_back():
    fake = _Fastly(fail_on=lambda m, p: m == "PUT" and p.endswith("/logging/s3/Fastly%20Object%20Storage%20Logs"))
    with pytest.raises(RuntimeError, match="edit rejected"):
        _run_disable(fake)
    order = _order(fake.calls)
    assert "validate" not in order
    assert order[-1] == "activate:200"


def test_disable_failed_rollback_is_logged_not_masked(caplog):
    fake = _Fastly(validate_ok=False, rollback_raises=True)
    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(RuntimeError, match="Validation failed"):
            _run_disable(fake)
    assert any("rollback activation of version 200 failed" in r.getMessage() for r in caplog.records)


def test_disable_does_not_tear_down_compute_service_when_vcl_phase_fails():
    fake = _Fastly(validate_ok=False)
    delete_mock = MagicMock()
    from contextlib import ExitStack

    with ExitStack() as stack:
        for p in _patched(fake):
            stack.enter_context(p)
        stack.enter_context(patch.object(sso, "delete_scoring_service", delete_mock))
        with pytest.raises(RuntimeError):
            sso.disable_scoring(LOG_SVC, TOKEN)
    delete_mock.assert_not_called()


def _run_teardown(fake: _Fastly, *, delete_mock=None):
    from contextlib import ExitStack

    meta = {"scoring_service_id": SCORE_SVC, "scoring_keys_store_id": "K", "scoring_config_store_id": "C"}
    delete_mock = delete_mock if delete_mock is not None else MagicMock(return_value=[])
    with ExitStack() as stack:
        stack.enter_context(patch.object(sso, "fastly", fake))
        stack.enter_context(patch("backend.core.fastly.service.fastly", fake))
        stack.enter_context(patch.object(sso, "delete_scoring_service", delete_mock))
        return sso.teardown_scoring_resources(LOG_SVC, meta, TOKEN), delete_mock


def test_teardown_strip_order_is_clone_comment_validate_activate_then_compute_delete():
    fake = _Fastly()
    failed, delete_mock = _run_teardown(fake)
    assert failed == []
    assert _order(fake.calls)[:2] == ["clone", "comment"]
    assert _order(fake.calls)[-2:] == ["validate", "activate:201"]
    delete_mock.assert_called_once()


def test_teardown_strip_validation_failure_never_activates_anything_and_aborts_before_compute_delete():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Aborting scoring teardown"):
        _run_teardown(fake)
    order = _order(fake.calls)
    assert "activate:201" not in order
    assert not any(o.startswith("activate:") for o in order), "teardown strip must not roll back by activating"


def test_teardown_strip_failure_does_not_delete_the_compute_service():
    fake = _Fastly(validate_ok=False)
    delete_mock = MagicMock(return_value=[])
    with pytest.raises(RuntimeError, match="Aborting scoring teardown"):
        _run_teardown(fake, delete_mock=delete_mock)
    delete_mock.assert_not_called()


def test_teardown_strip_status_messages_report_the_failure():
    fake = _Fastly(validate_ok=False)
    msgs: list[str] = []
    meta = {"scoring_service_id": SCORE_SVC}
    with (
        patch.object(sso, "fastly", fake),
        patch("backend.core.fastly.service.fastly", fake),
        patch.object(sso, "delete_scoring_service"),
    ):
        with pytest.raises(RuntimeError):
            sso.teardown_scoring_resources(LOG_SVC, meta, TOKEN, status_cb=msgs.append)
    assert any(m.startswith("❌ Could not strip scoring VCL: Validation failed") for m in msgs)


def _swap_cfg() -> dict:
    return {"service_id": LOG_SVC, "scoring": {"enabled": True}}


def _swap_fake(**kw) -> _Fastly:
    return _Fastly(active=300, clone_number=301, **kw)


def _run_swap(fn, fake: _Fastly, **fn_kwargs):
    from contextlib import ExitStack

    with ExitStack() as stack:
        stack.enter_context(patch.object(sso.svcconfig, "load_config", return_value=_swap_cfg()))
        stack.enter_context(patch.object(sso.svcconfig, "_save_config_unlocked"))
        stack.enter_context(patch.object(sso, "fastly", fake))
        stack.enter_context(patch("backend.core.fastly.service.fastly", fake))
        return fn(LOG_SVC, TOKEN, **fn_kwargs)


@pytest.mark.parametrize(
    ("fn", "kwargs"),
    [
        (sso.update_recv_exclusion_regex, {"new_regex": "^/healthz$"}),
        (sso.update_enforce_status_code, {"new_status_code": 403}),
    ],
)
def test_swap_success_order_is_clone_comment_snippet_validate_activate(fn, kwargs):
    fake = _swap_fake()
    _run_swap(fn, fake, **kwargs)
    order = _order(fake.calls)
    assert order[0] == "clone"
    assert order[1] == "comment"
    assert order[-2:] == ["validate", "activate:301"]
    assert fake.kwargs_for(lambda m, p: p.endswith("/version/301/activate")) == [{"token": TOKEN}]


@pytest.mark.parametrize(
    ("fn", "kwargs"),
    [
        (sso.update_recv_exclusion_regex, {"new_regex": "bad"}),
        (sso.update_enforce_status_code, {"new_status_code": 403}),
    ],
)
def test_swap_validation_failure_reactivates_previous_version_after_the_failure_line(fn, kwargs, capsys):
    fake = _swap_fake(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_swap(fn, fake, **kwargs)
    order = _order(fake.calls)
    assert order[-2:] == ["validate", "activate:300"]
    assert "activate:301" not in order
    assert "failed: Validation failed" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("fn", "kwargs"),
    [
        (sso.update_recv_exclusion_regex, {"new_regex": "bad"}),
        (sso.update_enforce_status_code, {"new_status_code": 403}),
    ],
)
def test_swap_failed_rollback_is_logged_not_masked(fn, kwargs, caplog):
    fake = _swap_fake(validate_ok=False, rollback_raises=True)
    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(RuntimeError, match="Validation failed"):
            _run_swap(fn, fake, **kwargs)
    assert any("rollback activation of version 300 failed" in r.getMessage() for r in caplog.records)


def test_swap_clone_failure_still_reactivates_previous_version():
    fake = _swap_fake(fail_on=lambda m, p: p.endswith("/clone"))
    with pytest.raises(RuntimeError, match="edit rejected"):
        _run_swap(sso.update_recv_exclusion_regex, fake, new_regex="^/x$")
    assert _order(fake.calls)[-1] == "activate:300"


def _run_wasm(fake: _Fastly, tmp_path, *, matrix_store="MATRIX_STORE", raw=None):
    from contextlib import ExitStack

    pkg = tmp_path / "session-scorer.tar.gz"
    pkg.write_bytes(b"wasm-bytes")
    raw = raw if raw is not None else MagicMock(return_value={})
    with ExitStack() as stack:
        stack.enter_context(patch.object(sso, "_SCORER_PACKAGE", pkg))
        stack.enter_context(patch.object(sso, "fastly", fake))
        stack.enter_context(patch("backend.core.fastly.service.fastly", fake))
        stack.enter_context(patch.object(sso, "fastly_raw", raw))
        return sso._deploy_wasm_package(SCORE_SVC, matrix_store, TOKEN), raw


def _wasm_fake(**kw) -> _Fastly:
    return _Fastly(active=5, clone_number=6, **kw)


def test_wasm_deploy_order_is_clone_link_upload_activate_with_no_validate_or_comment(tmp_path):
    order_events: list[str] = []
    fake = _wasm_fake()
    raw = MagicMock(side_effect=lambda *a, **k: order_events.append("package") or {})
    _run_wasm(fake, tmp_path, raw=raw)
    calls = fake.calls
    assert ("PUT", f"/service/{SCORE_SVC}/version/5/clone") in calls
    clone_idx = calls.index(("PUT", f"/service/{SCORE_SVC}/version/5/clone"))
    link_idx = calls.index(("POST", f"/service/{SCORE_SVC}/version/6/resource"))
    activate_idx = calls.index(("PUT", f"/service/{SCORE_SVC}/version/6/activate"))
    assert clone_idx < link_idx < activate_idx
    assert order_events == ["package"]
    assert not any(p.endswith("/validate") for _, p in calls), "Compute deploy never called /validate"
    assert ("PUT", f"/service/{SCORE_SVC}/version/6") not in calls, "Compute deploy never set a comment"


def test_wasm_deploy_activate_keeps_expect_empty(tmp_path):
    fake = _wasm_fake()
    _run_wasm(fake, tmp_path)
    assert fake.kwargs_for(lambda m, p: p.endswith("/version/6/activate")) == [{"token": TOKEN, "expect_empty": True}]


def test_wasm_deploy_clones_the_highest_version_not_the_active_one(tmp_path):
    class _Versions(_Fastly):
        def __call__(self, method, path, body=None, **kwargs):
            if method == "GET" and path.endswith("/version"):
                self.calls.append((method, path))
                self.kwargs.append(kwargs)
                return [{"number": 3, "active": True}, {"number": 9, "active": False}]
            return super().__call__(method, path, body, **kwargs)

    fake = _Versions(active=3, clone_number=10)
    _run_wasm(fake, tmp_path)
    assert ("PUT", f"/service/{SCORE_SVC}/version/9/clone") in fake.calls


def test_wasm_deploy_upload_failure_leaves_the_draft_unactivated_and_makes_no_rollback_call(tmp_path):
    fake = _wasm_fake()
    raw = MagicMock(side_effect=RuntimeError("upload failed"))
    with pytest.raises(RuntimeError, match="upload failed"):
        _run_wasm(fake, tmp_path, raw=raw)
    assert not any(p.endswith("/activate") for _, p in fake.calls)


def test_wasm_deploy_without_matrix_store_skips_the_resource_link(tmp_path):
    fake = _wasm_fake()
    _run_wasm(fake, tmp_path, matrix_store="")
    assert not any(p.endswith("/resource") for _, p in fake.calls)
    assert ("PUT", f"/service/{SCORE_SVC}/version/6/activate") in fake.calls


SID = "svc"


def _api_order(calls: list[tuple[str, str]]) -> list[str]:
    out: list[str] = []
    for method, path in calls:
        if path.endswith("/clone"):
            out.append("clone")
        elif path.endswith("/validate"):
            out.append("validate")
        elif path.endswith("/activate"):
            out.append("activate:" + path.split("/version/")[1].split("/")[0])
        elif method == "PUT" and path.rstrip("/").rsplit("/", 1)[-1].isdigit() and "/version/" in path:
            out.append("comment")
        elif method == "POST" and path.endswith("/logging/s3"):
            out.append("post-endpoint")
        elif method == "DELETE":
            out.append("delete")
    return out


class _FastlyApi:
    def __init__(self, *, clone_number=11, validate_ok=True, fail_on=None, rollback_raises=False, active_ver=10):
        self.calls: list[tuple[str, str]] = []
        self.kwargs: list[dict] = []
        self.clone_number = clone_number
        self.validate_ok = validate_ok
        self.fail_on = fail_on
        self.rollback_raises = rollback_raises
        self.active_ver = active_ver

    def __call__(self, method, path, body=None, **kwargs):
        self.calls.append((method, path))
        self.kwargs.append(kwargs)
        if self.fail_on and self.fail_on(method, path):
            raise RuntimeError("edit rejected")
        if self.rollback_raises and path.endswith(f"/version/{self.active_ver}/activate"):
            raise RuntimeError("rollback api down")
        if path.endswith("/clone"):
            return {"number": self.clone_number}
        if path.endswith("/validate"):
            return {"status": "ok"} if self.validate_ok else {"status": "error", "errors": ["bad"]}
        if method == "GET":
            return []
        return {}


def _remove_endpoint(fake: _FastlyApi):
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake),
    ):
        return fastly_api.remove_logging_endpoint(SID, "MyEndpoint", "tok")


def test_remove_endpoint_comment_is_put_between_validate_and_activate():
    fake = _FastlyApi()
    _remove_endpoint(fake)
    assert _api_order(fake.calls) == ["clone", "delete", "validate", "comment", "activate:11"]


def test_remove_endpoint_keeps_expect_empty_on_deletes_and_not_on_activate():
    fake = _FastlyApi()
    _remove_endpoint(fake)
    delete_kwargs = [kw for (m, p), kw in zip(fake.calls, fake.kwargs) if m == "DELETE"]
    assert delete_kwargs and all(kw.get("expect_empty") is True for kw in delete_kwargs)
    activate_kwargs = [kw for (m, p), kw in zip(fake.calls, fake.kwargs) if p.endswith("/version/11/activate")]
    assert activate_kwargs == [{"token": "tok"}]


def test_remove_endpoint_validation_failure_reactivates_previous_version_without_comment():
    fake = _FastlyApi(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _remove_endpoint(fake)
    assert _api_order(fake.calls) == ["clone", "delete", "validate", "activate:10"]


def test_remove_endpoint_edit_failure_skips_validate_and_rolls_back():
    fake = _FastlyApi(fail_on=lambda m, p: m == "DELETE" and "/logging/s3/" in p)
    with pytest.raises(RuntimeError, match="edit rejected"):
        _remove_endpoint(fake)
    assert _api_order(fake.calls) == ["clone", "delete", "activate:10"]


def test_remove_endpoint_failed_rollback_is_logged_not_masked(caplog):
    fake = _FastlyApi(validate_ok=False, rollback_raises=True)
    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(RuntimeError, match="Validation failed"):
            _remove_endpoint(fake)
    assert any("rollback activation of version 10 failed" in r.getMessage() for r in caplog.records)


_MERGE_SID = "logsvc1"


@pytest.fixture
def reconcile():
    with (
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile,
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=True),
    ):
        mock_reconcile.return_value = SimpleNamespace(activated_version=3)
        yield mock_reconcile


def test_ensure_cdn_service_creates_config_when_absent(reconcile):
    assert svcconfig.load_config(_MERGE_SID) is None

    reconcile.side_effect = lambda **kw: svcconfig.mutate_config(
        _MERGE_SID, lambda c: c.setdefault("fos_proxy", {}).update(service_id="cdn123")
    )
    out = fastly_api.ensure_cdn_service({"service_id": _MERGE_SID, "fos_bucket": "b"}, "AK", "SK", "tok")

    saved = json.loads(svcconfig.config_path(_MERGE_SID).read_text())
    assert saved["fos_bucket"] == "b"
    assert saved["fos_access_key_id"] == "AK"
    assert saved["fos_secret_access_key"] == "SK"
    assert saved["rum"]["cid_salt"]
    assert out["id"] == "cdn123"


def test_ensure_cdn_service_merges_into_existing_config_and_keeps_cid_salt(reconcile):
    svcconfig.save_config(_MERGE_SID, {"service_id": _MERGE_SID, "keep_me": 1, "rum": {"enabled": True}})
    salt = svcconfig.load_config(_MERGE_SID)["rum"]["cid_salt"]

    # the orchestrator's in-memory cfg replaces the rum block wholesale, without the salt
    fastly_api.ensure_cdn_service(
        {"service_id": _MERGE_SID, "fos_bucket": "b", "rum": {"enabled": False}}, "AK", "SK", "tok"
    )

    saved = json.loads(svcconfig.config_path(_MERGE_SID).read_text())
    assert saved["keep_me"] == 1
    assert saved["fos_bucket"] == "b"
    assert saved["fos_access_key_id"] == "AK"
    assert saved["rum"]["enabled"] is False
    assert saved["rum"]["cid_salt"] == salt


def test_redeploy_cdn_vcl_sets_rate_limiting_without_touching_other_keys():
    svcconfig.save_config(
        _MERGE_SID,
        {"service_id": _MERGE_SID, "cdn_service_id": "cdn9", "fos_proxy": {"service_id": "cdn9"}, "other": [1, 2]},
    )
    with patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile:
        mock_reconcile.return_value = SimpleNamespace(activated_version=4)
        assert fastly_api.redeploy_cdn_vcl("cdn9", "tok", rate_limiting=False) == 4

    saved = json.loads(svcconfig.config_path(_MERGE_SID).read_text())
    assert saved["fos_proxy"] == {"service_id": "cdn9", "rate_limiting_enabled": False}
    assert saved["other"] == [1, 2]


def test_redeploy_cdn_vcl_creates_missing_fos_proxy_block():
    svcconfig.save_config(_MERGE_SID, {"service_id": _MERGE_SID, "cdn_service_id": "cdn9"})
    with patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_reconcile:
        mock_reconcile.return_value = SimpleNamespace(activated_version=5)
        fastly_api.redeploy_cdn_vcl("cdn9", "tok", rate_limiting=True)

    assert svcconfig.load_config(_MERGE_SID)["fos_proxy"] == {"rate_limiting_enabled": True}


def _drain(gen):
    return list(gen)


@pytest.fixture
def stored():
    svcconfig.save_config(_MERGE_SID, {"service_id": _MERGE_SID, "keep_me": 1, "provisioning": {"keep_prov": "x"}})
    return svcconfig.load_config(_MERGE_SID)


@pytest.fixture
def fastly_stubs():
    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None) as rl,
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id=_MERGE_SID, activated_version=11, changes_applied=["fmt"]),
        ) as reconcile,
    ):
        yield rl, reconcile


def test_persists_requested_fields_and_keeps_everything_else(stored, fastly_stubs):

    salt = stored["rum"]["cid_salt"]

    events = _drain(
        fastly_api.update_logging_endpoint(
            {
                "logging_service_id": _MERGE_SID,
                "provisioning": {"endpoint_name": "EP"},
                "sample_rate": 50,
                "edge_only": True,
                "log_period": 120,
                "fos_path": "/raw/%H/",
            },
            "tok",
        )
    )

    saved = _persisted()
    assert saved["provisioning"] == {"keep_prov": "x", "sample_rate": 50, "edge_only": True, "endpoint_name": "EP"}
    assert saved["log_period"] == 120
    assert saved["fos_path"] == "/raw/%H/"
    assert saved["keep_me"] == 1
    assert saved["rum"]["cid_salt"] == salt
    assert events[-1]["type"] == "done"


def test_rate_limit_probe_result_lands_without_clobbering_a_concurrent_writer(stored, fastly_stubs):
    rl, _ = fastly_stubs

    def probe(_token, _sid):
        assert not _lock_held(), "config lock held across a Fastly call"
        svcconfig.mutate_config(_MERGE_SID, lambda c: c.update(concurrent_key="kept"))
        return True

    rl.side_effect = probe

    _drain(fastly_api.update_logging_endpoint({"logging_service_id": _MERGE_SID, "log_period": 60}, "tok"))

    saved = _persisted()
    assert saved["concurrent_key"] == "kept"
    assert saved["provisioning"]["rate_limiting"] is True
    assert saved["keep_me"] == 1


def test_config_lock_is_not_held_while_reconciling(stored, fastly_stubs):
    _, reconcile = fastly_stubs
    seen: list[bool] = []

    def spy(*_a, **_kw):
        seen.append(_lock_held())
        return ReconciliationResult(service_id=_MERGE_SID, activated_version=11, changes_applied=["fmt"])

    reconcile.side_effect = spy

    _drain(fastly_api.update_logging_endpoint({"logging_service_id": _MERGE_SID, "log_period": 60}, "tok"))

    assert seen == [False]


def test_missing_config_raises_before_any_fastly_call(fastly_stubs):
    rl, reconcile = fastly_stubs

    with pytest.raises(RuntimeError, match="not found|empty"):
        _drain(fastly_api.update_logging_endpoint({"logging_service_id": _MERGE_SID, "log_period": 60}, "tok"))

    rl.assert_not_called()
    reconcile.assert_not_called()
    assert not svcconfig.config_path(_MERGE_SID).exists()


def _state(**overrides) -> dict:
    base = {
        "logging_service_id": _MERGE_SID,
        "service_name": "Test Service",
        "fos_region": "us-east-1",
        "fos_bucket_name": "test-bucket",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "secret",
        "cdn_url": "https://test.example",
        "cdn_secret": "cdn-secret",
        "cdn_service_id": "cdn-1",
        "admin_token": "tok",
        "log_period": 60,
        "log_fields": {"groups": ["A"]},
    }
    base.update(overrides)
    return base


def test_first_write_creates_the_config_with_a_cid_salt():

    assert svcconfig.load_config(_MERGE_SID) is None

    orchestrator.write_service_config(_state())

    saved = _persisted()
    assert saved["service_id"] == _MERGE_SID
    assert saved["name"] == "Test Service"
    assert saved["fos_bucket"] == "test-bucket"
    assert saved["fastly_api_key"] == "tok"
    assert saved["provisioning"]["cron_sync"]["interval_seconds"] == 60
    assert saved["created_at"]
    assert saved["rum"]["cid_salt"]


def test_rewrite_keeps_code_managed_blocks_and_the_cid_salt():
    svcconfig.save_config(
        _MERGE_SID,
        {
            "service_id": _MERGE_SID,
            "scoring": {"enabled": True, "scoring_service_id": "S"},
            "cmcd": {"enabled": True},
            "ngwaf_workspace_id": "ws",
            "rum": {"faro_version": "1.2.3"},
            "log_fields": {"groups": ["A"], "custom_fields": [{"name": "mine", "duckdb_type": "VARCHAR"}]},
        },
    )
    salt = svcconfig.load_config(_MERGE_SID)["rum"]["cid_salt"]
    created_at = svcconfig.load_config(_MERGE_SID)["created_at"]

    orchestrator.write_service_config(_state())

    saved = _persisted()
    assert saved["scoring"] == {"enabled": True, "scoring_service_id": "S"}
    assert saved["cmcd"] == {"enabled": True}
    assert saved["ngwaf_workspace_id"] == "ws"
    assert saved["rum"]["faro_version"] == "1.2.3"
    assert saved["rum"]["cid_salt"] == salt
    assert saved["created_at"] == created_at
    assert [cf["name"] for cf in saved["log_fields"]["custom_fields"]] == ["mine"]


def test_rewrite_does_not_replace_the_cid_salt_with_one_from_the_request_body():
    svcconfig.save_config(_MERGE_SID, {"service_id": _MERGE_SID})
    salt = svcconfig.load_config(_MERGE_SID)["rum"]["cid_salt"]

    orchestrator.write_service_config(_state(rum={"enabled": True, "cid_salt": "attacker-chosen"}))

    saved = _persisted()
    assert saved["rum"]["cid_salt"] == salt
    assert saved["rum"]["enabled"] is True


def test_rewrite_keeps_stored_keys_the_wizard_does_not_own():
    svcconfig.save_config(
        _MERGE_SID,
        {
            "service_id": _MERGE_SID,
            "fos_proxy": {"service_id": "cdn-proxy"},
            "status": {"state": "ok"},
            "last_activated_version": 7,
            "rum_vcl_sha": "sha",
        },
    )

    orchestrator.write_service_config(_state())

    saved = _persisted()
    assert saved["fos_proxy"] == {"service_id": "cdn-proxy"}
    assert saved["status"] == {"state": "ok"}
    assert saved["last_activated_version"] == 7
    assert saved["rum_vcl_sha"] == "sha"
    assert saved["fos_bucket"] == "test-bucket"


_USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}


def _names(fields: list[dict]) -> set[str]:
    return {cf["name"] for cf in fields}


def test_flags_read_both_features():
    assert system_feature_flags({}) == (False, False)
    assert system_feature_flags({"cmcd": {"enabled": True}}) == (False, True)
    assert system_feature_flags({"scoring": {"enabled": True}}) == (True, False)
    assert system_feature_flags({"scoring": {"enabled": True}, "cmcd": {"enabled": True}}) == (True, True)


def test_flags_tolerate_null_blocks():
    """A config that carries ``cmcd: null`` must not raise."""
    assert system_feature_flags({"cmcd": None, "scoring": None}) == (False, False)


def test_cmcd_fields_reinjected_when_enabled():
    """THE REGRESSION: a non-empty list that omits cmcd_* must regain them."""
    out = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=False, cmcd_enabled=True)
    names = _names(out)
    for name in _CMCD_FIELD_NAMES:
        assert name in names, f"CMCD field {name!r} was not re-injected"
    assert "my_custom" in names, "user custom_field was wrongly stripped"


def test_cmcd_fields_stripped_when_disabled():
    """Disable must converge too — stale cmcd_* entries are removed."""
    stale = [_USER_FIELD, {"name": "cmcd_sid", "duckdb_type": "VARCHAR"}]
    out = reconcile_system_custom_fields(stale, scoring_enabled=False, cmcd_enabled=False)
    assert _names(out) == {"my_custom"}


def test_stale_cmcd_entries_replaced_by_canonical():
    """A remote/partial cmcd_* entry is replaced, not duplicated."""
    stale = [{"name": "cmcd_sid", "duckdb_type": "WRONG", "enabled": False}]
    out = reconcile_system_custom_fields(stale, scoring_enabled=False, cmcd_enabled=True)
    sids = [cf for cf in out if cf["name"] == "cmcd_sid"]
    assert len(sids) == 1, "cmcd_sid duplicated instead of replaced"
    assert sids[0]["duckdb_type"] == "VARCHAR"
    assert sids[0]["enabled"] is True


def test_both_features_coexist():
    out = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=True, cmcd_enabled=True)
    names = _names(out)
    assert _CMCD_FIELD_NAMES <= names
    assert _SCORING_FIELD_NAMES <= names
    assert "my_custom" in names


def test_scoring_enabled_does_not_drag_in_cmcd():
    """The two features are independent — scoring on, CMCD off."""
    out = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=True, cmcd_enabled=False)
    names = _names(out)
    assert _SCORING_FIELD_NAMES <= names
    assert not (_CMCD_FIELD_NAMES & names)


def test_none_input_is_safe():
    out = reconcile_system_custom_fields(None, scoring_enabled=False, cmcd_enabled=True)
    assert _CMCD_FIELD_NAMES <= _names(out)


def test_returned_entries_are_copies():
    """Callers mutating the result must not corrupt the module-level canon."""
    first = reconcile_system_custom_fields(None, scoring_enabled=False, cmcd_enabled=True)
    first[0]["label"] = "MUTATED"
    second = reconcile_system_custom_fields(None, scoring_enabled=False, cmcd_enabled=True)
    assert second[0]["label"] != "MUTATED"


def test_idempotent():
    once = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=True, cmcd_enabled=True)
    twice = reconcile_system_custom_fields(once, scoring_enabled=True, cmcd_enabled=True)
    assert _names(once) == _names(twice)
    assert len(once) == len(twice), "repeat reconcile duplicated entries"


def test_cfg_level_reconcile_mutates_in_place():
    cfg = {
        "cmcd": {"enabled": True},
        "log_fields": {"schema_version": 2, "custom_fields": [_USER_FIELD]},
    }
    reconcile_cfg_system_custom_fields(cfg)
    names = _names(cfg["log_fields"]["custom_fields"])
    assert _CMCD_FIELD_NAMES <= names
    assert "my_custom" in names


def test_cfg_level_reconcile_creates_missing_log_fields():
    """A config with no log_fields block still gets the v2 default shape."""
    cfg = {"cmcd": {"enabled": True}}
    reconcile_cfg_system_custom_fields(cfg)
    assert cfg["log_fields"]["schema_version"] == 2
    assert _CMCD_FIELD_NAMES <= _names(cfg["log_fields"]["custom_fields"])


def test_cfg_level_reconcile_strips_when_feature_off():
    cfg = {
        "log_fields": {
            "schema_version": 2,
            "custom_fields": [_USER_FIELD, {"name": "cmcd_br", "duckdb_type": "INTEGER"}],
        }
    }
    reconcile_cfg_system_custom_fields(cfg)
    assert _names(cfg["log_fields"]["custom_fields"]) == {"my_custom"}


# ── Merged from tests/provision/test_session_scoring_config_merge.py ──
_SID_session_scoring_config_merge = "logsvc1"

USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}

_META = {
    "scoring_service_id": "ScoringSvcZ",
    "scoring_service_name": f"Session Scoring Service for {_SID_session_scoring_config_merge}",
    "scoring_domain": "scorer.example.test",
    "scoring_keys_store_id": "KEYS",
    "scoring_config_store_id": "CFG",
    "scoring_matrix_store_id": "MATRIX",
    "request_secret": "s" * 64,
}


def _concurrent_write(**top_level) -> None:

    assert not _lock_held(), "config lock held across a Fastly call"

    def _apply(c: dict) -> None:
        c.update(top_level)
        c.setdefault("log_fields", {}).setdefault("custom_fields", []).append(dict(USER_FIELD))

    svcconfig.mutate_config(_SID_session_scoring_config_merge, _apply)


def _field_names(cfg: dict) -> set[str]:
    return {cf["name"] for cf in cfg["log_fields"]["custom_fields"]}


@pytest.fixture
def base_cfg():
    svcconfig.save_config(
        _SID_session_scoring_config_merge,
        {
            "service_id": _SID_session_scoring_config_merge,
            "keep_me": 1,
            "log_fields": {"groups": ["A"], "custom_fields": []},
            "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        },
    )
    return svcconfig.load_config(_SID_session_scoring_config_merge)


@pytest.fixture
def enable_stubs():
    with (
        patch.object(sso, "ensure_scoring_service", return_value=dict(_META)) as ensure,
        patch.object(
            sso, "_deploy_wasm_package", return_value={"sha": "pkg", "version": 3, "files_hash": "fh"}
        ) as deploy,
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", MagicMock(return_value={})),
        patch.object(sso, "get_active_version", return_value=10),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile,
    ):
        reconcile.return_value = MagicMock(error=None, activated_version=11)
        yield MagicMock(ensure=ensure, deploy=deploy, reconcile=reconcile)


@contextmanager
def _fake_draft(*_a, **_kw):
    yield 201


@pytest.fixture
def disable_stubs():
    with (
        patch.object(sso, "get_active_version", return_value=200),
        patch.object(sso, "draft_version", _fake_draft),
        patch.object(sso, "_remove_scoring_snippets") as snippets,
        patch.object(sso, "_remove_scoring_backend"),
        patch.object(sso, "ensure_vcl_snippet"),
        patch.object(sso, "fastly", MagicMock(return_value={})),
        patch("backend.core.fastly.service.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.generate_capture_vcl", return_value={"recv": ""}),
        patch.object(sso, "delete_scoring_service", return_value=[]) as delete,
        patch.object(sso, "_delete_scoring_matrix_from_fos"),
        patch("backend.state_sync.export_admin_state"),
    ):
        yield MagicMock(snippets=snippets, delete=delete)


@pytest.fixture
def enabled_cfg(base_cfg):
    def _apply(c: dict) -> None:
        c["scoring"] = {
            "enabled": True,
            "scoring_service_id": "ScoringSvcZ",
            "scoring_keys_store_id": "KEYS",
            "scoring_config_store_id": "CFG",
            "scoring_matrix_store_id": "MATRIX",
            "exclude_url_regex": "operator-set",
        }
        sso._add_scoring_custom_fields(c)

    svcconfig.mutate_config(_SID_session_scoring_config_merge, _apply)
    return svcconfig.load_config(_SID_session_scoring_config_merge)


def test_enable_persists_scoring_block_and_fields_and_keeps_the_rest(base_cfg, enable_stubs):
    salt = base_cfg["rum"]["cid_salt"]

    sso.enable_scoring(_SID_session_scoring_config_merge, TOKEN)

    saved = _persisted()
    assert saved["scoring"]["enabled"] is True
    assert saved["scoring"]["scoring_service_id"] == "ScoringSvcZ"
    assert saved["scoring"]["deployed_package_sha"] == "pkg"
    assert saved["scoring"]["request_secret"] == "s" * 64
    assert set(sso._SCORING_FIELD_NAMES) <= _field_names(saved)
    assert saved["keep_me"] == 1
    assert saved["log_fields"]["groups"] == ["A"]
    assert saved["rum"]["cid_salt"] == salt


def test_enable_does_not_hold_the_config_lock_across_fastly_calls(base_cfg, enable_stubs):
    held: list[bool] = []
    enable_stubs.ensure.side_effect = lambda *_a, **_kw: (held.append(_lock_held()), dict(_META))[1]
    enable_stubs.deploy.side_effect = lambda *_a, **_kw: (
        held.append(_lock_held()),
        {"sha": "pkg", "version": 3, "files_hash": "fh"},
    )[1]
    enable_stubs.reconcile.side_effect = lambda *_a, **_kw: (
        held.append(_lock_held()),
        MagicMock(error=None, activated_version=11),
    )[1]

    sso.enable_scoring(_SID_session_scoring_config_merge, TOKEN)

    assert held == [False, False, False]


def test_enable_keeps_writes_made_while_the_scorer_deploys(base_cfg, enable_stubs):
    def _deploy(*_a, **_kw):
        _concurrent_write(concurrent_key="kept")
        return {"sha": "pkg", "version": 3, "files_hash": "fh"}

    enable_stubs.deploy.side_effect = _deploy

    sso.enable_scoring(_SID_session_scoring_config_merge, TOKEN)

    saved = _persisted()
    assert saved["concurrent_key"] == "kept"
    assert "my_custom" in _field_names(saved)
    assert set(sso._SCORING_FIELD_NAMES) <= _field_names(saved)
    assert saved["scoring"]["enabled"] is True


def test_enable_keeps_an_operator_regex_change_made_while_deploying(enabled_cfg, enable_stubs):
    def _deploy(*_a, **_kw):
        svcconfig.mutate_config(
            _SID_session_scoring_config_merge, lambda c: c["scoring"].update(exclude_url_regex="changed-mid-deploy")
        )
        return {"sha": "pkg", "version": 3, "files_hash": "fh"}

    enable_stubs.deploy.side_effect = _deploy

    sso.enable_scoring(_SID_session_scoring_config_merge, TOKEN)

    assert _persisted()["scoring"]["exclude_url_regex"] == "changed-mid-deploy"


def test_enable_rollback_removes_only_scoring_state_and_keeps_concurrent_writes(base_cfg, enable_stubs):
    def _reconcile(*_a, **_kw):
        _concurrent_write(concurrent_key="kept")
        raise RuntimeError("reconcile exploded")

    enable_stubs.reconcile.side_effect = _reconcile

    with pytest.raises(RuntimeError, match="reconcile exploded"):
        sso.enable_scoring(_SID_session_scoring_config_merge, TOKEN)

    saved = _persisted()
    assert "scoring" not in saved
    assert not (set(sso._SCORING_FIELD_NAMES) & _field_names(saved))
    assert "my_custom" in _field_names(saved)
    assert saved["concurrent_key"] == "kept"
    assert saved["rum"]["cid_salt"] == base_cfg["rum"]["cid_salt"]


def test_disable_strips_scoring_and_keeps_user_fields(enabled_cfg, disable_stubs):
    sso.disable_scoring(_SID_session_scoring_config_merge, TOKEN)

    saved = _persisted()
    assert "scoring" not in saved
    assert not (set(sso._SCORING_FIELD_NAMES) & _field_names(saved))
    assert saved["keep_me"] == 1
    assert saved["rum"]["cid_salt"] == enabled_cfg["rum"]["cid_salt"]


def test_disable_keeps_writes_made_during_the_vcl_phase(enabled_cfg, disable_stubs):
    disable_stubs.snippets.side_effect = lambda *_a, **_kw: _concurrent_write(concurrent_key="kept")

    sso.disable_scoring(_SID_session_scoring_config_merge, TOKEN)

    saved = _persisted()
    assert saved["concurrent_key"] == "kept"
    assert "my_custom" in _field_names(saved)
    assert "scoring" not in saved


def test_disable_keeps_writes_made_during_compute_teardown(enabled_cfg, disable_stubs):
    def _delete(*_a, **_kw):
        _concurrent_write(concurrent_key="kept")
        return []

    disable_stubs.delete.side_effect = _delete

    sso.disable_scoring(_SID_session_scoring_config_merge, TOKEN)

    saved = _persisted()
    assert saved["concurrent_key"] == "kept"
    assert "scoring" not in saved


@pytest.fixture
def swap_stubs():
    with (
        patch.object(sso, "get_active_version", return_value=200),
        patch.object(sso, "_clone_swap_snippet", return_value=201),
    ):
        yield


def test_update_recv_exclusion_regex_sets_only_that_key(enabled_cfg, swap_stubs):
    sso.update_recv_exclusion_regex(_SID_session_scoring_config_merge, TOKEN, new_regex="  \\.foo$ ")

    saved = _persisted()
    assert saved["scoring"]["exclude_url_regex"] == "\\.foo$"
    assert saved["scoring"]["scoring_service_id"] == "ScoringSvcZ"
    assert saved["keep_me"] == 1


def test_update_enforce_status_code_sets_only_that_key(enabled_cfg, swap_stubs):
    sso.update_enforce_status_code(_SID_session_scoring_config_merge, TOKEN, new_status_code=403)

    saved = _persisted()
    assert saved["scoring"]["enforce_status_code"] == 403
    assert saved["scoring"]["exclude_url_regex"] == "operator-set"
    assert saved["keep_me"] == 1


@pytest.mark.parametrize(
    "call",
    [
        lambda: sso.update_recv_exclusion_regex(_SID_session_scoring_config_merge, TOKEN, new_regex="x"),
        lambda: sso.update_enforce_status_code(_SID_session_scoring_config_merge, TOKEN, new_status_code=403),
    ],
)
def test_updates_refuse_and_write_nothing_when_scoring_is_disabled(base_cfg, swap_stubs, call):
    before = _persisted()

    with pytest.raises(RuntimeError, match="not enabled"):
        call()

    assert _persisted() == before
    assert "scoring" not in _persisted()
