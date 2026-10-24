"""Tests for backend.provision.session_scoring_orchestrator.

Covers the happy-path Fastly call order, rollback shape, per-tenant matrix
resolution, Wasm package deployment, smart-redeploy skipping, full teardown,
re-publish helpers (``update_recv_exclusion_regex``, ``update_enforce_status_code``),
request-secret self-heal on re-enable, backend/snippet 404-tolerance helpers,
and status_cb fan-out.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.provision import session_scoring_orchestrator as sso
from backend.provision.session_scoring_vcl import (
    SCORING_DELIVER_NAME,
    SCORING_FETCH_NAME,
    SCORING_RECV_NAME,
    scoring_snippet_names,
)
from tests._helpers.scoring_orchestrator import (
    make_happy_path_fastly_mock,
    make_happy_path_fastly_mock_with_post_secret,
    make_scoring_meta,
)

LOG_SVC = "TestLogSvc456"
SCORE_SVC = "ScoringSvcZ"
TOKEN = "FAKE_TOKEN"
KEYS_STORE = "KEYS_STORE_ID"
CFG_STORE = "CFG_STORE_ID"


@pytest.fixture(autouse=True)
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


# ── _add_scoring_custom_fields / _remove_scoring_custom_fields ────────────────


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


def test_add_custom_fields_is_idempotent():
    """Re-running enable_scoring shouldn't duplicate the scoring fields."""
    cfg = {"log_fields": {"custom_fields": []}}
    sso._add_scoring_custom_fields(cfg)
    sso._add_scoring_custom_fields(cfg)
    names = [cf["name"] for cf in cfg["log_fields"]["custom_fields"]]
    assert len(names) == len(sso._SCORING_FIELD_NAMES)
    assert sorted(names) == sorted(sso._SCORING_FIELD_NAMES)


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


def test_remove_custom_fields_handles_missing_log_fields():
    """A config without a log_fields block should not crash on remove."""
    cfg = {}
    sso._remove_scoring_custom_fields(cfg)
    assert cfg == {}


# ── _remove_scoring_backend / _remove_scoring_snippets: 404 tolerance ─────


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


# ── enable_scoring: happy path & drift identity ──────────────────────────────


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


def test_scorer_vcl_fingerprint_deterministic_and_service_scoped():
    """The VCL drift fingerprint depends only on the generator code + service id."""
    a = sso.scorer_vcl_fingerprint(LOG_SVC)
    assert a == sso.scorer_vcl_fingerprint(LOG_SVC)
    assert sso.scorer_vcl_fingerprint("SomeOtherSvc") != a


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


# ── enable_scoring: missing-config + secret heal + rollback branches ────────


def test_enable_scoring_raises_when_no_config():
    """A logging service ID we don't recognise should fail fast."""
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.enable_scoring("UnknownSvc", TOKEN)


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


# ── disable_scoring ─────────────────────────────────────────────────────────


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


# ── _resolve_tenant_matrix_for_deploy ────────────────────────────────────────


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


def test_resolve_tenant_matrix_returns_none_when_nothing_trained(tmp_path, monkeypatch):
    """No local file AND FOS returns nothing → None, deploy proceeds with empty default."""
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)
    fake_root.write_text('{"vocab_size": 99, "version": "leaked-from-another-tenant"}')

    with patch("backend.state_sync.fetch_matrix_from_fos", return_value=None):
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)

    assert resolved is None


def test_resolve_tenant_matrix_returns_none_when_fos_returns_empty_vocab(tmp_path, monkeypatch):
    """vocab_size==0 means an untrained default; treat it as no matrix."""
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)

    with patch("backend.state_sync.fetch_matrix_from_fos", return_value={"vocab_size": 0}):
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)

    assert resolved is None
    assert not sso._tenant_matrix_path(LOG_SVC).exists()


# ── _deploy_wasm_package & _write_matrix_to_kv ──────────────────────────────


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


def test_deploy_wasm_package_missing_artifact_raises(tmp_path, monkeypatch):
    """A missing committed package is an operator error, not a silent no-op."""
    monkeypatch.setattr(sso, "_SCORER_PACKAGE", tmp_path / "does-not-exist.tar.gz")
    with patch.object(sso, "fastly"), patch.object(sso, "fastly_raw"):
        with pytest.raises(RuntimeError, match="prebuilt scorer package not found"):
            sso._deploy_wasm_package(SCORE_SVC, "MATRIX_STORE", TOKEN)


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


# ── Smart redeploy: edge-checked skip of no-op legs ──────────────────────────


def _scoring_meta_full() -> dict:
    return {
        "enabled": True,
        "scoring_service_id": SCORE_SVC,
        "scoring_keys_store_id": "KEYS",
        "scoring_config_store_id": "CFG",
        "scoring_matrix_store_id": "MTX",
        "scoring_domain": "scorer.edgecompute.app",
    }


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


# ── teardown_scoring_resources ───────────────────────────────────────────────


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


# ── update_recv_exclusion_regex ──────────────────────────────────────────────


def test_update_recv_exclusion_regex_requires_scoring_enabled():
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": False}}
    with patch.object(sso.svcconfig, "load_config", return_value=cfg):
        with pytest.raises(RuntimeError, match="not enabled"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/healthz$")


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


def test_update_recv_exclusion_regex_raises_when_no_config():
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/$")


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


# ── update_enforce_status_code ───────────────────────────────────────────────


def test_update_enforce_status_code_requires_scoring_enabled():
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": False}}
    with patch.object(sso.svcconfig, "load_config", return_value=cfg):
        with pytest.raises(RuntimeError, match="not enabled"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=418)


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


def test_update_enforce_status_code_raises_when_no_config():
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=403)
