"""Tests for backend.provision.session_scoring_orchestrator, call order, CMCD fields, and config merge."""

from __future__ import annotations

import hashlib
import json
import logging
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from backend.provision.cmcd_fields import (
    _CMCD_FIELD_NAMES,
    reconcile_cfg_system_custom_fields,
    reconcile_system_custom_fields,
    system_feature_flags,
)
from backend.provision.session_scoring_vcl import (
    DEFAULT_ASSET_EXT_REGEX,
    SCORING_DELIVER_NAME,
    SCORING_FETCH_NAME,
    SCORING_RECV_NAME,
    scoring_snippet_names,
)
from backend.scoring.matrix import serialize_kv
from typer.testing import CliRunner

from backend import config as svcconfig
from backend.provision import cli, fastly_api, orchestrator
from backend.provision import session_scoring_orchestrator as sso
from backend.provision.cli import app
from backend.provision.declarative.reconciler import ReconciliationResult
from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES
from tests._helpers.config_fakes import (
    config_lock_held as _lock_held,
)
from tests._helpers.config_fakes import (
    make_happy_path_fastly_mock,
    make_happy_path_fastly_mock_with_post_secret,
    make_scoring_meta,
    mutate_via_save,
)
from tests._helpers.config_fakes import (
    read_persisted_config as _persisted,
)

LOG_SVC = "TestLogSvc456"
SCORE_SVC = "ScoringSvcZ"
TOKEN = "FAKE_TOKEN"
KEYS_STORE = "KEYS_STORE_ID"
CFG_STORE = "CFG_STORE_ID"
SID = "svc"
_MERGE_SID = "logsvc1"
_USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}


@pytest.fixture
def _no_local_config_writes():
    with patch("backend.provision.declarative.reconciler._update_local_config"):
        yield


def _scoring_meta(*, with_secret: bool = True) -> dict:
    return make_scoring_meta(
        log_svc=LOG_SVC, score_svc=SCORE_SVC, keys_store=KEYS_STORE, cfg_store=CFG_STORE, with_secret=with_secret
    )


def _happy_path_fastly_mock():
    return make_happy_path_fastly_mock(log_svc=LOG_SVC)


def _happy_path_fastly_mock_with_post_secret(*, post_secret_409: bool = False):
    return make_happy_path_fastly_mock_with_post_secret(
        log_svc=LOG_SVC, keys_store=KEYS_STORE, post_secret_409=post_secret_409
    )


def _make_clone_snippet_fastly_mock(
    *, active_version: int = 300, validate_ok: bool = True, validate_error: str = "bad VCL"
) -> MagicMock:
    draft_version = active_version + 1

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": active_version, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/{active_version}/clone"):
            return {"number": draft_version}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/{draft_version}/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/{draft_version}/validate"):
            return {"status": "ok"} if validate_ok else {"status": "error", "errors": [validate_error]}
        return {}

    return MagicMock(side_effect=side_effect)


def _scoring_meta_full() -> dict:
    return {
        "enabled": True,
        "scoring_service_id": SCORE_SVC,
        "scoring_keys_store_id": "KEYS",
        "scoring_config_store_id": "CFG",
        "scoring_matrix_store_id": "MTX",
        "scoring_domain": "scorer.edgecompute.app",
    }


# ── Custom Fields & Removal Helpers ──
@pytest.mark.usefixtures("_no_local_config_writes")
def test_scoring_custom_fields_and_removal_helpers():
    cfg = {"log_fields": {"custom_fields": [{"name": "user_id", "collection_stage": "edge", "enabled": True}]}}
    sso._add_scoring_custom_fields(cfg)
    sso._add_scoring_custom_fields(cfg)
    names = [cf["name"] for cf in cfg["log_fields"]["custom_fields"]]
    assert "user_id" in names and set(_SCORING_FIELD_NAMES) <= set(names)
    assert len(names) == len(_SCORING_FIELD_NAMES) + 1

    sso._remove_scoring_custom_fields(cfg)
    assert [cf["name"] for cf in cfg["log_fields"]["custom_fields"]] == ["user_id"]
    empty_cfg: dict = {}
    sso._remove_scoring_custom_fields(empty_cfg)
    assert empty_cfg == {}

    # Backend 404 vs 500
    with patch.object(sso, "fastly", MagicMock(side_effect=RuntimeError("404 Not Found"))):
        sso._remove_scoring_backend(LOG_SVC, 200, TOKEN)
    with (
        patch.object(sso, "fastly", MagicMock(side_effect=RuntimeError("500 internal"))),
        pytest.raises(RuntimeError, match="500 internal"),
    ):
        sso._remove_scoring_backend(LOG_SVC, 200, TOKEN)

    # Snippets skips absent and tolerates 404
    for del_err in (None, RuntimeError("404 Not Found")):

        def _snip_se(method, path, *_a, _err=del_err, **_kw):
            if method == "GET" and path.endswith("/snippet"):
                return [{"name": SCORING_RECV_NAME}]
            if method == "DELETE" and _err:
                raise _err
            return {}

        fm = MagicMock(side_effect=_snip_se)
        with patch.object(sso, "fastly", fm), patch("backend.core.fastly.service.fastly", fm):
            sso._remove_scoring_snippets(LOG_SVC, 200, TOKEN)
        deletes = [c for c in fm.call_args_list if c.args[0] == "DELETE"]
        assert len(deletes) == 1 and SCORING_RECV_NAME.replace(" ", "%20") in deletes[0].args[1]


# ── Enable / Disable / Teardown / Matrix / Wasm ──
@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_stages_identity_and_overrides():
    a = sso.scorer_vcl_fingerprint(LOG_SVC)
    assert a == sso.scorer_vcl_fingerprint(LOG_SVC) and sso.scorer_vcl_fingerprint("SomeOtherSvc") != a

    with (
        patch.object(sso.svcconfig, "load_config", return_value=None),
        pytest.raises(RuntimeError, match="No config found"),
    ):
        sso.enable_scoring("UnknownSvc", TOKEN)

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
        patch.object(sso, "_deploy_wasm_package", return_value={"sha": "deadbeef_pkg_sha", "version": 7}) as wasm_mock,
        patch.object(sso, "_write_matrix_to_kv") as kv_mock,
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        result = sso.enable_scoring(LOG_SVC, TOKEN)

    assert (result["scoring_service_id"], result["logging_service_active_version"]) == (SCORE_SVC, 101)
    wasm_mock.assert_called_once_with(
        SCORE_SVC, "MATRIX_STORE", TOKEN, status_cb=None, prior_package_sha="", prior_files_hash=""
    )
    kv_mock.assert_called_once_with("MATRIX_STORE", LOG_SVC, TOKEN, status_cb=None)
    reconcile_mock.assert_called_once_with(LOG_SVC, TOKEN, status_cb=None)
    final_cfg = save_mock.call_args_list[-1].args[1]
    assert final_cfg["scoring"]["enabled"] is True
    assert final_cfg["scoring"]["deployed_package_sha"] == "deadbeef_pkg_sha"
    assert final_cfg["scoring"]["deployed_vcl_sha"] == a == sso.shipped_scorer_identity(LOG_SVC)["vcl_sha"]
    assert final_cfg["scoring"]["exclude_url_regex"] == DEFAULT_ASSET_EXT_REGEX

    # Re-enable preserves operator overrides & reuses active version when reconciler reports no change
    cfg_reenable = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        "scoring": {
            **_scoring_meta_full(),
            "request_secret": "sek",
            "deployed_package_sha": "PKG",
            "exclude_url_regex": r"^/(healthz|metrics)$",
            "enforce_status_code": 403,
        },
    }
    shared_v8 = MagicMock(
        side_effect=lambda m, p, *_a, **_k: (
            [{"number": 8, "active": True}] if (m, p) == ("GET", f"/service/{LOG_SVC}/version") else {}
        )
    )
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg_reenable),
        patch.object(sso.svcconfig, "_save_config_unlocked") as save_re,
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(
            sso, "_deploy_wasm_package", return_value={"version": 2, "sha": "PKG", "files_hash": "F", "skipped": True}
        ),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch.object(sso, "fastly", shared_v8),
        patch("backend.core.fastly.service.fastly", shared_v8),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id=LOG_SVC, activated_version=None),
        ),
    ):
        res_re = sso.enable_scoring(LOG_SVC, TOKEN)
    saved_re = save_re.call_args_list[-1].args[1]
    assert res_re["logging_service_active_version"] == 8
    assert saved_re["scoring"]["exclude_url_regex"] == r"^/(healthz|metrics)$"
    assert saved_re["scoring"]["enforce_status_code"] == 403


@pytest.mark.usefixtures("_no_local_config_writes")
def test_enable_scoring_secret_healing_status_cb_and_rollback():
    for post_409, exp_method, exp_path in (
        (False, "POST", f"/resources/stores/config/{KEYS_STORE}/item"),
        (True, "PATCH", f"/resources/stores/config/{KEYS_STORE}/item/request_secret"),
    ):
        cfg = {
            "service_id": LOG_SVC,
            "log_fields": {"custom_fields": []},
            "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        }
        fm = _happy_path_fastly_mock_with_post_secret(post_secret_409=post_409)
        cb = MagicMock()
        with (
            patch.object(sso.svcconfig, "load_config", return_value=cfg),
            patch.object(sso.svcconfig, "_save_config_unlocked"),
            patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta(with_secret=False)),
            patch.object(sso, "_deploy_wasm_package"),
            patch.object(sso, "_write_matrix_to_kv"),
            patch.object(sso, "fastly", fm),
            patch("backend.core.fastly.service.fastly", fm),
            patch("backend.provision.declarative.fastly_integration.fastly", fm),
            patch("backend.core.fastly.client.fastly", fm),
        ):
            sso.enable_scoring(LOG_SVC, TOKEN, status_cb=cb)
        matched = [c for c in fm.call_args_list if c.args[0] == exp_method and c.args[1] == exp_path]
        assert len(matched) == 1
        joined = " ".join(c.args[0] for c in cb.call_args_list).lower()
        for needle in ("enabling", "cloning", "snippets", "log format", "validating", "activating", "done"):
            assert needle in joined

    # Missing keys_store_id aborts
    meta_no_store = _scoring_meta(with_secret=False)
    meta_no_store.pop("scoring_keys_store_id")
    with (
        patch.object(
            sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "log_fields": {"custom_fields": []}}
        ),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "ensure_scoring_service", return_value=meta_no_store),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        pytest.raises(RuntimeError, match="scoring_keys_store_id"),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN)

    # Rollback on validation failure
    save_calls = []
    fm_rb = _happy_path_fastly_mock()
    with (
        patch.object(
            sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "log_fields": {"custom_fields": []}}
        ),
        patch.object(
            sso.svcconfig, "_save_config_unlocked", side_effect=lambda _s, c, **_k: save_calls.append(dict(c))
        ),
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fm_rb),
        patch("backend.core.fastly.service.fastly", fm_rb),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            side_effect=RuntimeError("Validation failed"),
        ),
        pytest.raises(RuntimeError, match="Validation failed"),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN)
    assert "scoring" not in save_calls[-1]


@pytest.mark.usefixtures("_no_local_config_writes")
def test_disable_and_teardown_scoring():
    # No-op when not enabled
    fm_noop = MagicMock()
    with (
        patch.object(
            sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "log_fields": {"custom_fields": []}}
        ),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fm_noop),
        patch.object(sso, "delete_scoring_service") as del_noop,
    ):
        sso.disable_scoring(LOG_SVC, TOKEN)
    fm_noop.assert_not_called()
    del_noop.assert_not_called()

    # Full teardown when enabled
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

    def _disable_se(method, path, *_a, **_k):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 200, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/200/clone"):
            return {"number": 201}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/snippet"):
            return [{"name": SCORING_RECV_NAME}, {"name": SCORING_FETCH_NAME}, {"name": SCORING_DELIVER_NAME}]
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/logging/s3"):
            return [{"name": "Fastly Object Storage Logs"}]
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/201/validate"):
            return {"status": "ok"}
        return {}

    fm_dis = MagicMock(side_effect=_disable_se)
    saved = []
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=lambda _s, c, **_k: saved.append(dict(c))),
        patch.object(sso, "fastly", fm_dis),
        patch("backend.core.fastly.service.fastly", fm_dis),
        patch.object(sso, "delete_scoring_service", return_value=[]) as del_mock,
        patch.object(sso, "_delete_scoring_matrix_from_fos") as fos_mock,
    ):
        sso.disable_scoring(LOG_SVC, TOKEN)
    del_mock.assert_called_once()
    fos_mock.assert_called_once_with(LOG_SVC)
    assert "scoring" not in saved[-1] and [cf["name"] for cf in saved[-1]["log_fields"]["custom_fields"]] == ["user_id"]

    # Teardown resources strips VCL then deletes, and still deletes if version probe 503s
    order = []

    def _td_se(method, path, *_a, **_k):
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

    fm_td = MagicMock(side_effect=_td_se)
    with (
        patch.object(sso, "fastly", fm_td),
        patch("backend.core.fastly.service.fastly", fm_td),
        patch.object(sso, "delete_scoring_service", side_effect=lambda *_a, **_k: order.append("delete") or []),
    ):
        assert sso.teardown_scoring_resources(LOG_SVC, _scoring_meta_full(), TOKEN) == []
    assert order == ["activate", "delete"]

    fm_503 = MagicMock(side_effect=RuntimeError("Fastly 503 on version probe"))
    with (
        patch.object(sso, "fastly", fm_503),
        patch("backend.core.fastly.service.fastly", fm_503),
        patch.object(sso, "delete_scoring_service", return_value=[]) as del_503,
    ):
        assert sso.teardown_scoring_resources(LOG_SVC, _scoring_meta_full(), TOKEN) == []
    del_503.assert_called_once()


@pytest.mark.usefixtures("_no_local_config_writes")
def test_tenant_matrix_and_wasm_deploy(tmp_path, monkeypatch):
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)
    fake_root.write_text('{"vocab_size": 99, "version": "leaked"}')

    # Nothing trained / empty vocab -> None & no KV write
    raw_calls = []
    for fos_val in (None, {"vocab_size": 0}):
        with (
            patch("backend.state_sync.fetch_matrix_from_fos", return_value=fos_val),
            patch.object(sso, "fastly_raw", side_effect=lambda *a, **_k: raw_calls.append(a)),
        ):
            assert sso._resolve_tenant_matrix_for_deploy(LOG_SVC) is None
            sso._write_matrix_to_kv("MATRIX_STORE", LOG_SVC, TOKEN)
    assert raw_calls == []

    # Fetches from FOS when local missing
    with patch("backend.state_sync.fetch_matrix_from_fos", return_value={"vocab_size": 11, "version": "fos-1"}) as mf:
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)
    assert resolved == sso._tenant_matrix_path(LOG_SVC) and mf.call_count == 1

    # Local wins without FOS round-trip & writes KV
    resolved.write_text(json.dumps({"vocab_size": 5, "version": "tenant-only"}))
    with patch("backend.state_sync.fetch_matrix_from_fos") as mf2:
        assert sso._resolve_tenant_matrix_for_deploy(LOG_SVC) == resolved
    mf2.assert_not_called()

    with patch.object(sso, "fastly_raw", side_effect=lambda m, p, d, **_k: raw_calls.append((m, p, d))):
        sso._write_matrix_to_kv("MATRIX_STORE", LOG_SVC, TOKEN)
    assert raw_calls == [
        (
            "PUT",
            "/resources/stores/kv/MATRIX_STORE/keys/matrix",
            serialize_kv({"vocab_size": 5, "version": "tenant-only"}),
        )
    ]

    # Missing wasm package raises
    monkeypatch.setattr(sso, "_SCORER_PACKAGE", tmp_path / "missing.tar.gz")
    with (
        patch.object(sso, "fastly"),
        patch.object(sso, "fastly_raw"),
        pytest.raises(RuntimeError, match="prebuilt scorer package not found"),
    ):
        sso._deploy_wasm_package(SCORE_SVC, "MATRIX_STORE", TOKEN)

    # Deploy wasm upload vs skip when unchanged
    pkg = tmp_path / "session-scorer.tar.gz"
    pkg.write_bytes(b"FAKEWASMPKG")
    sha = hashlib.sha256(b"FAKEWASMPKG").hexdigest()
    monkeypatch.setattr(sso, "_SCORER_PACKAGE", pkg)

    def _wasm_se(method, path, *_a, **_k):
        if (method, path) == ("GET", f"/service/{SCORE_SVC}/version"):
            return [{"number": 9, "active": True}]
        if (method, path) == ("PUT", f"/service/{SCORE_SVC}/version/9/clone"):
            return {"number": 10}
        if (method, path) == ("GET", f"/service/{SCORE_SVC}/version/9/package"):
            return {"metadata": {"files_hash": "EDGEHASH"}}
        return {}

    fm_w = MagicMock(side_effect=_wasm_se)
    raw_w = MagicMock(return_value={})
    with (
        patch.object(sso, "fastly", fm_w),
        patch("backend.core.fastly.service.fastly", fm_w),
        patch.object(sso, "fastly_raw", raw_w),
    ):
        res_up = sso._deploy_wasm_package(SCORE_SVC, "MATRIX_STORE", TOKEN)
        assert (res_up["sha"], res_up["version"], res_up["skipped"]) == (sha, 10, False)
        raw_w.reset_mock()
        res_skip = sso._deploy_wasm_package(
            SCORE_SVC, "MATRIX_STORE", TOKEN, prior_package_sha=sha, prior_files_hash="EDGEHASH"
        )
        assert (res_skip["sha"], res_skip["version"], res_skip["skipped"]) == (sha, 9, True)
        raw_w.assert_not_called()


# ── Update Regex & Enforce Status Code ──
@pytest.mark.usefixtures("_no_local_config_writes")
@pytest.mark.parametrize(
    ("fn", "arg_name", "custom_val", "reset_val", "res_key", "default_val"),
    [
        (
            sso.update_recv_exclusion_regex,
            "new_regex",
            r"^/(healthz|metrics)$",
            "   ",
            "effective_regex",
            DEFAULT_ASSET_EXT_REGEX,
        ),
        (sso.update_enforce_status_code, "new_status_code", 418, None, "effective_status_code", 429),
    ],
)
def test_update_scoring_settings_matrix(fn, arg_name, custom_val, reset_val, res_key, default_val):
    # No config -> raises
    with (
        patch.object(sso.svcconfig, "load_config", return_value=None),
        pytest.raises(RuntimeError, match="No config found"),
    ):
        fn(LOG_SVC, TOKEN, **{arg_name: custom_val})

    # Not enabled -> raises
    with (
        patch.object(sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "scoring": {"enabled": False}}),
        pytest.raises(RuntimeError, match="not enabled"),
    ):
        fn(LOG_SVC, TOKEN, **{arg_name: custom_val})

    # Happy path without and with request_secret
    fm = _make_clone_snippet_fastly_mock(active_version=300)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "scoring": {"enabled": True}}),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=save_mock),
        patch.object(sso, "fastly", fm),
        patch("backend.core.fastly.service.fastly", fm),
    ):
        res = fn(LOG_SVC, TOKEN, **{arg_name: custom_val})
    assert (res["logging_service_active_version"], res["is_default"], res[res_key]) == (301, False, custom_val)

    # Reset to default
    with (
        patch.object(
            sso.svcconfig,
            "load_config",
            return_value={"service_id": LOG_SVC, "scoring": {"enabled": True, "request_secret": "s" * 64}},
        ),
        patch.object(sso.svcconfig, "_save_config_unlocked", side_effect=save_mock),
        patch.object(sso, "fastly", fm),
        patch("backend.core.fastly.service.fastly", fm),
    ):
        res_def = fn(LOG_SVC, TOKEN, **{arg_name: reset_val})
    assert (res_def["is_default"], res_def[res_key]) == (True, default_val)

    # Validate failure rolls back to 300
    fm_fail = _make_clone_snippet_fastly_mock(active_version=300, validate_ok=False)
    with (
        patch.object(sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "scoring": {"enabled": True}}),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fm_fail),
        patch("backend.core.fastly.service.fastly", fm_fail),
        pytest.raises(RuntimeError, match="Validation failed"),
    ):
        fn(LOG_SVC, TOKEN, **{arg_name: custom_val})
    calls = [(c.args[0], c.args[1]) for c in fm_fail.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/300/activate") in calls and (
        "PUT",
        f"/service/{LOG_SVC}/version/301/activate",
    ) not in calls


@pytest.mark.usefixtures("_no_local_config_writes")
def test_update_recv_exclusion_regex_raises_when_no_active_version():
    fm = MagicMock(return_value=[])
    with (
        patch.object(sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "scoring": {"enabled": True}}),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fm),
        patch("backend.core.fastly.service.fastly", fm),
        pytest.raises(RuntimeError, match="no active version"),
    ):
        sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/$")


# ── Call Order Tests ──
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
        elif method == "POST" and path.endswith("/logging/s3"):
            out.append("post-endpoint")
        elif method == "DELETE":
            out.append("delete")
    return out


class _Fastly:
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


def _run_disable(fake: _Fastly, *, delete_mock=None):
    cfg = {
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
    delete_mock = delete_mock if delete_mock is not None else MagicMock(return_value=[])
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fake),
        patch("backend.core.fastly.service.fastly", fake),
        patch.object(sso, "delete_scoring_service", delete_mock),
        patch.object(sso, "_delete_scoring_matrix_from_fos"),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch("backend.state_sync.export_admin_state"),
    ):
        return sso.disable_scoring(LOG_SVC, TOKEN)


def test_disable_call_order_and_rollback(caplog):
    fake_ok = _Fastly()
    _run_disable(fake_ok)
    order = _order(fake_ok.calls)
    assert order[:2] == ["clone", "comment"] and order[-2:] == ["validate", "activate:201"]
    assert fake_ok.kwargs_for(lambda m, p: p.endswith("/version/201/activate")) == [{"token": TOKEN}]

    fake_val_fail = _Fastly(validate_ok=False)
    del_mock = MagicMock()
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_disable(fake_val_fail, delete_mock=del_mock)
    assert _order(fake_val_fail.calls)[-2:] == ["validate", "activate:200"]
    del_mock.assert_not_called()

    fake_edit_fail = _Fastly(
        fail_on=lambda m, p: m == "PUT" and p.endswith("/logging/s3/Fastly%20Object%20Storage%20Logs")
    )
    with pytest.raises(RuntimeError, match="edit rejected"):
        _run_disable(fake_edit_fail)
    assert "validate" not in _order(fake_edit_fail.calls) and _order(fake_edit_fail.calls)[-1] == "activate:200"

    fake_rb_fail = _Fastly(validate_ok=False, rollback_raises=True)
    with (
        caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"),
        pytest.raises(RuntimeError, match="Validation failed"),
    ):
        _run_disable(fake_rb_fail)
    assert any("rollback activation of version 200 failed" in r.getMessage() for r in caplog.records)


def test_teardown_call_order():
    meta = {"scoring_service_id": SCORE_SVC, "scoring_keys_store_id": "K", "scoring_config_store_id": "C"}
    fake_ok = _Fastly()
    del_ok = MagicMock(return_value=[])
    with (
        patch.object(sso, "fastly", fake_ok),
        patch("backend.core.fastly.service.fastly", fake_ok),
        patch.object(sso, "delete_scoring_service", del_ok),
    ):
        assert sso.teardown_scoring_resources(LOG_SVC, meta, TOKEN) == []
    assert _order(fake_ok.calls)[:2] == ["clone", "comment"] and _order(fake_ok.calls)[-2:] == [
        "validate",
        "activate:201",
    ]
    del_ok.assert_called_once()

    fake_fail = _Fastly(validate_ok=False)
    del_fail = MagicMock(return_value=[])
    msgs: list[str] = []
    with (
        patch.object(sso, "fastly", fake_fail),
        patch("backend.core.fastly.service.fastly", fake_fail),
        patch.object(sso, "delete_scoring_service", del_fail),
        pytest.raises(RuntimeError, match="Aborting scoring teardown"),
    ):
        sso.teardown_scoring_resources(LOG_SVC, meta, TOKEN, status_cb=msgs.append)
    assert not any(o.startswith("activate:") for o in _order(fake_fail.calls))
    del_fail.assert_not_called()
    assert any(m.startswith("❌ Could not strip scoring VCL: Validation failed") for m in msgs)


@pytest.mark.parametrize(
    ("fn", "kwargs"),
    [
        (sso.update_recv_exclusion_regex, {"new_regex": "^/healthz$"}),
        (sso.update_enforce_status_code, {"new_status_code": 403}),
    ],
)
def test_swap_call_order_and_failures(fn, kwargs, capsys, caplog):
    def _run_swap(fake):
        with (
            patch.object(
                sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "scoring": {"enabled": True}}
            ),
            patch.object(sso.svcconfig, "_save_config_unlocked"),
            patch.object(sso, "fastly", fake),
            patch("backend.core.fastly.service.fastly", fake),
        ):
            return fn(LOG_SVC, TOKEN, **kwargs)

    fake_ok = _Fastly(active=300, clone_number=301)
    _run_swap(fake_ok)
    assert _order(fake_ok.calls)[:2] == ["clone", "comment"] and _order(fake_ok.calls)[-2:] == [
        "validate",
        "activate:301",
    ]
    assert fake_ok.kwargs_for(lambda m, p: p.endswith("/version/301/activate")) == [{"token": TOKEN}]

    fake_fail = _Fastly(active=300, clone_number=301, validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_swap(fake_fail)
    assert (
        _order(fake_fail.calls)[-2:] == ["validate", "activate:300"]
        and "failed: Validation failed" in capsys.readouterr().err
    )

    fake_rb = _Fastly(active=300, clone_number=301, validate_ok=False, rollback_raises=True)
    with (
        caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"),
        pytest.raises(RuntimeError, match="Validation failed"),
    ):
        _run_swap(fake_rb)
    assert any("rollback activation of version 300 failed" in r.getMessage() for r in caplog.records)


def test_swap_clone_failure_reactivates_previous_version():
    fake = _Fastly(active=300, clone_number=301, fail_on=lambda m, p: p.endswith("/clone"))
    with (
        patch.object(sso.svcconfig, "load_config", return_value={"service_id": LOG_SVC, "scoring": {"enabled": True}}),
        patch.object(sso.svcconfig, "_save_config_unlocked"),
        patch.object(sso, "fastly", fake),
        patch("backend.core.fastly.service.fastly", fake),
        pytest.raises(RuntimeError, match="edit rejected"),
    ):
        sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/x$")
    assert _order(fake.calls)[-1] == "activate:300"


def test_wasm_deploy_call_order(tmp_path):
    pkg = tmp_path / "session-scorer.tar.gz"
    pkg.write_bytes(b"wasm-bytes")

    def _run_w(fake, *, matrix_store="MATRIX_STORE", raw=None):
        raw = raw if raw is not None else MagicMock(return_value={})
        with (
            patch.object(sso, "_SCORER_PACKAGE", pkg),
            patch.object(sso, "fastly", fake),
            patch("backend.core.fastly.service.fastly", fake),
            patch.object(sso, "fastly_raw", raw),
        ):
            return sso._deploy_wasm_package(SCORE_SVC, matrix_store, TOKEN)

    order_events: list[str] = []
    fake = _Fastly(active=5, clone_number=6)
    _run_w(fake, raw=MagicMock(side_effect=lambda *_a, **_k: order_events.append("package") or {}))
    calls = fake.calls
    assert (
        calls.index(("PUT", f"/service/{SCORE_SVC}/version/5/clone"))
        < calls.index(("POST", f"/service/{SCORE_SVC}/version/6/resource"))
        < calls.index(("PUT", f"/service/{SCORE_SVC}/version/6/activate"))
    )
    assert order_events == ["package"]
    assert not any(p.endswith("/validate") for _, p in calls)
    assert fake.kwargs_for(lambda m, p: p.endswith("/version/6/activate")) == [{"token": TOKEN, "expect_empty": True}]

    # Clones highest version
    class _Versions(_Fastly):
        def __call__(self, method, path, body=None, **kwargs):
            if method == "GET" and path.endswith("/version"):
                self.calls.append((method, path))
                self.kwargs.append(kwargs)
                return [{"number": 3, "active": True}, {"number": 9, "active": False}]
            return super().__call__(method, path, body, **kwargs)

    fake_v = _Versions(active=3, clone_number=10)
    _run_w(fake_v)
    assert ("PUT", f"/service/{SCORE_SVC}/version/9/clone") in fake_v.calls

    # Upload failure leaves draft unactivated
    fake_up_fail = _Fastly(active=5, clone_number=6)
    with pytest.raises(RuntimeError, match="upload failed"):
        _run_w(fake_up_fail, raw=MagicMock(side_effect=RuntimeError("upload failed")))
    assert not any(p.endswith("/activate") for _, p in fake_up_fail.calls)

    # Without matrix store skips resource link
    fake_no_mtx = _Fastly(active=5, clone_number=6)
    _run_w(fake_no_mtx, matrix_store="")
    assert not any(p.endswith("/resource") for _, p in fake_no_mtx.calls)


def test_remove_endpoint_call_order(caplog):
    def _run_rm(fake):
        with (
            patch("backend.provision.fastly_api.get_active_version", return_value=10),
            patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
            patch("backend.provision.fastly_api.fastly", side_effect=fake),
        ):
            return fastly_api.remove_logging_endpoint(SID, "MyEndpoint", "tok")

    fake_ok = _Fastly(active=10, clone_number=11)
    _run_rm(fake_ok)
    assert _order(fake_ok.calls) == ["clone", "delete", "validate", "comment", "activate:11"]
    del_kw = [kw for (m, _), kw in zip(fake_ok.calls, fake_ok.kwargs) if m == "DELETE"]
    assert del_kw and all(kw.get("expect_empty") is True for kw in del_kw)
    assert fake_ok.kwargs_for(lambda m, p: p.endswith("/version/11/activate")) == [{"token": "tok"}]

    fake_val = _Fastly(active=10, clone_number=11, validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_rm(fake_val)
    assert _order(fake_val.calls) == ["clone", "delete", "validate", "activate:10"]

    fake_edit = _Fastly(active=10, clone_number=11, fail_on=lambda m, p: m == "DELETE" and "/logging/s3/" in p)
    with pytest.raises(RuntimeError, match="edit rejected"):
        _run_rm(fake_edit)
    assert _order(fake_edit.calls) == ["clone", "delete", "activate:10"]

    fake_rb = _Fastly(active=10, clone_number=11, validate_ok=False, rollback_raises=True)
    with (
        caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"),
        pytest.raises(RuntimeError, match="Validation failed"),
    ):
        _run_rm(fake_rb)
    assert any("rollback activation of version 10 failed" in r.getMessage() for r in caplog.records)


# ── CDN Service, Logging Endpoint & Config Merge ──
def test_ensure_cdn_service_and_redeploy_cdn_vcl():
    with (
        patch("backend.provision.declarative.reconciler.reconcile_cdn_service_state") as mock_rec,
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=True),
    ):
        mock_rec.side_effect = lambda **_kw: (
            svcconfig.mutate_config(_MERGE_SID, lambda c: c.setdefault("fos_proxy", {}).update(service_id="cdn123")),
            SimpleNamespace(activated_version=3),
        )[1]
        out = fastly_api.ensure_cdn_service({"service_id": _MERGE_SID, "fos_bucket": "b"}, "AK", "SK", "tok")
        saved = _persisted()
        assert (saved["fos_bucket"], saved["fos_access_key_id"], saved["fos_secret_access_key"], out["id"]) == (
            "b",
            "AK",
            "SK",
            "cdn123",
        )
        salt = saved["rum"]["cid_salt"]
        assert salt

        svcconfig.save_config(
            _MERGE_SID, {"service_id": _MERGE_SID, "keep_me": 1, "rum": {"enabled": True, "cid_salt": salt}}
        )
        mock_rec.side_effect = None
        mock_rec.return_value = SimpleNamespace(activated_version=3)
        fastly_api.ensure_cdn_service(
            {"service_id": _MERGE_SID, "fos_bucket": "b", "rum": {"enabled": False}}, "AK", "SK", "tok"
        )
        saved2 = _persisted()
        assert (saved2["keep_me"], saved2["rum"]["enabled"], saved2["rum"]["cid_salt"]) == (1, False, salt)

        svcconfig.save_config(
            _MERGE_SID,
            {"service_id": _MERGE_SID, "cdn_service_id": "cdn9", "fos_proxy": {"service_id": "cdn9"}, "other": [1, 2]},
        )
        mock_rec.return_value = SimpleNamespace(activated_version=4)
        assert fastly_api.redeploy_cdn_vcl("cdn9", "tok", rate_limiting=False) == 4
        assert _persisted()["fos_proxy"] == {"service_id": "cdn9", "rate_limiting_enabled": False}

        svcconfig.save_config(_MERGE_SID, {"service_id": _MERGE_SID, "cdn_service_id": "cdn9"})
        fastly_api.redeploy_cdn_vcl("cdn9", "tok", rate_limiting=True)
        assert svcconfig.load_config(_MERGE_SID)["fos_proxy"] == {"rate_limiting_enabled": True}


def test_update_logging_endpoint_config_merge():
    with (
        patch("backend.provision.fastly_api.account_has_rate_limiting", return_value=None) as rl,
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=ReconciliationResult(service_id=_MERGE_SID, activated_version=11, changes_applied=["fmt"]),
        ) as rec,
    ):
        # Missing config raises before any Fastly call
        with pytest.raises(RuntimeError, match="not found|empty"):
            list(fastly_api.update_logging_endpoint({"logging_service_id": _MERGE_SID, "log_period": 60}, "tok"))
        rl.assert_not_called()
        rec.assert_not_called()

        svcconfig.save_config(_MERGE_SID, {"service_id": _MERGE_SID, "keep_me": 1, "provisioning": {"keep_prov": "x"}})
        salt = svcconfig.load_config(_MERGE_SID)["rum"]["cid_salt"]
        seen_locks: list[bool] = []

        def _probe(_tok, _sid):
            assert not _lock_held()
            svcconfig.mutate_config(_MERGE_SID, lambda c: c.update(concurrent_key="kept"))
            return True

        rl.side_effect = _probe
        rec.side_effect = lambda *_a, **_k: (
            seen_locks.append(_lock_held()),
            ReconciliationResult(service_id=_MERGE_SID, activated_version=11, changes_applied=["fmt"]),
        )[1]

        events = list(
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
        assert saved["provisioning"] == {
            "keep_prov": "x",
            "sample_rate": 50,
            "edge_only": True,
            "endpoint_name": "EP",
            "rate_limiting": True,
        }
        assert (
            saved["log_period"],
            saved["fos_path"],
            saved["keep_me"],
            saved["concurrent_key"],
            saved["rum"]["cid_salt"],
        ) == (120, "/raw/%H/", 1, "kept", salt)
        assert events[-1]["type"] == "done" and seen_locks == [False]


def test_write_service_config_merges_and_preserves_state():
    def _state(**overrides) -> dict:
        return {
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
            **overrides,
        }

    orchestrator.write_service_config(_state())
    saved1 = _persisted()
    salt = saved1["rum"]["cid_salt"]
    created_at = saved1["created_at"]
    assert (
        saved1["service_id"],
        saved1["name"],
        saved1["fos_bucket"],
        saved1["provisioning"]["cron_sync"]["interval_seconds"],
    ) == (_MERGE_SID, "Test Service", "test-bucket", 60)
    assert salt and created_at

    svcconfig.save_config(
        _MERGE_SID,
        {
            "service_id": _MERGE_SID,
            "created_at": created_at,
            "scoring": {"enabled": True, "scoring_service_id": "S"},
            "cmcd": {"enabled": True},
            "ngwaf_workspace_id": "ws",
            "rum": {"faro_version": "1.2.3", "cid_salt": salt},
            "fos_proxy": {"service_id": "cdn-proxy"},
            "status": {"state": "ok"},
            "last_activated_version": 7,
            "rum_vcl_sha": "sha",
            "log_fields": {"groups": ["A"], "custom_fields": [{"name": "mine", "duckdb_type": "VARCHAR"}]},
        },
    )
    orchestrator.write_service_config(_state())
    saved2 = _persisted()
    assert saved2["scoring"] == {"enabled": True, "scoring_service_id": "S"}
    assert saved2["cmcd"] == {"enabled": True}
    assert (saved2["ngwaf_workspace_id"], saved2["rum"]["faro_version"], saved2["rum"]["cid_salt"]) == (
        "ws",
        "1.2.3",
        salt,
    )
    assert (
        saved2["created_at"],
        saved2["fos_proxy"],
        saved2["status"],
        saved2["last_activated_version"],
        saved2["rum_vcl_sha"],
    ) == (
        created_at,
        {"service_id": "cdn-proxy"},
        {"state": "ok"},
        7,
        "sha",
    )
    assert [cf["name"] for cf in saved2["log_fields"]["custom_fields"]] == ["mine"]

    orchestrator.write_service_config(_state(rum={"enabled": True, "cid_salt": "attacker-chosen"}))
    saved3 = _persisted()
    assert (saved3["rum"]["cid_salt"], saved3["rum"]["enabled"]) == (salt, True)


# ── CMCD & System Custom Fields Reconciliation ──
def test_cmcd_and_system_custom_fields_reconciliation():
    assert system_feature_flags({}) == (False, False)
    assert system_feature_flags({"cmcd": None, "scoring": None}) == (False, False)
    assert system_feature_flags({"cmcd": {"enabled": True}}) == (False, True)
    assert system_feature_flags({"scoring": {"enabled": True}}) == (True, False)
    assert system_feature_flags({"scoring": {"enabled": True}, "cmcd": {"enabled": True}}) == (True, True)

    def _names(fields: list[dict]) -> set[str]:
        return {cf["name"] for cf in fields}

    out_cmcd = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=False, cmcd_enabled=True)
    assert _CMCD_FIELD_NAMES <= _names(out_cmcd) and "my_custom" in _names(out_cmcd)

    out_off = reconcile_system_custom_fields(
        [_USER_FIELD, {"name": "cmcd_sid", "duckdb_type": "VARCHAR"}], scoring_enabled=False, cmcd_enabled=False
    )
    assert _names(out_off) == {"my_custom"}

    out_stale = reconcile_system_custom_fields(
        [{"name": "cmcd_sid", "duckdb_type": "WRONG", "enabled": False}], scoring_enabled=False, cmcd_enabled=True
    )
    sids = [cf for cf in out_stale if cf["name"] == "cmcd_sid"]
    assert len(sids) == 1 and (sids[0]["duckdb_type"], sids[0]["enabled"]) == ("VARCHAR", True)

    out_both = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=True, cmcd_enabled=True)
    assert (_CMCD_FIELD_NAMES | _SCORING_FIELD_NAMES | {"my_custom"}) <= _names(out_both)
    assert len(reconcile_system_custom_fields(out_both, scoring_enabled=True, cmcd_enabled=True)) == len(out_both)

    out_score_only = reconcile_system_custom_fields([_USER_FIELD], scoring_enabled=True, cmcd_enabled=False)
    assert _SCORING_FIELD_NAMES <= _names(out_score_only) and not (_CMCD_FIELD_NAMES & _names(out_score_only))

    first = reconcile_system_custom_fields(None, scoring_enabled=False, cmcd_enabled=True)
    assert _CMCD_FIELD_NAMES <= _names(first)
    first[0]["label"] = "MUTATED"
    assert reconcile_system_custom_fields(None, scoring_enabled=False, cmcd_enabled=True)[0]["label"] != "MUTATED"

    cfg1 = {"cmcd": {"enabled": True}, "log_fields": {"schema_version": 2, "custom_fields": [_USER_FIELD]}}
    reconcile_cfg_system_custom_fields(cfg1)
    assert (_CMCD_FIELD_NAMES | {"my_custom"}) <= _names(cfg1["log_fields"]["custom_fields"])

    cfg2 = {"cmcd": {"enabled": True}}
    reconcile_cfg_system_custom_fields(cfg2)
    assert cfg2["log_fields"]["schema_version"] == 2 and _CMCD_FIELD_NAMES <= _names(
        cfg2["log_fields"]["custom_fields"]
    )

    cfg3 = {
        "log_fields": {
            "schema_version": 2,
            "custom_fields": [_USER_FIELD, {"name": "cmcd_br", "duckdb_type": "INTEGER"}],
        }
    }
    reconcile_cfg_system_custom_fields(cfg3)
    assert _names(cfg3["log_fields"]["custom_fields"]) == {"my_custom"}


# ── Session Scoring Config Merge & Lock Hygiene ──
_META_MERGE = {
    "scoring_service_id": "ScoringSvcZ",
    "scoring_service_name": f"Session Scoring Service for {_MERGE_SID}",
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
        c.setdefault("log_fields", {}).setdefault("custom_fields", []).append(dict(_USER_FIELD))

    svcconfig.mutate_config(_MERGE_SID, _apply)


@contextmanager
def _fake_draft(*_a, **_kw):
    yield 201


def test_session_scoring_config_merge_and_lock_release():
    def _init_base():
        svcconfig.save_config(
            _MERGE_SID,
            {
                "service_id": _MERGE_SID,
                "keep_me": 1,
                "log_fields": {"groups": ["A"], "custom_fields": []},
                "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
            },
        )
        return svcconfig.load_config(_MERGE_SID)

    base = _init_base()
    salt = base["rum"]["cid_salt"]
    held: list[bool] = []

    with (
        patch.object(
            sso,
            "ensure_scoring_service",
            side_effect=lambda *_a, **_k: (held.append(_lock_held()), dict(_META_MERGE))[1],
        ),
        patch.object(
            sso,
            "_deploy_wasm_package",
            side_effect=lambda *_a, **_k: (
                held.append(_lock_held()),
                _concurrent_write(concurrent_key="kept"),
                {"sha": "pkg", "version": 3, "files_hash": "fh"},
            )[2],
        ),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", MagicMock(return_value={})),
        patch.object(sso, "get_active_version", return_value=10),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            side_effect=lambda *_a, **_k: (held.append(_lock_held()), MagicMock(error=None, activated_version=11))[1],
        ),
    ):
        sso.enable_scoring(_MERGE_SID, TOKEN)

    saved = _persisted()
    fnames = {cf["name"] for cf in saved["log_fields"]["custom_fields"]}
    assert held == [False, False, False]
    assert (
        saved["scoring"]["enabled"],
        saved["scoring"]["scoring_service_id"],
        saved["scoring"]["deployed_package_sha"],
        saved["concurrent_key"],
        saved["keep_me"],
        saved["rum"]["cid_salt"],
    ) == (
        True,
        "ScoringSvcZ",
        "pkg",
        "kept",
        1,
        salt,
    )
    assert (set(_SCORING_FIELD_NAMES) | {"my_custom"}) <= fnames

    # Operator regex change mid-deploy preserved
    with (
        patch.object(sso, "ensure_scoring_service", return_value=dict(_META_MERGE)),
        patch.object(
            sso,
            "_deploy_wasm_package",
            side_effect=lambda *_a, **_k: (
                svcconfig.mutate_config(
                    _MERGE_SID, lambda c: c["scoring"].update(exclude_url_regex="changed-mid-deploy")
                ),
                {"sha": "pkg", "version": 3, "files_hash": "fh"},
            )[1],
        ),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", MagicMock(return_value={})),
        patch.object(sso, "get_active_version", return_value=10),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            return_value=MagicMock(error=None, activated_version=11),
        ),
    ):
        sso.enable_scoring(_MERGE_SID, TOKEN)
    assert _persisted()["scoring"]["exclude_url_regex"] == "changed-mid-deploy"

    # Rollback removes only scoring state and keeps concurrent writes
    _init_base()
    with (
        patch.object(sso, "ensure_scoring_service", return_value=dict(_META_MERGE)),
        patch.object(sso, "_deploy_wasm_package", return_value={"sha": "pkg", "version": 3, "files_hash": "fh"}),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", MagicMock(return_value={})),
        patch.object(sso, "get_active_version", return_value=10),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch(
            "backend.provision.declarative.reconciler.reconcile_vcl_state",
            side_effect=lambda *_a, **_k: (
                _concurrent_write(concurrent_key="kept_rb"),
                (_ for _ in ()).throw(RuntimeError("reconcile exploded")),
            ),
        ),
        pytest.raises(RuntimeError, match="reconcile exploded"),
    ):
        sso.enable_scoring(_MERGE_SID, TOKEN)
    saved_rb = _persisted()
    rb_fnames = {cf["name"] for cf in saved_rb["log_fields"]["custom_fields"]}
    assert "scoring" not in saved_rb and not (set(_SCORING_FIELD_NAMES) & rb_fnames)
    assert "my_custom" in rb_fnames and saved_rb["concurrent_key"] == "kept_rb"

    # Disable keeps concurrent writes during VCL phase and compute teardown
    svcconfig.mutate_config(
        _MERGE_SID,
        lambda c: (
            c.update(
                scoring={"enabled": True, "scoring_service_id": "ScoringSvcZ", "exclude_url_regex": "operator-set"}
            ),
            sso._add_scoring_custom_fields(c),
        ),
    )
    with (
        patch.object(sso, "get_active_version", return_value=200),
        patch.object(sso, "_clone_swap_snippet", return_value=201),
    ):
        sso.update_recv_exclusion_regex(_MERGE_SID, TOKEN, new_regex="  \\.foo$ ")
        assert _persisted()["scoring"]["exclude_url_regex"] == "\\.foo$"
        sso.update_enforce_status_code(_MERGE_SID, TOKEN, new_status_code=403)
        assert _persisted()["scoring"]["enforce_status_code"] == 403

    with (
        patch.object(sso, "get_active_version", return_value=200),
        patch.object(sso, "draft_version", _fake_draft),
        patch.object(sso, "_remove_scoring_snippets", side_effect=lambda *_a, **_k: _concurrent_write(vcl_key="kept")),
        patch.object(sso, "_remove_scoring_backend"),
        patch.object(sso, "ensure_vcl_snippet"),
        patch.object(sso, "fastly", MagicMock(return_value={})),
        patch("backend.core.fastly.service.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.generate_capture_vcl", return_value={"recv": ""}),
        patch.object(
            sso, "delete_scoring_service", side_effect=lambda *_a, **_k: (_concurrent_write(compute_key="kept"), [])[1]
        ),
        patch.object(sso, "_delete_scoring_matrix_from_fos"),
        patch("backend.state_sync.export_admin_state"),
    ):
        sso.disable_scoring(_MERGE_SID, TOKEN)
    saved_dis = _persisted()
    assert "scoring" not in saved_dis and saved_dis["vcl_key"] == "kept" and saved_dis["compute_key"] == "kept"

    # Updates refuse and write nothing when scoring is disabled
    before = _persisted()
    for call in (
        lambda: sso.update_recv_exclusion_regex(_MERGE_SID, TOKEN, new_regex="x"),
        lambda: sso.update_enforce_status_code(_MERGE_SID, TOKEN, new_status_code=403),
    ):
        with pytest.raises(RuntimeError, match="not enabled"):
            call()
    assert _persisted() == before


# ── Merged from tests/provision/test_provision_cli.py ──

# ── Merged from tests/test_provision_cli.py ──
runner = CliRunner()

SUBCOMMANDS = (
    "provision",
    "teardown",
    "invite-analyst",
    "update-logs",
    "update-cdn",
    "list-groups",
    "list-fields",
)


def test_top_level_help_lists_all_subcommands():
    """``--help`` enumerates every subcommand — losing one usually
    means it was un-registered."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    for sub in SUBCOMMANDS:
        assert sub in result.output, f"subcommand {sub!r} missing from --help"


def test_no_args_prints_help_and_exits_nonzero():
    """``no_args_is_help=True`` → bare invocation prints help with
    non-zero exit, so shell-script misuse surfaces."""
    result = runner.invoke(app, [])
    assert result.exit_code != 0
    assert "provision" in result.output


def test_each_subcommand_has_working_help():
    """Each subcommand's ``--help`` panel renders — guards against
    typos in option/docstring definitions."""
    for sub in SUBCOMMANDS:
        result = runner.invoke(app, [sub, "--help"])
        assert result.exit_code == 0, f"--help for {sub!r} failed: {result.output}"


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_parses_edge_only_true_and_invokes_wizard(mock_provision):
    """``--edge-only`` arrives at wizard as ``True`` — typer's
    Optional[bool] toggle is the trickiest part of the surface."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        result = runner.invoke(app, ["provision", "--yes", "--token", "t", "--service-id", "s", "--edge-only"])
    assert result.exit_code == 0, result.output
    args = mock_wizard.call_args.args[0]
    assert args.edge_only is True
    assert args.token == "t"
    assert args.service_id == "s"
    assert args.yes is True


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_parses_no_edge_only_as_false(mock_provision):
    """``--no-edge-only`` half of the toggle arrives as ``False``."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        result = runner.invoke(app, ["provision", "--yes", "--service-id", "s", "--no-edge-only"])
    assert result.exit_code == 0, result.output
    assert mock_wizard.call_args.args[0].edge_only is False


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_edge_only_default_is_none(mock_provision):
    """When NEITHER flag is supplied, wizard sees ``None`` so its own
    default-resolution (yes→True, interactive→prompt) can run."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        runner.invoke(app, ["provision", "--yes", "--service-id", "s"])
    assert mock_wizard.call_args.args[0].edge_only is None


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_collects_repeated_enable_group_into_list(mock_provision):
    """Repeated ``--enable-group`` accumulates — collapsing would
    silently drop all but the last admin-supplied group."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        result = runner.invoke(
            app,
            [
                "provision",
                "--yes",
                "--service-id",
                "s",
                "--enable-group",
                "A",
                "--enable-group",
                "B",
                "--disable-field",
                "client.geo.region",
            ],
        )
    assert result.exit_code == 0, result.output
    args = mock_wizard.call_args.args[0]
    assert args.enable_group == ["A", "B"]
    assert args.disable_field == ["client.geo.region"]


@patch("backend.provision.orchestrator.provision", return_value=[])
def test_provision_applies_documented_default_values(mock_provision):
    """Defaults match the docker-compose-relied-upon contract:
    commit_interval_mins=5, log_retention_days=30, disable_*=False."""
    with patch("backend.provision.cli.wizard") as mock_wizard:
        runner.invoke(app, ["provision", "--yes", "--service-id", "s"])
    args = mock_wizard.call_args.args[0]
    assert args.commit_interval_mins == 5
    assert args.log_retention_days == 30
    assert args.disable_cron_sync is False
    assert args.disable_cron_compact is False
    assert args.disable_delete_after is False


def test_provision_rejects_out_of_range_sample_rate():
    """``--sample-rate`` is min=1, max=100 at the typer layer so the
    error points at the right flag (not the wizard's defence clamp)."""
    result = runner.invoke(app, ["provision", "--yes", "--service-id", "s", "--sample-rate", "500"])
    assert result.exit_code != 0


def test_teardown_propagates_no_remove_flags_to_handler():
    """``--no-remove-*`` flags arrive True — dropping any one silently
    widens the teardown blast radius."""
    with patch("backend.provision.cli.handle_teardown") as mock_handler:
        result = runner.invoke(
            app,
            [
                "teardown",
                "--yes",
                "--bucket",
                "b",
                "--token",
                "t",
                "--no-remove-logging",
                "--no-remove-cdn",
                "--no-remove-bucket",
            ],
        )
    assert result.exit_code == 0, result.output
    args = mock_handler.call_args.args[0]
    assert args.no_remove_logging is True
    assert args.no_remove_cdn is True
    assert args.no_remove_bucket is True
    assert args.bucket == "b"
    assert args.token == "t"


def test_teardown_remove_data_default_is_false():
    """``--remove-data`` is opt-in — default True would auto-delete
    the local DuckDB/cache on every teardown."""
    with patch("backend.provision.cli.handle_teardown") as mock_handler:
        runner.invoke(app, ["teardown", "--yes", "--bucket", "b", "--token", "t"])
    args = mock_handler.call_args.args[0]
    assert args.remove_data is False
    assert args.no_remove_logging is False
    assert args.no_remove_cdn is False
    assert args.no_remove_bucket is False


def test_update_logs_parses_dry_run_and_list_flags():
    """``--dry-run`` + repeated group flags arrive intact — the
    preset-swap merge-guard handler test fires these exact flags."""
    with patch("backend.provision.cli.handle_update_logs") as mock_handler:
        result = runner.invoke(
            app,
            [
                "update-logs",
                "--service-id",
                "svc",
                "--dry-run",
                "--preset",
                "standard",
                "--enable-group",
                "L",
                "--disable-group",
                "M",
            ],
        )
    assert result.exit_code == 0, result.output
    args = mock_handler.call_args.args[0]
    assert args.dry_run is True
    assert args.preset == "standard"
    assert args.enable_group == ["L"]
    assert args.disable_group == ["M"]
    assert args.service_id == "svc"


def test_invite_analyst_parses_minimal_flags():
    """Exposes only ``--yes`` and ``--service-id`` — adding flags
    without threading them through the handler has bitten this before."""
    with patch("backend.provision.cli.handle_invite_analyst") as mock_handler:
        result = runner.invoke(app, ["invite-analyst", "--yes", "--service-id", "svc-x"])
    assert result.exit_code == 0, result.output
    args = mock_handler.call_args.args[0]
    assert args.yes is True
    assert args.service_id == "svc-x"


def test_update_cdn_parses_service_and_token():
    """Simplest dispatcher — the two flags must still reach the handler."""
    with patch("backend.provision.cli.handle_update_cdn") as mock_handler:
        result = runner.invoke(app, ["update-cdn", "--service-id", "svc", "--token", "tok"])
    assert result.exit_code == 0, result.output
    args = mock_handler.call_args.args[0]
    assert args.service_id == "svc"
    assert args.token == "tok"


def test_list_groups_runs_without_service_id():
    """No ``--service-id`` succeeds — list-groups is sometimes the
    FIRST command an admin runs (no services yet)."""
    with patch("backend.provision.cli.handle_list_groups") as mock_handler:
        result = runner.invoke(app, ["list-groups"])
    assert result.exit_code == 0, result.output
    assert mock_handler.call_args.args[0].service_id is None


def test_list_fields_takes_no_args_and_runs():
    """``list-fields`` is argument-free — guards against adding a
    required option that breaks the catalog diagnostic."""
    with patch("backend.provision.cli.handle_list_fields") as mock_handler:
        result = runner.invoke(app, ["list-fields"])
    assert result.exit_code == 0, result.output
    assert mock_handler.call_count == 1


def test_unknown_subcommand_exits_nonzero():
    """Typo in subcommand → non-zero exit (shell scripts key on it)."""
    assert runner.invoke(app, ["nonexistent-subcommand"]).exit_code != 0


def test_unknown_flag_on_provision_exits_nonzero():
    """Unknown flag on a real subcommand exits non-zero (typer
    default) — silently ignoring would mask renamed-flag migrations."""
    result = runner.invoke(app, ["provision", "--yes", "--service-id", "s", "--this-flag-does-not-exist"])
    assert result.exit_code != 0


# ── Merged from tests/test_provision_cli_handlers.py ──
def _args(**overrides):
    """argparse.Namespace stand-in with the union of all CLI flags
    pre-populated to safe defaults — every handler reads ``getattr(args, X)``
    so missing attrs would AttributeError. The wizard takes the most
    flags, so default everything it inspects to None."""
    defaults = {
        "yes": True,  # non-interactive
        "token": None,
        "service_id": None,
        "endpoint_name": None,
        "region": None,
        "bucket": None,
        "prefix": None,
        "sample_rate": None,
        "edge_only": None,
        "period": None,
        "cdn_name": None,
        "cdn_prefix": None,
        "shield": None,
        "delete_after": True,
        "disable_delete_after": False,
        "disable_cron_sync": False,
        "commit_interval_mins": 5,
        "disable_cron_compact": False,
        "log_retention_days": 30,
        "preset": None,
        "enable_group": None,
        "disable_group": None,
        "enable_field": None,
        "disable_field": None,
        "dry_run": False,
        "remove_data": False,
        "no_remove_logging": False,
        "no_remove_cdn": False,
        "no_remove_bucket": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_handle_list_groups_prints_table_with_enabled_column(capsys):
    """Diagnostic command; pinned to ensure the column order (Group /
    Enabled / Bytes / Fields) stays stable since users grep the output."""
    cli.handle_list_groups(_args())
    out = capsys.readouterr().out
    assert "Group" in out and "Enabled" in out and "Bytes" in out
    # "(core)" row always present (group=None)
    assert "(core)" in out


def test_handle_list_groups_reads_existing_service_config(capsys):
    """When ``--service-id`` is provided, the "Enabled" column reflects
    that service's actual config — pinned because admins use the flag
    to audit a deployed service's setup."""
    fake_cfg = {"log_fields": {"groups": ["A"]}}
    with patch("backend.config.load_config", return_value=fake_cfg):
        cli.handle_list_groups(_args(service_id="svc-123"))
    out = capsys.readouterr().out
    # Just verify it didn't crash and rendered output
    assert "(core)" in out


def test_handle_list_fields_prints_field_catalog(capsys):
    """Pinned to ensure the field table renders — admins use this to
    discover field IDs for ``--enable-field`` flags."""
    cli.handle_list_fields(_args())
    out = capsys.readouterr().out
    assert "Field" in out and "Group" in out and "Type" in out
    # At least one core field always present
    assert "client.ip" in out or "timestamp" in out or "method" in out


def test_handle_teardown_fails_when_no_service_and_no_bucket(capsys):
    """Without a service config OR a ``--bucket`` flag, the handler
    can't know what to delete — must exit non-zero, not silently no-op.
    Pinned because a silent no-op would mislead admins into thinking
    the teardown succeeded."""
    with patch("backend.config.list_service_ids", return_value=[]):
        with pytest.raises(SystemExit) as exc:
            cli.handle_teardown(_args())
    assert exc.value.code == 1


def test_handle_teardown_falls_back_to_bucket_flag_when_no_config(capsys):
    """If no service config exists but ``--bucket`` is provided, the
    handler proceeds with a minimal state. Pinned because this is the
    recovery path for "deleted the config but the bucket still exists"
    cleanup scenarios."""
    with (
        patch("backend.config.list_service_ids", return_value=[]),
        patch("backend.provision.cli.perform_teardown", return_value=iter([])),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        # token from env so the prompt path is skipped under --yes
        cli.handle_teardown(_args(bucket="orphan-bucket", token="t"))


def test_handle_teardown_loads_state_from_service_config(monkeypatch):
    monkeypatch.delenv("FASTLY_API_KEY", raising=False)
    """When the service config exists, teardown state pulls the bucket,
    region, keys, endpoint name etc. from there. Pinned because losing
    this would force admins to re-supply every flag."""
    fake_cfg = {
        "fos_bucket": "my-bucket",
        "fos_region": "us-east-1",
        "fastly_api_key": "tok",
        "provisioning": {
            "fos_key_id": "k1",
            "endpoint_name": "MyEndpoint",
            "cdn_service_id": "cdn1",
            "cdn_url": "https://x.example",
        },
    }
    perform_calls = []

    def _record_teardown(state, token, opts=None):
        perform_calls.append((state, token, opts))
        return iter([])

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.cli.perform_teardown", side_effect=_record_teardown),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        cli.handle_teardown(_args(service_id="svc-1"))

    assert len(perform_calls) == 1
    state, token, opts = perform_calls[0]
    assert state["fos_bucket_name"] == "my-bucket"
    assert state["endpoint_name"] == "MyEndpoint"
    assert state["cdn_service_id"] == "cdn1"
    assert token == "tok"
    # All remove_* flags default to True
    assert opts == {"remove_logging": True, "remove_cdn": True, "remove_bucket": True, "remove_scoring": True}


def test_handle_teardown_carries_scoring_block_into_state():
    """The teardown state must carry cfg['scoring'] so perform_teardown can tear
    down the Compute scorer + its stores. Pinned because the root cause of the
    orphaned-scoring bug was the CLI state builder omitting the scoring block."""
    fake_cfg = {
        "fos_bucket": "my-bucket",
        "fastly_api_key": "tok",
        "provisioning": {"endpoint_name": "E", "cdn_service_id": "cdn1"},
        "scoring": {
            "enabled": True,
            "scoring_service_id": "SCORESVC",
            "scoring_keys_store_id": "KEYS",
            "scoring_config_store_id": "CFG",
            "scoring_matrix_store_id": "MTX",
        },
    }
    perform_calls = []

    def _record(state, token, opts=None):
        perform_calls.append(state)
        return iter([])

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.cli.perform_teardown", side_effect=_record),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        cli.handle_teardown(_args(service_id="svc-1", token="tok"))

    assert len(perform_calls) == 1
    assert perform_calls[0]["scoring"]["enabled"] is True
    assert perform_calls[0]["scoring"]["scoring_service_id"] == "SCORESVC"


def test_handle_teardown_respects_no_remove_flags():
    """The ``--no-remove-logging`` / ``--no-remove-cdn`` /
    ``--no-remove-bucket`` flags must propagate through to the
    orchestrator's opts dict. Pinned because partial teardowns are the
    only safe way to remove an analyst service without breaking the
    admin one."""
    perform_calls = []

    def _record(state, token, opts=None):
        perform_calls.append(opts)
        return iter([])

    with (
        patch("backend.config.list_service_ids", return_value=[]),
        patch("backend.provision.cli.perform_teardown", side_effect=_record),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        cli.handle_teardown(
            _args(
                bucket="b",
                token="t",
                no_remove_logging=True,
                no_remove_cdn=True,
                no_remove_bucket=False,
            )
        )
    assert perform_calls == [
        {"remove_logging": False, "remove_cdn": False, "remove_bucket": True, "remove_scoring": True}
    ]


def test_handle_teardown_exits_1_on_orchestrator_exception(capsys):
    """Any exception raised by perform_teardown surfaces as exit code 1
    plus a "Teardown failed:" message — pinned because the exit code is
    what shell scripts key on."""
    with (
        patch("backend.config.list_service_ids", return_value=[]),
        patch("backend.provision.cli.perform_teardown", side_effect=RuntimeError("network down")),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_teardown(_args(bucket="b", token="t"))
    assert exc.value.code == 1
    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "Teardown failed" in combined or "network down" in combined


def test_handle_invite_analyst_fails_without_service_id(capsys):
    """No --service-id and no configured services → exit 1. Pinned
    because the handler is sometimes invoked from a CI step where
    no services have been provisioned yet."""
    with patch("backend.config.list_service_ids", return_value=[]):
        with pytest.raises(SystemExit) as exc:
            cli.handle_invite_analyst(_args())
    assert exc.value.code == 1


def test_handle_invite_analyst_auto_picks_single_service():
    """When exactly one service is configured, the handler picks it
    automatically rather than prompting. Pinned because most installs
    have a single service and the prompt would be friction."""
    fake_cfg = {"access_level": "read_write"}
    with (
        patch("backend.config.list_service_ids", return_value=["only-svc"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch(
            "backend.provision.cli.generate_analyst_invite",
            return_value={"service_id": "only-svc", "secret_key": "sk"},
        ),
    ):
        cli.handle_invite_analyst(_args())


def test_handle_invite_analyst_rejects_readonly_service(capsys):
    """An analyst can only be invited from a ``read_write`` service —
    inviting from another analyst's read-only replica must be refused.
    Pinned because the read-only invite would generate an unusable
    credential that fails on first read."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value={"access_level": "read_only"}),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_invite_analyst(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_invite_analyst_prints_invite_and_warns_about_secret(capsys):
    """Output must include the invite payload AND the "save the secret
    now" warning. Pinned because the secret can never be regenerated;
    losing the warning would lead to a lockout."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value={"access_level": "read_write"}),
        patch(
            "backend.provision.cli.generate_analyst_invite",
            return_value={"service_id": "svc", "secret_key": "abc123"},
        ),
    ):
        cli.handle_invite_analyst(_args(service_id="svc"))
    out = capsys.readouterr().out
    assert "abc123" in out
    assert "secret" in out.lower()


def test_handle_invite_analyst_exits_1_on_generation_failure(capsys):
    """If ``generate_analyst_invite`` raises (e.g., DB write failed),
    surface as exit 1 with the message. Pinned because silent failures
    here have led to admins thinking the invite went through."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value={"access_level": "read_write"}),
        patch("backend.provision.cli.generate_analyst_invite", side_effect=RuntimeError("db locked")),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_invite_analyst(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_logs_fails_when_no_services_configured(capsys):
    with patch("backend.config.list_service_ids", return_value=[]):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_logs(_args())
    assert exc.value.code == 1


def test_handle_update_logs_fails_when_config_missing(capsys):
    """``list_service_ids`` returns an id but ``load_config`` returns
    None — happens when the metadata file is mid-delete. Pinned
    because a None-returning load shouldn't crash the CLI."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value=None),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_logs(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_logs_dry_run_prints_format_and_returns(capsys):
    """``--dry-run`` prints the generated log format and returns
    without calling the Fastly API. Pinned because admins use this
    to preview before pushing."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value={"log_fields": {"groups": ["A"]}}),
        patch("backend.core.field_registry.generate_log_format", return_value="EXPECTED_LOG_FORMAT"),
        patch("backend.provision.cli.update_logging_endpoint") as mock_update,
    ):
        cli.handle_update_logs(_args(service_id="svc", dry_run=True))
    assert "EXPECTED_LOG_FORMAT" in capsys.readouterr().out
    # Crucially, the Fastly push must NOT have been called
    mock_update.assert_not_called()


def test_handle_update_logs_pushes_to_fastly_and_persists_config():
    """Non-dry-run: write the merged config + call
    ``update_logging_endpoint`` with the right shape. Pinned because
    the ``update_format=True`` flag is what tells the API to actually
    push the format (vs just bumping the version)."""
    write_calls = []
    update_calls = []

    def _record_update(cfg, token):
        update_calls.append((cfg, token))
        return iter([{"type": "done", "version": 42}])

    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={
                "fastly_api_key": "tok",
                "log_fields": {"groups": ["A"]},
                "provisioning": {"endpoint_name": "ep", "sample_rate": 50, "edge_only": True, "log_period": 60},
            },
        ),
        patch("backend.provision.cli.write_service_config", side_effect=lambda c: write_calls.append(c)),
        patch("backend.provision.cli.update_logging_endpoint", side_effect=_record_update),
        patch("backend.core.field_registry.format_hash", return_value="hash123"),
    ):
        cli.handle_update_logs(_args(service_id="svc"))

    assert len(write_calls) == 1
    persisted = write_calls[0]
    assert persisted["log_fields"]["format_hash"] == "hash123"
    assert "format_updated_at" in persisted["log_fields"]

    assert len(update_calls) == 1
    cfg, token = update_calls[0]
    assert cfg["update_format"] is True
    assert cfg["logging_service_id"] == "svc"
    assert cfg["sample_rate"] == 50
    assert token == "tok"


def test_handle_update_logs_preserves_scoring_custom_fields_on_preset_swap():
    """REGRESSION (sibling of 2026-06-02 state_sync incident): running
    ``handle_update_logs`` with a --preset (or --enable-group/--disable-group)
    argument used to clobber the entire cfg.log_fields, silently dropping
    the scoring custom_fields that ``enable_scoring`` had injected.
    The new merge guard preserves existing custom_fields and re-injects
    ``_SCORING_CUSTOM_FIELDS`` when scoring is enabled."""
    write_calls = []

    def _record_update(cfg, token):
        return iter([{"type": "done", "version": 1}])

    pre_scoring_field = {"name": "edge_score", "duckdb_type": "INTEGER", "enabled": True}

    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={
                "fastly_api_key": "tok",
                "scoring": {"enabled": True},
                "log_fields": {
                    "groups": ["A"],
                    "custom_fields": [pre_scoring_field],
                },
                "provisioning": {"endpoint_name": "ep"},
            },
        ),
        patch("backend.provision.cli.write_service_config", side_effect=lambda c: write_calls.append(c)),
        patch("backend.provision.cli.update_logging_endpoint", side_effect=_record_update),
        patch("backend.core.field_registry.format_hash", return_value="h"),
    ):
        # --preset triggers the rebuild path (the bug-bait branch). Without
        # the merge guard, the persisted cfg's custom_fields would be empty.
        cli.handle_update_logs(_args(service_id="svc", preset="standard"))

    from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES

    assert len(write_calls) == 1
    persisted_names = {cf["name"] for cf in write_calls[0]["log_fields"]["custom_fields"]}
    for name in _SCORING_FIELD_NAMES:
        assert name in persisted_names, f"scoring field {name!r} dropped on preset swap"


def test_handle_update_logs_exits_1_on_orchestrator_exception():
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={"fastly_api_key": "tok", "log_fields": {"groups": ["A"]}, "provisioning": {}},
        ),
        patch("backend.provision.cli.write_service_config"),
        patch("backend.provision.cli.update_logging_endpoint", side_effect=RuntimeError("api down")),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_logs(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_cdn_fails_when_no_services(capsys):
    with patch("backend.config.list_service_ids", return_value=[]):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_cdn(_args())
    assert exc.value.code == 1


def test_handle_update_cdn_fails_when_config_missing(capsys):
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value=None),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_cdn(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_cdn_uses_cdn_service_id_from_provisioning():
    """The CDN service ID lives under ``cfg['provisioning']['cdn_service_id']``
    — pinned because reading it from the wrong slot would re-deploy
    against the customer's CDN service (data loss)."""
    redeploy_calls = []
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={
                "fastly_api_key": "tok",
                "provisioning": {"cdn_service_id": "cdn-target", "rate_limiting": True},
            },
        ),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=None),
        patch(
            "backend.provision.cli.redeploy_cdn_vcl",
            side_effect=lambda sid, token, rate_limiting=True: redeploy_calls.append((sid, token, rate_limiting)) or 5,
        ),
    ):
        cli.handle_update_cdn(_args(service_id="svc"))
    # Detection inconclusive (None) → fall back to the persisted flag (True).
    assert redeploy_calls == [("cdn-target", "tok", True)]


def test_handle_update_cdn_falls_back_to_find_by_name():
    """When ``cdn_service_id`` is missing from the config (config-
    corruption recovery), fall back to looking up by the configured
    CDN service name. Pinned because the fallback prevents admins
    from having to manually re-link a partial provision."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={
                "fastly_api_key": "tok",
                "cdn_service_name": "MyCDNService",
                "provisioning": {},
            },
        ),
        patch("backend.provision.cli.find_service_by_name", return_value={"id": "found-cdn-id"}),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=None),
        patch("backend.provision.cli.redeploy_cdn_vcl", return_value=7) as mock_redeploy,
    ):
        cli.handle_update_cdn(_args(service_id="svc"))
    # The fallback ID must reach redeploy_cdn_vcl
    args_used, kwargs_used = mock_redeploy.call_args
    assert args_used[0] == "found-cdn-id"


def test_handle_update_cdn_fails_when_cdn_lookup_returns_none():
    """find_service_by_name → None → exit 1 (rather than passing
    None as the service ID, which would 404 the API)."""
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={"fastly_api_key": "tok", "cdn_service_name": "X", "provisioning": {}},
        ),
        patch("backend.provision.cli.find_service_by_name", return_value=None),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_cdn(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_cdn_exits_1_on_redeploy_exception():
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch(
            "backend.config.load_config",
            return_value={"fastly_api_key": "tok", "provisioning": {"cdn_service_id": "cdn"}},
        ),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=None),
        patch("backend.provision.cli.redeploy_cdn_vcl", side_effect=RuntimeError("vcl rejected")),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_update_cdn(_args(service_id="svc"))
    assert exc.value.code == 1


def test_handle_update_cdn_reprobes_persists_and_applies_new_entitlement():
    """A customer who LOST edge rate limiting (or never had it) is detected on
    redeploy: the refreshed flag is persisted via save_config AND passed to
    redeploy_cdn_vcl so the CDN deploys without ratecounters."""
    saved = []
    redeploy_calls = []
    cfg = {
        "service_id": "svc",
        "fastly_api_key": "tok",
        # persisted flag says True, but the account no longer has the feature.
        "provisioning": {"cdn_service_id": "cdn", "rate_limiting": True},
    }
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(lambda sid, c: saved.append((sid, c)))),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=False),
        patch(
            "backend.provision.cli.redeploy_cdn_vcl",
            side_effect=lambda sid, token, rate_limiting=True: redeploy_calls.append(rate_limiting) or 9,
        ),
    ):
        cli.handle_update_cdn(_args(service_id="svc"))

    # Refreshed flag persisted under provisioning.rate_limiting...
    assert saved and saved[0][0] == "svc"
    assert saved[0][1]["provisioning"]["rate_limiting"] is False
    # ...and the deploy uses the freshly-detected value, not the stale True.
    assert redeploy_calls == [False]


def test_handle_update_cdn_does_not_persist_when_flag_unchanged():
    """Detection matches the persisted flag → no redundant save_config write."""
    saved = []
    cfg = {
        "service_id": "svc",
        "fastly_api_key": "tok",
        "provisioning": {"cdn_service_id": "cdn", "rate_limiting": True},
    }
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.mutate_config", side_effect=mutate_via_save(lambda sid, c: saved.append((sid, c)))),
        patch("backend.provision.cli.account_has_rate_limiting", return_value=True),
        patch("backend.provision.cli.redeploy_cdn_vcl", return_value=9),
    ):
        cli.handle_update_cdn(_args(service_id="svc"))

    assert saved == []


def test_wizard_yes_mode_builds_complete_config_with_defaults():
    """The ``--yes`` path skips every prompt and returns a fully-formed
    config. Pinned because the unattended-install flow (and the
    `/api/provision/start` API route) drive the same code path."""
    fake_svc = {"id": "svc-1", "name": "Customer Service"}
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value=fake_svc),
    ):
        cfg = cli.wizard(_args(token="tok", service_id="svc-1"))

    # Spot-check the canonical keys the orchestrator expects
    assert cfg["admin_token"] == "tok"
    assert cfg["logging_service_id"] == "svc-1"
    assert cfg["service_name"] == "Customer Service"
    assert cfg["fos_region"] == "us-east-1"
    assert cfg["fos_bucket_name"] == "fos-svc-1-logs"
    assert cfg["sample_rate"] == 100
    assert cfg["edge_only"] is True
    assert cfg["cdn_url"].startswith("https://svc-1.")
    assert cfg["delete_after"] is True
    assert "cdn_secret" in cfg and len(cfg["cdn_secret"]) >= 16
    assert cfg["log_fields"]["preset"] == "standard"


def test_wizard_exits_1_when_log_format_validation_fails(capsys):
    """If the pre-flight log-format check produces errors, the wizard
    aborts BEFORE asking for a token. Pinned because a bad log format
    means the deploy would fail anyway — better to fail fast than
    after the admin has typed in a token."""
    with patch("backend.provision.cli.validate_log_format", return_value=["LOG_FORMAT_TOO_LONG"]):
        with pytest.raises(SystemExit) as exc:
            cli.wizard(_args(token="tok", service_id="svc-1"))
    assert exc.value.code == 1
    assert "LOG_FORMAT_TOO_LONG" in capsys.readouterr().out


def test_wizard_sanitises_service_id_for_bucket_name():
    """Bucket name derives from service_id with non-alphanumeric chars
    replaced. Pinned because S3-compatible bucket names reject ``_``,
    ``.``, uppercase, etc., and a leaked unsafe id would fail at
    ``CreateBucket`` time."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg = cli.wizard(_args(token="t", service_id="My_Service.ID_123"))
    assert cfg["fos_bucket_name"] == "fos-my-service-id-123-logs"
    # No invalid bucket-name chars leak through
    assert "_" not in cfg["fos_bucket_name"]
    assert "." not in cfg["fos_bucket_name"]


def test_wizard_clamps_sample_rate_to_1_to_100():
    """sample_rate is clamped client-side to [1, 100] regardless of
    what the args say — pinned because Fastly rejects out-of-range
    values and clamping in the wizard prevents a confusing 422 from
    the deploy step."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg_high = cli.wizard(_args(token="t", service_id="s", sample_rate=500))
        cfg_low = cli.wizard(_args(token="t", service_id="s", sample_rate=-5))
    assert cfg_high["sample_rate"] == 100
    assert cfg_low["sample_rate"] == 1


def test_wizard_disable_delete_after_overrides_yes_default():
    """The wizard's ``--yes`` default for ``delete_after`` is True,
    but ``--disable-delete-after`` must flip it off. Pinned because
    customers retaining raw logs for compliance need this explicit
    opt-out."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg = cli.wizard(_args(token="t", service_id="s", disable_delete_after=True))
    assert cfg["delete_after"] is False


def test_wizard_disable_cron_flags_propagate_to_config():
    """Cron sync + cron compact default to enabled; the two
    ``--disable-*`` flags flip them off. Pinned because the cron
    scheduler reads these flags exactly once at startup."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg = cli.wizard(
            _args(token="t", service_id="s", disable_cron_sync=True, disable_cron_compact=True),
        )
    assert cfg["enable_cron_sync"] is False
    assert cfg["enable_cron_compact"] is False


def test_wizard_passes_log_fields_args_through_to_builder():
    """``--enable-group`` / ``--disable-group`` etc. must reach the
    log_fields config so the resulting VCL matches the admin's
    intent. Pinned because losing the wiring would silently fall
    back to the standard preset."""
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
    ):
        cfg = cli.wizard(
            _args(
                token="t",
                service_id="s",
                enable_group=["L"],
                disable_field=["client.geo.region"],
            ),
        )
    # The L group was added on top of the standard preset
    assert "L" in cfg["log_fields"]["groups"]
    # The disable_field override surfaces as False
    assert cfg["log_fields"]["field_overrides"].get("client.geo.region") is False


def test_wizard_confirmation_no_exits_silently(monkeypatch):
    """When the user answers No to the "Proceed?" prompt in
    interactive mode, exit with code 0 (success). Pinned because
    losing this would exit 1, making it look like a failure when
    the user just cancelled."""
    monkeypatch.setenv("FASTLY_API_KEY", "")
    args = _args(yes=False, token="t", service_id="s")

    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}),
        # All interactive prompts → defaults
        patch(
            "backend.provision.cli.ask", side_effect=lambda *a, **k: a[1] if len(a) > 1 else (k.get("default") or "")
        ),
        patch("backend.provision.cli.ask_int", side_effect=lambda *a, **k: 100),
        # ask_yes returns True for "edge_only", "delete_after"; False for "Proceed?"
        patch("backend.provision.cli.ask_yes", side_effect=[True, True, False]),
    ):
        with pytest.raises(SystemExit) as exc:
            cli.wizard(args)

    assert exc.value.code == 0


def test_handle_teardown_no_user_confirmation_exits_silently():
    """When the user answers No to the "This cannot be undone" prompt
    in interactive mode, exit with code 0. Pinned because losing
    this would treat the user's cancellation as a teardown failure."""
    fake_cfg = {"fos_bucket": "my-bucket", "fastly_api_key": "tok", "provisioning": {}}

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.cli.ask_yes", return_value=False),
        patch("backend.provision.cli.perform_teardown") as mock_teardown,
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_teardown(_args(yes=False, service_id="svc-1"))

    assert exc.value.code == 0
    mock_teardown.assert_not_called()


def test_handle_teardown_prompts_for_token_when_not_in_env_or_config():
    """When no --token, no FASTLY_API_KEY env var, and no
    stored fastly_api_key — prompt for it interactively. Pinned
    because losing this would exit 1 immediately even when the
    user could supply the token at runtime."""
    fake_cfg = {"fos_bucket": "b", "provisioning": {}}  # no fastly_api_key

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("os.getenv", return_value=None),
        patch("backend.provision.cli.ask", return_value="prompted-token"),
        patch("backend.provision.cli.perform_teardown", return_value=iter([])),
        patch("backend.provision.cli.cleanup_local_data"),
    ):
        # Should NOT exit (the prompt provides the token)
        cli.handle_teardown(_args(service_id="svc-1"))


def test_handle_teardown_exits_1_when_prompt_returns_no_token():
    """If the prompt returns empty, exit 1. Pinned because pressing
    Enter at the prompt without typing should fail-fast."""
    fake_cfg = {"fos_bucket": "b", "provisioning": {}}

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("os.getenv", return_value=None),
        patch("backend.provision.cli.ask", return_value=""),  # empty prompt response
    ):
        with pytest.raises(SystemExit) as exc:
            cli.handle_teardown(_args(service_id="svc-1"))
    assert exc.value.code == 1


def test_handle_invite_analyst_multi_service_prompts_for_id():
    """When multiple services exist and no --service-id was supplied,
    print a menu and prompt for the ID. Pinned because losing this
    would auto-pick the first service silently (wrong service in a
    multi-tenant install)."""
    cfgs_by_id = {
        "svc-a": {"name": "Svc A", "access_level": "read_write"},
        "svc-b": {"name": "Svc B", "access_level": "read_write"},
    }

    with (
        patch("backend.config.list_service_ids", return_value=["svc-a", "svc-b"]),
        patch("backend.config.load_config", side_effect=lambda sid: cfgs_by_id.get(sid)),
        patch("backend.provision.cli.ask", return_value="svc-b"),
        patch(
            "backend.provision.cli.generate_analyst_invite",
            return_value={"service_id": "svc-b", "secret_key": "sk"},
        ),
    ):
        # Should NOT raise — the prompt picks svc-b
        cli.handle_invite_analyst(_args(service_id=None))


def test_wizard_default_token_from_env(monkeypatch):
    """When no ``--token`` is provided, the wizard reads ``FASTLY_API_KEY``
    from the environment. Pinned because docker-compose deployments
    set the env var rather than passing flags."""
    monkeypatch.setenv("FASTLY_API_KEY", "env-tok")
    with (
        patch("backend.provision.cli.validate_log_format", return_value=[]),
        patch("backend.provision.cli.fastly", return_value={"name": "x"}) as mock_fastly,
    ):
        cli.wizard(_args(service_id="s"))
    # fastly was called with the env-derived token
    _, kwargs = mock_fastly.call_args
    assert kwargs.get("token") == "env-tok"


def test_handle_enable_scoring_dispatches_to_orchestrator():
    """enable-scoring resolves service + token from config and calls
    session_scoring_orchestrator.enable_scoring. Pinned so the CLI redeploy
    path stays wired to the same flow the admin UI button runs."""
    fake_cfg = {"fastly_api_key": "tok", "scoring": {"enabled": True}}
    calls = []

    def _record(sid, token, **kw):
        calls.append((sid, token))
        return {"logging_service_active_version": 471}

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.session_scoring_orchestrator.enable_scoring", side_effect=_record),
    ):
        cli.handle_enable_scoring(_args(service_id="svc-1"))

    assert calls == [("svc-1", "tok")]


def test_handle_enable_scoring_no_services_exits():
    """No configured service → clean sys.exit, not a traceback."""
    with (
        patch("backend.config.list_service_ids", return_value=[]),
        pytest.raises(SystemExit),
    ):
        cli.handle_enable_scoring(_args(service_id=None))


def test_handle_disable_scoring_dispatches_to_orchestrator():
    """disable-scoring (with --yes, non-interactive) resolves service + token
    and calls session_scoring_orchestrator.disable_scoring."""
    fake_cfg = {"fastly_api_key": "tok", "scoring": {"enabled": True}}
    calls = []

    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch(
            "backend.provision.session_scoring_orchestrator.disable_scoring",
            side_effect=lambda sid, token, **kw: calls.append((sid, token)),
        ),
    ):
        cli.handle_disable_scoring(_args(service_id="svc-1", yes=True))

    assert calls == [("svc-1", "tok")]


def test_handle_disable_scoring_aborts_without_confirmation():
    """Without --yes the handler prompts; a 'no' answer aborts via sys.exit
    BEFORE touching the orchestrator."""
    fake_cfg = {"fastly_api_key": "tok", "scoring": {"enabled": True}}
    with (
        patch("backend.config.list_service_ids", return_value=["svc-1"]),
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.provision.cli.ask_yes", return_value=False),
        patch("backend.provision.session_scoring_orchestrator.disable_scoring") as mock_disable,
        pytest.raises(SystemExit),
    ):
        cli.handle_disable_scoring(_args(service_id="svc-1", yes=False))
    mock_disable.assert_not_called()
