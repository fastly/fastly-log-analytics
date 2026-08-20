"""Tests for backend.provision.session_scoring_orchestrator.

The orchestrator is the most consequential code in the scoring stack — a
single botched VCL activation could break the customer's live service.
These tests pin the contract: every Fastly API call we expect to see in
the right order, plus the rollback path on failure."""

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

LOG_SVC = "TestLogSvc123"
SCORE_SVC = "ScoringSvcId"
TOKEN = "FAKE_TOKEN"


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
    # Robust to field-set growth: exactly one of each, no duplicates.
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
    sso._remove_scoring_custom_fields(cfg)  # should not raise
    assert cfg == {}


# ── enable_scoring: happy path ────────────────────────────────────────────────


def _ensure_scoring_meta() -> dict:
    return {
        "scoring_service_id": SCORE_SVC,
        "scoring_service_name": f"Session Scoring Service for {LOG_SVC}",
        "scoring_domain": f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app",
        "scoring_keys_store_id": "KEYS_STORE",
        "scoring_config_store_id": "CFG_STORE",
        "scoring_matrix_store_id": "MATRIX_STORE",
        "aes_key_hex": "00" * 32,
        "request_secret": "fake_request_secret_for_tests_" + "x" * 32,
    }


def _happy_path_fastly_mock():
    """Returns sensible defaults for every Fastly API call enable_scoring
    makes after Stage 1 (ensure_scoring_service). The actual calls are
    asserted on the returned mock."""
    versions = [{"number": 100, "active": True}]
    s3_endpoints = [{"name": "Fastly Object Storage Logs"}]
    snippets = []
    backends = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        # Active-version probe
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return versions
        # Clone v100 → v101
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/100/clone"):
            return {"number": 101}
        # Comment update
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/101"):
            return {}
        # Backend probe (no scoring backend yet)
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/backend"):
            return backends
        # Add backend
        if (method, path) == ("POST", f"/service/{LOG_SVC}/version/101/backend"):
            backends.append({"name": body["name"]})
            return {"name": body["name"]}
        # Snippet GET (idempotency probes from ensure_vcl_snippet)
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/snippet"):
            return snippets
        # Snippet POST (create)
        if (method, path) == ("POST", f"/service/{LOG_SVC}/version/101/snippet"):
            snippets.append(
                {"name": body["name"], "type": body["type"], "content": body["content"], "priority": body["priority"]}
            )
            return snippets[-1]
        # s3 logging list
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/logging/s3"):
            return s3_endpoints
        # s3 logging endpoint update (format)
        if method == "PUT" and "/logging/s3/" in path:
            return {}
        # Validate
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/validate"):
            return {"status": "ok"}
        # Activate
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/101/activate"):
            return {}
        return {}

    return MagicMock(side_effect=side_effect)


def _happy_path_fastly_mock_with_post_secret(*, post_secret_409: bool = False):
    """Variant that also handles the request_secret heal POST/PATCH on
    the keys ConfigStore."""
    base_mock = _happy_path_fastly_mock()
    base_side_effect = base_mock.side_effect

    def side_effect(method, path, body=None, token=None, **kwargs):
        # Secret heal endpoints.
        if (method, path) == ("POST", "/resources/stores/config/KEYS_STORE/item"):
            if post_secret_409:
                raise RuntimeError("409 Conflict — item already exists")
            return {"item_key": body["item_key"]}
        if method == "PATCH" and path == "/resources/stores/config/KEYS_STORE/item/request_secret":
            return {"ok": True}
        return base_side_effect(method, path, body, token=token, **kwargs)

    return MagicMock(side_effect=side_effect)


def test_enable_scoring_happy_path_runs_all_stages(monkeypatch, tmp_path):
    """End-to-end enable on a clean service: ensure_scoring → wasm deploy
    → declarative reconciler."""
    # Pre-existing config — no scoring block yet.
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }

    fastly_mock = _happy_path_fastly_mock()

    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=_ensure_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package") as wasm_mock,
        patch.object(sso, "_write_matrix_to_kv") as kv_mock,
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        result = sso.enable_scoring(LOG_SVC, TOKEN)

    # Returned dict carries the scoring metadata + new active version.
    assert result["scoring_service_id"] == SCORE_SVC
    assert result["logging_service_active_version"] == 101

    # Prebuilt package deployed to the scoring service via the API (no build),
    # with the matrix KV store id so the link is ensured on the activated
    # version, and the tenant matrix seeded into KV (tenant-scoped — never the
    # legacy shared matrix.json, see audit finding #005).
    wasm_mock.assert_called_once_with(
        SCORE_SVC, "MATRIX_STORE", TOKEN, status_cb=None, prior_package_sha="", prior_files_hash=""
    )
    kv_mock.assert_called_once_with("MATRIX_STORE", LOG_SVC, TOKEN, status_cb=None)

    # Config was saved with the scoring block + custom fields.
    saved_calls = save_mock.call_args_list
    assert len(saved_calls) >= 1
    final_cfg = saved_calls[-1].args[1]
    assert final_cfg["scoring"]["enabled"] is True
    assert final_cfg["scoring"]["scoring_service_id"] == SCORE_SVC
    field_names = [cf["name"] for cf in final_cfg["log_fields"]["custom_fields"]]
    assert "edge_score" in field_names

    # The right reconciler call happened.
    reconcile_mock.assert_called_once_with(LOG_SVC, TOKEN, status_cb=None)


def test_enable_scoring_stamps_deploy_identity_for_drift_detection(monkeypatch):
    """enable_scoring stamps the deployed Wasm sha + VCL fingerprint into
    cfg.scoring so /scoring/status can flag when the edge falls behind a newer
    shipped build. The stamp must equal what shipped_scorer_identity recomputes,
    so a freshly-enabled service shows NO drift."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=_ensure_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package", return_value={"sha": "deadbeef_pkg_sha", "version": 7}),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    saved_cfg = save_mock.call_args_list[-1].args[1]
    # The Wasm sha is exactly what _deploy_wasm_package reported uploading.
    assert saved_cfg["scoring"]["deployed_package_sha"] == "deadbeef_pkg_sha"
    # The VCL fingerprint matches the canonical generator fingerprint — so a
    # status check immediately after enable computes identical hashes → no drift.
    assert saved_cfg["scoring"]["deployed_vcl_sha"] == sso.scorer_vcl_fingerprint(LOG_SVC)
    assert saved_cfg["scoring"]["deployed_vcl_sha"] == sso.shipped_scorer_identity(LOG_SVC)["vcl_sha"]


def test_scorer_vcl_fingerprint_deterministic_and_service_scoped():
    """The VCL drift fingerprint depends only on the generator code + service
    id (canonical secret/config), so it's stable across calls and distinct per
    service — operator regex/enforce/key tweaks must not shift it."""
    a = sso.scorer_vcl_fingerprint(LOG_SVC)
    assert a == sso.scorer_vcl_fingerprint(LOG_SVC)
    assert sso.scorer_vcl_fingerprint("SomeOtherSvc") != a


def test_deploy_wasm_package_returns_uploaded_bytes_sha(monkeypatch, tmp_path):
    """_deploy_wasm_package returns the sha256 of the exact package bytes it
    uploaded (the deployed_package_sha drift stamp) plus the new Compute
    version it activated (surfaced in the two-phase SSE log)."""
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
        # get_active_version (smart-redeploy skip probe) resolves via the
        # service module's own fastly binding — patch it too or it hits the real
        # api.fastly.com. prior_package_sha defaults to "" so no skip happens.
        patch("backend.core.fastly.service.fastly", shared),
        patch.object(sso, "fastly_raw", MagicMock(return_value={})),
    ):
        result = sso._deploy_wasm_package("SCORESVC", "MATRIX_STORE", TOKEN)
    assert result["sha"] == expected
    # The cloned draft (base v5 → clone "number": 6) is the version activated.
    assert result["version"] == 6
    # New: an empty package GET → no edge files_hash, and not a skip.
    assert result["skipped"] is False


def test_enable_scoring_populates_default_exclude_url_regex_on_first_enable(monkeypatch):
    """On first turn-on, enable_scoring must persist the bundled
    DEFAULT_ASSET_EXT_REGEX literal into cfg.scoring.exclude_url_regex
    so the admin UI's textarea is pre-populated with a real editable
    value instead of an empty box hiding behind a "show default" toggle."""
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
        patch.object(sso.svcconfig, "save_config") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=_ensure_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    saved_cfg = save_mock.call_args_list[-1].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] == DEFAULT_ASSET_EXT_REGEX, (
        "first enable must materialise the default regex into cfg so the "
        "admin UI shows the actual value (per operator UX feedback)"
    )


def test_enable_scoring_preserves_operator_overrides_on_reenable(monkeypatch):
    """Re-enabling scoring (e.g. via the admin UI to refresh provisioning)
    must NOT wipe operator-tunable overrides. The previous implementation
    replaced the entire ``scoring`` block, silently dropping any
    exclude_url_regex or enforce_status_code the operator had set."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
        # Pre-existing scoring block with operator overrides.
        "scoring": {
            "enabled": True,
            "exclude_url_regex": r"^/(healthz|metrics)$",
            "enforce_status_code": 403,
        },
    }
    fastly_mock = _happy_path_fastly_mock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=_ensure_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    saved_cfg = save_mock.call_args_list[-1].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] == r"^/(healthz|metrics)$", (
        "re-enable must preserve operator's exclude_url_regex override"
    )
    assert saved_cfg["scoring"]["enforce_status_code"] == 403, (
        "re-enable must preserve operator's enforce_status_code override"
    )


def test_enable_scoring_adds_backend_with_correct_shape(monkeypatch):
    """The scoring backend is managed and deployed via the Declarative Reconciler."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "ensure_scoring_service", return_value=_ensure_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    reconcile_mock.assert_called_once_with(LOG_SVC, TOKEN, status_cb=None)


def test_enable_scoring_installs_all_six_named_snippets(monkeypatch):
    """The scoring VCL is managed and deployed via the Declarative Reconciler."""
    from backend.provision.declarative.reconciler import ReconciliationResult

    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "ensure_scoring_service", return_value=_ensure_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=101)
        sso.enable_scoring(LOG_SVC, TOKEN)

    reconcile_mock.assert_called_once_with(LOG_SVC, TOKEN, status_cb=None)


# ── enable_scoring: rollback path ─────────────────────────────────────────────


def test_enable_scoring_rolls_back_on_validation_failure(monkeypatch):
    """When reconcile_vcl_state raises an exception, we MUST revert the on-disk config changes."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }

    fastly_mock = _happy_path_fastly_mock_with_post_secret()
    save_calls = []

    def fake_save(svc_id, c):
        save_calls.append(dict(c))

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config", side_effect=fake_save),
        patch.object(sso, "ensure_scoring_service", return_value=_ensure_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.side_effect = RuntimeError("Validation failed")
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.enable_scoring(LOG_SVC, TOKEN)

    # Config was reverted (no "scoring" key in the final saved state).
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
        patch.object(sso.svcconfig, "save_config"),
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
                # Pre-existing unrelated custom field — must survive the strip.
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
            # Pretend all 3 scoring snippets are present so DELETE fires.
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

    def fake_save(svc_id, c):
        saved.append(dict(c))

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config", side_effect=fake_save),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch.object(sso, "delete_scoring_service", return_value=[]) as delete_mock,
        patch.object(sso, "_delete_scoring_matrix_from_fos") as fos_cleanup_mock,
    ):
        sso.disable_scoring(LOG_SVC, TOKEN)

    # Snippets removed (3 DELETE /snippet/{name} calls)
    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    for name in (SCORING_RECV_NAME, SCORING_FETCH_NAME, SCORING_DELIVER_NAME):
        # url-encoded ':' becomes %3A but ' ' becomes %20 — match by presence
        assert any(
            method == "DELETE"
            and path.startswith(f"/service/{LOG_SVC}/version/201/snippet/")
            and (name.replace(" ", "%20").replace(":", "%3A") in path)
            for method, path in calls
        ), f"DELETE for {name} not found"
    # Backend removed
    assert any(
        method == "DELETE" and path.startswith(f"/service/{LOG_SVC}/version/201/backend/") for method, path in calls
    )
    # New version activated
    assert ("PUT", f"/service/{LOG_SVC}/version/201/activate") in calls
    # Compute teardown ran
    delete_mock.assert_called_once()
    # FOS matrix cleanup ran (inverse of publish_matrix_to_fos)
    fos_cleanup_mock.assert_called_once_with(LOG_SVC)
    # Scoring block cleared from config
    assert "scoring" not in saved[-1]
    # Custom fields stripped (only user_id remains)
    final_field_names = [cf["name"] for cf in saved[-1].get("log_fields", {}).get("custom_fields", [])]
    assert final_field_names == ["user_id"]


# ── _resolve_tenant_matrix_for_deploy ────────────────────────────────────────


def test_resolve_tenant_matrix_prefers_local_tenant_path(tmp_path, monkeypatch):
    """Local ``matrix_{sid}.json`` wins without an FOS round-trip — keeps
    deploys fast on the host that just ran retrain."""
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
    """No local file → fall back to FOS for this tenant and materialise
    it locally. Covers the cross-host case: retrain ran on a different
    backend than the one now invoking enable_scoring."""
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
    """No local file AND FOS returns nothing → None, deploy proceeds with
    empty default. Pinned because returning the legacy shared
    ``matrix.json`` here would re-introduce the cross-tenant leak audit
    finding #005 closed."""
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)

    # Pre-fix code would have picked this up — make sure we don't.
    fake_root.write_text('{"vocab_size": 99, "version": "leaked-from-another-tenant"}')

    with patch("backend.state_sync.fetch_matrix_from_fos", return_value=None):
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)

    assert resolved is None


def test_resolve_tenant_matrix_returns_none_when_fos_returns_empty_vocab(tmp_path, monkeypatch):
    """vocab_size==0 means an untrained default; treat it as no matrix
    so deploy_wasm.sh's vocab-size guard isn't tripped."""
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)

    with patch("backend.state_sync.fetch_matrix_from_fos", return_value={"vocab_size": 0}):
        resolved = sso._resolve_tenant_matrix_for_deploy(LOG_SVC)

    assert resolved is None
    assert not sso._tenant_matrix_path(LOG_SVC).exists()


# ── _deploy_wasm_package: API package deploy + matrix-KV link ────────────────


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
            return [{"number": 1}, {"number": 3}]  # highest = 3
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
    # Cloned the highest version, linked the matrix store on the draft, activated v4.
    assert ("PUT", f"/service/{SCORE_SVC}/version/3/clone") in methods
    assert any(p.endswith("/version/4/resource") and (b or {}).get("name") == "scoring_matrix" for m, p, b in calls)
    assert ("PUT", f"/service/{SCORE_SVC}/version/4/activate") in methods
    # Package uploaded as raw multipart to v4.
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


# ── _write_matrix_to_kv: tenant-scoped matrix → KV (no leak) ─────────────────


def test_write_matrix_to_kv_writes_tenant_matrix(tmp_path, monkeypatch):
    """The tenant's local matrix is PUT to the scoring_matrix KV store under
    the agreed key — never the legacy shared matrix.json (audit #005)."""
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
    # The KV payload is the compact FSM1 binary (780b152), not JSON. The
    # version string is encoded verbatim, so it still discriminates the
    # tenant matrix from the legacy shared one (audit #005).
    from backend.scoring.matrix import serialize_kv

    assert isinstance(data, (bytes, bytearray))
    assert data[:4] == b"FSM1"
    assert b"tenant-only" in data, "must encode the tenant matrix, not the legacy shared one (#005)"
    assert data == serialize_kv({"vocab_size": 5, "version": "tenant-only"})


def test_write_matrix_to_kv_skips_when_no_tenant_matrix(tmp_path, monkeypatch):
    """No tenant matrix anywhere → no KV write (L2 stays disabled). An
    adversarial legacy shared matrix.json must NOT be uploaded."""
    fake_root = tmp_path / "matrix.json"
    monkeypatch.setattr(sso, "_MATRIX_PATH", fake_root)
    fake_root.write_text('{"vocab_size": 99, "version": "leaked"}')  # legacy shared — must be ignored

    raw_calls = []
    with (
        patch.object(sso, "fastly_raw", side_effect=lambda *a, **k: raw_calls.append(a)),
        patch("backend.state_sync.fetch_matrix_from_fos", return_value=None),
    ):
        sso._write_matrix_to_kv("MATRIX_STORE", LOG_SVC, TOKEN)

    assert raw_calls == [], "must not upload the legacy shared matrix.json (audit #005)"


# ── Smart redeploy: edge-checked skip of no-op legs (Part D) ──────────────────


def _scoring_meta_full() -> dict:
    return {
        "enabled": True,
        "scoring_service_id": SCORE_SVC,
        "scoring_keys_store_id": "KEYS",
        "scoring_config_store_id": "CFG",
        "scoring_matrix_store_id": "MTX",
        "scoring_domain": "scorer.edgecompute.app",
    }


def test_scoring_vcl_matches_edge_true_when_snippets_and_backend_match():
    """_scoring_vcl_matches_edge returns True only when every live scoring
    snippet body + the backend override_host match what we'd deploy now."""
    names = scoring_snippet_names()
    want = {n: f"body::{n}" for n in names}

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET" and path.endswith("/version/7/snippet"):
            return [{"name": n, "content": f"body::{n}"} for n in names]
        if method == "GET" and "/backend/" in path:
            return {"override_host": "scorer.edgecompute.app"}
        return {}

    with patch.object(sso, "fastly", MagicMock(side_effect=side_effect)):
        assert sso._scoring_vcl_matches_edge(LOG_SVC, 7, want, "scorer.edgecompute.app", TOKEN) is True


def test_scoring_vcl_matches_edge_false_when_a_snippet_differs():
    names = scoring_snippet_names()
    want = {n: f"body::{n}" for n in names}

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET" and path.endswith("/version/7/snippet"):
            live = [{"name": n, "content": f"body::{n}"} for n in names]
            live[0]["content"] = "DRIFTED"  # one snippet changed at the edge
            return live
        if method == "GET" and "/backend/" in path:
            return {"override_host": "scorer.edgecompute.app"}
        return {}

    with patch.object(sso, "fastly", MagicMock(side_effect=side_effect)):
        assert sso._scoring_vcl_matches_edge(LOG_SVC, 7, want, "scorer.edgecompute.app", TOKEN) is False


def test_deploy_wasm_package_skips_when_package_and_edge_unchanged(tmp_path):
    """_deploy_wasm_package skips the upload (no clone/activate) when the
    committed sha matches the prior stamp AND the live edge files_hash matches —
    returns skipped=True with the live active version."""
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
    # No upload happened.
    raw.assert_not_called()
    # No clone / activate (only the GET probes ran).
    methods_paths = [(c.args[0], c.args[1]) for c in shared.call_args_list]
    assert not any(m == "PUT" and "/clone" in p for m, p in methods_paths)
    assert not any(m == "PUT" and p.endswith("/activate") for m, p in methods_paths)


def test_enable_skips_logging_redeploy_when_vcl_matches_edge():
    """On a redeploy where the live VCL already matches, enable_scoring skips the
    logging-service clone/activate entirely (no version bump)."""
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
    meta = _ensure_scoring_meta()
    from backend.provision.declarative.reconciler import ReconciliationResult

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config") as save_mock,
        patch.object(sso, "ensure_scoring_service", return_value=meta),
        patch.object(
            sso, "_deploy_wasm_package", return_value={"version": 2, "sha": "PKG", "files_hash": "F", "skipped": True}
        ),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "_scoring_vcl_matches_edge", return_value=True),
        patch.object(sso, "_publish_scoring_fos_side_effects"),
        patch.object(sso, "fastly", shared),
        patch("backend.core.fastly.service.fastly", shared),
        patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as reconcile_mock,
    ):
        reconcile_mock.return_value = ReconciliationResult(service_id=LOG_SVC, activated_version=None)
        result = sso.enable_scoring(LOG_SVC, TOKEN)

    # No new logging version — we reused the active one.
    assert result["logging_service_active_version"] == 8
    reconcile_mock.assert_called_once_with(LOG_SVC, TOKEN, status_cb=None)


# ── teardown_scoring_resources (full-teardown scoring step, Part A) ───────────


def test_teardown_scoring_resources_strips_vcl_then_deletes():
    """teardown_scoring_resources strips the scoring VCL on a fresh clone
    (validate→activate) and THEN deletes the Compute service + stores — order
    matters so the live edge never references a deleted backend."""
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
    # VCL stripped on the cloned draft + activated.
    assert ("PUT", f"/service/{LOG_SVC}/version/50/clone") in methods_paths
    assert any(m == "DELETE" and "/version/51/snippet/" in p for m, p in methods_paths)
    assert any(m == "DELETE" and "/version/51/backend/" in p for m, p in methods_paths)
    assert ("PUT", f"/service/{LOG_SVC}/version/51/activate") in methods_paths
    # Resources deleted with the right ids — AFTER the activate.
    delete_mock.assert_called_once()
    assert delete_mock.call_args.kwargs["scoring_keys_store_id"] == "KEYS"
    assert delete_mock.call_args.kwargs["scoring_config_store_id"] == "CFG"
    assert delete_mock.call_args.kwargs["scoring_matrix_store_id"] == "MTX"
    assert order == ["activate", "delete_scoring_service"]


def test_teardown_scoring_resources_deletes_even_if_vcl_strip_fails():
    """A VCL-strip failure must not block the owned-resource deletion (the
    operator cares most about not paying for an orphaned Compute service)."""

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
    delete_mock.assert_called_once()  # resources still torn down
