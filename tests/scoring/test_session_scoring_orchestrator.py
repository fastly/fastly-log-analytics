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
    SCORING_BACKEND_API_NAME,
    SCORING_DELIVER_NAME,
    SCORING_ENFORCE_NAME,
    SCORING_FETCH_NAME,
    SCORING_MISS_NAME,
    SCORING_PASS_NAME,
    SCORING_RECV_NAME,
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
    ):
        assert required in names


def test_add_custom_fields_is_idempotent():
    """Re-running enable_scoring shouldn't duplicate the 6 fields."""
    cfg = {"log_fields": {"custom_fields": []}}
    sso._add_scoring_custom_fields(cfg)
    sso._add_scoring_custom_fields(cfg)
    names = [cf["name"] for cf in cfg["log_fields"]["custom_fields"]]
    assert len(names) == 6  # not 12


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


def test_enable_scoring_happy_path_runs_all_stages(monkeypatch, tmp_path):
    """End-to-end enable on a clean service: ensure_scoring → wasm deploy
    → clone → backend → snippets → format update → validate → activate."""
    # Pre-existing config — no scoring block yet.
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
        patch.object(sso, "_deploy_wasm") as wasm_mock,
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.enable_scoring(LOG_SVC, TOKEN)

    # Returned dict carries the scoring metadata + new active version.
    assert result["scoring_service_id"] == SCORE_SVC
    assert result["logging_service_active_version"] == 101

    # Wasm deploy was triggered.
    wasm_mock.assert_called_once_with(SCORE_SVC, TOKEN, status_cb=None)

    # Config was saved with the scoring block + custom fields.
    saved_calls = save_mock.call_args_list
    assert len(saved_calls) >= 1
    final_cfg = saved_calls[-1].args[1]
    assert final_cfg["scoring"]["enabled"] is True
    assert final_cfg["scoring"]["scoring_service_id"] == SCORE_SVC
    field_names = [cf["name"] for cf in final_cfg["log_fields"]["custom_fields"]]
    assert "edge_score" in field_names

    # The right Fastly mutations happened.
    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/100/clone") in calls
    assert ("POST", f"/service/{LOG_SVC}/version/101/backend") in calls
    assert ("POST", f"/service/{LOG_SVC}/version/101/snippet") in calls
    assert ("GET", f"/service/{LOG_SVC}/version/101/validate") in calls
    assert ("PUT", f"/service/{LOG_SVC}/version/101/activate") in calls


def test_enable_scoring_populates_default_exclude_url_regex_on_first_enable(monkeypatch):
    """On first turn-on, enable_scoring must persist the bundled
    DEFAULT_ASSET_EXT_REGEX literal into cfg.scoring.exclude_url_regex
    so the admin UI's textarea is pre-populated with a real editable
    value instead of an empty box hiding behind a "show default" toggle."""
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
        patch.object(sso, "_deploy_wasm"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
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
        patch.object(sso, "_deploy_wasm"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN)

    saved_cfg = save_mock.call_args_list[-1].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] == r"^/(healthz|metrics)$", (
        "re-enable must preserve operator's exclude_url_regex override"
    )
    assert saved_cfg["scoring"]["enforce_status_code"] == 403, (
        "re-enable must preserve operator's enforce_status_code override"
    )


def test_enable_scoring_adds_backend_with_correct_shape(monkeypatch):
    """The scoring backend payload must use the SNI + cert hostnames that
    match the Compute service domain — required for TLS to terminate
    correctly at edgecompute.app."""
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
        patch.object(sso, "_deploy_wasm"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN)

    backend_post = next(
        c for c in fastly_mock.call_args_list if c.args[:2] == ("POST", f"/service/{LOG_SVC}/version/101/backend")
    )
    body = backend_post.args[2]
    # Fastly auto-prefixes backend names with F_, so we POST the raw name
    # and the VCL ends up referencing the F_-prefixed form.
    assert body["name"] == SCORING_BACKEND_API_NAME
    assert body["address"].endswith("session-scorer.edgecompute.app")
    assert body["use_ssl"] is True
    assert body["ssl_cert_hostname"] == body["address"]
    assert body["ssl_sni_hostname"] == body["address"]
    # Aggressive timeouts — Wasm runs in ~600µs, intra-Fastly network
    # adds ~5-20ms. 50ms gives ~2.5x typical round-trip.
    assert body["connect_timeout"] == 50
    assert body["first_byte_timeout"] == 50


def test_enable_scoring_installs_all_six_named_snippets(monkeypatch):
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
        patch.object(sso, "_deploy_wasm"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN)

    snippet_posts = [
        c for c in fastly_mock.call_args_list if c.args[:2] == ("POST", f"/service/{LOG_SVC}/version/101/snippet")
    ]
    posted_names = [c.args[2]["name"] for c in snippet_posts]
    for name in (
        SCORING_RECV_NAME,
        SCORING_PASS_NAME,
        SCORING_FETCH_NAME,
        SCORING_DELIVER_NAME,
        SCORING_MISS_NAME,
        SCORING_ENFORCE_NAME,
    ):
        assert name in posted_names


# ── enable_scoring: rollback path ─────────────────────────────────────────────


def test_enable_scoring_rolls_back_on_validation_failure(monkeypatch):
    """When validate returns non-ok, we MUST re-activate the prior version
    and revert the config changes."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 100, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/100/clone"):
            return {"number": 101}
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/101"):
            return {}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/backend"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/logging/s3"):
            return [{"name": "Fastly Object Storage Logs"}]
        # Validate FAILS
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/validate"):
            return {"status": "error", "errors": ["something is broken"]}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    save_calls = []

    def fake_save(svc_id, c):
        save_calls.append(dict(c))

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config", side_effect=fake_save),
        patch.object(sso, "ensure_scoring_service", return_value=_ensure_scoring_meta()),
        patch.object(sso, "_deploy_wasm"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.enable_scoring(LOG_SVC, TOKEN)

    # Rollback must re-activate the OLD version (100), not the failing one (101).
    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/100/activate") in calls
    assert ("PUT", f"/service/{LOG_SVC}/version/101/activate") not in calls

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
        patch.object(sso, "delete_scoring_service") as delete_mock,
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
    # Scoring block cleared from config
    assert "scoring" not in saved[-1]
    # Custom fields stripped (only user_id remains)
    final_field_names = [cf["name"] for cf in saved[-1].get("log_fields", {}).get("custom_fields", [])]
    assert final_field_names == ["user_id"]
