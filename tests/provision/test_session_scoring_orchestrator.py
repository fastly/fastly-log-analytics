"""Coverage-fill tests for backend.provision.session_scoring_orchestrator.

A companion to the contract-pinning suite in
``tests/scoring/test_session_scoring_orchestrator.py``: that file pins the
happy-path Fastly call order, the rollback shape, and the per-tenant matrix
resolution. This file fills the remaining 40% — the lighter-weight
re-publish helpers (``update_recv_exclusion_regex``,
``update_enforce_status_code``), the request-secret self-heal branch on
re-enable, backend drift / DELETE / 404-tolerance helpers, status_cb
fan-out, and the disable_scoring failure-mid-VCL rollback.

All Fastly traffic is mocked via the same pattern the sibling suite uses:
``patch.object(sso, 'fastly', mock)`` plus a parallel
``patch('backend.core.fastly.service.fastly', mock)`` so the helpers in
``backend.core.fastly.service`` (``get_active_version``,
``ensure_vcl_snippet``, ``list_vcl_snippets``) also route through the
mock.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from backend.provision import session_scoring_orchestrator as sso
from backend.provision.session_scoring_vcl import (
    SCORING_BACKEND_API_NAME,
    SCORING_RECV_NAME,
)

LOG_SVC = "TestLogSvc456"
SCORE_SVC = "ScoringSvcZ"
TOKEN = "FAKE_TOKEN"
KEYS_STORE = "KEYS_STORE_ID"
CFG_STORE = "CFG_STORE_ID"


def _scoring_meta(*, with_secret: bool = True) -> dict:
    meta = {
        "scoring_service_id": SCORE_SVC,
        "scoring_service_name": f"Session Scoring Service for {LOG_SVC}",
        "scoring_domain": f"fos-{LOG_SVC.lower()}-session-scorer.edgecompute.app",
        "scoring_keys_store_id": KEYS_STORE,
        "scoring_config_store_id": CFG_STORE,
        "aes_key_hex": "ab" * 32,
    }
    if with_secret:
        meta["request_secret"] = "secret_" + "y" * 56
    return meta


# ── _add_scoring_backend: drift detection + PUT-update path ─────────────────


def test_add_scoring_backend_no_op_when_matching_settings_present():
    """A backend that already exists with the right shape should be left
    untouched (no PUT) — this is the idempotent re-run case."""
    domain = "fos-x-session-scorer.edgecompute.app"
    existing = {
        "name": SCORING_BACKEND_API_NAME,
        "address": domain,
        "port": 443,
        "use_ssl": True,
        "ssl_cert_hostname": domain,
        "ssl_sni_hostname": domain,
        "override_host": domain,
        "ssl_check_cert": False,
        "auto_loadbalance": False,
        # Must match the orchestrator's current payload (first_byte lowered
        # 200→100 on 2026-06-22) or the drift check below sees a diff and PUTs.
        "connect_timeout": 100,
        "first_byte_timeout": 100,
        "between_bytes_timeout": 200,
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET" and path.endswith("/backend"):
            return [existing]
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch.object(sso, "fastly", fastly_mock):
        sso._add_scoring_backend(LOG_SVC, 50, domain, TOKEN)

    methods = [c.args[0] for c in fastly_mock.call_args_list]
    assert "POST" not in methods
    assert "PUT" not in methods


def test_add_scoring_backend_updates_on_drift():
    """When the stored backend has stale settings (e.g. old timeouts) we
    PUT-update it rather than no-op or POST a duplicate."""
    domain = "fos-x-session-scorer.edgecompute.app"
    drifted = {
        "name": SCORING_BACKEND_API_NAME,
        "address": domain,
        "connect_timeout": 5000,  # differs from the 100 in code → drift
        "first_byte_timeout": 5000,
        "between_bytes_timeout": 5000,
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET" and path.endswith("/backend"):
            return [drifted]
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch.object(sso, "fastly", fastly_mock):
        sso._add_scoring_backend(LOG_SVC, 50, domain, TOKEN)

    puts = [c for c in fastly_mock.call_args_list if c.args[0] == "PUT"]
    assert len(puts) == 1
    assert puts[0].args[1].endswith(f"/backend/{SCORING_BACKEND_API_NAME}")
    # POST path must NOT have fired — that'd 409 against the existing entry.
    assert not any(c.args[0] == "POST" for c in fastly_mock.call_args_list)


def test_add_scoring_backend_swallows_runtime_error_on_probe():
    """If the GET /backend probe blows up (transient API hiccup), fall
    through to the POST path rather than crashing the whole enable."""
    domain = "fos-x-session-scorer.edgecompute.app"

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET" and path.endswith("/backend"):
            raise RuntimeError("transient 500 from Fastly")
        return {"ok": True}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch.object(sso, "fastly", fastly_mock):
        sso._add_scoring_backend(LOG_SVC, 50, domain, TOKEN)

    # POST fired — the probe failure shouldn't block the create.
    assert any(c.args[0] == "POST" and c.args[1].endswith("/backend") for c in fastly_mock.call_args_list)


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
        # Should not raise.
        sso._remove_scoring_backend(LOG_SVC, 200, TOKEN)


def test_remove_scoring_backend_propagates_non_404():
    """Non-404 failures must surface — a 500 here means the API is broken
    and the operator should see the failure, not have it silently
    swallowed."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "DELETE":
            raise RuntimeError("500 internal")
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with patch.object(sso, "fastly", fastly_mock):
        with pytest.raises(RuntimeError, match="500 internal"):
            sso._remove_scoring_backend(LOG_SVC, 200, TOKEN)


def test_remove_scoring_snippets_skips_absent_names():
    """When only some scoring snippets are present, DELETE only fires for
    those — avoids spamming the API with 404s."""

    def side_effect(method, path, body=None, token=None, **kwargs):
        if method == "GET" and path.endswith("/snippet"):
            # Only the recv snippet is present.
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
        sso._remove_scoring_snippets(LOG_SVC, 200, TOKEN)  # must not raise


# ── enable_scoring: missing-config + secret heal branches ───────────────────


def test_enable_scoring_raises_when_no_config():
    """A logging service ID we don't recognise should fail fast with a
    clear message instead of crashing later in ensure_scoring_service."""
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.enable_scoring("UnknownSvc", TOKEN)


def test_enable_scoring_heals_missing_request_secret_via_post(monkeypatch):
    """When ensure_scoring_service comes back without a request_secret AND
    the prior config block has none either, enable_scoring must mint a
    fresh one and POST it into the keys ConfigStore — without that
    self-heal, every snippet body would lack the shield-auth value and
    the activation would 422 at validate.

    Pinned because the failure mode here is silent: the validate step
    catches it, rollback fires, and the operator sees "scoring failed"
    with no hint that the request_secret store was the cause."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    meta = _scoring_meta(with_secret=False)

    fastly_mock = _happy_path_fastly_mock_with_post_secret()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "ensure_scoring_service", return_value=meta),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN)

    # The healing POST hit the keys store with item_key=request_secret.
    secret_posts = [
        c
        for c in fastly_mock.call_args_list
        if c.args[0] == "POST" and c.args[1] == f"/resources/stores/config/{KEYS_STORE}/item"
    ]
    assert len(secret_posts) == 1
    assert secret_posts[0].args[2]["item_key"] == "request_secret"
    # 64-hex-char value (token_hex(32) → 64 chars).
    assert len(secret_posts[0].args[2]["item_value"]) == 64


def test_enable_scoring_heals_request_secret_via_patch_when_post_409s(monkeypatch):
    """If the keys store already has a stale request_secret entry, POST
    409s and we must fall through to PATCH — covers the upgrade path
    where the entry exists but the value was lost from cfg."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    meta = _scoring_meta(with_secret=False)

    happy = _happy_path_fastly_mock_with_post_secret(post_secret_409=True)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "ensure_scoring_service", return_value=meta),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", happy),
        patch("backend.core.fastly.service.fastly", happy),
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
    are missing scoring_keys_store_id, we have nowhere to write the
    minted secret — must raise rather than silently lose it."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    meta = _scoring_meta(with_secret=False)
    meta.pop("scoring_keys_store_id")  # also absent from cfg.scoring

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "ensure_scoring_service", return_value=meta),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
    ):
        with pytest.raises(RuntimeError, match="scoring_keys_store_id"):
            sso.enable_scoring(LOG_SVC, TOKEN)


def test_enable_scoring_status_cb_fires_on_each_stage():
    """The SSE bridge in routers/session_scoring.py expects status_cb to
    be called at each major stage boundary so the UI's progress bar
    advances. Pin the contract on those call sites at minimum: enabling,
    cloning, snippets, format, validate, activate, done."""
    cfg = {
        "service_id": LOG_SVC,
        "log_fields": {"custom_fields": []},
        "provisioning": {"endpoint_name": "Fastly Object Storage Logs"},
    }
    fastly_mock = _happy_path_fastly_mock()
    cb = MagicMock()

    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "ensure_scoring_service", return_value=_scoring_meta()),
        patch.object(sso, "_deploy_wasm_package"),
        patch.object(sso, "_write_matrix_to_kv"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        sso.enable_scoring(LOG_SVC, TOKEN, status_cb=cb)

    msgs = [c.args[0] for c in cb.call_args_list]
    # Combined sanity: at least the enable/clone/snippets/format/validate/
    # activate/done beats fired.
    joined = " ".join(msgs).lower()
    for needle in ("enabling", "cloning", "snippets", "log format", "validating", "activating", "done"):
        assert needle in joined, f"status_cb missed beat: {needle!r} (got {msgs!r})"


# ── disable_scoring: failure-mid-VCL rollback ────────────────────────────────


def test_disable_scoring_rolls_back_when_validate_fails():
    """If validate fails during disable, the prior active version must
    be re-activated so the customer's service doesn't get stranded on a
    broken draft. delete_scoring_service MUST NOT run — that'd nuke the
    Compute service even though the VCL still references it."""
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
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
        patch.object(sso, "delete_scoring_service") as delete_mock,
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.disable_scoring(LOG_SVC, TOKEN)

    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    # Rollback re-activated v200 (the working one).
    assert ("PUT", f"/service/{LOG_SVC}/version/200/activate") in calls
    # The broken draft (v201) was never activated.
    assert ("PUT", f"/service/{LOG_SVC}/version/201/activate") not in calls
    # The Compute service was NOT torn down — VCL still references it.
    delete_mock.assert_not_called()


# ── update_recv_exclusion_regex ──────────────────────────────────────────────


def test_update_recv_exclusion_regex_requires_scoring_enabled():
    """Refuse to repaint the recv snippet if scoring isn't on yet — the
    operator's flow is: enable_scoring first, THEN tune the regex."""
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": False}}
    with patch.object(sso.svcconfig, "load_config", return_value=cfg):
        with pytest.raises(RuntimeError, match="not enabled"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/healthz$")


def test_update_recv_exclusion_regex_requires_request_secret():
    """Re-publishing the recv snippet without the request_secret would
    emit invalid VCL (the snippet bakes the secret into its header
    check). Fail loudly here instead of letting Fastly's validate trip
    on a confusing error."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True},  # no request_secret
    }
    with patch.object(sso.svcconfig, "load_config", return_value=cfg):
        with pytest.raises(RuntimeError, match="request_secret"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/healthz$")


def test_update_recv_exclusion_regex_happy_path_persists_and_activates():
    """Happy path: persist the override, clone the active version, swap
    just the recv snippet, validate, activate. Empty/whitespace regex
    persists as None (canonical "use default") in cfg."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 300, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/300/clone"):
            return {"number": 301}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/301/snippet"):
            return []  # ensure_vcl_snippet idempotency probe
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/301/validate"):
            return {"status": "ok"}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config", side_effect=save_mock),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex=r"^/(healthz|metrics)$")

    assert result["logging_service_active_version"] == 301
    assert result["is_default"] is False
    assert result["effective_regex"] == r"^/(healthz|metrics)$"
    # Persisted into cfg.
    saved_cfg = save_mock.call_args_list[0].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] == r"^/(healthz|metrics)$"
    # The recv snippet POST/PUT happened on v301.
    snippet_writes = [
        c
        for c in fastly_mock.call_args_list
        if c.args[0] in ("POST", "PUT") and "/snippet" in c.args[1] and "/version/301" in c.args[1]
    ]
    assert snippet_writes, "expected at least one snippet write on the draft"
    # v301 was activated.
    assert ("PUT", f"/service/{LOG_SVC}/version/301/activate") in [
        (c.args[0], c.args[1]) for c in fastly_mock.call_args_list
    ]


def test_update_recv_exclusion_regex_empty_string_persists_as_none():
    """Empty/whitespace input from the UI is the "use default" signal;
    persist it as None so a future default change auto-picks up."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
            "exclude_url_regex": r"^/old$",  # prior override
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 300, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/300/clone"):
            return {"number": 301}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/301/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/301/validate"):
            return {"status": "ok"}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config", side_effect=save_mock),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="   ")

    saved_cfg = save_mock.call_args_list[0].args[1]
    assert saved_cfg["scoring"]["exclude_url_regex"] is None
    assert result["is_default"] is True


def test_update_recv_exclusion_regex_rolls_back_on_validate_failure():
    """When validate trips on the new recv body, re-activate the prior
    version so the service isn't left on a broken draft."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 300, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/300/clone"):
            return {"number": 301}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/301/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/301/validate"):
            return {"status": "error", "errors": ["bad regex"]}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="bad")

    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/300/activate") in calls
    assert ("PUT", f"/service/{LOG_SVC}/version/301/activate") not in calls


# ── update_enforce_status_code ───────────────────────────────────────────────


def test_update_enforce_status_code_requires_scoring_enabled():
    cfg = {"service_id": LOG_SVC, "scoring": {"enabled": False}}
    with patch.object(sso.svcconfig, "load_config", return_value=cfg):
        with pytest.raises(RuntimeError, match="not enabled"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=418)


def test_update_enforce_status_code_requires_request_secret():
    """The enforce snippet bakes the request_secret into its shield-auth
    boundary check — re-publishing without it would emit invalid VCL.
    Pinned because the previous version of this function only required
    the secret for the recv path."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {"enabled": True},  # no request_secret
    }
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
    ):
        with pytest.raises(RuntimeError, match="request_secret"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=418)


def test_update_enforce_status_code_happy_path():
    """Non-default code: persist, clone, swap the enforce snippet,
    validate, activate. result.is_default reflects whether the resolved
    code is the bundled default 429."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 400, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/400/clone"):
            return {"number": 401}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/401/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/401/validate"):
            return {"status": "ok"}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config", side_effect=save_mock),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=418)

    assert result["effective_status_code"] == 418
    assert result["is_default"] is False
    assert result["logging_service_active_version"] == 401
    # Persisted override stays in cfg (not None — it's non-default).
    saved_cfg = save_mock.call_args_list[0].args[1]
    assert saved_cfg["scoring"]["enforce_status_code"] == 418
    # Enforce snippet write happened on the draft.
    snippet_writes = [
        c
        for c in fastly_mock.call_args_list
        if c.args[0] in ("POST", "PUT") and "/snippet" in c.args[1] and "/version/401" in c.args[1]
    ]
    assert snippet_writes


def test_update_enforce_status_code_none_persists_as_none_and_uses_default():
    """new_status_code=None → resolve to bundled default 429 →
    is_default=True → persist as None in cfg (canonical sentinel)."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
            "enforce_status_code": 418,  # prior non-default override
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 400, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/400/clone"):
            return {"number": 401}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/401/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/401/validate"):
            return {"status": "ok"}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    save_mock = MagicMock()
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config", side_effect=save_mock),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        result = sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=None)

    assert result["is_default"] is True
    assert result["effective_status_code"] == 429
    saved_cfg = save_mock.call_args_list[0].args[1]
    # None sentinel — NOT 429 — so a future default change auto-picks up.
    assert saved_cfg["scoring"]["enforce_status_code"] is None


def test_update_enforce_status_code_rolls_back_on_validate_failure():
    """Validation failure during enforce repaint must re-activate the
    prior version."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return [{"number": 400, "active": True}]
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/400/clone"):
            return {"number": 401}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/401/snippet"):
            return []
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/401/validate"):
            return {"status": "error", "errors": ["bad enforce"]}
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        with pytest.raises(RuntimeError, match="Validation failed"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=403)

    calls = [(c.args[0], c.args[1]) for c in fastly_mock.call_args_list]
    assert ("PUT", f"/service/{LOG_SVC}/version/400/activate") in calls
    assert ("PUT", f"/service/{LOG_SVC}/version/401/activate") not in calls


def test_update_recv_exclusion_regex_raises_when_no_config():
    """Defensive: unknown service id fails fast."""
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/$")


def test_update_enforce_status_code_raises_when_no_config():
    """Defensive: unknown service id fails fast."""
    with patch.object(sso.svcconfig, "load_config", return_value=None):
        with pytest.raises(RuntimeError, match="No config found"):
            sso.update_enforce_status_code(LOG_SVC, TOKEN, new_status_code=403)


def test_update_recv_exclusion_regex_raises_when_no_active_version():
    """If the logging service has no active version, fail before mutating
    anything — there's nothing to clone."""
    cfg = {
        "service_id": LOG_SVC,
        "scoring": {
            "enabled": True,
            "request_secret": "s" * 64,
        },
    }

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return []  # no versions → get_active_version returns None
        return {}

    fastly_mock = MagicMock(side_effect=side_effect)
    with (
        patch.object(sso.svcconfig, "load_config", return_value=cfg),
        patch.object(sso.svcconfig, "save_config"),
        patch.object(sso, "fastly", fastly_mock),
        patch("backend.core.fastly.service.fastly", fastly_mock),
    ):
        with pytest.raises(RuntimeError, match="no active version"):
            sso.update_recv_exclusion_regex(LOG_SVC, TOKEN, new_regex="^/$")


# ── Helpers ─────────────────────────────────────────────────────────────────


def _happy_path_fastly_mock():
    """Same shape as the sibling-file helper but defined locally so the
    two suites don't have to share private fixture imports."""
    versions = [{"number": 100, "active": True}]
    s3_endpoints = [{"name": "Fastly Object Storage Logs"}]
    snippets = []
    backends = []

    def side_effect(method, path, body=None, token=None, **kwargs):
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version"):
            return versions
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/100/clone"):
            return {"number": 101}
        if (method, path) == ("PUT", f"/service/{LOG_SVC}/version/101"):
            return {}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/backend"):
            return backends
        if (method, path) == ("POST", f"/service/{LOG_SVC}/version/101/backend"):
            backends.append({"name": body["name"]})
            return {"name": body["name"]}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/snippet"):
            return snippets
        if (method, path) == ("POST", f"/service/{LOG_SVC}/version/101/snippet"):
            snippets.append({"name": body["name"]})
            return snippets[-1]
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/logging/s3"):
            return s3_endpoints
        if method == "PUT" and "/logging/s3/" in path:
            return {}
        if (method, path) == ("GET", f"/service/{LOG_SVC}/version/101/validate"):
            return {"status": "ok"}
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
        if (method, path) == ("POST", f"/resources/stores/config/{KEYS_STORE}/item"):
            if post_secret_409:
                raise RuntimeError("409 Conflict — item already exists")
            return {"item_key": body["item_key"]}
        if method == "PATCH" and path == f"/resources/stores/config/{KEYS_STORE}/item/request_secret":
            return {"ok": True}
        return base_side_effect(method, path, body, token=token, **kwargs)

    return MagicMock(side_effect=side_effect)
