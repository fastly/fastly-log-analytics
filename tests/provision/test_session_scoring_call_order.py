from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from backend.provision import fastly_api
from backend.provision import session_scoring_orchestrator as sso

# ── From test_session_scoring_call_order.py ──

"""Fastly call order for the session-scoring clone-validate-activate sites, pinned across the draft_version migration."""


LOG_SVC = "TestLogSvc456"


SCORE_SVC = "ScoringSvcZ"


TOKEN = "FAKE_TOKEN"


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


# ── From test_fastly_api_call_order.py ──

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


# ── Merged from test_cdn_service_config_writes.py ──

import json
from types import SimpleNamespace

import pytest

from backend import config as svcconfig

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


# ── Merged from test_update_logging_endpoint_config_merge.py ──


import pytest

from backend.provision.declarative.reconciler import ReconciliationResult
from tests._helpers.config_fakes import config_lock_held as _lock_held
from tests._helpers.config_fakes import read_persisted_config as _persisted

_MERGE_SID = "logsvc1"


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


# ── Merged from test_write_service_config_merge.py ──

from backend.provision import orchestrator

_MERGE_SID = "logsvc1"


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


# ── Merged from test_system_fields.py ──

from backend.provision.cmcd_fields import _CMCD_FIELD_NAMES
from backend.provision.session_scoring_orchestrator import _SCORING_FIELD_NAMES
from backend.provision.system_fields import (
    reconcile_cfg_system_custom_fields,
    reconcile_system_custom_fields,
    system_feature_flags,
)

_USER_FIELD = {"name": "my_custom", "duckdb_type": "VARCHAR", "enabled": True}


def _names(fields: list[dict]) -> set[str]:
    return {cf["name"] for cf in fields}


# ── system_feature_flags ─────────────────────────────────────────────────────


def test_flags_read_both_features():
    assert system_feature_flags({}) == (False, False)
    assert system_feature_flags({"cmcd": {"enabled": True}}) == (False, True)
    assert system_feature_flags({"scoring": {"enabled": True}}) == (True, False)
    assert system_feature_flags({"scoring": {"enabled": True}, "cmcd": {"enabled": True}}) == (True, True)


def test_flags_tolerate_null_blocks():
    """A config that carries ``cmcd: null`` must not raise."""
    assert system_feature_flags({"cmcd": None, "scoring": None}) == (False, False)


# ── reconcile_system_custom_fields ───────────────────────────────────────────


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


# ── reconcile_cfg_system_custom_fields ───────────────────────────────────────


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
