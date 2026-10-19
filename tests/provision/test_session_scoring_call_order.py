"""Fastly call order for the session-scoring clone-validate-activate sites, pinned across the draft_version migration."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest

from backend.provision import session_scoring_orchestrator as sso

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
        patch.object(sso.svcconfig, "save_config"),
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


# ── disable_scoring ─────────────────────────────────────────────────────────


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


# ── teardown_scoring_resources (VCL strip) ──────────────────────────────────


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


# ── update_recv_exclusion_regex / update_enforce_status_code (_clone_swap_snippet) ──


def _swap_cfg() -> dict:
    return {"service_id": LOG_SVC, "scoring": {"enabled": True}}


def _swap_fake(**kw) -> _Fastly:
    return _Fastly(active=300, clone_number=301, **kw)


def _run_swap(fn, fake: _Fastly, **fn_kwargs):
    from contextlib import ExitStack

    with ExitStack() as stack:
        stack.enter_context(patch.object(sso.svcconfig, "load_config", return_value=_swap_cfg()))
        stack.enter_context(patch.object(sso.svcconfig, "save_config"))
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


# ── _deploy_wasm_package (Compute clone -> link -> upload -> activate) ──────


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
