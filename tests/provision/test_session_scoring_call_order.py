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
