"""Fastly call order for the fastly_api clone-validate-activate sites, pinned across the draft_version migration."""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from backend.provision import fastly_api

SID = "svc"


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
        elif method == "POST" and path.endswith("/logging/s3"):
            out.append("post-endpoint")
        elif method == "DELETE":
            out.append("delete")
    return out


class _Fastly:
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


_CFG = {
    "logging_service_id": SID,
    "provisioning": {"endpoint_name": "MyEndpoint"},
    "fos_region": "us-east-1",
    "fos_bucket_name": "b",
    "log_period": 60,
}


def _add_endpoint(fake: _Fastly, status_cb=None):
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=[]),
        patch("backend.provision.fastly_api.ensure_condition"),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.install_capture_snippets"),
        patch("backend.provision.fastly_api.load_log_format", return_value="format"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake),
    ):
        return fastly_api.ensure_logging_endpoint(_CFG, "AK", "SK", "tok", status_cb=status_cb)


def test_ensure_endpoint_success_order_is_clone_comment_endpoint_validate_activate():
    fake = _Fastly()
    assert _add_endpoint(fake) == 11
    assert _order(fake.calls) == ["clone", "comment", "post-endpoint", "validate", "activate:11"]
    activate_kwargs = [kw for (m, p), kw in zip(fake.calls, fake.kwargs) if p.endswith("/version/11/activate")]
    assert activate_kwargs == [{"token": "tok"}]


def test_ensure_endpoint_validation_failure_reactivates_previous_version_only():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _add_endpoint(fake)
    assert _order(fake.calls) == ["clone", "comment", "post-endpoint", "validate", "activate:10"]


def test_ensure_endpoint_edit_failure_skips_validate_and_rolls_back():
    fake = _Fastly(fail_on=lambda m, p: m == "POST" and p.endswith("/logging/s3"))
    with pytest.raises(RuntimeError, match="edit rejected"):
        _add_endpoint(fake)
    assert _order(fake.calls) == ["clone", "comment", "post-endpoint", "activate:10"]


def test_ensure_endpoint_failed_rollback_is_logged_not_masked(caplog):
    fake = _Fastly(validate_ok=False, rollback_raises=True)
    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(RuntimeError, match="Validation failed"):
            _add_endpoint(fake)
    assert any("rollback activation of version 10 failed" in r.getMessage() for r in caplog.records)


def test_ensure_endpoint_status_messages_keep_their_wording_and_order():
    msgs: list[str] = []
    _add_endpoint(_Fastly(), status_cb=msgs.append)
    assert [m for m in msgs if "alidating" in m or "ctivating" in m or "now active" in m] == [
        "⏳ Validating draft configuration...",
        "⏳ Activating version 11...",
        "✅ Version 11 is now active.",
    ]
