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


def _remove_endpoint(fake: _Fastly):
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=["MyEndpoint"]),
        patch("backend.provision.fastly_api.fastly", side_effect=fake),
    ):
        return fastly_api.remove_logging_endpoint(SID, "MyEndpoint", "tok")


def test_remove_endpoint_comment_is_put_between_validate_and_activate():
    fake = _Fastly()
    _remove_endpoint(fake)
    assert _order(fake.calls) == ["clone", "delete", "validate", "comment", "activate:11"]


def test_remove_endpoint_keeps_expect_empty_on_deletes_and_not_on_activate():
    fake = _Fastly()
    _remove_endpoint(fake)
    delete_kwargs = [kw for (m, p), kw in zip(fake.calls, fake.kwargs) if m == "DELETE"]
    assert delete_kwargs and all(kw.get("expect_empty") is True for kw in delete_kwargs)
    activate_kwargs = [kw for (m, p), kw in zip(fake.calls, fake.kwargs) if p.endswith("/version/11/activate")]
    assert activate_kwargs == [{"token": "tok"}]


def test_remove_endpoint_validation_failure_reactivates_previous_version_without_comment():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _remove_endpoint(fake)
    assert _order(fake.calls) == ["clone", "delete", "validate", "activate:10"]


def test_remove_endpoint_edit_failure_skips_validate_and_rolls_back():
    fake = _Fastly(fail_on=lambda m, p: m == "DELETE" and "/logging/s3/" in p)
    with pytest.raises(RuntimeError, match="edit rejected"):
        _remove_endpoint(fake)
    assert _order(fake.calls) == ["clone", "delete", "activate:10"]


def test_remove_endpoint_failed_rollback_is_logged_not_masked(caplog):
    fake = _Fastly(validate_ok=False, rollback_raises=True)
    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(RuntimeError, match="Validation failed"):
            _remove_endpoint(fake)
    assert any("rollback activation of version 10 failed" in r.getMessage() for r in caplog.records)
