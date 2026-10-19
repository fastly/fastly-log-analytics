"""enable_cmcd must keep its exact Fastly call order across the draft_version migration."""

from __future__ import annotations

import pytest

from tests.provision.test_cmcd_orchestrator import _FastlyRecorder, _run_enable


def _order(calls):
    out = []
    for method, path in calls:
        if path.endswith("/clone"):
            out.append("clone")
        elif path.endswith("/validate"):
            out.append("validate")
        elif path.endswith("/activate"):
            out.append("activate:" + path.split("/version/")[1].split("/")[0])
        elif "/logging/s3/" in path:
            out.append("log-format")
        elif method == "PUT" and path.rstrip("/").endswith("/version/7"):
            out.append("comment")
    return out


def test_success_order_is_clone_comment_log_format_validate_activate():
    fake = _FastlyRecorder()
    _run_enable([], fake)
    assert _order(fake.calls) == ["clone", "comment", "log-format", "validate", "activate:7"]


def test_validation_failure_activates_previous_version_and_never_the_draft():
    fake = _FastlyRecorder(validate_ok=False)
    saved: list[dict] = []
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_enable(saved, fake)
    assert _order(fake.calls) == ["clone", "comment", "log-format", "validate", "activate:5"]
    assert "cmcd" not in saved[-1]


def test_status_messages_keep_their_order():
    msgs: list[str] = []
    _run_enable([], _FastlyRecorder(), status_cb=msgs.append)
    assert [m for m in msgs if m.startswith(("Cloning", "Validating", "Activating"))] == [
        "Cloning active version 5...",
        "Validating draft version 7...",
        "Activating version 7...",
    ]
    assert msgs[-1].startswith("CMCD v1 collection enabled")
