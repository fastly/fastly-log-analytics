"""disable_cmcd must keep its exact Fastly call order across the draft_version migration."""

from __future__ import annotations

import logging

import pytest

from tests.provision.test_cmcd_enable_call_order import _order
from tests.provision.test_cmcd_orchestrator import _cfg, _FastlyRecorder, _run_disable

_ENABLED = {"enabled": True, "mode": "query_string", "version": 1}


def test_success_order_is_clone_comment_log_format_validate_activate():
    fake = _FastlyRecorder()
    _run_disable([], fake, cfg=_cfg(cmcd=dict(_ENABLED)))
    assert _order(fake.calls) == ["clone", "comment", "log-format", "validate", "activate:7"]


def test_validation_failure_activates_previous_version_and_never_the_draft():
    fake = _FastlyRecorder(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        _run_disable([], fake, cfg=_cfg(cmcd=dict(_ENABLED)))
    assert _order(fake.calls) == ["clone", "comment", "log-format", "validate", "activate:5"]


def test_edit_failure_skips_validate_and_activates_previous_version():
    class _FormatPutFails(_FastlyRecorder):
        def __call__(self, method, path, body=None, **kwargs):
            if "/logging/s3/" in path:
                self.calls.append((method, path))
                raise RuntimeError("format put rejected")
            return super().__call__(method, path, body, **kwargs)

    fake = _FormatPutFails()
    with pytest.raises(RuntimeError, match="format put rejected"):
        _run_disable([], fake, cfg=_cfg(cmcd=dict(_ENABLED)))
    assert _order(fake.calls) == ["clone", "comment", "log-format", "activate:5"]


def test_failed_rollback_is_logged_and_does_not_mask_the_original_error(caplog):
    class _RollbackFails(_FastlyRecorder):
        def __call__(self, method, path, body=None, **kwargs):
            if path.endswith("/version/5/activate"):
                self.calls.append((method, path))
                raise RuntimeError("rollback api down")
            return super().__call__(method, path, body, **kwargs)

    fake = _RollbackFails(validate_ok=False)
    with caplog.at_level(logging.WARNING):
        with pytest.raises(RuntimeError, match="Validation failed"):
            _run_disable([], fake, cfg=_cfg(cmcd=dict(_ENABLED)))
    assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)


def test_success_status_message_is_emitted_after_activation():
    msgs: list[str] = []
    fake = _FastlyRecorder()
    _run_disable([], fake, cfg=_cfg(cmcd=dict(_ENABLED)), status_cb=msgs.append)
    assert any(m.startswith("CMCD collection disabled — version 7 active") for m in msgs)
    assert _order(fake.calls)[-1] == "activate:7"
