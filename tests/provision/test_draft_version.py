"""draft_version: pins the clone -> comment -> body -> validate -> activate -> rollback call order."""

from __future__ import annotations

import pytest

from backend.provision.draft_version import draft_version

SID = "svc"


class _Fastly:
    def __init__(self, *, validate_ok=True, rollback_raises=False):
        self.calls: list[tuple[str, str]] = []
        self.validate_ok = validate_ok
        self.rollback_raises = rollback_raises

    def __call__(self, method, path, body=None, **kwargs):
        self.calls.append((method, path))
        if path.endswith("/clone"):
            return {"number": 9}
        if path.endswith("/validate"):
            return {"status": "ok"} if self.validate_ok else {"status": "error", "errors": ["bad"]}
        if self.rollback_raises and path == f"/service/{SID}/version/5/activate":
            raise RuntimeError("rollback failed")
        return {}


def test_success_call_order_with_comment_and_status_messages():
    fake = _Fastly()
    msgs: list[str] = []
    with draft_version(SID, "t", base_version=5, call=fake, comment="c", status_cb=msgs.append) as new_ver:
        assert new_ver == 9
        fake.calls.append(("BODY", "mutate"))
    assert fake.calls == [
        ("PUT", f"/service/{SID}/version/5/clone"),
        ("PUT", f"/service/{SID}/version/9"),
        ("BODY", "mutate"),
        ("GET", f"/service/{SID}/version/9/validate"),
        ("PUT", f"/service/{SID}/version/9/activate"),
    ]
    assert msgs == ["Validating draft version 9...", "Activating version 9..."]


def test_no_comment_skips_the_comment_put():
    fake = _Fastly()
    with draft_version(SID, "t", base_version=5, call=fake):
        pass
    assert ("PUT", f"/service/{SID}/version/9") not in fake.calls


def test_body_failure_rolls_back_to_base_and_never_validates():
    fake = _Fastly()
    with pytest.raises(ValueError, match="mutation boom"):
        with draft_version(SID, "t", base_version=5, call=fake):
            raise ValueError("mutation boom")
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")
    assert not any(p.endswith("/validate") for _, p in fake.calls)


def test_validation_failure_raises_and_rolls_back_without_activating_draft():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        with draft_version(SID, "t", base_version=5, call=fake):
            pass
    assert ("PUT", f"/service/{SID}/version/9/activate") not in fake.calls
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")


def test_rollback_disabled_makes_no_rollback_call():
    fake = _Fastly(validate_ok=False)
    with pytest.raises(RuntimeError, match="Validation failed"):
        with draft_version(SID, "t", base_version=5, call=fake, rollback=False):
            pass
    assert ("PUT", f"/service/{SID}/version/5/activate") not in fake.calls


def test_failed_rollback_does_not_mask_the_original_error():
    fake = _Fastly(rollback_raises=True)
    with pytest.raises(ValueError, match="original"):
        with draft_version(SID, "t", base_version=5, call=fake):
            raise ValueError("original")
