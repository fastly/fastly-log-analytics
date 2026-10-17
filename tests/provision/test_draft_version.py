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


def test_comment_before_activate_puts_comment_between_validate_and_activate():
    fake = _Fastly()
    with draft_version(SID, "t", base_version=5, call=fake, comment="c", comment_before_activate=True):
        fake.calls.append(("BODY", "mutate"))
    assert fake.calls == [
        ("PUT", f"/service/{SID}/version/5/clone"),
        ("BODY", "mutate"),
        ("GET", f"/service/{SID}/version/9/validate"),
        ("PUT", f"/service/{SID}/version/9"),
        ("PUT", f"/service/{SID}/version/9/activate"),
    ]


def test_validate_false_skips_validation_entirely():
    fake = _Fastly(validate_ok=False)
    with draft_version(SID, "t", base_version=5, call=fake, validate=False):
        pass
    assert not any(p.endswith("/validate") for _, p in fake.calls)
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/9/activate")


def test_expect_empty_activate_applies_to_the_draft_activation_only():
    seen: list[tuple[str, dict]] = []

    def call(method, path, body=None, **kwargs):
        seen.append((path, kwargs))
        if path.endswith("/clone"):
            return {"number": 9}
        return {"status": "ok"}

    with draft_version(SID, "t", base_version=5, call=call, expect_empty_activate=True):
        pass
    activate = [kw for p, kw in seen if p.endswith("/version/9/activate")]
    assert activate == [{"token": "t", "expect_empty": True}]


def test_expect_empty_not_sent_by_default():
    seen: list[dict] = []

    def call(method, path, body=None, **kwargs):
        if path.endswith("/activate"):
            seen.append(kwargs)
        if path.endswith("/clone"):
            return {"number": 9}
        return {"status": "ok"}

    with draft_version(SID, "t", base_version=5, call=call):
        pass
    assert seen == [{"token": "t"}]


def test_custom_status_messages_replace_the_defaults():
    msgs: list[str] = []
    with draft_version(
        SID,
        "t",
        base_version=5,
        call=_Fastly(),
        status_cb=msgs.append,
        validate_msg="V{ver}",
        activate_msg="A{ver}",
    ):
        pass
    assert msgs == ["V9", "A9"]


def test_on_failure_runs_before_the_rollback_call():
    fake = _Fastly()
    seen_at: list[int] = []
    with pytest.raises(ValueError, match="boom"):
        with draft_version(
            SID,
            "t",
            base_version=5,
            call=fake,
            on_failure=lambda exc: seen_at.append(len(fake.calls)),
        ):
            raise ValueError("boom")
    assert seen_at == [1]
    assert fake.calls[-1] == ("PUT", f"/service/{SID}/version/5/activate")


def test_failed_rollback_is_logged_at_warning(caplog):
    import logging

    fake = _Fastly(rollback_raises=True)
    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        with pytest.raises(ValueError, match="original"):
            with draft_version(SID, "t", base_version=5, call=fake):
                raise ValueError("original")
    assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)


def test_rollback_to_version_logs_and_swallows_runtime_error(caplog):
    import logging

    from backend.provision.draft_version import rollback_to_version

    with caplog.at_level(logging.WARNING, logger="backend.provision.draft_version"):
        rollback_to_version(SID, "t", 5, call=_Fastly(rollback_raises=True))
    assert any("rollback activation of version 5 failed" in r.getMessage() for r in caplog.records)
