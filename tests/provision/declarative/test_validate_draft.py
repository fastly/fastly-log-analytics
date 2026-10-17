"""``_validate_draft`` must treat a non-ok validation status as a failure, not as valid."""

from __future__ import annotations

from unittest.mock import patch

from backend.provision.declarative import reconciler


def test_validate_draft_returns_empty_string_when_valid():
    with patch.object(reconciler.fastly_integration, "validate_version", return_value=True):
        assert reconciler._validate_draft("svc", "tok", 3) == ""


def test_validate_draft_reports_non_ok_status_without_http_error():
    with patch.object(reconciler.fastly_integration, "validate_version", return_value=False):
        error = reconciler._validate_draft("svc", "tok", 3)
    assert error
    assert "validation" in error.lower()


def test_validate_draft_returns_message_when_validation_raises():
    with patch.object(
        reconciler.fastly_integration, "validate_version", side_effect=RuntimeError("VCL validation failed: boom")
    ):
        assert "boom" in reconciler._validate_draft("svc", "tok", 3)


def test_update_local_config_goes_through_the_locked_mutate_config(tmp_path, monkeypatch):
    """Activation metadata must be written via mutate_config (cross-process lock, fresh read)."""
    import json

    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", tmp_path)
    svcconfig.save_config("svc-recon", {"service_id": "svc-recon", "other": "keep"})

    calls: list[str] = []
    real = svcconfig.mutate_config

    def spy(service_id, mutate, **kw):
        calls.append(service_id)
        return real(service_id, mutate, **kw)

    monkeypatch.setattr(svcconfig, "mutate_config", spy)
    reconciler._update_local_config("svc-recon", lambda c: c.__setitem__("last_activated_version", 7))

    assert calls == ["svc-recon"]
    saved = json.loads((tmp_path / "svc-recon.json").read_text())
    assert saved["last_activated_version"] == 7
    assert saved["other"] == "keep"


def test_update_local_config_skips_a_read_only_volume(monkeypatch):
    from backend import config as svcconfig

    def boom(*a, **k):
        raise OSError("read-only file system")

    monkeypatch.setattr(svcconfig, "mutate_config", boom)
    reconciler._update_local_config("svc-x", lambda c: None)  # must not raise
