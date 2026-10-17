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
