"""Unit tests for backend.provision.legacy_endpoint_cleanup.

cleanup_legacy_logging_endpoints() is entirely non-blocking best-effort
work run during reconciliation: every failure mode must return False (or
True only on confirmed cleanup) rather than raise, so a legacy-endpoint
hiccup never breaks the reconciliation it's embedded in. ``fastly`` and
``get_active_version`` are patched directly (module-attribute boundary,
same pattern as fastly_integration) — no real network call is made.
"""

from __future__ import annotations

from unittest.mock import patch

from backend.provision import legacy_endpoint_cleanup as lec


def test_activate_false_short_circuits_without_any_api_calls():
    with patch.object(lec, "get_active_version") as mock_get_active, patch.object(lec, "fastly") as mock_fastly:
        result = lec.cleanup_legacy_logging_endpoints("svc1", "tok", activate=False)

    assert result is False
    mock_get_active.assert_not_called()
    mock_fastly.assert_not_called()


def test_no_active_version_returns_false():
    with patch.object(lec, "get_active_version", return_value=None):
        result = lec.cleanup_legacy_logging_endpoints("svc1", "tok")
    assert result is False


def test_non_list_endpoints_response_returns_false():
    with (
        patch.object(lec, "get_active_version", return_value=5),
        patch.object(lec, "fastly", return_value={"unexpected": "shape"}),
    ):
        result = lec.cleanup_legacy_logging_endpoints("svc1", "tok")
    assert result is False


def test_no_legacy_endpoints_present_returns_false():
    endpoints = [{"name": "Fastly Log Analytics Request Logs"}, {"name": "Some Other Endpoint"}]
    with patch.object(lec, "get_active_version", return_value=5), patch.object(lec, "fastly", return_value=endpoints):
        result = lec.cleanup_legacy_logging_endpoints("svc1", "tok")
    assert result is False


def test_legacy_endpoints_removed_and_new_version_activated():
    calls: list[tuple[str, str]] = []
    endpoints = [
        {"name": "Fastly Object Storage Logs"},
        {"name": "Fastly RUM Logs"},
        {"name": "keep me"},
    ]

    def _fake(method, path, *args, **kwargs):
        calls.append((method, path))
        if method == "GET" and path.endswith("/logging/s3"):
            return endpoints
        if method == "PUT" and path.endswith("/clone"):
            return {"number": 6}
        return {}

    status_messages: list[str] = []
    with patch.object(lec, "get_active_version", return_value=5), patch.object(lec, "fastly", side_effect=_fake):
        result = lec.cleanup_legacy_logging_endpoints("svc1", "tok", status_cb=status_messages.append)

    assert result is True
    # Both legacy endpoints deleted from the cloned version, then validated + activated.
    deletes = [c for c in calls if c[0] == "DELETE"]
    assert len(deletes) == 2
    assert all("/version/6/logging/s3/" in c[1] for c in deletes)
    assert ("GET", "/service/svc1/version/6/validate") in calls
    assert ("PUT", "/service/svc1/version/6/activate") in calls
    assert any("Cleaned 2 legacy endpoints" in m for m in status_messages)


def test_per_endpoint_delete_failure_is_swallowed_and_cleanup_still_activates():
    endpoints = [{"name": "Fastly Object Storage Logs"}]

    def _fake(method, path, *args, **kwargs):
        if method == "GET" and path.endswith("/logging/s3"):
            return endpoints
        if method == "PUT" and path.endswith("/clone"):
            return {"number": 6}
        if method == "DELETE":
            raise RuntimeError("HTTP 500 upstream flaked")
        return {}

    with patch.object(lec, "get_active_version", return_value=5), patch.object(lec, "fastly", side_effect=_fake):
        result = lec.cleanup_legacy_logging_endpoints("svc1", "tok")

    # Delete failure is non-fatal: cleanup still reports success (version was
    # cloned/validated/activated even though the delete didn't take).
    assert result is True


def test_unexpected_exception_is_caught_and_reports_warning_via_status_cb():
    status_messages: list[str] = []

    def _raise(*args, **kwargs):
        raise RuntimeError("boom: network unreachable")

    with patch.object(lec, "get_active_version", side_effect=_raise):
        result = lec.cleanup_legacy_logging_endpoints("svc1", "tok", status_cb=status_messages.append)

    assert result is False
    assert any("warning" in m.lower() for m in status_messages)


def test_works_without_status_cb():
    """status_cb is optional — must not raise when omitted, on every branch."""
    with patch.object(lec, "get_active_version", return_value=None):
        assert lec.cleanup_legacy_logging_endpoints("svc1", "tok") is False

    with patch.object(lec, "get_active_version", side_effect=RuntimeError("boom")):
        assert lec.cleanup_legacy_logging_endpoints("svc1", "tok") is False
