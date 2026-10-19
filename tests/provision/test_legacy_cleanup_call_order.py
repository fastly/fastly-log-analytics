"""cleanup_legacy_logging_endpoints keeps its Fastly call order across the draft_version migration."""

from __future__ import annotations

from unittest.mock import patch

from backend.provision import legacy_endpoint_cleanup as lec

SID = "svc"
ENDPOINTS = [{"name": "Fastly Object Storage Logs"}, {"name": "Fastly RUM Logs"}, {"name": "keep"}]


def _fake(calls, *, validate_ok=True):
    def _call(method, path, body=None, **kwargs):
        calls.append((method, path))
        if method == "GET" and path.endswith("/logging/s3"):
            return ENDPOINTS
        if path.endswith("/clone"):
            return {"number": 8}
        if path.endswith("/validate"):
            return {"status": "ok"} if validate_ok else {"status": "error", "errors": ["nope"]}
        return {}

    return _call


def test_success_order_is_list_clone_deletes_validate_activate():
    calls: list[tuple[str, str]] = []
    with patch.object(lec, "get_active_version", return_value=5), patch.object(lec, "fastly", side_effect=_fake(calls)):
        assert lec.cleanup_legacy_logging_endpoints(SID, "tok") is True
    assert calls == [
        ("GET", f"/service/{SID}/version/5/logging/s3"),
        ("PUT", f"/service/{SID}/version/5/clone"),
        ("DELETE", f"/service/{SID}/version/8/logging/s3/Fastly%20Object%20Storage%20Logs"),
        ("DELETE", f"/service/{SID}/version/8/logging/s3/Fastly%20RUM%20Logs"),
        ("GET", f"/service/{SID}/version/8/validate"),
        ("PUT", f"/service/{SID}/version/8/activate"),
    ]


def test_failed_validation_never_activates_and_does_not_touch_the_old_version():
    calls: list[tuple[str, str]] = []
    msgs: list[str] = []
    with (
        patch.object(lec, "get_active_version", return_value=5),
        patch.object(lec, "fastly", side_effect=_fake(calls, validate_ok=False)),
    ):
        assert lec.cleanup_legacy_logging_endpoints(SID, "tok", status_cb=msgs.append) is False
    assert not any(p.endswith("/activate") for _, p in calls)
    assert any("Legacy endpoint cleanup warning" in m and "Validation failed" in m for m in msgs)
