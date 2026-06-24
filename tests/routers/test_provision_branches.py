"""Defensive-branch coverage for backend/routers/provision.py.

The bulk of provision-router testing lives in test_provision.py and
test_provision_teardown_auth.py. This file targets the smaller helpers
and easy error paths."""

from __future__ import annotations

import urllib.error
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.routers.provision import (
    _check_domain_available,
    _require_json_content_type,
)


def _client() -> TestClient:
    return TestClient(app)


# ── _check_domain_available: error / outcome branches ─────────────────────


def test_check_domain_available_returns_false_on_200():
    """A 200 means the domain IS registered — NOT available."""
    fake_resp = MagicMock()
    cm = MagicMock()
    cm.__enter__.return_value = fake_resp
    cm.__exit__.return_value = False
    with (
        patch("urllib.request.urlopen", return_value=cm),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        avail, reason = _check_domain_available("foo.global.ssl.fastly.net")
    assert avail is False
    assert "200" in (reason or "")


def test_check_domain_available_recognises_fastly_unprovisioned_body():
    """HTTPError with the canonical 'check that this domain has been
    added to a service' body → available=True (not yet provisioned)."""
    err = urllib.error.HTTPError(
        url="https://foo.example/",
        code=403,
        msg="Forbidden",
        hdrs=None,  # type: ignore[arg-type]
        fp=MagicMock(read=lambda: b"Please check that this domain has been added to a service."),
    )
    err.read = lambda: b"Please check that this domain has been added to a service."  # type: ignore[method-assign]
    with (
        patch("urllib.request.urlopen", side_effect=err),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        avail, reason = _check_domain_available("foo.example")
    assert avail is True
    assert reason is None


def test_check_domain_available_other_http_error_returns_false():
    err = urllib.error.HTTPError(
        url="https://foo.example/",
        code=500,
        msg="Server Error",
        hdrs=None,  # type: ignore[arg-type]
        fp=MagicMock(read=lambda: b"Internal Server Error"),
    )
    err.read = lambda: b"Internal Server Error"  # type: ignore[method-assign]
    with (
        patch("urllib.request.urlopen", side_effect=err),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        avail, reason = _check_domain_available("foo.example")
    assert avail is False
    assert "already registered" in (reason or "")


def test_check_domain_available_url_error_treated_as_available():
    """URLError (DNS / connection failure) means the domain likely
    isn't registered at Fastly — return True with a hint."""
    err = urllib.error.URLError(reason="Name or service not known")
    with (
        patch("urllib.request.urlopen", side_effect=err),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        avail, reason = _check_domain_available("nonexistent.example")
    assert avail is True
    assert "DNS/Connection" in (reason or "")


def test_check_domain_available_general_exception_returns_false():
    """Anything else (unexpected exception types) returns False with
    the exception text as reason — defensive fallback at line 48-49."""
    with (
        patch("urllib.request.urlopen", side_effect=RuntimeError("unexpected")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        avail, reason = _check_domain_available("foo.example")
    assert avail is False
    assert "unexpected" in (reason or "")


# ── _require_json_content_type: CSRF defense ──────────────────────────────


def test_require_json_content_type_accepts_application_json():
    """Plain application/json content-type passes through."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [(b"content-type", b"application/json")],
    }
    req = Request(scope, lambda: None)
    # Must not raise.
    _require_json_content_type(req)


def test_require_json_content_type_accepts_charset_suffix():
    """``application/json; charset=utf-8`` also accepted via startswith."""
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [(b"content-type", b"application/json; charset=utf-8")],
    }
    req = Request(scope, lambda: None)
    _require_json_content_type(req)


def test_require_json_content_type_rejects_text_plain():
    """CSRF defense: ``text/plain`` (browser-default for simple forms)
    must be rejected with 415 so cross-origin POSTs are forced through
    a CORS preflight."""
    from fastapi import HTTPException
    from starlette.requests import Request

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "headers": [(b"content-type", b"text/plain")],
    }
    req = Request(scope, lambda: None)
    with pytest.raises(HTTPException) as exc:
        _require_json_content_type(req)
    assert exc.value.status_code == 415


def test_require_json_content_type_rejects_missing_header():
    """No content-type header at all → also 415."""
    from fastapi import HTTPException
    from starlette.requests import Request

    scope = {"type": "http", "method": "POST", "path": "/", "headers": []}
    req = Request(scope, lambda: None)
    with pytest.raises(HTTPException) as exc:
        _require_json_content_type(req)
    assert exc.value.status_code == 415


# ── /api/provision/check-domain ────────────────────────────────────────────


def test_check_domain_rejects_empty_prefix():
    resp = _client().get("/api/provision/check-domain?prefix=")
    assert resp.status_code in (400, 422)


def test_check_domain_rejects_prefix_with_invalid_chars():
    """Prefix with non-alphanumeric (spaces, slashes, etc.) → not
    available with a clear reason string."""
    resp = _client().get("/api/provision/check-domain?prefix=foo_bar")
    body = resp.json()
    assert body["available"] is False
    assert "alphanumeric" in body["reason"]


def test_check_domain_rejects_prefix_starting_with_hyphen():
    resp = _client().get("/api/provision/check-domain?prefix=-foo")
    body = resp.json()
    assert body["available"] is False


def test_check_domain_valid_prefix_calls_check_helper():
    """Valid prefix → _check_domain_available is called. Mock the
    helper to skip the network call."""
    with patch(
        "backend.routers.provision._check_domain_available",
        return_value=(True, None),
    ) as mock_check:
        resp = _client().get("/api/provision/check-domain?prefix=foo-bar")
    assert resp.status_code == 200
    body = resp.json()
    assert body["available"] is True
    mock_check.assert_called_once()
    # The helper is called with the full domain (prefix + .global.ssl.fastly.net).
    assert "foo-bar.global.ssl.fastly.net" in mock_check.call_args[0][0]


# ── /api/provision/check-fos: ClientError code mapping ───────────────────


def test_check_fos_returns_ok_on_successful_list():
    """Happy path: list_objects_v2 succeeds → ok=True."""
    fake_client = MagicMock()
    fake_client.list_objects_v2.return_value = {"Contents": []}
    with patch("backend.core.duckdb._get_fos_client", return_value=fake_client):
        resp = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "k", "secret_key": "s"},
        )
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


@pytest.mark.parametrize(
    "code,expected_in_msg",
    [
        ("AccessDenied", "Access Denied"),
        ("InvalidAccessKeyId", "Access Denied"),
        ("SignatureDoesNotMatch", "Access Denied"),
        ("NoSuchBucket", "Bucket not found"),
        ("IllegalLocationConstraintException", "Region mismatch"),
    ],
)
def test_check_fos_maps_boto_client_error_codes_to_user_messages(code, expected_in_msg):
    """Boto3 ClientError codes get rewritten to user-friendly strings."""
    import botocore.exceptions

    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = botocore.exceptions.ClientError(
        {"Error": {"Code": code, "Message": "raw"}}, "ListObjectsV2"
    )
    with patch("backend.core.duckdb._get_fos_client", return_value=fake_client):
        resp = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "k", "secret_key": "s"},
        )
    body = resp.json()
    assert body["ok"] is False
    assert expected_in_msg in body["error"]


def test_check_fos_treats_endpoint_connection_error_as_region_mismatch():
    """A bad region → EndpointConnectionError → rewritten to a
    user-friendly 'verify the FOS Region' message (line 188-189)."""
    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = RuntimeError(
        "EndpointConnectionError: Could not connect to the endpoint URL"
    )
    with patch("backend.core.duckdb._get_fos_client", return_value=fake_client):
        resp = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "bad", "access_key": "k", "secret_key": "s"},
        )
    body = resp.json()
    assert body["ok"] is False
    assert "verify the FOS Region" in body["error"]


def test_check_fos_passes_through_unknown_errors():
    """Unknown exception types → raw error text in response. Pinned
    so the user gets SOMETHING actionable rather than a silent
    'ok: false'."""
    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = RuntimeError("weird unknown error xyz")
    with patch("backend.core.duckdb._get_fos_client", return_value=fake_client):
        resp = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "k", "secret_key": "s"},
        )
    body = resp.json()
    assert body["ok"] is False
    assert "weird unknown error xyz" in body["error"]


# ── /api/provision/validate: empty / missing fields ─────────────────────


def test_provision_validate_rejects_missing_token():
    """Token and service_id are both required."""
    resp = _client().post("/api/provision/validate", json={"token": "", "service_id": "svc"})
    assert resp.status_code == 400


def test_provision_validate_rejects_missing_service_id():
    resp = _client().post("/api/provision/validate", json={"token": "tok", "service_id": ""})
    assert resp.status_code == 400


def test_provision_validate_swallows_token_info_failure_continues_with_service_fetch():
    """If /tokens/self fails, validate logs and continues — the empty
    token_info dict in the response is the visible signal. Line 96-97."""

    def _fake_fastly(method, path, *args, **kwargs):
        if path == "/tokens/self":
            raise RuntimeError("token endpoint down")
        if path.startswith("/service/"):
            return {"name": "My Service"}
        return {}

    with patch("backend.core.fastly.client.fastly", side_effect=_fake_fastly):
        resp = _client().post(
            "/api/provision/validate",
            json={"token": "tok", "service_id": "svc-abc"},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["service_name"] == "My Service"
    assert body["token_info"] == {}  # empty because /tokens/self failed
