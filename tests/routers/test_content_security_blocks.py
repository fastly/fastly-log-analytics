"""Streaming → Content Security blocker settings (URL and auth header) and how they are used when publishing."""

from __future__ import annotations

import json

import httpx
import pytest

from backend.utils import blocklist_client
from tests.conftest import MOCK_SERVICE_ID

_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}
_URL = "https://blocklist.example.com/hook"


class _Recorder:
    """MockTransport handler that records every call and replies with ``status``."""

    def __init__(self, status: int = 200):
        self.status = status
        self.calls: list[tuple[str, str, dict]] = []
        self.headers: list[httpx.Headers] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append((request.method, str(request.url), json.loads(request.content or b"{}")))
        self.headers.append(request.headers)
        return httpx.Response(self.status, headers={"location": "https://elsewhere.example.com/"})


@pytest.fixture
def remote(monkeypatch):
    rec = _Recorder()
    monkeypatch.setattr(blocklist_client, "_TRANSPORT", httpx.MockTransport(rec))
    return rec


@pytest.fixture
def configured(client):
    resp = client.put("/api/admin/content-security/settings", json={"blocklist_url": _URL})
    assert resp.status_code == 200
    return _URL


def _publish_change(client):
    """Any saved change triggers a publish to the blocker."""
    return client.post("/api/admin/content-security/policies", headers=_HEADERS, json={"name": "Test"})


def _audit_events():
    from backend.core.metadata.state import list_audit

    return [(e["event_type"], json.loads(e["details"])) for e in list_audit(MOCK_SERVICE_ID)]


# ── Settings ─────────────────────────────────────────────────────────────────


def test_settings_default_empty(client):
    resp = client.get("/api/admin/content-security/settings")
    assert resp.status_code == 200
    assert resp.json()["blocklist_url"] == ""


def test_settings_round_trip_and_clear(client):
    resp = client.put("/api/admin/content-security/settings", json={"blocklist_url": f"  {_URL}  "})
    assert resp.json()["blocklist_url"] == _URL
    assert client.get("/api/admin/content-security/settings").json()["blocklist_url"] == _URL

    resp = client.put("/api/admin/content-security/settings", json={"blocklist_url": ""})
    assert resp.json()["blocklist_url"] == ""


@pytest.mark.parametrize(
    "url",
    ["ftp://example.com/x", "not a url", "/relative/path", "https://user:pass@example.com/hook"],
)
def test_settings_rejects_invalid_url(client, url):
    resp = client.put("/api/admin/content-security/settings", json={"blocklist_url": url})
    assert resp.status_code == 422
    assert client.get("/api/admin/content-security/settings").json()["blocklist_url"] == ""


# ── Auth header ──────────────────────────────────────────────────────────────

_SECRET = "Bearer s3cr3t-token-value"


def _put(client, **body):
    return client.put("/api/admin/content-security/settings", json={"blocklist_url": _URL, **body})


def _stored_header():
    from backend import config as svcconfig

    return svcconfig.load_content_security_config()["blocklist_auth_header"]


def test_auth_header_value_is_never_returned(client):
    resp = _put(client, auth_header=f"  Authorization :  {_SECRET} ")
    assert resp.status_code == 200
    assert resp.json()["auth_header_name"] == "Authorization"
    assert _SECRET not in resp.text
    get = client.get("/api/admin/content-security/settings")
    assert get.json()["auth_header_name"] == "Authorization"
    assert _SECRET not in get.text
    # Stored normalized.
    assert _stored_header() == f"Authorization: {_SECRET}"


def test_auth_header_kept_when_omitted_and_removed_with_empty_string(client):
    _put(client, auth_header=f"Authorization: {_SECRET}")
    # Same server, header omitted: kept.
    assert _put(client).json()["auth_header_name"] == "Authorization"
    assert _put(client, blocklist_url=_URL + "/v2").json()["auth_header_name"] == "Authorization"
    # Explicit "" removes it.
    assert _put(client, auth_header="").json()["auth_header_name"] is None
    assert _stored_header() == ""


def test_auth_header_not_carried_to_a_different_server(client):
    _put(client, auth_header=f"Authorization: {_SECRET}")
    resp = _put(client, blocklist_url="https://other.example.com/hook")
    assert resp.status_code == 422
    assert resp.json()["detail"]["error"] == "auth_header_reentry_required"
    # Nothing changed.
    assert client.get("/api/admin/content-security/settings").json()["blocklist_url"] == _URL

    # Re-entering (or removing) it with the new URL is fine.
    ok = _put(client, blocklist_url="https://other.example.com/hook", auth_header="X-Api-Key: abc")
    assert ok.status_code == 200
    assert ok.json()["auth_header_name"] == "X-Api-Key"


def test_clearing_url_drops_auth_header(client):
    _put(client, auth_header=f"Authorization: {_SECRET}")
    resp = client.put("/api/admin/content-security/settings", json={"blocklist_url": ""})
    assert resp.json()["auth_header_name"] is None
    assert _stored_header() == ""


@pytest.mark.parametrize(
    "header",
    [
        "Bearer no-colon",
        "Authorization:",
        ": value",
        "Bad Name: value",
        "Authorization: Bearer x\r\nX-Injected: 1",
        "Content-Type: text/plain",
        "Host: evil.example.com",
        "Authorization: " + "a" * 5000,
    ],
    ids=["no_colon", "no_value", "no_name", "space_in_name", "crlf", "content_type", "host", "too_long"],
)
def test_invalid_auth_header_rejected_without_echoing_it(client, header):
    resp = _put(client, auth_header=header)
    assert resp.status_code == 422
    assert resp.json()["detail"]["error"] == "invalid_header"
    if len(header) > 20:
        assert header not in resp.text
    assert _stored_header() == ""


def test_publishes_send_the_auth_header(client, remote):
    _put(client, auth_header=f"Authorization: {_SECRET}")
    policy = _publish_change(client).json()["policy"]
    client.put(f"/api/admin/content-security/policies/{policy['id']}", headers=_HEADERS, json={"name": "Renamed"})
    assert [h.get("authorization") for h in remote.headers] == [_SECRET, _SECRET]
    # The body's content type is still ours.
    assert remote.headers[0]["content-type"] == "application/json"
    assert [m for m, _, _ in remote.calls] == ["PUT", "PUT"]


def test_no_auth_header_sent_when_none_configured(client, remote, configured):
    _publish_change(client)
    assert "authorization" not in remote.headers[0]


def test_auth_header_never_logged(client, remote, caplog):
    import logging

    caplog.set_level(logging.DEBUG)
    _put(client, auth_header=f"Authorization: {_SECRET}")
    remote.status = 500
    _publish_change(client)
    assert _SECRET not in caplog.text
    assert _SECRET not in json.dumps(_audit_events())


# ── Access control ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "path",
    [
        "/api/admin/content-security/settings",
        "/api/admin/content-security/policies",
        "/api/admin/content-security/policies/abc/rules",
        "/api/admin/content-security/assignments",
        "/api/admin/content-security/assignments/abc",
        "/api/admin/content-security/effective",
        "/api/admin/content-security/live-rule",
        "/api/admin/content-security/publish",
        "/api/admin/content-security/sync-status",
    ],
)
def test_content_security_routes_blocked_for_analysts_in_middleware(path):
    from backend.utils.remote_access import _is_blocked_path

    assert _is_blocked_path(path)


def test_content_security_routes_reject_analyst_sessions(client, remote, configured):
    """Second layer behind the middleware: require_admin on every route."""
    from fastapi import HTTPException

    from backend.deps import require_admin
    from backend.main import app

    def deny():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = deny
    try:
        assert _publish_change(client).status_code == 403
        assert client.get("/api/admin/content-security/policies", headers=_HEADERS).status_code == 403
        assert client.post("/api/admin/content-security/publish", headers=_HEADERS).status_code == 403
        assert client.get("/api/admin/content-security/settings").status_code == 403
    finally:
        app.dependency_overrides.pop(require_admin, None)
    assert remote.calls == []
