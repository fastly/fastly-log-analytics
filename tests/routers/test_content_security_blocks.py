"""Streaming → Content Security blocklist: settings + block/unblock via the operator's HTTP function."""

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


def _cond(category, value):
    return {"category": category, "value": value}


def _post(client, conditions, **extra):
    return client.post("/api/admin/content-security/blocks", headers=_HEADERS, json={"conditions": conditions, **extra})


def _block(client, category="country", value="CN"):
    """The common single-condition block."""
    return _post(client, [_cond(category, value)])


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


def test_block_and_unblock_send_the_auth_header(client, remote):
    _put(client, auth_header=f"Authorization: {_SECRET}")
    block = _block(client).json()
    client.delete(f"/api/admin/content-security/blocks/{block['id']}", headers=_HEADERS)
    assert [h.get("authorization") for h in remote.headers] == [_SECRET, _SECRET]
    # The body's content type is still ours.
    assert remote.headers[0]["content-type"] == "application/json"


def test_no_auth_header_sent_when_none_configured(client, remote, configured):
    _block(client)
    assert "authorization" not in remote.headers[0]


def test_auth_header_never_logged(client, remote, caplog):
    import logging

    caplog.set_level(logging.DEBUG)
    _put(client, auth_header=f"Authorization: {_SECRET}")
    remote.status = 500
    _block(client)
    assert _SECRET not in caplog.text
    assert _SECRET not in json.dumps(_audit_events())


# ── Block ────────────────────────────────────────────────────────────────────


def test_list_reports_unconfigured(client):
    resp = client.get("/api/admin/content-security/blocks", headers=_HEADERS)
    assert resp.status_code == 200
    body = resp.json()
    assert body["configured"] is False
    assert body["blocks"] == []


def test_block_requires_configured_url(client, remote):
    resp = _block(client)
    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "blocklist_not_configured"
    assert remote.calls == []


def test_block_posts_conditions_array_and_records_block(client, remote, configured):
    resp = _block(client, "proxy_type", " hosting ")
    assert resp.status_code == 201
    block = resp.json()
    assert block["conditions"] == [_cond("proxy_type", "hosting")]
    assert block["unblocked_at"] is None

    # Always an array, even for one condition.
    assert remote.calls == [("POST", _URL, {"kbps": 0, "conditions": [_cond("proxy_type", "hosting")]})]

    listing = client.get("/api/admin/content-security/blocks", headers=_HEADERS).json()
    assert listing["configured"] is True
    assert [b["id"] for b in listing["blocks"]] == [block["id"]]

    assert (
        "content_security_block",
        {"block_id": block["id"], "conditions": [_cond("proxy_type", "hosting")], "kbps": 0, "status": 200},
    ) in _audit_events()


def test_duplicate_active_block_rejected_without_calling_remote(client, remote, configured):
    assert _block(client).status_code == 201
    resp = _block(client)
    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "already_blocked"
    assert len(remote.calls) == 1


@pytest.mark.parametrize("status", [500, 404, 302])
def test_remote_failure_returns_502_and_records_nothing(client, remote, configured, status):
    """A non-2xx (including a redirect, which is never followed) is a failure."""
    remote.status = status
    resp = _block(client, "ip", "203.0.113.7")
    assert resp.status_code == 502
    assert resp.json()["detail"]["error"] == "blocklist_call_failed"
    assert len(remote.calls) == 1

    assert client.get("/api/admin/content-security/blocks", headers=_HEADERS).json()["blocks"] == []
    events = _audit_events()
    assert [e for e, _ in events] == ["content_security_block_failed"]
    assert str(status) in events[0][1]["error"]


def test_remote_unreachable_returns_502(client, configured, monkeypatch):
    def boom(request):
        raise httpx.ConnectError("refused", request=request)

    monkeypatch.setattr(blocklist_client, "_TRANSPORT", httpx.MockTransport(boom))
    resp = _block(client)
    assert resp.status_code == 502
    assert "ConnectError" in resp.json()["detail"]["message"]


def test_ip_block_validates_and_normalizes(client, remote, configured):
    assert _block(client, "ip", "not-an-ip").status_code == 422
    resp = _block(client, "ip", "2001:DB8::0001")
    assert resp.status_code == 201
    assert resp.json()["conditions"] == [_cond("ip", "2001:db8::1")]
    assert remote.calls[-1][2] == {"kbps": 0, "conditions": [_cond("ip", "2001:db8::1")]}


def test_throttle_sends_kbps_and_records_it(client, remote, configured):
    resp = _post(client, [_cond("country", "RS")], kbps=256)
    assert resp.status_code == 201
    block = resp.json()
    assert block["kbps"] == 256
    assert remote.calls == [("POST", _URL, {"kbps": 256, "conditions": [_cond("country", "RS")]})]
    assert (
        "content_security_block",
        {
            "block_id": block["id"],
            "conditions": [_cond("country", "RS")],
            "kbps": 256,
            "status": 200,
        },
    ) in _audit_events()

    # Removing it sends the same body, rate included.
    client.delete(f"/api/admin/content-security/blocks/{block['id']}", headers=_HEADERS)
    assert remote.calls[-1] == ("DELETE", _URL, {"kbps": 256, "conditions": [_cond("country", "RS")]})


def test_block_defaults_to_kbps_zero(client, remote, configured):
    assert _block(client).json()["kbps"] == 0
    assert remote.calls[0][2]["kbps"] == 0


def test_one_active_rule_per_condition_set_regardless_of_rate(client, remote, configured):
    assert _post(client, [_cond("country", "RS")], kbps=128).status_code == 201
    resp = _block(client, "country", "RS")
    assert resp.status_code == 409
    assert "already throttled to 128 kbps" in resp.json()["detail"]["message"]
    assert len(remote.calls) == 1


@pytest.mark.parametrize("kbps", [-1, 10_000_001, "fast"])
def test_invalid_kbps_rejected(client, remote, configured, kbps):
    assert _post(client, [_cond("country", "RS")], kbps=kbps).status_code == 422
    assert remote.calls == []


def test_unknown_category_rejected(client, remote, configured):
    assert _block(client, "city", "Paris").status_code == 422
    assert remote.calls == []


def test_multiple_conditions_are_sent_together_in_canonical_order(client, remote, configured):
    resp = _post(client, [_cond("proxy_type", "hosting"), _cond("country", "RS")])
    assert resp.status_code == 201
    expected = [_cond("country", "RS"), _cond("proxy_type", "hosting")]
    assert resp.json()["conditions"] == expected
    assert remote.calls == [("POST", _URL, {"kbps": 0, "conditions": expected})]

    # Same condition set in another order is the same block.
    dup = _post(client, [_cond("country", "RS"), _cond("proxy_type", "hosting")])
    assert dup.status_code == 409
    assert dup.json()["detail"]["message"] == (
        "country=RS AND proxy_type=hosting is already blocked. Remove that rule before adding a different one."
    )
    assert len(remote.calls) == 1

    # A subset is a different (broader) block.
    assert _block(client, "country", "RS").status_code == 201


@pytest.mark.parametrize(
    "conditions",
    [
        [],
        [_cond("country", "RS"), _cond("country", "CN")],
        [_cond("country", "")],
        [_cond("asn", "Comcast")],
        [_cond("asn", "0")],
        [_cond("asn", "4294967296")],
    ],
    ids=["empty", "duplicate_category", "blank_value", "asn_not_numeric", "asn_zero", "asn_out_of_range"],
)
def test_invalid_condition_sets_rejected(client, remote, configured, conditions):
    assert _post(client, conditions).status_code == 422
    assert remote.calls == []


def test_asn_block_normalizes_to_bare_number(client, remote, configured):
    resp = _block(client, "asn", "as7922")
    assert resp.status_code == 201
    assert resp.json()["conditions"] == [_cond("asn", "7922")]
    assert remote.calls[-1] == ("POST", _URL, {"kbps": 0, "conditions": [_cond("asn", "7922")]})

    # "AS7922" and "7922" are the same rule.
    assert _block(client, "asn", "7922").status_code == 409


# ── Unblock ──────────────────────────────────────────────────────────────────


def test_unblock_sends_delete_with_same_body(client, remote, configured):
    block = _block(client, "subscriber_id", "acct-123").json()

    resp = client.delete(f"/api/admin/content-security/blocks/{block['id']}", headers=_HEADERS)
    assert resp.status_code == 200
    assert resp.json()["unblocked_at"] is not None
    assert remote.calls[-1] == ("DELETE", _URL, {"kbps": 0, "conditions": [_cond("subscriber_id", "acct-123")]})
    assert client.get("/api/admin/content-security/blocks", headers=_HEADERS).json()["blocks"] == []
    assert "content_security_unblock" in [e for e, _ in _audit_events()]

    # Idempotent: reverting again is a no-op that doesn't re-call the function.
    again = client.delete(f"/api/admin/content-security/blocks/{block['id']}", headers=_HEADERS)
    assert again.status_code == 200
    assert len(remote.calls) == 2

    # And the value can be blocked again afterwards.
    assert _block(client, "subscriber_id", "acct-123").status_code == 201


def test_unblock_multi_condition_block_sends_all_conditions(client, remote, configured):
    block = _post(client, [_cond("ip", "203.0.113.7"), _cond("country", "RS")]).json()
    assert client.delete(f"/api/admin/content-security/blocks/{block['id']}", headers=_HEADERS).status_code == 200
    assert remote.calls[-1] == (
        "DELETE",
        _URL,
        {"kbps": 0, "conditions": [_cond("country", "RS"), _cond("ip", "203.0.113.7")]},
    )


def test_unblock_failure_keeps_block_active(client, remote, configured):
    block = _block(client).json()
    remote.status = 503
    resp = client.delete(f"/api/admin/content-security/blocks/{block['id']}", headers=_HEADERS)
    assert resp.status_code == 502
    active = client.get("/api/admin/content-security/blocks", headers=_HEADERS).json()["blocks"]
    assert [b["id"] for b in active] == [block["id"]]
    assert "content_security_unblock_failed" in [e for e, _ in _audit_events()]


def test_unblock_unknown_id_404(client, remote, configured):
    resp = client.delete("/api/admin/content-security/blocks/nope", headers=_HEADERS)
    assert resp.status_code == 404


# ── Access control ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "path",
    [
        "/api/admin/content-security/settings",
        "/api/admin/content-security/blocks",
        "/api/admin/content-security/blocks/abc",
    ],
)
def test_blocklist_routes_blocked_for_analysts_in_middleware(path):
    from backend.utils.remote_access import _is_blocked_path

    assert _is_blocked_path(path)


def test_blocklist_routes_reject_analyst_sessions(client, remote, configured):
    """Second layer behind the middleware: require_admin on every route."""
    from fastapi import HTTPException

    from backend.deps import require_admin
    from backend.main import app

    def deny():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = deny
    try:
        assert _block(client).status_code == 403
        assert client.get("/api/admin/content-security/blocks", headers=_HEADERS).status_code == 403
        assert client.get("/api/admin/content-security/settings").status_code == 403
    finally:
        app.dependency_overrides.pop(require_admin, None)
    assert remote.calls == []
