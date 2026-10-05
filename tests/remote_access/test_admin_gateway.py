from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest

from backend.utils.admin_gateway import validate_admin_gateway_config
from backend.utils.remote_access import RemoteAccessMiddleware, is_request_remote

SECRET = "test-gateway-secret-with-at-least-32-characters"
HEADER = "X-Admin-Gateway-Token"


def _request(peer: str, headers: dict[str, str] | None = None) -> StarletteRequest:
    return StarletteRequest(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/bootstrap",
            "headers": [(key.lower().encode(), value.encode()) for key, value in (headers or {}).items()],
            "client": (peer, 1234),
            "server": ("testserver", 80),
            "scheme": "http",
            "query_string": b"",
        }
    )


@pytest.fixture
def gateway_client(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    manager = MagicMock()
    manager.is_sharing_active.return_value = False
    manager.validate_session.return_value = None
    monkeypatch.setattr("backend.utils.remote_access.get_tunnel_manager", lambda: manager)
    app = FastAPI()
    app.add_middleware(RemoteAccessMiddleware)

    @app.get("/api/admin/probe")
    def probe(request: Request) -> dict[str, bool]:
        return {"is_remote": request.state.is_remote}

    @app.get("/api/health")
    def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/api/admin/probe")
    def mutate() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/admin/recheck")
    def recheck(request: Request) -> dict[str, bool]:
        return {
            "is_remote": is_request_remote(request),
            "credential_visible": HEADER in request.headers,
        }

    with TestClient(app) as client:
        yield client


@pytest.mark.security_regression
@pytest.mark.parametrize("peer", ["127.0.0.1", "10.12.1.2", "203.0.113.1"])
def test_valid_gateway_authenticates_independently_of_peer(monkeypatch, peer):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    assert not is_request_remote(_request(peer, {HEADER: SECRET}))


@pytest.mark.security_regression
@pytest.mark.parametrize("peer", ["127.0.0.1", "10.12.1.2", "203.0.113.1"])
def test_required_gateway_disables_ip_based_admin(monkeypatch, peer):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("LOCAL_ADMIN_CIDRS", "10.12.0.0/16")
    assert is_request_remote(_request(peer))


@pytest.mark.security_regression
@pytest.mark.parametrize("supplied", ["forged", "", "non-ascii-\u00e9"])
def test_wrong_gateway_token_does_not_authenticate(monkeypatch, supplied):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    assert is_request_remote(_request("127.0.0.1", {HEADER: supplied}))


@pytest.mark.security_regression
def test_short_or_missing_configured_secret_cannot_authenticate(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", "short")
    assert is_request_remote(_request("127.0.0.1", {HEADER: "short"}))
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET")
    assert is_request_remote(_request("127.0.0.1", {HEADER: SECRET}))


def test_legacy_loopback_behavior_remains_when_gateway_unconfigured(monkeypatch):
    monkeypatch.delenv("ADMIN_GATEWAY_REQUIRED", raising=False)
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    assert not is_request_remote(_request("127.0.0.1"))
    assert is_request_remote(_request("203.0.113.1"))


def test_authenticated_gateway_replaces_legacy_browser_admin_token(gateway_client, monkeypatch):
    monkeypatch.setenv("ADMIN_SHARED_SECRET", "legacy-browser-token")
    response = gateway_client.get("/api/admin/probe", headers={HEADER: SECRET})
    assert response.status_code == 200
    assert response.json() == {"is_remote": False}
    assert SECRET not in response.text


def test_gateway_scrubs_credentials_and_retains_authenticated_request_state(gateway_client):
    response = gateway_client.get("/api/admin/recheck", headers={HEADER: SECRET})
    assert response.status_code == 200
    assert response.json() == {"is_remote": False, "credential_visible": False}


@pytest.mark.security_regression
def test_forged_gateway_header_is_rejected_even_on_loopback(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: "forged"})
    assert response.status_code == 401
    assert response.json() == {"error": "admin_gateway_invalid"}


@pytest.mark.security_regression
def test_uncredentialed_loopback_cannot_reach_admin_in_required_mode(gateway_client):
    response = gateway_client.get("/api/admin/probe")
    assert response.status_code == 401
    assert response.json()["error"] == "unauthenticated"


def test_required_gateway_does_not_break_cheap_liveness(gateway_client):
    response = gateway_client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"ok": True}


@pytest.mark.security_regression
def test_public_marker_cannot_be_promoted_with_gateway_token(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: SECRET, "X-Proxied-By-Caddy": "1"})
    assert response.status_code == 401
    assert response.json()["error"] == "admin_gateway_invalid"


@pytest.mark.security_regression
def test_duplicate_gateway_headers_are_rejected(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers=[(HEADER, SECRET), (HEADER, "forged")])
    assert response.status_code == 401


def test_required_gateway_without_secret_fails_configuration(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.delenv("ADMIN_GATEWAY_SECRET", raising=False)
    with pytest.raises(ValueError, match="at least 32"):
        validate_admin_gateway_config()


def test_configured_short_gateway_secret_fails_configuration(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", "short")
    with pytest.raises(ValueError, match="at least 32"):
        validate_admin_gateway_config()


def test_configured_gateway_passes_configuration(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "1")
    monkeypatch.setenv("ADMIN_GATEWAY_SECRET", SECRET)
    validate_admin_gateway_config()


def test_invalid_required_mode_fails_configuration(monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_REQUIRED", "true")
    with pytest.raises(ValueError, match="must be 0 or 1"):
        validate_admin_gateway_config()


@pytest.mark.security_regression
@pytest.mark.parametrize("origin", ["https://attacker.example", "null", "https://testserver.attacker.example"])
def test_gateway_blocks_cross_origin_browser_mutations(gateway_client, origin):
    response = gateway_client.post("/api/admin/probe", headers={HEADER: SECRET, "Origin": origin})
    assert response.status_code == 403
    assert response.json()["error"] == "admin_origin_not_allowed"


def test_gateway_accepts_same_origin_browser_mutations(gateway_client):
    response = gateway_client.post("/api/admin/probe", headers={HEADER: SECRET, "Origin": "https://testserver"})
    assert response.status_code == 200


def test_gateway_accepts_certificate_authenticated_cli_without_origin(gateway_client):
    response = gateway_client.post("/api/admin/probe", headers={HEADER: SECRET})
    assert response.status_code == 200


@pytest.mark.security_regression
def test_gateway_rejects_browser_cross_site_fetch_metadata(gateway_client):
    response = gateway_client.get("/api/admin/probe", headers={HEADER: SECRET, "Sec-Fetch-Site": "cross-site"})
    assert response.status_code == 403
