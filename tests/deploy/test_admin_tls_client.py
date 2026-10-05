"""Scoped stdlib transport tests; no deployment environment or real credentials."""

import json
from pathlib import Path
from urllib.request import Request

import pytest

from scripts.lib import admin_tls


@pytest.fixture
def configured(monkeypatch, tmp_path):
    for name in (
        "ADMIN_GATEWAY_ENDPOINTS",
        "ADMIN_GATEWAY_CLIENT_ORIGIN",
        "ADMIN_GATEWAY_CLIENT_CERT",
        "ADMIN_GATEWAY_CLIENT_KEY",
        "ADMIN_GATEWAY_SERVER_CA",
    ):
        monkeypatch.delenv(name, raising=False)
    config = {"origin": "https://admin.example.com:8443"}
    for name in ("cert", "key", "ca"):
        path = tmp_path / name
        path.write_text("fixture")
        path.chmod(0o600)
        config[name] = str(path)
    monkeypatch.setenv("ADMIN_GATEWAY_ENDPOINTS", json.dumps({"remote-high-scale": config}))
    return config


def test_environment_alias_and_exact_origin(configured):
    assert admin_tls.admin_origin("remote-hs") == "https://admin.example.com:8443"
    assert admin_tls.admin_origin("remote-standard") is None
    assert admin_tls.is_admin_url("https://admin.example.com:8443/api/health")
    assert not admin_tls.is_admin_url("https://admin.example.com/api/health")
    assert not admin_tls.is_admin_url("https://edge.example.com/api/health")


def test_unconfigured_origin_never_loads_client_context(configured, monkeypatch):
    monkeypatch.setattr(admin_tls, "_context", lambda _: pytest.fail("Identity must not reach edge"))
    monkeypatch.setattr(admin_tls, "urlopen", lambda request, timeout: (request, timeout))
    assert admin_tls.admin_urlopen("https://edge.example.com/probe", timeout=3) == ("https://edge.example.com/probe", 3)


def test_configured_origin_uses_scoped_opener(configured, monkeypatch):
    context = object()
    monkeypatch.setattr(admin_tls, "_context", lambda _: context)
    monkeypatch.setattr(admin_tls, "HTTPSHandler", lambda context: context)
    captured = {}

    class Opener:
        def open(self, request, timeout):
            captured["request"] = request
            return "response"

    def build(*handlers):
        captured["handlers"] = handlers
        return Opener()

    monkeypatch.setattr(admin_tls, "build_opener", build)
    assert admin_tls.admin_urlopen("https://admin.example.com:8443/api/health") == "response"
    assert captured["handlers"][0] is context
    redirect = captured["handlers"][1]
    with pytest.raises(ValueError, match="cross-origin"):
        redirect.redirect_request(Request(configured["origin"]), None, 302, "", {}, "https://edge.example.com")


@pytest.mark.parametrize("header", ["X-Admin-Token", "X-Admin-Gateway-Token"])
def test_certificate_transport_rejects_server_token_headers(configured, header):
    with pytest.raises(ValueError, match="must not send"):
        admin_tls.admin_urlopen(Request(configured["origin"], headers={header: "forged"}))


def test_invalid_or_incomplete_config_fails_closed(configured, monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_ENDPOINTS", "{bad-json")
    with pytest.raises(ValueError, match="valid JSON"):
        admin_tls.admin_origin("remote-hs")
    monkeypatch.setenv(
        "ADMIN_GATEWAY_ENDPOINTS", json.dumps({"remote-high-scale": {"origin": "http://admin.example.com"}})
    )
    with pytest.raises(ValueError, match="needs nonempty"):
        admin_tls.is_admin_url("https://edge.example.com")


def test_public_private_key_permissions_rejected(configured):
    Path(configured["key"]).chmod(0o644)
    with pytest.raises(ValueError, match="owner-only"):
        admin_tls.admin_origin("remote-hs")


def test_two_targets_keep_distinct_identity_and_unrelated_target_legacy(configured, monkeypatch):
    second = {**configured, "origin": "https://vm-admin.example.com:9443"}
    monkeypatch.setenv(
        "ADMIN_GATEWAY_ENDPOINTS",
        json.dumps(
            {
                "remote-high-scale": configured,
                "remote-standard": second,
            }
        ),
    )
    assert admin_tls.admin_origin("remote-standard") == second["origin"]
    assert admin_tls.admin_origin("local-std") is None
    result = admin_tls.admin_tls_config("remote-std")
    result["origin"] = "https://changed.example.com"
    assert admin_tls.admin_origin("remote-standard") == second["origin"]


def test_tls_context_keeps_verification_enabled(configured, monkeypatch):
    calls = {}

    class Context:
        def load_cert_chain(self, cert, key, password):
            calls["identity"] = (cert, key)
            assert password() == ""

    def create_default_context(*, cafile):
        calls["ca"] = cafile
        return Context()

    monkeypatch.setattr(admin_tls.ssl, "create_default_context", create_default_context)
    admin_tls.validate_admin_tls_config()
    assert calls["ca"] == configured["ca"]
    assert calls["identity"] == (configured["cert"], configured["key"])
