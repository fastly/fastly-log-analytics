from __future__ import annotations

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


@pytest.mark.parametrize("header", ["X-Admin-Gateway-Token"])
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


# ── Merged from test_report_admin_tls.py ──────────────────────────────────────────────────────

import http.client
import io
import sys
import threading
from http.server import ThreadingHTTPServer
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

with patch.dict(sys.modules, {"validate_credentials": Mock(name="operator_credentials")}):
    from scripts.dev import report_server


@pytest.fixture
def report(monkeypatch):
    # The report server memoizes live bootstrap/error/credential responses at module level;
    # without a reset the first test's response is served from cache to every later one.
    monkeypatch.setattr(report_server, "_BOOTSTRAP_CACHE", {})
    monkeypatch.setattr(report_server, "_CRED_CACHE", None)
    monkeypatch.setattr(report_server, "_MONITORED_ERRORS_CACHE", None)
    origins = []
    requests = []
    monkeypatch.setattr(
        report_server,
        "admin_origin",
        lambda environment: origins.append(environment) or "https://admin.example.com:8443",
    )

    def upstream(request, timeout):
        requests.append((request.full_url, timeout, dict(request.header_items())))
        response = io.BytesIO(b'{"active_service_id":"test-service"}')
        response.status = 200
        return response

    monkeypatch.setattr(report_server, "admin_urlopen", upstream)
    server = ThreadingHTTPServer(("127.0.0.1", 0), report_server.ReportRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requests, origins
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_report_bootstrap_uses_exact_origin_certificate_transport(report):
    origin, requests, origins = report
    with urlopen(f"{origin}/relics/bootstrap_status.json?env=remote-hs", timeout=5) as response:
        body = json.load(response)
    assert body == {"ok": True, "status": 200, "data": {"active_service_id": "test-service"}}
    assert origins == ["remote-hs"]
    assert requests == [("https://admin.example.com:8443/api/bootstrap", 3, {"Accept": "application/json"})]


def test_report_rejects_unknown_environment_before_certificate_selection(report):
    origin, requests, origins = report
    with pytest.raises(HTTPError) as error:
        urlopen(f"{origin}/relics/bootstrap_status.json?env=unknown", timeout=5)
    assert error.value.code == 400
    error.value.close()
    assert not requests
    assert not origins


@pytest.mark.parametrize("path", ["/", "/reports", "/reports/"])
def test_report_root_redirect_is_bodyless_and_keeps_connection_usable(report, path):
    origin, _, _ = report
    host, port = origin.removeprefix("http://").split(":")
    connection = http.client.HTTPConnection(host, int(port), timeout=2)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        assert response.status == 302
        assert response.getheader("Location") == "/reports/deploys/current/index.html"
        assert response.getheader("Content-Length") == "0"
        assert response.read() == b""
        connection.request("GET", "/relics/bootstrap_status.json?env=remote-hs")
        follow_up = connection.getresponse()
        assert follow_up.status == 200
        follow_up.read()
    finally:
        connection.close()
