"""scripts/lib/gce_ssh.sh, readiness probes, admin TLS, CLI, prom, streaming fixture, profile_insights, check_osv, port_forward, and run_contract_backend tests."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import json
import os
import re
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import Request

import duckdb
import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripts import check_environment_health as health
from scripts import run_contract_backend
from scripts.dev import audit_environments as audit
from scripts.dev import measure_e2e_freshness as freshness
from scripts.dev.measure_e2e_freshness import (
    compute_lag_statistics,
    configured_polling,
    cron_completion_time,
    load_environment_config,
    new_terminal_run,
    rum_beacon_count,
    seconds_between,
)
from scripts.lib import admin_tls, environments, readiness
from scripts.lib import admin_tls as admin_http
from scripts.lib import environments as prom
from scripts.lib import port_forward as pf
from scripts.lib.environments import add_common_args, remediation_command
from scripts.lib.section_measurements import BUDGETS
from scripts.run_contract_backend import install_streaming_fixture

REPO = Path(__file__).resolve().parents[2]
LIB = REPO / "scripts" / "lib" / "gce_ssh.sh"
LAUNCHER = REPO / "scripts" / "run_contract_backend.py"
SCRIPT_PATH = REPO / "scripts" / "check_osv.py"
# ── Merged from test_health_probes.py ──────────────────────────────────────────────────────

HEALTHY: dict[str, tuple[int, Any]] = {
    "GET /api/health": (200, {"status": "ok", "version": "1.2.3"}),
    "GET /api/bootstrap": (200, {"active_service_id": "svc1"}),
    "POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz": (404, {"error": "nope"}),
    "GET /api/share/auth-config": (200, {"passcode_enabled": True}),
    "GET /api/services": (401, {"error": "unauthenticated"}),
}


class Stub:
    def __init__(self) -> None:
        self.routes: dict[str, tuple[int, Any]] = dict(HEALTHY)
        self.seen: list[tuple[str, dict[str, str], bytes]] = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def _serve(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                stub.seen.append((f"{self.command} {self.path}", dict(self.headers), body))
                status, payload = stub.routes.get(f"{self.command} {self.path}", (404, {"error": "missing"}))
                raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            do_GET = do_POST = _serve  # noqa: N815

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def target(self, name: str = "stub") -> health.EnvironmentTarget:
        return health.EnvironmentTarget(name, "Stub", self.origin, self.origin, "svc1", "fix-it")


@pytest.fixture
def probe_stub(monkeypatch: pytest.MonkeyPatch) -> Iterator[Stub]:
    for name in (
        "ADMIN_GATEWAY_ENDPOINTS",
        "ADMIN_GATEWAY_CLIENT_ORIGIN",
        "ADMIN_GATEWAY_CLIENT_CERT",
        "ADMIN_GATEWAY_CLIENT_KEY",
        "ADMIN_GATEWAY_SERVER_CA",
    ):
        monkeypatch.delenv(name, raising=False)
    server = Stub()
    yield server
    server.server.shutdown()
    server.server.server_close()


class _SleepStub:
    def __init__(self, sleep: Any) -> None:
        self.sleep = sleep

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


def dead_origin() -> str:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{sock.getsockname()[1]}"


def test_probe_reports_status_body_latency_and_errors(probe_stub: Stub) -> None:
    result = readiness.probe(f"{probe_stub.origin}/api/health", timeout=5)
    assert (result.status, result.failed, result.http_error) == (200, False, False)
    assert result.json() == {"status": "ok", "version": "1.2.3"}
    assert result.content_type == "application/json" and result.latency_ms >= 0

    probe_stub.routes["GET /api/health"] = (503, {"error": "busy"})
    err_res = readiness.probe(f"{probe_stub.origin}/api/health", timeout=5)
    assert (err_res.status, err_res.http_error, err_res.reason) == (503, True, "Service Unavailable")
    assert err_res.error == "HTTP Error 503: Service Unavailable" and err_res.json() == {"error": "busy"}

    dead_res = readiness.probe(f"{dead_origin()}/x", timeout=2)
    assert dead_res.status is None and dead_res.failed and not dead_res.http_error
    assert isinstance(dead_res.exc, urllib.error.URLError)


def test_probe_delegates_to_admin_urlopen_with_request_details(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake(request: Any, timeout: float) -> Any:
        captured.update(
            url=request.full_url,
            method=request.get_method(),
            data=request.data,
            timeout=timeout,
            headers=dict(request.header_items()),
        )
        raise OSError("stop")

    monkeypatch.setattr(readiness, "admin_urlopen", fake)
    result = readiness.probe("http://x/y", timeout=7, headers={"X-A": "1"}, data=b"{}", method="POST")
    assert result.error == "stop"
    assert captured == {"url": "http://x/y", "method": "POST", "data": b"{}", "timeout": 7, "headers": {"X-a": "1"}}


def test_health_ok_and_failure_modes(probe_stub: Stub) -> None:
    result = health.check_backend_health(probe_stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version, result.error) == (True, 200, "1.2.3", None)
    headers = next(h for line, h, _ in probe_stub.seen if line == "GET /api/health")
    assert headers["User-Agent"] == "FLA-HealthCheck/1.0" and headers["Accept"] == "application/json"

    for route, expected, expected_err in [
        ((503, {"x": 1}), (False, 503, None), "HTTPError 503: Service Unavailable"),
        (
            (200, {"status": "degraded", "version": "9"}),
            (False, 200, "9"),
            'Unexpected payload: {"status": "degraded", "version": "9"}',
        ),
        ((200, b"<html>"), (False, None, None), "Expecting value: line 1 column 1 (char 0)"),
    ]:
        probe_stub.routes["GET /api/health"] = route
        r = health.check_backend_health(probe_stub.target(), timeout_s=5)
        assert (r.backend_ok, r.status_code, r.version) == expected
        assert r.error == expected_err

    dead_target = health.EnvironmentTarget("d", "A", dead_origin(), "", "svc1", "fix")
    r_dead = health.check_backend_health(dead_target, timeout_s=2)
    assert (r_dead.backend_ok, r_dead.status_code) == (False, None)
    assert r_dead.error and "Connection refused" in r_dead.error


def test_tenancy_ok_and_failure_modes(probe_stub: Stub) -> None:
    result = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (result.service_match, result.rejection_ok, result.rejection_code, result.error) == (True, True, 404, None)
    assert result.resolved_service_id == "svc1"
    line, headers, body = next(s for s in probe_stub.seen if s[0].startswith("POST"))
    assert line == "POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"
    assert json.loads(body) == {"time_range": "24h"} and headers["Content-Type"] == "application/json"

    probe_stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (200, {})
    r200 = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (r200.rejection_ok, r200.rejection_code) == (False, 200)
    assert r200.error == "Tenancy breach: fake tenant returned HTTP 200 (expected 400/401/403/404)"

    probe_stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (500, {})
    r500 = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (r500.rejection_ok, r500.rejection_code) == (False, 500)

    probe_stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (404, {"error": "nope"})
    probe_stub.routes["GET /api/bootstrap"] = (200, {"active_service_id": "other"})
    rmis = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (rmis.service_match, rmis.rejection_ok) == (False, True)
    assert rmis.error == "Service mismatch: got 'other', expected 'svc1'"


@pytest.mark.parametrize(
    ("route", "message"),
    [
        ((500, {"e": 1}), "Failed to fetch bootstrap: HTTP Error 500: Internal Server Error"),
        ((200, b"nope"), "Failed to fetch bootstrap: Expecting value: line 1 column 1 (char 0)"),
        ((200, [1]), "Failed to fetch bootstrap: 'list' object has no attribute 'get'"),
    ],
)
def test_tenancy_bootstrap_failures(probe_stub: Stub, route: tuple[int, Any], message: str) -> None:
    probe_stub.routes["GET /api/bootstrap"] = route
    result = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (result.resolved_service_id, result.service_match, result.rejection_code) == (None, False, None)
    assert result.error == message
    assert not any(line.startswith("POST") for line, _, _ in probe_stub.seen)


def test_rbac_ok_and_failure_modes(probe_stub: Stub) -> None:
    result = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert (result.auth_config_ok, result.unauth_analyst_blocked, result.unauth_analyst_code, result.error) == (
        True,
        True,
        401,
        None,
    )
    headers = next(h for line, h, _ in probe_stub.seen if line == "GET /api/share/auth-config")
    assert headers["X-Remote-Analyst"] == "1"
    svc_headers = next(h for line, h, _ in probe_stub.seen if line == "GET /api/services")
    assert svc_headers["X-Remote-Analyst"] == "1"
    assert "X-Admin-Gateway-Token" not in svc_headers

    probe_stub.routes["GET /api/services"] = (403, {"error": "forbidden"})
    r403 = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert (r403.auth_config_ok, r403.unauth_analyst_blocked, r403.unauth_analyst_code, r403.error) == (
        True,
        True,
        403,
        None,
    )

    probe_stub.routes["GET /api/services"] = (200, {"services": []})
    r_breach = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert (r_breach.auth_config_ok, r_breach.unauth_analyst_blocked, r_breach.unauth_analyst_code) == (
        True,
        False,
        200,
    )
    assert (
        r_breach.error == "RBAC breach: unauthenticated analyst on /api/services returned HTTP 200 (expected 401/403)"
    )

    probe_stub.routes["GET /api/services"] = (401, {"error": "unauthenticated"})
    probe_stub.routes["GET /api/share/auth-config"] = (200, {"other": 1})
    r1 = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert r1.auth_config_ok is False and r1.unauth_analyst_blocked is True
    assert r1.error == "Auth config endpoint failed or returned unexpected payload"

    probe_stub.routes["GET /api/share/auth-config"] = (404, {})
    r2 = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert (r2.auth_config_ok, r2.unauth_analyst_blocked, r2.unauth_analyst_code) == (False, False, None)
    assert r2.error == "Failed to query /api/share/auth-config: HTTP Error 404: Not Found"


def _run_main(monkeypatch: pytest.MonkeyPatch, target: health.EnvironmentTarget, *extra: str) -> int:
    monkeypatch.setattr(health, "ENVIRONMENTS", {target.name: target})
    monkeypatch.setattr("sys.argv", ["check_environment_health.py", "--env", target.name, *extra])
    return health.main()


def test_main_text_and_json_reports_golden(
    probe_stub: Stub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_main(monkeypatch, probe_stub.target("stub-env")) == 0
    out = re.sub(r"\d+\.\dms", "<lat>", capsys.readouterr().out)
    assert out.splitlines()[6:] == [
        "stub-env               | PASS (200) | svc1 (OK)              | PASS (404)  | PASS (401) | <lat>",
        "=" * 96,
        "ALL TARGET ENVIRONMENTS HEALTHY, TENANCY VERIFIED & RBAC ENFORCED!",
        "=" * 96,
        "",
    ]

    assert _run_main(monkeypatch, probe_stub.target("stub-env"), "--json") == 0
    (row,) = json.loads(capsys.readouterr().out)
    row.pop("latency_ms")
    assert row == {
        "env": "stub-env",
        "architecture": "Stub",
        "service_id": "svc1",
        "backend_url": probe_stub.origin,
        "health_ok": True,
        "health_status_code": 200,
        "version": "1.2.3",
        "tenancy_service_match": True,
        "tenancy_rejection_ok": True,
        "tenancy_rejection_code": 404,
        "rbac_auth_config_ok": True,
        "rbac_unauth_blocked": True,
        "rbac_unauth_code": 401,
        "error": None,
        "remediation": None,
    }

    probe_stub.routes["GET /api/health"] = (503, {})
    assert _run_main(monkeypatch, probe_stub.target("stub-env")) == 1
    fail_lines = re.sub(r"\d+\.\dms", "<lat>", capsys.readouterr().out).splitlines()[6:]
    assert fail_lines[0].startswith("stub-env               | FAIL     | svc1 (OK)")
    assert fail_lines[1:3] == ["   [!] Health Error: HTTPError 503: Service Unavailable", "   [!] Fix command: fix-it"]
    assert "FAILURE: 1/1 environments failed checks." in fail_lines


def test_audit_get_and_post_json(probe_stub: Stub, monkeypatch: pytest.MonkeyPatch) -> None:
    status, data = audit.http_get_json(f"{probe_stub.origin}/api/health", headers={"X-T": "1"}, timeout=5)
    assert (status, data) == (200, {"status": "ok", "version": "1.2.3"})
    _, headers, _ = probe_stub.seen[-1]
    assert headers["User-Agent"] == "FLA-AuditTool/1.0" and headers["X-T"] == "1"

    probe_stub.routes.update(
        {"GET /e1": (500, {"detail": "boom"}), "GET /e2": (502, b"<html>"), "GET /e3": (200, b"<html>")}
    )
    assert audit.http_get_json(f"{probe_stub.origin}/e1") == (500, {"detail": "boom"})
    assert audit.http_get_json(f"{probe_stub.origin}/e2") == (502, {"error": "HTTP Error 502: Bad Gateway"})
    assert audit.http_get_json(f"{probe_stub.origin}/e3") == (0, {"error": "Expecting value: line 1 column 1 (char 0)"})
    status, data = audit.http_get_json(f"{dead_origin()}/x", timeout=2)
    assert status == 0 and "Connection refused" in data["error"]

    probe_stub.routes["POST /p"] = (422, {"detail": "bad"})
    assert audit.http_post_json(f"{probe_stub.origin}/p", {"a": 1}, timeout=5) == (422, {"detail": "bad"})
    line, headers, body = probe_stub.seen[-1]
    assert (line, json.loads(body), headers["Content-Type"]) == ("POST /p", {"a": 1}, "application/json")
    probe_stub.routes["POST /q"] = (200, {"ok": True})
    assert audit.http_post_json(f"{probe_stub.origin}/q", timeout=5) == (200, {"ok": True})
    assert json.loads(probe_stub.seen[-1][2]) == {}

    probe_stub.routes["POST /p500"] = (500, {"e": 1})
    monkeypatch.setattr(audit, "time", _SleepStub(lambda _s: pytest.fail("must not retry HTTP errors")))
    assert audit.http_post_json(f"{probe_stub.origin}/p500", {}, timeout=5) == (500, {"e": 1})
    assert [line for line, _, _ in probe_stub.seen].count("POST /p500") == 1


def test_audit_post_json_retries_transport_failure_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    real = audit.probe

    def counting(url: str, **kwargs: Any) -> readiness.ProbeResult:
        calls.append(url)
        return real(url, **kwargs)

    sleeps: list[float] = []
    monkeypatch.setattr(audit, "probe", counting)
    monkeypatch.setattr(audit, "time", _SleepStub(sleeps.append))
    status, data = audit.http_post_json(f"{dead_origin()}/x", {}, timeout=2)
    assert status == 0 and "Connection refused" in data["error"]
    assert len(calls) == 2 and sleeps == [0.5]


def test_load_persisted_deploy_env_populates_admin_origins_and_respects_existing_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    tracked_keys = (
        "FLA_ADMIN_DEPLOY_ENV",
        "LOCAL_STANDARD_ADMIN_MTLS",
        "GCE_ADMIN_MTLS",
        "REMOTE_HS_ADMIN_MTLS",
        "ADMIN_GATEWAY_ENDPOINTS",
    )
    for key in tracked_keys:
        monkeypatch.setenv(key, "")
        monkeypatch.delenv(key, raising=False)

    endpoint_files: dict[str, str] = {}
    for name in ("cert", "key", "ca"):
        p = tmp_path / name
        p.write_text("pem")
        p.chmod(0o600)
        endpoint_files[name] = str(p)

    endpoints = {
        "local-standard": {"origin": "https://127.0.0.1:9443", **endpoint_files},
        "remote-standard": {"origin": "https://203.0.113.10:9443", **endpoint_files},
        "remote-high-scale": {"origin": "https://10.0.0.10:8443", **endpoint_files},
    }
    deploy_env = tmp_path / "deploy.env"
    deploy_env.write_text(
        f"LOCAL_STANDARD_ADMIN_MTLS=1\nGCE_ADMIN_MTLS=1\nREMOTE_HS_ADMIN_MTLS=1\n"
        f"ADMIN_GATEWAY_ENDPOINTS='{json.dumps(endpoints)}'\n"
    )
    deploy_env.chmod(0o600)
    monkeypatch.setenv("FLA_ADMIN_DEPLOY_ENV", str(deploy_env))
    monkeypatch.setenv("GCE_ADMIN_MTLS", "0")

    try:
        loaded = environments.load_persisted_deploy_env()
        assert loaded["LOCAL_STANDARD_ADMIN_MTLS"] == "1" and loaded["REMOTE_HS_ADMIN_MTLS"] == "1"
        assert environments.os.environ["GCE_ADMIN_MTLS"] == "0"

        audit.refresh_environment_origins()
        assert audit.ENVIRONMENTS["local-standard"].backend_url == "https://127.0.0.1:9443"
        assert audit.ENVIRONMENTS["remote-standard"].backend_url == "https://203.0.113.10:9443"
        assert audit.ENVIRONMENTS["remote-high-scale"].backend_url == "https://10.0.0.10:8443"
    finally:
        for key in tracked_keys:
            monkeypatch.delenv(key, raising=False)
        audit.refresh_environment_origins()


def test_audit_target_private_mtls_timeout_adds_vpn_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(audit, "is_admin_url", lambda url: url.startswith("https://10.0.0.10:8443"))
    monkeypatch.setattr(
        audit, "http_get_json", lambda url, headers=None, timeout=5.0: (0, {"error": "<urlopen error timed out>"})
    )
    env = audit.EnvironmentConfig(
        "remote-high-scale",
        "High-Scale (Remote/K8s)",
        "https://10.0.0.10:8443",
        "https://10.0.0.10:8443/dashboard",
        "svc-hs",
        True,
    )
    res = audit.audit_target(env, timeout=1.0)
    assert not res.reachable
    assert any("VPN/tunnel" in err and "10.0.0.10:8443" in err for err in res.errors)


# ── Merged from test_lib_helpers.py ──────────────────────────────────────────────────────


def test_total_deadline_cancels_a_stalled_response_body(monkeypatch: pytest.MonkeyPatch) -> None:
    closed = []

    class Stalled(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"{"
            await asyncio.Event().wait()

        async def aclose(self) -> None:
            closed.append(True)

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=Stalled()))
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: ssl.create_default_context())
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=transport))
    with pytest.raises(TimeoutError):
        asyncio.run(admin_http.async_admin_json(Request("https://example.com/api/test", data=b"{}"), timeout=0.05))
    assert closed


def test_deadline_transport_preserves_request_and_refuses_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"location": "https://other.example.com/private"})

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(respond)
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: ssl.create_default_context())
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=transport))
    request = Request(
        "https://example.com/api/test", data=b'{"filters":{}}', headers={"X-Page-Load-ID": "test-correlation"}
    )
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(admin_http.async_admin_json(request, timeout=1))
    assert len(seen) == 1
    assert (seen[0].method, seen[0].content, seen[0].headers["x-page-load-id"]) == (
        "POST",
        b'{"filters":{}}',
        "test-correlation",
    )


@pytest.mark.parametrize(
    "url,headers",
    [
        ("https://other.example.com/api/test", {}),
        ("https://example.com/api/test", {"X-Admin-Gateway-Token": "do-not-send"}),
    ],
)
def test_deadline_transport_rejects_untrusted_identity_use_before_tls(
    monkeypatch: pytest.MonkeyPatch, url: str, headers: dict
) -> None:
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: pytest.fail("TLS identity must not be loaded"))
    with pytest.raises(ValueError):
        asyncio.run(admin_http.async_admin_json(Request(url, headers=headers), timeout=1))


# ── Merged from test_cli.py ──


def _parser(**kwargs: object) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="t")
    add_common_args(parser, **kwargs)  # type: ignore[arg-type]
    return parser


def test_common_args_behavior() -> None:
    for bad_arg in (["--timeout", "1"], ["--json"]):
        with pytest.raises(SystemExit):
            _parser().parse_args(bad_arg)

    parser = _parser(timeout_default=5.0, timeout_label="HTTP request")
    assert parser.parse_args([]).timeout == 5.0
    assert parser.parse_args(["--timeout", "2.5"]).timeout == 2.5
    assert "HTTP request timeout in seconds (default: 5.0)" in parser.format_help()

    jparser = _parser(json_help="Output raw JSON results")
    assert jparser.parse_args([]).json is False
    assert jparser.parse_args(["--json"]).json is True
    assert "Output raw JSON results" in jparser.format_help()


@pytest.mark.parametrize(
    ("module", "default"),
    [("scripts.check_environment_health", 5.0), ("scripts.dev.audit_environments", 10.0)],
)
def test_adopting_scripts_keep_their_timeout_defaults(
    module: str, default: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    mod = importlib.import_module(module)
    monkeypatch.setattr("sys.argv", ["prog", "--help"])
    with pytest.raises(SystemExit):
        mod.main()
    out = capsys.readouterr().out
    assert f"timeout in seconds (default: {default})" in out and "--json" in out


# ── Merged from test_prom.py ──


def test_histogram_quantile_matches_the_legacy_query_text() -> None:
    assert prom.histogram_quantile(0.95, "http_server_duration_milliseconds_bucket") == (
        "histogram_quantile(0.95,sum by (le)(rate(http_server_duration_milliseconds_bucket[5m])))"
    )


@pytest.fixture
def prom_server() -> Iterator[tuple[str, list[str]]]:
    seen: list[str] = []
    body: dict[str, object] = {"data": {"result": [{"value": [1700000000, "12.5"]}]}}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            seen.append(parse_qs(urlparse(self.path).query)["query"][0])
            if "boom" in seen[-1]:
                self.send_response(500)
                self.end_headers()
                return
            payload = {"data": {"result": []}} if "empty" in seen[-1] else body
            raw = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/", seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_instant_returns_sample_or_none_on_errors(prom_server: tuple[str, list[str]]) -> None:
    base, seen = prom_server
    assert prom.instant(base, "up{job='a b'}") == 12.5
    assert seen == ["up{job='a b'}"]
    assert prom.instant(base, "empty") is None
    assert prom.instant(base, "boom") is None
    assert prom.instant("http://127.0.0.1:1", "up", timeout=1) is None


# ── Merged from test_streaming_e2e_fixture.py ──


def test_streaming_fixture_requires_an_isolated_contract_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTRACT_STREAMING_FIXTURES", "1")
    monkeypatch.delenv("CONTRACT_CONFIGS_DIR", raising=False)
    monkeypatch.delenv("CONTRACT_DATA_DIR", raising=False)
    monkeypatch.setattr(sys, "argv", ["run_contract_backend.py"])
    with pytest.raises(ValueError, match="isolated contract sandbox"):
        run_contract_backend.main()


def test_streaming_fixture_is_cookie_scoped_and_isolated() -> None:
    app = FastAPI()

    @app.post("/api/cmcd/aggregates")
    def aggregates() -> dict:
        return {"available": False}

    install_streaming_fixture(app)
    with TestClient(app) as browser, TestClient(app) as other_browser:
        payload = {"available": True, "overview": {"active_sessions": 1234}}
        assert browser.post("/api/__e2e__/streaming-fixture", json=payload).status_code == 200
        body = {"range_token": "24h", "bucket_seconds": 300, "top_n": 30}
        with TestClient(app) as ssr:
            ssr.cookies.update(browser.cookies)
            assert ssr.post("/api/cmcd/aggregates", json=body).json() == payload
        assert browser.get("/api/__e2e__/streaming-fixture").json() == {"bodies": [body]}
        assert other_browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}

        other_browser.post("/api/__e2e__/streaming-fixture", json={"available": False, "has_data": False})
        assert browser.post("/api/cmcd/aggregates", json={}).json() == payload
        assert other_browser.post("/api/cmcd/aggregates", json={}).json() == {"available": False, "has_data": False}

        assert browser.delete("/api/__e2e__/streaming-fixture").status_code == 200
        assert browser.get("/api/__e2e__/streaming-fixture").status_code == 404
        assert browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}


# ── Merged from test_profile_insights.py ──


def test_run_profile_and_cli_output(tmp_path: Path):
    from scripts.dev.profile_insights import format_markdown_report, run_profile
    from scripts.dev.profile_insights import main as profile_main

    res = run_profile(total_rows=1000, runs=2, num_files=3, window_hours=1.0, baseline_hours=24.0)
    assert res["runs"] == 2 and res["dataset"]["total_rows"] == 1000
    assert res["cold_total"]["wall_p50_ms"] > 0 and res["warm_total"]["wall_p50_ms"] >= 0
    assert len(res["top_10_by_wall_p50"]) == 10 and len(res["top_10_by_cpu_p50"]) == 10

    op_names = {op["operation"] for op in res["all_operations"]}
    assert {"phase:scratch_table_build", "coalesced:city", "coalesced:url", "coalesced:ip", "coalesced:waf"}.issubset(
        op_names
    )
    assert any(name.startswith("insight:") for name in op_names)

    md = format_markdown_report(res)
    assert "# Insights Performance Profile" in md and "Top 10 Costs by Wall-Clock p50" in md

    md_out, json_out = tmp_path / "profile.md", tmp_path / "profile.json"
    rc = profile_main(
        ["--rows", "500", "--runs", "1", "--files", "2", "--output", str(md_out), "--json-output", str(json_out)]
    )
    assert rc == 0 and md_out.exists() and json_out.exists()
    assert json.loads(json_out.read_text())["dataset"]["total_rows"] == 500


# ── Merged from test_check_osv.py ──


@pytest.fixture(scope="module")
def osv():
    spec = importlib.util.spec_from_file_location("check_osv", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_osv"] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "vector,expected",
    [
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H", 10.0),
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H", 7.5),
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:C/C:L/I:L/A:N", 6.1),
        ("CVSS:3.1/AV:N/AC:H/PR:L/UI:N/S:U/C:L/I:N/A:N", 3.1),
    ],
)
def test_cvss3_base_score_known_vectors(osv, vector, expected):
    assert osv.cvss3_base_score(vector) == pytest.approx(expected, abs=0.05)


def test_cvss3_base_score_rejects_v4_vector(osv):
    assert osv.cvss3_base_score("CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N") is None


@pytest.mark.parametrize(
    ("vuln", "expected"),
    [
        (
            {
                "id": "CVE-2021-44228",
                "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
            },
            "CRITICAL",
        ),
        (
            {
                "id": "X",
                "database_specific": {"severity": "LOW"},
                "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
            },
            "CRITICAL",
        ),
        ({"id": "X", "database_specific": {"severity": "critical"}}, "CRITICAL"),
        ({"id": "X"}, "UNKNOWN"),
        (
            {
                "id": "CVE-2099-0001",
                "severity": [
                    {"type": "CVSS_V4", "score": "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N"}
                ],
            },
            "UNKNOWN",
        ),
    ],
)
def test_severity_for_vuln_resolution(osv, vuln, expected):
    assert osv.severity_for_vuln(vuln) == expected


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected_rc"),
    [
        (0, '{"results": []}', 0),
        (2, '{"results": []}', 1),
        (127, '{"results": [{"packages": [{"package": {"name": "pkg"}, "vulnerabilities": []}]}]}', 1),
        (-1, '{"results": []}', 1),
        (1, "", 1),
        (1, "not-json", 1),
        (1, "[]", 1),
        (
            1,
            json.dumps(
                {
                    "results": [
                        {
                            "source": {"path": "uv.lock"},
                            "packages": [
                                {
                                    "package": {"name": "foo", "version": "1.0"},
                                    "vulnerabilities": [{"id": "CVE-1", "database_specific": {"severity": "HIGH"}}],
                                }
                            ],
                        }
                    ]
                }
            ),
            0,
        ),
        (
            1,
            json.dumps(
                {
                    "results": [
                        {
                            "source": {"path": "uv.lock"},
                            "packages": [
                                {
                                    "package": {"name": "foo", "version": "1.0"},
                                    "vulnerabilities": [{"id": "CVE-2", "database_specific": {"severity": "CRITICAL"}}],
                                }
                            ],
                        }
                    ]
                }
            ),
            1,
        ),
    ],
)
def test_check_osv_main_exit_code_and_json_handling(
    osv, monkeypatch: pytest.MonkeyPatch, returncode: int, stdout: str, expected_rc: int
) -> None:
    monkeypatch.setattr(
        osv.subprocess,
        "run",
        lambda *_a, **_kw: subprocess.CompletedProcess(
            args=["osv-scanner"], returncode=returncode, stdout=stdout, stderr="scanner stderr"
        ),
    )
    with pytest.raises(SystemExit) as excinfo:
        osv.main()
    if expected_rc == 0:
        assert excinfo.value.code == 0
    else:
        assert excinfo.value.code not in (0, None)


def test_deploy_common_fallback_ssh_requires_verified_host_key(tmp_path: Path) -> None:
    deploy_common = REPO / "scripts" / "lib" / "deploy_common.sh"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    args_file = tmp_path / "ssh_args.txt"
    for tool in ("gcloud", "ssh", "scp"):
        stub = bin_dir / tool
        if tool == "gcloud":
            stub.write_text("#!/usr/bin/env bash\nexit 1\n")
        else:
            stub.write_text(f'#!/usr/bin/env bash\nprintf "%s\\n" "$@" > "{args_file}"\nexit 0\n')
        stub.chmod(0o755)

    empty_home = tmp_path / "home"
    (empty_home / ".ssh").mkdir(parents=True)

    base_env = {
        "PATH": f"{bin_dir}:/usr/bin:/bin",
        "HOME": str(empty_home),
        "GCE_VM_NAME": "vm-test",
        "GCE_ZONE": "us-central1-a",
        "GCE_PROJECT": "proj-test",
        "GCE_VM_SSH": "user@203.0.113.10",
        "FLA_BOUNDED_RUN": "0",
        "GCE_ADMIN_BOOTSTRAP": "0",
    }
    script = f'source "{deploy_common}" && setup_deploy_gce_ssh && "${{GCE_SSH[@]}}" --command "echo hello"'
    res_missing = subprocess.run(["bash", "-c", script], env=base_env, text=True, capture_output=True, check=False)
    assert res_missing.returncode != 0
    assert "no trusted known_hosts file found" in res_missing.stderr

    known_hosts = tmp_path / "known_hosts"
    known_hosts.write_text("203.0.113.10 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAITest\n")
    env_with_kh = {**base_env, "GCE_SSH_KNOWN_HOSTS": str(known_hosts)}
    res_ok = subprocess.run(["bash", "-c", script], env=env_with_kh, text=True, capture_output=True, check=False)
    assert res_ok.returncode == 0, res_ok.stderr
    recorded = args_file.read_text().splitlines()
    assert "StrictHostKeyChecking=yes" in recorded
    assert "StrictHostKeyChecking=no" not in recorded
    assert f"UserKnownHostsFile={known_hosts}" in recorded


# ── Merged from test_port_forward.py ──────────────────────────────────────────────────────

_PF_STUB = """#!{python}
import json, os, signal, socket, subprocess, sys, time
argv = sys.argv[1:]
state = os.environ["STUB_STATE"]
mode = os.environ.get("STUB_MODE", "ok")
local = int(argv[-1].split(":")[0])
n = len([f for f in os.listdir(state) if f.startswith("call-")])
child = subprocess.Popen(["/bin/sleep", "120"])
with open(os.path.join(state, f"call-{{n}}.json"), "w") as fh:
    json.dump({{"argv": argv, "pid": os.getpid(), "child": child.pid}}, fh)
def term(signum, frame):
    open(os.path.join(state, f"term-{{n}}"), "w").close()
    sys.exit(0)
signal.signal(signal.SIGTERM, term)
if mode == "exit-early":
    sys.stderr.write("error: unable to forward port because pod is not running\\n")
    sys.exit(1)
if mode == "never-ready":
    time.sleep(120)
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", local))
srv.listen(5)
print(f"Forwarding from 127.0.0.1:{{local}} -> 80", flush=True)
if mode == "die-once" and n == 0:
    time.sleep(0.6)
    sys.exit(1)
while True:
    time.sleep(0.1)
"""


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def kubectl(tmp_path, monkeypatch):
    state = tmp_path / "state"
    state.mkdir()
    stub = tmp_path / "kubectl"
    stub.write_text(_PF_STUB.format(python=sys.executable))
    stub.chmod(0o755)
    monkeypatch.setenv("STUB_STATE", str(state))

    class K:
        path = str(stub)

        @staticmethod
        def calls() -> list[dict]:
            return [json.loads(p.read_text()) for p in sorted(state.glob("call-*.json"))]

        @staticmethod
        def terminated() -> list[str]:
            return sorted(p.name for p in state.glob("term-*"))

    yield K
    for call in K.calls():
        for pid in (call["pid"], call["child"]):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


def wait_dead(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not alive(pid):
            return True
        time.sleep(0.05)
    return False


def test_argv_and_parse_forward():
    fwd = pf.Forward("svc/back end;$(x)", 8002, 8000, namespace="ns'; rm", context="c x")
    assert fwd.argv("kubectl") == [
        "kubectl",
        "--context",
        "c x",
        "--namespace",
        "ns'; rm",
        "port-forward",
        "--address",
        "127.0.0.1",
        "svc/back end;$(x)",
        "8002:8000",
    ]
    assert pf.parse_forward("svc/backend-svc:8002:8000", namespace="ns") == pf.Forward(
        "svc/backend-svc", 8002, 8000, namespace="ns"
    )


@pytest.mark.parametrize("bad", ["svc/x", "svc/x:80", "svc/x:a:b", ":1:2", "svc/x:0:80", "svc/x:80:70000"])
def test_parse_forward_rejects_malformed_specs(bad):
    with pytest.raises(ValueError):
        pf.parse_forward(bad)


def test_port_in_use_is_refused_before_spawning(kubectl):
    first = free_port()
    for make_specs in (
        lambda taken: [pf.Forward("svc/x", taken, 80)],
        lambda taken: [pf.Forward("svc/a", first, 80), pf.Forward("svc/b", taken, 80)],
    ):
        with socket.socket() as blocker:
            blocker.bind(("127.0.0.1", 0))
            blocker.listen(1)
            with pytest.raises(pf.PortInUse):
                pf.ForwardSet(make_specs(blocker.getsockname()[1]), kubectl=kubectl.path).start()
    assert kubectl.calls() == []


def test_early_exit_and_readiness_timeout_clean_up(kubectl, monkeypatch):
    monkeypatch.setenv("STUB_MODE", "exit-early")
    with pytest.raises(pf.ForwardExited, match="pod is not running"):
        pf.ForwardSet([pf.Forward("svc/x", free_port(), 80)], kubectl=kubectl.path, ready_timeout=15).start()
    assert wait_dead(kubectl.calls()[-1]["child"])

    monkeypatch.setenv("STUB_MODE", "never-ready")
    with pytest.raises(pf.ReadinessTimeout):
        pf.ForwardSet([pf.Forward("svc/x", free_port(), 80)], kubectl=kubectl.path, ready_timeout=2.0).start()
    call = kubectl.calls()[-1]
    assert wait_dead(call["pid"]) and wait_dead(call["child"])


def test_poll_restarts_within_budget_and_raises_when_spent(kubectl, monkeypatch):
    monkeypatch.setenv("STUB_MODE", "die-once")
    port = free_port()
    fwd_set = pf.ForwardSet([pf.Forward("svc/x", port, 80)], kubectl=kubectl.path, ready_timeout=15, max_restarts=1)
    fwd_set.start()
    try:
        deadline = time.monotonic() + 10
        while len(kubectl.calls()) < 2 and time.monotonic() < deadline:
            fwd_set.poll()
            time.sleep(0.1)
        assert len(kubectl.calls()) == 2
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            pass
    finally:
        fwd_set.stop()


def test_poll_raises_when_restart_budget_is_spent(kubectl, monkeypatch):
    monkeypatch.setenv("STUB_MODE", "die-once")
    fwd_set = pf.ForwardSet(
        [pf.Forward("svc/x", free_port(), 80)], kubectl=kubectl.path, ready_timeout=15, max_restarts=0
    )
    fwd_set.start()
    try:
        deadline = time.monotonic() + 10
        with pytest.raises(pf.ForwardExited, match="budget exhausted"):
            while time.monotonic() < deadline:
                fwd_set.poll()
                time.sleep(0.1)
    finally:
        fwd_set.stop()


def _cli(kubectl, *extra):
    return subprocess.Popen(
        [sys.executable, "-m", "scripts.lib.port_forward", "--kubectl", kubectl.path, *extra],
        cwd=REPO,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_cli_cleans_up_on_signal(kubectl, sig):
    port = free_port()
    proc = _cli(kubectl, f"svc/x:{port}:80")
    try:
        assert proc.stdout is not None and proc.stdout.readline().startswith("port-forward ready:")
        (call,) = kubectl.calls()
        proc.send_signal(sig)
        assert proc.wait(timeout=20) == 128 + sig
    finally:
        if proc.poll() is None:
            proc.kill()
    assert wait_dead(call["pid"]) and wait_dead(call["child"]) and not pf.port_in_use(port)


def test_cli_runs_command_and_reports_port_in_use(kubectl):
    port = free_port()
    code = f"import socket,sys; socket.create_connection(('127.0.0.1', {port}), timeout=2).close(); sys.exit(7)"
    proc = _cli(kubectl, f"svc/x:{port}:80", "--", sys.executable, "-c", code)
    out, _ = proc.communicate(timeout=30)
    assert proc.returncode == 7 and "port-forward ready:" in out
    (call,) = kubectl.calls()
    assert wait_dead(call["pid"]) and wait_dead(call["child"])

    with socket.socket() as blocker:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        taken = blocker.getsockname()[1]
        proc2 = _cli(kubectl, f"svc/x:{taken}:80")
        _, err = proc2.communicate(timeout=20)
    assert proc2.returncode == 1 and f"port {taken} is already in use" in err


def test_remote_high_scale_remediation_uses_the_supervisor(monkeypatch):
    monkeypatch.delenv("REMOTE_HIGH_SCALE_FORWARD_CMD", raising=False)
    monkeypatch.setenv("K8S_NAMESPACE", "team-ns")
    cmd = remediation_command("remote-high-scale")
    assert cmd.startswith("python3 -m scripts.lib.port_forward --namespace team-ns ") and "&" not in cmd
    for spec in cmd.split()[-2:]:
        pf.parse_forward(spec)


# ── Merged from test_run_contract_backend.py ──────────────────────────────────────────────────────


def run_probe(code: str, overrides: dict[str, str]) -> subprocess.CompletedProcess:
    env = {key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ}
    env.update(overrides)
    return subprocess.run(
        [sys.executable, "-B", "-c", code], cwd=REPO, env=env, text=True, capture_output=True, timeout=30, check=False
    )


@pytest.mark.parametrize("relative", [False, True])
def test_real_paths_resolve_inside_sandbox(tmp_path, relative):
    sandbox = tmp_path / "sandbox"
    configs, data = sandbox / "configs", sandbox / "data"
    overrides = {
        "CONTRACT_CONFIGS_DIR": str(configs),
        "CONTRACT_DATA_DIR": str(data),
        "REMOTE_SHARE_DB_DIR": str(tmp_path / "wrong-share-dir"),
    }
    if relative:
        overrides = {key: os.path.relpath(value, REPO) for key, value in overrides.items()}
    result = run_probe(
        f"""
import json, os, runpy, sys
from pathlib import Path
def audit(event, args):
    if event in ("sqlite3.connect", "socket.connect"):
        raise AssertionError("Probe must not open databases or network connections")
sys.addaudithook(audit)
launcher = runpy.run_path({str(LAUNCHER)!r})
launcher["_maybe_redirect_config_paths"]()
from backend import config
paths = {{
    "cwd": Path.cwd(),
    "configs": config.CONFIGS_DIR,
    "data": config.DATA_DIR,
    "duckdb": config.duckdb_path("sandbox-service"),
    "usage_config": config._USAGE_LOGGING_CONFIG_PATH,
    "buffer_cache": Path("cache"),
}}
print(json.dumps({{key: str(Path(value).resolve()) for key, value in paths.items()}}))
""",
        overrides,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "cwd": str(sandbox),
        "configs": str(configs),
        "data": str(data),
        "duckdb": str(data / "services/sandbox-service.duckdb"),
        "usage_config": str(data / "system/usage_logging.json"),
        "buffer_cache": str(sandbox / "cache"),
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs"},
        {"CONTRACT_DATA_DIR": "sandbox/data"},
        {"CONTRACT_CONFIGS_DIR": "", "CONTRACT_DATA_DIR": ""},
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs", "CONTRACT_DATA_DIR": "elsewhere/data"},
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs", "CONTRACT_DATA_DIR": "sandbox/custom-data"},
    ],
)
def test_incomplete_or_inconsistent_overrides_fail_before_import(overrides):
    result = run_probe(
        f"""
import runpy, sys
def audit(event, args):
    if event in ("os.mkdir", "sqlite3.connect", "socket.connect"):
        raise AssertionError("Invalid overrides must not cause I/O")
sys.addaudithook(audit)
launcher = runpy.run_path({str(LAUNCHER)!r})
try:
    launcher["_maybe_redirect_config_paths"]()
except ValueError:
    assert "backend.config" not in sys.modules
else:
    raise AssertionError("Unsafe sandbox overrides accepted")
""",
        overrides,
    )
    assert result.returncode == 0, result.stderr


def test_no_overrides_and_main_chdir(tmp_path):
    r1 = run_probe(
        f"""
import os, runpy, sys
launcher = runpy.run_path({str(LAUNCHER)!r})
before = os.getcwd(), list(sys.path), dict(os.environ)
launcher["_maybe_redirect_config_paths"]()
assert before == (os.getcwd(), sys.path, dict(os.environ))
assert "backend.config" not in sys.modules
""",
        {},
    )
    assert r1.returncode == 0, r1.stderr

    r2 = run_probe(
        f"""
import runpy, sys, types
from pathlib import Path
sys.path = [p for p in sys.path if p and Path(p).resolve() != Path({str(REPO)!r})]
def run(app, **kwargs):
    from backend import config
    assert app == "backend.main:app"
    assert Path.cwd() == Path({str(tmp_path)!r})
    assert config.CONFIGS_DIR == Path({str(tmp_path / "configs")!r})
sys.modules["uvicorn"] = types.SimpleNamespace(run=run)
sys.argv = [{str(LAUNCHER)!r}, "--port", "13003"]
runpy.run_path({str(LAUNCHER)!r}, run_name="__main__")
""",
        {"CONTRACT_CONFIGS_DIR": str(tmp_path / "configs"), "CONTRACT_DATA_DIR": str(tmp_path / "data")},
    )
    assert r2.returncode == 0, r2.stderr


# ── Merged from test_measure_e2e_freshness.py ──────────────────────────────────────────────────────


def test_load_environment_config_reads_only_explicit_environment_values():
    config = load_environment_config(
        "local-std",
        {
            "FLA_FRESHNESS_LOCAL_STD_SERVICE_ID": "test-service",
            "FLA_FRESHNESS_LOCAL_STD_BACKEND_URL": "http://127.0.0.1:8000",
            "FLA_FRESHNESS_LOCAL_STD_CDN_DOMAIN": "https://cdn.example.test",
        },
    )
    assert config == {
        "service_id": "test-service",
        "backend_url": "http://127.0.0.1:8000",
        "cdn_domain": "https://cdn.example.test",
        "name": "Local Standard",
    }


def test_load_environment_config_rejects_missing_and_invalid_urls():
    with pytest.raises(ValueError, match="FLA_FRESHNESS_REMOTE_HS_CDN_DOMAIN"):
        load_environment_config(
            "remote-hs",
            {
                "FLA_FRESHNESS_REMOTE_HS_SERVICE_ID": "test-service",
                "FLA_FRESHNESS_REMOTE_HS_BACKEND_URL": "http://127.0.0.1:8000",
            },
        )
    with pytest.raises(ValueError, match="absolute HTTP"):
        load_environment_config(
            "local-std",
            {
                "FLA_FRESHNESS_LOCAL_STD_SERVICE_ID": "test-service",
                "FLA_FRESHNESS_LOCAL_STD_BACKEND_URL": "file:///tmp/backend",
                "FLA_FRESHNESS_LOCAL_STD_CDN_DOMAIN": "https://cdn.example.test",
            },
        )


@pytest.mark.parametrize(
    ("service", "expected"),
    [
        (
            {"log_period": 60, "cron_sync": {"polling_mode": "adaptive", "interval_mins": 2}},
            {
                "mode": "adaptive",
                "interval_seconds": 120,
                "interval_source": "interval_mins",
                "adaptive_followup_interval_seconds": 3,
                "adaptive_max_followups": 2,
            },
        ),
        (
            {"log_period": 60, "cron_sync": {"interval_seconds": 0}},
            {
                "mode": "adaptive",
                "interval_seconds": 30,
                "interval_source": "log_period",
                "adaptive_followup_interval_seconds": 3,
                "adaptive_max_followups": 2,
            },
        ),
        (
            {"log_period": 1, "cron_sync": {"polling_mode": "regular", "interval_seconds": 2}},
            {
                "mode": "regular",
                "interval_seconds": 5,
                "interval_source": "interval_seconds",
                "adaptive_followup_interval_seconds": None,
                "adaptive_max_followups": 0,
            },
        ),
    ],
)
def test_configured_polling_reports_scheduler_interval_and_mode(service, expected):
    assert configured_polling({"services": [service | {"service_id": "svc"}]}, "svc") == expected


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({}, "services response"),
        ({"services": []}, "requested service"),
        ({"services": [{"service_id": "svc", "cron_sync": {"polling_mode": "continuous"}}]}, "polling_mode"),
        ({"services": [{"service_id": "svc", "cron_sync": []}]}, "cron_sync"),
    ],
)
def test_configured_polling_rejects_missing_or_invalid_config(payload, message):
    with pytest.raises(ValueError, match=message):
        configured_polling(payload, "svc")


def test_cron_completion_time_and_new_terminal_run_and_seconds_between():
    assert cron_completion_time({"started_at": "2026-09-29T12:00:00Z", "duration_s": 2.5}) == datetime(
        2026, 9, 29, 12, 0, 2, 500000, tzinfo=UTC
    )
    assert cron_completion_time({"started_at": "invalid", "duration_s": 2}) is None
    assert cron_completion_time({"started_at": "2026-09-29T12:00:00Z", "duration_s": "invalid"}) is None
    assert new_terminal_run({"entries": [{"id": 3, "status": "warning"}]}, 2) == {"id": 3, "status": "warning"}
    for entry in (
        [{"id": 3, "status": "running"}],
        [{"id": 2, "status": "success"}],
        [{"id": "invalid", "status": "success"}],
        [],
    ):
        assert new_terminal_run({"entries": entry}, 2) is None
    later = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)
    assert seconds_between(later, datetime(2026, 9, 29, 12, 0, 1, tzinfo=UTC)) == -1.0
    assert seconds_between(later, None) is None


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"beacons": 5}, 5),
        ({"recent_beacons": "8"}, 8),
        ({"beacons": None, "recent_beacons": 3}, 3),
        ({"beacons": "invalid"}, None),
        ("invalid", None),
    ],
)
def test_rum_beacon_count_supports_standard_and_high_scale_responses(response, expected):
    assert rum_beacon_count(response) == expected


def test_compute_lag_statistics_empty_and_percentiles_and_shared_budget(monkeypatch):
    empty = compute_lag_statistics([])
    assert empty["sample_count"] == 0 and empty["target_status"] == "UNMEASURED"
    assert empty["header_lag"] == {"min": None, "max": None, "avg": None, "p95": None, "p99": None}
    assert empty["dashboard_lag"] == {"min": None, "max": None, "avg": None, "p95": None, "p99": None}

    pct = compute_lag_statistics(
        [{"elapsed_s": 6, "header_lag_s": lag, "dashboard_lag_s": lag} for lag in range(1, 21)]
    )
    assert pct["header_lag"]["p95"] == 19 and pct["header_lag"]["p99"] == 20
    assert pct["dashboard_lag"]["p95"] == 19 and pct["dashboard_lag"]["p99"] == 20

    monkeypatch.setitem(BUDGETS, "peak_freshness_p95_ms", 25000)
    stats = compute_lag_statistics([{"elapsed_s": 6, "header_lag_s": 25.0, "dashboard_lag_s": 25.0}])
    assert stats["target_p95_s"] == 25 and stats["target_status"] == "WITHIN TARGET"


@pytest.mark.parametrize(
    ("header_lag", "dashboard_lag", "expected"),
    [(30.2, 10.0, "ELEVATED"), (10.0, 30.2, "ELEVATED"), (20.0, 20.0, "WITHIN TARGET"), (20.04, 20.04, "ELEVATED")],
)
def test_compute_lag_statistics_assesses_both_p95s_not_the_mean(header_lag, dashboard_lag, expected):
    stats = compute_lag_statistics(
        [
            {"elapsed_s": 6, "header_lag_s": 1.0, "dashboard_lag_s": 1.0},
            {"elapsed_s": 8, "header_lag_s": header_lag, "dashboard_lag_s": dashboard_lag},
        ]
    )
    assert stats["header_lag"]["avg"] < 20 and stats["dashboard_lag"]["avg"] < 20 and stats["target_status"] == expected


@pytest.mark.parametrize(("header_lag", "dashboard_lag"), [(None, 10), (10, None)])
def test_compute_lag_statistics_does_not_pass_an_unmeasured_surface(header_lag, dashboard_lag):
    stats = compute_lag_statistics([{"elapsed_s": 6, "header_lag_s": header_lag, "dashboard_lag_s": dashboard_lag}])
    assert stats["target_status"] == "UNMEASURED"


def test_compute_lag_statistics_filters_pre_traffic_and_computes_stats():
    t0 = datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC)
    samples = [
        {"elapsed_s": 2.0, "header_latest_ts": t0, "header_lag_s": 65.0, "dashboard_lag_s": 70.0},
        {
            "elapsed_s": 4.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 10, tzinfo=UTC),
            "header_lag_s": 10.0,
            "dashboard_lag_s": 12.0,
        },
        {
            "elapsed_s": 6.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 15, tzinfo=UTC),
            "header_lag_s": 15.0,
            "dashboard_lag_s": 16.0,
        },
        {
            "elapsed_s": 8.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 20, tzinfo=UTC),
            "header_lag_s": 20.0,
            "dashboard_lag_s": 22.0,
        },
    ]
    stats = compute_lag_statistics(samples, baseline_latest_ts=t0)
    assert stats["sample_count"] == 3 and stats["total_samples"] == 4
    assert (
        stats["header_lag"]["min"],
        stats["header_lag"]["max"],
        stats["header_lag"]["avg"],
        stats["header_lag"]["p95"],
    ) == (10.0, 20.0, 15.0, 20.0)
    assert (
        stats["dashboard_lag"]["min"],
        stats["dashboard_lag"]["max"],
        stats["dashboard_lag"]["avg"],
        stats["dashboard_lag"]["p95"],
    ) == (12.0, 22.0, 16.7, 22.0)


def test_correlated_vitals_keys_normalise_and_reject_invalid_rows(monkeypatch):
    probe = {"key": "10000001", "sent": 0.0}
    rows = [{"probe_key": 10000001.0, "timestamp": "2026-10-22T00:00:00Z", "matches": 1}]
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (200, {"data": rows, "truncated": False}))
    result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "vitals", [probe])
    assert result["error"] is None and "first_visible" in probe

    req_probe = {"key": "/?probe=owned&seq=0", "sent": 0.0}
    for bad_rows in (
        [{"probe_key": "/unrelated", "timestamp": "2026-10-22T00:00:00Z", "matches": 1}],
        [{"probe_key": req_probe["key"], "timestamp": "2026-10-22T00:00:00Z", "matches": 2}],
        [{"probe_key": req_probe["key"], "timestamp": None, "matches": 1}],
    ):
        monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (200, {"data": bad_rows, "truncated": False}))
        res = freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [req_probe])
        assert res["error"] == "invalid_or_ambiguous_correlation"
        assert "first_visible" not in req_probe and "last_absent" not in req_probe


def test_correlated_sender_separates_payloads_and_never_supplies_request_id(monkeypatch):
    requests = []

    class Response:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class Opener:
        def open(self, request, **kwargs):
            requests.append(request)
            return Response()

    def build(handler):
        assert handler.redirect_request(None, None, 302, "", {}, "https://unapproved.example.test") is None
        return Opener()

    monkeypatch.setattr(freshness.urllib.request, "build_opener", build)
    for stream in ("request", "vitals", "errors"):
        assert freshness.send_correlated_probe("https://edge.example.test/", stream) == 204
    assert requests[0].data is None
    assert "measurements" in json.loads(requests[1].data) and "exceptions" not in json.loads(requests[1].data)
    assert "exceptions" in json.loads(requests[2].data) and "measurements" not in json.loads(requests[2].data)
    assert all(not req.has_header("Fastly-request-id") for req in requests)


@pytest.mark.parametrize(("status", "payload"), [(503, {}), (0, "private failure"), (200, {"data": []})])
def test_correlated_query_failure_is_not_negative_visibility(monkeypatch, status, payload):
    probe = {"key": "10000001", "sent": 0.0}
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (status, payload))
    result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "vitals", [probe])
    assert result["error"] == "query_failed_or_incomplete"
    assert "last_absent" not in probe and "first_visible" not in probe and "private failure" not in str(result)


@pytest.mark.parametrize(
    ("cache", "cached", "current"),
    [
        (None, False, False),
        ({"state": "unknown"}, False, False),
        ({"state": "hit"}, True, False),
        ({"state": "miss", "single_flight": "follower"}, False, False),
        ({"state": "miss"}, True, False),
        ({"state": "miss"}, False, True),
        ({"state": "bypass"}, False, True),
    ],
)
def test_correlated_cached_or_unknown_absence_does_not_advance_interval(monkeypatch, cache, cached, current):
    probe = {"key": "owned", "sent": 0.0, "last_absent": 1.0}
    payload = {"data": [], "truncated": False, "_is_cached": cached}
    if cache is not None:
        payload["_response_cache"] = cache
    monkeypatch.setattr(freshness.time, "monotonic", lambda: 10.0)
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (200, payload))

    result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [probe])

    assert result["error"] is None
    assert probe["last_absent"] == (10.0 if current else 1.0)
    assert result["negative_observation_current"] is current


def test_correlated_query_uses_exact_safe_literals_and_separate_datasets(monkeypatch):
    calls = []
    monkeypatch.setattr(
        freshness,
        "http_post",
        lambda url, body, headers, **kw: (
            calls.append(body),
            (200, {"data": [], "truncated": False, "_response_cache": {"state": "bypass"}}),
        )[1],
    )
    for high_scale in (False, True):
        for stream in ("request", "vitals", "errors"):
            key = "10000001" if stream == "vitals" else "/?customer=O'Brien&probe=owned"
            probe = {"key": key, "sent": 0.0}
            result = freshness.poll_correlated_stream("https://admin.example.test", "svc", stream, [probe], high_scale)
            assert result["error"] is None and "last_absent" in probe
            body = calls[-1]
            column = {"request": "url", "vitals": "metric_value", "errors": "error_message"}[stream]
            literal = "10000001" if stream == "vitals" else "'/?customer=O''Brien&probe=owned'"
            assert f"{column} IN ({literal})" in body["sql"]
            assert "cid" not in body["sql"] and "client_id" not in body["sql"]
            assert body["dataset"] == {"request": "logs", "vitals": "client_vitals", "errors": "client_errors"}[stream]
            assert "LIMIT 100" in body["sql"] and "INTERVAL 1 HOUR" in body["sql"] and body["max_rows"] == 100
            assert ("metric_name = 'LCP'" in body["sql"]) == (stream == "vitals")


def test_correlated_upper_bound_and_censoring_and_population_floor(monkeypatch):
    times = iter([112.0, 115.0, 130.0, 136.0])
    monkeypatch.setattr(freshness.time, "monotonic", lambda: next(times))
    source_time = "2099-01-01T00:00:00Z"
    probe = {"stream": "request", "key": "owned", "sent": 100.0, "response": 101.0, "http_status": 200}
    responses = iter(
        [
            (200, {"data": [], "truncated": False, "_response_cache": {"state": "miss"}}),
            (200, {"data": [{"probe_key": "owned", "timestamp": source_time, "matches": 1}], "truncated": False}),
        ]
    )
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: next(responses))
    freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [probe])
    freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [probe])
    report = freshness.summarize_correlated([probe], ended=140)
    sample = report["request"]["samples"][0]
    assert sample["event_to_queryable_upper_bound_s"] == 36 and sample["send_to_queryable_interval_s"] == [12, 36]
    assert sample["source_timestamp"] == source_time and sample["left_censored"] is False
    assert report["request"]["target_assessment"] == "incomplete_not_proven_failure"
    assert report["vitals"]["observed"] == 0 and report["errors"]["complete"] is False

    bad_probes = [
        {"stream": "vitals", "key": "quiet", "sent": 1, "response": 2, "http_status": 204},
        {"stream": "errors", "key": "bad", "sent": 3, "response": 4, "http_status": 503, "first_visible": 5},
    ]
    bad_rep = freshness.summarize_correlated(bad_probes, ended=30)
    assert bad_rep["vitals"]["right_censored"] == 1 and bad_rep["vitals"]["upper_bound_s"]["p95"] is None
    assert bad_rep["vitals"]["samples"][0]["send_to_queryable_interval_s"] == [0, None]
    assert bad_rep["vitals"]["samples"][0]["followup_duration_s"] == 29
    assert (
        bad_rep["errors"]["http_errors"] == 1
        and bad_rep["errors"]["observed"] == 0
        and bad_rep["errors"]["complete"] is False
    )

    def pop_report(n):
        ps = [
            {"stream": "request", "key": str(i), "sent": 0, "response": 1, "http_status": 200, "first_visible": 15}
            for i in range(n)
        ]
        return freshness.summarize_correlated(ps, ended=20)["request"]

    assert pop_report(29)["p95_population_sufficient"] is False
    sufficient = pop_report(30)
    assert sufficient["p95_population_sufficient"] is True and sufficient["p99_population_sufficient"] is False
    assert sufficient["left_censored"] == 30 and sufficient["target_assessment"] == "within_target_upper_bound"


def test_correlated_sql_finds_only_exact_recent_events(monkeypatch):
    key = "/approved/O'Brien?probe=owned&seq=0"
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE TABLE logs(url VARCHAR, timestamp TIMESTAMPTZ)")
        connection.execute(
            "INSERT INTO logs VALUES (?, CURRENT_TIMESTAMP), (?, CURRENT_TIMESTAMP), (?, CURRENT_TIMESTAMP - INTERVAL 2 HOUR)",
            [key, key + "unrelated", "/old?probe=owned"],
        )

        def post(url, body, headers, **kwargs):
            result = connection.execute(body["sql"])
            columns = [col[0] for col in result.description]
            rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
            for row in rows:
                row["timestamp"] = row["timestamp"].isoformat()
            return 200, {"data": rows, "truncated": False, "_response_cache": {"state": "bypass"}}

        monkeypatch.setattr(freshness, "http_post", post)
        probes = [
            {"stream": "request", "key": k, "sent": 0, "response": 1, "http_status": 200}
            for k in (key, "/old?probe=owned", "/absent?probe=owned")
        ]
        result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", probes)
        assert result["error"] is None and "first_visible" in probes[0]
        assert all("first_visible" not in p and "last_absent" in p for p in probes[1:])


def test_correlated_runner_uses_approved_path_and_separate_paced_beacons(monkeypatch):
    now = [100.0]
    sends, polls = [], []
    monkeypatch.setattr(freshness.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(freshness.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(
        freshness, "http_get", lambda *a, **kw: (200, {"services": [{"service_id": "svc", "rum_enabled": True}]})
    )
    monkeypatch.setattr(
        freshness,
        "send_correlated_probe",
        lambda url, stream, value=None: (
            sends.append((now[0], stream, url, value)),
            204 if stream != "request" else 200,
        )[1],
    )
    monkeypatch.setattr(
        freshness,
        "http_post",
        lambda url, body, headers, **kw: (
            polls.append((now[0], body["dataset"])),
            (200, {"data": [], "truncated": False}),
        )[1],
    )

    report = freshness.measure_correlated_freshness(
        {"service_id": "svc", "backend_url": "https://admin.example.test"},
        "https://edge.example.test/?existing=keep",
        num_requests=1,
        num_rum=1,
        num_errors=1,
        max_wait_s=25,
    )
    assert [s[1] for s in sends] == ["request", "vitals", "errors"]
    assert all(b[0] - a[0] >= 1 for a, b in zip(sends, sends[1:]))
    request_url = urlparse(sends[0][2])
    assert request_url.path == "/" and parse_qs(request_url.query)["existing"] == ["keep"]
    assert parse_qs(sends[1][2].split("?", 1)[1])["cid"] != parse_qs(sends[2][2].split("?", 1)[1])["cid"]
    vitals_qs = parse_qs(sends[1][2].split("?", 1)[1])
    assert vitals_qs["rum_metric_value"] == [sends[1][3]] and int(sends[1][3]) >= 10_000_000
    errors_qs = parse_qs(sends[2][2].split("?", 1)[1])
    assert errors_qs["rum_error_message"] == [sends[2][3]] and sends[2][3].startswith("fla-probe-")
    assert sends[0][3] is None
    for dataset in ("logs", "client_vitals", "client_errors"):
        times = [t for t, d in polls if d == dataset]
        assert all(b - a >= 10 for a, b in zip(times, times[1:]))
    assert report["qualification"] == "incomplete"
    assert {"peak_conditions_not_verified", "request_population_incomplete_or_below_floor"} <= set(
        report["qualification_gaps"]
    )
    assert report["measurement_contract"]["event_clock_point_latency_s"] is None
    assert report["streams"]["request"]["right_censored"] == 1 and "probe=" not in json.dumps(report)
    assert "query_cache_effects_unseparated" in report["qualification_gaps"]
    assert "query_cache_effects_unseparated" in report["unknowns"]


@pytest.mark.parametrize("kwargs", [{"poll_interval_s": 9}, {"num_requests": 101}, {"max_wait_s": 601}])
def test_correlated_runner_rejects_unsafe_limits_before_io(monkeypatch, kwargs):
    def _unexpected_http_get(*_a, **_kw):
        raise AssertionError("unsafe configuration reached I/O")

    monkeypatch.setattr(freshness, "http_get", _unexpected_http_get)
    with pytest.raises(ValueError):
        freshness.measure_correlated_freshness(
            {"service_id": "svc", "backend_url": "https://admin.example.test"}, "https://edge.example.test/", **kwargs
        )


def test_print_active_scorecard_verified_and_ended_span_guard(capsys):
    from backend.core.request_telemetry.tracker import RequestTelemetry

    freshness.print_active_scorecard(
        [
            {
                "name": "Local Standard",
                "deployment_mode": "standard",
                "qualification": "verified",
                "active_benchmark": {
                    "header_lag": {"min": 1.0, "max": 3.0, "avg": 2.0, "p95": 3.0},
                    "dashboard_lag": {"avg": 2.5},
                    "requests_sent": 300,
                    "target_status": "WITHIN TARGET",
                    "request_event_latency_s": 2.0,
                    "rum_lag_s": 4.1,
                    "clock_uncertainty_s": 0.01,
                },
            }
        ]
    )
    out = capsys.readouterr().out
    assert "Qualification verified: request/RUM event latency and clock uncertainty measured" in out
    assert "Local Standard: req=2.0s rum=4.1s clock=±0.01s" in out

    class _EndedSpan:
        def is_recording(self) -> bool:
            return False

        def set_attribute(self, *_a, **_kw) -> None:
            raise AssertionError("set_attribute called on ended span")

    rt = RequestTelemetry("GET", "/api/health")
    rt._root_span = _EndedSpan()  # type: ignore[assignment]
    rt.end_request(200)
    assert rt._root_span is None
