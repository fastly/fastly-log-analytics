"""Characterization of the health/audit scripts on the shared probe API (S1/S11).

A local stub backend stands in for an environment. Expected values here were
produced by the pre-refactor hand-rolled ``urllib`` implementations, so they
pin behavior across the move onto ``scripts.lib.readiness.probe``.
"""

from __future__ import annotations

import json
import re
import threading
import time
import urllib.error
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from scripts import check_environment_health as health
from scripts.dev import audit_environments as audit
from scripts.lib import readiness

HEALTHY: dict[str, tuple[int, Any]] = {
    "GET /api/health": (200, {"status": "ok", "version": "1.2.3"}),
    "GET /api/bootstrap": (200, {"active_service_id": "svc1"}),
    "POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz": (404, {"error": "nope"}),
    "GET /api/share/auth-config": (200, {"passcode_enabled": True}),
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
        return health.EnvironmentTarget(
            name=name,
            architecture="Stub",
            backend_url=self.origin,
            frontend_url=self.origin,
            fastly_service_id="svc1",
            remediation_cmd="fix-it",
        )


@pytest.fixture
def stub(monkeypatch: pytest.MonkeyPatch) -> Iterator[Stub]:
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
    """Module stand-in that overrides sleep only for the audit module (the real ``time.sleep`` is global)."""

    def __init__(self, sleep: Any) -> None:
        self.sleep = sleep

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


def dead_origin() -> str:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{sock.getsockname()[1]}"


def test_probe_reports_status_body_and_latency(stub: Stub) -> None:
    result = readiness.probe(f"{stub.origin}/api/health", timeout=5)
    assert (result.status, result.failed, result.http_error) == (200, False, False)
    assert result.json() == {"status": "ok", "version": "1.2.3"}
    assert result.content_type == "application/json"
    assert result.latency_ms >= 0


def test_probe_http_error_keeps_status_and_bounded_body(stub: Stub) -> None:
    stub.routes["GET /api/health"] = (503, {"error": "busy"})
    result = readiness.probe(f"{stub.origin}/api/health", timeout=5)
    assert (result.status, result.http_error, result.reason) == (503, True, "Service Unavailable")
    assert result.error == "HTTP Error 503: Service Unavailable"
    assert result.json() == {"error": "busy"}


def test_probe_transport_failure_never_raises() -> None:
    result = readiness.probe(f"{dead_origin()}/x", timeout=2)
    assert result.status is None and result.failed and not result.http_error
    assert isinstance(result.exc, urllib.error.URLError)


def test_probe_delegates_to_admin_urlopen_with_request_details(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake(request: Any, timeout: float) -> Any:
        captured.update(url=request.full_url, method=request.get_method(), data=request.data, timeout=timeout)
        captured["headers"] = dict(request.header_items())
        raise OSError("stop")

    monkeypatch.setattr(readiness, "admin_urlopen", fake)
    result = readiness.probe("http://x/y", timeout=7, headers={"X-A": "1"}, data=b"{}", method="POST")
    assert result.error == "stop"
    assert captured == {
        "url": "http://x/y",
        "method": "POST",
        "data": b"{}",
        "timeout": 7,
        "headers": {"X-a": "1"},
    }


def test_health_ok(stub: Stub) -> None:
    result = health.check_backend_health(stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version, result.error) == (True, 200, "1.2.3", None)
    headers = next(h for line, h, _ in stub.seen if line == "GET /api/health")
    assert headers["User-Agent"] == "FLA-HealthCheck/1.0"
    assert headers["Accept"] == "application/json"


def test_health_http_error(stub: Stub) -> None:
    stub.routes["GET /api/health"] = (503, {"x": 1})
    result = health.check_backend_health(stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version) == (False, 503, None)
    assert result.error == "HTTPError 503: Service Unavailable"


def test_health_unexpected_payload(stub: Stub) -> None:
    stub.routes["GET /api/health"] = (200, {"status": "degraded", "version": "9"})
    result = health.check_backend_health(stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version) == (False, 200, "9")
    assert result.error == 'Unexpected payload: {"status": "degraded", "version": "9"}'


def test_health_non_json_body(stub: Stub) -> None:
    stub.routes["GET /api/health"] = (200, b"<html>")
    result = health.check_backend_health(stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version) == (False, None, None)
    assert result.error == "Expecting value: line 1 column 1 (char 0)"


def test_health_connection_refused() -> None:
    target = health.EnvironmentTarget("d", "A", dead_origin(), "", "svc1", "fix")
    result = health.check_backend_health(target, timeout_s=2)
    assert (result.backend_ok, result.status_code) == (False, None)
    assert result.error and "Connection refused" in result.error


def test_tenancy_ok_posts_expected_probe(stub: Stub) -> None:
    result = health.check_tenancy(stub.target(), timeout_s=5)
    assert (result.service_match, result.rejection_ok, result.rejection_code, result.error) == (True, True, 404, None)
    assert result.resolved_service_id == "svc1"
    line, headers, body = next(s for s in stub.seen if s[0].startswith("POST"))
    assert line == "POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"
    assert json.loads(body) == {"time_range": "24h"}
    assert headers["Content-Type"] == "application/json"


def test_tenancy_breach_when_fake_tenant_gets_200(stub: Stub) -> None:
    stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (200, {})
    result = health.check_tenancy(stub.target(), timeout_s=5)
    assert (result.rejection_ok, result.rejection_code) == (False, 200)
    assert result.error == "Tenancy breach: fake tenant returned HTTP 200 (expected 400/401/403/404)"


def test_tenancy_unsafe_5xx_is_not_a_rejection(stub: Stub) -> None:
    stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (500, {})
    result = health.check_tenancy(stub.target(), timeout_s=5)
    assert (result.rejection_ok, result.rejection_code) == (False, 500)


def test_tenancy_service_mismatch(stub: Stub) -> None:
    stub.routes["GET /api/bootstrap"] = (200, {"active_service_id": "other"})
    result = health.check_tenancy(stub.target(), timeout_s=5)
    assert (result.service_match, result.rejection_ok) == (False, True)
    assert result.error == "Service mismatch: got 'other', expected 'svc1'"


@pytest.mark.parametrize(
    ("route", "message"),
    [
        ((500, {"e": 1}), "Failed to fetch bootstrap: HTTP Error 500: Internal Server Error"),
        ((200, b"nope"), "Failed to fetch bootstrap: Expecting value: line 1 column 1 (char 0)"),
        ((200, [1]), "Failed to fetch bootstrap: 'list' object has no attribute 'get'"),
    ],
)
def test_tenancy_bootstrap_failures(stub: Stub, route: tuple[int, Any], message: str) -> None:
    stub.routes["GET /api/bootstrap"] = route
    result = health.check_tenancy(stub.target(), timeout_s=5)
    assert (result.resolved_service_id, result.service_match, result.rejection_code) == (None, False, None)
    assert result.error == message
    assert not any(line.startswith("POST") for line, _, _ in stub.seen)


def test_rbac_ok_sends_remote_analyst_header(stub: Stub) -> None:
    result = health.check_rbac(stub.target(), timeout_s=5)
    assert (result.auth_config_ok, result.unauth_analyst_blocked, result.unauth_analyst_code, result.error) == (
        True,
        True,
        200,
        None,
    )
    headers = next(h for line, h, _ in stub.seen if line == "GET /api/share/auth-config")
    assert headers["X-Remote-Analyst"] == "1"


def test_rbac_missing_flag_and_http_error(stub: Stub) -> None:
    stub.routes["GET /api/share/auth-config"] = (200, {"other": 1})
    result = health.check_rbac(stub.target(), timeout_s=5)
    assert result.auth_config_ok is False and result.unauth_analyst_blocked is True
    assert result.error == "Auth config endpoint failed or returned unexpected payload"
    stub.routes["GET /api/share/auth-config"] = (404, {})
    result = health.check_rbac(stub.target(), timeout_s=5)
    assert (result.auth_config_ok, result.unauth_analyst_blocked, result.unauth_analyst_code) == (False, False, None)
    assert result.error == "Failed to query /api/share/auth-config: HTTP Error 404: Not Found"


def _run_main(monkeypatch: pytest.MonkeyPatch, target: health.EnvironmentTarget, *extra: str) -> int:
    monkeypatch.setattr(health, "ENVIRONMENTS", {target.name: target})
    monkeypatch.setattr("sys.argv", ["check_environment_health.py", "--env", target.name, *extra])
    return health.main()


def test_main_text_report_golden(
    stub: Stub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_main(monkeypatch, stub.target("stub-env")) == 0
    out = re.sub(r"\d+\.\dms", "<lat>", capsys.readouterr().out)
    assert out.splitlines()[6:] == [
        "stub-env               | PASS (200) | svc1 (OK)              | PASS (404)  | PASS (200) | <lat>",
        "=" * 96,
        "ALL TARGET ENVIRONMENTS HEALTHY, TENANCY VERIFIED & RBAC ENFORCED!",
        "=" * 96,
        "",
    ]


def test_main_text_report_failure_golden(
    stub: Stub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    stub.routes["GET /api/health"] = (503, {})
    assert _run_main(monkeypatch, stub.target("stub-env")) == 1
    out = re.sub(r"\d+\.\dms", "<lat>", capsys.readouterr().out)
    lines = out.splitlines()[6:]
    assert lines[0].startswith("stub-env               | FAIL     | svc1 (OK)")
    assert lines[1:3] == ["   [!] Health Error: HTTPError 503: Service Unavailable", "   [!] Fix command: fix-it"]
    assert "FAILURE: 1/1 environments failed checks." in lines


def test_main_json_report_golden(
    stub: Stub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_main(monkeypatch, stub.target("stub-env"), "--json") == 0
    (row,) = json.loads(capsys.readouterr().out)
    row.pop("latency_ms")
    assert row == {
        "env": "stub-env",
        "architecture": "Stub",
        "service_id": "svc1",
        "backend_url": stub.origin,
        "health_ok": True,
        "health_status_code": 200,
        "version": "1.2.3",
        "tenancy_service_match": True,
        "tenancy_rejection_ok": True,
        "tenancy_rejection_code": 404,
        "rbac_auth_config_ok": True,
        "rbac_unauth_blocked": True,
        "rbac_unauth_code": 200,
        "error": None,
        "remediation": None,
    }


def test_audit_get_json(stub: Stub) -> None:
    status, data = audit.http_get_json(f"{stub.origin}/api/health", headers={"X-T": "1"}, timeout=5)
    assert (status, data) == (200, {"status": "ok", "version": "1.2.3"})
    _, headers, _ = stub.seen[-1]
    assert headers["User-Agent"] == "FLA-AuditTool/1.0" and headers["X-T"] == "1"


def test_audit_get_json_error_shapes(stub: Stub) -> None:
    stub.routes["GET /e1"] = (500, {"detail": "boom"})
    stub.routes["GET /e2"] = (502, b"<html>")
    stub.routes["GET /e3"] = (200, b"<html>")
    assert audit.http_get_json(f"{stub.origin}/e1") == (500, {"detail": "boom"})
    assert audit.http_get_json(f"{stub.origin}/e2") == (502, {"error": "HTTP Error 502: Bad Gateway"})
    assert audit.http_get_json(f"{stub.origin}/e3") == (0, {"error": "Expecting value: line 1 column 1 (char 0)"})
    status, data = audit.http_get_json(f"{dead_origin()}/x", timeout=2)
    assert status == 0 and "Connection refused" in data["error"]


def test_audit_post_json_sends_payload_and_parses_error_body(stub: Stub) -> None:
    stub.routes["POST /p"] = (422, {"detail": "bad"})
    assert audit.http_post_json(f"{stub.origin}/p", {"a": 1}, timeout=5) == (422, {"detail": "bad"})
    line, headers, body = stub.seen[-1]
    assert (line, json.loads(body)) == ("POST /p", {"a": 1})
    assert headers["Content-Type"] == "application/json"
    stub.routes["POST /q"] = (200, {"ok": True})
    assert audit.http_post_json(f"{stub.origin}/q", timeout=5) == (200, {"ok": True})
    assert json.loads(stub.seen[-1][2]) == {}


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


def test_audit_post_json_does_not_retry_http_errors(stub: Stub, monkeypatch: pytest.MonkeyPatch) -> None:
    stub.routes["POST /p"] = (500, {"e": 1})
    monkeypatch.setattr(audit, "time", _SleepStub(lambda _s: pytest.fail("must not retry HTTP errors")))
    assert audit.http_post_json(f"{stub.origin}/p", {}, timeout=5) == (500, {"e": 1})
    assert [line for line, _, _ in stub.seen].count("POST /p") == 1


def test_load_persisted_deploy_env_populates_admin_origins_and_respects_existing_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    from scripts.lib import environments

    tracked_keys = (
        "FLA_ADMIN_DEPLOY_ENV",
        "LOCAL_STANDARD_ADMIN_MTLS",
        "GCE_ADMIN_MTLS",
        "ELEVATION_ADMIN_MTLS",
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
        f"LOCAL_STANDARD_ADMIN_MTLS=1\n"
        f"GCE_ADMIN_MTLS=1\n"
        f"ELEVATION_ADMIN_MTLS=1\n"
        f"ADMIN_GATEWAY_ENDPOINTS='{json.dumps(endpoints)}'\n"
    )
    deploy_env.chmod(0o600)
    monkeypatch.setenv("FLA_ADMIN_DEPLOY_ENV", str(deploy_env))
    monkeypatch.setenv("GCE_ADMIN_MTLS", "0")

    try:
        loaded = environments.load_persisted_deploy_env()
        assert loaded["LOCAL_STANDARD_ADMIN_MTLS"] == "1"
        assert loaded["ELEVATION_ADMIN_MTLS"] == "1"
        # Existing process env wins over persisted deploy.env
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
        audit,
        "http_get_json",
        lambda url, headers=None, timeout=5.0: (0, {"error": "<urlopen error timed out>"}),
    )
    env = audit.EnvironmentConfig(
        name="remote-high-scale",
        architecture="High-Scale (Remote/K8s)",
        backend_url="https://10.0.0.10:8443",
        frontend_url="https://10.0.0.10:8443/dashboard",
        service_id="svc-hs",
        is_high_scale=True,
    )
    res = audit.audit_target(env, timeout=1.0)
    assert not res.reachable
    assert any("VPN/tunnel" in err and "10.0.0.10:8443" in err for err in res.errors)
