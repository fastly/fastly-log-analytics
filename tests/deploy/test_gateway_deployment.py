from __future__ import annotations

"""Deployment patches retain Helm ownership and preserve unrelated settings."""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/deploy_admin_gateway.py"
SPEC = importlib.util.spec_from_file_location("deploy_admin_gateway", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_application_patch_keeps_unrelated_env_and_appends_exact_host():
    current = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "backend",
                            "env": [
                                {"name": "LOCAL_HOSTS", "value": "backend-svc,frontend-svc"},
                                {"name": "ADMIN_GATEWAY_SECRET", "value": "obsolete"},
                                {
                                    "name": "UNCHANGED",
                                    "valueFrom": {"secretKeyRef": {"name": "original", "key": "value"}},
                                },
                            ],
                        }
                    ]
                }
            }
        }
    }
    patch = MODULE.application_patch(current, "backend", "operator-secret", "admin.example.com", True)
    env = patch["spec"]["template"]["spec"]["containers"][0]["env"]
    assert env[0] == {"$patch": "replace"}
    mapped = {item["name"]: item for item in env[1:]}
    assert mapped["LOCAL_HOSTS"]["value"] == "backend-svc,frontend-svc,admin.example.com"
    assert mapped["ADMIN_GATEWAY_REQUIRED"]["value"] == "1"
    assert "value" not in mapped["ADMIN_GATEWAY_SECRET"]
    assert mapped["UNCHANGED"]["valueFrom"]["secretKeyRef"]["name"] == "original"


def test_virtualserver_patch_strips_credentials_but_preserves_analyst_marker():
    current = {
        "spec": {
            "routes": [
                {"path": "/api", "action": {"pass": "backend"}},
                {
                    "path": "/",
                    "action": {
                        "proxy": {
                            "upstream": "frontend",
                            "requestHeaders": {
                                "set": [
                                    {"name": "X-Remote-Analyst", "value": "true"},
                                    {"name": "x-admin-gateway-token", "value": "obsolete"},
                                ]
                            },
                            "responseHeaders": {"hide": ["Server"]},
                        }
                    },
                },
            ]
        }
    }
    patch = MODULE.public_routes_patch(current)
    for route in patch["spec"]["routes"]:
        proxy = route["action"]["proxy"]
        headers = {entry["name"].lower(): entry["value"] for entry in proxy["requestHeaders"]["set"]}
        assert headers["x-admin-gateway-token"] == ""
        assert headers["x-proxied-by-caddy"] == "true"
    frontend = patch["spec"]["routes"][1]["action"]["proxy"]
    assert {"name": "X-Remote-Analyst", "value": "true"} in frontend["requestHeaders"]["set"]
    assert frontend["responseHeaders"]["hide"] == ["Server"]
    assert current["spec"]["routes"][0]["action"] == {"pass": "backend"}


def test_virtualserver_unsupported_routes_fail_closed():
    with pytest.raises(ValueError, match="unsupported route"):
        MODULE.public_routes_patch({"spec": {"routes": [{"path": "/", "action": {"redirect": {"url": "/"}}}]}})


def test_frontend_activation_requires_gateway_and_replaces_old_literal_secret():
    current = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "frontend",
                            "env": [
                                {"name": "ADMIN_GATEWAY_SECRET", "value": "obsolete"},
                                {"name": "ADMIN_GATEWAY_REQUIRED", "value": "0"},
                                {"name": "API_PROXY_URL", "value": "http://backend:8000"},
                            ],
                        }
                    ]
                }
            }
        }
    }
    patch = MODULE.application_patch(current, "frontend", "operator-secret", "admin.example.com", False)
    entries = patch["spec"]["template"]["spec"]["containers"][0]["env"][1:]
    env = {entry["name"]: entry for entry in entries}
    assert env["ADMIN_GATEWAY_REQUIRED"]["value"] == "1"
    assert env["ADMIN_GATEWAY_SECRET"]["valueFrom"]["secretKeyRef"]["name"] == "operator-secret"
    assert "value" not in env["ADMIN_GATEWAY_SECRET"]
    assert env["API_PROXY_URL"]["value"] == "http://backend:8000"


@pytest.mark.parametrize(
    "image",
    [
        "registry.example.com/mirror/library/caddy:2.11.4-alpine",
        "registry.example.com:5000/caddy@sha256:" + "b" * 64,
        "registry.example.com/mirror/caddy:2.11.4-alpine@sha256:" + "c" * 64,
    ],
)
def test_image_reference_accepts_registry_tag_or_digest(image):
    assert MODULE.IMAGE_REF.fullmatch(image)


@pytest.mark.parametrize("image", ["caddy", "registry.example.com/caddy latest", "Registry/Caddy:1", "x/y:$(id)"])
def test_image_reference_rejects_malformed(image):
    assert not MODULE.IMAGE_REF.fullmatch(image)


def test_helm_diagnostics_are_redacted(monkeypatch):
    secret = "s" * 40
    monkeypatch.setattr(MODULE, "SENSITIVE", [secret])
    stderr = (
        "Error: admission webhook denied: emptydir-sizelimit: sizeLimit required\n"
        f"token={secret}\n-----BEGIN PRIVATE KEY-----\nMIIabc\n-----END PRIVATE KEY-----\n" + "Q" * 60
    )

    class Result:
        returncode = 1
        stdout = ""

    Result.stderr = stderr
    monkeypatch.setattr(MODULE.subprocess, "run", lambda *a, **k: Result())
    with pytest.raises(ValueError) as caught:
        MODULE.run("helm", "upgrade", diagnose=True)
    message = str(caught.value)
    assert "emptydir-sizelimit" in message
    assert secret not in message and "MIIabc" not in message and "Q" * 60 not in message
    with pytest.raises(ValueError) as payload_failure:
        MODULE.run("kubectl", "apply", payload={"data": secret}, diagnose=True)
    assert "admission" not in str(payload_failure.value)
    with pytest.raises(ValueError) as default_failure:
        MODULE.run("helm", "upgrade")
    assert "admission" not in str(default_failure.value)


def test_gateway_phase_requires_purpose_built_image():
    # Fails during argument validation, before any cluster access.
    args = [sys.executable, str(SCRIPT), "--phase", "gateway", "--dry-run", "--namespace", "example"]
    for flag in (
        "host",
        "cert-dir",
        "backend-upstream",
        "frontend-upstream",
        "backend-deployment",
        "frontend-deployment",
        "public-virtualserver",
    ):
        args += [f"--{flag}", "admin.example.com" if flag == "host" else "placeholder"]
    result = subprocess.run(args, capture_output=True, text=True)
    assert result.returncode == 2
    assert "--image is required for gateway" in result.stderr


# ── Merged from test_compose_yaml_hook.py ──────────────────────────────────────────────────────

"""Known Compose tags receive narrow syntax validation, not blanket exclusions."""

import re
from pathlib import Path

import yaml


def test_custom_compose_tags_use_narrow_syntax_hook():
    root = Path(__file__).resolve().parents[2]
    config = yaml.safe_load((root / ".pre-commit-config.yaml").read_text())
    hooks = [hook for repo in config["repos"] for hook in repo["hooks"] if hook["id"] == "check-yaml"]
    generic = next(hook for hook in hooks if "alias" not in hook)
    tagged = next(hook for hook in hooks if hook.get("alias") == "check-compose-tagged-yaml")
    assert tagged["args"] == ["--unsafe"]
    for filename in (
        "docker-compose.prod.yml",
        "docker-compose.admin-mtls.prod.yml",
    ):
        assert re.search(generic["exclude"], filename)
        assert re.search(tagged["files"], filename)
    for filename in ("docker-compose.yml", "docker-compose.admin-mtls.yml", "config.yml", ".github/workflows/ci.yml"):
        assert not re.search(generic["exclude"], filename)
        assert not re.search(tagged["files"], filename)


# ── Merged from tests/deploy/test_otel_deployment_config.py ──

import json
import os
import shutil
import socket
import threading
import time
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[2]
OTEL_ENV = (
    "OTEL_EXPORTER",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
)


def _render_prod_compose(otel_values: dict[str, str] | None = None) -> dict:
    env = os.environ.copy()
    for name in OTEL_ENV:
        env.pop(name, None)
    env.update(otel_values or {})
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            "docker-compose.yml",
            "-f",
            "docker-compose.prod.yml",
            "config",
            "--format",
            "json",
        ],
        cwd=ROOT,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_prod_compose_keeps_otel_export_disabled_by_default() -> None:
    backend_env = _render_prod_compose()["services"]["backend"]["environment"]

    assert backend_env["OTEL_EXPORTER"] == "none"
    assert backend_env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == ""
    assert backend_env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == ""


def test_prod_compose_passes_operator_otel_settings_without_defaults() -> None:
    metrics = "metrics-endpoint-from-operator"
    traces = "traces-endpoint-from-operator"
    backend_env = _render_prod_compose(
        {
            "OTEL_EXPORTER": "otlp",
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": metrics,
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": traces,
        }
    )["services"]["backend"]["environment"]

    assert backend_env["OTEL_EXPORTER"] == "otlp"
    assert backend_env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == metrics
    assert backend_env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == traces


# ── Merged from tests/deploy/test_public_gateway_runtime.py ──


class HeaderEcho(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(dict(self.headers)).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def test_public_gateway_strips_both_admin_credentials_and_keeps_analyst_marker(tmp_path):
    if not shutil.which("caddy"):
        if os.getenv("ADMIN_MTLS_TEST_REQUIRED") == "1":
            pytest.fail("Missing validation tool: caddy >=2.8 (install explicitly)")
        pytest.skip("Optional Caddy runtime not installed; scripts/validate_admin_gateway.sh requires it")
    rendered = subprocess.run(
        [
            "helm",
            "template",
            "public-test",
            str(ROOT / "deploy/chart/fastly-log-analytics"),
            "--set",
            "adminGateway.enabled=true",
            "--set",
            "adminGateway.existingSecret=operator-secret",
            "--set",
            "adminGateway.host=admin.example.com",
            "--set",
            "adminGateway.image=registry.example.com/fla-admin-gateway:test",
        ],
        capture_output=True,
        text=True,
    )
    assert rendered.returncode == 0, rendered.stderr
    docs = list(yaml.safe_load_all(rendered.stdout))
    config_map = next(
        doc for doc in docs if doc["kind"] == "ConfigMap" and doc["metadata"]["name"].endswith("-public-gateway")
    )
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), HeaderEcho)
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
    config = config_map["data"]["Caddyfile"].replace(":8080 {", f":{port} {{")
    for component, app_port in (("backend", 8000), ("frontend", 3000)):
        config = config.replace(
            f"public-test-fastly-log-analytics-{component}:{app_port}", f"127.0.0.1:{upstream.server_port}"
        )
    path = tmp_path / "Caddyfile"
    path.write_text(config)
    process = subprocess.Popen(
        ["caddy", "run", "--config", str(path), "--adapter", "caddyfile"],
        env={**os.environ, "XDG_DATA_HOME": str(tmp_path / "data"), "XDG_CONFIG_HOME": str(tmp_path / "config")},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert process.poll() is None, "Public Caddy failed to start"
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.05)
        else:
            pytest.fail("Public Caddy readiness timed out")
        for request_path in ("/dashboard", "/api/bootstrap"):
            connection = HTTPConnection("127.0.0.1", port, timeout=3)
            try:
                connection.request(
                    "GET",
                    request_path,
                    headers={
                        "X-Admin-Gateway-Token": "forged",
                        "X-Remote-Analyst": "true",
                    },
                )
                response = connection.getresponse()
                assert response.status == 200
                headers = {key.lower(): value for key, value in json.loads(response.read()).items()}
                assert "x-admin-gateway-token" not in headers
                assert headers["x-remote-analyst"] == "true"
                assert headers["x-proxied-by-caddy"] == "true"
            finally:
                connection.close()
    finally:
        process.terminate()
        process.communicate(timeout=10)
        upstream.shutdown()
        upstream.server_close()
        thread.join(timeout=5)
        for child in tuple(tmp_path.iterdir()):
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()


# ── Merged from tests/deploy/test_readiness.py ───────────────────────────────

from scripts.lib import readiness

SECRET_BODY = "never-print-this-body"
ADMIN = {"settings": {"is_remote_analyst": False}, "services": [{"id": "svc"}], "note": SECRET_BODY}


class Stub:
    def __init__(self):
        self.routes = {
            "/dashboard": (200, "text/html; charset=utf-8", f"<html>{SECRET_BODY}</html>"),
            "/api/health": (200, "application/json", json.dumps({"status": "ok", "version": "3.0.0-beta3"})),
            "/api/bootstrap": (200, "application/json", json.dumps(ADMIN)),
        }
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                status, content_type, body = stub.routes.get(self.path, (404, "text/plain", "missing"))
                payload = body.encode()
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def json(self, path, payload, status=200):
        self.routes[path] = (status, "application/json", json.dumps(payload))


@pytest.fixture
def stub(monkeypatch):
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


def run(stub, capsys, *extra):
    code = readiness.main(
        [
            "--name",
            "Env",
            "--frontend",
            stub.origin,
            "--backend",
            stub.origin,
            "--attempts",
            "2",
            "--interval",
            "0",
            *extra,
        ]
    )
    out = capsys.readouterr()
    assert SECRET_BODY not in out.out + out.err
    return code, out.out


def test_ready_checks_frontend_backend_and_admin(stub, capsys):
    code, out = run(stub, capsys, "--expect-version", "3.0.0b3")
    assert code == 0
    assert "ready on attempt 1/2: frontend, backend, admin OK" in out


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda s: s.routes.update({"/dashboard": (500, "text/html", "x")}), "frontend /dashboard HTTP 500"),
        (lambda s: s.routes.update({"/dashboard": (200, "application/json", "{}")}), "not HTML"),
        (lambda s: s.json("/api/health", {"status": "initializing", "version": "3.0.0-beta3"}), "status=initializing"),
        (lambda s: s.json("/api/health", {}, status=503), "backend /api/health HTTP 503"),
        (
            lambda s: s.json("/api/bootstrap", {"settings": {"is_remote_analyst": True, "needs_login": True}}),
            "not an admin",
        ),
        (lambda s: s.json("/api/bootstrap", {"settings": {"is_remote_analyst": False}, "services": []}), "0 services"),
        (lambda s: s.routes.update({"/api/bootstrap": (200, "application/json", "not json")}), "not JSON"),
    ],
)
def test_each_unready_side_fails_after_bounded_attempts(stub, capsys, mutate, reason):
    mutate(stub)
    code, out = run(stub, capsys)
    assert code == 1
    assert out.count("not ready") == 2
    assert "FAILED readiness after 2 attempts" in out
    assert reason in out


def test_version_mismatch_fails(stub, capsys):
    code, out = run(stub, capsys, "--expect-version", "9.9.9")
    assert code == 1
    assert "backend version 3.0.0-beta3 != expected 9.9.9" in out


def test_no_admin_skips_bootstrap(stub, capsys):
    stub.json("/api/bootstrap", {"settings": {"is_remote_analyst": True, "needs_login": True}})
    code, out = run(stub, capsys, "--no-admin")
    assert code == 0
    assert "frontend, backend OK" in out


def test_recovers_within_attempts(stub, capsys):
    stub.json("/api/health", {"status": "initializing", "version": "3.0.0-beta3"})
    real = readiness.check_backend
    calls = []

    def flaky(*args):
        calls.append(1)
        if len(calls) == 2:
            stub.json("/api/health", {"status": "ok", "version": "3.0.0-beta3"})
        return real(*args)

    readiness.check_backend = flaky
    try:
        code, out = run(stub, capsys)
    finally:
        readiness.check_backend = real
    assert code == 0
    assert "attempt 1/2 not ready" in out


def test_unreachable_reports_exception_type_only(capsys):
    code = readiness.main(
        ["--name", "Down", "--frontend", "http://127.0.0.1:9", "--backend", "http://127.0.0.1:9", "--attempts", "1"]
    )
    out = capsys.readouterr().out
    assert code == 1
    assert "frontend request failed: URLError" in out


def test_invalid_tls_configuration_exits_2(stub, capsys, monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_ENDPOINTS", "not-json")
    code = readiness.main(["--name", "Cfg", "--frontend", stub.origin, "--backend", stub.origin])
    assert code == 2
    assert "readiness configuration invalid" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("live", "pep440"), [("3.0.0-beta3", "3.0.0b3"), ("v3.1.0-rc1", "3.1.0rc1"), ("3.0.0", "3.0.0")]
)
def test_normalize_version(live, pep440):
    assert readiness.normalize_version(live) == readiness.normalize_version(pep440)


def test_pyproject_version_is_readable():
    assert readiness.pyproject_version()


def test_oversized_frontend_rejected_not_truncated(stub, capsys, monkeypatch):
    monkeypatch.setattr(readiness, "_MAX_HTML_BYTES", 16)
    stub.routes["/dashboard"] = (200, "text/html", "<html>" + "x" * 64 + "</html>")
    code, out = run(stub, capsys)
    assert code == 1
    assert "exceeds size limit" in out


def test_untrusted_health_values_are_bounded_and_printable(stub, capsys):
    stub.json("/api/health", {"status": "bad\nline" + "z" * 200, "version": "3.0.0-beta3"})
    code, out = run(stub, capsys)
    assert code == 1
    assert "bad?line" in out and "z" * 41 not in out
    stub.json("/api/health", {"status": "ok", "version": {"nested": 1}})
    code, out = run(stub, capsys, "--expect-version", "3.0.0b3")
    assert "backend version <dict>" in out


@pytest.mark.parametrize("flag", ["--bootstrap-timeout", "--timeout"])
def test_non_positive_timeouts_rejected(flag):
    with pytest.raises(SystemExit) as exc:
        readiness.main(["--name", "x", "--frontend", "http://a", "--backend", "http://a", flag, "0"])
    assert exc.value.code == 2


def test_http_error_responses_are_closed(monkeypatch):
    import io
    from urllib.error import HTTPError

    closed = []

    class Tracked(io.BytesIO):
        def close(self):
            closed.append(True)
            super().close()

    def fail(url, timeout):
        raise HTTPError(url, 503, "busy", {}, Tracked(b"body"))

    monkeypatch.setattr(readiness, "admin_urlopen", fail)
    assert readiness._get("http://x/api/health", 1, 10) == (503, "", b"")
    assert closed


# ── Merged from tests/deploy/test_admin_tls_client.py ────────────────────────

"""Scoped stdlib transport tests; no deployment environment or real credentials."""

import http.client
import io
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

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
