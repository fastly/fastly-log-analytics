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
