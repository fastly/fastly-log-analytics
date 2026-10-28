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


def _render_standard_observability_compose(
    prod: bool, otel_values: dict[str, str] | None = None, *, admin: bool = False
) -> dict:
    env = os.environ.copy()
    for name in (*OTEL_ENV, "COMPOSE_FILE", "COMPOSE_PROFILES"):
        env.pop(name, None)
    env.update(otel_values or {})
    files = ["docker-compose.yml"]
    if prod:
        files.append("docker-compose.prod.yml")
    if admin:
        env.update(
            ADMIN_GATEWAY_HOST="admin.example.test",
            ADMIN_GATEWAY_CERT_DIR="/operator/certificates",
            ADMIN_GATEWAY_REQUIRED="1",
            ADMIN_GATEWAY_SECRET="test-fixture-not-a-credential",
            LOCAL_HOSTS="admin.example.test",
        )
        files.append("docker-compose.admin-mtls.yml")
        if prod:
            files.append("docker-compose.admin-mtls.prod.yml")
    files.append("docker-compose.observability.standard.yml")
    if prod:
        files.append("docker-compose.observability.standard.prod.yml")
    command = ["docker", "compose", "--env-file", "/dev/null"]
    for filename in files:
        command.extend(["-f", filename])
    result = subprocess.run(
        [*command, "config", "--format", "json"], cwd=ROOT, env=env, check=True, capture_output=True, text=True
    )
    return json.loads(result.stdout)


@pytest.mark.parametrize("prod", [False, True])
def test_standard_observability_is_private_bounded_and_has_no_grafana(prod: bool) -> None:
    config = _render_standard_observability_compose(prod)
    services = config["services"]
    assert set(services) == {"backend", "frontend", "caddy", "postgres", "prometheus", "tempo"}
    multipod = yaml.safe_load((ROOT / "docker-compose.observability.yml").read_text())
    for name, target in (("prometheus", "/prometheus"), ("tempo", "/var/tempo")):
        service = services[name]
        assert service["image"] == multipod["services"][name]["image"]
        assert service["network_mode"] == ("host" if prod else "service:backend")
        assert not service.get("ports") and not service.get("networks")
        assert int(service["mem_limit"]) == 512 * 1024**2
        assert service["memswap_limit"] == service["mem_limit"]
        assert float(service["cpus"]) == 0.5
        assert service["read_only"] and service["cap_drop"] == ["ALL"]
        assert service["logging"]["options"] == {"max-size": "10m", "max-file": "3"}
        volume = next(volume for volume in service["volumes"] if volume["target"] == target)
        assert volume["type"] == "volume"
        assert volume["source"] in config["volumes"]
    backend = services["backend"]
    # Local has only its existing Caddy listener; prod retains no published ports.
    assert {port["target"] for port in backend.get("ports", [])} == (set() if prod else {80})
    assert backend["environment"]["TRUSTED_PROXY_IPS"] == "127.0.0.1"
    if prod:
        assert backend["environment"]["REQUIRE_PROXY_HEADERS"] == "1"
        assert "--forwarded-allow-ips=127.0.0.1" in backend["command"]
        assert services["caddy"]["image"] == "caddy-ratelimit:2-alpine"


@pytest.mark.parametrize("prod", [False, True])
def test_standard_observability_requires_explicit_exporter_settings(prod: bool) -> None:
    env = _render_standard_observability_compose(prod)["services"]["backend"]["environment"]
    assert env["OTEL_EXPORTER"] == "none"
    assert all(env[name] == "" for name in OTEL_ENV[1:])
    requested = {
        "OTEL_EXPORTER": "otlp",
        "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT": "http://127.0.0.1:9090/api/v1/otlp/v1/metrics",
        "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT": "http://127.0.0.1:4318/v1/traces",
    }
    env = _render_standard_observability_compose(prod, requested)["services"]["backend"]["environment"]
    assert {name: env[name] for name in OTEL_ENV} == requested


@pytest.mark.parametrize("prod", [False, True])
def test_standard_observability_preserves_required_admin_gateway(prod: bool) -> None:
    config = _render_standard_observability_compose(prod, admin=True)
    services = config["services"]
    for name in ("backend", "frontend"):
        assert services[name]["environment"]["ADMIN_GATEWAY_REQUIRED"] == "1"
        assert services[name]["environment"]["ADMIN_GATEWAY_SECRET"] == "test-fixture-not-a-credential"
    assert services["admin-gateway"]["network_mode"] == ("host" if prod else "service:backend")
    assert services["admin-gateway"]["read_only"] is True
    assert not ({9090, 3200, 4318} & {port["target"] for port in services["backend"].get("ports", [])})


def test_standard_observability_receivers_bind_only_loopback_with_retention() -> None:
    prometheus = yaml.safe_load((ROOT / "observability/standard/prometheus.yml").read_text())
    assert prometheus["scrape_configs"] == []
    assert "remote_write" not in prometheus
    overlay = yaml.safe_load((ROOT / "docker-compose.observability.standard.yml").read_text())
    command = overlay["services"]["prometheus"]["command"]
    assert "--web.listen-address=127.0.0.1:9090" in command
    assert "--web.enable-otlp-receiver" in command
    assert "--storage.tsdb.retention.time=24h" in command
    assert "--storage.tsdb.retention.size=512MB" in command
    assert "--web.enable-admin-api" not in command
    assert "--web.enable-remote-write-receiver" not in command
    tempo = yaml.safe_load((ROOT / "observability/standard/tempo.yml").read_text())
    assert tempo["server"]["http_listen_address"] == "127.0.0.1"
    assert tempo["server"]["grpc_listen_address"] == "127.0.0.1"
    assert tempo["memberlist"]["bind_addr"] == ["127.0.0.1"]
    assert tempo["distributor"]["receivers"]["otlp"]["protocols"] == {"http": {"endpoint": "127.0.0.1:4318"}}
    assert tempo["compactor"]["compaction"]["block_retention"] == "24h"
    assert tempo["storage"]["trace"]["local"]["path"] == "/var/tempo/blocks"
    assert tempo["storage"]["trace"]["wal"]["path"] == "/var/tempo/wal"
    assert tempo["usage_report"]["reporting_enabled"] is False
    assert "metrics_generator" not in tempo


def test_standard_observability_verifier_is_copied_into_runtime_image() -> None:
    dockerfile = (ROOT / "backend/Dockerfile").read_text().split(" AS runner", 1)[1]
    assert "COPY --chown=app:app scripts/dev/verify_observability.py scripts/dev/" in dockerfile
    assert "--create-home" in dockerfile and "USER app" in dockerfile


def test_standard_observability_static_gate_is_mirrored_in_make_and_ci() -> None:
    make = (ROOT / "Makefile").read_text().split("\ndeploy-validate:\n", 1)[1].split("\n\n", 1)[0]
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text())
    step = next(
        step
        for job in workflow["jobs"].values()
        for step in job.get("steps", [])
        if step.get("name") == "Deploy config validation (compose + helm chart)"
    )
    for files in (
        ["docker-compose.yml", "docker-compose.observability.standard.yml"],
        [
            "docker-compose.yml",
            "docker-compose.prod.yml",
            "docker-compose.observability.standard.yml",
            "docker-compose.observability.standard.prod.yml",
        ],
    ):
        command = "docker compose " + " ".join("-f " + name for name in files) + " config -q"
        assert command in make
        assert command in step["run"].splitlines()
    private_helm = (
        "helm template ci-validate deploy/chart/fastly-log-analytics --set global.privateObservability.enabled=true"
    )
    assert private_helm in make and private_helm in step["run"]


def _private_observability_runner_source() -> str:
    runner = ROOT / "scripts/dev/deploy_test_all.sh"
    if not runner.exists():
        pytest.skip("Canonical deployment runner is operator-local and gitignored")
    return runner.read_text()


def _private_observability_python(marker: str) -> str:
    source = _private_observability_runner_source()
    return source.split(f"<<'{marker}'\n", 1)[1].split(f"\n{marker}", 1)[0]


@pytest.mark.parametrize(
    "blocked",
    [
        "RESUME_VERIFICATION",
        "RESET_RUNNING",
        "WATCH_ONLY",
        "GCE_ADMIN_BOOTSTRAP",
        "GCE_ADMIN_FIREWALL_APPLY",
        "IGNORE_LOCAL_STD",
        "SKIP_GCE",
        "SKIP_REMOTE_STD",
        "SKIP_REMOTE_HS",
    ],
)
def test_private_observability_runner_rejects_non_rollout_modes(blocked: str) -> None:
    source = _private_observability_runner_source()
    start = source.index("validate_standard_observability_mode() {")
    end = source.index("\nvalidate_standard_observability_mode || exit 1", start)
    env = {
        **os.environ,
        "LOCAL_STANDARD_OBSERVABILITY": "1",
        "GCE_STANDARD_OBSERVABILITY": "1",
        "HIGH_SCALE_OBSERVABILITY": "1",
        "RESUME_VERIFICATION": "0",
        "RESET_RUNNING": "0",
        "WATCH_ONLY": "0",
        "ADMIN_GATEWAY_PHASE": "",
        "GCE_ADMIN_BOOTSTRAP": "0",
        "GCE_ADMIN_FIREWALL_APPLY": "0",
        "IGNORE_LOCAL_STD": "0",
        "SKIP_GCE": "0",
        "SKIP_REMOTE_STD": "0",
        "SKIP_REMOTE_HS": "0",
        blocked: "1",
    }
    result = subprocess.run(
        ["bash", "-uc", source[start:end] + "\nvalidate_standard_observability_mode"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 1
    # Default mode must leave the legacy resume/watch/bootstrap path alone.
    env.update(LOCAL_STANDARD_OBSERVABILITY="0", GCE_STANDARD_OBSERVABILITY="0", HIGH_SCALE_OBSERVABILITY="0")
    result = subprocess.run(
        ["bash", "-uc", source[start:end] + "\nvalidate_standard_observability_mode"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0


def test_private_observability_env_preserves_mtls_and_rejects_conflicting_pipeline(tmp_path, monkeypatch) -> None:
    program = _private_observability_python("FLA_STANDARD_OBSERVABILITY_ENV")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 0))
    path = tmp_path / ".env"
    original = (
        "COMPOSE_FILE=docker-compose.yml:docker-compose.prod.yml:"
        "docker-compose.admin-mtls.yml:docker-compose.admin-mtls.prod.yml\n"
        "ADMIN_GATEWAY_REQUIRED=1\nUNCHANGED=preserved\n"
    )
    path.write_text(original)
    path.chmod(0o600)
    exec(compile(program, "<private-observability-env>", "exec"), {})
    first = path.read_text()
    assert "ADMIN_GATEWAY_REQUIRED=1\nUNCHANGED=preserved\n" in first
    assert "admin-mtls.prod.yml:docker-compose.observability.standard.yml:" in first
    assert "docker-compose.observability.standard.prod.yml" in first
    assert path.stat().st_mode & 0o777 == 0o600
    exec(compile(program, "<private-observability-env>", "exec"), {})
    assert path.read_text() == first  # Repeat opt-in is idempotent.
    path.write_text(original + "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=https://external.example.test/v1/traces\n")
    with pytest.raises(SystemExit, match="Conflicting"):
        exec(compile(program, "<private-observability-env>", "exec"), {})
    assert "external.example.test" in path.read_text()
    path.write_text(original)
    monkeypatch.setattr(subprocess, "run", lambda *args, **kwargs: subprocess.CompletedProcess(args, 1))
    with pytest.raises(SystemExit, match="VM .env unchanged"):
        exec(compile(program, "<private-observability-env>", "exec"), {})
    assert path.read_text() == original


@pytest.mark.parametrize("failure", [None, "emit", "verify", "verify-request", "incomplete"])
def test_private_observability_runner_requires_real_verifier_success(tmp_path, monkeypatch, failure, capsys) -> None:
    program = _private_observability_python("FLA_STANDARD_OBSERVABILITY_VERIFY")
    calls = []
    app_receipt = {}
    requests = []
    import shlex

    import httpx

    from scripts.lib import admin_tls

    monkeypatch.setattr(admin_tls, "admin_request_context", lambda request: True)

    class Client:
        def __init__(self, **kwargs):
            assert kwargs == {"verify": True, "trust_env": False, "follow_redirects": False, "timeout": 10}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get(self, url, headers):
            requests.append((url, headers))
            return httpx.Response(200)

    monkeypatch.setattr(httpx, "Client", Client)

    def run(command, **kwargs):
        calls.append(command)
        # Both local argv and the SSH shell string must contain the real CLI.
        text = " ".join(command)
        if failure and f"verify_observability {failure}" in text:
            raise subprocess.CalledProcessError(1, command)
        args = shlex.split(command[-1].split(" && ", 1)[1]) if command[0] == "ssh-test" else command
        if "os.write" in text:
            app_receipt.update(json.loads(args[-1]))
        if "read_text()" in text:
            body = {"metric_observed": failure != "incomplete", "trace_observed": True}
            if "fla-http-delivery" in text:
                body.update(scope="application_http_delivery", trace_id=app_receipt["trace_id"], path="/api/health")
            return subprocess.CompletedProcess(command, 0, stdout=json.dumps(body))
        return subprocess.CompletedProcess(command, 0, stdout='{"completed": true}')

    obs_root = tmp_path / "obs"
    obs_root.mkdir()
    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            str(obs_root),
            "1",
            "1",
            "1",
            "fixture",
            "http://127.0.0.1",
            "https://admin.example.test",
            "https://cluster.example.test",
            "2",
            "-p",
            "source-root-project",
            "ssh-test",
        ],
    )
    if failure:
        with pytest.raises(SystemExit) as caught:
            exec(compile(program, "<private-observability-verify>", "exec"), {})
        assert caught.value.code == 1
        assert "UNVERIFIED" in capsys.readouterr().err
    else:
        exec(compile(program, "<private-observability-verify>", "exec"), {})
        assert len(calls) == 37
        assert calls[0][:4] == ["docker", "compose", "-p", "source-root-project"]
        assert calls[9][:2] == ["ssh-test", "--command"]
        assert any("deployment/high-scale-worker" in command for command in calls)
        assert any("deployment/worker" in command for command in calls)
        assert len(list(obs_root.glob("observability-*/*.json"))) == 16
        assert len(requests) == 3  # One cheap existing health request per env; no edge traffic.
        assert all(request[0].endswith("/api/health") for request in requests)
        assert len({request[1]["traceparent"] for request in requests}) == 3
    for directory in obs_root.iterdir():
        assert directory.stat().st_mode & 0o777 == 0o700
        for path in directory.iterdir():
            assert path.stat().st_mode & 0o777 == 0o600
            assert "http://" not in path.read_text() and "https://" not in path.read_text()


# ── Merged from tests/deploy/test_public_gateway_runtime.py ──


def _minimal_observability_chart(*extra: str) -> list[dict]:
    result = subprocess.run(
        [
            "helm",
            "template",
            "telemetry-test",
            str(ROOT / "deploy/chart/fastly-log-analytics"),
            "--set",
            "global.privateObservability.enabled=true",
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def test_private_helm_receivers_are_bounded_persistent_and_have_no_grafana() -> None:
    docs = _minimal_observability_chart()
    deployments = {doc["metadata"]["name"]: doc for doc in docs if doc["kind"] == "Deployment"}
    receivers = [doc for name, doc in deployments.items() if name.endswith(("-prometheus", "-tempo"))]
    assert len(receivers) == 2
    assert not any(name.endswith(("-grafana", "-exporter")) for name in deployments)
    telemetry_pvcs = [
        doc for doc in docs if doc["kind"] == "PersistentVolumeClaim" and "-telemetry-" in doc["metadata"]["name"]
    ]
    assert len(telemetry_pvcs) == 2
    for pvc in telemetry_pvcs:
        assert pvc["spec"]["storageClassName"] == "hd-base-ext4"
        assert pvc["spec"]["resources"]["requests"]["storage"] == "6Gi"
    for receiver in receivers:
        spec = receiver["spec"]
        assert spec["strategy"]["type"] == "Recreate"
        pod = spec["template"]["spec"]
        assert pod["securityContext"]["runAsNonRoot"] is True
        container = pod["containers"][0]
        assert container["resources"]["limits"] == {"cpu": "500m", "memory": "512Mi"}
        assert container["securityContext"]["readOnlyRootFilesystem"] is True
        assert container["env"] == [{"name": "GOMEMLIMIT", "value": "384MiB"}]
        assert any("persistentVolumeClaim" in volume for volume in pod["volumes"])
    prometheus = next(doc for doc in receivers if doc["metadata"]["name"].endswith("-prometheus"))
    args = prometheus["spec"]["template"]["spec"]["containers"][0]["args"]
    assert "--storage.tsdb.retention.time=24h" in args
    assert "--storage.tsdb.retention.size=512MB" in args
    assert "--web.enable-remote-write-receiver" not in args
    services = [doc for doc in docs if doc["kind"] == "Service" and doc["metadata"]["name"] in {"prometheus", "tempo"}]
    assert len(services) == 2
    assert all(doc["spec"].get("type", "ClusterIP") == "ClusterIP" for doc in services)
    policies = [doc for doc in docs if doc["kind"] == "NetworkPolicy"]
    assert any(doc["spec"].get("ingress") for doc in policies)
    assert all(doc["spec"].get("egress") == [] for doc in policies)


def test_private_helm_wires_backend_celery_and_real_high_scale_worker_without_duplicate_env() -> None:
    docs = _minimal_observability_chart(
        "--set",
        "highScale.enabled=true",
        "--set",
        "highScale.sharedState.existingClaim=test-app-state",
        "--set",
        "config.deploymentMode=high_scale",
        "--set",
        "config.ducklakeCatalog=postgresql://fixture/catalog",
        "--set",
        "secrets.existingSecret=fixture-secret",
    )
    deployments = [doc for doc in docs if doc["kind"] == "Deployment"]
    selected = [doc for doc in deployments if doc["metadata"]["name"].endswith(("-backend", "-worker", "-ingest"))]
    assert len(selected) == 3
    for doc in selected:
        entries = doc["spec"]["template"]["spec"]["containers"][0]["env"]
        env = {entry["name"]: entry.get("value") for entry in entries}
        assert len(env) == len(entries)
        assert env["OTEL_EXPORTER"] == "otlp"
        assert env["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] == "http://prometheus:9090/api/v1/otlp/v1/metrics"
        assert env["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] == "http://tempo:4318/v1/traces"
    policy = next(doc for doc in docs if doc["kind"] == "NetworkPolicy" and doc["metadata"]["name"].endswith("-ingest"))
    assert {9090, 4318} <= {port["port"] for rule in policy["spec"]["egress"] for port in rule.get("ports", [])}


@pytest.mark.parametrize(
    "extra",
    [
        ["--set", "observability.enabled=true"],
        ["--set", "otel.exporter=none"],
        [
            "--set",
            "highScale.enabled=true",
            "--set",
            "highScale.sharedState.existingClaim=fixture",
            "--set",
            "highScale.env[0].name=OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "--set",
            "highScale.env[0].value=https://external.example.test/v1/metrics",
        ],
    ],
)
def test_private_helm_rejects_mixed_profiles_and_conflicting_sdk_configuration(extra) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        _minimal_observability_chart(*extra)


@pytest.mark.parametrize(
    "origin",
    [
        "http://0.0.0.0:9090",
        "https://example.test",
        "http://user:secret@127.0.0.1:9090",
        "http://127.0.0.1:9090/?token=secret",
        'http://127.0.0.1:9090/" onload="bad',
        "http://127.0.0.1:9090#secret",
        "http://localhost:9090",
    ],
)
def test_metrics_ui_links_reject_non_private_or_credential_bearing_urls(origin) -> None:
    from scripts.lib.observability_ui import metrics_ui_link

    assert metrics_ui_link(origin, False) == ""
    with pytest.raises(ValueError):
        metrics_ui_link(origin, True)


def test_metrics_ui_link_requires_real_browser_and_query_editor(monkeypatch) -> None:
    from scripts.lib import observability_ui as ui

    calls = []

    def run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout='{"ui_verified":true}')

    monkeypatch.setattr(subprocess, "run", run)
    ui.verify_ui("http://127.0.0.1:19090")
    assert calls[0][:2] == ["node", "-e"]
    browser = calls[0][2]
    assert "chromium.launch" in browser and ".cm-editor" in browser
    assert "name:'Execute'" in browser and "response.status() !== 200" in browser
    link = ui.metrics_ui_link("http://127.0.0.1:19090", True)
    assert 'href="http://127.0.0.1:19090/query"' in link and "Metrics UI" in link
    assert "noopener noreferrer" in link and "expires" in link
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(a, 0, stdout="{}"))
    with pytest.raises(ValueError, match="not verified"):
        ui.verify_ui("http://127.0.0.1:19090")


def test_metrics_ui_local_proxy_preserves_path_but_never_forwards_operator_auth(monkeypatch) -> None:
    import base64

    from scripts.lib import observability_ui as ui

    seen = []

    def run(command, **kwargs):
        seen.append((command, json.loads(kwargs["input"])))
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({"status": 200, "type": "text/html", "body": base64.b64encode(b"fixture").decode()}),
        )

    monkeypatch.setattr(subprocess, "run", run)
    server = ui._proxy_server(0, ["docker", "compose", "exec", "-T", "backend"])
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        connection = HTTPConnection("127.0.0.1", server.server_port, timeout=3)
        connection.request("GET", "/query", headers={"Authorization": "private", "Cookie": "private"})
        response = connection.getresponse()
        assert response.status == 200 and response.read() == b"fixture"
        assert seen[0][1]["path"] == "/query"
        assert seen[0][1]["headers"] == {"Content-Type": "application/json"}
        assert server.server_address[0] == "127.0.0.1"
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


@pytest.mark.parametrize("failure", [False, True])
def test_private_runner_ui_links_are_opt_in_per_environment_and_fail_closed(tmp_path, monkeypatch, failure) -> None:
    from scripts.lib import observability_ui as ui

    program = _private_observability_python("FLA_OBSERVABILITY_UI")
    starts = []

    def start(directory, mode, command, port):
        starts.append((mode, command))
        return directory / "private-state.json", f"http://127.0.0.1:{port}"

    def verify(origin, receipt_path):
        assert receipt_path.is_file()
        if failure:
            raise ValueError("Fixture browser did not render Prometheus")

    class Connection:
        def __enter__(self):
            pass

        def __exit__(self, *args):
            pass

    monkeypatch.setattr(ui, "start_access", start)
    monkeypatch.setattr(ui, "verify_ui", verify)
    monkeypatch.setattr(ui, "unused_loopback_port", lambda: 19090)
    monkeypatch.setattr(socket, "create_connection", lambda *a, **kw: Connection())
    directory = tmp_path / "metrics-ui.fixture"
    directory.mkdir()
    receipts = tmp_path / "observability-fixture"
    receipts.mkdir()
    for label in ("local-standard", "remote-standard", "remote-high-scale"):
        (receipts / f"{label}-backend-receipt.json").write_text("{}")
    args = ["runner", str(directory), "1", "1", "1", "fixture", "2", "-p", "fixture-project", "gcloud"]
    monkeypatch.setattr(sys, "argv", args)
    if failure:
        with pytest.raises(ValueError, match="did not render"):
            exec(compile(program, "<private-metrics-ui>", "exec"), {})
        assert not list(directory.glob("*-link.json"))
    else:
        exec(compile(program, "<private-metrics-ui>", "exec"), {})
        assert len(starts) == 3
        assert starts[0][0] == "proxy"
        assert starts[0][1] == ["docker", "compose", "-p", "fixture-project", "exec", "-T", "backend"]
        assert "--ssh-flag=-L127.0.0.1:19090:127.0.0.1:9090" in starts[1][1]
        assert starts[2][1] == [
            "kubectl",
            "port-forward",
            "-n",
            "fixture",
            "--address=127.0.0.1",
            "service/prometheus",
            "19090:9090",
        ]
        for label in ("local-standard", "remote-standard", "remote-high-scale"):
            path = directory / f"{label}-link.json"
            assert json.loads(path.read_text())["ui_verified"] is True
            assert path.stat().st_mode & 0o777 == 0o600
    # All defaults off means no access process, browser or report URL.
    starts.clear()
    args[2:5] = ["0", "0", "0"]
    exec(compile(program, "<private-metrics-ui-default>", "exec"), {})
    assert not starts
    source = _private_observability_runner_source()
    for label, variable in (
        ("local-std", "local_metrics_ui"),
        ("remote-std", "remote_std_metrics_ui"),
        ("remote-hs", "remote_hs_metrics_ui"),
    ):
        assert f'<div id="{label}-creds"></div>\n              <div>${{{variable}}}</div>' in source
    assert '"$status" != "SUCCESS" && -n "${OBSERVABILITY_UI_DIRECTORY:-}"' in source
    assert 'observability_ui stop-directory "$OBSERVABILITY_UI_DIRECTORY"' in source


def test_private_runner_hs_reuses_only_existing_receiver_templates_and_preserves_app_ownership(
    tmp_path, monkeypatch
) -> None:
    program = _private_observability_python("FLA_HS_OBSERVABILITY")
    real_run = subprocess.run
    commands = []
    applied = []

    def run(command, **kwargs):
        commands.append(command)
        if command[0] == "helm":
            return real_run(command, cwd=ROOT, **kwargs)
        if command[1:3] == ["apply", "-f"]:
            applied.extend(doc for doc in yaml.safe_load_all(kwargs["input"]) if doc)
        body = ""
        if command[1] == "get" and command[2].startswith("deployment/"):
            name = command[2].split("/")[1]
            body = json.dumps({"spec": {"template": {"spec": {"containers": [{"name": name, "env": []}]}}}})
        return subprocess.CompletedProcess(command, 0, stdout=body)

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(sys, "argv", ["runner", "fixture"])
    exec(compile(program, "<private-hs-receivers>", "exec"), {})
    assert applied
    assert {doc["kind"] for doc in applied} == {
        "Deployment",
        "Service",
        "ConfigMap",
        "PersistentVolumeClaim",
        "NetworkPolicy",
    }
    assert not any("grafana" in doc["metadata"]["name"] for doc in applied)
    assert all(doc["metadata"]["namespace"] == "fixture" for doc in applied)
    changes = [command for command in commands if command[1:3] == ["set", "env"]]
    assert {command[3] for command in changes} == {
        "deployment/backend",
        "deployment/high-scale-worker",
        "deployment/worker",
    }
    assert all("OTEL_EXPORTER=otlp" in command for command in changes)
    assert not any("upgrade" in command for command in commands)

    def conflicting(command, **kwargs):
        if command[0] == "kubectl" and command[1] == "get":
            return subprocess.CompletedProcess(command, 0, stdout='{"metadata":{"labels":{}}}')
        return run(command, **kwargs)

    monkeypatch.setattr(subprocess, "run", conflicting)
    with pytest.raises(SystemExit, match="independently owned"):
        exec(compile(program, "<private-hs-collision>", "exec"), {})


def test_private_runner_clean_source_export_freezes_invoked_entrypoint() -> None:
    source = _private_observability_runner_source()
    snapshot = 'cp -p "$FLA_SELF" "$FLA_EXPORT_DIR/scripts/dev/deploy_test_all.sh"'
    assert snapshot in source
    assert source.index("git archive --format=tar HEAD") < source.index(snapshot)
    assert source.index(snapshot) < source.index('exec bash "$FLA_EXPORT_DIR/scripts/dev/deploy_test_all.sh"')


def test_private_runner_receiver_apply_failure_retains_private_exit_and_stderr(tmp_path, monkeypatch) -> None:
    program = _private_observability_python("FLA_HS_OBSERVABILITY")
    real_run = subprocess.run
    (tmp_path / "relics").mkdir()
    changes = []

    def run(command, **kwargs):
        if command[0] == "helm":
            return real_run(command, cwd=ROOT, **kwargs)
        if command[1:3] == ["apply", "-f"]:
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, command, stderr="fixture admission refusal")
            return subprocess.CompletedProcess(command, 1, stdout="", stderr="fixture admission refusal")
        changes.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(sys, "argv", ["runner", "fixture", str(tmp_path)])
    with pytest.raises(SystemExit, match="private diagnostic"):
        exec(compile(program, "<private-hs-apply-failure>", "exec"), {})
    evidence = tmp_path / "relics/observability-hs-apply.json"
    assert evidence.stat().st_mode & 0o777 == 0o600
    assert json.loads(evidence.read_text()) == {"exit_code": 1, "stderr": "fixture admission refusal"}
    assert not any(command[1:3] in (["set", "env"], ["rollout", "restart"]) for command in changes)


def test_metrics_ui_receiver_mismatch_never_runs_browser_or_publishes_link(tmp_path, monkeypatch) -> None:
    from datetime import UTC, datetime
    from uuid import uuid4

    from scripts.dev import verify_observability as verifier
    from scripts.lib import observability_ui as ui

    receipt = tmp_path / "receipt.json"
    ui.write_private(
        receipt,
        {
            "probe_id": str(uuid4()),
            "trace_id": uuid4().hex,
            "span_id": uuid4().hex[:16],
            "service_name": "fastly-log-analytics",
            "service_instance_id": str(uuid4()),
            "emitted_at": datetime.now(UTC).isoformat(),
        },
    )
    monkeypatch.setattr(verifier, "_receiver_json", lambda *a, **kw: None)
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: pytest.fail("Wrong receiver must not reach browser"))
    with pytest.raises(ValueError, match="does not reach"):
        ui.verify_ui("http://127.0.0.1:19090", receipt_path=receipt)


def test_metrics_ui_stop_guards_recycled_pids_and_expired_report_links(tmp_path, monkeypatch) -> None:
    from scripts.lib import observability_ui as ui

    state = tmp_path / "ui-fixture.json"
    ui.write_private(state, {"token": "fixture-token"})
    ui.write_private(state.with_suffix(".pid"), {"pid": 12345, "token": "fixture-token"})
    kills = []
    monkeypatch.setattr(os, "killpg", lambda *args: kills.append(args))
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **kw: subprocess.CompletedProcess(a, 0, stdout="unrelated-process")
    )
    ui.stop_access(state)
    assert not kills
    monkeypatch.setattr(
        subprocess,
        "run",
        lambda *a, **kw: subprocess.CompletedProcess(
            a,
            0,
            stdout="python -m scripts.lib.observability_ui serve /private/state fixture-token",
        ),
    )
    ui.stop_access(state)
    assert kills and kills[0][0] == 12345
    link = tmp_path / "local-standard-link.json"
    ui.write_private(link, {"ui_verified": True, "expires_at": 0})
    assert ui.report_link(link) == ""


@pytest.mark.parametrize("assets_available", [True, False])
def test_metrics_ui_browser_requires_real_asset_render_not_just_http_200(assets_available) -> None:
    from scripts.lib import observability_ui as ui

    dependency = ROOT / "frontend/node_modules/@playwright/test"
    if not dependency.exists():
        pytest.skip("Install frontend Playwright dependencies for the offline UI fixture")
    installed = subprocess.run(
        [
            "node",
            "-e",
            "const {chromium}=require('./frontend/node_modules/@playwright/test'); "
            "process.exit(require('fs').existsSync(chromium.executablePath())?0:1)",
        ],
        cwd=ROOT,
        capture_output=True,
        timeout=10,
    )
    if installed.returncode:
        pytest.skip("Install Playwright Chromium for the offline UI fixture")

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == "/assets/editor.js":
                if not assets_available:
                    self.send_error(404)
                    return
                body = b"document.body.innerHTML='<div class=\"cm-editor\">fixture</div><button>Execute</button>';"
                content_type = "application/javascript"
            else:
                body = b"<title>Prometheus</title><script defer src='/assets/editor.js'></script><body></body>"
                content_type = "text/html"
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        if assets_available:
            ui.verify_ui(f"http://127.0.0.1:{server.server_port}")
        else:
            with pytest.raises(subprocess.CalledProcessError):
                ui.verify_ui(f"http://127.0.0.1:{server.server_port}")
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


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
from urllib.error import HTTPError, URLError
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


# A realistic authenticated /api/bootstrap payload: non-null header_badge with
# separate request-event and RUM sub-objects, plus a non-empty services list.
# Synthetic placeholder values only — no real service ids, IPs, or paths.
_FULL_BOOTSTRAP_PAYLOAD = {
    "active_service_id": "test-service",
    "header_badge": {
        "request": {
            "total_rows": 217465,
            "latest_log_at": "2026-10-26T16:00:00Z",
            "last_sync_at": "2026-10-26T16:05:00Z",
        },
        "rum": {
            "total_rows": 48210,
            "latest_log_at": "2026-10-26T15:59:30Z",
            "last_sync_at": "2026-10-26T16:04:45Z",
        },
    },
    "services": [{"service_id": "test-service", "name": "Test Service"}],
}


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
        response = io.BytesIO(json.dumps(_FULL_BOOTSTRAP_PAYLOAD).encode("utf-8"))
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
    assert body == {"ok": True, "status": 200, "data": _FULL_BOOTSTRAP_PAYLOAD}
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


def test_report_bootstrap_surfaces_http_error_in_body(report, monkeypatch):
    origin, requests, _origins = report

    def fail(request, timeout):
        requests.append((request.full_url, timeout, dict(request.header_items())))
        raise HTTPError(request.full_url, 503, "Service Unavailable", {}, None)

    monkeypatch.setattr(report_server, "admin_urlopen", fail)
    with urlopen(f"{origin}/relics/bootstrap_status.json?env=remote-hs", timeout=5) as response:
        assert response.status == 200
        body = json.load(response)
    assert body == {
        "ok": False,
        "status": 503,
        "data": None,
        "error": "HTTP 503 Service Unavailable",
    }
    assert len(requests) == 1


def test_report_bootstrap_surfaces_timeout_in_body(report, monkeypatch):
    origin, requests, _origins = report

    def fail(request, timeout):
        requests.append((request.full_url, timeout, dict(request.header_items())))
        raise URLError("timed out")

    monkeypatch.setattr(report_server, "admin_urlopen", fail)
    with urlopen(f"{origin}/relics/bootstrap_status.json?env=remote-hs", timeout=5) as response:
        assert response.status == 200
        body = json.load(response)
    assert body["ok"] is False
    assert body["status"] == 0
    assert body["data"] is None
    assert body["error"]
    assert "timed out" in body["error"]
    assert len(requests) == 1


def test_report_bootstrap_surfaces_unconfigured_endpoint_in_body(report, monkeypatch):
    origin, requests, _origins = report
    monkeypatch.setattr(report_server, "admin_origin", lambda env: None)
    with urlopen(f"{origin}/relics/bootstrap_status.json?env=remote-hs", timeout=5) as response:
        assert response.status == 200
        body = json.load(response)
    assert body["ok"] is False
    assert body["status"] == 0
    assert body["data"] is None
    assert "admin gateway endpoint not configured" in body["error"]
    assert not requests


def test_report_bootstrap_surfaces_missing_header_badge_in_body(report, monkeypatch):
    origin, requests, _origins = report

    def upstream_without_stats(request, timeout):
        requests.append((request.full_url, timeout, dict(request.header_items())))
        payload = io.BytesIO(json.dumps({"active_service_id": "test-service"}).encode("utf-8"))
        payload.status = 200
        return payload

    monkeypatch.setattr(report_server, "admin_urlopen", upstream_without_stats)
    with urlopen(f"{origin}/relics/bootstrap_status.json?env=remote-hs", timeout=5) as response:
        assert response.status == 200
        body = json.load(response)
    assert body["ok"] is False
    assert "no header_badge/services" in body["error"]
    assert len(requests) == 1


def _write_env_file(tmp_path, body, *, mode=0o600):
    env_path = tmp_path / "deploy.env"
    env_path.write_text(body, encoding="utf-8")
    env_path.chmod(mode)
    return env_path


def test_parse_admin_env_file_keeps_only_allow_listed_keys(tmp_path):
    env_path = _write_env_file(
        tmp_path,
        "ADMIN_GATEWAY_ENDPOINTS={}\n"
        "export ADMIN_GATEWAY_CLIENT_ORIGIN=https://admin.example.com:8443\n"
        "# a comment line\n"
        "\n"
        "EVIL_KEY=should-be-dropped\n",
    )
    parsed = report_server._parse_admin_env_file(env_path)
    assert parsed["ADMIN_GATEWAY_ENDPOINTS"] == "{}"
    assert parsed["ADMIN_GATEWAY_CLIENT_ORIGIN"] == "https://admin.example.com:8443"
    assert "EVIL_KEY" not in parsed


def test_parse_admin_env_file_strips_matching_quotes(tmp_path):
    env_path = _write_env_file(
        tmp_path,
        "ADMIN_GATEWAY_SECRET=\"quoted-secret\"\nADMIN_GATEWAY_HOST='single-quoted'\n",
    )
    parsed = report_server._parse_admin_env_file(env_path)
    assert parsed["ADMIN_GATEWAY_SECRET"] == "quoted-secret"
    assert parsed["ADMIN_GATEWAY_HOST"] == "single-quoted"


def test_parse_admin_env_file_rejects_group_or_world_readable(tmp_path):
    env_path = _write_env_file(
        tmp_path,
        "ADMIN_GATEWAY_ENDPOINTS={}\n",
        mode=0o644,
    )
    assert report_server._parse_admin_env_file(env_path) == {}


def test_parse_admin_env_file_rejects_nul_in_value(tmp_path):
    env_path = _write_env_file(
        tmp_path,
        "ADMIN_GATEWAY_SECRET=bad\x00value\n",
    )
    assert report_server._parse_admin_env_file(env_path) == {}


def test_parse_admin_env_file_missing_file_returns_empty(tmp_path):
    assert report_server._parse_admin_env_file(tmp_path / "absent.env") == {}


def test_load_admin_env_fallback_no_op_when_primary_key_present(tmp_path):
    env_path = _write_env_file(tmp_path, "ADMIN_GATEWAY_HOST=from-file\n")
    environ = {"ADMIN_GATEWAY_ENDPOINTS": "{}"}
    assert report_server._load_admin_env_fallback(path=env_path, environ=environ) is False
    assert "ADMIN_GATEWAY_HOST" not in environ


def test_load_admin_env_fallback_adopts_allow_listed_keys(tmp_path):
    env_path = _write_env_file(
        tmp_path,
        "ADMIN_GATEWAY_ENDPOINTS={}\nADMIN_GATEWAY_HOST=from-file\nEVIL_KEY=nope\n",
    )
    environ = {}
    assert report_server._load_admin_env_fallback(path=env_path, environ=environ) is True
    assert environ["ADMIN_GATEWAY_ENDPOINTS"] == "{}"
    assert environ["ADMIN_GATEWAY_HOST"] == "from-file"
    assert "EVIL_KEY" not in environ


def test_load_admin_env_fallback_does_not_overwrite_existing_non_primary_key(tmp_path):
    env_path = _write_env_file(
        tmp_path,
        "ADMIN_GATEWAY_ENDPOINTS={}\nADMIN_GATEWAY_HOST=from-file\n",
    )
    environ = {"ADMIN_GATEWAY_HOST": "already-set"}
    assert report_server._load_admin_env_fallback(path=env_path, environ=environ) is True
    assert environ["ADMIN_GATEWAY_HOST"] == "already-set"
    assert environ["ADMIN_GATEWAY_ENDPOINTS"] == "{}"


def test_load_admin_env_fallback_returns_false_on_rejected_file(tmp_path):
    env_path = _write_env_file(tmp_path, "ADMIN_GATEWAY_ENDPOINTS={}\n", mode=0o644)
    environ = {}
    assert report_server._load_admin_env_fallback(path=env_path, environ=environ) is False
    assert environ == {}


def test_default_admin_deploy_env_honors_override(tmp_path, monkeypatch):
    override = tmp_path / "custom-deploy.env"
    monkeypatch.setenv("FLA_ADMIN_DEPLOY_ENV", str(override))
    assert report_server._default_admin_deploy_env() == override


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


@pytest.fixture
def private_report(report, tmp_path, monkeypatch):
    _, requests, origins = report
    root = (tmp_path / "report").resolve()
    root.mkdir()
    (root / "index.html").write_text("live deployment report")
    (root / "logs").mkdir()
    (root / "logs" / "deploy.log").write_text("deployment started")
    (tmp_path / "outside.txt").write_text("not part of the report")
    (root / "escape").symlink_to(tmp_path / "outside.txt")
    monkeypatch.setattr(report_server.PrivateReportRequestHandler, "report_root", root, raising=False)
    monkeypatch.setattr(report_server.PrivateReportRequestHandler, "route_prefix", "/private-test/", raising=False)
    monkeypatch.setattr(report_server, "_CURRENT_REPORT_LINK", root)
    server = ThreadingHTTPServer(("127.0.0.1", 0), report_server.PrivateReportRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/private-test/", requests, origins
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_private_report_serves_live_files_and_head(private_report):
    origin, _, _ = private_report
    with urlopen(f"{origin}index.html", timeout=5) as response:
        assert response.read() == b"live deployment report"
        assert response.headers["Referrer-Policy"] == "no-referrer"
    with urlopen(f"{origin}logs/deploy.log", timeout=5) as response:
        assert response.read() == b"deployment started"
    request = Request(f"{origin}logs/deploy.log", method="HEAD")
    with urlopen(request, timeout=5) as response:
        assert response.status == 200
        assert response.read() == b""


def test_private_report_keeps_same_origin_bootstrap_proxy(private_report):
    origin, requests, origins = private_report
    with urlopen(f"{origin}relics/bootstrap_status.json?env=remote-hs", timeout=5) as response:
        body = json.load(response)
    assert body["data"] == _FULL_BOOTSTRAP_PAYLOAD
    assert origins == ["remote-hs"]
    assert len(requests) == 1


@pytest.mark.parametrize(
    ("path", "headers", "status"),
    [
        ("../outside.txt", {}, 403),
        ("..%2foutside.txt", {}, 403),
        ("escape", {}, 403),
        ("logs/", {}, 403),
        ("relics/credentials_status.json", {}, 403),
        ("index.html", {"Origin": "https://foreign.example.com"}, 404),
        ("index.html", {"Host": "foreign.example.com"}, 404),
    ],
)
def test_private_report_rejects_access_outside_its_scope(private_report, path, headers, status):
    origin, requests, origins = private_report
    request = Request(f"{origin}{path}", headers=headers)
    with pytest.raises(HTTPError) as error:
        urlopen(request, timeout=5)
    assert error.value.code == status
    error.value.close()
    assert not requests
    assert not origins


def test_private_report_requires_its_private_url_prefix(private_report):
    origin, _, _ = private_report
    with pytest.raises(HTTPError) as error:
        urlopen(f"{origin.removesuffix('/private-test/')}/index.html", timeout=5)
    assert error.value.code == 404
    error.value.close()


@pytest.mark.parametrize("signal", ["warning", "WARN", "failed", "unauthorized", "fatal", "critical"])
def test_deploy_log_monitor_captures_warning_and_failure_signals(signal):
    source = (ROOT / "scripts/lib/deploy_common.sh").read_text()
    monitor = source.split("start_log_monitor() {", 1)[1].split("\nemit_deploy_report_script()", 1)[0]
    patterns = re.findall(r'grep --line-buffered -E -i "([^"]+)"', monitor)
    assert len(patterns) == 7
    for pattern in patterns:
        result = subprocess.run(
            ["grep", "-E", "-i", pattern],
            input=f"2026-01-01T00:00:01Z [{signal}] test connection unavailable\n",
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert result.returncode == 0, f"capture filter omitted {signal}"


def test_private_runner_keeps_watch_capture_until_exit_report(tmp_path):
    source = _private_observability_runner_source()
    cleanup = source.split("on_exit_cleanup() {", 1)[1].split("\n}\n", 1)[0]
    # The watch snapshot must still exist when the final EXIT report copies it.
    assert 'rm -f "$LOG_MONITOR_TEMP"' in cleanup
    assert cleanup.index("save_deploy_report") < cleanup.index('rm -f "$LOG_MONITOR_TEMP"')
    assert source.count('rm -f "$LOG_MONITOR_TEMP"') == 1

    report_dir = tmp_path / "report"
    (report_dir / "logs").mkdir(parents=True)
    capture = tmp_path / "watch.log"
    capture.write_text("[Local Standard/backend] [warning] test connection unavailable\n")
    copy_statement = next(line for line in source.splitlines() if "monitored_errors.log" in line and "cp " in line)
    result = subprocess.run(
        ["bash", "-ec", copy_statement + '\nrm -f "$LOG_MONITOR_TEMP"'],
        env={**os.environ, "REPORT_DIR": str(report_dir), "LOG_MONITOR_TEMP": str(capture)},
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0
    assert not capture.exists()
    assert "[warning] test connection unavailable" in (report_dir / "logs/monitored_errors.log").read_text()


def test_deploy_report_missing_watch_snapshot_is_not_a_clean_scan():
    source = (ROOT / "scripts/lib/deploy_common.sh").read_text()
    script = source.split("emit_deploy_report_script() {", 1)[1].split("cat <<EOF\n", 1)[1].split("\nEOF", 1)[0]
    script = script.removeprefix("  <script>").split("</script>", 1)[0].replace("${status}", "SUCCESS")
    program = """
const vm = require('node:vm');
const pre = { innerHTML: 'monitor pending' };
const count = { textContent: '' };
const context = {
  document: {
    addEventListener() {},
    getElementById(id) { return id === 'monitored-errors-log' ? pre : count; }
  },
  window: {}, setInterval() {}, setTimeout() {},
  fetch: async () => ({ ok: false, status: 404 })
};
vm.createContext(context);
vm.runInContext(SOURCE, context);
(async () => {
  await context.updateLogPre('monitored-errors-log', 'logs/monitored_errors.log');
  process.stdout.write(JSON.stringify({ text: pre.textContent || pre.innerHTML, count: count.textContent }));
})().catch(error => { console.error(error); process.exitCode = 1; });
"""
    result = subprocess.run(
        ["node", "-e", program.replace("SOURCE", json.dumps(script))],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    body = json.loads(result.stdout)
    assert "unavailable" in body["text"].lower()
    assert "zero exceptions" not in body["text"].lower()
    assert "unavailable" in body["count"].lower()
