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
                                    {"name": "x-admin-token", "value": "obsolete"},
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
        assert headers["x-admin-token"] == ""
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
