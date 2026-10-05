"""Portable gateway and app opt-in render contracts; no Helm secrets in values."""

import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
GATEWAY = ROOT / "deploy/chart/admin-gateway"
APP = ROOT / "deploy/chart/fastly-log-analytics"
GATEWAY_VALUES = (
    "host=admin.example.com",
    "existingSecret=operator-gateway",
    "frontendUpstream=frontend:3000",
    "backendUpstream=backend:8000",
    "image=registry.example.com/fla-admin-gateway:test",
)


def render(chart, values, success=True):
    command = ["helm", "template", "test", str(chart)]
    for value in values:
        command += ["--set", value]
    result = subprocess.run(command, capture_output=True, text=True)
    assert (result.returncode == 0) == success, result.stderr
    return list(yaml.safe_load_all(result.stdout)) if success else result.stderr


def test_gateway_secret_reference_and_tls_only_service():
    docs = render(GATEWAY, GATEWAY_VALUES)
    config = next(doc for doc in docs if doc["kind"] == "ConfigMap")
    assert config["data"]["Caddyfile"] == (GATEWAY / "files/Caddyfile").read_text()
    assert "require_and_verify" in config["data"]["Caddyfile"]
    assert "tls internal" not in config["data"]["Caddyfile"]
    assert not any(doc["kind"] in {"Secret", "Ingress"} for doc in docs)
    service = next(doc for doc in docs if doc["kind"] == "Service")
    assert service["spec"]["type"] == "ClusterIP"
    assert service["spec"]["ports"] == [{"name": "mtls", "port": 8443, "targetPort": "mtls"}]
    pod = next(doc for doc in docs if doc["kind"] == "Deployment")["spec"]["template"]["spec"]
    assert pod["automountServiceAccountToken"] is False
    env = {item["name"]: item for item in pod["containers"][0]["env"]}
    assert env["ADMIN_GATEWAY_SECRET"]["valueFrom"]["secretKeyRef"] == {
        "name": "operator-gateway",
        "key": "ADMIN_GATEWAY_SECRET",
    }
    secret = next(item["secret"] for item in pod["volumes"] if "secret" in item)
    assert {item["key"] for item in secret["items"]} == {"server.crt", "server.key", "client-ca.crt"}
    policy = next(doc for doc in docs if doc["kind"] == "NetworkPolicy")
    assert policy["spec"]["ingress"][0]["ports"][0]["port"] == 8443


@pytest.mark.parametrize(
    "host",
    ["", "*.example.com", "https://example.com", "example.com:443", "bad host", "bad..host", "-bad.host", "bad\nhost"],
)
def test_gateway_rejects_missing_or_wildcard_host(host):
    values = [value for value in GATEWAY_VALUES if not value.startswith("host=")]
    assert "explicit DNS hostname" in render(GATEWAY, [*values, f"host={host}"], success=False)


def test_gateway_requires_existing_secret_and_upstreams():
    for key, expected in (("existingSecret", "existingSecret"), ("frontendUpstream", "frontendUpstream")):
        values = [value for value in GATEWAY_VALUES if not value.startswith(f"{key}=")]
        assert expected in render(GATEWAY, values, success=False)


def test_internal_lb_reservation_has_no_gateway_or_secrets():
    docs = render(
        GATEWAY, ("gateway.enabled=false", "service.type=LoadBalancer", "service.internalProvider=gke-internal")
    )
    assert [doc["kind"] for doc in docs] == ["Service"]
    service = docs[0]
    assert service["metadata"]["annotations"]["cloud.google.com/load-balancer-type"] == "Internal"
    assert service["metadata"]["annotations"]["networking.gke.io/internal-load-balancer-allow-global-access"] == "true"
    assert service["spec"]["ports"][0]["port"] == 8443


def test_bare_loadbalancer_fails_closed():
    assert "internalProvider=gke-internal" in render(
        GATEWAY, (*GATEWAY_VALUES, "service.type=LoadBalancer"), success=False
    )


def test_internal_lb_active_gateway_keeps_original_tls_contract():
    docs = render(GATEWAY, (*GATEWAY_VALUES, "service.type=LoadBalancer", "service.internalProvider=gke-internal"))
    service = next(doc for doc in docs if doc["kind"] == "Service")
    assert service["metadata"]["annotations"]["cloud.google.com/load-balancer-type"] == "Internal"
    assert any(doc["kind"] == "Deployment" for doc in docs)


def test_app_default_is_unchanged_and_mtls_wires_both_containers():
    normal = render(APP, ())
    assert not any("public-gateway" in doc.get("metadata", {}).get("name", "") for doc in normal)
    docs = render(
        APP,
        (
            "adminGateway.enabled=true",
            "adminGateway.existingSecret=operator-gateway",
            "adminGateway.host=admin.example.com",
            "adminGateway.image=registry.example.com/fla-admin-gateway:test",
        ),
    )
    for component in ("frontend", "backend"):
        deployment = next(
            doc for doc in docs if doc["kind"] == "Deployment" and doc["metadata"]["name"].endswith(f"-{component}")
        )
        env = {item["name"]: item for item in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
        assert env["ADMIN_GATEWAY_SECRET"]["valueFrom"]["secretKeyRef"]["name"] == "operator-gateway"
        assert env["ADMIN_GATEWAY_REQUIRED"]["value"] == "1"
        if component == "backend":
            assert env["ADMIN_GATEWAY_REQUIRED"]["value"] == "1"
            assert "admin.example.com" in env["LOCAL_HOSTS"]["value"].split(",")
    ingress = next(doc for doc in docs if doc["kind"] == "Ingress")
    for rule in ingress["spec"]["rules"]:
        assert all(path["backend"]["service"]["name"].endswith("-public-gateway") for path in rule["http"]["paths"])
    public = next(
        doc for doc in docs if doc["kind"] == "ConfigMap" and doc["metadata"]["name"].endswith("-public-gateway")
    )
    assert "request_header -X-Admin-Gateway-Token" in public["data"]["Caddyfile"]
    assert "request_header -X-Admin-Token" in public["data"]["Caddyfile"]
    assert "-X-Remote-Analyst" not in public["data"]["Caddyfile"]


def _empty_dirs(docs):
    return [
        volume["emptyDir"]
        for doc in docs
        if doc and doc["kind"] == "Deployment"
        for volume in doc["spec"]["template"]["spec"].get("volumes", [])
        if "emptyDir" in volume
    ]


def test_every_emptydir_is_size_capped_for_admission_policies():
    gateway = _empty_dirs(render(GATEWAY, GATEWAY_VALUES))
    app = _empty_dirs(
        render(
            APP,
            (
                "adminGateway.enabled=true",
                "adminGateway.existingSecret=operator-gateway",
                "adminGateway.host=admin.example.com",
                "adminGateway.image=registry.example.com/fla-admin-gateway:test",
            ),
        )
    )
    assert len(gateway) == 2
    assert all(volume.get("sizeLimit") for volume in gateway + app)
    assert all(volume["sizeLimit"] == "64Mi" for volume in gateway)


def test_gateway_image_override_renders_approved_mirror():
    image = "registry.example.com/mirror/library/caddy:2.11.4-alpine@sha256:" + "a" * 64
    docs = render(GATEWAY, (*GATEWAY_VALUES, f"image={image}"))
    deployment = next(doc for doc in docs if doc["kind"] == "Deployment")
    assert deployment["spec"]["template"]["spec"]["containers"][0]["image"] == image


def test_gateway_image_is_required_in_both_charts():
    # The upstream caddy image cannot exec with all capabilities dropped.
    without = tuple(value for value in GATEWAY_VALUES if not value.startswith("image="))
    render(GATEWAY, without, success=False)
    render(
        APP,
        (
            "adminGateway.enabled=true",
            "adminGateway.existingSecret=operator-gateway",
            "adminGateway.host=admin.example.com",
        ),
        success=False,
    )
    assert render(GATEWAY, (*without, "gateway.enabled=false"))


def test_jenkins_publishes_gateway_image_alongside_app_images():
    jenkinsfile = (ROOT / "Jenkinsfile").read_text()
    assert "dockerFile: 'deploy/admin-gateway/Dockerfile'" in jenkinsfile
    assert "FLA_ADMIN_GATEWAY_IMAGE" in jenkinsfile
    # Built, commit-tagged and release-tagged on the same paths as the app images.
    assert jenkinsfile.count("containerName: adminGatewayImage") == 2
    dockerfile = (ROOT / "deploy/admin-gateway/Dockerfile").read_text()
    assert 'test -z "$(getcap /usr/bin/caddy)"' in dockerfile
    assert "USER 1000:1000" in dockerfile
