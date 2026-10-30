"""Real OpenSSL/Caddy TLS contract tests; all PKI/processes are temporary."""

from __future__ import annotations

import contextlib
import hmac
import json
import os
import shutil
import socket
import ssl
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

from scripts.lib import admin_tls

ROOT = Path(__file__).resolve().parents[1]
CLI = ROOT / "scripts/admin_certificates.py"
CONFIG = ROOT / "deploy/chart/admin-gateway/files/Caddyfile"


def cli(*args: object, success: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run([sys.executable, str(CLI), *(str(arg) for arg in args)], capture_output=True, text=True)
    assert (result.returncode == 0) == success, result.stderr
    return result


def openssl(*args: object) -> str:
    result = subprocess.run(["openssl", *(str(arg) for arg in args)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return result.stdout


@pytest.fixture(scope="module")
def certificate_temp(tmp_path_factory):
    temp = tmp_path_factory.mktemp("admin-pki")
    temp.chmod(0o700)
    try:
        yield temp
    finally:
        shutil.rmtree(temp)


@pytest.fixture(scope="module")
def pki(certificate_temp):
    assert shutil.which("openssl"), "Missing validation tool: openssl (install explicitly)"
    temp = certificate_temp
    password = temp / "password"
    password.write_text("temporary-test-password")
    password.chmod(0o600)
    state, rogue = temp / "state", temp / "rogue"
    cli("init", "--out", state, "--password-file", password)
    cli("init", "--out", rogue, "--password-file", password)
    server = temp / "server"
    cli("server", "--out", server, "--state", state, "--password-file", password, "--host", "admin.localhost")
    client = temp / "client"
    cli("client-request", "--out", client, "--name", "operator")
    cli("sign-client", "--out", client, "--state", state, "--password-file", password, "--csr", client / "operator.csr")
    wrong = temp / "wrong"
    cli("client-request", "--out", wrong, "--name", "rogue")
    cli("sign-client", "--out", wrong, "--state", rogue, "--password-file", password, "--csr", wrong / "rogue.csr")
    # Deliberately expired and wrong-purpose leaves: test helpers only,
    # production CLI rejects nonpositive lifetimes and enforces purpose.
    from datetime import UTC, datetime

    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.x509.oid import ExtendedKeyUsageOID

    from scripts.admin_certificates import _cert_builder

    ca_cert = x509.load_pem_x509_certificate((state / "client-ca.crt").read_bytes())
    ca_key = serialization.load_pem_private_key(
        (state / "client-ca.key").read_bytes(), password=b"temporary-test-password"
    )
    csr_obj = x509.load_pem_x509_csr((client / "operator.csr").read_bytes())
    expired = (
        _cert_builder(
            csr_obj.subject,
            ca_cert.subject,
            csr_obj.public_key(),
            ca_key.public_key(),
            365,
            start=datetime(2020, 1, 1, tzinfo=UTC),
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    (client / "expired.crt").write_bytes(expired.public_bytes(serialization.Encoding.PEM))
    wrong_purpose = (
        _cert_builder(csr_obj.subject, ca_cert.subject, csr_obj.public_key(), ca_key.public_key(), 1)
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False)
        .sign(ca_key, hashes.SHA256())
    )
    (client / "wrong-purpose.crt").write_bytes(wrong_purpose.public_bytes(serialization.Encoding.PEM))
    bundle = temp / "bundle"
    cli(
        "bundle",
        "--out",
        bundle,
        "--state",
        state,
        "--server-cert",
        server / "server.crt",
        "--server-key",
        server / "server.key",
    )
    return {
        "temp": temp,
        "password": password,
        "state": state,
        "server": server,
        "client": client,
        "wrong": wrong,
        "bundle": bundle,
    }


def test_certificate_lifecycle_and_key_boundaries(pki):
    state, client = pki["state"], pki["client"]
    cli("check", "--cert", client / "client.crt", "--ca", state / "client-ca.crt")
    cli("check", "--cert", pki["server"] / "server.crt", "--ca", state / "server-ca.crt", "--purpose", "sslserver")
    cli(
        "export-client",
        "--out",
        client,
        "--key",
        client / "operator.key",
        "--cert",
        client / "client.crt",
        "--ca",
        state / "client-ca.crt",
        "--password-file",
        pki["password"],
    )
    assert (client / "client.p12").stat().st_mode & 0o077 == 0
    assert set(path.name for path in pki["bundle"].iterdir()) == {
        "server.crt",
        "server.key",
        "client-ca.crt",
        "ADMIN_GATEWAY_SECRET",
    }
    assert all(path.stat().st_mode & 0o077 == 0 for path in pki["bundle"].iterdir())
    assert "ENCRYPTED PRIVATE KEY" in (state / "client-ca.key").read_text()
    result = cli("client-request", "--out", client, "--name", "operator", success=False)
    assert "File exists" in result.stderr
    result = cli("check", "--cert", client / "expired.crt", "--ca", state / "client-ca.crt", success=False)
    assert "failed" in result.stderr


def test_rejects_repository_outputs_and_unsafe_input(pki):
    result = cli("init", "--out", ROOT / "must-not-exist-pki", "--password-file", pki["password"], success=False)
    assert "outside the repository" in result.stderr
    assert not (ROOT / "must-not-exist-pki").exists()
    result = cli("client-request", "--out", pki["temp"] / "bad-client", "--name", "../escape", success=False)
    assert "name must" in result.stderr
    result = cli(
        "server",
        "--out",
        pki["temp"] / "bad-server",
        "--host",
        "*.example.com",
        "--state",
        pki["state"],
        "--password-file",
        pki["password"],
        success=False,
    )
    assert "explicit DNS hostname" in result.stderr
    result = cli(
        "init", "--out", pki["temp"] / "bad-days", "--days", "0", "--password-file", pki["password"], success=False
    )
    assert "--days" in result.stderr


def test_server_certificate_supports_ip_san_without_dns(pki):
    server = pki["temp"] / "ip-server"
    cli("server", "--out", server, "--state", pki["state"], "--password-file", pki["password"], "--host", "10.20.30.40")
    openssl("x509", "-in", server / "server.crt", "-checkip", "10.20.30.40", "-noout")
    extensions = openssl("x509", "-in", server / "server.crt", "-noout", "-ext", "subjectAltName")
    assert "IP Address:10.20.30.40" in extensions
    assert "DNS:" not in extensions


class Echo(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/api/fail":
            self.connection.close()
            return
        body = json.dumps({"path": self.path, "headers": dict(self.headers)}).encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@contextlib.contextmanager
def run_gateway(pki, host: str, bundle: Path, name: str):
    if not shutil.which("caddy"):
        if os.getenv("ADMIN_MTLS_TEST_REQUIRED") == "1":
            pytest.fail("Missing validation tool: caddy >=2.8 (install explicitly)")
        pytest.skip("Optional Caddy runtime not installed; scripts/validate_admin_gateway.sh requires it")
    upstream = ThreadingHTTPServer(("127.0.0.1", 0), Echo)
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
    config = pki["temp"] / f"{name}.Caddyfile"
    config.write_text(CONFIG.read_text().replace("/certs/", f"{bundle}/"))
    env = {
        **os.environ,
        "ADMIN_GATEWAY_HOST": host,
        "ADMIN_GATEWAY_PORT": str(port),
        "ADMIN_GATEWAY_BIND": "127.0.0.1",
        "ADMIN_GATEWAY_SECRET": (pki["state"] / "gateway-secret").read_text(),
        "ADMIN_GATEWAY_FRONTEND": f"127.0.0.1:{upstream.server_port}",
        "ADMIN_GATEWAY_BACKEND": f"127.0.0.1:{upstream.server_port}",
        "XDG_DATA_HOME": str(pki["temp"] / f"{name}-data"),
        "XDG_CONFIG_HOME": str(pki["temp"] / f"{name}-config"),
    }
    process = subprocess.Popen(
        ["caddy", "run", "--config", str(config), "--adapter", "caddyfile"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            assert process.poll() is None, "Caddy failed to start"
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                    break
            except OSError:
                time.sleep(0.05)
        else:
            pytest.fail("Caddy readiness timed out")
        yield port, env
    finally:
        process.terminate()
        _, logs = process.communicate(timeout=10)
        upstream.shutdown()
        upstream.server_close()
        thread.join(timeout=5)
        # No gateway credential in Caddy startup/runtime logs.
        assert env["ADMIN_GATEWAY_SECRET"] not in logs


@pytest.fixture(scope="module")
def gateway(pki):
    with run_gateway(pki, "admin.localhost", pki["bundle"], "dns") as running:
        yield running


@pytest.fixture(scope="module")
def ip_gateway(pki):
    # IP-literal origin (Compose 127.0.0.1, Kubernetes LoadBalancer IP):
    # browsers, curl and Python send NO SNI for an IP host.
    server, bundle = pki["temp"] / "ip-gateway-server", pki["temp"] / "ip-gateway-bundle"
    cli("server", "--out", server, "--state", pki["state"], "--password-file", pki["password"], "--host", "127.0.0.1")
    cli(
        "bundle",
        "--out",
        bundle,
        "--state",
        pki["state"],
        "--server-cert",
        server / "server.crt",
        "--server-key",
        server / "server.key",
    )
    with run_gateway(pki, "127.0.0.1", bundle, "ip") as running:
        yield running


def tls_request(
    pki, gateway, cert=None, key=None, path="/dashboard", *, sni="admin.localhost", host=None, check_hostname=True
):
    """``sni=None`` sends no SNI. ``check_hostname=False`` is test-only: the
    server chain is still verified against the test CA so a failure isolates
    the server's client-auth decision from the client's name check."""
    context = ssl.create_default_context(cafile=str(pki["state"] / "server-ca.crt"))
    if sni is None or not check_hostname:
        context.check_hostname = False
    if cert:
        context.load_cert_chain(str(cert), str(key))
    port, _ = gateway
    host = host or sni or "admin.localhost"
    with socket.create_connection(("127.0.0.1", port), timeout=3) as raw:
        with context.wrap_socket(raw, server_hostname=sni) as connection:
            connection.sendall(
                (
                    f"GET {path} HTTP/1.1\r\nHost: {host}:{port}\r\n"
                    "Connection: close\r\nX-Admin-Gateway-Token: forged\r\n"
                    "X-Remote-Analyst: true\r\nX-Proxied-By-Caddy: true\r\n\r\n"
                ).encode()
            )
            parts = []
            while data := connection.recv(65536):
                parts.append(data)
            return b"".join(parts)


def bad_client(pki, client_type):
    return {
        "missing": (None, None),
        "untrusted": (pki["wrong"] / "client.crt", pki["wrong"] / "rogue.key"),
        "expired": (pki["client"] / "expired.crt", pki["client"] / "operator.key"),
        "wrong-purpose": (pki["client"] / "wrong-purpose.crt", pki["client"] / "operator.key"),
    }[client_type]


@pytest.mark.parametrize("client_type", ["missing", "untrusted", "expired", "wrong-purpose"])
def test_mtls_rejects_bad_clients_before_http(pki, gateway, client_type):
    with pytest.raises((ssl.SSLError, ConnectionResetError)):
        tls_request(pki, gateway, *bad_client(pki, client_type))


# Regression: with a host-addressed site Caddy only attached client auth to an
# SNI-matched policy, so a no-SNI ClientHello fell through to a catch-all
# policy (TLS internal error, or a handshake WITHOUT client auth when a cert
# matched the local address).
@pytest.mark.parametrize("sni", [None, "127.0.0.1", "evil.example"], ids=["no-sni", "ip-host", "foreign-sni"])
@pytest.mark.parametrize("client_type", ["missing", "untrusted", "expired", "wrong-purpose"])
def test_ip_origin_rejects_bad_clients_for_every_sni(pki, ip_gateway, client_type, sni):
    # Hostname check off so a foreign SNI cannot fail client-side first; a
    # client-side verification error would make this a false positive.
    with pytest.raises((ssl.SSLError, ConnectionResetError)) as excinfo:
        tls_request(pki, ip_gateway, *bad_client(pki, client_type), sni=sni, host="127.0.0.1", check_hostname=False)
    assert not isinstance(excinfo.value, ssl.SSLCertVerificationError)


def test_ip_origin_valid_client_with_foreign_sni_reaches_upstream(pki, ip_gateway):
    # Positive control for the rejection matrix: the same foreign SNI with a
    # valid client certificate and the exact admin Host is served.
    response = tls_request(
        pki,
        ip_gateway,
        pki["client"] / "client.crt",
        pki["client"] / "operator.key",
        "/api/bootstrap",
        sni="evil.example",
        host="127.0.0.1",
        check_hostname=False,
    )
    assert response.startswith(b"HTTP/1.1 200")
    headers = {k.lower(): v for k, v in json.loads(response.split(b"\r\n\r\n", 1)[1])["headers"].items()}
    assert hmac.compare_digest(headers["x-admin-gateway-token"], ip_gateway[1]["ADMIN_GATEWAY_SECRET"])


def test_ip_origin_client_still_verifies_server_name(pki, ip_gateway):
    # Production clients keep hostname verification: the IP-SAN server cert
    # does not satisfy a foreign name.
    with pytest.raises(ssl.SSLCertVerificationError):
        tls_request(pki, ip_gateway, pki["client"] / "client.crt", pki["client"] / "operator.key", sni="evil.example")


@pytest.mark.parametrize("sni", [None, "127.0.0.1"], ids=["no-sni", "ip-host"])
def test_ip_origin_without_sni_serves_valid_client(pki, ip_gateway, sni):
    # server_hostname="127.0.0.1" is an IP literal: CPython sends no SNI but
    # still verifies the server certificate's IP SAN.
    response = tls_request(
        pki,
        ip_gateway,
        pki["client"] / "client.crt",
        pki["client"] / "operator.key",
        "/api/bootstrap",
        sni=sni,
        host="127.0.0.1",
    )
    assert response.startswith(b"HTTP/1.1 200")
    headers = {k.lower(): v for k, v in json.loads(response.split(b"\r\n\r\n", 1)[1])["headers"].items()}
    assert hmac.compare_digest(headers["x-admin-gateway-token"], ip_gateway[1]["ADMIN_GATEWAY_SECRET"])
    assert b"forged" not in response


@pytest.mark.parametrize("host", ["evil.example", "localhost", "127.0.0.2"])
def test_valid_client_with_foreign_host_header_gets_no_upstream(pki, ip_gateway, host):
    response = tls_request(
        pki, ip_gateway, pki["client"] / "client.crt", pki["client"] / "operator.key", sni=None, host=host
    )
    assert not response.startswith(b"HTTP/1.1 200")
    assert ip_gateway[1]["ADMIN_GATEWAY_SECRET"].encode() not in response
    assert b"x-admin-gateway-token" not in response.lower()


@pytest.mark.parametrize("path", ["/dashboard", "/api/bootstrap", "/api/admin/system-metrics"])
def test_mtls_overwrites_credentials_and_removes_analyst_markers(pki, gateway, path):
    response = tls_request(pki, gateway, pki["client"] / "client.crt", pki["client"] / "operator.key", path)
    assert response.startswith(b"HTTP/1.1 200")
    echoed = json.loads(response.split(b"\r\n\r\n", 1)[1])
    headers = {name.lower(): value for name, value in echoed["headers"].items()}
    assert hmac.compare_digest(headers["x-admin-gateway-token"], gateway[1]["ADMIN_GATEWAY_SECRET"])
    assert "x-remote-analyst" not in headers
    assert "x-proxied-by-caddy" not in headers
    assert echoed["path"] == path
    assert b"forged" not in response


def test_caddyfile_keeps_mtls_and_share_sites_isolated():
    share_caddyfile = (ROOT / "Caddyfile").read_text()
    assert "8443" not in share_caddyfile
    assert ":80 {" in share_caddyfile
    assert "https://:{$ADMIN_GATEWAY_PORT:8443}" in CONFIG.read_text()


def test_runtime_config_does_not_embed_gateway_credential(pki, gateway):
    config = pki["temp"] / "dns.Caddyfile"
    result = subprocess.run(["caddy", "adapt", "--config", str(config)], env=gateway[1], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert gateway[1]["ADMIN_GATEWAY_SECRET"] not in result.stdout


def test_every_tls_handshake_shares_one_client_auth_policy(pki, gateway):
    # A second (catch-all) policy is exactly what let no-SNI handshakes skip
    # client auth; strict_sni_host may only be off while this holds.
    result = subprocess.run(
        ["caddy", "adapt", "--config", str(CONFIG)], env=gateway[1], capture_output=True, text=True, check=True
    )
    (server,) = json.loads(result.stdout)["apps"]["http"]["servers"].values()
    (policy,) = server["tls_connection_policies"]
    assert "match" not in policy
    assert policy["client_authentication"]["mode"] == "require_and_verify"
    assert server["strict_sni_host"] is False


def test_upstream_error_logs_do_not_contain_gateway_credential(pki, gateway):
    # Force Caddy's default HTTP error logger, not just its access logger.
    # The fixture checks all runtime log output after process termination.
    response = tls_request(pki, gateway, pki["client"] / "client.crt", pki["client"] / "operator.key", "/api/fail")
    assert response.startswith(b"HTTP/1.1 502")
    assert gateway[1]["ADMIN_GATEWAY_SECRET"].encode() not in response


def test_stdlib_diagnostic_transport_verifies_real_server_and_client_tls(pki, monkeypatch):
    server_dir = pki["temp"] / "diagnostic-server"
    cli(
        "server",
        "--out",
        server_dir,
        "--state",
        pki["state"],
        "--password-file",
        pki["password"],
        "--host",
        "127.0.0.1",
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), Echo)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(server_dir / "server.crt", server_dir / "server.key")
    context.load_verify_locations(pki["state"] / "client-ca.crt")
    context.verify_mode = ssl.CERT_REQUIRED
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f"https://127.0.0.1:{server.server_port}"
    for name in (
        "ADMIN_GATEWAY_CLIENT_ORIGIN",
        "ADMIN_GATEWAY_CLIENT_CERT",
        "ADMIN_GATEWAY_CLIENT_KEY",
        "ADMIN_GATEWAY_SERVER_CA",
    ):
        monkeypatch.delenv(name, raising=False)
    config = {
        "origin": origin,
        "cert": str(pki["client"] / "client.crt"),
        "key": str(pki["client"] / "operator.key"),
        "ca": str(pki["state"] / "server-ca.crt"),
    }
    try:
        monkeypatch.setenv("ADMIN_GATEWAY_ENDPOINTS", json.dumps({"remote-high-scale": config}))
        admin_tls.validate_admin_tls_config()
        with admin_tls.admin_urlopen(f"{origin}/api/bootstrap") as response:
            assert response.status == 200
        monkeypatch.setenv("ADMIN_GATEWAY_ENDPOINTS", "{}")
        with pytest.raises(OSError):
            admin_tls.admin_urlopen(f"{origin}/api/bootstrap")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


IMAGE_CONTEXT = ROOT / "deploy/admin-gateway"
# Mirrors docker-compose.admin-mtls.yml and the chart's container securityContext.
HARDENING = ("--user", "1000:1000", "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true")


def _docker(*args: str, env=None, stdin: bytes | None = None, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(["docker", *args], input=stdin, env=env, capture_output=True, timeout=300)
    assert not check or result.returncode == 0, result.stderr.decode(errors="replace")
    return result


@pytest.fixture(scope="module")
def gateway_image():
    if not shutil.which("docker") or _docker("info", check=False).returncode != 0:
        if os.getenv("ADMIN_MTLS_TEST_REQUIRED") == "1":
            pytest.fail("Missing validation tool: running docker daemon (start it explicitly)")
        pytest.skip("Docker daemon unavailable; scripts/validate_admin_gateway.sh requires it")
    tag = f"fla-admin-gateway-test:{os.getpid()}"
    _docker("build", "-q", "-t", tag, str(IMAGE_CONTEXT))
    try:
        yield tag
    finally:
        _docker("rmi", "-f", tag, check=False)


def test_upstream_caddy_filecap_cannot_exec_with_all_capabilities_dropped(gateway_image):
    # Root-cause pin: the official binary's cap_net_bind_service file capability
    # is outside the empty bounding set, so the kernel refuses exec outright.
    upstream = _docker("run", "--rm", *HARDENING, "caddy:2.11.7-alpine", "caddy", "version", check=False)
    assert upstream.returncode != 0
    assert b"operation not permitted" in upstream.stderr + upstream.stdout
    stripped = _docker("run", "--rm", *HARDENING, gateway_image, "caddy", "version")
    assert stripped.stdout.startswith(b"v2.11.7")


@pytest.fixture(scope="module")
def container_gateway(pki, gateway_image):
    # Stream bundle + shared Caddyfile into a tmpfs instead of bind-mounting, so
    # the test does not depend on which host paths the Docker VM shares.
    staging = pki["temp"] / "image-input"
    staging.mkdir(mode=0o700)
    (staging / "Caddyfile").write_bytes(CONFIG.read_bytes())
    for name in ("server.crt", "server.key", "client-ca.crt"):
        shutil.copyfile(pki["bundle"] / name, staging / name)
    archive = subprocess.run(["tar", "-c", "-f", "-", "-C", str(staging), "."], capture_output=True, check=True).stdout
    shutil.rmtree(staging)
    env = {**os.environ, "ADMIN_GATEWAY_SECRET": (pki["state"] / "gateway-secret").read_text()}
    name = f"fla-admin-gateway-test-{os.getpid()}"
    tmpfs = "mode=0700,uid=1000,gid=1000"
    process = subprocess.Popen(
        [
            "docker",
            "run",
            "-i",
            "--name",
            name,
            *HARDENING,
            "--tmpfs",
            f"/certs:{tmpfs}",
            "--tmpfs",
            f"/data:{tmpfs}",
            "--tmpfs",
            f"/config:{tmpfs}",
            "-p",
            "127.0.0.1::8443",
            "-e",
            "ADMIN_GATEWAY_HOST=admin.localhost",
            "-e",
            "ADMIN_GATEWAY_BIND=0.0.0.0",
            "-e",
            "ADMIN_GATEWAY_PORT=8443",
            "-e",
            "ADMIN_GATEWAY_SECRET",
            "-e",
            "ADMIN_GATEWAY_FRONTEND=127.0.0.1:9",
            "-e",
            "ADMIN_GATEWAY_BACKEND=127.0.0.1:9",
            "--entrypoint",
            "sh",
            gateway_image,
            "-c",
            "tar -x -f - -C /certs && exec caddy run --config /certs/Caddyfile --adapter caddyfile",
        ],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        process.stdin.write(archive)
        process.stdin.close()
        deadline = time.monotonic() + 30
        while _docker("port", name, "8443/tcp", check=False).returncode != 0:
            assert process.poll() is None and time.monotonic() < deadline, "gateway container exited"
            time.sleep(0.2)
        port = int(_docker("port", name, "8443/tcp").stdout.decode().strip().rsplit(":", 1)[1])
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            assert process.poll() is None, _docker("logs", name, check=False).stderr.decode(errors="replace")
            if b"serving initial configuration" in _docker("logs", name).stderr:
                break
            time.sleep(0.2)
        else:
            pytest.fail("containerised gateway readiness timed out")
        yield port, env
    finally:
        logs = _docker("logs", name, check=False)
        _docker("rm", "-f", name, check=False)
        process.wait(timeout=30)
        assert env["ADMIN_GATEWAY_SECRET"].encode() not in logs.stdout + logs.stderr


def test_hardened_container_enforces_client_mtls(pki, container_gateway):
    with pytest.raises((ssl.SSLError, ConnectionResetError)):
        tls_request(pki, container_gateway)
    with pytest.raises((ssl.SSLError, ConnectionResetError)):
        tls_request(pki, container_gateway, sni=None)
    for sni in ("admin.localhost", None):
        # sni=None reproduces the live IP-origin failure: the container's local
        # address matches no certificate, so the catch-all policy had none.
        response = tls_request(
            pki,
            container_gateway,
            pki["client"] / "client.crt",
            pki["client"] / "operator.key",
            sni=sni,
            host="admin.localhost",
        )
        # Unreachable upstream: a 502 proves TLS + client auth succeeded in-container.
        assert response.startswith(b"HTTP/1.1 502")
        assert b"forged" not in response


# ── Merged from tests/test_security_topology.py ──

"""Consolidated tests for test_security_topology."""


# Trust topology invariant — every test in this file pins one of the two
# protections that keep request.client.host on the real client IP.
# (scoped pytestmark) pytestmark = pytest.mark.security_regression
import asyncio

import pytest

# ── 1. Startup assertion behavior ──────────────────────────────────────────


@pytest.mark.security_regression
def test_startup_assertion_warns_when_unset(monkeypatch):
    """No env vars set → warns but does not raise (local dev mode)."""
    from unittest.mock import patch

    from backend import main

    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("REQUIRE_PROXY_HEADERS", raising=False)
    monkeypatch.delenv("STRICT_DATA_DIR_CHECK", raising=False)

    with patch.object(main.logging, "warning") as mock_warning:
        main._enforce_proxy_headers_configured()

    assert mock_warning.called, "expected warning when TRUSTED_PROXY_IPS unset"
    call_msg = " ".join(str(a) for a in mock_warning.call_args.args)
    assert "TRUSTED_PROXY_IPS" in call_msg


@pytest.mark.security_regression
def test_startup_assertion_passes_when_set(monkeypatch):
    """With TRUSTED_PROXY_IPS set, function returns cleanly."""
    from unittest.mock import patch

    from backend import main

    monkeypatch.setenv("TRUSTED_PROXY_IPS", "127.0.0.1")
    with patch.object(main.logging, "info") as mock_info:
        # Should not raise.
        main._enforce_proxy_headers_configured()
    assert mock_info.called, "expected info log when TRUSTED_PROXY_IPS is set"
    call_msg = " ".join(str(a) for a in mock_info.call_args.args)
    assert "proxy-headers trust set" in call_msg


@pytest.mark.security_regression
def test_startup_assertion_fatals_when_strict(monkeypatch):
    """In strict mode (REQUIRE_PROXY_HEADERS=1) the missing env var aborts boot."""
    from backend import main

    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.setenv("REQUIRE_PROXY_HEADERS", "1")

    with pytest.raises(RuntimeError, match="TRUSTED_PROXY_IPS"):
        main._enforce_proxy_headers_configured()


@pytest.mark.security_regression
def test_startup_assertion_fatals_when_strict_data_dir_check_set(monkeypatch):
    """Production sets STRICT_DATA_DIR_CHECK=1 — that should also force strict
    proxy-headers enforcement so prod can't accidentally boot without it."""
    from backend import main

    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("REQUIRE_PROXY_HEADERS", raising=False)
    monkeypatch.setenv("STRICT_DATA_DIR_CHECK", "1")

    with pytest.raises(RuntimeError, match="TRUSTED_PROXY_IPS"):
        main._enforce_proxy_headers_configured()


@pytest.mark.security_regression
@pytest.mark.parametrize("var", ["TRUSTED_PROXY_IPS", "UVICORN_FORWARDED_ALLOW_IPS"])
@pytest.mark.parametrize("value", ["*", "127.0.0.1,*", "*,10.0.0.0/8"])
def test_startup_assertion_rejects_wildcard_trust(monkeypatch, var, value):
    """`*` in either trust variable means uvicorn honors XFF from ANY peer —
    any direct client can stamp X-Forwarded-For: 127.0.0.1 and be classified
    loopback-admin. The guard must refuse to boot, strict mode or not."""
    from backend import main

    monkeypatch.delenv("TRUSTED_PROXY_IPS", raising=False)
    monkeypatch.delenv("UVICORN_FORWARDED_ALLOW_IPS", raising=False)
    monkeypatch.delenv("REQUIRE_PROXY_HEADERS", raising=False)
    monkeypatch.delenv("STRICT_DATA_DIR_CHECK", raising=False)
    monkeypatch.setenv(var, value)

    with pytest.raises(RuntimeError, match="wildcard"):
        main._enforce_proxy_headers_configured()


@pytest.mark.security_regression
def test_startup_assertion_accepts_pinned_subnet(monkeypatch):
    """A CIDR-pinned trust set (the multipod compose topology) boots cleanly."""
    from unittest.mock import patch

    from backend import main

    monkeypatch.setenv("TRUSTED_PROXY_IPS", "172.28.0.0/16")
    monkeypatch.setenv("UVICORN_FORWARDED_ALLOW_IPS", "172.28.0.0/16")
    with patch.object(main.logging, "info") as mock_info:
        main._enforce_proxy_headers_configured()
    assert mock_info.called


# ── 2. uvicorn ProxyHeadersMiddleware end-to-end ───────────────────────────


def _run_through_proxy_middleware(*, peer_ip: str, xff: str | None, trusted_hosts: str | None):
    """Drive uvicorn's ProxyHeadersMiddleware and return the rewritten client."""
    from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

    captured: dict = {}

    async def fake_app(scope, receive, send):
        captured["client"] = scope.get("client")
        captured["scheme"] = scope.get("scheme")
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"", "more_body": False})

    headers: list[tuple[bytes, bytes]] = []
    if xff:
        headers.append((b"x-forwarded-for", xff.encode()))

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": headers,
        "client": (peer_ip, 12345),
        "server": ("127.0.0.1", 8000),
    }

    if trusted_hosts is None:
        middleware = ProxyHeadersMiddleware(fake_app)
    else:
        middleware = ProxyHeadersMiddleware(fake_app, trusted_hosts=trusted_hosts)

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    sent: list = []

    async def send(message):
        sent.append(message)

    # asyncio.run creates a fresh event loop per call — needed under
    # python 3.13+/pytest-asyncio mode where get_event_loop() in a sync
    # test no longer auto-creates a loop and reusing one across tests
    # is order-dependent.
    asyncio.run(middleware(scope, receive, send))
    return captured


@pytest.mark.security_regression
def test_proxy_headers_trusts_xff_only_from_loopback():
    """The production config (trusted_hosts="127.0.0.1") must rewrite XFF when
    the peer is 127.0.0.1 (= Caddy on this host) and IGNORE XFF when the peer
    is anything else."""
    # Loopback peer + XFF set → middleware rewrites to the XFF value.
    res = _run_through_proxy_middleware(peer_ip="127.0.0.1", xff="203.0.113.7", trusted_hosts="127.0.0.1")
    assert res["client"][0] == "203.0.113.7", (
        f"loopback peer + XFF=203.0.113.7 should rewrite client to 203.0.113.7, got {res['client']}"
    )

    # Non-loopback peer + XFF set → must NOT rewrite (untrusted XFF source).
    res = _run_through_proxy_middleware(peer_ip="8.8.8.8", xff="203.0.113.7", trusted_hosts="127.0.0.1")
    assert res["client"][0] == "8.8.8.8", (
        f"non-loopback peer should keep its peer IP regardless of XFF, got {res['client']}"
    )


@pytest.mark.security_regression
def test_without_proxy_headers_xff_is_ignored_entirely():
    """Mirror of the pre-patch behavior: with no trusted_hosts configured,
    uvicorn defaults to trusting no proxy. The XFF header has no effect and
    request.client.host is the raw peer.

    This is what a deployment that DROPPED --proxy-headers would look like —
    Caddy-proxied requests would all appear as 127.0.0.1 and the IP-based
    gates would all collapse. The test proves the flag is load-bearing.
    """
    # uvicorn defaults to trusted_hosts="127.0.0.1" when constructed with no
    # args, so to simulate "dropped --proxy-headers" we explicitly pass
    # trusted_hosts="" which disables all proxy trust.
    res = _run_through_proxy_middleware(peer_ip="127.0.0.1", xff="203.0.113.7", trusted_hosts="")
    assert res["client"][0] == "127.0.0.1", f"with no proxy trust, peer IP should win, got {res['client']}"


# ── 3. Middleware-layer Host-spoof regression ──────────────────────────────


@pytest.mark.security_regression
def test_admin_host_spoof_rejected_after_phase0_fix():
    """Security regression test: a remote peer (uvicorn rewrote
    request.client.host to a public IP via --proxy-headers) sending
    `Host: localhost` must NOT be treated as local-admin."""
    from fastapi import FastAPI
    from starlette.requests import Request

    from backend.utils import tunnel
    from backend.utils.remote_access import is_request_remote

    # Reset tunnel manager to ensure we don't accidentally honor the x-remote-analyst
    # fallback during this test.
    tunnel.reset_for_tests()

    async def _fake_send(_):
        pass

    async def _fake_receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    # Synthesise a request whose TCP peer is a public IP and Host header is
    # spoofed to 'localhost'. Pre-fix this would have been classified local;
    # post-fix it's remote because the peer != loopback.
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/dashboard",
        "headers": [(b"host", b"localhost")],
        "query_string": b"",
        "client": ("8.8.8.8", 31337),
        "app": FastAPI(),
    }
    request = Request(scope, receive=_fake_receive)
    assert is_request_remote(request) is True, "remote peer with Host:localhost must classify as remote"

    # And the inverse: a loopback peer with NO marker → admin (the only path
    # legitimate admin connections take).
    scope_admin = dict(scope)
    scope_admin["client"] = ("127.0.0.1", 31337)
    scope_admin["headers"] = [(b"host", b"localhost")]
    req_admin = Request(scope_admin, receive=_fake_receive)
    assert is_request_remote(req_admin) is False, "loopback peer + no marker = admin"


# ── Merged from test_trust_topology.py ──────────────────────────────────────────────

from pathlib import Path

import pytest

# (scoped pytestmark) pytestmark = pytest.mark.security_regression


REPO_ROOT = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (REPO_ROOT / rel).read_text(encoding="utf-8")


# ── 1. Caddyfile trust topology ──────────────────────────────────────────────


@pytest.mark.security_regression
def test_caddyfile_has_from_fastly_remote_ip_matcher():
    """The ``@from_fastly_v4`` matcher gates XFF rewrite on the TCP peer
    being in Fastly's published edge ranges. Without it, a direct
    port-80 attacker can spoof X-Forwarded-For and bypass IP-based
    gates."""
    caddyfile = _read("Caddyfile")
    assert "@from_fastly_v4" in caddyfile, "missing @from_fastly_v4 matcher"
    assert "remote_ip " in caddyfile, "missing remote_ip directive"


@pytest.mark.security_regression
def test_caddyfile_rewrites_xff_only_when_peer_is_fastly():
    """The ``request_header @from_fastly_v4 X-Forwarded-For
    {http.request.header.Fastly-Client-IP}`` line is the trust-handoff
    moment — must stay scoped to the matcher."""
    caddyfile = _read("Caddyfile")
    assert "request_header @from_fastly_v4 X-Forwarded-For {http.request.header.Fastly-Client-IP}" in caddyfile, (
        "XFF rewrite missing or unscoped from @from_fastly_v4"
    )


@pytest.mark.security_regression
def test_caddyfile_share_login_rate_limit_present():
    """5 share-login attempts per minute, keyed by Fastly-Client-IP.
    Bounds brute force on the share passcode."""
    caddyfile = _read("Caddyfile")
    assert "/api/share/login" in caddyfile
    assert "rate_limit @share_login" in caddyfile
    assert "events 5" in caddyfile, "share-login rate limit no longer 5 events"


@pytest.mark.security_regression
def test_caddyfile_injects_proxied_by_caddy_header():
    """``X-Proxied-By-Caddy`` is the marker the frontend middleware reads
    to block /admin from anything that isn't reaching us through Caddy.
    Direct SSH-tunnel admin access has no such header → reaches /admin.
    Spoofing prevented because Caddy sets it unconditionally
    (overwriting any upstream value)."""
    caddyfile = _read("Caddyfile")
    assert 'request_header X-Proxied-By-Caddy "true"' in caddyfile


# ── 2. docker-compose.prod.yml backend hardening ─────────────────────────────


@pytest.mark.security_regression
def test_compose_prod_backend_binds_loopback_only():
    """``--host 127.0.0.1`` keeps the backend off the public interface.
    Combined with the GCP/AWS/Azure firewall this is defense in depth."""
    compose = _read("docker-compose.prod.yml")
    assert '"--host",\n        "127.0.0.1",' in compose, "backend --host not loopback"


@pytest.mark.security_regression
def test_compose_prod_backend_passes_proxy_headers_flag():
    """uvicorn ``--proxy-headers`` populates request.client.host from
    X-Forwarded-For (only when the TCP peer is in
    ``--forwarded-allow-ips``)."""
    compose = _read("docker-compose.prod.yml")
    assert '"--proxy-headers"' in compose


@pytest.mark.security_regression
def test_compose_prod_backend_pins_forwarded_allow_ips_to_loopback():
    """``--forwarded-allow-ips=127.0.0.1`` means uvicorn only trusts XFF
    from loopback — i.e. only Caddy on the same host. Removing this
    re-opens the leftmost-XFF spoof + the admin Host-spoof bypass."""
    compose = _read("docker-compose.prod.yml")
    assert '"--forwarded-allow-ips=127.0.0.1"' in compose


@pytest.mark.security_regression
def test_compose_prod_backend_has_memory_cap():
    """Container memory cap so an OOM-killer event doesn't take out the
    whole VM (sshd, caddy). Pre-2026-06-04 absence of this took down
    the host multiple times."""
    compose = _read("docker-compose.prod.yml")
    assert "mem_limit:" in compose, "backend mem_limit missing"
    assert "memswap_limit:" in compose, "backend memswap_limit missing"


@pytest.mark.security_regression
def test_compose_prod_bounds_duckdb_connections_below_container_limit():
    """The single-host deployment must survive dashboard and cron overlap."""
    compose = _read("docker-compose.prod.yml")
    assert "DUCKDB_POOL_WARM_AT_BOOT=true" in compose
    assert "DUCKDB_POOL_MAX_SIZE=4" in compose
    assert "DUCKDB_POOL_CONN_MEMORY_LIMIT=${DUCKDB_POOL_CONN_MEMORY_LIMIT:-6GB}" in compose
    assert "DUCKDB_MEMORY_LIMIT=6GB" in compose
    assert "PARTIAL_HOUR_MERGE_ENABLED=${PARTIAL_HOUR_MERGE_ENABLED:-true}" in compose


# ── 3. backend/main.py middleware order (ADR-04) ─────────────────────────────


@pytest.mark.security_regression
def test_middleware_order_declaration_matches_runtime():
    """``MIDDLEWARE_ORDER`` tuple in main.py matches the actual
    ``app.user_middleware`` tuple at boot. A reorder that compiles is
    not enough to ship — the boot assertion in main.py crashes start-up
    on divergence; this test catches the same drift at PR time."""
    from backend.main import MIDDLEWARE_ORDER, app

    actual = tuple(m.cls.__name__ for m in app.user_middleware)
    assert actual == MIDDLEWARE_ORDER


@pytest.mark.security_regression
def test_middleware_order_is_compress_outermost_cors_innermost():
    """Spelled-out order assertion (independent of MIDDLEWARE_ORDER) so
    a refactor of the tuple constant has to face this test, not just
    rewrite the assertion target."""
    from backend.main import app

    names = tuple(m.cls.__name__ for m in app.user_middleware)
    assert names[0] == "CompressMiddleware", "Compress not outermost"
    assert names[-1] == "CORSMiddleware", "CORS not innermost"
    # The two telemetry layers sit between Compress and RemoteAccess
    assert "BaseHTTPMiddleware" in names, "telemetry decorator missing"
    assert "TelemetryResponseBodyMiddleware" in names, "telemetry body backstop missing"
    assert "RemoteAccessMiddleware" in names, "remote-access firewall missing"


@pytest.mark.security_regression
def test_assert_middleware_order_crashes_on_violation():
    """``assert_middleware_order()`` must FAIL LOUDLY (RuntimeError) when
    the declared order doesn't match the actual order — the boot guard
    is only useful if it actually fires."""
    from fastapi import FastAPI
    from starlette.middleware.cors import CORSMiddleware

    from backend.main import assert_middleware_order

    bad_app = FastAPI()
    # Add CORS without anything else — won't match MIDDLEWARE_ORDER
    bad_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    with pytest.raises(RuntimeError, match="Middleware order violation"):
        assert_middleware_order(bad_app)


# ── Merged from test_no_trace_leakage_sweep.py ──────────────────────────────────────────────


import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

# Trace-leakage sweep is a known-easy-to-silently-regress guard against
# returning traceback strings in JSON error responses.
# (scoped pytestmark) pytestmark = pytest.mark.security_regression


def _make_leaky_app() -> FastAPI:
    """Build a tiny FastAPI app with two route shapes:

    - ``/leaky``: handler raises a generic exception → wrapped by
      ``query_errors`` → must NOT leak ``trace``.
    - ``/explicit-http``: raises HTTPException directly → must NOT leak
      ``trace`` either (defensive — the decorator passes these through but
      an upstream FastAPI bug could double-encode).
    """
    from backend.utils.router_utils import query_errors

    app = FastAPI()

    @app.get("/leaky")
    @query_errors(status_code=500)
    def leaky():
        raise RuntimeError("downstream said no")

    @app.get("/explicit-http")
    def explicit_http():
        raise HTTPException(status_code=500, detail={"error": "explicit"})

    return app


@pytest.mark.security_regression
def test_query_errors_decorator_does_not_leak_trace():
    app = _make_leaky_app()
    with TestClient(app) as client:
        r = client.get("/leaky")
    assert r.status_code == 500, r.text
    body = r.json()
    detail = body.get("detail", body)
    assert "trace" not in json.dumps(detail).lower() or "traceback" not in r.text.lower(), (
        f"stack-trace leakage regression — response leaked traceback. body={r.text[:500]}"
    )
    # Also assert the structured detail does not include a 'trace' key
    if isinstance(detail, dict):
        assert "trace" not in detail, f"detail leaked 'trace' key: {detail}"


@pytest.mark.security_regression
def test_explicit_httpexception_does_not_leak_trace():
    app = _make_leaky_app()
    with TestClient(app) as client:
        r = client.get("/explicit-http")
    assert r.status_code == 500
    body = r.json()
    detail = body.get("detail", body)
    if isinstance(detail, dict):
        assert "trace" not in detail


@pytest.mark.security_regression
@pytest.mark.parametrize(
    "url",
    [
        # Routes known to be reachable without auth/state, that should
        # consistently 500 when fed garbage. The sweep doesn't need to be
        # exhaustive — it just needs broad enough coverage that any
        # regression-shaped change to query_errors is likely to be
        # exercised by at least one route in the parametrize list.
        "/api/log-fields/catalog?service_id=does-not-exist-svc-id",
    ],
)
def test_real_routes_do_not_leak_trace_on_forced_500(url):
    """Hit a handful of real routes and assert their response (regardless
    of status) does not contain a ``trace`` key or a 'Traceback' substring.

    These should respond cleanly (200 / 400 / 404) under normal conditions
    — they don't need to be 500-ing to satisfy the test. We're guarding
    against the regression in which they DO 500 + leak.
    """
    from backend.main import app

    with TestClient(app) as client:
        r = client.get(url)

    # No 'Traceback' substring anywhere in the response body — even if the
    # status code is 200/404/400, no leakage is allowed.
    assert "Traceback (most recent call last)" not in r.text, (
        f"response leaked a Python traceback for {url}; body[:500]={r.text[:500]}"
    )

    # If the response is JSON, additionally assert no detail.trace key.
    try:
        body = r.json()
    except json.JSONDecodeError:
        return
    detail = body.get("detail") if isinstance(body, dict) else None
    if isinstance(detail, dict):
        assert "trace" not in detail, f"{url} response leaked detail.trace key: {detail}"
