"""Real OpenSSL/Caddy TLS contract tests; all PKI/processes are temporary."""

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
    # Deliberately expired leaf: test helper only, production CLI rejects
    # nonpositive lifetimes.
    (temp / "index").write_text("")
    (temp / "serial").write_text("1000\n")
    ca_config = temp / "expired-ca.conf"
    ca_config.write_text(
        f"[ca]\ndefault_ca=local\n[local]\ndatabase={temp}/index\n"
        f"new_certs_dir={temp}\ncertificate={state}/client-ca.crt\n"
        f"private_key={state}/client-ca.key\nserial={temp}/serial\n"
        "default_md=sha256\npolicy=subject\n[subject]\ncommonName=supplied\n"
    )
    openssl(
        "ca",
        "-batch",
        "-config",
        ca_config,
        "-in",
        client / "operator.csr",
        "-passin",
        f"file:{password}",
        "-startdate",
        "20200101000000Z",
        "-enddate",
        "20210101000000Z",
        "-notext",
        "-out",
        client / "expired.crt",
    )
    ext = temp / "server-only.ext"
    ext.write_text("basicConstraints=critical,CA:FALSE\nextendedKeyUsage=serverAuth\nkeyUsage=digitalSignature\n")
    openssl(
        "x509",
        "-req",
        "-in",
        client / "operator.csr",
        "-CA",
        state / "client-ca.crt",
        "-CAkey",
        state / "client-ca.key",
        "-passin",
        f"file:{password}",
        "-set_serial",
        "101",
        "-days",
        "1",
        "-extfile",
        ext,
        "-out",
        client / "wrong-purpose.crt",
    )
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


@pytest.fixture(scope="module")
def gateway(pki):
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
    config = pki["temp"] / "Caddyfile"
    config.write_text(CONFIG.read_text().replace("/certs/", f"{pki['bundle']}/"))
    env = {
        **os.environ,
        "ADMIN_GATEWAY_HOST": "admin.localhost",
        "ADMIN_GATEWAY_PORT": str(port),
        "ADMIN_GATEWAY_BIND": "127.0.0.1",
        "ADMIN_GATEWAY_SECRET": (pki["state"] / "gateway-secret").read_text(),
        "ADMIN_GATEWAY_FRONTEND": f"127.0.0.1:{upstream.server_port}",
        "ADMIN_GATEWAY_BACKEND": f"127.0.0.1:{upstream.server_port}",
        "XDG_DATA_HOME": str(pki["temp"] / "caddy-data"),
        "XDG_CONFIG_HOME": str(pki["temp"] / "caddy-config"),
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


def tls_request(pki, gateway, cert=None, key=None, path="/dashboard"):
    context = ssl.create_default_context(cafile=str(pki["state"] / "server-ca.crt"))
    if cert:
        context.load_cert_chain(str(cert), str(key))
    port, _ = gateway
    with socket.create_connection(("127.0.0.1", port), timeout=3) as raw:
        with context.wrap_socket(raw, server_hostname="admin.localhost") as connection:
            connection.sendall(
                (
                    f"GET {path} HTTP/1.1\r\nHost: admin.localhost:{port}\r\n"
                    "Connection: close\r\nX-Admin-Gateway-Token: forged\r\nX-Admin-Token: forged\r\n"
                    "X-Remote-Analyst: true\r\nX-Proxied-By-Caddy: true\r\n\r\n"
                ).encode()
            )
            parts = []
            while data := connection.recv(65536):
                parts.append(data)
            return b"".join(parts)


@pytest.mark.parametrize("client_type", ["missing", "untrusted", "expired", "wrong-purpose"])
def test_mtls_rejects_bad_clients_before_http(pki, gateway, client_type):
    choices = {
        "missing": (None, None),
        "untrusted": (pki["wrong"] / "client.crt", pki["wrong"] / "rogue.key"),
        "expired": (pki["client"] / "expired.crt", pki["client"] / "operator.key"),
        "wrong-purpose": (pki["client"] / "wrong-purpose.crt", pki["client"] / "operator.key"),
    }
    with pytest.raises((ssl.SSLError, ConnectionResetError)):
        tls_request(pki, gateway, *choices[client_type])


@pytest.mark.parametrize("path", ["/dashboard", "/api/bootstrap", "/api/admin/system-metrics"])
def test_mtls_overwrites_credentials_and_removes_analyst_markers(pki, gateway, path):
    response = tls_request(pki, gateway, pki["client"] / "client.crt", pki["client"] / "operator.key", path)
    assert response.startswith(b"HTTP/1.1 200")
    echoed = json.loads(response.split(b"\r\n\r\n", 1)[1])
    headers = {name.lower(): value for name, value in echoed["headers"].items()}
    assert hmac.compare_digest(headers["x-admin-gateway-token"], gateway[1]["ADMIN_GATEWAY_SECRET"])
    assert "x-admin-token" not in headers
    assert "x-remote-analyst" not in headers
    assert "x-proxied-by-caddy" not in headers
    assert echoed["path"] == path
    assert b"forged" not in response


def test_runtime_config_does_not_embed_gateway_credential(pki, gateway):
    config = pki["temp"] / "Caddyfile"
    result = subprocess.run(["caddy", "adapt", "--config", str(config)], env=gateway[1], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert gateway[1]["ADMIN_GATEWAY_SECRET"] not in result.stdout


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
