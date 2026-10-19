"""Real OpenSSL/Caddy TLS contract tests; all PKI/processes are temporary."""

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
                    "Connection: close\r\nX-Admin-Gateway-Token: forged\r\nX-Admin-Token: forged\r\n"
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
    assert "x-admin-token" not in headers
    assert "x-remote-analyst" not in headers
    assert "x-proxied-by-caddy" not in headers
    assert echoed["path"] == path
    assert b"forged" not in response


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
    upstream = _docker("run", "--rm", *HARDENING, "caddy:2.11.4-alpine", "caddy", "version", check=False)
    assert upstream.returncode != 0
    assert b"operation not permitted" in upstream.stderr + upstream.stdout
    stripped = _docker("run", "--rm", *HARDENING, gateway_image, "caddy", "version")
    assert stripped.stdout.startswith(b"v2.11.4")


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
