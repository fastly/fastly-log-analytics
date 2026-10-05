"""Exercise the actual rendered public gateway's credential-stripping behavior."""

import json
import os
import shutil
import socket
import subprocess
import threading
import time
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


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
                        "X-Admin-Token": "forged",
                        "X-Remote-Analyst": "true",
                    },
                )
                response = connection.getresponse()
                assert response.status == 200
                headers = {key.lower(): value for key, value in json.loads(response.read()).items()}
                assert "x-admin-gateway-token" not in headers
                assert "x-admin-token" not in headers
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
