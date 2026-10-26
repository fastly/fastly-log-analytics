"""Private, short-lived Prometheus UI access for the canonical deploy runner.

No receiver is published. Remote access uses owned SSH/kubectl forwards; local
Docker loopback access uses a bounded HTTP relay through backend-container exec.
The supervisor outlives a successful bounded runner, but expires after one hour.
"""

from __future__ import annotations

import argparse
import base64
import html
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[2]
LIFETIME = 3600
MAX_BODY = 1024 * 1024
CONTAINER_REQUEST = """
import base64,http.client,json,sys
r=json.load(sys.stdin)
c=http.client.HTTPConnection("127.0.0.1",9090,timeout=10)
c.request(r["method"],r["path"],body=base64.b64decode(r["body"]),headers=r["headers"])
s=c.getresponse()
b=s.read(16777217)
if len(b)>16777216: raise ValueError("Prometheus response too large")
print(json.dumps({"status":s.status,"type":s.getheader("Content-Type","application/octet-stream"),
                 "body":base64.b64encode(b).decode()}))
c.close()
"""
BROWSER_CHECK = """
const { chromium } = require('./frontend/node_modules/@playwright/test');
(async () => {
  const browser = await chromium.launch({headless: true});
  try {
    const page = await browser.newPage();
    await page.route('**/*', route => new URL(route.request().url()).origin === process.argv[1]
      ? route.continue() : route.abort());
    const response = await page.goto(process.argv[1] + '/query', {timeout: 30000});
    if (!response || response.status() !== 200) throw Error('UI document unavailable');
    if (!(await page.title()).includes('Prometheus')) throw Error('Wrong UI');
    await page.locator('.cm-editor').first().waitFor({state:'visible', timeout:30000});
    await page.getByRole('button', {name:'Execute',exact:true}).waitFor({state:'visible',timeout:30000});
    if (new URL(page.url()).origin !== process.argv[1]) throw Error('UI redirected');
    console.log(JSON.stringify({ui_verified:true}));
  } finally { await browser.close(); }
})().catch(() => { console.error('Prometheus browser verification failed'); process.exit(1); });
"""


def private_origin(origin: str) -> str:
    """Only numeric IPv4 loopback origins, never auth, query or path values."""
    parts = urlsplit(origin)
    if (
        parts.scheme != "http"
        or parts.hostname != "127.0.0.1"
        or parts.username is not None
        or parts.password is not None
        or parts.path
        or parts.query
        or parts.fragment
        or not parts.port
        or origin != f"http://127.0.0.1:{parts.port}"
    ):
        raise ValueError("Metrics UI requires a credential-free loopback origin")
    return origin


def metrics_ui_link(origin: str, verified: bool) -> str:
    """Never publish an unverified forward or an arbitrary HTML/URL value."""
    if not verified:
        return ""
    url = html.escape(private_origin(origin) + "/query", quote=True)
    return (
        f'<a href="{url}" target="_blank" rel="noopener noreferrer" '
        'class="text-sky-400 hover:underline">Metrics UI</a>'
        ' <span class="text-slate-400 text-[9px]">(private; expires within 1h)</span>'
    )


def verify_ui(origin: str, receipt_path: Path | None = None) -> None:
    """Require a rendered real query editor, not port/health/API success."""
    private_origin(origin)
    if receipt_path is not None:
        # Reuse the canonical verifier's bounded query/identity checks. This is
        # UI-to-receiver attribution, not a second implementation of OTLP proof.
        import httpx

        from scripts.dev.verify_observability import ProbeReceipt, _metric_matches, _receiver_json

        receipt = ProbeReceipt.model_validate_json(receipt_path.read_text())
        if not -5 <= time.time() - receipt.emitted_at.timestamp() <= 300:
            raise ValueError("UI attribution receipt expired")
        query = (
            f'app_observability_probe_total{{job="{receipt.service_name}",instance="{receipt.service_instance_id}"}}'
        )
        with httpx.Client(verify=True, trust_env=False, follow_redirects=False) as client:
            payload = _receiver_json(client, origin + "/api/v1/query", params={"query": query}, remaining=10)
        if payload is None or not _metric_matches(
            payload, job=receipt.service_name, instance=str(receipt.service_instance_id), emitted_at=receipt.emitted_at
        ):
            raise ValueError("Metrics UI forward does not reach the verified receiver")
    result = subprocess.run(
        ["node", "-e", BROWSER_CHECK, origin],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
        timeout=75,
    )
    if json.loads(result.stdout).get("ui_verified") is not True:
        raise ValueError("Prometheus UI was not verified")


def unused_loopback_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def write_private(path: Path, data: dict) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(data, stream)


def start_access(directory: Path, mode: str, command: list[str], port: int) -> tuple[Path, str]:
    """Start one runner-owned supervisor; its private state permits exact stop."""
    if mode not in {"proxy", "forward"} or not command:
        raise ValueError("Unknown private UI access mode")
    token = uuid4().hex
    state = directory / f"ui-{token}.json"
    write_private(state, {"token": token, "mode": mode, "command": command, "port": port})
    process = subprocess.Popen(
        [sys.executable, "-m", "scripts.lib.observability_ui", "serve", str(state), token],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    # PID is separate so the supervisor can read immutable input before or after
    # the parent writes it. No shell job is disowned by the deployment runner.
    write_private(state.with_suffix(".pid"), {"pid": process.pid, "token": token})
    return state, private_origin(f"http://127.0.0.1:{port}")


def stop_access(state: Path) -> None:
    """Signal only the still-owned supervisor, guarding against PID recycling."""
    data = json.loads(state.read_text())
    pid = json.loads(state.with_suffix(".pid").read_text())["pid"]
    command = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    ).stdout
    if "scripts.lib.observability_ui serve" in command and data["token"] in command:
        try:
            os.killpg(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass


def report_link(path: Path) -> str:
    """Hide expired/dead access, even in a report refreshed after cleanup."""
    if not path.exists():
        return ""
    data = json.loads(path.read_text())
    if data.get("ui_verified") is not True or data["expires_at"] <= time.time():
        return ""
    state = Path(data["state"])
    owned = json.loads(state.read_text())
    pid = json.loads(state.with_suffix(".pid").read_text())["pid"]
    command = subprocess.run(
        ["ps", "-p", str(pid), "-o", "command="],
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    ).stdout
    if "scripts.lib.observability_ui serve" not in command or owned["token"] not in command:
        return ""
    origin = private_origin(data["origin"])
    with socket.create_connection(("127.0.0.1", urlsplit(origin).port), timeout=1):
        pass
    return metrics_ui_link(origin, True)


def _proxy_server(port: int, command: list[str]) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            pass  # Paths/queries must not enter logs.

        def do_GET(self):
            self.relay()

        def do_POST(self):
            self.relay()

        def relay(self):
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > MAX_BODY or self.headers.get("Transfer-Encoding"):
                    raise ValueError("Unsupported body")
                if not self.path.startswith("/") or self.path.startswith("//"):
                    raise ValueError("Origin-form request required")
                payload = {
                    "method": self.command,
                    "path": self.path,
                    "body": base64.b64encode(self.rfile.read(length)).decode(),
                    "headers": {"Content-Type": self.headers.get("Content-Type", "application/json")},
                }
                response = subprocess.run(
                    [*command, "python", "-c", CONTAINER_REQUEST],
                    input=json.dumps(payload),
                    capture_output=True,
                    text=True,
                    check=True,
                    timeout=15,
                )
                data = json.loads(response.stdout)
                body = base64.b64decode(data["body"], validate=True)
                self.send_response(int(data["status"]))
                self.send_header("Content-Type", data["type"])
                self.send_header("Content-Length", str(len(body)))
                self.send_header("X-Content-Type-Options", "nosniff")
                self.end_headers()
                self.wfile.write(body)
            except (ValueError, OSError, subprocess.SubprocessError):
                self.send_error(502, "Private Metrics UI unavailable")

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    return server


def _serve(state: Path, token: str) -> None:
    config = json.loads(state.read_text())
    if config["token"] != token or state.stat().st_mode & 0o077:
        raise ValueError("Invalid supervisor ownership")
    child = None
    server = None
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    try:
        if config["mode"] == "proxy":
            server = _proxy_server(config["port"], config["command"])
            threading.Thread(target=server.serve_forever, daemon=True).start()
        else:
            child = subprocess.Popen(config["command"], stdin=subprocess.DEVNULL)
        deadline = time.monotonic() + LIFETIME
        while time.monotonic() < deadline and not stop.wait(1):
            if child and child.poll() is not None:
                return
    finally:
        if server:
            server.shutdown()
            server.server_close()
        if child and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["serve", "stop", "stop-directory", "link"])
    parser.add_argument("state", type=Path)
    parser.add_argument("token", nargs="?")
    args = parser.parse_args()
    if args.operation == "link":
        try:
            print(report_link(args.state))
        except (OSError, ValueError, KeyError, subprocess.SubprocessError):
            print("")  # Never fall back to an unchecked URL.
    elif args.operation == "stop-directory":
        for path in args.state.glob("*-link.json"):
            path.unlink()
        for path in args.state.glob("ui-*.json"):
            stop_access(path)
    elif args.operation == "stop":
        stop_access(args.state)
    else:
        _serve(args.state, args.token)


if __name__ == "__main__":
    main()
