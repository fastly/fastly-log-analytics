#!/usr/bin/env python3
"""Local HTTP server for viewing deployment reports (reports/deploys/).

Serves the repo root as static files like a plain directory server (so the
report's log <pre> tags, screenshots, etc. all load normally), but any
request path ending in 'relics/credentials_status.json' is intercepted and
answered LIVE: it runs the Fastly API / FOS S3 / CDN edge checks against
each environment's backend right now, on that request, instead of reading a
snapshot file written at some earlier point in the deploy pipeline. This is
what makes the report's live status panel genuinely real-time under its 4s
poll interval, rather than polling a JSON file that only updates whenever
validate_credentials.py last happened to run.

A request path ending in 'relics/bootstrap_status.json?env=<id>' is answered
the same way, proxying a live GET to that environment's own '/api/bootstrap'
server-side. This exists solely because the report page (served from
localhost:41705) and each environment's backend (127.0.0.1:80/8081/3001/3002)
are different origins, and none of those backends send
Access-Control-Allow-Origin for this origin — a plain browser-side fetch()
to '/api/bootstrap' is CORS-blocked outright (unlike the no-cors reachability
probe the report also does, which only needs to know *that* something
answered, not read the response body). Proxying it through this same-origin
Python server sidesteps that entirely, since server-to-server HTTP has no
CORS concept.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, str(Path(__file__).parent))
import validate_credentials as vc  # noqa: E402

# Must mirror the `envsMapping` URLs baked into the report's <script> block
# (deploy_test_all.sh's save_deploy_report) — this is the server-side half of
# that same per-environment URL list.
ENV_BASE_URLS: dict[str, str] = {
    "local-std": "http://127.0.0.1",
    "local-hs": "http://127.0.0.1:8081",
    "remote-std": "http://127.0.0.1:3001",
    "remote-hs": "http://127.0.0.1:3002",
}


class ReportRequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 (stdlib override)
        parts = urlsplit(self.path)
        if parts.path.endswith("relics/credentials_status.json"):
            self._serve_live_credentials_status()
            return
        if parts.path.endswith("relics/bootstrap_status.json"):
            env_id = parse_qs(parts.query).get("env", [""])[0]
            self._serve_live_bootstrap_status(env_id)
            return
        super().do_GET()

    def _serve_live_credentials_status(self) -> None:
        try:
            body = json.dumps(vc.compute_all(), indent=2).encode("utf-8")
            status = 200
        except Exception as exc:  # pragma: no cover - defensive, keep the report alive
            body = json.dumps({"error": str(exc)}).encode("utf-8")
            status = 500
        self._send_json(status, body)

    def _serve_live_bootstrap_status(self, env_id: str) -> None:
        base_url = ENV_BASE_URLS.get(env_id)
        if not base_url:
            self._send_json(400, json.dumps({"error": f"unknown env '{env_id}'"}).encode("utf-8"))
            return
        try:
            request = urllib.request.Request(
                f"{base_url}/api/bootstrap",
                headers={"Accept": "application/json"},
            )
            with urllib.request.urlopen(request, timeout=3) as response:
                payload = response.read()
                body = json.dumps({"ok": True, "status": response.status, "data": json.loads(payload)}).encode("utf-8")
        except urllib.error.HTTPError as exc:
            body = json.dumps({"ok": False, "status": exc.code, "data": None}).encode("utf-8")
        except Exception as exc:  # pragma: no cover - defensive, keep the report alive
            body = json.dumps({"ok": False, "status": 0, "data": None, "error": str(exc)}).encode("utf-8")
        self._send_json(200, body)

    def _send_json(self, status: int, body: bytes) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        pass  # keep console output quiet, matches prior `http.server` usage here


def main() -> None:
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 41705
    server = ThreadingHTTPServer(("127.0.0.1", port), ReportRequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
