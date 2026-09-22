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
"""

from __future__ import annotations

import json
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import validate_credentials as vc  # noqa: E402


class ReportRequestHandler(SimpleHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 (stdlib override)
        path_only = self.path.split("?", 1)[0]
        if path_only.endswith("relics/credentials_status.json"):
            self._serve_live_credentials_status()
            return
        super().do_GET()

    def _serve_live_credentials_status(self) -> None:
        try:
            body = json.dumps(vc.compute_all(), indent=2).encode("utf-8")
            status = 200
        except Exception as exc:  # pragma: no cover - defensive, keep the report alive
            body = json.dumps({"error": str(exc)}).encode("utf-8")
            status = 500
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
