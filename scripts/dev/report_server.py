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

A request path ending in 'relics/monitored_errors_live.json' is answered by
re-reading (and re-filtering) the raw per-container log files under
reports/deploys/current/logs/*.log LIVE, on that request — not the
monitored_errors.log snapshot baked once by deploy_test_all.sh's background
tailer at report-generation time. This is what lets the "Monitored Logs &
Exception Dumps" panel stay accurate for errors that land in a container's
log stream *after* the report was generated (or between deploys entirely,
since 'current' always points at the most recent deploy's log directory).

A bare 'GET /' (or '/reports/', '/reports/index.html') is redirected to
'reports/deploys/current/index.html' so there is always one stable,
bookmarkable URL for "what's the state of things right now" — the report
under 'current' is regenerated in place by every deploy_test_all.sh run, and
its ports/credentials/bootstrap panels already poll live every 4s regardless
of whether a deploy is actively running.
"""

from __future__ import annotations

import json
import re
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

# Same source/tag pairing deploy_test_all.sh's background log monitor uses,
# and the same case-insensitive signal regex — kept in sync by hand since one
# lives in bash and the other here; if you add a stream to one, add it to
# the other.
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CURRENT_REPORT_LINK = _REPO_ROOT / "reports" / "deploys" / "current"
_LOG_SOURCES: list[tuple[str, str]] = [
    ("local_std_backend.log", "Local Standard/backend"),
    ("local_std_frontend.log", "Local Standard/frontend"),
    ("local_hs_backend.log", "Local High-Scale/backend"),
    ("local_hs_frontend.log", "Local High-Scale/frontend"),
    ("remote_std_backend.log", "Remote Standard/backend"),
    ("remote_std_frontend.log", "Remote Standard/frontend"),
    ("remote_hs_backend.log", "Remote High-Scale/backend"),
    ("remote_hs_frontend.log", "Remote High-Scale/frontend"),
]
_SIGNAL_RE = re.compile(r"error|exception|traceback|failed|unauthorized|warning", re.IGNORECASE)
_TAIL_LINES = 400


class ReportRequestHandler(SimpleHTTPRequestHandler):
    # SimpleHTTPRequestHandler defaults to HTTP/1.0, which closes the TCP connection
    # after every single response. The report page fires ~14 same-origin log-file
    # fetches every 1s (plus the live relics/* endpoints); without keep-alive, Chrome
    # has to open+tear down a fresh connection per request and, under that churn, its
    # HTTP/1.1 connection racing/coalescing logic cancels some of the duplicate/queued
    # connection attempts -- visible in DevTools as spurious "net::ERR_ABORTED" console
    # lines even though the underlying fetch() call itself always resolves successfully
    # (verified: every fetch returns 200 in a few ms; only the raced low-level TCP
    # attempt gets logged as failed). Enabling HTTP/1.1 keep-alive lets the browser
    # reuse one persistent connection per origin instead of racing new ones, which
    # eliminates that noise. Content-Length is already set on every response path here
    # (send_head()'s static-file path and _send_json()), so keep-alive is safe.
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 (stdlib override)
        parts = urlsplit(self.path)
        if parts.path in ("/", "/reports", "/reports/"):
            self._redirect_to_current_report()
            return
        if parts.path.endswith("relics/credentials_status.json"):
            self._serve_live_credentials_status()
            return
        if parts.path.endswith("relics/bootstrap_status.json"):
            env_id = parse_qs(parts.query).get("env", [""])[0]
            self._serve_live_bootstrap_status(env_id)
            return
        if parts.path.endswith("relics/monitored_errors_live.json"):
            self._serve_live_monitored_errors()
            return
        super().do_GET()

    def _redirect_to_current_report(self) -> None:
        self.send_response(302)
        self.send_header("Location", "/reports/deploys/current/index.html")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

    def _serve_live_monitored_errors(self) -> None:
        try:
            lines: list[str] = []
            for filename, tag in _LOG_SOURCES:
                log_path = _CURRENT_REPORT_LINK / "logs" / filename
                if not log_path.is_file():
                    continue
                try:
                    raw_lines = log_path.read_text(errors="replace").splitlines()
                except OSError:
                    continue
                for line in raw_lines[-_TAIL_LINES:]:
                    if _SIGNAL_RE.search(line):
                        lines.append(f"[{tag}] {line}")
            body = json.dumps({"ok": True, "count": len(lines), "text": "\n".join(lines)}).encode("utf-8")
            status = 200
        except Exception as exc:  # pragma: no cover - defensive, keep the report alive
            body = json.dumps({"ok": False, "error": str(exc)}).encode("utf-8")
            status = 500
        self._send_json(status, body)

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
