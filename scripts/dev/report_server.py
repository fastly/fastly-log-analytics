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
localhost:41705) and each environment's backend (local or native mTLS gateway)
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

import argparse
import json
import os
import re
import secrets
import stat
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
try:
    from scripts.lib import environments as vc  # noqa: E402
except ImportError:
    vc = None  # type: ignore[assignment]

from scripts.lib.environments import admin_origin, admin_urlopen, validate_admin_tls_config  # noqa: E402

# Must mirror the `envsMapping` URLs baked into the report's <script> block
# (deploy_test_all.sh's save_deploy_report) — this is the server-side half of
# that same per-environment URL list.
ENV_BASE_URLS: dict[str, str] = {
    "local-std": "http://127.0.0.1",
    # Remotes resolve only through configured native admin mTLS endpoints.
    "remote-std": "",
    "remote-hs": "",
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
    ("remote_std_backend.log", "Remote Standard/backend"),
    ("remote_std_frontend.log", "Remote Standard/frontend"),
    ("remote_hs_backend.log", "Remote High-Scale/backend"),
    ("remote_hs_frontend.log", "Remote High-Scale/frontend"),
]
_SIGNAL_RE = re.compile(r"error|exception|traceback|failed|unauthorized|warning", re.IGNORECASE)
_TAIL_LINES = 400

_CRED_LOCK = threading.Lock()
_CRED_CACHE: tuple[float, bytes, int] | None = None
_CRED_TTL_SECONDS = 10.0

_BOOTSTRAP_LOCKS: dict[str, threading.Lock] = {env_id: threading.Lock() for env_id in ENV_BASE_URLS}
_BOOTSTRAP_CACHE: dict[str, tuple[float, bytes]] = {}
_BOOTSTRAP_TTL_SECONDS = 3.0

_MONITORED_ERRORS_LOCK = threading.Lock()
_MONITORED_ERRORS_CACHE: tuple[float, bytes, int] | None = None
_MONITORED_ERRORS_TTL_SECONDS = 2.0


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
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _serve_live_monitored_errors(self) -> None:
        global _MONITORED_ERRORS_CACHE
        now = time.time()
        if _MONITORED_ERRORS_CACHE is not None and (now - _MONITORED_ERRORS_CACHE[0]) < _MONITORED_ERRORS_TTL_SECONDS:
            self._send_json(_MONITORED_ERRORS_CACHE[2], _MONITORED_ERRORS_CACHE[1])
            return

        with _MONITORED_ERRORS_LOCK:
            if (
                _MONITORED_ERRORS_CACHE is not None
                and (time.time() - _MONITORED_ERRORS_CACHE[0]) < _MONITORED_ERRORS_TTL_SECONDS
            ):
                self._send_json(_MONITORED_ERRORS_CACHE[2], _MONITORED_ERRORS_CACHE[1])
                return
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
            _MONITORED_ERRORS_CACHE = (time.time(), body, status)
            self._send_json(status, body)

    def _serve_live_credentials_status(self) -> None:
        global _CRED_CACHE
        now = time.time()
        if _CRED_CACHE is not None and (now - _CRED_CACHE[0]) < _CRED_TTL_SECONDS:
            self._send_json(_CRED_CACHE[2], _CRED_CACHE[1])
            return

        with _CRED_LOCK:
            if _CRED_CACHE is not None and (time.time() - _CRED_CACHE[0]) < _CRED_TTL_SECONDS:
                self._send_json(_CRED_CACHE[2], _CRED_CACHE[1])
                return
            try:
                if vc is None:
                    raise RuntimeError("validate_credentials module not available")
                body = json.dumps(vc.compute_all(), indent=2).encode("utf-8")
                status = 200
            except Exception as exc:  # pragma: no cover - defensive, keep the report alive
                body = json.dumps({"error": str(exc)}).encode("utf-8")
                status = 500
            _CRED_CACHE = (time.time(), body, status)
            self._send_json(status, body)

    def _serve_live_bootstrap_status(self, env_id: str) -> None:
        if env_id not in ENV_BASE_URLS:
            self._send_json(400, json.dumps({"error": f"unknown env '{env_id}'"}).encode("utf-8"))
            return
        base_url = admin_origin(env_id)
        if not base_url:
            # No admin mTLS endpoint is configured for this env (deploy.env not
            # loaded by the server process). Do NOT fall back to a plain-HTTP
            # origin: an unauthenticated /api/bootstrap returns the anonymous
            # `needs_login` stub (null header_badge, empty services), which the
            # report would render as misleading empty/zero stats. Surface the
            # real cause instead of a silent fallback.
            self._send_json(
                200,
                json.dumps(
                    {
                        "ok": False,
                        "status": 0,
                        "data": None,
                        "error": f"admin gateway endpoint not configured for '{env_id}'",
                    }
                ).encode("utf-8"),
            )
            return

        cached = _BOOTSTRAP_CACHE.get(env_id)
        if cached is not None and (time.time() - cached[0]) < _BOOTSTRAP_TTL_SECONDS:
            self._send_json(200, cached[1])
            return

        lock = _BOOTSTRAP_LOCKS.setdefault(env_id, threading.Lock())
        with lock:
            cached = _BOOTSTRAP_CACHE.get(env_id)
            if cached is not None and (time.time() - cached[0]) < _BOOTSTRAP_TTL_SECONDS:
                self._send_json(200, cached[1])
                return
            try:
                request = urllib.request.Request(
                    f"{base_url}/api/bootstrap",
                    headers={"Accept": "application/json"},
                )
                with admin_urlopen(request, timeout=3) as response:
                    payload = response.read()
                    data = json.loads(payload)
                    header_badge = data.get("header_badge") if isinstance(data, dict) else None
                    services = data.get("services") if isinstance(data, dict) else None
                    if not header_badge or not services:
                        # Authenticated fetch succeeded but the backend returned
                        # the anonymous/degraded shape (no header_badge or no
                        # services). Surface it rather than rendering empty or
                        # invented-zero stats.
                        body = json.dumps(
                            {
                                "ok": False,
                                "status": response.status,
                                "data": data,
                                "error": "admin bootstrap returned no header_badge/services "
                                "(admin identity not accepted?)",
                            }
                        ).encode("utf-8")
                    else:
                        body = json.dumps({"ok": True, "status": response.status, "data": data}).encode("utf-8")
            except urllib.error.HTTPError as exc:
                reason = getattr(exc, "reason", None)
                error_text = f"HTTP {exc.code} {reason}".strip() if reason else f"HTTP {exc.code}"
                body = json.dumps({"ok": False, "status": exc.code, "data": None, "error": error_text}).encode("utf-8")
            except Exception as exc:  # pragma: no cover - defensive, keep the report alive
                body = json.dumps({"ok": False, "status": 0, "data": None, "error": str(exc)}).encode("utf-8")
            _BOOTSTRAP_CACHE[env_id] = (time.time(), body)
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


class PrivateReportRequestHandler(ReportRequestHandler):
    report_root: Path
    route_prefix: str

    def _allow_request(self) -> bool:
        host = self.headers.get("Host", "")
        port = self.server.server_address[1]
        origin = self.headers.get("Origin")
        path = urlsplit(self.path).path
        if (
            host not in {f"127.0.0.1:{port}", f"localhost:{port}"}
            or (origin is not None and origin != f"http://{host}")
            or not path.startswith(self.route_prefix)
        ):
            self.send_error(404)
            return False
        relative = unquote(path[len(self.route_prefix) :])
        if not (self.report_root / relative).resolve().is_relative_to(self.report_root):
            self.send_error(403)
            return False
        return True

    def do_GET(self) -> None:  # noqa: N802
        if not self._allow_request():
            return
        if urlsplit(self.path).path.endswith("relics/credentials_status.json"):
            self._send_json(403, b'{"error":"credential probes disabled for private reports"}')
            return
        super().do_GET()

    def do_HEAD(self) -> None:  # noqa: N802
        if self._allow_request():
            super().do_HEAD()

    def translate_path(self, path: str) -> str:
        relative = unquote(urlsplit(path).path[len(self.route_prefix) :])
        return str(self.report_root / (relative or "index.html"))

    def list_directory(self, path: str) -> None:
        self.send_error(403)

    def end_headers(self) -> None:
        self.send_header("Referrer-Policy", "no-referrer")
        super().end_headers()


# Admin mTLS keys the report server will adopt from the operator's gitignored
# deploy.env when the process was spawned without them. Mirrors the admin subset
# of deploy_test_all.sh's FLA_ADMIN_PERSIST_KEYS plus the single-identity legacy
# vars admin_tls._settings() also reads. Only these keys are ever adopted;
# nothing else in the file is touched, and secrets live inside the file (the
# path itself is a conventional, non-secret location).
_ADMIN_ENV_ALLOW_KEYS: frozenset[str] = frozenset(
    {
        "ADMIN_GATEWAY_ENDPOINTS",
        "ADMIN_GATEWAY_HOST",
        "ADMIN_GATEWAY_CERT_DIR",
        "ADMIN_GATEWAY_SECRET",
        "ADMIN_GATEWAY_REQUIRED",
        "ADMIN_GATEWAY_CALLERS_READY",
        "ADMIN_GATEWAY_UID",
        "ADMIN_GATEWAY_GID",
        "ADMIN_GATEWAY_CLIENT_ORIGIN",
        "ADMIN_GATEWAY_CLIENT_CERT",
        "ADMIN_GATEWAY_CLIENT_KEY",
        "ADMIN_GATEWAY_SERVER_CA",
        "ADMIN_GATEWAY_ENVIRONMENT",
        "LOCAL_STANDARD_ADMIN_MTLS",
        "GCE_ADMIN_MTLS",
        "ELEVATION_ADMIN_MTLS",
        "LOCAL_HOSTS",
        "ELEVATION_ADMIN_GATEWAY_HOST",
        "ELEVATION_ADMIN_GATEWAY_CERT_DIR",
        "GCE_ADMIN_BUNDLE_DIR",
        "NODE_EXTRA_CA_CERTS",
    }
)

# If any of these already exist in the environment, the process already carries
# an admin identity and the fallback is a no-op.
_ADMIN_ENV_PRIMARY_KEYS: tuple[str, ...] = (
    "ADMIN_GATEWAY_ENDPOINTS",
    "ADMIN_GATEWAY_CLIENT_ORIGIN",
)


def _default_admin_deploy_env() -> Path:
    override = os.environ.get("FLA_ADMIN_DEPLOY_ENV")
    if override:
        return Path(override)
    return Path.home() / ".config" / "fastly-log-analytics" / "admin" / "deploy.env"


def _parse_admin_env_file(path: Path) -> dict[str, str]:
    """Hand-rolled dotenv parse restricted to allow-listed admin keys.

    Returns {} on any read error or when the file fails the regular-file /
    owner / mode-600 checks. Avoids a python-dotenv dependency so this stays
    usable in a standalone script.
    """
    try:
        info = path.lstat()
    except OSError:
        return {}
    if not stat.S_ISREG(info.st_mode):
        return {}
    if info.st_uid != os.getuid():
        return {}
    if info.st_mode & 0o077 != 0:
        return {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    parsed: dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if key not in _ADMIN_ENV_ALLOW_KEYS:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        if "\0" in value:
            continue
        parsed[key] = value
    return parsed


def _load_admin_env_fallback(path: Path | None = None, environ: dict[str, str] | None = None) -> bool:
    """Populate admin mTLS env vars from the operator deploy.env if absent.

    A long-lived report server spawned without the admin gateway environment
    cannot resolve any admin origin, so every environment's live bootstrap
    stats panel renders "Stats unavailable". If the process already carries an
    admin identity (ADMIN_GATEWAY_ENDPOINTS or a single-origin
    ADMIN_GATEWAY_CLIENT_ORIGIN), this is a no-op. Otherwise it adopts ONLY the
    allow-listed admin keys from the gitignored deploy.env. Returns True when
    any key was adopted; silent no-op (returns False) when the file is absent or
    fails validation.
    """
    env = environ if environ is not None else os.environ
    if any(env.get(key) for key in _ADMIN_ENV_PRIMARY_KEYS):
        return False
    env_path = path if path is not None else _default_admin_deploy_env()
    parsed = _parse_admin_env_file(env_path)
    if not parsed:
        return False
    adopted = False
    for key, value in parsed.items():
        if env.get(key):
            continue
        env[key] = value
        adopted = True
    return adopted


def main() -> None:
    global _CURRENT_REPORT_LINK
    _load_admin_env_fallback()
    parser = argparse.ArgumentParser()
    parser.add_argument("port", nargs="?", type=int, default=41705)
    parser.add_argument("--private-root", type=Path)
    parser.add_argument("--access-file", type=Path)
    args = parser.parse_args()
    handler = ReportRequestHandler
    if args.private_root is not None:
        if args.access_file is None:
            parser.error("--private-root requires --access-file")
        root = args.private_root.resolve(strict=True)
        if not root.is_dir():
            parser.error("--private-root must be a directory")
        PrivateReportRequestHandler.report_root = root
        PrivateReportRequestHandler.route_prefix = f"/{secrets.token_urlsafe(24)}/"
        _CURRENT_REPORT_LINK = root
        handler = PrivateReportRequestHandler
    elif args.access_file is not None:
        parser.error("--access-file requires --private-root")
    validate_admin_tls_config()
    with ThreadingHTTPServer(("127.0.0.1", args.port), handler) as server:
        if args.private_root is not None:
            port = server.server_address[1]
            url = f"http://127.0.0.1:{port}{PrivateReportRequestHandler.route_prefix}index.html"
            fd = os.open(args.access_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w") as stream:
                json.dump({"url": url, "pid": os.getpid()}, stream)
        server.serve_forever()


if __name__ == "__main__":
    main()
