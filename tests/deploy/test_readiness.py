import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from scripts.lib import readiness

SECRET_BODY = "never-print-this-body"
ADMIN = {"settings": {"is_remote_analyst": False}, "services": [{"id": "svc"}], "note": SECRET_BODY}


class Stub:
    def __init__(self):
        self.routes = {
            "/dashboard": (200, "text/html; charset=utf-8", f"<html>{SECRET_BODY}</html>"),
            "/api/health": (200, "application/json", json.dumps({"status": "ok", "version": "3.0.0-beta3"})),
            "/api/bootstrap": (200, "application/json", json.dumps(ADMIN)),
        }
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                status, content_type, body = stub.routes.get(self.path, (404, "text/plain", "missing"))
                payload = body.encode()
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def json(self, path, payload, status=200):
        self.routes[path] = (status, "application/json", json.dumps(payload))


@pytest.fixture
def stub(monkeypatch):
    for name in (
        "ADMIN_GATEWAY_ENDPOINTS",
        "ADMIN_GATEWAY_CLIENT_ORIGIN",
        "ADMIN_GATEWAY_CLIENT_CERT",
        "ADMIN_GATEWAY_CLIENT_KEY",
        "ADMIN_GATEWAY_SERVER_CA",
    ):
        monkeypatch.delenv(name, raising=False)
    server = Stub()
    yield server
    server.server.shutdown()
    server.server.server_close()


def run(stub, capsys, *extra):
    code = readiness.main(
        [
            "--name",
            "Env",
            "--frontend",
            stub.origin,
            "--backend",
            stub.origin,
            "--attempts",
            "2",
            "--interval",
            "0",
            *extra,
        ]
    )
    out = capsys.readouterr()
    assert SECRET_BODY not in out.out + out.err
    return code, out.out


def test_ready_checks_frontend_backend_and_admin(stub, capsys):
    code, out = run(stub, capsys, "--expect-version", "3.0.0b3")
    assert code == 0
    assert "ready on attempt 1/2: frontend, backend, admin OK" in out


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda s: s.routes.update({"/dashboard": (500, "text/html", "x")}), "frontend /dashboard HTTP 500"),
        (lambda s: s.routes.update({"/dashboard": (200, "application/json", "{}")}), "not HTML"),
        (lambda s: s.json("/api/health", {"status": "initializing", "version": "3.0.0-beta3"}), "status=initializing"),
        (lambda s: s.json("/api/health", {}, status=503), "backend /api/health HTTP 503"),
        (
            lambda s: s.json("/api/bootstrap", {"settings": {"is_remote_analyst": True, "needs_login": True}}),
            "not an admin",
        ),
        (lambda s: s.json("/api/bootstrap", {"settings": {"is_remote_analyst": False}, "services": []}), "0 services"),
        (lambda s: s.routes.update({"/api/bootstrap": (200, "application/json", "not json")}), "not JSON"),
    ],
)
def test_each_unready_side_fails_after_bounded_attempts(stub, capsys, mutate, reason):
    mutate(stub)
    code, out = run(stub, capsys)
    assert code == 1
    assert out.count("not ready") == 2
    assert "FAILED readiness after 2 attempts" in out
    assert reason in out


def test_version_mismatch_fails(stub, capsys):
    code, out = run(stub, capsys, "--expect-version", "9.9.9")
    assert code == 1
    assert "backend version 3.0.0-beta3 != expected 9.9.9" in out


def test_no_admin_skips_bootstrap(stub, capsys):
    stub.json("/api/bootstrap", {"settings": {"is_remote_analyst": True, "needs_login": True}})
    code, out = run(stub, capsys, "--no-admin")
    assert code == 0
    assert "frontend, backend OK" in out


def test_recovers_within_attempts(stub, capsys):
    stub.json("/api/health", {"status": "initializing", "version": "3.0.0-beta3"})
    real = readiness.check_backend
    calls = []

    def flaky(*args):
        calls.append(1)
        if len(calls) == 2:
            stub.json("/api/health", {"status": "ok", "version": "3.0.0-beta3"})
        return real(*args)

    readiness.check_backend = flaky
    try:
        code, out = run(stub, capsys)
    finally:
        readiness.check_backend = real
    assert code == 0
    assert "attempt 1/2 not ready" in out


def test_unreachable_reports_exception_type_only(capsys):
    code = readiness.main(
        ["--name", "Down", "--frontend", "http://127.0.0.1:9", "--backend", "http://127.0.0.1:9", "--attempts", "1"]
    )
    out = capsys.readouterr().out
    assert code == 1
    assert "frontend request failed: URLError" in out


def test_invalid_tls_configuration_exits_2(stub, capsys, monkeypatch):
    monkeypatch.setenv("ADMIN_GATEWAY_ENDPOINTS", "not-json")
    code = readiness.main(["--name", "Cfg", "--frontend", stub.origin, "--backend", stub.origin])
    assert code == 2
    assert "readiness configuration invalid" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("live", "pep440"), [("3.0.0-beta3", "3.0.0b3"), ("v3.1.0-rc1", "3.1.0rc1"), ("3.0.0", "3.0.0")]
)
def test_normalize_version(live, pep440):
    assert readiness.normalize_version(live) == readiness.normalize_version(pep440)


def test_pyproject_version_is_readable():
    assert readiness.pyproject_version()


def test_oversized_frontend_rejected_not_truncated(stub, capsys, monkeypatch):
    monkeypatch.setattr(readiness, "_MAX_HTML_BYTES", 16)
    stub.routes["/dashboard"] = (200, "text/html", "<html>" + "x" * 64 + "</html>")
    code, out = run(stub, capsys)
    assert code == 1
    assert "exceeds size limit" in out


def test_untrusted_health_values_are_bounded_and_printable(stub, capsys):
    stub.json("/api/health", {"status": "bad\nline" + "z" * 200, "version": "3.0.0-beta3"})
    code, out = run(stub, capsys)
    assert code == 1
    assert "bad?line" in out and "z" * 41 not in out
    stub.json("/api/health", {"status": "ok", "version": {"nested": 1}})
    code, out = run(stub, capsys, "--expect-version", "3.0.0b3")
    assert "backend version <dict>" in out


@pytest.mark.parametrize("flag", ["--bootstrap-timeout", "--timeout"])
def test_non_positive_timeouts_rejected(flag):
    with pytest.raises(SystemExit) as exc:
        readiness.main(["--name", "x", "--frontend", "http://a", "--backend", "http://a", flag, "0"])
    assert exc.value.code == 2


def test_http_error_responses_are_closed(monkeypatch):
    import io
    from urllib.error import HTTPError

    closed = []

    class Tracked(io.BytesIO):
        def close(self):
            closed.append(True)
            super().close()

    def fail(url, timeout):
        raise HTTPError(url, 503, "busy", {}, Tracked(b"body"))

    monkeypatch.setattr(readiness, "admin_urlopen", fail)
    assert readiness._get("http://x/api/health", 1, 10) == (503, "", b"")
    assert closed
