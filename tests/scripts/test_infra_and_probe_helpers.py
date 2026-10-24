from __future__ import annotations

"""scripts/lib/gce_ssh.sh builds the remote command from a quoted argv (S10).

A stub ``gcloud`` on PATH records the exact argv it receives and, like a real
remote login shell, re-parses ``--command`` with ``bash -c``. Hostile inputs
must arrive on the "remote" side byte-for-byte and never execute.
"""

import json
import os
import re
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LIB = REPO / "scripts" / "lib" / "gce_ssh.sh"
TRACKED_SCRIPTS = [
    REPO / "scripts" / "backup_service_configs.sh",
    REPO / "scripts" / "dev" / "snapshot_prod_to_dev.sh",
    REPO / "scripts" / "dev" / "sync-from-remote.sh",
]

HOSTILE = [
    "plain",
    "with space",
    "it's",
    'dq"uote',
    "semi;colon",
    "$(touch CANARY)",
    "`touch CANARY`",
    "a && touch CANARY",
    "pipe | touch CANARY",
    "new\nline",
    "back\\slash",
    "glob*?[x]",
    "$HOME ${HOME}",
    "-leading-dash",
    "",
    "~/tilde",
]

STUB = """#!{python}
import json, os, subprocess, sys
argv = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as fh:
    fh.write(json.dumps(argv) + "\\n")
if argv[:2] == ["compute", "ssh"]:
    cmd = [a for a in argv if a.startswith("--command=")][0][len("--command="):]
    sys.exit(subprocess.call(["bash", "-c", cmd]))
if argv[:2] == ["storage", "cp"]:
    import shutil
    shutil.copy(argv[2], os.environ["STUB_UPLOAD"])
    sys.exit(0)
if argv[:2] == ["storage", "ls"]:
    print("2048  " + argv[-1])
    sys.exit(0)
sys.exit(97)
"""

DUMP = """import os, sys
with open(os.environ["DUMP_OUT"], "wb") as fh:
    fh.write(b"\\0".join(a.encode() for a in sys.argv[1:]))
"""


@pytest.fixture
def gce_stub(tmp_path):
    bindir = tmp_path / "bin"
    bindir.mkdir()
    gcloud = bindir / "gcloud"
    gcloud.write_text(STUB.format(python=sys.executable))
    gcloud.chmod(0o755)
    sudo = bindir / "sudo"
    sudo.write_text('#!/bin/sh\nif [ "$1" = "-n" ]; then shift; fi\nexec "$@"\n')
    sudo.chmod(0o755)
    log = tmp_path / "calls.jsonl"
    env = {
        "PATH": f"{bindir}:{os.environ['PATH']}",
        "HOME": str(tmp_path),
        "STUB_LOG": str(log),
        "STUB_UPLOAD": str(tmp_path / "uploaded.tar.gz"),
        "DUMP_OUT": str(tmp_path / "dump.bin"),
    }

    def calls() -> list[list[str]]:
        if not log.exists():
            return []
        return [json.loads(line) for line in log.read_text().splitlines()]

    return env, calls, tmp_path


def run_bash(body: str, env: dict[str, str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["bash", "-c", f'source "{LIB}"\n{body}'],
        env=env,
        cwd=cwd,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


def test_exact_argv_for_simple_command(gce_stub):
    env, calls, tmp = gce_stub
    r = run_bash("gce_ssh_run my-vm us-central1-a --quiet -- echo /a b", env, tmp)
    assert r.returncode == 0, r.stderr
    assert calls() == [["compute", "ssh", "my-vm", "--zone=us-central1-a", "--quiet", "--command=echo /a b"]]


def test_exact_argv_quotes_hostile_arg(gce_stub):
    env, calls, tmp = gce_stub
    r = run_bash("gce_ssh_run vm z1 -- echo 'a b;$(x)'", env, tmp)
    assert r.returncode == 0, r.stderr
    (argv,) = calls()
    assert argv == ["compute", "ssh", "vm", "--zone=z1", r"--command=echo a\ b\;\$\(x\)"]


@pytest.mark.parametrize("value", HOSTILE)
def test_run_roundtrips_hostile_argument_without_executing(gce_stub, value):
    env, calls, tmp = gce_stub
    dump = tmp / "dump.py"
    dump.write_text(DUMP)
    r = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{LIB}"\ngce_ssh_run vm z1 -- "$1" "$2" "$3" "$4"',
            "_",
            sys.executable,
            str(dump),
            value,
            value,
        ],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert r.returncode == 0, r.stderr
    assert (tmp / "dump.bin").read_bytes() == b"\0".join([value.encode(), value.encode()])
    assert not (tmp / "CANARY").exists()
    assert len(calls()) == 1


@pytest.mark.parametrize("value", HOSTILE)
def test_script_binds_hostile_args_as_positionals(gce_stub, value):
    env, calls, tmp = gce_stub
    script = 'printf "%s\\0" "$#" "$@" > "$DUMP_OUT"'
    r = subprocess.run(
        [
            "bash",
            "-c",
            f'source "{LIB}"\ngce_ssh_script vm z1 --quiet -- "$1" "$2" "$3"',
            "_",
            script,
            value,
            "second " + value,
        ],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert r.returncode == 0, r.stderr
    got = (tmp / "dump.bin").read_bytes().split(b"\0")[:-1]
    assert got == [b"2", value.encode(), ("second " + value).encode()]
    assert not (tmp / "CANARY").exists()
    (argv,) = calls()
    assert argv[:5] == ["compute", "ssh", "vm", "--zone=z1", "--quiet"]
    assert argv[5].startswith("--command=bash -c ")


@pytest.mark.parametrize(
    "bad",
    ["", "-oProxyCommand=x", "vm name", "vm;touch CANARY", "vm$(x)", "vm\nx", ".hidden", "a/b"],
)
@pytest.mark.parametrize("slot", ["instance", "zone"])
def test_rejects_unsafe_instance_or_zone(gce_stub, bad, slot):
    env, calls, tmp = gce_stub
    inst, zone = (bad, "z1") if slot == "instance" else ("vm", bad)
    r = subprocess.run(
        ["bash", "-c", f'source "{LIB}"\ngce_ssh_run "$1" "$2" -- true', "_", inst, zone],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert r.returncode == 2
    assert calls() == []


def test_rejects_non_flag_before_separator_and_missing_command(gce_stub):
    env, calls, tmp = gce_stub
    assert run_bash("gce_ssh_run vm z1 oops -- true", env, tmp).returncode == 2
    assert run_bash("gce_ssh_run vm z1 --quiet", env, tmp).returncode == 2
    assert run_bash("gce_ssh_run vm z1 --quiet --", env, tmp).returncode == 2
    assert run_bash("gce_ssh_script vm z1 --", env, tmp).returncode == 2
    assert calls() == []


def test_exit_status_and_stdout_pass_through(gce_stub):
    env, _, tmp = gce_stub
    r = run_bash("gce_ssh_script vm z1 -- 'echo out; exit 7'", env, tmp)
    assert r.returncode == 7
    assert r.stdout == "out\n"


def test_no_ssh_command_string_building_or_eval_in_lib():
    text = LIB.read_text()
    assert not re.search(r"\beval\b", re.sub(r"^\s*#.*$", "", text, flags=re.M))


@pytest.mark.parametrize("script", TRACKED_SCRIPTS, ids=lambda p: p.name)
def test_tracked_scripts_use_the_shared_helper(script):
    text = script.read_text()
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    assert "source" in code and "lib/gce_ssh.sh" in code
    assert not re.search(r"^\s*(if\s+!?\s*)?(\w+=\"?\$\()?gcloud compute ssh\b", code, re.M)
    assert "--command=" not in code


def test_backup_script_end_to_end_with_hostile_paths(gce_stub):
    env, calls, tmp = gce_stub
    configs = tmp / "it's a dir; $(touch CANARY)" / "configs"
    configs.mkdir(parents=True)
    (configs / "svc.json").write_bytes(os.urandom(4096))
    marker = tmp / "mark dir" / "m'arker.json"
    env = {
        **env,
        "BACKUP_BUCKET": "gs://b",
        "BACKUP_INSTANCE": "vm-1",
        "BACKUP_ZONE": "us-central1-a",
        "BACKUP_CONFIGS_DIR": str(configs),
        "BACKUP_MARKER_VM_PATH": str(marker),
    }
    r = subprocess.run(
        ["bash", str(REPO / "scripts" / "backup_service_configs.sh")],
        env=env,
        cwd=tmp,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert r.returncode == 0, r.stderr + r.stdout
    assert not (tmp / "CANARY").exists()
    with tarfile.open(tmp / "uploaded.tar.gz") as tf:
        assert [m.name for m in tf.getmembers() if m.isfile()] == ["configs/svc.json"]
    payload = json.loads(marker.read_text())
    assert payload["remote"].startswith("gs://b/configs/")
    ssh_calls = [c for c in calls() if c[:2] == ["compute", "ssh"]]
    assert len(ssh_calls) == 2
    assert all(c[2] == "vm-1" and c[3] == "--zone=us-central1-a" for c in ssh_calls)


# ── Merged from test_health_probes.py ──────────────────────────────────────────────────────

"""Characterization of the health/audit scripts on the shared probe API (S1/S11).

A local stub backend stands in for an environment. Expected values here were
produced by the pre-refactor hand-rolled ``urllib`` implementations, so they
pin behavior across the move onto ``scripts.lib.readiness.probe``.
"""

import threading
import time
import urllib.error
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from scripts import check_environment_health as health
from scripts.dev import audit_environments as audit
from scripts.lib import readiness

HEALTHY: dict[str, tuple[int, Any]] = {
    "GET /api/health": (200, {"status": "ok", "version": "1.2.3"}),
    "GET /api/bootstrap": (200, {"active_service_id": "svc1"}),
    "POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz": (404, {"error": "nope"}),
    "GET /api/share/auth-config": (200, {"passcode_enabled": True}),
}


class Stub:
    def __init__(self) -> None:
        self.routes: dict[str, tuple[int, Any]] = dict(HEALTHY)
        self.seen: list[tuple[str, dict[str, str], bytes]] = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def _serve(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                stub.seen.append((f"{self.command} {self.path}", dict(self.headers), body))
                status, payload = stub.routes.get(f"{self.command} {self.path}", (404, {"error": "missing"}))
                raw = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            do_GET = do_POST = _serve  # noqa: N815

            def log_message(self, *args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.origin = f"http://127.0.0.1:{self.server.server_port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def target(self, name: str = "stub") -> health.EnvironmentTarget:
        return health.EnvironmentTarget(
            name=name,
            architecture="Stub",
            backend_url=self.origin,
            frontend_url=self.origin,
            fastly_service_id="svc1",
            remediation_cmd="fix-it",
        )


@pytest.fixture
def probe_stub(monkeypatch: pytest.MonkeyPatch) -> Iterator[Stub]:
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


class _SleepStub:
    """Module stand-in that overrides sleep only for the audit module (the real ``time.sleep`` is global)."""

    def __init__(self, sleep: Any) -> None:
        self.sleep = sleep

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


def dead_origin() -> str:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{sock.getsockname()[1]}"


def test_probe_reports_status_body_and_latency(probe_stub: Stub) -> None:
    result = readiness.probe(f"{probe_stub.origin}/api/health", timeout=5)
    assert (result.status, result.failed, result.http_error) == (200, False, False)
    assert result.json() == {"status": "ok", "version": "1.2.3"}
    assert result.content_type == "application/json"
    assert result.latency_ms >= 0


def test_probe_http_error_keeps_status_and_bounded_body(probe_stub: Stub) -> None:
    probe_stub.routes["GET /api/health"] = (503, {"error": "busy"})
    result = readiness.probe(f"{probe_stub.origin}/api/health", timeout=5)
    assert (result.status, result.http_error, result.reason) == (503, True, "Service Unavailable")
    assert result.error == "HTTP Error 503: Service Unavailable"
    assert result.json() == {"error": "busy"}


def test_probe_transport_failure_never_raises() -> None:
    result = readiness.probe(f"{dead_origin()}/x", timeout=2)
    assert result.status is None and result.failed and not result.http_error
    assert isinstance(result.exc, urllib.error.URLError)


def test_probe_delegates_to_admin_urlopen_with_request_details(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake(request: Any, timeout: float) -> Any:
        captured.update(url=request.full_url, method=request.get_method(), data=request.data, timeout=timeout)
        captured["headers"] = dict(request.header_items())
        raise OSError("stop")

    monkeypatch.setattr(readiness, "admin_urlopen", fake)
    result = readiness.probe("http://x/y", timeout=7, headers={"X-A": "1"}, data=b"{}", method="POST")
    assert result.error == "stop"
    assert captured == {
        "url": "http://x/y",
        "method": "POST",
        "data": b"{}",
        "timeout": 7,
        "headers": {"X-a": "1"},
    }


def test_health_ok(probe_stub: Stub) -> None:
    result = health.check_backend_health(probe_stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version, result.error) == (True, 200, "1.2.3", None)
    headers = next(h for line, h, _ in probe_stub.seen if line == "GET /api/health")
    assert headers["User-Agent"] == "FLA-HealthCheck/1.0"
    assert headers["Accept"] == "application/json"


def test_health_http_error(probe_stub: Stub) -> None:
    probe_stub.routes["GET /api/health"] = (503, {"x": 1})
    result = health.check_backend_health(probe_stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version) == (False, 503, None)
    assert result.error == "HTTPError 503: Service Unavailable"


def test_health_unexpected_payload(probe_stub: Stub) -> None:
    probe_stub.routes["GET /api/health"] = (200, {"status": "degraded", "version": "9"})
    result = health.check_backend_health(probe_stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version) == (False, 200, "9")
    assert result.error == 'Unexpected payload: {"status": "degraded", "version": "9"}'


def test_health_non_json_body(probe_stub: Stub) -> None:
    probe_stub.routes["GET /api/health"] = (200, b"<html>")
    result = health.check_backend_health(probe_stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version) == (False, None, None)
    assert result.error == "Expecting value: line 1 column 1 (char 0)"


def test_health_connection_refused() -> None:
    target = health.EnvironmentTarget("d", "A", dead_origin(), "", "svc1", "fix")
    result = health.check_backend_health(target, timeout_s=2)
    assert (result.backend_ok, result.status_code) == (False, None)
    assert result.error and "Connection refused" in result.error


def test_tenancy_ok_posts_expected_probe(probe_stub: Stub) -> None:
    result = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (result.service_match, result.rejection_ok, result.rejection_code, result.error) == (True, True, 404, None)
    assert result.resolved_service_id == "svc1"
    line, headers, body = next(s for s in probe_stub.seen if s[0].startswith("POST"))
    assert line == "POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"
    assert json.loads(body) == {"time_range": "24h"}
    assert headers["Content-Type"] == "application/json"


def test_tenancy_breach_when_fake_tenant_gets_200(probe_stub: Stub) -> None:
    probe_stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (200, {})
    result = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (result.rejection_ok, result.rejection_code) == (False, 200)
    assert result.error == "Tenancy breach: fake tenant returned HTTP 200 (expected 400/401/403/404)"


def test_tenancy_unsafe_5xx_is_not_a_rejection(probe_stub: Stub) -> None:
    probe_stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (500, {})
    result = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (result.rejection_ok, result.rejection_code) == (False, 500)


def test_tenancy_service_mismatch(probe_stub: Stub) -> None:
    probe_stub.routes["GET /api/bootstrap"] = (200, {"active_service_id": "other"})
    result = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (result.service_match, result.rejection_ok) == (False, True)
    assert result.error == "Service mismatch: got 'other', expected 'svc1'"


@pytest.mark.parametrize(
    ("route", "message"),
    [
        ((500, {"e": 1}), "Failed to fetch bootstrap: HTTP Error 500: Internal Server Error"),
        ((200, b"nope"), "Failed to fetch bootstrap: Expecting value: line 1 column 1 (char 0)"),
        ((200, [1]), "Failed to fetch bootstrap: 'list' object has no attribute 'get'"),
    ],
)
def test_tenancy_bootstrap_failures(probe_stub: Stub, route: tuple[int, Any], message: str) -> None:
    probe_stub.routes["GET /api/bootstrap"] = route
    result = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (result.resolved_service_id, result.service_match, result.rejection_code) == (None, False, None)
    assert result.error == message
    assert not any(line.startswith("POST") for line, _, _ in probe_stub.seen)


def test_rbac_ok_sends_remote_analyst_header(probe_stub: Stub) -> None:
    result = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert (result.auth_config_ok, result.unauth_analyst_blocked, result.unauth_analyst_code, result.error) == (
        True,
        True,
        200,
        None,
    )
    headers = next(h for line, h, _ in probe_stub.seen if line == "GET /api/share/auth-config")
    assert headers["X-Remote-Analyst"] == "1"


def test_rbac_missing_flag_and_http_error(probe_stub: Stub) -> None:
    probe_stub.routes["GET /api/share/auth-config"] = (200, {"other": 1})
    result = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert result.auth_config_ok is False and result.unauth_analyst_blocked is True
    assert result.error == "Auth config endpoint failed or returned unexpected payload"
    probe_stub.routes["GET /api/share/auth-config"] = (404, {})
    result = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert (result.auth_config_ok, result.unauth_analyst_blocked, result.unauth_analyst_code) == (False, False, None)
    assert result.error == "Failed to query /api/share/auth-config: HTTP Error 404: Not Found"


def _run_main(monkeypatch: pytest.MonkeyPatch, target: health.EnvironmentTarget, *extra: str) -> int:
    monkeypatch.setattr(health, "ENVIRONMENTS", {target.name: target})
    monkeypatch.setattr("sys.argv", ["check_environment_health.py", "--env", target.name, *extra])
    return health.main()


def test_main_text_report_golden(
    probe_stub: Stub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_main(monkeypatch, probe_stub.target("stub-env")) == 0
    out = re.sub(r"\d+\.\dms", "<lat>", capsys.readouterr().out)
    assert out.splitlines()[6:] == [
        "stub-env               | PASS (200) | svc1 (OK)              | PASS (404)  | PASS (200) | <lat>",
        "=" * 96,
        "ALL TARGET ENVIRONMENTS HEALTHY, TENANCY VERIFIED & RBAC ENFORCED!",
        "=" * 96,
        "",
    ]


def test_main_text_report_failure_golden(
    probe_stub: Stub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    probe_stub.routes["GET /api/health"] = (503, {})
    assert _run_main(monkeypatch, probe_stub.target("stub-env")) == 1
    out = re.sub(r"\d+\.\dms", "<lat>", capsys.readouterr().out)
    lines = out.splitlines()[6:]
    assert lines[0].startswith("stub-env               | FAIL     | svc1 (OK)")
    assert lines[1:3] == ["   [!] Health Error: HTTPError 503: Service Unavailable", "   [!] Fix command: fix-it"]
    assert "FAILURE: 1/1 environments failed checks." in lines


def test_main_json_report_golden(
    probe_stub: Stub, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_main(monkeypatch, probe_stub.target("stub-env"), "--json") == 0
    (row,) = json.loads(capsys.readouterr().out)
    row.pop("latency_ms")
    assert row == {
        "env": "stub-env",
        "architecture": "Stub",
        "service_id": "svc1",
        "backend_url": probe_stub.origin,
        "health_ok": True,
        "health_status_code": 200,
        "version": "1.2.3",
        "tenancy_service_match": True,
        "tenancy_rejection_ok": True,
        "tenancy_rejection_code": 404,
        "rbac_auth_config_ok": True,
        "rbac_unauth_blocked": True,
        "rbac_unauth_code": 200,
        "error": None,
        "remediation": None,
    }


def test_audit_get_json(probe_stub: Stub) -> None:
    status, data = audit.http_get_json(f"{probe_stub.origin}/api/health", headers={"X-T": "1"}, timeout=5)
    assert (status, data) == (200, {"status": "ok", "version": "1.2.3"})
    _, headers, _ = probe_stub.seen[-1]
    assert headers["User-Agent"] == "FLA-AuditTool/1.0" and headers["X-T"] == "1"


def test_audit_get_json_error_shapes(probe_stub: Stub) -> None:
    probe_stub.routes["GET /e1"] = (500, {"detail": "boom"})
    probe_stub.routes["GET /e2"] = (502, b"<html>")
    probe_stub.routes["GET /e3"] = (200, b"<html>")
    assert audit.http_get_json(f"{probe_stub.origin}/e1") == (500, {"detail": "boom"})
    assert audit.http_get_json(f"{probe_stub.origin}/e2") == (502, {"error": "HTTP Error 502: Bad Gateway"})
    assert audit.http_get_json(f"{probe_stub.origin}/e3") == (0, {"error": "Expecting value: line 1 column 1 (char 0)"})
    status, data = audit.http_get_json(f"{dead_origin()}/x", timeout=2)
    assert status == 0 and "Connection refused" in data["error"]


def test_audit_post_json_sends_payload_and_parses_error_body(probe_stub: Stub) -> None:
    probe_stub.routes["POST /p"] = (422, {"detail": "bad"})
    assert audit.http_post_json(f"{probe_stub.origin}/p", {"a": 1}, timeout=5) == (422, {"detail": "bad"})
    line, headers, body = probe_stub.seen[-1]
    assert (line, json.loads(body)) == ("POST /p", {"a": 1})
    assert headers["Content-Type"] == "application/json"
    probe_stub.routes["POST /q"] = (200, {"ok": True})
    assert audit.http_post_json(f"{probe_stub.origin}/q", timeout=5) == (200, {"ok": True})
    assert json.loads(probe_stub.seen[-1][2]) == {}


def test_audit_post_json_retries_transport_failure_once(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    real = audit.probe

    def counting(url: str, **kwargs: Any) -> readiness.ProbeResult:
        calls.append(url)
        return real(url, **kwargs)

    sleeps: list[float] = []
    monkeypatch.setattr(audit, "probe", counting)
    monkeypatch.setattr(audit, "time", _SleepStub(sleeps.append))
    status, data = audit.http_post_json(f"{dead_origin()}/x", {}, timeout=2)
    assert status == 0 and "Connection refused" in data["error"]
    assert len(calls) == 2 and sleeps == [0.5]


def test_audit_post_json_does_not_retry_http_errors(probe_stub: Stub, monkeypatch: pytest.MonkeyPatch) -> None:
    probe_stub.routes["POST /p"] = (500, {"e": 1})
    monkeypatch.setattr(audit, "time", _SleepStub(lambda _s: pytest.fail("must not retry HTTP errors")))
    assert audit.http_post_json(f"{probe_stub.origin}/p", {}, timeout=5) == (500, {"e": 1})
    assert [line for line, _, _ in probe_stub.seen].count("POST /p") == 1


def test_load_persisted_deploy_env_populates_admin_origins_and_respects_existing_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    from scripts.lib import environments

    tracked_keys = (
        "FLA_ADMIN_DEPLOY_ENV",
        "LOCAL_STANDARD_ADMIN_MTLS",
        "GCE_ADMIN_MTLS",
        "ELEVATION_ADMIN_MTLS",
        "ADMIN_GATEWAY_ENDPOINTS",
    )
    for key in tracked_keys:
        monkeypatch.setenv(key, "")
        monkeypatch.delenv(key, raising=False)

    endpoint_files: dict[str, str] = {}
    for name in ("cert", "key", "ca"):
        p = tmp_path / name
        p.write_text("pem")
        p.chmod(0o600)
        endpoint_files[name] = str(p)

    endpoints = {
        "local-standard": {"origin": "https://127.0.0.1:9443", **endpoint_files},
        "remote-standard": {"origin": "https://203.0.113.10:9443", **endpoint_files},
        "remote-high-scale": {"origin": "https://10.0.0.10:8443", **endpoint_files},
    }
    deploy_env = tmp_path / "deploy.env"
    deploy_env.write_text(
        f"LOCAL_STANDARD_ADMIN_MTLS=1\n"
        f"GCE_ADMIN_MTLS=1\n"
        f"ELEVATION_ADMIN_MTLS=1\n"
        f"ADMIN_GATEWAY_ENDPOINTS='{json.dumps(endpoints)}'\n"
    )
    deploy_env.chmod(0o600)
    monkeypatch.setenv("FLA_ADMIN_DEPLOY_ENV", str(deploy_env))
    monkeypatch.setenv("GCE_ADMIN_MTLS", "0")

    try:
        loaded = environments.load_persisted_deploy_env()
        assert loaded["LOCAL_STANDARD_ADMIN_MTLS"] == "1"
        assert loaded["ELEVATION_ADMIN_MTLS"] == "1"
        # Existing process env wins over persisted deploy.env
        assert environments.os.environ["GCE_ADMIN_MTLS"] == "0"

        audit.refresh_environment_origins()
        assert audit.ENVIRONMENTS["local-standard"].backend_url == "https://127.0.0.1:9443"
        assert audit.ENVIRONMENTS["remote-standard"].backend_url == "https://203.0.113.10:9443"
        assert audit.ENVIRONMENTS["remote-high-scale"].backend_url == "https://10.0.0.10:8443"
    finally:
        for key in tracked_keys:
            monkeypatch.delenv(key, raising=False)
        audit.refresh_environment_origins()


def test_audit_target_private_mtls_timeout_adds_vpn_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(audit, "is_admin_url", lambda url: url.startswith("https://10.0.0.10:8443"))
    monkeypatch.setattr(
        audit,
        "http_get_json",
        lambda url, headers=None, timeout=5.0: (0, {"error": "<urlopen error timed out>"}),
    )
    env = audit.EnvironmentConfig(
        name="remote-high-scale",
        architecture="High-Scale (Remote/K8s)",
        backend_url="https://10.0.0.10:8443",
        frontend_url="https://10.0.0.10:8443/dashboard",
        service_id="svc-hs",
        is_high_scale=True,
    )
    res = audit.audit_target(env, timeout=1.0)
    assert not res.reachable
    assert any("VPN/tunnel" in err and "10.0.0.10:8443" in err for err in res.errors)


# ── Merged from test_lib_helpers.py ──────────────────────────────────────────────────────

import asyncio
import ssl
from urllib.request import Request

import httpx
import pytest

from scripts.lib import admin_tls
from scripts.lib import admin_tls as admin_http


def test_total_deadline_cancels_a_stalled_response_body(monkeypatch: pytest.MonkeyPatch) -> None:
    closed = []

    class Stalled(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"{"
            await asyncio.Event().wait()

        async def aclose(self) -> None:
            closed.append(True)

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=Stalled()))
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: ssl.create_default_context())
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=transport))
    with pytest.raises(TimeoutError):
        asyncio.run(admin_http.async_admin_json(Request("https://example.com/api/test", data=b"{}"), timeout=0.05))
    assert closed


def test_deadline_transport_preserves_request_and_refuses_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    seen = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"location": "https://other.example.com/private"})

    real_client = httpx.AsyncClient
    transport = httpx.MockTransport(respond)
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: ssl.create_default_context())
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: real_client(**kwargs, transport=transport))
    request = Request(
        "https://example.com/api/test", data=b'{"filters":{}}', headers={"X-Page-Load-ID": "test-correlation"}
    )
    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(admin_http.async_admin_json(request, timeout=1))
    assert len(seen) == 1
    assert seen[0].method == "POST"
    assert seen[0].content == b'{"filters":{}}'
    assert seen[0].headers["x-page-load-id"] == "test-correlation"


@pytest.mark.parametrize(
    "url,headers",
    [
        ("https://other.example.com/api/test", {}),
        ("https://example.com/api/test", {"X-Admin-Gateway-Token": "do-not-send"}),
    ],
)
def test_deadline_transport_rejects_untrusted_identity_use_before_tls(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
    headers: dict,
) -> None:
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})

    def forbidden(config: dict) -> ssl.SSLContext:
        pytest.fail("TLS identity must not be loaded")

    monkeypatch.setattr(admin_tls, "_context", forbidden)
    with pytest.raises(ValueError):
        asyncio.run(admin_http.async_admin_json(Request(url, headers=headers), timeout=1))


# ── Merged from test_cli.py ──

import argparse

import pytest

from scripts.lib.environments import add_common_args


def _parser(**kwargs: object) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="t")
    add_common_args(parser, **kwargs)  # type: ignore[arg-type]
    return parser


def test_adds_nothing_unless_asked() -> None:
    with pytest.raises(SystemExit):
        _parser().parse_args(["--timeout", "1"])
    with pytest.raises(SystemExit):
        _parser().parse_args(["--json"])


def test_timeout_default_type_and_help() -> None:
    parser = _parser(timeout_default=5.0, timeout_label="HTTP request")
    assert parser.parse_args([]).timeout == 5.0
    assert parser.parse_args(["--timeout", "2.5"]).timeout == 2.5
    assert "HTTP request timeout in seconds (default: 5.0)" in parser.format_help()


def test_json_flag() -> None:
    parser = _parser(json_help="Output raw JSON results")
    assert parser.parse_args([]).json is False
    assert parser.parse_args(["--json"]).json is True
    assert "Output raw JSON results" in parser.format_help()


@pytest.mark.parametrize(
    ("module", "default"),
    [("scripts.check_environment_health", 5.0), ("scripts.dev.audit_environments", 10.0)],
)
def test_adopting_scripts_keep_their_timeout_defaults(
    module: str, default: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import importlib

    mod = importlib.import_module(module)
    monkeypatch.setattr("sys.argv", ["prog", "--help"])
    with pytest.raises(SystemExit):
        mod.main()
    out = capsys.readouterr().out
    assert f"timeout in seconds (default: {default})" in out
    assert "--json" in out


# ── Merged from test_prom.py ──

from http.server import HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

from scripts.lib import environments as prom


def test_histogram_quantile_matches_the_legacy_query_text() -> None:
    assert prom.histogram_quantile(0.95, "http_server_duration_milliseconds_bucket") == (
        "histogram_quantile(0.95,sum by (le)(rate(http_server_duration_milliseconds_bucket[5m])))"
    )


@pytest.fixture
def prom_server() -> Iterator[tuple[str, list[str]]]:
    seen: list[str] = []
    body: dict[str, object] = {"data": {"result": [{"value": [1700000000, "12.5"]}]}}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            seen.append(parse_qs(urlparse(self.path).query)["query"][0])
            if "boom" in seen[-1]:
                self.send_response(500)
                self.end_headers()
                return
            payload = {"data": {"result": []}} if "empty" in seen[-1] else body
            raw = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def log_message(self, *args: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/", seen
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_instant_returns_first_sample_and_sends_the_query(prom_server: tuple[str, list[str]]) -> None:
    base, seen = prom_server
    assert prom.instant(base, "up{job='a b'}") == 12.5
    assert seen == ["up{job='a b'}"]


def test_instant_is_none_for_empty_result_http_error_and_unreachable(prom_server: tuple[str, list[str]]) -> None:
    base, _ = prom_server
    assert prom.instant(base, "empty") is None
    assert prom.instant(base, "boom") is None
    assert prom.instant("http://127.0.0.1:1", "up", timeout=1) is None


# ── Merged from test_streaming_e2e_fixture.py ──


import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripts import run_contract_backend
from scripts.run_contract_backend import install_streaming_fixture


def test_streaming_fixture_requires_an_isolated_contract_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTRACT_STREAMING_FIXTURES", "1")
    monkeypatch.delenv("CONTRACT_CONFIGS_DIR", raising=False)
    monkeypatch.delenv("CONTRACT_DATA_DIR", raising=False)
    monkeypatch.setattr(sys, "argv", ["run_contract_backend.py"])
    with pytest.raises(ValueError, match="isolated contract sandbox"):
        run_contract_backend.main()


def test_streaming_fixture_is_cookie_scoped_and_records_ssr_requests() -> None:
    app = FastAPI()

    @app.post("/api/cmcd/aggregates")
    def aggregates() -> dict:
        return {"available": False}

    install_streaming_fixture(app)
    with TestClient(app) as browser, TestClient(app) as other_browser:
        payload = {"available": True, "overview": {"active_sessions": 1234}}
        assert browser.post("/api/__e2e__/streaming-fixture", json=payload).status_code == 200
        body = {"range_token": "24h", "bucket_seconds": 300, "top_n": 30}
        with TestClient(app) as ssr:
            ssr.cookies.update(browser.cookies)
            assert ssr.post("/api/cmcd/aggregates", json=body).json() == payload
        assert browser.get("/api/__e2e__/streaming-fixture").json() == {"bodies": [body]}
        assert other_browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}
        assert browser.delete("/api/__e2e__/streaming-fixture").status_code == 200
        assert browser.get("/api/__e2e__/streaming-fixture").status_code == 404
        assert browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}


def test_unavailable_fixture_does_not_reuse_another_browsers_populated_response() -> None:
    app = FastAPI()
    install_streaming_fixture(app)
    with TestClient(app) as populated, TestClient(app) as unavailable:
        populated.post("/api/__e2e__/streaming-fixture", json={"available": True})
        unavailable.post("/api/__e2e__/streaming-fixture", json={"available": False, "has_data": False})
        assert populated.post("/api/cmcd/aggregates", json={}).json() == {"available": True}
        assert unavailable.post("/api/cmcd/aggregates", json={}).json() == {"available": False, "has_data": False}


# ── Merged from test_profile_insights.py ──

from pathlib import Path

from scripts.dev.profile_insights import format_markdown_report, main, run_profile


def test_run_profile_collects_phases_coalesced_and_insights():
    res = run_profile(total_rows=1000, runs=2, num_files=3, window_hours=1.0, baseline_hours=24.0)

    assert res["runs"] == 2
    assert res["dataset"]["total_rows"] == 1000
    assert res["cold_total"]["wall_p50_ms"] > 0
    assert res["warm_total"]["wall_p50_ms"] >= 0
    assert len(res["top_10_by_wall_p50"]) == 10
    assert len(res["top_10_by_cpu_p50"]) == 10

    op_names = {op["operation"] for op in res["all_operations"]}
    assert "phase:scratch_table_build" in op_names
    assert "coalesced:city" in op_names
    assert "coalesced:url" in op_names
    assert "coalesced:ip" in op_names
    assert "coalesced:waf" in op_names
    assert any(name.startswith("insight:") for name in op_names)

    md = format_markdown_report(res)
    assert "# Insights Performance Profile" in md
    assert "Top 10 Costs by Wall-Clock p50" in md


def test_profile_insights_cli_writes_markdown_and_json(tmp_path: Path):
    md_out = tmp_path / "profile.md"
    json_out = tmp_path / "profile.json"
    rc = main(
        [
            "--rows",
            "500",
            "--runs",
            "1",
            "--files",
            "2",
            "--output",
            str(md_out),
            "--json-output",
            str(json_out),
        ]
    )
    assert rc == 0
    assert md_out.exists()
    assert json_out.exists()
    payload = json.loads(json_out.read_text())
    assert payload["dataset"]["total_rows"] == 500


# ── Merged from test_check_osv.py ──

import importlib.util

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "check_osv.py"


@pytest.fixture(scope="module")
def osv():
    spec = importlib.util.spec_from_file_location("check_osv", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_osv"] = module
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "vector,expected",
    [
        # Log4Shell (CVE-2021-44228) — the canonical 10.0 CRITICAL.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H", 10.0),
        # A textbook HIGH (7.5): network, no priv, availability impact only.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H", 7.5),
        # A MEDIUM (6.1): reflected XSS shape, scope changed.
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:C/C:L/I:L/A:N", 6.1),
        # A LOW (3.1).
        ("CVSS:3.1/AV:N/AC:H/PR:L/UI:N/S:U/C:L/I:N/A:N", 3.1),
    ],
)
def test_cvss3_base_score_known_vectors(osv, vector, expected):
    assert osv.cvss3_base_score(vector) == pytest.approx(expected, abs=0.05)


def test_cvss3_base_score_rejects_v4_vector(osv):
    # CVSS_V4 uses a different formula we do not implement → None.
    assert osv.cvss3_base_score("CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N") is None


def test_severity_from_cvss_array_when_database_specific_absent(osv):
    """The fail-open regression: CRITICAL severity carried ONLY in the CVSS
    array (no database_specific) must still resolve to CRITICAL."""
    vuln = {
        "id": "CVE-2021-44228",
        "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
    }
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_takes_max_across_sources(osv):
    """database_specific says LOW but the CVSS vector says CRITICAL → CRITICAL."""
    vuln = {
        "id": "X",
        "database_specific": {"severity": "LOW"},
        "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
    }
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_database_specific_still_honored(osv):
    vuln = {"id": "X", "database_specific": {"severity": "critical"}}
    assert osv.severity_for_vuln(vuln) == "CRITICAL"


def test_severity_unknown_when_no_signal(osv):
    assert osv.severity_for_vuln({"id": "X"}) == "UNKNOWN"


def test_unparsed_v4_only_vector_is_unknown_not_crash(osv):
    vuln = {
        "id": "CVE-2099-0001",
        "severity": [{"type": "CVSS_V4", "score": "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N"}],
    }
    # No qualitative source and an unparseable vector → UNKNOWN (surfaced via stderr).
    assert osv.severity_for_vuln(vuln) == "UNKNOWN"


# ── Merged from test_port_forward.py ──────────────────────────────────────────────────────

"""scripts/lib/port_forward.py against a stub ``kubectl`` (no cluster involved).

The stub parses the real ``port-forward ... LOCAL:REMOTE`` argv, binds LOCAL
like kubectl would, and records its argv/pid. STUB_MODE selects misbehaviour.
"""

import signal
import socket
from pathlib import Path

import pytest

from scripts.lib import port_forward as pf

REPO = Path(__file__).resolve().parents[2]

_PF_STUB = """#!{python}
import json, os, signal, socket, subprocess, sys, time
argv = sys.argv[1:]
state = os.environ["STUB_STATE"]
mode = os.environ.get("STUB_MODE", "ok")
local = int(argv[-1].split(":")[0])
n = len([f for f in os.listdir(state) if f.startswith("call-")])
child = subprocess.Popen(["/bin/sleep", "120"])
with open(os.path.join(state, f"call-{{n}}.json"), "w") as fh:
    json.dump({{"argv": argv, "pid": os.getpid(), "child": child.pid}}, fh)
def term(signum, frame):
    open(os.path.join(state, f"term-{{n}}"), "w").close()
    sys.exit(0)
signal.signal(signal.SIGTERM, term)
if mode == "exit-early":
    sys.stderr.write("error: unable to forward port because pod is not running\\n")
    sys.exit(1)
if mode == "never-ready":
    time.sleep(120)
srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(("127.0.0.1", local))
srv.listen(5)
print(f"Forwarding from 127.0.0.1:{{local}} -> 80", flush=True)
if mode == "die-once" and n == 0:
    time.sleep(0.6)
    sys.exit(1)
while True:
    time.sleep(0.1)
"""


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def kubectl(tmp_path, monkeypatch):
    state = tmp_path / "state"
    state.mkdir()
    stub = tmp_path / "kubectl"
    stub.write_text(_PF_STUB.format(python=sys.executable))
    stub.chmod(0o755)
    monkeypatch.setenv("STUB_STATE", str(state))

    class K:
        path = str(stub)

        @staticmethod
        def calls() -> list[dict]:
            return [json.loads(p.read_text()) for p in sorted(state.glob("call-*.json"))]

        @staticmethod
        def terminated() -> list[str]:
            return sorted(p.name for p in state.glob("term-*"))

    yield K
    for call in K.calls():
        for pid in (call["pid"], call["child"]):
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def wait_dead(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not alive(pid):
            return True
        time.sleep(0.05)
    return False


def test_argv_is_a_list_with_hostile_values_untouched():
    fwd = pf.Forward("svc/back end;$(x)", 8002, 8000, namespace="ns'; rm", context="c x")
    assert fwd.argv("kubectl") == [
        "kubectl",
        "--context",
        "c x",
        "--namespace",
        "ns'; rm",
        "port-forward",
        "--address",
        "127.0.0.1",
        "svc/back end;$(x)",
        "8002:8000",
    ]


@pytest.mark.parametrize("bad", ["svc/x", "svc/x:80", "svc/x:a:b", ":1:2", "svc/x:0:80", "svc/x:80:70000"])
def test_parse_forward_rejects_malformed_specs(bad):
    with pytest.raises(ValueError):
        pf.parse_forward(bad)


def test_parse_forward_roundtrip():
    assert pf.parse_forward("svc/backend-svc:8002:8000", namespace="ns") == pf.Forward(
        "svc/backend-svc", 8002, 8000, namespace="ns"
    )


def test_port_in_use_is_refused_before_spawning(kubectl):
    with socket.socket() as blocker:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        port = blocker.getsockname()[1]
        with pytest.raises(pf.PortInUse):
            pf.ForwardSet([pf.Forward("svc/x", port, 80)], kubectl=kubectl.path).start()
    assert kubectl.calls() == []


def test_early_exit_surfaces_kubectl_stderr_and_cleans_up(kubectl, monkeypatch):
    monkeypatch.setenv("STUB_MODE", "exit-early")
    with pytest.raises(pf.ForwardExited, match="pod is not running"):
        pf.ForwardSet([pf.Forward("svc/x", free_port(), 80)], kubectl=kubectl.path, ready_timeout=15).start()
    (call,) = kubectl.calls()
    assert wait_dead(call["child"])


def test_readiness_timeout_kills_the_forward(kubectl, monkeypatch):
    monkeypatch.setenv("STUB_MODE", "never-ready")
    with pytest.raises(pf.ReadinessTimeout):
        pf.ForwardSet([pf.Forward("svc/x", free_port(), 80)], kubectl=kubectl.path, ready_timeout=2.0).start()
    (call,) = kubectl.calls()
    assert wait_dead(call["pid"])
    assert wait_dead(call["child"])


def test_any_taken_port_refuses_the_whole_set_before_spawning(kubectl):
    first, taken = free_port(), free_port()
    with socket.socket() as blocker:
        blocker.bind(("127.0.0.1", taken))
        blocker.listen(1)
        with pytest.raises(pf.PortInUse):
            pf.ForwardSet(
                [pf.Forward("svc/a", first, 80), pf.Forward("svc/b", taken, 80)], kubectl=kubectl.path
            ).start()
    assert kubectl.calls() == []


def test_poll_restarts_a_dropped_forward_within_budget(kubectl, monkeypatch):
    monkeypatch.setenv("STUB_MODE", "die-once")
    port = free_port()
    fwd_set = pf.ForwardSet([pf.Forward("svc/x", port, 80)], kubectl=kubectl.path, ready_timeout=15, max_restarts=1)
    fwd_set.start()
    try:
        deadline = time.monotonic() + 10
        while len(kubectl.calls()) < 2 and time.monotonic() < deadline:
            fwd_set.poll()
            time.sleep(0.1)
        assert len(kubectl.calls()) == 2
        with socket.create_connection(("127.0.0.1", port), timeout=2):
            pass
    finally:
        fwd_set.stop()


def test_poll_raises_when_restart_budget_is_spent(kubectl, monkeypatch):
    monkeypatch.setenv("STUB_MODE", "die-once")
    fwd_set = pf.ForwardSet(
        [pf.Forward("svc/x", free_port(), 80)], kubectl=kubectl.path, ready_timeout=15, max_restarts=0
    )
    fwd_set.start()
    try:
        deadline = time.monotonic() + 10
        with pytest.raises(pf.ForwardExited, match="budget exhausted"):
            while time.monotonic() < deadline:
                fwd_set.poll()
                time.sleep(0.1)
    finally:
        fwd_set.stop()


def _cli(kubectl, *extra):
    return subprocess.Popen(
        [sys.executable, "-m", "scripts.lib.port_forward", "--kubectl", kubectl.path, *extra],
        cwd=REPO,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT])
def test_cli_cleans_up_on_signal(kubectl, sig):
    port = free_port()
    proc = _cli(kubectl, f"svc/x:{port}:80")
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().startswith("port-forward ready:")
        (call,) = kubectl.calls()
        proc.send_signal(sig)
        assert proc.wait(timeout=20) == 128 + sig
    finally:
        if proc.poll() is None:
            proc.kill()
    assert wait_dead(call["pid"])
    assert wait_dead(call["child"])
    assert not pf.port_in_use(port)


def test_cli_runs_command_then_tears_down_and_returns_its_status(kubectl):
    port = free_port()
    code = f"import socket,sys; socket.create_connection(('127.0.0.1', {port}), timeout=2).close(); sys.exit(7)"
    proc = _cli(kubectl, f"svc/x:{port}:80", "--", sys.executable, "-c", code)
    out, _ = proc.communicate(timeout=30)
    assert proc.returncode == 7
    assert "port-forward ready:" in out
    (call,) = kubectl.calls()
    assert wait_dead(call["pid"])
    assert wait_dead(call["child"])


def test_cli_reports_port_in_use(kubectl):
    with socket.socket() as blocker:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        port = blocker.getsockname()[1]
        proc = _cli(kubectl, f"svc/x:{port}:80")
        _, err = proc.communicate(timeout=20)
    assert proc.returncode == 1
    assert f"port {port} is already in use" in err
    assert kubectl.calls() == []


def test_remote_high_scale_remediation_uses_the_supervisor(monkeypatch):
    from scripts.lib.environments import remediation_command

    monkeypatch.delenv("REMOTE_HIGH_SCALE_FORWARD_CMD", raising=False)
    monkeypatch.setenv("ELEVATION_NAMESPACE", "team-ns")
    cmd = remediation_command("remote-high-scale")
    assert cmd.startswith("python3 -m scripts.lib.port_forward --namespace team-ns ")
    assert "&" not in cmd
    for spec in cmd.split()[-2:]:
        pf.parse_forward(spec)


# ── Merged from test_run_contract_backend.py ──────────────────────────────────────────────────────

"""Fresh-process checks: no conftest path patches can hide launcher escapes."""

from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
LAUNCHER = REPO / "scripts" / "run_contract_backend.py"


def run_probe(code: str, overrides: dict[str, str]) -> subprocess.CompletedProcess:
    env = {key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ}
    env.update(overrides)
    return subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=REPO,
        env=env,
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )


@pytest.mark.parametrize("relative", [False, True])
def test_real_paths_resolve_inside_sandbox(tmp_path, relative):
    sandbox = tmp_path / "sandbox"
    configs = sandbox / "configs"
    data = sandbox / "data"
    overrides = {
        "CONTRACT_CONFIGS_DIR": str(configs),
        "CONTRACT_DATA_DIR": str(data),
        "REMOTE_SHARE_DB_DIR": str(tmp_path / "wrong-share-dir"),
    }
    if relative:
        overrides = {key: os.path.relpath(value, REPO) for key, value in overrides.items()}
    result = run_probe(
        f"""
import json, os, runpy, sys
from pathlib import Path

# Path-only probes must never open a database, even on a regressed launcher.
def audit(event, args):
    if event in ("sqlite3.connect", "socket.connect"):
        raise AssertionError("Probe must not open databases or network connections")
sys.addaudithook(audit)
launcher = runpy.run_path({str(LAUNCHER)!r})
launcher["_maybe_redirect_config_paths"]()
from backend import config
from backend.core.metadata import base, usage_log_db
paths = {{
    "cwd": Path.cwd(),
    "configs": config.CONFIGS_DIR,
    "data": config.DATA_DIR,
    "duckdb": config.duckdb_path("sandbox-service"),
    "metadata": base.db_path("sandbox-service"),
    "usage_db": usage_log_db.db_path("sandbox-service"),
    "usage_config": config._USAGE_LOGGING_CONFIG_PATH,
    "buffer_cache": Path("cache"),
}}
print(json.dumps({{key: str(Path(value).resolve()) for key, value in paths.items()}}))
""",
        overrides,
    )
    assert result.returncode == 0, result.stderr
    paths = json.loads(result.stdout)
    assert paths == {
        "cwd": str(sandbox),
        "configs": str(configs),
        "data": str(data),
        "duckdb": str(data / "services/sandbox-service.duckdb"),
        "metadata": str(data / "services/sandbox-service.metadata.db"),
        "usage_db": str(data / "services/sandbox-service.usage_log.db"),
        "usage_config": str(data / "system/usage_logging.json"),
        "buffer_cache": str(sandbox / "cache"),
    }


@pytest.mark.parametrize(
    "overrides",
    [
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs"},
        {"CONTRACT_DATA_DIR": "sandbox/data"},
        {"CONTRACT_CONFIGS_DIR": "", "CONTRACT_DATA_DIR": ""},
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs", "CONTRACT_DATA_DIR": "elsewhere/data"},
        {"CONTRACT_CONFIGS_DIR": "sandbox/configs", "CONTRACT_DATA_DIR": "sandbox/custom-data"},
    ],
)
def test_incomplete_or_inconsistent_overrides_fail_before_import(overrides):
    result = run_probe(
        f"""
import runpy, sys
def audit(event, args):
    if event in ("os.mkdir", "sqlite3.connect", "socket.connect"):
        raise AssertionError("Invalid overrides must not cause I/O")
sys.addaudithook(audit)
launcher = runpy.run_path({str(LAUNCHER)!r})
try:
    launcher["_maybe_redirect_config_paths"]()
except ValueError:
    assert "backend.config" not in sys.modules
else:
    raise AssertionError("Unsafe sandbox overrides accepted")
""",
        overrides,
    )
    assert result.returncode == 0, result.stderr


def test_no_overrides_is_a_noop():
    result = run_probe(
        f"""
import os, runpy, sys
launcher = runpy.run_path({str(LAUNCHER)!r})
before = os.getcwd(), list(sys.path), dict(os.environ)
launcher["_maybe_redirect_config_paths"]()
assert before == (os.getcwd(), sys.path, dict(os.environ))
assert "backend.config" not in sys.modules
""",
        {},
    )
    assert result.returncode == 0, result.stderr


def test_main_retains_repo_importability_after_chdir(tmp_path):
    result = run_probe(
        f"""
import runpy, sys, types
from pathlib import Path
sys.path = [p for p in sys.path if p and Path(p).resolve() != Path({str(REPO)!r})]
def run(app, **kwargs):
    from backend import config
    assert app == "backend.main:app"
    assert Path.cwd() == Path({str(tmp_path)!r})
    assert config.CONFIGS_DIR == Path({str(tmp_path / "configs")!r})
sys.modules["uvicorn"] = types.SimpleNamespace(run=run)
sys.argv = [{str(LAUNCHER)!r}, "--port", "13003"]
runpy.run_path({str(LAUNCHER)!r}, run_name="__main__")
""",
        {"CONTRACT_CONFIGS_DIR": str(tmp_path / "configs"), "CONTRACT_DATA_DIR": str(tmp_path / "data")},
    )
    assert result.returncode == 0, result.stderr
