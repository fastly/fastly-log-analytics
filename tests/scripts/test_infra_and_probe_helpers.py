"""scripts/lib/gce_ssh.sh, readiness probes, admin TLS, CLI, prom, streaming fixture, profile_insights, check_osv, port_forward, and run_contract_backend tests."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import importlib.util
import json
import os
import random
import re
import signal
import socket
import ssl
import subprocess
import sys
import tarfile
import threading
import time
import urllib.error
from collections.abc import Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from urllib.request import Request

import duckdb
import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from scripts import check_environment_health as health
from scripts import run_contract_backend
from scripts.dev import audit_environments as audit
from scripts.dev import measure_e2e_freshness as freshness
from scripts.dev import scale_harness
from scripts.dev.measure_e2e_freshness import (
    compute_lag_statistics,
    configured_polling,
    cron_completion_time,
    load_environment_config,
    new_terminal_run,
    rum_beacon_count,
    seconds_between,
)
from scripts.dev.scale_harness import (
    TrafficProfile,
    build_request,
    build_request_url,
    freshness_lag_seconds,
    parse_stages,
    render_report,
)
from scripts.lib import admin_tls, environments, readiness
from scripts.lib import admin_tls as admin_http
from scripts.lib import environments as prom
from scripts.lib import log_factory as lf
from scripts.lib import port_forward as pf
from scripts.lib.environments import add_common_args, remediation_command
from scripts.lib.log_factory import BOT_USER_AGENTS, build_cmcd_request, build_cmcd_sessions
from scripts.lib.section_measurements import BUDGETS
from scripts.run_contract_backend import install_streaming_fixture

REPO = Path(__file__).resolve().parents[2]
LIB = REPO / "scripts" / "lib" / "gce_ssh.sh"
LAUNCHER = REPO / "scripts" / "run_contract_backend.py"
SCRIPT_PATH = REPO / "scripts" / "check_osv.py"
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
import json, os, shutil, subprocess, sys
argv = sys.argv[1:]
with open(os.environ["STUB_LOG"], "a") as fh:
    fh.write(json.dumps(argv) + "\\n")
if argv[:2] == ["compute", "ssh"]:
    cmd = [a for a in argv if a.startswith("--command=")][0][len("--command="):]
    sys.exit(subprocess.call(["bash", "-c", cmd]))
if argv[:2] == ["storage", "cp"]:
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
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    return env, calls, tmp_path


def run_bash(body: str, env: dict[str, str], cwd: Path, *args: str) -> subprocess.CompletedProcess:
    cmd = ["bash", "-c", f'source "{LIB}"\n{body}']
    if args:
        cmd.extend(["_", *args])
    return subprocess.run(cmd, env=env, cwd=cwd, text=True, capture_output=True, timeout=30, check=False)


def test_exact_argv_for_simple_and_hostile_commands(gce_stub):
    env, calls, tmp = gce_stub
    r = run_bash("gce_ssh_run my-vm us-central1-a --quiet -- echo /a b", env, tmp)
    assert r.returncode == 0, r.stderr
    assert calls() == [["compute", "ssh", "my-vm", "--zone=us-central1-a", "--quiet", "--command=echo /a b"]]

    r2 = run_bash("gce_ssh_run vm z1 -- echo 'a b;$(x)'", env, tmp)
    assert r2.returncode == 0, r2.stderr
    assert calls()[-1] == ["compute", "ssh", "vm", "--zone=z1", r"--command=echo a\ b\;\$\(x\)"]


@pytest.mark.parametrize("value", HOSTILE)
def test_run_roundtrips_hostile_argument_without_executing(gce_stub, value):
    env, calls, tmp = gce_stub
    dump = tmp / "dump.py"
    dump.write_text(DUMP)
    r = run_bash('gce_ssh_run vm z1 -- "$1" "$2" "$3" "$4"', env, tmp, sys.executable, str(dump), value, value)
    assert r.returncode == 0, r.stderr
    assert (tmp / "dump.bin").read_bytes() == b"\0".join([value.encode(), value.encode()])
    assert not (tmp / "CANARY").exists()
    assert len(calls()) == 1


@pytest.mark.parametrize("value", HOSTILE)
def test_script_binds_hostile_args_as_positionals(gce_stub, value):
    env, calls, tmp = gce_stub
    script = 'printf "%s\\0" "$#" "$@" > "$DUMP_OUT"'
    r = run_bash('gce_ssh_script vm z1 --quiet -- "$1" "$2" "$3"', env, tmp, script, value, "second " + value)
    assert r.returncode == 0, r.stderr
    got = (tmp / "dump.bin").read_bytes().split(b"\0")[:-1]
    assert got == [b"2", value.encode(), ("second " + value).encode()]
    assert not (tmp / "CANARY").exists()
    (argv,) = calls()
    assert argv[:5] == ["compute", "ssh", "vm", "--zone=z1", "--quiet"]
    assert argv[5].startswith("--command=bash -c ")


@pytest.mark.parametrize(
    "bad", ["", "-oProxyCommand=x", "vm name", "vm;touch CANARY", "vm$(x)", "vm\nx", ".hidden", "a/b"]
)
@pytest.mark.parametrize("slot", ["instance", "zone"])
def test_rejects_unsafe_instance_or_zone(gce_stub, bad, slot):
    env, calls, tmp = gce_stub
    inst, zone = (bad, "z1") if slot == "instance" else ("vm", bad)
    assert run_bash('gce_ssh_run "$1" "$2" -- true', env, tmp, inst, zone).returncode == 2
    assert calls() == []


def test_rejects_non_flag_before_separator_and_missing_command_and_passes_exit(gce_stub):
    env, calls, tmp = gce_stub
    for cmd in (
        "gce_ssh_run vm z1 oops -- true",
        "gce_ssh_run vm z1 --quiet",
        "gce_ssh_run vm z1 --quiet --",
        "gce_ssh_script vm z1 --",
    ):
        assert run_bash(cmd, env, tmp).returncode == 2
    assert calls() == []

    r = run_bash("gce_ssh_script vm z1 -- 'echo out; exit 7'", env, tmp)
    assert (r.returncode, r.stdout) == (7, "out\n")


def test_no_ssh_command_string_building_or_eval_in_lib():
    text = LIB.read_text()
    assert not re.search(r"\beval\b", re.sub(r"^\s*#.*$", "", text, flags=re.M))


@pytest.mark.parametrize("script", TRACKED_SCRIPTS, ids=lambda p: p.name)
def test_tracked_scripts_use_the_shared_helper(script):
    code = "\n".join(line for line in script.read_text().splitlines() if not line.lstrip().startswith("#"))
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
    assert json.loads(marker.read_text())["remote"].startswith("gs://b/configs/")
    ssh_calls = [c for c in calls() if c[:2] == ["compute", "ssh"]]
    assert len(ssh_calls) == 2
    assert all(c[2] == "vm-1" and c[3] == "--zone=us-central1-a" for c in ssh_calls)


# ── Merged from test_health_probes.py ──────────────────────────────────────────────────────

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
        return health.EnvironmentTarget(name, "Stub", self.origin, self.origin, "svc1", "fix-it")


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
    def __init__(self, sleep: Any) -> None:
        self.sleep = sleep

    def __getattr__(self, name: str) -> Any:
        return getattr(time, name)


def dead_origin() -> str:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"http://127.0.0.1:{sock.getsockname()[1]}"


def test_probe_reports_status_body_latency_and_errors(probe_stub: Stub) -> None:
    result = readiness.probe(f"{probe_stub.origin}/api/health", timeout=5)
    assert (result.status, result.failed, result.http_error) == (200, False, False)
    assert result.json() == {"status": "ok", "version": "1.2.3"}
    assert result.content_type == "application/json" and result.latency_ms >= 0

    probe_stub.routes["GET /api/health"] = (503, {"error": "busy"})
    err_res = readiness.probe(f"{probe_stub.origin}/api/health", timeout=5)
    assert (err_res.status, err_res.http_error, err_res.reason) == (503, True, "Service Unavailable")
    assert err_res.error == "HTTP Error 503: Service Unavailable" and err_res.json() == {"error": "busy"}

    dead_res = readiness.probe(f"{dead_origin()}/x", timeout=2)
    assert dead_res.status is None and dead_res.failed and not dead_res.http_error
    assert isinstance(dead_res.exc, urllib.error.URLError)


def test_probe_delegates_to_admin_urlopen_with_request_details(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake(request: Any, timeout: float) -> Any:
        captured.update(
            url=request.full_url,
            method=request.get_method(),
            data=request.data,
            timeout=timeout,
            headers=dict(request.header_items()),
        )
        raise OSError("stop")

    monkeypatch.setattr(readiness, "admin_urlopen", fake)
    result = readiness.probe("http://x/y", timeout=7, headers={"X-A": "1"}, data=b"{}", method="POST")
    assert result.error == "stop"
    assert captured == {"url": "http://x/y", "method": "POST", "data": b"{}", "timeout": 7, "headers": {"X-a": "1"}}


def test_health_ok_and_failure_modes(probe_stub: Stub) -> None:
    result = health.check_backend_health(probe_stub.target(), timeout_s=5)
    assert (result.backend_ok, result.status_code, result.version, result.error) == (True, 200, "1.2.3", None)
    headers = next(h for line, h, _ in probe_stub.seen if line == "GET /api/health")
    assert headers["User-Agent"] == "FLA-HealthCheck/1.0" and headers["Accept"] == "application/json"

    for route, expected, expected_err in [
        ((503, {"x": 1}), (False, 503, None), "HTTPError 503: Service Unavailable"),
        (
            (200, {"status": "degraded", "version": "9"}),
            (False, 200, "9"),
            'Unexpected payload: {"status": "degraded", "version": "9"}',
        ),
        ((200, b"<html>"), (False, None, None), "Expecting value: line 1 column 1 (char 0)"),
    ]:
        probe_stub.routes["GET /api/health"] = route
        r = health.check_backend_health(probe_stub.target(), timeout_s=5)
        assert (r.backend_ok, r.status_code, r.version) == expected
        assert r.error == expected_err

    dead_target = health.EnvironmentTarget("d", "A", dead_origin(), "", "svc1", "fix")
    r_dead = health.check_backend_health(dead_target, timeout_s=2)
    assert (r_dead.backend_ok, r_dead.status_code) == (False, None)
    assert r_dead.error and "Connection refused" in r_dead.error


def test_tenancy_ok_and_failure_modes(probe_stub: Stub) -> None:
    result = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (result.service_match, result.rejection_ok, result.rejection_code, result.error) == (True, True, 404, None)
    assert result.resolved_service_id == "svc1"
    line, headers, body = next(s for s in probe_stub.seen if s[0].startswith("POST"))
    assert line == "POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"
    assert json.loads(body) == {"time_range": "24h"} and headers["Content-Type"] == "application/json"

    probe_stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (200, {})
    r200 = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (r200.rejection_ok, r200.rejection_code) == (False, 200)
    assert r200.error == "Tenancy breach: fake tenant returned HTTP 200 (expected 400/401/403/404)"

    probe_stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (500, {})
    r500 = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (r500.rejection_ok, r500.rejection_code) == (False, 500)

    probe_stub.routes["POST /api/dashboard/bundle?service_id=nonexistent_tenant_xyz"] = (404, {"error": "nope"})
    probe_stub.routes["GET /api/bootstrap"] = (200, {"active_service_id": "other"})
    rmis = health.check_tenancy(probe_stub.target(), timeout_s=5)
    assert (rmis.service_match, rmis.rejection_ok) == (False, True)
    assert rmis.error == "Service mismatch: got 'other', expected 'svc1'"


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


def test_rbac_ok_and_failure_modes(probe_stub: Stub) -> None:
    result = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert (result.auth_config_ok, result.unauth_analyst_blocked, result.unauth_analyst_code, result.error) == (
        True,
        True,
        200,
        None,
    )
    headers = next(h for line, h, _ in probe_stub.seen if line == "GET /api/share/auth-config")
    assert headers["X-Remote-Analyst"] == "1"

    probe_stub.routes["GET /api/share/auth-config"] = (200, {"other": 1})
    r1 = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert r1.auth_config_ok is False and r1.unauth_analyst_blocked is True
    assert r1.error == "Auth config endpoint failed or returned unexpected payload"

    probe_stub.routes["GET /api/share/auth-config"] = (404, {})
    r2 = health.check_rbac(probe_stub.target(), timeout_s=5)
    assert (r2.auth_config_ok, r2.unauth_analyst_blocked, r2.unauth_analyst_code) == (False, False, None)
    assert r2.error == "Failed to query /api/share/auth-config: HTTP Error 404: Not Found"


def _run_main(monkeypatch: pytest.MonkeyPatch, target: health.EnvironmentTarget, *extra: str) -> int:
    monkeypatch.setattr(health, "ENVIRONMENTS", {target.name: target})
    monkeypatch.setattr("sys.argv", ["check_environment_health.py", "--env", target.name, *extra])
    return health.main()


def test_main_text_and_json_reports_golden(
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

    probe_stub.routes["GET /api/health"] = (503, {})
    assert _run_main(monkeypatch, probe_stub.target("stub-env")) == 1
    fail_lines = re.sub(r"\d+\.\dms", "<lat>", capsys.readouterr().out).splitlines()[6:]
    assert fail_lines[0].startswith("stub-env               | FAIL     | svc1 (OK)")
    assert fail_lines[1:3] == ["   [!] Health Error: HTTPError 503: Service Unavailable", "   [!] Fix command: fix-it"]
    assert "FAILURE: 1/1 environments failed checks." in fail_lines


def test_audit_get_and_post_json(probe_stub: Stub, monkeypatch: pytest.MonkeyPatch) -> None:
    status, data = audit.http_get_json(f"{probe_stub.origin}/api/health", headers={"X-T": "1"}, timeout=5)
    assert (status, data) == (200, {"status": "ok", "version": "1.2.3"})
    _, headers, _ = probe_stub.seen[-1]
    assert headers["User-Agent"] == "FLA-AuditTool/1.0" and headers["X-T"] == "1"

    probe_stub.routes.update(
        {"GET /e1": (500, {"detail": "boom"}), "GET /e2": (502, b"<html>"), "GET /e3": (200, b"<html>")}
    )
    assert audit.http_get_json(f"{probe_stub.origin}/e1") == (500, {"detail": "boom"})
    assert audit.http_get_json(f"{probe_stub.origin}/e2") == (502, {"error": "HTTP Error 502: Bad Gateway"})
    assert audit.http_get_json(f"{probe_stub.origin}/e3") == (0, {"error": "Expecting value: line 1 column 1 (char 0)"})
    status, data = audit.http_get_json(f"{dead_origin()}/x", timeout=2)
    assert status == 0 and "Connection refused" in data["error"]

    probe_stub.routes["POST /p"] = (422, {"detail": "bad"})
    assert audit.http_post_json(f"{probe_stub.origin}/p", {"a": 1}, timeout=5) == (422, {"detail": "bad"})
    line, headers, body = probe_stub.seen[-1]
    assert (line, json.loads(body), headers["Content-Type"]) == ("POST /p", {"a": 1}, "application/json")
    probe_stub.routes["POST /q"] = (200, {"ok": True})
    assert audit.http_post_json(f"{probe_stub.origin}/q", timeout=5) == (200, {"ok": True})
    assert json.loads(probe_stub.seen[-1][2]) == {}

    probe_stub.routes["POST /p500"] = (500, {"e": 1})
    monkeypatch.setattr(audit, "time", _SleepStub(lambda _s: pytest.fail("must not retry HTTP errors")))
    assert audit.http_post_json(f"{probe_stub.origin}/p500", {}, timeout=5) == (500, {"e": 1})
    assert [line for line, _, _ in probe_stub.seen].count("POST /p500") == 1


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


def test_load_persisted_deploy_env_populates_admin_origins_and_respects_existing_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
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
        f"LOCAL_STANDARD_ADMIN_MTLS=1\nGCE_ADMIN_MTLS=1\nELEVATION_ADMIN_MTLS=1\n"
        f"ADMIN_GATEWAY_ENDPOINTS='{json.dumps(endpoints)}'\n"
    )
    deploy_env.chmod(0o600)
    monkeypatch.setenv("FLA_ADMIN_DEPLOY_ENV", str(deploy_env))
    monkeypatch.setenv("GCE_ADMIN_MTLS", "0")

    try:
        loaded = environments.load_persisted_deploy_env()
        assert loaded["LOCAL_STANDARD_ADMIN_MTLS"] == "1" and loaded["ELEVATION_ADMIN_MTLS"] == "1"
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
        audit, "http_get_json", lambda url, headers=None, timeout=5.0: (0, {"error": "<urlopen error timed out>"})
    )
    env = audit.EnvironmentConfig(
        "remote-high-scale",
        "High-Scale (Remote/K8s)",
        "https://10.0.0.10:8443",
        "https://10.0.0.10:8443/dashboard",
        "svc-hs",
        True,
    )
    res = audit.audit_target(env, timeout=1.0)
    assert not res.reachable
    assert any("VPN/tunnel" in err and "10.0.0.10:8443" in err for err in res.errors)


# ── Merged from test_lib_helpers.py ──────────────────────────────────────────────────────


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
    assert (seen[0].method, seen[0].content, seen[0].headers["x-page-load-id"]) == (
        "POST",
        b'{"filters":{}}',
        "test-correlation",
    )


@pytest.mark.parametrize(
    "url,headers",
    [
        ("https://other.example.com/api/test", {}),
        ("https://example.com/api/test", {"X-Admin-Gateway-Token": "do-not-send"}),
    ],
)
def test_deadline_transport_rejects_untrusted_identity_use_before_tls(
    monkeypatch: pytest.MonkeyPatch, url: str, headers: dict
) -> None:
    monkeypatch.setattr(admin_tls, "_settings", lambda: {"test": {"origin": "https://example.com:443"}})
    monkeypatch.setattr(admin_tls, "_context", lambda config: pytest.fail("TLS identity must not be loaded"))
    with pytest.raises(ValueError):
        asyncio.run(admin_http.async_admin_json(Request(url, headers=headers), timeout=1))


# ── Merged from test_cli.py ──


def _parser(**kwargs: object) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="t")
    add_common_args(parser, **kwargs)  # type: ignore[arg-type]
    return parser


def test_common_args_behavior() -> None:
    for bad_arg in (["--timeout", "1"], ["--json"]):
        with pytest.raises(SystemExit):
            _parser().parse_args(bad_arg)

    parser = _parser(timeout_default=5.0, timeout_label="HTTP request")
    assert parser.parse_args([]).timeout == 5.0
    assert parser.parse_args(["--timeout", "2.5"]).timeout == 2.5
    assert "HTTP request timeout in seconds (default: 5.0)" in parser.format_help()

    jparser = _parser(json_help="Output raw JSON results")
    assert jparser.parse_args([]).json is False
    assert jparser.parse_args(["--json"]).json is True
    assert "Output raw JSON results" in jparser.format_help()


@pytest.mark.parametrize(
    ("module", "default"),
    [("scripts.check_environment_health", 5.0), ("scripts.dev.audit_environments", 10.0)],
)
def test_adopting_scripts_keep_their_timeout_defaults(
    module: str, default: float, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    mod = importlib.import_module(module)
    monkeypatch.setattr("sys.argv", ["prog", "--help"])
    with pytest.raises(SystemExit):
        mod.main()
    out = capsys.readouterr().out
    assert f"timeout in seconds (default: {default})" in out and "--json" in out


# ── Merged from test_prom.py ──


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


def test_instant_returns_sample_or_none_on_errors(prom_server: tuple[str, list[str]]) -> None:
    base, seen = prom_server
    assert prom.instant(base, "up{job='a b'}") == 12.5
    assert seen == ["up{job='a b'}"]
    assert prom.instant(base, "empty") is None
    assert prom.instant(base, "boom") is None
    assert prom.instant("http://127.0.0.1:1", "up", timeout=1) is None


# ── Merged from test_streaming_e2e_fixture.py ──


def test_streaming_fixture_requires_an_isolated_contract_sandbox(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CONTRACT_STREAMING_FIXTURES", "1")
    monkeypatch.delenv("CONTRACT_CONFIGS_DIR", raising=False)
    monkeypatch.delenv("CONTRACT_DATA_DIR", raising=False)
    monkeypatch.setattr(sys, "argv", ["run_contract_backend.py"])
    with pytest.raises(ValueError, match="isolated contract sandbox"):
        run_contract_backend.main()


def test_streaming_fixture_is_cookie_scoped_and_isolated() -> None:
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

        other_browser.post("/api/__e2e__/streaming-fixture", json={"available": False, "has_data": False})
        assert browser.post("/api/cmcd/aggregates", json={}).json() == payload
        assert other_browser.post("/api/cmcd/aggregates", json={}).json() == {"available": False, "has_data": False}

        assert browser.delete("/api/__e2e__/streaming-fixture").status_code == 200
        assert browser.get("/api/__e2e__/streaming-fixture").status_code == 404
        assert browser.post("/api/cmcd/aggregates", json=body).json() == {"available": False}


# ── Merged from test_profile_insights.py ──


def test_run_profile_and_cli_output(tmp_path: Path):
    from scripts.dev.profile_insights import format_markdown_report, run_profile
    from scripts.dev.profile_insights import main as profile_main

    res = run_profile(total_rows=1000, runs=2, num_files=3, window_hours=1.0, baseline_hours=24.0)
    assert res["runs"] == 2 and res["dataset"]["total_rows"] == 1000
    assert res["cold_total"]["wall_p50_ms"] > 0 and res["warm_total"]["wall_p50_ms"] >= 0
    assert len(res["top_10_by_wall_p50"]) == 10 and len(res["top_10_by_cpu_p50"]) == 10

    op_names = {op["operation"] for op in res["all_operations"]}
    assert {"phase:scratch_table_build", "coalesced:city", "coalesced:url", "coalesced:ip", "coalesced:waf"}.issubset(
        op_names
    )
    assert any(name.startswith("insight:") for name in op_names)

    md = format_markdown_report(res)
    assert "# Insights Performance Profile" in md and "Top 10 Costs by Wall-Clock p50" in md

    md_out, json_out = tmp_path / "profile.md", tmp_path / "profile.json"
    rc = profile_main(
        ["--rows", "500", "--runs", "1", "--files", "2", "--output", str(md_out), "--json-output", str(json_out)]
    )
    assert rc == 0 and md_out.exists() and json_out.exists()
    assert json.loads(json_out.read_text())["dataset"]["total_rows"] == 500


# ── Merged from test_check_osv.py ──


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
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H", 10.0),
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:N/I:N/A:H", 7.5),
        ("CVSS:3.1/AV:N/AC:L/PR:N/UI:R/S:C/C:L/I:L/A:N", 6.1),
        ("CVSS:3.1/AV:N/AC:H/PR:L/UI:N/S:U/C:L/I:N/A:N", 3.1),
    ],
)
def test_cvss3_base_score_known_vectors(osv, vector, expected):
    assert osv.cvss3_base_score(vector) == pytest.approx(expected, abs=0.05)


def test_cvss3_base_score_rejects_v4_vector(osv):
    assert osv.cvss3_base_score("CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N") is None


@pytest.mark.parametrize(
    ("vuln", "expected"),
    [
        (
            {
                "id": "CVE-2021-44228",
                "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
            },
            "CRITICAL",
        ),
        (
            {
                "id": "X",
                "database_specific": {"severity": "LOW"},
                "severity": [{"type": "CVSS_V3", "score": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:C/C:H/I:H/A:H"}],
            },
            "CRITICAL",
        ),
        ({"id": "X", "database_specific": {"severity": "critical"}}, "CRITICAL"),
        ({"id": "X"}, "UNKNOWN"),
        (
            {
                "id": "CVE-2099-0001",
                "severity": [
                    {"type": "CVSS_V4", "score": "CVSS:4.0/AV:N/AC:L/AT:N/PR:N/UI:N/VC:H/VI:H/VA:H/SC:N/SI:N/SA:N"}
                ],
            },
            "UNKNOWN",
        ),
    ],
)
def test_severity_for_vuln_resolution(osv, vuln, expected):
    assert osv.severity_for_vuln(vuln) == expected


# ── Merged from test_port_forward.py ──────────────────────────────────────────────────────

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
        return True
    except ProcessLookupError:
        return False


def wait_dead(pid: int, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not alive(pid):
            return True
        time.sleep(0.05)
    return False


def test_argv_and_parse_forward():
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
    assert pf.parse_forward("svc/backend-svc:8002:8000", namespace="ns") == pf.Forward(
        "svc/backend-svc", 8002, 8000, namespace="ns"
    )


@pytest.mark.parametrize("bad", ["svc/x", "svc/x:80", "svc/x:a:b", ":1:2", "svc/x:0:80", "svc/x:80:70000"])
def test_parse_forward_rejects_malformed_specs(bad):
    with pytest.raises(ValueError):
        pf.parse_forward(bad)


def test_port_in_use_is_refused_before_spawning(kubectl):
    first = free_port()
    for make_specs in (
        lambda taken: [pf.Forward("svc/x", taken, 80)],
        lambda taken: [pf.Forward("svc/a", first, 80), pf.Forward("svc/b", taken, 80)],
    ):
        with socket.socket() as blocker:
            blocker.bind(("127.0.0.1", 0))
            blocker.listen(1)
            with pytest.raises(pf.PortInUse):
                pf.ForwardSet(make_specs(blocker.getsockname()[1]), kubectl=kubectl.path).start()
    assert kubectl.calls() == []


def test_early_exit_and_readiness_timeout_clean_up(kubectl, monkeypatch):
    monkeypatch.setenv("STUB_MODE", "exit-early")
    with pytest.raises(pf.ForwardExited, match="pod is not running"):
        pf.ForwardSet([pf.Forward("svc/x", free_port(), 80)], kubectl=kubectl.path, ready_timeout=15).start()
    assert wait_dead(kubectl.calls()[-1]["child"])

    monkeypatch.setenv("STUB_MODE", "never-ready")
    with pytest.raises(pf.ReadinessTimeout):
        pf.ForwardSet([pf.Forward("svc/x", free_port(), 80)], kubectl=kubectl.path, ready_timeout=2.0).start()
    call = kubectl.calls()[-1]
    assert wait_dead(call["pid"]) and wait_dead(call["child"])


def test_poll_restarts_within_budget_and_raises_when_spent(kubectl, monkeypatch):
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
        assert proc.stdout is not None and proc.stdout.readline().startswith("port-forward ready:")
        (call,) = kubectl.calls()
        proc.send_signal(sig)
        assert proc.wait(timeout=20) == 128 + sig
    finally:
        if proc.poll() is None:
            proc.kill()
    assert wait_dead(call["pid"]) and wait_dead(call["child"]) and not pf.port_in_use(port)


def test_cli_runs_command_and_reports_port_in_use(kubectl):
    port = free_port()
    code = f"import socket,sys; socket.create_connection(('127.0.0.1', {port}), timeout=2).close(); sys.exit(7)"
    proc = _cli(kubectl, f"svc/x:{port}:80", "--", sys.executable, "-c", code)
    out, _ = proc.communicate(timeout=30)
    assert proc.returncode == 7 and "port-forward ready:" in out
    (call,) = kubectl.calls()
    assert wait_dead(call["pid"]) and wait_dead(call["child"])

    with socket.socket() as blocker:
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        taken = blocker.getsockname()[1]
        proc2 = _cli(kubectl, f"svc/x:{taken}:80")
        _, err = proc2.communicate(timeout=20)
    assert proc2.returncode == 1 and f"port {taken} is already in use" in err


def test_remote_high_scale_remediation_uses_the_supervisor(monkeypatch):
    monkeypatch.delenv("REMOTE_HIGH_SCALE_FORWARD_CMD", raising=False)
    monkeypatch.setenv("ELEVATION_NAMESPACE", "team-ns")
    cmd = remediation_command("remote-high-scale")
    assert cmd.startswith("python3 -m scripts.lib.port_forward --namespace team-ns ") and "&" not in cmd
    for spec in cmd.split()[-2:]:
        pf.parse_forward(spec)


# ── Merged from test_run_contract_backend.py ──────────────────────────────────────────────────────


def run_probe(code: str, overrides: dict[str, str]) -> subprocess.CompletedProcess:
    env = {key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ}
    env.update(overrides)
    return subprocess.run(
        [sys.executable, "-B", "-c", code], cwd=REPO, env=env, text=True, capture_output=True, timeout=30, check=False
    )


@pytest.mark.parametrize("relative", [False, True])
def test_real_paths_resolve_inside_sandbox(tmp_path, relative):
    sandbox = tmp_path / "sandbox"
    configs, data = sandbox / "configs", sandbox / "data"
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
    assert json.loads(result.stdout) == {
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


def test_no_overrides_and_main_chdir(tmp_path):
    r1 = run_probe(
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
    assert r1.returncode == 0, r1.stderr

    r2 = run_probe(
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
    assert r2.returncode == 0, r2.stderr


# ── Merged from tests/scripts/test_scale_and_freshness_harness.py ──

NOW = datetime(2026, 10, 19, 12, 0, 0, tzinfo=UTC)

RECORD_KEYS = {
    "service_id",
    "timestamp",
    "geo_city",
    "geo_region",
    "geo_country_code",
    "server_pop",
    "browser",
    "os",
    "device",
    "rum_cid",
    "req_id",
    "fastly_req_id",
    "request_event_id",
    "url",
    "rum_body",
}


def _digest(obj: object) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()


def test_records_are_deterministic_for_a_seed_and_match_golden_digest() -> None:
    a = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    b = lf.generate_rum_records("svc", 200, NOW, random.Random(7))
    c = lf.generate_rum_records("svc", 200, NOW, random.Random(8))
    assert a == b and a != c
    assert _digest(a) == "884741d052c6a872939dce4c7b001739062f79dde8d457a4ec4e490d49555614"


def test_record_shape_matches_ingest_contract() -> None:
    records = lf.generate_rum_records("svc-1", 300, NOW, random.Random(1))
    assert len({r["timestamp"] for r in records}) == 300
    assert records[0]["timestamp"] == "2026-10-19T12:00:00.000Z"
    assert records[1]["timestamp"] == "2026-10-19T11:59:59.950Z"
    kinds: set[str] = set()
    for rec in records:
        assert set(rec) == RECORD_KEYS and rec["service_id"] == "svc-1"
        assert rec["req_id"].startswith("req-") and len(rec["req_id"]) == 4 + 16
        assert rec["fastly_req_id"] == rec["request_event_id"] == rec["req_id"]
        assert rec["rum_cid"].startswith("cid-") and rec["url"].startswith("http://localhost/")
        body = json.loads(rec["rum_body"])
        assert body["meta"]["page"]["url"] == rec["url"] and body["meta"]["os"]["name"] == rec["os"]
        assert body["meta"]["browser"] == {"name": rec["browser"], "mobile": rec["device"] == "Mobile"}
        (kind,) = set(body) - {"meta"}
        kinds.add(kind)
    assert kinds == {"measurements", "events", "exceptions"}
    assert len({r["req_id"] for r in records}) == 300


def test_vitals_ratings_follow_thresholds() -> None:
    profile = lf.FILE_RUM_PROFILE
    cases = [
        ("LCP", 2500, "good"),
        ("LCP", 4000, "needs_improvement"),
        ("LCP", 4001, "poor"),
        ("CLS", 0.1, "good"),
        ("CLS", 0.25, "needs_improvement"),
        ("CLS", 0.26, "poor"),
        ("INP", 200, "good"),
        ("INP", 500, "needs_improvement"),
        ("INP", 501, "poor"),
        ("TTFB", 800, "good"),
        ("TTFB", 801, "poor"),
        ("FCP", 100, "good"),
        ("FCP", 101, "poor"),
    ]
    for metric, value, rating in cases:
        assert lf.rum_rating(metric, value, profile) == rating
    assert lf.rum_rating("FID", 700, lf.HARNESS_RUM_PROFILE) == "good"


def test_harness_beacon_url_carries_vcl_identity_parameters_and_is_seeded() -> None:
    url = lf.build_rum_beacon_url("https://logs.example.com/", random.Random(3))
    assert url == lf.build_rum_beacon_url("https://logs.example.com", random.Random(3))
    parsed = urlparse(url)
    assert parsed.path == "/rum-beacon"
    qs = parse_qs(parsed.query)
    assert set(qs) == {"rum_metric_name", "rum_metric_value", "rum_cid", "cid", "req", "rum_pathname"}
    assert qs["cid"] == qs["rum_cid"]
    assert qs["req"][0] == "req-" + qs["cid"][0].removeprefix("cid-")
    assert len(qs["req"][0]) == 4 + 32


def test_harness_beacon_body_golden_and_shape() -> None:
    url = "https://h.example.com/rum-beacon?rum_metric_name=LCP&rum_metric_value=3500&rum_pathname=%2Fdashboard"
    bodies = [lf.build_rum_beacon_body(url, random.Random(s), NOW) for s in range(40)]
    assert bodies == [lf.build_rum_beacon_body(url, random.Random(s), NOW) for s in range(40)]
    assert _digest(bodies) == "4aaf0a1622b62558e7684d394595ce31be0232b4a2517ece00d8485dc184b2b4"
    seen = set()
    for body in bodies:
        assert body["meta"]["page"]["url"] == "https://h.example.com/dashboard"
        assert body["meta"]["browser"]["name"] in lf.HARNESS_RUM_PROFILE.browsers
        (kind,) = set(body) - {"meta"}
        seen.add(kind)
        if kind == "measurements":
            (m,) = body["measurements"]
            assert m["type"] == "web-vitals" and m["values"] == {"LCP": 3500.0}
            assert m["context"]["rating"] == "needs_improvement"
        if kind == "exceptions":
            assert body["exceptions"][0]["stacktrace"]["frames"][0]["filename"] == "static/chunks/main.js"
    assert seen == {"measurements", "events", "exceptions"}


def test_beacon_body_defaults_when_query_is_missing_or_bad() -> None:
    body = lf.build_rum_beacon_body("https://h.example.com/rum-beacon?rum_metric_value=abc", random.Random(0), NOW)
    assert body["meta"]["page"]["url"] == "https://h.example.com/"
    if "measurements" in body:
        assert body["measurements"][0]["values"] == {"LCP": 1200.0}


def test_source_ip_is_seeded_and_in_range() -> None:
    assert lf.random_source_ip(random.Random(5)) == lf.random_source_ip(random.Random(5))
    rng = random.Random(9)
    for _ in range(200):
        a, b, c, d = (int(x) for x in lf.random_source_ip(rng).split("."))
        assert 12 <= a <= 223 and all(1 <= x <= 254 for x in (b, c, d))


def test_scale_harness_build_request_url_uses_injected_rng() -> None:
    assert scale_harness.build_request_url("https://x.example.com", 0.0, random.Random(1)) == "https://x.example.com"
    seeded = [scale_harness.build_request_url("https://x.example.com", 1.0, random.Random(4)) for _ in range(2)]
    assert seeded[0] == seeded[1] and "/rum-beacon?" in seeded[0]
    rng = random.Random(11)
    beacons = sum(
        "/rum-beacon?" in scale_harness.build_request_url("https://x.example.com", 0.5, rng) for _ in range(200)
    )
    assert 60 < beacons < 140


@pytest.mark.parametrize("seed", [0, 1, 2])
def test_global_random_state_is_untouched_when_rng_is_injected(seed: int) -> None:
    random.seed(1234)
    expected = random.random()
    random.seed(1234)
    lf.generate_rum_records("svc", 50, NOW, random.Random(seed))
    lf.build_rum_beacon_url("https://x.example.com", random.Random(seed))
    assert random.random() == expected


# ── Merged from test_measure_e2e_freshness.py ──────────────────────────────────────────────────────


def test_load_environment_config_reads_only_explicit_environment_values():
    config = load_environment_config(
        "local-std",
        {
            "FLA_FRESHNESS_LOCAL_STD_SERVICE_ID": "test-service",
            "FLA_FRESHNESS_LOCAL_STD_BACKEND_URL": "http://127.0.0.1:8000",
            "FLA_FRESHNESS_LOCAL_STD_CDN_DOMAIN": "https://cdn.example.test",
        },
    )
    assert config == {
        "service_id": "test-service",
        "backend_url": "http://127.0.0.1:8000",
        "cdn_domain": "https://cdn.example.test",
        "name": "Local Standard",
    }


def test_load_environment_config_rejects_missing_and_invalid_urls():
    with pytest.raises(ValueError, match="FLA_FRESHNESS_REMOTE_HS_CDN_DOMAIN"):
        load_environment_config(
            "remote-hs",
            {
                "FLA_FRESHNESS_REMOTE_HS_SERVICE_ID": "test-service",
                "FLA_FRESHNESS_REMOTE_HS_BACKEND_URL": "http://127.0.0.1:8000",
            },
        )
    with pytest.raises(ValueError, match="absolute HTTP"):
        load_environment_config(
            "local-std",
            {
                "FLA_FRESHNESS_LOCAL_STD_SERVICE_ID": "test-service",
                "FLA_FRESHNESS_LOCAL_STD_BACKEND_URL": "file:///tmp/backend",
                "FLA_FRESHNESS_LOCAL_STD_CDN_DOMAIN": "https://cdn.example.test",
            },
        )


@pytest.mark.parametrize(
    ("service", "expected"),
    [
        (
            {"log_period": 60, "cron_sync": {"polling_mode": "adaptive", "interval_mins": 2}},
            {
                "mode": "adaptive",
                "interval_seconds": 120,
                "interval_source": "interval_mins",
                "adaptive_followup_interval_seconds": 3,
                "adaptive_max_followups": 2,
            },
        ),
        (
            {"log_period": 60, "cron_sync": {"interval_seconds": 0}},
            {
                "mode": "adaptive",
                "interval_seconds": 30,
                "interval_source": "log_period",
                "adaptive_followup_interval_seconds": 3,
                "adaptive_max_followups": 2,
            },
        ),
        (
            {"log_period": 1, "cron_sync": {"polling_mode": "regular", "interval_seconds": 2}},
            {
                "mode": "regular",
                "interval_seconds": 5,
                "interval_source": "interval_seconds",
                "adaptive_followup_interval_seconds": None,
                "adaptive_max_followups": 0,
            },
        ),
    ],
)
def test_configured_polling_reports_scheduler_interval_and_mode(service, expected):
    assert configured_polling({"services": [service | {"service_id": "svc"}]}, "svc") == expected


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({}, "services response"),
        ({"services": []}, "requested service"),
        ({"services": [{"service_id": "svc", "cron_sync": {"polling_mode": "continuous"}}]}, "polling_mode"),
        ({"services": [{"service_id": "svc", "cron_sync": []}]}, "cron_sync"),
    ],
)
def test_configured_polling_rejects_missing_or_invalid_config(payload, message):
    with pytest.raises(ValueError, match=message):
        configured_polling(payload, "svc")


def test_cron_completion_time_and_new_terminal_run_and_seconds_between():
    assert cron_completion_time({"started_at": "2026-09-29T12:00:00Z", "duration_s": 2.5}) == datetime(
        2026, 9, 29, 12, 0, 2, 500000, tzinfo=UTC
    )
    assert cron_completion_time({"started_at": "invalid", "duration_s": 2}) is None
    assert cron_completion_time({"started_at": "2026-09-29T12:00:00Z", "duration_s": "invalid"}) is None
    assert new_terminal_run({"entries": [{"id": 3, "status": "warning"}]}, 2) == {"id": 3, "status": "warning"}
    for entry in (
        [{"id": 3, "status": "running"}],
        [{"id": 2, "status": "success"}],
        [{"id": "invalid", "status": "success"}],
        [],
    ):
        assert new_terminal_run({"entries": entry}, 2) is None
    later = datetime(2026, 9, 29, 12, 0, 0, tzinfo=UTC)
    assert seconds_between(later, datetime(2026, 9, 29, 12, 0, 1, tzinfo=UTC)) == -1.0
    assert seconds_between(later, None) is None


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"beacons": 5}, 5),
        ({"recent_beacons": "8"}, 8),
        ({"beacons": None, "recent_beacons": 3}, 3),
        ({"beacons": "invalid"}, None),
        ("invalid", None),
    ],
)
def test_rum_beacon_count_supports_standard_and_high_scale_responses(response, expected):
    assert rum_beacon_count(response) == expected


def test_compute_lag_statistics_empty_and_percentiles_and_shared_budget(monkeypatch):
    empty = compute_lag_statistics([])
    assert empty["sample_count"] == 0 and empty["target_status"] == "UNMEASURED"
    assert empty["header_lag"] == {"min": None, "max": None, "avg": None, "p95": None, "p99": None}
    assert empty["dashboard_lag"] == {"min": None, "max": None, "avg": None, "p95": None, "p99": None}

    pct = compute_lag_statistics(
        [{"elapsed_s": 6, "header_lag_s": lag, "dashboard_lag_s": lag} for lag in range(1, 21)]
    )
    assert pct["header_lag"]["p95"] == 19 and pct["header_lag"]["p99"] == 20
    assert pct["dashboard_lag"]["p95"] == 19 and pct["dashboard_lag"]["p99"] == 20

    monkeypatch.setitem(BUDGETS, "peak_freshness_p95_ms", 25000)
    stats = compute_lag_statistics([{"elapsed_s": 6, "header_lag_s": 25.0, "dashboard_lag_s": 25.0}])
    assert stats["target_p95_s"] == 25 and stats["target_status"] == "WITHIN TARGET"


@pytest.mark.parametrize(
    ("header_lag", "dashboard_lag", "expected"),
    [(30.2, 10.0, "ELEVATED"), (10.0, 30.2, "ELEVATED"), (20.0, 20.0, "WITHIN TARGET"), (20.04, 20.04, "ELEVATED")],
)
def test_compute_lag_statistics_assesses_both_p95s_not_the_mean(header_lag, dashboard_lag, expected):
    stats = compute_lag_statistics(
        [
            {"elapsed_s": 6, "header_lag_s": 1.0, "dashboard_lag_s": 1.0},
            {"elapsed_s": 8, "header_lag_s": header_lag, "dashboard_lag_s": dashboard_lag},
        ]
    )
    assert stats["header_lag"]["avg"] < 20 and stats["dashboard_lag"]["avg"] < 20 and stats["target_status"] == expected


@pytest.mark.parametrize(("header_lag", "dashboard_lag"), [(None, 10), (10, None)])
def test_compute_lag_statistics_does_not_pass_an_unmeasured_surface(header_lag, dashboard_lag):
    stats = compute_lag_statistics([{"elapsed_s": 6, "header_lag_s": header_lag, "dashboard_lag_s": dashboard_lag}])
    assert stats["target_status"] == "UNMEASURED"


def test_compute_lag_statistics_filters_pre_traffic_and_computes_stats():
    t0 = datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC)
    samples = [
        {"elapsed_s": 2.0, "header_latest_ts": t0, "header_lag_s": 65.0, "dashboard_lag_s": 70.0},
        {
            "elapsed_s": 4.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 10, tzinfo=UTC),
            "header_lag_s": 10.0,
            "dashboard_lag_s": 12.0,
        },
        {
            "elapsed_s": 6.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 15, tzinfo=UTC),
            "header_lag_s": 15.0,
            "dashboard_lag_s": 16.0,
        },
        {
            "elapsed_s": 8.0,
            "header_latest_ts": datetime(2026, 10, 9, 12, 0, 20, tzinfo=UTC),
            "header_lag_s": 20.0,
            "dashboard_lag_s": 22.0,
        },
    ]
    stats = compute_lag_statistics(samples, baseline_latest_ts=t0)
    assert stats["sample_count"] == 3 and stats["total_samples"] == 4
    assert (
        stats["header_lag"]["min"],
        stats["header_lag"]["max"],
        stats["header_lag"]["avg"],
        stats["header_lag"]["p95"],
    ) == (10.0, 20.0, 15.0, 20.0)
    assert (
        stats["dashboard_lag"]["min"],
        stats["dashboard_lag"]["max"],
        stats["dashboard_lag"]["avg"],
        stats["dashboard_lag"]["p95"],
    ) == (12.0, 22.0, 16.7, 22.0)


def test_correlated_vitals_keys_normalise_and_reject_invalid_rows(monkeypatch):
    probe = {"key": "10000001", "sent": 0.0}
    rows = [{"probe_key": 10000001.0, "timestamp": "2026-10-22T00:00:00Z", "matches": 1}]
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (200, {"data": rows, "truncated": False}))
    result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "vitals", [probe])
    assert result["error"] is None and "first_visible" in probe

    req_probe = {"key": "/?probe=owned&seq=0", "sent": 0.0}
    for bad_rows in (
        [{"probe_key": "/unrelated", "timestamp": "2026-10-22T00:00:00Z", "matches": 1}],
        [{"probe_key": req_probe["key"], "timestamp": "2026-10-22T00:00:00Z", "matches": 2}],
        [{"probe_key": req_probe["key"], "timestamp": None, "matches": 1}],
    ):
        monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (200, {"data": bad_rows, "truncated": False}))
        res = freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [req_probe])
        assert res["error"] == "invalid_or_ambiguous_correlation"
        assert "first_visible" not in req_probe and "last_absent" not in req_probe


def test_correlated_sender_separates_payloads_and_never_supplies_request_id(monkeypatch):
    requests = []

    class Response:
        status = 204

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    class Opener:
        def open(self, request, **kwargs):
            requests.append(request)
            return Response()

    def build(handler):
        assert handler.redirect_request(None, None, 302, "", {}, "https://unapproved.example.test") is None
        return Opener()

    monkeypatch.setattr(freshness.urllib.request, "build_opener", build)
    for stream in ("request", "vitals", "errors"):
        assert freshness.send_correlated_probe("https://edge.example.test/", stream) == 204
    assert requests[0].data is None
    assert "measurements" in json.loads(requests[1].data) and "exceptions" not in json.loads(requests[1].data)
    assert "exceptions" in json.loads(requests[2].data) and "measurements" not in json.loads(requests[2].data)
    assert all(not req.has_header("Fastly-request-id") for req in requests)


@pytest.mark.parametrize(("status", "payload"), [(503, {}), (0, "private failure"), (200, {"data": []})])
def test_correlated_query_failure_is_not_negative_visibility(monkeypatch, status, payload):
    probe = {"key": "10000001", "sent": 0.0}
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: (status, payload))
    result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "vitals", [probe])
    assert result["error"] == "query_failed_or_incomplete"
    assert "last_absent" not in probe and "first_visible" not in probe and "private failure" not in str(result)


def test_correlated_query_uses_exact_safe_literals_and_separate_datasets(monkeypatch):
    calls = []
    monkeypatch.setattr(
        freshness,
        "http_post",
        lambda url, body, headers, **kw: (calls.append(body), (200, {"data": [], "truncated": False}))[1],
    )
    for high_scale in (False, True):
        for stream in ("request", "vitals", "errors"):
            key = "10000001" if stream == "vitals" else "/?customer=O'Brien&probe=owned"
            probe = {"key": key, "sent": 0.0}
            result = freshness.poll_correlated_stream("https://admin.example.test", "svc", stream, [probe], high_scale)
            assert result["error"] is None and "last_absent" in probe
            body = calls[-1]
            column = {"request": "url", "vitals": "metric_value", "errors": "error_message"}[stream]
            literal = "10000001" if stream == "vitals" else "'/?customer=O''Brien&probe=owned'"
            assert f"{column} IN ({literal})" in body["sql"]
            assert "cid" not in body["sql"] and "client_id" not in body["sql"]
            assert body["dataset"] == {"request": "logs", "vitals": "client_vitals", "errors": "client_errors"}[stream]
            assert "LIMIT 100" in body["sql"] and "INTERVAL 1 HOUR" in body["sql"] and body["max_rows"] == 100
            assert ("metric_name = 'LCP'" in body["sql"]) == (stream == "vitals")


def test_correlated_upper_bound_and_censoring_and_population_floor(monkeypatch):
    times = iter([112.0, 115.0, 130.0, 136.0])
    monkeypatch.setattr(freshness.time, "monotonic", lambda: next(times))
    source_time = "2099-01-01T00:00:00Z"
    probe = {"stream": "request", "key": "owned", "sent": 100.0, "response": 101.0, "http_status": 200}
    responses = iter(
        [
            (200, {"data": [], "truncated": False}),
            (200, {"data": [{"probe_key": "owned", "timestamp": source_time, "matches": 1}], "truncated": False}),
        ]
    )
    monkeypatch.setattr(freshness, "http_post", lambda *a, **kw: next(responses))
    freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [probe])
    freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", [probe])
    report = freshness.summarize_correlated([probe], ended=140)
    sample = report["request"]["samples"][0]
    assert sample["event_to_queryable_upper_bound_s"] == 36 and sample["send_to_queryable_interval_s"] == [12, 36]
    assert sample["source_timestamp"] == source_time and sample["left_censored"] is False
    assert report["request"]["target_assessment"] == "incomplete_not_proven_failure"
    assert report["vitals"]["observed"] == 0 and report["errors"]["complete"] is False

    bad_probes = [
        {"stream": "vitals", "key": "quiet", "sent": 1, "response": 2, "http_status": 204},
        {"stream": "errors", "key": "bad", "sent": 3, "response": 4, "http_status": 503, "first_visible": 5},
    ]
    bad_rep = freshness.summarize_correlated(bad_probes, ended=30)
    assert bad_rep["vitals"]["right_censored"] == 1 and bad_rep["vitals"]["upper_bound_s"]["p95"] is None
    assert bad_rep["vitals"]["samples"][0]["send_to_queryable_interval_s"] == [0, None]
    assert bad_rep["vitals"]["samples"][0]["followup_duration_s"] == 29
    assert (
        bad_rep["errors"]["http_errors"] == 1
        and bad_rep["errors"]["observed"] == 0
        and bad_rep["errors"]["complete"] is False
    )

    def pop_report(n):
        ps = [
            {"stream": "request", "key": str(i), "sent": 0, "response": 1, "http_status": 200, "first_visible": 15}
            for i in range(n)
        ]
        return freshness.summarize_correlated(ps, ended=20)["request"]

    assert pop_report(29)["p95_population_sufficient"] is False
    sufficient = pop_report(30)
    assert sufficient["p95_population_sufficient"] is True and sufficient["p99_population_sufficient"] is False
    assert sufficient["left_censored"] == 30 and sufficient["target_assessment"] == "within_target_upper_bound"


def test_correlated_sql_finds_only_exact_recent_events(monkeypatch):
    key = "/approved/O'Brien?probe=owned&seq=0"
    with duckdb.connect(":memory:") as connection:
        connection.execute("CREATE TABLE logs(url VARCHAR, timestamp TIMESTAMPTZ)")
        connection.execute(
            "INSERT INTO logs VALUES (?, CURRENT_TIMESTAMP), (?, CURRENT_TIMESTAMP), (?, CURRENT_TIMESTAMP - INTERVAL 2 HOUR)",
            [key, key + "unrelated", "/old?probe=owned"],
        )

        def post(url, body, headers, **kwargs):
            result = connection.execute(body["sql"])
            columns = [col[0] for col in result.description]
            rows = [dict(zip(columns, row, strict=True)) for row in result.fetchall()]
            for row in rows:
                row["timestamp"] = row["timestamp"].isoformat()
            return 200, {"data": rows, "truncated": False}

        monkeypatch.setattr(freshness, "http_post", post)
        probes = [
            {"stream": "request", "key": k, "sent": 0, "response": 1, "http_status": 200}
            for k in (key, "/old?probe=owned", "/absent?probe=owned")
        ]
        result = freshness.poll_correlated_stream("https://admin.example.test", "svc", "request", probes)
        assert result["error"] is None and "first_visible" in probes[0]
        assert all("first_visible" not in p and "last_absent" in p for p in probes[1:])


def test_correlated_runner_uses_approved_path_and_separate_paced_beacons(monkeypatch):
    now = [100.0]
    sends, polls = [], []
    monkeypatch.setattr(freshness.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(freshness.time, "sleep", lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.setattr(
        freshness, "http_get", lambda *a, **kw: (200, {"services": [{"service_id": "svc", "rum_enabled": True}]})
    )
    monkeypatch.setattr(
        freshness,
        "send_correlated_probe",
        lambda url, stream, value=None: (
            sends.append((now[0], stream, url, value)),
            204 if stream != "request" else 200,
        )[1],
    )
    monkeypatch.setattr(
        freshness,
        "http_post",
        lambda url, body, headers, **kw: (
            polls.append((now[0], body["dataset"])),
            (200, {"data": [], "truncated": False}),
        )[1],
    )

    report = freshness.measure_correlated_freshness(
        {"service_id": "svc", "backend_url": "https://admin.example.test"},
        "https://edge.example.test/?existing=keep",
        num_requests=1,
        num_rum=1,
        num_errors=1,
        max_wait_s=25,
    )
    assert [s[1] for s in sends] == ["request", "vitals", "errors"]
    assert all(b[0] - a[0] >= 1 for a, b in zip(sends, sends[1:]))
    request_url = urlparse(sends[0][2])
    assert request_url.path == "/" and parse_qs(request_url.query)["existing"] == ["keep"]
    assert parse_qs(sends[1][2].split("?", 1)[1])["cid"] != parse_qs(sends[2][2].split("?", 1)[1])["cid"]
    vitals_qs = parse_qs(sends[1][2].split("?", 1)[1])
    assert vitals_qs["rum_metric_value"] == [sends[1][3]] and int(sends[1][3]) >= 10_000_000
    errors_qs = parse_qs(sends[2][2].split("?", 1)[1])
    assert errors_qs["rum_error_message"] == [sends[2][3]] and sends[2][3].startswith("fla-probe-")
    assert sends[0][3] is None
    for dataset in ("logs", "client_vitals", "client_errors"):
        times = [t for t, d in polls if d == dataset]
        assert all(b - a >= 10 for a, b in zip(times, times[1:]))
    assert report["qualification"] == "incomplete"
    assert {"peak_conditions_not_verified", "request_population_incomplete_or_below_floor"} <= set(
        report["qualification_gaps"]
    )
    assert report["measurement_contract"]["event_clock_point_latency_s"] is None
    assert report["streams"]["request"]["right_censored"] == 1 and "probe=" not in json.dumps(report)


@pytest.mark.parametrize("kwargs", [{"poll_interval_s": 9}, {"num_requests": 101}, {"max_wait_s": 601}])
def test_correlated_runner_rejects_unsafe_limits_before_io(monkeypatch, kwargs):
    monkeypatch.setattr(
        freshness,
        "http_get",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("unsafe configuration reached I/O")),
    )
    with pytest.raises(ValueError):
        freshness.measure_correlated_freshness(
            {"service_id": "svc", "backend_url": "https://admin.example.test"}, "https://edge.example.test/", **kwargs
        )


# ── Merged from test_scale_harness.py ──────────────────────────────────────────────────────


def test_parse_stages_and_high_rate_gate() -> None:
    stages = parse_stages("100:10,500:2")
    assert [(stage.target_rps, stage.duration_s) for stage in stages] == [(100, 10), (500, 2)]
    with pytest.raises(ValueError, match="allow-high-rate"):
        parse_stages("2500:10")
    assert parse_stages("2500:10", allow_high_rate=True)[0].target_rps == 2500


def test_rum_urls_and_freshness_lag_and_render_report() -> None:
    url = build_request_url("https://logs.example.com", 1.0)
    query = parse_qs(urlparse(url).query)
    assert query["cid"][0] == query["rum_cid"][0]
    assert query["req"][0].startswith("req-") and query["req"][0] != query["cid"][0]

    now = datetime(2026, 9, 6, 17, 42, 35, tzinfo=UTC)
    assert freshness_lag_seconds("2026-09-06T17:32:08Z", now) == 627.0 and freshness_lag_seconds(None, now) is None

    report = {
        "started_at": "2026-09-06T17:00:00+00:00",
        "stages": [
            {
                "index": 1,
                "result": {
                    "target_rps": 100,
                    "achieved_rps": 98,
                    "completed": 10,
                    "successful": 10,
                    "latency_ms": {"p95": 20},
                },
                "checkpoint": {},
            }
        ],
    }
    assert "| 1 | 100 | 98.00 | 10 | 10 | 20 | - |" in render_report(report)


def _cmcd_profile(rng: random.Random) -> TrafficProfile:
    return TrafficProfile(cmcd_ratio=1.0, sessions=build_cmcd_sessions(rng, 5))


def test_cmcd_requests_headers_startup_flag_v2_and_ratio() -> None:
    rng = random.Random(7)
    url, headers = build_request("https://logs.example.com", 0.0, rng, _cmcd_profile(rng))
    parsed = urlparse(url)
    assert parsed.path.startswith("/video/")
    cmcd = parse_qs(parsed.query)["CMCD"][0].split(",")
    assert {"CMCD-Object", "CMCD-Request", "CMCD-Session", "CMCD-Status"} <= headers.keys()
    for header in ("CMCD-Object", "CMCD-Request", "CMCD-Session", "CMCD-Status"):
        assert set(headers[header].split(",")) <= set(cmcd)
    assert any(field.startswith("sid=") for field in cmcd) and any(field.startswith("ot=") for field in cmcd)

    rng3 = random.Random(3)
    session = build_cmcd_sessions(rng3, 1)[0]
    session.version = 1
    first = build_cmcd_request(session, rng3)[1]["CMCD-Request"].split(",")
    second = build_cmcd_request(session, rng3)[1]["CMCD-Request"].split(",")
    assert "su" in first and "su" not in second

    rng_v2 = random.Random(3)
    session_v2 = build_cmcd_sessions(rng_v2, 1)[0]
    session_v2.version = 2
    assert "su=?1" in build_cmcd_request(session_v2, rng_v2)[1]["CMCD-Request"].split(",")

    rng11 = random.Random(11)
    profile = TrafficProfile(cmcd_ratio=0.5, sessions=build_cmcd_sessions(rng11, 5))
    cmcd_hits = sum("CMCD" in build_request("https://x.example.com", 0.0, rng11, profile)[0] for _ in range(400))
    assert 120 < cmcd_hits < 280


def test_default_profile_diversify_and_rum_beacon_isolation() -> None:
    assert build_request("https://x.example.com", 0.0, random.Random(1), None) == ("https://x.example.com", {})
    assert build_request("https://x.example.com", 0.0, random.Random(1), TrafficProfile()) == (
        "https://x.example.com",
        {},
    )

    rng = random.Random(5)
    results = [build_request("https://x.example.com", 0.0, rng, TrafficProfile(diversify=True)) for _ in range(300)]
    assert len({urlparse(url).path for url, _ in results}) > 5
    assert any(ua in BOT_USER_AGENTS for ua in (headers["User-Agent"] for _, headers in results))

    rng2 = random.Random(2)
    profile = TrafficProfile(cmcd_ratio=1.0, diversify=True, sessions=build_cmcd_sessions(rng2, 3))
    url, headers = build_request("https://x.example.com", 1.0, rng2, profile)
    assert urlparse(url).path == "/rum-beacon" and headers == {}


# ── Merged from test_verify_dashboard*.py ──────────────────────────────────────────────────────

_VERIFY_JS = Path(__file__).resolve().parents[2] / "scripts" / "verify_dashboard.js"
_CLASSIFIER_JS = Path(__file__).resolve().parents[2] / "scripts" / "lib" / "console_blip.js"
_MIN_RECENT_MINUTES = 10


def _src() -> str:
    return _VERIFY_JS.read_text()


def _classifier_src() -> str:
    return _CLASSIFIER_JS.read_text()


def test_verify_dashboard_logging_and_navigation_resilience_contracts() -> None:
    src = _src()
    assert re.search(r"function ts\(\)", src) and "`${ts()} [Browser Console]" in src
    assert "requestfailed" in src and "gotoWithShellReady" in src
    m_attempts = re.search(r"attempts\s*=\s*(\d+)", src)
    assert m_attempts and 2 <= int(m_attempts.group(1)) <= 6
    assert "gotoWithShellReady(rumPage, rumUrl30d" in src and not re.search(r"rumPage\.goto\(rumUrl30d", src)
    assert "gotoWithShellReady(networkPage, networkUrl30d" in src or (
        "gotoWithShellReady(routePage, targetUrl" in src and "routePath: '/network'" in src
    )
    assert not re.search(r"networkPage\.goto\(networkUrl30d", src)
    m_settle = re.search(r"RUM_RENDER_SETTLE_MS\s*=\s*(\d+)", src)
    assert m_settle and int(m_settle.group(1)) >= 8000
    assert "RUM_RENDER_SETTLE_MS" in src and src.count("waitForFunction") >= 1
    assert not re.search(r"rumPage\.waitForTimeout\(4000\)", src)


def test_verify_dashboard_recency_window_contracts() -> None:
    src = _src()
    m = re.search(r'RECENT_RANGE\s*=\s*"(\d+m)"', src)
    assert m, 'verify_dashboard.js must define a RECENT_RANGE = "<n>m" constant'
    assert int(m.group(1)[:-1]) >= _MIN_RECENT_MINUTES
    assert len(re.findall(r"getRecentWindow\([^,\n]+,\s*RECENT_RANGE\b", src)) >= 2
    assert not re.findall(r'(?:getRecentWindow\([^,\n]+,|"range",)\s*"(\d+m)"', src)
    assert not re.search(r"\d+\s*-\s*\d+\s*min.*floor|floor.*\d+\s*-\s*\d+\s*min", src)


def test_verify_dashboard_resource_classification_and_transient_tolerance() -> None:
    src, classifier = _src(), _classifier_src()
    assert re.search(r"GENERIC_RESOURCE_FAILURE_RE", src) and "function isCriticalResourceUrl" in src
    assert re.search(r"requestfailed.*?isCriticalResourceUrl", src, re.S)
    assert "text.includes('woff2')" not in src and "/api/web-vitals" in src and "/api/ux-events" in src

    m = re.search(r"TRANSIENT_ERROR_BUDGET\s*=\s*(\d+)", src)
    assert m and 1 <= int(m.group(1)) <= 20
    assert re.search(r"status\s*===\s*503", src) and "ERR_CONNECTION_REFUSED" in src
    assert not re.search(r"status\s*>=\s*400[^\n]*transient\s*=\s*true", src)
    assert "ERR_FAILED" in classifier and ("ChunkLoadError" in classifier or "Failed to load chunk" in classifier)
    assert "require('./lib/console_blip')" in src or 'require("./lib/console_blip")' in src


@pytest.mark.parametrize(
    ("charts", "expected"),
    [
        ([], False),
        ([{"width": 524, "height": 300, "data": [{"x": [], "y": []}]}], False),
        ([{"width": 1, "height": 1, "data": [{"x": [0], "y": [0]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [0]}]}], True),
        ([{"width": 524, "height": 300, "data": [{"values": [10]}]}], True),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [None]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": ["10"]}]}], False),
        ([{"width": 524, "height": 300, "data": [{"x": ["2026-01-01"], "y": [float("nan")]}]}], False),
    ],
)
def test_rum_chart_requires_real_geometry_and_points(charts: list[dict], expected: bool) -> None:
    source = _src()
    start = source.index("function hasPopulatedRumChart()")
    end = source.index("\n}", start) + 2
    script = (
        source[start:end]
        + f"\nconst charts = {json.dumps(charts)};"
        + "\nglobal.document = {querySelectorAll: selector => {"
        + "if (selector !== 'main .js-plotly-plot') throw Error('Unscoped chart selector');"
        + "return charts.map(c => ({data:c.data,getBoundingClientRect:()=>c}));}};"
        + "\nconsole.log(JSON.stringify(hasPopulatedRumChart()));"
    )
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, check=True)
    assert json.loads(result.stdout) is expected


def test_remote_rum_chart_failure_cannot_warn_and_proceed() -> None:
    source = _src()
    assert "waitForFunction(hasPopulatedRumChart, null, { timeout: 90000 })" in source
    assert "No populated full-sized RUM chart" in source
    assert "No visible Plotly charts found on the RUM page for remote cloud environment" not in source


def _verify_node(script: str) -> dict:
    result = subprocess.run(["node", "-e", script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _frontend_node_runtime() -> str:
    return r"""
const frontendFs = require('node:fs'), frontendPath = require('node:path'), frontendVm = require('node:vm');
const frontendRoot = frontendPath.resolve('frontend');
const frontendRequire = require('node:module').createRequire(frontendPath.join(frontendRoot, 'package.json'));
const ts = frontendRequire('typescript'), modules = new Map();
let now = Date.parse('2026-10-24T23:39:18.378Z');
class Clock extends Date {
  constructor(...args) { super(...(args.length ? args : [now])); }
  static now() { return now; }
}
const frontendWindow = {
  location: {}, history: {replaceState(_state, _title, href) {
    frontendWindow.location = {href, search: new URL(href).search};
  }},
};
function loadFrontend(relativePath) {
  if (relativePath === 'lib/date.ts' || relativePath === 'lib/format.ts') relativePath = 'lib/utils.ts';
  const filename = frontendPath.resolve(frontendRoot, relativePath);
  if (modules.has(filename)) return modules.get(filename).exports;
  const module = {exports: {}};
  modules.set(filename, module);
  const code = ts.transpileModule(frontendFs.readFileSync(filename, 'utf8'), {
    compilerOptions: {module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022}, fileName: filename,
  }).outputText;
  frontendVm.runInNewContext(code, {
    module, exports: module.exports, Date: Clock, window: frontendWindow, URL, URLSearchParams, console,
    require: name => name.startsWith('@/') ? loadFrontend(name.slice(2) + '.ts') : frontendRequire(name),
  }, {filename});
  return module.exports;
}
function resolveNavigation(href) {
  frontendWindow.location = {href, search: new URL(href).search};
  const {useFilterStore} = loadFrontend('stores/filterStore.ts');
  const utilsMod = loadFrontend('lib/utils.ts');
  useFilterStore.getState().resetAll();
  utilsMod._resetUrlHydrationFlag();
  utilsMod.hydrateFilterStoreFromUrl();
  const state = useFilterStore.getState();
  return utilsMod.resolveRangeWire({...state, anchor: state.endTime});
}
"""


@pytest.mark.parametrize("stream", ["REQUEST", "RUM"])
@pytest.mark.parametrize(
    ("age", "fresh"),
    [
        ("0s ago", True),
        ("59s ago", True),
        ("1m 0s ago", True),
        ("14m 59s ago", True),
        ("15m 0s ago", False),
        ("6h 26m ago", False),
        ("6h 26m 1s ago", False),
        ("1d 0h 0m ago", False),
        ("Never", False),
        ("—", False),
        ("-", False),
        ("", False),
        ("just now", False),
        ("tomorrow", False),
        ("ago", False),
    ],
)
def test_verify_header_requires_bounded_event_age(stream: str, age: str, fresh: bool) -> None:
    source = _src()
    helpers = source[source.index("function assertRecentHeader(") : source.index("function isRecentRequestResponse(")]
    header = f"{stream}\nlatest:\n{age}\ntotal: 999999"
    result = _verify_node(
        'const RECENT_RANGE = "15m"; const messages = []; const console = {log: msg => messages.push(msg)};\n'
        + helpers
        + f"\nlet fresh = true; try {{ assertRecentHeader({json.dumps(header)}, {json.dumps(stream)}); }}"
        + " catch { fresh = false; } process.stdout.write(JSON.stringify({fresh, messages}));"
    )
    assert result["fresh"] is fresh and bool(result["messages"]) is fresh
    assert not any("updating!" in message for message in result["messages"])


@pytest.mark.parametrize("stream", ["REQUEST", "RUM"])
@pytest.mark.parametrize(
    ("age_seconds", "expected_age", "fresh"),
    [
        (0, "0s ago", True),
        (59, "59s ago", True),
        (60, "1m 0s ago", True),
        (899, "14m 59s ago", True),
        (900, "15m 0s ago", False),
        (3600, "1h 0m 0s ago", False),
        (23161, "6h 26m 1s ago", False),
        (86400, "1d 0h 0m ago", False),
        (-1, "just now", False),
    ],
)
def test_verify_header_accepts_real_ui_duration_shapes(
    stream: str, age_seconds: int, expected_age: str, fresh: bool
) -> None:
    source = _src()
    helper = source[source.index("function assertRecentHeader(") : source.index("function getRecentWindow(")]
    result = _verify_node(
        _frontend_node_runtime()
        + '\nconst RECENT_RANGE = "15m";\n'
        + helper
        + f"\nconst age = loadFrontend('lib/date.ts').formatTimeAgo(new Clock(now - {age_seconds} * 1000));"
        + "\nlet fresh = true; const originalLog = console.log; console.log = () => {};"
        + f"\ntry {{ assertRecentHeader({json.dumps(stream)} + '\\nlatest:\\n' + age, {json.dumps(stream)}); }}"
        + " catch { fresh = false; } console.log = originalLog; process.stdout.write(JSON.stringify({age, fresh}));"
    )
    assert result == {"age": expected_age, "fresh": fresh}


@pytest.mark.parametrize("route", ["/dashboard", "/rum"])
def test_verify_absolute_url_resolves_to_exact_frontend_request(route: str) -> None:
    source = _src()
    helper = source[source.index("function getRecentWindow(") : source.index("function hasRecentWindow(")]
    result = _verify_node(
        _frontend_node_runtime()
        + "\nconst recentWindow = frontendVm.runInNewContext("
        + json.dumps(
            helper
            + f"\ngetRecentWindow('https://test.invalid{route}?range=24h&start_time=old&end_time=old&view=kept', '15m')"
        )
        + ", {Date: Clock, URL}); const wire = resolveNavigation(recentWindow.url);"
        + "\nprocess.stdout.write(JSON.stringify({recentWindow, wire}));"
    )
    window, wire = result["recentWindow"], result["wire"]
    assert parse_qs(urlparse(window["url"]).query) == {
        "start_time": ["2026-10-24T23:24:18.378Z"],
        "end_time": ["2026-10-24T23:39:18.378Z"],
        "view": ["kept"],
    }
    assert wire["rangeToken"] is None
    assert wire["rangeBody"] == {"start_time": window["start_time"], "end_time": window["end_time"]}
    assert wire["rangeKey"] == f"abs:{window['start_time']}|{window['end_time']}"


@pytest.mark.parametrize(
    ("overrides", "accepted"),
    [
        ({}, True),
        ({"path": "/api/dashboard/aggregates"}, True),
        ({"range": "24h"}, False),
        ({"range": "15m"}, False),
        ({"range": "auto"}, False),
        ({"start": None}, False),
        ({"end": None}, False),
        ({"start": "invalid"}, False),
        ({"start": "2020-01-01T00:00:00Z"}, False),
        ({"end": "2099-01-01T00:00:00Z"}, False),
        ({"offset": 86400000}, False),
        ({"duration": 86400000}, False),
        ({"offset": 1001}, False),
        ({"expected": None}, False),
        ({"path": "/api/admin/sync-status"}, False),
        ({"origin": "https://other.invalid"}, False),
        ({"method": "GET"}, False),
        ({"ok": False}, False),
    ],
)
def test_verify_request_evidence_requires_actual_recent_window(overrides: dict, accepted: bool) -> None:
    source = _src()
    helpers = source[source.index("function hasRecentWindow(") : source.index("// Helper to append")]
    result = _verify_node(
        'const RECENT_RANGE = "15m"; const baseUrl = "https://test.invalid/dashboard";\n'
        + helpers
        + f"\nconst opts = Object.assign({{path: '/api/dashboard/bundle', origin: 'https://test.invalid', range: null, duration: 900000, offset: 1000, method: 'POST', ok: true}}, {json.dumps(overrides)});"
        + """
const selectedEnd = Date.now() - 1000;
const expectedWindow = Object.hasOwn(opts, 'expected') ? opts.expected : {
  start_time: new Date(selectedEnd - 900000).toISOString(), end_time: new Date(selectedEnd).toISOString(),
};
const end = selectedEnd + 1000 - opts.offset;
const body = {
  range_token: opts.range,
  start_time: Object.hasOwn(opts, 'start') ? opts.start : new Date(end - opts.duration).toISOString(),
  end_time: Object.hasOwn(opts, 'end') ? opts.end : new Date(end).toISOString(),
};
const response = {
  url: () => opts.origin + opts.path, ok: () => opts.ok,
  request: () => ({method: () => opts.method, postDataJSON: () => body}),
};
const query = new URLSearchParams(Object.entries(body).filter(([, value]) => value !== null));
const rumResponse = {
  ...response, url: () => opts.origin + '/api/services/test/rum/analytics?' + query,
  request: () => ({method: () => opts.method === 'POST' ? 'GET' : 'POST'}),
};
process.stdout.write(JSON.stringify({
  accepted: isRecentRequestResponse(response, expectedWindow), rumAccepted: isRecentRumResponse(rumResponse, expectedWindow),
}));
"""
    )
    assert result["accepted"] is accepted
    if "path" not in overrides:
        assert result["rumAccepted"] is accepted


@pytest.mark.parametrize("count", [0, 149, 150, 999, None, "999", -1, 1.5, True])
def test_verify_window_count_never_uses_lifetime_total(count: object) -> None:
    source = _src()
    helpers = source[source.index("async function recentRequestCount(") : source.index("// Helper to append")]
    result = _verify_node(
        helpers
        + f"\n(async () => {{ const count = {json.dumps(count)};"
        + """
const values = await Promise.all([
  recentRequestCount(null),
  recentRequestCount({url: () => 'https://test.invalid/api/dashboard/bundle',
    json: async () => ({total_rows: 999999, aggregates: {total_rows: count}})}),
  recentRequestCount({url: () => 'https://test.invalid/api/dashboard/aggregates',
    json: async () => ({total_rows: count})}),
]);
process.stdout.write(JSON.stringify({values}));
})();
"""
    )
    expected = count if type(count) is int and count >= 0 else None
    assert result["values"] == [None, expected, expected]


@pytest.mark.parametrize(
    ("flags", "request_count", "beacons", "age", "charts", "exit_code"),
    [
        (["--no-traffic"], None, 0, "6h 26m ago", True, 0),
        (["--no-traffic"], None, 0, "Never", False, 1),
        (["--no-traffic"], None, 0, "6h 26m ago", "/rum", 1),
        (["--no-traffic"], None, 0, "6h 26m ago", "/streaming", 1),
        ([], None, 100, "1s ago", True, 1),
        ([], 149, 100, "1s ago", True, 1),
        ([], 150, 49, "1s ago", True, 1),
        ([], 150, 50, "6h 26m ago", True, 1),
        ([], 150, 50, {"REQUEST": "1s ago", "RUM": "6h 26m ago"}, True, 1),
        ([], 150, 50, "1s ago", True, 0),
        ([], [149, 150], [49, 50], "1s ago", True, 0),
        (["--no-trafic"], 150, 50, "1s ago", True, 1),
    ],
)
def test_verify_script_modes_without_browser_or_network(
    flags: list[str],
    request_count: int | list[int] | None,
    beacons: int | list[int],
    age: str | dict,
    charts: bool | str,
    exit_code: int,
) -> None:
    options = {"flags": flags, "requestCount": request_count, "beacons": beacons, "age": age, "charts": charts}
    result = _verify_node(
        _frontend_node_runtime()
        + f"\nconst vm = require('node:vm'); const source = {json.dumps(_src())}; const opts = {json.dumps(options)};"
        + r"""
const visits = [], logs = [], resolvedRequests = [], attempts = {};
const chart = {data: [{y: [100]}], getBoundingClientRect: () => ({width: 500, height: 300})};
let currentUrl = '', pendingResponse = null, displayedBeacons = 1000;
const hasCharts = () => opts.charts !== false && (typeof opts.charts !== 'string' || new URL(currentUrl).pathname !== opts.charts);
const document = {
  body: {get innerText() {
    const age = stream => typeof opts.age === 'string' ? opts.age : opts.age[stream];
    return `REQUEST\nlatest:\n${age('REQUEST')}\ntotal: 999,999\nRUM\nlatest:\n${age('RUM')}\ntotal: 999,999\nTOTAL BEACONS\n${displayedBeacons}\n`
      + 'Largest Contentful Paint GOOD Global Health Total Sessions Traffic Quality Breakdown Origin Error Rate Slowest URLs';
  }},
  querySelector: selector => selector === 'footer' ? {innerText: 'commit:test standard local'} : hasCharts() ? chart : null,
  querySelectorAll: () => hasCharts() ? [chart] : [],
};
const page = {
  on() {}, async goto(url) {
    currentUrl = url; visits.push(url); now += 1000; displayedBeacons = 1000;
    if (pendingResponse) {
      const pending = pendingResponse; pendingResponse = null;
      const wire = resolveNavigation(url), body = wire.rangeBody, rum = new URL(url).pathname === '/rum';
      const key = rum ? 'rum' : 'dashboard', attempt = attempts[key] || 0; attempts[key] = attempt + 1;
      const counts = rum ? opts.beacons : opts.requestCount;
      const count = Array.isArray(counts) ? counts[Math.min(attempt, counts.length - 1)] : counts;
      if (rum) displayedBeacons = count;
      const response = {
        url: () => 'https://test.invalid' + (rum ? '/api/services/test/rum/analytics?' + new URLSearchParams(body) : '/api/dashboard/bundle'),
        ok: () => true, request: () => ({method: () => rum ? 'GET' : 'POST', postDataJSON: () => body}),
        json: async () => rum ? {beacon_count: count} : {aggregates: {total_rows: count}},
      };
      const accepted = pending.predicate(response);
      resolvedRequests.push({navigation: url, body, rangeToken: wire.rangeToken, accepted});
      if (count !== null && accepted) pending.resolve(response);
      else pending.reject(Error('no window evidence'));
    }
    return {ok: () => true};
  },
  async waitForSelector() {}, async waitForTimeout(ms) { now += ms; },
  async waitForFunction(fn) { if (!fn()) throw Error('predicate not satisfied'); },
  waitForResponse(predicate) { return new Promise((resolve, reject) => { pendingResponse = {predicate, resolve, reject}; }); },
  locator: () => ({first: () => ({waitFor: async () => { if (!hasCharts()) throw Error('no chart'); }})}),
  async evaluate(fn) { return fn.toString().includes('performance.getEntries') ? {} : fn(); },
  async close() {}, async screenshot() {},
};
const context = {newPage: async () => page, close: async () => {}, request: {get: async () => ({ok: () => false})}};
const browser = {newContext: async () => context, close: async () => {}};
const dependencies = {
  playwright: {chromium: {launch: async () => browser}}, path: require('node:path'), child_process: {},
  './lib/admin_gateway_client.cjs': {adminGatewayContextOptions: () => ({})},
  './lib/section_measurements.cjs': {collectResponseMeasurements: () => ({snapshot: async () => [], gaps: () => [], close() {}}), budgets: {}},
  './lib/console_blip': {isTransientConsoleBlip: () => false},
};
new Promise(resolve => {
  const execution = vm.runInNewContext(source, {
    require(name) { if (!(name in dependencies)) throw Error('unexpected dependency: ' + name); return dependencies[name]; },
    process: {argv: ['node', 'verify_dashboard.js', 'https://test.invalid/dashboard', 'test', 'standard', 'local', ...opts.flags],
      exit(code) { resolve(code); throw Error('exit:' + code); }},
    console: Object.fromEntries(['log', 'warn', 'error'].map(level => [level, (...args) => logs.push(args.join(' '))])),
    URL, URLSearchParams, Date: Clock, document, __dirname: '/scripts',
  });
  execution?.catch(error => { if (!error.message.startsWith('exit:')) throw error; });
}).then(code => process.stdout.write(JSON.stringify({code, visits, resolvedRequests, logs})));
"""
    )
    assert result["code"] == exit_code
    messages = "\n".join(result["logs"])
    assert "Header request count and page metrics are consistent!" not in messages
    if flags == ["--no-traffic"]:
        assert result["resolvedRequests"] == [] and all("start_time" not in url for url in result["visits"])
        assert "[Dashboard 15m] NOT RUN" in messages and "latest event age" not in messages
        if exit_code == 0:
            assert "[RUM 15m] NOT RUN" in messages
            assert {urlparse(url).path for url in result["visits"]} == {
                "/dashboard",
                "/rum",
                "/network",
                "/streaming",
                "/security",
                "/origin",
                "/performance",
            }
    elif not flags:
        assert result["resolvedRequests"]
        for request in result["resolvedRequests"]:
            query = parse_qs(urlparse(request["navigation"]).query)
            assert "range" not in query and request["rangeToken"] is None and request["accepted"] is True
            assert request["body"] == {"start_time": query["start_time"][0], "end_time": query["end_time"][0]}
            start, end = (datetime.fromisoformat(request["body"][key]) for key in ("start_time", "end_time"))
            assert (end - start).total_seconds() == 900
        if isinstance(request_count, list):
            for route in ("/dashboard", "/rum"):
                bounds = [
                    r["body"]["end_time"] for r in result["resolvedRequests"] if urlparse(r["navigation"]).path == route
                ]
                assert len(bounds) == 2 and bounds[0] < bounds[1]
        assert "NOT RUN" not in messages
        if request_count is None:
            assert "UNVERIFIED" in messages and "requested-window aggregate contains" not in messages


@pytest.mark.parametrize("no_traffic", ["0", "1"])
def test_verify_canonical_runner_propagates_no_traffic(no_traffic: str) -> None:
    runner = _VERIFY_JS.parent / "dev" / "deploy_test_all.sh"
    if not runner.exists():
        pytest.skip("Canonical deployment runner is operator-local and gitignored")
    source = runner.read_text()
    start = source.index("    local verify_flags=()")
    end = source.index('\n  } >"$9"', start)
    script = (
        "node() { printf '%s\\n' \"$@\"; }\n"
        "validate_env() {\nlocal NO_TRAFFIC=$1 COMMIT_HASH=test\n"
        "set -- label x x x x https://test.invalid/dashboard standard local\n"
        + source[start:end]
        + f"\n}}\nvalidate_env {no_traffic}"
    )
    result = subprocess.run(["bash", "-c", script], capture_output=True, text=True, check=True, timeout=10)
    assert result.stdout.splitlines() == [
        "../scripts/verify_dashboard.js",
        "https://test.invalid/dashboard",
        "test",
        "standard",
        "local",
        *(["--no-traffic"] if no_traffic == "1" else []),
    ]
