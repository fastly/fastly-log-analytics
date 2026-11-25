"""Shared offline fixtures, network guard, SQLite metadata shim, and falco helpers."""

from __future__ import annotations

import asyncio
import json
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import threading
from collections.abc import Mapping, MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Literal

import pytest
from fastapi.testclient import TestClient

import backend.core.request_context as req_ctx_mod
from backend import config as svcconfig
from backend.core import fos_client, share_db
from backend.core.metadata import fos_usage, pg_schema
from backend.high_scale.archive import ServingWatermark
from backend.high_scale.ingest_controller import (
    HighScaleService,
    HighScaleServiceRegistry,
    get_high_scale_service_registry,
)
from backend.high_scale.postgres_control import ConsoleAccess, console_tables
from backend.main import app
from backend.utils import tunnel

pytestmark = pytest.mark.no_postgres

REPO_ROOT = Path(__file__).resolve().parents[2]
FASTLY_STUBS_VCL = REPO_ROOT / "tests" / "fixtures" / "fastly_stubs.vcl"

# 18 shared PII keys enforced across both High-Scale (SEC-08) and Cloud (SEC-19)
SHARED_18_PII_KEYS: tuple[str, ...] = (
    "ip",
    "client_ip",
    "c_ip",
    "ip_address",
    "remote_addr",
    "oip",
    "x-forwarded-for",
    "x_forwarded_for",
    "true-client-ip",
    "true_client_ip",
    "fastly-client-ip",
    "fastly_client_ip",
    "cookie_session",
    "cid",
    "client_id",
    "edge_sid",
    "sid",
    "session_id",
)

DEFAULT_ANALYST_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Chrome/131.0.0.0 Safari/537.36"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Guarantee every test under tests/e2e_security carries @pytest.mark.no_postgres."""
    sec_dir = str(Path(__file__).resolve().parent)
    for item in items:
        if str(item.fspath).startswith(sec_dir):
            item.add_marker(pytest.mark.no_postgres)


def _is_loopback_target(address: Any) -> bool:
    """Allow AF_UNIX sockets and IPv4/IPv6 loopback targets only."""
    if isinstance(address, str | bytes):
        return True
    if not isinstance(address, tuple) or not address:
        return False
    host = str(address[0]).strip().lower()
    return host in {"127.0.0.1", "::1", "localhost", "::ffff:127.0.0.1"}


@pytest.fixture(autouse=True)
def block_external_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail closed on any outbound non-loopback TCP/UDP socket connection."""
    orig_connect = socket.socket.connect
    orig_connect_ex = socket.socket.connect_ex
    orig_create_connection = socket.create_connection

    def guarded_connect(self: socket.socket, address: Any) -> Any:
        if self.family == getattr(socket, "AF_UNIX", -1) or _is_loopback_target(address):
            return orig_connect(self, address)
        raise RuntimeError(f"Offline safety guard blocked outbound socket.connect to {address!r}")

    def guarded_connect_ex(self: socket.socket, address: Any) -> int:
        if self.family == getattr(socket, "AF_UNIX", -1) or _is_loopback_target(address):
            return orig_connect_ex(self, address)
        raise RuntimeError(f"Offline safety guard blocked outbound socket.connect_ex to {address!r}")

    def guarded_create_connection(address: Any, *args: Any, **kwargs: Any) -> socket.socket:
        if _is_loopback_target(address):
            return orig_create_connection(address, *args, **kwargs)
        raise RuntimeError(f"Offline safety guard blocked outbound create_connection to {address!r}")

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
    monkeypatch.setattr(socket, "create_connection", guarded_create_connection)


class _SqlitePgShim:
    """Thread-safe SQLite connection proxy compatible with pg_pool and share_db."""

    def __init__(self, con: sqlite3.Connection, lock: threading.RLock) -> None:
        self._con = con
        self._lock = lock
        self._service_id: str | None = None

    def execute(self, sql: str, params: Any = ()) -> sqlite3.Cursor:
        with self._lock:
            return self._con.execute(sql, params)

    def executemany(self, sql: str, seq: Any) -> sqlite3.Cursor:
        with self._lock:
            return self._con.executemany(sql, seq)

    def executescript(self, sql: str) -> sqlite3.Cursor:
        with self._lock:
            return self._con.executescript(sql)

    def cursor(self) -> sqlite3.Cursor:
        return self._con.cursor()

    def commit(self) -> None:
        with self._lock:
            self._con.commit()

    def rollback(self) -> None:
        with self._lock:
            self._con.rollback()

    def close(self) -> None:
        pass

    def __enter__(self) -> _SqlitePgShim:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Literal[False]:
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False


class RecordingClickHouseClient:
    """ClickHouse execution-boundary recorder invoked AFTER real SQL validation/rewriting."""

    def __init__(self, handler: Any = None) -> None:
        self.responses: dict[str, Any] = {}
        self.calls: list[tuple[str, dict[str, Any] | None, dict[str, Any] | None]] = []
        self._handler = handler

    def set_handler(self, handler: Any) -> None:
        self._handler = handler

    def execute(
        self,
        sql: str,
        params: dict[str, Any] | None = None,
        *,
        settings: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        self.calls.append((sql, dict(params) if params else None, dict(settings) if settings else None))
        if "FROM system.one" in sql:
            return [{"n": 0}]
        if self._handler is not None:
            return self._handler(sql, params, settings)
        return []


@pytest.fixture(autouse=True)
def isolated_e2e_security_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Provide per-test isolated SQLite metadata/share DB, service configs, and clean state."""
    db_path = tmp_path / "e2e_meta.sqlite"
    raw_con = sqlite3.connect(str(db_path), check_same_thread=False)
    raw_con.row_factory = sqlite3.Row
    raw_con.execute("PRAGMA foreign_keys = ON")
    shim = _SqlitePgShim(raw_con, threading.RLock())

    for ddl in (*pg_schema._SCHEMA, *share_db._SCHEMA, *fos_usage._SCHEMA):
        raw_con.execute(ddl.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT"))
    raw_con.commit()

    for target in (
        "backend.core.metadata.pg_pool.get_pg_thread_connection",
        "backend.core.metadata.pg_pool.get_pg_readonly_connection",
        "backend.core.metadata.base.get_pg_thread_connection",
        "backend.core.share_db.get_global_share_con",
    ):
        monkeypatch.setattr(target, lambda: shim)
    monkeypatch.setattr("backend.core.metadata.pg_pool.close_all_pg_connections", lambda: None)

    sandbox_configs = tmp_path / "configs"
    sandbox_data = tmp_path / "data"
    sandbox_services = sandbox_data / "services"
    sandbox_ngwaf = sandbox_data / "ngwaf"
    sandbox_cache = sandbox_data / "cache"
    sandbox_system = sandbox_data / "system"
    for d in (sandbox_configs, sandbox_data, sandbox_services, sandbox_ngwaf, sandbox_cache, sandbox_system):
        d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(svcconfig, "CONFIGS_DIR", sandbox_configs)
    monkeypatch.setattr(svcconfig, "DATA_DIR", sandbox_data)
    monkeypatch.setattr(svcconfig, "SERVICES_DATA_DIR", sandbox_services)
    monkeypatch.setattr(svcconfig, "NGWAF_DATA_DIR", sandbox_ngwaf)
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", sandbox_cache)
    monkeypatch.setattr(svcconfig, "SYSTEM_DATA_DIR", sandbox_system)
    if hasattr(svcconfig, "ACTIVE_SERVICE_FILE"):
        active_file = sandbox_system / "active_service"
        active_file.write_text("svc-a", encoding="utf-8")
        monkeypatch.setattr(svcconfig, "ACTIVE_SERVICE_FILE", active_file)

    cache_a = sandbox_cache / "bucket-a"
    cache_b = sandbox_cache / "bucket-b"
    cache_a.mkdir(parents=True, exist_ok=True)
    cache_b.mkdir(parents=True, exist_ok=True)

    def _sandboxed_cache_dir(source: dict[str, Any] | None = None) -> str:
        src = source or fos_client._DEFAULT_SOURCE
        if "_cache_dir_override" in src:
            return str(src["_cache_dir_override"])
        bucket = str(src.get("bucket") or src.get("fos_bucket") or "default").strip()
        return str(sandbox_cache / bucket)

    monkeypatch.setattr(fos_client, "_cache_dir", _sandboxed_cache_dir)

    configs: dict[str, dict[str, Any]] = {
        "svc-a": {
            "service_id": "svc-a",
            "name": "Customer A Production",
            "fos_bucket": "bucket-a",
            "bucket": "bucket-a",
            "fos_region": "us-east-1",
            "region": "us-east-1",
            "fos_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "us-east-1.object.fastlystorage.app",
            "fos_access_key_id": "test-sentinel-access-a-0001",
            "access_key_id": "test-sentinel-access-a-0001",
            "fos_secret_access_key": "test-sentinel-secret-a-0001",
            "secret_access_key": "test-sentinel-secret-a-0001",
            "cdn_url": "https://cdn-svc-a.global.ssl.fastly.net",
            "cdn_secret": "test-sentinel-cdn-key-a-0001",  # gitleaks:allow
            "cdn_service_id": "cdn-svc-a",
            "ngwaf_workspace_id": "ws-sentinel-a-0001",
            "cron_stats": {"enabled": True, "interval_s": 60},
            "rum": {
                "enabled": True,
                "cid_salt": "test-sentinel-cid-salt-a-0001",
                "log_period": 60,
                "faro_version": "1.19.0",
            },
            "provisioning": {
                "endpoint_name": "Fastly Object Storage Logs",
                "cdn_service_id": "cdn-svc-a",
                "cdn_url": "https://cdn-svc-a.global.ssl.fastly.net",
            },
            "_cache_dir_override": str(cache_a),
        },
        "svc-b": {
            "service_id": "svc-b",
            "name": "Customer B Enterprise",
            "fos_bucket": "bucket-b",
            "bucket": "bucket-b",
            "fos_region": "us-east-1",
            "region": "us-east-1",
            "fos_endpoint": "us-east-1.object.fastlystorage.app",
            "endpoint": "us-east-1.object.fastlystorage.app",
            "fos_access_key_id": "test-sentinel-access-b-0002",
            "access_key_id": "test-sentinel-access-b-0002",
            "fos_secret_access_key": "test-sentinel-secret-b-0002",
            "secret_access_key": "test-sentinel-secret-b-0002",
            "cdn_url": "https://cdn-svc-b.global.ssl.fastly.net",
            "cdn_secret": "test-sentinel-cdn-key-b-0002",  # gitleaks:allow
            "cdn_service_id": "cdn-svc-b",
            "ngwaf_workspace_id": "ws-sentinel-b-0002",
            "cron_stats": {"enabled": True, "interval_s": 60},
            "rum": {
                "enabled": True,
                "cid_salt": "test-sentinel-cid-salt-b-0002",
                "log_period": 60,
                "faro_version": "1.19.0",
            },
            "provisioning": {
                "endpoint_name": "Fastly Object Storage Logs",
                "cdn_service_id": "cdn-svc-b",
                "cdn_url": "https://cdn-svc-b.global.ssl.fastly.net",
            },
            "_cache_dir_override": str(cache_b),
        },
    }
    orig_get_active_service_id = svcconfig.get_active_service_id
    for sid, cfg in configs.items():
        svcconfig.save_config(sid, cfg)
    svcconfig.invalidate_config_cache()
    monkeypatch.setattr("backend.config.get_active_service_id", lambda fallback_to_first=True: "svc-a")
    fos_client.reload_default_source()

    # Prevent background realtime polling threads and infinite SSE hangs in tests
    from backend.core import realtime as rt_mod

    monkeypatch.setattr(rt_mod.poller, "ensure_polling", lambda *a, **kw: None)
    monkeypatch.setattr("backend.routers.control_room._fetch_seed_ticks", lambda sid, count=60: [])
    monkeypatch.setattr("backend.routers.control_room._recent_high_scale_errors", lambda svc: [])
    monkeypatch.setattr("backend.routers.control_room.recent_high_scale_error_events", lambda *a, **kw: [])

    async def _finite_rt_sub(_sid: str):
        yield {"timestamp": "2026-06-12T00:00:00Z", "requests": 1}

    monkeypatch.setattr(rt_mod.publisher, "subscribe", _finite_rt_sub)

    from sse_starlette.sse import AppStatus

    AppStatus.should_exit = False
    setattr(AppStatus, "should_exit_event", None)
    app.dependency_overrides.clear()
    tunnel.reset_for_tests()
    setattr(req_ctx_mod, "_EPHEMERAL_KEY", None)
    req_ctx_mod._key = None
    monkeypatch.setattr("backend.utils.telemetry.start_proxy_server", lambda: None)
    monkeypatch.setattr("backend.utils.telemetry.stop_proxy_server", lambda: None)

    yield SimpleNamespace(
        shim=shim,
        configs=configs,
        tmp_path=tmp_path,
        cache_a=cache_a,
        cache_b=cache_b,
        orig_get_active_service_id=orig_get_active_service_id,
    )

    AppStatus.should_exit = False
    setattr(AppStatus, "should_exit_event", None)
    app.dependency_overrides.clear()
    tunnel.reset_for_tests()
    setattr(req_ctx_mod, "_EPHEMERAL_KEY", None)
    req_ctx_mod._key = None
    with svcconfig._config_cache_lock:
        svcconfig._config_cache.clear()
    raw_con.close()


@pytest.fixture
def ch_recorder(monkeypatch: pytest.MonkeyPatch) -> RecordingClickHouseClient:
    """Wire RecordingClickHouseClient + ConsoleAccess for svc-a and svc-b."""
    recorder = RecordingClickHouseClient()

    def _wm(sid: str, domain: str = "request") -> ServingWatermark:
        return ServingWatermark(
            service_id=sid,
            domain=domain,
            owner_epoch=1,
            coverage_start=datetime(2026, 1, 1, tzinfo=UTC),
            coverage_end=datetime(2026, 12, 31, tzinfo=UTC),
            last_accepted_cursor="c1",
            last_archived_event_id="e1",
            last_visible_event_id="e1",
            exact=True,
        )

    reg = HighScaleServiceRegistry(
        {
            sid: HighScaleService(
                service_id=sid,
                client=recorder,
                cursor_secret=b"test-sentinel-cursor-secret-32b!",
                request_watermark=_wm(sid, "request"),
                rum_watermarks={
                    "rum_vitals": _wm(sid, "rum_vitals"),
                    "rum_errors": _wm(sid, "rum_errors"),
                },
                cmcd_watermark=_wm(sid, "cmcd"),
            )
            for sid in ("svc-a", "svc-b")
        }
    )
    monkeypatch.setattr("backend.high_scale.ingest_controller.get_high_scale_service_registry", lambda: reg)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: reg
    monkeypatch.setattr(
        "backend.high_scale.postgres_control.get_console_clients",
        lambda: SimpleNamespace(
            client_for=lambda sid, masked=True: ConsoleAccess(
                client=recorder,
                tables=console_tables(sid, masked),
            )
        ),
    )
    return recorder


def make_client(
    peer_ip: str = "127.0.0.1",
    *,
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> TestClient:
    """Create an in-process ASGI TestClient with an explicit TCP peer IP."""
    client = TestClient(
        app,
        client=(peer_ip, 50000),
        headers=headers,
        cookies=cookies,
        raise_server_exceptions=False,
    )
    return client


def probe_sse_status(
    path: str,
    *,
    peer_ip: str = "203.0.113.10",
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> tuple[int, str]:
    """Open an SSE/streaming route via ASGI 3.0, read status + error body, and disconnect cleanly."""

    async def _run() -> tuple[int, str]:
        raw_path, _, query = path.partition("?")
        hdr_map: dict[str, str] = {"host": "share.example.test"}
        if headers:
            for k, v in headers.items():
                hdr_map[k.lower()] = v
        if cookies:
            cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
            hdr_map["cookie"] = f"{hdr_map['cookie']}; {cookie_str}" if "cookie" in hdr_map else cookie_str

        scope = {
            "type": "http",
            "asgi": {"version": "3.0", "spec_version": "2.3"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "https",
            "path": raw_path,
            "raw_path": raw_path.encode("utf-8"),
            "query_string": query.encode("utf-8"),
            "root_path": "",
            "headers": [(k.encode("latin-1"), v.encode("latin-1")) for k, v in hdr_map.items()],
            "client": (peer_ip, 50000),
            "server": ("share.example.test", 443),
            "state": {},
        }

        status_code = 500
        body_chunks: list[bytes] = []
        req_sent = False
        disconnect_ev = asyncio.Event()

        async def receive() -> dict[str, Any]:
            nonlocal req_sent
            if not req_sent:
                req_sent = True
                return {"type": "http.request", "body": b"", "more_body": False}
            await disconnect_ev.wait()
            return {"type": "http.disconnect"}

        async def send(message: MutableMapping[str, Any]) -> None:
            nonlocal status_code
            msg_type = message.get("type")
            if msg_type == "http.response.start":
                status_code = int(message.get("status", 500))
                if status_code == 200:
                    disconnect_ev.set()
            elif msg_type == "http.response.body":
                chunk = message.get("body", b"")
                if chunk:
                    body_chunks.append(chunk)
                if not message.get("more_body", False):
                    disconnect_ev.set()

        await asyncio.wait_for(app(scope, receive, send), timeout=5.0)
        return status_code, b"".join(body_chunks).decode("utf-8", errors="replace")

    return asyncio.run(_run())


@dataclass
class AnalystSessionBundle:
    """Provisioned remote-share invite + active analyst session for tests."""

    invite_id: str
    session_id: str
    email: str
    passcode: str
    public_host: str
    client_ip: str
    user_agent: str

    def headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        base = {
            "Host": self.public_host,
            "Origin": f"https://{self.public_host}",
            "User-Agent": self.user_agent,
            "X-Proxied-By-Caddy": "true",
            "Fastly-Client-IP": self.client_ip,
        }
        if extra:
            base.update(extra)
        return base

    def cookies(self) -> dict[str, str]:
        return {"analyst_session_id": self.session_id}


def provision_analyst_session(
    *,
    service_ids: list[str] | None = None,
    mask_ips: bool = False,
    client_ip: str = "203.0.113.25",
    ip_whitelist: str | None = None,
    user_agent: str = DEFAULT_ANALYST_UA,
    query_start_time: str | None = None,
    query_end_time: str | None = None,
    public_host: str = "share.example.test",
    email: str = "analyst@tenant.example.test",
    passcode: str = "Str0ng!Passcode#2026",
    accept_tos: bool = True,
) -> AnalystSessionBundle:
    """Enable remote sharing on a synthetic public endpoint and mint an active analyst session."""
    mgr = tunnel.get_tunnel_manager()
    mgr.start_sharing(public_endpoint=f"https://{public_host}")
    share_db.set_setting("sharing_enabled", "true")

    expires = (datetime.now(UTC) + timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
    invite = share_db.create_remote_invite(
        name="Security Test Analyst",
        email=email,
        passcode=passcode,
        expires_at_utc=expires,
        ip_whitelist=ip_whitelist,
        service_ids=service_ids if service_ids is not None else ["svc-a"],
        pii_policy={"mask_ips": mask_ips},
        query_start_time=query_start_time,
        query_end_time=query_end_time,
        allow_concurrent_sessions=True,
    )
    if accept_tos:
        tos = share_db.get_latest_tos()
        tos_ver = str((tos or {}).get("version") or "1")
        share_db.mark_tos_accepted(invite["id"], tos_ver)
        invite = share_db.get_remote_invite(invite["id"]) or invite

    session = mgr.create_session(
        invite=invite,
        ip_address=client_ip,
        user_agent=user_agent,
        headers={"User-Agent": user_agent},
    )
    return AnalystSessionBundle(
        invite_id=invite["id"],
        session_id=session.session_id,
        email=email,
        passcode=passcode,
        public_host=public_host,
        client_ip=client_ip,
        user_agent=user_agent,
    )


@dataclass
class FalcoResult:
    """Structured result from running falco lint or falco test."""

    returncode: int
    stdout: str
    stderr: str
    payload: dict[str, Any] | None


def _falco_bin() -> str:
    found = shutil.which("falco") or "/opt/homebrew/bin/falco"
    if not Path(found).exists():
        raise RuntimeError("falco binary not found on PATH or /opt/homebrew/bin/falco")
    return found


def run_falco_lint(
    main_vcl: str,
    *,
    extra_files: dict[str, str] | None = None,
) -> FalcoResult:
    """Run `falco lint -json` on temporary VCL files."""
    with tempfile.TemporaryDirectory(prefix="fla-e2e-falco-lint-") as tmp:
        tmpdir = Path(tmp)
        main_path = tmpdir / "main.vcl"
        main_path.write_text(main_vcl, encoding="utf-8")
        for rel, content in (extra_files or {}).items():
            target = tmpdir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        proc = subprocess.run(
            [_falco_bin(), "lint", "-I", str(tmpdir), "-json", str(main_path)],
            cwd=str(tmpdir),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        parsed = None
        if proc.stdout.strip():
            try:
                parsed = json.loads(proc.stdout)
            except json.JSONDecodeError:
                parsed = None
        return FalcoResult(proc.returncode, proc.stdout, proc.stderr, parsed)


def run_falco_test(
    main_vcl: str,
    test_vcl: str,
    *,
    extra_files: dict[str, str] | None = None,
    inject_ssl: bool = True,
) -> FalcoResult:
    """Run `falco test -json` on temporary main.vcl + suite.test.vcl."""
    all_vcl = main_vcl + "\n".join((extra_files or {}).values())
    if inject_ssl and "if (!req.is_ssl" in all_vcl:
        test_vcl = test_vcl.replace(
            'testing.call_subroutine("vcl_recv");',
            'testing.inject_variable("req.is_ssl", true);\n  testing.call_subroutine("vcl_recv");',
        )
    with tempfile.TemporaryDirectory(prefix="fla-e2e-falco-test-") as tmp:
        tmpdir = Path(tmp)
        main_path = tmpdir / "main.vcl"
        test_path = tmpdir / "suite.test.vcl"
        main_path.write_text(main_vcl, encoding="utf-8")
        test_path.write_text(test_vcl, encoding="utf-8")
        for rel, content in (extra_files or {}).items():
            target = tmpdir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content, encoding="utf-8")
        proc = subprocess.run(
            [_falco_bin(), "test", "-I", str(tmpdir), "-json", str(main_path)],
            cwd=str(tmpdir),
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        parsed = None
        if proc.stdout.strip():
            try:
                parsed = json.loads(proc.stdout)
            except json.JSONDecodeError:
                parsed = None
        return FalcoResult(proc.returncode, proc.stdout, proc.stderr, parsed)


@pytest.fixture(scope="module")
def _cloud_server_process() -> Any:
    """Module-scoped shell-test-server process to keep Cloud HTTP suites sub-second."""
    from tests.e2e_security.cloud_harness import CloudTestServer

    srv = CloudTestServer()
    try:
        srv.start()
        yield srv
    finally:
        srv.stop()


@pytest.fixture
def cloud_server(_cloud_server_process: Any) -> Any:
    """Per-test reset CloudTestServer instance."""
    _cloud_server_process.reset()
    return _cloud_server_process
