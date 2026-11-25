"""Campaign D1 Iteration 1 security hardening regression tests.

Covers:
- WORKER-01: Bounded gzip/zstd decompression ceiling in high-scale decoder and ingest parser,
  plus bounded corrupt-container capture.
- ROUTER-01: HistoricalJobManager per-owner concurrency cap, aggregate retained output memory
  eviction, lowered default max_jobs, and HTTP 429 mapping in submit_query.
- ROUTER-02: Route-level Depends(require_admin) on usage, audit-logs, cron-runs, and
  control-room wizard routes, plus analyst blocklist coverage in remote_access.
- DOCKER-01: Fail-closed POSTGRES_PASSWORD / CLICKHOUSE_PASSWORD and disabled
  CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT in docker-compose.prod.yml.
- K8S-02 & K8S-03: ClickHouse automountServiceAccountToken: false and seccompProfile:
  RuntimeDefault across ClickHouse, Keeper, and FLA Helm charts.
"""

from __future__ import annotations

import gzip
import shutil
import subprocess
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
import zstandard as zstd
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from backend.core.ingest import parser as ingest_parser
from backend.core.request_context import require_admin
from backend.high_scale import query as hs_query
from backend.high_scale.archive import HistoricalJobManager, HistoricalJobState
from backend.high_scale.query_service import QueryIntent, QueryPlan, QueryTier
from backend.routers import audit as audit_mod
from backend.routers import usage as usage_mod
from backend.routers.admin import high_scale as hs_router_mod
from backend.routers.services import views as views_mod
from backend.utils.remote_access import _is_blocked_path

pytestmark = [pytest.mark.no_postgres, pytest.mark.security_regression]

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _cold_plan(service_id: str = "svc-d1", max_rows: int = 100) -> QueryPlan:
    now = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)
    return QueryPlan(
        service_id=service_id,
        domain="request",
        intent=QueryIntent.RAW,
        start=now - timedelta(days=30),
        end=now - timedelta(days=29),
        tier=QueryTier.COLD_JOB,
        sources=("queued_query_job",),
        queued=True,
        bounded=True,
        max_rows=max_rows,
        reason="test",
    )


def _wait_for_state(
    manager: HistoricalJobManager, job_id: str, expected: HistoricalJobState, timeout: float = 2.0
) -> Any:
    deadline = datetime.now(UTC) + timedelta(seconds=timeout)
    while datetime.now(UTC) < deadline:
        job = manager.get(job_id)
        if job is not None and job.state is expected:
            return job
        threading.Event().wait(0.01)
    raise AssertionError(f"job {job_id} did not reach {expected}")


# ── WORKER-01 ────────────────────────────────────────────────────────────────


def test_worker01_decode_payload_rejects_gzip_and_zstd_bombs() -> None:
    assert getattr(hs_query, "MAX_DECOMPRESSED_OBJECT_BYTES", None) == 128 * 1024 * 1024

    raw_bomb = b"A" * (512 * 1024)
    gz_bomb = gzip.compress(raw_bomb)
    zstd_bomb = zstd.ZstdCompressor().compress(raw_bomb)

    with pytest.raises(ValueError, match="Decompressed payload exceeds maximum allowed size"):
        hs_query._decode_payload(gz_bomb, max_decompressed_bytes=4096)

    with pytest.raises(ValueError, match="Decompressed payload exceeds maximum allowed size"):
        hs_query._decode_payload(zstd_bomb, max_decompressed_bytes=4096)

    with pytest.raises(ValueError, match="Decompressed payload exceeds maximum allowed size"):
        hs_query._decode_payload(raw_bomb, max_decompressed_bytes=4096)

    # Normal payloads under the ceiling still decode cleanly.
    small = b'{"status":200}\n'
    assert hs_query._decode_payload(gzip.compress(small), max_decompressed_bytes=4096) == small
    assert hs_query._decode_payload(zstd.ZstdCompressor().compress(small), max_decompressed_bytes=4096) == small


def test_worker01_open_compressed_log_rejects_gzip_and_zstd_bombs(tmp_path: Path) -> None:
    assert getattr(ingest_parser, "MAX_DECOMPRESSED_OBJECT_BYTES", None) == 128 * 1024 * 1024

    bomb_lines = (b'{"timestamp":"2026-11-25T00:00:00Z","pad":"' + b"x" * 1000 + b'"}\n') * 64
    gz_path = tmp_path / "bomb.log.gz"
    gz_path.write_bytes(gzip.compress(bomb_lines))
    zstd_path = tmp_path / "bomb.log.zst"
    zstd_path.write_bytes(zstd.ZstdCompressor().compress(bomb_lines))

    with pytest.raises(ValueError, match="Decompressed log exceeds maximum allowed size"):
        with ingest_parser._open_compressed_log(str(gz_path), max_decompressed_bytes=4096) as fh:
            for _ in fh:
                pass

    with pytest.raises(ValueError, match="Decompressed log exceeds maximum allowed size"):
        with ingest_parser._open_compressed_log(str(zstd_path), max_decompressed_bytes=4096) as fh:
            for _ in fh:
                pass

    # Valid small files still stream cleanly.
    small_line = b'{"timestamp":"2026-11-25T00:00:00Z"}\n'
    ok_gz = tmp_path / "ok.log.gz"
    ok_gz.write_bytes(gzip.compress(small_line))
    with ingest_parser._open_compressed_log(str(ok_gz), max_decompressed_bytes=4096) as fh:
        assert list(fh) == [small_line]


def test_worker01_capture_corrupt_container_bounds_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    corrupt_file = tmp_path / "huge-corrupt.gz"
    corrupt_file.write_bytes(b"\x1f\x8b" + b"Z" * 16_384)
    captured: dict[str, Any] = {}

    def _fake_capture_evidence(service_id: str, source_type: str, object_key: str, payload: bytes, **kwargs: Any):
        captured["payload"] = payload
        return {"cap_evictions": 0, "quarantine_capture_failures": 0}

    monkeypatch.setattr("backend.core.metadata.ingest_log.capture_evidence", _fake_capture_evidence)

    outcome = ingest_parser._capture_corrupt_container(
        "svc-d1",
        "request",
        "logs/huge-corrupt.gz",
        str(corrupt_file),
        OSError("bad gzip"),
        max_bytes=1024,
    )
    assert outcome["corrupt_containers"] == 1
    assert len(captured["payload"]) == 1024


# ── ROUTER-01 ────────────────────────────────────────────────────────────────


def test_router01_historical_job_manager_per_owner_cap_and_memory_eviction() -> None:
    default_mgr = HistoricalJobManager()
    try:
        assert default_mgr._max_jobs <= 128
    finally:
        default_mgr.shutdown()

    # 1) Per-owner active concurrency cap.
    gate = threading.Event()
    started = threading.Event()
    manager = HistoricalJobManager(max_workers=2, max_active_per_owner=2, max_total_output_bytes=64 * 1024)
    try:

        def _blocking_producer():
            started.set()
            gate.wait(2.0)
            yield {"id": 1}

        jid1 = manager.submit(_cold_plan(), _blocking_producer, owner_key="analyst:sess-1")
        assert started.wait(1.0)
        jid2 = manager.submit(_cold_plan(), _blocking_producer, owner_key="analyst:sess-1")

        # 3rd active job for the same owner is rejected.
        with pytest.raises(RuntimeError, match="too many active historical jobs"):
            manager.submit(_cold_plan(), lambda: [{"id": 3}], owner_key="analyst:sess-1")

        # A different owner is still permitted.
        jid_other = manager.submit(_cold_plan(), lambda: [{"id": 4}], owner_key="analyst:sess-2")
        gate.set()
        _wait_for_state(manager, jid1, HistoricalJobState.COMPLETED)
        _wait_for_state(manager, jid2, HistoricalJobState.COMPLETED)
        _wait_for_state(manager, jid_other, HistoricalJobState.COMPLETED)
    finally:
        gate.set()
        manager.shutdown()

    # 2) Aggregate retained output memory budget evicts oldest completed payloads.
    mem_mgr = HistoricalJobManager(
        max_workers=1,
        max_output_bytes=500,
        max_total_output_bytes=800,
        terminal_grace_ttl=timedelta(minutes=5),
    )
    try:
        row_payload = {"v": "x" * 100}  # ~108 bytes serialized + 256B row overhead = ~364 bytes
        j1 = mem_mgr.submit(_cold_plan(), lambda: [row_payload])
        _wait_for_state(mem_mgr, j1, HistoricalJobState.COMPLETED)
        j2 = mem_mgr.submit(_cold_plan(), lambda: [row_payload])
        _wait_for_state(mem_mgr, j2, HistoricalJobState.COMPLETED)
        j3 = mem_mgr.submit(_cold_plan(), lambda: [row_payload])
        _wait_for_state(mem_mgr, j3, HistoricalJobState.COMPLETED)

        snap1 = mem_mgr.get(j1)
        snap2 = mem_mgr.get(j2)
        snap3 = mem_mgr.get(j3)
        assert snap1 is not None and snap2 is not None and snap3 is not None
        assert snap1.output_bytes == 0 and snap1.rows == ()
        assert snap3.output_bytes > 0 and len(snap3.rows) == 1
        assert snap1.output_bytes + snap2.output_bytes + snap3.output_bytes <= 800
    finally:
        mem_mgr.shutdown()


def test_router01_submit_query_passes_owner_key_and_maps_limit_to_429(monkeypatch: pytest.MonkeyPatch) -> None:
    gate = threading.Event()
    started = threading.Event()
    manager = HistoricalJobManager(max_workers=1, max_active_per_owner=1)

    fake_service = SimpleNamespace(manifest_catalog=object(), archive=object())
    monkeypatch.setattr(hs_router_mod, "_resolve_scoped_high_scale_service", lambda *a, **kw: fake_service)
    monkeypatch.setattr(
        hs_router_mod,
        "_clamp_and_parse_window",
        lambda ctx, s, e: (
            datetime(2026, 10, 1, 0, 0, tzinfo=UTC),
            datetime(2026, 10, 2, 0, 0, tzinfo=UTC),
        ),
    )

    def _blocking_producer(*args: Any, **kwargs: Any):
        def _gen():
            started.set()
            gate.wait(2.0)
            yield {"id": 1}

        return _gen

    monkeypatch.setattr(hs_router_mod, "raw_query_producer", _blocking_producer)

    ctx = SimpleNamespace(
        service_id="svc-d1",
        analyst_session=SimpleNamespace(session_id="sess-429"),
    )
    req = SimpleNamespace(
        domain="request",
        start_time="2026-10-01T00:00:00Z",
        end_time="2026-10-02T00:00:00Z",
        limit=10,
        cursor=None,
    )

    try:
        resp = hs_router_mod.submit_query("svc-d1", req, ctx=ctx, registry=SimpleNamespace(), jobs=manager)
        assert resp.job_id is not None
        assert started.wait(1.0)
        assert manager._jobs[resp.job_id].owner_key == "analyst:sess-429"

        with pytest.raises(HTTPException) as exc_info:
            hs_router_mod.submit_query("svc-d1", req, ctx=ctx, registry=SimpleNamespace(), jobs=manager)
        assert exc_info.value.status_code == 429
    finally:
        gate.set()
        manager.shutdown()


# ── ROUTER-02 ────────────────────────────────────────────────────────────────


def _router_has_require_admin(router_obj: Any) -> bool:
    return any(getattr(dep, "dependency", None) is require_admin for dep in (router_obj.dependencies or ()))


def _make_analyst_app(*routers: Any) -> TestClient:
    app = FastAPI()

    @app.middleware("http")
    async def _inject_analyst(request: Request, call_next: Any):
        request.state.analyst_session = SimpleNamespace(session_id="sess-1", service_ids=["svc-d1"])
        request.state.is_remote = True
        return await call_next(request)

    for r in routers:
        app.include_router(r)
    return TestClient(app)


def test_router02_usage_audit_cron_and_wizard_enforce_route_level_require_admin() -> None:
    assert _router_has_require_admin(usage_mod.usage_router)
    assert _router_has_require_admin(views_mod.audit_router)
    assert _router_has_require_admin(views_mod.cron_router)

    wizard_routes = {
        (tuple(sorted(r.methods)), r.path): r
        for r in audit_mod.router.routes
        if "control-room/wizard" in getattr(r, "path", "")
    }
    assert (("GET",), "/api/services/{service_id}/control-room/wizard/state") in wizard_routes
    assert (("POST",), "/api/services/{service_id}/control-room/wizard/step") in wizard_routes
    for route in wizard_routes.values():
        assert any(getattr(dep, "dependency", None) is require_admin for dep in route.dependencies)

    # Bypassing RemoteAccessMiddleware, route-level Depends(require_admin) still rejects analysts with 403.
    client = _make_analyst_app(
        usage_mod.usage_router,
        views_mod.audit_router,
        views_mod.cron_router,
        audit_mod.router,
    )
    assert client.get("/api/usage/platform").status_code == 403
    assert client.get("/api/audit-logs").status_code == 403
    assert client.get("/api/cron-runs").status_code == 403
    assert client.get("/api/services/svc-d1/control-room/wizard/state").status_code == 403
    assert client.post("/api/services/svc-d1/control-room/wizard/step", json={"step": "connect"}).status_code == 403


def test_router02_remote_access_blocks_control_room_wizard() -> None:
    assert _is_blocked_path("/api/services/svc-d1/control-room/wizard/state") is True
    assert _is_blocked_path("/api/services/svc-d1/control-room/wizard/step") is True


# ── DOCKER-01 ────────────────────────────────────────────────────────────────


class _ComposeLoader(yaml.SafeLoader):
    pass


_ComposeLoader.add_constructor("!reset", lambda loader, node: None)
_ComposeLoader.add_constructor("!override", lambda loader, node: loader.construct_sequence(node))


def test_docker01_prod_compose_fail_closed_passwords_and_no_access_management() -> None:
    compose_path = _REPO_ROOT / "docker-compose.prod.yml"
    raw = compose_path.read_text(encoding="utf-8")

    assert "fla_local_dev" not in raw
    assert "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1" not in raw

    parsed = yaml.load(raw, Loader=_ComposeLoader)
    services = parsed["services"]

    for svc_name in ("backend", "worker"):
        env = services[svc_name]["environment"]
        assert any("${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}" in entry for entry in env)
        assert any("${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is required}" in entry for entry in env)

    pg_env = services["postgres"]["environment"]
    assert "POSTGRES_PASSWORD=${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}" in pg_env

    ch_env = services["clickhouse"]["environment"]
    assert "CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD:?CLICKHOUSE_PASSWORD is required}" in ch_env
    assert "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=0" in ch_env


# ── K8S-02 & K8S-03 ──────────────────────────────────────────────────────────


def _helm_render(chart_rel_path: str, *extra_args: str) -> list[dict[str, Any]]:
    helm_bin = shutil.which("helm") or "/opt/homebrew/bin/helm"
    cmd = [helm_bin, "template", "d1-test", str(_REPO_ROOT / chart_rel_path), *extra_args]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return [doc for doc in yaml.safe_load_all(res.stdout) if isinstance(doc, dict)]


def test_k8s02_k8s03_helm_charts_render_hardened_security_context() -> None:
    ch_docs = _helm_render(
        "deploy/chart/clickhouse",
        "--set",
        "keeper.enabled=true",
        "--set",
        "auth.existingSecret=d1-test-ch-secret",
    )

    service_accounts = [d for d in ch_docs if d.get("kind") == "ServiceAccount"]
    assert service_accounts, "ClickHouse chart must render a ServiceAccount"
    for sa in service_accounts:
        assert sa.get("automountServiceAccountToken") is False

    statefulsets = [d for d in ch_docs if d.get("kind") == "StatefulSet"]
    assert len(statefulsets) >= 2, "Expected both ClickHouse shard and Keeper StatefulSets"
    for sts in statefulsets:
        pod_sc = sts["spec"]["template"]["spec"]["securityContext"]
        assert pod_sc.get("seccompProfile", {}).get("type") == "RuntimeDefault", sts["metadata"]["name"]

    fla_docs = _helm_render("deploy/chart/fastly-log-analytics", "--set", "secrets.existingSecret=d1-test-secret")
    deployments = [d for d in fla_docs if d.get("kind") == "Deployment"]
    assert deployments, "FLA chart must render Deployments"
    for dep in deployments:
        pod_sc = dep["spec"]["template"]["spec"]["securityContext"]
        assert pod_sc.get("seccompProfile", {}).get("type") == "RuntimeDefault", dep["metadata"]["name"]


# ── Wave 1 Verifier Follow-Ups ───────────────────────────────────────────────


def test_w1_bounded_stream_reader_enforces_limit_across_textiowrapper_and_buffered_methods(tmp_path: Path) -> None:
    """TextIOWrapper (read1), readlines, readinto, and readinto1 must enforce the decompressed cap."""
    import io

    from backend.core.ingest.parser import _open_compressed_log

    gz_path = tmp_path / "bomb_buffered.log.gz"
    gz_path.write_bytes(gzip.compress(b"A" * 50_000))

    with pytest.raises(ValueError, match="maximum allowed size"):
        with _open_compressed_log(str(gz_path), max_decompressed_bytes=4096) as fh:
            io.TextIOWrapper(fh, encoding="utf-8").readline()

    with pytest.raises(ValueError, match="maximum allowed size"):
        with _open_compressed_log(str(gz_path), max_decompressed_bytes=4096) as fh:
            fh.read1(50_000)

    with pytest.raises(ValueError, match="maximum allowed size"):
        with _open_compressed_log(str(gz_path), max_decompressed_bytes=4096) as fh:
            fh.readlines()

    with pytest.raises(ValueError, match="maximum allowed size"):
        with _open_compressed_log(str(gz_path), max_decompressed_bytes=4096) as fh:
            fh.readinto(bytearray(50_000))


def test_w1_historical_jobs_evicts_terminal_slots_and_accounts_row_overhead() -> None:
    """Completed jobs must not block new submissions at max_jobs, and tiny rows must account for dict overhead."""
    from backend.high_scale.archive.jobs import HistoricalJobLimitError, HistoricalJobManager, HistoricalJobState

    manager = HistoricalJobManager(
        max_workers=2,
        max_jobs=3,
        max_active_per_owner=2,
        max_output_bytes=4096,
        max_total_output_bytes=8192,
    )
    try:
        plan = _cold_plan(max_rows=50)
        # Fill all 3 slots with completed jobs from analyst:a; 4th job from admin must evict oldest terminal job.
        for _ in range(4):
            jid = manager.submit(plan, lambda: [{"x": 1}], owner_key="analyst:a")
            for _ in range(100):
                job = manager.get(jid)
                if job and job.state is HistoricalJobState.COMPLETED:
                    break
                time.sleep(0.01)
            assert job is not None and job.state is HistoricalJobState.COMPLETED

        admin_jid = manager.submit(plan, lambda: [{"x": 2}], owner_key="admin")
        for _ in range(100):
            admin_job = manager.get(admin_jid)
            if admin_job and admin_job.state is HistoricalJobState.COMPLETED:
                break
            time.sleep(0.01)
        assert admin_job is not None and admin_job.state is HistoricalJobState.COMPLETED

        # Row overhead floor: 20 empty dicts `{}` must exceed 20 * 256 = 5120 > max_output_bytes=4096 -> FAILED
        tiny_jid = manager.submit(plan, lambda: [{} for _ in range(20)], owner_key="analyst:b")
        for _ in range(100):
            tiny_job = manager.get(tiny_jid)
            if tiny_job and tiny_job.state is HistoricalJobState.FAILED:
                break
            time.sleep(0.01)
        assert tiny_job is not None and tiny_job.state is HistoricalJobState.FAILED
        assert "byte limit" in (tiny_job.error or "")

        # When all slots are genuinely ACTIVE, reject with HistoricalJobLimitError (HTTP 429) not plain RuntimeError.
        gate = threading.Event()
        try:
            manager.submit(plan, lambda: (gate.wait(2.0), iter(()))[1], owner_key="owner:1")
            manager.submit(plan, lambda: (gate.wait(2.0), iter(()))[1], owner_key="owner:1")
            manager.submit(plan, lambda: (gate.wait(2.0), iter(()))[1], owner_key="owner:2")
            with pytest.raises(HistoricalJobLimitError) as exc_info:
                manager.submit(plan, lambda: [{"x": 3}], owner_key="owner:3")
            assert exc_info.value.status_code == 429
        finally:
            gate.set()
    finally:
        manager.shutdown(wait=True)


def test_w1_log_field_audit_enforces_route_level_require_admin() -> None:
    """GET /api/services/{service_id}/log-field-audit must enforce route-level Depends(require_admin)."""
    assert _router_has_require_admin(audit_mod.router)
    client = _make_analyst_app(audit_mod.router)
    resp = client.get("/api/services/svc-d1/log-field-audit")
    assert resp.status_code == 403
