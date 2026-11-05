from __future__ import annotations

import asyncio
import json
import threading
import time
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from functools import partial
from io import StringIO
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from backend import config
from backend.core.request_context import get_source, require_admin
from backend.main import app
from backend.routers.admin import events as ev
from backend.routers.admin import health as sampler
from backend.routers.admin import router as admin_router
from backend.utils.remote_access import _is_blocked_path
from tests._helpers.sources import make_source
from tests._helpers.wire import await_until
from tests.conftest import MOCK_SERVICE_ID

_make_source = partial(make_source, "svc-admin-test", kind="cloud")
Usage = namedtuple("Usage", ["total", "used", "free"])
Snippet = namedtuple("Snippet", ["name"])
EXPECTED_KEYS = (
    "health_snapshot",
    "metric_history_1h",
    "queries_summary",
    "slow_queries_count",
    "log_accounting",
    "metadata_storage",
    "system_jobs",
)


class _FakeRequest:
    async def is_disconnected(self) -> bool:
        return False


async def _drive(channels: str, service_id: str | None, n_frames: int) -> list[dict]:
    resp = await ev.admin_events_stream(
        request=_FakeRequest(),  # type: ignore[arg-type]
        channels=channels,
        service_id=service_id,
    )
    agen = resp.body_iterator
    frames: list[dict] = []
    try:
        for _ in range(n_frames):
            frames.append(json.loads(await agen.__anext__()))
    finally:
        await agen.aclose()
    return frames


@pytest.mark.asyncio
async def test_offer_drops_oldest_when_full():
    q: asyncio.Queue = asyncio.Queue(maxsize=2)
    for i in range(5):
        ev._offer(q, {"channel": "system-metrics", "data": {"n": i}})
    drained = [q.get_nowait() for _ in range(q.qsize())]
    assert len(drained) == 2 and drained[-1]["data"]["n"] == 4 and drained[0]["data"]["n"] > 2


@pytest.mark.parametrize(
    ("channels", "expected_err"),
    [("sync-status,bogus", "unknown_channel"), (" , ", "channels_required")],
)
def test_events_stream_channel_validation_422(channels, expected_err):
    with TestClient(app) as c:
        resp = c.get("/api/admin/events/stream", params={"channels": channels})
        assert resp.status_code == 422 and resp.json()["detail"]["error"] == expected_err


@pytest.mark.asyncio
async def test_sync_status_channel_wraps_initial_and_published(monkeypatch):
    monkeypatch.setattr(ev, "compute_sync_status_cached", lambda _sid: {"local_rows": 7})

    async def _fake_subscribe(_svc):
        yield {"local_rows": 8, "latest_log_at": "2026-06-26T00:00:00Z"}

    monkeypatch.setattr(ev.sync_status_publisher, "subscribe", _fake_subscribe)
    frames = await _drive("sync-status", "svc-1", n_frames=2)
    assert frames[0] == {"channel": "sync-status", "data": {"local_rows": 7}}
    assert frames[1]["channel"] == "sync-status" and frames[1]["data"]["local_rows"] == 8


@pytest.mark.asyncio
async def test_sync_status_channel_polls_when_publisher_idle(monkeypatch):
    snapshots = iter(
        [
            {"local_rows": 10, "latest_log_at": "2026-11-02T21:00:00Z"},
            {"local_rows": 10, "latest_log_at": "2026-11-02T21:00:00Z"},
            {"local_rows": 25, "latest_log_at": "2026-11-02T21:00:05Z"},
        ]
    )
    monkeypatch.setattr(ev, "compute_sync_status_cached", lambda _sid: next(snapshots))
    monkeypatch.setattr(ev, "_SYNC_STATUS_SAMPLE_INTERVAL_SECONDS", 0.01)

    async def _idle_subscribe(_svc):
        await asyncio.sleep(60)
        yield {}

    monkeypatch.setattr(ev.sync_status_publisher, "subscribe", _idle_subscribe)
    frames = await _drive("sync-status", "svc-hs-poll", n_frames=2)
    assert frames[0]["data"]["local_rows"] == 10
    assert frames[1]["data"]["local_rows"] == 25
    assert frames[1]["data"]["latest_log_at"] == "2026-11-02T21:00:05Z"


@pytest.mark.asyncio
async def test_system_metrics_channel_wraps_sample(monkeypatch):
    bundle = {"health_snapshot": {"vcpus": 4}, "slow_queries_count": None}

    async def _fake_cached(_sid):
        return bundle

    monkeypatch.setattr(ev, "sample_system_metrics_cached", _fake_cached)
    frames = await _drive("system-metrics", "svc-1", n_frames=1)
    assert frames[0] == {"channel": "system-metrics", "data": bundle}


@pytest.mark.asyncio
async def test_share_channel_wraps_payload_serviceless(monkeypatch):
    import backend.utils.tunnel as tunnel

    payload = {"sharing_active": True, "public_url": "https://x.test", "active_session_count": 1}
    monkeypatch.setattr(tunnel, "build_share_live_payload", lambda: payload)
    frames = await _drive("share", None, n_frames=1)
    assert frames[0] == {"channel": "share", "data": payload}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("channels", "svc_id"),
    [("sync-status,cron-runs,system-metrics", None), ("system-metrics", "svc-1")],
)
async def test_stream_skips_unneeded_or_serviceless_feeders(monkeypatch, channels, svc_id):
    async def _fake_cached(_sid):
        return {"health_snapshot": {"vcpus": 2}}

    monkeypatch.setattr(ev, "sample_system_metrics_cached", _fake_cached)
    called = {"sync": False, "cron": False}

    async def _sync_sub(_svc):
        called["sync"] = True
        yield {}

    async def _cron_sub(_svc):
        called["cron"] = True
        yield {}

    monkeypatch.setattr(ev.sync_status_publisher, "subscribe", _sync_sub)
    monkeypatch.setattr(ev.cron_runs_publisher, "subscribe", _cron_sub)
    frames = await _drive(channels, svc_id, n_frames=1)
    assert frames[0]["channel"] == "system-metrics" and called == {"sync": False, "cron": False}


@pytest.mark.asyncio
async def test_cleanup_cancels_publisher_subscription(monkeypatch):
    subs: dict[str, int] = {}

    async def _fake_subscribe(sid: str):
        subs[sid] = subs.get(sid, 0) + 1
        try:
            await asyncio.Future()
            yield {}
        finally:
            subs[sid] = max(0, subs.get(sid, 1) - 1)

    monkeypatch.setattr(ev.sync_status_publisher, "subscribe", _fake_subscribe)
    monkeypatch.setattr(ev.sync_status_publisher, "subscriber_count", lambda sid: subs.get(sid, 0))
    monkeypatch.setattr(ev, "compute_sync_status_cached", lambda _sid: None)

    async def _fake_cached(_sid):
        return {"health_snapshot": {}}

    monkeypatch.setattr(ev, "sample_system_metrics_cached", _fake_cached)
    resp = await ev.admin_events_stream(
        request=_FakeRequest(),  # type: ignore[arg-type]
        channels="sync-status,system-metrics",
        service_id="svc-cleanup",
    )
    agen = resp.body_iterator
    json.loads(await agen.__anext__())
    await await_until(lambda: ev.sync_status_publisher.subscriber_count("svc-cleanup") == 1, timeout=2.0)
    await agen.aclose()
    await await_until(lambda: ev.sync_status_publisher.subscriber_count("svc-cleanup") == 0, timeout=2.0)


@pytest.mark.asyncio
async def test_initial_snapshot_runs_off_event_loop_thread(monkeypatch):
    invoked: list[str] = []

    def _fake_compute(_sid):
        invoked.append(threading.current_thread().name)
        return {"local_rows": 1}

    monkeypatch.setattr(ev, "compute_sync_status_cached", _fake_compute)

    async def _empty_subscribe(_svc):
        return
        yield

    monkeypatch.setattr(ev.sync_status_publisher, "subscribe", _empty_subscribe)
    await _drive("sync-status", "svc-thread", n_frames=1)
    assert invoked and all(t != threading.main_thread().name for t in invoked)


def test_get_pop_locations(client):
    fake_pops = [
        {"code": "LAX", "name": "Los Angeles", "coordinates": {"latitude": 33.9, "longitude": -118.4}},
        {"code": "SJC", "name": "San Jose", "coordinates": {"latitude": 37.4, "longitude": -121.9}},
    ]
    with patch("backend.utils.pop_utils.get_pop_locations", return_value=fake_pops):
        resp = client.get("/api/admin/pop-locations")
    assert resp.status_code == 200 and len(resp.json()["pops"]) == 2

    with patch("backend.utils.pop_utils.get_pop_locations", return_value=[]):
        resp2 = client.get("/api/admin/pop-locations")
    assert resp2.status_code == 200 and resp2.json()["pops"] == []


def test_sync_status_returns_configured_false_when_no_service():
    with patch("backend.core.fos_client.get_source_for_service", return_value=None), TestClient(app) as c:
        resp = c.get("/api/sync-status")
    assert resp.status_code == 200 and resp.json()["configured"] is False


def test_sync_status_error_mapping(client):
    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.fos_client.get_source_for_service", return_value=fake_src),
        patch(
            "backend.routers.admin.sync_status.compute_sync_status_cached",
            side_effect=RuntimeError("config corrupt"),
        ),
    ):
        resp = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 500
    assert resp.json()["detail"]["error"] == "sync_status_failed"


def test_sync_status_routes_high_scale_and_fallback_modes(client):
    fake_src = {"name": "high_scale_svc", "service_id": "HS123", "bucket": "b"}
    cached_metrics = {"configured": True, "local_rows": 1000, "latest_log_at": "2026-09-16T06:00:00Z"}
    with (
        patch("backend.core.fos_client.get_source_for_service", return_value=fake_src),
        patch("backend.routers.admin.sync_status.compute_sync_status_cached", return_value=cached_metrics),
    ):
        resp = client.get("/api/sync-status?service_id=HS123")
        assert resp.status_code == 200 and resp.json()["local_rows"] == 1000

    fake_std = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.fos_client.get_source_for_service", return_value=fake_std),
        patch("backend.routers.admin.sync_status.compute_sync_status_cached", return_value=None),
        patch("backend.config.get_status", return_value={"configured": True, "local_rows": 42}),
        patch("backend.cron.scheduler.get_latest_progress_for_service", return_value=None),
        patch("backend.config.load_config", return_value={}),
    ):
        resp2 = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
        assert resp2.status_code == 200 and resp2.json()["local_rows"] == 42


def test_log_extents_endpoint_variants(client):
    with patch("backend.core.fos_client.get_source_for_service", return_value=None), TestClient(app) as c:
        r0 = c.get("/api/log-extents")
    b0 = r0.json()
    assert r0.status_code == 200 and b0["configured"] is False
    assert "ngwaf_workspace_id" not in b0 and "active_run" not in b0

    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    cached = {
        "earliest_log_at": "2026-06-09T00:00:00Z",
        "latest_log_at": "2026-06-10T12:34:56Z",
        "ngwaf_workspace_id": "ws-should-not-leak",
        "active_run": {"task": "sync", "status": "running"},
    }
    with (
        patch("backend.core.fos_client.get_source_for_service", return_value=fake_src),
        patch("backend.config.get_status", return_value=cached),
    ):
        r1 = client.get("/api/log-extents", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    b1 = r1.json()
    assert r1.status_code == 200 and b1["configured"] is True
    assert b1["earliest_log_at"] == "2026-06-09T00:00:00Z" and b1["latest_log_at"] == "2026-06-10T12:34:56Z"
    assert "ngwaf_workspace_id" not in b1 and "active_run" not in b1

    with (
        patch("backend.core.fos_client.get_source_for_service", return_value=fake_src),
        patch("backend.config.get_status", return_value={}),
    ):
        r2 = client.get("/api/log-extents", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    b2 = r2.json()
    assert (
        r2.status_code == 200
        and b2["configured"] is True
        and b2["earliest_log_at"] is None
        and b2["latest_log_at"] is None
    )


def test_admin_ingested_files_get_and_500_pairs(client):
    ret = [
        {
            "file_name": "log-001.gz",
            "ingested_at": "2026-01-01T00:00:00Z",
            "row_count": 100,
            "file_size_bytes": 5000,
        }
    ]
    with patch("backend.core.fos_client.get_ingested_files", return_value=ret):
        resp = client.get("/api/admin/ingested-files", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["files"]) == 1 and body["files"][0]["file_name"] == "log-001.gz"

    with patch("backend.core.fos_client.get_ingested_files", side_effect=RuntimeError("boom")):
        resp_err = client.get("/api/admin/ingested-files", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp_err.status_code == 500


def test_usage_logging_get_and_patch(client, tmp_path, monkeypatch):
    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_usage_logging_config({"enabled": True, "retention_days": 60, "class_a_rate_per_1k": 0.123})
    resp = client.get("/api/admin/usage-logging")
    assert resp.status_code == 200 and resp.json()["enabled"] is True and resp.json()["retention_days"] == 60

    r_patch = client.patch(
        "/api/admin/usage-logging",
        json={"enabled": True, "retention_days": 90, "junk_field": "should_be_dropped", "another_unknown": 123},
    )
    b_patch = r_patch.json()
    assert r_patch.status_code == 200 and b_patch["enabled"] is True and b_patch["retention_days"] == 90
    assert "junk_field" not in b_patch and "another_unknown" not in b_patch

    config.save_usage_logging_config({"enabled": False, "retention_days": 30})
    r_part = client.patch("/api/admin/usage-logging", json={"enabled": True})
    assert r_part.status_code == 200 and r_part.json()["enabled"] is True and r_part.json()["retention_days"] == 30


def test_usage_log_endpoint_pagination_and_validation(client):
    fake_rows = [
        {
            "id": i,
            "timestamp": f"2026-01-01T00:00:0{i}Z",
            "service_id": MOCK_SERVICE_ID,
            "operation_class": cls,
            "operation_type": op,
            "url": None,
            "bytes": None,
            "duration_ms": None,
            "function_name": None,
            "process_context": None,
            "status": "ok",
        }
        for i, (cls, op) in [(1, ("A", "PUT_OBJECT")), (2, ("B", "GET_OBJECT"))]
    ]
    fake_agg = {
        "total_class_a": 100,
        "total_class_b": 200,
        "total_cdn_downloads": 50,
        "total_cdn_bytes": 10485760,
        "total_fos_bytes": 0,
        "class_a_breakdown": {"PUT_OBJECT": 100},
        "class_b_breakdown": {"GET_OBJECT": 200},
    }
    with patch("backend.core.metadata.get_usage_logs", return_value=(fake_rows, 2, fake_agg)):
        resp = client.get(
            "/api/admin/usage-log",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"page": 1, "page_size": 10},
        )
    body = resp.json()
    assert resp.status_code == 200 and body["total"] == 2 and len(body["entries"]) == 2
    assert body["aggregate"]["total_class_a"] == 100 and body["aggregate"]["total_class_b"] == 200

    for bad_params in ({"page_size": 99999}, {"page": 0}):
        r_bad = client.get("/api/admin/usage-log", headers={"x-fastly-service-id": MOCK_SERVICE_ID}, params=bad_params)
        assert r_bad.status_code == 422


def test_system_jobs_endpoint(client):
    resp = client.get("/api/admin/system-jobs")
    assert resp.status_code == 200 and isinstance(resp.json().get("jobs"), list)
    ids = {j["id"] for j in resp.json()["jobs"]}
    assert {"share_audit_purge", "bot_data_refresh", "rdns_enrichment"} <= ids


def _get_health(client):
    resp = client.get("/api/admin/health-snapshot")
    assert resp.status_code == 200
    return resp.json()


def test_health_snapshot_returns_shape_with_known_collectors(client):
    body = _get_health(client)
    for k in (
        "load",
        "vcpus",
        "memory",
        "data_mount",
        "root_disk",
        "in_flight_runs",
        "recent_cron_failures",
        "observability",
        "config_backup",
        "celery",
    ):
        assert k in body


@pytest.mark.parametrize(
    ("target", "exc", "field", "expected"),
    [
        ("os.getloadavg", OSError("no load avg"), "load", None),
        ("builtins.open", FileNotFoundError("no /proc/meminfo"), "memory", None),
        ("backend.cron.scheduler.list_active_runs", RuntimeError("boom"), "in_flight_runs", []),
        ("multiprocessing.cpu_count", NotImplementedError(), "vcpus", None),
    ],
)
def test_health_snapshot_collector_failures_degrade_gracefully(client, target, exc, field, expected):
    with patch(target, side_effect=exc):
        body = _get_health(client)
    assert body[field] == expected


def test_health_snapshot_celery_and_disk_failures(client):
    with (
        patch("backend.config.DEPLOYMENT_MODE", "high_scale"),
        patch("backend.celery_app.app.control.inspect", side_effect=RuntimeError("broker down")),
    ):
        assert _get_health(client)["celery"] is None

    with patch("shutil.disk_usage", side_effect=OSError("missing mount")):
        body = _get_health(client)
    assert body["data_mount"] is None and body["root_disk"] is None


def test_health_snapshot_memory_and_disk_stats(client):
    fake_meminfo = (
        "MemTotal:        8388608 kB\nMemFree:         1048576 kB\n"
        "MemAvailable:    4194304 kB\nBuffers:          524288 kB\n"
    )
    real_open = open
    with patch(
        "builtins.open",
        side_effect=lambda p, *a, **kw: StringIO(fake_meminfo) if p == "/proc/meminfo" else real_open(p, *a, **kw),
    ):
        mem = _get_health(client)["memory"]
    assert mem == {"total_mb": 8192, "available_mb": 4096, "used_pct": 50.0}

    with patch("shutil.disk_usage", return_value=Usage(total=100 * 1024**3, used=25 * 1024**3, free=75 * 1024**3)):
        dm = _get_health(client)["data_mount"]
    assert (dm["total_gb"], dm["used_gb"], dm["free_gb"], dm["used_pct"]) == (100.0, 25.0, 75.0, 25.0)

    with patch("shutil.disk_usage", return_value=Usage(total=0, used=0, free=0)):
        assert _get_health(client)["data_mount"]["used_pct"] is None


def test_health_snapshot_in_flight_and_fos(client):
    fake_runs = [
        {"run_id": 101, "service_id": "svc-a", "task": "sync", "started_at": "2026-06-12T10:00:00Z", "rows_done": 1234},
        {"run_id": 102, "service_id": "svc-b", "task": "metadata_cleanup", "started_at": 1781758323.4424183},
    ]
    with patch("backend.cron.scheduler.list_active_runs", return_value=fake_runs):
        runs = _get_health(client)["in_flight_runs"]
    assert len(runs) == 2 and set(runs[0].keys()) == {"run_id", "service_id", "task", "started_at"}
    assert isinstance(runs[1]["started_at"], str) and "T" in runs[1]["started_at"]

    svc_cfg = [{"service_id": "svc-a", "name": "svc-a"}]
    with (
        patch("backend.config.list_configs", return_value=svc_cfg),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch(
            "backend.core.fos_client.fos_reachable", return_value={"reachable": True, "latency_ms": 45}
        ) as mock_reach,
    ):
        r_fos = client.get("/api/admin/health-snapshot?probe_fos=True")
        assert r_fos.status_code == 200 and r_fos.json()["fos"]["svc-a"]["reachable"] is True
        mock_reach.assert_called_once()

    with (
        patch("backend.config.list_configs", return_value=svc_cfg),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.fos_client.fos_reachable", side_effect=RuntimeError("S3 connection timed out")),
    ):
        r_fos_err = client.get("/api/admin/health-snapshot?probe_fos=True").json()["fos"]["svc-a"]
        assert r_fos_err["reachable"] is False and "timed out" in r_fos_err["error"]


@pytest.fixture
def overview_client():
    with TestClient(app) as c:
        yield c


def test_admin_sync_status_and_health_aliases(overview_client: TestClient):
    src = _make_source()
    fake_payload = {
        "configured": True,
        "busy": False,
        "storage_mode": "clickhouse",
        "access_level": "read_write",
        "local_rows": 1250,
        "earliest_log_at": "2026-10-15T20:00:00Z",
        "latest_log_at": "2026-10-16T20:00:00Z",
    }
    with (
        patch("backend.core.fos_client.get_source_for_service", return_value=src),
        patch("backend.routers.admin.sync_status.compute_sync_status_cached", return_value=fake_payload),
    ):
        for p in ("/api/admin/sync-status", "/api/sync-status"):
            r = overview_client.get(f"{p}?service_id=svc-admin-test&skip_fos=true")
            assert r.status_code == 200 and r.json()["local_rows"] == 1250 and r.json()["access_level"] == "read_write"

    for path in ("/api/admin/system-metrics", "/api/admin/health", "/api/admin/health-snapshot"):
        resp = overview_client.get(path)
        assert resp.status_code == 200
        assert {"load", "vcpus", "in_flight_runs", "recent_cron_failures", "observability"} <= set(resp.json())


def test_admin_log_accounting_routes(overview_client: TestClient):
    src = _make_source()
    app.dependency_overrides[get_source] = lambda: src
    fake_accounting = {
        "by": "hour",
        "from_ts": "2026-10-15T20:00:00Z",
        "to_ts": "2026-10-16T20:00:00Z",
        "buckets": [],
        "totals": {
            "fastly_requests": 1000,
            "our_rows": 1000,
            "gap": 0,
            "gap_pct": 0.0,
            "worst_bucket_ts": None,
            "worst_bucket_gap_pct": None,
        },
        "sustained_loss": None,
        "catchup": {"latest_ingest_ts": "2026-10-16T19:59:50Z", "lag_seconds": 10, "status": "caught_up"},
        "section_timings": [],
    }
    try:
        with patch("backend.routers.admin.log_accounting.compute_log_accounting", return_value=fake_accounting):
            ra1 = overview_client.get(f"/api/admin/log-accounting/{src['service_id']}?hours=24")
            ra2 = overview_client.get(f"/api/admin/log-accounting?service_id={src['service_id']}&hours=24")
            assert ra1.status_code == 200 and ra1.json()["totals"]["fastly_requests"] == 1000
            assert ra2.status_code == 200 and ra2.json()["totals"]["our_rows"] == 1000
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_admin_manual_cron_trigger_routes_and_rbac(overview_client: TestClient):
    import sys

    src = _make_source()
    app.dependency_overrides[get_source] = lambda: src
    try:
        with (
            patch.dict(
                sys.modules,
                {"backend.repositories.dashboard": SimpleNamespace(invalidate_service=lambda *a, **k: None)},
            ),
            patch(
                "backend.utils.router_utils.start_or_resume_cron",
                return_value={"ok": True, "message": "Started.", "run_id": "42"},
            ) as mock_start,
        ):
            sid = src["service_id"]
            assert overview_client.post(f"/api/admin/sync/{sid}").status_code == 200 and mock_start.called
            assert overview_client.post(f"/api/admin/full-sweep/{sid}").status_code == 200
            assert overview_client.post(f"/api/admin/gap-heal/{sid}").status_code == 200
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_analyst_path_b_rejected_with_403_on_admin_overview_routes():
    for path in (
        "/api/admin/sync-status",
        "/api/sync-status",
        "/api/admin/system-metrics",
        "/api/admin/health",
        "/api/admin/health-snapshot",
        "/api/admin/log-accounting/svc-admin-test",
        "/api/admin/sync/svc-admin-test",
        "/api/admin/full-sweep/svc-admin-test",
        "/api/admin/gap-heal/svc-admin-test",
    ):
        assert _is_blocked_path(path) is True

    fake_req = SimpleNamespace(
        state=SimpleNamespace(analyst_session=SimpleNamespace(session_id="sess_analyst")),
        headers={},
        client=SimpleNamespace(host="203.0.113.55"),
    )
    with pytest.raises(HTTPException) as exc:
        require_admin(fake_req)  # type: ignore[arg-type]
    assert exc.value.status_code == 403


@pytest.mark.parametrize(
    ("endpoint", "patch_target", "payload"),
    [
        (
            "/api/admin/reset-logs",
            "backend.core.reset.reset_service_logs",
            {"confirm": "service123", "delete_raw_logs": False, "preserve_usage_history": True},
        ),
        (
            "/api/admin/reset-rum",
            "backend.core.reset.reset_service_rum",
            {"confirm": "service123", "delete_raw_logs": False},
        ),
    ],
)
def test_reset_endpoints(overview_client, override_source, endpoint, patch_target, payload):
    bad_payload = {**payload, "confirm": "wrong"}
    assert overview_client.post(endpoint, json=bad_payload).status_code == 400

    def _fake_reset(*_a, **_kw):
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "message": "done"}

    with patch(patch_target, side_effect=_fake_reset):
        resp = overview_client.post(endpoint, json=payload)
        assert resp.status_code == 200 and "text/event-stream" in resp.headers["content-type"]
        events = [json.loads(line[5:]) for line in resp.iter_lines() if line.startswith("data:")]
        assert [e["type"] for e in events] == ["status", "done"]


@pytest.mark.parametrize("window", ["30m", "1h", "6h", "24h", "7d"])
def test_metric_history_batch_windows(window: str):
    with TestClient(app) as c:
        r = c.get(f"/api/admin/metric-history/batch?since={window}")
    assert r.status_code == 200
    assert r.json()["series"] == {}


@pytest.mark.parametrize("bad_window", ["invalid", "0h", "-5h", "1w", "abc", ""])
def test_metric_history_batch_rejects_invalid_window(bad_window: str):
    with TestClient(app) as c:
        r = c.get(f"/api/admin/metric-history/batch?since={bad_window}")
    assert r.status_code == 400 and "Invalid lookback window" in r.json()["detail"]["error"]


def test_metric_history_batch_rejects_analyst_path_b():
    assert _is_blocked_path("/api/admin/metric-history/batch") is True
    analyst_app = FastAPI()

    @analyst_app.middleware("http")
    async def _stamp_analyst(request: Request, call_next):
        request.state.analyst_session = SimpleNamespace(
            session_id="sess-analyst-1", service_ids=["svc-a"], email="analyst@example.com"
        )
        return await call_next(request)

    analyst_app.include_router(admin_router)
    with TestClient(analyst_app) as c:
        r = c.get("/api/admin/metric-history/batch?since=1h")
    assert r.status_code == 403 and r.json().get("detail", r.json())["error"] == "admin_only"


@pytest.fixture
def _reset_metrics_cache():
    sampler.reset_metrics_cache()
    yield
    sampler.reset_metrics_cache()


@pytest.mark.usefixtures("_reset_metrics_cache")
def test_sampler_shape_and_isolation():
    payload = sampler.sample_system_metrics("svc-test")
    assert all(k in payload for k in EXPECTED_KEYS)
    json.dumps(payload)

    with patch.object(sampler, "_sample_health_snapshot", side_effect=RuntimeError("boom")):
        p_fail = sampler.sample_system_metrics("svc-test")
    assert p_fail["health_snapshot"] is None and all(k in p_fail for k in EXPECTED_KEYS)

    p_none = sampler.sample_system_metrics(None)
    assert all(k in p_none for k in EXPECTED_KEYS)
    assert (p_none["slow_queries_count"], p_none["log_accounting"], p_none["metadata_storage"]) == (None, None, None)


@pytest.mark.usefixtures("_reset_metrics_cache")
@pytest.mark.asyncio
async def test_cached_sampler_ttl_single_flight_and_errors(monkeypatch):
    calls: list[str | None] = []

    def _count(sid):
        calls.append(sid)
        return {"health_snapshot": {"n": len(calls)}}

    monkeypatch.setattr(sampler, "sample_system_metrics", _count)
    first = await sampler.sample_system_metrics_cached("svc")
    second = await sampler.sample_system_metrics_cached("svc")
    assert len(calls) == 1 and first is second

    await sampler.sample_system_metrics_cached("a")
    await sampler.sample_system_metrics_cached("b")
    await sampler.sample_system_metrics_cached(None)
    assert calls == ["svc", "a", "b", None]

    sampler.reset_metrics_cache()
    calls.clear()

    def _slow(sid):
        calls.append(sid)
        time.sleep(0.05)
        return {"health_snapshot": {"n": len(calls)}}

    monkeypatch.setattr(sampler, "sample_system_metrics", _slow)
    results = await asyncio.gather(*(sampler.sample_system_metrics_cached("svc") for _ in range(3)))
    assert len(calls) == 1 and all(r == results[0] for r in results)

    sampler.reset_metrics_cache()
    calls.clear()
    monkeypatch.setattr(sampler, "_CACHE_TTL_SECONDS", 0.0)
    monkeypatch.setattr(sampler, "sample_system_metrics", _count)
    await sampler.sample_system_metrics_cached("svc")
    await sampler.sample_system_metrics_cached("svc")
    assert len(calls) == 2

    sampler.reset_metrics_cache()
    calls.clear()

    def _flaky(sid):
        calls.append(sid)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return {"health_snapshot": {"ok": True}}

    monkeypatch.setattr(sampler, "sample_system_metrics", _flaky)
    with pytest.raises(RuntimeError):
        await sampler.sample_system_metrics_cached("svc")
    assert (await sampler.sample_system_metrics_cached("svc")) == {"health_snapshot": {"ok": True}} and len(calls) == 2


def test_bot_sources_and_rdns_endpoints(client):
    fake_sources = [
        {
            "id": "well-known-bots",
            "name": "Well-Known Bots (arcjet)",
            "url": "https://example.com/bots.json",
            "enabled": True,
            "last_updated": "2026-10-11T00:00:00Z",
            "entry_count": 42,
        }
    ]
    fake_rdns = {"total": 120, "pending": 10, "last_enrichment_at": "2026-10-11T00:05:00Z"}
    with (
        patch("backend.utils.bot_sources.get_all_sources_meta", return_value=fake_sources),
        patch("backend.utils.bot_sources.get_stats", return_value=fake_rdns),
    ):
        resp = client.get("/api/admin/bot-sources")
    data = resp.json()
    assert resp.status_code == 200 and data["sources"][0]["entry_count"] == 42 and data["rdns"]["total"] == 120

    fake_meta = {
        "id": "well-known-bots",
        "name": "Well-Known Bots",
        "last_updated": "2026-10-11T02:00:00Z",
        "entry_count": 100,
    }
    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=fake_meta) as mock_fetch:
        r_ref = client.post("/api/admin/bot-sources/well-known-bots/refresh")
    assert r_ref.status_code == 200 and r_ref.json()["ok"] is True and r_ref.json()["source"]["entry_count"] == 100
    mock_fetch.assert_called_once_with("well-known-bots")

    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=ValueError("Unknown bot source")):
        assert client.post("/api/admin/bot-sources/invalid-source/refresh").status_code == 404
    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=RuntimeError("Connection timeout")):
        assert client.post("/api/admin/bot-sources/well-known-bots/refresh").status_code == 502

    with patch(
        "backend.utils.bot_sources.refresh_all_sources",
        return_value=[{"id": "well-known-bots", "entry_count": 100}, {"id": "tor-exit-nodes", "entry_count": 50}],
    ):
        r_all = client.post("/api/admin/bot-sources/refresh").json()
        assert r_all["ok"] is True and r_all["updated_count"] == 2 and r_all["failed_count"] == 0
        r_alias = client.post("/api/admin/bots/refresh").json()
        assert r_alias["ok"] is True and r_alias["updated_count"] == 2

    with patch(
        "backend.utils.bot_sources.refresh_all_sources",
        return_value=[
            {"id": "well-known-bots", "entry_count": 100},
            {"id": "tor-exit-nodes", "failed": True, "error": "503 Unavailable", "entry_count": 0},
        ],
    ):
        r_part = client.post("/api/admin/bot-sources/refresh").json()
        assert r_part["ok"] is False and r_part["updated_count"] == 1 and r_part["failed_count"] == 1

    with patch("backend.utils.bot_sources.refresh_all_sources", side_effect=RuntimeError("Fatal")):
        assert client.post("/api/admin/bot-sources/refresh").status_code == 502

    fake_summary = {"resolved": 25, "errors": 0, "discovered": 5, "batch_limit": 50}
    with (
        patch("backend.utils.bot_sources.enrich_batch", return_value=fake_summary) as mock_enrich,
        patch("backend.utils.bot_sources.get_stats", return_value={"total": 500, "resolved": 450, "pending": 50}),
    ):
        re = client.post("/api/admin/rdns/enrich?limit=50")
        assert re.status_code == 200 and re.json()["summary"] == fake_summary
        mock_enrich.assert_called_once_with(limit=50)
        rs = client.get("/api/admin/rdns/stats")
        assert rs.status_code == 200 and rs.json()["stats"]["total"] == 500


# ── Merged from tests/routers/test_high_scale.py ──

from dataclasses import replace

from backend.core.request_context import build_request_context
from backend.core.request_telemetry import current_clickhouse_scope
from backend.high_scale.archive import ExportManager, ServingWatermark, get_export_manager
from backend.high_scale.ingest_controller import HighScaleService, HighScaleServiceRegistry
from backend.utils.remote_access import TimeBounds


class _QueryClient:
    def __init__(self, rows):
        self.rows = rows
        self.scopes = []

    def execute(self, sql, params=None):
        self.scopes.append(current_clickhouse_scope())
        return self.rows


def _service(service_id="test-service-id"):
    watermark = ServingWatermark(
        service_id,
        "request",
        1,
        datetime(2026, 9, 11, 20, 0, tzinfo=UTC),
        datetime(2026, 9, 11, 20, 5, tzinfo=UTC),
        "cursor",
        "event-2",
        "event-2",
        True,
    )
    client = _QueryClient(
        [
            {
                "event_id": "event-1",
                "timestamp": datetime(2026, 9, 11, 20, 1, tzinfo=UTC),
                "service_id": service_id,
                "client_ip": "203.0.113.42",
                "url": "/",
            },
            {
                "event_id": "event-2",
                "timestamp": datetime(2026, 9, 11, 20, 2, tzinfo=UTC),
                "service_id": service_id,
                "client_ip": "203.0.113.43",
                "url": "/next",
            },
        ]
    )
    rum_watermark = ServingWatermark(
        service_id,
        "rum_vitals",
        1,
        watermark.coverage_start,
        watermark.coverage_end,
        "cursor",
        "rum-event-2",
        "rum-event-2",
        True,
    )
    cmcd_watermark = ServingWatermark(
        service_id,
        "cmcd",
        1,
        watermark.coverage_start,
        watermark.coverage_end,
        "cursor",
        "cmcd-event-2",
        "cmcd-event-2",
        True,
    )
    return HighScaleService(
        service_id,
        client,
        b"test-secret",
        watermark,
        {"rum_vitals": rum_watermark},
        cmcd_watermark,
    )


def _registry(service=None):
    return HighScaleServiceRegistry({service.service_id: service} if service else {})


def _analyst_context(source, con):
    from tests.conftest import make_test_request_context

    session = SimpleNamespace(
        service_ids=[source["service_id"]],
        pii_policy={"mask_ips": True},
    )
    return make_test_request_context(
        con=con,
        service_id=source["service_id"],
        source=source,
        session=session,
        path="/api/high-scale/services/{service_id}/request-facts",
        time_bounds=TimeBounds(
            start=datetime(2026, 9, 11, 19, 0, tzinfo=UTC),
            end=datetime(2026, 9, 11, 21, 0, tzinfo=UTC),
        ),
    )


def test_request_facts_route_registered():
    assert "/api/high-scale/services/{service_id}/request-facts" in app.openapi()["paths"]


def test_rum_facts_route_registered():
    assert "/api/high-scale/services/{service_id}/rum-facts/{domain}" in app.openapi()["paths"]


def test_cmcd_facts_route_registered():
    assert "/api/high-scale/services/{service_id}/cmcd-facts" in app.openapi()["paths"]


def test_export_routes_registered():
    paths = app.openapi()["paths"]
    assert "/api/high-scale/services/{service_id}/exports" in paths
    assert "/api/high-scale/services/{service_id}/exports/{export_id}" in paths
    assert "/api/high-scale/services/{service_id}/exports/{export_id}/cancel" in paths


def test_admin_can_request_and_read_export(client, test_service_source, monkeypatch):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    manager = ExportManager(max_workers=1, max_rows=10, max_bytes=10_000)
    monkeypatch.setattr("backend.routers.admin.high_scale.metadata.record_audit", lambda *args, **kwargs: None)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    app.dependency_overrides[get_export_manager] = lambda: manager
    try:
        response = client.post(
            f"/api/high-scale/services/{test_service_source['service_id']}/exports",
            json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
        )
        assert response.status_code == 200, response.text
        export_id = response.json()["export_id"]
        status = client.get(f"/api/high-scale/services/{test_service_source['service_id']}/exports/{export_id}")
        assert status.status_code == 200, status.text
        assert status.json()["state"] == "completed"
        assert status.json()["rows_written"] == 2
    finally:
        manager.shutdown()


def test_export_cancel_is_admin_only(client, test_service_source, monkeypatch):
    from fastapi import HTTPException

    from backend.core.request_context import require_admin
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    manager = ExportManager(max_workers=1)
    monkeypatch.setattr("backend.routers.admin.high_scale.metadata.record_audit", lambda *args, **kwargs: None)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    app.dependency_overrides[get_export_manager] = lambda: manager

    def reject_non_admin():
        raise HTTPException(status_code=403, detail={"error": "admin_only"})

    app.dependency_overrides[require_admin] = reject_non_admin
    try:
        response = client.post(
            f"/api/high-scale/services/{test_service_source['service_id']}/exports",
            json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
        )
        assert response.status_code == 403
    finally:
        manager.shutdown()
        app.dependency_overrides.pop(require_admin, None)


def test_admin_can_query_request_facts(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
            "limit": 1,
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["rows"][0]["client_ip"] == "203.0.113.42"
    assert body["metadata"]["watermark"]["service_id"] == test_service_source["service_id"]
    assert body["next_cursor"]


def test_analyst_request_facts_masks_client_ip(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    app.dependency_overrides[build_request_context] = lambda: _analyst_context(test_service_source, None)
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["rows"][0]["client_ip"] == "203.0.113.xxx"


def test_request_facts_rejects_service_binding_mismatch(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    class _MismatchRegistry:
        def resolve(self, service_id):
            return _service("other-service-id")

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _MismatchRegistry()
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "high_scale_service_mismatch"


def test_request_facts_rejects_non_high_scale_service(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry()
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_service_not_configured"


def test_request_facts_rejects_invalid_cursor(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
            "cursor": "not-a-valid-cursor",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"]["error"] == "bad_request"


def test_rum_facts_queries_configured_domain(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/rum-facts/rum_vitals",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["metadata"]["watermark"]["domain"] == "rum_vitals"


def test_rum_facts_rejects_unconfigured_domain(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/rum-facts/rum_errors",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_domain_not_configured"


def test_cmcd_facts_queries_configured_domain(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    service = _service()
    watermark_scopes = []

    def watermark():
        watermark_scopes.append(current_clickhouse_scope())
        return service.cmcd_watermark

    scoped_service = replace(service, cmcd_watermark=watermark)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(scoped_service)
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/cmcd-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["metadata"]["watermark"]["domain"] == "cmcd"
    watermark_timing, timing = body["_section_timings"]
    assert watermark_timing["section"] == watermark_scopes[0]["section"] == "cmcd.watermark"
    assert watermark_timing["section_id"] == watermark_scopes[0]["section_id"]
    assert watermark_timing["source"] == watermark_scopes[0]["source"] == "projected_fact"
    assert watermark_timing["activity"] == "overhead"
    assert watermark_timing["section_id"] != timing["section_id"]
    scope = service.client.scopes[0]
    assert timing["section"] == "cmcd.facts"
    assert timing["source"] == scope["source"] == "projected_fact"
    assert timing["section_id"] == scope["section_id"]
    assert timing["timing_kind"] == "client_section_wall"
    assert timing["server_cpu_ms"] is None


class _AggregateQueryClient:
    def __init__(self, rows):
        self.rows = rows
        self.last_sql = None
        self.last_params = None

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.last_params = params
        return self.rows


def test_aggregates_queries_configured_domain_and_dimension(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    service = _service()
    object.__setattr__(
        service,
        "client",
        _AggregateQueryClient(
            [
                {"value": "/", "aggregate_count": 3, "total_count": 4},
                {"value": "/next", "aggregate_count": 1, "total_count": 4},
            ]
        ),
    )
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/aggregates",
        json={
            "domain": "request",
            "dimension": "url",
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["domain"] == "request"
    assert body["dimension"] == "url"
    assert body["request_count"] == 4
    assert body["top_values"] == [{"value": "/", "count": 3}, {"value": "/next", "count": 1}]
    assert body["exact"] is True


def test_aggregates_rejects_unconfigured_domain(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/aggregates",
        json={
            "domain": "rum_errors",
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_domain_not_configured"


def test_aggregates_masks_client_ip_dimension_for_analysts(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    service = _service()
    object.__setattr__(
        service,
        "client",
        _AggregateQueryClient([{"value": "203.0.113.42", "aggregate_count": 1, "total_count": 1}]),
    )
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)
    app.dependency_overrides[build_request_context] = lambda: _analyst_context(test_service_source, None)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/aggregates",
        json={
            "domain": "request",
            "dimension": "client_ip",
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
        },
    )

    assert response.status_code == 200, response.text
    value = response.json()["top_values"][0]["value"]
    assert value != "203.0.113.42"


def _iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def test_queries_recent_tier_returns_rows_immediately(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    service = _service()
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)
    now = datetime.now(UTC)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/queries",
        json={
            "domain": "request",
            "start_time": _iso(now - timedelta(hours=2)),
            "end_time": _iso(now - timedelta(hours=1)),
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["plan"]["tier"] == "recent_facts"
    assert body["job_id"] is None
    assert len(body["rows"]) == 2


def test_queries_cold_tier_without_archive_configured_reports_unavailable(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    service = _service()
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)
    now = datetime.now(UTC)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/queries",
        json={
            "domain": "request",
            "start_time": _iso(now - timedelta(days=60)),
            "end_time": _iso(now - timedelta(days=60) + timedelta(hours=1)),
        },
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_cold_query_not_configured"


def test_queries_cold_tier_submits_and_completes_a_historical_job(client, test_service_source):
    from backend.high_scale.archive import (
        ArchiveArtifact,
        ArchiveManifest,
        ArchivePublication,
        ArchiveSourceObject,
        InMemoryObjectStore,
    )
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    now = datetime.now(UTC)
    coverage_start = now - timedelta(days=60)
    coverage_end = coverage_start + timedelta(minutes=1)
    rows = ({"event_id": "1", "timestamp": _iso(coverage_start + timedelta(seconds=10))},)
    artifact_bytes = _parquet_bytes(rows)
    source = ArchiveSourceObject("test-service-id", "request", "raw/request/a.gz", "sha256:source", 4, "v1")
    manifest = ArchiveManifest(
        "manifest-1",
        source,
        ArchiveArtifact(
            "s3://archive/artifacts/manifest-1.parquet",
            f"sha256:{_sha256(artifact_bytes)}",
            len(artifact_bytes),
            len(rows),
            len(artifact_bytes),
            "digest",
            "request.v1",
            "normalize.v1",
        ),
        coverage_start,
        coverage_end,
        now + timedelta(days=1),
        now + timedelta(days=2),
        1,
    )
    store = InMemoryObjectStore()
    archive = ArchivePublication(store)
    archive.publish(manifest, artifact_bytes)

    class _Catalog:
        def manifests_covering(self, service_id, domain, start, end):
            return (manifest,)

    service = _service()
    object.__setattr__(service, "manifest_catalog", _Catalog())
    object.__setattr__(service, "archive", archive)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/queries",
        json={
            "domain": "request",
            "start_time": _iso(coverage_start),
            "end_time": _iso(coverage_end),
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["plan"]["tier"] == "cold_job"
    job_id = body["job_id"]
    assert job_id

    import time

    deadline = time.monotonic() + 2
    job_body = None
    while time.monotonic() < deadline:
        job_response = client.get(f"/api/high-scale/services/{test_service_source['service_id']}/queries/{job_id}")
        job_body = job_response.json()
        if job_body["state"] in {"completed", "failed"}:
            break
        time.sleep(0.01)

    assert job_body["state"] == "completed", job_body
    assert job_body["rows"] == [{"event_id": "1", "timestamp": _iso(coverage_start + timedelta(seconds=10))}]


def _parquet_bytes(rows) -> bytes:
    from io import BytesIO

    import pyarrow as pa
    import pyarrow.parquet as pq

    output = BytesIO()
    pq.write_table(pa.Table.from_pylist(list(rows)), output)
    return output.getvalue()


def _sha256(value: bytes) -> str:
    import hashlib

    return hashlib.sha256(value).hexdigest()


def test_request_facts_clickhouse_filters_and_telemetry(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    service = _service()
    agg_client = _AggregateQueryClient(
        [
            {
                "event_id": "event-filtered-1",
                "timestamp": datetime(2026, 9, 11, 20, 1, tzinfo=UTC),
                "service_id": test_service_source["service_id"],
                "status": 503,
                "host": "api.example.com",
                "pop": "IAD",
                "country": "US",
                "client_ip": "203.0.113.42",
                "url": "/v1/checkout",
            }
        ]
    )
    object.__setattr__(service, "client", agg_client)
    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(service)

    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        headers={"X-Page-Load-ID": "page-load-hs-test"},
        json={
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
            "status": 503,
            "domain": "api.example.com",
            "pop": "IAD",
            "country": "US",
            "batch_id": "batch-42",
            "limit": 10,
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["metadata"]["mode"] == "clickhouse"
    assert len(body["rows"]) == 1
    assert body["rows"][0]["status"] == 503
    assert agg_client.last_params["filter_status"] == "503"
    assert agg_client.last_params["filter_domain"] == "api.example.com"
    assert agg_client.last_params["filter_pop"] == "IAD"
    assert agg_client.last_params["filter_country"] == "US"
    assert agg_client.last_params["filter_batch_id"] == "batch-42"
    assert isinstance(body.get("_debug_queries"), list)
    assert len(body["_debug_queries"]) >= 1


def test_request_facts_root_get_and_post_clickhouse(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())

    res_get = client.get(
        "/api/high-scale/request-facts",
        params={
            "service_id": test_service_source["service_id"],
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
            "status": 200,
        },
    )
    assert res_get.status_code == 200, res_get.text
    get_body = res_get.json()
    assert get_body["metadata"]["mode"] == "clickhouse"
    assert len(get_body["rows"]) == 2
    assert get_body["rows"][0]["url"] == "/"


def test_request_facts_root_get_masks_analyst_ip(client, test_service_source):
    from backend.high_scale.ingest_controller import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    app.dependency_overrides[build_request_context] = lambda: _analyst_context(test_service_source, None)

    response = client.get(
        "/api/high-scale/request-facts",
        params={
            "service_id": test_service_source["service_id"],
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
        },
    )
    assert response.status_code == 200, response.text
    rows = response.json()["rows"]
    assert len(rows) == 2
    assert rows[0]["client_ip"] == "203.0.113.xxx"


def test_logs_page_admin_and_service_view_caches_hit_when_pytest_env_unset(client, override_source, monkeypatch):
    from backend.routers.admin.compaction import _METADATA_STORAGE_CACHE
    from backend.routers.admin.downloads import _RAW_TREE_CACHE, _STORAGE_TREE_CACHE
    from backend.routers.admin.sync_status import _INGESTED_FILES_CACHE
    from backend.routers.services.views import _AUDIT_LOGS_CACHE, _CRON_LOGS_CACHE

    for c in (
        _RAW_TREE_CACHE,
        _STORAGE_TREE_CACHE,
        _INGESTED_FILES_CACHE,
        _METADATA_STORAGE_CACHE,
        _AUDIT_LOGS_CACHE,
        _CRON_LOGS_CACHE,
    ):
        c.clear()

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    try:
        with patch("backend.core.fos_client.get_raw_tree_node", return_value={"children": []}) as m_raw:
            assert client.get("/api/admin/raw-tree?prefix=p").status_code == 200
            assert client.get("/api/admin/raw-tree?prefix=p").status_code == 200
            assert m_raw.call_count == 1

        with patch("backend.core.fos_client.get_storage_tree_node", return_value={"children": []}) as m_st:
            assert client.get("/api/admin/storage-tree?prefix=p").status_code == 200
            assert client.get("/api/admin/storage-tree?prefix=p").status_code == 200
            assert m_st.call_count == 1

        with patch("backend.core.fos_client.get_ingested_files", return_value=[]) as m_ing:
            assert client.get("/api/admin/ingested-files").status_code == 200
            assert client.get("/api/admin/ingested-files").status_code == 200
            assert m_ing.call_count == 1

        fake_meta_stats = {
            "tables": {"usage_log": {"rows": 1, "bytes": 10}},
            "db_bytes": 100,
            "db_path": "/tmp/metadata.db",
        }
        with (
            patch("backend.core.metadata.get_metadata_storage_stats", return_value=fake_meta_stats) as m_meta,
            patch("backend.config.load_config", return_value={}),
            patch("backend.core.metadata.is_ingested_files_dedup_active", return_value=True),
        ):
            assert client.get("/api/admin/metadata-storage").status_code == 200
            assert client.get("/api/admin/metadata-storage").status_code == 200
            assert m_meta.call_count == 1

        with patch("backend.core.metadata.get_audit_logs", return_value=(0, [])) as m_audit:
            assert client.get("/api/audit-logs").status_code == 200
            assert client.get("/api/audit-logs").status_code == 200
            assert m_audit.call_count == 1

        with patch("backend.routers.services.views.get_cron_logs", return_value=(0, [])) as m_cron:
            assert client.get("/api/cron-runs").status_code == 200
            assert client.get("/api/cron-runs").status_code == 200
            assert m_cron.call_count == 1
    finally:
        for c in (
            _RAW_TREE_CACHE,
            _STORAGE_TREE_CACHE,
            _INGESTED_FILES_CACHE,
            _METADATA_STORAGE_CACHE,
            _AUDIT_LOGS_CACHE,
            _CRON_LOGS_CACHE,
        ):
            c.clear()


def test_admin_suite_caches_hit_when_pytest_env_unset(client, monkeypatch):
    from backend.routers.admin import admin_queries, health

    for c in (
        health._HEALTH_SNAPSHOT_CACHE,
        health._CELERY_STATUS_CACHE,
        admin_queries._SLOW_QUERIES_CACHE,
    ):
        c.clear()

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    try:
        assert client.get("/api/admin/slow-queries/count?since_hours=24&threshold_ms=1000").status_code == 200
        assert len(admin_queries._SLOW_QUERIES_CACHE) == 1
        assert client.get("/api/admin/slow-queries/count?since_hours=24&threshold_ms=1000").status_code == 200
        assert client.delete("/api/admin/slow-queries").status_code == 200
        assert len(admin_queries._SLOW_QUERIES_CACHE) == 0

        assert client.get("/api/admin/celery/status").status_code == 200
        assert len(health._CELERY_STATUS_CACHE) == 1
        assert client.get("/api/admin/celery/status").status_code == 200

        snap1 = health.health_snapshot(probe_fos=False)
        assert len(health._HEALTH_SNAPSHOT_CACHE) == 1
        snap2 = health.health_snapshot(probe_fos=False)
        assert snap1 == snap2
        health.reset_metrics_cache()
        assert len(health._HEALTH_SNAPSHOT_CACHE) == 0
        assert len(health._CELERY_STATUS_CACHE) == 0
    finally:
        for c in (
            health._HEALTH_SNAPSHOT_CACHE,
            health._CELERY_STATUS_CACHE,
            admin_queries._SLOW_QUERIES_CACHE,
        ):
            c.clear()


@pytest.mark.parametrize(
    ("tag", "expected"),
    [
        ("camp-p10-s3-pg04-cold", "api:GET /api/health @harness=camp-p10-s3-pg04-cold#plid-1"),
        ("bad tag;drop", "api:GET /api/health#plid-1"),
    ],
)
def test_harness_header_tags_process_context_before_page_load_suffix(client, monkeypatch, tag, expected):
    import contextlib

    from backend.utils import telemetry

    seen: list[str] = []
    real_scope = telemetry.process_context_scope

    @contextlib.contextmanager
    def _recording_scope(name):
        seen.append(name)
        with real_scope(name):
            yield

    monkeypatch.setattr(telemetry, "process_context_scope", _recording_scope)
    client.get("/api/health", headers={"X-FLA-Harness": tag, "X-Page-Load-ID": "plid-1"})
    assert expected in seen
