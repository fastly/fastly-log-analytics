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
from backend.deps import get_source, require_admin
from backend.utils.date_utils import iso_z
from backend.utils.remote_access import _is_blocked_path
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from backend import config
from backend.core import metric_snapshots
from backend.core.duckdb import DBBusyError
from backend.main import app
from backend.routers.admin import events as ev
from backend.routers.admin import health as sampler
from backend.routers.admin import router as admin_router
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
    ev.sync_status_publisher.bind_loop(asyncio.get_running_loop())
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
    with patch("backend.core.duckdb.get_source_for_service", return_value=None), TestClient(app) as c:
        resp = c.get("/api/sync-status")
    assert resp.status_code == 200 and resp.json()["configured"] is False


@pytest.mark.parametrize(
    ("exc", "expected_status", "check_busy"),
    [(DBBusyError("locked"), 503, True), (RuntimeError("config corrupt"), 500, False)],
)
def test_sync_status_error_mapping(client, exc, expected_status, check_busy):
    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.core.duckdb.get_sync_status", side_effect=exc),
    ):
        mock_conn.return_value = type("C", (), {"close": lambda self: None})()
        resp = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == expected_status
    if check_busy:
        assert resp.json()["detail"]["busy"] is True


def test_sync_status_routes_high_scale_and_durable_modes(client):
    fake_src = {"name": "high_scale_svc", "service_id": "HS123", "bucket": "b"}
    cached_metrics = {"configured": True, "local_rows": 1000, "latest_log_at": "2026-09-16T06:00:00Z"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.high_scale.registry_bootstrap.get_high_scale_service_registry") as mock_reg,
        patch("backend.routers.admin.sync_status.compute_sync_status_cached", return_value=cached_metrics),
        patch("backend.core.duckdb.get_connection") as mock_conn,
    ):
        mock_reg.return_value.resolve.return_value = object()
        resp = client.get("/api/sync-status?service_id=HS123")
        assert resp.status_code == 200 and resp.json()["local_rows"] == 1000
        mock_conn.assert_not_called()

    fake_std = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_std),
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch("backend.core.duckdb.get_connection") as mock_conn2,
        patch("backend.core.duckdb.get_sync_status", return_value={"configured": True}),
    ):
        mock_conn2.return_value = type("C", (), {"close": lambda self: None})()
        resp2 = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
        assert resp2.status_code == 200
        mock_conn2.assert_called_once_with(source=fake_std, max_wait=5, skip_view_update=True, read_only=True)


def test_log_extents_endpoint_variants(client):
    with patch("backend.core.duckdb.get_source_for_service", return_value=None), TestClient(app) as c:
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
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.config.get_status", return_value=cached),
    ):
        r1 = client.get("/api/log-extents", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    b1 = r1.json()
    assert r1.status_code == 200 and b1["configured"] is True
    assert b1["earliest_log_at"] == "2026-06-09T00:00:00Z" and b1["latest_log_at"] == "2026-06-10T12:34:56Z"
    assert "ngwaf_workspace_id" not in b1 and "active_run" not in b1

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
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


@pytest.mark.parametrize(
    ("url", "target", "ret", "check_fn"),
    [
        (
            "/api/admin/ingested-files",
            "backend.core.duckdb.get_ingested_files",
            [
                {
                    "file_name": "log-001.gz",
                    "ingested_at": "2026-01-01T00:00:00Z",
                    "row_count": 100,
                    "file_size_bytes": 5000,
                }
            ],
            lambda b: len(b["files"]) == 1 and b["files"][0]["file_name"] == "log-001.gz",
        ),
        (
            "/api/admin/iceberg-info",
            "backend.core.storage.get_table_info",
            {"table_name": "logs_test_service", "snapshots": 7, "data_files": 42, "size_bytes": 52428800},
            lambda b: b["data_files"] == 42 and b["snapshots"] == 7,
        ),
        (
            "/api/admin/iceberg-calendar",
            "backend.core.storage.get_snapshot_calendar",
            {"2026-01-01": {"data_files": 12, "size_bytes": 4096}},
            lambda b: b["2026-01-01"]["data_files"] == 12 and "_debug_calls" in b,
        ),
    ],
)
def test_admin_get_and_500_pairs(client, url, target, ret, check_fn):
    with patch(target, return_value=ret):
        resp = client.get(url, headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 200 and check_fn(resp.json())

    with patch(target, side_effect=RuntimeError("boom")):
        resp_err = client.get(url, headers={"x-fastly-service-id": MOCK_SERVICE_ID})
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
        "compaction",
        "pool_wait",
        "scheduler_last_tick_age_s",
        "recent_cron_failures",
        "observability",
        "config_backup",
    ):
        assert k in body


@pytest.mark.parametrize(
    ("target", "exc", "field", "expected"),
    [
        ("os.getloadavg", OSError("no load avg"), "load", None),
        ("builtins.open", FileNotFoundError("no /proc/meminfo"), "memory", None),
        ("backend.cron_progress.list_active_runs", RuntimeError("boom"), "in_flight_runs", []),
        ("backend.config.list_configs", RuntimeError("boom"), "compaction", {}),
        ("backend.core.duckdb_pool.get_all_stats", RuntimeError("boom"), "pool_wait", []),
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


def test_health_snapshot_in_flight_compaction_pool_and_fos(client):
    fake_runs = [
        {"run_id": 101, "service_id": "svc-a", "task": "sync", "started_at": "2026-06-12T10:00:00Z", "rows_done": 1234},
        {"run_id": 102, "service_id": "svc-b", "task": "metadata_cleanup", "started_at": 1781758323.4424183},
    ]
    with patch("backend.cron_progress.list_active_runs", return_value=fake_runs):
        runs = _get_health(client)["in_flight_runs"]
    assert len(runs) == 2 and set(runs[0].keys()) == {"run_id", "service_id", "task", "started_at"}
    assert isinstance(runs[1]["started_at"], str) and "T" in runs[1]["started_at"]

    fake_configs = [{"service_id": "svc-good", "name": "svc-good"}, {"service_id": "svc-bad", "name": "svc-bad"}]
    with (
        patch("backend.config.list_configs", return_value=fake_configs),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch(
            "backend.core.local_compaction.compaction_stats",
            side_effect=lambda s: (
                (_ for _ in ()).throw(RuntimeError("fail")) if s["name"] == "svc-bad" else {"partitions": 7}
            ),
        ),
    ):
        comp = _get_health(client)["compaction"]
    assert comp["svc-good"]["partitions"] == 7 and comp["svc-bad"] is None

    fake_stats = [{"pool": "default", "wait_p50_ms": 5.0, "wait_p95_ms": 12.0}]
    with patch("backend.core.duckdb_pool.get_all_stats", return_value=fake_stats):
        assert _get_health(client)["pool_wait"] == fake_stats

    svc_cfg = [{"service_id": "svc-a", "name": "svc-a"}]
    with (
        patch("backend.config.list_configs", return_value=svc_cfg),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.duckdb.fos_reachable", return_value={"reachable": True, "latency_ms": 45}) as mock_reach,
    ):
        r_fos = client.get("/api/admin/health-snapshot?probe_fos=True")
        assert r_fos.status_code == 200 and r_fos.json()["fos"]["svc-a"]["reachable"] is True
        mock_reach.assert_called_once()

    with (
        patch("backend.config.list_configs", return_value=svc_cfg),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.duckdb.fos_reachable", side_effect=RuntimeError("S3 connection timed out")),
    ):
        r_fos_err = client.get("/api/admin/health-snapshot?probe_fos=True").json()["fos"]["svc-a"]
        assert r_fos_err["reachable"] is False and "timed out" in r_fos_err["error"]


@pytest.mark.parametrize(
    ("cfg", "active_ver", "snippets", "expect_clean", "err_sub", "rec_sub"),
    [
        (None, None, [], False, "not configured", None),
        ({"service_id": "svc-a"}, None, [], False, "No Fastly API token", None),
        ({"service_id": "svc-a", "admin_token": "token"}, None, [], False, "No active version found", None),
        (
            {"service_id": "svc-a", "admin_token": "token"},
            5,
            [Snippet(name="Fastly Log Analytics - Recv")],
            True,
            None,
            None,
        ),
        ({"service_id": "svc-a", "admin_token": "token"}, 5, [], False, None, "no snippets found"),
        (RuntimeError("disk crash"), None, [], False, "disk crash", None),
    ],
)
def test_vcl_health_scenarios(client, cfg, active_ver, snippets, expect_clean, err_sub, rec_sub):
    load_ctx = (
        patch("backend.config.load_config", side_effect=cfg)
        if isinstance(cfg, Exception)
        else patch("backend.config.load_config", return_value=cfg)
    )
    with (
        load_ctx,
        patch("backend.provision.declarative.fastly_integration.fetch_active_version", return_value=active_ver),
        patch("backend.provision.declarative.fastly_integration.fetch_snippets", return_value=snippets),
    ):
        resp = client.get("/api/admin/vcl-health?service_id=svc-a")
    assert resp.status_code == 200
    body = resp.json()
    assert body["is_clean"] is expect_clean
    if expect_clean:
        assert body["legacy_snippets_found"] == 0 and body["consolidated_snippets_found"] == 1
    if err_sub:
        assert err_sub in body["error"]
    if rec_sub:
        assert rec_sub in body["recommendation"]


@pytest.fixture
def overview_client():
    with TestClient(app) as c:
        yield c


def test_admin_sync_status_and_health_aliases(overview_client: TestClient):
    src = _make_source()
    fake_payload = {
        "configured": True,
        "busy": False,
        "storage_mode": "ducklake",
        "local_rows": 1250,
        "earliest_log_at": "2026-10-15T20:00:00Z",
        "latest_log_at": "2026-10-16T20:00:00Z",
    }
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.routers.admin.sync_status.compute_sync_status_cached", return_value=fake_payload),
    ):
        for p in ("/api/admin/sync-status", "/api/sync-status"):
            r = overview_client.get(f"{p}?service_id=svc-admin-test&skip_fos=true")
            assert r.status_code == 200 and r.json()["local_rows"] == 1250

    for path in ("/api/admin/system-metrics", "/api/admin/health", "/api/admin/health-snapshot"):
        resp = overview_client.get(path)
        assert resp.status_code == 200
        assert {"load", "vcpus", "in_flight_runs", "compaction", "pool_wait", "recent_cron_failures"} <= set(
            resp.json()
        )


def test_admin_compaction_and_log_accounting_routes(overview_client: TestClient):
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
        r1 = overview_client.get(f"/api/admin/compaction-status/{src['service_id']}")
        r2 = overview_client.get(f"/api/admin/compaction-status?service_id={src['service_id']}")
        assert r1.status_code == 200 and r2.status_code == 200
        assert {"partitions", "total_files", "partitions_above_3"} <= set(r1.json())
        assert r2.json()["total_files"] == r1.json()["total_files"]

        with patch("backend.routers.admin.log_accounting.compute_log_accounting", return_value=fake_accounting):
            ra1 = overview_client.get(f"/api/admin/log-accounting/{src['service_id']}?hours=24")
            ra2 = overview_client.get(f"/api/admin/log-accounting?service_id={src['service_id']}&hours=24")
            assert ra1.status_code == 200 and ra1.json()["totals"]["fastly_requests"] == 1000
            assert ra2.status_code == 200 and ra2.json()["totals"]["our_rows"] == 1000
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_admin_manual_cron_trigger_routes_and_rbac(overview_client: TestClient):
    src = _make_source()
    app.dependency_overrides[get_source] = lambda: src
    try:
        with (
            patch(
                "backend.utils.router_utils.start_or_resume_cron",
                return_value={"ok": True, "message": "Started.", "run_id": 42},
            ) as mock_start,
            patch(
                "backend.core.local_compaction.compact_local_partitions",
                return_value={
                    "partitions_checked": 2,
                    "partitions_compacted": 1,
                    "files_removed": 3,
                    "rows_preserved": 500,
                    "dups_removed": 0,
                },
            ),
            patch("backend.cron.jobs.compaction._run_optimize", return_value={"files_rewritten": 4, "files_added": 1}),
        ):
            sid = src["service_id"]
            assert overview_client.post(f"/api/admin/sync/{sid}").status_code == 200 and mock_start.called
            assert overview_client.post(f"/api/admin/commit/{sid}").status_code == 202
            assert overview_client.post(f"/api/admin/compact/{sid}").json()["partitions_compacted"] == 1
            assert overview_client.post(f"/api/admin/optimize/{sid}").json()["files_rewritten"] == 4
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_analyst_path_b_rejected_with_403_on_admin_overview_routes():
    for path in (
        "/api/admin/sync-status",
        "/api/sync-status",
        "/api/admin/system-metrics",
        "/api/admin/health",
        "/api/admin/health-snapshot",
        "/api/admin/compaction-status",
        "/api/admin/compaction-status/svc-admin-test",
        "/api/admin/log-accounting/svc-admin-test",
        "/api/admin/sync/svc-admin-test",
        "/api/admin/commit/svc-admin-test",
        "/api/admin/compact/svc-admin-test",
        "/api/admin/optimize/svc-admin-test",
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


def test_iceberg_info_calendar_and_commit_endpoints(overview_client, override_source):
    fake_info = {
        "table_name": "logs",
        "table_location": "s3://bucket/prefix/ducklake",
        "snapshots": 5,
        "data_files": 12,
        "size_bytes": 1024,
        "latest_snapshot_at": "2026-08-19T00:00:00Z",
        "buffer_files": 1,
        "buffer_size_bytes": 512,
    }
    with patch("backend.core.storage.get_table_info", return_value=fake_info) as mock_get:
        r_info = overview_client.get("/api/admin/iceberg-info?table_name=logs")
        assert r_info.status_code == 200 and r_info.json()["size_bytes"] == 1024
        mock_get.assert_called_once_with(override_source, table_name="logs")

    fake_cal = {"2026-08-19": {"data_files": 5, "size_bytes": 1024}}
    with patch("backend.core.storage.get_snapshot_calendar", return_value=fake_cal) as mock_cal:
        r_cal = overview_client.get("/api/admin/iceberg-calendar?table_name=logs")
        assert r_cal.status_code == 200 and r_cal.json()["2026-08-19"] == {"data_files": 5, "size_bytes": 1024}
        mock_cal.assert_called_once_with(override_source, table_name="logs")

    with patch("backend.utils.router_utils.start_or_resume_cron", return_value={"ok": True}) as mock_start:
        assert overview_client.post("/api/admin/commit-iceberg").status_code == 202 and mock_start.called


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


def test_metric_history_batch_groups_by_scope_key():
    t1 = iso_z(datetime.now(UTC) - timedelta(minutes=10))
    t2 = iso_z(datetime.now(UTC) - timedelta(minutes=5))
    metric_snapshots.record_snapshot("cpu_load_1m", 0.7, ts=t1)
    metric_snapshots.record_snapshot("cpu_load_1m", 0.9, ts=t2)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 5.0, service_id="svc-a", ts=t2)
    metric_snapshots.record_snapshot("cron_duration_ms", 800.0, service_id="svc-a", task="sync", ts=t2)

    with TestClient(app) as c:
        r = c.get("/api/admin/metric-history/batch?since=1h")
    assert r.status_code == 200
    series = r.json()["series"]
    assert {"cpu_load_1m", "pool_wait_p95_ms|svc-a", "cron_duration_ms|svc-a|sync"} <= set(series)
    cpu_points = series["cpu_load_1m"]
    assert len(cpu_points) >= 2 and cpu_points[-2]["value"] == pytest.approx(0.7)
    assert cpu_points[-1]["value"] == pytest.approx(0.9) and cpu_points[-2]["ts"] < cpu_points[-1]["ts"]


@pytest.mark.parametrize(("window", "expected_points"), [("30m", 1), ("1h", 2), ("6h", 3), ("24h", 4), ("7d", 5)])
def test_metric_history_batch_windows(window: str, expected_points: int):
    metric_name = f"test_window_metric_{window}"
    now = datetime.now(UTC)
    for val, delta in [
        (10.0, timedelta(minutes=10)),
        (20.0, timedelta(minutes=45)),
        (30.0, timedelta(hours=3)),
        (40.0, timedelta(hours=12)),
        (50.0, timedelta(days=3)),
    ]:
        metric_snapshots.record_snapshot(metric_name, val, ts=iso_z(now - delta))

    with TestClient(app) as c:
        r = c.get(f"/api/admin/metric-history/batch?since={window}")
    assert r.status_code == 200 and len(r.json()["series"].get(metric_name, [])) == expected_points


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
        patch("backend.utils.rdns_cache.get_stats", return_value=fake_rdns),
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
        patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary) as mock_enrich,
        patch("backend.utils.rdns_cache.get_stats", return_value={"total": 500, "resolved": 450, "pending": 50}),
    ):
        re = client.post("/api/admin/rdns/enrich?limit=50")
        assert re.status_code == 200 and re.json()["summary"] == fake_summary
        mock_enrich.assert_called_once_with(limit=50)
        rs = client.get("/api/admin/rdns/stats")
        assert rs.status_code == 200 and rs.json()["stats"]["total"] == 500


# ── Merged from tests/routers/test_high_scale.py ──

from dataclasses import replace

from backend.core.high_scale_sections import current_clickhouse_scope
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.exports import ExportManager, get_export_manager
from backend.high_scale.registry_bootstrap import HighScaleService, HighScaleServiceRegistry
from backend.utils.remote_access import TimeBounds

from backend.core.request_context import build_request_context


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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    manager = ExportManager(max_workers=1, max_rows=10, max_bytes=10_000)
    monkeypatch.setattr("backend.routers.high_scale.metadata.record_audit", lambda *args, **kwargs: None)
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
    from backend.deps import require_admin
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry
    from fastapi import HTTPException

    manager = ExportManager(max_workers=1)
    monkeypatch.setattr("backend.routers.high_scale.metadata.record_audit", lambda *args, **kwargs: None)
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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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


def test_analyst_request_facts_masks_client_ip(client, in_memory_duckdb, test_service_source):
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    app.dependency_overrides[build_request_context] = lambda: _analyst_context(test_service_source, in_memory_duckdb)
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["rows"][0]["client_ip"] == "203.0.113.xxx"


def test_request_facts_rejects_service_binding_mismatch(client, test_service_source):
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry()
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_service_not_configured"


def test_request_facts_rejects_invalid_cursor(client, test_service_source):
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/rum-facts/rum_vitals",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 200, response.text
    assert response.json()["metadata"]["watermark"]["domain"] == "rum_vitals"


def test_rum_facts_rejects_unconfigured_domain(client, test_service_source):
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry(_service())
    response = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/rum-facts/rum_errors",
        json={"start_time": "2026-09-11T20:00:00Z", "end_time": "2026-09-11T21:00:00Z"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "high_scale_domain_not_configured"


def test_cmcd_facts_queries_configured_domain(client, test_service_source):
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.archive_models import ArchiveArtifact, ArchiveManifest, ArchiveSourceObject
    from backend.high_scale.archive_publication import ArchivePublication, InMemoryObjectStore
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

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


def test_request_facts_standard_mode_duckdb_emulation_and_filters(client, in_memory_duckdb, test_service_source):
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    in_memory_duckdb.execute(
        """
        CREATE OR REPLACE TABLE logs (
            timestamp TIMESTAMP WITH TIME ZONE,
            status INTEGER,
            method VARCHAR,
            host VARCHAR,
            url VARCHAR,
            pop VARCHAR,
            geo_country_code VARCHAR,
            client_ip VARCHAR,
            cache_status VARCHAR,
            time_elapsed BIGINT
        )
        """
    )
    in_memory_duckdb.execute(
        """
        INSERT INTO logs VALUES
        ('2026-09-11T20:01:00Z', 200, 'GET', 'www.example.com', '/home', 'IAD', 'US', '198.51.100.10', 'HIT', 1200),
        ('2026-09-11T20:02:00Z', 503, 'POST', 'api.example.com', '/pay', 'LHR', 'GB', '198.51.100.25', 'PASS', 45000)
        """
    )

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry()

    # Service-scoped POST with emulate_standard=True falls back cleanly to DuckDB
    res_post = client.post(
        f"/api/high-scale/services/{test_service_source['service_id']}/request-facts",
        headers={"X-Page-Load-ID": "page-load-emul-test"},
        json={
            "start_time": "2026-09-11T20:00:00Z",
            "end_time": "2026-09-11T21:00:00Z",
            "status": 503,
            "domain": "api.example.com",
            "pop": "LHR",
            "country": "GB",
            "emulate_standard": True,
        },
    )
    assert res_post.status_code == 200, res_post.text
    body = res_post.json()
    assert body["metadata"]["mode"] == "duckdb_emulation"
    assert len(body["rows"]) == 1
    assert body["rows"][0]["status"] == 503
    assert body["rows"][0]["host"] == "api.example.com"
    assert body["rows"][0]["pop"] == "LHR"
    assert body["rows"][0]["country"] == "GB"
    assert body["rows"][0]["client_ip"] == "198.51.100.25"
    assert isinstance(body.get("_debug_queries"), list)
    assert len(body["_debug_queries"]) >= 1

    # Direct GET /api/high-scale/request-facts
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
    assert get_body["metadata"]["mode"] == "duckdb_emulation"
    assert len(get_body["rows"]) == 1
    assert get_body["rows"][0]["url"] == "/home"


def test_request_facts_standard_emulation_masks_analyst_ip(client, in_memory_duckdb, test_service_source):
    from backend.high_scale.registry_bootstrap import get_high_scale_service_registry

    in_memory_duckdb.execute(
        """
        CREATE OR REPLACE TABLE logs (
            timestamp TIMESTAMP WITH TIME ZONE,
            status INTEGER,
            method VARCHAR,
            host VARCHAR,
            url VARCHAR,
            pop VARCHAR,
            geo_country_code VARCHAR,
            client_ip VARCHAR
        )
        """
    )
    in_memory_duckdb.execute(
        """
        INSERT INTO logs VALUES
        ('2026-09-11T20:01:00Z', 200, 'GET', 'www.example.com', '/home', 'IAD', 'US', '198.51.100.77')
        """
    )

    app.dependency_overrides[get_high_scale_service_registry] = lambda: _registry()
    app.dependency_overrides[build_request_context] = lambda: _analyst_context(test_service_source, in_memory_duckdb)

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
    assert len(rows) == 1
    assert rows[0]["client_ip"] == "198.51.100.xxx"


# ── Merged from tests/routers/test_provision_lifecycle_and_teardown.py ──

import io
import os
import urllib.error
import zipfile
from contextlib import ExitStack, contextmanager
from io import BytesIO
from unittest.mock import MagicMock

import botocore.exceptions
import pytest
from backend.core.faro_versions import DEFAULT_FARO_VERSION
from backend.provision.fos_setup import object_storage_enabled, product_enabled
from backend.routers.provision import _check_domain_available

from backend import config as svcconfig
from backend.provision import fos_setup
from backend.provision.fastly_api import _CDN_NO_CACHE_404_SNIPPET_NAME, _CDN_SNIPPETS
from backend.utils.router_utils import require_json_content_type as _require_json_content_type
from tests._helpers.wire import parse_sse_events as _parse_sse_events

_FAKE_CFG = {
    "service_id": "svc123",
    "name": "Test Service",
    "fos_bucket": "bucket",
    "fos_region": "us-east-1",
    "fos_endpoint": "us-east-1.object.fastlystorage.app",
}
_VALID_TOKEN = "test-admin-token"


def _client() -> TestClient:
    return TestClient(app)


def _seed_cfg(sid: str, **overrides) -> dict:
    cfg = {
        "service_id": sid,
        "logging_service_id": sid,
        "name": f"Test Svc {sid}",
        "fos_bucket": f"test-bucket-{sid}",
        "fos_region": "us-east-1",
        "fos_access_key_id": "AKIA",
        "fos_secret_access_key": "SECRET",
        "fastly_api_key": "server-stored-key-MUST-NOT-BE-USED",
        "cdn_secret": "x",
        "provisioning": {
            "fos_key_id": "fk_1",
            "endpoint_name": "EP",
            "cdn_service_id": "cdn_svc_1",
            "cdn_url": "https://cdn.example/",
        },
        **overrides,
    }
    svcconfig.save_config(sid, cfg)
    return cfg


def _fake_perform_teardown(state, token, opts=None):
    yield {"type": "status", "message": "removing logging endpoint (mock)"}
    yield {"type": "status", "message": "removing FOS bucket (mock)"}
    yield {"type": "done", "message": "teardown complete (mock)"}


def _auth_fastly_ok(
    service_id: str = "svc-1",
    customer_id: str = "cust-T",
    *,
    scope="global",
    services=None,
    service_customer_id: str | None = None,
):
    svcs = [] if services is None else services
    svc_cid = service_customer_id or customer_id
    return lambda method, path, *, token, **kw: (
        {"id": "tok", "user_id": "u", "scope": scope, "services": svcs, "customer_id": customer_id}
        if path == "/tokens/self"
        else {"id": path.rsplit("/", 1)[-1] if "/service/" in path else service_id, "customer_id": svc_cid}
    )


@contextmanager
def _teardown_patches(sid: str = "svc-1", *, teardown_fn=_fake_perform_teardown, fastly_fn=None):
    with (
        patch("backend.provision.perform_teardown", side_effect=teardown_fn),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        patch("backend.utils.fastly_auth.fastly", side_effect=fastly_fn or _auth_fastly_ok(sid)),
    ):
        yield


def _destructive_payload(sid: str, **overrides) -> dict:
    return {
        "service_id": sid,
        "remove_logging": True,
        "remove_cdn": True,
        "remove_bucket": True,
        "remove_cache": False,
        **overrides,
    }


# ── Merged from tests/routers/test_provision_lifecycle.py ──


def test_teardown_no_service_id_returns_404(isolated_configs_dir):
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json={"service_id": "nope-not-a-real-svc"})
    assert r.status_code == 404


def test_teardown_emits_done_and_removes_config(isolated_configs_dir, tmp_path, monkeypatch):
    sid = "svc-teardown-1"
    _seed_cfg(sid, fos_bucket="td-test-bucket", fastly_api_key="fastly-test-token")
    assert os.path.exists(svcconfig.config_path(sid)), "precondition: config seeded"

    with _teardown_patches(sid), TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json=_destructive_payload(sid, token="test-tok", remove_cron=False),
        )

    assert r.status_code == 200
    events = _parse_sse_events(r.text)
    assert events, f"expected SSE events, got: {r.text[:300]!r}"
    assert "done" in [e.get("type") for e in events]
    assert not os.path.exists(svcconfig.config_path(sid)), "config should have been removed"


def test_teardown_remove_cache_clears_service_metadata(isolated_configs_dir):
    sid = "svc-teardown-meta"
    _seed_cfg(sid, fos_bucket="td-meta-bucket", fastly_api_key="fastly-test-token")
    meta_calls: list[str] = []

    with (
        _teardown_patches(sid),
        patch("backend.core.metadata.teardown", side_effect=lambda s: meta_calls.append(s)),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/teardown",
            json=_destructive_payload(
                sid,
                token="test-tok",
                remove_logging=False,
                remove_cdn=False,
                remove_bucket=False,
                remove_cache=True,
                remove_cron=False,
            ),
        )

    assert r.status_code == 200
    assert meta_calls == [sid], f"metadata.teardown({sid!r}) must be called on remove_cache; got {meta_calls}"


def test_teardown_skips_perform_teardown_when_no_logging_service(isolated_configs_dir):
    sid = "svc-teardown-min"
    svcconfig.save_config(sid, {"service_id": sid, "name": "minimal"})

    with _teardown_patches(sid), TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": "test-tok", "remove_cache": False},
        )
    assert r.status_code == 200
    assert not os.path.exists(svcconfig.config_path(sid))


def test_execute_delegates_to_provision_with_query_args(isolated_configs_dir):
    sid = "svc-create-1"
    captured: dict = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "message": "provision complete"}

    with (
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="My Service"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json={
                "token": "test-token",
                "service_id": sid,
                "fos_bucket_name": "create-test-bucket",
                "fos_region": "us-east-1",
                "endpoint_name": "Test Logger",
                "edge_only": True,
                "log_period": "60",
                "enable_cron_sync": True,
                "enable_cron_compact": True,
            },
        )

    assert r.status_code == 200, r.text[:500]
    cfg = captured.get("cfg")
    assert cfg is not None, "provision() was never called"
    assert cfg["logging_service_id"] == sid
    assert cfg["fos_bucket_name"] == "create-test-bucket"
    assert cfg["endpoint_name"] == "Test Logger"
    assert cfg["fos_region"] == "us-east-1"
    assert "done" in [e.get("type") for e in _parse_sse_events(r.text)]


def test_execute_propagates_orchestrator_error_event(isolated_configs_dir):
    def fake_provision(cfg, _resume_from_state=False):
        yield {"type": "status", "message": "starting"}
        yield {"type": "error", "message": "Fastly API: 503 Service Unavailable"}

    with (
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json={"token": "tok", "service_id": "svc-create-err", "fos_bucket_name": "create-err-bucket"},
        )

    events = _parse_sse_events(r.text)
    err = next((e for e in events if e.get("type") == "error"), None)
    assert err is not None and "503" in err["message"]


# ── Merged from tests/routers/test_provision_teardown_auth.py ──


@pytest.mark.security_regression
def test_destructive_teardown_without_token_rejected_401(isolated_configs_dir):
    """No token + destructive flags = 401, no fallback to server creds."""
    sid = "svc-auth-1"
    _seed_cfg(sid)
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "token_required"


@pytest.mark.security_regression
def test_destructive_teardown_with_read_only_scope_rejected(isolated_configs_dir):
    """``global:read`` does not grant destructive ops — must be rejected."""
    sid = "svc-auth-2"
    _seed_cfg(sid)

    def _fake_fastly(method, path, *, token, **kw):
        assert path == "/tokens/self"
        return {"id": "tok_ro", "user_id": "u", "scope": "global:read", "services": []}

    with patch("backend.utils.fastly_auth.fastly", side_effect=_fake_fastly), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="ro-token"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "insufficient_scope"


@pytest.mark.security_regression
def test_destructive_teardown_with_purge_scope_rejected(isolated_configs_dir):
    """purge_select / purge_all also reject — only ``global`` qualifies."""
    sid = "svc-auth-3"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, scope="purge_select purge_all")
    with patch("backend.utils.fastly_auth.fastly", side_effect=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="purge-token"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "insufficient_scope"


@pytest.mark.security_regression
def test_destructive_teardown_token_bound_to_wrong_service_rejected(isolated_configs_dir):
    """Token has 'global' but services=[other-id] — must reject (service mismatch)."""
    sid = "svc-auth-4"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, services=["different-svc"])
    with patch("backend.utils.fastly_auth.fastly", side_effect=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="bound-token"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "service_not_authorized"


@pytest.mark.security_regression
def test_destructive_teardown_with_global_token_proceeds(isolated_configs_dir):
    """Valid 'global' token + matching/empty services → proceeds (200 SSE)."""
    sid = "svc-auth-5"
    _seed_cfg(sid)
    with _teardown_patches(sid, fastly_fn=_auth_fastly_ok(sid, "cust-A")), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="valid-global-token"))
    assert r.status_code == 200, r.text[:500]


@pytest.mark.security_regression
def test_destructive_teardown_service_in_bound_list_proceeds(isolated_configs_dir):
    """Token bound to [sid, other] — matches target → proceeds."""
    sid = "svc-auth-6"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, "cust-B", services=[sid, "other-svc"])
    with _teardown_patches(sid, fastly_fn=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="bound-valid"))
    assert r.status_code == 200


@pytest.mark.security_regression
def test_cache_only_teardown_requires_auth_gate(isolated_configs_dir):
    """Cache-only cleanup still requires a token to prevent unauthenticated local state destruction."""
    sid = "svc-auth-7"
    _seed_cfg(sid)
    payload = _destructive_payload(
        sid, remove_logging=False, remove_cdn=False, remove_bucket=False, remove_cache=True, remove_cron=False
    )
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=payload)
    assert r.status_code == 401
    assert "token_required" in r.json()["detail"]["error"]


@pytest.mark.security_regression
def test_destructive_teardown_fastly_unreachable_rejects(isolated_configs_dir):
    """If Fastly's /tokens/self errors out, fail closed rather than falling back to server creds."""
    sid = "svc-auth-8"
    _seed_cfg(sid)
    with (
        patch("backend.utils.fastly_auth.fastly", side_effect=RuntimeError("Network error on GET /tokens/self")),
        TestClient(app) as client,
    ):
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="any"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "token_validation_failed"


@pytest.mark.security_regression
def test_destructive_teardown_scope_as_list_accepted(isolated_configs_dir):
    """Some Fastly token responses return scope as a JSON list; validator must normalize both forms."""
    sid = "svc-auth-9"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, "cust-L", scope=["global", "purge_all"])
    with _teardown_patches(sid, fastly_fn=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="list-scope"))
    assert r.status_code == 200


@pytest.mark.security_regression
def test_destructive_teardown_tenant_mismatch_rejected(isolated_configs_dir):
    """Token with 'global' scope under a different customer_id from target service is rejected."""
    sid = "svc-auth-tenant-mismatch"
    _seed_cfg(sid)
    fastly_fn = _auth_fastly_ok(sid, "cust-ATTACKER", service_customer_id="cust-VICTIM")
    with patch("backend.utils.fastly_auth.fastly", side_effect=fastly_fn), TestClient(app) as client:
        r = client.post("/api/provision/teardown", json=_destructive_payload(sid, token="cross-tenant-token"))
    assert r.status_code == 401
    assert r.json()["detail"]["error"] == "tenant_mismatch"


@pytest.mark.security_regression
def test_destructive_teardown_get_method_rejected(isolated_configs_dir):
    """CSRF defense: teardown must not be triggerable via GET."""
    sid = "svc-csrf-rejected"
    _seed_cfg(sid)
    with TestClient(app) as client:
        r = client.get(
            "/api/provision/teardown",
            params={"service_id": sid, "token": "anything", "remove_logging": "true"},
        )
    assert r.status_code in (404, 405), f"GET must be rejected; got {r.status_code}: {r.text[:300]}"


@pytest.mark.security_regression
def test_destructive_teardown_text_plain_content_type_rejected(isolated_configs_dir):
    """Regression for audit finding 012: text/plain POST must be rejected with 415."""
    sid = "svc-csrf-text-plain"
    _seed_cfg(sid)
    with TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            content='{"service_id":"' + sid + '","remove_logging":true}',
            headers={"Content-Type": "text/plain"},
        )
    assert r.status_code == 415


# ── Merged from tests/routers/test_provision_teardown_idempotency.py ──


def test_teardown_is_idempotent_second_call_is_404(isolated_configs_dir):
    """Two back-to-back teardown calls: first 200 + cleans up, second 404."""
    sid = "svc-idem-1"
    _seed_cfg(sid)

    with _teardown_patches(sid, fastly_fn=_auth_fastly_ok(sid, "cust-IDEM")), TestClient(app) as client:
        r1 = client.post(
            "/api/provision/teardown",
            json=_destructive_payload(sid, token=_VALID_TOKEN, remove_cron=False),
        )
        assert r1.status_code == 200
        assert any(e.get("type") == "done" for e in _parse_sse_events(r1.text))
        assert not os.path.exists(svcconfig.config_path(sid))

        r2 = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
        )

    assert r2.status_code == 404
    assert "detail" in r2.json()


def test_teardown_unknown_service_id_never_provisioned_is_404(isolated_configs_dir):
    with TestClient(app) as client:
        r = client.post("/api/provision/teardown", json={"service_id": "never-existed-svc", "remove_cache": False})
    assert r.status_code == 404


def test_teardown_preserves_duckdb_when_remove_cache_false(isolated_configs_dir, tmp_path):
    """``remove_cache=False`` MUST leave the per-service DuckDB file intact."""
    sid = "svc-idem-keep-cache"
    _seed_cfg(sid)

    db_path = svcconfig.duckdb_path(sid)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with open(db_path, "w") as f:
        f.write("placeholder-duckdb-bytes")
    assert os.path.exists(db_path)

    with _teardown_patches(sid, fastly_fn=_auth_fastly_ok(sid, "cust-IDEM")), TestClient(app) as client:
        r = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False, "remove_cron": False},
        )

    assert r.status_code == 200
    assert not os.path.exists(svcconfig.config_path(sid))
    assert os.path.exists(db_path)


def test_teardown_partial_failure_in_perform_teardown_still_succeeds(isolated_configs_dir):
    """If ``perform_teardown`` raises mid-stream, SSE emits error, config is removed, and re-run is 404."""
    sid = "svc-idem-partial-fail"
    _seed_cfg(sid)

    def _boom_teardown(state, token, opts=None):
        yield {"type": "status", "message": "removing logging endpoint (mock)"}
        raise RuntimeError("Fastly API: 503 Service Unavailable (mock)")

    with (
        _teardown_patches(sid, teardown_fn=_boom_teardown, fastly_fn=_auth_fastly_ok(sid, "cust-IDEM")),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
        )

    assert r.status_code == 200
    assert "error" in [e.get("type") for e in _parse_sse_events(r.text)]
    assert not os.path.exists(svcconfig.config_path(sid))

    with TestClient(app) as client:
        r2 = client.post(
            "/api/provision/teardown",
            json={"service_id": sid, "token": _VALID_TOKEN, "remove_cache": False},
        )
    assert r2.status_code == 404


# ── Merged from tests/routers/test_provision_wizard_e2e.py ──


def _fake_ensure_fos_access_key(
    description, state, token, *, permission="read-write-objects", buckets=None, status_cb=None
):
    if status_cb:
        status_cb(f"⏳ Creating {permission} access key (mock)…")
    suffix = "temp" if "temp-admin" in description else "perm"
    return {"id": f"AKIA_TEST_{suffix}", "access_key": f"AKIA_TEST_{suffix}", "secret_key": f"SECRET_TEST_{suffix}"}


def _fake_ensure_fos_bucket(bucket, region, access_key, secret_key, *, service_id=None, status_cb=None):
    if status_cb:
        status_cb(f"🪣 Bucket {bucket} in {region} (mock)…")
    return True


def _fake_ensure_cdn_service(cfg, fos_access_key, fos_secret_key, token, status_cb=None, on_created=None):
    if status_cb:
        status_cb("🌐 CDN service (mock)…")
    if on_created:
        on_created("cdn-svc-mock-123")
    return {"id": "cdn-svc-mock-123", "name": cfg.get("cdn_service_name") or "Mock CDN Service", "version": 1}


def _fake_ensure_logging_via_reconciler(state, token, status_cb=None):
    if status_cb:
        status_cb("📡 Logging endpoint (mock)…")
    return 42


def _fake_delete_fos_access_key(key_id, token, status_cb=None):
    if status_cb:
        status_cb(f"🧹 Deleted {key_id} (mock)…")


@contextmanager
def _wizard_env(tmp_path, monkeypatch, *, service_name="Wizard E2E Service", extra_patches=()):
    monkeypatch.setattr(svcconfig, "DATA_DIR", tmp_path / "data")
    (tmp_path / "data").mkdir(exist_ok=True)
    base_patches = [
        patch("backend.provision.orchestrator.ensure_fos_access_key", side_effect=_fake_ensure_fos_access_key),
        patch("backend.provision.orchestrator.ensure_fos_bucket", side_effect=_fake_ensure_fos_bucket),
        patch("backend.provision.orchestrator.ensure_cdn_service", side_effect=_fake_ensure_cdn_service),
        patch(
            "backend.provision.orchestrator.ensure_logging_via_reconciler",
            side_effect=_fake_ensure_logging_via_reconciler,
        ),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=_fake_delete_fos_access_key),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.config.fetch_service_name", return_value=service_name),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler"),
    ]
    with ExitStack() as stack:
        for p in [*base_patches, *extra_patches]:
            stack.enter_context(p)
        yield


def _wizard_payload(sid: str, bucket: str, **overrides) -> dict:
    return {
        "token": "fake-fastly-token",
        "service_id": sid,
        "service_name": "Wizard E2E Service",
        "endpoint_name": "Wizard E2E Logger",
        "fos_region": "us-east-1",
        "fos_bucket_name": bucket,
        "fos_prefix": "logs/",
        "sample_rate": "100",
        "edge_only": True,
        "log_period": "60",
        "enable_cron_sync": True,
        "delete_after": True,
        "commit_interval_mins": 5,
        "enable_cron_compact": True,
        "log_retention_days": 30,
        **overrides,
    }


def test_wizard_execute_runs_orchestrator_and_bootstrap_sees_service(isolated_configs_dir, tmp_path, monkeypatch):
    """End-to-end: execute → orchestrator yields done → config persists → bootstrap lists svc."""
    sid = "svc-wizard-e2e-1"
    extras = [patch("backend.core.storage.init_storage_table", side_effect=RuntimeError("storage init skipped (test)"))]
    with _wizard_env(tmp_path, monkeypatch, extra_patches=extras), TestClient(app) as client:
        r = client.post("/api/provision/execute", json=_wizard_payload(sid, "wizard-e2e-bucket"))

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "done" in types and "error" not in types

    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    for n in range(1, 9):
        assert f"Step {n}/8" in status_msgs

    cfg_path = svcconfig.config_path(sid)
    assert os.path.exists(cfg_path)
    written = json.loads(cfg_path.read_text())
    assert written["service_id"] == sid
    assert written["fos_bucket"] == "wizard-e2e-bucket"
    assert written["fos_region"] == "us-east-1"
    assert written["fos_access_key_id"] == "AKIA_TEST_perm"
    assert written["fos_secret_access_key"] == "SECRET_TEST_perm"
    assert written["fastly_api_key"] == "fake-fastly-token"
    assert written["cdn_service_id"] == "cdn-svc-mock-123"
    assert isinstance(written["provisioning"]["endpoint_name"], str) and written["provisioning"]["endpoint_name"]
    assert isinstance(written["cdn_secret"], str) and written["cdn_secret"]

    with TestClient(app) as client:
        boot = client.get("/api/bootstrap")
    assert boot.status_code == 200
    assert sid in [s.get("service_id") for s in boot.json()["services"]]


def test_wizard_execute_e2e_storage_init_success(isolated_configs_dir, tmp_path, monkeypatch, s3_mock):
    """R-15: positive-path variant exercising real `init_storage_table` against moto S3."""
    sid = "svc-wizard-storage-success"
    monkeypatch.setattr(svcconfig, "CACHE_DATA_DIR", tmp_path / "data" / "cache")

    with (
        _wizard_env(tmp_path, monkeypatch, service_name="Wizard Storage Success"),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json=_wizard_payload(
                sid,
                "test-bucket",
                service_name="Wizard Storage Success",
                endpoint_name="Wizard Storage Logger",
            ),
        )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    types = [e.get("type") for e in events]
    assert "done" in types and "error" not in types

    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    for n in range(1, 9):
        assert f"Step {n}/8" in status_msgs
    assert svcconfig.config_path(sid).exists()

    with TestClient(app) as client:
        boot = client.get("/api/bootstrap")
    assert boot.status_code == 200
    assert sid in [s.get("service_id") for s in boot.json()["services"]]


def test_wizard_execute_rolls_back_on_helper_failure(isolated_configs_dir, tmp_path, monkeypatch):
    """Mid-pipeline helper failure emits error event, runs rollback, and removes config."""
    sid = "svc-wizard-e2e-fail"

    def _boom_cdn(cfg, fos_access_key, fos_secret_key, token, status_cb=None):
        raise RuntimeError("Fastly API: 500 Internal Server Error (mock)")

    extras = [
        patch("backend.provision.orchestrator.ensure_cdn_service", side_effect=_boom_cdn),
        patch("backend.provision.orchestrator.remove_logging_endpoint"),
        patch("backend.provision.orchestrator.delete_cdn_service"),
        patch("backend.provision.orchestrator.delete_fos_bucket"),
        patch("backend.core.fastly.client.fastly", return_value={"data": []}),
    ]
    with (
        _wizard_env(tmp_path, monkeypatch, service_name="Wizard E2E Service Fail", extra_patches=extras),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json=_wizard_payload(
                sid,
                "wizard-e2e-fail-bucket",
                service_name="Wizard E2E Service Fail",
                endpoint_name="Wizard E2E Logger Fail",
            ),
        )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    assert "error" in [e.get("type") for e in events]

    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    assert "Step 3/8" in status_msgs and "Step 8/8" not in status_msgs
    assert not os.path.exists(svcconfig.config_path(sid))


def test_wizard_execute_rollback_actually_invokes_resource_deletion(isolated_configs_dir, tmp_path, monkeypatch):
    """Mid-pipeline failure at step 7 must invoke resource deletion helpers for steps 1-6."""
    sid = "svc-wizard-teardown-trace"
    teardown_calls: list[tuple[str, dict]] = []

    def _track(name):
        def _capture(*args, **kwargs):
            teardown_calls.append((name, {"args": args, "kwargs": kwargs}))
            return None

        return _capture

    def _boom_logging_endpoint(state, token, status_cb=None):
        raise RuntimeError("Fastly API: 502 Bad Gateway on logging endpoint (mock)")

    extras = [
        patch("backend.provision.orchestrator.ensure_logging_via_reconciler", side_effect=_boom_logging_endpoint),
        patch("backend.provision.orchestrator.delete_fos_access_key", side_effect=_track("delete_fos_access_key")),
        patch("backend.provision.orchestrator.delete_fos_bucket", side_effect=_track("delete_fos_bucket")),
        patch("backend.provision.orchestrator.delete_cdn_service", side_effect=_track("delete_cdn_service")),
        patch("backend.provision.orchestrator.remove_logging_endpoint", side_effect=_track("remove_logging_endpoint")),
        patch("backend.core.fastly.client.fastly", return_value={"data": []}),
    ]
    with (
        _wizard_env(tmp_path, monkeypatch, service_name="Wizard Teardown Trace", extra_patches=extras),
        TestClient(app) as client,
    ):
        r = client.post(
            "/api/provision/execute",
            json=_wizard_payload(
                sid,
                "wizard-teardown-bucket",
                service_name="Wizard Teardown Trace",
                endpoint_name="Wizard Teardown Logger",
                cdn_service_name="Wizard Teardown CDN",
            ),
        )

    assert r.status_code == 200, r.text[:500]
    events = _parse_sse_events(r.text)
    assert "error" in [e.get("type") for e in events]
    status_msgs = " | ".join(e.get("message", "") for e in events if e.get("type") == "status")
    assert "Step 6/8" in status_msgs

    called_names = [name for name, _ in teardown_calls]
    for expected in ("delete_cdn_service", "remove_logging_endpoint", "delete_fos_bucket", "delete_fos_access_key"):
        assert expected in called_names, f"expected {expected} in {called_names}"
    assert not os.path.exists(svcconfig.config_path(sid))


# ── Merged from tests/routers/test_provision.py ──


# ─── /api/provision/services/{id}/ngwaf-workspace & /ngwaf-workspaces ────────


def test_set_ngwaf_workspace_saves_body_and_ignores_query_param():
    saved: dict = {}
    cfg_with_key = dict(_FAKE_CFG, fastly_api_key="test-stored-key")

    def fake_mutate(sid, fn, **kwargs):
        cfg = dict(cfg_with_key)
        fn(cfg)
        saved.update(cfg)
        return cfg

    with (
        patch("backend.config.load_config", return_value=cfg_with_key),
        patch("backend.config.mutate_config", side_effect=fake_mutate),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_validate,
    ):
        resp = _client().patch(
            "/api/provision/services/svc123/ngwaf-workspace?token=test-stored-key",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
        assert resp.status_code == 200
        mock_validate.assert_called_once_with("test-stored-key", service_id="svc123")
        assert resp.json()["ngwaf_workspace_id"] == "workspace-abc"
        assert saved.get("ngwaf_workspace_id") == "workspace-abc"

        saved.clear()
        resp2 = _client().patch(
            "/api/provision/services/svc123/ngwaf-workspace?workspace_id=workspace-abc&token=test-stored-key",
            json={},
        )
        assert resp2.status_code == 200
        assert saved.get("ngwaf_workspace_id") is None


@pytest.mark.parametrize(
    ("query", "cfg_key", "fastly_effect", "expected_error"),
    [
        ("", None, None, "token_required"),
        ("?token=attacker-supplied-wrong-token", "legit-stored-key", RuntimeError("HTTP 401"), None),
        (
            "?token=stored-read-only-token",
            "stored-read-only-token",
            {"id": "tok-id", "scope": "global:read", "services": [], "customer_id": "cust-T"},
            "insufficient_scope",
        ),
    ],
)
def test_set_ngwaf_workspace_auth_rejections(query, cfg_key, fastly_effect, expected_error):
    cfg = dict(_FAKE_CFG, **({"fastly_api_key": cfg_key} if cfg_key else {}))
    fastly_kw = (
        {"side_effect": fastly_effect} if isinstance(fastly_effect, Exception) else {"return_value": fastly_effect}
    )
    with (
        patch("backend.config.load_config", return_value=cfg),
        patch("backend.config.save_config"),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", **fastly_kw),
    ):
        resp = _client().patch(
            f"/api/provision/services/svc123/ngwaf-workspace{query}",
            json={"ngwaf_workspace_id": "workspace-abc"},
        )
    assert resp.status_code == 401
    if expected_error:
        assert resp.json()["detail"]["error"] == expected_error


def test_ngwaf_workspaces_listing_variants():
    cfg_ro = dict(_FAKE_CFG, fastly_api_key="stored-read-only-token")
    ro_data = {"id": "tok-id", "scope": "global:read", "services": [], "customer_id": "cust-T"}
    with (
        patch("backend.config.load_config", return_value=cfg_ro),
        patch("backend.utils.fastly_auth.fastly", return_value=ro_data),
    ):
        r_ro = _client().get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc123", "token": "stored-read-only-token"},
        )
        assert r_ro.status_code == 401
        assert r_ro.json()["detail"]["error"] == "insufficient_scope"

    with patch("backend.config.get_fastly_api_key", return_value=""):
        r_no_tok = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc"})
        assert r_no_tok.status_code == 401
        assert r_no_tok.json()["detail"]["error"] == "token_required"

    def _mock_resp(payload: bytes):
        m = MagicMock()
        m.read.return_value = payload
        m.status = 200
        m.__enter__ = lambda s: s
        m.__exit__ = MagicMock(return_value=False)
        return m

    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch(
            "urllib.request.urlopen",
            return_value=_mock_resp(b'{"data": [{"id": "ws-1", "name": "Prod"}, {"id": "ws-2", "name": "Staging"}]}'),
        ),
        patch("backend.utils.fastly_auth.validate_destructive_token") as mock_val,
    ):
        r_data = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_data.status_code == 200
        assert r_data.json()["workspaces"][0] == {"id": "ws-1", "name": "Prod"}
        mock_val.assert_called_once_with("tok", service_id="svc")

        r_hdr = _client().get(
            "/api/provision/ngwaf-workspaces",
            params={"service_id": "svc"},
            headers={"Authorization": "Bearer tok"},
        )
        assert r_hdr.status_code == 200
        assert r_hdr.json()["workspaces"][0] == {"id": "ws-1", "name": "Prod"}

    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch(
            "urllib.request.urlopen",
            return_value=_mock_resp(b'{"workspaces": [{"id": "ws-old", "attributes": {"name": "OldShape"}}]}'),
        ),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_old = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_old.json()["workspaces"][0]["name"] == "OldShape"

    with (
        patch("backend.config.get_fastly_api_key", return_value="tok"),
        patch("urllib.request.urlopen", return_value=_mock_resp(b'{"data": []}')),
        patch("backend.provision.fastly", return_value={"id": "tok-id", "name": "automation"}),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_empty = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "tok"})
        assert r_empty.json()["workspaces"] == []
        assert "automation" in r_empty.json()["error_hint"].lower()

    err_401 = urllib.error.HTTPError(
        url="https://api.fastly.com/ngwaf/v1/workspaces",
        code=401,
        msg="unauthorized",
        hdrs={},  # type: ignore[arg-type]
        fp=BytesIO(b"unauthorized"),
    )
    with (
        patch("backend.config.get_fastly_api_key", return_value="bad-tok"),
        patch("urllib.request.urlopen", side_effect=err_401),
        patch("backend.utils.fastly_auth.validate_destructive_token"),
    ):
        r_401 = _client().get("/api/provision/ngwaf-workspaces", params={"service_id": "svc", "token": "bad-tok"})
        assert r_401.status_code == 400
        assert r_401.json()["detail"]["error"] == "ngwaf_token_invalid"


# ─── /api/provision/services & /validate ─────────────────────────────────────


def test_provision_list_services_flows(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path)
    config.save_config("svc-existing", {"service_id": "svc-existing"})

    fake_services = [
        {"id": "svc-existing", "name": "Already Done", "type": "vcl"},
        {"id": "svc-new", "name": "Fresh", "type": "vcl"},
        {"id": "svc-wasm", "name": "Compute", "type": "wasm"},
    ]
    with (
        patch("backend.core.fastly.client.fastly", return_value=fake_services),
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=True),
    ):
        resp = _client().get("/api/provision/services?token=tok")
        assert resp.status_code == 200
        assert {s["id"]: s["provisioned"] for s in resp.json()} == {"svc-existing": True, "svc-new": False}

    assert _client().get("/api/provision/services").status_code == 422

    with (
        patch("backend.core.fastly.client.fastly", return_value=fake_services[:1]),
        patch("backend.provision.fos_setup.object_storage_enabled", return_value=False),
        patch("backend.config.list_service_ids", return_value=[]),
    ):
        r_no_os = _client().get("/api/provision/services", params={"token": "tok"})
        assert r_no_os.status_code == 400
        assert r_no_os.json()["detail"]["error"] == "object_storage_not_enabled"

    probe_called: list[bool] = []
    with (
        patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("HTTP 401 GET /service")),
        patch(
            "backend.provision.fos_setup.object_storage_enabled",
            side_effect=lambda *a, **k: probe_called.append(True) or False,
        ),
    ):
        r_bad = _client().get("/api/provision/services", params={"token": "bad"})
        assert r_bad.status_code == 400
        assert r_bad.json()["detail"]["error"] == "list_services_failed"
        assert probe_called == []


def test_provision_validate_flows():
    for bad_body in (
        {"service_id": "svc"},
        {"token": "tok"},
        {"token": "", "service_id": "svc"},
        {"token": "tok", "service_id": ""},
    ):
        assert _client().post("/api/provision/validate", json=bad_body).status_code == 400

    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=lambda m, p, **kw: (
            {"id": "tok-1", "name": "Test Token", "user_id": "u1"}
            if "/tokens/self" in p
            else {"name": "My Production CDN"}
        ),
    ):
        resp = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc.with/specials!"})
        assert resp.status_code == 200
        body = resp.json()
        assert body["service_name"] == "My Production CDN"
        assert not any(ch in body["defaults"]["fos_bucket_name"] for ch in ("/", ".", "!"))
        assert body["token_info"]["type"] == "user"

    with patch(
        "backend.core.fastly.client.fastly",
        side_effect=lambda m, p, **kw: {"id": "tok-bot", "name": "Bot Token"} if "/tokens/self" in p else {"name": "X"},
    ):
        resp_bot = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc1"})
        assert resp_bot.json()["token_info"]["type"] == "automation"

    def _fail_tok(m, p, *a, **kw):
        if p == "/tokens/self":
            raise RuntimeError("token endpoint down")
        return {"name": "My Service"}

    with patch("backend.core.fastly.client.fastly", side_effect=_fail_tok):
        resp_swallow = _client().post("/api/provision/validate", json={"token": "tok", "service_id": "svc-abc"})
        assert resp_swallow.status_code == 200
        assert resp_swallow.json()["token_info"] == {}


# ─── /api/provision/check-domain & _check_domain_available ───────────────────


@pytest.mark.parametrize(
    ("query", "mock_ret", "expected_avail", "expected_call_arg"),
    [
        ("prefix=-bad", None, False, None),
        ("prefix=foo_bar", None, False, None),
        ("prefix=valid-prefix-1", (True, None), True, "valid-prefix-1.global.ssl.fastly.net"),
        ("prefix=my.custom.domain.com&is_custom=true", (True, None), True, "my.custom.domain.com"),
        (
            "prefix=my.custom.domain.com&is_custom=true",
            (False, "Domain already registered or in use"),
            False,
            "my.custom.domain.com",
        ),
    ],
)
def test_check_domain_routes(query, mock_ret, expected_avail, expected_call_arg):
    assert _client().get("/api/provision/check-domain?prefix=").status_code in (400, 422)
    with patch("backend.routers.provision._check_domain_available", return_value=mock_ret) as mock_check:
        resp = _client().get(f"/api/provision/check-domain?{query}")
    assert resp.status_code == 200
    assert resp.json()["available"] is expected_avail
    if expected_call_arg:
        mock_check.assert_called_once_with(expected_call_arg)


def test_check_domain_available_helper_branches():
    fake_resp = MagicMock()
    fake_resp.__enter__ = lambda s: s
    fake_resp.__exit__ = MagicMock(return_value=False)
    with patch("urllib.request.urlopen", return_value=fake_resp), patch("backend.utils.telemetry.tracked_call"):
        ok, reason = _check_domain_available("test.global.ssl.fastly.net")
        assert ok is False and "in use" in (reason or "").lower()

    def _http_404(body: bytes):
        return urllib.error.HTTPError(url="https://x", code=404, msg="not found", hdrs={}, fp=BytesIO(body))  # type: ignore[arg-type]

    with (
        patch(
            "urllib.request.urlopen",
            side_effect=_http_404(b"... Please check that this domain has been added to a service. ..."),
        ),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        assert _check_domain_available("test.global.ssl.fastly.net") == (True, None)

    with (
        patch("urllib.request.urlopen", side_effect=_http_404(b"some other body")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        ok_o, reason_o = _check_domain_available("test.global.ssl.fastly.net")
        assert ok_o is False and "already registered" in (reason_o or "")

    with (
        patch("urllib.request.urlopen", side_effect=urllib.error.URLError("dns failed")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        ok_dns, reason_dns = _check_domain_available("test.global.ssl.fastly.net")
        assert ok_dns is True and "DNS/Connection" in (reason_dns or "")

    with (
        patch("urllib.request.urlopen", side_effect=RuntimeError("unexpected")),
        patch("backend.utils.telemetry.tracked_call"),
    ):
        ok_ex, reason_ex = _check_domain_available("foo.example")
        assert ok_ex is False and "unexpected" in (reason_ex or "")


# ─── /api/provision/check-fos & /terraform/* ─────────────────────────────────


def test_check_fos_success_and_sample_get():
    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = [{"Contents": []}, {"Contents": []}]
    fos_req = {"bucket": "b", "region": "us-east-1", "access_key": "ak", "secret_key": "sk"}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client) as get_client:
        resp = _client().post("/api/provision/check-fos", json=fos_req)
    assert resp.status_code == 200 and resp.json()["ok"] is True
    get_client.assert_called_once_with("ak", "sk", "us-east-1", bucket_name="b", context="provision:check-fos")
    assert [c.kwargs["Prefix"] for c in fake_client.list_objects_v2.call_args_list] == ["raw/request/", "raw/rum/"]

    fake_client2 = MagicMock()
    sample_key = "raw/request/year=2026/month=09/day=22/hour=13/minute=00/object.gz"
    fake_client2.list_objects_v2.return_value = {"Contents": [{"Key": sample_key}]}
    fake_client2.get_object.return_value = {"Body": MagicMock()}
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client2):
        resp2 = _client().post("/api/provision/check-fos", json=fos_req)
    assert resp2.status_code == 200 and resp2.json()["ok"] is True
    fake_client2.get_object.assert_called_once_with(Bucket="b", Key=sample_key, Range="bytes=0-0")


def _fos_client_err(code: str) -> botocore.exceptions.ClientError:
    return botocore.exceptions.ClientError({"Error": {"Code": code, "Message": "raw"}}, "ListObjectsV2")


@pytest.mark.parametrize(
    ("exc", "expected_in_msg"),
    [
        (_fos_client_err("AccessDenied"), "Access Denied"),
        (_fos_client_err("InvalidAccessKeyId"), "Access Denied"),
        (_fos_client_err("SignatureDoesNotMatch"), "Access Denied"),
        (_fos_client_err("NoSuchBucket"), "Bucket not found"),
        (_fos_client_err("IllegalLocationConstraintException"), "Region mismatch"),
        (RuntimeError("EndpointConnectionError: Could not connect to the endpoint URL"), "verify the FOS Region"),
        (RuntimeError("weird unknown error xyz"), "weird unknown error xyz"),
    ],
)
def test_check_fos_error_mappings(exc, expected_in_msg):
    fake_client = MagicMock()
    fake_client.list_objects_v2.side_effect = exc
    with patch("backend.routers.provision._get_fos_s3_client", return_value=fake_client):
        resp = _client().post(
            "/api/provision/check-fos",
            json={"bucket": "b", "region": "us-east-1", "access_key": "k", "secret_key": "s"},
        )
    body = resp.json()
    assert body["ok"] is False and expected_in_msg in body["error"]


def test_terraform_preview_and_export():
    fake_files = {"main.tf": 'resource "fastly_service_vcl" "x" {}\n', "instructions": "# Setup steps\n"}
    with patch("backend.utils.terraform_gen.generate_terraform", return_value=fake_files):
        prev = _client().post(
            "/api/provision/terraform/preview",
            json={
                "service_id": "svc-1",
                "fos_bucket": "b",
                "cdn_service_name": "CDN",
                "cdn_url": "https://x.global.ssl.fastly.net",
                "fos_region": "us-east-1",
            },
        )
        assert prev.status_code == 200 and "main.tf" in prev.json()

        exp = _client().post("/api/provision/terraform/export", json={"service_id": "svc"})
        assert exp.status_code == 200
        assert exp.headers["content-type"] == "application/zip"
        assert "fastly-log-analysis-terraform.zip" in exp.headers["content-disposition"]
        names = zipfile.ZipFile(io.BytesIO(exp.content)).namelist()
        assert "README.md" in names and "main.tf" in names and "instructions" not in names


# ─── /api/provision/ingest ───────────────────────────────────────────────────


def test_ingest_validation_and_key_flows(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    assert _client().post("/api/provision/ingest", json={"service_id": "svc"}).status_code == 400

    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=ValueError("unknown period: fortnight")),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc")),
    ):
        r_period = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc", "log_period": "1 fortnight"},
        )
        assert r_period.status_code == 400
        assert r_period.json()["detail"]["error"] == "invalid_log_period"

    new_key = {"access_key": "AKNEW", "secret_key": "SKNEW", "id": "kid"}
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch("backend.provision.ensure_fos_access_key", return_value=new_key) as mock_ensure,
        patch("backend.provision.write_service_config") as mock_write,
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_new = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc-1", "fos_bucket_name": "b", "fos_region": "us-east-1"},
        )
        assert r_new.status_code == 200
        mock_ensure.assert_called_once()
        persisted = mock_write.call_args[0][0]
        assert persisted["fos_access_key_id"] == "AKNEW" and persisted["fos_secret_access_key"] == "SKNEW"

    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch("backend.provision.ensure_fos_access_key", side_effect=RuntimeError("rate limited")),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_err = _client().post(
            "/api/provision/ingest",
            json={"token": "t", "service_id": "svc-1", "fos_bucket_name": "b"},
        )
        assert r_err.status_code == 400
        assert r_err.json()["detail"]["error"] == "ensure_access_key_failed"

    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key") as mock_find,
        patch("backend.provision.ensure_fos_access_key") as mock_ensure2,
        patch("backend.provision.write_service_config"),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        r_prov = _client().post(
            "/api/provision/ingest",
            json={
                "token": "t",
                "service_id": "svc-1",
                "fos_bucket_name": "b",
                "fos_access_key": "EXISTING_AK",
                "fos_secret_key": "EXISTING_SK",
            },
        )
        assert r_prov.status_code == 200
        mock_find.assert_not_called()
        mock_ensure2.assert_not_called()


def test_ingest_rerun_preserves_faro_pin_and_cdn_secret(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        "svc-1",
        {
            "service_id": "svc-1",
            "cdn_secret": "persisted-cdn-secret",
            "rum_enabled": True,
            "rum": {
                "enabled": True,
                "enabled_at": "2026-01-01T00:00:00Z",
                "faro_version": "9.9.9",
                "faro_content_hash": "existing-hash",
                "faro_fos_etag_md5": "existing-etag",
            },
        },
    )
    captured: dict = {}
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.provision.parse_period", side_effect=lambda x: 60),
        patch("backend.provision.find_fos_key", return_value=None),
        patch(
            "backend.provision.ensure_fos_access_key",
            return_value={"access_key": "AK", "secret_key": "SK", "id": "kid"},
        ),
        patch("backend.provision.write_service_config", side_effect=lambda st: captured.update(state=st)),
        patch("backend.provision._sync_crontab"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc-1")),
    ):
        base_req = {"token": "t", "service_id": "svc-1", "fos_bucket_name": "b", "rum_enabled": True}
        resp = _client().post("/api/provision/ingest", json=base_req)
        assert resp.status_code == 200
        assert captured["state"]["cdn_secret"] == "persisted-cdn-secret"
        assert captured["state"]["rum"]["faro_version"] == "9.9.9"
        assert captured["state"]["rum"]["faro_content_hash"] == "existing-hash"

        resp_override = _client().post("/api/provision/ingest", json={**base_req, "rum": {"faro_version": "1.0.0"}})
        assert resp_override.status_code == 200
        assert captured["state"]["rum"]["faro_version"] == "1.0.0"


# ─── /api/provision/execute ──────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("payload_extra", "parse_period_effect", "domain_avail", "expected_err_sub"),
    [
        ({"fos_bucket_name": "invalid_underscore_name"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "bad--bucket"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "-leading-hyphen"}, 60, (True, None), "invalid_bucket"),
        ({"fos_bucket_name": "valid-bucket", "fos_prefix": "x'; DROP TABLE--"}, 60, (True, None), "prefix"),
        (
            {"fos_bucket_name": "valid-bucket", "log_period": "fortnight"},
            ValueError("unknown: fortnight"),
            (True, None),
            "invalid_log_period",
        ),
        (
            {"fos_bucket_name": "valid-bucket", "cdn_url": "https://taken.global.ssl.fastly.net"},
            60,
            (False, "Domain already registered or in use"),
            "unavailable",
        ),
    ],
)
def test_provision_execute_validation_rejections(payload_extra, parse_period_effect, domain_avail, expected_err_sub):
    pp_kw = (
        {"side_effect": parse_period_effect}
        if isinstance(parse_period_effect, Exception)
        else {"return_value": parse_period_effect}
    )
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", **pp_kw),
        patch("backend.routers.provision._check_domain_available", return_value=domain_avail),
    ):
        r = _client().post(
            "/api/provision/execute",
            json={"token": "tok", "service_id": "svc-1", **payload_extra},
        )
    assert r.status_code == 400
    assert expected_err_sub in r.json()["detail"]["error"].lower()


def test_provision_execute_dns_fallback_and_faro_version_threading():
    captured: dict = {}

    def fake_provision(cfg, _resume_from_state=False):
        captured["cfg"] = cfg
        yield {"type": "done", "message": "ok"}

    with (
        patch("backend.utils.pop_utils.fetch_pop_locations"),
        patch("backend.config.fetch_service_name", return_value="x"),
        patch("backend.provision.parse_period", return_value=60),
        patch(
            "backend.routers.provision._check_domain_available",
            return_value=(False, "DNS resolution failed transiently"),
        ),
        patch("backend.provision.provision", side_effect=fake_provision),
        patch("backend.provision._sync_crontab"),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.core.metadata.record_audit"),
    ):
        base_req = {"token": "tok", "service_id": "svc-1", "fos_bucket_name": "valid-bucket", "rum_enabled": True}
        r1 = _client().post(
            "/api/provision/execute",
            json={**base_req, "cdn_url": "https://x.global.ssl.fastly.net", "faro_version": "1.2.3"},
        )
        assert r1.status_code == 200
        assert captured["cfg"]["rum"]["faro_version"] == "1.2.3"

        r2 = _client().post("/api/provision/execute", json=base_req)
        assert r2.status_code == 200
        assert captured["cfg"]["rum"]["faro_version"] == DEFAULT_FARO_VERSION


# ─── /api/provision/check-config ─────────────────────────────────────────────

_CHECK_PARAMS = {"token": "t", "service_id": "log-svc", "cdn_service_id": "cdn-svc", "bucket": "b"}
_REQUIRED_SNIPPETS = [
    "Fastly Log Analysis Capture",
    "Fastly Log Analysis Miss",
    "Fastly Log Analysis Pass",
]
_OPTIONAL_SNIPPETS = [
    "Fastly Log Analysis Origin Fetch",
    "Fastly Log Analysis Origin Error",
    "Fastly Log Analysis Origin Deliver",
]
_REQUIRED_CDN_SNIPPETS = [
    "cdn-swr-shield-disable",
    "cdn-race-condition-generation",
    "cdn-no-cache-404",
]


def _check_config_fastly_router(*, endpoints, snippets, dicts, backends, cdn_snippets):
    def fake_fastly(method, path, token=None, **_):
        if path == "/service/log-svc/version/active":
            return {"number": 7}
        if "/version/7/logging/s3" in path:
            return endpoints
        if "/version/7/snippet" in path:
            return snippets
        if "/version/active/dictionary" in path:
            return dicts
        if "/version/active/backend" in path:
            return backends
        if path == "/service/cdn-svc/version/active":
            return {"number": 3}
        if "/version/3/snippet" in path:
            return cdn_snippets
        raise AssertionError(f"unexpected fastly path: {path}")

    return fake_fastly


def test_check_config_logging_and_cdn_permutations():
    by_name = {entry[0]: entry for entry in _CDN_SNIPPETS}
    assert _CDN_NO_CACHE_404_SNIPPET_NAME in by_name
    _name, snippet_type, content, _priority = by_name[_CDN_NO_CACHE_404_SNIPPET_NAME]
    assert snippet_type == "fetch"
    assert 'req.method == "GET"' in content and 'req.method == "HEAD"' in content
    assert "beresp.status == 404" in content and "set beresp.cacheable = false;" in content

    full_dicts = [{"name": "fos_credentials"}, {"name": "cdn_auth"}]
    full_backends = [{"address": "x.fastlystorage.app"}]
    full_cdn_snips = [{"name": s} for s in _REQUIRED_CDN_SNIPPETS]

    # 1. No matching logging endpoint
    side_no_ep = _check_config_fastly_router(
        endpoints=[{"name": "other", "bucket_name": "different-bucket"}],
        snippets=[],
        dicts=full_dicts,
        backends=full_backends,
        cdn_snippets=full_cdn_snips,
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_no_ep):
        body = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert body["logging_service"]["ok"] is False
        assert "No S3 logging endpoint" in body["logging_service"]["details"]

    # 2. Missing core logging snippets + missing CDN perf snippets
    side_missing_snip = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "fos-sample"}],
        snippets=[{"name": "Fastly Log Analysis Capture"}],
        dicts=full_dicts,
        backends=full_backends,
        cdn_snippets=[{"name": "cdn-swr-shield-disable"}],
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_missing_snip):
        res = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert "missing CORE snippets" in res["logging_service"]["details"]
        assert res["cdn_service"]["ok"] is False
        assert "cdn-no-cache-404" in res["cdn_service"]["details"]

    # 3. Full logging + optional origin snippets + full CDN
    side_full = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": "fos-sample"}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS + _OPTIONAL_SNIPPETS],
        dicts=full_dicts,
        backends=full_backends,
        cdn_snippets=full_cdn_snips,
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_full):
        res_full = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert res_full["logging_service"]["ok"] is True
        assert "3 origin metric snippets" in res_full["logging_service"]["details"]
        assert res_full["cdn_service"]["ok"] is True
        assert "fully configured" in res_full["cdn_service"]["details"]

    # 4. No response condition warns + CDN missing dicts/backend
    side_warn = _check_config_fastly_router(
        endpoints=[{"name": "fos-logs", "bucket_name": "b", "response_condition": None}],
        snippets=[{"name": s} for s in _REQUIRED_SNIPPETS],
        dicts=[],
        backends=[],
        cdn_snippets=[],
    )
    with patch("backend.core.fastly.client.fastly", side_effect=side_warn):
        res_warn = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert res_warn["logging_service"]["ok"] is True
        assert "no response condition" in res_warn["logging_service"]["details"].lower()
        assert res_warn["cdn_service"]["ok"] is False
        assert "fos_credentials" in res_warn["cdn_service"]["details"]

    # 5. Logging & CDN exceptions captured in details
    with patch("backend.core.fastly.client.fastly", side_effect=RuntimeError("Fastly 503")):
        res_err = _client().get("/api/provision/check-config", params=_CHECK_PARAMS).json()
        assert "Fastly 503" in res_err["logging_service"]["details"]
        assert "Fastly 503" in res_err["cdn_service"]["details"]


# ─── /api/provision/teardown, product helpers, /reconcile, JSON content-type ─


def test_teardown_endpoint_flows():
    with patch("backend.config.load_config", return_value=None):
        resp_404 = _client().post("/api/provision/teardown", json={})
        assert resp_404.status_code == 404
        assert "No service config" in resp_404.json()["detail"]["error"]

    fake_cfg = {
        "service_id": "svc",
        "fastly_api_key": "key",
        "fos_bucket": "test-bucket",
        "fos_region": "us-east-1",
        "name": "Test Svc",
        "provisioning": {},
    }
    sync_calls: list[bool] = []
    fake_scheduler = MagicMock()
    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent-config.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab", side_effect=lambda: sync_calls.append(True)),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.core.storage.clear_source_caches"),
        patch(
            "backend.provision.perform_teardown",
            return_value=iter([{"type": "status", "message": "removed s3 bucket"}]),
        ),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc", "cust-X")),
    ):
        resp = _client().post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": True},
        )
        assert resp.status_code == 200
        assert "removed s3 bucket" in resp.text and '"type": "done"' in resp.text and "Cron jobs updated" in resp.text
        assert sync_calls == [True]
        fake_scheduler.reload.assert_called_once()

    def _boom(state, token, opts=None):
        raise RuntimeError("Fastly API rate-limited")
        yield

    with (
        patch("backend.config.load_config", return_value=fake_cfg),
        patch("backend.config.config_path", return_value="/tmp/nonexistent.json"),
        patch("backend.config.duckdb_path", return_value="/tmp/nonexistent.duckdb"),
        patch("backend.provision._sync_crontab"),
        patch("backend.cron.scheduler.get_scheduler", return_value=MagicMock()),
        patch("backend.core.storage.clear_source_caches"),
        patch("backend.provision.perform_teardown", side_effect=_boom),
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.utils.fastly_auth.fastly", side_effect=_auth_fastly_ok("svc", "cust-X")),
    ):
        resp_err = _client().post(
            "/api/provision/teardown",
            json={"service_id": "svc", "token": "test-tok", "remove_cache": False, "remove_cron": False},
        )
        assert resp_err.status_code == 200
        assert '"type": "error"' in resp_err.text and "Fastly API rate-limited" in resp_err.text


def test_product_and_object_storage_helpers():
    with patch("backend.provision.fos_setup.fastly", return_value={"product": {"id": "object_storage"}}):
        assert object_storage_enabled("tok") is True
    with patch(
        "backend.provision.fos_setup.fastly",
        side_effect=RuntimeError("HTTP 404 GET /enabled-products/v1/object_storage"),
    ):
        assert object_storage_enabled("tok") is False
    for err_msg in ("HTTP 503 GET x", "Network error on GET x"):
        with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError(err_msg)):
            assert object_storage_enabled("tok") is True

    with patch("backend.provision.fos_setup.fastly", return_value={"product": {"id": "kv_store"}}) as m:
        assert product_enabled("tok", "kv_store") is True
        assert m.call_args.args[:2] == ("GET", "/enabled-products/v1/kv_store")
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 404 not found")):
        assert product_enabled("tok", "kv_store") is False
    with patch("backend.provision.fos_setup.fastly", side_effect=RuntimeError("HTTP 502 bad gateway")):
        assert product_enabled("tok", "kv_store") is True

    with patch("backend.provision.fos_setup.product_enabled", return_value=True) as m_del:
        assert fos_setup.object_storage_enabled("tok") is True
        assert m_del.call_args.args == ("tok", "object_storage")


def test_reconcile_route_and_json_content_type():
    post_op = app.openapi()["paths"]["/api/provision/reconcile"]["post"]
    assert [p["name"] for p in post_op.get("parameters", []) if p.get("in") == "query"] == []
    assert "requestBody" in post_op

    with patch("backend.provision.declarative.reconciler.reconcile_vcl_state") as mock_reconcile:
        resp = _client().post("/api/provision/reconcile", json={})
        assert resp.status_code == 400
        mock_reconcile.assert_not_called()

    for ct in (b"application/json", b"application/json; charset=utf-8"):
        req = Request({"type": "http", "method": "POST", "path": "/", "headers": [(b"content-type", ct)]}, lambda: None)
        _require_json_content_type(req)

    for bad_hdrs in ([(b"content-type", b"text/plain")], []):
        req = Request({"type": "http", "method": "POST", "path": "/", "headers": bad_hdrs}, lambda: None)
        with pytest.raises(HTTPException) as exc:
            _require_json_content_type(req)
        assert exc.value.status_code == 415
