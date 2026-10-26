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
from backend import system_metrics_sampler as sampler
from backend.core import metric_snapshots
from backend.core.duckdb import DBBusyError
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.deps import get_source, require_admin
from backend.main import app
from backend.routers.admin import events as ev
from backend.routers.admin import router as admin_router
from backend.utils.date_utils import iso_z
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
            "backend.core.iceberg.get_table_info",
            {"table_name": "logs_test_service", "snapshots": 7, "data_files": 42, "size_bytes": 52428800},
            lambda b: b["data_files"] == 42 and b["snapshots"] == 7,
        ),
        (
            "/api/admin/iceberg-calendar",
            "backend.core.iceberg.get_snapshot_calendar",
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
        "access_level": "read_write",
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
            assert r.status_code == 200 and r.json()["local_rows"] == 1250 and r.json()["access_level"] == "read_write"

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

        ro_src = _make_source(access_level="read_only")
        app.dependency_overrides[get_source] = lambda: ro_src
        for path in (f"/api/admin/sync/{ro_src['service_id']}", f"/api/admin/commit/{ro_src['service_id']}"):
            assert overview_client.post(path).status_code == 403
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


def test_iceberg_and_rebuild_local_view_endpoints(overview_client, override_source):
    fake_info = {
        "table_name": "logs",
        "table_location": "s3://bucket/prefix/iceberg",
        "snapshots": 5,
        "data_files": 12,
        "size_bytes": 1024,
        "latest_snapshot_at": "2026-08-19T00:00:00Z",
        "buffer_files": 1,
        "buffer_size_bytes": 512,
    }
    with patch("backend.core.iceberg.get_table_info", return_value=fake_info) as mock_get:
        r_info = overview_client.get("/api/admin/iceberg-info?table_name=logs")
        assert r_info.status_code == 200 and r_info.json()["size_bytes"] == 1024
        mock_get.assert_called_once_with(override_source, table_name="logs")

    fake_cal = {"2026-08-19": {"data_files": 5, "size_bytes": 1024}}
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=fake_cal) as mock_cal:
        r_cal = overview_client.get("/api/admin/iceberg-calendar?table_name=logs")
        assert r_cal.status_code == 200 and r_cal.json()["2026-08-19"] == {"data_files": 5, "size_bytes": 1024}
        mock_cal.assert_called_once_with(override_source, table_name="logs")

    with patch("backend.utils.router_utils.start_or_resume_cron", return_value={"ok": True}) as mock_start:
        assert overview_client.post("/api/admin/commit-iceberg").status_code == 202 and mock_start.called

    with (
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.duckdb.start_cron_run", return_value="run-123") as mock_start_cron,
        patch("backend.cron_progress.start_progress") as mock_prog,
        patch("threading.Thread") as mock_thread,
    ):
        resp = overview_client.post("/api/admin/rebuild-local-view")
        assert resp.status_code == 202 and resp.json()["run_id"] == "run-123"
        mock_clear.assert_called_once_with("service123")
        mock_start_cron.assert_called_once_with(override_source, "metadata_sync")
        mock_prog.assert_called_once_with("run-123", service_id="service123", task="metadata_sync")
        mock_thread.assert_called_once()

    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value="/tmp"),
        patch("os.path.exists", return_value=False),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("busy")),
    ):
        assert overview_client.post("/api/admin/rebuild-local-view").status_code == 503


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

    override_source["access_level"] = "read_only"
    assert overview_client.post(endpoint, json=payload).status_code == 403


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
