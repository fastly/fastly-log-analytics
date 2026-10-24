"""Multiplexed admin event stream: ``GET /api/admin/events/stream``.

Collapses sync-status + cron-runs + system-metrics + share onto ONE
connection. Covers channel validation, envelope framing, the serviceless
union (service-scoped feeders skipped when no service; global feeders —
system-metrics + share — still run), drop-oldest fan-in, and feeder
cleanup on disconnect.

Streaming is exercised by driving the handler directly and pulling a fixed
number of frames off ``response.body_iterator`` then ``aclose()``-ing it —
the same approach test_admin_system_metrics_stream.py uses, because a
``while True`` fan-in generator would deadlock TestClient's sync portal
(which buffers the whole body). TestClient is used only for the
immediate-return 422 validation cases.
"""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from functools import partial
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from backend import system_metrics_sampler as sampler
from backend.core import metric_snapshots
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.deps import get_source, require_admin
from backend.main import app
from backend.routers.admin import events as ev
from backend.routers.admin import router as admin_router
from backend.utils.date_utils import iso_z
from backend.utils.remote_access import _is_blocked_path
from tests._helpers.sources import make_source
from tests.conftest import MOCK_SERVICE_ID
from tests.utils.polling import await_until


# ── Merged from tests/routers/test_admin_events_stream.py ──
class _FakeRequest:
    """Minimal Request stand-in: never reports disconnected, so the test
    controls stream lifetime via ``body_iterator.aclose()``."""

    async def is_disconnected(self) -> bool:
        return False


async def _drive(channels: str, service_id: str | None, n_frames: int) -> list[dict]:
    """Open the merged stream, pull ``n_frames`` envelopes, then aclose()
    (which runs the handler's finally → cancels + gathers feeders)."""
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
    """Fan-in is bounded drop-oldest so a bursty channel can't grow memory
    or block its feeder — the newest frame must always land."""
    q: asyncio.Queue = asyncio.Queue(maxsize=2)
    for i in range(5):
        ev._offer(q, {"channel": "system-metrics", "data": {"n": i}})
    drained = [q.get_nowait() for _ in range(q.qsize())]
    assert len(drained) == 2
    assert drained[-1]["data"]["n"] == 4  # newest survived
    assert drained[0]["data"]["n"] > 2  # oldest dropped


def test_unknown_channel_is_422():
    with TestClient(app) as client:
        resp = client.get("/api/admin/events/stream", params={"channels": "sync-status,bogus"})
        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "unknown_channel"


def test_empty_channels_is_422():
    with TestClient(app) as client:
        resp = client.get("/api/admin/events/stream", params={"channels": " , "})
        assert resp.status_code == 422
        assert resp.json()["detail"]["error"] == "channels_required"


@pytest.mark.asyncio
async def test_sync_status_channel_wraps_initial_and_published(monkeypatch):
    """sync-status emits the cached initial snapshot then publisher events,
    each wrapped in a ``{channel, data}`` envelope."""
    monkeypatch.setattr(ev, "compute_sync_status_cached", lambda _sid: {"local_rows": 7})

    async def _fake_subscribe(_svc):
        yield {"local_rows": 8, "latest_log_at": "2026-06-26T00:00:00Z"}

    monkeypatch.setattr(ev.sync_status_publisher, "subscribe", _fake_subscribe)

    frames = await _drive("sync-status", "svc-1", n_frames=2)
    assert frames[0] == {"channel": "sync-status", "data": {"local_rows": 7}}
    assert frames[1]["channel"] == "sync-status"
    assert frames[1]["data"]["local_rows"] == 8


@pytest.mark.asyncio
async def test_system_metrics_channel_wraps_sample(monkeypatch):
    """system-metrics emits the (deduped) sampler payload wrapped in an envelope."""
    bundle = {"health_snapshot": {"vcpus": 4}, "slow_queries_count": None}

    async def _fake_cached(_sid):
        return bundle

    monkeypatch.setattr(ev, "sample_system_metrics_cached", _fake_cached)

    frames = await _drive("system-metrics", "svc-1", n_frames=1)
    assert frames[0] == {"channel": "system-metrics", "data": bundle}


@pytest.mark.asyncio
async def test_share_channel_wraps_payload_serviceless(monkeypatch):
    """share is global-admin: it streams even with no service (service_id=None),
    wrapping the lean tunnel-live payload in a ``{channel, data}`` envelope.
    The feeder lazy-imports build_share_live_payload from backend.utils.tunnel,
    so patch it on that package."""
    import backend.utils.tunnel as tunnel

    payload = {"sharing_active": True, "public_url": "https://x.test", "active_session_count": 1}
    monkeypatch.setattr(tunnel, "build_share_live_payload", lambda: payload)

    frames = await _drive("share", None, n_frames=1)
    assert frames[0] == {"channel": "share", "data": payload}


@pytest.mark.asyncio
async def test_serviceless_runs_only_global_metrics(monkeypatch):
    """With no service, the service-scoped feeders (sync-status, cron-runs)
    are skipped entirely; only the global feeders (system-metrics) stream."""
    bundle = {"health_snapshot": {"vcpus": 2}}

    async def _fake_cached(_sid):
        return bundle

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

    frames = await _drive("sync-status,cron-runs,system-metrics", None, n_frames=1)
    assert frames[0]["channel"] == "system-metrics"
    assert called == {"sync": False, "cron": False}


@pytest.mark.asyncio
async def test_only_requested_channels_feed(monkeypatch):
    """Requesting just system-metrics must not subscribe to the publishers."""

    async def _fake_cached(_sid):
        return {"health_snapshot": {}}

    monkeypatch.setattr(ev, "sample_system_metrics_cached", _fake_cached)

    touched = {"sync": False, "cron": False}

    async def _sync_sub(_svc):
        touched["sync"] = True
        yield {}

    async def _cron_sub(_svc):
        touched["cron"] = True
        yield {}

    monkeypatch.setattr(ev.sync_status_publisher, "subscribe", _sync_sub)
    monkeypatch.setattr(ev.cron_runs_publisher, "subscribe", _cron_sub)

    await _drive("system-metrics", "svc-1", n_frames=1)
    assert touched == {"sync": False, "cron": False}


@pytest.mark.asyncio
async def test_cleanup_cancels_publisher_subscription(monkeypatch):
    """When the stream closes, the finally block cancels every feeder so
    the publisher's per-service subscriber set drops back to empty — no
    leaked subscriptions accumulating one per opened-then-closed tab."""
    ev.sync_status_publisher.bind_loop(asyncio.get_running_loop())
    # No initial snapshot so the sync-status feeder goes straight to the
    # real publisher.subscribe() and registers its queue.
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
    # Pull the guaranteed system-metrics initial frame, then wait for the
    # sync-status feeder to register against the real publisher.
    json.loads(await agen.__anext__())
    await await_until(
        lambda: ev.sync_status_publisher.subscriber_count("svc-cleanup") == 1,
        timeout=2.0,
        message="sync-status feeder never registered its subscription",
    )

    await agen.aclose()
    await await_until(
        lambda: ev.sync_status_publisher.subscriber_count("svc-cleanup") == 0,
        timeout=2.0,
        message="feeder subscription leaked after stream close",
    )


@pytest.mark.asyncio
async def test_initial_snapshot_runs_off_event_loop_thread(monkeypatch):
    """The sync-status initial snapshot calls ``compute_sync_status_cached``,
    which does synchronous disk I/O. The feeder must offload it via
    ``asyncio.to_thread`` so it doesn't stall the event loop for every other
    concurrent request on the single-worker backend (regression F008,
    carried over from the old per-endpoint stream)."""
    invoked: list[str] = []

    def _fake_compute(_sid):
        invoked.append(threading.current_thread().name)
        return {"local_rows": 1}

    monkeypatch.setattr(ev, "compute_sync_status_cached", _fake_compute)

    async def _empty_subscribe(_svc):
        return
        yield  # pragma: no cover — keeps this an async generator

    monkeypatch.setattr(ev.sync_status_publisher, "subscribe", _empty_subscribe)

    await _drive("sync-status", "svc-thread", n_frames=1)

    assert invoked, "compute_sync_status_cached was never called"
    main = threading.main_thread().name
    for tname in invoked:
        assert tname != main, (
            f"compute_sync_status_cached ran on the event-loop thread ({tname}) — "
            "the feeder must wrap it in asyncio.to_thread"
        )


# ── Merged from tests/routers/test_admin_get_endpoints.py ──
def test_get_pop_locations_returns_cached_pops(client):
    fake_pops = [
        {"code": "LAX", "name": "Los Angeles", "coordinates": {"latitude": 33.9, "longitude": -118.4}},
        {"code": "SJC", "name": "San Jose", "coordinates": {"latitude": 37.4, "longitude": -121.9}},
    ]
    with patch("backend.utils.pop_utils.get_pop_locations", return_value=fake_pops):
        resp = client.get("/api/admin/pop-locations")

    assert resp.status_code == 200
    data = resp.json()
    assert "pops" in data
    assert len(data["pops"]) == 2


def test_get_pop_locations_returns_empty_list_when_cache_missing(client):
    """No cache file → empty list (not 500). Pinned because fresh
    installs hit this on the first dashboard load."""
    with patch("backend.utils.pop_utils.get_pop_locations", return_value=[]):
        resp = client.get("/api/admin/pop-locations")

    assert resp.status_code == 200
    assert resp.json()["pops"] == []


def test_sync_status_returns_configured_false_when_no_service():
    """Bare sync-status call with no service set up → ``configured=False``
    (not a 500). Pinned because the FE keys on this to render the
    "configure a service" empty state."""
    from backend.main import app

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        from fastapi.testclient import TestClient

        with TestClient(app) as c:
            resp = c.get("/api/sync-status")

    assert resp.status_code == 200
    assert resp.json()["configured"] is False


def test_sync_status_returns_503_on_db_busy(client):
    """``DBBusyError`` from get_sync_status → 503 with ``busy: true``.
    Pinned because the frontend keys on this exact shape to keep
    cached data instead of clearing the UI."""
    from backend.core.duckdb import DBBusyError

    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.core.duckdb.get_sync_status", side_effect=DBBusyError("locked")),
    ):
        mock_conn.return_value = type("C", (), {"close": lambda self: None})()
        resp = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 503
    assert resp.json()["detail"]["busy"] is True


def test_sync_status_500s_on_unexpected_exception(client):
    """Non-DBBusy exception → 500 with the error string. Pinned
    because losing this would surface the dashboard banner without
    a debuggable message."""
    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.core.duckdb.get_sync_status", side_effect=RuntimeError("config corrupt")),
    ):
        mock_conn.return_value = type("C", (), {"close": lambda self: None})()
        resp = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 500


def test_sync_status_routes_high_scale_service_without_duckdb(client):
    fake_src = {"name": "high_scale_svc", "service_id": "HS123", "bucket": "b"}
    cached_metrics = {
        "configured": True,
        "local_rows": 1000,
        "latest_log_at": "2026-09-16T06:00:00Z",
    }
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.high_scale.registry.get_high_scale_service_registry") as mock_reg,
        patch("backend.routers.admin.sync_status.compute_sync_status_cached", return_value=cached_metrics),
        patch("backend.core.duckdb.get_connection") as mock_conn,
    ):
        mock_reg.return_value.resolve.return_value = object()
        resp = client.get("/api/sync-status?service_id=HS123")
        assert resp.status_code == 200
        assert resp.json()["local_rows"] == 1000
        mock_conn.assert_not_called()


def test_sync_status_uses_read_only_in_durable_serving_mode(client):
    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.core.duckdb.get_sync_status", return_value={"configured": True}),
    ):
        mock_conn.return_value = type("C", (), {"close": lambda self: None})()
        resp = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
        assert resp.status_code == 200
        mock_conn.assert_called_once_with(source=fake_src, max_wait=5, skip_view_update=True, read_only=True)


def test_log_extents_returns_configured_false_when_no_service():
    """No service set up → ``configured=False`` (same shape as
    sync-status). The FilterBar uses this to short-circuit the
    snap-to-extents flow."""
    from backend.main import app

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        from fastapi.testclient import TestClient

        with TestClient(app) as c:
            resp = c.get("/api/log-extents")

    assert resp.status_code == 200
    body = resp.json()
    assert body["configured"] is False
    assert "ngwaf_workspace_id" not in body
    assert "active_run" not in body


def test_log_extents_returns_cached_extents(client):
    """Reads only the persisted status snapshot — no DuckDB hit, no
    503 path. Confirms the extents come through and the analyst-
    sensitive fields stay out."""
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
        resp = client.get("/api/log-extents", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["configured"] is True
    assert body["earliest_log_at"] == "2026-06-09T00:00:00Z"
    assert body["latest_log_at"] == "2026-06-10T12:34:56Z"
    assert "ngwaf_workspace_id" not in body
    assert "active_run" not in body


def test_log_extents_returns_null_extents_when_cache_empty(client):
    """Pre-first-cron-tick state: status dict empty → extents are
    null but ``configured`` is true. FilterBar's refetchInterval
    keeps polling until extents populate."""
    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.config.get_status", return_value={}),
    ):
        resp = client.get("/api/log-extents", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["configured"] is True
    assert body["earliest_log_at"] is None
    assert body["latest_log_at"] is None


def test_ingested_files_returns_list_from_repo(client):
    fake_files = [
        {"file_name": "log-001.gz", "ingested_at": "2026-01-01T00:00:00Z", "row_count": 100, "file_size_bytes": 5000},
        {"file_name": "log-002.gz", "ingested_at": "2026-01-02T00:00:00Z", "row_count": 200, "file_size_bytes": 8000},
    ]
    with patch("backend.core.duckdb.get_ingested_files", return_value=fake_files):
        resp = client.get("/api/admin/ingested-files", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    files = resp.json()["files"]
    assert len(files) == 2
    assert files[0]["file_name"] == "log-001.gz"


def test_ingested_files_500s_on_exception(client):
    with patch("backend.core.duckdb.get_ingested_files", side_effect=RuntimeError("S3 down")):
        resp = client.get("/api/admin/ingested-files", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 500


def test_iceberg_info_returns_table_metadata(client):
    """Response is shaped by ``IcebergTableInfoResponse``: the route's
    return dict must include the fields the model requires."""
    fake_info = {
        "table_name": "logs_test_service",
        "snapshots": 7,
        "data_files": 42,
        "size_bytes": 1024 * 1024 * 50,
    }
    with patch("backend.core.iceberg.get_table_info", return_value=fake_info):
        resp = client.get("/api/admin/iceberg-info", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["data_files"] == 42
    assert body["snapshots"] == 7


def test_iceberg_info_500s_on_table_load_failure(client):
    with patch("backend.core.iceberg.get_table_info", side_effect=RuntimeError("manifest gone")):
        resp = client.get("/api/admin/iceberg-info", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 500


def test_iceberg_calendar_returns_per_date_file_counts(client):
    """The calendar drives the date-picker heatmap in the admin UI.
    Pinned because losing the ``_debug_calls`` extension would
    break the debug-panel telemetry overlay."""
    fake_calendar = {
        "2026-01-01": {"data_files": 12, "size_bytes": 4096},
    }
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=fake_calendar):
        resp = client.get(
            "/api/admin/iceberg-calendar",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["2026-01-01"]["data_files"] == 12
    assert "_debug_calls" in body  # telemetry overlay


def test_iceberg_calendar_500s_on_exception(client):
    with patch(
        "backend.core.iceberg.get_snapshot_calendar",
        side_effect=RuntimeError("partition spec corrupt"),
    ):
        resp = client.get(
            "/api/admin/iceberg-calendar",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 500


def test_get_bot_sources_returns_sources_and_rdns_stats(client):
    fake_sources = [
        {"id": "well-known-bots", "name": "Arcjet", "enabled": True, "last_updated": "2026-01-01T00:00:00Z"}
    ]
    fake_rdns = {"total": 1000, "pending": 50, "last_enrichment_at": "2026-05-15T00:00:00Z"}

    with (
        patch("backend.utils.bot_sources.get_all_sources_meta", return_value=fake_sources),
        patch("backend.utils.rdns_cache.get_stats", return_value=fake_rdns),
    ):
        resp = client.get("/api/admin/bot-sources")

    assert resp.status_code == 200
    body = resp.json()
    assert body["sources"][0]["id"] == "well-known-bots"
    assert body["rdns"]["total"] == 1000


def test_usage_logging_get_returns_current_config(client, tmp_path, monkeypatch):
    from backend import config

    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_usage_logging_config({"enabled": True, "retention_days": 60, "class_a_rate_per_1k": 0.123})

    resp = client.get("/api/admin/usage-logging")
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert body["retention_days"] == 60


def test_usage_logging_patch_updates_only_allowed_fields(client, tmp_path, monkeypatch):
    """Only the documented field list updates; arbitrary keys are
    silently dropped. Pinned because letting unknown keys land in
    the JSON file would slowly corrupt the schema."""
    from backend import config

    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    resp = client.patch(
        "/api/admin/usage-logging",
        json={
            "enabled": True,
            "retention_days": 90,
            "junk_field": "should_be_dropped",
            "another_unknown": 123,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert body["retention_days"] == 90
    # Unknown fields don't surface in the response
    assert "junk_field" not in body
    assert "another_unknown" not in body


def test_usage_logging_patch_partial_update_preserves_unchanged_fields(client, tmp_path, monkeypatch):
    """PATCH applies only the keys present in the body — pinned because
    the FE uses PATCH for partial updates and must not null the rest."""
    from backend import config

    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_usage_logging_config({"enabled": False, "retention_days": 30})
    resp = client.patch(
        "/api/admin/usage-logging",
        json={"enabled": True},
    )
    assert resp.status_code == 200
    assert resp.json()["enabled"] is True
    # The unchanged field is preserved
    assert resp.json()["retention_days"] == 30


def test_usage_log_endpoint_returns_paginated_rows(client):
    """The endpoint joins usage_log rows with aggregate cost figures.
    Pinned because the FE renders entries + aggregate as separate
    sections; losing the aggregate breakdown would zero the cost card."""
    fake_rows = [
        {
            "id": 1,
            "timestamp": "2026-01-01T00:00:00Z",
            "service_id": MOCK_SERVICE_ID,
            "operation_class": "A",
            "operation_type": "PUT_OBJECT",
            "url": None,
            "bytes": None,
            "duration_ms": None,
            "function_name": None,
            "process_context": None,
            "status": "ok",
        },
        {
            "id": 2,
            "timestamp": "2026-01-01T00:00:01Z",
            "service_id": MOCK_SERVICE_ID,
            "operation_class": "B",
            "operation_type": "GET_OBJECT",
            "url": None,
            "bytes": None,
            "duration_ms": None,
            "function_name": None,
            "process_context": None,
            "status": "ok",
        },
    ]
    fake_agg = {
        "total_class_a": 100,
        "total_class_b": 200,
        "total_cdn_downloads": 50,
        "total_cdn_bytes": 10485760,  # 10 MB
        "total_fos_bytes": 0,
        "class_a_breakdown": {"PUT_OBJECT": 100},
        "class_b_breakdown": {"GET_OBJECT": 200},
    }

    with patch(
        "backend.core.metadata.get_usage_logs",
        return_value=(fake_rows, 2, fake_agg),
    ):
        resp = client.get(
            "/api/admin/usage-log",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"page": 1, "page_size": 10},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert len(body["entries"]) == 2
    assert body["aggregate"]["total_class_a"] == 100
    assert body["aggregate"]["total_class_b"] == 200


def test_usage_log_endpoint_validates_page_size_upper_bound(client):
    """``page_size`` clamped to ≤ 1000 by FastAPI ``Query(le=1000)``.
    Pinned because a million-row response would OOM the frontend."""
    resp = client.get(
        "/api/admin/usage-log",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"page_size": 99999},
    )
    assert resp.status_code == 422


def test_usage_log_endpoint_validates_page_minimum(client):
    resp = client.get(
        "/api/admin/usage-log",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"page": 0},
    )
    assert resp.status_code == 422


def test_system_jobs_endpoint_returns_200_with_jobs_array(client):
    """The endpoint surfaces the APScheduler job list to the admin
    UI's "background jobs" panel. Pinned at the structural level
    (200 + jobs array present) because the per-job content depends
    on the live scheduler state."""
    resp = client.get("/api/admin/system-jobs")
    assert resp.status_code == 200
    body = resp.json()
    assert "jobs" in body
    assert isinstance(body["jobs"], list)


def test_system_jobs_endpoint_includes_share_audit_purge(client):
    """Pinned because the share-audit-purge cron must appear on the
    admin Data Management page next to bot_data_refresh and
    rdns_enrichment — otherwise admins have no way to see whether the
    retention sweep is running."""
    resp = client.get("/api/admin/system-jobs")
    assert resp.status_code == 200
    ids = {j["id"] for j in resp.json()["jobs"]}
    assert "share_audit_purge" in ids
    assert "bot_data_refresh" in ids
    assert "rdns_enrichment" in ids


_ = pytest


# ── Merged from tests/routers/test_admin_health_snapshot.py ──
def _get_health(client):
    resp = client.get("/api/admin/health-snapshot")
    assert resp.status_code == 200
    return resp.json()


def test_health_snapshot_returns_shape_with_known_collectors(client):
    """Smoke: the endpoint returns the documented keys regardless of
    which collectors succeed on the current host."""
    body = _get_health(client)

    assert "load" in body
    assert "vcpus" in body
    assert "memory" in body
    assert "data_mount" in body
    assert "root_disk" in body
    assert "in_flight_runs" in body
    assert "compaction" in body
    assert "pool_wait" in body
    # SRE observability pass additions (SRE-06 / 03 / 20 / 11).
    assert "scheduler_last_tick_age_s" in body
    assert "recent_cron_failures" in body
    assert "observability" in body
    assert "config_backup" in body


def test_health_snapshot_celery_failure_keeps_valid_response(client):
    with (
        patch("backend.config.DEPLOYMENT_MODE", "high_scale"),
        patch("backend.celery_app.app.control.inspect", side_effect=RuntimeError("broker down")),
    ):
        body = _get_health(client)
    assert body["celery"] is None


def test_health_snapshot_load_failure_renders_none(client):
    with patch("os.getloadavg", side_effect=OSError("no load avg")):
        body = _get_health(client)
    assert body["load"] is None


def test_health_snapshot_memory_failure_renders_none(client):
    """Non-Linux runners can't open /proc/meminfo — the endpoint
    returns memory=None rather than 500."""
    with patch("builtins.open", side_effect=FileNotFoundError("no /proc/meminfo")):
        body = _get_health(client)
    assert body["memory"] is None


def test_health_snapshot_memory_succeeds_when_meminfo_readable(client):
    """When /proc/meminfo IS readable (Linux prod), populated stats
    come through with the documented shape."""
    fake_meminfo = (
        "MemTotal:        8388608 kB\n"
        "MemFree:         1048576 kB\n"
        "MemAvailable:    4194304 kB\n"
        "Buffers:          524288 kB\n"
    )

    real_open = open

    def _fake_open(path, *args, **kwargs):
        if path == "/proc/meminfo":
            from io import StringIO

            return StringIO(fake_meminfo)
        return real_open(path, *args, **kwargs)

    with patch("builtins.open", side_effect=_fake_open):
        body = _get_health(client)

    assert body["memory"] is not None
    assert body["memory"]["total_mb"] == 8192
    assert body["memory"]["available_mb"] == 4096
    assert body["memory"]["used_pct"] == 50.0


def test_health_snapshot_disk_failure_renders_none_for_each_mount(client):
    """If shutil.disk_usage raises (e.g. mount point doesn't exist),
    the per-mount field is None — both /app/data and / get the same
    treatment."""
    with patch("shutil.disk_usage", side_effect=OSError("missing mount")):
        body = _get_health(client)
    assert body["data_mount"] is None
    assert body["root_disk"] is None


def test_health_snapshot_disk_returns_stats_when_available(client):
    """shutil.disk_usage returning a (total, used, free) namedtuple
    flows through into the rounded GB fields the UI renders."""
    from collections import namedtuple

    Usage = namedtuple("Usage", ["total", "used", "free"])
    # 100 GB total, 25 GB used.
    fake = Usage(total=100 * 1024**3, used=25 * 1024**3, free=75 * 1024**3)

    with patch("shutil.disk_usage", return_value=fake):
        body = _get_health(client)

    assert body["data_mount"]["total_gb"] == 100.0
    assert body["data_mount"]["used_gb"] == 25.0
    assert body["data_mount"]["free_gb"] == 75.0
    assert body["data_mount"]["used_pct"] == 25.0


def test_health_snapshot_disk_zero_total_renders_pct_none(client):
    """Edge case: tmpfs / virtual mounts can report total=0; ``used_pct``
    must collapse to None rather than ZeroDivisionError."""
    from collections import namedtuple

    Usage = namedtuple("Usage", ["total", "used", "free"])
    fake = Usage(total=0, used=0, free=0)

    with patch("shutil.disk_usage", return_value=fake):
        body = _get_health(client)

    assert body["data_mount"]["used_pct"] is None


def test_health_snapshot_in_flight_runs_failure_renders_empty(client):
    """If list_active_runs raises (corrupt cron-progress state), the
    field degrades to []; the UI's empty state then renders cleanly."""
    with patch("backend.cron_progress.list_active_runs", side_effect=RuntimeError("boom")):
        body = _get_health(client)
    assert body["in_flight_runs"] == []


def test_health_snapshot_in_flight_runs_returns_simplified_shape(client):
    """The endpoint projects the full progress dict down to four fields
    the SystemHealthCard renders. Pinned because dropping a key here
    silently breaks the card."""
    fake_runs = [
        {
            "run_id": 101,
            "service_id": "svc-a",
            "task": "sync",
            "started_at": "2026-06-12T10:00:00Z",
            "rows_done": 1234,  # extra field should NOT leak
        },
        {
            "run_id": 102,
            "service_id": "svc-b",
            "task": "metadata_cleanup",
            "started_at": "2026-06-12T10:05:00Z",
        },
    ]
    with patch("backend.cron_progress.list_active_runs", return_value=fake_runs):
        body = _get_health(client)

    assert len(body["in_flight_runs"]) == 2
    keys_seen = set(body["in_flight_runs"][0].keys())
    assert keys_seen == {"run_id", "service_id", "task", "started_at"}, (
        f"unexpected/missing fields in projected in_flight_runs entry: {keys_seen}"
    )


def test_health_snapshot_in_flight_started_at_float_epoch_coerced_to_string(client):
    """REGRESSION: cron_progress stores ``started_at`` as a float epoch
    (``time.time()``), but ``HealthInFlightRun.started_at`` is typed
    ``str | None``. The endpoint must coerce it — otherwise FastAPI raises
    ResponseValidationError → 500 whenever a cron run is genuinely in flight
    (which is why it only surfaced right after a restart, while a sync was
    running).

    The sibling shape test above used an ISO *string* fixture — what the
    response model wants, not what the producer actually emits — so it never
    exercised this path. Derive the fixture from the producer (float epoch).
    """
    fake_runs = [
        {"run_id": 7, "service_id": "svc-a", "task": "sync", "started_at": 1781758323.4424183},
    ]
    with patch("backend.cron_progress.list_active_runs", return_value=fake_runs):
        body = _get_health(client)  # _get_health asserts 200 — 500 under the bug

    entry = body["in_flight_runs"][0]
    assert isinstance(entry["started_at"], str), "float epoch must be coerced to a string"
    assert "T" in entry["started_at"], "started_at should be an ISO-8601 timestamp"


def test_health_snapshot_compaction_failure_renders_empty_dict(client):
    """The compaction block iterates list_configs(); if THAT raises,
    the whole compaction field collapses to {} (per-service failures
    inside the loop already render as ``None`` per service)."""
    with patch("backend.config.list_configs", side_effect=RuntimeError("boom")):
        body = _get_health(client)
    assert body["compaction"] == {}


def test_health_snapshot_compaction_per_service_failure_renders_none(client):
    """A per-service compaction_stats failure must collapse to None
    for that service WITHOUT taking down the whole compaction map."""
    fake_configs = [{"service_id": "svc-good", "name": "svc-good"}, {"service_id": "svc-bad", "name": "svc-bad"}]

    def _stats(src):
        if src["name"] == "svc-bad":
            raise RuntimeError("simulated per-svc failure")
        # Field is "partitions" — matches both compaction_stats() output
        # and the CompactionStatsResponse model in backend.models.admin.
        return {"partitions": 7}

    with (
        patch("backend.config.list_configs", return_value=fake_configs),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.local_compaction.compaction_stats", side_effect=_stats),
    ):
        body = _get_health(client)

    # The typed CompactionStatsResponse populates the partitions field
    # and fills the rest with defaults. We pin the field we care about
    # (partitions=7 propagates) rather than full equality.
    assert body["compaction"]["svc-good"]["partitions"] == 7
    assert body["compaction"]["svc-bad"] is None


def test_health_snapshot_pool_wait_failure_renders_empty_list(client):
    """get_all_stats raising returns ``pool_wait=[]`` so the Pool Wait
    card renders the empty state instead of disappearing."""
    with patch("backend.core.duckdb_pool.get_all_stats", side_effect=RuntimeError("boom")):
        body = _get_health(client)
    assert body["pool_wait"] == []


def test_health_snapshot_pool_wait_returns_pool_stats(client):
    fake_stats = [
        {"pool": "default", "wait_p50_ms": 5.0, "wait_p95_ms": 12.0},
        {"pool": "cron", "wait_p50_ms": 1.0, "wait_p95_ms": 3.0},
    ]
    with patch("backend.core.duckdb_pool.get_all_stats", return_value=fake_stats):
        body = _get_health(client)
    assert body["pool_wait"] == fake_stats


def test_health_snapshot_vcpus_failure_renders_none(client):
    with patch("multiprocessing.cpu_count", side_effect=NotImplementedError):
        body = _get_health(client)
    assert body["vcpus"] is None


def test_health_snapshot_with_probe_fos(client):
    """Verify that probe_fos=1 triggers FOS reachability probe across all configs."""
    fake_configs = [{"service_id": "svc-a", "name": "svc-a"}]
    fake_reachable = {"reachable": True, "latency_ms": 45}

    with (
        patch("backend.config.list_configs", return_value=fake_configs),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.duckdb.fos_reachable", return_value=fake_reachable) as mock_reachable,
    ):
        resp = client.get("/api/admin/health-snapshot?probe_fos=True")
        assert resp.status_code == 200
        body = resp.json()
        assert body["fos"]["svc-a"]["reachable"] is True
        mock_reachable.assert_called_once()


def test_health_snapshot_with_probe_fos_failure(client):
    """Verify that probe_fos=1 handles FOS reachability probe failures gracefully per service."""
    fake_configs = [{"service_id": "svc-a", "name": "svc-a"}]

    with (
        patch("backend.config.list_configs", return_value=fake_configs),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.duckdb.fos_reachable", side_effect=RuntimeError("S3 connection timed out")),
    ):
        resp = client.get("/api/admin/health-snapshot?probe_fos=True")
        assert resp.status_code == 200
        body = resp.json()
        assert body["fos"]["svc-a"]["reachable"] is False
        assert "timed out" in body["fos"]["svc-a"]["error"]


def test_vcl_health_not_configured(client):
    """Verify /api/admin/vcl-health returns an error when service is not configured."""
    with patch("backend.config.load_config", return_value=None):
        resp = client.get("/api/admin/vcl-health?service_id=nonexistent")
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_clean"] is False
        assert "not configured" in body["error"]


def test_vcl_health_no_token(client):
    """Verify /api/admin/vcl-health returns an error when no API token is in config."""
    with patch("backend.config.load_config", return_value={"service_id": "svc-a"}):
        resp = client.get("/api/admin/vcl-health?service_id=svc-a")
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_clean"] is False
        assert "No Fastly API token" in body["error"]


def test_vcl_health_no_active_version(client):
    """Verify /api/admin/vcl-health returns an error when no active version is fetched."""
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-a", "admin_token": "token"}),
        patch("backend.provision.declarative.fastly_integration.fetch_active_version", return_value=None),
    ):
        resp = client.get("/api/admin/vcl-health?service_id=svc-a")
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_clean"] is False
        assert "No active version found" in body["error"]


def test_vcl_health_migration_complete(client):
    """Verify /api/admin/vcl-health reports a clean state when only consolidated snippets are present."""
    from collections import namedtuple

    Snippet = namedtuple("Snippet", ["name"])
    snippets = [Snippet(name="Fastly Log Analytics - Recv")]

    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-a", "admin_token": "token"}),
        patch("backend.provision.declarative.fastly_integration.fetch_active_version", return_value=5),
        patch("backend.provision.declarative.fastly_integration.fetch_snippets", return_value=snippets),
    ):
        resp = client.get("/api/admin/vcl-health?service_id=svc-a")
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_clean"] is True
        assert body["legacy_snippets_found"] == 0
        assert body["consolidated_snippets_found"] == 1


def test_vcl_health_unexpected_state(client):
    """Verify /api/admin/vcl-health handles unexpected states (no snippets found)."""
    with (
        patch("backend.config.load_config", return_value={"service_id": "svc-a", "admin_token": "token"}),
        patch("backend.provision.declarative.fastly_integration.fetch_active_version", return_value=5),
        patch("backend.provision.declarative.fastly_integration.fetch_snippets", return_value=[]),
    ):
        resp = client.get("/api/admin/vcl-health?service_id=svc-a")
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_clean"] is False
        assert "no snippets found" in body["recommendation"]


def test_vcl_health_exception_handled(client):
    """Verify /api/admin/vcl-health handles general exceptions gracefully."""
    with patch("backend.config.load_config", side_effect=RuntimeError("disk crash")):
        resp = client.get("/api/admin/vcl-health?service_id=svc-a")
        assert resp.status_code == 200
        body = resp.json()
        assert body["is_clean"] is False
        assert "disk crash" in body["error"]


# ── Merged from tests/routers/test_admin_overview.py ──
_make_source = partial(make_source, "svc-admin-test", kind="cloud")


@pytest.fixture
def overview_client():
    with TestClient(app) as c:
        yield c


def test_admin_sync_status_alias_and_primary(overview_client: TestClient):
    """Both /api/admin/sync-status and /api/sync-status return typed SyncStatusResponse."""
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
        patch(
            "backend.routers.admin.sync_status.compute_sync_status_cached",
            return_value=fake_payload,
        ),
    ):
        r1 = overview_client.get("/api/admin/sync-status?service_id=svc-admin-test&skip_fos=true")
        assert r1.status_code == 200
        body1 = r1.json()
        assert body1["configured"] is True
        assert body1["access_level"] == "read_write"
        assert body1["local_rows"] == 1250

        r2 = overview_client.get("/api/sync-status?service_id=svc-admin-test&skip_fos=true")
        assert r2.status_code == 200
        assert r2.json()["local_rows"] == 1250


def test_admin_system_metrics_and_health_aliases(overview_client: TestClient):
    """/api/admin/system-metrics, /api/admin/health, and /api/admin/health-snapshot return HealthSnapshotResponse."""
    for path in (
        "/api/admin/system-metrics",
        "/api/admin/health",
        "/api/admin/health-snapshot",
    ):
        resp = overview_client.get(path)
        assert resp.status_code == 200, f"{path} returned {resp.status_code}"
        data = resp.json()
        assert "load" in data
        assert "vcpus" in data
        assert "in_flight_runs" in data
        assert "compaction" in data
        assert "pool_wait" in data
        assert "recent_cron_failures" in data


def test_admin_compaction_status_endpoints(overview_client: TestClient):
    """/api/admin/compaction-status and /api/admin/compaction-status/{service_id} return CompactionStatsResponse."""
    src = _make_source()
    app.dependency_overrides[get_source] = lambda: src
    try:
        r1 = overview_client.get(f"/api/admin/compaction-status/{src['service_id']}")
        assert r1.status_code == 200
        body1 = r1.json()
        assert "partitions" in body1
        assert "total_files" in body1
        assert "partitions_above_3" in body1

        r2 = overview_client.get(f"/api/admin/compaction-status?service_id={src['service_id']}")
        assert r2.status_code == 200
        assert r2.json()["total_files"] == body1["total_files"]
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_admin_log_accounting_path_and_query(overview_client: TestClient):
    """/api/admin/log-accounting/{service_id} and /api/admin/log-accounting return LogAccountingResponse."""
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
        "catchup": {
            "latest_ingest_ts": "2026-10-16T19:59:50Z",
            "lag_seconds": 10,
            "status": "caught_up",
        },
        "section_timings": [],
    }
    try:
        with patch(
            "backend.routers.admin.log_accounting.compute_log_accounting",
            return_value=fake_accounting,
        ):
            r1 = overview_client.get(f"/api/admin/log-accounting/{src['service_id']}?hours=24")
            assert r1.status_code == 200
            assert r1.json()["totals"]["gap_pct"] == 0.0
            assert r1.json()["totals"]["fastly_requests"] == 1000

            r2 = overview_client.get(f"/api/admin/log-accounting?service_id={src['service_id']}&hours=24")
            assert r2.status_code == 200
            assert r2.json()["totals"]["our_rows"] == 1000
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_admin_manual_cron_trigger_routes(overview_client: TestClient):
    """Manual trigger routes (/sync/{id}, /commit/{id}, /compact/{id}, /optimize/{id}) return typed responses."""
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
            patch(
                "backend.cron.jobs.optimize._run_optimize",
                return_value={"files_rewritten": 4, "files_added": 1},
            ),
        ):
            r_sync = overview_client.post(f"/api/admin/sync/{src['service_id']}")
            assert r_sync.status_code == 200
            assert r_sync.json()["ok"] is True
            assert mock_start.called

            r_commit = overview_client.post(f"/api/admin/commit/{src['service_id']}")
            assert r_commit.status_code == 202
            assert r_commit.json()["ok"] is True

            r_compact = overview_client.post(f"/api/admin/compact/{src['service_id']}")
            assert r_compact.status_code == 200
            assert r_compact.json()["partitions_compacted"] == 1

            r_optimize = overview_client.post(f"/api/admin/optimize/{src['service_id']}")
            assert r_optimize.status_code == 200
            assert r_optimize.json()["files_rewritten"] == 4
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_admin_manual_triggers_reject_read_only_service(overview_client: TestClient):
    """Analyst Path A (read_only service config) receives 403 Forbidden on manual write/ingest triggers."""
    ro_src = _make_source(access_level="read_only")
    app.dependency_overrides[get_source] = lambda: ro_src
    try:
        for path in (
            f"/api/admin/sync/{ro_src['service_id']}",
            f"/api/admin/commit/{ro_src['service_id']}",
        ):
            resp = overview_client.post(path)
            assert resp.status_code == 403, f"Expected 403 on {path} for read_only service, got {resp.status_code}"
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_analyst_path_b_rejected_with_403_on_admin_overview_routes():
    """Analyst Path B sessions are blocked by _is_blocked_path and require_admin on /api/admin/* routes."""
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
        assert _is_blocked_path(path) is True, f"Expected {path} to be blocked for Analyst Path B"

    fake_req = SimpleNamespace(
        state=SimpleNamespace(analyst_session=SimpleNamespace(session_id="sess_analyst")),
        headers={},
        client=SimpleNamespace(host="203.0.113.55"),
    )
    with pytest.raises(HTTPException) as exc:
        require_admin(fake_req)  # type: ignore[arg-type]
    assert exc.value.status_code == 403


def test_iceberg_info_endpoint(overview_client, override_source):

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
        response = overview_client.get("/api/admin/iceberg-info?table_name=logs")
        assert response.status_code == 200
        data = response.json()
        assert data["size_bytes"] == 1024
        assert data["table_name"] == "logs"
        mock_get.assert_called_once_with(override_source, table_name="logs")


def test_iceberg_calendar_endpoint(overview_client, override_source):
    fake_calendar = {
        "2026-08-19": {"data_files": 5, "size_bytes": 1024},
    }
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=fake_calendar) as mock_get:
        response = overview_client.get("/api/admin/iceberg-calendar?table_name=logs")
        assert response.status_code == 200
        data = response.json()
        assert data["2026-08-19"] == {"data_files": 5, "size_bytes": 1024}
        mock_get.assert_called_once_with(override_source, table_name="logs")


def test_iceberg_commit_endpoint(overview_client, override_source):
    with patch("backend.utils.router_utils.start_or_resume_cron", return_value={"ok": True}) as mock_start:
        response = overview_client.post("/api/admin/commit-iceberg")
        assert response.status_code == 202
        mock_start.assert_called_once()


def test_rebuild_local_view_endpoint_success(overview_client, override_source):
    with (
        patch("backend.core.iceberg.clear_source_caches") as mock_clear,
        patch("backend.core.duckdb.start_cron_run", return_value="run-123") as mock_start_cron,
        patch("backend.cron_progress.start_progress") as mock_prog,
        patch("threading.Thread") as mock_thread,
    ):
        response = overview_client.post("/api/admin/rebuild-local-view")
        assert response.status_code == 202
        assert response.json()["run_id"] == "run-123"
        mock_clear.assert_called_once_with("service123")
        mock_start_cron.assert_called_once_with(override_source, "metadata_sync")
        mock_prog.assert_called_once_with("run-123", service_id="service123", task="metadata_sync")
        mock_thread.assert_called_once()


def test_rebuild_local_view_endpoint_busy(overview_client, override_source):
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value="/tmp"),
        patch("os.path.exists", return_value=False),
        patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError("busy")),
    ):
        response = overview_client.post("/api/admin/rebuild-local-view")
        assert response.status_code == 503


def test_reset_logs_endpoint_read_only(overview_client, override_source):
    override_source["access_level"] = "read_only"
    response = overview_client.post(
        "/api/admin/reset-logs",
        json={"confirm": "service123", "delete_raw_logs": False, "preserve_usage_history": True},
    )
    assert response.status_code == 403


def test_reset_logs_endpoint_confirm_mismatch(overview_client, override_source):
    response = overview_client.post(
        "/api/admin/reset-logs",
        json={"confirm": "wrong", "delete_raw_logs": False, "preserve_usage_history": True},
    )
    assert response.status_code == 400


def test_reset_logs_endpoint_success(overview_client, override_source):
    def fake_reset(*args, **kwargs):
        yield {"type": "status", "message": "starting"}
        yield {"type": "done", "message": "done"}

    with patch("backend.core.reset.reset_service_logs", side_effect=fake_reset):
        response = overview_client.post(
            "/api/admin/reset-logs",
            json={"confirm": "service123", "delete_raw_logs": False, "preserve_usage_history": True},
        )
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        events = []
        for line in response.iter_lines():
            if line.startswith("data:"):
                events.append(json.loads(line[5:]))
        assert len(events) == 2
        assert events[0]["type"] == "status"
        assert events[1]["type"] == "done"


def test_reset_rum_endpoint_read_only(overview_client, override_source):
    override_source["access_level"] = "read_only"
    response = overview_client.post(
        "/api/admin/reset-rum",
        json={"confirm": "service123", "delete_raw_logs": False},
    )
    assert response.status_code == 403


def test_reset_rum_endpoint_confirm_mismatch(overview_client, override_source):
    response = overview_client.post(
        "/api/admin/reset-rum",
        json={"confirm": "wrong", "delete_raw_logs": False},
    )
    assert response.status_code == 400


def test_reset_rum_endpoint_success(overview_client, override_source):
    def fake_reset(*args, **kwargs):
        yield {"type": "status", "message": "starting"}

    with patch("backend.core.reset.reset_service_rum", side_effect=fake_reset):
        response = overview_client.post(
            "/api/admin/reset-rum",
            json={"confirm": "service123", "delete_raw_logs": False},
        )
        assert response.status_code == 200
        assert "text/event-stream" in response.headers["content-type"]
        events = []
        for line in response.iter_lines():
            if line.startswith("data:"):
                events.append(json.loads(line[5:]))
        assert len(events) == 1
        assert events[0]["type"] == "status"


def test_metric_history_batch_groups_by_scope_key():
    t1 = iso_z(datetime.now(UTC) - timedelta(minutes=10))
    t2 = iso_z(datetime.now(UTC) - timedelta(minutes=5))
    metric_snapshots.record_snapshot("cpu_load_1m", 0.7, ts=t1)
    metric_snapshots.record_snapshot("cpu_load_1m", 0.9, ts=t2)
    metric_snapshots.record_snapshot("pool_wait_p95_ms", 5.0, service_id="svc-a", ts=t2)
    metric_snapshots.record_snapshot("cron_duration_ms", 800.0, service_id="svc-a", task="sync", ts=t2)

    with TestClient(app) as overview_client:
        r = overview_client.get("/api/admin/metric-history/batch?since=1h")

    assert r.status_code == 200
    series = r.json()["series"]
    assert "cpu_load_1m" in series
    assert "pool_wait_p95_ms|svc-a" in series
    assert "cron_duration_ms|svc-a|sync" in series
    # Points within a series are ordered chronologically ascending
    cpu_points = series["cpu_load_1m"]
    assert len(cpu_points) >= 2
    assert cpu_points[-2]["value"] == pytest.approx(0.7)
    assert cpu_points[-1]["value"] == pytest.approx(0.9)
    assert cpu_points[-2]["ts"] < cpu_points[-1]["ts"]


@pytest.mark.parametrize(
    ("window", "expected_points"),
    [
        ("30m", 1),
        ("1h", 2),
        ("6h", 3),
        ("24h", 4),
        ("7d", 5),
    ],
)
def test_metric_history_batch_windows(window: str, expected_points: int):
    metric_name = f"test_window_metric_{window}"
    now = datetime.now(UTC)
    metric_snapshots.record_snapshot(metric_name, 10.0, ts=iso_z(now - timedelta(minutes=10)))
    metric_snapshots.record_snapshot(metric_name, 20.0, ts=iso_z(now - timedelta(minutes=45)))
    metric_snapshots.record_snapshot(metric_name, 30.0, ts=iso_z(now - timedelta(hours=3)))
    metric_snapshots.record_snapshot(metric_name, 40.0, ts=iso_z(now - timedelta(hours=12)))
    metric_snapshots.record_snapshot(metric_name, 50.0, ts=iso_z(now - timedelta(days=3)))

    with TestClient(app) as overview_client:
        r = overview_client.get(f"/api/admin/metric-history/batch?since={window}")

    assert r.status_code == 200
    body = r.json()
    assert "series" in body
    pts = body["series"].get(metric_name, [])
    assert len(pts) == expected_points


@pytest.mark.parametrize("bad_window", ["invalid", "0h", "-5h", "1w", "abc", ""])
def test_metric_history_batch_rejects_invalid_window(bad_window: str):
    with TestClient(app) as overview_client:
        r = overview_client.get(f"/api/admin/metric-history/batch?since={bad_window}")

    assert r.status_code == 400
    assert "Invalid lookback window" in r.json()["detail"]["error"]


def test_metric_history_batch_rejects_analyst_path_b():
    # 1. Middleware path gate blocks /api/admin/metric-history/batch
    assert _is_blocked_path("/api/admin/metric-history/batch") is True

    # 2. Route-level Depends(require_admin) also rejects Analyst Path B sessions with 403 admin_only
    analyst_app = FastAPI()

    @analyst_app.middleware("http")
    async def _stamp_analyst(request: Request, call_next):
        request.state.analyst_session = SimpleNamespace(
            session_id="sess-analyst-1",
            service_ids=["svc-a"],
            email="analyst@example.com",
        )
        return await call_next(request)

    analyst_app.include_router(admin_router)

    with TestClient(analyst_app) as overview_client:
        r = overview_client.get("/api/admin/metric-history/batch?since=1h")

    assert r.status_code == 403
    detail = r.json().get("detail", r.json())
    assert detail["error"] == "admin_only"


# ── Merged from tests/routers/test_admin_system_metrics_stream.py ──
EXPECTED_KEYS = (
    "health_snapshot",
    "metric_history_1h",
    "queries_summary",
    "slow_queries_count",
    "log_accounting",
    "metadata_storage",
    "system_jobs",
)


@pytest.mark.usefixtures("_reset_metrics_cache")
def test_sampler_returns_all_slice_keys():
    """The sampler is what shapes the SSE payload. The seven keys
    must match the React Query keys the frontend hook dispatches to
    via setQueryData — drift between them silently breaks the cards."""
    from backend.system_metrics_sampler import sample_system_metrics

    payload = sample_system_metrics("svc-test")
    for key in EXPECTED_KEYS:
        assert key in payload, f"missing slice key: {key}"


@pytest.mark.usefixtures("_reset_metrics_cache")
def test_sampler_output_is_json_serializable():
    """Each slice must be plain-dict / list / scalar — the SSE
    endpoint calls ``json.dumps`` on the bundled payload, and one
    pydantic-model leak (LogAccountingBucket was the original
    offender) silently aborts the generator and leaves the browser
    with a 200-but-empty stream. Pinning JSON-serializability per
    slice catches the regression at the sampler boundary instead of
    via an empty UI."""
    import json

    from backend.system_metrics_sampler import sample_system_metrics

    payload = sample_system_metrics("svc-test")
    for key, value in payload.items():
        try:
            json.dumps(value)
        except TypeError as exc:
            raise AssertionError(f"slice {key!r} is not JSON-serializable: {exc}") from exc
    # Top-level too, in case some slice nests poorly.
    json.dumps(payload)


@pytest.mark.usefixtures("_reset_metrics_cache")
def test_sampler_isolates_failing_component():
    """One sampler helper raising must not tank the whole snapshot —
    the failing slice degrades to None while every other slice
    returns whatever it can. Lets the rest of the admin overview
    stay readable when (e.g.) the scheduler is mid-restart."""
    from backend import system_metrics_sampler

    with patch.object(
        system_metrics_sampler,
        "_sample_health_snapshot",
        side_effect=RuntimeError("boom"),
    ):
        payload = system_metrics_sampler.sample_system_metrics("svc-test")
    assert payload["health_snapshot"] is None
    # Every other key still present (some may also be None on hosts
    # where /proc/meminfo or the scheduler isn't running, but the
    # shape must include them so the frontend dispatch loop sees
    # every slice).
    for key in EXPECTED_KEYS:
        assert key in payload


@pytest.mark.usefixtures("_reset_metrics_cache")
def test_sampler_serviceless_degrades_to_global():
    """With no service (fresh install), the three service-scoped slices
    are DETERMINISTICALLY None — each helper short-circuits on a falsy
    service_id — while the global slices are still attempted, and the
    call never raises. This degrade-to-global contract is what lets the
    stream handler accept a serviceless connection and still deliver live
    host/process status. (Global slices are best-effort: they may be None
    on minimal hosts, same as with a service, so we don't pin them here.)"""
    from backend.system_metrics_sampler import sample_system_metrics

    payload = sample_system_metrics(None)
    for key in EXPECTED_KEYS:
        assert key in payload, f"missing slice key: {key}"
    assert payload["slow_queries_count"] is None
    assert payload["log_accounting"] is None
    assert payload["metadata_storage"] is None


@pytest.fixture
def _reset_metrics_cache():
    """Module-level cache + per-key asyncio.Locks would otherwise bleed
    across tests (and locks bind to a now-closed loop) under -n auto."""
    sampler.reset_metrics_cache()
    yield
    sampler.reset_metrics_cache()


@pytest.mark.usefixtures("_reset_metrics_cache")
@pytest.mark.asyncio
async def test_cached_hit_within_ttl_skips_recompute(monkeypatch):
    calls: list[str | None] = []

    def _count(sid):
        calls.append(sid)
        return {"health_snapshot": {"n": len(calls)}}

    monkeypatch.setattr(sampler, "sample_system_metrics", _count)

    first = await sampler.sample_system_metrics_cached("svc")
    second = await sampler.sample_system_metrics_cached("svc")
    assert len(calls) == 1, "second call within TTL must reuse the cached snapshot"
    assert first is second  # shared (never-mutated) dict


@pytest.mark.usefixtures("_reset_metrics_cache")
@pytest.mark.asyncio
async def test_cached_single_flight_collapses_concurrent(monkeypatch):
    """Aligned ticks (e.g. all tabs reconnecting after a deploy) must collapse
    to ONE real recompute — the lock + double-check is what does it."""
    import time as _time

    calls: list[str | None] = []

    def _slow(sid):
        # Runs in a worker thread (to_thread); sleep so all three callers are
        # guaranteed to pass the outer cache check and queue on the lock.
        calls.append(sid)
        _time.sleep(0.05)
        return {"health_snapshot": {"n": len(calls)}}

    monkeypatch.setattr(sampler, "sample_system_metrics", _slow)

    results = await asyncio.gather(
        sampler.sample_system_metrics_cached("svc"),
        sampler.sample_system_metrics_cached("svc"),
        sampler.sample_system_metrics_cached("svc"),
    )
    assert len(calls) == 1, "concurrent callers must share one in-flight sample"
    assert all(r == results[0] for r in results)


@pytest.mark.usefixtures("_reset_metrics_cache")
@pytest.mark.asyncio
async def test_cached_isolates_by_service(monkeypatch):
    calls: list[str | None] = []

    def _record(sid):
        calls.append(sid)
        return {"sid": sid}

    monkeypatch.setattr(sampler, "sample_system_metrics", _record)

    await sampler.sample_system_metrics_cached("a")
    await sampler.sample_system_metrics_cached("b")
    await sampler.sample_system_metrics_cached(None)  # serviceless/global key
    assert calls == ["a", "b", None], "each distinct service_id (incl. None) computes once"


@pytest.mark.usefixtures("_reset_metrics_cache")
@pytest.mark.asyncio
async def test_cached_recomputes_when_ttl_elapsed(monkeypatch):
    monkeypatch.setattr(sampler, "_CACHE_TTL_SECONDS", 0.0)  # nothing stays fresh
    calls: list[str | None] = []

    def _count(sid):
        calls.append(sid)
        return {"health_snapshot": {"n": len(calls)}}

    monkeypatch.setattr(sampler, "sample_system_metrics", _count)

    await sampler.sample_system_metrics_cached("svc")
    await sampler.sample_system_metrics_cached("svc")
    assert len(calls) == 2, "an elapsed TTL must recompute (no stale data across ticks)"


@pytest.mark.usefixtures("_reset_metrics_cache")
@pytest.mark.asyncio
async def test_cached_does_not_cache_exceptions(monkeypatch):
    calls: list[str | None] = []

    def _flaky(sid):
        calls.append(sid)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return {"health_snapshot": {"ok": True}}

    monkeypatch.setattr(sampler, "sample_system_metrics", _flaky)

    with pytest.raises(RuntimeError):
        await sampler.sample_system_metrics_cached("svc")
    # The failure was NOT cached — the next caller retries and succeeds.
    result = await sampler.sample_system_metrics_cached("svc")
    assert result == {"health_snapshot": {"ok": True}}
    assert len(calls) == 2


# ── Merged from tests/routers/test_admin_bot_sources.py ──
bot_sources_client = TestClient(app)


class TestAdminBotSources:
    def test_get_bot_sources_returns_sources_and_rdns_stats(self):
        """Returns source metadata list and rDNS cache statistics."""
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
            resp = bot_sources_client.get("/api/admin/bot-sources")

        assert resp.status_code == 200
        data = resp.json()
        assert "sources" in data
        assert len(data["sources"]) == 1
        assert data["sources"][0]["id"] == "well-known-bots"
        assert data["sources"][0]["entry_count"] == 42
        assert "rdns" in data
        assert data["rdns"]["total"] == 120

    def test_refresh_single_bot_source_success(self):
        """POST /api/admin/bot-sources/{source_id}/refresh re-caches the specified source."""
        fake_meta = {
            "id": "well-known-bots",
            "name": "Well-Known Bots (arcjet)",
            "last_updated": "2026-10-11T02:00:00Z",
            "entry_count": 100,
        }

        with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=fake_meta) as mock_fetch:
            resp = bot_sources_client.post("/api/admin/bot-sources/well-known-bots/refresh")

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["source"]["id"] == "well-known-bots"
        assert data["source"]["entry_count"] == 100
        mock_fetch.assert_called_once_with("well-known-bots")

    def test_refresh_single_bot_source_not_found(self):
        """Unknown source_id returns 404."""
        with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=ValueError("Unknown bot source")):
            resp = bot_sources_client.post("/api/admin/bot-sources/invalid-source/refresh")

        assert resp.status_code == 404
        data = resp.json()
        assert "detail" in data

    def test_refresh_single_bot_source_fetch_failed(self):
        """Network/feed failure in single source refresh returns 502."""
        with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=RuntimeError("Connection timeout")):
            resp = bot_sources_client.post("/api/admin/bot-sources/well-known-bots/refresh")

        assert resp.status_code == 502
        data = resp.json()
        assert "detail" in data

    def test_refresh_all_bot_sources_all_succeed(self):
        """POST /api/admin/bot-sources/refresh triggers full refresh across all sources."""
        fake_results = [
            {"id": "well-known-bots", "entry_count": 100},
            {"id": "tor-exit-nodes", "entry_count": 50},
        ]

        with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results):
            resp = bot_sources_client.post("/api/admin/bot-sources/refresh")

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["updated_count"] == 2
        assert data["failed_count"] == 0
        assert len(data["sources"]) == 2

    def test_refresh_all_bot_sources_partial_failure(self):
        """POST /api/admin/bot-sources/refresh reports ok=False when any source fails."""
        fake_results = [
            {"id": "well-known-bots", "entry_count": 100},
            {"id": "tor-exit-nodes", "failed": True, "error": "503 Unavailable", "entry_count": 0},
        ]

        with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results):
            resp = bot_sources_client.post("/api/admin/bot-sources/refresh")

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is False
        assert data["updated_count"] == 1
        assert data["failed_count"] == 1

    def test_refresh_all_bot_sources_exception_returns_502(self):
        """Unhandled exception in refresh_all_sources returns 502."""
        with patch("backend.utils.bot_sources.refresh_all_sources", side_effect=RuntimeError("Fatal subsystem error")):
            resp = bot_sources_client.post("/api/admin/bot-sources/refresh")

        assert resp.status_code == 502

    def test_refresh_bots_alias_endpoint(self):
        """POST /api/admin/bots/refresh behaves identically to /api/admin/bot-sources/refresh."""
        fake_results = [{"id": "well-known-bots", "entry_count": 100}]

        with patch("backend.utils.bot_sources.refresh_all_sources", return_value=fake_results):
            resp = bot_sources_client.post("/api/admin/bots/refresh")

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["updated_count"] == 1

    def test_rdns_enrich_and_stats_endpoints(self):
        """POST /api/admin/rdns/enrich triggers batch enrichment and GET /api/admin/rdns/stats returns stats."""
        fake_summary = {"resolved": 25, "errors": 0, "discovered": 5, "batch_limit": 50}
        fake_stats = {"total": 500, "resolved": 450, "pending": 50}

        with (
            patch("backend.utils.rdns_cache.enrich_batch", return_value=fake_summary) as mock_enrich,
            patch("backend.utils.rdns_cache.get_stats", return_value=fake_stats),
        ):
            enrich_resp = bot_sources_client.post("/api/admin/rdns/enrich?limit=50")
            assert enrich_resp.status_code == 200
            enrich_data = enrich_resp.json()
            assert enrich_data["ok"] is True
            assert enrich_data["summary"] == fake_summary
            mock_enrich.assert_called_once_with(limit=50)

            stats_resp = bot_sources_client.get("/api/admin/rdns/stats")
            assert stats_resp.status_code == 200
            stats_data = stats_resp.json()
            assert stats_data["ok"] is True
            assert stats_data["stats"] == fake_stats
