"""Consolidated tests for test_sync_status_and_cron_sse."""

from __future__ import annotations

import asyncio
import copy
import importlib
import json
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from backend.cron_runs_publisher import CronRunsPublisher
from backend.high_scale.archive_models import ServingWatermark
from backend.high_scale.registry_bootstrap import HighScaleService
from backend.sync_status_publisher import SyncStatusPublisher
from backend.sync_status_snapshot import compute_sync_status_cached
from fastapi.testclient import TestClient

from backend import cron_progress
from backend import request_metrics_observer as module
from backend.main import app
from tests._helpers.wire import await_until


@pytest.fixture
def snapshot_state(monkeypatch):
    state = {
        "latest_log_at": "2026-09-05T01:00:00Z",
        "local_rows": 7,
        "request": {"latest_log_at": "2026-09-07T19:31:22Z", "total_rows": 7},
        "rum": {"latest_log_at": "2026-09-08T01:00:00Z", "total_rows": 3},
    }
    summary = {"latest_file_name": "logs_2026-09-06T17-19-00.gz", "total_rows": 99}
    ledger = MagicMock()
    ledger.execute.return_value.fetchone.return_value = None
    monkeypatch.setattr("backend.config.get_status", lambda _: copy.deepcopy(state))
    monkeypatch.setattr("backend.config.load_config", lambda _: {})
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service", lambda _: {"name": "snapshot-svc", "duckdb_path": __file__}
    )
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: ".")
    monkeypatch.setattr("backend.sync_status_snapshot._get_dir_size", lambda _: 0)
    monkeypatch.setattr("backend.core.metadata.get_ingested_files_status_summary", lambda _: summary)
    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda _: ledger)
    monkeypatch.setattr("backend.cron_progress.get_latest_progress_for_service", lambda _: None)
    monkeypatch.setattr(
        "backend.core.duckdb.get_connection", lambda *a, **kw: pytest.fail("cached status must not acquire DuckDB")
    )
    return state, summary, ledger


@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
@pytest.mark.parametrize("use_ledger", [False, True])
def test_file_overlay_preserves_current_request_extent(snapshot_state, request_latest, use_ledger):
    state, summary, ledger = snapshot_state
    state["request"]["latest_log_at"] = request_latest
    if request_latest is None:
        state["request"]["total_rows"] = state["local_rows"] = 0
    if use_ledger:
        ledger.execute.return_value.fetchone.return_value = (summary["latest_file_name"],)
        summary["latest_file_name"] = None

    snap = compute_sync_status_cached("snapshot-svc")
    assert snap["latest_log_at"] == snap["request"]["latest_log_at"] == request_latest
    assert snap["latest_ingested_file_at"] == snap["latest_available_file_at"] == "2026-09-06 17:19:00"
    assert snap["local_rows"] == state["local_rows"] and snap["rum"] == state["rum"]


@pytest.mark.parametrize("flat_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
def test_legacy_flat_extent_is_not_a_filename_fallback(snapshot_state, flat_latest):
    state, _, _ = snapshot_state
    del state["request"]
    state["latest_log_at"] = flat_latest
    state["local_rows"] = 0
    snap = compute_sync_status_cached("snapshot-svc")
    assert snap["latest_log_at"] == flat_latest and snap["local_rows"] == 0


def _bind_high_scale_service(monkeypatch, *, include_rum: bool) -> None:
    class Client:
        def execute(self, sql, params=None):
            domain = (params or {}).get("domain")
            if domain == "request":
                return [
                    {
                        "total_rows": 12,
                        "earliest_log_at": datetime(2026, 9, 15, 18, 0, tzinfo=UTC),
                        "latest_log_at": datetime(2026, 9, 15, 19, 0, tzinfo=UTC),
                    }
                ]
            if not include_rum:
                return [{"total_rows": 0, "earliest_log_at": None, "latest_log_at": None}]
            rows = 8 if domain == "rum_vitals" else 2
            minute = 1 if domain == "rum_vitals" else 2
            return [
                {
                    "total_rows": rows,
                    "earliest_log_at": datetime(2026, 9, 15, 18, 0, tzinfo=UTC),
                    "latest_log_at": datetime(2026, 9, 15, 19, minute, tzinfo=UTC),
                }
            ]

    wm = ServingWatermark(
        service_id="high-scale-svc",
        domain="request",
        owner_epoch=1,
        coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
        coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
        last_accepted_cursor=None,
        last_archived_event_id=None,
        last_visible_event_id=None,
        exact=True,
    )
    service = HighScaleService(
        service_id="high-scale-svc", client=Client(), cursor_secret=b"secret", request_watermark=wm
    )
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service", lambda _: {"name": "high-scale-svc", "access_level": "read_write"}
    )
    monkeypatch.setattr(
        "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda _, sid: service})(),
    )
    monkeypatch.setattr(
        "backend.config.get_status", lambda _: pytest.fail("high-scale must not use stale config status")
    )


def test_high_scale_snapshot_and_log_extents_use_clickhouse_metrics(monkeypatch):
    _bind_high_scale_service(monkeypatch, include_rum=True)
    snapshot = compute_sync_status_cached("high-scale-svc")
    assert (snapshot["request"]["total_rows"], snapshot["rum"]["total_rows"], snapshot["local_rows"]) == (12, 10, 22)
    assert snapshot["earliest_log_at"] == "2026-09-15T18:00:00+00:00"
    assert snapshot["latest_log_at"] == "2026-09-15T19:00:00+00:00"

    _bind_high_scale_service(monkeypatch, include_rum=False)
    router = importlib.import_module("backend.routers.admin.sync_status")
    extents = router.log_extents("high-scale-svc")
    assert extents.configured is True
    assert extents.earliest_log_at == "2026-09-15T18:00:00+00:00"
    assert extents.latest_log_at == "2026-09-15T19:00:00+00:00"


@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
def test_cached_rest_and_extents_use_request_snapshot(snapshot_state, request_latest):
    state, _, _ = snapshot_state
    state["request"]["latest_log_at"] = request_latest
    if request_latest is None:
        state["local_rows"] = state["request"]["total_rows"] = 0
    router = importlib.import_module("backend.routers.admin.sync_status")
    payload = router.sync_status("snapshot-svc", skip_fos=True, force=False).model_dump()
    assert payload["latest_log_at"] == payload["request"]["latest_log_at"] == request_latest
    assert payload["local_rows"] == state["local_rows"]
    assert router.log_extents("snapshot-svc").latest_log_at == request_latest


@pytest.mark.asyncio
@pytest.mark.parametrize("admin", [False, True])
@pytest.mark.parametrize("request_latest", ["2026-09-07T19:31:22Z", None])
async def test_initial_sse_uses_request_extent(snapshot_state, admin, request_latest):
    state, _, _ = snapshot_state
    state["request"]["latest_log_at"] = request_latest

    class Request:
        async def is_disconnected(self):
            return False

    if admin:
        from backend.routers.admin.events import admin_events_stream

        response = await admin_events_stream(Request(), channels="sync-status", service_id="snapshot-svc")
    else:
        from backend.routers.admin.sync_status import log_extents_stream

        response = await log_extents_stream(Request(), service_id="snapshot-svc")
    iterator = response.body_iterator
    try:
        payload = json.loads(await asyncio.wait_for(anext(iterator), timeout=2))
        if admin:
            payload = payload["data"]
        assert payload["latest_log_at"] == payload["request"]["latest_log_at"] == request_latest
        if not admin:
            assert "latest_ingested_file_at" not in payload
    finally:
        await iterator.aclose()


# ── SSE Publishers (SyncStatusPublisher & CronRunsPublisher) ────────────────


def _parse_sse_data_events(text: str) -> list[dict]:
    out: list[dict] = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        data_parts = [line[5:].strip() for line in chunk.strip().split("\n") if line.startswith("data:")]
        if data_parts:
            try:
                out.append(json.loads("".join(data_parts)))
            except json.JSONDecodeError:
                pass
    return out


@pytest.mark.parametrize("pub_cls", [SyncStatusPublisher, CronRunsPublisher])
class TestSsePublishers:
    @pytest.mark.asyncio
    async def test_publish_from_worker_thread_and_noop_without_subscribers(self, pub_cls):
        pub = pub_cls()
        pub.publish("svc", {"x": 1})  # before loop bound: silent no-op
        pub.bind_loop(asyncio.get_running_loop())
        pub.publish("svc-nobody", {"x": 1})
        assert pub.subscriber_count("svc-nobody") == 0

        async def consume_one():
            async for payload in pub.subscribe("svc-1"):
                return payload
            return None

        consumer = asyncio.create_task(consume_one())
        await await_until(lambda: pub.subscriber_count("svc-1") >= 1, timeout=2.0, message="subscriber not registered")
        msg = {"event": "test", "run_id": 42, "local_rows": 42}
        threading.Thread(target=lambda: pub.publish("svc-1", msg)).start()
        assert await asyncio.wait_for(consumer, timeout=2.0) == msg

    @pytest.mark.asyncio
    async def test_subscribe_isolates_by_service_id_and_cleans_up_on_aclose(self, pub_cls):
        pub = pub_cls()
        pub.bind_loop(asyncio.get_running_loop())
        received_a: list[dict] = []
        received_b: list[dict] = []

        async def reader(svc, sink):
            async for p in pub.subscribe(svc):
                sink.append(p)
                return

        task_a = asyncio.create_task(reader("svc-A", received_a))
        task_b = asyncio.create_task(reader("svc-B", received_b))
        await await_until(
            lambda: pub.subscriber_count("svc-A") >= 1 and pub.subscriber_count("svc-B") >= 1, timeout=2.0
        )

        pub.publish("svc-A", {"who": "A", "run_id": 1})
        await asyncio.wait_for(task_a, timeout=2.0)
        assert received_b == [] and received_a == [{"who": "A", "run_id": 1}]
        task_b.cancel()
        try:
            await task_b
        except asyncio.CancelledError:
            pass

        gen = pub.subscribe("svc-clean")
        task = asyncio.create_task(gen.__anext__())
        await await_until(lambda: pub.subscriber_count("svc-clean") == 1, timeout=2.0)
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, StopAsyncIteration):
            pass
        await gen.aclose()
        assert pub.subscriber_count("svc-clean") == 0

    @pytest.mark.asyncio
    async def test_overflow_drops_oldest_for_last_write_wins(self, pub_cls):
        pub = pub_cls()
        pub.bind_loop(asyncio.get_running_loop())
        received: list[int] = []

        async def consume_4():
            async for payload in pub.subscribe("svc"):
                received.append(payload["n"])
                if len(received) >= 4:
                    return

        consumer = asyncio.create_task(consume_4())
        await await_until(lambda: pub.subscriber_count("svc") >= 1, timeout=2.0)
        for n in range(10):
            pub.publish("svc", {"n": n})

        await asyncio.wait_for(consumer, timeout=2.0)
        assert len(received) == 4 and received[-1] == 9 and received[0] > 0


def test_log_extents_stream_requires_service_id_and_projects_safe_fields():
    with TestClient(app) as client:
        resp = client.get("/api/log-extents/stream")
        assert resp.status_code == 400 and resp.json()["detail"] == {"error": "service_id_required"}

    full_snapshot = {
        "configured": True,
        "latest_log_at": "2026-06-15T22:46:46+00:00",
        "local_rows": 6_659_858,
        "ngwaf_workspace_id": "example_corp.test",
        "active_run": {"type": "status", "message": "0.6s Processing 12 files..."},
        "cdn_service_id": "ExampleCdnId0000000001",
        "logging_service_id": "ExampleSvcId0000000001",
        "schema": [{"name": "ip", "type": "VARCHAR"}],
        "duckdb_size_bytes": 451_651_568,
        "iceberg_files": 1238,
    }
    pushed_full = {**full_snapshot, "latest_log_at": "2026-06-15T22:48:00+00:00", "local_rows": 6_660_000}

    from backend.routers.admin import sync_status as router_module

    pushed_event = asyncio.Event()

    async def fake_subscribe(_svc_id):
        for _ in range(20):
            if pushed_event.is_set():
                yield pushed_full
                return
            await asyncio.sleep(0.1)

    def trigger_push():
        loop = router_module.sync_status_publisher._loop
        if loop:
            loop.call_soon_threadsafe(pushed_event.set)

    threading.Timer(0.3, trigger_push).start()
    with (
        patch.object(router_module, "compute_sync_status_cached", return_value=full_snapshot),
        patch.object(router_module.sync_status_publisher, "subscribe", fake_subscribe),
        TestClient(app) as client,
        client.stream("GET", "/api/log-extents/stream", headers={"x-service-id": "svc-analyst"}) as resp,
    ):
        assert resp.status_code == 200
        body = "".join(resp.iter_text())

    events = _parse_sse_data_events(body)
    assert len(events) >= 2
    assert events[0] == {
        "latest_log_at": "2026-06-15T22:46:46+00:00",
        "local_rows": 6_659_858,
        "request": None,
        "rum": None,
    }
    assert {
        "latest_log_at": "2026-06-15T22:48:00+00:00",
        "local_rows": 6_660_000,
        "request": None,
        "rum": None,
    } in events
    for ev in events:
        assert set(ev.keys()) == {"latest_log_at", "local_rows", "request", "rum"}


# ── cron_progress ────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_progress_state():
    for store in (
        cron_progress._progress,
        cron_progress._last_update,
        cron_progress._run_metadata,
        cron_progress._terminal_run_ids,
    ):
        store.clear()
    yield
    for store in (
        cron_progress._progress,
        cron_progress._last_update,
        cron_progress._run_metadata,
        cron_progress._terminal_run_ids,
    ):
        store.clear()


def test_start_add_and_get_progress_lifecycle(monkeypatch):
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    assert cron_progress._progress[1] == []
    meta = cron_progress._run_metadata[1]
    assert (meta["service_id"], meta["task"]) == ("svc-a", "sync") and isinstance(meta["started_at"], float)

    # Idempotent start_progress preserves existing events and metadata
    cron_progress.add_progress(1, {"type": "step", "i": 1})
    cron_progress.start_progress(1, service_id="other", task="other")
    assert len(cron_progress._progress[1]) == 1 and cron_progress._run_metadata[1]["service_id"] == "svc-a"

    # Unknown run is silently ignored by add_progress and returns None on get_progress
    cron_progress.add_progress(999, {"type": "step"})
    assert 999 not in cron_progress._progress and cron_progress.get_progress(999) is None

    # Slice and copy semantics on get_progress
    for i in range(2, 5):
        cron_progress.add_progress(1, {"type": "step", "i": i})
    snap = cron_progress.get_progress(1)
    assert len(snap) == 4
    assert cron_progress.get_progress(1, start_idx=2) == [{"type": "step", "i": 3}, {"type": "step", "i": 4}]
    cron_progress.add_progress(1, {"type": "step", "i": 5})
    assert len(snap) == 4  # copy not mutated

    # Monotonic timestamp advance on add_progress
    ticks = iter([1_700_000_000.0, 1_700_000_001.0])
    monkeypatch.setattr(cron_progress.time, "time", lambda: next(ticks))
    cron_progress.start_progress(2)
    initial_ts = cron_progress._last_update[2]
    cron_progress.add_progress(2, {"type": "step"})
    assert cron_progress._last_update[2] > initial_ts


def test_get_latest_progress_for_service():
    assert cron_progress.get_latest_progress_for_service("svc-nothing") is None

    # No events yet -> returns task placeholder
    cron_progress.start_progress(1, service_id="svc-a", task="sync_logs")
    assert cron_progress.get_latest_progress_for_service("svc-a") == {"task": "sync_logs"}

    # Active step event includes task
    cron_progress.add_progress(1, {"type": "step", "msg": "downloading"})
    assert cron_progress.get_latest_progress_for_service("svc-a") == {
        "type": "step",
        "msg": "downloading",
        "task": "sync_logs",
    }

    # Newer run_id wins when multiple active; other service runs are ignored
    cron_progress.start_progress(2, service_id="svc-a", task="sync")
    cron_progress.add_progress(2, {"type": "step", "i": "new"})
    cron_progress.start_progress(3, service_id="svc-b", task="sync")
    cron_progress.add_progress(3, {"type": "step", "i": "other"})
    assert cron_progress.get_latest_progress_for_service("svc-a")["i"] == "new"

    # Terminal events (done / error) are skipped
    cron_progress.add_progress(1, {"type": "done", "rows": 100})
    cron_progress.add_progress(2, {"type": "error", "msg": "S3 down"})
    assert cron_progress.get_latest_progress_for_service("svc-a") is None


@pytest.mark.parametrize(
    ("last_event", "final_event", "expected_last_type", "expected_done_count"),
    [
        ({"type": "step"}, {"type": "done", "rows": 42}, "done", 1),
        ({"type": "status", "message": "view refresh: 12ms"}, None, "done", 1),
        ({"type": "done", "rows": 100}, None, "done", 1),
        ({"type": "error", "message": "S3 timeout"}, None, "error", 0),
        ({"type": "status", "message": "x"}, {"type": "done", "rows": 42}, "done", 1),
    ],
)
def test_end_progress_terminal_event_semantics(last_event, final_event, expected_last_type, expected_done_count):
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, last_event)
    if final_event is None:
        cron_progress.end_progress(1)
    else:
        cron_progress.end_progress(1, final_event=final_event)

    events = cron_progress._progress[1]
    assert events[-1].get("type") == expected_last_type
    assert sum(1 for e in events if e.get("type") == "done") == expected_done_count
    assert cron_progress.list_active_runs() == []

    cron_progress.end_progress(999, {"type": "done"})
    assert 999 not in cron_progress._progress


def test_cleanup_progress_and_concurrent_writers():
    cron_progress.cleanup_progress()  # safe on empty state
    cron_progress.start_progress(1)
    cron_progress.start_progress(2)
    cron_progress._last_update[1] = time.time() - 3700
    cron_progress._last_update[2] = time.time() - 3500
    cron_progress.cleanup_progress()
    assert 1 not in cron_progress._progress and 1 not in cron_progress._run_metadata
    assert 2 in cron_progress._progress

    cron_progress.start_progress(3)
    t1 = threading.Thread(target=lambda: [cron_progress.add_progress(3, {"t": "A", "i": i}) for i in range(100)])
    t2 = threading.Thread(target=lambda: [cron_progress.add_progress(3, {"t": "B", "i": i}) for i in range(100)])
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    assert len(cron_progress._progress[3]) == 200


def test_list_active_runs_and_reap_zombies():
    assert cron_progress.list_active_runs() == []

    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "step", "i": 1})
    cron_progress.start_progress(2, service_id="svc-a", task="sync")
    cron_progress.add_progress(2, {"type": "done", "rows": 100})
    cron_progress.start_progress(3, service_id="svc-b", task="metadata_sync")
    cron_progress.add_progress(3, {"type": "error", "msg": "S3 timeout"})
    cron_progress.start_progress(4, service_id="svc-b", task="commit")

    out_by_id = {entry["run_id"]: entry for entry in cron_progress.list_active_runs()}
    assert set(out_by_id) == {1, 4}
    assert (out_by_id[1]["service_id"], out_by_id[1]["task"]) == ("svc-a", "sync")
    assert isinstance(out_by_id[1]["started_at"], float)

    # DB cross-check filters terminal runs but keeps None / running
    for db_status, expected_ids in ((None, {1, 4}), ("running", {1, 4}), ("success", set())):
        cron_progress._terminal_run_ids.clear()
        with patch("backend.core.metadata.get_cron_run_status", return_value=db_status):
            assert {e["run_id"] for e in cron_progress.list_active_runs()} == expected_ids

    # Zombie filtering (>5 min stale) and reap_zombie_runs
    cron_progress._terminal_run_ids.clear()
    cron_progress._last_update[1] = time.time() - 400
    assert {e["run_id"] for e in cron_progress.list_active_runs()} == {4}
    assert cron_progress.reap_zombie_runs() == 1
    assert 1 not in cron_progress._run_metadata and 4 in cron_progress._run_metadata
    assert (
        cron_progress._progress[1][-1]["type"] == "error"
        and "zombie" in cron_progress._progress[1][-1]["message"].lower()
    )


# ── RequestMetricsObserver ───────────────────────────────────────────────────


@pytest.fixture
def observer_state(monkeypatch):
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setattr(module.config, "list_configs", lambda: [{"name": "test-service"}])
    monkeypatch.setattr(module.config, "config_to_source", lambda cfg: cfg)
    state = {"request": {"total_rows": 1, "latest_log_at": "2026-09-07T00:00:00Z", "last_sync_at": None}}
    persisted: dict = {}
    calls: list[str] = []

    def refresh(source, **kwargs):
        persisted.clear()
        persisted.update(copy.deepcopy(state))
        calls.append("persist")
        return copy.deepcopy(state)

    def publish(sid, snapshot):
        assert snapshot == persisted
        calls.append("publish")

    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh)
    monkeypatch.setattr(module, "compute_sync_status_cached", lambda sid: copy.deepcopy(persisted))
    monkeypatch.setattr(module.publisher, "publish", publish)
    return module.RequestMetricsObserver(), state, persisted, calls


def test_observer_persistence_dedup_failure_backoff_and_high_scale_skip(observer_state, monkeypatch, caplog):
    observer, state, persisted, calls = observer_state
    observer.reconcile()
    observer.reconcile()
    assert calls == ["persist", "publish", "persist"]
    state["request"]["last_sync_at"] = "2026-09-08T18:00:00Z"
    observer.reconcile()
    assert calls[-2:] == ["persist", "publish"]
    state["request"].update(latest_log_at=None, total_rows=0)
    observer.reconcile()
    assert calls[-2:] == ["persist", "publish"]

    # Failed observation preserves last success and backs off
    before = copy.deepcopy(persisted)
    calls.clear()
    refresh_fail = MagicMock(side_effect=RuntimeError("unavailable"))
    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh_fail)
    monkeypatch.setattr(module, "FAILURE_BACKOFF_BASE_S", 10.0)
    observer.reconcile()
    observer.reconcile()
    assert persisted == before and calls == [] and "request_metrics.refresh_failed" in caplog.text
    assert refresh_fail.call_count == 1 and observer._failure_backoff_s["test-service"] == 20.0

    # High-scale registered service is skipped
    refresh_hs = MagicMock()
    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh_hs)
    monkeypatch.setattr(
        module,
        "get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda self, sid: object()})(),
    )
    module.RequestMetricsObserver().reconcile()
    refresh_hs.assert_not_called()


def test_concurrent_passes_coalesce_without_overlapping(observer_state, monkeypatch):
    observer, _, _, calls = observer_state
    entered, release = threading.Event(), threading.Event()
    refresh = module.refresh_durable_request_metrics

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(2)
        return refresh(*args, **kwargs)

    monkeypatch.setattr(module, "refresh_durable_request_metrics", slow)
    thread = threading.Thread(target=observer.reconcile)
    thread.start()
    try:
        assert entered.wait(2)
        observer.reconcile()
        assert calls == []
    finally:
        release.set()
        thread.join(2)
    assert not thread.is_alive() and calls == ["persist", "publish"]


def test_observer_lifecycle_modes_and_bounded_stop(observer_state, monkeypatch, caplog):
    # Disabled in file mode
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: False)
    file_obs = module.RequestMetricsObserver()
    file_obs.start()
    assert file_obs._thread is None
    file_obs.stop()

    # Periodic reconciliation continues and stops cleanly even with FLA_DEV_NO_CRONS=1
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    observer, _, _, calls = observer_state
    monkeypatch.setattr(module, "RECONCILE_INTERVAL_S", 0.01)
    twice = threading.Event()
    orig_reconcile = observer.reconcile

    def observed_pass():
        orig_reconcile()
        if calls.count("persist") >= 2:
            twice.set()

    monkeypatch.setattr(observer, "reconcile", observed_pass)
    observer.start()
    first_thread = observer._thread
    try:
        assert twice.wait(2)
        observer.start()
        assert observer._thread is first_thread
    finally:
        observer.stop()
    assert not first_thread.is_alive() and observer._thread is None and calls.count("publish") == 1

    # Bounded stop timeout prevents overlapping restart
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(module, "STOP_TIMEOUT_S", 0.01)
    monkeypatch.setattr(observer, "reconcile", lambda: (entered.set(), release.wait(2)))
    observer.start()
    blocked_thread = observer._thread
    try:
        assert entered.wait(2)
        observer.stop()
        assert blocked_thread.is_alive()
        observer.start()
        assert observer._thread is blocked_thread and "request_metrics.stop_timeout" in caplog.text
    finally:
        release.set()
        blocked_thread.join(2)
        observer.stop()
    assert observer._thread is None


@pytest.mark.asyncio
async def test_application_lifespan_owns_observer_order(monkeypatch):
    from backend import main

    calls = []

    @asynccontextmanager
    async def application(app):
        calls.append("application_start")
        yield
        calls.append("application_stop")

    monkeypatch.setattr(main, "_application_lifespan", application)
    monkeypatch.setattr(module.observer, "start", lambda: calls.append("observer_start"))
    monkeypatch.setattr(module.observer, "stop", lambda: calls.append("observer_stop"))
    monkeypatch.setattr(main.request_telemetry, "force_flush", lambda: None)
    monkeypatch.setattr(main.clickhouse_observer, "stop_clickhouse_observer", lambda: None)
    monkeypatch.setattr(main.clickhouse_client, "close_clickhouse_client", lambda: None)
    async with main.lifespan(main.app):
        calls.append("serving")
    assert calls == ["application_start", "observer_start", "serving", "observer_stop", "application_stop"]
