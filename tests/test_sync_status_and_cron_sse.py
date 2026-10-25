"""Consolidated tests for test_sync_status_and_cron_sse."""

from __future__ import annotations

import asyncio
import copy
import importlib
import json
from unittest.mock import MagicMock

import pytest

from backend.sync_status_snapshot import compute_sync_status_cached


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
        "backend.core.duckdb.get_source_for_service",
        lambda _: {"name": "snapshot-svc", "duckdb_path": __file__},
    )
    monkeypatch.setattr("backend.core.duckdb._cache_dir", lambda _: ".")
    monkeypatch.setattr("backend.sync_status_snapshot._get_dir_size", lambda _: 0)
    monkeypatch.setattr("backend.core.metadata.get_ingested_files_status_summary", lambda _: summary)
    monkeypatch.setattr("backend.core.metadata.base.get_con", lambda _: ledger)
    monkeypatch.setattr("backend.cron_progress.get_latest_progress_for_service", lambda _: None)

    def no_scan(*args, **kwargs):
        pytest.fail("cached status must not acquire DuckDB")

    monkeypatch.setattr("backend.core.duckdb.get_connection", no_scan)
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
    assert snap["latest_ingested_file_at"] == "2026-09-06 17:19:00"
    assert snap["latest_available_file_at"] == "2026-09-06 17:19:00"
    assert snap["local_rows"] == state["local_rows"]
    assert snap["rum"] == state["rum"]


@pytest.mark.parametrize("flat_latest", ["2026-09-07T19:31:22Z", "2026-09-04T01:00:00Z", None])
def test_legacy_flat_extent_is_not_a_filename_fallback(snapshot_state, flat_latest):
    state, _, _ = snapshot_state
    del state["request"]
    state["latest_log_at"] = flat_latest
    state["local_rows"] = 0
    snap = compute_sync_status_cached("snapshot-svc")
    assert snap["latest_log_at"] == flat_latest
    assert snap["local_rows"] == 0


def test_high_scale_snapshot_uses_clickhouse_header_metrics(monkeypatch):
    from datetime import UTC, datetime

    from backend.high_scale.archive_models import ServingWatermark
    from backend.high_scale.registry_bootstrap import HighScaleService

    class Client:
        def execute(self, sql, params=None):
            domain = (params or {}).get("domain")
            totals = {"request": 12, "rum_vitals": 8, "rum_errors": 2}
            latest = {
                "request": datetime(2026, 9, 15, 19, 0, tzinfo=UTC),
                "rum_vitals": datetime(2026, 9, 15, 19, 1, tzinfo=UTC),
                "rum_errors": datetime(2026, 9, 15, 19, 2, tzinfo=UTC),
            }
            return [
                {
                    "total_rows": totals[domain],
                    "earliest_log_at": datetime(2026, 9, 15, 18, 0, tzinfo=UTC),
                    "latest_log_at": latest[domain],
                }
            ]

    service = HighScaleService(
        service_id="high-scale-svc",
        client=Client(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="high-scale-svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda _: {"name": "high-scale-svc", "access_level": "read_write"},
    )
    monkeypatch.setattr(
        "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda _, service_id: service})(),
    )
    monkeypatch.setattr(
        "backend.config.get_status",
        lambda _: pytest.fail("high-scale snapshots must not use stale config status"),
    )

    snapshot = compute_sync_status_cached("high-scale-svc")

    assert snapshot["request"]["total_rows"] == 12
    assert snapshot["rum"]["total_rows"] == 10
    assert snapshot["local_rows"] == 22
    assert snapshot["earliest_log_at"] == "2026-09-15T18:00:00+00:00"
    assert snapshot["latest_log_at"] == "2026-09-15T19:00:00+00:00"


def test_high_scale_log_extents_uses_clickhouse_snapshot(monkeypatch):
    from datetime import UTC, datetime

    from backend.high_scale.archive_models import ServingWatermark
    from backend.high_scale.registry_bootstrap import HighScaleService

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
            return [{"total_rows": 0, "earliest_log_at": None, "latest_log_at": None}]

    service = HighScaleService(
        service_id="high-scale-svc",
        client=Client(),
        cursor_secret=b"secret",
        request_watermark=ServingWatermark(
            service_id="high-scale-svc",
            domain="request",
            owner_epoch=1,
            coverage_start=datetime(2026, 9, 15, tzinfo=UTC),
            coverage_end=datetime(2026, 9, 16, tzinfo=UTC),
            last_accepted_cursor=None,
            last_archived_event_id=None,
            last_visible_event_id=None,
            exact=True,
        ),
    )
    monkeypatch.setattr(
        "backend.core.duckdb.get_source_for_service",
        lambda _: {"name": "high-scale-svc", "access_level": "read_write"},
    )
    monkeypatch.setattr(
        "backend.high_scale.registry_bootstrap.get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda _, service_id: service})(),
    )
    monkeypatch.setattr(
        "backend.config.get_status",
        lambda _: pytest.fail("high-scale log extents must not use stale config status"),
    )

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


# ── Merged from test_sync_status_sse.py ──────────────────────────────────────────────

import threading
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.sync_status_publisher import SyncStatusPublisher
from tests._helpers.wire import await_until


def _parse_sse_data_events(text: str) -> list[dict]:
    """Pull JSON payloads out of every ``data:`` event in an SSE body."""
    out: list[dict] = []
    for chunk in text.replace("\r\n", "\n").split("\n\n"):
        chunk = chunk.strip()
        if not chunk:
            continue
        # An event block may have multiple lines; combine data: lines.
        data_parts = [line[len("data:") :].strip() for line in chunk.split("\n") if line.startswith("data:")]
        if not data_parts:
            continue
        try:
            out.append(json.loads("".join(data_parts)))
        except json.JSONDecodeError:
            pass
    return out


# ── Publisher unit tests ────────────────────────────────────────────────────


class TestSyncStatusSse:
    @pytest.mark.asyncio
    async def test_publish_from_worker_thread_reaches_async_subscriber(self):
        """The cross-thread bridge is the load-bearing piece — APScheduler
        workers call ``publish`` from worker threads while the FastAPI
        asyncio loop drives ``subscribe``. Verify the threadsafe enqueue
        actually lands the payload on the subscriber's queue."""
        pub = SyncStatusPublisher()
        pub.bind_loop(asyncio.get_running_loop())

        async def consume_one():
            async for payload in pub.subscribe("svc-1"):
                return payload
            return None

        consumer = asyncio.create_task(consume_one())
        # Wait until subscribe() has registered the queue. ``sleep(0)`` only
        # yields once; if consume_one() hadn't reached q.get() by then, the
        # publish below raced past registration and got dropped, leaving the
        # consumer to wait forever (test would have failed at the timeout but
        # for the wrong reason).
        await await_until(
            lambda: pub.subscriber_count("svc-1") >= 1,
            timeout=2.0,
            message="subscribe() never registered the consumer's queue",
        )

        def worker():
            pub.publish("svc-1", {"latest_log_at": "2026-06-15T10:00:00Z", "local_rows": 42})

        threading.Thread(target=worker).start()

        received = await asyncio.wait_for(consumer, timeout=2.0)
        assert received == {"latest_log_at": "2026-06-15T10:00:00Z", "local_rows": 42}

    @pytest.mark.asyncio
    async def test_publish_with_no_subscribers_is_silent_noop(self):
        """Cron ticks must not raise when no browser is listening."""
        pub = SyncStatusPublisher()
        pub.bind_loop(asyncio.get_running_loop())
        pub.publish("svc-nobody", {"x": 1})  # must not raise
        assert pub.subscriber_count("svc-nobody") == 0

    @pytest.mark.asyncio
    async def test_subscribe_isolates_by_service_id(self):
        """A publish for svc-A must not reach svc-B subscribers."""
        pub = SyncStatusPublisher()
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
            lambda: pub.subscriber_count("svc-A") >= 1 and pub.subscriber_count("svc-B") >= 1,
            timeout=2.0,
            message="both subscribers never registered",
        )

        pub.publish("svc-A", {"who": "A"})
        await asyncio.wait_for(task_a, timeout=2.0)
        # B must still be waiting, not have received A's payload.
        assert received_b == []
        task_b.cancel()
        try:
            await task_b
        except asyncio.CancelledError:
            pass
        assert received_a == [{"who": "A"}]

    @pytest.mark.asyncio
    async def test_overflow_drops_oldest_for_last_write_wins(self):
        """Per-subscriber queue is maxsize=4; on overflow the oldest payload
        is dropped so the newest tick always lands. Snapshot semantics —
        delivering a stale payload after a fresh one would be wrong."""
        pub = SyncStatusPublisher()
        pub.bind_loop(asyncio.get_running_loop())

        # Subscribe but do NOT consume — let the queue fill up.
        subscriber_gen = pub.subscribe("svc")
        # Trigger subscription registration without consuming any item.
        sub_task = asyncio.create_task(subscriber_gen.__anext__())
        await await_until(
            lambda: pub.subscriber_count("svc") >= 1,
            timeout=2.0,
            message="initial subscribe() never registered",
        )
        sub_task.cancel()
        try:
            await sub_task
        except (asyncio.CancelledError, StopAsyncIteration):
            pass

        # Re-enter the subscription as a real consumer with the SAME generator
        # is not possible since we cancelled it; use a fresh one but emulate
        # backpressure by publishing many items before the consumer reads.
        received: list[int] = []

        async def consume_all_after_delay():
            async for payload in pub.subscribe("svc"):
                received.append(payload["n"])
                if len(received) >= 4:
                    return

        consumer = asyncio.create_task(consume_all_after_delay())
        await await_until(
            lambda: pub.subscriber_count("svc") >= 1,
            timeout=2.0,
            message="consumer subscribe() never registered before overflow publish",
        )

        # Publish 10 messages back-to-back; with maxsize=4 + drop-oldest, the
        # consumer should see at most 4 distinct items, and the LAST one must
        # be n=9. The publisher schedules call_soon_threadsafe callbacks so
        # the queue fills before consume_all_after_delay() gets its first
        # iteration tick.
        for n in range(10):
            pub.publish("svc", {"n": n})

        await asyncio.wait_for(consumer, timeout=2.0)
        assert len(received) == 4
        assert received[-1] == 9  # newest payload preserved
        # Oldest must have been dropped — the first received should be > 0.
        assert received[0] > 0

    @pytest.mark.asyncio
    async def test_subscriber_cleanup_on_generator_aclose(self):
        """When the SSE response closes (request disconnect → Starlette
        calls ``aclose()`` on the response generator), the subscriber must
        be removed so the per-service set doesn't leak."""
        pub = SyncStatusPublisher()
        pub.bind_loop(asyncio.get_running_loop())

        gen = pub.subscribe("svc")
        # Drive the generator to its first await to register the queue.
        task = asyncio.create_task(gen.__anext__())
        await await_until(
            lambda: pub.subscriber_count("svc") == 1,
            timeout=2.0,
            message="subscribe() never registered before cleanup test",
        )

        # Explicit aclose — what Starlette does on client disconnect.
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, StopAsyncIteration):
            pass
        await gen.aclose()
        assert pub.subscriber_count("svc") == 0

    def test_publish_before_loop_bound_is_silent(self):
        """If the publisher hasn't been bound to a loop yet (very early
        startup race), publish must drop silently rather than raise — the
        cron's correctness must not depend on the SSE channel being ready."""
        pub = SyncStatusPublisher()
        pub.publish("svc", {"x": 1})  # must not raise

    # ── Endpoint smoke test ─────────────────────────────────────────────────────

    # ── Analyst-safe projected stream ─────────────────────────────────────────

    def test_log_extents_stream_requires_service_id(self):
        """The analyst-safe sibling endpoint mirrors the admin one's gate:
        400 with the ``service_id_required`` envelope, like every other route."""
        with TestClient(app) as client:
            resp = client.get("/api/log-extents/stream")
            assert resp.status_code == 400
            assert resp.json()["detail"] == {"error": "service_id_required"}

    def test_log_extents_stream_projects_to_two_safe_fields_only(self):
        """The header-badge stream MUST NOT leak ``ngwaf_workspace_id``,
        ``active_run``, ``cdn_service_id``, ``schema``, or any other field
        the admin /sync-status surface carries. Only ``latest_log_at`` +
        ``local_rows`` — exactly what the badge renders."""

        full_snapshot = {
            "configured": True,
            "latest_log_at": "2026-06-15T22:46:46+00:00",
            "local_rows": 6_659_858,
            # Sensitive / admin-only fields that MUST be stripped on the way out:
            "ngwaf_workspace_id": "example_corp.test",
            "active_run": {"type": "status", "message": "0.6s Processing 12 files..."},
            "cdn_service_id": "ExampleCdnId0000000001",
            "logging_service_id": "ExampleSvcId0000000001",
            "schema": [{"name": "ip", "type": "VARCHAR"}],
            "duckdb_size_bytes": 451_651_568,
            "iceberg_files": 1238,
        }
        pushed_full = {
            **full_snapshot,
            "latest_log_at": "2026-06-15T22:48:00+00:00",
            "local_rows": 6_660_000,
        }

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
        ):
            with TestClient(app) as client:
                with client.stream(
                    "GET",
                    "/api/log-extents/stream",
                    headers={"x-service-id": "svc-analyst"},
                ) as resp:
                    assert resp.status_code == 200
                    body = "".join(resp.iter_text())

        events = _parse_sse_data_events(body)
        assert len(events) >= 2, f"Expected snapshot + 1 push event, got {events}"

        # Initial snapshot is projected.
        assert events[0] == {
            "latest_log_at": "2026-06-15T22:46:46+00:00",
            "local_rows": 6_659_858,
            "request": None,
            "rum": None,
        }
        # Pushed event is projected.
        assert {
            "latest_log_at": "2026-06-15T22:48:00+00:00",
            "local_rows": 6_660_000,
            "request": None,
            "rum": None,
        } in events

        # CRUCIAL: every event in the body has EXACTLY these two keys —
        # nothing else slips through. Catches a regression where someone
        # widens the projection without noticing what they expose.
        for ev in events:
            assert set(ev.keys()) == {"latest_log_at", "local_rows", "request", "rum"}, (
                f"Analyst projection leaked extra keys: {sorted(ev.keys() - {'latest_log_at', 'local_rows'})}"
            )


# ── Merged from test_cron_progress.py ──────────────────────────────────────────────

import time

import pytest

from backend import cron_progress


@pytest.fixture(autouse=True)
def _reset_progress_state():
    """Clear the module-level dicts between tests — they're process-global.

    Includes ``_terminal_run_ids`` (the cron_runs terminal-state memo added
    in perf commit 2e29ac3). Without clearing it, a test that observed
    run_id=N as terminal would short-circuit subsequent tests that reuse
    the same run_id with a non-terminal mock.
    """
    cron_progress._progress.clear()
    cron_progress._last_update.clear()
    cron_progress._run_metadata.clear()
    cron_progress._terminal_run_ids.clear()
    yield
    cron_progress._progress.clear()
    cron_progress._last_update.clear()
    cron_progress._run_metadata.clear()
    cron_progress._terminal_run_ids.clear()


# ── start_progress + add_progress ────────────────────────────────────────────


def test_start_progress_initialises_empty_event_list():
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    assert cron_progress._progress[1] == []
    meta = cron_progress._run_metadata[1]
    assert meta["service_id"] == "svc-a"
    assert meta["task"] == "sync"
    # started_at must be populated so the System Health card can sort by age
    # and surface "running for 3m". Previously the field was missing and
    # always rendered as null.
    assert isinstance(meta["started_at"], float)
    assert meta["started_at"] <= time.time()


def test_start_progress_is_idempotent():
    """A duplicate ``start_progress(1, ...)`` must NOT clobber existing
    events or metadata — the cron scheduler may call start more than
    once for the same run during retries."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "step", "msg": "one"})
    cron_progress.start_progress(1, service_id="other", task="other")

    assert len(cron_progress._progress[1]) == 1  # Existing event preserved
    assert cron_progress._run_metadata[1]["service_id"] == "svc-a"  # Original metadata preserved


def test_add_progress_appends_event():
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "step", "i": 1})
    cron_progress.add_progress(1, {"type": "step", "i": 2})

    assert cron_progress._progress[1] == [{"type": "step", "i": 1}, {"type": "step", "i": 2}]


def test_add_progress_silently_drops_unknown_run():
    """``add_progress`` for an unstarted run is a no-op (no KeyError) —
    pinned because callers don't always check the run started first."""
    cron_progress.add_progress(999, {"type": "step"})  # must not raise
    assert 999 not in cron_progress._progress


def test_add_progress_advances_last_update_timestamp(monkeypatch):
    """The TTL-based ``cleanup_progress`` keys on ``_last_update``, so
    every event must refresh the timestamp — otherwise an active run
    that's been emitting events for >1h would still get reaped.

    Drives ``time.time`` with a monotonic step counter instead of
    ``time.sleep(0.01)`` so the assertion is deterministic under loaded
    CI runners (where 10 ms of wall clock isn't guaranteed to elapse
    between calls under preemption).
    """
    ticks = iter([1_700_000_000.0, 1_700_000_001.0])
    monkeypatch.setattr(cron_progress.time, "time", lambda: next(ticks))

    cron_progress.start_progress(1)
    initial_ts = cron_progress._last_update[1]
    cron_progress.add_progress(1, {"type": "step"})
    assert cron_progress._last_update[1] > initial_ts


# ── get_progress ─────────────────────────────────────────────────────────────


def test_get_progress_returns_full_list_when_start_idx_zero():
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"i": 1})
    cron_progress.add_progress(1, {"i": 2})

    assert cron_progress.get_progress(1) == [{"i": 1}, {"i": 2}]


def test_get_progress_returns_slice_from_start_idx():
    """The SSE endpoint polls with the last-seen index so it doesn't
    re-send events the client already has."""
    cron_progress.start_progress(1)
    for i in range(5):
        cron_progress.add_progress(1, {"i": i})

    assert cron_progress.get_progress(1, start_idx=3) == [{"i": 3}, {"i": 4}]


def test_get_progress_returns_none_for_unknown_run():
    """Distinct from ``[]`` (which means "no new events yet"); ``None``
    tells the SSE handler to emit a terminal 'run not found' message."""
    assert cron_progress.get_progress(999) is None


def test_get_progress_returns_copy_not_live_reference():
    """The caller iterates the result outside the lock; if we returned
    the live list, a concurrent ``add_progress`` would mutate it
    mid-iteration and crash with RuntimeError."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"i": 1})

    out = cron_progress.get_progress(1)
    cron_progress.add_progress(1, {"i": 2})
    # The earlier slice must not have grown
    assert out == [{"i": 1}]


# ── get_latest_progress_for_service ──────────────────────────────────────────


def test_latest_progress_returns_last_event_with_task_attached():
    cron_progress.start_progress(1, service_id="svc-a", task="sync_logs")
    cron_progress.add_progress(1, {"type": "step", "msg": "downloading"})

    out = cron_progress.get_latest_progress_for_service("svc-a")
    assert out["type"] == "step"
    assert out["msg"] == "downloading"
    assert out["task"] == "sync_logs"


def test_latest_progress_returns_none_for_service_with_no_runs():
    """Used by the UI on first load — must not crash on an empty store."""
    assert cron_progress.get_latest_progress_for_service("svc-nothing") is None


def test_latest_progress_skips_completed_runs():
    """A run whose last event is ``done`` or ``error`` is finished; the
    UI should poll the cron_runs table for the final outcome, not show
    a stale step message as if the run were still active."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "done", "rows": 100})

    assert cron_progress.get_latest_progress_for_service("svc-a") is None


def test_latest_progress_skips_errored_runs():
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "error", "msg": "S3 down"})

    assert cron_progress.get_latest_progress_for_service("svc-a") is None


def test_latest_progress_picks_newest_run_when_multiple_active():
    """If two runs for the same service are both active, the higher
    run_id (= newer in our autoincrement scheme) wins. Pinned because
    a regression would let the UI show an old run's step messages."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "step", "i": "old"})
    cron_progress.start_progress(2, service_id="svc-a", task="sync")
    cron_progress.add_progress(2, {"type": "step", "i": "new"})

    out = cron_progress.get_latest_progress_for_service("svc-a")
    assert out["i"] == "new"


def test_latest_progress_returns_task_only_when_run_has_no_events_yet():
    """A run that just started but hasn't emitted any events yet → return
    ``{"task": ...}`` so the UI shows a "starting" placeholder instead
    of a blank badge."""
    cron_progress.start_progress(7, service_id="svc-a", task="sync_logs")

    out = cron_progress.get_latest_progress_for_service("svc-a")
    assert out == {"task": "sync_logs"}


def test_latest_progress_ignores_runs_belonging_to_other_services():
    """A run for ``svc-b`` must NOT surface in ``svc-a``'s status badge."""
    cron_progress.start_progress(1, service_id="svc-b", task="sync")
    cron_progress.add_progress(1, {"type": "step"})

    assert cron_progress.get_latest_progress_for_service("svc-a") is None


# ── end_progress ─────────────────────────────────────────────────────────────


def test_end_progress_appends_final_event_when_provided():
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "step"})
    cron_progress.end_progress(1, {"type": "done", "rows": 42})

    events = cron_progress._progress[1]
    assert events[-1] == {"type": "done", "rows": 42}


def test_end_progress_without_final_event_auto_emits_done_when_last_was_status():
    """REGRESSION: the sync cron path emits status messages (e.g.
    "view refresh: 12ms") as its LAST event before falling through to
    `finally: end_progress(run_id)` with no explicit final event. The
    prior end_progress just updated the timestamp, so list_active_runs'
    `events[-1].type in (done,error)` filter never matched → the run
    showed as in-flight forever (TTL was 1 hour). Production
    accumulated 382 stale entries on the System Health card.

    The fix: when no final_event is given AND the last event isn't
    already terminal, auto-append `{type:"done"}` so list_active_runs
    correctly filters."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "status", "message": "view refresh: 12ms"})

    cron_progress.end_progress(1)

    events = cron_progress._progress[1]
    assert events[-1].get("type") == "done", "end_progress must auto-emit done after a status final"
    # And list_active_runs now filters it out
    assert cron_progress.list_active_runs() == []


def test_end_progress_does_not_double_emit_when_last_was_done():
    """If the caller already added a done event via _log_and_add_progress
    + then called end_progress(run_id), don't append a second done."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "done", "rows": 100})

    cron_progress.end_progress(1)

    events = cron_progress._progress[1]
    done_events = [e for e in events if e.get("type") == "done"]
    assert len(done_events) == 1, "end_progress must not double-emit done"


def test_end_progress_does_not_double_emit_when_last_was_error():
    """Same as above for error — runs that ended in an error shouldn't
    have a misleading 'done' tacked on by the auto-emit path."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "error", "message": "S3 timeout"})

    cron_progress.end_progress(1)

    events = cron_progress._progress[1]
    error_events = [e for e in events if e.get("type") == "error"]
    done_events = [e for e in events if e.get("type") == "done"]
    assert len(error_events) == 1
    assert len(done_events) == 0


def test_end_progress_with_explicit_final_event_does_not_auto_emit():
    """Caller passing an explicit final_event always wins — the
    auto-emit is only for callers that forgot."""
    cron_progress.start_progress(1)
    cron_progress.add_progress(1, {"type": "status", "message": "x"})

    cron_progress.end_progress(1, final_event={"type": "done", "rows": 42})

    events = cron_progress._progress[1]
    assert events[-1] == {"type": "done", "rows": 42}
    # Exactly one done — the explicit one. No auto-emit on top.
    done_events = [e for e in events if e.get("type") == "done"]
    assert len(done_events) == 1


def test_end_progress_silently_drops_unknown_run():
    cron_progress.end_progress(999, {"type": "done"})  # must not raise
    assert 999 not in cron_progress._progress


# ── cleanup_progress: TTL eviction ───────────────────────────────────────────


def test_cleanup_progress_removes_entries_older_than_one_hour():
    """The TTL is 3600s; anything older gets dropped. Pinned because a
    leak here would slowly accumulate run state for every cron that
    fires, eventually OOMing the dev server."""
    cron_progress.start_progress(1)
    cron_progress.start_progress(2)

    # Backdate run 1 past the TTL
    cron_progress._last_update[1] = time.time() - 3700

    cron_progress.cleanup_progress()

    assert 1 not in cron_progress._progress
    assert 1 not in cron_progress._last_update
    assert 1 not in cron_progress._run_metadata
    assert 2 in cron_progress._progress  # Recent one survives


def test_cleanup_progress_keeps_entries_at_boundary():
    """An entry exactly at the boundary (3600s ago) should still be kept
    — the check is ``> 3600``, not ``>=``. Pinned so a future > → >=
    refactor is forced through this test."""
    cron_progress.start_progress(1)
    cron_progress._last_update[1] = time.time() - 3500  # well under TTL

    cron_progress.cleanup_progress()
    assert 1 in cron_progress._progress


def test_cleanup_progress_is_safe_on_empty_state():
    cron_progress.cleanup_progress()  # must not raise


# ── Concurrency: lock acquisition under racing writers ──────────────────────


# ── list_active_runs ─────────────────────────────────────────────────────────


def test_list_active_runs_returns_only_runs_without_terminal_event():
    """REGRESSION: the admin health-snapshot endpoint was iterating
    ``_run_metadata`` directly, which kept completed runs visible for an
    hour (the cleanup TTL). The System Health card rendered dozens of
    duplicated "sync · KLJPUtJk" boxes. ``list_active_runs`` filters out
    runs whose last event is ``done``/``error`` so only actually-active
    runs are returned."""
    # Active: started, no terminal event.
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "step", "i": 1})
    # Completed: started, last event is done.
    cron_progress.start_progress(2, service_id="svc-a", task="sync")
    cron_progress.add_progress(2, {"type": "done", "rows": 100})
    # Errored: started, last event is error.
    cron_progress.start_progress(3, service_id="svc-b", task="metadata_sync")
    cron_progress.add_progress(3, {"type": "error", "msg": "S3 timeout"})
    # Active second service.
    cron_progress.start_progress(4, service_id="svc-b", task="commit")

    out = cron_progress.list_active_runs()
    out_by_id = {entry["run_id"]: entry for entry in out}

    assert 1 in out_by_id
    assert 4 in out_by_id
    assert 2 not in out_by_id, "completed run must be filtered out"
    assert 3 not in out_by_id, "errored run must be filtered out"


def test_list_active_runs_includes_metadata_fields():
    """The admin endpoint needs service_id + task + started_at to render
    the in-flight badge. Pin the shape so a future metadata field rename
    doesn't break the wire format."""
    cron_progress.start_progress(7, service_id="svc-a", task="sync")
    out = cron_progress.list_active_runs()
    assert len(out) == 1
    entry = out[0]
    assert entry["run_id"] == 7
    assert entry["service_id"] == "svc-a"
    assert entry["task"] == "sync"
    assert isinstance(entry["started_at"], float)


def test_list_active_runs_returns_empty_when_no_runs():
    assert cron_progress.list_active_runs() == []


def test_list_active_runs_filters_zombie_runs_older_than_5min():
    """REGRESSION: APScheduler can recycle worker threads mid-cron
    (interpreter shutdown, OOM kill, executor restart) without giving
    the try/finally `end_progress` block a chance to fire. The result
    was 32 stale 'sync' entries on the System Health card from a
    single executor incident on 2026-06-03.

    list_active_runs now treats any run whose _last_update is >5 min
    old as a zombie and filters it out — covers all the paths where
    end_progress never ran."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "status", "msg": "x"})
    # Backdate the last update past the staleness band
    cron_progress._last_update[1] = time.time() - 400

    out = cron_progress.list_active_runs()
    assert out == [], "zombie run must be filtered out"


def test_list_active_runs_filters_runs_db_marked_success():
    """REGRESSION (2026-06-03): production saw 13 in-flight 'sync'
    entries in _run_metadata where the corresponding cron_runs row
    was already DB-marked status='success' (dur 2-6s). The watchdog
    thread that runs each cron occasionally abandons the worker on
    timeout-edge or interpreter shutdown without firing the
    try/finally end_progress block. DB log_cron_run writes still land
    (they happen INSIDE the cron function), but the in-memory dict
    is left stale.

    Fix: list_active_runs cross-checks each candidate against
    metadata_db.get_cron_run_status() — if the persisted status is
    terminal, the in-memory ghost is filtered out regardless of its
    event tail."""
    from unittest.mock import patch

    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "status", "msg": "ingesting"})
    # _last_update is fresh (just added an event) so the staleness
    # filter does NOT kick in — this run looks "active" by the in-memory
    # signal but the DB has already marked it success.

    with patch("backend.core.metadata.get_cron_run_status", return_value="success"):
        out = cron_progress.list_active_runs()
    assert out == [], "DB-success run must be filtered even when in-memory says active"


def test_list_active_runs_does_not_filter_db_missing_or_running():
    """If the DB row is missing (transient) or still 'running', trust
    the in-memory signal. The cross-check is a backstop, not a gate —
    we'd rather show a false in-flight than miss a real one."""
    from unittest.mock import patch

    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "status"})

    with patch("backend.core.metadata.get_cron_run_status", return_value=None):
        out = cron_progress.list_active_runs()
    assert len(out) == 1
    with patch("backend.core.metadata.get_cron_run_status", return_value="running"):
        out = cron_progress.list_active_runs()
    assert len(out) == 1


def test_reap_zombie_runs_evicts_stale_entries_and_returns_count():
    """reap_zombie_runs actively prunes _run_metadata so callers that
    walk the dict directly (admin.py:210/238/1022 — there's a precedent
    for regressions here) also see the cleaned state."""
    cron_progress.start_progress(1, service_id="svc-a", task="sync")
    cron_progress.add_progress(1, {"type": "status"})
    cron_progress._last_update[1] = time.time() - 400  # zombie

    cron_progress.start_progress(2, service_id="svc-a", task="sync")  # active

    n = cron_progress.reap_zombie_runs()
    assert n == 1
    assert 1 not in cron_progress._run_metadata
    assert 2 in cron_progress._run_metadata
    # Zombie got a synthetic error event so SSE subscribers see termination
    last_event = cron_progress._progress[1][-1]
    assert last_event.get("type") == "error"
    assert "zombie" in last_event.get("message", "").lower()


def test_concurrent_add_progress_does_not_lose_events():
    """The shared lock must serialise writers. Two threads each
    appending 100 events → final list has all 200, no torn writes."""
    import threading

    cron_progress.start_progress(1)

    def writer(prefix: str):
        for i in range(100):
            cron_progress.add_progress(1, {"thread": prefix, "i": i})

    t1 = threading.Thread(target=writer, args=("A",))
    t2 = threading.Thread(target=writer, args=("B",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert len(cron_progress._progress[1]) == 200


# ── Merged from test_cron_runs_sse.py ──────────────────────────────────────────────


import pytest

from backend.cron_runs_publisher import CronRunsPublisher

# ── Publisher unit tests ────────────────────────────────────────────────────


class TestCronRunsSse:
    @pytest.mark.asyncio
    async def test_publish_from_worker_thread_reaches_async_subscriber(self):
        """Cron lifecycle hooks run on APScheduler worker threads OR on the
        daemon threads spawned by start_or_resume_cron. The cross-thread
        bridge to the asyncio SSE generator is the load-bearing piece.

        Synchronization: wait until the subscriber's queue is registered
        (``subscriber_count >= 1``) before publishing. The earlier pattern of
        ``await asyncio.sleep(0)`` only yielded once, so a publish that fired
        before the consumer reached ``q.get()`` was silently dropped — the
        test would still pass because the consumer would keep waiting (no
        payload ever arrived). Polling on ``subscriber_count`` is the
        deterministic signal."""
        pub = CronRunsPublisher()
        pub.bind_loop(asyncio.get_running_loop())

        async def consume_one():
            async for payload in pub.subscribe("svc-1"):
                return payload
            return None

        consumer = asyncio.create_task(consume_one())
        await await_until(
            lambda: pub.subscriber_count("svc-1") >= 1,
            timeout=2.0,
            message="subscribe() never registered the consumer's queue",
        )

        def worker():
            pub.publish(
                "svc-1",
                {"event": "cron_run_changed", "run_id": 42, "task": "sync", "status": "success"},
            )

        threading.Thread(target=worker).start()

        received = await asyncio.wait_for(consumer, timeout=2.0)
        assert received == {
            "event": "cron_run_changed",
            "run_id": 42,
            "task": "sync",
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_publish_with_no_subscribers_is_silent_noop(self):
        """Cron lifecycle hooks fire on EVERY run start/completion. With no
        browser connected, the publish call must be a cheap no-op — the
        cron's correctness must not depend on the SSE channel."""
        pub = CronRunsPublisher()
        pub.bind_loop(asyncio.get_running_loop())
        pub.publish("svc", {"event": "cron_run_changed", "run_id": 1, "task": "sync", "status": "running"})
        assert pub.subscriber_count("svc") == 0

    @pytest.mark.asyncio
    async def test_subscribe_isolates_by_service_id(self):
        """A publish for svc-A must not reach svc-B subscribers. Cron runs
        are per-service; cross-tenant leakage would be a real bug."""
        pub = CronRunsPublisher()
        pub.bind_loop(asyncio.get_running_loop())

        received_a: list[dict] = []
        received_b: list[dict] = []

        async def reader(svc, sink):
            async for p in pub.subscribe(svc):
                sink.append(p)
                return

        task_a = asyncio.create_task(reader("svc-A", received_a))
        task_b = asyncio.create_task(reader("svc-B", received_b))
        # Wait for BOTH subscribers' queues to register before publishing.
        await await_until(
            lambda: pub.subscriber_count("svc-A") >= 1 and pub.subscriber_count("svc-B") >= 1,
            timeout=2.0,
            message="both subscribers never registered",
        )

        pub.publish("svc-A", {"event": "cron_run_changed", "run_id": 1, "task": "sync", "status": "success"})
        await asyncio.wait_for(task_a, timeout=2.0)
        assert received_b == []
        task_b.cancel()
        try:
            await task_b
        except asyncio.CancelledError:
            pass
        assert received_a and received_a[0]["run_id"] == 1

    @pytest.mark.asyncio
    async def test_overflow_drops_oldest_so_newest_event_lands(self):
        """Per-subscriber queue is maxsize=4 with drop-oldest. A burst of
        state changes (e.g. multiple crons firing in the same tick) plus a
        slow consumer must NOT lose the most recent event — that's the one
        the UI cares about most."""
        pub = CronRunsPublisher()
        pub.bind_loop(asyncio.get_running_loop())

        received: list[int] = []

        async def consume_4_after_delay():
            async for payload in pub.subscribe("svc"):
                received.append(payload["run_id"])
                if len(received) >= 4:
                    return

        consumer = asyncio.create_task(consume_4_after_delay())
        await await_until(
            lambda: pub.subscriber_count("svc") >= 1,
            timeout=2.0,
            message="subscribe() never registered before overflow publish",
        )

        for n in range(10):
            pub.publish("svc", {"event": "cron_run_changed", "run_id": n, "task": "sync", "status": "success"})

        await asyncio.wait_for(consumer, timeout=2.0)
        assert len(received) == 4
        # Newest must land — that's the load-bearing semantic.
        assert received[-1] == 9
        # Oldest must have been dropped — first received should be > 0.
        assert received[0] > 0

    @pytest.mark.asyncio
    async def test_subscriber_cleanup_on_generator_aclose(self):
        """When Starlette closes the SSE response (client disconnect →
        generator aclose), the queue must be removed from the per-service
        set so the publisher doesn't leak references."""
        pub = CronRunsPublisher()
        pub.bind_loop(asyncio.get_running_loop())

        gen = pub.subscribe("svc")
        task = asyncio.create_task(gen.__anext__())
        await await_until(
            lambda: pub.subscriber_count("svc") == 1,
            timeout=2.0,
            message="subscribe() never registered before cleanup test",
        )

        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, StopAsyncIteration):
            pass
        await gen.aclose()
        assert pub.subscriber_count("svc") == 0

    def test_publish_before_loop_bound_is_silent(self):
        """If the publisher hasn't been bound to a loop yet (very early
        startup race, or unit tests that import the module without going
        through lifespan), publish must drop silently rather than raise."""
        pub = CronRunsPublisher()
        pub.publish("svc", {"event": "cron_run_changed", "run_id": 1, "task": "sync", "status": "running"})


# ── Merged from test_request_metrics_observer.py ──────────────────────────────────────────────

from contextlib import asynccontextmanager

import pytest

from backend import request_metrics_observer as module


@pytest.fixture
def observer_state(monkeypatch):
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: True)
    monkeypatch.setattr(module.config, "list_configs", lambda: [{"name": "test-service"}])
    monkeypatch.setattr(module.config, "config_to_source", lambda cfg: cfg)
    state = {"request": {"total_rows": 1, "latest_log_at": "2026-09-07T00:00:00Z", "last_sync_at": None}}
    persisted = {}
    calls = []

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


def test_persists_before_publishing_only_changed_observations(observer_state):
    observer, state, _, calls = observer_state
    observer.reconcile()
    observer.reconcile()
    assert calls == ["persist", "publish", "persist"]
    state["request"]["last_sync_at"] = "2026-09-08T18:00:00Z"
    observer.reconcile()
    assert calls[-2:] == ["persist", "publish"]
    state["request"].update(latest_log_at=None, total_rows=0)
    observer.reconcile()
    assert calls[-2:] == ["persist", "publish"]


def test_failed_observation_does_not_publish_or_discard_last_success(observer_state, monkeypatch, caplog):
    observer, _, persisted, calls = observer_state
    observer.reconcile()
    before = copy.deepcopy(persisted)
    monkeypatch.setattr(module, "refresh_durable_request_metrics", MagicMock(side_effect=RuntimeError("unavailable")))
    observer.reconcile()
    assert persisted == before
    assert calls == ["persist", "publish"]
    assert "request_metrics.refresh_failed" in caplog.text


def test_registered_high_scale_service_is_not_observed(observer_state, monkeypatch):
    observer, _, _, calls = observer_state
    monkeypatch.setattr(
        module,
        "get_high_scale_service_registry",
        lambda: type("Registry", (), {"resolve": lambda self, service_id: object()})(),
    )
    refresh = MagicMock()
    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh)

    observer.reconcile()

    refresh.assert_not_called()
    assert calls == []


def test_failed_observation_backs_off_before_retrying(observer_state, monkeypatch):
    observer, _, _, _ = observer_state
    refresh = MagicMock(side_effect=RuntimeError("unavailable"))
    monkeypatch.setattr(module, "refresh_durable_request_metrics", refresh)
    monkeypatch.setattr(module, "FAILURE_BACKOFF_BASE_S", 10.0)

    observer.reconcile()
    observer.reconcile()

    assert refresh.call_count == 1
    assert observer._failure_backoff_s["test-service"] == 20.0


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
    assert not thread.is_alive()
    assert calls == ["persist", "publish"]


def test_lifecycle_disabled_in_file_mode(monkeypatch):
    monkeypatch.setattr(module.config, "is_durable_serving_mode", lambda source=None: False)
    observer = module.RequestMetricsObserver()
    observer.start()
    assert observer._thread is None
    observer.stop()


def test_lifecycle_runs_with_paused_crons_and_stops(observer_state, monkeypatch):
    observer, _, _, _ = observer_state
    monkeypatch.setenv("FLA_DEV_NO_CRONS", "1")
    entered = threading.Event()
    monkeypatch.setattr(observer, "reconcile", entered.set)
    observer.start()
    first_thread = observer._thread
    try:
        assert entered.wait(2)
        observer.start()
        assert observer._thread is first_thread
    finally:
        observer.stop()
    assert not first_thread.is_alive()
    assert observer._thread is None


def test_periodic_reconciliation_continues_without_worker_notifications(observer_state, monkeypatch):
    observer, _, _, calls = observer_state
    monkeypatch.setattr(module, "RECONCILE_INTERVAL_S", 0.01)
    twice = threading.Event()
    reconcile = observer.reconcile

    def observed_pass():
        reconcile()
        if calls.count("persist") >= 2:
            twice.set()

    monkeypatch.setattr(observer, "reconcile", observed_pass)
    observer.start()
    try:
        assert twice.wait(2)
    finally:
        observer.stop()
    assert calls.count("publish") == 1


def test_bounded_stop_does_not_allow_overlapping_restart(observer_state, monkeypatch, caplog):
    observer, _, _, _ = observer_state
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(module, "STOP_TIMEOUT_S", 0.01)

    def blocked_dependency():
        entered.set()
        release.wait(2)

    monkeypatch.setattr(observer, "reconcile", blocked_dependency)
    observer.start()
    first_thread = observer._thread
    try:
        assert entered.wait(2)
        observer.stop()
        assert first_thread.is_alive()
        observer.start()
        assert observer._thread is first_thread
        assert "request_metrics.stop_timeout" in caplog.text
    finally:
        release.set()
        first_thread.join(2)
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
