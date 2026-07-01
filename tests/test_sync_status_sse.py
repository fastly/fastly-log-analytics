"""Sync-status push channel: in-process publisher + analyst-safe stream.

Covers the cross-thread bridge from APScheduler cron workers
(``publish()``) to the asyncio SSE generator (``subscribe()``), the
bounded-queue drop-oldest semantics, and the analyst-safe
``/api/log-extents/stream`` projection. The admin sync-status SSE
delivery that consumes this publisher now lives on the multiplexed
``/api/admin/events/stream`` (sync-status channel) — see
tests/routers/test_admin_events_stream.py.
"""

from __future__ import annotations

import asyncio
import json
import threading
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.sync_status_publisher import SyncStatusPublisher
from tests.utils.polling import await_until

# ── Publisher unit tests ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_from_worker_thread_reaches_async_subscriber():
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
async def test_publish_with_no_subscribers_is_silent_noop():
    """Cron ticks must not raise when no browser is listening."""
    pub = SyncStatusPublisher()
    pub.bind_loop(asyncio.get_running_loop())
    pub.publish("svc-nobody", {"x": 1})  # must not raise
    assert pub.subscriber_count("svc-nobody") == 0


@pytest.mark.asyncio
async def test_subscribe_isolates_by_service_id():
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
async def test_overflow_drops_oldest_for_last_write_wins():
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
async def test_subscriber_cleanup_on_generator_aclose():
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


def test_publish_before_loop_bound_is_silent():
    """If the publisher hasn't been bound to a loop yet (very early
    startup race), publish must drop silently rather than raise — the
    cron's correctness must not depend on the SSE channel being ready."""
    pub = SyncStatusPublisher()
    pub.publish("svc", {"x": 1})  # must not raise


# ── Endpoint smoke test ─────────────────────────────────────────────────────


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


# ── Analyst-safe projected stream ─────────────────────────────────────────


def test_log_extents_stream_requires_service_id():
    """The analyst-safe sibling endpoint mirrors the admin one's gate.

    422 (request-param validation), per the codebase-wide convention.
    """
    with TestClient(app) as client:
        resp = client.get("/api/log-extents/stream")
        assert resp.status_code == 422


def test_log_extents_stream_projects_to_two_safe_fields_only():
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
    assert events[0] == {"latest_log_at": "2026-06-15T22:46:46+00:00", "local_rows": 6_659_858}
    # Pushed event is projected.
    assert {"latest_log_at": "2026-06-15T22:48:00+00:00", "local_rows": 6_660_000} in events

    # CRUCIAL: every event in the body has EXACTLY these two keys —
    # nothing else slips through. Catches a regression where someone
    # widens the projection without noticing what they expose.
    for ev in events:
        assert set(ev.keys()) == {"latest_log_at", "local_rows"}, (
            f"Analyst projection leaked extra keys: {sorted(ev.keys() - {'latest_log_at', 'local_rows'})}"
        )
