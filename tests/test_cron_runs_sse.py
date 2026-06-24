"""Cron-runs push channel: in-process publisher + SSE endpoint.

Covers the cross-thread bridge from cron lifecycle hooks
(``start_cron_run`` / ``log_cron_run``) to the asyncio SSE generator,
bounded-queue drop-oldest semantics, and the
``/api/cron-runs/stream`` endpoint's tickle delivery.
"""

from __future__ import annotations

import asyncio
import json
import threading
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.cron_runs_publisher import CronRunsPublisher
from backend.main import app
from tests.utils.polling import await_until

# ── Publisher unit tests ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_from_worker_thread_reaches_async_subscriber():
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
async def test_publish_with_no_subscribers_is_silent_noop():
    """Cron lifecycle hooks fire on EVERY run start/completion. With no
    browser connected, the publish call must be a cheap no-op — the
    cron's correctness must not depend on the SSE channel."""
    pub = CronRunsPublisher()
    pub.bind_loop(asyncio.get_running_loop())
    pub.publish("svc", {"event": "cron_run_changed", "run_id": 1, "task": "sync", "status": "running"})
    assert pub.subscriber_count("svc") == 0


@pytest.mark.asyncio
async def test_subscribe_isolates_by_service_id():
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
async def test_overflow_drops_oldest_so_newest_event_lands():
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
async def test_subscriber_cleanup_on_generator_aclose():
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


def test_publish_before_loop_bound_is_silent():
    """If the publisher hasn't been bound to a loop yet (very early
    startup race, or unit tests that import the module without going
    through lifespan), publish must drop silently rather than raise."""
    pub = CronRunsPublisher()
    pub.publish("svc", {"event": "cron_run_changed", "run_id": 1, "task": "sync", "status": "running"})


# ── Endpoint smoke test ─────────────────────────────────────────────────────


def _parse_sse_data_events(text: str) -> list[dict]:
    """Pull JSON payloads out of every ``data:`` event in an SSE body.

    Tolerates both LF and CRLF separators — sse-starlette emits CRLF
    (see useSSE.ts comment for the longer story behind that)."""
    out: list[dict] = []
    normalized = text.replace("\r\n", "\n")
    for chunk in normalized.split("\n\n"):
        chunk = chunk.strip()
        if not chunk:
            continue
        data_parts = [line[len("data:") :].strip() for line in chunk.split("\n") if line.startswith("data:")]
        if not data_parts:
            continue
        try:
            out.append(json.loads("".join(data_parts)))
        except json.JSONDecodeError:
            pass
    return out


def test_stream_requires_service_id():
    """No x-service-id → 400, not a hung connection."""
    with TestClient(app) as client:
        resp = client.get("/api/cron-runs/stream")
        assert resp.status_code == 400


def test_stream_delivers_publisher_event():
    """End-to-end wiring: connect → publish from a worker thread → the
    JSON tickle arrives in the response body."""

    pushed = {
        "event": "cron_run_changed",
        "run_id": 999,
        "task": "sync",
        "status": "success",
        "ts": "2026-06-15T23:37:41Z",
    }

    from backend.routers.services import cron as router_module

    pushed_event = asyncio.Event()

    async def fake_subscribe(_svc_id):
        # Wait briefly for the test thread to signal, then yield once.
        for _ in range(20):
            if pushed_event.is_set():
                yield pushed
                return
            await asyncio.sleep(0.1)

    def trigger_push():
        loop = router_module.cron_runs_publisher._loop
        if loop:
            loop.call_soon_threadsafe(pushed_event.set)

    threading.Timer(0.3, trigger_push).start()

    with patch.object(router_module.cron_runs_publisher, "subscribe", fake_subscribe):
        with TestClient(app) as client:
            with client.stream(
                "GET",
                "/api/cron-runs/stream",
                headers={"x-service-id": "svc-tickle"},
            ) as resp:
                assert resp.status_code == 200
                body = "".join(resp.iter_text())

    events = _parse_sse_data_events(body)
    assert pushed in events
