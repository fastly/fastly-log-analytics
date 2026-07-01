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

import pytest
from fastapi.testclient import TestClient

from backend.main import app
from backend.routers.admin import events as ev
from tests.utils.polling import await_until


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


# ── _offer drop-oldest ──────────────────────────────────────────────────────


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


# ── channel validation (immediate return → TestClient) ──────────────────────


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


# ── envelope framing ────────────────────────────────────────────────────────


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


# ── share channel ───────────────────────────────────────────────────────────


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


# ── serviceless union ───────────────────────────────────────────────────────


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


# ── channel filtering ───────────────────────────────────────────────────────


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


# ── feeder cleanup on disconnect ────────────────────────────────────────────


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


# ── off-event-loop offload ──────────────────────────────────────────────────


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
