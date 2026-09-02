"""Valkey SSE backplane contract (``SSE_BACKPLANE=valkey``, multi-pod).

This class is what makes SSE work across pods: a client connects to one
pod while the event that must reach it is produced on another (or in a
Celery worker with no asyncio loop at all). The behaviours pinned here
are the ones whose failure mode is a *silently* frozen UI rather than an
error — the reason the backplane grew failure counters in the first place.

Redis itself is not under test, so the client is faked. What IS under
test is the routing (loop-bound vs worker/sync vs loop-shutting-down),
the never-raise-never-silently-drop contract, channel naming (a channel
collision here would leak one tenant's events to another), and the
subscriber's reconnect-instead-of-hang behaviour.
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from backend.utils import valkey_publisher as vp
from backend.utils.valkey_publisher import ValkeyPublisher


@pytest.fixture(autouse=True)
def _isolate_module_globals():
    """``PUBLISH_STATS``, the rate-limit clock and the cached sync client are
    all module-global — without a reset, assertions here would be cumulative
    and order-dependent (the same test-isolation trap as
    ``_lock_retry_count`` in tests/core/test_duckdb_concurrency.py)."""
    saved_stats = dict(vp.PUBLISH_STATS)
    saved_client = vp._sync_redis
    for k in vp.PUBLISH_STATS:
        vp.PUBLISH_STATS[k] = 0
    vp._last_error_log.clear()
    vp._sync_redis = None
    yield
    vp.PUBLISH_STATS.clear()
    vp.PUBLISH_STATS.update(saved_stats)
    vp._last_error_log.clear()
    vp._sync_redis = saved_client


def _fake_sync_client():
    """Sync Redis stand-in whose pipeline records the staged commands."""
    client = MagicMock()
    pipe = MagicMock()
    pipe.staged = []
    pipe.publish.side_effect = lambda ch, msg: pipe.staged.append(("publish", ch, msg))
    pipe.rpush.side_effect = lambda k, v: pipe.staged.append(("rpush", k, v))
    pipe.ltrim.side_effect = lambda k, a, b: pipe.staged.append(("ltrim", k, a, b))
    client.pipeline.return_value = pipe
    client.pipe = pipe
    return client


# ── channel naming (tenant isolation) ────────────────────────────────────────


def test_channel_is_scoped_by_prefix_and_service():
    """Two services must never share a channel — a collision would fan one
    tenant's events out to another tenant's subscribers."""
    pub = ValkeyPublisher(channel_prefix="sync-status")
    assert pub._channel("svc-a") == "sse:sync-status:svc-a"
    assert pub._channel("svc-b") != pub._channel("svc-a")

    other_feed = ValkeyPublisher(channel_prefix="cron-runs")
    assert other_feed._channel("svc-a") != pub._channel("svc-a")


# ── publish routing ──────────────────────────────────────────────────────────


def test_publish_without_bound_loop_uses_sync_client():
    """The bug this class exists to fix: in a Celery worker ``bind_loop`` is
    never called, and worker-originated cron/sync events were silently
    dropped. No loop bound MUST mean a real sync publish."""
    client = _fake_sync_client()
    pub = ValkeyPublisher(channel_prefix="sync-status")

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"status": "ok"})

    assert ("publish", "sse:sync-status:svc-a", json.dumps({"status": "ok"})) in client.pipe.staged
    client.pipe.execute.assert_called_once()
    assert vp.PUBLISH_STATS["published"] == 1
    assert vp.PUBLISH_STATS["publish_errors"] == 0


def test_publish_failure_counts_and_does_not_raise():
    """A dead backplane must be diagnosable from the counter, and must never
    propagate into the caller — publishers are cron/ingest paths whose real
    work already succeeded."""
    client = MagicMock()
    client.pipeline.side_effect = RuntimeError("valkey down")
    pub = ValkeyPublisher()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"status": "ok"})  # must not raise

    assert vp.PUBLISH_STATS["publish_errors"] == 1
    assert vp.PUBLISH_STATS["published"] == 0


def test_publish_with_bound_loop_goes_through_the_loop():
    """With a loop bound (the FastAPI process) the publish rides asyncio, not
    the sync client."""
    pub = ValkeyPublisher()
    loop = MagicMock()
    pub._loop = loop
    pub._redis = MagicMock()

    with patch.object(vp, "_get_sync_redis") as sync_client:
        pub.publish("svc-a", {"status": "ok"})

    loop.call_soon_threadsafe.assert_called_once()
    sync_client.assert_not_called()


def test_publish_falls_back_to_sync_when_loop_is_gone():
    """Loop shutting down must degrade to a sync publish rather than drop the
    event on the floor."""
    client = _fake_sync_client()
    pub = ValkeyPublisher()
    loop = MagicMock()
    loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
    pub._loop = loop
    pub._redis = MagicMock()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"status": "ok"})

    assert ("publish", "sse:sync-status:svc-a", json.dumps({"status": "ok"})) in client.pipe.staged
    assert vp.PUBLISH_STATS["published"] == 1


@pytest.mark.asyncio
async def test_async_publish_counts_error_without_raising():
    """The coroutine the loop schedules must swallow-and-count too — an
    exception there would surface as an unretrieved-task warning, not a
    diagnosable counter."""
    pub = ValkeyPublisher()
    redis = MagicMock()
    redis.pipeline.side_effect = RuntimeError("broker gone")
    pub._redis = redis
    pub._loop = asyncio.get_running_loop()

    pub.publish("svc-a", {"status": "ok"})
    # Two hops: call_soon_threadsafe runs the callback that creates the
    # future, then the future itself needs a turn.
    for _ in range(4):
        await asyncio.sleep(0)

    assert vp.PUBLISH_STATS["publish_errors"] == 1


# ── history ──────────────────────────────────────────────────────────────────


def test_history_is_not_written_when_disabled():
    """``history_size=0`` (the sync-status feed) must not accumulate keys in
    Valkey — that would be an unbounded memory leak per service."""
    client = _fake_sync_client()
    pub = ValkeyPublisher(history_size=0)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"n": 1})

    assert not [c for c in client.pipe.staged if c[0] in ("rpush", "ltrim")]


def test_history_is_capped_when_enabled():
    """With history on, every publish appends AND trims — the trim is what
    bounds growth."""
    client = _fake_sync_client()
    pub = ValkeyPublisher(history_size=5)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        pub.publish("svc-a", {"n": 1})

    assert ("rpush", "sse:sync-status:svc-a:history", json.dumps({"n": 1})) in client.pipe.staged
    assert ("ltrim", "sse:sync-status:svc-a:history", -5, -1) in client.pipe.staged


def test_get_recent_ticks_empty_when_history_disabled():
    pub = ValkeyPublisher(history_size=0)
    with patch.object(vp, "_get_sync_redis") as client:
        assert pub.get_recent_ticks("svc-a") == []
    client.assert_not_called()


def test_get_recent_ticks_decodes_history():
    client = MagicMock()
    client.lrange.return_value = [json.dumps({"n": 1}), json.dumps({"n": 2})]
    pub = ValkeyPublisher(history_size=10)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.get_recent_ticks("svc-a", count=2) == [{"n": 1}, {"n": 2}]

    client.lrange.assert_called_once_with("sse:sync-status:svc-a:history", -2, -1)


def test_get_recent_ticks_survives_a_broken_broker():
    """The SSE seed must degrade to "no history" rather than 500 the stream."""
    client = MagicMock()
    client.lrange.side_effect = RuntimeError("valkey down")
    pub = ValkeyPublisher(history_size=10)

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.get_recent_ticks("svc-a") == []


# ── subscriber_count (cross-pod) ─────────────────────────────────────────────


def test_subscriber_count_reads_across_pods():
    """This is what suspends the RT poller when nobody is watching — it must
    reflect subscribers on OTHER pods, which is why it goes to Valkey."""
    client = MagicMock()
    client.pubsub_numsub.return_value = [("sse:sync-status:svc-a", 3)]
    pub = ValkeyPublisher()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.subscriber_count("svc-a") == 3


def test_subscriber_count_is_zero_on_error():
    client = MagicMock()
    client.pubsub_numsub.side_effect = RuntimeError("down")
    pub = ValkeyPublisher()

    with patch.object(vp, "_get_sync_redis", return_value=client):
        assert pub.subscriber_count("svc-a") == 0


# ── subscribe ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_subscribe_before_bind_loop_raises():
    """Explicit contract: subscribing is only meaningful in the SSE-serving
    process, and a silent no-op here would hang the stream forever."""
    pub = ValkeyPublisher()
    with pytest.raises(RuntimeError, match="bind_loop"):
        await anext(pub.subscribe("svc-a"))


class _FakePubSub:
    """pubsub stand-in yielding a scripted frame sequence."""

    def __init__(self, frames, fail_after=False):
        self._frames = frames
        self._fail_after = fail_after
        self.subscribed: list[str] = []

    async def subscribe(self, channel):
        self.subscribed.append(channel)

    async def unsubscribe(self, channel):
        pass

    async def close(self):
        pass

    async def listen(self):
        for f in self._frames:
            yield f
        if self._fail_after:
            raise RuntimeError("connection lost")


@pytest.mark.asyncio
async def test_subscribe_yields_decoded_payloads_and_skips_control_frames():
    pub = ValkeyPublisher()
    frames = [
        {"type": "subscribe", "data": 1},  # control frame — must be skipped
        {"type": "message", "data": json.dumps({"n": 1})},
        {"type": "message", "data": json.dumps({"n": 2})},
    ]
    fake = _FakePubSub(frames)
    pub._redis = MagicMock()
    pub._redis.pubsub.return_value = fake

    got = []
    async for payload in pub.subscribe("svc-a"):
        got.append(payload)
        if len(got) == 2:
            break

    assert got == [{"n": 1}, {"n": 2}]
    assert fake.subscribed == ["sse:sync-status:svc-a"]


@pytest.mark.asyncio
async def test_subscribe_counts_undecodable_payload_and_keeps_streaming():
    """One poisoned message must not kill the stream — it is counted and the
    next good frame still arrives."""
    pub = ValkeyPublisher()
    frames = [
        {"type": "message", "data": "{not json"},
        {"type": "message", "data": json.dumps({"n": 7})},
    ]
    pub._redis = MagicMock()
    pub._redis.pubsub.return_value = _FakePubSub(frames)

    got = []
    async for payload in pub.subscribe("svc-a"):
        got.append(payload)
        break

    assert got == [{"n": 7}]
    assert vp.PUBLISH_STATS["decode_errors"] == 1


@pytest.mark.asyncio
async def test_subscribe_reconnects_after_broker_loss():
    """A Valkey restart must reconnect with backoff, not leave the SSE
    generator dead and the client hanging on an open connection."""
    pub = ValkeyPublisher()
    first = _FakePubSub([{"type": "message", "data": json.dumps({"n": 1})}], fail_after=True)
    second = _FakePubSub([{"type": "message", "data": json.dumps({"n": 2})}])
    pub._redis = MagicMock()
    pub._redis.pubsub.side_effect = [first, second]

    sleeps: list[float] = []

    async def _no_sleep(secs):
        sleeps.append(secs)

    got = []
    with patch.object(vp.asyncio, "sleep", _no_sleep):
        async for payload in pub.subscribe("svc-a"):
            got.append(payload)
            if len(got) == 2:
                break

    assert got == [{"n": 1}, {"n": 2}], "must resume delivery on the reconnected subscription"
    assert sleeps, "reconnect must back off rather than hot-loop"
    assert second.subscribed == ["sse:sync-status:svc-a"]
