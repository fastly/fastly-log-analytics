"""Tests for the system-metrics sampler.

``sample_system_metrics`` shapes the bundled payload that the
``system-metrics`` channel of the multiplexed admin event stream
(``/api/admin/events/stream``) pushes to the browser. The interesting
behaviour lives in the sampler, which we exercise here directly; the
streaming wiring (channel framing, serviceless union, cleanup) is
covered by tests/routers/test_admin_events_stream.py and the frontend's
useAdminEventStream tests.

What we cover here:
- The sampler returns a dict with all seven expected slice keys.
- A failing sampler component degrades to None for that slice
  without nulling the rest of the payload.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from backend import system_metrics_sampler as sampler

EXPECTED_KEYS = (
    "health_snapshot",
    "metric_history_1h",
    "queries_summary",
    "slow_queries_count",
    "log_accounting",
    "metadata_storage",
    "system_jobs",
)


def test_sampler_returns_all_slice_keys():
    """The sampler is what shapes the SSE payload. The seven keys
    must match the React Query keys the frontend hook dispatches to
    via setQueryData — drift between them silently breaks the cards."""
    from backend.system_metrics_sampler import sample_system_metrics

    payload = sample_system_metrics("svc-test")
    for key in EXPECTED_KEYS:
        assert key in payload, f"missing slice key: {key}"


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


# ── Process-wide dedup (sample_system_metrics_cached) ────────────────────────
#
# The multiplexed admin event stream runs one system-metrics feeder loop per
# connection, but the SAMPLE is deduped process-wide so N admin tabs cost one
# recompute per window, not N. These pin that contract: cache-hit within TTL,
# single-flight collapse of concurrent callers, per-service isolation, expiry,
# and no caching of exceptions.


@pytest.fixture(autouse=True)
def _reset_metrics_cache():
    """Module-level cache + per-key asyncio.Locks would otherwise bleed
    across tests (and locks bind to a now-closed loop) under -n auto."""
    sampler.reset_metrics_cache()
    yield
    sampler.reset_metrics_cache()


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
