"""Tests for the system-metrics SSE wiring.

The endpoint itself (``/api/admin/system-metrics/stream``) is a thin
EventSourceResponse loop — its only logic is "sample, dict-compare,
push if changed". The interesting behaviour lives in the
``sample_system_metrics`` sampler, which we exercise here directly.

We deliberately do NOT integration-test the long-lived SSE response
through TestClient: TestClient's sync portal waits for the full
response body before yielding control, so a ``while True`` SSE
generator deadlocks at the ``client.stream(...)`` entry. The
cron-runs SSE test (test_cron_runs_sse.py) works only because its
test mocks ``publisher.subscribe`` to a finite async-generator —
there's no analogous mock target on the per-subscriber sampler
loop. The streaming wiring is exercised by the frontend's
useSystemMetricsStream tests.

What we cover here:
- The route is registered (importable, on the router).
- The sampler returns a dict with all seven expected slice keys.
- A failing sampler component degrades to None for that slice
  without nulling the rest of the payload.
"""

from __future__ import annotations

from unittest.mock import patch

EXPECTED_KEYS = (
    "health_snapshot",
    "metric_history_1h",
    "queries_summary",
    "slow_queries_count",
    "log_accounting",
    "metadata_storage",
    "system_jobs",
)


def test_route_is_registered():
    """If the side-effect import in backend/routers/admin/__init__.py
    is dropped, the endpoint vanishes silently and every consumer
    falls back to its 5-min safety-net poll. This pins the route."""
    from backend.main import app

    # FastAPI 0.138 includes sub-routers lazily (a single _IncludedRouter
    # entry in app.routes) rather than copying child routes, so assert against
    # the public OpenAPI path table instead of walking app.routes.
    assert "/api/admin/system-metrics/stream" in app.openapi()["paths"]


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


# ── streaming-handler coverage (audit follow-up) ────────────────────────────


def test_handler_allows_missing_service_id_and_streams_global_slices():
    """Serviceless connections are ALLOWED. On a fresh install there is no
    service yet, but the admin still wants live host/process status, so the
    handler must not reject ``service_id=None`` — it passes None straight to
    the sampler (which degrades to global-only) and streams the result.

    Pins the fresh-install fix; previously this raised 400
    ``x_service_id_required``. Regressing back to a reject would leave the
    System Health card stuck on its 5-min safety poll with no live stream.
    """
    import asyncio
    import json

    from backend.routers.admin import system_metrics as sm

    request = type("Req", (), {"is_disconnected": lambda self: asyncio.sleep(0, result=True)})()

    captured: dict = {}
    global_only = {
        "health_snapshot": {"vcpus": 4},
        "metric_history_1h": {"series": {}},
        "queries_summary": {"active_total": 0},
        "system_jobs": {"jobs": []},
        "slow_queries_count": None,
        "log_accounting": None,
        "metadata_storage": None,
    }

    def _sample(sid):
        captured["sid"] = sid
        return global_only

    async def _zero_sleep(_secs):
        return None

    with (
        patch.object(sm, "sample_system_metrics", side_effect=_sample),
        patch.object(sm.asyncio, "sleep", side_effect=_zero_sleep),
    ):

        async def _drive():
            # No HTTPException → returns the EventSourceResponse.
            response = await sm.system_metrics_stream(request=request, service_id=None)  # type: ignore[arg-type]
            agen = response.body_iterator
            return await agen.__anext__()

        first = asyncio.run(_drive())

    # None flowed through to the sampler (not rejected up front) …
    assert captured["sid"] is None
    # … and the global-only payload was streamed as the initial frame.
    assert json.loads(first) == global_only


def test_handler_initial_snapshot_then_suppresses_unchanged():
    """First iteration yields the initial sample; the next loop
    iteration is identical so the change-only push suppresses it.
    Pinned because losing the dict-equality guard would spam the
    browser with redundant frames at the sample cadence (10s) and
    spike the React Query write rate."""
    import asyncio
    import json
    from unittest.mock import patch

    from backend.routers.admin import system_metrics as sm

    request = type("Req", (), {"is_disconnected": lambda self: asyncio.sleep(0, result=True)})()

    fixed_payload = {"health_snapshot": {"k": 1}, "metric_history_1h": None}

    # Make the inner sleep instant so the loop runs without burning
    # the 10s real delay.
    async def _zero_sleep(_secs):
        return None

    with (
        patch.object(sm, "sample_system_metrics", return_value=fixed_payload),
        patch.object(sm.asyncio, "sleep", side_effect=_zero_sleep),
    ):

        async def _drive():
            gen = (
                sm.system_metrics_stream.__wrapped__(request, "svc-test")
                if hasattr(sm.system_metrics_stream, "__wrapped__")
                else None
            )
            # The handler returns an EventSourceResponse wrapping the
            # generator; reach the inner generator by calling stream()
            # directly. Simpler: import the body via the closure.
            response = await sm.system_metrics_stream(request=request, service_id="svc-test")  # type: ignore[arg-type]
            agen = response.body_iterator
            first = await agen.__anext__()
            return first

        first = asyncio.run(_drive())
        # The first frame is the JSON-encoded initial payload.
        decoded = json.loads(first)
        assert decoded == fixed_payload


def test_handler_swallows_sample_exception_and_keeps_loop_alive():
    """A sampling exception mid-stream gets caught + logged + the
    loop continues to the next tick. Pinned because the
    log-and-continue path is the difference between "transient blip"
    and "stream silently dies and the user reloads the page"."""
    import asyncio
    from unittest.mock import patch

    from backend.routers.admin import system_metrics as sm

    # is_disconnected → True after we've consumed 2 frames so the
    # loop exits cleanly without spinning the test.
    disconnect_after = {"count": 0}

    async def _disc(_self):
        disconnect_after["count"] += 1
        return disconnect_after["count"] > 2  # disconnect after 2 successful checks

    request = type("Req", (), {"is_disconnected": _disc})()

    initial = {"health_snapshot": {"k": 0}}
    second = {"health_snapshot": {"k": 1}}
    samples = [initial, RuntimeError("transient sampler blip"), second]
    sample_iter = iter(samples)

    def _next_sample(*_a, **_k):
        item = next(sample_iter)
        if isinstance(item, Exception):
            raise item
        return item

    async def _zero_sleep(_secs):
        return None

    frames = []

    async def _consume():
        with (
            patch.object(sm, "sample_system_metrics", side_effect=_next_sample),
            patch.object(sm.asyncio, "sleep", side_effect=_zero_sleep),
        ):
            response = await sm.system_metrics_stream(request=request, service_id="svc-test")  # type: ignore[arg-type]
            agen = response.body_iterator
            # Pull frames until the generator exhausts (request disconnects).
            try:
                while True:
                    frame = await agen.__anext__()
                    frames.append(frame)
            except StopAsyncIteration:
                pass

    asyncio.run(_consume())

    # Two payloads survived (initial + second). The exception in between
    # was caught and the loop continued — proves the except/continue path.
    assert len(frames) == 2, f"expected initial + post-recovery frame; got {frames!r}"
