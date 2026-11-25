"""Unit, snapshot, and contract tests for Insights envelopes and router helpers."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.models.analytics.insights import InsightsResponse
from backend.repositories.utils.response_shaping import build_insights_envelope
from backend.routers import insights as insights_router_mod


def test_build_insights_envelope_snapshot_and_wire_contract() -> None:
    env = build_insights_envelope(
        None,
        window_start="2026-11-18T10:00:00Z",
        window_end="2026-11-18T11:00:00Z",
        baseline_start="2026-11-17T10:00:00Z",
        baseline_end="2026-11-18T10:00:00Z",
        computed_at="",
        window_hours=1.0,
        baseline_hours=24.0,
        window_total_requests=0,
        pending=True,
        refreshing=True,
        refresh_error=None,
    )
    assert env == {
        "insights": [],
        "window_start": "2026-11-18T10:00:00Z",
        "window_end": "2026-11-18T11:00:00Z",
        "baseline_start": "2026-11-17T10:00:00Z",
        "baseline_end": "2026-11-18T10:00:00Z",
        "computed_at": "",
        "window_hours": 1.0,
        "baseline_hours": 24.0,
        "window_total_requests": 0,
        "pending": True,
        "refreshing": True,
        "refresh_error": None,
    }
    validated = InsightsResponse.with_telemetry(**env)
    assert validated.insights == []
    assert validated.pending is True
    assert validated.refreshing is True
    assert validated.computed_at == ""


def test_analyst_lookback_clamp_admin_vs_analyst_session() -> None:
    admin_ctx = SimpleNamespace(analyst_session=None)
    assert insights_router_mod._analyst_lookback_clamp(admin_ctx, 24.0, 1.0) == (None, None, False, None)  # type: ignore[arg-type]

    session = SimpleNamespace(
        query_start_time="2026-11-18T00:00:00Z",
        query_end_time="2026-11-18T12:00:00Z",
        query_window_hours=None,
        pii_policy={"mask_ips": True},
    )
    analyst_ctx = SimpleNamespace(
        analyst_session=session,
        clamp=lambda s, e: ("2026-11-18T00:00:00Z", "2026-11-18T12:00:00Z"),
    )
    clamp_start, clamp_end, mask_ips, cache_key = insights_router_mod._analyst_lookback_clamp(
        analyst_ctx,  # type: ignore[arg-type]
        24.0,
        1.0,
    )
    assert clamp_start == "2026-11-18T00:00:00Z"
    assert clamp_end == "2026-11-18T12:00:00Z"
    assert mask_ips is True
    assert isinstance(cache_key, str) and len(cache_key) > 0


def test_get_insights_sem_keys_strictly_by_service() -> None:
    sem_a = insights_router_mod._get_insights_sem("svc-a", 1.0, 24.0)
    sem_a_same = insights_router_mod._get_insights_sem("svc-a", 1.0, 24.0)
    sem_a_diff = insights_router_mod._get_insights_sem("svc-a", 4.0, 24.0)
    sem_b = insights_router_mod._get_insights_sem("svc-b", 1.0, 24.0)
    assert sem_a is sem_a_same
    assert sem_a is sem_a_diff
    assert sem_a is not sem_b


@pytest.mark.no_postgres
def test_insights_response_preserves_window_total_requests_and_router_honors_end_time() -> None:
    import asyncio
    from unittest.mock import patch

    from backend.models.analytics.insights import InsightsRequest

    env = build_insights_envelope(
        [],
        window_start="2026-11-18T10:00:00Z",
        window_end="2026-11-18T11:00:00Z",
        baseline_start="2026-11-17T10:00:00Z",
        baseline_end="2026-11-18T10:00:00Z",
        computed_at="2026-11-18T11:00:00Z",
        window_hours=1.0,
        baseline_hours=24.0,
        window_total_requests=97583,
    )
    validated = InsightsResponse.with_telemetry(**env)
    assert validated.window_total_requests == 97583
    assert validated.model_dump()["window_total_requests"] == 97583

    req = InsightsRequest(window_size_hrs=6, baseline_hours=24, end_time="2026-11-24T18:00:00Z")
    assert req.end_time == "2026-11-24T18:00:00Z"

    captured: dict[str, object] = {}

    def fake_hs_insights(svc, r, clamp_start, clamp_end, mask_ips, **kwargs):
        captured["clamp_start"] = clamp_start
        captured["clamp_end"] = clamp_end
        return validated

    admin_ctx = SimpleNamespace(service_id="svc-a", analyst_session=None)
    with (
        patch("backend.routers.insights.record_insights_request"),
        patch("backend.routers.insights.require_high_scale", return_value=SimpleNamespace(service_id="svc-a")),
        patch("backend.routers.insights.hs_insights", side_effect=fake_hs_insights),
    ):
        asyncio.run(insights_router_mod.insights_endpoint(req, admin_ctx))  # type: ignore[arg-type]
    assert captured["clamp_start"] is None
    assert captured["clamp_end"] == "2026-11-24T18:00:00Z"
