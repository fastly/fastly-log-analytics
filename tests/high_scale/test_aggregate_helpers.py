"""Unit, snapshot, and contract tests for High-Scale aggregate query helpers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from backend.high_scale import aggregate_query as aq
from backend.high_scale.archive import ServingWatermark


def _wm(
    *,
    service_id: str = "svc-1",
    domain: str = "request",
    coverage_end: datetime | None = datetime(2026, 11, 18, 12, 0, tzinfo=UTC),
) -> ServingWatermark:
    return ServingWatermark(
        service_id=service_id,
        domain=domain,
        owner_epoch=1,
        coverage_start=datetime(2026, 11, 17, 12, 0, tzinfo=UTC),
        coverage_end=coverage_end,
        last_accepted_cursor="cur-1",
        last_archived_event_id="evt-1",
        last_visible_event_id="evt-1",
        exact=True,
    )


def test_aggregate_request_validate_edge_cases() -> None:
    t0 = datetime(2026, 11, 18, 10, 0, tzinfo=UTC)
    t1 = datetime(2026, 11, 18, 11, 0, tzinfo=UTC)

    aq.AggregateRequest(service_id="svc-1", domain="request", start=t0, end=t1).validate()

    with pytest.raises(ValueError, match="service id is required"):
        aq.AggregateRequest(service_id="  ", domain="request").validate()

    with pytest.raises(ValueError, match="unsupported aggregate domain"):
        aq.AggregateRequest(service_id="svc-1", domain="unknown").validate()

    with pytest.raises(ValueError, match="both start and end"):
        aq.AggregateRequest(service_id="svc-1", domain="request", start=t0, end=None).validate()

    with pytest.raises(ValueError, match="inverted"):
        aq.AggregateRequest(service_id="svc-1", domain="request", start=t1, end=t0).validate()


def test_aggregate_response_snapshot_and_freshness_clamping() -> None:
    wm = _wm(coverage_end=datetime(2026, 11, 18, 12, 0, tzinfo=UTC))
    resp = aq._aggregate_response(
        service_id="svc-1",
        domain="request",
        request_count=420,
        top_values=(("/api/v1", 300), ("/health", 120)),
        watermark=wm,
        now=datetime(2026, 11, 18, 12, 1, 30, tzinfo=UTC),
    )
    assert resp == aq.AggregateResponse(
        service_id="svc-1",
        domain="request",
        request_count=420,
        top_values=(("/api/v1", 300), ("/health", 120)),
        coverage=1.0,
        watermark=wm,
        freshness_lag_seconds=90.0,
        exact=True,
        approximation_error=None,
        counters=(("request_events", 420),),
    )

    # Observed before coverage_end clamps freshness_lag_seconds to 0.0
    ahead = aq._aggregate_response(
        service_id="svc-1",
        domain="cmcd",
        request_count=0,
        top_values=(),
        watermark=_wm(domain="cmcd", coverage_end=datetime(2026, 11, 18, 12, 5, tzinfo=UTC)),
        now=datetime(2026, 11, 18, 12, 0, tzinfo=UTC),
    )
    assert ahead.freshness_lag_seconds == 0.0
    assert ahead.coverage == 1.0
    assert ahead.counters == (("cmcd_events", 0),)


def test_dimension_query_params_strips_stale_dim_keys() -> None:
    placeholders, params = aq._dimension_query_params(
        {"service_id": "svc-1", "dim_0": "stale", "dim_99": "stale", "start": "x"},
        ("url", "country"),
    )
    assert placeholders == ["{dim_0:String}", "{dim_1:String}"]
    assert params == {"service_id": "svc-1", "start": "x", "dim_0": "url", "dim_1": "country"}


def test_partial_minute_window_and_require_window_bounds() -> None:
    t0 = datetime(2026, 11, 18, 12, 0, 0, tzinfo=UTC)
    t1 = datetime(2026, 11, 18, 12, 5, 0, tzinfo=UTC)
    assert aq._is_partial_minute_window(None, t1) is False
    assert aq._is_partial_minute_window(t0, t1) is False
    assert aq._is_partial_minute_window(t0.replace(second=15), t1) is True
    assert aq._is_partial_minute_window(t0, t1.replace(microsecond=1)) is True

    assert aq._require_window_bounds(t0, t1) == (t0, t1)
    with pytest.raises(ValueError, match="both range bounds"):
        aq._require_window_bounds(t0, None)


def test_can_use_hourly_rollup_and_edge_time_clause() -> None:
    client = SimpleNamespace(enable_hourly_rollups=True)
    st = datetime(2026, 11, 17, 10, 15, 0, tzinfo=UTC)
    et = datetime(2026, 11, 18, 12, 30, 0, tzinfo=UTC)

    bounds = aq.can_use_hourly_rollup(client, "request", st, et)
    assert bounds == (
        datetime(2026, 11, 17, 11, 0, 0, tzinfo=UTC),
        datetime(2026, 11, 18, 12, 0, 0, tzinfo=UTC),
    )
    assert aq._hourly_edge_time_clause(st, et, *bounds) == (
        "((bucket_start >= {start:DateTime64(3)} AND bucket_start < {h_start:DateTime64(3)}) "
        "OR (bucket_start >= {h_end:DateTime64(3)} AND bucket_start < {end:DateTime64(3)}))"
    )

    # Exact hour-aligned boundaries produce no edge clause
    exact_st = datetime(2026, 11, 17, 11, 0, 0, tzinfo=UTC)
    exact_et = datetime(2026, 11, 18, 12, 0, 0, tzinfo=UTC)
    exact_bounds = aq.can_use_hourly_rollup(client, "request", exact_st, exact_et)
    assert exact_bounds == (exact_st, exact_et)
    assert aq._hourly_edge_time_clause(exact_st, exact_et, *exact_bounds) is None

    # Disqualified cases: non-request domain, <24h, partial minute, disabled client
    assert aq.can_use_hourly_rollup(client, "cmcd", st, et) is None
    assert aq.can_use_hourly_rollup(client, "request", st, st + timedelta(hours=23)) is None
    assert aq.can_use_hourly_rollup(client, "request", st.replace(second=1), et) is None
    assert aq.can_use_hourly_rollup(SimpleNamespace(enable_hourly_rollups=False), "request", st, et) is None
