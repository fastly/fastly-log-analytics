from __future__ import annotations

import pytest

from backend.high_scale import dashboard, security
from backend.models.dashboard import AggregatesRequest
from tests.high_scale import test_cmcd as cmcd_cases
from tests.high_scale import test_dashboard as dashboard_cases
from tests.high_scale import test_performance as performance_cases
from tests.high_scale import test_security as security_cases


def test_high_scale_performance_cache_observations_do_not_execute_the_repeat() -> None:
    client = performance_cases._Client()
    first = performance_cases._run(client, sections={"waterfall"})
    count = len(client.calls)
    assert count > 0
    assert first.response_cache.state == "miss"
    hit = performance_cases._run(client, sections={"waterfall"})
    assert hit.response_cache.state == "hit"
    assert hit.is_cached is True
    assert len(client.calls) == count


def test_high_scale_security_cache_observations_do_not_execute_the_repeat() -> None:
    client = security_cases._Client()
    service = security_cases._service(client)
    req = security_cases._req()
    first = security.security_aggregates(
        service, req, security_cases.START, security_cases.END, sections={"ipv6_adoption"}
    )
    count = len(client.calls)
    assert count > 0
    assert first.response_cache.state == "miss"
    hit = security.security_aggregates(
        service, req, security_cases.START, security_cases.END, sections={"ipv6_adoption"}
    )
    assert hit.response_cache.state == "hit"
    assert hit.is_cached is True
    assert len(client.calls) == count


def test_high_scale_cmcd_cache_observations_do_not_execute_the_repeat() -> None:
    client = cmcd_cases._Client()
    first = cmcd_cases._run(client, sections={"overview"})
    count = len(client.calls)
    assert count > 0
    assert first["_response_cache"]["state"] == "miss"
    hit = cmcd_cases._run(client, sections={"overview"})
    assert hit["_response_cache"]["state"] == "hit"
    assert len(client.calls) == count


def test_high_scale_dashboard_cache_observations_survive_the_typed_response() -> None:
    service, client = dashboard_cases._recording_service()
    req = AggregatesRequest(fields=["url"], include_time_series=False)
    first = dashboard.aggregates(service, req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    count = len(client.calls)
    assert count > 0
    assert first.model_dump(by_alias=True)["_response_cache"]["state"] == "miss"
    hit = dashboard.aggregates(service, req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    assert hit.model_dump(by_alias=True)["_response_cache"]["state"] == "hit"
    assert hit.is_cached is True
    assert len(client.calls) == count


@pytest.mark.parametrize("filtered", [False, True])
@pytest.mark.parametrize(
    ("start", "end"),
    [
        ("2026-01-01T00:00:01Z", "2026-01-02T00:00:00Z"),
        ("2026-01-01T00:00:00Z", "2026-01-02T00:00:01Z"),
        ("2026-01-01T00:00:00Z", "2026-01-02T00:00:00.001Z"),
    ],
)
def test_dashboard_cache_does_not_alias_different_event_windows(filtered: bool, start: str, end: str) -> None:
    service, client = dashboard_cases._recording_service()
    req = AggregatesRequest(
        fields=["url"],
        include_time_series=False,
        include_map_data=False,
        filters={"country": {"mode": "include", "values": ["US"]}} if filtered else {},
    )
    dashboard.aggregates(service, req, "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z")
    previous_calls = len(client.calls)

    changed = dashboard.aggregates(service, req, start, end)

    assert changed.response_cache.state == "miss"
    assert len(client.calls) > previous_calls
    changed_calls = len(client.calls)
    repeat = dashboard.aggregates(service, req, start, end)
    assert repeat.response_cache.state == "hit"
    assert len(client.calls) == changed_calls
