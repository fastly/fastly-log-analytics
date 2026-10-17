"""Fairness wiring: the worker loop selects pages through FairScheduler."""

from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field

import pytest

from backend.high_scale.orchestration import PageRun
from backend.high_scale.worker import FairPageSelector, HighScaleWorkerLoop, _MultiServiceWorkerLoop, _weights_from_env


@dataclass
class _Coordinator:
    calls: list[tuple[str, str]] = field(default_factory=list)

    def run_page(self, *, service_id: str, domain: str) -> PageRun:
        self.calls.append((service_id, domain))
        return PageRun(None, None, 1, 1, 0, 0)


def _loop(services, domains, **kwargs):
    coordinator = _Coordinator()
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=services,
        domains=domains,
        interval_seconds=0,
        **kwargs,
    )
    return loop, coordinator


def test_uncapped_loop_keeps_the_original_service_and_domain_order() -> None:
    loop, coordinator = _loop(("svc-z", "svc-a", "svc-m"), ("request", "rum_vitals", "rum_errors"))

    results = loop.run_once()

    expected = [(s, d) for s in ("svc-z", "svc-a", "svc-m") for d in ("request", "rum_vitals", "rum_errors")]
    assert coordinator.calls == expected
    assert [(r.service_id, r.domain) for r in results] == expected


def test_capped_loop_runs_at_most_capacity_pages_per_tick_and_never_drops_work() -> None:
    loop, coordinator = _loop(("hot", "small"), ("d1", "d2", "d3"), pages_per_tick=2)

    per_tick = []
    for _ in range(3):
        before = len(coordinator.calls)
        loop.run_once()
        per_tick.append(coordinator.calls[before:])

    assert all(len(calls) == 2 for calls in per_tick)
    assert set(coordinator.calls) == {(s, d) for s in ("hot", "small") for d in ("d1", "d2", "d3")}


def test_every_service_keeps_minimum_progress_under_a_cap() -> None:
    loop, coordinator = _loop(("hot", "a", "b"), ("d1", "d2", "d3", "d4"), pages_per_tick=3, service_weights={"hot": 5})

    for _ in range(4):
        before = len(coordinator.calls)
        loop.run_once()
        tick = coordinator.calls[before:]
        assert {service for service, _ in tick} >= {"hot", "a", "b"}


def test_weights_let_a_dominant_service_borrow_idle_capacity() -> None:
    # The dominant service sorts LAST so the extra capacity can only reach it
    # through its weight, not through alphabetical dispatch order.
    loop, coordinator = _loop(
        ("asmall", "zhot"), ("d1", "d2", "d3", "d4", "d5", "d6"), pages_per_tick=5, service_weights={"zhot": 2}
    )

    loop.run_once()

    counts = Counter(service for service, _ in coordinator.calls)
    assert counts == {"asmall": 2, "zhot": 3}


def test_deferred_pages_are_logged(caplog: pytest.LogCaptureFixture) -> None:
    loop, _ = _loop(("a", "b"), ("d1", "d2"), pages_per_tick=2)

    with caplog.at_level(logging.INFO, logger="backend.high_scale.worker"):
        loop.run_once()

    record = next(r for r in caplog.records if r.getMessage() == "high_scale.fair_dispatch")
    assert (record.dispatched, record.deferred, record.capacity) == (2, 2, 2)  # type: ignore[attr-defined]


def test_no_fair_dispatch_log_when_everything_runs(caplog: pytest.LogCaptureFixture) -> None:
    loop, _ = _loop(("a", "b"), ("d1",))

    with caplog.at_level(logging.INFO, logger="backend.high_scale.worker"):
        loop.run_once()

    assert not [r for r in caplog.records if r.getMessage() == "high_scale.fair_dispatch"]


def test_page_failures_do_not_stop_the_rest_of_the_selection() -> None:
    class _Flaky(_Coordinator):
        def run_page(self, *, service_id: str, domain: str) -> PageRun:
            if (service_id, domain) == ("a", "d1"):
                raise RuntimeError("boom")
            return super().run_page(service_id=service_id, domain=domain)

    coordinator = _Flaky()
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("a", "b"),
        domains=("d1",),
        interval_seconds=0,
    )

    results = loop.run_once()

    assert [(r.service_id, r.error is not None) for r in results] == [("a", True), ("b", False)]


def test_multi_service_loop_dispatches_each_service_through_its_own_coordinator() -> None:
    coordinators = {"a": _Coordinator(), "b": _Coordinator()}
    loop = _MultiServiceWorkerLoop(
        coordinators=tuple((sid, c) for sid, c in coordinators.items()),  # type: ignore[arg-type]
        domains=("d1", "d2"),
        interval_seconds=0,
        pages_per_tick=2,
    )

    loop.run_once()
    loop.run_once()

    assert coordinators["a"].calls == [("a", "d1"), ("a", "d2")]
    assert coordinators["b"].calls == [("b", "d1"), ("b", "d2")]


def test_capacity_below_the_service_count_is_rejected() -> None:
    with pytest.raises(ValueError, match="at least the number of services"):
        FairPageSelector((("a", "d"), ("b", "d")), capacity=1)


def test_weights_environment_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_SERVICE_WEIGHTS", "svc-a=3, svc-b=1")
    assert _weights_from_env() == {"svc-a": 3, "svc-b": 1}

    monkeypatch.setenv("HIGH_SCALE_SERVICE_WEIGHTS", "svc-a=0")
    with pytest.raises(ValueError, match="positive_integer"):
        _weights_from_env()

    monkeypatch.setenv("HIGH_SCALE_SERVICE_WEIGHTS", "no-equals")
    with pytest.raises(ValueError, match="positive_integer"):
        _weights_from_env()

    monkeypatch.delenv("HIGH_SCALE_SERVICE_WEIGHTS")
    assert _weights_from_env() == {}
