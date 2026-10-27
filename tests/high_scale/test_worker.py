from __future__ import annotations

import logging
from collections import Counter
from dataclasses import dataclass, field
from threading import Event
from unittest.mock import MagicMock, patch

import pytest
from backend.core.clickhouse_schema import maintain_clickhouse_system_tables
from backend.high_scale.archive_models import HighScaleConfig, TopologyProfile, from_environment
from backend.high_scale.orchestration import PageRun

from backend.high_scale import worker
from backend.high_scale.worker import (
    FairPageSelector,
    HighScaleWorkerLoop,
    _MultiServiceWorkerLoop,
    _weights_from_env,
)


@dataclass
class _Coordinator:
    calls: list[tuple[str, str]]

    def run_page(self, *, service_id: str, domain: str) -> PageRun:
        self.calls.append((service_id, domain))
        return PageRun(None, None, 1, 1, 0, 0)


def test_worker_loop_processes_each_configured_service_domain_once() -> None:
    coordinator = _Coordinator([])
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc-a", "svc-b"),
        domains=("request", "rum"),
        interval_seconds=0,
    )

    results = loop.run_once()

    assert coordinator.calls == [
        ("svc-a", "request"),
        ("svc-a", "rum"),
        ("svc-b", "request"),
        ("svc-b", "rum"),
    ]
    assert [(result.service_id, result.domain, result.page.processed) for result in results] == [
        ("svc-a", "request", 1),
        ("svc-a", "rum", 1),
        ("svc-b", "request", 1),
        ("svc-b", "rum", 1),
    ]


def test_worker_loop_stops_after_requested_iterations() -> None:
    coordinator = _Coordinator([])
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc",),
        domains=("request",),
        interval_seconds=0,
    )

    loop.run(max_iterations=2)

    assert coordinator.calls == [("svc", "request"), ("svc", "request")]


class _BlockingSweeper:
    def __init__(self) -> None:
        self.started = Event()
        self.release = Event()
        self.calls = 0

    def sweep(self, *, service_id: str, limit: int) -> None:
        self.calls += 1
        self.started.set()
        self.release.wait(timeout=5)


def test_slow_deletion_sweep_does_not_block_ingest_pages() -> None:
    coordinator = _Coordinator([])
    sweeper = _BlockingSweeper()
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc",),
        domains=("request",),
        interval_seconds=0,
        deletion_sweeper=sweeper,  # type: ignore[arg-type]
    )

    loop.run_once()
    assert sweeper.started.wait(timeout=2)
    loop.run_once()
    loop.run_once()

    assert coordinator.calls == [("svc", "request")] * 3
    assert sweeper.calls == 1
    sweeper.release.set()
    loop.wait_for_deletion_sweep(timeout=2)
    loop.run_once()
    loop.wait_for_deletion_sweep(timeout=2)
    assert sweeper.calls == 2


def test_worker_loop_with_interval_sleeps_and_stops() -> None:
    coordinator = _Coordinator([])
    sleeps: list[float] = []
    loop = HighScaleWorkerLoop(
        coordinator=coordinator,  # type: ignore[arg-type]
        service_ids=("svc",),
        domains=("request",),
        interval_seconds=2.0,
        sleeper=sleeps.append,
    )

    loop.run(max_iterations=2)

    assert coordinator.calls == [("svc", "request"), ("svc", "request")]
    assert len(sleeps) == 1
    assert 1.0 <= sleeps[0] <= 2.0


def test_multi_service_worker_loop_with_interval_sleeps_and_stops() -> None:
    coord_a = _Coordinator([])
    coord_b = _Coordinator([])
    sleeps: list[float] = []
    loop = _MultiServiceWorkerLoop(
        coordinators=(("svc-a", coord_a), ("svc-b", coord_b)),  # type: ignore[arg-type]
        domains=("request",),
        interval_seconds=2.0,
    )
    loop._sleeper = sleeps.append

    loop.run(max_iterations=2)

    assert coord_a.calls == [("svc-a", "request"), ("svc-a", "request")]
    assert coord_b.calls == [("svc-b", "request"), ("svc-b", "request")]
    assert len(sleeps) == 1
    assert 1.0 <= sleeps[0] <= 2.0


def _config(profile: str) -> HighScaleConfig:
    return HighScaleConfig(
        enabled=True,
        clickhouse_url="http://clickhouse",
        fos_bucket="archive",
        archive_manifest_dsn="postgresql://manifest",
        worker_queue_url="redis://queue",
        profile=profile,
    )


def test_config_resolves_its_profile_through_the_topology_module() -> None:
    topology = _config("portable-production").topology_profile()

    assert (topology.name, topology.clickhouse_replicas_per_shard, topology.keeper_members) == (
        "portable-production",
        2,
        3,
    )
    assert topology.production_ha is True


def test_config_validation_rejects_unknown_profiles_via_the_topology_module() -> None:
    with pytest.raises(ValueError, match="local or portable-production"):
        _config("elevation-magic").validate()


def test_config_validation_accepts_every_profile_the_topology_module_builds() -> None:
    _config("local").validate()
    _config("portable-production").validate()


def test_validation_follows_the_topology_module_not_a_duplicated_name_list() -> None:
    sentinel = TopologyProfile("sentinel", 1, 1, True, False, 1)
    with patch("backend.high_scale.models.build_topology_profile", return_value=sentinel):
        assert _config("anything").topology_profile() is sentinel
        _config("anything").validate()


def test_from_environment_applies_the_profile_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in {
        "HIGH_SCALE_ENABLED": "1",
        "HIGH_SCALE_CLICKHOUSE_URL": "http://clickhouse",
        "HIGH_SCALE_FOS_BUCKET": "archive",
        "HIGH_SCALE_ARCHIVE_MANIFEST_DSN": "postgresql://manifest",
        "HIGH_SCALE_WORKER_QUEUE_URL": "redis://queue",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("HIGH_SCALE_PROFILE", "portable-production")
    assert from_environment().topology_profile().production_ha is True

    monkeypatch.setenv("HIGH_SCALE_PROFILE", "nope")
    with pytest.raises(ValueError, match="local or portable-production"):
        from_environment()


def test_worker_startup_logs_the_resolved_topology(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(worker, "from_environment", lambda: _config("portable-production"))
    monkeypatch.delenv("HIGH_SCALE_SERVICE_IDS", raising=False)
    monkeypatch.setattr(worker.app_config, "list_configs", lambda: [])

    with caplog.at_level(logging.INFO, logger="backend.high_scale.worker"):
        with pytest.raises(ValueError, match="HIGH_SCALE_SERVICE_IDS is required"):
            worker.build_worker_from_environment()

    record = next(r for r in caplog.records if r.getMessage() == "high_scale.topology_profile")
    assert (record.profile, record.keeper_members, record.production_ha) == ("portable-production", 3, True)  # type: ignore[attr-defined]


def test_worker_startup_rejects_an_unknown_profile_before_any_work(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(worker, "from_environment", lambda: _config("bogus"))

    with pytest.raises(ValueError, match="local or portable-production"):
        worker.build_worker_from_environment()


def test_health_snapshot_reports_the_configured_topology(client, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_PROFILE", "portable-production")
    with patch("backend.config.is_high_scale_mode", return_value=True):
        body = client.get("/api/admin/health-snapshot").json()

    assert body["high_scale"]["topology"] == {
        "name": "portable-production",
        "clickhouse_replicas_per_shard": 2,
        "keeper_members": 3,
        "persistent_serving_storage": True,
        "production_ha": True,
        "shard_count": 3,
    }


def test_health_snapshot_degrades_to_no_topology_for_an_unknown_profile(
    client, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HIGH_SCALE_PROFILE", "bogus")
    with patch("backend.config.is_high_scale_mode", return_value=True):
        resp = client.get("/api/admin/health-snapshot")

    assert resp.status_code == 200
    assert resp.json()["high_scale"]["topology"] is None


def test_maintain_clickhouse_system_tables_suppresses_http_413() -> None:
    client = MagicMock()
    client.execute.side_effect = RuntimeError("ClickHouse HTTP 413: Request Entity Too Large")

    maintain_clickhouse_system_tables(client)
    assert client.execute.called


def test_worker_run_once_continues_when_maintenance_fails(caplog: pytest.LogCaptureFixture) -> None:
    coordinator = MagicMock()
    coordinator.run_page.return_value = MagicMock()

    worker_loop = HighScaleWorkerLoop(
        coordinator=coordinator,
        service_ids=("svc_test",),
        domains=("request",),
        interval_seconds=0.0,
    )

    worker_loop._last_system_cleanup = 0.0

    mock_client = MagicMock()
    mock_client.execute.side_effect = RuntimeError("HTTP 413")

    with (
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=mock_client),
        patch("backend.utils.telemetry.run_usage_log_cleanup", return_value=None),
        caplog.at_level(logging.DEBUG),
    ):
        results = worker_loop.run_once()

    assert len(results) == 1
    assert results[0].error is None
    assert coordinator.run_page.called


@dataclass
class _Coordinator_batch_ingest:
    calls: list[tuple[str, str]] = field(default_factory=list)

    def run_page(self, *, service_id: str, domain: str) -> PageRun:
        self.calls.append((service_id, domain))
        return PageRun(None, None, 1, 1, 0, 0)


def _loop(services, domains, **kwargs):
    coordinator = _Coordinator_batch_ingest()
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
    class _Flaky(_Coordinator_batch_ingest):
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
    coordinators = {"a": _Coordinator_batch_ingest(), "b": _Coordinator_batch_ingest()}
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
