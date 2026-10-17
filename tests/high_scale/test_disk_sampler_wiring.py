"""Disk guard wiring: 60 s sampler -> evaluate_disk_state -> health snapshot."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from structlog.testing import capture_logs

from backend.core import metric_snapshots
from backend.cron.jobs.metric_snapshot import _run_metric_snapshot, _sample_high_scale_disk
from backend.high_scale import disk_sampler

_TB = 1 << 40


class _FakeClickHouse:
    def __init__(self, free_fraction: float, total: int = _TB) -> None:
        self.total = total
        self.free = int(total * free_fraction)
        self.calls = 0

    def storage_health(self) -> dict:
        self.calls += 1
        return {"up": 1, "disk_free": self.free, "disk_total": self.total}


@pytest.fixture(autouse=True)
def _reset_sampler():
    disk_sampler.reset_for_tests()
    yield
    disk_sampler.reset_for_tests()


def _history(metric: str):
    return metric_snapshots.get_history(metric, since=datetime.now(UTC) - timedelta(hours=1))


def test_sampler_reads_real_volume_stats_and_evaluates_mode() -> None:
    probe = _FakeClickHouse(free_fraction=0.12)

    snapshot = disk_sampler.sample_disk_state(probe)

    assert probe.calls == 1
    assert snapshot.state.mode == "protective"
    assert snapshot.total_bytes == _TB
    assert disk_sampler.latest_disk_snapshot() is snapshot


def test_one_batch_bytes_raises_the_reserve_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("HIGH_SCALE_DISK_ONE_BATCH_BYTES", str(_TB))

    snapshot = disk_sampler.sample_disk_state(_FakeClickHouse(free_fraction=0.5))

    assert snapshot.state.reserve_bytes == _TB
    assert snapshot.state.mode == "warning"


def test_mode_transitions_are_logged_once_each() -> None:
    with capture_logs() as logs:
        disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
        disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
        disk_sampler.sample_disk_state(_FakeClickHouse(0.08))

    changes = [e for e in logs if e["event"] == "high_scale.disk_mode_changed"]
    assert [(e["previous_mode"], e["mode"]) for e in changes] == [(None, "normal"), ("normal", "emergency")]
    assert changes[-1]["log_level"] == "warning"


def test_metric_snapshot_tick_samples_disk_in_high_scale_mode() -> None:
    probe = _FakeClickHouse(free_fraction=0.12)
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=probe),
    ):
        _run_metric_snapshot()

    assert probe.calls == 1
    assert _history("high_scale_disk_mode_level")[-1]["value"] == 2.0
    assert _history("high_scale_disk_free_fraction")[-1]["value"] == pytest.approx(0.12, abs=1e-3)


def test_metric_snapshot_tick_never_touches_clickhouse_in_standard_mode() -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=False),
        patch("backend.core.clickhouse_client.get_clickhouse_client") as get_client,
    ):
        assert _sample_high_scale_disk() == 0
        _run_metric_snapshot()

    get_client.assert_not_called()
    assert disk_sampler.latest_disk_snapshot() is None
    assert _history("high_scale_disk_mode_level") == []


def test_probe_failure_is_logged_and_never_breaks_the_tick() -> None:
    class _Down:
        def storage_health(self) -> dict:
            raise RuntimeError("clickhouse unavailable")

    with (
        capture_logs() as logs,
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=_Down()),
    ):
        assert _sample_high_scale_disk() == 0

    assert any(e["event"] == "high_scale.disk_sample_failed" and e["error_kind"] == "RuntimeError" for e in logs)
    assert disk_sampler.latest_disk_snapshot() is None


def test_health_snapshot_surfaces_the_sampled_mode(client) -> None:
    probe = _FakeClickHouse(free_fraction=0.09)
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.core.clickhouse_client.get_clickhouse_client", return_value=probe),
    ):
        _run_metric_snapshot()
        resp = client.get("/api/admin/health-snapshot")

    assert resp.status_code == 200
    disk = resp.json()["high_scale"]["disk"]
    assert disk["mode"] == "emergency"
    assert disk["free_fraction"] == pytest.approx(0.09, abs=1e-3)
    assert disk["total_bytes"] == _TB
    assert disk["sample_age_s"] >= 0


def test_health_snapshot_high_scale_block_is_empty_before_the_first_sample(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=True):
        body = client.get("/api/admin/health-snapshot").json()

    assert body["high_scale"] == {"disk": None}


def test_health_snapshot_omits_the_high_scale_key_in_standard_mode(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=False):
        body = client.get("/api/admin/health-snapshot").json()

    assert "high_scale" not in body
