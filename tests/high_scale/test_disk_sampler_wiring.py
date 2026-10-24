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
    return metric_snapshots.get_batch(since=datetime.now(UTC) - timedelta(hours=1)).get(metric, [])


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


def test_mode_transitions_are_logged_once_each(monkeypatch) -> None:
    # Record through the module logger directly: structlog's capture_logs() relies on the global
    # processor chain, which loggers cached by earlier tests in the same process bypass, and the
    # module keeps the last snapshot in process state that must start empty for a None->mode edge.
    events: list[tuple[str, dict]] = []

    class _Recorder:
        def info(self, event: str, **kw) -> None:
            events.append(("info", {"event": event, **kw}))

        def warning(self, event: str, **kw) -> None:
            events.append(("warning", {"event": event, **kw}))

    monkeypatch.setattr(disk_sampler, "logger", _Recorder())
    monkeypatch.setattr(disk_sampler, "_latest", None)

    disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
    disk_sampler.sample_disk_state(_FakeClickHouse(0.50))
    disk_sampler.sample_disk_state(_FakeClickHouse(0.08))

    changes = [(level, e) for level, e in events if e["event"] == "high_scale.disk_mode_changed"]
    assert [(e["previous_mode"], e["mode"]) for _, e in changes] == [(None, "normal"), ("normal", "emergency")]
    assert changes[-1][0] == "warning"


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

    assert body["high_scale"]["disk"] is None


def test_health_snapshot_omits_the_high_scale_key_in_standard_mode(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=False):
        body = client.get("/api/admin/health-snapshot").json()

    assert "high_scale" not in body


# ── Merged from test_disk_guard.py ──

import pytest

from backend.high_scale.disk_guard import VolumeStats, evaluate_disk_state


def volume_stats(*, free_fraction: float, total_bytes: int = 1_000_000_000, one_batch_bytes: int = 0) -> VolumeStats:
    return VolumeStats(
        total_bytes=total_bytes,
        free_bytes=int(total_bytes * free_fraction),
        one_batch_bytes=one_batch_bytes,
    )


def test_disk_guard_enters_emergency_mode_at_ten_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.10)).mode == "emergency"


def test_disk_guard_enters_emergency_below_ten_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.05)).mode == "emergency"


def test_disk_guard_is_protective_between_ten_and_fifteen_percent() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.12)).mode == "protective"


def test_disk_guard_warns_below_the_twenty_percent_reserve() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.18)).mode == "warning"


def test_disk_guard_is_normal_with_ample_free_space() -> None:
    assert evaluate_disk_state(volume_stats(free_fraction=0.50)).mode == "normal"


def test_disk_guard_reserve_floor_is_the_greater_of_twenty_percent_or_one_batch() -> None:
    # 5% free (50MB) is well above a tiny one-batch size, but still below
    # the 20% floor, so the batch-size floor does not mask a real warning.
    state = evaluate_disk_state(volume_stats(free_fraction=0.19, total_bytes=1_000_000_000, one_batch_bytes=1_000_000))
    assert state.mode == "warning"
    assert state.reserve_bytes == 200_000_000


def test_disk_guard_reserve_floor_uses_batch_size_when_larger_than_twenty_percent() -> None:
    state = evaluate_disk_state(
        volume_stats(free_fraction=0.50, total_bytes=1_000_000_000, one_batch_bytes=900_000_000)
    )
    assert state.reserve_bytes == 900_000_000
    assert state.mode == "warning"


def test_disk_guard_rejects_free_bytes_above_total() -> None:
    with pytest.raises(ValueError, match="free_bytes"):
        VolumeStats(total_bytes=100, free_bytes=200, one_batch_bytes=0)


# ── Merged from test_retention.py ──

from backend.high_scale.retention import ServiceProfile, StorageProfile, recommend_retention


def profile(rate: float, *, average_row_bytes: float = 500.0) -> ServiceProfile:
    return ServiceProfile(events_per_second=rate, average_row_bytes=average_row_bytes)


def storage(total_bytes: int) -> StorageProfile:
    return StorageProfile(total_bytes=total_bytes)


def test_high_volume_service_gets_shorter_hot_recommendation() -> None:
    low = recommend_retention(profile(1_000), storage(1 << 40))
    high = recommend_retention(profile(1_000_000), storage(1 << 40))

    assert low.hot_days > high.hot_days


def test_low_volume_service_hits_the_baseline_hot_target() -> None:
    recommendation = recommend_retention(profile(1), storage(1 << 40))

    assert recommendation.hot_days == 1.0


def test_warm_recommendation_never_exceeds_the_baseline_target() -> None:
    recommendation = recommend_retention(profile(1), storage(1 << 50))

    assert recommendation.warm_days == 30.0


def test_zero_rate_service_is_rejected() -> None:
    import pytest

    with pytest.raises(ValueError, match="events_per_second"):
        recommend_retention(profile(0), storage(1 << 40))


def test_recommendation_never_exceeds_the_protected_reserve() -> None:
    recommendation = recommend_retention(profile(1_000_000), storage(1 << 30))

    assert recommendation.hot_days >= 0.0
    assert recommendation.warm_days >= recommendation.hot_days


# ── Merged from test_retention_endpoint.py ──


import pytest

from backend.utils.remote_access import _is_blocked_path

_PATH = "/api/admin/high-scale/retention-recommendation"


def _cfg(retention_days: int | None = None) -> dict:
    cfg: dict = {"service_id": "svc-hs", "provisioning": {"cron_sync": {}}}
    if retention_days is not None:
        cfg["provisioning"]["cron_sync"]["data_retention_days"] = retention_days
    return cfg


def _get(client, cfg, **params):
    query = {"service_id": "svc-hs", "events_per_second": 1000, **params}
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=cfg),
    ):
        return client.get(_PATH, params=query)


def test_recommendation_matches_the_pure_function_for_the_request(client) -> None:
    resp = _get(client, _cfg(), total_bytes=_TB, average_row_bytes=800)

    assert resp.status_code == 200
    body = resp.json()
    expected = recommend_retention(ServiceProfile(1000, 800), StorageProfile(_TB))
    assert body["recommended_hot_days"] == pytest.approx(expected.hot_days)
    assert body["recommended_warm_days"] == pytest.approx(expected.warm_days)
    assert body["reasoning"] == expected.reasoning
    assert body["volume_total_source"] == "request"
    assert body["source"] == "recommendation"
    assert body["effective_retention_days"] == pytest.approx(expected.warm_days)


def test_explicit_per_service_retention_always_wins(client) -> None:
    body = _get(client, _cfg(retention_days=7), total_bytes=_TB).json()

    assert body["source"] == "override"
    assert body["explicit_retention_days"] == 7
    assert body["effective_retention_days"] == 7.0
    # The advisory numbers are still reported alongside the override.
    assert body["recommended_warm_days"] > 0


def test_explicit_keep_forever_is_an_override_not_a_recommendation(client) -> None:
    body = _get(client, _cfg(retention_days=0), total_bytes=_TB).json()

    assert body["source"] == "override"
    assert body["explicit_retention_days"] == 0
    assert body["effective_retention_days"] is None


def test_volume_defaults_to_the_sampled_serving_volume(client) -> None:
    class _Probe:
        def storage_health(self) -> dict:
            return {"disk_total": 2 * _TB, "disk_free": _TB}

    disk_sampler.sample_disk_state(_Probe())

    body = _get(client, _cfg()).json()

    assert body["volume_total_bytes"] == 2 * _TB
    assert body["volume_total_source"] == "sampled"


def test_unknown_volume_is_a_409_not_a_guess(client) -> None:
    resp = _get(client, _cfg())

    assert resp.status_code == 409
    assert resp.json()["detail"]["error"] == "high_scale_volume_unknown"


def test_endpoint_is_unavailable_outside_high_scale_mode(client) -> None:
    with patch("backend.config.is_high_scale_mode", return_value=False):
        resp = client.get(_PATH, params={"service_id": "svc-hs", "events_per_second": 1000, "total_bytes": _TB})

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "high_scale_not_enabled"


def test_unknown_service_is_a_404(client) -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=None),
    ):
        resp = client.get(_PATH, params={"service_id": "nope", "events_per_second": 1, "total_bytes": _TB})

    assert resp.status_code == 404
    assert resp.json()["detail"]["error"] == "service_not_found"


def test_non_positive_rate_is_rejected(client) -> None:
    resp = _get(client, _cfg(), total_bytes=_TB, events_per_second=0)

    assert resp.status_code == 422


def test_recommendation_is_logged_with_its_inputs(client) -> None:
    with capture_logs() as logs:
        _get(client, _cfg(retention_days=14), total_bytes=_TB)

    event = next(e for e in logs if e["event"] == "high_scale.retention_recommended")
    assert event["service_id"] == "svc-hs"
    assert event["source"] == "override"
    assert event["explicit_retention_days"] == 14


def test_endpoint_is_analyst_blocked_by_the_admin_prefix() -> None:
    assert _is_blocked_path(_PATH) is True


def test_recommendation_never_writes_service_config(client) -> None:
    with (
        patch("backend.config.is_high_scale_mode", return_value=True),
        patch("backend.config.load_config", return_value=_cfg()),
        patch("backend.config.save_config") as save,
    ):
        resp = client.get(_PATH, params={"service_id": "svc-hs", "events_per_second": 1000, "total_bytes": _TB})

    assert resp.status_code == 200
    save.assert_not_called()
