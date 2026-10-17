"""Topology wiring: config validation, sizing warnings, worker startup, health snapshot."""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

from backend.high_scale import worker
from backend.high_scale.models import HighScaleConfig, HighScaleSizing, TopologyProfile, recommend_high_scale_sizing


def _config(profile: str) -> HighScaleConfig:
    return HighScaleConfig(
        enabled=True,
        clickhouse_url="http://clickhouse",
        fos_bucket="archive",
        archive_manifest_dsn="postgresql://manifest",
        worker_queue_url="redis://queue",
        profile=profile,
    )


def _sizing(profile: str, replicas: int) -> HighScaleSizing:
    return HighScaleSizing(
        service_id="svc",
        request_rate_per_second=10,
        rum_rate_per_second=0,
        row_width_bytes=100,
        enabled_field_count=1,
        bytes_per_enabled_field=0,
        disk_capacity_bytes=10**12,
        profile=profile,
        replicas_per_shard=replicas,
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
    with patch("backend.high_scale.topology.build_topology_profile", return_value=sentinel):
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
    assert HighScaleConfig.from_environment().topology_profile().production_ha is True

    monkeypatch.setenv("HIGH_SCALE_PROFILE", "nope")
    with pytest.raises(ValueError, match="local or portable-production"):
        HighScaleConfig.from_environment()


def test_sizing_warnings_are_derived_from_the_profile() -> None:
    assert recommend_high_scale_sizing(_sizing("local", 1)).warnings == ()
    assert recommend_high_scale_sizing(_sizing("portable-production", 2)).warnings == ()
    assert "does not provide replicated serving storage" in recommend_high_scale_sizing(_sizing("local", 2)).warnings[0]
    assert "at least 2 replicas per shard" in recommend_high_scale_sizing(_sizing("portable-production", 1)).warnings[0]

    stricter = TopologyProfile("portable-production", 3, 3, True, True, 3)
    with patch("backend.high_scale.topology.build_topology_profile", return_value=stricter):
        warnings = recommend_high_scale_sizing(_sizing("portable-production", 2)).warnings
    assert "at least 3 replicas per shard" in warnings[0]


def test_sizing_rejects_an_unknown_profile() -> None:
    with pytest.raises(ValueError, match="local or portable-production"):
        recommend_high_scale_sizing(_sizing("bogus", 1))


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
