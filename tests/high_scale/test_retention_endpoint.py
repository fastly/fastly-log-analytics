"""Retention recommendation wiring: admin endpoint -> recommend_retention."""

from __future__ import annotations

from unittest.mock import patch

import pytest
from structlog.testing import capture_logs

from backend.high_scale import disk_sampler
from backend.high_scale.retention import ServiceProfile, StorageProfile, recommend_retention
from backend.utils.remote_access import _is_blocked_path

_PATH = "/api/admin/high-scale/retention-recommendation"
_TB = 1 << 40


def _cfg(retention_days: int | None = None) -> dict:
    cfg: dict = {"service_id": "svc-hs", "provisioning": {"cron_sync": {}}}
    if retention_days is not None:
        cfg["provisioning"]["cron_sync"]["data_retention_days"] = retention_days
    return cfg


@pytest.fixture(autouse=True)
def _reset_sampler():
    disk_sampler.reset_for_tests()
    yield
    disk_sampler.reset_for_tests()


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
