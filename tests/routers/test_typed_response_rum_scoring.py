"""RUM and scoring typed response models keep the handlers' wire payloads."""

from __future__ import annotations

import pytest

from tests.conftest import MOCK_SERVICE_ID


def _body(resp):
    return {k: v for k, v in resp.json().items() if not k.startswith("_")}


@pytest.mark.parametrize(
    "model_name,payload",
    [
        ("RumStatusResponse", {"enabled": False, "enabled_at": None}),
        (
            "RumStatusResponse",
            {
                "enabled": True,
                "enabled_at": "2026-01-01T00:00:00+00:00",
                "deployed_vcl_sha": "a",
                "current_vcl_sha": "b",
                "vcl_drift": True,
                "capture_vitals": True,
                "capture_performance": False,
                "capture_errors": True,
                "capture_events": True,
                "custom_condition": "",
            },
        ),
        ("RumSettingsUpdateResponse", {"ok": True, "message": "m", "skipped": False}),
        (
            "RumBeaconHealthResponse",
            {
                "enabled": True,
                "beacon_fire_rate": 3,
                "recent_beacons": 3,
                "last_beacon_time": None,
                "setup_complete": True,
                "message": "x",
                "_debug_queries": [{"sql": "s"}],
                "_debug_sqlite": [],
                "_debug_calls": [],
                "_is_cached": False,
            },
        ),
        (
            "RumAnalyticsResponse",
            {
                "is_mock": False,
                "no_data": True,
                "beacon_count": 0,
                "pageview_count": 0,
                "interaction_count": 0,
                "error_count": 0,
                "message": "Waiting",
                "vitals": {"lcp": {"p75": None, "distribution": None}},
                "worst_pages": [],
                "errors": [],
                "trends": {"timestamps": []},
                "environments": {"browsers": {}},
                "_debug_queries": [],
            },
        ),
    ],
)
def test_rum_models_roundtrip(model_name, payload):
    from backend.models import rum

    model = getattr(rum, model_name)
    assert model.model_validate(payload).model_dump(mode="json") == payload


def test_scoring_status_route_disabled_shape(client, monkeypatch):
    from backend import config as svcconfig

    monkeypatch.setattr(svcconfig, "load_config", lambda sid: {"service_id": sid})
    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")
    assert resp.status_code == 200
    assert _body(resp) == {"enabled": False}


def test_scoring_status_and_config_match_producer(client, monkeypatch):
    from backend import config as svcconfig
    from backend.routers import session_scoring as ss

    cfg = {
        "service_id": MOCK_SERVICE_ID,
        "scoring": {"enabled": True, "request_secret": "s3cret", "custom_key": [1, 2], "deployed_vcl_sha": "v"},
    }
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: cfg)
    expected = ss.scoring_status(MOCK_SERVICE_ID)

    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/status")
    assert resp.status_code == 200
    assert _body(resp) == expected
    assert "request_secret" not in resp.json()

    composite = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/config")
    assert composite.status_code == 200
    assert _body(composite)["status"] == expected


def test_scoring_label_delete_model_roundtrip():
    from backend.models.session_scoring import ScoringLabelDeleteResponse

    payload = {"status": "success", "id": "abc"}
    assert ScoringLabelDeleteResponse.model_validate(payload).model_dump(mode="json") == payload
