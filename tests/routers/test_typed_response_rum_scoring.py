"""RUM and scoring typed response models keep the handlers' wire payloads."""

from __future__ import annotations

import json

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
                "vitals": {
                    "lcp": {"p75": None, "distribution": None},
                    "cls": {"p75": None, "distribution": None},
                    "inp": {"p75": None, "distribution": None},
                    "fid": {"p75": None, "fcp": None, "ttfb": None},
                },
                "worst_pages": [],
                "errors": [],
                "trends": {"timestamps": [], "lcp": [], "cls": [], "error_rate": []},
                "environments": {"browsers": {}, "os": {}, "devices": {}},
                "_debug_queries": [],
            },
        ),
        (
            "RumAnalyticsResponse",
            {
                "is_mock": False,
                "no_data": False,
                "beacon_count": 12,
                "pageview_count": 10,
                "interaction_count": 1,
                "error_count": 1,
                "vitals": {
                    "lcp": {"p75": 2.5, "distribution": {"good": 80, "needs_improvement": 15, "poor": 5}},
                    "cls": {"p75": 0.1, "distribution": {"good": 90, "needs_improvement": 10, "poor": 0}},
                    "inp": {"p75": 180, "distribution": {"good": 70, "needs_improvement": 20, "poor": 10}},
                    "fid": {"p75": 3.0, "fcp": 1.2, "ttfb": 0.4},
                },
                "worst_pages": [
                    {"path": "/a", "views": 4, "avg_load_time": 1, "lcp_p75": None, "cls_p75": 0.05, "error_rate": 0.0}
                ],
                "errors": [{"message": "boom", "file": "a.js", "line": 3, "col": 4, "count": 2}],
                "trends": {
                    "timestamps": ["2026-01-01T00:00:00+00:00"],
                    "lcp": [None],
                    "cls": [0.1],
                    "error_rate": [0.0],
                    "pageviews": [10],
                    "interactions": [1],
                    "errors": [1],
                },
                "environments": {"browsers": {"Chrome": 9}, "os": {"macOS": 9}, "devices": {"desktop": 9}},
            },
        ),
    ],
)
def test_rum_models_roundtrip(model_name, payload):
    from backend.models import rum

    model = getattr(rum, model_name)
    validated = model.model_validate(payload)
    assert validated.model_dump(mode="json") == payload
    # Same bytes, not just equal values: an int must not come back as 5.0.
    assert validated.model_dump_json() == json.dumps(payload, separators=(",", ":"))


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


def test_scoring_status_declares_config_identity_keys():
    from backend.models.session_scoring import ScoringStatusResponse

    payload = {
        "enabled": True,
        "scoring_service_id": "svc",
        "scoring_service_name": "n",
        "scoring_domain": "d.example",
        "matrix_version": "2026-01-01",
        "enabled_at": "2026-01-01T00:00:00+00:00",
    }
    declared = set(ScoringStatusResponse.model_fields)
    assert set(payload) <= declared
    assert ScoringStatusResponse.model_validate(payload).model_dump(mode="json", exclude_unset=True) == payload


def test_scoring_labels_list_matches_producer(client, monkeypatch):
    from backend.scoring import labels as _labels

    rows = [
        {
            "id": "a1",
            "service_id": MOCK_SERVICE_ID,
            "sid": "s1",
            "label": lbl,
            "notes": "n",
            "flagged_by": "admin",
            "sample_ip": "",
            "sample_ua": "",
            "sample_url": "",
            "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00",
        }
        for lbl in ("good", "bad", "neutral")
    ]
    counts = {"good": 1, "bad": 1, "neutral": 1}
    monkeypatch.setattr(_labels, "list_labels", lambda sid, limit=500: rows)
    monkeypatch.setattr(_labels, "counts_by_label", lambda sid: counts)

    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/scoring/labels")
    assert resp.status_code == 200
    assert _body(resp) == {"labels": rows, "counts": counts}
    assert list(resp.json()["counts"]) == ["good", "bad", "neutral"]


def test_scoring_label_row_rejects_unknown_label():
    from pydantic import ValidationError

    from backend.models.session_scoring import ScoringLabelRow

    assert ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "good"}).label == "good"
    with pytest.raises(ValidationError):
        ScoringLabelRow.model_validate({"id": "a", "sid": "s", "label": "maybe"})


def test_scoring_label_literal_matches_allowed_labels():
    import typing

    from backend.models.session_scoring import ScoringLabelRow
    from backend.scoring.labels import ALLOWED_LABELS

    ann = ScoringLabelRow.model_fields["label"].annotation
    assert set(typing.get_args(ann)) == set(ALLOWED_LABELS)
