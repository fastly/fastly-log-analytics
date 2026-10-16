"""Control Room typed response models keep the handlers' wire payloads."""

from __future__ import annotations

from tests.conftest import MOCK_SERVICE_ID


def _body(resp):
    return {k: v for k, v in resp.json().items() if not k.startswith("_")}


def test_control_room_tabs_match_stub_payloads(client):
    from backend.routers.control_room import TAB_STUBS

    for tab, stub in TAB_STUBS.items():
        resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/control-room/{tab}")
        assert resp.status_code == 200, tab
        assert _body(resp) == stub, tab


def test_control_room_models_roundtrip_producer_shapes():
    from backend.models.control_room import (
        CorrelateResponse,
        LogFieldAuditResponse,
        RealtimeSeedResponse,
        WizardStateResponse,
        WizardStepResponse,
    )

    cases = [
        (RealtimeSeedResponse, {"ticks": [{"ts": 1, "rps": 2.5, "nested": {"a": [1]}}]}),
        (
            LogFieldAuditResponse,
            {
                "service_id": "svc",
                "enabled_fields": ["ip", "url"],
                "features": [
                    {"name": "Insights", "required_fields": ["ip"], "status": "ok", "missing_fields": []},
                ],
            },
        ),
        (
            CorrelateResponse,
            {
                "dimension": "country",
                "top": [{"value": "US", "count": 3}],
                "freshness": {"latest_log_at": None, "lag_seconds": 0},
            },
        ),
        (WizardStateResponse, {"service_id": "svc", "completed_steps": ["a"], "is_complete": False}),
        (WizardStepResponse, {"recorded": True, "step": "a"}),
    ]
    for model, payload in cases:
        assert model.model_validate(payload).model_dump(mode="json") == payload, model.__name__
