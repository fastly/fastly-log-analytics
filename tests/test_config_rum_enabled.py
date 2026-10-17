"""One definition of "RUM enabled": ``rum.enabled`` wins when present and non-None, else root ``rum_enabled``.

This is the semantics the scheduler and the RUM cron jobs always used; status/health surfaces used to OR the
two fields and could report "enabled" for a service whose crons were off.
"""

from __future__ import annotations

import pytest

from backend.config import is_rum_enabled


@pytest.mark.parametrize(
    "cfg,expected",
    [
        (None, False),
        ({}, False),
        ({"rum_enabled": True}, True),
        ({"rum_enabled": False}, False),
        ({"rum": {"enabled": True}}, True),
        ({"rum": {"enabled": False}}, False),
        ({"rum": {"enabled": False}, "rum_enabled": True}, False),
        ({"rum": {"enabled": True}, "rum_enabled": False}, True),
        ({"rum": {"enabled": None}, "rum_enabled": True}, True),
        ({"rum": {}, "rum_enabled": True}, True),
        ({"rum": "garbage", "rum_enabled": True}, True),
        ({"rum": None, "rum_enabled": True}, True),
        ({"rum": {"enabled": 1}}, True),
        ({"rum": {"enabled": 0}, "rum_enabled": True}, False),
    ],
)
def test_is_rum_enabled_semantics(cfg, expected):
    assert is_rum_enabled(cfg) is expected


def test_rum_status_route_agrees_with_the_scheduler_when_fields_disagree(client, monkeypatch):
    from backend import config as svcconfig
    from tests.conftest import MOCK_SERVICE_ID

    disagreeing = {"service_id": MOCK_SERVICE_ID, "rum": {"enabled": False}, "rum_enabled": True}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: dict(disagreeing))
    resp = client.get(f"/api/services/{MOCK_SERVICE_ID}/rum/status")
    assert resp.status_code == 200
    assert resp.json()["enabled"] is is_rum_enabled(disagreeing) is False


_CASES = [
    ({"rum": {"enabled": False}, "rum_enabled": True}, False),
    ({"rum": {"enabled": True}, "rum_enabled": False}, True),
    ({"rum_enabled": True}, True),
    ({"rum": {"enabled": True}}, True),
    ({"rum": {"enabled": None}, "rum_enabled": True}, True),
    ({}, False),
]


@pytest.mark.parametrize("extra,expected", _CASES)
def test_feature_state_from_config_uses_the_shared_resolution(extra, expected):
    from backend.provision.declarative.state import FeatureState

    state = FeatureState.from_config({"service_id": "svc", "log_period": 60, **extra})
    assert state.rum_enabled is expected is is_rum_enabled(extra)


@pytest.mark.parametrize("extra,expected", _CASES)
def test_terraform_generation_uses_the_shared_resolution(extra, expected, monkeypatch):
    from backend.provision.declarative.state import FeatureState
    from backend.utils import terraform_gen

    seen: list[dict] = []
    real = FeatureState.from_config

    def spy(cfg):
        seen.append(cfg)
        return real(cfg)

    monkeypatch.setattr(terraform_gen.FeatureState, "from_config", staticmethod(spy))
    terraform_gen.generate_terraform({"logging_service_id": "svc", "log_period": 60, **extra}, "AK", "SK")
    assert seen[0]["rum_enabled"] is expected


@pytest.mark.parametrize("extra,expected", _CASES)
def test_ensure_logging_endpoint_rum_beacon_exclusion_follows_the_shared_resolution(extra, expected):
    from unittest.mock import patch

    from backend.provision import fastly_api

    conditions: list[str] = []

    def fake_fastly(method, path, body=None, **kwargs):
        if path.endswith("/clone"):
            return {"number": 11}
        if path.endswith("/validate"):
            return {"status": "ok"}
        return {}

    cfg = {
        "logging_service_id": "svc",
        "provisioning": {"endpoint_name": "E"},
        "fos_region": "us-east-1",
        "fos_bucket_name": "b",
        "log_period": 60,
        **extra,
    }
    with (
        patch("backend.provision.fastly_api.get_active_version", return_value=10),
        patch("backend.provision.fastly_api.list_s3_endpoints", return_value=[]),
        patch(
            "backend.provision.fastly_api.ensure_condition",
            side_effect=lambda n, stmt, *a, **k: conditions.append(stmt),
        ),
        patch("backend.provision.fastly_api.ensure_vcl_snippet"),
        patch("backend.provision.fastly_api.install_capture_snippets"),
        patch("backend.provision.fastly_api.load_log_format", return_value="f"),
        patch("backend.provision.fastly_api.fastly", side_effect=fake_fastly),
    ):
        fastly_api.ensure_logging_endpoint(cfg, "AK", "SK", "tok")

    assert ('req.url.path != "/rum-beacon"' in conditions[0]) is expected


@pytest.mark.parametrize(
    ("final_cfg", "expected"),
    [
        ({"rum": {"enabled": False}, "rum_enabled": True}, False),
        ({"rum": {"enabled": True}, "rum_enabled": False}, True),
        ({"rum_enabled": True}, True),
    ],
)
def test_rum_enable_done_event_reports_the_shared_resolution(final_cfg, expected, client, monkeypatch):
    import json

    from backend import config as svcconfig
    from tests.conftest import MOCK_SERVICE_ID

    state = {"cfg": {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "TOKEN"}}
    monkeypatch.setattr(svcconfig, "load_config", lambda sid: dict(state["cfg"]))

    def fake_run_with_events(func, *args, **kwargs):
        yield {"type": "status", "message": "enabling RUM"}
        state["cfg"] = {"service_id": MOCK_SERVICE_ID, "fastly_api_key": "TOKEN", **final_cfg}

    monkeypatch.setattr("backend.routers.rum.run_with_events", fake_run_with_events)
    monkeypatch.setattr("backend.cron.scheduler.get_scheduler", lambda: type("S", (), {"reload": lambda self: None})())

    resp = client.post(f"/api/services/{MOCK_SERVICE_ID}/rum/enable", json={"activate": True})
    assert resp.status_code == 200
    events = [json.loads(line[len("data: ") :]) for line in resp.text.splitlines() if line.startswith("data: ")]
    done = next(e for e in events if e["type"] == "done")
    assert done["rum"]["enabled"] is expected
