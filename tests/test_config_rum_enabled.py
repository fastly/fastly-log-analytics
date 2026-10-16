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
