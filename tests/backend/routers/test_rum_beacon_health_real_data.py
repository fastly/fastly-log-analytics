"""Tests for backend.routers.rum — GET /rum/beacon-health, real-data branch.

test_rum_analytics.py's test_rum_beacon_health_endpoint only exercises the
"RUM not enabled" short-circuit. These tests cover the real-DB query path:
beacon counting within the last hour, last_beacon_time conversion, and the
fail-open except branch when the DB query itself blows up.
"""

from __future__ import annotations

import datetime

from fastapi.testclient import TestClient

from backend.core.metadata import get_con
from backend.main import app

client = TestClient(app)


def _clear(service_id: str) -> None:
    db = get_con(service_id)
    db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
    db.commit()


def test_enabled_with_recent_beacons_reports_fire_rate_and_setup_complete(monkeypatch):
    service_id = "test_beacon_health_recent"
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda sid: {"service_id": sid, "rum": {"enabled": True}} if sid == service_id else None,
    )
    _clear(service_id)
    db = get_con(service_id)
    recent = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)
    db.execute(
        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
        (service_id, recent.strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
    )
    db.commit()

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 1
    assert data["beacon_fire_rate"] == 1
    assert data["setup_complete"] is True
    assert data["message"] == "Script installed and firing"
    assert data["last_beacon_time"] is not None
    _clear(service_id)


def test_enabled_with_no_recent_beacons_reports_waiting(monkeypatch):
    service_id = "test_beacon_health_none_recent"
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda sid: {"service_id": sid, "rum": {"enabled": True}} if sid == service_id else None,
    )
    _clear(service_id)
    db = get_con(service_id)
    # A beacon exists but it's outside the 1-hour window.
    stale = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=3)
    db.execute(
        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
        (service_id, stale.strftime("%Y-%m-%dT%H:%M:%SZ"), "{}"),
    )
    db.commit()

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 0
    assert data["setup_complete"] is False
    assert data["message"] == "Waiting for beacons..."
    assert data["last_beacon_time"] is None
    _clear(service_id)


def test_db_failure_is_caught_and_reported_in_message(monkeypatch):
    service_id = "test_beacon_health_db_failure"
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda sid: {"service_id": sid, "rum": {"enabled": True}} if sid == service_id else None,
    )

    def _raise(_sid):
        raise RuntimeError("db is locked")

    monkeypatch.setattr("backend.routers.rum.get_con", _raise)

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["setup_complete"] is False
    assert "Setup check failed" in data["message"]
    assert "db is locked" in data["message"]
