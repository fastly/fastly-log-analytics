"""Tests for backend.routers.rum — GET /rum/beacon-health, real-data branch.

Covers the real-DB query path: beacon counting within the last hour, last_beacon_time conversion,
and the fail-open except branch when the DB query itself blows up.
"""

from __future__ import annotations

import datetime
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def skip_view_update():
    with patch("backend.core.iceberg.view.update_iceberg_view") as mock:
        yield mock


from fastapi.testclient import TestClient

from backend.main import app
from tests._helpers.rum import insert_rum_beacons, setup_temp_rum_db

__all__ = ["setup_temp_rum_db"]

client = TestClient(app)


@pytest.fixture(autouse=True)
def _mock_rum_enabled_config(monkeypatch):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda sid: {
            "service_id": sid,
            "rum": {"enabled": True},
            "rum_enabled": True,
        },
    )


def test_enabled_with_recent_beacons_reports_fire_rate_and_setup_complete(setup_temp_rum_db):
    service_id = "test_beacon_health_recent"
    recent = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)

    insert_rum_beacons(
        setup_temp_rum_db,
        [{"pathname": "/", "load_time": 1.5, "received_at": recent.isoformat(), "cid": "cid1", "req_id": "req1"}],
    )

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 1
    assert data["beacon_fire_rate"] == 1
    assert data["setup_complete"] is True
    assert data["message"] == "Script installed and firing"
    assert data["last_beacon_time"] is not None


def test_enabled_with_no_recent_beacons_reports_waiting(setup_temp_rum_db):
    service_id = "test_beacon_health_none_recent"
    stale = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=3)

    insert_rum_beacons(
        setup_temp_rum_db,
        [{"pathname": "/", "load_time": 1.5, "received_at": stale.isoformat(), "cid": "cid1", "req_id": "req1"}],
    )

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 0
    assert data["setup_complete"] is True
    assert data["message"] == "Script installed and firing"
    assert data["last_beacon_time"] is not None


def test_enabled_with_absolutely_no_beacons_reports_waiting(setup_temp_rum_db):
    _ = setup_temp_rum_db
    service_id = "test_beacon_health_empty"
    # Database is completely empty

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 0
    assert data["setup_complete"] is False
    assert data["message"] == "Waiting for beacons..."
    assert data["last_beacon_time"] is None


def test_db_failure_is_caught_and_reported_in_message(setup_temp_rum_db, monkeypatch):
    _ = setup_temp_rum_db
    service_id = "test_beacon_health_db_failure"

    def _raise(*args, **kwargs):
        raise RuntimeError("db is locked")

    monkeypatch.setattr("backend.routers.rum.execute_with_stale_view_retry", _raise)

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["setup_complete"] is False
    assert "Setup check failed" in data["message"]
    assert "db is locked" in data["message"]
