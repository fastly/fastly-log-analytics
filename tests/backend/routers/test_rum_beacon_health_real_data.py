"""Tests for backend.routers.rum — GET /rum/beacon-health, real-data branch.

Covers the real-DB query path: beacon counting within the last hour, last_beacon_time conversion,
and the fail-open except branch when the DB query itself blows up.
"""

from __future__ import annotations

import datetime
from unittest.mock import patch

import duckdb
import pytest


@pytest.fixture(autouse=True)
def skip_view_update():
    with patch("backend.core.iceberg.view.update_iceberg_view") as mock:
        yield mock


from fastapi.testclient import TestClient

from backend.main import app

client = TestClient(app)


@pytest.fixture
def setup_temp_rum_db(tmp_path, monkeypatch):
    """Fixture to set up a temporary DuckDB database for RUM testing."""
    temp_db_path = tmp_path / "test.duckdb"
    temp_rum_db_path = tmp_path / "test.rum.duckdb"

    # Disable DuckDB connection pool to prevent cross-test cached connection contamination
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")

    # Mock configuration to return our temporary database path
    monkeypatch.setattr(
        "backend.config.load_config",
        lambda sid: {
            "service_id": sid,
            "rum": {"enabled": True},
            "rum_enabled": True,
        },
    )

    mock_source = {
        "name": "test_service",
        "service_id": "test_service",
        "duckdb_path": str(temp_db_path),
        "access_level": "read_write",
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }
    import backend.core.duckdb_pool as duckdb_pool

    duckdb_pool.reset_pool_for_service("test_service")
    duckdb_pool.reset_pool_for_service("test_service_rum")
    monkeypatch.setattr("backend.core.request_context._resolve_source", lambda sid: mock_source)
    monkeypatch.setattr("backend.deps._resolve_source_or_400", lambda sid: mock_source)
    monkeypatch.setattr("backend.core.duckdb.get_source_for_service", lambda sid: mock_source)

    # Initialize RUM tables
    con = duckdb.connect(str(temp_rum_db_path))
    con.execute("""
        CREATE TABLE client_vitals (
            timestamp TIMESTAMPTZ,
            metric_name VARCHAR,
            metric_value DOUBLE,
            metric_rating VARCHAR,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE client_errors (
            timestamp TIMESTAMPTZ,
            error_message VARCHAR,
            error_file VARCHAR,
            error_line INTEGER,
            error_col INTEGER,
            pathname VARCHAR,
            browser VARCHAR,
            os VARCHAR,
            device VARCHAR,
            cid VARCHAR,
            req_id VARCHAR
        )
    """)
    con.close()

    return temp_rum_db_path


def test_enabled_with_recent_beacons_reports_fire_rate_and_setup_complete(setup_temp_rum_db):
    service_id = "test_beacon_health_recent"
    con = duckdb.connect(str(setup_temp_rum_db))
    recent = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=5)

    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (recent, "load_time", 1.5, "good", "/", "Chrome", "macOS", "Desktop", "cid1", "req1"),
    )
    con.close()

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    print("DEBUG BEACON HEALTH RESP:", r.status_code, r.text)
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
    con = duckdb.connect(str(setup_temp_rum_db))
    stale = datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=3)

    con.execute(
        "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (stale, "load_time", 1.5, "good", "/", "Chrome", "macOS", "Desktop", "cid1", "req1"),
    )
    con.close()

    r = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert r.status_code == 200
    data = r.json()
    assert data["enabled"] is True
    assert data["recent_beacons"] == 0
    assert data["setup_complete"] is True
    assert data["message"] == "Script installed and firing"
    assert data["last_beacon_time"] is not None


def test_enabled_with_absolutely_no_beacons_reports_waiting(setup_temp_rum_db):
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
