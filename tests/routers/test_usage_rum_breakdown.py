"""Tests for GET /api/usage/rum-breakdown.

Computes RUM beacon volume + estimated FOS Class A operation cost, grouped
by day, from the same per-service SQLite ``rum_beacons`` table the RUM
router reads. Uses the real metadata DB (get_con) rather than mocking it,
so a broken date-bucketing GROUP BY or cost formula fails the assertions
instead of a wired-through mock always agreeing with itself.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from backend.core.metadata import get_con
from backend.deps import get_source
from backend.main import app


@pytest.fixture
def test_client():
    return TestClient(app)


def _clear(service_id: str) -> None:
    db = get_con(service_id)
    db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
    db.commit()


def _seed(service_id: str, received_at: str, count: int) -> None:
    db = get_con(service_id)
    for _ in range(count):
        db.execute(
            "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
            (service_id, received_at, "{}"),
        )
    db.commit()


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_computes_beacon_counts_and_cost(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_basic"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.01}

    _seed(service_id, "2026-08-05T10:00:00Z", 3)
    _seed(service_id, "2026-08-06T10:00:00Z", 7)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
        )
        assert r.status_code == 200, r.text
        body = r.json()

        assert body["total_beacons"] == 10
        assert body["total_estimated_class_a"] == 10
        assert body["class_a_rate_per_1k"] == 0.01
        # 10 ops / 1000 * 0.01 = 0.0001
        assert body["total_estimated_cost_usd"] == pytest.approx(0.0001, abs=1e-8)

        by_date = {p["date"]: p for p in body["data"]}
        assert by_date["2026-08-05"]["beacon_count"] == 3
        assert by_date["2026-08-06"]["beacon_count"] == 7
        assert by_date["2026-08-06"]["estimated_cost_usd"] == pytest.approx((7 / 1000.0) * 0.01, abs=1e-8)
    finally:
        app.dependency_overrides.pop(get_source, None)
        _clear(service_id)


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_no_beacons_returns_zeroed_response(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_empty"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.005}

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-07T00:00:00Z"},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
        assert body["total_estimated_cost_usd"] == 0.0
    finally:
        app.dependency_overrides.pop(get_source, None)


def test_rum_breakdown_no_resolvable_service_returns_not_found_note(test_client):
    source = {"name": None, "service_id": None}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get("/api/usage/rum-breakdown")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
        assert body["note"] == "Service not found."
    finally:
        app.dependency_overrides.pop(get_source, None)


@patch("backend.config.load_usage_logging_config")
def test_rum_breakdown_clamps_sub_hourly_granularity_to_daily(mock_load_rates, test_client):
    service_id = "test_usage_rum_breakdown_clamp"
    _clear(service_id)
    mock_load_rates.return_value = {"class_a_rate_per_1k": 0.005}
    _seed(service_id, "2026-08-05T10:00:00Z", 1)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get(
            "/api/usage/rum-breakdown",
            params={"start": "2026-08-05T00:00:00Z", "end": "2026-08-06T00:00:00Z", "by": "minute"},
        )
        assert r.status_code == 200
        body = r.json()
        assert "adjusted to daily granularity" in body["note"]
    finally:
        app.dependency_overrides.pop(get_source, None)
        _clear(service_id)


def test_rum_breakdown_db_query_failure_falls_back_to_zero_counts(test_client, monkeypatch):
    service_id = "test_usage_rum_breakdown_db_failure"

    def _raise(_sid):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("backend.core.metadata.get_con", _raise)

    source = {"name": service_id, "service_id": service_id}
    app.dependency_overrides[get_source] = lambda: source
    try:
        r = test_client.get("/api/usage/rum-breakdown")
        assert r.status_code == 200
        body = r.json()
        assert body["data"] == []
        assert body["total_beacons"] == 0
    finally:
        app.dependency_overrides.pop(get_source, None)
