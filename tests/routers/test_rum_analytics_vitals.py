"""Tests for backend.routers.rum GET /rum/analytics — real Faro beacon shape.

This test file has been fully modernized to use the DuckDB analytical tables
(client_vitals, client_errors) instead of the obsolete SQLite prototype tables.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
import time_machine


@pytest.fixture(autouse=True)
def skip_view_update():
    with patch("backend.core.iceberg.view.update_iceberg_view") as mock:
        yield mock


from fastapi.testclient import TestClient

from backend.main import app
from tests._helpers.rum import insert_rum_beacons as _insert
from tests._helpers.rum import setup_temp_rum_db

__all__ = ["setup_temp_rum_db"]

client = TestClient(app)


def _faro_beacon(path: str, lcp: float, cls: float, inp: float, load_time: float) -> dict:
    return {
        "meta": {
            "page": {"url": f"https://example.test{path}"},
            "browser": {"name": "Chrome", "version": "120.0", "mobile": False},
            "os": {"name": "macOS", "version": "14.0"},
        },
        "events": [
            {
                "name": "faro.performance.navigation",
                "attributes": {"pageLoadTime": str(load_time)},
            }
        ],
        "measurements": [
            {"type": "web-vitals", "values": {"lcp": lcp, "cls": cls, "inp": inp}},
        ],
    }


@time_machine.travel("2026-09-22 12:30:00+00:00", tick=False)
def test_real_faro_shaped_beacons_produce_correct_p75_and_avg_load_time(setup_temp_rum_db):
    service_id = "test_vitals_faro_shape"
    # 4 beacons on the same page: LCP values 1.0, 2.0, 3.0, 4.0 -> p75 index
    # int(4*0.75)=3 -> sorted[3] == 4.0. Load times 1,2,3,4 -> avg 2.5.
    beacons = [_faro_beacon("/home", lcp, lcp / 2, lcp * 50, lcp) for lcp in (1.0, 2.0, 3.0, 4.0)]
    _insert(setup_temp_rum_db, beacons)

    r = client.get(f"/api/services/{service_id}/rum/analytics")
    assert r.status_code == 200
    data = r.json()
    assert data["is_mock"] is False

    # Continuous interpolation: 75% between values
    assert data["vitals"]["lcp"]["p75"] == 3.25
    assert data["vitals"]["cls"]["p75"] == 1.625
    assert data["vitals"]["inp"]["p75"] == 162

    worst = data["worst_pages"]
    assert len(worst) == 1
    assert worst[0]["path"] == "/home"
    assert worst[0]["avg_load_time"] == 2.5
    assert worst[0]["lcp_p75"] == 3.25


def test_faro_exception_event_is_counted_and_grouped(setup_temp_rum_db):
    service_id = "test_vitals_faro_exception"
    beacon = _faro_beacon("/checkout", 2.0, 0.02, 100, 2.0)

    # Faro structured stacktrace frame to make sure extract works
    beacon["exceptions"] = [
        {
            "value": "TypeError: boom",
            "type": "TypeError",
            "stacktrace": {"frames": [{"filename": "app.js", "lineno": 42, "colno": 7}]},
        }
    ]
    # Need >=10 total beacons for the real-data path (not the "no_data" short-circuit).
    beacons = [beacon] + [_faro_beacon("/other", 1.0, 0.01, 50, 1.0) for _ in range(10)]
    _insert(setup_temp_rum_db, beacons)

    r = client.get(f"/api/services/{service_id}/rum/analytics")
    data = r.json()
    assert not data["is_mock"]
    errors = data["errors"]
    assert len(errors) == 1
    assert errors[0]["message"] == "TypeError: boom"
    assert errors[0]["file"] == "app.js"
    assert errors[0]["line"] == 42
    assert errors[0]["count"] == 1

    checkout_page = next(p for p in data["worst_pages"] if p["path"] == "/checkout")
    assert checkout_page["error_rate"] == 100.0


def test_exclude_filter_mode_removes_matching_beacons(setup_temp_rum_db):
    service_id = "test_vitals_exclude_filter"
    beacons = []
    for i in range(15):
        b = _faro_beacon(f"/p{i}", 1.5, 0.02, 80, 1.5)
        b["meta"]["browser"] = {"name": "Chrome"} if i < 5 else {"name": "Safari"}
        beacons.append(b)
    _insert(setup_temp_rum_db, beacons)

    filters = json.dumps({"browser": {"mode": "exclude", "values": ["Chrome"]}})
    r = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
    assert r.status_code == 200
    data = r.json()
    assert not data["is_mock"]
    # 10 Safari beacons should remain; 5 Chrome beacons excluded.
    assert data["environments"]["browsers"] == {"Safari": 10}


def test_path_extraction_falls_back_to_page_url_when_no_pathname(setup_temp_rum_db):
    service_id = "test_vitals_url_fallback"
    beacons = []
    for i in range(12):
        b = {
            "meta": {
                "page": {"url": "https://example.test/from-url//nested"},
                "browser": {"name": "Chrome"},
                "os": {"name": "macOS"},
            },
            "events": [
                {
                    "name": "faro.performance.navigation",
                    "attributes": {"pageLoadTime": "1.5"},
                }
            ],
            "measurements": [
                {"type": "web-vitals", "values": {"lcp": 1.5, "cls": 0.02, "inp": 80}},
            ],
        }
        beacons.append(b)
    _insert(setup_temp_rum_db, beacons)

    r = client.get(f"/api/services/{service_id}/rum/analytics")
    data = r.json()
    assert not data["is_mock"]
    paths = [p["path"] for p in data["worst_pages"]]
    # DuckDB returns exact extracted pathname
    assert paths == ["/from-url//nested"]


def test_processing_exception_returns_500(setup_temp_rum_db):
    service_id = "test_vitals_mock_fallback"
    _insert(setup_temp_rum_db, [_faro_beacon("/x", 1.0, 0.01, 50, 1.0) for _ in range(11)])

    import backend.routers.rum as rum_module

    # Mock execute_with_stale_view_retry to raise RuntimeError
    def _exploding_execute(*args, **kwargs):
        raise RuntimeError("simulated DB corruption mid-query")

    original = rum_module.execute_with_stale_view_retry
    rum_module.execute_with_stale_view_retry = _exploding_execute
    try:
        r = client.get(f"/api/services/{service_id}/rum/analytics")
        assert r.status_code == 500
        data = r.json()
        assert data["detail"]["message"] == "RUM analytics query failed"
        assert "simulated DB corruption" not in r.text
        assert data["detail"]["error_id"]
    finally:
        rum_module.execute_with_stale_view_retry = original
