import datetime
import json

from fastapi.testclient import TestClient

from backend.core.metadata import get_con
from backend.main import app

client = TestClient(app)


def test_rum_analytics_fallback() -> None:
    # Target service id that has no RUM data yet, should return "no data" response
    response = client.get("/api/services/svc-test-rum-fallback/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False
    assert data["no_data"] is True
    assert "beacon_count" in data
    assert data["beacon_count"] < 10
    assert "message" in data
    assert "vitals" in data
    assert "worst_pages" in data
    assert "errors" in data
    assert "trends" in data
    assert "environments" in data

    # Verify empty collections
    assert data["worst_pages"] == []
    assert data["errors"] == []
    assert data["trends"]["lcp"] == []


def test_rum_live_events() -> None:
    response = client.get("/api/services/svc-test-rum-fallback/rum/live-events")
    assert response.status_code == 200
    events = response.json()
    assert isinstance(events, list)
    assert len(events) == 5
    for e in events:
        assert "time" in e
        assert "type" in e
        assert "path" in e
        assert "desc" in e


def test_rum_analytics_real_data() -> None:
    service_id = "test_service_real_data"
    db = get_con(service_id)

    # Ensure RUM table exists and clear previous test entries
    db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
    db.commit()

    # Populate with 12 distinct mock beacons (>= 10) to trigger real aggregation path
    test_beacons = [
        {
            "pathname": "/pricing",
            "load_time": 2.1,
            "lcp": 2.3,
            "cls": 0.02,
            "inp": 120,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
            "exceptions": [{"value": "TypeError: null is not an object", "filename": "main.js"}],
        },
        {
            "pathname": "/",
            "load_time": 1.4,
            "lcp": 1.6,
            "cls": 0.01,
            "inp": 80,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
        },
        {
            "pathname": "/pricing",
            "load_time": 2.5,
            "lcp": 2.8,
            "cls": 0.03,
            "inp": 150,
            "meta": {"browser": "Safari", "os": "iOS", "device": "Mobile"},
        },
        {
            "pathname": "/docs",
            "load_time": 1.8,
            "lcp": 1.9,
            "cls": 0.05,
            "inp": 90,
            "meta": {"browser": "Firefox", "os": "Linux", "device": "Desktop"},
        },
        {
            "pathname": "/",
            "load_time": 1.2,
            "lcp": 1.4,
            "cls": 0.01,
            "inp": 70,
            "meta": {"browser": "Chrome", "os": "Windows", "device": "Desktop"},
        },
        {
            "pathname": "/checkout",
            "load_time": 3.2,
            "lcp": 3.5,
            "cls": 0.12,
            "inp": 220,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
            "exceptions": [{"value": "TypeError: null is not an object", "filename": "main.js"}],
        },
        {
            "pathname": "/pricing",
            "load_time": 2.0,
            "lcp": 2.2,
            "cls": 0.02,
            "inp": 110,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
        },
        {
            "pathname": "/",
            "load_time": 1.5,
            "lcp": 1.7,
            "cls": 0.02,
            "inp": 85,
            "meta": {"browser": "Safari", "os": "macOS", "device": "Desktop"},
        },
        {
            "pathname": "/docs",
            "load_time": 1.9,
            "lcp": 2.1,
            "cls": 0.04,
            "inp": 100,
            "meta": {"browser": "Chrome", "os": "Android", "device": "Mobile"},
        },
        {
            "pathname": "/",
            "load_time": 1.3,
            "lcp": 1.5,
            "cls": 0.01,
            "inp": 75,
            "meta": {"browser": "Edge", "os": "Windows", "device": "Desktop"},
        },
        {
            "pathname": "/pricing",
            "load_time": 2.2,
            "lcp": 2.4,
            "cls": 0.02,
            "inp": 130,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
        },
        {
            "pathname": "/checkout",
            "load_time": 3.0,
            "lcp": 3.2,
            "cls": 0.10,
            "inp": 200,
            "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
            "exceptions": [{"value": "ReferenceError: x is not defined", "filename": "checkout.js"}],
        },
    ]

    for i, b in enumerate(test_beacons):
        received_at = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=i)).isoformat()
        db.execute(
            "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
            (service_id, received_at, json.dumps(b)),
        )
    db.commit()

    try:
        # Hit analytics endpoint
        response = client.get(f"/api/services/{service_id}/rum/analytics")
        assert response.status_code == 200
        data = response.json()
        assert data["is_mock"] is False

        # Verify vitals calculations
        vitals = data["vitals"]
        assert "lcp" in vitals
        assert "cls" in vitals
        assert "inp" in vitals

        # Verify LCP distribution calculation
        # Good limit is <= 2.5s, Poor is > 4.0s. All LCPs in mock are <= 3.5s, so good/needs-improvement
        assert vitals["lcp"]["distribution"]["good"] > 0
        assert vitals["lcp"]["distribution"]["poor"] == 0

        # Verify browser, OS and device aggregates
        envs = data["environments"]
        assert envs["browsers"].get("Chrome", 0) == 8
        assert envs["os"].get("macOS", 0) == 7
        assert envs["devices"].get("Desktop", 0) == 10

        # Verify error grouping (2 main.js errors, 1 checkout.js error)
        errors = data["errors"]
        assert len(errors) == 2
        assert errors[0]["count"] == 2
        assert errors[1]["count"] == 1

        # Hit live-events endpoint
        response_live = client.get(f"/api/services/{service_id}/rum/live-events")
        assert response_live.status_code == 200
        events = response_live.json()
        assert len(events) == 10  # recent 10 events

    finally:
        # Cleanup
        db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
        db.commit()


def test_rum_analytics_date_filtering() -> None:
    service_id = "test_service_date_filter"
    db = get_con(service_id)

    db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
    db.commit()

    for i in range(20):
        b = {"pathname": f"/page_{i}"}
        if i < 10:
            received_at = "2026-08-05T10:00:00+00:00"
        else:
            received_at = "2026-08-05T12:00:00+00:00"

        db.execute(
            "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
            (service_id, received_at, json.dumps(b)),
        )
    db.commit()

    try:
        response = client.get(f"/api/services/{service_id}/rum/analytics?start_time=2026-08-05T11:00:00+00:00")
        assert response.status_code == 200
        data = response.json()
        assert not data["is_mock"]

        paths = [p["path"] for p in data["worst_pages"]]
        for p in paths:
            assert int(p.split("_")[1]) >= 10

        response = client.get(f"/api/services/{service_id}/rum/analytics?end_time=2026-08-05T11:00:00+00:00")
        data = response.json()
        assert not data["is_mock"]
        paths = [p["path"] for p in data["worst_pages"]]
        for p in paths:
            assert int(p.split("_")[1]) < 10

    finally:
        db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
        db.commit()


def test_rum_status_endpoint() -> None:
    service_id = "svc-test-rum-status"
    response = client.get(f"/api/services/{service_id}/rum/status")
    assert response.status_code == 200
    data = response.json()
    assert "enabled" in data
    assert isinstance(data["enabled"], bool)


def test_rum_beacon_health_endpoint() -> None:
    service_id = "svc-test-rum-health"
    response = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert response.status_code == 200
    data = response.json()
    assert "enabled" in data
    assert "recent_beacons" in data
    assert "last_beacon_time" in data
    assert "setup_complete" in data


def test_rum_analytics_with_filters() -> None:
    service_id = "test_service_with_filters"
    db = get_con(service_id)

    db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
    db.commit()

    # Insert 15 beacons to satisfy the minimum requirements
    for i in range(15):
        b = {
            "pathname": f"/page_{i}",
            "load_time": 2.0,
            "lcp": 2.1,
            "cls": 0.02,
            "inp": 100,
            "meta": {
                "browser": "Chrome" if i < 8 else "Safari",
                "os": "macOS" if i < 10 else "iOS",
                "device": "Desktop" if i < 12 else "Mobile",
            },
        }
        db.execute(
            "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
            (service_id, "2026-08-05T12:00:00+00:00", json.dumps(b)),
        )
    db.commit()

    try:
        # Filter browser Chrome
        filters = json.dumps({"browser": {"mode": "include", "values": ["Chrome"]}})
        response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
        assert response.status_code == 200
        data = response.json()
        assert not data["is_mock"]
        assert data["environments"]["browsers"] == {"Chrome": 8}

        # Filter browser Safari
        filters = json.dumps({"browser": {"mode": "include", "values": ["Safari"]}})
        response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
        assert response.status_code == 200
        data = response.json()
        assert not data["is_mock"]
        assert data["environments"]["browsers"] == {"Safari": 7}

        # Filter non-existent browser should return zero matching count and empty/no_data=False dictionary
        filters = json.dumps({"browser": {"mode": "include", "values": ["Firefox"]}})
        response = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
        assert response.status_code == 200
        data = response.json()
        assert data["beacon_count"] == 0
        assert data["is_mock"] is False
        assert data["no_data"] is False

    finally:
        db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
        db.commit()
