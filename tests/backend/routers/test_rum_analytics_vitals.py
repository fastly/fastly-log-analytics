"""Tests for backend.routers.rum GET /rum/analytics — real Faro beacon shape.

test_rum_analytics.py's fixtures use a flattened, non-Faro beacon shape
(top-level "lcp"/"cls"/"inp" keys), which never exercises the actual
Faro SDK event/measurement extraction the handler is written for
(``events: [{"name": "faro.performance.navigation", ...}]`` and
``measurements: [{"type": "web-vitals", "values": {...}}]``). Vitals
computed from the wrong beacon shape default to a hardcoded 1.9/0.05
fallback that would silently pass the existing distribution-only
assertions — these tests seed real Faro-shaped beacons and assert on the
actual computed p75/avg_load_time values, so a broken extractor fails
loudly instead of returning plausible-looking mock numbers.
"""

from __future__ import annotations

import datetime
import json

from fastapi.testclient import TestClient

from backend.core.metadata import get_con
from backend.main import app

client = TestClient(app)


def _clear(service_id: str) -> None:
    db = get_con(service_id)
    db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
    db.commit()


def _insert(service_id: str, beacons: list[dict]) -> None:
    db = get_con(service_id)
    for i, b in enumerate(beacons):
        received_at = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=i)).isoformat()
        db.execute(
            "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
            (service_id, received_at, json.dumps(b)),
        )
    db.commit()


def _faro_beacon(path: str, lcp: float, cls: float, inp: float, load_time: float) -> dict:
    return {
        "meta": {"page": {"url": f"https://example.test{path}"}, "browser": "Chrome", "os": "macOS"},
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


def test_real_faro_shaped_beacons_produce_correct_p75_and_avg_load_time():
    service_id = "test_vitals_faro_shape"
    _clear(service_id)
    # 4 beacons on the same page: LCP values 1.0, 2.0, 3.0, 4.0 -> p75 index
    # int(4*0.75)=3 -> sorted[3] == 4.0. Load times 1,2,3,4 -> avg 2.5.
    beacons = [_faro_beacon("/home", lcp, lcp / 2, lcp * 50, lcp) for lcp in (1.0, 2.0, 3.0, 4.0)]
    _insert(service_id, beacons)

    try:
        r = client.get(f"/api/services/{service_id}/rum/analytics")
        assert r.status_code == 200
        data = r.json()
        assert data["is_mock"] is False

        assert data["vitals"]["lcp"]["p75"] == 4.0
        assert data["vitals"]["cls"]["p75"] == 2.0
        assert data["vitals"]["inp"]["p75"] == 200  # int() truncation of 4.0*50

        worst = data["worst_pages"]
        assert len(worst) == 1
        assert worst[0]["path"] == "/home"
        assert worst[0]["avg_load_time"] == 2.5
        assert worst[0]["lcp_p75"] == 4.0
    finally:
        _clear(service_id)


def test_faro_exception_event_is_counted_and_grouped():
    service_id = "test_vitals_faro_exception"
    _clear(service_id)
    beacon = _faro_beacon("/checkout", 2.0, 0.02, 100, 2.0)
    beacon["events"].append(
        {
            "name": "faro.exception",
            "attributes": {"value": "TypeError: boom", "filename": "app.js", "lineno": 42, "colno": 7},
        }
    )
    # Need >=10 total beacons for the real-data path (not the "no_data" short-circuit).
    beacons = [beacon] + [_faro_beacon("/other", 1.0, 0.01, 50, 1.0) for _ in range(10)]
    _insert(service_id, beacons)

    try:
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
    finally:
        _clear(service_id)


def test_exclude_filter_mode_removes_matching_beacons():
    service_id = "test_vitals_exclude_filter"
    _clear(service_id)
    beacons = []
    for i in range(15):
        b = _faro_beacon(f"/p{i}", 1.5, 0.02, 80, 1.5)
        b["meta"]["browser"] = "Chrome" if i < 5 else "Safari"
        beacons.append(b)
    _insert(service_id, beacons)

    try:
        filters = json.dumps({"browser": {"mode": "exclude", "values": ["Chrome"]}})
        r = client.get(f"/api/services/{service_id}/rum/analytics?filters={filters}")
        assert r.status_code == 200
        data = r.json()
        assert not data["is_mock"]
        # 10 Safari beacons should remain; 5 Chrome beacons excluded.
        assert data["environments"]["browsers"] == {"Safari": 10}
    finally:
        _clear(service_id)


def test_path_extraction_falls_back_to_page_url_when_no_pathname():
    service_id = "test_vitals_url_fallback"
    _clear(service_id)
    beacons = []
    for i in range(12):
        b = {
            "meta": {"page": {"url": "https://example.test/from-url//nested"}, "browser": "Chrome", "os": "macOS"},
            "events": [],
            "measurements": [],
        }
        beacons.append(b)
    _insert(service_id, beacons)

    try:
        r = client.get(f"/api/services/{service_id}/rum/analytics")
        data = r.json()
        assert not data["is_mock"]
        paths = [p["path"] for p in data["worst_pages"]]
        # Double-slash collapse: "/from-url//nested" -> "/from-url/nested"
        assert paths == ["/from-url/nested"]
    finally:
        _clear(service_id)


def test_processing_exception_falls_back_to_mock_response():
    service_id = "test_vitals_mock_fallback"
    _clear(service_id)
    _insert(service_id, [_faro_beacon("/x", 1.0, 0.01, 50, 1.0) for _ in range(11)])

    import backend.routers.rum as rum_module

    real_get_con = rum_module.get_con
    call_count = {"n": 0}

    class _ExplodingDB:
        def __getattr__(self, name):
            real_db = real_get_con(service_id)
            return getattr(real_db, name)

        def execute(self, sql, params=()):
            call_count["n"] += 1
            # Allow the first two queries (total_service_beacons check +
            # normalize) through to the real DB, then blow up on the main
            # aggregation query to force the outer except -> mock path.
            if call_count["n"] <= 2:
                return real_get_con(service_id).execute(sql, params)
            raise RuntimeError("simulated DB corruption mid-query")

    def _fake_get_con(sid):
        return _ExplodingDB()

    original = rum_module.get_con
    rum_module.get_con = _fake_get_con
    try:
        r = client.get(f"/api/services/{service_id}/rum/analytics")
        assert r.status_code == 200
        data = r.json()
        assert data["is_mock"] is True
        assert data["vitals"]["lcp"]["p75"] == 2.0
        assert len(data["trends"]["timestamps"]) > 0
    finally:
        rum_module.get_con = original
        _clear(service_id)
