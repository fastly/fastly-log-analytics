"""Tests for backend.routers.rum GET /rum/analytics — real Faro beacon shape.

This test file has been fully modernized to use the DuckDB analytical tables
(client_vitals, client_errors) instead of the obsolete SQLite prototype tables.
"""

from __future__ import annotations

import datetime
import json

import duckdb
import pytest
from fastapi.testclient import TestClient

from backend.main import app

client = TestClient(app)


@pytest.fixture
def setup_temp_rum_db(tmp_path, monkeypatch):
    """Fixture that initializes a temporary isolated DuckDB database with the RUM schema."""
    temp_db_path = tmp_path / "test.rum.duckdb"

    # Initialize tables
    con = duckdb.connect(str(temp_db_path))
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

    import uuid

    unique_id = f"test_service_{uuid.uuid4().hex[:8]}"

    # mock_source has test.duckdb so rum_source_for resolves it to test.rum.duckdb
    mock_source = {
        "name": unique_id,
        "service_id": unique_id,
        "duckdb_path": str(tmp_path / "test.duckdb"),
        "access_level": "read_write",
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }

    monkeypatch.setattr("backend.core.request_context._resolve_source", lambda service_id, read_only=False: mock_source)

    return temp_db_path


def _insert(temp_rum_db_path, beacons: list[dict]) -> None:
    from backend.core.rum_ingest import extract_metrics_from_faro_payload

    con = duckdb.connect(str(temp_rum_db_path))
    vitals_rows = []
    errors_rows = []

    for i, b in enumerate(beacons):
        received_at = datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=i + 5)
        req_id = f"req_{i}_{id(b)}"

        # Parse Faro payload
        extracted = extract_metrics_from_faro_payload(b, {})

        for item in extracted:
            metric_name = item.get("metric_name")
            if metric_name == "exception":
                errors_rows.append(
                    (
                        received_at,
                        item.get("error_message"),
                        item.get("error_file"),
                        item.get("error_line"),
                        item.get("error_col"),
                        item.get("pathname"),
                        item.get("browser"),
                        item.get("os"),
                        item.get("device"),
                        item.get("cid"),
                        req_id,
                    )
                )
            else:
                val = item.get("metric_value")
                # Convert pageLoadTime (usually string) to float if needed
                if metric_name == "pageLoadTime" and val is not None:
                    try:
                        val = float(val)
                    except Exception:
                        pass

                # Give proper metric rating
                rating = item.get("metric_rating")
                if not rating and val is not None:
                    try:
                        val_float = float(val)
                        if metric_name == "lcp":
                            rating = (
                                "good" if val_float <= 2.5 else ("needs_improvement" if val_float <= 4.0 else "poor")
                            )
                        elif metric_name == "cls":
                            rating = (
                                "good" if val_float <= 0.1 else ("needs_improvement" if val_float <= 0.25 else "poor")
                            )
                        elif metric_name == "inp":
                            rating = (
                                "good" if val_float <= 200 else ("needs_improvement" if val_float <= 500 else "poor")
                            )
                        else:
                            rating = "good"
                    except Exception:
                        rating = "good"

                vitals_rows.append(
                    (
                        received_at,
                        metric_name,
                        val,
                        rating,
                        item.get("pathname"),
                        item.get("browser"),
                        item.get("os"),
                        item.get("device"),
                        item.get("cid"),
                        req_id,
                    )
                )

    if vitals_rows:
        con.executemany("INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", vitals_rows)
    if errors_rows:
        con.executemany("INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", errors_rows)

    con.close()


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


def test_processing_exception_falls_back_to_mock_response(setup_temp_rum_db):
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
        assert r.status_code == 200
        data = r.json()
        assert data["is_mock"] is True
        assert data["vitals"]["lcp"]["p75"] == 2.0
        assert len(data["trends"]["timestamps"]) > 0
    finally:
        rum_module.execute_with_stale_view_retry = original
