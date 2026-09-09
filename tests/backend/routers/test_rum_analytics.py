import asyncio
import datetime
import json
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pytest
from fastapi.testclient import TestClient


@pytest.fixture(autouse=True)
def skip_view_update(request):
    if request.node.name == "test_rum_analytics_cold_start_readonly":
        yield None
    else:
        with patch("backend.core.iceberg.view.update_iceberg_view") as mock:
            yield mock


from backend.core.request_context import RequestContext
from backend.main import app
from backend.routers import rum as rum_router
from backend.utils.remote_access import get_analyst_time_bounds

client = TestClient(app)


@pytest.fixture(scope="function")
def setup_temp_rum_db(tmp_path, monkeypatch):
    """Sets up a temporary DuckDB database for RUM and mocks the source resolver."""
    base_db_path = tmp_path / "test.duckdb"
    rum_db_path = tmp_path / "test.rum.duckdb"

    # Initialize DuckDB schema
    con = duckdb.connect(str(rum_db_path))
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
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
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
            req_id VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            pop VARCHAR,
            tls VARCHAR,
            ttfb DOUBLE
        )
    """)
    con.close()
    con.close()

    import uuid

    unique_id = f"test_service_{uuid.uuid4().hex[:8]}"

    # mock_source has test.duckdb so rum_source_for resolves it to test.rum.duckdb
    mock_source = {
        "name": unique_id,
        "service_id": unique_id,
        "duckdb_path": str(base_db_path),
        "access_level": "read_write",
        "endpoint": "localhost",
        "access_key_id": "mock",
        "secret_access_key": "mock",
        "region": "mock",
    }

    import backend.core.duckdb_pool as duckdb_pool

    duckdb_pool.reset_pool_for_service("test_service")
    duckdb_pool.reset_pool_for_service("test_service_rum")
    monkeypatch.setattr("backend.core.request_context._resolve_source", lambda service_id, read_only=False: mock_source)
    monkeypatch.setattr("backend.deps._resolve_source_or_400", lambda service_id, read_only=False: mock_source)

    return rum_db_path


def _insert_beacons(temp_rum_db_path, beacons_list):
    """Helper to parse Faro-like beacons and insert them into DuckDB tables."""
    import backend.core.duckdb_pool as duckdb_pool

    duckdb_pool.shutdown_all()

    vitals_rows = []
    errors_rows = []

    for i, b in enumerate(beacons_list):
        received_at = b.get("received_at")
        if not received_at:
            # Shift back by a shorter amount to ensure it is in the last 24h filter
            received_at = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(minutes=i + 5)).isoformat()

        # Parse timestamp
        dt = datetime.datetime.fromisoformat(received_at.replace("Z", "+00:00"))

        meta = b.get("meta") or {}
        browser_val = meta.get("browser") or "Chrome"
        os_val = meta.get("os") or "macOS"
        device_val = meta.get("device") or "Desktop"
        pathname_val = b.get("pathname") or "/"
        cid_val = b.get("cid") or "test_cid_1"
        req_id_val = b.get("req_id") if "req_id" in b else f"req_{id(b)}"

        # Get optional edge fields or default
        city_val = b.get("city") or "Austin"
        region_val = b.get("region") or "TX"
        country_val = b.get("country") or "US"
        pop_val = b.get("pop") or "SIN"
        tls_val = b.get("tls") or "1.3"
        ttfb_val = float(b["ttfb"]) if "ttfb" in b else 45.0

        # Load times / performance vitals
        if "load_time" in b:
            vitals_rows.append(
                (
                    dt,
                    "load_time",
                    float(b["load_time"]),
                    "good" if b["load_time"] <= 2.0 else "poor",
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

        if "lcp" in b:
            val = float(b["lcp"])
            if val <= 2.5:
                rating = "good"
            elif val <= 4.0:
                rating = "needs_improvement"
            else:
                rating = "poor"
            vitals_rows.append(
                (
                    dt,
                    "lcp",
                    val,
                    rating,
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

        if "cls" in b:
            val = float(b["cls"])
            if val <= 0.1:
                rating = "good"
            elif val <= 0.25:
                rating = "needs_improvement"
            else:
                rating = "poor"
            vitals_rows.append(
                (
                    dt,
                    "cls",
                    val,
                    rating,
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

        if "inp" in b:
            val = float(b["inp"])
            if val <= 200:
                rating = "good"
            elif val <= 500:
                rating = "needs_improvement"
            else:
                rating = "poor"
            vitals_rows.append(
                (
                    dt,
                    "inp",
                    val,
                    rating,
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

        # Exceptions
        exceptions = b.get("exceptions") or []
        for exc in exceptions:
            errors_rows.append(
                (
                    dt,
                    exc.get("value") or exc.get("message") or "TypeError",
                    exc.get("filename") or "main.js",
                    0,
                    0,
                    pathname_val,
                    browser_val,
                    os_val,
                    device_val,
                    cid_val,
                    req_id_val,
                    city_val,
                    region_val,
                    country_val,
                    pop_val,
                    tls_val,
                    ttfb_val,
                )
            )

    con = duckdb.connect(str(temp_rum_db_path), read_only=False)
    try:
        if vitals_rows:
            con.executemany(
                "INSERT INTO client_vitals VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", vitals_rows
            )
        if errors_rows:
            con.executemany(
                "INSERT INTO client_errors VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", errors_rows
            )
    finally:
        con.close()


def test_rum_analytics_fallback(setup_temp_rum_db) -> None:
    # Target service id that has no RUM data yet, should return "no data" response
    response = client.get("/api/services/svc-test-rum-fallback/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    assert data["is_mock"] is False
    assert data["no_data"] is True
    assert "beacon_count" in data
    assert data["beacon_count"] == 0
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


def test_rum_live_events(setup_temp_rum_db) -> None:
    # 1. When empty, should return a clean empty list
    response = client.get("/api/services/svc-test-rum-fallback/rum/live-events")
    assert response.status_code == 200
    events = response.json()
    assert isinstance(events, list)
    assert len(events) == 0

    # 2. Insert a real test beacon
    _insert_beacons(
        setup_temp_rum_db,
        [
            {
                "pathname": "/live-test",
                "load_time": 1.5,
                "lcp": 1.7,
                "cls": 0.01,
                "inp": 100,
                "meta": {"browser": "Chrome", "os": "macOS", "device": "Desktop"},
            }
        ],
    )

    # 3. Verify they are now returned in the ticker
    response = client.get("/api/services/svc-test-rum-fallback/rum/live-events")
    assert response.status_code == 200
    events = response.json()
    assert isinstance(events, list)
    assert len(events) == 4
    assert events[0]["path"] == "/live-test"
    assert events[0]["browser"] == "Chrome"
    assert events[0]["os"] == "macOS"


def test_rum_analytics_real_data(setup_temp_rum_db) -> None:
    service_id = "test_service_real_data"

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
            "req_id": "",
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

    _insert_beacons(setup_temp_rum_db, test_beacons)

    # Hit analytics endpoint
    response = client.get(f"/api/services/{service_id}/rum/analytics")
    assert response.status_code == 200
    data = response.json()
    print("DEBUG DATA:", json.dumps(data, indent=2))
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


def test_rum_analytics_date_filtering(setup_temp_rum_db) -> None:
    service_id = "test_service_date_filter"

    test_beacons = []
    for i in range(20):
        b = {"pathname": f"/page_{i}", "load_time": 1.5}
        if i < 10:
            b["received_at"] = "2026-08-05T10:00:00+00:00"
        else:
            b["received_at"] = "2026-08-05T12:00:00+00:00"
        test_beacons.append(b)

    _insert_beacons(setup_temp_rum_db, test_beacons)

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


def test_rum_status_endpoint(setup_temp_rum_db) -> None:
    service_id = "svc-test-rum-status"
    response = client.get(f"/api/services/{service_id}/rum/status")
    assert response.status_code == 200
    data = response.json()
    assert "enabled" in data
    assert isinstance(data["enabled"], bool)


def test_rum_beacon_health_endpoint(setup_temp_rum_db) -> None:
    service_id = "svc-test-rum-health"
    response = client.get(f"/api/services/{service_id}/rum/beacon-health")
    assert response.status_code == 200
    data = response.json()
    assert "enabled" in data
    assert "recent_beacons" in data
    assert "last_beacon_time" in data
    assert "setup_complete" in data


def test_rum_analytics_with_filters(setup_temp_rum_db) -> None:
    service_id = "test_service_with_filters"

    # Insert 15 beacons to satisfy the minimum requirements
    test_beacons = []
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
        test_beacons.append(b)

    _insert_beacons(setup_temp_rum_db, test_beacons)

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


# ── Analyst invite-window clamp (security follow-up) ──────────────────────


def _fake_request(analyst_session: object | None) -> SimpleNamespace:
    """Minimal stand-in — only ``request.state.analyst_session`` is read by
    ``get_analyst_time_bounds``/``clamp_or_400``. Same idiom as
    ``test_session_scoring_router.py``'s ``_fake_request``."""
    return SimpleNamespace(state=SimpleNamespace(analyst_session=analyst_session))


def _analyst_session(window_hours: int = 1) -> SimpleNamespace:
    return SimpleNamespace(query_window_hours=window_hours, query_start_time=None, query_end_time=None)


@pytest.mark.security_regression
def test_analyst_request_outside_invite_window_is_clamped(setup_temp_rum_db) -> None:
    """An analyst scoped to a 1-hour invite who asks for start_time way in
    the past must not see beacons outside that window — even though the
    beacon itself belongs to their own service."""
    service_id = "test_rum_analytics_clamp_analyst"

    old_ts = (datetime.datetime.now(datetime.UTC) - datetime.timedelta(days=30)).isoformat()
    recent_ts = datetime.datetime.now(datetime.UTC).isoformat()

    test_beacons = [
        {"pathname": "/thirty-days-old", "load_time": 1.5, "received_at": old_ts},
        {"pathname": "/just-now", "load_time": 1.5, "received_at": recent_ts},
    ]
    _insert_beacons(setup_temp_rum_db, test_beacons)

    # Analyst asking for a range far wider than their 1h invite window.
    analyst_sess = _analyst_session(window_hours=1)
    analyst_req = _fake_request(analyst_sess)
    import typing

    analyst_time_bounds = get_analyst_time_bounds(typing.cast(typing.Any, analyst_req))

    analyst_ctx = RequestContext(
        service_id=service_id,
        source={
            "name": "test_service",
            "service_id": "test_service",
            "duckdb_path": str(setup_temp_rum_db).replace(".rum.duckdb", ".duckdb"),
            "access_level": "read_write",
            "endpoint": "localhost",
            "access_key_id": "mock",
            "secret_access_key": "mock",
            "region": "mock",
        },
        con=None,
        telemetry=typing.cast(
            typing.Any,
            SimpleNamespace(
                start_section=lambda *a, **kw: SimpleNamespace(__enter__=lambda *x: None, __exit__=lambda *x: None)
            ),
        ),
        analyst_session=analyst_sess,
        read_only=True,
        time_bounds=analyst_time_bounds,
    )

    result = asyncio.run(
        rum_router.rum_analytics(
            request=typing.cast(typing.Any, analyst_req),
            start_time="2000-01-01T00:00:00Z",
            end_time=None,
            filters=None,
            ctx=analyst_ctx,
        )
    )
    paths = [p["path"] for p in result["worst_pages"]]
    assert "/thirty-days-old" not in paths, "analyst invite-window clamp did not apply"

    from backend.utils.remote_access import TimeBounds

    # Negative control: admin (no analyst_session) with the same
    # far-in-the-past start_time is NOT clamped — sees the old beacon.
    admin_ctx = RequestContext(
        service_id=service_id,
        source={
            "name": "test_service",
            "service_id": "test_service",
            "duckdb_path": str(setup_temp_rum_db).replace(".rum.duckdb", ".duckdb"),
            "access_level": "read_write",
            "endpoint": "localhost",
            "access_key_id": "mock",
            "secret_access_key": "mock",
            "region": "mock",
        },
        con=None,
        telemetry=typing.cast(
            typing.Any,
            SimpleNamespace(
                start_section=lambda *a, **kw: SimpleNamespace(__enter__=lambda *x: None, __exit__=lambda *x: None)
            ),
        ),
        analyst_session=None,
        read_only=True,
        time_bounds=TimeBounds(None, None),
    )

    admin_result = asyncio.run(
        rum_router.rum_analytics(
            request=typing.cast(typing.Any, _fake_request(None)),
            start_time="2000-01-01T00:00:00Z",
            end_time=None,
            filters=None,
            ctx=admin_ctx,
        )
    )
    admin_paths = [p["path"] for p in admin_result["worst_pages"]]
    assert "/thirty-days-old" in admin_paths


def test_rum_analytics_cold_start_readonly(tmp_path, monkeypatch):
    """Verifies that rum_analytics handles a cold start gracefully on a read-only DuckDB connection."""
    # Create a fresh database that has NO tables or views
    temp_db_path = tmp_path / "cold_start.rum.duckdb"
    con = duckdb.connect(str(temp_db_path))
    con.close()

    # Open the connection in read_only mode to simulate a read-only database environment
    ro_con = duckdb.connect(str(temp_db_path), read_only=True)

    try:
        from backend.core.iceberg.view import update_iceberg_view

        source = {
            "name": "cold_start::rum",
            "service_id": "cold_start",
            "duckdb_path": str(tmp_path / "cold_start.duckdb"),
            "access_level": "read_only",
        }

        # This should execute successfully and create the empty temporary views
        # client_vitals and client_errors in the read-only catalog
        update_iceberg_view(ro_con, source)

        # Confirm the views are created successfully and can be queried
        res_vitals = ro_con.execute("SELECT COUNT(*) FROM client_vitals").fetchone()
        res_errors = ro_con.execute("SELECT COUNT(*) FROM client_errors").fetchone()

        assert res_vitals[0] == 0
        assert res_errors[0] == 0

    finally:
        ro_con.close()
