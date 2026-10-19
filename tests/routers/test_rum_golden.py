"""Golden-output pins for the RUM analytics / live-events wire bodies.

The SQL behind these routes lives in ``backend/repositories/rum.py``; these tests
pin the JSON a fixed fixture produces so any SQL relocation or refactor is provably
byte-identical. Regenerate deliberately with ``UPDATE_GOLDEN=1``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
from fastapi.testclient import TestClient

from backend.main import app
from tests.routers import test_rum_analytics as _base

client = TestClient(app)
_insert_beacons = _base._insert_beacons
setup_temp_rum_db = _base.setup_temp_rum_db
GOLDEN_DIR = Path(__file__).parent / "golden"
SERVICE_ID = "svc-rum-golden"
WINDOW = "start_time=2026-08-05T00:00:00Z&end_time=2026-08-05T23:59:59Z"


@pytest.fixture
def rum_db(request):
    return request.getfixturevalue("setup_temp_rum_db")


@pytest.fixture(autouse=True)
def _skip_view_update():
    with patch("backend.core.iceberg.view.update_iceberg_view"):
        yield


def _beacons() -> list[dict]:
    out: list[dict] = []
    specs = [
        ("10:05:00", "/home", "Chrome", "macOS", "Desktop", 1.2, 2100.0, 0.05, 150.0, False),
        ("10:20:00", "/home", "Firefox", "Windows", "Desktop", 2.4, 3400.0, 0.15, 320.0, False),
        ("10:40:00", "/cart", "Chrome", "Android", "Mobile", 3.1, 4800.0, 0.3, 650.0, True),
        ("11:10:00", "/cart", "Safari", "macOS", "Desktop", 0.9, 1800.0, 0.02, 90.0, True),
        ("11:50:00", "/search", "Chrome", "Windows", "Desktop", 1.7, 2600.0, 0.11, 210.0, False),
        ("13:30:00", "/home", "Chrome", "macOS", "Desktop", 1.1, 2000.0, 0.04, 120.0, False),
    ]
    for i, (hms, path, browser, os_, device, load, lcp, cls, inp, err) in enumerate(specs):
        b = {
            "received_at": f"2026-08-05T{hms}+00:00",
            "pathname": path,
            "meta": {"browser": browser, "os": os_, "device": device},
            "load_time": load,
            "lcp": lcp,
            "cls": cls,
            "inp": inp,
            "cid": f"cid_{i}",
            "req_id": f"req_{i}",
        }
        if err:
            n = 2 if i == 2 else 1
            b["exceptions"] = [{"value": f"Boom {i}", "filename": "app.js"}] * n
        out.append(b)
    return out


def _check(name: str, body) -> None:
    if isinstance(body, dict):
        body = {k: v for k, v in body.items() if not k.startswith("_debug")}
    path = GOLDEN_DIR / f"rum_{name}.json"
    rendered = json.dumps(body, indent=2, sort_keys=True) + "\n"
    if os.getenv("UPDATE_GOLDEN") == "1":
        path.write_text(rendered)
    assert path.exists(), f"missing golden {path.name}; run with UPDATE_GOLDEN=1"
    assert json.loads(path.read_text()) == json.loads(rendered)


def _get(url: str):
    r = client.get(url)
    assert r.status_code == 200, r.text
    return r.json()


def test_golden_empty(rum_db) -> None:
    _check("analytics_empty", _get(f"/api/services/{SERVICE_ID}/rum/analytics?{WINDOW}"))


def test_golden_raw_long_window(rum_db) -> None:
    _insert_beacons(rum_db, _beacons())
    _check("analytics_raw_long", _get(f"/api/services/{SERVICE_ID}/rum/analytics?{WINDOW}"))


def test_golden_raw_short_window(rum_db) -> None:
    _insert_beacons(rum_db, _beacons())
    url = f"/api/services/{SERVICE_ID}/rum/analytics?start_time=2026-08-05T10:00:00Z&end_time=2026-08-05T11:30:00Z"
    _check("analytics_raw_short", _get(url))


def test_golden_filtered(rum_db) -> None:
    _insert_beacons(rum_db, _beacons())
    flt = json.dumps({"browser": {"mode": "include", "values": ["Chrome"]}})
    params = {"start_time": "2026-08-05T00:00:00Z", "end_time": "2026-08-05T23:59:59Z", "filters": flt}
    resp = client.get(f"/api/services/{SERVICE_ID}/rum/analytics", params=params)
    assert resp.status_code == 200, resp.text
    _check("analytics_filtered", resp.json())


def test_golden_rollup(rum_db) -> None:
    import backend.core.duckdb_pool as duckdb_pool
    from backend.core.rollups.rum import recompute_rum_aggregates

    _insert_beacons(rum_db, _beacons())
    duckdb_pool.shutdown_all()
    con = duckdb.connect(str(rum_db), read_only=False)
    try:
        recompute_rum_aggregates(con, SERVICE_ID)
        assert con.execute("SELECT COUNT(*) FROM rum_vitals_aggregates").fetchone()[0] > 0
    finally:
        con.close()
    _check("analytics_rollup", _get(f"/api/services/{SERVICE_ID}/rum/analytics?{WINDOW}"))


def test_golden_live_events(rum_db) -> None:
    # One metric (or one error) per timestamp: live-events orders by timestamp only, so
    # same-timestamp rows would make the golden order nondeterministic.
    beacons = []
    for i, b in enumerate(_beacons()):
        base = {k: b[k] for k in ("pathname", "meta", "cid", "req_id")}
        beacons.append({**base, "received_at": b["received_at"], "lcp": b["lcp"]})
        if i % 2:
            beacons.append(
                {**base, "received_at": b["received_at"].replace(":00+", ":30+"), "exceptions": [{"value": f"Err {i}"}]}
            )
    _insert_beacons(rum_db, beacons)
    _check("live_events", {"events": _get(f"/api/services/{SERVICE_ID}/rum/live-events?{WINDOW}")})


def test_golden_beacon_health_disabled(rum_db) -> None:
    _check("beacon_health_disabled", _get(f"/api/services/{SERVICE_ID}/rum/beacon-health"))
