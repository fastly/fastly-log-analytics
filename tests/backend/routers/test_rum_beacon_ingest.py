"""Tests for backend.routers.rum — POST /rum-beacon (unauthenticated beacon
ingest) and normalize_rum_beacons_timestamps().

/rum-beacon has no auth (it's hit directly by browsers) and must never
surface a 5xx or crash the page — every internal failure is designed to
fail open (204/404/413). These tests exercise the real SQLite metadata DB
(get_con), same pattern as test_rum_analytics.py, rather than mocking the
DB — the goal is to verify beacons are ACTUALLY persisted, not merely that
the handler returns 204.
"""

from __future__ import annotations

import datetime
import json

from fastapi.testclient import TestClient

from backend.core.metadata import get_con
from backend.main import app
from backend.routers.rum import normalize_rum_beacons_timestamps

client = TestClient(app)


def _clear(service_id: str) -> None:
    db = get_con(service_id)
    db.execute("DELETE FROM rum_beacons WHERE service_id = ?", (service_id,))
    db.commit()


def _beacon_rows(service_id: str) -> list[tuple]:
    db = get_con(service_id)
    cur = db.execute("SELECT received_at, beacon_data FROM rum_beacons WHERE service_id = ? ORDER BY id", (service_id,))
    return cur.fetchall()


# ── POST /rum-beacon ─────────────────────────────────────────────────────


def test_missing_service_id_returns_204_without_touching_db():
    r = client.post("/api/services/rum-beacon", params={"payload": json.dumps({"x": 1})})
    assert r.status_code == 204


def test_unknown_service_returns_404():
    r = client.post(
        "/api/services/rum-beacon",
        params={"service_id": "svc-definitely-not-configured-xyz", "payload": json.dumps({"x": 1})},
    )
    assert r.status_code == 404


def test_oversized_payload_rejected_with_413(monkeypatch):
    service_id = "test_rum_beacon_ingest_oversized"
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid} if sid == service_id else None)
    _clear(service_id)

    huge_payload = json.dumps({"pad": "x" * 60000})
    r = client.post("/api/services/rum-beacon", params={"service_id": service_id, "payload": huge_payload})
    assert r.status_code == 413
    assert _beacon_rows(service_id) == []


def test_valid_query_param_payload_is_persisted(monkeypatch):
    service_id = "test_rum_beacon_ingest_valid"
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid} if sid == service_id else None)
    _clear(service_id)

    beacon = {"pathname": "/checkout", "events": [{"name": "faro.performance.navigation"}]}
    r = client.post("/api/services/rum-beacon", params={"service_id": service_id, "payload": json.dumps(beacon)})
    assert r.status_code == 204

    rows = _beacon_rows(service_id)
    assert len(rows) == 1
    stored = json.loads(rows[0][1])
    assert stored["pathname"] == "/checkout"
    _clear(service_id)


def test_malformed_json_payload_is_swallowed_and_nothing_is_persisted(monkeypatch):
    service_id = "test_rum_beacon_ingest_malformed"
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid} if sid == service_id else None)
    _clear(service_id)

    r = client.post("/api/services/rum-beacon", params={"service_id": service_id, "payload": "{not valid json"})
    assert r.status_code == 204
    assert _beacon_rows(service_id) == []


def test_empty_payload_returns_204(monkeypatch):
    service_id = "test_rum_beacon_ingest_empty"
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid} if sid == service_id else None)
    _clear(service_id)

    r = client.post("/api/services/rum-beacon", params={"service_id": service_id})
    assert r.status_code == 204
    assert _beacon_rows(service_id) == []


def test_db_insert_failure_still_returns_204(monkeypatch):
    """Fail-open contract: a broken metadata DB must never surface as a
    5xx to the browser sending the beacon."""
    service_id = "test_rum_beacon_ingest_db_failure"
    monkeypatch.setattr("backend.config.load_config", lambda sid: {"service_id": sid} if sid == service_id else None)

    def _raise(_sid):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("backend.routers.rum.get_con", _raise)

    r = client.post("/api/services/rum-beacon", params={"service_id": service_id, "payload": json.dumps({"x": 1})})
    assert r.status_code == 204


# ── normalize_rum_beacons_timestamps ─────────────────────────────────────


def test_normalize_leaves_already_z_formatted_timestamps_untouched():
    service_id = "test_normalize_already_z"
    _clear(service_id)
    db = get_con(service_id)
    db.execute(
        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
        (service_id, "2026-08-05T10:00:00Z", "{}"),
    )
    db.commit()

    normalize_rum_beacons_timestamps(db)

    cur = db.execute("SELECT received_at FROM rum_beacons WHERE service_id = ?", (service_id,))
    assert cur.fetchone()[0] == "2026-08-05T10:00:00Z"
    _clear(service_id)


def test_normalize_rewrites_space_separated_timestamp_to_iso_z():
    service_id = "test_normalize_space_separated"
    _clear(service_id)
    db = get_con(service_id)
    db.execute(
        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
        (service_id, "2026-08-05 10:00:00", "{}"),
    )
    db.commit()

    normalize_rum_beacons_timestamps(db)

    cur = db.execute("SELECT received_at FROM rum_beacons WHERE service_id = ?", (service_id,))
    normalized = cur.fetchone()[0]
    assert normalized == "2026-08-05T10:00:00Z"
    _clear(service_id)


def test_normalize_leaves_unparseable_timestamp_unmodified_and_does_not_raise():
    service_id = "test_normalize_unparseable"
    _clear(service_id)
    db = get_con(service_id)
    db.execute(
        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
        (service_id, "not-a-real-timestamp", "{}"),
    )
    db.commit()

    # Must not raise even though nothing here is parseable.
    normalize_rum_beacons_timestamps(db)

    cur = db.execute("SELECT received_at FROM rum_beacons WHERE service_id = ?", (service_id,))
    assert cur.fetchone()[0] == "not-a-real-timestamp"
    _clear(service_id)


# ── GET /rum/live-events ─────────────────────────────────────────────────


def test_live_events_url_fallback_and_exception_typing():
    service_id = "test_live_events_url_fallback"
    _clear(service_id)
    db = get_con(service_id)
    beacon_with_exception = {
        "meta": {"page": {"url": "https://example.test/from-url//nested"}, "browser": "Firefox", "os": "Linux"},
        "exceptions": [{"value": "ReferenceError: x is not defined"}],
    }
    db.execute(
        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
        (service_id, "2026-08-05T10:00:00Z", json.dumps(beacon_with_exception)),
    )
    db.commit()

    r = client.get(f"/api/services/{service_id}/rum/live-events")
    assert r.status_code == 200
    events = r.json()
    assert len(events) == 1
    assert events[0]["type"] == "error"
    assert events[0]["desc"] == "ReferenceError: x is not defined"
    # Double-slash collapse via the url-derived path fallback.
    assert events[0]["path"] == "/from-url/nested"
    assert events[0]["browser"] == "Firefox"
    _clear(service_id)


def test_live_events_malformed_row_is_skipped_not_crashed():
    service_id = "test_live_events_malformed_row"
    _clear(service_id)
    db = get_con(service_id)
    db.execute(
        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
        (service_id, "2026-08-05T10:00:00Z", "{not valid json"),
    )
    db.commit()

    r = client.get(f"/api/services/{service_id}/rum/live-events")
    assert r.status_code == 200
    # No real events parsed -> falls through to the synthetic activity ticks.
    events = r.json()
    assert len(events) == 5
    _clear(service_id)


def test_live_events_db_failure_returns_empty_list(monkeypatch):
    service_id = "test_live_events_db_failure"

    def _raise(_sid):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr("backend.routers.rum.get_con", _raise)

    r = client.get(f"/api/services/{service_id}/rum/live-events")
    assert r.status_code == 200
    assert r.json() == []


def test_normalize_no_rows_needing_update_is_a_noop():
    service_id = "test_normalize_noop"
    _clear(service_id)
    db = get_con(service_id)
    now_z = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    db.execute(
        "INSERT INTO rum_beacons (service_id, received_at, beacon_data) VALUES (?, ?, ?)",
        (service_id, now_z, "{}"),
    )
    db.commit()

    # Should return early (no rows match `NOT LIKE '%Z'`) without error.
    normalize_rum_beacons_timestamps(db)

    cur = db.execute("SELECT received_at FROM rum_beacons WHERE service_id = ?", (service_id,))
    assert cur.fetchone()[0] == now_z
    _clear(service_id)
