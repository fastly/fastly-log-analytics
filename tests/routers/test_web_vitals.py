"""POST /api/web-vitals — client perf telemetry collector."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from backend.core import web_vitals_store
from backend.main import app

client = TestClient(app)


def test_web_vitals_accepts_well_formed_payload():
    r = client.post(
        "/api/web-vitals",
        json={
            "id": "v1-1234",
            "name": "LCP",
            "value": 1850.5,
            "rating": "good",
            "pathname": "/dashboard",
            "navigation_type": "navigate",
            "delta": 1850.5,
        },
    )
    assert r.status_code == 200, r.text
    # Admin responses get _debug_calls / _debug_queries / _is_cached
    # injected by the middleware when DEBUG_RESPONSES is on; just assert
    # the contract field exists.
    assert r.json().get("ok") is True


def test_web_vitals_rejects_unknown_metric_name():
    """Pydantic Literal enforces the SDK's known-name set so a typo
    can't pollute the log stream with garbage metric names."""
    r = client.post(
        "/api/web-vitals",
        json={
            "id": "v1-bad",
            "name": "NOT_A_REAL_METRIC",
            "value": 0,
            "rating": "good",
        },
    )
    assert r.status_code == 422


def test_web_vitals_rejects_unknown_rating():
    r = client.post(
        "/api/web-vitals",
        json={
            "id": "v1-bad-rating",
            "name": "CLS",
            "value": 0.05,
            "rating": "amazing",  # invalid
        },
    )
    assert r.status_code == 422


def test_web_vitals_accepts_optional_fields_missing():
    """pathname / navigation_type / delta are optional — many SDK
    deliveries omit them (notably FCP / TTFB don't carry navigation_type
    in older browsers)."""
    r = client.post(
        "/api/web-vitals",
        json={
            "id": "v1-minimal",
            "name": "TTFB",
            "value": 220,
            "rating": "good",
        },
    )
    assert r.status_code == 200


def test_web_vitals_collection_off_by_default_persists_nothing(monkeypatch, tmp_path):
    """With WEB_VITALS_COLLECT unset the endpoint accepts the POST (200)
    but writes no sample — collection is opt-in."""
    monkeypatch.delenv("WEB_VITALS_COLLECT", raising=False)
    sink = tmp_path / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)

    r = client.post(
        "/api/web-vitals",
        json={"id": "off-1", "name": "LCP", "value": 1200, "rating": "good", "pathname": "/dashboard"},
    )
    assert r.status_code == 200
    assert r.json().get("ok") is True
    assert not sink.exists()


def test_web_vitals_collection_on_persists_one_jsonl_line(monkeypatch, tmp_path):
    """With WEB_VITALS_COLLECT=1 the endpoint appends a flat, analyzer-
    friendly JSON line carrying the metric fields + a timestamp + cohort."""
    monkeypatch.setenv("WEB_VITALS_COLLECT", "1")
    sink = tmp_path / "web_vitals.jsonl"
    monkeypatch.setattr(web_vitals_store, "LOG_PATH", sink)

    r = client.post(
        "/api/web-vitals",
        json={
            "id": "on-1",
            "name": "LCP",
            "value": 2412.0,
            "rating": "good",
            "pathname": "/dashboard",
            "navigation_type": "navigate",
            "delta": 2412.0,
        },
    )
    assert r.status_code == 200

    lines = sink.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["name"] == "LCP"
    assert rec["value"] == 2412.0
    assert rec["rating"] == "good"
    assert rec["pathname"] == "/dashboard"
    # Loopback test client is classified as admin; ts is stamped server-side.
    assert rec["cohort"] == "admin"
    assert rec["ts"].endswith("Z")


def test_page_telemetry_endpoint(monkeypatch, tmp_path):
    from backend import config as svcconfig
    from backend.core.metadata import usage_log, usage_log_db

    monkeypatch.setattr(usage_log_db, "_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(svcconfig, "get_active_service_id", lambda: "demo_service")

    usage_log.log_telemetry_query("demo_service", "page-456", "req-789", "duckdb", "SELECT 42", 12.3)

    response = client.get("/api/debug/page-telemetry?page_load_id=page-456")
    assert response.status_code == 200
    data = response.json()
    assert "queries" in data
    assert data["queries"][0]["sql_query"] == "SELECT 42"


# ── POST /api/ux-events ───────────────────────────────────────────────────


def test_ux_events_accepts_well_formed_payload():
    r = client.post(
        "/api/ux-events",
        json={
            "event": "column_reordered",
            "pathname": "/sessions",
            "component_id": "Live Sessions",
            "details": {"column_id": "ja4", "from_index": 4, "to_index": 1, "column_count": 9},
        },
    )
    assert r.status_code == 200, r.text
    assert r.json().get("ok") is True


def test_ux_events_accepts_minimal_payload():
    """Only ``event`` is required — pathname / component_id / details
    are optional so the caller can opt out of context per-event."""
    r = client.post("/api/ux-events", json={"event": "filter_cleared"})
    assert r.status_code == 200, r.text


def test_ux_events_rejects_missing_event_field():
    r = client.post("/api/ux-events", json={"pathname": "/dashboard"})
    assert r.status_code == 422


def test_ux_events_rejects_extra_top_level_field():
    """``extra="forbid"`` so a typo in the SPA caller surfaces as a 422
    rather than silently landing in the log under an unread key."""
    r = client.post(
        "/api/ux-events",
        json={"event": "column_reordered", "typo_field": "ignored"},
    )
    assert r.status_code == 422


def test_ux_events_caps_event_name_length():
    """80-char cap keeps a misbehaving client from pumping arbitrary
    strings into the log stream."""
    r = client.post("/api/ux-events", json={"event": "x" * 81})
    assert r.status_code == 422


def test_ux_events_logs_admin_cohort_for_loopback_request(caplog):
    """Loopback request (no analyst session) is tagged ``admin`` so log
    slicing can split per-cohort."""
    import logging

    with caplog.at_level(logging.INFO, logger="backend.ux_events"):
        r = client.post("/api/ux-events", json={"event": "column_reordered"})
    assert r.status_code == 200
    matched = [rec for rec in caplog.records if rec.message == "ux_event"]
    assert matched, "expected at least one ux_event log record"
    assert getattr(matched[-1], "ux_cohort", None) == "admin"
    assert getattr(matched[-1], "ux_event", None) == "column_reordered"
