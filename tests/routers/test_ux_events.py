"""POST /api/ux-events — client UX telemetry collector."""

from __future__ import annotations

from fastapi.testclient import TestClient

from backend.main import app

client = TestClient(app)


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
