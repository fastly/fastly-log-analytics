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
