"""Tests for ``backend.routers.dashboard``.

The dashboard router is thin — each endpoint dispatches to
``backend.repositories.dashboard`` (already covered) — but the
``/bundle`` composite has real logic for sub-response stitching that
the dedicated /aggregates + /top-bots paths don't exercise.

Repository functions are stubbed so the tests focus on the router's
own choices: HTTP shape, composite stitching, debug-key lifting, the
fields-filter short-circuit on top_bots, and CSV response packaging.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pandas as pd
import pytest


@pytest.fixture
def stub_aggregates(monkeypatch):
    """Replace the repository's get_aggregates with a deterministic stub.

    Yields the stub so tests can assert on call args / change the return.
    """
    stub = MagicMock(
        return_value={
            "totals": {"requests": 100, "errors_5xx": 2},
            "section_timings": [{"section": "agg:query", "time_ms": 5.0}],
            "debug_queries": [{"sql": "SELECT 1", "time_ms": 1.2, "rows": 1}],
            "debug_calls": [{"caller": "x", "fastly_calls": 0}],
        }
    )
    monkeypatch.setattr("backend.repositories.dashboard.get_aggregates", stub)
    return stub


@pytest.fixture
def stub_top_bots(monkeypatch):
    stub = MagicMock(
        return_value={
            "bots": [{"name": "Googlebot", "count": 42}],
            "ngwaf_bots": [],
            "section_timings": [{"section": "bots:query", "time_ms": 3.0}],
            "debug_queries": [{"sql": "SELECT bot", "time_ms": 0.5, "rows": 1}],
            "debug_calls": [],
        }
    )
    monkeypatch.setattr("backend.repositories.security.get_top_bots", stub)
    return stub


# ── /api/dashboard/bundle ─────────────────────────────────────────────────────


def test_bundle_returns_both_subresponses(client, stub_aggregates, stub_top_bots):
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "aggregates" in body
    assert "top_bots" in body
    # Composite emits its own top-level _section_timings tracking both
    # sub-queries' wall-clock.
    assert "_section_timings" in body
    sections = [s["section"] for s in body["_section_timings"]]
    assert "bundle:aggregates" in sections
    assert "bundle:top_bots" in sections


def test_bundle_short_circuits_top_bots_when_fields_filter_excludes(client, stub_aggregates, stub_top_bots):
    """When the request's fields filter doesn't include _bot_name /
    _ngwaf_bot_name, top_bots returns the empty-list shape without
    even hitting the repository (saves the COUNT scan)."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "fields": ["country", "url"],  # no _bot_name / _ngwaf_bot_name
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["top_bots"] == {"bots": [], "ngwaf_bots": []}
    # Stub never invoked because the short-circuit fired first.
    stub_top_bots.assert_not_called()
    # Aggregates still ran.
    stub_aggregates.assert_called_once()


def test_bundle_calls_top_bots_when_bot_field_requested(client, stub_aggregates, stub_top_bots):
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
            "fields": ["country", "_bot_name"],
        },
    )
    assert resp.status_code == 200
    stub_top_bots.assert_called_once()


def test_bundle_lifts_debug_keys_into_top_level(client, stub_aggregates, stub_top_bots):
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
        },
    )
    body = resp.json()
    # debug_queries from BOTH sub-responses concatenated under the
    # underscored top-level key (frontend DebugPanel reads this).
    assert "_debug_queries" in body
    assert len(body["_debug_queries"]) == 2  # one from aggregates, one from top_bots
    sql_texts = [q["sql"] for q in body["_debug_queries"]]
    assert "SELECT 1" in sql_texts
    assert "SELECT bot" in sql_texts


def test_bundle_renames_subresponse_section_timings(client, stub_aggregates, stub_top_bots):
    """The composite has no response_model, so the rename from bare
    `section_timings` → `_section_timings` has to happen in the router
    body (Pydantic's serialization_alias does it for the dedicated
    endpoints). Pin that rename."""
    resp = client.post(
        "/api/dashboard/bundle",
        json={
            "start_time": "2026-06-12T00:00:00Z",
            "end_time": "2026-06-12T01:00:00Z",
            "filters": {},
            "chart_metric": "requests",
            "chart_interval": "minute",
        },
    )
    body = resp.json()
    # Each sub-response now exposes _section_timings (renamed), not the
    # original bare key.
    assert "_section_timings" in body["aggregates"]
    assert "section_timings" not in body["aggregates"]
    assert "_section_timings" in body["top_bots"]


# ── /api/dashboard/raw/csv ───────────────────────────────────────────────────


def test_raw_csv_returns_csv_attachment(client, monkeypatch):
    df = pd.DataFrame({"timestamp": ["2026-06-12T00:00:00Z"], "status": [200]})
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: df)

    resp = client.post(
        "/api/dashboard/raw/csv",
        json={"start_time": "2026-06-12T00:00:00Z", "end_time": "2026-06-12T01:00:00Z", "filters": {}},
    )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "attachment" in resp.headers.get("content-disposition", "")
    # Header + 1 data row.
    text = resp.text
    assert "timestamp,status" in text
    assert "200" in text


def test_raw_csv_returns_empty_body_when_no_rows(client, monkeypatch):
    monkeypatch.setattr("backend.repositories.dashboard.get_raw_df", lambda **kw: pd.DataFrame())

    resp = client.post(
        "/api/dashboard/raw/csv",
        json={"start_time": "2026-06-12T00:00:00Z", "end_time": "2026-06-12T01:00:00Z", "filters": {}},
    )

    assert resp.status_code == 200
    assert resp.text == ""
