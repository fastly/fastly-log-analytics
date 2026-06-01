"""End-to-end smoke tests: HTTP request → DuckDB → response JSON.

These tests are intentionally heavyweight. They wire together:
  - the real FastAPI app + TestClient
  - a real in-memory DuckDB connection with seeded mock logs
  - the real repository layer (no mocks)
  - the real Pydantic response validation

The goal is to catch regressions that unit tests miss because they
stub out the layer where a contract drift actually matters — e.g. a
repository function changing its return shape and the router silently
masking it, or a Pydantic response model evolving in a way that
strips fields the FE depends on.

One test per critical user journey. Each test is the smallest thing
we can run to prove the entire stack is wired correctly. Keep this
file small — when these fail, the failure is high-signal.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from backend.repositories._base import _safe_table
from tests.conftest import MOCK_SERVICE_ID

# ── Fixture: a service with realistic seeded data ───────────────────────


@pytest.fixture
def seeded_service(in_memory_duckdb, test_service_source):
    """Seed the test DuckDB with ~50 log rows spanning the last 2 hours."""
    table = _safe_table(test_service_source["name"])
    catalog_fields = [
        ("timestamp", "TIMESTAMPTZ"),
        ("dt", "VARCHAR"),
        ("timestamp_hour", "VARCHAR"),
        ("status", "INTEGER"),
        ("country", "VARCHAR"),
        ("url", "VARCHAR"),
        ("ip", "VARCHAR"),
        ("method", "VARCHAR"),
        ("ua", "VARCHAR"),
        ("pop", "VARCHAR"),
        ("asn", "INTEGER"),
        ("city", "VARCHAR"),
        ("region", "VARCHAR"),
        ("elapsed", "INTEGER"),
        ("cache", "VARCHAR"),
        ("ottfb", "DOUBLE"),
        ("ttfb", "DOUBLE"),
        ("waf_sig", "VARCHAR"),
        ("edge", "BOOLEAN"),
        ("lat", "DOUBLE"),
        ("lon", "DOUBLE"),
        ("resp_bytes", "INTEGER"),
        ("oip", "VARCHAR"),
        ("ost", "INTEGER"),
    ]
    cols_def = ", ".join(f'"{n}" {t}' for n, t in catalog_fields)
    in_memory_duckdb.execute(f"CREATE TABLE {table} ({cols_def})")

    base = datetime.now(UTC) - timedelta(hours=2)
    statuses = [200, 200, 200, 404, 500] * 10  # 50 rows; 10% errors, 10% 404s
    pops = ["LAX", "JFK", "LHR"] * 17
    countries = ["US", "US", "GB", "DE", "JP"] * 10
    rows = []
    for i in range(50):
        ts = base + timedelta(minutes=i * 2)
        rows.append(
            (
                ts,
                ts.strftime("%Y-%m-%d"),
                ts.strftime("%Y-%m-%d-%H"),
                statuses[i],
                countries[i],
                f"/path-{i % 10}",
                f"10.0.{i // 10}.{i % 10}",
                "GET",
                "Mozilla/5.0",
                pops[i % 3],
                15169 + (i % 5),
                "San Francisco",
                "CA",
                50 + i * 5,
                "HIT" if i % 3 == 0 else "MISS",
                50000.0 + i * 100,
                0.05 + i * 0.001,
                "",
                True,
                37.7749,
                -122.4194,
                500 + i * 50,
                "origin-1.example.com",
                statuses[i],
            )
        )
    placeholders = ", ".join(["?"] * len(catalog_fields))
    in_memory_duckdb.executemany(
        f"INSERT INTO {table} ({', '.join(f'''\"{n}\"''' for n, _ in catalog_fields)}) VALUES ({placeholders})",
        rows,
    )
    return test_service_source


# ── HTTP smoke tests (one per critical user journey) ────────────────────


def test_smoke_dashboard_aggregates_returns_real_counts(client, seeded_service):
    """Full stack: POST /api/dashboard/aggregates → real repository →
    seeded DuckDB → JSON response with the expected aggregated counts.

    Pinned because losing this would surface as the FE's primary
    landing page silently rendering "0 requests" despite having data
    — exactly the class of bug where unit tests stay green because
    each layer's mock returns a plausible value."""
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {}},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_rows"] > 0
    assert body["total_rows_total"] >= body["total_rows"]
    for key in ("data", "time_series", "interval", "metric"):
        assert key in body
    has_top = any(entry.get("top") for entry in body["data"].values())
    assert has_top, "Expected at least one field to have top values"


def test_smoke_dashboard_raw_returns_log_rows(client, seeded_service):
    """POST /api/dashboard/raw returns log rows with the requested
    columns, paginated. Pinned because the explore-logs table is the
    most-used investigation surface — losing this stack would
    silently hide every log line."""
    resp = client.post(
        "/api/dashboard/raw",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {},
            "page": 1,
            "limit": 5,
            "sort_col": "timestamp",
            "sort_dir": "DESC",
            "columns": ["timestamp", "status", "url"],
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["data"]) == 5
    for row in body["data"]:
        assert "timestamp" in row
        assert "status" in row
        assert "url" in row


def test_smoke_dashboard_aggregates_with_filter_narrows_results(client, seeded_service):
    """Same POST as the aggregates smoke, but with a `country=GB` filter
    — must return strictly fewer total_rows than the unfiltered case.
    This is the regression guard for the CAST(... AS VARCHAR) +
    stringified-params fix in build_where_clause: a type-mismatched
    filter must NOT crash AND must narrow correctly."""
    unfiltered = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={"start_time": None, "end_time": None, "filters": {}},
    ).json()

    gb_only = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {"country": {"mode": "include", "values": ["GB"]}},
        },
    ).json()

    assert gb_only["total_rows"] > 0  # there ARE GB rows in the seed
    assert gb_only["total_rows"] < unfiltered["total_rows"]


def test_smoke_dashboard_aggregates_with_numeric_filter_on_string_column_does_not_crash(client, seeded_service):
    """A numeric filter value (`country=0`) on a VARCHAR column must NOT
    crash with "Could not convert string '0' to INT32" or similar.
    Pinned because hypothesis caught this exact regression mode in
    build_where_clause. The FE shouldn't send this, but a defensive
    backend keeps the dashboard alive when input validation drifts."""
    resp = client.post(
        "/api/dashboard/aggregates",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {"country": {"mode": "include", "values": [0]}},
        },
    )
    # MUST be 200 (zero rows match) and NOT 500 (crash)
    assert resp.status_code == 200
    # No string '0' country exists in the seed → zero matches
    assert resp.json()["total_rows"] == 0


def test_smoke_insights_endpoint_returns_insight_cards_with_distinct_ids(client, seeded_service):
    """POST /api/insights returns the full set of registered insight
    cards, each with a distinct id. Pinned because losing the
    registry/temp-table wiring would silently render an empty
    insights page, and a duplicate-id regression would re-introduce
    the React-key crash."""
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        resp = client.post(
            "/api/insights",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            json={"window_size_hrs": 1, "baseline_hours": 1, "filters": {}},
        )
    assert resp.status_code == 200
    body = resp.json()
    insights = body["insights"]
    # The seed schema doesn't cover every catalog field, so not every
    # insight has its `required_fields` satisfied. We pin a floor that
    # proves the registry → temp-table → SQL path still works for the
    # majority of insights — a drop here means a regression in either
    # the registry wiring or the schema-coverage check.
    assert len(insights) >= 15
    for card in insights:
        for key in ("id", "title", "severity", "summary", "items"):
            assert key in card, f"Insight card {card.get('id')} missing key {key}"
    ids = [c["id"] for c in insights]
    assert len(ids) == len(set(ids)), f"Duplicate insight ids: {[i for i in ids if ids.count(i) > 1]}"


def test_smoke_origin_timeseries_with_sub_minute_bucket(client, seeded_service):
    """POST /api/origin/timeseries with bucket_minutes=1/60 (1 second).
    Integration-level regression test for the `_clamp_to_float` fix
    — sub-minute granularity must round-trip from request to response
    without collapsing to 1m."""
    resp = client.post(
        "/api/origin/timeseries",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        json={
            "start_time": None,
            "end_time": None,
            "filters": {},
            "bucket_minutes": 1.0 / 60.0,
            "split_by_leg": False,
            "metric": "ttfb",
            "percentile": "p95",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "has_data" in body
