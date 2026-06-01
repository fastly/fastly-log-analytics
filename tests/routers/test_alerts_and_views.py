"""HTTP-layer contract tests for alerts and saved-views mutation endpoints.

Operational state lives in per-service SQLite via ``backend.core.metadata_db``;
the ``isolate_metadata_db`` autouse fixture in conftest.py points the module
at a tmp_path so each test gets a fresh per-service file.

Covers:
  POST   /api/alerts/
  PATCH  /api/alerts/{alert_id}/enabled
  DELETE /api/alerts/{alert_id}
  POST   /api/views/
  DELETE /api/views/{view_id}
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from backend.core import metadata_db

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SERVICE_ID = "test-service-id"

_ALERT_BODY = {
    "service_id": _SERVICE_ID,
    "name": "High 5xx Rate",
    "category": "reliability",
    "metric": "5xx_rate",
    "evaluation_type": "absolute",
    "evaluation_scope": "all",
    "operator": ">",
    "threshold": 5.0,
    "window_min": 5.0,
}

_VIEW_BODY = {
    "service_id": _SERVICE_ID,
    "name": "My View",
    "filters_json": '{"status": [500]}',
    "page": "dashboard",
}


def _alert_row(alert_id: str) -> dict | None:
    con = metadata_db.get_con(_SERVICE_ID)
    row = con.execute(
        "SELECT name, metric, threshold, enabled FROM alerts WHERE id = ?",
        (alert_id,),
    ).fetchone()
    return dict(row) if row else None


def _view_row(view_id: str) -> dict | None:
    con = metadata_db.get_con(_SERVICE_ID)
    row = con.execute(
        "SELECT name, filters_json, page FROM views WHERE id = ?",
        (view_id,),
    ).fetchone()
    return dict(row) if row else None


# ---------------------------------------------------------------------------
# Alerts: POST
# ---------------------------------------------------------------------------


def test_create_alert_persists_to_db(client):
    response = client.post("/api/alerts/", json=_ALERT_BODY, headers={"x-fastly-service-id": _SERVICE_ID})

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["status"] == "success"
    alert_id = data["id"]
    assert alert_id

    row = _alert_row(alert_id)
    assert row is not None
    assert row["name"] == "High 5xx Rate"
    assert row["metric"] == "5xx_rate"
    assert row["threshold"] == pytest.approx(5.0)


def test_create_alert_returns_correct_fields(client):
    response = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "name": "Latency Alert", "metric": "p95_latency"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 200
    data = response.json()["data"]
    assert data["status"] == "success"
    assert data["id"]


def test_create_alert_invalid_metric_returns_422(client):
    response = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "metric": "nonexistent_metric"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 422


def test_create_alert_invalid_operator_returns_422(client):
    response = client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "operator": "!="},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Alerts: PATCH enabled
# ---------------------------------------------------------------------------


def _seed_alert(client) -> str:
    resp = client.post("/api/alerts/", json=_ALERT_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    assert resp.status_code == 200
    return resp.json()["data"]["id"]


def test_toggle_alert_disabled(client):
    alert_id = _seed_alert(client)

    response = client.patch(
        f"/api/alerts/{alert_id}/enabled",
        json={"enabled": False},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )

    assert response.status_code == 200
    row = _alert_row(alert_id)
    assert row is not None
    assert row["enabled"] == 0


def test_toggle_alert_re_enabled(client):
    alert_id = _seed_alert(client)
    client.patch(
        f"/api/alerts/{alert_id}/enabled",
        json={"enabled": False},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    response = client.patch(
        f"/api/alerts/{alert_id}/enabled",
        json={"enabled": True},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )

    assert response.status_code == 200
    row = _alert_row(alert_id)
    assert row is not None
    assert row["enabled"] == 1


def test_toggle_alert_enabled_field_required(client):
    alert_id = _seed_alert(client)
    response = client.patch(
        f"/api/alerts/{alert_id}/enabled",
        json={},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Alerts: DELETE
# ---------------------------------------------------------------------------


def test_delete_alert_removes_from_db(client):
    alert_id = _seed_alert(client)

    response = client.delete(f"/api/alerts/{alert_id}", headers={"x-fastly-service-id": _SERVICE_ID})

    assert response.status_code == 200
    assert response.json()["data"]["status"] == "success"

    assert _alert_row(alert_id) is None


def test_delete_alert_idempotent(client):
    """Deleting a non-existent alert should still return 200 (no-op)."""
    response = client.delete("/api/alerts/non-existent-id", headers={"x-fastly-service-id": _SERVICE_ID})
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Alerts: POST /preview
# ---------------------------------------------------------------------------
#
# ``preview_alert`` builds a time-bucketed query against the service's
# DuckDB-backed logs table and returns ``{times, values, type}``. The bucket
# granularity is picked from ``lookback_hours``; ``relative_*`` evaluation
# types also pull a historical series shifted by ``comparison_period_min``.


def _seed_preview_table(con, *, name: str = "logs_test_service", row_count: int = 10) -> None:
    """Create the table ``preview_alert`` will query, seeded with rows in
    the last 5 minutes so the default lookback covers them."""
    con.execute(
        f"CREATE TABLE {name} ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    rows = [
        (now - timedelta(seconds=i * 10), 200 if i % 2 == 0 else 500, i % 2 == 0, 50.0, 100, "HIT", 1024, 200, 100)
        for i in range(row_count)
    ]
    con.executemany(
        f"INSERT INTO {name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )


def _preview_body(**overrides) -> dict:
    body = {
        "service_id": _SERVICE_ID,
        "name": "Preview Alert",
        "category": "reliability",
        "metric": "5xx",
        "evaluation_type": "absolute",
        "evaluation_scope": "all",
        "operator": ">",
        "threshold": 1.0,
        "window_min": 5.0,
    }
    body.update(overrides)
    return body


def test_preview_alert_returns_series_for_absolute_metric(client, in_memory_duckdb):
    """Happy path: a seeded logs table → preview returns aligned times/values."""
    _seed_preview_table(in_memory_duckdb)

    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=_preview_body())

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["type"] == "absolute"
    assert "times" in data and "values" in data
    assert len(data["times"]) == len(data["values"])
    # Some 5xx in the seeded data → at least one non-zero value
    assert sum(data["values"]) > 0


def test_preview_alert_unknown_service_returns_404(client):
    """No source → 404 (don't silently build a query against a missing table)."""
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        resp = client.post("/api/alerts/preview", json=_preview_body())
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Service not found"


def test_preview_alert_relative_evaluation_returns_hist_values(client, in_memory_duckdb):
    """``relative_increase`` returns a parallel ``hist_values`` series shifted
    by ``comparison_period_min`` — needed to render the comparison overlay."""
    _seed_preview_table(in_memory_duckdb)

    body = _preview_body(
        evaluation_type="relative_increase",
        comparison_period_min=60,
        threshold=1.5,
    )
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview?lookback_hours=2", json=body)

    assert resp.status_code == 200
    data = resp.json()["data"]
    assert data["type"] == "relative"
    assert "hist_values" in data
    assert len(data["hist_values"]) == len(data["values"])


def test_preview_alert_edge_scope_filters_to_edge_rows(client, in_memory_duckdb):
    """``evaluation_scope='edge'`` must only count rows where ``edge=true``."""
    in_memory_duckdb.execute(
        "CREATE TABLE logs_test_service ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    # 4 edge=false 500s, 0 edge=true 500s — scope=edge must report 0
    in_memory_duckdb.executemany(
        "INSERT INTO logs_test_service VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [(now - timedelta(seconds=i), 500, False, 50.0, 100, "MISS", 1024, 200, 100) for i in range(4)],
    )

    body = _preview_body(evaluation_scope="edge")
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=body)

    assert resp.status_code == 200
    assert sum(resp.json()["data"]["values"]) == 0


def test_preview_alert_origin_scope_filters_to_origin_rows(client, in_memory_duckdb):
    """``evaluation_scope='origin'`` mirrors edge — must only count
    ``edge=false`` rows. Pinned because the WHERE-clause assembly is hand-
    rolled per scope; a typo would flip the meaning silently."""
    in_memory_duckdb.execute(
        "CREATE TABLE logs_test_service ("
        "timestamp TIMESTAMPTZ, status INTEGER, edge BOOLEAN, ottfb DOUBLE, "
        "elapsed BIGINT, cache VARCHAR, resp_bytes BIGINT, req_bytes BIGINT, req_header_bytes BIGINT)"
    )
    now = datetime.now(UTC)
    in_memory_duckdb.executemany(
        "INSERT INTO logs_test_service VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        # All 500s on edge=true → origin-scoped count is 0
        [(now - timedelta(seconds=i), 500, True, 50.0, 100, "MISS", 1024, 200, 100) for i in range(3)],
    )

    body = _preview_body(evaluation_scope="origin")
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=body)

    assert resp.status_code == 200
    assert sum(resp.json()["data"]["values"]) == 0


def test_preview_alert_query_failure_returns_error_field(client, in_memory_duckdb):
    """If the generated SQL fails (e.g. table doesn't exist) the endpoint
    must return a 200 with ``error`` set — the UI displays the error
    inline rather than treating it as a transport failure."""
    # No table created — query will raise; preview should return error
    with patch(
        "backend.core.duckdb.get_source_for_service",
        return_value={"name": "test_service", "service_id": _SERVICE_ID},
    ):
        resp = client.post("/api/alerts/preview", json=_preview_body())

    assert resp.status_code == 200
    body = resp.json()
    assert body.get("data") is None
    assert body.get("error")  # populated with the exception message


@pytest.mark.parametrize(
    "lookback_hours,expected_interval",
    [
        (1, "1 minute"),  # <= 180 min → 1m buckets
        (12, "15 minutes"),  # <= 1440 min → 15m buckets
        (48, "1 hour"),  # > 1440 min → 1h buckets
    ],
)
def test_preview_alert_bucket_granularity_scales_with_lookback(
    client, in_memory_duckdb, lookback_hours, expected_interval
):
    """The bucket size cascade (1m / 15m / 1h) is purely lookback-driven;
    each branch is exercised so a refactor of the thresholds doesn't
    silently widen buckets on the short-window dashboards."""
    _seed_preview_table(in_memory_duckdb, row_count=5)
    captured: dict = {}

    from backend.utils import telemetry as _telemetry

    real_track = _telemetry.track_query

    def _spy(con, q, params, label):
        captured.setdefault("q", q)
        return real_track(con, q, params, label)

    # ``preview_alert`` imports ``track_query`` inside the function body,
    # so patching ``backend.utils.telemetry.track_query`` (the source
    # module) is what reaches the call site.
    with (
        patch(
            "backend.core.duckdb.get_source_for_service",
            return_value={"name": "test_service", "service_id": _SERVICE_ID},
        ),
        patch("backend.utils.telemetry.track_query", _spy),
    ):
        resp = client.post(f"/api/alerts/preview?lookback_hours={lookback_hours}", json=_preview_body())

    assert resp.status_code == 200
    assert expected_interval in captured["q"]


# ---------------------------------------------------------------------------
# Views: POST
# ---------------------------------------------------------------------------


def test_create_view_persists_to_db(client):
    response = client.post("/api/views/", json=_VIEW_BODY, headers={"x-fastly-service-id": _SERVICE_ID})

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "success"
    view_id = data["id"]
    assert view_id

    row = _view_row(view_id)
    assert row is not None
    assert row["name"] == "My View"
    assert row["page"] == "dashboard"


def test_create_view_returns_id(client):
    response = client.post("/api/views/", json=_VIEW_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    assert response.status_code == 200
    assert response.json()["id"]


def test_create_view_missing_required_field_returns_422(client):
    response = client.post(
        "/api/views/",
        json={"service_id": "svc1"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Views: DELETE
# ---------------------------------------------------------------------------


def _seed_view(client) -> str:
    resp = client.post("/api/views/", json=_VIEW_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    assert resp.status_code == 200
    return resp.json()["id"]


def test_delete_view_removes_from_db(client):
    view_id = _seed_view(client)

    response = client.delete(f"/api/views/{view_id}", headers={"x-fastly-service-id": _SERVICE_ID})

    assert response.status_code == 200
    assert response.json()["status"] == "success"

    assert _view_row(view_id) is None


def test_delete_view_idempotent(client):
    """Deleting a non-existent view should still return 200."""
    response = client.delete("/api/views/nonexistent-view-id", headers={"x-fastly-service-id": _SERVICE_ID})
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Round-trip: create then list
# ---------------------------------------------------------------------------


def test_list_alerts_after_create(client):
    client.post("/api/alerts/", json=_ALERT_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    client.post(
        "/api/alerts/",
        json={**_ALERT_BODY, "name": "Second Alert"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )

    response = client.get(f"/api/alerts/{_SERVICE_ID}")

    assert response.status_code == 200
    data = response.json()["data"]
    names = [a["name"] for a in data]
    assert "High 5xx Rate" in names
    assert "Second Alert" in names


def test_list_views_after_create(client):
    client.post("/api/views/", json=_VIEW_BODY, headers={"x-fastly-service-id": _SERVICE_ID})
    client.post(
        "/api/views/",
        json={**_VIEW_BODY, "name": "Second View"},
        headers={"x-fastly-service-id": _SERVICE_ID},
    )

    response = client.get(f"/api/views/{_SERVICE_ID}")

    assert response.status_code == 200
    names = [v["name"] for v in response.json()]
    assert "My View" in names
    assert "Second View" in names
