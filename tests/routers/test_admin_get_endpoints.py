"""Tests for the GET-side of ``backend.routers.admin``.

The mutation endpoints have their own file
(``test_admin_mutations.py``); this file pins the read endpoints
that previously had zero direct coverage:

  - ``/api/admin/pop-locations``
  - ``/api/sync-status``
  - ``/api/log-extents``
  - ``/api/admin/ingested-files``
  - ``/api/admin/iceberg-info``
  - ``/api/admin/iceberg-calendar``
  - ``/api/admin/bot-sources``
  - ``/api/admin/usage-logging`` (GET, POST, PATCH)
  - ``/api/admin/usage-log``
  - ``/api/admin/system-jobs``
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from tests.conftest import MOCK_SERVICE_ID

# ── /api/admin/pop-locations ───────────────────────────────────────────────


def test_get_pop_locations_returns_cached_pops(client):
    fake_pops = [
        {"code": "LAX", "name": "Los Angeles", "coordinates": {"latitude": 33.9, "longitude": -118.4}},
        {"code": "SJC", "name": "San Jose", "coordinates": {"latitude": 37.4, "longitude": -121.9}},
    ]
    with patch("backend.utils.pop_utils.get_pop_locations", return_value=fake_pops):
        resp = client.get("/api/admin/pop-locations")

    assert resp.status_code == 200
    data = resp.json()
    assert "pops" in data
    assert len(data["pops"]) == 2


def test_get_pop_locations_returns_empty_list_when_cache_missing(client):
    """No cache file → empty list (not 500). Pinned because fresh
    installs hit this on the first dashboard load."""
    with patch("backend.utils.pop_utils.get_pop_locations", return_value=[]):
        resp = client.get("/api/admin/pop-locations")

    assert resp.status_code == 200
    assert resp.json()["pops"] == []


# ── /api/sync-status ─────────────────────────────────────────────────


def test_sync_status_returns_configured_false_when_no_service():
    """Bare sync-status call with no service set up → ``configured=False``
    (not a 500). Pinned because the FE keys on this to render the
    "configure a service" empty state."""
    from backend.main import app

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        from fastapi.testclient import TestClient

        with TestClient(app) as c:
            resp = c.get("/api/sync-status")

    assert resp.status_code == 200
    assert resp.json()["configured"] is False


def test_sync_status_returns_503_on_db_busy(client):
    """``DBBusyError`` from get_sync_status → 503 with ``busy: true``.
    Pinned because the frontend keys on this exact shape to keep
    cached data instead of clearing the UI."""
    from backend.core.duckdb import DBBusyError

    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.core.duckdb.get_sync_status", side_effect=DBBusyError("locked")),
    ):
        mock_conn.return_value = type("C", (), {"close": lambda self: None})()
        resp = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 503
    assert resp.json()["detail"]["busy"] is True


def test_sync_status_500s_on_unexpected_exception(client):
    """Non-DBBusy exception → 500 with the error string. Pinned
    because losing this would surface the dashboard banner without
    a debuggable message."""
    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.get_connection") as mock_conn,
        patch("backend.core.duckdb.get_sync_status", side_effect=RuntimeError("config corrupt")),
    ):
        mock_conn.return_value = type("C", (), {"close": lambda self: None})()
        resp = client.get("/api/sync-status", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 500


# ── /api/log-extents ─────────────────────────────────────────────────


def test_log_extents_returns_configured_false_when_no_service():
    """No service set up → ``configured=False`` (same shape as
    sync-status). The FilterBar uses this to short-circuit the
    snap-to-extents flow."""
    from backend.main import app

    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        from fastapi.testclient import TestClient

        with TestClient(app) as c:
            resp = c.get("/api/log-extents")

    assert resp.status_code == 200
    body = resp.json()
    assert body["configured"] is False
    assert "ngwaf_workspace_id" not in body
    assert "active_run" not in body


def test_log_extents_returns_cached_extents(client):
    """Reads only the persisted status snapshot — no DuckDB hit, no
    503 path. Confirms the extents come through and the analyst-
    sensitive fields stay out."""
    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    cached = {
        "earliest_log_at": "2026-06-09T00:00:00Z",
        "latest_log_at": "2026-06-10T12:34:56Z",
        "ngwaf_workspace_id": "ws-should-not-leak",
        "active_run": {"task": "sync", "status": "running"},
    }
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.config.get_status", return_value=cached),
    ):
        resp = client.get("/api/log-extents", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["configured"] is True
    assert body["earliest_log_at"] == "2026-06-09T00:00:00Z"
    assert body["latest_log_at"] == "2026-06-10T12:34:56Z"
    assert "ngwaf_workspace_id" not in body
    assert "active_run" not in body


def test_log_extents_returns_null_extents_when_cache_empty(client):
    """Pre-first-cron-tick state: status dict empty → extents are
    null but ``configured`` is true. FilterBar's refetchInterval
    keeps polling until extents populate."""
    fake_src = {"name": "test_service", "service_id": MOCK_SERVICE_ID, "bucket": "b"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.config.get_status", return_value={}),
    ):
        resp = client.get("/api/log-extents", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["configured"] is True
    assert body["earliest_log_at"] is None
    assert body["latest_log_at"] is None


# ── /api/admin/ingested-files ──────────────────────────────────────────────


def test_ingested_files_returns_list_from_repo(client):
    fake_files = [
        {"file_name": "log-001.gz", "ingested_at": "2026-01-01T00:00:00Z", "row_count": 100, "file_size_bytes": 5000},
        {"file_name": "log-002.gz", "ingested_at": "2026-01-02T00:00:00Z", "row_count": 200, "file_size_bytes": 8000},
    ]
    with patch("backend.core.duckdb.get_ingested_files", return_value=fake_files):
        resp = client.get("/api/admin/ingested-files", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    files = resp.json()["files"]
    assert len(files) == 2
    assert files[0]["file_name"] == "log-001.gz"


def test_ingested_files_500s_on_exception(client):
    with patch("backend.core.duckdb.get_ingested_files", side_effect=RuntimeError("S3 down")):
        resp = client.get("/api/admin/ingested-files", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 500


# ── /api/admin/iceberg-info ────────────────────────────────────────────────


def test_iceberg_info_returns_table_metadata(client):
    """Response is shaped by ``IcebergTableInfoResponse``: the route's
    return dict must include the fields the model requires."""
    fake_info = {
        "table_name": "logs_test_service",
        "snapshots": 7,
        "data_files": 42,
        "size_bytes": 1024 * 1024 * 50,
    }
    with patch("backend.core.iceberg.get_table_info", return_value=fake_info):
        resp = client.get("/api/admin/iceberg-info", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["data_files"] == 42
    assert body["snapshots"] == 7


def test_iceberg_info_500s_on_table_load_failure(client):
    with patch("backend.core.iceberg.get_table_info", side_effect=RuntimeError("manifest gone")):
        resp = client.get("/api/admin/iceberg-info", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 500


# ── /api/admin/iceberg-calendar ────────────────────────────────────────────


def test_iceberg_calendar_returns_per_date_file_counts(client):
    """The calendar drives the date-picker heatmap in the admin UI.
    Pinned because losing the ``_debug_calls`` extension would
    break the debug-panel telemetry overlay."""
    fake_calendar = {
        "calendar": [{"date": "2026-01-01", "data_files": 12}],
        "ok": True,
    }
    with patch("backend.core.iceberg.get_snapshot_calendar", return_value=fake_calendar):
        resp = client.get(
            "/api/admin/iceberg-calendar",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["calendar"][0]["date"] == "2026-01-01"
    assert "_debug_calls" in body  # telemetry overlay


def test_iceberg_calendar_500s_on_exception(client):
    with patch(
        "backend.core.iceberg.get_snapshot_calendar",
        side_effect=RuntimeError("partition spec corrupt"),
    ):
        resp = client.get(
            "/api/admin/iceberg-calendar",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 500


# ── /api/admin/bot-sources (GET) ───────────────────────────────────────────


def test_get_bot_sources_returns_sources_and_rdns_stats(client):
    fake_sources = [
        {"id": "well-known-bots", "name": "Arcjet", "enabled": True, "last_updated": "2026-01-01T00:00:00Z"}
    ]
    fake_rdns = {"total": 1000, "pending": 50, "last_enrichment_at": "2026-05-15T00:00:00Z"}

    with (
        patch("backend.utils.bot_sources.get_all_sources_meta", return_value=fake_sources),
        patch("backend.utils.rdns_cache.get_stats", return_value=fake_rdns),
    ):
        resp = client.get("/api/admin/bot-sources")

    assert resp.status_code == 200
    body = resp.json()
    assert body["sources"][0]["id"] == "well-known-bots"
    assert body["rdns"]["total"] == 1000


# ── /api/admin/usage-logging (GET, POST, PATCH) ────────────────────────────


def test_usage_logging_get_returns_current_config(client, tmp_path, monkeypatch):
    from backend import config

    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_usage_logging_config({"enabled": True, "retention_days": 60, "class_a_rate_per_1k": 0.123})

    resp = client.get("/api/admin/usage-logging")
    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert body["retention_days"] == 60


def test_usage_logging_post_updates_only_allowed_fields(client, tmp_path, monkeypatch):
    """Only the documented field list updates; arbitrary keys are
    silently dropped. Pinned because letting unknown keys land in
    the JSON file would slowly corrupt the schema."""
    from backend import config

    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    resp = client.post(
        "/api/admin/usage-logging",
        json={
            "enabled": True,
            "retention_days": 90,
            "junk_field": "should_be_dropped",
            "another_unknown": 123,
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["enabled"] is True
    assert body["retention_days"] == 90
    # Unknown fields don't surface in the response
    assert "junk_field" not in body
    assert "another_unknown" not in body


def test_usage_logging_patch_is_same_as_post(client, tmp_path, monkeypatch):
    """PATCH and POST share the same handler — pinned because the FE
    uses PATCH for partial updates and we want them equivalent."""
    from backend import config

    monkeypatch.setattr(config, "SYSTEM_DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "_USAGE_LOGGING_CONFIG_PATH", tmp_path / "usage_logging.json")
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)

    config.save_usage_logging_config({"enabled": False, "retention_days": 30})
    resp = client.patch(
        "/api/admin/usage-logging",
        json={"enabled": True},
    )
    assert resp.status_code == 200
    assert resp.json()["enabled"] is True
    # The unchanged field is preserved
    assert resp.json()["retention_days"] == 30


# ── /api/admin/usage-log ───────────────────────────────────────────────────


def test_usage_log_endpoint_returns_paginated_rows(client):
    """The endpoint joins usage_log rows with aggregate cost figures.
    Pinned because the FE renders entries + aggregate as separate
    sections; losing the aggregate breakdown would zero the cost card."""
    fake_rows = [
        {
            "id": 1,
            "timestamp": "2026-01-01T00:00:00Z",
            "service_id": MOCK_SERVICE_ID,
            "operation_class": "A",
            "operation_type": "PUT_OBJECT",
            "url": None,
            "bytes": None,
            "duration_ms": None,
            "function_name": None,
            "process_context": None,
            "status": "ok",
        },
        {
            "id": 2,
            "timestamp": "2026-01-01T00:00:01Z",
            "service_id": MOCK_SERVICE_ID,
            "operation_class": "B",
            "operation_type": "GET_OBJECT",
            "url": None,
            "bytes": None,
            "duration_ms": None,
            "function_name": None,
            "process_context": None,
            "status": "ok",
        },
    ]
    fake_agg = {
        "total_class_a": 100,
        "total_class_b": 200,
        "total_cdn_downloads": 50,
        "total_cdn_bytes": 10485760,  # 10 MB
        "total_fos_bytes": 0,
        "class_a_breakdown": {"PUT_OBJECT": 100},
        "class_b_breakdown": {"GET_OBJECT": 200},
    }

    with patch(
        "backend.core.metadata_db.get_usage_logs",
        return_value=(fake_rows, 2, fake_agg),
    ):
        resp = client.get(
            "/api/admin/usage-log",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"page": 1, "page_size": 10},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert len(body["entries"]) == 2
    assert body["aggregate"]["total_class_a"] == 100
    assert body["aggregate"]["total_class_b"] == 200


def test_usage_log_endpoint_validates_page_size_upper_bound(client):
    """``page_size`` clamped to ≤ 1000 by FastAPI ``Query(le=1000)``.
    Pinned because a million-row response would OOM the frontend."""
    resp = client.get(
        "/api/admin/usage-log",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"page_size": 99999},
    )
    assert resp.status_code == 422


def test_usage_log_endpoint_validates_page_minimum(client):
    resp = client.get(
        "/api/admin/usage-log",
        headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        params={"page": 0},
    )
    assert resp.status_code == 422


# ── /api/admin/system-jobs ─────────────────────────────────────────────────


def test_system_jobs_endpoint_returns_200_with_jobs_array(client):
    """The endpoint surfaces the APScheduler job list to the admin
    UI's "background jobs" panel. Pinned at the structural level
    (200 + jobs array present) because the per-job content depends
    on the live scheduler state."""
    resp = client.get("/api/admin/system-jobs")
    assert resp.status_code == 200
    body = resp.json()
    assert "jobs" in body
    assert isinstance(body["jobs"], list)


def test_system_jobs_endpoint_includes_share_audit_purge(client):
    """Pinned because the share-audit-purge cron must appear on the
    admin Data Management page next to bot_data_refresh and
    rdns_enrichment — otherwise admins have no way to see whether the
    retention sweep is running."""
    resp = client.get("/api/admin/system-jobs")
    assert resp.status_code == 200
    ids = {j["id"] for j in resp.json()["jobs"]}
    assert "share_audit_purge" in ids
    assert "bot_data_refresh" in ids
    assert "rdns_enrichment" in ids


# silence ruff unused imports
_ = pytest
