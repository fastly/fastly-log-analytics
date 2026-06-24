"""HTTP-layer contract tests for admin mutation endpoints.

Covers:
  POST /api/admin/pop-locations/refresh
  POST /api/admin/ingest-logs
  POST /api/admin/commit-iceberg
  POST /api/admin/bot-sources/{source_id}/refresh
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient

from backend.deps import get_source
from backend.main import app

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEST_SOURCE = {
    "name": "test_service",
    "service_id": "svc1",
    "bucket": "my-bucket",
    "region": "us-east-1",
    "access_key_id": "AKID",
    "secret_access_key": "SEC",
    "storage_mode": "cloud",
}


def _client_with_source(source=None):
    src = source or _TEST_SOURCE
    c = TestClient(app)
    app.dependency_overrides[get_source] = lambda: src
    return c


def _reset():
    app.dependency_overrides.pop(get_source, None)


# ---------------------------------------------------------------------------
# POST /api/admin/pop-locations/refresh
# ---------------------------------------------------------------------------


def test_pop_locations_refresh_calls_fetch_with_token():
    """Token query param must be forwarded to fetch_pop_locations."""
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True) as mock_fetch,
        patch("backend.utils.pop_utils.get_pop_locations", return_value=[]),
    ):
        client = TestClient(app)
        response = client.post("/api/admin/pop-locations/refresh?token=myapikey")

    assert response.status_code == 200
    mock_fetch.assert_called_once_with("myapikey")


def test_pop_locations_refresh_missing_token_returns_400():
    client = TestClient(app)
    response = client.post("/api/admin/pop-locations/refresh?token=")
    assert response.status_code == 400


def test_pop_locations_refresh_fetch_failure_returns_502():
    with patch("backend.utils.pop_utils.fetch_pop_locations", return_value=False):
        client = TestClient(app)
        response = client.post("/api/admin/pop-locations/refresh?token=badkey")
    assert response.status_code == 502


def test_pop_locations_refresh_returns_pops_list():
    pops = [{"code": "JFK", "name": "New York (JFK)", "latitude": 40.6, "longitude": -73.8}]
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.utils.pop_utils.get_pop_locations", return_value=pops),
    ):
        client = TestClient(app)
        response = client.post("/api/admin/pop-locations/refresh?token=tok")

    assert response.status_code == 200
    assert response.json()["pops"][0]["code"] == "JFK"


# ---------------------------------------------------------------------------
# POST /api/admin/ingest-logs
# ---------------------------------------------------------------------------


def test_ingest_logs_starts_sync_and_returns_run_id():
    """POST ingest-logs should start a background thread and return a run_id."""
    try:
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE

        with (
            patch("backend.core.duckdb.start_cron_run", return_value=42),
            patch("backend.cron_progress.start_progress"),
            patch("backend.scheduler._run_service_cron"),
            patch("threading.Thread") as mock_thread,
        ):
            mock_t = MagicMock()
            mock_thread.return_value = mock_t

            client = TestClient(app)
            response = client.post("/api/admin/ingest-logs")

        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["run_id"] == 42
        mock_t.start.assert_called_once()
    finally:
        _reset()


def test_ingest_logs_already_running_returns_existing_run_id():
    """If start_cron_run raises RuntimeError (busy), return existing run_id."""
    from backend.cron_progress import _run_metadata

    run_id = 99
    _run_metadata[run_id] = {"service_id": "test_service", "task": "sync"}
    try:
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE

        with patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("already running")):
            client = TestClient(app)
            response = client.post("/api/admin/ingest-logs")

        assert response.status_code == 200
        data = response.json()
        assert data["ok"] is True
        assert data["run_id"] == run_id
    finally:
        _reset()
        _run_metadata.pop(run_id, None)


def test_ingest_logs_read_only_source_starts_metadata_sync():
    """Read-only sources trigger metadata_sync, not full sync."""
    ro_source = {**_TEST_SOURCE, "access_level": "read_only"}
    try:
        app.dependency_overrides[get_source] = lambda: ro_source

        with (
            patch("backend.core.duckdb.start_cron_run", return_value=7),
            patch("backend.cron_progress.start_progress"),
            patch("backend.scheduler._run_metadata_sync") as mock_sync,
            patch("backend.scheduler._run_service_cron") as mock_cron,
            patch("threading.Thread") as mock_thread,
        ):
            mock_t = MagicMock()
            mock_thread.return_value = mock_t

            client = TestClient(app)
            response = client.post("/api/admin/ingest-logs")

        assert response.status_code == 200
        assert response.json()["run_id"] == 7
        # The thread target must be metadata_sync, not full sync
        call_kwargs = mock_thread.call_args.kwargs
        assert call_kwargs["target"] is mock_sync
        assert call_kwargs["target"] is not mock_cron
    finally:
        _reset()


# ---------------------------------------------------------------------------
# POST /api/admin/commit-iceberg
# ---------------------------------------------------------------------------


def test_commit_iceberg_starts_commit_thread_and_returns_run_id():
    try:
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE

        with (
            patch("backend.core.duckdb.start_cron_run", return_value=55),
            patch("backend.cron_progress.start_progress"),
            patch("backend.scheduler._run_commit"),
            patch("threading.Thread") as mock_thread,
        ):
            mock_t = MagicMock()
            mock_thread.return_value = mock_t

            client = TestClient(app)
            response = client.post("/api/admin/commit-iceberg")

        assert response.status_code == 202
        data = response.json()
        assert data["ok"] is True
        assert data["run_id"] == 55
        mock_t.start.assert_called_once()
    finally:
        _reset()


def test_commit_iceberg_already_running_returns_existing_run_id():
    from backend.cron_progress import _run_metadata

    run_id = 88
    _run_metadata[run_id] = {"service_id": "test_service", "task": "commit"}
    try:
        app.dependency_overrides[get_source] = lambda: _TEST_SOURCE

        with patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("busy")):
            client = TestClient(app)
            response = client.post("/api/admin/commit-iceberg")

        assert response.status_code == 202
        assert response.json()["run_id"] == run_id
    finally:
        _reset()
        _run_metadata.pop(run_id, None)


# ---------------------------------------------------------------------------
# POST /api/admin/bot-sources/{source_id}/refresh
# ---------------------------------------------------------------------------


def test_bot_source_refresh_calls_fetch_and_returns_meta():
    meta = {"id": "spamhaus", "url": "https://example.com/list.txt", "entry_count": 1000}

    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=meta):
        client = TestClient(app)
        response = client.post("/api/admin/bot-sources/spamhaus/refresh")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert response.json()["source"]["id"] == "spamhaus"


def test_bot_source_refresh_unknown_source_returns_404():
    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=ValueError("Unknown source")):
        client = TestClient(app)
        response = client.post("/api/admin/bot-sources/unknown/refresh")

    assert response.status_code == 404


def test_bot_source_refresh_fetch_failure_returns_502():
    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=RuntimeError("network error")):
        client = TestClient(app)
        response = client.post("/api/admin/bot-sources/spamhaus/refresh")

    assert response.status_code == 502
