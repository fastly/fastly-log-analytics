"""Tests for ``backend.routers.admin`` — POST/DELETE/PATCH and download/export endpoints.

The GET-side admin endpoints are covered by
[test_admin_get_endpoints.py](tests/routers/test_admin_get_endpoints.py).
This file pins the **mutating** and **streaming/download** endpoints:

  - POST /admin/pop-locations/refresh
  - POST /admin/ingest-logs (read_only vs read_write branches)
  - POST /admin/sync/{service_id}
  - GET  /admin/raw-tree, /admin/iceberg-tree (file browser)
  - GET  /download (local cache / CDN stream / FOS stream)
  - POST /admin/commit-iceberg (manual flush trigger)
  - POST /admin/rebuild-local-view
  - POST /admin/bot-sources/{id}/refresh
  - DELETE /admin/usage-log
  - GET  /admin/usage-log/export
  - GET  /download-folder, /download-all
  - GET  /sync-status
"""

from __future__ import annotations

import contextlib
import contextvars
import io
import queue
import zipfile
from unittest.mock import MagicMock, patch

import pytest
from backend.cron_progress import _run_metadata
from fastapi import HTTPException
from fastapi.testclient import TestClient

from backend.core.duckdb import DBBusyError
from backend.core.metadata.cron_log import CronLeaseHeldError
from backend.deps import get_con, get_service_id, get_source, require_admin
from backend.main import app
from backend.routers.admin import (
    ClientDisconnected,
    _fetch_file_to_zip,
    _get_dir_size,
    _QueueFile,
    _stream_from_worker,
)
from tests._helpers.wire import wait_until
from tests.conftest import MOCK_SERVICE_ID

SVC_HEADERS = {"x-fastly-service-id": MOCK_SERVICE_ID}


@contextlib.contextmanager
def _without_service_overrides():
    """Temporarily pop ``get_source`` and ``get_service_id`` dependency overrides."""
    old_source = app.dependency_overrides.pop(get_source, None)
    old_sid = app.dependency_overrides.pop(get_service_id, None)
    try:
        yield
    finally:
        if old_source is not None:
            app.dependency_overrides[get_source] = old_source
        if old_sid is not None:
            app.dependency_overrides[get_service_id] = old_sid


@contextlib.contextmanager
def _override_dep(dep, fn):
    """Temporarily set a FastAPI dependency override and restore on exit."""
    old = app.dependency_overrides.get(dep)
    app.dependency_overrides[dep] = fn
    try:
        yield
    finally:
        if old is not None:
            app.dependency_overrides[dep] = old
        else:
            app.dependency_overrides.pop(dep, None)


def _fake_paginator_with_pages(pages):
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    return paginator


def _make_usage_row(
    *,
    timestamp: str = "2026-05-18T00:00:00Z",
    service_id: str = "svc",
    operation_class: str = "Class A",
    operation_type: str = "PUT_OBJECT",
    url: str = "https://x.example/raw/file.gz",
    bytes_: int = 1024,
    duration_ms: float = 12.5,
    function_name: str = "ingest",
    process_context: str = "cron:sync",
    status: str = "OK",
) -> dict:
    return {
        "timestamp": timestamp,
        "service_id": service_id,
        "operation_class": operation_class,
        "operation_type": operation_type,
        "url": url,
        "bytes": bytes_,
        "duration_ms": duration_ms,
        "function_name": function_name,
        "process_context": process_context,
        "status": status,
    }


# ── POST /admin/pop-locations/refresh ──────────────────────────────────────


@pytest.mark.parametrize(
    ("url", "expected_status"),
    [
        ("/api/admin/pop-locations/refresh", 422),
        ("/api/admin/pop-locations/refresh?token=   ", 400),
    ],
)
def test_refresh_pop_locations_token_validation(client, url: str, expected_status: int):
    """Missing ``token`` query param → 422; whitespace-only token → 400."""
    assert client.post(url).status_code == expected_status


def test_refresh_pop_locations_502s_when_fetch_fails(client):
    """``fetch_pop_locations`` returning False → 502 mentioning the API key."""
    with patch("backend.utils.pop_utils.fetch_pop_locations", return_value=False):
        resp = client.post("/api/admin/pop-locations/refresh?token=bad-key")
    assert resp.status_code == 502
    assert "api key" in resp.json()["detail"]["error"].lower()


def test_refresh_pop_locations_returns_refreshed_pops(client):
    """Happy path: fetch returns True, response contains the refreshed pop list."""
    fake_pops = [{"code": "iad-va-us", "name": "IAD", "latitude": 38.94, "longitude": -77.46}]
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.utils.pop_utils.get_pop_locations", return_value=fake_pops),
    ):
        resp = client.post("/api/admin/pop-locations/refresh?token=valid-key")
    assert resp.status_code == 200
    pops = resp.json()["pops"]
    assert len(pops) == 1 and pops[0]["code"] == "iad-va-us" and pops[0]["latitude"] == 38.94


# ── POST /admin/ingest-logs, /admin/sync/{service_id}, /admin/commit-iceberg ──


@pytest.mark.parametrize(
    ("endpoint", "run_id", "expected_task", "expected_status", "target_patch"),
    [
        ("/api/admin/ingest-logs", "run-123", "log_discovery", 200, "backend.cron.jobs.sync._run_log_discovery_cron"),
        (
            f"/api/admin/sync/{MOCK_SERVICE_ID}",
            "run-sync-789",
            "log_discovery",
            200,
            "backend.cron.jobs.sync._run_log_discovery_cron",
        ),
        ("/api/admin/commit-iceberg", "commit-run-789", "commit", 202, "backend.cron.jobs.commit._run_commit"),
    ],
)
def test_background_task_endpoints_start_job(
    client, endpoint: str, run_id: str, expected_task: str, expected_status: int, target_patch: str
):
    """Read-write ingest, per-service sync, and commit-iceberg start background cron tasks and return run_id."""
    started = {}

    def fake_start_cron_run(src, task):
        started["task"] = task
        return run_id

    with (
        patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
        patch("backend.cron_progress.start_progress"),
        patch(target_patch),
        patch("backend.repositories.dashboard.invalidate_service"),
    ):
        resp = client.post(endpoint, headers=SVC_HEADERS)

    assert resp.status_code == expected_status
    body = resp.json()
    assert body["run_id"] == run_id and started["task"] == expected_task
    if expected_status == 200:
        assert body["ok"] is True and "started" in body["message"].lower()


def test_ingest_endpoint_read_only_starts_metadata_sync(client, test_service_source):
    """Read-only (analyst) services trigger the metadata_sync path instead of full sync."""
    started = {}

    def fake_start_cron_run(src, task):
        started["task"] = task
        return "run-meta-456"

    app.dependency_overrides[get_source] = lambda: {**test_service_source, "access_level": "read_only"}
    with (
        patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
        patch("backend.cron_progress.start_progress"),
        patch("backend.cron.jobs.metadata._run_metadata_sync"),
    ):
        resp = client.post("/api/admin/ingest-logs", headers=SVC_HEADERS)

    assert resp.status_code == 200 and started["task"] == "metadata_sync"
    assert "metadata sync" in resp.json()["message"].lower()


def test_sync_service_endpoint_rejects_analyst_session_with_403(client):
    """Analyst Path B (remote share) is blocked with 403 by require_admin."""

    def reject_admin():
        raise HTTPException(
            status_code=403, detail={"error": "admin_only", "message": "This operation requires admin access."}
        )

    with _override_dep(require_admin, reject_admin):
        resp = client.post(f"/api/admin/sync/{MOCK_SERVICE_ID}", headers=SVC_HEADERS)
        assert resp.status_code == 403 and resp.json()["detail"]["error"] == "admin_only"


def test_sync_service_endpoint_rejects_read_only_source_with_403(client, test_service_source):
    """Analyst Path A (read_only instance) is disabled from triggering log discovery."""
    with _override_dep(get_source, lambda: {**test_service_source, "access_level": "read_only"}):
        resp = client.post(f"/api/admin/sync/{MOCK_SERVICE_ID}", headers=SVC_HEADERS)
        assert resp.status_code == 403
        assert "read-only" in resp.json()["detail"].lower()


@pytest.mark.parametrize(
    ("endpoint", "exc"),
    [
        ("/api/admin/ingest-logs", CronLeaseHeldError("commit already running")),
        ("/api/admin/commit-iceberg", RuntimeError("locked")),
    ],
)
def test_background_task_endpoints_503_when_busy_and_no_active_run(client, endpoint: str, exc: Exception):
    """When start_cron_run fails and no matching run is in _run_metadata → 503 with busy: true."""
    _run_metadata.clear()
    with patch("backend.core.duckdb.start_cron_run", side_effect=exc):
        resp = client.post(endpoint, headers=SVC_HEADERS)

    assert resp.status_code == 503
    assert resp.json()["detail"]["busy"] is True


@pytest.mark.parametrize(
    ("endpoint", "task", "existing_id", "expected_status"),
    [
        ("/api/admin/ingest-logs", "log_discovery", "existing-run-id", 200),
        ("/api/admin/commit-iceberg", "commit", "existing-commit", 202),
    ],
)
def test_background_task_endpoints_return_existing_run_when_already_running(
    client, test_service_source, endpoint: str, task: str, existing_id: str, expected_status: int
):
    """Same-task already-running → return existing run_id instead of 503."""
    _run_metadata.clear()
    _run_metadata[existing_id] = {"service_id": test_service_source["name"], "task": task}
    try:
        with patch("backend.core.duckdb.start_cron_run", side_effect=CronLeaseHeldError(f"{task} busy")):
            resp = client.post(endpoint, headers=SVC_HEADERS)
    finally:
        _run_metadata.clear()

    assert resp.status_code == expected_status
    assert resp.json()["run_id"] == existing_id
    if expected_status == 200:
        assert "already running" in resp.json()["message"].lower()


# ── GET /admin/raw-tree, /admin/iceberg-tree ───────────────────────────────


@pytest.mark.parametrize(
    ("endpoint", "expected_root", "child_name", "child_size"),
    [
        ("/api/admin/raw-tree", "raw", "2026-01-01", 12345),
        ("/api/admin/iceberg-tree", "iceberg", "metadata", 0),
    ],
)
def test_tree_endpoints_pass_correct_root_and_return_children(
    client, endpoint: str, expected_root: str, child_name: str, child_size: int
):
    """Raw and iceberg tree endpoints pass their respective ``root`` and return ``nodes``."""
    fake_result = {"children": [{"name": child_name, "size": child_size, "type": "directory"}]}
    with patch("backend.core.duckdb.get_raw_tree_node", return_value=fake_result) as mock_get:
        resp = client.get(endpoint, headers=SVC_HEADERS)

    assert resp.status_code == 200
    nodes = resp.json()["nodes"]
    assert len(nodes) == 1 and nodes[0]["name"] == child_name
    assert mock_get.call_args[1].get("root") == expected_root


def test_raw_tree_returns_empty_when_no_children_key(client):
    """``get_raw_tree_node`` returning a dict without ``children`` returns ``nodes: []``."""
    with patch("backend.core.duckdb.get_raw_tree_node", return_value={}):
        resp = client.get("/api/admin/raw-tree", headers=SVC_HEADERS)
    assert resp.status_code == 200
    assert resp.json()["nodes"] == []


# ── GET /download ──────────────────────────────────────────────────────────


def test_download_file_400s_without_key(client):
    """Missing ``key`` query param → 400."""
    assert client.get("/api/download", headers=SVC_HEADERS).status_code == 400


def test_download_file_streams_through_backend_when_cdn_configured(client, test_service_source):
    """When ``cdn_url`` is configured, stream through backend with ``x-fastly-key`` header."""
    app.dependency_overrides[get_source] = lambda: {
        **test_service_source,
        "cdn_url": "https://cdn.example.com",
        "cdn_secret": "secret123",
    }

    captured_request = {}
    fake_response = MagicMock()
    fake_response.headers = {"Content-Type": "application/gzip", "Content-Length": "10"}
    chunks = iter([b"helloworld", b""])
    fake_response.read = lambda _n=None: next(chunks)
    fake_response.close = MagicMock()

    def fake_urlopen(req, timeout=None):
        captured_request["url"] = req.full_url
        captured_request["headers"] = dict(req.header_items())
        return fake_response

    with (
        patch("backend.core.duckdb._cache_dir", return_value="/nonexistent/path"),
        patch("urllib.request.urlopen", side_effect=fake_urlopen),
    ):
        resp = client.get(
            "/api/download",
            headers=SVC_HEADERS,
            params={"key": "raw/2026-01-01.log.gz"},
            follow_redirects=False,
        )

    assert resp.status_code == 200
    headers_lc = {k.lower(): v for k, v in captured_request["headers"].items()}
    assert headers_lc.get("x-fastly-key") == "secret123"
    assert "?key=" not in captured_request["url"]
    assert captured_request["url"].startswith("https://cdn.example.com/raw/")
    assert resp.content == b"helloworld"
    assert "filename=" in resp.headers.get("content-disposition", "")


def test_download_file_streams_from_fos_when_no_cdn(client, test_service_source):
    """No CDN → stream FOS object directly server-side."""
    fake_s3 = MagicMock()
    fake_body = MagicMock()
    fake_body.iter_chunks.return_value = [b"helloworld"]
    fake_s3.get_object.return_value = {
        "ContentType": "text/plain",
        "ContentLength": 10,
        "Body": fake_body,
    }
    app.dependency_overrides[get_source] = lambda: {**test_service_source, "bucket": "test-bucket"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value="/nonexistent"),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        resp = client.get(
            "/api/download",
            headers=SVC_HEADERS,
            params={"key": "raw/file.log"},
            follow_redirects=False,
        )

    assert resp.status_code == 200
    assert resp.content == b"helloworld"
    assert 'filename="file.log"' in resp.headers.get("content-disposition", "")
    assert "no-store" in resp.headers.get("cache-control", "")
    fake_s3.get_object.assert_called_once_with(Bucket="test-bucket", Key="raw/file.log")


def test_download_file_502s_when_fos_raises(client, test_service_source):
    """FOS object retrieval raising → 502 with ``fos_fetch_failed`` and ``error_id``."""
    fake_s3 = MagicMock()
    fake_s3.get_object.side_effect = RuntimeError("creds expired")
    app.dependency_overrides[get_source] = lambda: {**test_service_source, "bucket": "b"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value="/nonexistent"),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        resp = client.get("/api/download", headers=SVC_HEADERS, params={"key": "x"})
    assert resp.status_code == 502
    detail = resp.json()["detail"]
    assert detail["error"] == "fos_fetch_failed" and "error_id" in detail


def test_download_file_fails_on_sibling_prefix_partial_match(client, test_service_source):
    """Enforce directory-level boundaries: prefix 'tenant' rejects 'tenant-2/file.log' with 400."""
    app.dependency_overrides[get_source] = lambda: {
        **test_service_source,
        "prefix": "tenant",
        "bucket": "b",
    }
    resp = client.get("/api/download", headers=SVC_HEADERS, params={"key": "tenant-2/file.log"})
    assert resp.status_code == 400
    assert resp.json()["detail"]["error"] == "invalid_key"


# ── POST /admin/rebuild-local-view ────────────────────────────────────────


def test_rebuild_local_view_clears_caches_and_spawns_sync(client, tmp_path):
    """clear_source_caches MUST run, snapshot_files_cache.json removed, and _run_metadata_sync spawned."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache_file = cache_dir / "snapshot_files_cache.json"
    cache_file.write_text("{}")

    cleared: list[str] = []
    sync_calls: list[tuple] = []
    fake_thread = MagicMock()

    with (
        patch("backend.core.iceberg.clear_source_caches", side_effect=cleared.append),
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb.start_cron_run", return_value="rebuild-run-1"),
        patch("backend.cron_progress.start_progress"),
        patch(
            "backend.cron.jobs.metadata._run_metadata_sync",
            side_effect=lambda sid, run_id=None: sync_calls.append((sid, run_id)),
        ),
        patch("threading.Thread", return_value=fake_thread),
    ):
        resp = client.post("/api/admin/rebuild-local-view", headers=SVC_HEADERS)

    assert resp.status_code == 202
    body = resp.json()
    assert body["ok"] is True and body["run_id"] == "rebuild-run-1"
    assert len(cleared) == 1
    fake_thread.start.assert_called_once()


def test_rebuild_local_view_busy_returns_503(client, tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("sync locked")),
    ):
        resp = client.post("/api/admin/rebuild-local-view", headers=SVC_HEADERS)
    assert resp.status_code == 503
    assert resp.json()["detail"]["busy"] is True


# ── POST /admin/bot-sources/{source_id}/refresh ───────────────────────────


def test_refresh_bot_source_returns_meta_on_success(client):
    """Successful refresh returns ``{ok: true, source: <meta>}``."""
    fake_meta = {"id": "imperva", "row_count": 100, "updated_at": "2026-05-18T00:00:00Z"}
    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=fake_meta):
        resp = client.post("/api/admin/bot-sources/imperva/refresh")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True and body["source"] == fake_meta


@pytest.mark.parametrize(
    ("source_id", "exc", "expected_status"),
    [
        ("nonexistent", ValueError("unknown source"), 404),
        ("imperva", RuntimeError("network down"), 502),
    ],
)
def test_refresh_bot_source_error_codes(client, source_id: str, exc: Exception, expected_status: int):
    """ValueError → 404; generic RuntimeError → 502."""
    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=exc):
        resp = client.post(f"/api/admin/bot-sources/{source_id}/refresh")
    assert resp.status_code == expected_status


# ── DELETE /admin/usage-log ────────────────────────────────────────────────


def test_purge_usage_log_calls_metadata_db_clear(client, test_service_source):
    """DELETE clears all usage_log rows for the service."""
    with patch("backend.core.metadata.clear_usage_log") as mock_clear:
        resp = client.delete("/api/admin/usage-log", headers=SVC_HEADERS)

    assert resp.status_code == 200
    assert resp.json()["ok"] is True
    mock_clear.assert_called_once()
    assert mock_clear.call_args[0][0] == test_service_source["name"]


# ── _QueueFile & _stream_from_worker & _get_dir_size ──────────────────────


def test_queue_file_write_puts_bytes_on_queue_and_advances_offset():
    """Write puts bytes on the queue and advances ``offset``; ``flush()`` is a no-op."""
    q = queue.Queue()
    qf = _QueueFile(q)

    assert qf.write(b"hello") == 5
    assert qf.tell() == 5
    assert q.get_nowait() == b"hello"

    qf.write(b"world!")
    assert qf.tell() == 11
    qf.flush()


def test_stream_from_worker_yields_chunks_until_sentinel():
    """Worker puts bytes on the queue; sentinel ``None`` terminates the stream."""

    def worker(q):
        q.put(b"chunk1")
        q.put(b"chunk2")
        q.put(None)

    assert list(_stream_from_worker(worker)) == [b"chunk1", b"chunk2"]


def test_stream_from_worker_propagates_context_to_thread():
    """Telemetry context is copied into the worker thread."""
    test_var: contextvars.ContextVar = contextvars.ContextVar("test_var", default="default")
    test_var.set("from_request")
    captured: dict = {}

    def worker(q):
        captured["value"] = test_var.get()
        q.put(b"x")
        q.put(None)

    list(_stream_from_worker(worker))
    assert captured["value"] == "from_request"


def test_stream_from_worker_disconnect_closes_worker_thread():
    """When a streaming client disconnects, background thread receives ClientDisconnected and exits."""
    thread_failed = []
    thread_success = []

    def dummy_worker(q):
        try:
            for _ in range(100):
                q.put(b"some_bytes")
            thread_failed.append(True)
        except ClientDisconnected:
            thread_success.append(True)

    gen = _stream_from_worker(dummy_worker)
    assert next(gen) == b"some_bytes"
    gen.close()

    wait_until(lambda: bool(thread_success or thread_failed), timeout=1.0)
    assert thread_success == [True] and thread_failed == []


def test_get_dir_size_returns_zero_for_missing_or_unreadable_path(tmp_path):
    """Non-existent path or scandir PermissionError returns 0 instead of raising."""
    assert _get_dir_size(str(tmp_path / "does_not_exist")) == 0
    with patch("os.scandir", side_effect=PermissionError("denied")):
        assert _get_dir_size(str(tmp_path)) == 0


def test_get_dir_size_sums_recursively_across_subdirs(tmp_path):
    """Recursive walk sums file sizes across nested subdirs."""
    (tmp_path / "a.txt").write_bytes(b"x" * 100)
    sub = tmp_path / "sub" / "deeper"
    sub.mkdir(parents=True)
    (sub / "b.txt").write_bytes(b"y" * 250)
    assert _get_dir_size(str(tmp_path)) == 350


# ── GET /admin/usage-log/export ───────────────────────────────────────────


def test_usage_log_export_returns_csv_attachment_with_correct_filename(client):
    """Export streams CSV with the documented ``usage_log.csv`` filename."""
    with patch("backend.core.metadata.iter_usage_logs_chunks", return_value=iter([[_make_usage_row()]])):
        resp = client.get("/api/admin/usage-log/export", headers=SVC_HEADERS)

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "usage_log.csv" in resp.headers["content-disposition"]


def test_usage_log_export_includes_header_row_and_data_rows(client):
    """CSV starts with the 10-column header row, then per-row data."""
    fake_rows = [
        _make_usage_row(
            operation_class="Class B",
            operation_type="GET_OBJECT",
            url="https://x.example/raw/a.gz",
            bytes_=500,
            duration_ms=5.0,
            function_name="fetch",
            process_context="ui",
        ),
        _make_usage_row(
            timestamp="2026-05-18T00:01:00Z",
            operation_class="Class A",
            operation_type="PUT_OBJECT",
            url="https://x.example/raw/b.gz",
            bytes_=1000,
            duration_ms=10.0,
            function_name="ingest",
            process_context="cron",
        ),
    ]
    with patch("backend.core.metadata.iter_usage_logs_chunks", return_value=iter([fake_rows])):
        resp = client.get("/api/admin/usage-log/export", headers=SVC_HEADERS)

    text = resp.text
    header = text.splitlines()[0]
    for col in ("timestamp", "operation_class", "operation_type", "function_name"):
        assert col in header
    assert "PUT_OBJECT" in text and "GET_OBJECT" in text


def test_usage_log_export_returns_just_header_when_no_rows(client):
    """No rows in the time range → CSV with just the header."""
    with patch("backend.core.metadata.iter_usage_logs_chunks", return_value=iter([])):
        resp = client.get("/api/admin/usage-log/export", headers=SVC_HEADERS)

    assert resp.status_code == 200 and "timestamp" in resp.text
    assert len([line for line in resp.text.splitlines() if line.strip()]) == 1


def test_usage_log_export_escapes_formula_triggers_with_leading_whitespace(client):
    """Finding 015: whitespace-prefixed formula triggers (=, +, -, @) get a defensive ``'`` prefix."""
    fake_rows = [_make_usage_row(url=" =cmd|' /C calc.exe'!A0", bytes_=100, duration_ms=1.0, process_context="cron")]
    with patch("backend.core.metadata.iter_usage_logs_chunks", return_value=iter([fake_rows])):
        resp = client.get("/api/admin/usage-log/export", headers=SVC_HEADERS)

    text = resp.text
    assert "' =cmd|' /C calc.exe'!A0" in text or "\"' =cmd|' /C calc.exe'!A0\"" in text
    raw_count = text.count("=cmd|' /C calc.exe'!A0")
    apostrophe_count = text.count("' =cmd|' /C calc.exe'!A0")
    assert apostrophe_count >= 1 and raw_count == apostrophe_count


def test_usage_log_export_passes_filter_params_to_metadata_db(client):
    """``usage_type``, ``process_context``, ``operation_type`` query params flow into metadata_db."""
    capture = {}

    def fake_iter_chunks(**kwargs):
        capture.update(kwargs)
        return iter([])

    with patch("backend.core.metadata.iter_usage_logs_chunks", side_effect=fake_iter_chunks):
        client.get(
            "/api/admin/usage-log/export",
            headers=SVC_HEADERS,
            params={"usage_type": "FOS", "process_context": "cron:sync", "operation_type": "GET_OBJECT"},
        )

    assert capture.get("usage_type") == "FOS"
    assert capture.get("process_context") == "cron:sync"
    assert capture.get("operation_type") == "GET_OBJECT"


def test_usage_log_export_caps_at_max_rows_across_5k_chunks(client):
    """Pin (a) max_rows + chunk_size kwargs reach the helper, (b) streaming loop emits all yielded rows."""
    captured_kwargs: dict = {}

    def make_row(i: int) -> dict:
        return _make_usage_row(
            timestamp=f"2026-05-18T{i // 60:02d}:{i % 60:02d}:00Z",
            operation_class="Class B",
            operation_type="GET_OBJECT",
            url=f"https://x.example/raw/{i}.gz",
            bytes_=100,
            duration_ms=1.0,
            function_name="fetch",
            process_context="ui",
        )

    def fake_iter_chunks(**kwargs):
        captured_kwargs.update(kwargs)
        yield [make_row(i) for i in range(5000)]
        yield [make_row(i) for i in range(5000, 10000)]
        yield [make_row(i) for i in range(10000, 12000)]

    with patch("backend.core.metadata.iter_usage_logs_chunks", side_effect=fake_iter_chunks):
        resp = client.get("/api/admin/usage-log/export", headers=SVC_HEADERS, params={"max_rows": 12000})

    assert resp.status_code == 200
    assert captured_kwargs.get("max_rows") == 12000 and captured_kwargs.get("chunk_size") == 5000
    assert len([ln for ln in resp.text.split("\n") if ln]) == 12001


def test_usage_log_export_rejects_max_rows_above_ceiling(client):
    """The le=1_000_000 upper bound returns 422 before the handler runs."""
    resp = client.get("/api/admin/usage-log/export", headers=SVC_HEADERS, params={"max_rows": 5_000_000})
    assert resp.status_code == 422


# ── _fetch_file_to_zip helper ─────────────────────────────────────────────


def _run_fetch_file_to_zip(*, src: dict, cdn_url: str, urlopen_side_effect=None, fos_side_effect=None):
    fake_response = MagicMock()
    fake_response.headers = {}
    fake_response.read.side_effect = [b"chunk", b""]
    fake_response.__enter__ = lambda s: s
    fake_response.__exit__ = MagicMock(return_value=False)

    fos_client = MagicMock()
    if fos_side_effect is not None:
        fos_client.get_object.side_effect = fos_side_effect
    else:
        fos_body = MagicMock()
        fos_body.read.side_effect = [b"fos-data", b""]
        fos_client.get_object.return_value = {"Body": fos_body}

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        with (
            patch(
                "urllib.request.urlopen",
                side_effect=urlopen_side_effect,
                return_value=None if urlopen_side_effect else fake_response,
            ) as mock_open,
            patch("backend.utils.telemetry.record_cdn_call"),
        ):
            ok = _fetch_file_to_zip(src, fos_client, cdn_url, "raw/file.gz", "raw/file.gz", zf, "test-caller")
    return ok, mock_open, fos_client


def test_fetch_file_to_zip_uses_cdn_first_and_adds_secret_header():
    """When cdn URL + cdn_secret are set, fetch via HTTP from CDN with ``x-fastly-key`` and skip FOS."""
    ok, mock_open, fos_client = _run_fetch_file_to_zip(
        src={"bucket": "b", "cdn_secret": "my-secret"}, cdn_url="https://cdn.example"
    )
    assert ok is True
    req = mock_open.call_args[0][0]
    assert "cdn.example" in req.full_url and req.get_header("X-fastly-key") == "my-secret"
    fos_client.get_object.assert_not_called()


@pytest.mark.parametrize(
    ("cdn_url", "urlopen_err", "fos_err", "expected_ok", "expect_urlopen", "expect_fos"),
    [
        ("https://cdn.example", RuntimeError("CDN timeout"), None, True, True, True),
        ("", None, None, True, False, True),
        ("https://cdn.example", RuntimeError("CDN timeout"), RuntimeError("FOS down"), False, True, True),
    ],
)
def test_fetch_file_to_zip_fallback_and_failure_matrix(
    cdn_url: str, urlopen_err, fos_err, expected_ok: bool, expect_urlopen: bool, expect_fos: bool
):
    """Covers CDN failure → FOS fallback, direct FOS when no CDN, and False when both fail."""
    ok, mock_open, fos_client = _run_fetch_file_to_zip(
        src={"bucket": "b"}, cdn_url=cdn_url, urlopen_side_effect=urlopen_err, fos_side_effect=fos_err
    )
    assert ok is expected_ok
    assert mock_open.called is expect_urlopen
    assert fos_client.get_object.called is expect_fos


# ── GET /download-folder (zip streaming) ─────────────────────────────────


@pytest.mark.parametrize(
    ("prefix", "expected_filename"),
    [
        ("subdir", "subdir.zip"),
        ("", "raw.zip"),
    ],
)
def test_download_folder_returns_zip_with_expected_filename(
    client, test_service_source, prefix: str, expected_filename: str
):
    """Response is a zip stream with ``Content-Disposition: attachment`` naming folder or root."""
    fake_client = MagicMock()
    fake_client.get_paginator.return_value = _fake_paginator_with_pages([])

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
        patch("backend.routers.admin.downloads._fetch_file_to_zip"),
    ):
        resp = client.get("/api/download-folder", params={"prefix": prefix, "root": "raw"})

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/zip"
    disp = resp.headers.get("content-disposition", "")
    assert "attachment" in disp and f'filename="{expected_filename}"' in disp


def test_download_folder_invokes_fetch_for_each_listed_object(in_memory_duckdb):
    """Each non-directory key from the paginator triggers one ``_fetch_file_to_zip`` call."""
    src_with_bucket = {"name": "test_service", "service_id": "tsid", "bucket": "my-bucket"}
    fake_pages = [
        {"Contents": [{"Key": "raw/sub/a.parquet"}, {"Key": "raw/sub/b.parquet"}, {"Key": "raw/sub/"}]},
    ]
    fake_client = MagicMock()
    fake_client.get_paginator.return_value = _fake_paginator_with_pages(fake_pages)
    fetch_calls = []

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: src_with_bucket
    try:
        with (
            patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
            patch(
                "backend.routers.admin.downloads._fetch_file_to_zip",
                side_effect=lambda *a, **k: fetch_calls.append(a[3]),
            ),
            TestClient(app) as c,
        ):
            resp = c.get("/api/download-folder", params={"prefix": "sub", "root": "raw"})
            list(resp.iter_bytes())
    finally:
        app.dependency_overrides.clear()

    assert "raw/sub/a.parquet" in fetch_calls
    assert "raw/sub/b.parquet" in fetch_calls
    assert "raw/sub/" not in fetch_calls


# ── GET /download-all (full-service zip) ─────────────────────────────────


def test_download_all_400s_without_service_id(client):
    """Missing ``service_id`` query param → 400."""
    with _without_service_overrides():
        assert client.get("/api/download-all").status_code == 400


@pytest.mark.parametrize("ghost_id", ["ghost-svc", "ghost"])
def test_download_all_400s_when_service_not_found(client, ghost_id: str):
    """Unknown service_id → 400 with ``no_service: True``."""
    with _without_service_overrides():
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            resp = client.get("/api/download-all", params={"service_id": ghost_id})
        assert resp.status_code == 400
        assert resp.json()["detail"]["no_service"] is True


def test_download_all_returns_zip_with_service_named_filename(client, test_service_source):
    """Happy path: response has ``Content-Disposition`` filename containing ``service_id``."""
    src = {"name": "svc-123", "service_id": "svc-123", "bucket": "b", "cdn_url": ""}
    fake_client = MagicMock()
    fake_client.get_paginator.return_value = _fake_paginator_with_pages([])

    with _without_service_overrides():
        with (
            patch("backend.core.duckdb.get_source_for_service", return_value=src),
            patch("backend.config.load_config", return_value={"name": "svc", "service_id": "svc-123"}),
            patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
            patch("backend.routers.admin.downloads._fetch_file_to_zip"),
        ):
            resp = client.get("/api/download-all", params={"service_id": "svc-123"})

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/zip"
    assert 'filename="fastly_logs_svc-123.zip"' in resp.headers.get("content-disposition", "")


def test_download_all_rejects_path_traversal_in_prefix(client, tmp_path):
    """Finding Jun14-003: hostile ``prefix`` with ``../../`` cannot escape the service cache dir."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    sibling = tmp_path / "secret_outside_cache.txt"
    sibling.write_bytes(b"do not include")

    src = {
        "name": "svc-trav",
        "service_id": "svc-trav",
        "duckdb_path": str(tmp_path / "x.duckdb"),
        "bucket": "b",
        "prefix": "../../",
    }

    with _without_service_overrides():
        with (
            patch("backend.core.duckdb.get_source_for_service", return_value=src),
            patch("backend.config.load_config", return_value={"name": "svc-trav", "service_id": "svc-trav"}),
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        ):
            resp = client.get("/api/download-all", params={"service_id": "svc-trav", "include": "local"})
            body = b"".join(resp.iter_bytes())

    if resp.status_code == 200 and body:
        try:
            with zipfile.ZipFile(io.BytesIO(body)) as zf:
                names = set(zf.namelist())
        except zipfile.BadZipFile:
            names = set()
        for name in names:
            assert "secret_outside_cache" not in name


def test_download_all_local_mode_zips_duckdb_and_cache_files(client, tmp_path, test_service_source):
    """``include=local`` packs the local DuckDB file + every file under the per-service cache dir."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "parquet1.parquet").write_bytes(b"P1")
    (cache_dir / "parquet2.parquet").write_bytes(b"P2")
    db_path = tmp_path / "svc.duckdb"
    db_path.write_bytes(b"DUCKDB")

    src = {"name": "svc", "service_id": "svc", "duckdb_path": str(db_path), "bucket": "b"}

    with _without_service_overrides():
        with (
            patch("backend.core.duckdb.get_source_for_service", return_value=src),
            patch("backend.config.load_config", return_value={"name": "svc", "service_id": "svc"}),
            patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        ):
            resp = client.get("/api/download-all", params={"service_id": "svc", "include": "local"})
            body = b"".join(resp.iter_bytes())

    assert resp.status_code == 200
    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        names = set(zf.namelist())
    assert "svc.duckdb" in names
    assert any(n.endswith("parquet1.parquet") for n in names)
    assert any(n.endswith("parquet2.parquet") for n in names)


# ── GET /sync-status ─────────────────────────────────────────────────────


def test_sync_status_returns_configured_false_when_no_source():
    """No matching source → response with ``configured=False``."""
    app.dependency_overrides[get_service_id] = lambda: "ghost"
    try:
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            with TestClient(app) as c:
                resp = c.get("/api/sync-status")
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 200
    assert resp.json()["configured"] is False


def test_sync_status_returns_status_with_duckdb_size_and_ngwaf(client, tmp_path, test_service_source):
    """Happy path: response includes ``duckdb_size_bytes``, ``duckdb_exists``, and ``ngwaf_workspace_id``."""
    db_path = tmp_path / "svc.duckdb"
    db_path.write_bytes(b"x" * 1024)
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "p.parquet").write_bytes(b"y" * 512)

    src = {
        "name": "test_service",
        "service_id": "test-service-id",
        "duckdb_path": str(db_path),
    }

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.core.duckdb.get_sync_status",
            return_value={"local_rows": 100, "fos_total": 5, "ingested": 4, "fos_new": 1, "busy": False},
        ),
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.cron_progress.get_latest_progress_for_service", return_value=None),
        patch("backend.config.load_config", return_value={"ngwaf_workspace_id": "ws-123"}),
    ):
        resp = client.get("/api/sync-status")

    assert resp.status_code == 200
    body = resp.json()
    assert body["duckdb_size_bytes"] == 1536
    assert body["duckdb_exists"] is True
    assert body["ngwaf_workspace_id"] == "ws-123"
    assert body["local_rows"] == 100


def test_sync_status_marks_busy_when_active_run_exists(client, tmp_path, test_service_source):
    """If ``get_latest_progress_for_service`` returns a run, set ``busy=True`` and include ``active_run``."""
    src = {
        "name": "test_service",
        "service_id": "test-service-id",
        "duckdb_path": str(tmp_path / "missing.duckdb"),
    }
    active = {"run_id": 99, "task": "sync", "started_at": "2026-05-18T20:00:00Z"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.get_connection", return_value=MagicMock()),
        patch(
            "backend.core.duckdb.get_sync_status",
            return_value={"local_rows": 0, "fos_total": 0, "ingested": 0, "fos_new": 0, "busy": False},
        ),
        patch("backend.core.duckdb._cache_dir", return_value=str(tmp_path / "missing-cache")),
        patch("backend.cron_progress.get_latest_progress_for_service", return_value=active),
        patch("backend.config.load_config", return_value={}),
    ):
        resp = client.get("/api/sync-status")

    body = resp.json()
    assert body["busy"] is True
    assert body["active_run"]["run_id"] == 99


@pytest.mark.parametrize(
    ("exc", "expected_status", "expected_error"),
    [
        (DBBusyError("locked"), 503, "db_busy"),
        (RuntimeError("disk full at /mnt/internal/path"), 500, "sync_status_failed"),
    ],
)
def test_sync_status_error_responses(
    client, test_service_source, exc: Exception, expected_status: int, expected_error: str
):
    """DBBusyError → 503 with busy: True; unexpected RuntimeError → 500 with error_id and no path leak."""
    src = {"name": "test_service", "service_id": "test-service-id"}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.get_connection", side_effect=exc),
    ):
        resp = client.get("/api/sync-status")

    assert resp.status_code == expected_status
    detail = resp.json()["detail"]
    assert detail["error"] == expected_error
    if expected_status == 503:
        assert detail["busy"] is True
    else:
        assert "error_id" in detail
        assert "disk full" not in detail["error"]
        assert "/mnt/internal" not in str(detail)
