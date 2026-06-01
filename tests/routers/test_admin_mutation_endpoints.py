"""Tests for ``backend.routers.admin`` — POST/DELETE/PATCH endpoints.

The GET-side admin endpoints are covered by
[test_admin_get_endpoints.py](tests/routers/test_admin_get_endpoints.py).
This file pins the **mutating** endpoints:

  - POST /admin/pop-locations/refresh
  - POST /admin/ingest-logs (read_only vs read_write branches)
  - GET  /admin/raw-tree, /admin/iceberg-tree (file browser)
  - GET  /download (local cache / CDN redirect / FOS presigned)
  - POST /admin/commit-iceberg (manual flush trigger)
  - POST /admin/bot-sources/{id}/refresh
  - DELETE /admin/usage-log

These power the wizard's "Sync Now" / "Commit Now" buttons and the
file-tree downloads — losing any of these would silently disable a
visible admin action with no error to surface to the user.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from tests.conftest import MOCK_SERVICE_ID

# ── POST /admin/pop-locations/refresh ──────────────────────────────────────


def test_refresh_pop_locations_requires_token(client):
    """No ``token`` query param → 422 from FastAPI (Query(...) is
    required). Pinned because the FE keys on 422 to render the
    "missing token" form error."""
    resp = client.post("/api/admin/pop-locations/refresh")
    assert resp.status_code == 422


def test_refresh_pop_locations_400s_when_token_is_blank(client):
    """Whitespace-only token → 400 (handler strips and re-validates).
    Pinned because a blank token would land at the Fastly API and
    waste a round-trip before failing."""
    resp = client.post("/api/admin/pop-locations/refresh?token=   ")
    assert resp.status_code == 400


def test_refresh_pop_locations_502s_when_fetch_fails(client):
    """``fetch_pop_locations`` returning False → 502 with a friendly
    error message that mentions the API key. Pinned because the FE
    surfaces this string in a toast."""
    with patch("backend.utils.pop_utils.fetch_pop_locations", return_value=False):
        resp = client.post("/api/admin/pop-locations/refresh?token=bad-key")
    assert resp.status_code == 502
    assert "api key" in resp.json()["detail"]["error"].lower()


def test_refresh_pop_locations_returns_refreshed_pops(client):
    """Happy path: fetch returns True, response contains the refreshed
    pop list. Pinned because the FE re-renders the pop dropdown on
    the response."""
    fake_pops = [{"code": "iad-va-us", "name": "IAD", "latitude": 38.94, "longitude": -77.46}]
    with (
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True),
        patch("backend.utils.pop_utils.get_pop_locations", return_value=fake_pops),
    ):
        resp = client.post("/api/admin/pop-locations/refresh?token=valid-key")
    assert resp.status_code == 200
    pops = resp.json()["pops"]
    assert len(pops) == 1
    assert pops[0]["code"] == "iad-va-us"
    assert pops[0]["latitude"] == 38.94


# ── POST /admin/ingest-logs ────────────────────────────────────────────────


def test_ingest_endpoint_read_write_starts_sync_in_background_thread(client):
    """Read-write service → starts a sync thread, returns the run_id.
    Pinned because the FE polls for run_id to render the sync-progress
    spinner."""
    started = {}

    def fake_start_cron_run(src, task):
        started["task"] = task
        return "run-123"

    with (
        patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
        patch("backend.cron_progress.start_progress"),
        patch("backend.scheduler._run_service_cron"),
    ):
        resp = client.post(
            "/api/admin/ingest-logs",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["run_id"] == "run-123"
    assert "started" in body["message"].lower()
    # Read-write services run the regular sync (not metadata_sync)
    assert started["task"] == "sync"


def test_ingest_endpoint_read_only_starts_metadata_sync(client, test_service_source):
    """Read-only (analyst) services trigger the metadata_sync path
    instead of the full sync — pinned because raw-log ingest requires
    admin Fastly creds that analyst replicas don't have."""
    started = {}

    def fake_start_cron_run(src, task):
        started["task"] = task
        return "run-meta-456"

    # Override get_source to return a read-only source
    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: {**test_service_source, "access_level": "read_only"}

    with (
        patch("backend.core.duckdb.start_cron_run", side_effect=fake_start_cron_run),
        patch("backend.cron_progress.start_progress"),
        patch("backend.scheduler._run_metadata_sync"),
    ):
        resp = client.post("/api/admin/ingest-logs", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert started["task"] == "metadata_sync"
    assert "metadata sync" in resp.json()["message"].lower()


def test_ingest_endpoint_503s_when_busy_and_no_active_run(client):
    """``start_cron_run`` raises RuntimeError (a different task is
    already running) AND we can't find an existing matching run in
    _run_metadata → 503 with ``busy: true``. Pinned because the FE
    keys on the busy flag to keep the spinner alive."""
    from backend.cron_progress import _run_metadata

    # Clear any leftover metadata
    _run_metadata.clear()

    with patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("commit already running")):
        resp = client.post(
            "/api/admin/ingest-logs",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 503
    assert resp.json()["detail"]["busy"] is True


def test_ingest_endpoint_returns_existing_run_id_when_already_running(client, test_service_source):
    """Same-task already-running → return 200 with existing run_id
    + "already running" message instead of 503. Pinned because the FE
    treats this as success (the work is happening; we just join the
    existing run)."""
    from backend.cron_progress import _run_metadata

    _run_metadata.clear()
    _run_metadata["existing-run-id"] = {"service_id": test_service_source["name"], "task": "sync"}

    try:
        with patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("sync busy")):
            resp = client.post(
                "/api/admin/ingest-logs",
                headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            )
    finally:
        _run_metadata.clear()

    assert resp.status_code == 200
    assert resp.json()["run_id"] == "existing-run-id"
    assert "already running" in resp.json()["message"].lower()


# ── GET /admin/raw-tree, /admin/iceberg-tree ───────────────────────────────


def test_raw_tree_returns_children_from_get_raw_tree_node(client):
    """Raw tree endpoint passes ``root='raw'``. Pinned because the FE
    keys on the tree node shape (``children`` is the only key needed)."""
    fake_result = {"children": [{"name": "2026-01-01", "size": 12345, "type": "directory"}]}
    with patch("backend.core.duckdb.get_raw_tree_node", return_value=fake_result) as mock_get:
        resp = client.get("/api/admin/raw-tree", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    nodes = resp.json()["nodes"]
    assert len(nodes) == 1
    assert nodes[0]["name"] == "2026-01-01"
    # Verify it called with root='raw'
    _, kwargs = mock_get.call_args
    assert kwargs.get("root") == "raw"


def test_iceberg_tree_returns_children_from_get_raw_tree_node(client):
    """Iceberg tree endpoint passes ``root='iceberg'``. Pinned because
    the FE's two tabs (raw vs iceberg) differ only by which endpoint
    they hit, so the root parameter is what wires them apart."""
    fake_result = {"children": [{"name": "metadata", "size": 0, "type": "directory"}]}
    with patch("backend.core.duckdb.get_raw_tree_node", return_value=fake_result) as mock_get:
        resp = client.get("/api/admin/iceberg-tree", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    _, kwargs = mock_get.call_args
    assert kwargs.get("root") == "iceberg"


def test_raw_tree_returns_empty_when_no_children_key(client):
    """``get_raw_tree_node`` may return a dict without a ``children``
    key (empty folder). Pinned because dropping the ``.get(.., [])``
    fallback would 500 with KeyError."""
    with patch("backend.core.duckdb.get_raw_tree_node", return_value={}):
        resp = client.get("/api/admin/raw-tree", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 200
    assert resp.json()["nodes"] == []


# ── GET /download ──────────────────────────────────────────────────────────


def test_download_file_400s_without_key(client):
    """Missing ``key`` query param → 400 (not a 422 — the route does
    its own validation since the empty string is the default). Pinned
    because the FE would never construct this URL without a key, so
    a 400 here means a bug somewhere upstream."""
    resp = client.get("/api/download", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 400


def test_download_file_redirects_to_cdn_when_configured(client, test_service_source):
    """When the source has a ``cdn_url``, the route returns a 307
    redirect to the CDN. Pinned because hitting FOS directly when CDN
    is available wastes the customer's CDN cache + egress savings."""
    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: {
        **test_service_source,
        "cdn_url": "https://cdn.example.com",
        "cdn_secret": "secret123",
    }

    # Patch _cache_dir to a missing path so the local-file branch is skipped
    with patch("backend.core.duckdb._cache_dir", return_value="/nonexistent/path"):
        resp = client.get(
            "/api/download",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"key": "raw/2026-01-01.log.gz"},
            follow_redirects=False,
        )

    assert resp.status_code == 307
    location = resp.headers["location"]
    assert location.startswith("https://cdn.example.com/")
    # cdn_secret threaded into URL query
    assert "key=secret123" in location


def test_download_file_uses_presigned_url_when_no_cdn(client, test_service_source):
    """No CDN → presigned FOS URL. Pinned because the FE's "Download
    raw file" action must work even before a CDN is provisioned."""
    fake_s3 = MagicMock()
    fake_s3.generate_presigned_url.return_value = "https://fos.example/presigned?sig=abc"

    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: {**test_service_source, "bucket": "test-bucket"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value="/nonexistent"),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        resp = client.get(
            "/api/download",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"key": "raw/file.log"},
            follow_redirects=False,
        )

    assert resp.status_code == 307
    assert resp.headers["location"] == "https://fos.example/presigned?sig=abc"
    # Verify presigned URL was built with the correct bucket + key
    _, kwargs = fake_s3.generate_presigned_url.call_args
    assert kwargs["Params"]["Bucket"] == "test-bucket"
    assert kwargs["Params"]["Key"] == "raw/file.log"


def test_download_file_500s_when_presign_raises(client, test_service_source):
    """Presigned URL generation raising (expired creds, bad region)
    → 500 with the error. Pinned because the FE renders the error
    text in the download dialog."""
    fake_s3 = MagicMock()
    fake_s3.generate_presigned_url.side_effect = RuntimeError("creds expired")

    from backend.deps import get_source
    from backend.main import app

    app.dependency_overrides[get_source] = lambda: {**test_service_source, "bucket": "b"}

    with (
        patch("backend.core.duckdb._cache_dir", return_value="/nonexistent"),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_s3),
    ):
        resp = client.get(
            "/api/download",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={"key": "x"},
        )
    assert resp.status_code == 500


# ── POST /admin/commit-iceberg ────────────────────────────────────────────


def test_commit_iceberg_starts_commit_in_background(client):
    """Commit endpoint starts a background commit thread + returns
    ``run_id``. Pinned because the FE polls the run_id to update the
    "committing..." indicator."""
    started = {}

    def fake_start(src, task):
        started["task"] = task
        return "commit-run-789"

    with (
        patch("backend.core.duckdb.start_cron_run", side_effect=fake_start),
        patch("backend.cron_progress.start_progress"),
        patch("backend.scheduler._run_commit"),
    ):
        resp = client.post("/api/admin/commit-iceberg", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["run_id"] == "commit-run-789"
    assert started["task"] == "commit"


def test_commit_iceberg_returns_existing_run_when_already_running(client, test_service_source):
    """Same as ingest endpoint — existing run wins over 503."""
    from backend.cron_progress import _run_metadata

    _run_metadata.clear()
    _run_metadata["existing-commit"] = {"service_id": test_service_source["name"], "task": "commit"}

    try:
        with patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("commit busy")):
            resp = client.post("/api/admin/commit-iceberg", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    finally:
        _run_metadata.clear()

    assert resp.status_code == 200
    assert resp.json()["run_id"] == "existing-commit"


def test_commit_iceberg_503s_when_busy_and_no_active_run(client):
    from backend.cron_progress import _run_metadata

    _run_metadata.clear()
    with patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("locked")):
        resp = client.post("/api/admin/commit-iceberg", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 503
    assert resp.json()["detail"]["busy"] is True


# ── POST /admin/rebuild-local-view ────────────────────────────────────────


def test_rebuild_local_view_clears_caches_and_spawns_sync(client, tmp_path):
    """The hot signal: clear_source_caches MUST run AND the persistent
    snapshot_files_cache.json file MUST be removed AND a background
    _run_metadata_sync MUST be spawned. Pinned because dropping any of
    these three steps yields a "rebuild" that doesn't actually rebuild."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    cache_file = cache_dir / "snapshot_files_cache.json"
    cache_file.write_text("{}")

    cleared: list[str] = []
    sync_calls: list[tuple] = []

    def fake_clear(sid):
        cleared.append(sid)

    def fake_sync(sid, run_id=None):
        sync_calls.append((sid, run_id))

    fake_thread = MagicMock()

    with (
        patch("backend.core.iceberg.clear_source_caches", side_effect=fake_clear),
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb.start_cron_run", return_value="rebuild-run-1"),
        patch("backend.cron_progress.start_progress"),
        patch("backend.scheduler._run_metadata_sync", side_effect=fake_sync),
        patch("threading.Thread", return_value=fake_thread),
    ):
        resp = client.post("/api/admin/rebuild-local-view", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["run_id"] == "rebuild-run-1"
    assert len(cleared) == 1
    assert not cache_file.exists(), "persistent snapshot cache was not removed"
    fake_thread.start.assert_called_once()


def test_rebuild_local_view_busy_returns_503(client, tmp_path):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()

    with (
        patch("backend.core.iceberg.clear_source_caches"),
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
        patch("backend.core.duckdb.start_cron_run", side_effect=RuntimeError("sync locked")),
    ):
        resp = client.post("/api/admin/rebuild-local-view", headers={"x-fastly-service-id": MOCK_SERVICE_ID})
    assert resp.status_code == 503
    assert resp.json()["detail"]["busy"] is True


# ── POST /admin/bot-sources/{source_id}/refresh ───────────────────────────


def test_refresh_bot_source_returns_meta_on_success(client):
    """Successful refresh returns ``{ok: true, source: <meta>}``.
    Pinned because the FE updates the source's last-refreshed timestamp
    from the response meta."""
    fake_meta = {"id": "imperva", "row_count": 100, "updated_at": "2026-05-18T00:00:00Z"}
    with patch("backend.utils.bot_sources.fetch_and_cache_source", return_value=fake_meta):
        resp = client.post("/api/admin/bot-sources/imperva/refresh")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["source"] == fake_meta


def test_refresh_bot_source_404s_on_value_error(client):
    """Unknown source_id raises ValueError → 404 (not 500). Pinned
    because the FE distinguishes 404 (typo'd ID) from 502 (network
    failure) when retrying."""
    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=ValueError("unknown source")):
        resp = client.post("/api/admin/bot-sources/nonexistent/refresh")
    assert resp.status_code == 404


def test_refresh_bot_source_502s_on_generic_exception(client):
    """Network/parsing errors → 502 with the underlying error.
    Pinned because the FE renders the 502 text in the refresh tooltip."""
    with patch("backend.utils.bot_sources.fetch_and_cache_source", side_effect=RuntimeError("network down")):
        resp = client.post("/api/admin/bot-sources/imperva/refresh")
    assert resp.status_code == 502


# ── DELETE /admin/usage-log ────────────────────────────────────────────────


def test_purge_usage_log_calls_metadata_db_clear(client, test_service_source):
    """DELETE clears all usage_log rows for the service. Pinned because
    the FE renders a confirmation "all entries cleared" on this 200
    — silent no-op would mislead admins into thinking the clear failed."""
    with patch("backend.core.metadata_db.clear_usage_log") as mock_clear:
        resp = client.delete("/api/admin/usage-log", headers={"x-fastly-service-id": MOCK_SERVICE_ID})

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}
    # Confirm we cleared the right service
    mock_clear.assert_called_once()
    called_with = mock_clear.call_args[0][0]
    assert called_with == test_service_source["name"]


# ── _QueueFile (pure helper) ───────────────────────────────────────────────


def test_queue_file_write_puts_bytes_on_queue_and_advances_offset():
    """Write puts bytes on the queue and advances `offset`. Pinned
    because `zipfile.ZipFile` uses `tell()` to compute member offsets
    — losing the offset advance would corrupt the ZIP central
    directory."""
    import queue

    from backend.routers.admin import _QueueFile

    q = queue.Queue()
    qf = _QueueFile(q)

    n = qf.write(b"hello")
    assert n == 5
    assert qf.tell() == 5
    assert q.get_nowait() == b"hello"

    qf.write(b"world!")
    assert qf.tell() == 11


def test_queue_file_flush_is_noop():
    """`flush()` is a no-op (queues don't buffer). Pinned because
    zipfile calls `flush()` on the central-directory write and a
    raising flush would crash the worker."""
    import queue

    from backend.routers.admin import _QueueFile

    qf = _QueueFile(queue.Queue())
    qf.flush()  # must not raise


# ── _stream_from_worker (daemon-thread generator) ─────────────────────────


def test_stream_from_worker_yields_chunks_until_sentinel():
    """Worker puts bytes on the queue; the generator yields them one
    by one. The sentinel `None` terminates the stream. Pinned because
    losing the sentinel handling would hang the streaming response
    forever waiting for more chunks."""
    from backend.routers.admin import _stream_from_worker

    def worker(q):
        q.put(b"chunk1")
        q.put(b"chunk2")
        q.put(None)  # sentinel

    chunks = list(_stream_from_worker(worker))
    assert chunks == [b"chunk1", b"chunk2"]


def test_stream_from_worker_propagates_context_to_thread():
    """Telemetry context is copied into the worker thread so
    `record_call()` inside the worker lands in the request's usage
    log batch. Pinned because losing context propagation would
    silently drop the worker-side telemetry."""
    import contextvars

    from backend.routers.admin import _stream_from_worker

    test_var: contextvars.ContextVar = contextvars.ContextVar("test_var", default="default")
    test_var.set("from_request")

    captured: dict = {}

    def worker(q):
        captured["value"] = test_var.get()
        q.put(b"x")
        q.put(None)

    list(_stream_from_worker(worker))
    # Worker observed the value the caller set in its context
    assert captured["value"] == "from_request"


# ── _resolve_source (private helper) ──────────────────────────────────────


def test_resolve_source_returns_default_for_default_name():
    """``source_name == "default"`` returns the legacy default source
    dict (storage_mode=local). Pinned because old single-service
    installs still query as ``source=default``."""
    from backend.routers.admin import _resolve_source

    out = _resolve_source("default")
    # The legacy default source has at minimum `name` and `storage_mode`
    assert "name" in out


def test_resolve_source_merges_loaded_config_into_default(tmp_path, monkeypatch):
    """A known service name → merge its config_to_source onto the
    default base. Pinned because admin endpoints fall through to the
    default's region/endpoint when the service config omits them."""
    from backend import config
    from backend.routers.admin import _resolve_source

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")
    config.save_config(
        "svc-known",
        {"service_id": "svc-known", "fos_bucket": "b", "fos_region": "us-west-2"},
    )

    out = _resolve_source("svc-known")
    # The loaded config's bucket beats the default
    assert out.get("bucket") == "b" or out.get("fos_bucket") == "b"


def test_resolve_source_returns_default_for_unknown_name(tmp_path, monkeypatch):
    """Unknown service name → fall back to the default source (not
    a 404). Pinned because admin endpoints prefer "render default
    data" over "fail loudly" — the user might be on the wrong
    service-id but the page should still load."""
    from backend import config
    from backend.routers.admin import _resolve_source

    monkeypatch.setattr(config, "CONFIGS_DIR", tmp_path / "cfgs")

    out = _resolve_source("ghost-service")
    assert "name" in out  # The default source has a name


# ── _get_dir_size (recursive disk-walk helper) ────────────────────────────


def test_get_dir_size_returns_zero_for_missing_path(tmp_path):
    """Non-existent path → 0 (not crash). Pinned because the sync-
    status endpoint includes cache size in `duckdb_size_bytes` —
    missing cache dir shouldn't 500 the Settings card."""
    from backend.routers.admin import _get_dir_size

    size = _get_dir_size(str(tmp_path / "does_not_exist"))
    assert size == 0


def test_get_dir_size_sums_recursively_across_subdirs(tmp_path):
    """Recursive walk sums file sizes across nested subdirs. Pinned
    because the cache dir is multi-level (per-service / per-date /
    files); flat-sum would under-report."""
    from backend.routers.admin import _get_dir_size

    (tmp_path / "a.txt").write_bytes(b"x" * 100)
    sub = tmp_path / "sub" / "deeper"
    sub.mkdir(parents=True)
    (sub / "b.txt").write_bytes(b"y" * 250)

    size = _get_dir_size(str(tmp_path))
    assert size == 350


def test_get_dir_size_swallows_scandir_exception(tmp_path):
    """Permission denied / I/O error during walk → return partial
    sum (whatever we got before the error). Pinned because the
    cache dir can briefly become unreadable during teardown — the
    sync-status endpoint shouldn't break the Settings page while
    that happens."""
    from backend.routers.admin import _get_dir_size

    with patch("os.scandir", side_effect=PermissionError("denied")):
        size = _get_dir_size(str(tmp_path))
    assert size == 0


# ── /api/download-all: 400 + 404 short-circuits ───────────────────────────


def test_download_all_400s_without_service_id(client):
    """Missing ``service_id`` query param → 400. Pinned because the
    FE never constructs this URL without an ID; a 400 here means a
    bug upstream — better than confusing 404 from the source-lookup
    path."""
    resp = client.get("/api/download-all")
    assert resp.status_code == 400


def test_download_all_404s_when_service_not_found(client):
    """Unknown service_id → 404 (not 500). Pinned because admins
    sometimes edit URL params manually — the 404 is the clearest
    "fix your URL" signal."""
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        resp = client.get("/api/download-all", params={"service_id": "ghost-svc"})
    assert resp.status_code == 404


# ── GET /admin/usage-log/export ───────────────────────────────────────────


def test_usage_log_export_returns_csv_attachment_with_correct_filename(client):
    """Export streams CSV with the documented `usage_log.csv` filename.
    Pinned because the FE renders a "Download CSV" button that triggers
    this endpoint — the filename is what shows up in the user's
    Downloads folder."""
    fake_rows = [
        {
            "timestamp": "2026-05-18T00:00:00Z",
            "service_id": "svc",
            "operation_class": "Class A",
            "operation_type": "PUT_OBJECT",
            "url": "https://x.example/raw/file.gz",
            "bytes": 1024,
            "duration_ms": 12.5,
            "function_name": "ingest",
            "process_context": "cron:sync",
            "status": "OK",
        }
    ]
    with patch("backend.core.metadata_db.get_usage_logs", return_value=(fake_rows, 1, {})):
        resp = client.get(
            "/api/admin/usage-log/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/csv")
    assert "usage_log.csv" in resp.headers["content-disposition"]


def test_usage_log_export_includes_header_row_and_data_rows(client):
    """CSV starts with the 10-column header row, then per-row data.
    Pinned because admins import this into spreadsheets that key on
    the header — renaming would break their pivot tables."""
    fake_rows = [
        {
            "timestamp": "2026-05-18T00:00:00Z",
            "service_id": "svc",
            "operation_class": "Class B",
            "operation_type": "GET_OBJECT",
            "url": "https://x.example/raw/a.gz",
            "bytes": 500,
            "duration_ms": 5.0,
            "function_name": "fetch",
            "process_context": "ui",
            "status": "OK",
        },
        {
            "timestamp": "2026-05-18T00:01:00Z",
            "service_id": "svc",
            "operation_class": "Class A",
            "operation_type": "PUT_OBJECT",
            "url": "https://x.example/raw/b.gz",
            "bytes": 1000,
            "duration_ms": 10.0,
            "function_name": "ingest",
            "process_context": "cron",
            "status": "OK",
        },
    ]
    with patch("backend.core.metadata_db.get_usage_logs", return_value=(fake_rows, 2, {})):
        resp = client.get(
            "/api/admin/usage-log/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    text = resp.text
    # Header row present
    header = text.splitlines()[0]
    assert "timestamp" in header
    assert "operation_class" in header
    assert "operation_type" in header
    assert "function_name" in header
    # Data rows present
    assert "PUT_OBJECT" in text
    assert "GET_OBJECT" in text


def test_usage_log_export_returns_just_header_when_no_rows(client):
    """No rows in the time range → CSV with just the header (not
    error). Pinned because the FE renders an empty download as
    success — losing this would 500 on empty service days."""
    with patch("backend.core.metadata_db.get_usage_logs", return_value=([], 0, {})):
        resp = client.get(
            "/api/admin/usage-log/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
        )

    assert resp.status_code == 200
    text = resp.text
    # Just the header
    assert "timestamp" in text
    # No data rows means just one line (or possibly empty after header)
    lines = [line for line in text.splitlines() if line.strip()]
    assert len(lines) == 1


# ── _fetch_file_to_zip helper (new extraction) ────────────────────────────


def test_fetch_file_to_zip_uses_cdn_first_when_configured():
    """When cdn URL is set, fetch via HTTP from CDN (and falls back
    to FOS only on failure). Pinned because losing this would
    double FOS egress on every download — admins pay for CDN ←→
    FOS shielding for a reason."""
    import io
    import zipfile
    from unittest.mock import MagicMock

    from backend.routers.admin import _fetch_file_to_zip

    # Mock the urllib response
    fake_response = MagicMock()
    fake_response.headers = {}
    fake_response.read.side_effect = [b"chunk", b""]
    fake_response.__enter__ = lambda s: s
    fake_response.__exit__ = MagicMock(return_value=False)

    fos_client = MagicMock()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        with (
            patch("urllib.request.urlopen", return_value=fake_response) as mock_open,
            patch("backend.utils.telemetry.record_cdn_call"),
        ):
            ok = _fetch_file_to_zip(
                {"bucket": "b", "cdn_secret": "shh"},
                fos_client,
                "https://cdn.example",
                "raw/file.gz",
                "raw/file.gz",
                zf,
                "test-caller",
            )

    assert ok is True
    # CDN URL was used (urlopen called with cdn-built URL)
    req = mock_open.call_args[0][0]
    assert "cdn.example" in req.full_url
    # FOS NOT called (CDN succeeded)
    fos_client.get_object.assert_not_called()


def test_fetch_file_to_zip_falls_back_to_fos_on_cdn_failure():
    """If CDN fetch raises (timeout, 5xx), fall back to direct FOS.
    Pinned because losing this would 502 the download on every
    transient CDN hiccup."""
    import io
    import zipfile
    from unittest.mock import MagicMock

    from backend.routers.admin import _fetch_file_to_zip

    fos_client = MagicMock()
    fos_body = MagicMock()
    fos_body.read.side_effect = [b"fos-data", b""]
    fos_client.get_object.return_value = {"Body": fos_body}

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        with patch("urllib.request.urlopen", side_effect=RuntimeError("CDN timeout")):
            ok = _fetch_file_to_zip(
                {"bucket": "b"},
                fos_client,
                "https://cdn.example",
                "raw/file.gz",
                "raw/file.gz",
                zf,
                "test-caller",
            )

    assert ok is True
    # FOS was called as fallback
    fos_client.get_object.assert_called_once()


def test_fetch_file_to_zip_uses_fos_directly_when_no_cdn():
    """When `cdn` is empty/None, skip CDN and go straight to FOS.
    Pinned because pre-CDN-provisioning services have no cdn_url."""
    import io
    import zipfile
    from unittest.mock import MagicMock

    from backend.routers.admin import _fetch_file_to_zip

    fos_client = MagicMock()
    fos_body = MagicMock()
    fos_body.read.side_effect = [b"data", b""]
    fos_client.get_object.return_value = {"Body": fos_body}

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        with patch("urllib.request.urlopen") as mock_open:
            ok = _fetch_file_to_zip(
                {"bucket": "b"},
                fos_client,
                "",  # no CDN
                "raw/file.gz",
                "raw/file.gz",
                zf,
                "test-caller",
            )

    assert ok is True
    # CDN call NOT made
    mock_open.assert_not_called()
    fos_client.get_object.assert_called_once()


def test_fetch_file_to_zip_returns_false_when_both_cdn_and_fos_fail():
    """If both CDN and FOS fail, return False (not raise). Pinned
    because the caller iterates many files — one bad file shouldn't
    abort the whole zip."""
    import io
    import zipfile
    from unittest.mock import MagicMock

    from backend.routers.admin import _fetch_file_to_zip

    fos_client = MagicMock()
    fos_client.get_object.side_effect = RuntimeError("FOS down")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        with patch("urllib.request.urlopen", side_effect=RuntimeError("CDN timeout")):
            ok = _fetch_file_to_zip(
                {"bucket": "b"},
                fos_client,
                "https://cdn.example",
                "raw/file.gz",
                "raw/file.gz",
                zf,
                "test-caller",
            )

    assert ok is False


def test_fetch_file_to_zip_adds_cdn_secret_header_when_configured():
    """When `cdn_secret` is set, the helper adds `x-fastly-key`
    header. Pinned because the CDN VCL validates this header for
    auth — losing it would return 403 from CDN, falling back to
    direct FOS reads + losing CDN cache hits."""
    import io
    import zipfile
    from unittest.mock import MagicMock

    from backend.routers.admin import _fetch_file_to_zip

    fake_response = MagicMock()
    fake_response.headers = {}
    fake_response.read.side_effect = [b"data", b""]
    fake_response.__enter__ = lambda s: s
    fake_response.__exit__ = MagicMock(return_value=False)

    fos_client = MagicMock()

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        with (
            patch("urllib.request.urlopen", return_value=fake_response) as mock_open,
            patch("backend.utils.telemetry.record_cdn_call"),
        ):
            _fetch_file_to_zip(
                {"bucket": "b", "cdn_secret": "my-secret"},
                fos_client,
                "https://cdn.example",
                "raw/file.gz",
                "raw/file.gz",
                zf,
                "test-caller",
            )

    req = mock_open.call_args[0][0]
    # Header was added (urllib normalizes header names to title-case)
    assert req.get_header("X-fastly-key") == "my-secret"


def test_usage_log_export_passes_filter_params_to_metadata_db(client):
    """`usage_type`, `process_context`, `operation_type` query params
    flow into the metadata_db query. Pinned because the FE wires
    these filter inputs to the export — losing the wiring would
    silently export the unfiltered superset."""
    capture = {}

    def fake_get_logs(**kwargs):
        capture.update(kwargs)
        return ([], 0, {})

    with patch("backend.core.metadata_db.get_usage_logs", side_effect=fake_get_logs):
        client.get(
            "/api/admin/usage-log/export",
            headers={"x-fastly-service-id": MOCK_SERVICE_ID},
            params={
                "usage_type": "FOS",
                "process_context": "cron:sync",
                "operation_type": "GET_OBJECT",
            },
        )

    assert capture.get("usage_type") == "FOS"
    assert capture.get("process_context") == "cron:sync"
    assert capture.get("operation_type") == "GET_OBJECT"


# ── GET /admin/download-folder (zip streaming) ───────────────────────────


def _fake_paginator_with_pages(pages):
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    return paginator


def test_download_folder_returns_zip_with_attachment_disposition(client, test_service_source):
    """Happy path: the response is a zip stream with a
    `Content-Disposition: attachment` header naming the folder.
    Pinned because the FE keys on the header to trigger a browser
    download — missing attachment would render the bytes inline."""
    fake_client = MagicMock()
    fake_client.get_paginator.return_value = _fake_paginator_with_pages([])  # empty FOS

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
        patch("backend.routers.admin._fetch_file_to_zip"),  # never called for empty pages
    ):
        resp = client.get("/api/download-folder", params={"prefix": "subdir", "root": "raw"})

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/zip"
    assert "attachment" in resp.headers.get("content-disposition", "")
    assert 'filename="subdir.zip"' in resp.headers.get("content-disposition", "")


def test_download_folder_uses_root_as_filename_when_prefix_empty(client, test_service_source):
    """No `prefix` → filename derives from `root` (`raw.zip`). Pinned
    so a "download all of raw/" request doesn't end up with an empty
    filename that the browser refuses."""
    fake_client = MagicMock()
    fake_client.get_paginator.return_value = _fake_paginator_with_pages([])

    with (
        patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
        patch("backend.routers.admin._fetch_file_to_zip"),
    ):
        resp = client.get("/api/download-folder", params={"prefix": "", "root": "raw"})

    assert 'filename="raw.zip"' in resp.headers.get("content-disposition", "")


def test_download_folder_invokes_fetch_for_each_listed_object(in_memory_duckdb):
    """Each non-directory key from the paginator → one
    `_fetch_file_to_zip` call. Pinned because losing the loop would
    return an empty zip (no error, no warning, just silently
    missing logs)."""
    from fastapi.testclient import TestClient

    from backend.deps import get_con, get_meta_con, get_source
    from backend.main import app

    src_with_bucket = {"name": "test_service", "service_id": "tsid", "bucket": "my-bucket"}

    fake_pages = [
        {"Contents": [{"Key": "raw/sub/a.parquet"}, {"Key": "raw/sub/b.parquet"}, {"Key": "raw/sub/"}]},
    ]
    fake_client = MagicMock()
    fake_client.get_paginator.return_value = _fake_paginator_with_pages(fake_pages)
    fetch_calls = []

    app.dependency_overrides[get_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_meta_con] = lambda: in_memory_duckdb
    app.dependency_overrides[get_source] = lambda: src_with_bucket
    try:
        with (
            patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
            patch(
                "backend.routers.admin._fetch_file_to_zip",
                side_effect=lambda *a, **k: fetch_calls.append(a[3]),  # 4th arg is the key
            ),
            TestClient(app) as c,
        ):
            resp = c.get("/api/download-folder", params={"prefix": "sub", "root": "raw"})
            # Consume the streamed body so the worker runs to completion
            list(resp.iter_bytes())
    finally:
        app.dependency_overrides.clear()

    assert "raw/sub/a.parquet" in fetch_calls
    assert "raw/sub/b.parquet" in fetch_calls
    # Directory markers (ending in /) must NOT be downloaded
    assert "raw/sub/" not in fetch_calls

    # ── GET /download-all (full-service zip) ─────────────────────────────────


def test_download_all_404s_when_service_unknown(client):
    """Unknown service → 404. Pinned because the FE differentiates
    "no service selected" (400) from "service deleted between
    page-load and click" (404) — the user gets different help text."""
    with patch("backend.core.duckdb.get_source_for_service", return_value=None):
        resp = client.get("/api/download-all", params={"service_id": "ghost"})

    assert resp.status_code == 404
    assert "service not found" in resp.json()["detail"]["error"]


def test_download_all_returns_zip_with_service_named_filename(client, test_service_source):
    """Happy path: response has `Content-Disposition` filename
    containing the service_id. Pinned because admins identify the
    downloaded zip by service in their Downloads folder."""
    src = {"name": "svc", "service_id": "svc-123", "bucket": "b", "cdn_url": ""}
    fake_client = MagicMock()
    fake_client.get_paginator.return_value = _fake_paginator_with_pages([])

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb._get_fos_client", return_value=fake_client),
        patch("backend.routers.admin._fetch_file_to_zip"),
    ):
        resp = client.get("/api/download-all", params={"service_id": "svc-123"})

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/zip"
    assert 'filename="fastly_logs_svc-123.zip"' in resp.headers.get("content-disposition", "")


def test_download_all_local_mode_zips_duckdb_and_cache_files(client, tmp_path, test_service_source):
    """`include=local` packs the local DuckDB file + every file under
    the per-service cache dir. Pinned because the FE's "Export local
    cache" button relies on this — losing the cache-dir walk would
    silently produce a zip with only the duckdb file."""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    (cache_dir / "parquet1.parquet").write_bytes(b"P1")
    (cache_dir / "parquet2.parquet").write_bytes(b"P2")
    db_path = tmp_path / "svc.duckdb"
    db_path.write_bytes(b"DUCKDB")

    src = {"name": "svc", "service_id": "svc", "duckdb_path": str(db_path), "bucket": "b"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb._cache_dir", return_value=str(cache_dir)),
    ):
        resp = client.get("/api/download-all", params={"service_id": "svc", "include": "local"})
        body = b"".join(resp.iter_bytes())

    assert resp.status_code == 200
    import io
    import zipfile

    with zipfile.ZipFile(io.BytesIO(body)) as zf:
        names = set(zf.namelist())
    assert "svc.duckdb" in names
    # Both cache files captured
    assert any(n.endswith("parquet1.parquet") for n in names)
    assert any(n.endswith("parquet2.parquet") for n in names)


# ── GET /admin/sync-status (happy path body) ─────────────────────────────


def test_sync_status_returns_configured_false_when_no_source():
    """No matching source → response with `configured=False`. Pinned
    because the FE renders the "no service" empty state on this
    exact value — losing it would crash the dashboard."""
    from fastapi.testclient import TestClient

    from backend.deps import get_service_id
    from backend.main import app

    app.dependency_overrides[get_service_id] = lambda: "ghost"
    try:
        with patch("backend.core.duckdb.get_source_for_service", return_value=None):
            with TestClient(app) as c:
                resp = c.get("/api/sync-status")
    finally:
        app.dependency_overrides.clear()

    assert resp.status_code == 200
    body = resp.json()
    assert body["configured"] is False


def test_sync_status_returns_status_with_duckdb_size_and_ngwaf(client, tmp_path, test_service_source):
    """Happy path: response includes `duckdb_size_bytes` (db_size +
    cache_size), `duckdb_exists`, and `ngwaf_workspace_id` from cfg.
    Pinned because the dashboard renders three separate KPIs from
    those three keys."""
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

    fake_con = MagicMock()
    fake_con.close = MagicMock()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
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
    # 1024 (db) + 512 (cache) = 1536
    assert body["duckdb_size_bytes"] == 1536
    assert body["duckdb_exists"] is True
    assert body["ngwaf_workspace_id"] == "ws-123"
    assert body["local_rows"] == 100


def test_sync_status_marks_busy_when_active_run_exists(client, tmp_path, test_service_source):
    """If `get_latest_progress_for_service` returns a run, set
    `busy=True` and include `active_run` in the response. Pinned
    because the FE shows the inline cron-progress chip on `busy=True`
    — losing it would let "sync in progress" feel completely
    invisible."""
    src = {
        "name": "test_service",
        "service_id": "test-service-id",
        "duckdb_path": str(tmp_path / "missing.duckdb"),
    }
    active = {"run_id": 99, "task": "sync", "started_at": "2026-05-18T20:00:00Z"}
    fake_con = MagicMock()

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.get_connection", return_value=fake_con),
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


def test_sync_status_503s_on_db_busy_error(client, test_service_source):
    """`DBBusyError` from `get_connection` → 503 with `busy: True`.
    Pinned because React Query treats 503 as transient (will retry);
    a 500 here would surface the error toast to the user and abort
    the page render."""
    from backend.core.duckdb import DBBusyError

    src = {"name": "test_service", "service_id": "test-service-id"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.get_connection", side_effect=DBBusyError("locked")),
    ):
        resp = client.get("/api/sync-status")

    assert resp.status_code == 503
    body = resp.json()
    assert body["detail"]["busy"] is True
    assert "locked" in body["detail"]["error"]


def test_sync_status_500s_on_unexpected_exception(client, test_service_source):
    """Any other exception → 500 with the message. Pinned because
    losing this would surface a 200 with garbage data (the
    `with_telemetry` wrapper wouldn't be reached)."""
    src = {"name": "test_service", "service_id": "test-service-id"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=src),
        patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("disk full")),
    ):
        resp = client.get("/api/sync-status")

    assert resp.status_code == 500
    assert "disk full" in resp.json()["detail"]["error"]
