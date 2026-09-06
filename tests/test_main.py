"""Tests for ``backend.main`` startup helpers and middleware.

The FastAPI ``app`` itself is exercised end-to-end via TestClient in
the routers/ test suite. This file pins the startup-side helpers that
aren't reachable via HTTP requests:

  - ``_initialize_service``: reaps orphan cron rows + warms the
    service status cache. Reaping is the production fix for the
    "Loading logs..." stuck-sync bug, so a regression here would
    silently bring it back.
  - ``_ensure_pop_cache``: lazy POP cache prefetch on startup.
  - ``_background_startup``: scheduler bootstrap + service init.
  - ``telemetry_middleware``: per-request usage-log flush + cdn_service_id
    fallback resolution.
  - ``/api/health``: liveness probe (used by k8s/load balancers).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# ── _initialize_service ──────────────────────────────────────────────────────


def test_initialize_service_skips_when_no_service_id():
    from backend.main import _initialize_service

    # No service_id → early return without side-effects
    with (
        patch("backend.core.metadata.reap_running_cron_runs") as mock_reap,
        patch("backend.core.duckdb.get_source_for_service") as mock_get_src,
    ):
        _initialize_service({})

    mock_reap.assert_not_called()
    mock_get_src.assert_not_called()


def test_initialize_service_reaps_orphans_and_refreshes_status():
    from backend.main import _initialize_service

    fake_src = {"name": "svc-1", "service_id": "svc-1"}
    with (
        patch("backend.core.metadata.reap_running_cron_runs", return_value=3) as mock_reap,
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status") as mock_refresh,
        # The post-reap path now pre-warms the persistent view, sync-status
        # cache, and usage_log DB. They all touch real on-disk state which
        # is irrelevant to what this test pins (the reap + refresh
        # ordering), so swallow them here.
        patch("backend.main._ensure_persistent_view"),
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.core.metadata.usage_log_db.get_con"),
    ):
        _initialize_service({"service_id": "svc-1"})

    mock_reap.assert_called_once_with("svc-1")
    mock_refresh.assert_called_once_with("svc-1")


def test_initialize_service_logs_reaped_count_only_when_nonzero():
    """The "[fastapi] Service X: reaped N orphaned cron run(s)" log
    line is the operator's signal that orphans existed. Pinned to fire
    only when N>0 so a clean startup doesn't spam logs."""
    from backend.main import _initialize_service

    with (
        patch("backend.core.metadata.reap_running_cron_runs", return_value=0),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.main.logging.info") as mock_log_info,
    ):
        _initialize_service({"service_id": "svc-x"})

    # The "reaped" log is not emitted when count is zero
    reaped_logs = [c for c in mock_log_info.call_args_list if "reaped" in str(c)]
    assert reaped_logs == []


def test_initialize_service_tolerates_reap_failure():
    """If ``reap_running_cron_runs`` raises (locked DB, missing
    table), startup must continue — the reap is a hygiene step, not
    a correctness requirement. Other init work must still run."""
    from backend.main import _initialize_service

    fake_src = {"name": "svc-1"}
    with (
        patch("backend.core.metadata.reap_running_cron_runs", side_effect=RuntimeError("locked")),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status") as mock_refresh,
        patch("backend.main._ensure_persistent_view"),
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.core.metadata.usage_log_db.get_con"),
    ):
        # Must not raise; refresh_config_status should still run
        _initialize_service({"service_id": "svc-1"})

    mock_refresh.assert_called_once_with("svc-1")


def test_initialize_service_skips_status_refresh_when_no_source():
    """``get_source_for_service`` returns None → service is configured
    but has no resolvable source (mid-teardown?). Skip refresh."""
    from backend.main import _initialize_service

    with (
        patch("backend.core.metadata.reap_running_cron_runs", return_value=0),
        patch("backend.core.duckdb.get_source_for_service", return_value=None),
        patch("backend.core.duckdb.refresh_config_status") as mock_refresh,
    ):
        _initialize_service({"service_id": "svc-1"})

    mock_refresh.assert_not_called()


def test_initialize_service_swallows_unexpected_outer_exception():
    """The outer try/except catches anything — pinned because the
    ThreadPoolExecutor in ``_background_startup`` would otherwise
    surface this as a thread-pool .map() failure on the first bad
    service and abort initialisation for all others."""
    from backend.main import _initialize_service

    with patch(
        "backend.core.duckdb.get_source_for_service",
        side_effect=RuntimeError("unexpected"),
    ):
        _initialize_service({"service_id": "svc-1"})  # must not raise


def test_initialize_service_calls_ensure_persistent_view():
    """``_initialize_service`` must call ``_ensure_persistent_view`` after
    ``refresh_config_status``. Pinned because the scheduler only builds
    the Iceberg view at the END of a full sync cycle; without this
    startup call, the dashboard surfaces "Table does not exist" for
    minutes after a cold start where the persisted view was dropped."""
    from backend.main import _initialize_service

    fake_src = {"name": "svc-1", "service_id": "svc-1"}
    with (
        patch("backend.core.metadata.reap_running_cron_runs", return_value=0),
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.main._ensure_persistent_view") as mock_ensure,
    ):
        _initialize_service({"service_id": "svc-1"})

    mock_ensure.assert_called_once_with("svc-1", fake_src)


# ── _ensure_persistent_view ──────────────────────────────────────────────────


def test_ensure_persistent_view_always_rebuilds_at_startup():
    """The view is unconditionally rebuilt at startup — pre-warming the
    Iceberg view list so the first dashboard load after a restart
    doesn't pay the 500-900ms CREATE OR REPLACE VIEW cost mid-request.
    The previous "skip if exists" behaviour kept stale views around
    when buffer batches had been appended since the last writer close."""
    from backend.main import _ensure_persistent_view

    fake_src = {"name": "svc-1"}
    writer_con = MagicMock()

    with (
        patch("backend.core.duckdb.get_connection", return_value=writer_con) as mock_get_conn,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        _ensure_persistent_view("svc-1", fake_src)

    # Only the writer — no RO probe anymore
    assert mock_get_conn.call_count == 1
    _, kwargs = mock_get_conn.call_args
    assert kwargs.get("read_only") is False
    mock_update.assert_called_once_with(writer_con, fake_src)
    writer_con.close.assert_called_once()


def test_ensure_persistent_view_closes_writer_on_update_failure():
    """If ``update_iceberg_view`` raises (network, catalog corruption),
    the writer connection MUST still be closed — otherwise the .duckdb
    file's WAL lock leaks and the next process can't open it."""
    from backend.main import _ensure_persistent_view

    fake_src = {"name": "svc-1"}
    writer_con = MagicMock()

    with (
        patch("backend.core.duckdb.get_connection", return_value=writer_con),
        patch(
            "backend.core.iceberg.update_iceberg_view",
            side_effect=RuntimeError("catalog corrupt"),
        ),
    ):
        _ensure_persistent_view("svc-1", fake_src)  # must not raise

    writer_con.close.assert_called_once()


def test_ensure_persistent_view_swallows_outer_exceptions():
    """Any failure during the startup view-build must NOT abort
    ``_initialize_service`` — the dashboard would still work once the
    scheduler completes its first sync. Loud-fail here would prevent
    other services in the same ``ThreadPoolExecutor.map`` from
    initialising."""
    from backend.main import _ensure_persistent_view

    with patch(
        "backend.core.duckdb.get_connection",
        side_effect=RuntimeError("db locked"),
    ):
        _ensure_persistent_view("svc-1", {"name": "svc-1"})  # must not raise


# ── _ensure_pop_cache ────────────────────────────────────────────────────────


def test_ensure_pop_cache_skips_when_cache_already_exists(tmp_path):
    """Cache file exists → no fetch (POP locations rarely change, the
    prefetch is purely a UX optimisation for first-run installs)."""
    from backend.main import _ensure_pop_cache

    cache_file = tmp_path / "pop_cache.json"
    cache_file.write_text("[]")

    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(cache_file)),
        patch("backend.utils.pop_utils.fetch_pop_locations") as mock_fetch,
    ):
        _ensure_pop_cache()

    mock_fetch.assert_not_called()


def test_ensure_pop_cache_fetches_with_first_available_api_key(tmp_path):
    """Iterates configs to find the first non-empty Fastly API key —
    pinned because falling through silently here means no POP data is
    cached, and the map view degrades to gray dots."""
    from backend.main import _ensure_pop_cache

    missing_cache = tmp_path / "missing.json"
    configs = [
        {"service_id": "a", "fastly_api_key": ""},  # blank → skip
        {"service_id": "b", "fastly_api_key": "key-b"},  # used
        {"service_id": "c", "fastly_api_key": "key-c"},  # never reached
    ]

    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(missing_cache)),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=True) as mock_fetch,
    ):
        _ensure_pop_cache()

    mock_fetch.assert_called_once_with("key-b")


def test_ensure_pop_cache_logs_warning_on_fetch_failure(tmp_path):
    from backend.main import _ensure_pop_cache

    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(tmp_path / "missing.json")),
        patch("backend.config.list_configs", return_value=[{"service_id": "a", "fastly_api_key": "key"}]),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=False),
        patch("backend.main.logging.warning") as mock_warn,
    ):
        _ensure_pop_cache()

    # At least one warning about fetch failure
    assert any("failed" in str(c).lower() for c in mock_warn.call_args_list)


def test_ensure_pop_cache_no_api_key_skips_silently(tmp_path):
    """If no service has an API key, prefetch is skipped (no error).
    Pinned because raising here would block startup for an analyst
    install where the read-only deployment has no API keys."""
    from backend.main import _ensure_pop_cache

    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(tmp_path / "missing.json")),
        patch("backend.config.list_configs", return_value=[{"service_id": "a", "fastly_api_key": ""}]),
        patch("backend.utils.pop_utils.fetch_pop_locations") as mock_fetch,
    ):
        _ensure_pop_cache()  # must not raise

    mock_fetch.assert_not_called()


def test_ensure_pop_cache_swallows_unexpected_exceptions():
    """Any exception inside the helper must be caught — startup must
    not abort on a missing import or transient FS error."""
    from backend.main import _ensure_pop_cache

    with patch("backend.utils.pop_utils.CACHE_FILE", side_effect=RuntimeError("oops")):
        _ensure_pop_cache()  # must not raise


# ── _ensure_scoring_matrix ──────────────────────────────────────────────────


def test_ensure_scoring_matrix_writes_tenant_scoped_paths_for_each_service(tmp_path):
    """Pre-audit-finding-005 the boot helper wrote every service's FOS-pulled
    matrix to the shared ``matrix.json`` and broke after the first success,
    so service A's matrix would silently serve service B until B's first
    retrain. Pin the tenant-scoped path + no-early-break behaviour so the
    cross-tenant leak can't come back."""
    from backend.main import _ensure_scoring_matrix

    fake_matrix_path = tmp_path / "matrix.json"
    configs = [
        {"service_id": "svc-a", "scoring": {"enabled": True}},
        {"service_id": "svc-b", "scoring": {"enabled": True}},
        {"service_id": "svc-c", "scoring": {"enabled": False}},  # skipped
    ]
    matrices = {
        "svc-a": {"version": "a-v1", "vocab_size": 10},
        "svc-b": {"version": "b-v1", "vocab_size": 20},
    }

    with (
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", fake_matrix_path),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.state_sync.fetch_matrix_from_fos", side_effect=lambda sid: matrices.get(sid)),
    ):
        _ensure_scoring_matrix()

    # Both enabled services land in their own tenant-scoped file —
    # NOT the shared matrix.json.
    assert (tmp_path / "matrix_svc-a.json").exists()
    assert (tmp_path / "matrix_svc-b.json").exists()
    assert not fake_matrix_path.exists(), "shared matrix.json must not be written"

    import json as _json

    assert _json.loads((tmp_path / "matrix_svc-a.json").read_text())["version"] == "a-v1"
    assert _json.loads((tmp_path / "matrix_svc-b.json").read_text())["version"] == "b-v1"


def test_ensure_scoring_matrix_tolerates_per_service_failure(tmp_path):
    """One service's FOS fetch failing must not break the others —
    startup is best-effort, partial coverage > no coverage."""
    from backend.main import _ensure_scoring_matrix

    fake_matrix_path = tmp_path / "matrix.json"

    def fetch(sid):
        if sid == "svc-bad":
            raise RuntimeError("FOS unreachable")
        return {"version": "v1", "vocab_size": 5}

    configs = [
        {"service_id": "svc-bad", "scoring": {"enabled": True}},
        {"service_id": "svc-good", "scoring": {"enabled": True}},
    ]

    with (
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", fake_matrix_path),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.state_sync.fetch_matrix_from_fos", side_effect=fetch),
    ):
        _ensure_scoring_matrix()  # must not raise

    assert (tmp_path / "matrix_svc-good.json").exists()
    assert not (tmp_path / "matrix_svc-bad.json").exists()


def test_ensure_scoring_matrix_skips_services_without_scoring_enabled(tmp_path):
    """Services with no ``scoring`` block or ``enabled: false`` must not
    trigger a FOS fetch — bounds per-restart FOS calls to actual scorers."""
    from backend.main import _ensure_scoring_matrix

    fake_matrix_path = tmp_path / "matrix.json"
    configs = [
        {"service_id": "svc-off", "scoring": {"enabled": False}},
        {"service_id": "svc-none"},
    ]

    with (
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", fake_matrix_path),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.state_sync.fetch_matrix_from_fos") as mock_fetch,
    ):
        _ensure_scoring_matrix()

    mock_fetch.assert_not_called()


# ── _background_startup ────────────────────────────────────────────────────


def test_background_startup_reloads_db_and_initialises_each_service():
    from backend.main import _background_startup

    fake_scheduler = MagicMock()
    configs = [{"service_id": "a"}, {"service_id": "b"}]

    with (
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.main._initialize_service") as mock_init,
    ):
        _background_startup()

    fake_scheduler.start.assert_called_once()
    # Both services initialised
    initialised_ids = {call[0][0]["service_id"] for call in mock_init.call_args_list}
    assert initialised_ids == {"a", "b"}


def test_background_startup_tolerates_reload_default_source_failure():
    """If ``reload_default_source`` raises, startup must continue and
    still try to init the scheduler + services. Pinned because a
    transient FOS error here would otherwise leave the scheduler
    unstarted, breaking all crons."""
    from backend.main import _background_startup

    fake_scheduler = MagicMock()
    with (
        patch("backend.core.duckdb.reload_default_source", side_effect=RuntimeError("S3 down")),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.config.list_configs", return_value=[]),
    ):
        _background_startup()

    fake_scheduler.start.assert_called_once()


def test_background_startup_swallows_scheduler_failure():
    """If the scheduler itself blows up, log + return — don't crash
    the daemon thread (which would surface as a startup hang)."""
    from backend.main import _background_startup

    with (
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", side_effect=RuntimeError("scheduler oom")),
    ):
        _background_startup()  # must not raise


# ── /api/health ─────────────────────────────────────────────────────────────


def test_health_endpoint_returns_ok_status(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body


def test_health_endpoint_does_not_require_service_id(client):
    """Health probe must work without ANY service config — used by
    k8s liveness before the first svc is provisioned."""
    resp = client.get("/api/health")  # no x-fastly-service-id header
    assert resp.status_code == 200


def test_health_endpoint_deep_returns_shallow_for_remote_analyst():
    """Finding 003: ``GET /api/health?deep=1`` returned a per-service
    list including service_ids + last-sync error messages, which is
    operational metadata an analyst should not see. With the fix,
    remote (live-share) callers get the shallow ok/version response
    regardless of the ``deep`` flag — load-balancer liveness still
    works, infrastructure enumeration is blocked.

    Calls the handler directly with a mock Request so the patch can
    flip ``is_request_remote`` for the handler alone (going through
    TestClient would also flip it for the middleware, which would
    then 400 on the host_not_allowed gate before the handler runs)."""
    from unittest.mock import MagicMock, patch

    from backend.main import health_check

    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=["secret-svc-1", "secret-svc-2"]),
        patch("backend.utils.remote_access.is_request_remote", return_value=True),
    ):
        result = health_check(fake_request, deep=True)

    # Shallow result is a bare dict (no JSONResponse wrapping), so we
    # inspect it directly.
    assert isinstance(result, dict)
    assert result["status"] == "ok"
    assert "version" in result
    assert "services" not in result, f"deep=1 from remote leaked services: {result.get('services')!r}"
    payload_str = str(result)
    assert "secret-svc-1" not in payload_str
    assert "secret-svc-2" not in payload_str


def test_health_endpoint_deep_returns_full_payload_for_admin():
    """Companion: admin/loopback callers still get the full per-service
    deep report (not blocked, just the analyst path is hardened)."""
    import sqlite3
    from datetime import UTC, datetime
    from unittest.mock import MagicMock, patch

    from backend.main import health_check

    fake_request = MagicMock()
    with patch("backend.config.list_service_ids", return_value=["admin-svc-1"]):
        with patch("backend.core.metadata.get_con") as mock_get_con:
            con = sqlite3.connect(":memory:", check_same_thread=False)
            con.row_factory = sqlite3.Row
            con.execute("CREATE TABLE ingested_files (source_name TEXT, ingested_at TEXT)")
            con.execute("CREATE TABLE cron_runs (task TEXT, status TEXT, started_at TEXT, error_message TEXT)")
            now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
            con.execute("INSERT INTO ingested_files VALUES (?, ?)", ("admin-svc-1", now_str))
            mock_get_con.return_value = con
            with patch("backend.utils.remote_access.is_request_remote", return_value=False):
                result = health_check(fake_request, deep=True)

    assert isinstance(result, dict)
    assert result["status"] == "ok"
    assert "services" in result, "admin caller must get the per-service report"
    assert any(s["service_id"] == "admin-svc-1" for s in result["services"])


def test_health_endpoint_deep(client):
    """Deep health check verifies ingest freshness and cron status."""
    import sqlite3
    from datetime import UTC, datetime, timedelta
    from unittest.mock import patch

    with patch("backend.config.list_service_ids", return_value=["test-svc-1"]):
        with patch("backend.core.metadata.get_con") as mock_get_con:
            con = sqlite3.connect(":memory:", check_same_thread=False)
            con.row_factory = sqlite3.Row
            con.execute("CREATE TABLE ingested_files (source_name TEXT, ingested_at TEXT)")
            con.execute("CREATE TABLE cron_runs (task TEXT, status TEXT, started_at TEXT, error_message TEXT)")
            mock_get_con.return_value = con

            # 1. No ingested files -> OK
            resp = client.get("/api/health?deep=1")
            assert resp.status_code == 200
            assert resp.json()["status"] == "ok"

            # 2. Fresh ingest (using space separator as SQLite datetime('now') does) -> OK
            now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
            con.execute("INSERT INTO ingested_files VALUES (?, ?)", ("test-svc-1", now_str))
            resp = client.get("/api/health?deep=1")
            assert resp.status_code == 200
            assert resp.json()["status"] == "ok"

            # 3. Stale ingest -> OK overall but service status is 'stale'
            stale_str = (datetime.now(UTC) - timedelta(minutes=45)).strftime("%Y-%m-%d %H:%M:%S")
            con.execute("DELETE FROM ingested_files")
            con.execute("INSERT INTO ingested_files VALUES (?, ?)", ("test-svc-1", stale_str))
            resp = client.get("/api/health?deep=1")
            assert resp.status_code == 200
            assert resp.json()["status"] == "ok"
            assert resp.json()["services"][0]["status"] == "stale"


# ── telemetry middleware: cdn_service_id resolution ────────────────────────


def test_middleware_resolves_cdn_service_id_to_logging_service_id(client):
    """A request with ``x-fastly-service-id: <cdn-id>`` (not a
    logging-id) should still flush usage logs for the resolved
    logging service. Pinned because the CDN UI sends cdn_service_id,
    not the underlying logging service id."""
    from backend import config as svcconfig

    fake_configs = [
        {"service_id": "log-svc-1", "cdn_service_id": "cdn-svc-1"},
    ]

    with (
        patch.object(svcconfig, "load_config", return_value=None),
        patch.object(svcconfig, "get_cdn_service_id_map", return_value={"cdn-svc-1": "log-svc-1"}),
        patch.object(svcconfig, "get_active_service_id", return_value=None),
        patch("backend.utils.usage_logger.flush_usage_log") as mock_flush,
    ):
        client.get("/api/health", headers={"x-fastly-service-id": "cdn-svc-1"})

    # Middleware should have resolved cdn-svc-1 → log-svc-1 and flushed for it
    if mock_flush.called:
        # The resolved logging service id is what gets flushed
        flushed_sid = mock_flush.call_args[0][0]
        assert flushed_sid == "log-svc-1"


def test_middleware_flush_swallows_exceptions(client):
    """The flush call is wrapped in try/except — pinned because a
    usage-log write failure on a long-running query would otherwise
    fail the request AFTER the response was already streamed back."""
    with patch("backend.utils.usage_logger.flush_usage_log", side_effect=RuntimeError("disk full")):
        resp = client.get("/api/health")

    assert resp.status_code == 200  # request succeeds despite flush error


# ── _bounded_scheduler_shutdown ──────────────────────────────────────────────


def test_bounded_shutdown_returns_quickly_when_scheduler_finishes_fast():
    """The 60s timeout is a CEILING. When the scheduler has no running jobs
    (or they finish promptly), bounded shutdown should return in milliseconds
    — not sit and wait for the deadline. Pins that the success path doesn't
    accidentally introduce a fixed 60s pause on every clean restart."""
    import time

    from backend.main import _bounded_scheduler_shutdown

    fake_scheduler = MagicMock()
    fake_scheduler.shutdown = MagicMock(return_value=None)  # returns immediately

    t0 = time.monotonic()
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=10.0)
    elapsed = time.monotonic() - t0

    assert elapsed < 0.5, f"fast-path shutdown took {elapsed:.2f}s, expected <0.5s"
    fake_scheduler.shutdown.assert_called_once_with(wait=True)


def test_bounded_shutdown_caps_at_timeout_when_scheduler_hangs():
    """A scheduler.shutdown(wait=True) that takes longer than timeout_secs
    must NOT hang the lifespan past the deadline. Pins that the bounded
    wait actually releases — without this, a stuck cron would block the
    restart past Docker's stop_grace_period and trigger SIGKILL, which
    would defeat the whole graceful-shutdown contract."""
    import threading
    import time

    from backend.main import _bounded_scheduler_shutdown

    # Block forever in the fake scheduler so the bounded wait MUST trip
    # the timeout to return.
    never_returns = threading.Event()

    def _hangs_forever(wait):  # noqa: ARG001
        never_returns.wait()

    fake_scheduler = MagicMock()
    fake_scheduler.shutdown = _hangs_forever

    t0 = time.monotonic()
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=0.3)
    elapsed = time.monotonic() - t0

    # Released within the timeout (allow a small overshoot for thread sched).
    assert 0.25 <= elapsed < 0.6, f"bounded wait released at {elapsed:.2f}s, expected ~0.3s"

    # Unblock the worker so the daemon thread can exit cleanly.
    never_returns.set()


def test_bounded_shutdown_does_not_raise_when_shutdown_throws():
    """A scheduler.shutdown that raises mid-execution must not propagate
    out of bounded shutdown — the rest of the lifespan teardown (close
    DuckDB connections, etc.) needs to run regardless."""
    from backend.main import _bounded_scheduler_shutdown

    fake_scheduler = MagicMock()
    fake_scheduler.shutdown = MagicMock(side_effect=RuntimeError("apscheduler internal"))

    # Should not raise — the warning log is the only side-effect.
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=5.0)
