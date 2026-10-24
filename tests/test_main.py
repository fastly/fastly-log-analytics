"""Consolidated tests for test_main."""

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
        patch("backend.main.logger.info") as mock_log_info,
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
        patch("backend.main.logger.warning") as mock_warn,
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


# ── _maybe_instrument_asgi ────────────────────────────────────────────────────


def test_maybe_instrument_asgi_noop_when_otel_disabled():
    """The default/test-mode case: OTEL_EXPORTER=none (or pytest) means
    asgi_instrumentation_enabled() is False, so nothing gets wrapped."""
    from backend.main import _maybe_instrument_asgi

    fake_app = MagicMock()
    with (
        patch("backend.main.request_telemetry.asgi_instrumentation_enabled", return_value=False),
        patch("backend.main.FastAPIInstrumentor") as mock_instrumentor,
    ):
        _maybe_instrument_asgi(fake_app)

    mock_instrumentor.instrument_app.assert_not_called()


def test_maybe_instrument_asgi_wraps_app_when_otel_enabled():
    """When OTel is configured to export, the SDK must be initialised
    before instrument_app runs (see ensure_initialized's docstring: a
    process whose first request never calls get_tracer() would otherwise
    leave FastAPIInstrumentor's spans permanently no-op), and the
    receive/send child spans must stay excluded so a span-metrics
    processor doesn't bucket them as their own noisy series."""
    from backend.main import _maybe_instrument_asgi

    fake_app = MagicMock()
    with (
        patch("backend.main.request_telemetry.asgi_instrumentation_enabled", return_value=True),
        patch("backend.main.request_telemetry.ensure_initialized") as mock_ensure_init,
        patch("backend.main.FastAPIInstrumentor") as mock_instrumentor,
    ):
        _maybe_instrument_asgi(fake_app)

    mock_ensure_init.assert_called_once()
    mock_instrumentor.instrument_app.assert_called_once_with(fake_app, exclude_spans=["receive", "send"])


# ── Merged from test_main_startup.py ──


from backend.core import rollup_readiness as rr


def test_initialize_service_defers_durable_rollup_catchup():
    rr.reset_rollup_coverage_ready()
    cfg = {"service_id": "svc-durable"}
    fake_src = {"service_id": "svc-durable", "deployment_mode": "high_scale"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.main._ensure_persistent_view"),
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch(
            "backend.core.rollups.recompute.backfill_missing_hour_bundles",
            side_effect=AssertionError("startup rollup catch-up must be deferred"),
        ) as mock_backfill,
    ):
        from backend.main import _initialize_service

        _initialize_service(cfg)

    mock_backfill.assert_not_called()
    assert rr.rollup_coverage_ready("svc-durable") is False


def test_initialize_service_skips_catchup_for_file_mode_service():
    rr.reset_rollup_coverage_ready()
    cfg = {"service_id": "svc-file"}
    fake_src = {"service_id": "svc-file", "deployment_mode": "standard"}

    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.main._ensure_persistent_view"),
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.config.is_durable_serving_mode", return_value=False),
        patch("backend.core.rollups.recompute.backfill_missing_hour_bundles") as mock_backfill,
    ):
        from backend.main import _initialize_service

        _initialize_service(cfg)

    mock_backfill.assert_not_called()
    # File mode never needed the flag, but it should read False rather than
    # crash — nothing in the file-mode gate checks it (Task 3 only adds the
    # `and not rollup_coverage_ready(...)` clause behind `is_durable_serving_mode`,
    # so this is short-circuited before the flag is ever read in production
    # code — this assertion just documents the flag stays untouched here).
    assert rr.rollup_coverage_ready("svc-file") is False


# ── Merged from test_deps.py ──────────────────────────────────────────────

from unittest.mock import ANY

import pytest
from fastapi import HTTPException

from backend import deps
from backend.core.duckdb import DBBusyError


def _call_get_service_id(service=None, sid=None, x_fastly_service_id=None, x_service_id=None) -> str | None:
    """Call get_service_id directly without going through FastAPI's DI.

    The signature uses ``Query(default=None)`` / ``Header(default=None)``
    as parameter defaults — those are ``Query``/``Header`` instances,
    not None — so calling the function bare would fail the truthiness
    check. This helper bypasses by always passing all four explicitly.
    """
    request = MagicMock()
    request.path_params = {}
    return deps.get_service_id(
        request=request,
        service=service,
        sid=sid,
        x_fastly_service_id=x_fastly_service_id,
        x_service_id=x_service_id,
    )


# ── get_service_id: resolution priority ──────────────────────────────────────


def test_get_service_id_returns_explicit_id_when_config_loads():
    """When the provided id resolves to a real config, return it as-is."""
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": "abc"}):
        assert _call_get_service_id(sid="abc") == "abc"


def test_get_service_id_query_param_service_takes_priority():
    """``?service=`` is checked first — used by the frontend's URL state
    machine. Other params are tried in order: sid, x-fastly, x-service."""
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": "from-query"}):
        out = _call_get_service_id(
            service="from-query",
            sid="from-sid",
            x_fastly_service_id="from-fastly-header",
            x_service_id="from-svc-header",
        )
    assert out == "from-query"


def test_get_service_id_falls_back_to_x_fastly_header():
    """When no query params present, the x-fastly-service-id header wins."""
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": "hdr"}):
        out = _call_get_service_id(x_fastly_service_id="hdr")
    assert out == "hdr"


def test_get_service_id_x_service_id_header_used_as_last_resort():
    """x-service-id is the fallback when nothing else is set — kept for
    backwards-compat with older clients that don't send the Fastly-
    namespaced header."""
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": "old"}):
        out = _call_get_service_id(x_service_id="old")
    assert out == "old"


def test_get_service_id_resolves_cdn_service_id_to_logging_service_id():
    """A CDN service has its own Fastly service ID; when the frontend
    sends that, deps must look it up via ``cdn_service_id`` and return
    the *logging* service ID the rest of the app uses as the key."""
    with (
        patch("backend.deps.svcconfig.load_config", return_value=None),
        patch("backend.deps.svcconfig.get_cdn_service_id_map", return_value={"cdn-svc-1": "log-svc-1"}),
    ):
        assert _call_get_service_id(sid="cdn-svc-1") == "log-svc-1"


def test_get_service_id_passes_through_unknown_id_when_no_cdn_match():
    """Unknown id with no cdn match → return it anyway (don't 404 at the
    dep layer). Downstream ``get_source`` is what turns "unknown" into
    a 400 — keeping the resolution stage permissive lets routes that
    don't need a source still serve requests."""
    with (
        patch("backend.deps.svcconfig.load_config", return_value=None),
        patch("backend.deps.svcconfig.get_cdn_service_id_map", return_value={}),
    ):
        assert _call_get_service_id(sid="ghost") == "ghost"


def test_get_service_id_falls_back_to_active_when_nothing_provided():
    """No params/headers + active service set → return the active id."""
    with patch("backend.deps.svcconfig.get_active_service_id", return_value="active-svc"):
        assert _call_get_service_id() == "active-svc"


def test_get_service_id_returns_none_when_no_active_and_nothing_provided():
    """Bare install with no service configured → None. Routes that
    can handle None (like ``/api/presets``) return empty; routes that
    require it (via ``get_source``) raise 400."""
    with patch("backend.deps.svcconfig.get_active_service_id", return_value=None):
        assert _call_get_service_id() is None


# ── get_source: source lookup + 400 on miss ──────────────────────────────────


def test_get_source_returns_source_dict_when_found():
    fake_src = {"name": "logs_svc", "service_id": "svc"}
    with patch("backend.deps.db.get_source_for_service", return_value=fake_src):
        assert deps.get_source(service_id="svc") is fake_src


def test_get_source_raises_400_when_no_service_id():
    """No active service → 400 with ``no_service: true`` in the detail.
    The frontend keys on ``no_service`` to render the "configure first"
    onboarding state instead of a generic error banner."""
    with pytest.raises(HTTPException) as exc:
        deps.get_source(service_id=None)
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


def test_get_source_raises_400_when_lookup_returns_none():
    """Service id present but no source found (stale frontend URL after
    a teardown) → 400, same error shape as no-service-at-all."""
    with patch("backend.deps.db.get_source_for_service", return_value=None):
        with pytest.raises(HTTPException) as exc:
            deps.get_source(service_id="ghost")
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


# ── _ConnectionHolder: enter / DBBusyError / exit ────────────────────────────
#
# The pool path (``backend.core.duckdb_pool``) keeps connections alive across
# requests and ultimately calls ``backend.core.duckdb.get_connection`` rather
# than ``backend.deps.get_connection`` — so a mock at the deps-level reference
# doesn't intercept. Tests below that pin the always-fresh open/close
# lifecycle disable the pool via ``DUCKDB_CONNECTION_POOL=0``; this preserves
# the legacy contract assertions while keeping pool-specific behaviour in
# its own targeted tests further down.


@pytest.fixture
def disable_pool(monkeypatch):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    yield


def test_connection_holder_opens_and_closes(disable_pool):
    """Happy path: enter opens a connection via ``get_connection``,
    exit closes it cleanly."""
    fake_con = MagicMock()
    src = {"name": "x"}

    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        holder = deps._ConnectionHolder(src, read_only=True)
        con = holder.__enter__()
        assert con is fake_con
        # The kwargs the holder passes are part of the contract with get_connection
        mock_get.assert_called_once_with(source=src, max_wait=ANY, skip_view_update=False, read_only=True)
        holder.__exit__(None, None, None)
        fake_con.close.assert_called_once()


def test_connection_holder_skip_view_update_forwarded():
    """``skip_view_update=True`` (used by metadata routes) must propagate
    to ``get_connection``; otherwise admin routes would block on
    Iceberg manifest reads."""
    with patch("backend.deps.get_connection") as mock_get:
        holder = deps._ConnectionHolder({"name": "x"}, skip_view_update=True)
        holder.__enter__()
        assert mock_get.call_args.kwargs["skip_view_update"] is True


def test_connection_holder_default_max_wait_is_10s(disable_pool, monkeypatch):
    """The default checkout wait covers ordinary dashboard fan-out bursts
    without turning pool queuing into a user-visible 503."""
    monkeypatch.delenv("DUCKDB_POOL_MAX_WAIT_S", raising=False)
    fake_con = MagicMock()
    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        holder = deps._ConnectionHolder({"name": "x"}, read_only=True)
        holder.__enter__()
        assert mock_get.call_args.kwargs["max_wait"] == 10.0


def test_connection_holder_max_wait_env_override_respected(disable_pool, monkeypatch):
    monkeypatch.setenv("DUCKDB_POOL_MAX_WAIT_S", "9.5")
    fake_con = MagicMock()
    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        holder = deps._ConnectionHolder({"name": "x"}, read_only=True)
        holder.__enter__()
        assert mock_get.call_args.kwargs["max_wait"] == 9.5


def test_connection_holder_db_busy_maps_to_503(disable_pool):
    """``DBBusyError`` from get_connection → 503 with ``busy: true``.
    The 503 (rather than 500/400) is what makes the frontend's React
    Query layer keep its cached data instead of clearing the UI."""
    with patch("backend.deps.get_connection", side_effect=DBBusyError("locked")):
        holder = deps._ConnectionHolder({"name": "x"})
        with pytest.raises(HTTPException) as exc:
            holder.__enter__()
    assert exc.value.status_code == 503
    assert exc.value.detail["busy"] is True


def test_connection_holder_exit_tolerates_close_failure(disable_pool):
    """If ``close()`` raises (locked DB, double-close), the dependency
    must still tear down cleanly — otherwise the request fails AFTER
    the response was already sent."""
    fake_con = MagicMock()
    fake_con.close.side_effect = RuntimeError("already closed")

    with patch("backend.deps.get_connection", return_value=fake_con):
        holder = deps._ConnectionHolder({"name": "x"})
        holder.__enter__()
        holder.__exit__(None, None, None)  # must not raise


def test_connection_holder_exit_with_no_open_connection_is_noop():
    """If __enter__ never ran (or already exited), __exit__ should
    be a no-op rather than NoneType.close()."""
    holder = deps._ConnectionHolder({"name": "x"})
    # Never entered — con is None
    holder.__exit__(None, None, None)  # must not raise


# ── get_con: generator-style dependency ──────────────────────────────────────


def test_get_con_yields_connection_and_closes_after(disable_pool):
    """``get_con`` is a generator dep — FastAPI calls .send(None), gets
    the connection, then calls .close() after the response. The con
    must be closed at that point."""
    fake_con = MagicMock()

    with patch("backend.deps.get_connection", return_value=fake_con):
        gen = deps.get_con(source={"name": "x"})
        con = next(gen)
        assert con is fake_con
        # Drain the generator → triggers __exit__
        with pytest.raises(StopIteration):
            next(gen)
        fake_con.close.assert_called_once()


def test_get_meta_con_symbol_removed():
    """v2.0 cut: ``get_meta_con`` was deleted. The pool fingerprint check
    in ``duckdb_pool.checkout_connection`` skips ``update_iceberg_view``
    when the (view-cache identity, buffer mtime) tuple is unchanged, so
    the dedicated skip-view-update dep that bootstrap routes used is
    no longer needed. Pin removal so a future refactor doesn't quietly
    re-introduce it."""
    assert not hasattr(deps, "get_meta_con"), (
        "get_meta_con was removed at the v2.0 cut. Routes that used it should use "
        "get_con instead; the pool fingerprint check makes the skip-view "
        "optimization unnecessary."
    )


def test_get_con_default_is_read_only(disable_pool):
    """Dashboard queries default to read_only=True — otherwise long-running
    cron writes would block them. Pinning this default so a future
    refactor doesn't silently flip it."""
    fake_con = MagicMock()
    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        gen = deps.get_con(source={"name": "x"})
        next(gen)
        assert mock_get.call_args.kwargs["read_only"] is True


# ── _ConnectionHolder: pool path ─────────────────────────────────────────────


def test_connection_holder_pool_path_checks_out_and_returns():
    """When the pool is enabled (default), read_only requests route through
    ``duckdb_pool.checkout_connection`` instead of calling get_connection
    directly. The holder must still expose the connection on ``self.con``
    and let the pool reclaim it on exit (not call ``close`` on the
    connection itself — that would defeat reuse)."""
    from backend.core import duckdb_pool

    fake_con = MagicMock()

    # Build a context manager stub the holder's __enter__ will drive.
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=fake_con)
    cm.__exit__ = MagicMock(return_value=False)

    with patch.object(duckdb_pool, "checkout_connection", return_value=cm) as mock_checkout:
        holder = deps._ConnectionHolder({"name": "x"}, read_only=True)
        con = holder.__enter__()
        assert con is fake_con
        mock_checkout.assert_called_once()
        # Exit must forward to the pool context manager (which knows whether
        # to return-vs-discard); the holder itself MUST NOT call con.close.
        holder.__exit__(None, None, None)
        cm.__exit__.assert_called_once()
        fake_con.close.assert_not_called()


def test_connection_holder_pool_path_skipped_when_skip_view_update():
    """``skip_view_update`` paths can't pool — they need a fresh, un-bound
    connection. Verify the holder falls through to the legacy code path
    for that case even when the pool is enabled."""
    from backend.core import duckdb_pool

    fake_con = MagicMock()
    with (
        patch.object(duckdb_pool, "checkout_connection") as mock_checkout,
        patch("backend.deps.get_connection", return_value=fake_con) as mock_get,
    ):
        holder = deps._ConnectionHolder({"name": "x"}, skip_view_update=True)
        holder.__enter__()
        mock_checkout.assert_not_called()
        mock_get.assert_called_once()


def test_connection_holder_pool_path_discards_on_error():
    """If the request raised mid-query, the pool must mark the connection
    errored on exit so a poisoned connection doesn't get reused. We rely
    on passing the original exc_type to the pool's __exit__."""
    from backend.core import duckdb_pool

    fake_con = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=fake_con)
    cm.__exit__ = MagicMock(return_value=False)

    with patch.object(duckdb_pool, "checkout_connection", return_value=cm):
        holder = deps._ConnectionHolder({"name": "x"}, read_only=True)
        holder.__enter__()
        # Simulate request error
        holder.__exit__(RuntimeError, RuntimeError("boom"), None)
        # Pool CM sees the exc_type — its release() uses that to discard
        called_exc_type = cm.__exit__.call_args[0][0]
        assert called_exc_type is RuntimeError


# ── AnalyticsDeps: removed at v2.0 cut ───────────────────────────────────────


def test_analytics_deps_symbol_removed():
    """v2.0 cut Phase 8: the bundled ``AnalyticsDeps`` (get_source + get_con)
    was replaced by :class:`backend.core.request_context.RequestContext`
    via ``Depends(build_request_context)``. The new dep enforces analyst
    tenancy structurally (the old bundle skipped it because
    ``require_service_access`` was never wired as a sibling dep on any
    route). Pin removal so a refactor doesn't quietly re-introduce it."""
    assert not hasattr(deps, "AnalyticsDeps"), (
        "AnalyticsDeps was removed at v2.0 cut. Routes use "
        "RequestContext via Depends(build_request_context); access "
        "ctx.source / ctx.con / ctx.service_id."
    )


# ── Merged from test_sre_observability.py ──────────────────────────────────────────────

import json
import logging
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

# ── SRE-01 / SRE-02: the access log carries a correlation id + latency ──────


def test_admin_access_log_carries_rid_and_duration(client):
    """The [admin] access line must include rid=<id> and a (Nms) latency so an
    operator can find the slow request in `docker logs` AND join it to the
    slow_queries rows it spawned. Attaches a handler directly to the named
    logger so the assertion is robust to structlog's stdlib bridge."""
    captured: list[str] = []
    h = logging.Handler()
    h.emit = lambda record: captured.append(record.getMessage())  # type: ignore[method-assign]
    lg = logging.getLogger("backend.access.admin")
    lg.addHandler(h)
    old = lg.level
    lg.setLevel(logging.INFO)
    try:
        r = client.get("/api/health")
    finally:
        lg.removeHandler(h)
        lg.setLevel(old)

    assert r.status_code == 200
    assert any("rid=" in m and "ms)" in m for m in captured), captured
    # The minted id must be non-empty (not the "-" fallback) — SRE-01.
    assert not any("rid=- " in m for m in captured), captured


def test_new_request_id_is_nonempty_hex():
    from backend.utils.remote_access import _new_request_id

    rid = _new_request_id()
    assert isinstance(rid, str) and len(rid) == 16
    int(rid, 16)  # parses as hex
    assert _new_request_id() != rid  # not a constant


# ── SRE-09: @global_job bodies run inside the cron attribution scope ─────────


def test_global_job_runs_in_cron_attribution_scope(monkeypatch):
    import backend.utils.system_jobs as sj
    from backend.core.query_attribution import current_attribution
    from backend.cron.decorators import global_job

    monkeypatch.setattr(sj, "record_job_run", lambda *a, **k: None)
    seen: dict = {}

    @global_job("share_audit_purge", color="35", tag="purge", label="Purge")
    def _job():
        attr = current_attribution.get()
        seen["kind"] = attr.kind if attr else None
        seen["job"] = attr.cron_job if attr else None
        return "ok"

    _job()
    assert seen["kind"] == "cron"
    assert seen["job"] == "share_audit_purge"
    # Scope is popped on exit — no leak into the next APScheduler job.
    assert current_attribution.get() is None


# ── SRE-12 / SRE-16: pool reject + warm counters in stats() ──────────────────


def test_pool_stats_exposes_reject_and_warm_fields():
    from backend.core.duckdb_pool import _Pool

    p = _Pool("svc-sre12", max_size=2)
    s = p.stats()
    assert s["saturated_rejects_total"] == 0
    assert s["drain_rejects_total"] == 0
    assert "last_warmed_at" in s and s["last_warmed_at"] is None


def test_pool_saturation_increments_reject_counter():
    from backend.core.duckdb_pool import _Pool, _PoolBusy

    p = _Pool("svc-sat", max_size=1)
    # Pretend the one slot is checked out and nothing idle → next acquire with
    # a zero deadline hits the saturation branch immediately (no DuckDB build).
    p._in_use = 1
    with pytest.raises(_PoolBusy):
        p.acquire({"name": "svc-sat"}, max_wait=0)
    assert p.stats()["saturated_rejects_total"] == 1
    assert p.stats()["drain_rejects_total"] == 0


def test_pool_drain_increments_drain_reject_counter():
    from backend.core.duckdb_pool import _Pool, _PoolBusy

    p = _Pool("svc-drain", max_size=2)
    p.begin_drain()
    with pytest.raises(_PoolBusy):
        p.acquire({"name": "svc-drain"}, max_wait=0)
    assert p.stats()["drain_rejects_total"] == 1
    assert p.stats()["saturated_rejects_total"] == 0


# ── SRE-18: the history cap is 400 (the value the UI copy now states) ─────────


def test_history_cap_is_400():
    from backend.core.query_registry import _HISTORY_CAP, QueryRegistry

    assert _HISTORY_CAP == 400
    assert QueryRegistry()._history.maxlen == 400


# ── SRE-04 / SRE-05: deep /api/health sees the ingestion-frozen modes ────────


def _deep_health(cron_rows: list[tuple], *, ingest_minutes_ago: int = 1):
    """Drive health_check(deep=True) against an in-memory metadata.db seeded
    with one recent ingest + the given cron_runs rows. Returns (status_code,
    parsed_body).

    Each cron_rows tuple is (task, status, mins_ago, err) or, for SRE-22
    adaptive-staleness tests, (task, status, mins_ago, err, ingest_count) —
    the 5th element is optional (defaults to 0, i.e. an "empty tick" that
    adaptive_stale_minutes' gap calculation ignores)."""
    from backend.main import health_check

    now = datetime.now(UTC)
    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE ingested_files (source_name TEXT, ingested_at TEXT)")
    con.execute(
        "CREATE TABLE cron_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT, "
        "status TEXT, started_at TEXT, error_message TEXT, "
        "files_downloaded INTEGER DEFAULT 0, rows_ingested INTEGER DEFAULT 0)"
    )
    con.execute(
        "INSERT INTO ingested_files VALUES (?, ?)",
        ("svc", (now - timedelta(minutes=ingest_minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")),
    )
    for row in cron_rows:
        task, status, mins_ago, err = row[:4]
        ingest_count = row[4] if len(row) > 4 else 0
        con.execute(
            "INSERT INTO cron_runs (task, status, started_at, error_message, rows_ingested) VALUES (?, ?, ?, ?, ?)",
            (task, status, (now - timedelta(minutes=mins_ago)).strftime("%Y-%m-%d %H:%M:%S"), err, ingest_count),
        )
    con.commit()

    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.core.metadata.get_con", return_value=con),
        patch("backend.utils.remote_access.is_request_remote", return_value=False),
    ):
        result = health_check(fake_request, deep=True)

    if isinstance(result, dict):
        return 200, result
    return result.status_code, json.loads(result.body)


def test_deep_health_healthy_when_sync_recent_and_terminal():
    # Recent ingest + sync success + commit success + no running row → ok.
    code, body = _deep_health([("sync", "success", 2, None), ("commit", "success", 3, None)])
    assert code == 200
    assert body["status"] == "ok"


def test_deep_health_degrades_on_stuck_running_sync():
    # SRE-04: a sync row stuck 'running' past the threshold — invisible to the
    # success-only filter — must degrade rather than report the older success.
    code, body = _deep_health([("sync", "success", 120, None), ("sync", "running", 20, None)])
    assert code == 503
    svc = body["services"][0]
    assert svc["status"] == "degraded"
    assert "stuck running" in svc["reason"]


def test_deep_health_not_fooled_by_briefly_running_sync():
    # A sync that just started (under the threshold) is healthy in-progress,
    # NOT a stall — must stay ok.
    code, body = _deep_health([("sync", "success", 30, None), ("sync", "running", 2, None)])
    assert code == 200
    assert body["status"] == "ok"


def test_deep_health_degrades_on_commit_error():
    # SRE-05: a commit cron erroring every run (buffer growing,
    # nothing reaching the lake) is discovery-invisible — must degrade.
    code, body = _deep_health([("sync", "success", 2, None), ("commit", "error", 1, "boom")])
    assert code == 503
    svc = body["services"][0]
    assert svc["status"] == "degraded"
    assert "commit cron errored" in svc["reason"]


def test_deep_health_degrades_on_metadata_sync_error():
    # SRE-05: a read_only analyst service's only ingest path is metadata_sync.
    code, body = _deep_health([("metadata_sync", "error", 1, "fos down")])
    assert code == 503
    assert body["services"][0]["status"] == "degraded"
    assert "metadata_sync cron errored" in body["services"][0]["reason"]


def test_deep_health_ignores_metadata_sync_error_in_durable_mode():
    # Durable read-write services never schedule metadata_sync; an old
    # failure from before the topology switch must not keep readiness red.
    from backend.main import health_check

    now = datetime.now(UTC)
    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE ingested_files (source_name TEXT, ingested_at TEXT)")
    con.execute(
        "CREATE TABLE cron_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT, "
        "status TEXT, started_at TEXT, error_message TEXT, "
        "files_downloaded INTEGER DEFAULT 0, rows_ingested INTEGER DEFAULT 0)"
    )
    con.execute(
        "INSERT INTO ingested_files VALUES (?, ?)",
        ("svc", (now - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S")),
    )
    con.execute(
        "INSERT INTO cron_runs (task, status, started_at, error_message) VALUES (?, ?, ?, ?)",
        ("metadata_sync", "error", now.strftime("%Y-%m-%d %H:%M:%S"), "old topology"),
    )
    con.execute(
        "INSERT INTO cron_runs (task, status, started_at) VALUES (?, ?, ?)",
        ("sync", "success", now.strftime("%Y-%m-%d %H:%M:%S")),
    )
    con.commit()

    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=["svc"]),
        patch("backend.config.is_durable_serving_mode", return_value=True),
        patch("backend.core.metadata.get_con", return_value=con),
        patch("backend.utils.remote_access.is_request_remote", return_value=False),
    ):
        result = health_check(fake_request, deep=True)

    assert result["status"] == "ok"


# ── SRE-22: adaptive per-service staleness widening ──────────────────────────


def _steady_cadence_rows(n: int, gap_minutes: int, *, start_after_minutes: int) -> list[tuple]:
    """n non-empty successful sync runs, gap_minutes apart, the most recent
    ending start_after_minutes ago — the historical cadence the naive-stale
    ingest (at ingest_minutes_ago, seeded separately) needs to be judged
    against."""
    return [("sync", "success", start_after_minutes + i * gap_minutes, None, 5) for i in range(n)]


def test_deep_health_widens_staleness_for_historically_quiet_service():
    # This service has always had ~40min gaps between real ingests — an
    # organic quiet period this long is normal for it. The naive 30-min
    # default would flag it degraded at 35min stale; the widened threshold
    # (2 * 40 = 80min) must not.
    history = _steady_cadence_rows(12, gap_minutes=40, start_after_minutes=35)
    code, body = _deep_health(history, ingest_minutes_ago=35)
    assert code == 200
    svc = body["services"][0]
    assert svc["status"] == "ok"
    assert svc["stale_minutes_used"] == 80


def test_deep_health_still_degrades_on_a_real_outage():
    # Same historical cadence as above, but now it's been quiet for 200
    # minutes — well past even the widened (80min) threshold. Must still
    # flag stale (HTTP 200, 'stale'); adaptive widening isn't a way to silence a genuine outage.
    history = _steady_cadence_rows(12, gap_minutes=40, start_after_minutes=200)
    code, body = _deep_health(history, ingest_minutes_ago=200)
    assert code == 200
    svc = body["services"][0]
    assert svc["status"] == "stale"
    assert svc["stale_minutes_used"] == 80
    assert "no ingest since" in svc["reason"]


# ── SRE-03 / 06 / 11 / 13 / 20: health-snapshot additions ────────────────────


def test_health_snapshot_has_new_observability_fields(client, monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER", raising=False)
    monkeypatch.delenv("STRUCTLOG_FORMAT", raising=False)
    body = client.get("/api/admin/health-snapshot").json()
    for key in ("scheduler_last_tick_age_s", "recent_cron_failures", "observability", "config_backup"):
        assert key in body, f"{key} missing from health-snapshot"
    # SRE-20: default test env (no STRUCTLOG_FORMAT / OTEL_EXPORTER) → console/none.
    assert body["observability"] == {"log_format": "console", "otel_exporter": "none"}


def test_health_snapshot_scheduler_tick_age_surfaced(client):
    with patch("backend.core.metric_snapshots.last_snapshot_age_s", return_value=12.5):
        body = client.get("/api/admin/health-snapshot").json()
    assert body["scheduler_last_tick_age_s"] == 12.5


def test_health_snapshot_recent_cron_failures_aggregates_errors(client):
    latest = {
        "sync": {"status": "success", "started_at": "2026-06-20T10:00:00Z", "error_message": None},
        "commit": {"status": "error", "started_at": "2026-06-20T10:05:00Z", "error_message": "boom"},
    }
    with (
        patch("backend.config.list_service_ids", return_value=["svc-a"]),
        patch("backend.core.metadata.cron_log.latest_cron_per_task", return_value=latest),
    ):
        body = client.get("/api/admin/health-snapshot").json()

    fails = body["recent_cron_failures"]
    assert len(fails) == 1
    assert fails[0]["service_id"] == "svc-a"
    assert fails[0]["task"] == "commit"
    assert fails[0]["status"] == "error"


def test_health_snapshot_config_backup_reads_marker(client, tmp_path, monkeypatch):
    marker = tmp_path / "last_config_backup.json"
    at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    marker.write_text(json.dumps({"at": at, "remote": "gs://example/configs.tar.gz"}))
    monkeypatch.setenv("CONFIG_BACKUP_MARKER", str(marker))

    body = client.get("/api/admin/health-snapshot").json()
    cb = body["config_backup"]
    assert cb is not None
    assert cb["last_backup_at"] == at
    assert cb["age_s"] is not None and cb["age_s"] >= 0
    assert cb["source"] == "gs://example/configs.tar.gz"


def test_health_snapshot_config_backup_absent_when_no_marker(client, tmp_path, monkeypatch):
    monkeypatch.setenv("CONFIG_BACKUP_MARKER", str(tmp_path / "does-not-exist.json"))
    body = client.get("/api/admin/health-snapshot").json()
    assert body["config_backup"] is None


def test_health_snapshot_fos_probe_opt_in(client):
    # Default: no FOS probe (off → real network round-trip avoided).
    body = client.get("/api/admin/health-snapshot").json()
    assert body.get("fos") is None

    # probe_fos=1 populates per-service reachability.
    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-a", "name": "svc-a"}]),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.duckdb.fos_reachable", return_value={"reachable": True, "error": None}),
    ):
        body = client.get("/api/admin/health-snapshot?probe_fos=1").json()
    assert body["fos"]["svc-a"]["reachable"] is True


# ── Merged from test_client_disconnect_cancels_queries.py ──────────────────────────────────────────────

import asyncio
import contextvars

import pytest
from starlette.requests import Request

from backend.core.query_registry import query_registry


class _Conn:
    def __init__(self) -> None:
        self.interrupts = 0

    def interrupt(self) -> None:
        self.interrupts += 1


def _request(path: str = "/api/dashboard/bundle") -> Request:
    scope = {
        "type": "http",
        "method": "POST",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "headers": [],
        "client": ("203.0.113.9", 4242),
        "server": ("testserver", 80),
        "scheme": "http",
        "state": {},
    }
    return Request(scope)


@pytest.fixture(autouse=True)
def _clean_registry():
    before = dict(query_registry._queries)
    before_keys = list(query_registry._cancelled_keys)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(before)
    query_registry._cancelled_keys.clear()
    query_registry._cancelled_keys.extend(before_keys)


def test_a_cancelled_request_interrupts_the_queries_it_started():
    from backend.main import telemetry_middleware

    conn = _Conn()

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow()", service_id="svc", con=conn, pool_slot="svc#1")
        raise asyncio.CancelledError  # what Starlette does to the handler when the client disconnects

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(telemetry_middleware(_request(), call_next))

    assert conn.interrupts == 1


def test_a_request_that_completes_normally_interrupts_nothing():
    from backend.main import telemetry_middleware

    conn = _Conn()

    class _Response:
        status_code = 200

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT 1", service_id="svc", con=conn, pool_slot="svc#1")
        return _Response()

    asyncio.run(telemetry_middleware(_request(), call_next))

    assert conn.interrupts == 0


def test_a_cancelled_request_never_interrupts_another_requests_queries():
    from backend.main import telemetry_middleware

    mine, theirs = _Conn(), _Conn()
    started = asyncio.Event
    other_registered: list[int] = []

    async def other_request_query():
        # A concurrent request registers its own query under its own attribution.
        async def inner(request):
            qid = query_registry.register("DuckDB", "SELECT other()", service_id="svc", con=theirs, pool_slot="svc#2")
            other_registered.append(qid)

            class _R:
                status_code = 200

            return _R()

        await telemetry_middleware(_request("/api/network-health"), inner)

    async def scenario():
        await other_request_query()

        async def call_next(request):
            query_registry.register("DuckDB", "SELECT mine()", service_id="svc", con=mine, pool_slot="svc#1")
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await telemetry_middleware(_request(), call_next)

    del started
    asyncio.run(scenario())

    assert mine.interrupts == 1
    assert theirs.interrupts == 0


def test_a_real_duckdb_query_is_killed_when_its_request_is_abandoned():
    """End to end: real DuckDB statement in a worker thread, real telemetry middleware, request task cancelled."""
    import threading
    import time

    import duckdb
    import httpx
    from fastapi import FastAPI

    from backend.core.query_instrumentation import InstrumentedDuckDBConnection
    from backend.main import telemetry_middleware

    app = FastAPI()
    app.middleware("http")(telemetry_middleware)
    outcome: dict[str, object] = {}
    started = threading.Event()
    finished = threading.Event()

    @app.get("/slow")
    async def slow():
        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="svc")
        # Safety net so a regression fails the assertion below instead of hanging the suite.
        safety = threading.Timer(15, raw.interrupt)
        safety.daemon = True
        safety.start()

        def run() -> None:
            # Runs through asyncio.to_thread, like production work, so it inherits the request's context.
            started.set()
            t0 = time.monotonic()
            try:
                con.execute("SELECT count(*) FROM range(100000000000) a, range(100000000000) b").fetchall()
                outcome["result"] = "finished"
            except Exception as exc:  # noqa: BLE001
                outcome["result"] = type(exc).__name__
            finally:
                outcome["seconds"] = time.monotonic() - t0
                safety.cancel()
                finished.set()

        asyncio.get_running_loop().run_in_executor(None, contextvars.copy_context().run, run)
        await asyncio.sleep(3600)

    async def scenario() -> None:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            request_task = asyncio.create_task(client.get("/slow"))
            deadline = time.monotonic() + 10
            while not started.is_set() and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            await asyncio.sleep(0.3)  # let the statement actually start inside DuckDB
            request_task.cancel()  # the browser reloaded: Starlette cancels the handler
            with pytest.raises(asyncio.CancelledError):
                await request_task
            await asyncio.get_running_loop().run_in_executor(None, finished.wait, 12)

    asyncio.run(scenario())

    assert finished.is_set(), "the orphaned query must be interrupted, not left to run to completion"
    assert outcome["result"] == "InterruptException"
    assert outcome["seconds"] < 8  # type: ignore[operator]


def test_tcp_disconnect_via_asgi_receive_interrupts_sync_duckdb_query():
    """Uvicorn signals client TCP disconnect via the ASGI receive channel (request.is_disconnected())
    rather than cancelling the handler coroutine directly when a sync route is running in a threadpool.
    Verify _watch_disconnect() polls request.is_disconnected() and interrupts the query."""
    from backend.main import telemetry_middleware

    conn = _Conn()
    disconnected = False

    async def receive():
        nonlocal disconnected
        if disconnected:
            return {"type": "http.disconnect"}
        await asyncio.sleep(0.05)
        return {"type": "http.disconnect"}

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/api/network-health",
        "raw_path": b"/api/network-health",
        "query_string": b"",
        "headers": [],
        "client": ("203.0.113.9", 4242),
        "server": ("testserver", 80),
        "scheme": "http",
        "state": {},
    }
    req = Request(scope, receive=receive)
    req._stream_consumed = True  # type: ignore[attr-defined]

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow_post()", service_id="svc", con=conn, pool_slot="svc#1")
        nonlocal disconnected
        disconnected = True
        for _ in range(20):
            if conn.interrupts > 0:
                break
            await asyncio.sleep(0.05)

        class _R:
            status_code = 499

        return _R()

    asyncio.run(telemetry_middleware(req, call_next))
    assert conn.interrupts == 1


def test_cancelled_key_blocks_subsequent_queries_but_allows_drop_table_cleanup():
    """Once cancel_for_key(cancel_key) has fired, any subsequent query registered under the same
    cancel_key must raise duckdb.InterruptException immediately, EXCEPT DROP TABLE cleanup statements."""
    import duckdb

    from backend.core.query_instrumentation import InstrumentedDuckDBConnection
    from backend.core.query_registry import Attribution, current_attribution

    raw = duckdb.connect(":memory:")
    con = InstrumentedDuckDBConnection(raw, service_id="svc")
    con.execute("CREATE TEMP TABLE tmp_cleanup AS SELECT 1 AS x")

    token = current_attribution.set(
        Attribution(
            kind="admin", admin_id="local", request_path="/api/security/aggregates", cancel_key="ck-multi-query"
        )
    )
    try:
        query_registry.cancel_for_key("ck-multi-query")
        with pytest.raises(duckdb.InterruptException):
            con.execute("SELECT * FROM tmp_cleanup").fetchall()

        # Cleanup DROP statements in finally: blocks must still be allowed to run
        con.execute('DROP TABLE IF EXISTS "tmp_cleanup"')
    finally:
        current_attribution.reset(token)
        raw.close()


def test_query_runner_lazy_con_factory_defers_connection_checkout():
    """QueryRunner(con_factory, src) must not call con_factory() until a SQL statement actually executes."""
    import duckdb

    from backend.repositories._base import QueryRunner

    calls = 0
    raw = duckdb.connect(":memory:")

    def factory():
        nonlocal calls
        calls += 1
        return raw

    try:
        runner = QueryRunner(factory, {"name": "test_svc"})
        assert calls == 0, "con_factory must not be called on QueryRunner construction"
        # Rollup eligibility check that returns None early without touching DuckDB
        res = runner.try_network_rtt_from_rollup(
            "2026-01-01T00:00:00Z",
            "2026-01-01T01:00:00Z",
            top_asns=[15169],
            has_filters=True,
        )
        assert res is None
        assert calls == 0, "con_factory must not be called when rollup eligibility returns early"

        # Executing SQL triggers lazy connection acquisition exactly once
        assert runner.execute("SELECT 42").fetchone()[0] == 42
        assert calls == 1
        assert runner.execute("SELECT 43").fetchone()[0] == 43
        assert calls == 1
    finally:
        raw.close()
