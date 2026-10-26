"""Consolidated tests for test_main."""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import sqlite3
import threading
import time
from datetime import UTC, datetime, timedelta
from unittest.mock import ANY, MagicMock, patch

import duckdb
import httpx
import pytest
from backend.core.query_attribution import current_attribution
from backend.core.query_instrumentation import InstrumentedDuckDBConnection
from backend.core.query_registry import _HISTORY_CAP, Attribution, QueryRegistry, query_registry
from backend.cron.decorators import global_job
from fastapi import FastAPI, HTTPException
from starlette.requests import Request

from backend import config as svcconfig
from backend import deps
from backend.core import duckdb_pool
from backend.core import rollups as rr
from backend.core.duckdb import DBBusyError
from backend.core.duckdb_pool import _Pool, _PoolBusy
from backend.main import (
    _background_startup,
    _bounded_scheduler_shutdown,
    _ensure_persistent_view,
    _ensure_pop_cache,
    _ensure_scoring_matrix,
    _initialize_service,
    _maybe_instrument_asgi,
    health_check,
    telemetry_middleware,
)
from backend.repositories._base import QueryRunner
from backend.utils.remote_access import _new_request_id

# ── Shared Helpers ───────────────────────────────────────────────────────────


def _make_health_db(
    service_id: str = "svc",
    ingest_minutes_ago: int | None = 1,
    cron_rows: list[tuple] | None = None,
) -> sqlite3.Connection:
    now = datetime.now(UTC)
    con = sqlite3.connect(":memory:", check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("CREATE TABLE ingested_files (source_name TEXT, ingested_at TEXT)")
    con.execute(
        "CREATE TABLE cron_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, task TEXT, "
        "status TEXT, started_at TEXT, error_message TEXT, "
        "files_downloaded INTEGER DEFAULT 0, rows_ingested INTEGER DEFAULT 0)"
    )
    if ingest_minutes_ago is not None:
        ts = (now - timedelta(minutes=ingest_minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")
        con.execute("INSERT INTO ingested_files VALUES (?, ?)", (service_id, ts))
    for row in cron_rows or []:
        task, status, mins_ago, err = row[:4]
        ingest_count = row[4] if len(row) > 4 else 0
        started = (now - timedelta(minutes=mins_ago)).strftime("%Y-%m-%d %H:%M:%S")
        con.execute(
            "INSERT INTO cron_runs (task, status, started_at, error_message, rows_ingested) VALUES (?, ?, ?, ?, ?)",
            (task, status, started, err, ingest_count),
        )
    con.commit()
    return con


def _deep_health(
    cron_rows: list[tuple],
    *,
    ingest_minutes_ago: int = 1,
    service_id: str = "svc",
    is_remote: bool = False,
    durable: bool | None = None,
):
    """Drive health_check(deep=True) against an in-memory metadata.db."""
    con = _make_health_db(service_id=service_id, ingest_minutes_ago=ingest_minutes_ago, cron_rows=cron_rows)
    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=[service_id]),
        patch("backend.core.metadata.get_con", return_value=con),
        patch("backend.utils.remote_access.is_request_remote", return_value=is_remote),
    ):
        if durable is not None:
            with patch("backend.config.is_durable_serving_mode", return_value=durable):
                result = health_check(fake_request, deep=True)
        else:
            result = health_check(fake_request, deep=True)

    return (200, result) if isinstance(result, dict) else (result.status_code, json.loads(result.body))


def _call_get_service_id(service=None, sid=None, x_fastly_service_id=None, x_service_id=None) -> str | None:
    """Call get_service_id directly without going through FastAPI's DI."""
    request = MagicMock()
    request.path_params = {}
    return deps.get_service_id(
        request=request,
        service=service,
        sid=sid,
        x_fastly_service_id=x_fastly_service_id,
        x_service_id=x_service_id,
    )


def _run_init_service(
    cfg: dict,
    *,
    reap_ret: object = 0,
    src_ret: object = None,
):
    reap_kw = {"side_effect": reap_ret} if isinstance(reap_ret, Exception) else {"return_value": reap_ret}
    src_kw = {"side_effect": src_ret} if isinstance(src_ret, Exception) else {"return_value": src_ret}
    with (
        patch("backend.core.metadata.reap_running_cron_runs", **reap_kw) as mock_reap,
        patch("backend.core.duckdb.get_source_for_service", **src_kw) as mock_get_src,
        patch("backend.core.duckdb.refresh_config_status") as mock_refresh,
        patch("backend.main._ensure_persistent_view") as mock_ensure,
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.core.metadata.usage_log_db.get_con"),
        patch("backend.main.logger.info") as mock_log_info,
    ):
        _initialize_service(cfg)
    return mock_reap, mock_get_src, mock_refresh, mock_ensure, mock_log_info


# ── _initialize_service ──────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_skips_when_no_service_id():
    mock_reap, mock_get_src, _, _, _ = _run_init_service({})
    mock_reap.assert_not_called()
    mock_get_src.assert_not_called()


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_reaps_orphans_and_refreshes_status():
    fake_src = {"name": "svc-1", "service_id": "svc-1"}
    mock_reap, _, mock_refresh, mock_ensure, _ = _run_init_service(
        {"service_id": "svc-1"}, reap_ret=3, src_ret=fake_src
    )
    mock_reap.assert_called_once_with("svc-1")
    mock_refresh.assert_called_once_with("svc-1")
    mock_ensure.assert_called_once_with("svc-1", fake_src)


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_logs_reaped_count_only_when_nonzero():
    _, _, _, _, mock_log_info = _run_init_service({"service_id": "svc-x"}, reap_ret=0, src_ret=None)
    assert [c for c in mock_log_info.call_args_list if "reaped" in str(c)] == []


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_tolerates_reap_failure():
    _, _, mock_refresh, _, _ = _run_init_service(
        {"service_id": "svc-1"}, reap_ret=RuntimeError("locked"), src_ret={"name": "svc-1"}
    )
    mock_refresh.assert_called_once_with("svc-1")


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_skips_status_refresh_when_no_source():
    _, _, mock_refresh, _, _ = _run_init_service({"service_id": "svc-1"}, reap_ret=0, src_ret=None)
    mock_refresh.assert_not_called()


@pytest.mark.usefixtures("_clean_registry")
def test_initialize_service_swallows_unexpected_outer_exception():
    _run_init_service({"service_id": "svc-1"}, src_ret=RuntimeError("unexpected"))


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("service_id", "mode", "durable"),
    [
        ("svc-durable", "high_scale", True),
        ("svc-file", "standard", False),
    ],
)
def test_initialize_service_defers_or_skips_rollup_catchup(service_id: str, mode: str, durable: bool):
    rr.reset_rollup_coverage_ready()
    fake_src = {"service_id": service_id, "deployment_mode": mode}
    with (
        patch("backend.core.duckdb.get_source_for_service", return_value=fake_src),
        patch("backend.core.duckdb.refresh_config_status"),
        patch("backend.main._ensure_persistent_view"),
        patch("backend.routers.admin.compute_sync_status_cached"),
        patch("backend.config.is_durable_serving_mode", return_value=durable),
        patch(
            "backend.core.rollups.recompute.backfill_missing_hour_bundles",
            side_effect=AssertionError("startup rollup catch-up must be deferred"),
        ) as mock_backfill,
    ):
        _initialize_service({"service_id": service_id})

    mock_backfill.assert_not_called()
    assert rr.rollup_coverage_ready(service_id) is False


# ── _ensure_persistent_view ──────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_persistent_view_always_rebuilds_at_startup():
    fake_src = {"name": "svc-1"}
    writer_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_connection", return_value=writer_con) as mock_get_conn,
        patch("backend.core.iceberg.update_iceberg_view") as mock_update,
    ):
        _ensure_persistent_view("svc-1", fake_src)

    assert mock_get_conn.call_count == 1
    assert mock_get_conn.call_args.kwargs.get("read_only") is False
    mock_update.assert_called_once_with(writer_con, fake_src)
    writer_con.close.assert_called_once()


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_persistent_view_closes_writer_on_update_failure():
    writer_con = MagicMock()
    with (
        patch("backend.core.duckdb.get_connection", return_value=writer_con),
        patch("backend.core.iceberg.update_iceberg_view", side_effect=RuntimeError("catalog corrupt")),
    ):
        _ensure_persistent_view("svc-1", {"name": "svc-1"})
    writer_con.close.assert_called_once()


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_persistent_view_swallows_outer_exceptions():
    with patch("backend.core.duckdb.get_connection", side_effect=RuntimeError("db locked")):
        _ensure_persistent_view("svc-1", {"name": "svc-1"})


# ── _ensure_pop_cache ────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_pop_cache_skips_when_cache_already_exists(tmp_path):
    cache_file = tmp_path / "pop_cache.json"
    cache_file.write_text("[]")
    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(cache_file)),
        patch("backend.utils.pop_utils.fetch_pop_locations") as mock_fetch,
    ):
        _ensure_pop_cache()
    mock_fetch.assert_not_called()


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("configs", "fetch_ret", "expected_key", "expect_warn"),
    [
        (
            [
                {"service_id": "a", "fastly_api_key": ""},
                {"service_id": "b", "fastly_api_key": "key-b"},
                {"service_id": "c", "fastly_api_key": "key-c"},
            ],
            True,
            "key-b",
            False,
        ),
        ([{"service_id": "a", "fastly_api_key": "key"}], False, "key", True),
        ([{"service_id": "a", "fastly_api_key": ""}], True, None, False),
    ],
)
def test_ensure_pop_cache_missing_cache_scenarios(
    tmp_path, configs: list[dict], fetch_ret: bool, expected_key: str | None, expect_warn: bool
):
    with (
        patch("backend.utils.pop_utils.CACHE_FILE", str(tmp_path / "missing.json")),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.utils.pop_utils.fetch_pop_locations", return_value=fetch_ret) as mock_fetch,
        patch("backend.main.logger.warning") as mock_warn,
    ):
        _ensure_pop_cache()

    if expected_key is None:
        mock_fetch.assert_not_called()
    else:
        mock_fetch.assert_called_once_with(expected_key)
    if expect_warn:
        assert any("failed" in str(c).lower() for c in mock_warn.call_args_list)


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_pop_cache_swallows_unexpected_exceptions():
    with patch("backend.utils.pop_utils.CACHE_FILE", side_effect=RuntimeError("oops")):
        _ensure_pop_cache()


# ── _ensure_scoring_matrix ──────────────────────────────────────────────────


def _run_scoring_matrix(tmp_path, configs: list[dict], fetch_fn=None):
    fake_matrix_path = tmp_path / "matrix.json"
    fetch_kw = {"side_effect": fetch_fn} if fetch_fn is not None else {}
    with (
        patch("backend.provision.session_scoring_orchestrator._MATRIX_PATH", fake_matrix_path),
        patch("backend.config.list_configs", return_value=configs),
        patch("backend.state_sync.fetch_matrix_from_fos", **fetch_kw) as mock_fetch,
    ):
        _ensure_scoring_matrix()
    return fake_matrix_path, mock_fetch


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_scoring_matrix_writes_tenant_scoped_paths_for_each_service(tmp_path):
    configs = [
        {"service_id": "svc-a", "scoring": {"enabled": True}},
        {"service_id": "svc-b", "scoring": {"enabled": True}},
        {"service_id": "svc-c", "scoring": {"enabled": False}},
    ]
    matrices = {"svc-a": {"version": "a-v1", "vocab_size": 10}, "svc-b": {"version": "b-v1", "vocab_size": 20}}
    fake_matrix_path, _ = _run_scoring_matrix(tmp_path, configs, fetch_fn=lambda sid: matrices.get(sid))

    assert (tmp_path / "matrix_svc-a.json").exists()
    assert (tmp_path / "matrix_svc-b.json").exists()
    assert not fake_matrix_path.exists(), "shared matrix.json must not be written"
    assert json.loads((tmp_path / "matrix_svc-a.json").read_text())["version"] == "a-v1"
    assert json.loads((tmp_path / "matrix_svc-b.json").read_text())["version"] == "b-v1"


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_scoring_matrix_tolerates_per_service_failure(tmp_path):
    def fetch(sid):
        if sid == "svc-bad":
            raise RuntimeError("FOS unreachable")
        return {"version": "v1", "vocab_size": 5}

    configs = [
        {"service_id": "svc-bad", "scoring": {"enabled": True}},
        {"service_id": "svc-good", "scoring": {"enabled": True}},
    ]
    _run_scoring_matrix(tmp_path, configs, fetch_fn=fetch)
    assert (tmp_path / "matrix_svc-good.json").exists()
    assert not (tmp_path / "matrix_svc-bad.json").exists()


@pytest.mark.usefixtures("_clean_registry")
def test_ensure_scoring_matrix_skips_services_without_scoring_enabled(tmp_path):
    configs = [{"service_id": "svc-off", "scoring": {"enabled": False}}, {"service_id": "svc-none"}]
    _, mock_fetch = _run_scoring_matrix(tmp_path, configs)
    mock_fetch.assert_not_called()


# ── _background_startup ────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_background_startup_reloads_db_and_initialises_each_service():
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
    assert {call[0][0]["service_id"] for call in mock_init.call_args_list} == {"a", "b"}


@pytest.mark.usefixtures("_clean_registry")
def test_background_startup_tolerates_reload_and_scheduler_failures():
    fake_scheduler = MagicMock()
    with (
        patch("backend.core.duckdb.reload_default_source", side_effect=RuntimeError("S3 down")),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", return_value=fake_scheduler),
        patch("backend.config.list_configs", return_value=[]),
    ):
        _background_startup()
    fake_scheduler.start.assert_called_once()

    with (
        patch("backend.core.duckdb.reload_default_source"),
        patch("backend.main._ensure_pop_cache"),
        patch("backend.cron.scheduler.get_scheduler", side_effect=RuntimeError("scheduler oom")),
    ):
        _background_startup()


# ── /api/health ─────────────────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_health_endpoint_returns_ok_status_without_service_id(client):
    resp = client.get("/api/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert "version" in body


@pytest.mark.usefixtures("_clean_registry")
def test_health_endpoint_deep_returns_shallow_for_remote_analyst():
    fake_request = MagicMock()
    with (
        patch("backend.config.list_service_ids", return_value=["secret-svc-1", "secret-svc-2"]),
        patch("backend.utils.remote_access.is_request_remote", return_value=True),
    ):
        result = health_check(fake_request, deep=True)

    assert isinstance(result, dict)
    assert result["status"] == "ok" and "version" in result
    assert "services" not in result, f"deep=1 from remote leaked services: {result.get('services')!r}"
    payload_str = str(result)
    assert "secret-svc-1" not in payload_str and "secret-svc-2" not in payload_str


@pytest.mark.usefixtures("_clean_registry")
def test_health_endpoint_deep_returns_full_payload_for_admin():
    code, result = _deep_health([], ingest_minutes_ago=0, service_id="admin-svc-1", is_remote=False)
    assert code == 200 and result["status"] == "ok"
    assert "services" in result, "admin caller must get the per-service report"
    assert any(s["service_id"] == "admin-svc-1" for s in result["services"])


@pytest.mark.usefixtures("_clean_registry")
def test_health_endpoint_deep(client):
    con = _make_health_db(service_id="test-svc-1", ingest_minutes_ago=None)
    with (
        patch("backend.config.list_service_ids", return_value=["test-svc-1"]),
        patch("backend.core.metadata.get_con", return_value=con),
    ):
        resp = client.get("/api/health?deep=1")
        assert resp.status_code == 200 and resp.json()["status"] == "ok"

        now_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
        con.execute("INSERT INTO ingested_files VALUES (?, ?)", ("test-svc-1", now_str))
        resp = client.get("/api/health?deep=1")
        assert resp.status_code == 200 and resp.json()["status"] == "ok"

        stale_str = (datetime.now(UTC) - timedelta(minutes=45)).strftime("%Y-%m-%d %H:%M:%S")
        con.execute("DELETE FROM ingested_files")
        con.execute("INSERT INTO ingested_files VALUES (?, ?)", ("test-svc-1", stale_str))
        resp = client.get("/api/health?deep=1")
        assert resp.status_code == 200 and resp.json()["status"] == "ok"
        assert resp.json()["services"][0]["status"] == "stale"


# ── telemetry middleware: cdn_service_id resolution ────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_middleware_resolves_cdn_service_id_and_swallows_flush_exceptions(client):
    with (
        patch.object(svcconfig, "load_config", return_value=None),
        patch.object(svcconfig, "get_cdn_service_id_map", return_value={"cdn-svc-1": "log-svc-1"}),
        patch.object(svcconfig, "get_active_service_id", return_value=None),
        patch("backend.utils.telemetry.flush_usage_log") as mock_flush,
    ):
        client.get("/api/health", headers={"x-fastly-service-id": "cdn-svc-1"})
    if mock_flush.called:
        assert mock_flush.call_args[0][0] == "log-svc-1"

    with patch("backend.utils.telemetry.flush_usage_log", side_effect=RuntimeError("disk full")):
        assert client.get("/api/health").status_code == 200


# ── _bounded_scheduler_shutdown & _maybe_instrument_asgi ─────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_bounded_shutdown_fast_timeout_and_exception_paths():
    fake_scheduler = MagicMock()
    fake_scheduler.shutdown = MagicMock(return_value=None)
    t0 = time.monotonic()
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=10.0)
    assert time.monotonic() - t0 < 0.5
    fake_scheduler.shutdown.assert_called_once_with(wait=True)

    never_returns = threading.Event()
    fake_scheduler.shutdown = lambda wait: never_returns.wait()  # noqa: ARG005
    t0 = time.monotonic()
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=0.3)
    elapsed = time.monotonic() - t0
    assert 0.25 <= elapsed < 0.6, f"bounded wait released at {elapsed:.2f}s, expected ~0.3s"
    never_returns.set()

    fake_scheduler.shutdown = MagicMock(side_effect=RuntimeError("apscheduler internal"))
    _bounded_scheduler_shutdown(fake_scheduler, timeout_secs=5.0)


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize("enabled", [False, True])
def test_maybe_instrument_asgi(enabled: bool):
    fake_app = MagicMock()
    with (
        patch("backend.main.request_telemetry.asgi_instrumentation_enabled", return_value=enabled),
        patch("backend.main.request_telemetry.ensure_initialized") as mock_ensure_init,
        patch("backend.main.FastAPIInstrumentor") as mock_instrumentor,
    ):
        _maybe_instrument_asgi(fake_app)

    if enabled:
        mock_ensure_init.assert_called_once()
        mock_instrumentor.instrument_app.assert_called_once_with(fake_app, exclude_spans=["receive", "send"])
    else:
        mock_instrumentor.instrument_app.assert_not_called()


# ── Merged from test_deps.py ─────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"sid": "abc"}, "abc"),
        (
            {
                "service": "from-query",
                "sid": "from-sid",
                "x_fastly_service_id": "from-fastly-header",
                "x_service_id": "from-svc-header",
            },
            "from-query",
        ),
        ({"x_fastly_service_id": "hdr"}, "hdr"),
        ({"x_service_id": "old"}, "old"),
    ],
)
def test_get_service_id_resolution_priority(kwargs: dict, expected: str):
    with patch("backend.deps.svcconfig.load_config", return_value={"service_id": expected}):
        assert _call_get_service_id(**kwargs) == expected


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("sid", "cdn_map", "expected"),
    [
        ("cdn-svc-1", {"cdn-svc-1": "log-svc-1"}, "log-svc-1"),
        ("ghost", {}, "ghost"),
    ],
)
def test_get_service_id_cdn_fallback(sid: str, cdn_map: dict, expected: str):
    with (
        patch("backend.deps.svcconfig.load_config", return_value=None),
        patch("backend.deps.svcconfig.get_cdn_service_id_map", return_value=cdn_map),
    ):
        assert _call_get_service_id(sid=sid) == expected


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize("active_id", ["active-svc", None])
def test_get_service_id_falls_back_to_active_when_nothing_provided(active_id: str | None):
    with patch("backend.deps.svcconfig.get_active_service_id", return_value=active_id):
        assert _call_get_service_id() == active_id


# ── get_source: source lookup + 400 on miss ──────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_get_source_returns_source_dict_when_found():
    fake_src = {"name": "logs_svc", "service_id": "svc"}
    with patch("backend.deps.db.get_source_for_service", return_value=fake_src):
        assert deps.get_source(service_id="svc") is fake_src


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize("service_id", [None, "ghost"])
def test_get_source_raises_400_when_missing(service_id: str | None):
    with patch("backend.deps.db.get_source_for_service", return_value=None):
        with pytest.raises(HTTPException) as exc:
            deps.get_source(service_id=service_id)
    assert exc.value.status_code == 400
    assert exc.value.detail["no_service"] is True


# ── _ConnectionHolder: enter / DBBusyError / exit ────────────────────────────


@pytest.fixture
def disable_pool(monkeypatch):
    monkeypatch.setenv("DUCKDB_CONNECTION_POOL", "0")
    yield


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_opens_and_closes(disable_pool):
    fake_con = MagicMock()
    src = {"name": "x"}
    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        holder = deps._ConnectionHolder(src, read_only=True)
        assert holder.__enter__() is fake_con
        mock_get.assert_called_once_with(source=src, max_wait=ANY, skip_view_update=False, read_only=True)
        holder.__exit__(None, None, None)
        fake_con.close.assert_called_once()


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_skip_view_update_forwarded():
    with patch("backend.deps.get_connection") as mock_get:
        deps._ConnectionHolder({"name": "x"}, skip_view_update=True).__enter__()
        assert mock_get.call_args.kwargs["skip_view_update"] is True


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(("env_val", "expected_wait"), [(None, 10.0), ("9.5", 9.5)])
def test_connection_holder_max_wait(disable_pool, monkeypatch, env_val: str | None, expected_wait: float):
    if env_val is None:
        monkeypatch.delenv("DUCKDB_POOL_MAX_WAIT_S", raising=False)
    else:
        monkeypatch.setenv("DUCKDB_POOL_MAX_WAIT_S", env_val)
    with patch("backend.deps.get_connection", return_value=MagicMock()) as mock_get:
        deps._ConnectionHolder({"name": "x"}, read_only=True).__enter__()
        assert mock_get.call_args.kwargs["max_wait"] == expected_wait


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_db_busy_and_exit_edge_cases(disable_pool):
    with patch("backend.deps.get_connection", side_effect=DBBusyError("locked")):
        with pytest.raises(HTTPException) as exc:
            deps._ConnectionHolder({"name": "x"}).__enter__()
    assert exc.value.status_code == 503 and exc.value.detail["busy"] is True

    fake_con = MagicMock()
    fake_con.close.side_effect = RuntimeError("already closed")
    with patch("backend.deps.get_connection", return_value=fake_con):
        holder = deps._ConnectionHolder({"name": "x"})
        holder.__enter__()
        holder.__exit__(None, None, None)

    deps._ConnectionHolder({"name": "x"}).__exit__(None, None, None)


# ── get_con & removed symbols ────────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_get_con_yields_connection_and_closes_after(disable_pool):
    fake_con = MagicMock()
    with patch("backend.deps.get_connection", return_value=fake_con) as mock_get:
        gen = deps.get_con(source={"name": "x"})
        assert next(gen) is fake_con
        assert mock_get.call_args.kwargs["read_only"] is True
        with pytest.raises(StopIteration):
            next(gen)
        fake_con.close.assert_called_once()


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize("symbol", ["get_meta_con", "AnalyticsDeps"])
def test_removed_deps_symbols(symbol: str):
    assert not hasattr(deps, symbol), f"{symbol} was removed at the v2.0 cut"


# ── _ConnectionHolder: pool path ─────────────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_pool_path_checkout_return_and_error():
    fake_con = MagicMock()
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=fake_con)
    cm.__exit__ = MagicMock(return_value=False)

    with patch.object(duckdb_pool, "checkout_connection", return_value=cm) as mock_checkout:
        holder = deps._ConnectionHolder({"name": "x"}, read_only=True)
        assert holder.__enter__() is fake_con
        mock_checkout.assert_called_once()
        holder.__exit__(None, None, None)
        cm.__exit__.assert_called_once()
        fake_con.close.assert_not_called()

        cm.reset_mock()
        holder2 = deps._ConnectionHolder({"name": "x"}, read_only=True)
        holder2.__enter__()
        holder2.__exit__(RuntimeError, RuntimeError("boom"), None)
        assert cm.__exit__.call_args[0][0] is RuntimeError


@pytest.mark.usefixtures("_clean_registry")
def test_connection_holder_pool_path_skipped_when_skip_view_update():
    with (
        patch.object(duckdb_pool, "checkout_connection") as mock_checkout,
        patch("backend.deps.get_connection", return_value=MagicMock()) as mock_get,
    ):
        deps._ConnectionHolder({"name": "x"}, skip_view_update=True).__enter__()
        mock_checkout.assert_not_called()
        mock_get.assert_called_once()


# ── Merged from test_sre_observability.py ────────────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_admin_access_log_carries_rid_and_duration(client):
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
    assert not any("rid=- " in m for m in captured), captured


@pytest.mark.usefixtures("_clean_registry")
def test_new_request_id_is_nonempty_hex():
    rid = _new_request_id()
    assert isinstance(rid, str) and len(rid) == 16
    int(rid, 16)
    assert _new_request_id() != rid


@pytest.mark.usefixtures("_clean_registry")
def test_global_job_runs_in_cron_attribution_scope(monkeypatch):
    import backend.cron.decorators as sj

    monkeypatch.setattr(sj, "record_job_run", lambda *a, **k: None)
    seen: dict = {}

    @global_job("share_audit_purge", color="35", tag="purge", label="Purge")
    def _job():
        attr = current_attribution.get()
        seen["kind"] = attr.kind if attr else None
        seen["job"] = attr.cron_job if attr else None
        return "ok"

    _job()
    assert seen == {"kind": "cron", "job": "share_audit_purge"}
    assert current_attribution.get() is None


@pytest.mark.usefixtures("_clean_registry")
def test_pool_stats_and_reject_counters():
    p = _Pool("svc-sre12", max_size=2)
    s = p.stats()
    assert s["saturated_rejects_total"] == 0 and s["drain_rejects_total"] == 0
    assert "last_warmed_at" in s and s["last_warmed_at"] is None

    p_sat = _Pool("svc-sat", max_size=1)
    p_sat._in_use = 1
    with pytest.raises(_PoolBusy):
        p_sat.acquire({"name": "svc-sat"}, max_wait=0)
    assert p_sat.stats()["saturated_rejects_total"] == 1 and p_sat.stats()["drain_rejects_total"] == 0

    p_drain = _Pool("svc-drain", max_size=2)
    p_drain.begin_drain()
    with pytest.raises(_PoolBusy):
        p_drain.acquire({"name": "svc-drain"}, max_wait=0)
    assert p_drain.stats()["drain_rejects_total"] == 1 and p_drain.stats()["saturated_rejects_total"] == 0


@pytest.mark.usefixtures("_clean_registry")
def test_history_cap_is_400():
    assert _HISTORY_CAP == 400
    assert QueryRegistry()._history.maxlen == 400


# ── SRE-04 / SRE-05 / SRE-22: deep /api/health ──────────────────────────────


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("cron_rows", "expected_code", "expected_status", "reason_substr"),
    [
        ([("sync", "success", 2, None), ("commit", "success", 3, None)], 200, "ok", None),
        ([("sync", "success", 120, None), ("sync", "running", 20, None)], 503, "degraded", "stuck running"),
        ([("sync", "success", 30, None), ("sync", "running", 2, None)], 200, "ok", None),
        ([("sync", "success", 2, None), ("commit", "error", 1, "boom")], 503, "degraded", "commit cron errored"),
        ([("metadata_sync", "error", 1, "fos down")], 503, "degraded", "metadata_sync cron errored"),
    ],
)
def test_deep_health_cron_modes(
    cron_rows: list[tuple],
    expected_code: int,
    expected_status: str,
    reason_substr: str | None,
):
    code, body = _deep_health(cron_rows)
    assert code == expected_code
    if expected_code == 200:
        assert body["status"] == expected_status
    else:
        svc = body["services"][0]
        assert svc["status"] == expected_status
        if reason_substr:
            assert reason_substr in svc["reason"]


@pytest.mark.usefixtures("_clean_registry")
def test_deep_health_ignores_metadata_sync_error_in_durable_mode():
    code, result = _deep_health(
        [("metadata_sync", "error", 0, "old topology"), ("sync", "success", 0, None)],
        durable=True,
    )
    assert code == 200 and result["status"] == "ok"


@pytest.mark.usefixtures("_clean_registry")
@pytest.mark.parametrize(
    ("quiet_mins", "expected_status", "reason_substr"),
    [
        (35, "ok", None),
        (200, "stale", "no ingest since"),
    ],
)
def test_deep_health_adaptive_staleness(quiet_mins: int, expected_status: str, reason_substr: str | None):
    history = [("sync", "success", quiet_mins + i * 40, None, 5) for i in range(12)]
    code, body = _deep_health(history, ingest_minutes_ago=quiet_mins)
    assert code == 200
    svc = body["services"][0]
    assert svc["status"] == expected_status and svc["stale_minutes_used"] == 80
    if reason_substr:
        assert reason_substr in svc["reason"]


# ── SRE-03 / 06 / 11 / 13 / 20: health-snapshot additions ────────────────────


@pytest.mark.usefixtures("_clean_registry")
def test_health_snapshot_has_new_observability_fields(client, monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER", raising=False)
    monkeypatch.delenv("STRUCTLOG_FORMAT", raising=False)
    body = client.get("/api/admin/health-snapshot").json()
    for key in ("scheduler_last_tick_age_s", "recent_cron_failures", "observability", "config_backup"):
        assert key in body, f"{key} missing from health-snapshot"
    assert body["observability"] == {"log_format": "console", "otel_exporter": "none"}


@pytest.mark.usefixtures("_clean_registry")
def test_health_snapshot_scheduler_tick_age_surfaced(client):
    with patch("backend.core.metric_snapshots.last_snapshot_age_s", return_value=12.5):
        body = client.get("/api/admin/health-snapshot").json()
    assert body["scheduler_last_tick_age_s"] == 12.5


@pytest.mark.usefixtures("_clean_registry")
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
    assert fails[0]["service_id"] == "svc-a" and fails[0]["task"] == "commit" and fails[0]["status"] == "error"


@pytest.mark.usefixtures("_clean_registry")
def test_health_snapshot_config_backup_marker_present_and_absent(client, tmp_path, monkeypatch):
    marker = tmp_path / "last_config_backup.json"
    at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    marker.write_text(json.dumps({"at": at, "remote": "gs://example/configs.tar.gz"}))
    monkeypatch.setenv("CONFIG_BACKUP_MARKER", str(marker))

    cb = client.get("/api/admin/health-snapshot").json()["config_backup"]
    assert cb is not None and cb["last_backup_at"] == at
    assert cb["age_s"] is not None and cb["age_s"] >= 0
    assert cb["source"] == "gs://example/configs.tar.gz"

    monkeypatch.setenv("CONFIG_BACKUP_MARKER", str(tmp_path / "does-not-exist.json"))
    assert client.get("/api/admin/health-snapshot").json()["config_backup"] is None


@pytest.mark.usefixtures("_clean_registry")
def test_health_snapshot_fos_probe_opt_in(client):
    assert client.get("/api/admin/health-snapshot").json().get("fos") is None
    with (
        patch("backend.config.list_configs", return_value=[{"service_id": "svc-a", "name": "svc-a"}]),
        patch("backend.config.config_to_source", side_effect=lambda c: c),
        patch("backend.core.duckdb.fos_reachable", return_value={"reachable": True, "error": None}),
    ):
        body = client.get("/api/admin/health-snapshot?probe_fos=1").json()
    assert body["fos"]["svc-a"]["reachable"] is True


# ── Merged from test_client_disconnect_cancels_queries.py ────────────────────


class _Conn:
    def __init__(self) -> None:
        self.interrupts = 0

    def interrupt(self) -> None:
        self.interrupts += 1


class _Resp:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code


def _request(path: str = "/api/dashboard/bundle", receive=None) -> Request:
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
    return Request(scope, receive=receive) if receive is not None else Request(scope)


@pytest.fixture()
def _clean_registry():
    before = dict(query_registry._queries)
    before_keys = list(query_registry._cancelled_keys)
    yield
    query_registry._queries.clear()
    query_registry._queries.update(before)
    query_registry._cancelled_keys.clear()
    query_registry._cancelled_keys.extend(before_keys)


@pytest.mark.usefixtures("_clean_registry")
def test_a_cancelled_request_interrupts_the_queries_it_started():
    conn = _Conn()

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow()", service_id="svc", con=conn, pool_slot="svc#1")
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(telemetry_middleware(_request(), call_next))
    assert conn.interrupts == 1


@pytest.mark.usefixtures("_clean_registry")
def test_a_request_that_completes_normally_interrupts_nothing():
    conn = _Conn()

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT 1", service_id="svc", con=conn, pool_slot="svc#1")
        return _Resp(200)

    asyncio.run(telemetry_middleware(_request(), call_next))
    assert conn.interrupts == 0


@pytest.mark.usefixtures("_clean_registry")
def test_a_cancelled_request_never_interrupts_another_requests_queries():
    mine, theirs = _Conn(), _Conn()
    other_registered: list[int] = []

    async def other_request_query():
        async def inner(request):
            qid = query_registry.register("DuckDB", "SELECT other()", service_id="svc", con=theirs, pool_slot="svc#2")
            other_registered.append(qid)
            return _Resp(200)

        await telemetry_middleware(_request("/api/network-health"), inner)

    async def scenario():
        await other_request_query()

        async def call_next(request):
            query_registry.register("DuckDB", "SELECT mine()", service_id="svc", con=mine, pool_slot="svc#1")
            raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await telemetry_middleware(_request(), call_next)

    asyncio.run(scenario())
    assert mine.interrupts == 1 and theirs.interrupts == 0


@pytest.mark.usefixtures("_clean_registry")
def test_a_real_duckdb_query_is_killed_when_its_request_is_abandoned():
    app = FastAPI()
    app.middleware("http")(telemetry_middleware)
    outcome: dict[str, object] = {}
    started = threading.Event()
    finished = threading.Event()

    @app.get("/slow")
    async def slow():
        raw = duckdb.connect(":memory:")
        con = InstrumentedDuckDBConnection(raw, service_id="svc")
        safety = threading.Timer(15, raw.interrupt)
        safety.daemon = True
        safety.start()

        def run() -> None:
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
            await asyncio.sleep(0.3)
            request_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request_task
            await asyncio.get_running_loop().run_in_executor(None, finished.wait, 12)

    asyncio.run(scenario())
    assert finished.is_set(), "the orphaned query must be interrupted, not left to run to completion"
    assert outcome["result"] == "InterruptException"
    assert outcome["seconds"] < 8  # type: ignore[operator]


@pytest.mark.usefixtures("_clean_registry")
def test_tcp_disconnect_via_asgi_receive_interrupts_sync_duckdb_query():
    conn = _Conn()
    disconnected = False

    async def receive():
        nonlocal disconnected
        if not disconnected:
            await asyncio.sleep(0.05)
        return {"type": "http.disconnect"}

    req = _request("/api/network-health", receive=receive)
    req._stream_consumed = True  # type: ignore[attr-defined]

    async def call_next(request):
        query_registry.register("DuckDB", "SELECT slow_post()", service_id="svc", con=conn, pool_slot="svc#1")
        nonlocal disconnected
        disconnected = True
        for _ in range(20):
            if conn.interrupts > 0:
                break
            await asyncio.sleep(0.05)
        return _Resp(499)

    asyncio.run(telemetry_middleware(req, call_next))
    assert conn.interrupts == 1


@pytest.mark.usefixtures("_clean_registry")
def test_cancelled_key_blocks_subsequent_queries_but_allows_drop_table_cleanup():
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
        con.execute('DROP TABLE IF EXISTS "tmp_cleanup"')
    finally:
        current_attribution.reset(token)
        raw.close()


@pytest.mark.usefixtures("_clean_registry")
def test_query_runner_lazy_con_factory_defers_connection_checkout():
    calls = 0
    raw = duckdb.connect(":memory:")

    def factory():
        nonlocal calls
        calls += 1
        return raw

    try:
        runner = QueryRunner(factory, {"name": "test_svc"})
        assert calls == 0, "con_factory must not be called on QueryRunner construction"
        res = runner.try_network_rtt_from_rollup(
            "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", top_asns=[15169], has_filters=True
        )
        assert res is None and calls == 0
        assert runner.execute("SELECT 42").fetchone()[0] == 42 and calls == 1
        assert runner.execute("SELECT 43").fetchone()[0] == 43 and calls == 1
    finally:
        raw.close()


# ── Merged from tests/test_openapi_and_contracts.py ──

"""Consolidated tests for test_openapi_and_contracts."""


import os

import pytest

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SNAPSHOT_PATH = os.path.join(REPO_ROOT, "frontend", "openapi.json")


def test_openapi_snapshot_matches_live_app():
    """``frontend/openapi.json`` must match what the running app emits.

    When this fails, regenerate the snapshot locally:

        cd frontend && npm run gen:types

    Then commit ``frontend/openapi.json`` and ``types/api.generated.ts``.

    Failing here is a *signal*, not a flake — every backend change to a
    router, request/response model, or status-code annotation needs the
    frontend types regenerated so the openapi-fetch call sites stay
    type-correct against the real wire format.
    """
    from backend.main import app

    live = app.openapi()

    if not os.path.exists(SNAPSHOT_PATH):
        pytest.fail(f"Snapshot missing: {SNAPSHOT_PATH}\nRun: cd frontend && npm run gen:types")

    with open(SNAPSHOT_PATH) as f:
        snapshot = json.load(f)

    if live == snapshot:
        return

    # Helpful diff: name the top-level keys that drifted so the reader
    # can find the offending router without ploughing through 16k lines.
    live_paths = set(live.get("paths", {}).keys())
    snap_paths = set(snapshot.get("paths", {}).keys())
    added = sorted(live_paths - snap_paths)
    removed = sorted(snap_paths - live_paths)

    live_schemas = set(live.get("components", {}).get("schemas", {}).keys())
    snap_schemas = set(snapshot.get("components", {}).get("schemas", {}).keys())
    added_schemas = sorted(live_schemas - snap_schemas)
    removed_schemas = sorted(snap_schemas - live_schemas)

    # Find paths whose definitions changed (same key, different value)
    shared_paths = live_paths & snap_paths
    changed = sorted(p for p in shared_paths if live["paths"][p] != snapshot["paths"][p])

    msg = ["OpenAPI snapshot is stale. Regenerate with:\n"]
    msg.append("    cd frontend && npm run gen:types\n")
    msg.append("Then commit frontend/openapi.json and frontend/types/api.generated.ts.\n")
    if added:
        msg.append(f"\nRoutes added (in live, not in snapshot): {added}")
    if removed:
        msg.append(f"\nRoutes removed (in snapshot, not in live): {removed}")
    if changed:
        # Cap the list to avoid wall-of-text — first 10 is plenty to
        # point the reader at the offending router.
        shown = changed[:10]
        rest = len(changed) - len(shown)
        msg.append(f"\nRoutes whose schema drifted: {shown}")
        if rest > 0:
            msg.append(f" (+{rest} more)")
    if added_schemas:
        msg.append(f"\nModels added: {added_schemas}")
    if removed_schemas:
        msg.append(f"\nModels removed: {removed_schemas}")
    if not (added or removed or changed or added_schemas or removed_schemas):
        # Top-level diff (version bump, info change, etc.)
        live_keys = set(live.keys())
        snap_keys = set(snapshot.keys())
        msg.append(f"\nTop-level diff: live={sorted(live_keys)} snapshot={sorted(snap_keys)}")
        if "info" in live and "info" in snapshot and live["info"] != snapshot["info"]:
            msg.append(f"\ninfo block changed: live={live['info']} snapshot={snapshot['info']}")

    pytest.fail("\n".join(msg))


# ── Merged from test_openapi_typed_responses.py ──────────────────────────────────────────────


# Empty: every JSON route has a response_model. Never add an entry — type the route instead.
UNTYPED_JSON: frozenset[str] = frozenset()

NON_JSON_EXEMPT = frozenset(
    {
        "GET /api/admin/quarantine/download/{item_id}",
        "GET /api/services/{service_id}/control-room/error-stream",
        "GET /api/admin/events/stream",
        "GET /api/admin/usage-log/export",
        "GET /api/cron-runs/{run_id}/stream",
        "GET /api/download",
        "GET /api/download-all",
        "GET /api/download-folder",
        "GET /api/log-extents/stream",
        "GET /api/security/proxies/export",
        "GET /api/services/{service_id}/realtime-stream",
        "GET /api/share/oauth/authorize",
        "GET /api/share/oauth/callback",
        "GET /js/faro-sdk.js",
        "GET /js/rum.js",
        "POST /api/admin/metadata-cleanup",
        "POST /api/admin/metadata-cleanup/{service_id}",
        "POST /api/admin/reset-logs",
        "POST /api/admin/reset-rum",
        "POST /api/admin/share/backup/export",
        "POST /api/dashboard/raw/csv",
        "POST /api/provision/execute",
        "POST /api/provision/reconcile",
        "POST /api/provision/teardown",
        "POST /api/services/{service_id}/cmcd/disable",
        "POST /api/services/{service_id}/cmcd/enable",
        "POST /api/services/{service_id}/cron-settings",
        "POST /api/services/{service_id}/logging-settings/update",
        "POST /api/services/{service_id}/ngwaf-sync",
        "POST /api/services/{service_id}/rum/disable",
        "POST /api/services/{service_id}/rum/enable",
        "POST /api/services/{service_id}/rum/upgrade",
        "POST /api/services/{service_id}/scoring/disable",
        "POST /api/services/{service_id}/scoring/enable",
    }
)

UNTYPED_JSON_CEILING = 0
_METHODS = ("get", "post", "put", "patch", "delete")


def _is_untyped(schema: dict | None) -> bool:
    if not schema:
        return True
    if "$ref" in schema or "anyOf" in schema or "oneOf" in schema or "allOf" in schema:
        return False
    if schema.get("type") == "array":
        return False
    if schema.get("properties"):
        return False
    if schema.get("type") in (None, "object"):
        return schema.get("additionalProperties") in (None, True, {})
    return False


@pytest.fixture(scope="module")
def untyped_ops() -> set[str]:
    from backend.main import app

    ops: set[str] = set()
    for path, item in app.openapi()["paths"].items():
        for method in _METHODS:
            op = item.get(method)
            if not op:
                continue
            for code, resp in op.get("responses", {}).items():
                json_body = (resp.get("content") or {}).get("application/json")
                if code.startswith("2") and json_body is not None and _is_untyped(json_body.get("schema")):
                    ops.add(f"{method.upper()} {path}")
    return ops


def test_no_new_untyped_json_operations(untyped_ops):
    new = untyped_ops - UNTYPED_JSON - NON_JSON_EXEMPT
    assert not new, f"Add a response_model (extra='allow' + response_model_exclude_unset=True) to: {sorted(new)}"


def test_allowlists_have_no_stale_entries(untyped_ops):
    stale = (UNTYPED_JSON | NON_JSON_EXEMPT) - untyped_ops
    assert not stale, f"Now typed or removed; delete from the allowlists: {sorted(stale)}"


def test_untyped_json_backlog_only_shrinks():
    assert len(UNTYPED_JSON) <= UNTYPED_JSON_CEILING
    assert not UNTYPED_JSON & NON_JSON_EXEMPT


def test_export_downloads_declare_their_body_in_openapi():
    """File-download routes must say what they return so the client never sees ``unknown``."""
    from backend.main import app

    paths = app.openapi()["paths"]

    tf = paths["/api/provision/terraform/export"]["post"]["responses"]["200"]["content"]
    assert tf == {"application/zip": {"schema": {"type": "string", "format": "binary"}}}

    cf = paths["/api/services/{service_id}/custom-fields/export"]["get"]["responses"]["200"]["content"]
    assert cf == {"application/json": {"schema": {"$ref": "#/components/schemas/CustomFieldsExportFile"}}}


def test_custom_fields_export_file_matches_import_body():
    from backend.models.provision import CustomFieldsExportFile, CustomFieldsImportBody

    assert (
        CustomFieldsExportFile.model_json_schema()["properties"]
        == CustomFieldsImportBody.model_json_schema()["properties"]
    )


# ── Merged from test_error_envelope_contract.py ──────────────────────────────────────────────

from backend.main import app

_HTTP_METHODS = ("get", "post", "put", "patch", "delete")
# The middleware/handler-produced codes that DEFAULT_ERROR_RESPONSES
# documents. 422 is deliberately excluded here — it's covered by its own
# shape check (A); this set is the "did the router get the canonical
# mapping at all" signal (B).
_CANONICAL_CODES = {"400", "401", "403", "404", "429", "500", "502", "503"}

# Operations that intentionally keep FastAPI's auto-generated
# ``HTTPValidationError`` 422 instead of the canonical ``ErrorEnvelope``.
# ``GET /api/health`` is live-fuzzed by tests/test_schemathesis_smoke.py and
# has typed query params, so its documented 422 must match the REAL
# request-validation body. There is no app-wide RequestValidationError
# reshaper, so the only accurate documentation for its 422 is
# HTTPValidationError (reshaping the body would be an out-of-scope
# wire-format change). The carve-out asserts that exact shape below so it
# can't silently mask some other wrong model.
_ACCURATE_VALIDATION_422 = {"GET    /api/health"}


def _operations():
    """Yield ``(label, operation_dict)`` for every documented HTTP op."""
    paths = app.openapi()["paths"]
    for path, item in sorted(paths.items()):
        for method, op in item.items():
            if method in _HTTP_METHODS:
                yield f"{method.upper():6} {path}", op


def _ref_of(op: dict, code: str) -> str | None:
    """The response-schema ``$ref`` for ``code``, or None if absent."""
    try:
        return op["responses"][code]["content"]["application/json"]["schema"]["$ref"]
    except (KeyError, TypeError):
        return None


def test_documented_422_is_error_envelope():
    """(A) Any operation documenting a 422 must reference ErrorEnvelope,
    except the explicitly-carved-out auto-validation endpoints."""
    offenders = [
        (label, ref.split("/")[-1])
        for label, op in _operations()
        if label not in _ACCURATE_VALIDATION_422
        and (ref := _ref_of(op, "422")) is not None
        and not ref.endswith("ErrorEnvelope")
    ]
    assert not offenders, (
        "These operations document 422 with a non-canonical model "
        "(add responses=DEFAULT_ERROR_RESPONSES to the router):\n"
        + "\n".join(f"  {label} -> {model}" for label, model in offenders)
    )


def test_carved_out_422_is_the_accurate_auto_validation_shape():
    """The carve-out is tight: each exempted op must document 422 as
    FastAPI's HTTPValidationError (the real request-validation body), not
    some other accidental model — otherwise the exemption would mask a
    genuine drift."""
    ops = dict(_operations())
    for label in _ACCURATE_VALIDATION_422:
        assert label in ops, f"carve-out names a non-existent operation: {label}"
        ref = _ref_of(ops[label], "422")
        assert ref is not None and ref.endswith("HTTPValidationError"), (
            f"{label} is carved out of the ErrorEnvelope-422 rule but its 422 "
            f"is {ref!r}, not HTTPValidationError — remove the carve-out or fix the shape."
        )


def test_every_operation_documents_a_canonical_error_code():
    """(B) Every operation documents at least one canonical error code."""
    missing = [label for label, op in _operations() if not (_CANONICAL_CODES & set(op.get("responses", {}).keys()))]
    assert not missing, (
        "These operations document none of the canonical error codes "
        f"{sorted(_CANONICAL_CODES)} (missing DEFAULT_ERROR_RESPONSES):\n"
        + "\n".join(f"  {label}" for label in missing)
    )


# ── Merged from test_response_contract.py ──────────────────────────────────────────────


import pytest
from pydantic import AliasChoices

from tests._helpers.duckdb_fixtures import make_seeded_logs_con

# Telemetry / debug keys that every repo merges in via `**runner.telemetry()`.
# These are intentionally stripped from the FE-visible payload because the
# FE has its own request tracing. Listing them here documents the
# stripping intent so a new telemetry key surfacing in a different repo
# fails the test.
_ALLOWED_DROPPED_KEYS = {
    "debug_queries",
    "debug_calls",
    "_debug_queries",
    "_debug_calls",
    "_is_cached",
    # The session-scoring reads run through the `_cached` wrapper, which also
    # bakes a per-handler `_section_timings` entry into the envelope (the
    # dashboard repos emit it under the same name). It's telemetry, not data.
    "_section_timings",
    # `insights.get_insights` computes a window-total row count that the FE
    # doesn't currently render. Keeping it as repo-internal telemetry rather
    # than declaring it on InsightsResponse keeps the FE TS surface minimal.
    # Move to the model if/when the FE actually needs it.
    "window_total_requests",
}

_TABLE_NAME = "test_service"
_src = {"name": _TABLE_NAME, "service_id": "tsid"}


@pytest.fixture
def seeded_con():
    """Seeded DuckDB used across every contract test."""
    con = make_seeded_logs_con(_TABLE_NAME, num_rows=20)
    yield con
    con.close()


def _check_no_dropped_keys(repo_result: dict, response_model_cls, label: str):
    """Assert every key in ``repo_result`` is either a declared field on
    the response model OR in the allowlist of intentionally-stripped
    telemetry keys."""
    declared_fields = set(response_model_cls.model_fields.keys())
    for field in response_model_cls.model_fields.values():
        alias = field.validation_alias
        if isinstance(alias, str):
            declared_fields.add(alias)
        elif isinstance(alias, AliasChoices):
            declared_fields.update(choice for choice in alias.choices if isinstance(choice, str))
    repo_keys = set(repo_result.keys())
    dropped = repo_keys - declared_fields - _ALLOWED_DROPPED_KEYS
    assert not dropped, (
        f"{label}: repo returned keys not declared on {response_model_cls.__name__} "
        f"and not in the allowlist: {sorted(dropped)}\n"
        f"Either add them to the model (FE wants them) or add them to "
        f"_ALLOWED_DROPPED_KEYS (intentionally stripped)."
    )


# ── dashboard.get_aggregates ↔ AggregatesResponse ────────────────────────


def test_cache_observation_alias_is_declared_and_preserved():
    from backend.models.common import BaseResponse

    payload = {"_response_cache": {"state": "miss", "ttl_seconds": 15, "prewarmer": False}}
    _check_no_dropped_keys(payload, BaseResponse, "cache observation")
    assert BaseResponse(**payload).model_dump(by_alias=True)["_response_cache"] == payload["_response_cache"]
    with pytest.raises(AssertionError, match="undeclared_key"):
        _check_no_dropped_keys({"undeclared_key": True}, BaseResponse, "invalid observation")


def test_dashboard_aggregates_contract(seeded_con):
    """`get_aggregates` return keys must match `AggregatesResponse` —
    minus the explicit telemetry allowlist. Pinned because losing this
    would let a repo refactor silently drop a field the FE renders
    (the regression mode of the `window_total_requests` bug)."""
    from backend.models.dashboard import AggregatesResponse

    from backend.repositories.dashboard import _dashboard_cache, get_aggregates

    _dashboard_cache.clear()
    result = get_aggregates(
        con=seeded_con,
        src=_src,
        start_time=None,
        end_time=None,
        filters={},
        chart_interval="1 minute",
        chart_metric="requests",
    )
    _check_no_dropped_keys(result, AggregatesResponse, "dashboard.get_aggregates")


# ── insights.get_insights ↔ InsightsResponse ─────────────────────────────


def test_insights_get_insights_contract(seeded_con):
    """`get_insights` return keys must match `InsightsResponse`.

    Regression-pinned for the original drop: ``window_total_requests``
    was in the repo's return dict but not declared on
    ``InsightsResponse``, so Pydantic silently stripped it and the FE
    saw `undefined`. The fix was to add it to the model — this test
    keeps it from regressing."""
    from backend.models.insights import InsightsResponse

    from backend.repositories.insights import _insights_cache, get_insights

    _insights_cache.clear()
    with patch("backend.utils.pop_utils.get_pop_lat_lon_map", return_value={"LAX": (33.94, -118.4)}):
        result = get_insights(seeded_con, _src, window_hours=1, baseline_hours=1)
    _check_no_dropped_keys(result, InsightsResponse, "insights.get_insights")


# ── origin.get_summary ↔ OriginSummaryResponse ───────────────────────────


def test_origin_get_summary_contract(seeded_con):
    """`get_summary` return keys must match `OriginSummaryResponse`."""
    from backend.models.origin import OriginSummaryResponse

    from backend.repositories.origin import get_summary

    result = get_summary(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, OriginSummaryResponse, "origin.get_summary")


# ── origin.get_timeseries ↔ OriginTimeseriesResponse ─────────────────────


def test_origin_get_timeseries_contract(seeded_con):
    """`get_timeseries` return keys must match `OriginTimeseriesResponse`."""
    from backend.models.origin import OriginTimeseriesResponse

    from backend.repositories.origin import get_timeseries

    result = get_timeseries(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, OriginTimeseriesResponse, "origin.get_timeseries")


# ── performance.get_performance_aggregates ↔ PerformanceAggregatesResponse


def test_performance_aggregates_contract(seeded_con):
    """`get_performance_aggregates` return keys must match
    `PerformanceAggregatesResponse`."""
    from backend.models.performance import PerformanceAggregatesResponse
    from backend.repositories.performance import get_performance_aggregates

    result = get_performance_aggregates(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, PerformanceAggregatesResponse, "performance.get_performance_aggregates")


# ── security.get_security_aggregates ↔ SecurityAggregatesResponse ────────


def test_security_aggregates_contract(seeded_con):
    """`get_security_aggregates` return keys must match
    `SecurityAggregatesResponse`."""
    from backend.models.security import SecurityAggregatesResponse

    from backend.repositories.security import get_security_aggregates

    result = get_security_aggregates(seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, SecurityAggregatesResponse, "security.get_security_aggregates")


# ── network.get_health ↔ NetworkHealthResponse ───────────────────────────


def test_network_health_contract(seeded_con):
    """`get_health` return keys must match `NetworkHealthResponse`."""
    from backend.models.network import NetworkHealthResponse

    from backend.repositories.network import get_health

    result = get_health(lambda: seeded_con, _src, None, None, {})
    _check_no_dropped_keys(result, NetworkHealthResponse, "network.get_health")


# ── Session-scoring analyst-safe reads ↔ scoring response models ──────────
#
# Unlike the repo contracts above (which take a `con`), the scoring reads are
# router handlers that pull rows via `_query_logs` and route through `_cached`,
# so we patch `_query_logs`/`_table_columns`/labels and call the handler
# directly with a stub admin request (no analyst session → maximal key set).
# The handlers' `extra="allow"` + `response_model_exclude_unset` mean nothing
# is stripped at runtime; this guard keeps each DECLARED model a superset of
# the produced shape so the generated TS types stay complete and a future key
# can't slip past the FE contract.

import types
from unittest.mock import patch as _patch


def _admin_request():
    """Stub Request on the admin (loopback) path — no analyst session."""
    return types.SimpleNamespace(state=types.SimpleNamespace(analyst_session=None))


def _check_scoring_keys(produced: dict, model_cls, label: str):
    _check_no_dropped_keys(produced, model_cls, label)


def test_scoring_top_flagged_contract():
    from backend.models.session_scoring import ScoringTopFlaggedResponse, ScoringTopFlaggedRow

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [
        {
            "timestamp": "2026-06-01T10:00:00+00:00",
            "edge_sid": "a",
            "edge_score": 90,
            "edge_score_l1": 40,
            "edge_score_l2": 50,
            "edge_cookie_compliance": "ok",
            "edge_score_reason": "rare-transition",
            "ip": "1.1.1.1",
            "ua": "UA",
            "url": "/p",
            "status": 200,
            "country": "US",
        }
    ]
    with _patch("backend.repositories.session_scoring.query_logs", return_value=rows):
        result = ss.scoring_top_flagged(request=_admin_request(), service_id="csvc", since_hours=24, limit=50)
    _check_scoring_keys(result, ScoringTopFlaggedResponse, "scoring.top_flagged")
    _check_no_dropped_keys(result["rows"][0], ScoringTopFlaggedRow, "scoring.top_flagged[row]")


def test_scoring_score_distribution_contract():
    from backend.models.session_scoring import ScoringDistributionRow, ScoringScoreDistributionResponse

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [{"hour": "2026-06-01T10:00:00+00:00", "bucket": "75-100", "count": 5}]
    with _patch("backend.repositories.session_scoring.query_logs", return_value=rows):
        result = ss.scoring_score_distribution(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringScoreDistributionResponse, "scoring.score_distribution")
    _check_no_dropped_keys(result["rows"][0], ScoringDistributionRow, "scoring.score_distribution[row]")


def test_scoring_compliance_breakdown_contract():
    from backend.models.session_scoring import ScoringComplianceBreakdownResponse, ScoringComplianceRow

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [{"hour": "2026-06-01T10:00:00+00:00", "compliance": "ok", "count": 200}]
    with _patch("backend.repositories.session_scoring.query_logs", return_value=rows):
        result = ss.scoring_compliance_breakdown(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringComplianceBreakdownResponse, "scoring.compliance_breakdown")
    _check_no_dropped_keys(result["rows"][0], ScoringComplianceRow, "scoring.compliance_breakdown[row]")


def test_scoring_latency_timeseries_contract():
    """DICT-01: the latency rows carry the column-dependent percentile fields
    only on re-provisioned services — the maximal row must be a subset of
    ScoringLatencyRow so the FE useScorerTimeseries types stay complete."""
    from backend.models.session_scoring import ScoringLatencyRow, ScoringLatencyTimeseriesResponse

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    rows = [
        {
            "hour": "2026-06-17T10:00:00+00:00",
            "scored_count": 500,
            "fail_open_count": 30,
            "rtt_p50_us": 8000,
            "rtt_p95_us": 45000,
            "rtt_p99_us": 95000,
            "exec_p50_us": 540,
            "exec_p95_us": 900,
        }
    ]
    cols = {"edge_score", "edge_score_reason", "edge_score_rtt_us", "edge_score_exec_us"}
    with (
        _patch("backend.repositories.session_scoring.query_logs", return_value=rows),
        _patch.object(ss, "_table_columns", lambda sid: cols),
    ):
        result = ss.scoring_latency_timeseries(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringLatencyTimeseriesResponse, "scoring.latency_timeseries")
    _check_no_dropped_keys(result["rows"][0], ScoringLatencyRow, "scoring.latency_timeseries[row]")


def test_scoring_health_contract():
    from backend.models.session_scoring import (
        ScoringHealthResponse,
        ScoringLatencySnapshot,
        ScoringMatrixStaleness,
        ScoringReasonCount,
    )

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    agg = [
        {
            "total_edge_rows": 1000,
            "scored_rows": 800,
            "distinct_sids": 50,
            "avg_score": 12.5,
            "p50_score": 5,
            "p95_score": 60,
            "max_score": 100,
            "scorer_errors": 40,
            "top_reasons": [{"reason": "cookie-missing", "count": 10}],
            "fail_open_breakdown": [{"reason": "compute-unavailable-503", "count": 2}],
            "l2_evaluated": 200,
            "l2_high_count": 10,
            "rtt_p50_us": 8000,
            "rtt_p95_us": 42000,
            "rtt_p99_us": 91000,
            "rtt_max_us": 100000,
            "exec_p50_us": 540,
            "exec_p95_us": 880,
        }
    ]
    cols = {
        "edge_score",
        "edge_score_l2",
        "edge_score_reason",
        "edge_cookie_compliance",
        "edge_sid",
        "edge_score_rtt_us",
        "edge_score_exec_us",
    }
    with (
        _patch("backend.repositories.session_scoring.query_logs", return_value=agg),
        _patch.object(ss, "_table_columns", lambda sid: cols),
        _patch("backend.routers.session_scoring_admin.l2_enforce_readiness_block", lambda sid: {"enabled": False}),
    ):
        result = ss.scoring_health(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringHealthResponse, "scoring.health")
    _check_no_dropped_keys(result["latency"], ScoringLatencySnapshot, "scoring.health.latency")
    _check_no_dropped_keys(result["matrix_staleness"], ScoringMatrixStaleness, "scoring.health.matrix_staleness")
    _check_no_dropped_keys(result["top_reasons"][0], ScoringReasonCount, "scoring.health.top_reasons[item]")


def test_scoring_evaluation_contract():
    """Exercise the scored branch (the maximal key set: auc / passed /
    threshold / default_min_auc / n_reconstructed / n_labels_total)."""
    from backend.models.session_scoring import ScoringEvaluationResponse

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    result_obj = types.SimpleNamespace(n_good=5, n_bad=5, auc=0.9, passed=True, pass_threshold=0.7)
    with (
        _patch("backend.scoring.labels.counts_by_label", return_value={"good": 5, "bad": 5, "neutral": 1}),
        _patch("backend.scoring.labels.list_labels", return_value=[{"id": "1", "label": "good", "sid": "s1"}]),
        _patch.object(ss, "_load_matrix", lambda sid: {"version": "v9", "transitions": {}}),
        _patch.object(ss, "_reconstruct_labeled_sessions", lambda sid, labels: [({"max_edge_score": 90}, "bad")]),
        _patch("backend.scoring.evaluate.evaluate_from_persisted_scores", return_value=result_obj),
        _patch("backend.config.load_config", return_value={"scoring": {"matrix_version": "v8"}}),
    ):
        result = ss.scoring_evaluation(service_id="csvc")
    _check_scoring_keys(result, ScoringEvaluationResponse, "scoring.evaluation")


def test_scoring_curves_contract():
    """Exercise the scored branch (roc / pr / auc / average_precision)."""
    from backend.models.session_scoring import ScoringCurvePoint, ScoringCurvesResponse

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    sessions = [({"max_edge_score": 90}, "bad"), ({"max_edge_score": 5}, "good")]
    with (
        _patch("backend.scoring.labels.counts_by_label", return_value={"good": 5, "bad": 5}),
        _patch("backend.scoring.labels.list_labels", return_value=[{"id": "1", "label": "good", "sid": "s1"}]),
        _patch.object(ss, "_reconstruct_labeled_sessions", lambda sid, labels: sessions),
    ):
        result = ss.scoring_curves(service_id="csvc")
    _check_scoring_keys(result, ScoringCurvesResponse, "scoring.curves")
    if result.get("roc"):
        _check_no_dropped_keys(result["roc"][0], ScoringCurvePoint, "scoring.curves.roc[point]")
    if result.get("pr"):
        _check_no_dropped_keys(result["pr"][0], ScoringCurvePoint, "scoring.curves.pr[point]")


def test_scoring_threshold_preview_contract():
    from backend.models.session_scoring import ScoringThresholdBucket, ScoringThresholdPreviewResponse

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    agg = [
        {
            "total": 100,
            "flagged_total": 20,
            "flagged_good": 1,
            "flagged_bad": 5,
            "passed_good": 10,
            "passed_bad": 2,
        }
    ]
    with (
        _patch("backend.repositories.session_scoring.query_logs", return_value=agg),
        _patch("backend.scoring.labels.list_labels", return_value=[{"sid": "s1", "label": "bad"}]),
        _patch("backend.scoring.labels.counts_by_label", return_value={"good": 5, "bad": 5}),
    ):
        result = ss.scoring_threshold_preview(request=_admin_request(), service_id="csvc", threshold=75, since_hours=24)
    _check_scoring_keys(result, ScoringThresholdPreviewResponse, "scoring.threshold_preview")
    _check_no_dropped_keys(result["flagged"], ScoringThresholdBucket, "scoring.threshold_preview.flagged")
    _check_no_dropped_keys(result["passed"], ScoringThresholdBucket, "scoring.threshold_preview.passed")


def test_scoring_analytics_composite_contract():
    """Admin path includes all five analyst-safe sub-keys plus the two
    admin-only evaluation blocks — all must be declared on the composite."""
    import backend.routers.session_scoring_admin as ss_admin
    from backend.models.session_scoring import ScoringAnalyticsResponse

    from backend.routers import session_scoring as ss

    ss._analytics_cache.clear()
    with (
        _patch.object(ss, "scoring_top_flagged", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_score_distribution", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_compliance_breakdown", lambda **kw: {"rows": []}),
        _patch.object(ss, "scoring_latency_timeseries", lambda **kw: {"rows": [], "has_latency": False}),
        _patch.object(ss, "scoring_health", lambda **kw: {"since_hours": 24}),
        _patch.object(ss, "scoring_evaluation", lambda **kw: {"has_min_samples": False}),
        _patch.object(ss_admin, "scoring_evaluation_per_reason", lambda **kw: {"reasons": []}),
    ):
        result = ss.scoring_analytics_composite(request=_admin_request(), service_id="csvc", since_hours=24)
    _check_scoring_keys(result, ScoringAnalyticsResponse, "scoring.analytics")


# ── Merged from test_schemathesis_smoke.py ──────────────────────────────────────────────

import hypothesis
import pytest
import schemathesis
from schemathesis import checks

# Hypothesis settings profile: schemathesis fuzzes each endpoint with
# ~100 generated cases by default → ~5 min wall-clock for 3 endpoints.
# Cap at 20 cases per endpoint so the full test runs in <60s and fits
# under the pytest-timeout default. Bump locally with --hypothesis-profile=full
# when chasing a regression.
hypothesis.settings.register_profile(
    "schemathesis_smoke",
    max_examples=20,
    deadline=None,
    derandomize=True,
    suppress_health_check=list(hypothesis.HealthCheck),
)
hypothesis.settings.load_profile("schemathesis_smoke")


# Zero-side-effect read endpoints safe to fuzz without DB / network setup.
# Session-gated endpoints (share/tos, share/heartbeat) will 401 without a
# cookie — schemathesis still validates the 401 conforms to the documented
# ErrorEnvelope schema, which is the load-bearing contract.
ALLOWED_PATHS = frozenset(
    {
        "/api/health",
        "/api/bootstrap",
        "/api/schema",
        "/api/share/auth-config",
        "/api/share/tos",
        "/api/share/heartbeat",
        "/api/admin/app-config/query-monitor",
        "/api/log-fields/catalog",
    }
)

_PATH_REGEX = "|".join(p.replace("/", r"\/") for p in ALLOWED_PATHS)

# Load + filter the schema at module import. Schemathesis' @schema.parametrize
# decorator generates one pytest case per (op, body shape) combination so the
# test surface scales with ALLOWED_PATHS.
schema = schemathesis.openapi.from_asgi("/openapi.json", app)
schema = schema.include(path_regex=rf"^({_PATH_REGEX})$")


@pytest.mark.slow
@pytest.mark.timeout(120)  # schemathesis can take ~60-90s even with the smoke profile
@schema.parametrize()
def test_safe_read_endpoints_conform(case):
    """For every generated (path × header × query × body) case:

    - ``not_a_server_error`` — no input combination produces a 5xx.
    - ``status_code_conformance`` — the returned status code is in the
      route's documented ``responses=`` mapping (router-wide default
      now covers 400/401/403/404/422/429/500/502/503).
    - ``response_schema_conformance`` — the response body validates
      against the documented schema (``ErrorEnvelope`` for 4xx/5xx,
      the per-route ``response_model`` for 2xx).
    """
    response = case.call()
    case.validate_response(
        response,
        checks=(
            checks.not_a_server_error,
            checks.status_code_conformance,
            checks.response_schema_conformance,
        ),
    )


# ── Merged from test_changelog_breaking_parity.py ──────────────────────────────────────────────

import os
import re

from backend.main import app

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_BASELINE_PATH = os.path.join(_REPO_ROOT, "tests", "contract", "openapi_baseline.json")
_CHANGELOG_PATH = os.path.join(_REPO_ROOT, "CHANGELOG.md")


def _normalize_params(text: str) -> str:
    """Collapse ``{anything}`` path params to ``{}`` so a path matches the
    CHANGELOG regardless of which param name each side spells (the spec uses
    ``{service_id}``; prose often writes ``{id}``)."""
    return re.sub(r"\{[^}]+\}", "{}", text)


def _changelog_breaking_section() -> str:
    """Return all ``### Breaking`` sections across the changelog."""
    with open(_CHANGELOG_PATH) as f:
        text = f.read()
    headings = list(re.finditer(r"^### Breaking\s*$", text, flags=re.MULTILINE))
    sections = []
    for heading in headings:
        section_start = heading.end()
        next_version = re.search(r"^## ", text[section_start:], flags=re.MULTILINE)
        section_end = section_start + next_version.start() if next_version else len(text)
        sections.append(text[section_start:section_end])
    return "\n".join(sections)


def test_removed_paths_are_noted_in_changelog():
    with open(_BASELINE_PATH) as f:
        baseline = json.load(f)
    live = app.openapi()

    baseline_paths = set(baseline.get("paths", {}))
    live_paths = set(live.get("paths", {}))
    removed = sorted(baseline_paths - live_paths)

    breaking = _normalize_params(_changelog_breaking_section())
    missing = [p for p in removed if _normalize_params(p) not in breaking]

    assert not missing, (
        "These paths were removed from the OpenAPI surface but are not noted in "
        f"the CHANGELOG ### Breaking section: {missing}\n"
        "Add a Breaking bullet naming each path, then regenerate the baseline "
        "(jq '{paths: (.paths | with_entries(.value = {}))}' frontend/openapi.json > tests/contract/openapi_baseline.json) "
        "as a deliberate release step."
    )
